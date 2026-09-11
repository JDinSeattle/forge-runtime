package runnerclient

// This opt-in measurement uses an already running, operator-provisioned runner.
// It never starts/stops a service, mounts a volume, or removes a container.
import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
	pb "github.com/JDinSeattle/forge-runtime/proto/runner/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const p06Rounds = 5
const p06Poll = 50 * time.Millisecond
const p06Filename = "p06-shared-name.txt"

// Both workspaces execute the identical program; only marker and mode differ.
// The fixed file is checked throughout execution, including after its peer stops.
const p06Program = `import array, fcntl, json, os, pathlib, signal, socket, struct, sys, time
marker, mode = sys.argv[1:]
p = pathlib.Path("p06-shared-name.txt")
p.write_text(marker)
def read(name):
    return pathlib.Path(name).read_text().strip()
def network_facts():
    names = sorted(os.listdir("/sys/class/net"))
    addresses = {name: [] for name in names}
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as control:
        # Linux SIOCGIFCONF returns every IPv4 address, including aliases.
        stride = 40 if struct.calcsize("P") == 8 else 32
        buffer = array.array("B", b"\0" * 16384)
        result = fcntl.ioctl(control.fileno(), 0x8912, struct.pack("iP", len(buffer), buffer.buffer_info()[0]))
        used = struct.unpack_from("i", result)[0]
        if used < 0 or used % stride or used > len(buffer) - stride:
            raise RuntimeError("incomplete IPv4 interface inventory")
        raw = buffer.tobytes()
        for at in range(0, used, stride):
            name = raw[at:at+16].split(b"\0", 1)[0].decode().split(":", 1)[0]
            if name not in addresses or struct.unpack_from("H", raw, at+16)[0] != socket.AF_INET:
                raise RuntimeError("unexpected IPv4 interface record")
            addresses[name].append(socket.inet_ntoa(raw[at+20:at+24]))
        interfaces = []
        for name in names:
            request = struct.pack("256s", name.encode())
            flags = struct.unpack_from("H", fcntl.ioctl(control.fileno(), 0x8913, request), 16)[0]
            interfaces.append({"name": name, "flags": flags,
                "operstate": read("/sys/class/net/" + name + "/operstate"),
                "ipv4": sorted(set(addresses[name]))})
    ipv4_text, ipv6_text = read("/proc/net/route"), read("/proc/net/ipv6_route")
    ipv4 = []
    for line in ipv4_text.splitlines()[1:]:
        fields = line.split()
        if len(fields) != 11:
            raise RuntimeError("malformed IPv4 route")
        decode = lambda value: socket.inet_ntoa(bytes.fromhex(value)[::-1])
        ipv4.append({"interface": fields[0], "destination": decode(fields[1]),
            "gateway": decode(fields[2]), "mask": decode(fields[7]), "flags": int(fields[3], 16)})
    ipv6 = []
    for line in ipv6_text.splitlines():
        fields = line.split()
        if len(fields) != 10:
            raise RuntimeError("malformed IPv6 route")
        decode = lambda value: socket.inet_ntop(socket.AF_INET6, bytes.fromhex(value))
        ipv6.append({"interface": fields[9], "destination": decode(fields[0]),
            "prefix": int(fields[1], 16), "next_hop": decode(fields[4]), "flags": int(fields[8], 16)})
    return {"interfaces": interfaces, "ipv4_routes": ipv4, "ipv6_routes": ipv6,
        "ipv4_route_table": ipv4_text, "ipv6_route_table": ipv6_text}
status = dict(line.split(":", 1) for line in read("/proc/self/status").splitlines() if ":" in line)
facts = {"marker": marker, "uid": os.getuid(), "gid": os.getgid(),
         "memory_max": read("/sys/fs/cgroup/memory.max"), "pids_max": read("/sys/fs/cgroup/pids.max"),
         "cpu_max": read("/sys/fs/cgroup/cpu.max"), "cap_eff": status["CapEff"].strip(),
         "no_new_privs": status["NoNewPrivs"].strip(), "net_devices": sorted(os.listdir("/sys/class/net")),
         "network": network_facts(),
         "docker_socket": os.path.exists("/var/run/docker.sock"),
         "root_readonly": bool(os.statvfs("/").f_flag & os.ST_RDONLY),
         "tmp_bytes": os.statvfs("/tmp").f_blocks * os.statvfs("/tmp").f_frsize}
print("P06_READY " + json.dumps(facts, sort_keys=True), flush=True)
if mode == "cancel":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    child = os.fork()
    if child == 0:
        print("P06_CHILD " + marker, flush=True)
        time.sleep(45)
        os._exit(0)
until = time.monotonic() + (45 if mode == "cancel" else 1.0)
checks = 0
while time.monotonic() < until:
    if p.read_text() != marker:
        print("P06_ISOLATION_FAILURE " + marker, flush=True)
        sys.exit(31)
    checks += 1
    time.sleep(0.02)
print("P06_DONE " + json.dumps({"marker": marker, "checks": checks}), flush=True)
`

type p06Config struct {
	SigningKeyFile string                     `json:"signing_key_file"`
	ArtifactRoot   string                     `json:"artifact_root"`
	DockerHost     string                     `json:"docker_host"`
	DockerBinary   string                     `json:"docker_binary"`
	TestBackend    bool                       `json:"allow_test_backend"`
	Server         ServerConfig               `json:"server"`
	Profiles       map[string]sandbox.Profile `json:"profiles"`
	VolumeSlots    []sandbox.VolumeSpec       `json:"volume_slots"`
}

type p06Interface struct {
	Name      string   `json:"name"`
	Flags     uint32   `json:"flags"`
	Operstate string   `json:"operstate"`
	IPv4      []string `json:"ipv4"`
}

type p06IPv4Route struct {
	Interface   string `json:"interface"`
	Destination string `json:"destination"`
	Gateway     string `json:"gateway"`
	Mask        string `json:"mask"`
	Flags       uint32 `json:"flags"`
}

type p06IPv6Route struct {
	Interface   string `json:"interface"`
	Destination string `json:"destination"`
	Prefix      int    `json:"prefix"`
	NextHop     string `json:"next_hop"`
	Flags       uint32 `json:"flags"`
}

type p06Network struct {
	Interfaces []p06Interface `json:"interfaces"`
	IPv4Routes []p06IPv4Route `json:"ipv4_routes"`
	IPv6Routes []p06IPv6Route `json:"ipv6_routes"`
	IPv4Raw    string         `json:"ipv4_route_table"`
	IPv6Raw    string         `json:"ipv6_route_table"`
}

func p06ValidateNetwork(facts p06Network) error {
	// Linux IFF_UP/IFF_LOOPBACK and route RTF_UP/RTF_REJECT. Device names
	// themselves are never an isolation allowlist.
	const up, loopback, reject = uint32(1), uint32(8), uint32(0x200)
	interfaces := map[string]p06Interface{}
	loopbacks := 0
	for _, device := range facts.Interfaces {
		if device.Name == "" || interfaces[device.Name].Name != "" {
			return errors.New("missing or duplicate network interface")
		}
		interfaces[device.Name] = device
		if device.Flags&loopback != 0 {
			loopbacks++
			if device.Flags&up == 0 {
				return errors.New("loopback is not enabled")
			}
			for _, address := range device.IPv4 {
				ip := net.ParseIP(address)
				if ip == nil || ip.To4() == nil || !ip.IsLoopback() {
					return errors.New("nonloopback IPv4 address on loopback")
				}
			}
		} else if device.Flags&up != 0 || device.Operstate != "down" || len(device.IPv4) != 0 {
			return errors.New("additional interface is enabled, not down, or has an IPv4 address")
		}
	}
	if loopbacks == 0 {
		return errors.New("missing loopback facts")
	}
	for _, route := range facts.IPv4Routes {
		device, exists := interfaces[route.Interface]
		destination, gateway, mask := net.ParseIP(route.Destination), net.ParseIP(route.Gateway), net.ParseIP(route.Mask)
		if !exists || destination.To4() == nil || gateway.To4() == nil || mask.To4() == nil {
			return errors.New("invalid IPv4 route facts")
		}
		prefix, bits := net.IPMask(mask.To4()).Size()
		if bits != 32 {
			return errors.New("invalid IPv4 route mask")
		}
		if route.Flags&up != 0 && route.Flags&reject == 0 && (device.Flags&loopback == 0 || !destination.IsLoopback() || prefix < 8 || !gateway.IsUnspecified()) {
			return errors.New("usable nonloopback IPv4 route")
		}
	}
	for _, route := range facts.IPv6Routes {
		device, exists := interfaces[route.Interface]
		destination, gateway := net.ParseIP(route.Destination), net.ParseIP(route.NextHop)
		if !exists || destination == nil || destination.To4() != nil || gateway == nil || gateway.To4() != nil || route.Prefix < 0 || route.Prefix > 128 {
			return errors.New("invalid IPv6 route facts")
		}
		if route.Flags&up != 0 && route.Flags&reject == 0 && (device.Flags&loopback == 0 || !destination.IsLoopback() || route.Prefix != 128 || !gateway.IsUnspecified()) {
			return errors.New("usable nonloopback IPv6 route")
		}
	}
	return nil
}

type p06Stamp struct {
	UTC string `json:"utc"`
	NS  int64  `json:"since_experiment_start_ns"`
}

// Whitelisted Docker fields exclude container environment and unrelated jobs.
type p06Docker struct {
	ID     string
	Name   string
	Image  string
	Config struct {
		User   string
		Image  string
		Labels map[string]string
	}
	State struct {
		Running, OOMKilled    bool
		Pid, ExitCode         int
		StartedAt, FinishedAt string
	}
	HostConfig struct {
		Memory, MemorySwap, NanoCpus, PidsLimit int64
		ReadonlyRootfs                          bool
		NetworkMode                             string
		CapDrop, SecurityOpt                    []string
		Tmpfs                                   map[string]string
		LogConfig                               struct {
			Type   string
			Config map[string]string
		}
	}
	Mounts []struct {
		Type, Source, Destination string
		RW                        bool
	}
}

type p06Observation struct {
	Begin, End p06Stamp
	Kind       string
	Status     runner.Status `json:",omitempty"`
	Docker     *p06Docker    `json:",omitempty"`
	Output     string        `json:",omitempty"`
	Error      string        `json:",omitempty"`
}

type p06Sample struct {
	ID, Workspace, Mode, Marker         string
	Round                               int
	Request                             runner.OperationRequest
	StartBegin, StartAck                p06Stamp
	StartReturn                         p06Stamp
	StartStatus                         runner.Status
	FirstDaemonLog                      *p06Stamp
	ChildDaemonLogObserved              bool
	FirstTypedInspectLog                *p06Stamp
	CancelBegin, CancelAck              *p06Stamp
	FirstStoppedObservation             *p06Stamp
	LastRunningObservation              *p06Stamp
	FirstRunningObservation             *p06Stamp
	CancelReply                         *runner.Operation
	Final                               runner.Operation
	Observations                        []p06Observation
	ReceiptFile, TypedFile              string
	CgroupPath, BeforeTasks, AfterTasks string
	CgroupRemoved                       bool
	TasksAfterCancelAt                  *p06Stamp
	VerifiedContent                     string
	ReadOperation                       *runner.Operation
	Error                               string `json:",omitempty"`
}

type p06Workspace struct {
	ID                       domain.ID
	Revision                 uint64
	PrepareBegin, PrepareEnd p06Stamp
	Prepared                 runner.Workspace
	Unknown                  bool
	CleanupError             string `json:",omitempty"`
	Stop                     *runner.StopReceipt
	Snapshot                 *runner.Snapshot
	Released                 *runner.ReleaseResult
}

type p06Pair struct {
	Mode                      string
	Round                     int
	IDs                       [2]string
	BothRunning               bool
	DistinctMountSources      bool
	DaemonOverlapNS           int64
	BRunningAfterAStopped     bool
	BObservationAfterAStopped *p06Observation
}

type p06Peer struct {
	At               p06Stamp
	PID              int32
	UID, GID         uint32
	MatchesRunnerPID bool
}

type p06Report struct {
	Schema                                               int    `json:"schema"`
	Passed                                               bool   `json:"passed"`
	Error                                                string `json:"error,omitempty"`
	StartedUTC, EndedUTC                                 string
	RoundsPerMode, ProcessCommands                       int
	PollIntervalNS                                       int64
	ProgramSHA256, ConfigurationSHA256, TestBinarySHA256 string
	RunnerPID                                            int
	RunnerStartTicks, RunnerExecutableSHA256             string
	RunnerBuildInfo                                      string
	RunnerConfigArgumentMatches                          bool
	GRPCConnectionPeers                                  []p06Peer
	GoVersion, GOOS, GOARCH                              string
	LogicalCPUs, GOMAXPROCS                              int
	Profile                                              sandbox.Profile
	ConfiguredVolumeSlots                                int
	DockerVersion                                        json.RawMessage
	Sources                                              map[string]string
	Workspaces                                           []*p06Workspace
	Samples                                              []*p06Sample
	Pairs                                                []*p06Pair
	Distributions                                        map[string]p06Distribution
	Boundary                                             string
}

type p06Distribution struct {
	Count                      int
	MinMS, P50MS, P95MS, MaxMS float64
}

type p06Harness struct {
	c       p06Config
	client  *Client
	signer  *runner.Signer
	objects *os.Root
	dir     string
	start   time.Time
	report  *p06Report
	mu      sync.Mutex // serializes artifact copies from independently completed samples
}

func TestRealRunnerExecutionEvidence(t *testing.T) {
	if os.Getenv("FORGE_RUN_EXECUTION_EVIDENCE") != "1" {
		t.Skip("opt in with FORGE_RUN_EXECUTION_EVIDENCE=1 and an existing real mapped runner")
	}
	raw, err := os.ReadFile(os.Getenv("FORGE_REAL_RUNNER_CONFIG"))
	if err != nil {
		t.Fatal(err)
	}
	var c p06Config
	if err = json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	profile, ok := c.Profiles["python-clamp"]
	if !ok || c.TestBackend || c.DockerHost == "" || c.Server.UnixSocket == "" || len(c.VolumeSlots) != 4 {
		t.Fatal("requires existing real UDS runner, python-clamp profile, and original four-slot pool")
	}
	key, err := os.ReadFile(c.SigningKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := runner.NewSigner(key)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := os.OpenRoot(c.ArtifactRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	start := time.Now()
	dir := os.Getenv("FORGE_EXECUTION_EVIDENCE_DIR")
	if dir == "" {
		dir = filepath.Join("..", "..", "benchmarks", "results", "local", "runner-execution-"+start.UTC().Format("20060102T150405.000000000Z"))
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		t.Fatal("evidence leaf must be new and its parent must exist: ", err)
	}
	r := &p06Report{Schema: 1, StartedUTC: start.UTC().Format(time.RFC3339Nano), RoundsPerMode: p06Rounds, PollIntervalNS: int64(p06Poll), ProgramSHA256: p06Hash([]byte(p06Program)), ConfigurationSHA256: p06Hash(raw), GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, LogicalCPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0), Profile: profile, ConfiguredVolumeSlots: len(c.VolumeSlots), Sources: map[string]string{}, Boundary: "20 process commands in two fresh workspaces on an existing runner; direct typed Start acknowledgement, daemon active-log observation, and typed Inspect terminal-log observation are separate measurements. No PostgreSQL, worker, model, paid API, service restart, mount mutation, or unrelated cleanup. Poll/CLI overhead is included; finite observed samples have no borrowed latency SLO. Operator capabilities do not constitute PostgreSQL lease evidence."}
	h := &p06Harness{c: c, signer: signer, objects: objects, dir: dir, start: start, report: r}
	defer func() {
		r.EndedUTC = time.Now().UTC().Format(time.RFC3339Nano)
		r.Distributions = p06Distributions(r.Samples)
		h.mu.Lock()
		if saveErr := p06JSON(filepath.Join(dir, "report.json"), r); saveErr != nil {
			t.Error(saveErr)
		}
		h.mu.Unlock()
		t.Logf("execution evidence: %s passed=%t process_commands=%d", dir, r.Passed, r.ProcessCommands)
	}()
	if err = h.provenance(); err != nil {
		r.Error = err.Error()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	client, err := h.dial(ctx)
	if err != nil {
		r.Error = err.Error()
		t.Fatal(err)
	}
	h.client = client
	defer client.Close()
	err = h.run(ctx)
	cleanupErr := h.cleanup()
	identityErr := h.checkRunnerIdentity()
	if err = errors.Join(err, cleanupErr, identityErr); err != nil {
		r.Error = err.Error()
		t.Fatal(err)
	}
	r.Passed = true
}

func (h *p06Harness) stamp() p06Stamp {
	now := time.Now()
	return p06Stamp{UTC: now.UTC().Format(time.RFC3339Nano), NS: now.Sub(h.start).Nanoseconds()}
}

func (h *p06Harness) binding(w *p06Workspace) (runner.WorkspaceRequest, error) {
	now := time.Now()
	claims := runner.Claims{TenantID: "p06-execution", RunID: w.ID, WorkspaceID: w.ID, Epoch: 1, IssuedAt: now, ExpiresAt: now.Add(time.Minute), Permissions: []string{"prepare", "execute", "inspect", "cancel", "snapshot", "release"}}
	grant, err := h.signer.Sign(claims, now.Add(2*time.Minute))
	return runner.WorkspaceRequest{TenantID: claims.TenantID, RunID: w.ID, WorkspaceID: w.ID, Epoch: 1, Grant: grant}, err
}

func (h *p06Harness) provenance() error {
	if err := h.runnerIdentity(); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(h.dir, "command.py"), []byte(p06Program), 0600); err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	data, err := os.ReadFile(exe)
	if err != nil {
		return err
	}
	h.report.TestBinarySHA256 = p06Hash(data)
	// Retain the exact test executable, not only an ephemeral build pathname.
	if err = os.WriteFile(filepath.Join(h.dir, "runnerclient.test"), data, 0700); err != nil {
		return err
	}
	for _, path := range []string{"internal/runnerclient/execution_benchmark_test.go", "internal/runnerclient/client.go", "internal/runnerclient/mapping.go", "internal/runnerclient/server.go", "internal/runner/engine.go", "internal/runner/files.go", "internal/runner/journal.go", "internal/sandbox/docker.go", "proto/runner/v1/runner.proto", "go.mod", "go.sum"} {
		data, err = os.ReadFile(filepath.Join("..", "..", filepath.FromSlash(path)))
		if err != nil {
			return err
		}
		h.report.Sources[path] = p06Hash(data)
		name := filepath.Join(h.dir, "source", filepath.FromSlash(path)+".txt")
		if err = os.MkdirAll(filepath.Dir(name), 0700); err != nil {
			return err
		}
		if err = os.WriteFile(name, data, 0600); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data, err = h.docker(ctx, "version", "--format", "{{json .}}")
	if err != nil {
		return err
	}
	if !json.Valid(data) {
		return errors.New("Docker version did not return JSON")
	}
	h.report.DockerVersion = append(json.RawMessage(nil), data...)
	return nil
}

// The actual gRPC transport uses this dialer, rather than a separate probe of
// the pathname. SO_PEERCRED is a local connection-creation observation, not
// cryptographic session attestation. Every reconnect must match the same PID.
func (h *p06Harness) dial(ctx context.Context) (*Client, error) {
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, "unix", h.c.Server.UnixSocket)
		if err != nil {
			return nil, err
		}
		uc, ok := conn.(*net.UnixConn)
		if !ok {
			conn.Close()
			return nil, errors.New("unexpected non-Unix gRPC connection")
		}
		fd, err := uc.SyscallConn()
		if err != nil {
			conn.Close()
			return nil, err
		}
		var cred *unix.Ucred
		var socketErr error
		if err = fd.Control(func(raw uintptr) { cred, socketErr = unix.GetsockoptUcred(int(raw), unix.SOL_SOCKET, unix.SO_PEERCRED) }); err != nil {
			conn.Close()
			return nil, err
		}
		if socketErr != nil || cred == nil {
			conn.Close()
			return nil, errors.Join(socketErr, errors.New("missing UDS peer credentials"))
		}
		peer := p06Peer{At: h.stamp(), PID: cred.Pid, UID: cred.Uid, GID: cred.Gid, MatchesRunnerPID: int(cred.Pid) == h.report.RunnerPID}
		h.mu.Lock()
		h.report.GRPCConnectionPeers = append(h.report.GRPCConnectionPeers, peer)
		h.mu.Unlock()
		if !peer.MatchesRunnerPID {
			conn.Close()
			return nil, errors.New("actual gRPC UDS peer PID differs from inspected runner PID")
		}
		return conn, nil
	}
	call, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(call, "passthrough:///p06-runner-unix", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dialer), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(8<<20), grpc.MaxCallSendMsgSize(8<<20)), grpc.WithDisableRetry(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, rpc: pb.NewRunnerServiceClient(conn), timeout: 15 * time.Second, reconcileTimeout: 5 * time.Second}, nil
}

func p06ProcessStart(pid int) (string, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}
	// comm can contain spaces or parentheses; fields after its final ')' begin
	// with process state (field 3), so field 22 is element 19.
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return "", errors.New("invalid runner /proc stat")
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) <= 19 {
		return "", errors.New("short runner /proc stat")
	}
	if _, err = strconv.ParseUint(fields[19], 10, 64); err != nil {
		return "", err
	}
	return fields[19], nil
}

func (h *p06Harness) runnerIdentity() error {
	pid, err := strconv.Atoi(os.Getenv("FORGE_REAL_RUNNER_PID"))
	if err != nil || pid <= 0 {
		return errors.New("FORGE_REAL_RUNNER_PID must identify the existing forge-runner process, not rootlesskit")
	}
	start, err := p06ProcessStart(pid)
	if err != nil {
		return err
	}
	cmdline, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		return err
	}
	args := strings.Split(strings.TrimRight(string(cmdline), "\x00"), "\x00")
	if len(args) == 0 || !strings.Contains(filepath.Base(args[0]), "forge-runner") {
		return errors.New("PID is not a forge-runner executable")
	}
	expectedConfig, err := filepath.Abs(os.Getenv("FORGE_REAL_RUNNER_CONFIG"))
	if err != nil {
		return err
	}
	for i, arg := range args {
		if arg == "-config" && i+1 < len(args) && args[i+1] == expectedConfig {
			h.report.RunnerConfigArgumentMatches = true
		}
		if arg == "-config="+expectedConfig {
			h.report.RunnerConfigArgumentMatches = true
		}
	}
	if !h.report.RunnerConfigArgumentMatches {
		return errors.New("runner PID does not name the supplied configuration path")
	}
	procExe := fmt.Sprintf("/proc/%d/exe", pid)
	data, err := os.ReadFile(procExe)
	if err != nil {
		return err
	}
	h.report.RunnerPID, h.report.RunnerStartTicks, h.report.RunnerExecutableSHA256 = pid, start, p06Hash(data)
	if err = os.WriteFile(filepath.Join(h.dir, "forge-runner.executed"), data, 0700); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	build, err := exec.CommandContext(ctx, "go", "version", "-m", procExe).CombinedOutput()
	if err != nil {
		return fmt.Errorf("read runner build info: %w", err)
	}
	h.report.RunnerBuildInfo = string(build)
	return h.checkRunnerIdentity()
}

func (h *p06Harness) checkRunnerIdentity() error {
	start, err := p06ProcessStart(h.report.RunnerPID)
	if err != nil {
		return err
	}
	if start != h.report.RunnerStartTicks {
		return errors.New("runner process identity changed during experiment")
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/exe", h.report.RunnerPID))
	if err != nil {
		return err
	}
	if p06Hash(data) != h.report.RunnerExecutableSHA256 {
		return errors.New("runner executable changed during experiment")
	}
	data, err = os.ReadFile(os.Getenv("FORGE_REAL_RUNNER_CONFIG"))
	if err != nil {
		return err
	}
	if p06Hash(data) != h.report.ConfigurationSHA256 {
		return errors.New("configuration changed during experiment")
	}
	return nil
}

func (h *p06Harness) run(ctx context.Context) error {
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	for i := 0; i < 2; i++ {
		w := &p06Workspace{ID: domain.ID(fmt.Sprintf("p06-%s-%d", hex.EncodeToString(nonce[:]), i)), Revision: 1}
		h.report.Workspaces = append(h.report.Workspaces, w)
		b, err := h.binding(w)
		if err != nil {
			return err
		}
		w.PrepareBegin = h.stamp()
		w.Prepared, err = h.client.PrepareWorkspace(ctx, runner.PrepareRequest{WorkspaceRequest: b, SourceID: "clamp", ProfileID: "python-clamp"})
		w.PrepareEnd = h.stamp()
		if err != nil {
			return fmt.Errorf("prepare own workspace %s: %w", w.ID, err)
		}
		w.Revision = w.Prepared.Revision
	}
	for _, mode := range []string{"normal", "cancel"} {
		for round := 0; round < p06Rounds; round++ {
			if err := h.pair(ctx, mode, round); err != nil {
				return err
			}
		}
	}
	if h.report.ProcessCommands != 4*p06Rounds {
		return errors.New("unexpected process command count")
	}
	return nil
}

func (h *p06Harness) pair(ctx context.Context, mode string, round int) error {
	pair := &p06Pair{Mode: mode, Round: round}
	h.report.Pairs = append(h.report.Pairs, pair)
	samples := [2]*p06Sample{}
	for i, w := range h.report.Workspaces {
		samples[i] = &p06Sample{ID: fmt.Sprintf("%s-%s-%d", w.ID, mode, round), Workspace: string(w.ID), Mode: mode, Round: round, Marker: fmt.Sprintf("%s/%s/%d", w.ID, mode, round)}
		pair.IDs[i] = samples[i].ID
		h.report.Samples = append(h.report.Samples, samples[i])
	}
	var wg sync.WaitGroup
	errs := [2]error{}
	for i := range samples {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := samples[i]
			errs[i] = h.startOperation(ctx, h.report.Workspaces[i], s, "run_command", runner.CommandArgs{Command: []string{"python", "-I", "-B", "-c", p06Program, s.Marker, mode}})
		}(i)
	}
	wg.Wait()
	for _, s := range samples {
		if s.StartStatus != "" {
			h.report.ProcessCommands++
		}
	}
	if err := errors.Join(errs[:]...); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i, s := range samples {
			if err := h.observe(ctx, h.report.Workspaces[i], s); err != nil {
				return err
			}
		}
		if samples[0].LastRunningObservation != nil && samples[1].LastRunningObservation != nil {
			a, b := p06LastDocker(samples[0]), p06LastDocker(samples[1])
			if a != nil && b != nil && a.State.Running && b.State.Running {
				pair.BothRunning = true
				pair.DistinctMountSources = p06Mount(a) != "" && p06Mount(a) != p06Mount(b)
			}
		}
		if mode == "cancel" && samples[0].FirstDaemonLog != nil && samples[1].FirstDaemonLog != nil && samples[0].ChildDaemonLogObserved && samples[1].ChildDaemonLogObserved && pair.BothRunning {
			for i, s := range samples {
				if err := h.captureCgroup(ctx, s); err != nil {
					return err
				}
				if i == 0 {
					if err := h.cancelOperation(ctx, h.report.Workspaces[0], s); err != nil {
						return err
					}
					obs, err := h.dockerObservation(ctx, samples[1])
					if err != nil {
						return err
					}
					pair.BObservationAfterAStopped = &obs
					pair.BRunningAfterAStopped = obs.Docker != nil && obs.Docker.State.Running
					if !pair.BRunningAfterAStopped {
						return errors.New("peer B stopped when only A was cancelled")
					}
				} else if err := h.cancelOperation(ctx, h.report.Workspaces[1], s); err != nil {
					return err
				}
			}
			break
		}
		if samples[0].Final.Status.Terminal() && samples[1].Final.Status.Terminal() {
			break
		}
		timer := time.NewTimer(p06Poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if !pair.BothRunning || !pair.DistinctMountSources {
		return errors.New("missing simultaneous running/distinct mount observations")
	}
	a, b := p06LastDocker(samples[0]), p06LastDocker(samples[1])
	if a == nil || b == nil {
		return errors.New("missing final daemon facts for overlap audit")
	}
	astart, e1 := time.Parse(time.RFC3339Nano, a.State.StartedAt)
	bstart, e2 := time.Parse(time.RFC3339Nano, b.State.StartedAt)
	aend, e3 := time.Parse(time.RFC3339Nano, a.State.FinishedAt)
	bend, e4 := time.Parse(time.RFC3339Nano, b.State.FinishedAt)
	if err := errors.Join(e1, e2, e3, e4); err != nil {
		return err
	}
	start, end := astart, aend
	if bstart.After(start) {
		start = bstart
	}
	if bend.Before(end) {
		end = bend
	}
	pair.DaemonOverlapNS = end.Sub(start).Nanoseconds()
	if pair.DaemonOverlapNS <= 0 {
		return errors.New("actual daemon process lifetime intervals did not overlap")
	}
	for i, s := range samples {
		if s.FirstDaemonLog == nil || s.FirstTypedInspectLog == nil {
			return errors.New("missing separate active-daemon or terminal-Inspect log observation")
		}
		if mode == "normal" && s.Final.Status != runner.Succeeded {
			return fmt.Errorf("normal command %s ended %s", s.ID, s.Final.Status)
		}
		if mode == "cancel" && (s.Final.Status != runner.Cancelled || s.CancelBegin == nil || s.FirstStoppedObservation == nil) {
			return fmt.Errorf("cancel command %s lacks confirmed cancellation", s.ID)
		}
		if err := h.validateFinal(s); err != nil {
			return err
		}
		if err := h.saveOperation(s, s.Final); err != nil {
			return err
		}
		h.report.Workspaces[i].Revision = s.Final.AfterRevision
		if err := h.readMarker(ctx, h.report.Workspaces[i], s); err != nil {
			return err
		}
	}
	return nil
}

func (h *p06Harness) startOperation(ctx context.Context, w *p06Workspace, s *p06Sample, kind string, args any) error {
	b, err := h.binding(w)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(args)
	if err != nil {
		return err
	}
	encoded, err = domain.CanonicalJSON(encoded)
	if err != nil {
		return err
	}
	r := runner.OperationRequest{WorkspaceRequest: b, OperationID: domain.ID(s.ID), ExpectedRevision: w.Revision, Kind: kind, Args: encoded, ArgsHash: p06Hash(encoded), PolicyVersion: "p06-fixed-local-v1", Deadline: time.Now().Add(55 * time.Second)}
	request, err := startRequest(r)
	if err != nil {
		return err
	}
	s.Request = r
	s.Request.Grant = "" // grants must never enter the evidence bundle
	call, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	s.StartBegin = h.stamp()
	response, err := h.client.rpc.StartOperation(call, request) // direct typed RPC: no hidden Inspect fallback
	s.StartReturn = h.stamp()
	if err != nil {
		w.Unknown = true
		s.Error = "direct Start RPC did not acknowledge"
		return fmt.Errorf("%s: %w", s.ID, err)
	}
	s.StartAck = s.StartReturn
	o, err := fromOperation(response)
	if err != nil {
		w.Unknown = true
		return err
	}
	o.Request.Grant = ""
	if !p06SameIntent(s.Request, o.Request) {
		w.Unknown = true
		return errors.New("Start acknowledgement differs from submitted immutable intent")
	}
	s.StartStatus, s.Final = o.Status, o
	return nil
}

func (h *p06Harness) observe(ctx context.Context, w *p06Workspace, s *p06Sample) error {
	if s.Final.Status.Terminal() && s.FirstTypedInspectLog != nil {
		return nil
	}
	b, err := h.binding(w)
	if err != nil {
		return err
	}
	obs := p06Observation{Kind: "typed_inspect", Begin: h.stamp()}
	o, err := h.client.InspectOperation(ctx, runner.InspectRequest{WorkspaceRequest: b, OperationID: domain.ID(s.ID)})
	obs.End = h.stamp()
	if err != nil {
		obs.Error = err.Error()
		s.Observations = append(s.Observations, obs)
		return err
	}
	o.Request.Grant = ""
	if !p06SameIntent(s.Request, o.Request) {
		w.Unknown = true
		return errors.New("Inspect reply differs from submitted immutable intent")
	}
	obs.Status = o.Status
	if o.Status == runner.Unknown {
		w.Unknown = true
		s.Final = o
		s.Observations = append(s.Observations, obs)
		return fmt.Errorf("operation %s became unknown", s.ID)
	}
	var job sandbox.Job
	if len(o.Result) > 0 && json.Unmarshal(o.Result, &job) == nil && bytes.Contains(job.Output, []byte("P06_READY ")) {
		obs.Output = string(job.Output)
		if !o.Status.Terminal() {
			return errors.New("unexpected nonterminal typed log result; measurement boundary changed")
		}
		if s.FirstTypedInspectLog == nil {
			stamp := obs.End
			s.FirstTypedInspectLog = &stamp
		}
	}
	s.Observations = append(s.Observations, obs)
	s.Final = o
	_, err = h.dockerObservation(ctx, s)
	return err
}

func (h *p06Harness) docker(ctx context.Context, args ...string) ([]byte, error) {
	call, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	bin := h.c.DockerBinary
	if bin == "" {
		bin = "docker"
	}
	return exec.CommandContext(call, bin, append([]string{"--host", h.c.DockerHost}, args...)...).CombinedOutput()
}

func (h *p06Harness) dockerObservation(ctx context.Context, s *p06Sample) (p06Observation, error) {
	obs := p06Observation{Kind: "docker_inspect", Begin: h.stamp()}
	data, err := h.docker(ctx, "container", "inspect", s.Final.JobID)
	obs.End = h.stamp()
	if err != nil {
		// A durable Start acknowledgement can precede Docker create.
		if strings.Contains(string(data), "No such") {
			obs.Error = "container not yet created"
			s.Observations = append(s.Observations, obs)
			return obs, nil
		}
		return obs, fmt.Errorf("observe owned container: %w", err)
	}
	var rows []p06Docker
	if json.Unmarshal(data, &rows) != nil || len(rows) != 1 || rows[0].Config.Labels["forge.runtime"] != "1" || strings.TrimPrefix(rows[0].Name, "/") != s.Final.JobID {
		return obs, errors.New("unexpected container identity")
	}
	obs.Docker = &rows[0]
	s.Observations = append(s.Observations, obs)
	if rows[0].State.Running {
		stamp := obs.End
		s.LastRunningObservation = &stamp
		if s.FirstRunningObservation == nil {
			s.FirstRunningObservation = &stamp
		}
	}
	if !rows[0].State.Running && !strings.HasPrefix(rows[0].State.StartedAt, "0001-") && rows[0].State.StartedAt != "" && s.FirstStoppedObservation == nil {
		stamp := obs.End
		s.FirstStoppedObservation = &stamp
	}
	if rows[0].State.Running && (s.FirstDaemonLog == nil || (s.Mode == "cancel" && !s.ChildDaemonLogObserved)) {
		logs := p06Observation{Kind: "daemon_logs_while_running", Begin: h.stamp()}
		data, err = h.docker(ctx, "logs", s.Final.JobID)
		logs.End = h.stamp()
		if err != nil {
			return obs, err
		}
		logs.Output = string(data)
		s.Observations = append(s.Observations, logs)
		ready := bytes.Contains(data, []byte("P06_READY "))
		s.ChildDaemonLogObserved = bytes.Contains(data, []byte("P06_CHILD "+s.Marker))
		if ready {
			// A second inspect establishes that the log was observable before exit.
			after, readErr := h.docker(ctx, "container", "inspect", s.Final.JobID)
			var checked []p06Docker
			if readErr != nil || json.Unmarshal(after, &checked) != nil || len(checked) != 1 || !checked[0].State.Running {
				return obs, errors.New("first daemon log was not bracketed by running observations")
			}
			stamp := logs.End
			if s.FirstDaemonLog == nil {
				s.FirstDaemonLog = &stamp
			}
			s.Observations = append(s.Observations, p06Observation{Kind: "docker_running_after_first_log", Begin: logs.End, End: h.stamp(), Docker: &checked[0]})
		}
	}
	return obs, nil
}

func p06LastDocker(s *p06Sample) *p06Docker {
	for i := len(s.Observations) - 1; i >= 0; i-- {
		if s.Observations[i].Docker != nil {
			return s.Observations[i].Docker
		}
	}
	return nil
}

func p06Mount(d *p06Docker) string {
	if d != nil {
		for _, m := range d.Mounts {
			if m.Destination == "/workspace" && m.RW {
				return m.Source
			}
		}
	}
	return ""
}

func (h *p06Harness) captureCgroup(ctx context.Context, s *p06Sample) error {
	data, err := h.docker(ctx, "top", s.Final.JobID, "-eo", "pid,ppid,comm")
	if err != nil {
		return err
	}
	s.Observations = append(s.Observations, p06Observation{Kind: "docker_top_before_cancel", End: h.stamp(), Output: string(data)})
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 4 {
		return errors.New("cancel workload lacks init, parent and child")
	}
	fields := strings.Fields(lines[1])
	if len(fields) < 3 {
		return errors.New("malformed owned container top")
	}
	pid, err := strconv.Atoi(fields[0])
	if err != nil || pid <= 0 {
		return errors.New("invalid owned container PID")
	}
	data, err = os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "0::/") {
			s.CgroupPath = strings.TrimPrefix(line, "0::")
		}
	}
	if s.CgroupPath == "" || s.CgroupPath == "/" {
		return errors.New("missing dedicated cgroup")
	}
	data, err = os.ReadFile(filepath.Join("/sys/fs/cgroup", s.CgroupPath, "cgroup.procs"))
	if err != nil || len(strings.Fields(string(data))) < 3 {
		return errors.New("owned container cgroup does not contain parent/child tasks")
	}
	s.BeforeTasks = string(data)
	return nil
}

func (h *p06Harness) cancelOperation(ctx context.Context, w *p06Workspace, s *p06Sample) error {
	b, err := h.binding(w)
	if err != nil {
		return err
	}
	begin := h.stamp()
	s.CancelBegin = &begin
	o, err := h.client.CancelOperation(ctx, runner.InspectRequest{WorkspaceRequest: b, OperationID: domain.ID(s.ID)})
	ack := h.stamp()
	s.CancelAck = &ack
	if err != nil {
		w.Unknown = true
		return err
	}
	o.Request.Grant = ""
	s.CancelReply = &o
	if !p06SameIntent(s.Request, o.Request) {
		w.Unknown = true
		return errors.New("Cancel reply differs from submitted immutable intent")
	}
	if o.Status != runner.Cancelled {
		if o.Status == runner.Unknown {
			w.Unknown = true
		}
		return fmt.Errorf("Cancel returned %s", o.Status)
	}
	if err = h.observe(ctx, w, s); err != nil {
		return err
	}
	d := p06LastDocker(s)
	if d == nil || d.State.Running {
		return errors.New("cancel acknowledgement left a running container")
	}
	data, err := os.ReadFile(filepath.Join("/sys/fs/cgroup", s.CgroupPath, "cgroup.procs"))
	s.AfterTasks, s.CgroupRemoved = string(data), errors.Is(err, os.ErrNotExist)
	checked := h.stamp()
	s.TasksAfterCancelAt = &checked
	if !s.CgroupRemoved && (err != nil || len(strings.Fields(string(data))) != 0) {
		return errors.New("cancel acknowledgement left controlled descendant tasks")
	}
	return nil
}

func (h *p06Harness) validateFinal(s *p06Sample) error {
	var job sandbox.Job
	if json.Unmarshal(s.Final.Result, &job) != nil || job.Running || !job.Started || job.ID != s.Final.JobID || job.Truncated || s.Final.ResultTruncated {
		return errors.New("invalid final job facts")
	}
	if bytes.Contains(job.Output, []byte("P06_ISOLATION_FAILURE")) {
		return errors.New("concurrent workspace contents crossed")
	}
	if s.Mode == "normal" && (job.ExitCode != 0 || !bytes.Contains(job.Output, []byte("P06_DONE "))) {
		return errors.New("normal command did not complete its bounded checks")
	}
	var facts struct {
		Marker   string     `json:"marker"`
		UID      int        `json:"uid"`
		GID      int        `json:"gid"`
		Memory   string     `json:"memory_max"`
		PIDs     string     `json:"pids_max"`
		CPU      string     `json:"cpu_max"`
		CapEff   string     `json:"cap_eff"`
		NNP      string     `json:"no_new_privs"`
		Net      []string   `json:"net_devices"`
		Network  p06Network `json:"network"`
		Socket   bool       `json:"docker_socket"`
		Readonly bool       `json:"root_readonly"`
		TmpBytes int64      `json:"tmp_bytes"`
	}
	found := false
	for _, line := range strings.Split(string(job.Output), "\n") {
		if strings.HasPrefix(line, "P06_READY ") {
			if json.Unmarshal([]byte(strings.TrimPrefix(line, "P06_READY ")), &facts) != nil {
				return errors.New("invalid probe facts")
			}
			found = true
		}
	}
	p := h.report.Profile
	cpu := strings.Fields(facts.CPU)
	if !found || facts.Marker != s.Marker || facts.UID != 1000 || facts.GID != 1000 || facts.Memory != strconv.FormatInt(p.MemoryBytes, 10) || facts.PIDs != strconv.FormatInt(p.PIDs, 10) || strings.Trim(facts.CapEff, "0") != "" || facts.NNP != "1" || facts.Socket || !facts.Readonly || facts.TmpBytes <= 0 || facts.TmpBytes > 64<<20 || len(cpu) != 2 {
		return errors.New("actual process isolation/resource facts failed")
	}
	if err := p06ValidateNetwork(facts.Network); err != nil {
		return err
	}
	quota, e1 := strconv.ParseFloat(cpu[0], 64)
	period, e2 := strconv.ParseFloat(cpu[1], 64)
	if e1 != nil || e2 != nil || period <= 0 || math.Abs(quota/period-p.CPUs) > 0.001 {
		return errors.New("actual CPU quota differs from profile")
	}
	d := p06LastDocker(s)
	if d == nil || d.Config.Image != p.Image || d.Config.User != p.User || d.HostConfig.Memory != p.MemoryBytes || d.HostConfig.MemorySwap != p.MemoryBytes || d.HostConfig.PidsLimit != p.PIDs || d.HostConfig.NanoCpus != int64(p.CPUs*1e9) || !d.HostConfig.ReadonlyRootfs || d.HostConfig.NetworkMode != "none" || p06Mount(d) == "" {
		return errors.New("actual container configuration differs from pinned profile")
	}
	return nil
}

func (h *p06Harness) readMarker(ctx context.Context, w *p06Workspace, s *p06Sample) error {
	probe := &p06Sample{ID: s.ID + "-read", Workspace: s.Workspace}
	if err := h.startOperation(ctx, w, probe, "read_file", runner.ReadArgs{Path: p06Filename}); err != nil {
		return err
	}
	s.ReadOperation = &probe.Final
	for !probe.Final.Status.Terminal() {
		b, err := h.binding(w)
		if err != nil {
			return err
		}
		probe.Final, err = h.client.InspectOperation(ctx, runner.InspectRequest{WorkspaceRequest: b, OperationID: domain.ID(probe.ID)})
		probe.Final.Request.Grant = ""
		if err != nil {
			return err
		}
		if probe.Final.Status == runner.Unknown {
			w.Unknown = true
			return errors.New("file probe became unknown")
		}
		if !probe.Final.Status.Terminal() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p06Poll):
			}
		}
	}
	probe.Final.Request.Grant = ""
	var result struct{ Content, SHA256 string }
	if probe.Final.Status != runner.Succeeded || json.Unmarshal(probe.Final.Result, &result) != nil || result.Content != s.Marker || result.SHA256 != p06Hash([]byte(s.Marker)) {
		return errors.New("typed read_file did not return workspace's own marker/hash")
	}
	s.VerifiedContent, s.ReadOperation = result.Content, &probe.Final
	w.Revision = probe.Final.AfterRevision
	return h.saveOperation(probe, probe.Final)
}

func (h *p06Harness) saveOperation(s *p06Sample, op runner.Operation) error {
	if err := p06ValidateSampleOperation(s, op); err != nil {
		return err
	}
	if op.Receipt.Kind != "operation_receipt" {
		return errors.New("incorrect operation receipt kind")
	}
	name := "objects/" + op.Receipt.SHA256 + ".json"
	data, err := h.copyRef(op.Receipt, name)
	if err != nil {
		return err
	}
	var receipt runner.Operation
	if json.Unmarshal(data, &receipt) != nil || receipt.Request.Grant != "" {
		return errors.New("invalid or credential-bearing receipt")
	}
	expected := op
	expected.Request.Grant = ""
	expected.Request.Deadline = expected.Request.Deadline.UTC()
	receipt.Request.Deadline = receipt.Request.Deadline.UTC()
	expected.Receipt = artifact.Ref{}
	a, _ := json.Marshal(expected)
	b, _ := json.Marshal(receipt)
	if !bytes.Equal(a, b) {
		return errors.New("actual receipt bytes do not bind complete typed operation facts")
	}
	s.ReceiptFile, s.TypedFile = name, "operations/"+s.ID+".json"
	return p06JSON(filepath.Join(h.dir, s.TypedFile), op)
}

func p06SameIntent(want, got runner.OperationRequest) bool {
	return want.TenantID == got.TenantID && want.RunID == got.RunID && want.WorkspaceID == got.WorkspaceID && want.Epoch == got.Epoch &&
		want.OperationID == got.OperationID && want.ExpectedRevision == got.ExpectedRevision && want.Kind == got.Kind &&
		bytes.Equal(want.Args, got.Args) && want.ArgsHash == got.ArgsHash && want.PolicyVersion == got.PolicyVersion && want.Deadline.Equal(got.Deadline)
}

func p06ValidateSampleOperation(s *p06Sample, op runner.Operation) error {
	if s.Request.OperationID != domain.ID(s.ID) || s.Request.WorkspaceID != domain.ID(s.Workspace) || !p06SameIntent(s.Request, op.Request) {
		return errors.New("result does not bind the actual sample's complete immutable request")
	}
	if op.Receipt.TenantID != s.Request.TenantID || op.Receipt.RunID != s.Request.RunID || !op.Status.Terminal() {
		return errors.New("receipt must bind the current sample tenant/run and a terminal operation")
	}
	return nil
}

func (h *p06Harness) copyRef(ref artifact.Ref, name string) ([]byte, error) {
	if decoded, err := hex.DecodeString(ref.SHA256); err != nil || len(decoded) != 32 {
		return nil, errors.New("artifact digest must be SHA-256 hex")
	}
	if ref.TenantID != "p06-execution" || ref.RunID.Validate() != nil || ref.ObjectKey != string(ref.TenantID)+"/"+string(ref.RunID)+"/"+ref.SHA256 || len(ref.SHA256) != 64 || ref.Size <= 0 || ref.Size > 64<<20 {
		return nil, errors.New("artifact reference binding/size invalid")
	}
	owned := false
	for _, w := range h.report.Workspaces {
		owned = owned || w.ID == ref.RunID
	}
	if !owned {
		return nil, errors.New("artifact is not owned by experiment workspace")
	}
	f, err := h.objects.Open(ref.ObjectKey)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(f, ref.Size+1))
	closeErr := f.Close()
	if err = errors.Join(err, closeErr); err != nil {
		return nil, err
	}
	if int64(len(data)) != ref.Size || p06Hash(data) != ref.SHA256 {
		return nil, errors.New("artifact bytes do not match claimed hash/size")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	path := filepath.Join(h.dir, name)
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		return nil, err
	}
	return data, nil
}

func (h *p06Harness) cleanup() error {
	var errs []error
	for _, w := range h.report.Workspaces {
		if w.Unknown {
			w.CleanupError = "retained: operation outcome unknown"
			errs = append(errs, errors.New(w.CleanupError))
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := func() error {
			b, err := h.binding(w)
			if err != nil {
				return err
			}
			stop, err := h.client.StopWorkspace(ctx, b)
			if err != nil {
				return err
			}
			w.Stop = &stop
			if !stop.NoActiveOperations || !stop.Workspace.Stopped || !p06WorkspaceMatches(b, stop.Workspace) {
				return errors.New("no valid no-active stop proof")
			}
			stopData, err := h.copyRef(stop.Ref, "objects/"+stop.Ref.SHA256+".json")
			if err != nil {
				return err
			}
			if err = p06ValidateStop(stop, stopData); err != nil {
				return err
			}
			snapshot, err := h.client.SealSnapshot(ctx, b)
			if err != nil {
				return err
			}
			w.Snapshot = &snapshot
			if !p06WorkspaceMatches(b, snapshot.Workspace) || snapshot.Hash == "" {
				return errors.New("snapshot binding invalid")
			}
			snapshotData, err := h.copyRef(snapshot.Artifact, "objects/"+snapshot.Artifact.SHA256+".snapshot")
			if err != nil {
				return err
			}
			if err = p06ValidateSnapshot(snapshot, snapshotData); err != nil {
				return err
			}
			released, err := h.client.ReleaseWorkspace(ctx, b)
			if err != nil {
				return err
			}
			w.Released = &released
			if !released.Released || released.WorkspaceID != w.ID {
				return errors.New("release not confirmed")
			}
			return nil
		}()
		cancel()
		if err != nil {
			w.CleanupError = err.Error()
			errs = append(errs, fmt.Errorf("retained own workspace %s: %w", w.ID, err))
		}
	}
	return errors.Join(errs...)
}

func p06WorkspaceMatches(binding runner.WorkspaceRequest, workspace runner.Workspace) bool {
	return binding.TenantID == workspace.TenantID && binding.RunID == workspace.RunID && binding.WorkspaceID == workspace.ID && binding.Epoch == workspace.Epoch
}

func p06ValidateStop(stop runner.StopReceipt, data []byte) error {
	if stop.Ref.Kind != "workspace_stop" || stop.Ref.TenantID != stop.Workspace.TenantID || stop.Ref.RunID != stop.Workspace.RunID || !stop.NoActiveOperations || !stop.Workspace.Stopped || stop.Workspace.ActiveOperation != "" {
		return errors.New("invalid stop receipt facts")
	}
	var observed runner.StopReceipt
	if err := json.Unmarshal(data, &observed); err != nil {
		return err
	}
	expected := stop
	expected.Ref = artifact.Ref{}
	a, _ := json.Marshal(expected)
	b, _ := json.Marshal(observed)
	if !bytes.Equal(a, b) {
		return errors.New("artifact stop proof differs from typed reply")
	}
	return nil
}

func p06ValidateSnapshot(snapshot runner.Snapshot, data []byte) error {
	if snapshot.Artifact.Kind != "workspace_snapshot" || snapshot.Artifact.TenantID != snapshot.Workspace.TenantID || snapshot.Artifact.RunID != snapshot.Workspace.RunID || !snapshot.Workspace.Stopped || snapshot.Workspace.ActiveOperation != "" {
		return errors.New("snapshot does not bind a stopped workspace")
	}
	var observed struct {
		Workspace runner.Workspace `json:"workspace"`
		Files     map[string]struct {
			SHA256     string `json:"sha256"`
			Content    []byte `json:"content"`
			Executable bool   `json:"executable"`
		} `json:"files"`
	}
	if err := json.Unmarshal(data, &observed); err != nil {
		return err
	}
	a, _ := json.Marshal(snapshot.Workspace)
	b, _ := json.Marshal(observed.Workspace)
	if !bytes.Equal(a, b) || len(observed.Files) == 0 {
		return errors.New("snapshot workspace binding/files invalid")
	}
	names := make([]string, 0, len(observed.Files))
	for name, file := range observed.Files {
		if p06Hash(file.Content) != file.SHA256 {
			return errors.New("snapshot contains a mismatched file digest")
		}
		names = append(names, name)
	}
	sort.Strings(names)
	hash := sha256.New()
	for _, name := range names {
		file := observed.Files[name]
		_, _ = fmt.Fprintf(hash, "%d:%s:%s:%t\n", len(name), name, file.SHA256, file.Executable)
	}
	if hex.EncodeToString(hash.Sum(nil)) != snapshot.Hash {
		return errors.New("snapshot file/executable tree hash differs from typed reply")
	}
	return nil
}

func TestP06ArtifactOraclesRejectMismatchedProofs(t *testing.T) {
	w := runner.Workspace{TenantID: "p06-execution", RunID: "owned", ID: "owned", Epoch: 1, Revision: 2, Stopped: true}
	stop := runner.StopReceipt{Workspace: w, NoActiveOperations: true, Ref: artifact.Ref{Kind: "workspace_stop", TenantID: w.TenantID, RunID: w.RunID}}
	stored := stop
	stored.Ref = artifact.Ref{}
	raw, _ := json.Marshal(stored)
	if err := p06ValidateStop(stop, raw); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []func(*runner.StopReceipt){
		func(s *runner.StopReceipt) { s.NoActiveOperations = false },
		func(s *runner.StopReceipt) { s.Workspace.Epoch++ },
		func(s *runner.StopReceipt) { s.Workspace.ActiveOperation = "unsettled" },
	} {
		changed := stored
		mutation(&changed)
		raw, _ = json.Marshal(changed)
		if p06ValidateStop(stop, raw) == nil {
			t.Fatal("accepted mismatched no-active/epoch/active-operation proof")
		}
	}
	fileHash := p06Hash([]byte("marker"))
	file := map[string]any{"sha256": fileHash, "content": []byte("marker"), "executable": false}
	data := map[string]any{"workspace": w, "files": map[string]any{"same.txt": file}}
	snapshot := runner.Snapshot{Workspace: w, Hash: p06Hash([]byte(fmt.Sprintf("8:same.txt:%s:false\n", fileHash))), Artifact: artifact.Ref{Kind: "workspace_snapshot", TenantID: w.TenantID, RunID: w.RunID}}
	raw, _ = json.Marshal(data)
	if err := p06ValidateSnapshot(snapshot, raw); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []func(){
		func() { file["content"] = []byte("peer marker") },
		func() { file["content"] = []byte("marker"); file["executable"] = true },
		func() { file["executable"] = false; wrong := w; wrong.ID = "foreign"; data["workspace"] = wrong },
	} {
		mutation()
		raw, _ = json.Marshal(data)
		if p06ValidateSnapshot(snapshot, raw) == nil {
			t.Fatal("accepted altered content/executable/workspace snapshot")
		}
	}
}

func TestP06IntentOracleRejectsCrossSampleAndAlteredIntent(t *testing.T) {
	deadline := time.Date(2026, 9, 11, 15, 0, 0, 17, time.FixedZone("PDT", -7*60*60))
	request := runner.OperationRequest{WorkspaceRequest: runner.WorkspaceRequest{TenantID: "tenant", RunID: "run-a", WorkspaceID: "ws-a", Epoch: 3, Grant: "not-retained"}, OperationID: "op-a", ExpectedRevision: 9, Kind: "run_command", Args: json.RawMessage(`{"command":["python"]}`), PolicyVersion: "fixed-v1", Deadline: deadline}
	request.ArgsHash = p06Hash(request.Args)
	s := &p06Sample{ID: "op-a", Workspace: "ws-a", Request: request}
	op := runner.Operation{Request: request, Status: runner.Succeeded, Receipt: artifact.Ref{TenantID: "tenant", RunID: "run-a", Kind: "operation_receipt"}}
	op.Request.Grant = ""
	op.Request.Deadline = deadline.UTC()
	if err := p06ValidateSampleOperation(s, op); err != nil {
		t.Fatal("same instant in a different zone/grant must match: ", err)
	}
	mutations := map[string]func(*runner.Operation){
		"tenant":    func(o *runner.Operation) { o.Request.TenantID = "other" },
		"run":       func(o *runner.Operation) { o.Request.RunID = "run-b" },
		"workspace": func(o *runner.Operation) { o.Request.WorkspaceID = "ws-b" },
		"epoch":     func(o *runner.Operation) { o.Request.Epoch++ },
		"operation": func(o *runner.Operation) { o.Request.OperationID = "op-b" },
		"revision":  func(o *runner.Operation) { o.Request.ExpectedRevision++ },
		"kind":      func(o *runner.Operation) { o.Request.Kind = "verify" },
		"args": func(o *runner.Operation) {
			o.Request.Args = json.RawMessage(`{"command":["different"]}`)
			o.Request.ArgsHash = p06Hash(o.Request.Args)
		},
		"args_hash":      func(o *runner.Operation) { o.Request.ArgsHash = strings.Repeat("0", 64) },
		"policy":         func(o *runner.Operation) { o.Request.PolicyVersion = "other-v2" },
		"deadline":       func(o *runner.Operation) { o.Request.Deadline = o.Request.Deadline.Add(time.Nanosecond) },
		"receipt_tenant": func(o *runner.Operation) { o.Receipt.TenantID = "other" },
		"receipt_run":    func(o *runner.Operation) { o.Receipt.RunID = "run-b" },
		"nonterminal":    func(o *runner.Operation) { o.Status = runner.Running },
		"other_self_consistent_sample": func(o *runner.Operation) {
			o.Request.RunID = "run-b"
			o.Request.WorkspaceID = "ws-b"
			o.Request.OperationID = "op-b"
			o.Receipt.RunID = "run-b"
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := op
			mutate(&changed)
			if p06ValidateSampleOperation(s, changed) == nil {
				t.Fatal("accepted different original intent or another sample's receipt")
			}
		})
	}
}

func p06Hash(data []byte) string { digest := sha256.Sum256(data); return hex.EncodeToString(digest[:]) }

func p06JSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0600)
}

func p06Distributions(samples []*p06Sample) map[string]p06Distribution {
	values := map[string][]float64{}
	for _, s := range samples {
		add := func(name string, start, end p06Stamp) {
			if end.NS >= start.NS {
				values[s.Mode+"/"+name] = append(values[s.Mode+"/"+name], float64(end.NS-start.NS)/1e6)
			}
		}
		if s.StartAck.NS != 0 {
			add("direct_start_ack_ms", s.StartBegin, s.StartAck)
		}
		if s.FirstDaemonLog != nil {
			add("start_to_active_daemon_log_ms", s.StartBegin, *s.FirstDaemonLog)
		}
		if s.FirstRunningObservation != nil {
			add("start_to_first_observed_running_upper_bound_ms", s.StartBegin, *s.FirstRunningObservation)
		}
		if s.FirstTypedInspectLog != nil {
			add("start_to_terminal_typed_inspect_log_ms", s.StartBegin, *s.FirstTypedInspectLog)
		}
		if s.CancelBegin != nil && s.CancelAck != nil {
			add("cancel_rpc_ack_ms", *s.CancelBegin, *s.CancelAck)
		}
		if s.CancelBegin != nil && s.FirstStoppedObservation != nil {
			add("cancel_to_first_observed_stopped_upper_bound_ms", *s.CancelBegin, *s.FirstStoppedObservation)
		}
	}
	out := map[string]p06Distribution{}
	for name, v := range values {
		sort.Float64s(v)
		quantile := func(q float64) float64 {
			position := q * float64(len(v)-1)
			lo := int(position)
			hi := int(math.Ceil(position))
			return v[lo] + (v[hi]-v[lo])*(position-float64(lo))
		}
		out[name] = p06Distribution{Count: len(v), MinMS: v[0], P50MS: quantile(.5), P95MS: quantile(.95), MaxMS: v[len(v)-1]}
	}
	return out
}

func TestP06NetworkOracleRejectsUsableExternalConnectivity(t *testing.T) {
	baseline := p06Network{
		Interfaces: []p06Interface{{Name: "lo", Flags: 1 | 8 | 64, Operstate: "unknown", IPv4: []string{"127.0.0.1"}}, {Name: "arbitrary-kernel-device", Flags: 0, Operstate: "down"}},
		IPv4Routes: []p06IPv4Route{{Interface: "lo", Destination: "127.0.0.0", Gateway: "0.0.0.0", Mask: "255.0.0.0", Flags: 1}},
		IPv6Routes: []p06IPv6Route{{Interface: "lo", Destination: "::1", Prefix: 128, NextHop: "::", Flags: 1}, {Interface: "lo", Destination: "::", Prefix: 0, NextHop: "::", Flags: 0x200}},
	}
	if err := p06ValidateNetwork(baseline); err != nil {
		t.Fatalf("down/addressless extra interface and rejected default route are harmless: %v", err)
	}
	mutations := map[string]func(*p06Network){
		"external_interface_up": func(n *p06Network) { n.Interfaces[1].Flags |= 1 },
		"external_operstate_up": func(n *p06Network) { n.Interfaces[1].Operstate = "up" },
		"external_ipv4_address": func(n *p06Network) { n.Interfaces[1].IPv4 = []string{"192.0.2.1"} },
		"nonloopback_alias":     func(n *p06Network) { n.Interfaces[0].IPv4 = append(n.Interfaces[0].IPv4, "192.0.2.2") },
		"ipv4_external_route": func(n *p06Network) {
			n.IPv4Routes = append(n.IPv4Routes, p06IPv4Route{Interface: n.Interfaces[1].Name, Destination: "0.0.0.0", Gateway: "192.0.2.1", Mask: "0.0.0.0", Flags: 1})
		},
		"ipv4_nonloopback_destination": func(n *p06Network) { n.IPv4Routes[0].Destination = "10.0.0.0" },
		"ipv4_default_via_loopback":    func(n *p06Network) { n.IPv4Routes[0].Destination = "0.0.0.0"; n.IPv4Routes[0].Mask = "0.0.0.0" },
		"ipv4_gateway":                 func(n *p06Network) { n.IPv4Routes[0].Gateway = "192.0.2.1" },
		"ipv6_external_route": func(n *p06Network) {
			n.IPv6Routes = append(n.IPv6Routes, p06IPv6Route{Interface: n.Interfaces[1].Name, Destination: "::", Prefix: 0, NextHop: "2001:db8::1", Flags: 1})
		},
		"ipv6_default_via_loopback":    func(n *p06Network) { n.IPv6Routes[1].Flags = 1 },
		"ipv6_nonloopback_destination": func(n *p06Network) { n.IPv6Routes[0].Destination = "2001:db8::1" },
		"unreported_interface":         func(n *p06Network) { n.IPv4Routes[0].Interface = "missing" },
		"duplicate_interface":          func(n *p06Network) { n.Interfaces = append(n.Interfaces, n.Interfaces[0]) },
		"no_loopback":                  func(n *p06Network) { n.Interfaces = nil },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			body, err := json.Marshal(baseline)
			if err != nil {
				t.Fatal(err)
			}
			var changed p06Network
			if err := json.Unmarshal(body, &changed); err != nil {
				t.Fatal(err)
			}
			mutate(&changed)
			if p06ValidateNetwork(changed) == nil {
				t.Fatal("accepted external connectivity or incomplete network facts")
			}
		})
	}
}

func TestP06WorkspaceProofRejectsWrongOriginalBinding(t *testing.T) {
	request := runner.WorkspaceRequest{TenantID: "tenant", RunID: "run", WorkspaceID: "workspace", Epoch: 3}
	workspace := runner.Workspace{TenantID: "tenant", RunID: "run", ID: "workspace", Epoch: 3, Stopped: true}
	if !p06WorkspaceMatches(request, workspace) {
		t.Fatal("matching proof rejected")
	}
	for _, mutate := range []func(*runner.Workspace){
		func(w *runner.Workspace) { w.TenantID = "other" },
		func(w *runner.Workspace) { w.RunID = "other" },
		func(w *runner.Workspace) { w.ID = "other" },
		func(w *runner.Workspace) { w.Epoch++ },
	} {
		wrong := workspace
		mutate(&wrong)
		if p06WorkspaceMatches(request, wrong) {
			t.Fatal("foreign/self-consistent proof authorized cleanup of original workspace")
		}
	}
}
