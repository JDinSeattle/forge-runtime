import json, os, pathlib, signal, sys, time
marker, mode = sys.argv[1:]
p = pathlib.Path("p06-shared-name.txt")
p.write_text(marker)
def read(name):
    return pathlib.Path(name).read_text().strip()
status = dict(line.split(":", 1) for line in read("/proc/self/status").splitlines() if ":" in line)
facts = {"marker": marker, "uid": os.getuid(), "gid": os.getgid(),
         "memory_max": read("/sys/fs/cgroup/memory.max"), "pids_max": read("/sys/fs/cgroup/pids.max"),
         "cpu_max": read("/sys/fs/cgroup/cpu.max"), "cap_eff": status["CapEff"].strip(),
         "no_new_privs": status["NoNewPrivs"].strip(), "net_devices": sorted(os.listdir("/sys/class/net")),
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
