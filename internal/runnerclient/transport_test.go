package runnerclient

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/artifact"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	"github.com/JDinSeattle/forge-runtime/internal/sandbox"
)

func shortDir(t *testing.T) string {
	t.Helper()
	p, err := os.MkdirTemp("", "forge-rpc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(p) })
	return p
}
func engineFixture(t *testing.T) (*runner.Engine, runner.WorkspaceRequest) {
	t.Helper()
	root := shortDir(t)
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "main.py"), []byte("bug\n"), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := artifact.NewLocalStore(filepath.Join(root, "artifacts"), 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	signer, _ := runner.NewSigner([]byte(strings.Repeat("x", 32)))
	e, err := runner.Open(runner.Config{RootDir: filepath.Join(root, "runner"), JournalPath: filepath.Join(root, "journal.db"), Artifacts: store, Backend: &sandbox.TestBackend{}, Signer: signer, Sources: map[string]string{"source": source}, Profiles: map[string]sandbox.Profile{"python": {ID: "python"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	now := time.Now()
	grant, err := signer.Sign(runner.Claims{TenantID: "tenant", RunID: "run", WorkspaceID: "workspace", Epoch: 1, IssuedAt: now, ExpiresAt: now.Add(time.Minute), Permissions: []string{"prepare", "execute", "inspect", "cancel", "adopt", "snapshot", "release"}}, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	return e, runner.WorkspaceRequest{TenantID: "tenant", RunID: "run", WorkspaceID: "workspace", Epoch: 1, Grant: grant}
}
func closeServer(t *testing.T, s *Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Error(err)
	}
}
func operationRequest(r runner.WorkspaceRequest, id domain.ID) runner.OperationRequest {
	args := json.RawMessage(`{}`)
	digest := sha256.Sum256(args)
	return runner.OperationRequest{WorkspaceRequest: r, OperationID: id, ExpectedRevision: 1, Kind: "list_files", Args: args, ArgsHash: hex.EncodeToString(digest[:]), PolicyVersion: "policy-1", Deadline: time.Now().Add(time.Minute).UTC()}
}

func TestUDSContractAndPermissions(t *testing.T) {
	e, r := engineFixture(t)
	socket := filepath.Join(shortDir(t), "runner.sock")
	server, err := Serve(ServerConfig{UnixSocket: socket}, e)
	if err != nil {
		t.Fatal(err)
	}
	defer closeServer(t, server)
	info, err := os.Stat(socket)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("socket is not0600: %v %v", info, err)
	}
	client, err := Dial(context.Background(), ClientConfig{UnixSocket: socket})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	w, err := client.PrepareWorkspace(context.Background(), runner.PrepareRequest{WorkspaceRequest: r, SourceID: "source", ProfileID: "python"})
	if err != nil || w.Revision != 1 {
		t.Fatalf("prepare: %+v %v", w, err)
	}
	req := operationRequest(r, "operation")
	o, err := client.StartOperation(context.Background(), req)
	if err != nil || o.Request.ArgsHash != req.ArgsHash || string(o.Request.Args) != string(req.Args) {
		t.Fatalf("canonical args changed in transport: %+v %v", o, err)
	}
	deadline := time.Now().Add(time.Second)
	for !o.Status.Terminal() {
		o, err = client.InspectOperation(context.Background(), runner.InspectRequest{WorkspaceRequest: r, OperationID: "operation"})
		if err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("operation did not settle")
		}
		time.Sleep(time.Millisecond)
	}
	if o.Status != runner.Succeeded || o.Receipt.ObjectKey == "" {
		t.Fatal("receipt lost over protobuf")
	}
	bad := r
	bad.Grant = "tampered"
	if _, err = client.InspectOperation(context.Background(), runner.InspectRequest{WorkspaceRequest: bad, OperationID: "operation"}); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("grant authorization lost over RPC: %v", err)
	}
	snapshot, err := client.SealSnapshot(context.Background(), r)
	if err != nil || snapshot.Artifact.SHA256 == "" {
		t.Fatal(err)
	}
	stop, err := client.StopWorkspace(context.Background(), r)
	if err != nil || !stop.NoActiveOperations {
		t.Fatal(err)
	}
	release, err := client.ReleaseWorkspace(context.Background(), r)
	if err != nil || !release.Released {
		t.Fatal(err)
	}
}

type uncertainService struct {
	runner.Service
	mu        sync.Mutex
	operation runner.Operation
	starts    atomic.Int64
	inspects  atomic.Int64
	missing   bool
}

func (s *uncertainService) StartOperation(ctx context.Context, r runner.OperationRequest) (runner.Operation, error) {
	s.starts.Add(1)
	s.mu.Lock()
	s.operation = runner.Operation{Request: r, Status: runner.Running}
	s.mu.Unlock()
	<-ctx.Done()
	return runner.Operation{}, ctx.Err()
}
func (s *uncertainService) InspectOperation(_ context.Context, r runner.InspectRequest) (runner.Operation, error) {
	s.inspects.Add(1)
	if s.missing {
		return runner.Operation{}, domain.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.operation.Request.OperationID != r.OperationID {
		return runner.Operation{}, domain.ErrConflict
	}
	return s.operation, nil
}

func TestStartTimeoutInspectsSameIDWithoutResubmission(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "accepted", true: "unknown"}[missing], func(t *testing.T) {
			service := &uncertainService{missing: missing}
			socket := filepath.Join(shortDir(t), "runner.sock")
			server, err := Serve(ServerConfig{UnixSocket: socket}, service)
			if err != nil {
				t.Fatal(err)
			}
			defer closeServer(t, server)
			client, err := Dial(context.Background(), ClientConfig{UnixSocket: socket, RPCTimeout: 50 * time.Millisecond, ReconcileTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			r := runner.WorkspaceRequest{TenantID: "tenant", RunID: "run", WorkspaceID: "workspace", Epoch: 1, Grant: "opaque"}
			o, err := client.StartOperation(context.Background(), operationRequest(r, "stable-id"))
			if missing {
				if !errors.Is(err, ErrUncertainStart) {
					t.Fatalf("ambiguous start lost uncertainty: %v", err)
				}
			} else if err != nil || o.Request.OperationID != "stable-id" || o.Status != runner.Running {
				t.Fatalf("failed same-ID observation: %+v %v", o, err)
			}
			if service.starts.Load() != 1 || service.inspects.Load() != 1 {
				t.Fatalf("start=%d inspect=%d", service.starts.Load(), service.inspects.Load())
			}
		})
	}
}

func TestRemoteRequiresVerifiedMutualIdentity(t *testing.T) {
	e, _ := engineFixture(t)
	root := shortDir(t)
	ca, caKey, caPath := makeCA(t, root, "ca")
	serverCert, serverKey := makeLeaf(t, root, "server", "spiffe://forge/runner", ca, caKey)
	workerCert, workerKey := makeLeaf(t, root, "worker", "spiffe://forge/worker", ca, caKey)
	wrongCert, wrongKey := makeLeaf(t, root, "wrong", "spiffe://forge/outsider", ca, caKey)
	if _, err := Serve(ServerConfig{TCPAddress: "127.0.0.1:0"}, e); err == nil {
		t.Fatal("insecure TCP server accepted")
	}
	server, err := Serve(ServerConfig{TCPAddress: "127.0.0.1:0", TLS: TLSFiles{CertFile: serverCert, KeyFile: serverKey, CAFile: caPath, ExpectedPeerURI: "spiffe://forge/worker"}}, e)
	if err != nil {
		t.Fatal(err)
	}
	defer closeServer(t, server)
	base := ClientConfig{TCPAddress: server.Address(), RPCTimeout: time.Second, TLS: TLSFiles{CertFile: workerCert, KeyFile: workerKey, CAFile: caPath, ExpectedPeerURI: "spiffe://forge/runner", ServerName: "forge-runner.local"}}
	client, err := Dial(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	for _, change := range []func(*ClientConfig){func(c *ClientConfig) { c.TLS.CertFile, c.TLS.KeyFile = wrongCert, wrongKey }, func(c *ClientConfig) { c.TLS.ExpectedPeerURI = "spiffe://forge/imposter" }, func(c *ClientConfig) { _, _, otherCA := makeCA(t, root, "other-ca"); c.TLS.CAFile = otherCA }, func(c *ClientConfig) { c.TLS = TLSFiles{} }} {
		cfg := base
		change(&cfg)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		client, err := Dial(ctx, cfg)
		cancel()
		if err == nil {
			_ = client.Close()
			t.Fatal("unverified or wrong TLS identity accepted")
		}
	}
}

func makeCA(t *testing.T, root, name string) (*x509.Certificate, *ecdsa.PrivateKey, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, name+".pem")
	if err = os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	return parsed, key, path
}
func makeLeaf(t *testing.T, root, name, identity string, ca *x509.Certificate, caKey *ecdsa.PrivateKey) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := url.Parse(identity)
	cert := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"forge-runner.local"}, URIs: []*url.URL{uri}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, cert, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath := filepath.Join(root, name+".pem"), filepath.Join(root, name+".key")
	if err = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

func TestOversizedResultUsesDurableReceiptReference(t *testing.T) {
	r := runner.WorkspaceRequest{TenantID: "tenant", RunID: "run", WorkspaceID: "workspace", Epoch: 1}
	o := runner.Operation{Request: operationRequest(r, "operation"), Status: runner.Succeeded, Result: json.RawMessage(strings.Repeat("x", 2<<20)), Receipt: artifact.Ref{TenantID: "tenant", RunID: "run", ObjectKey: "tenant/run/digest"}}
	wire, err := operation(o)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip, err := fromOperation(wire)
	if err != nil {
		t.Fatal(err)
	}
	if !roundTrip.ResultTruncated || len(roundTrip.Result) != 0 || roundTrip.Receipt.ObjectKey != o.Receipt.ObjectKey {
		t.Fatal("oversized result became unbounded or silently lost its receipt")
	}
}
