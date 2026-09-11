// Package artifact stores immutable, tenant-scoped objects. Database publication
// is a separate transaction: Put makes a durable object, not an authorized API
// resource. Orphans must remain unlisted until the control plane commits a ref.
package artifact

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/JDinSeattle/forge-runtime/internal/dependency"
	"github.com/JDinSeattle/forge-runtime/internal/domain"
)

type Ref struct {
	TenantID  domain.ID `json:"tenant_id"`
	RunID     domain.ID `json:"run_id"`
	Kind      string    `json:"kind"`
	ObjectKey string    `json:"object_key"`
	SHA256    string    `json:"sha256"`
	Size      int64     `json:"size"`
}

type Store interface {
	Put(context.Context, domain.ID, domain.ID, string, io.Reader) (Ref, error)
	Open(context.Context, domain.ID, domain.ID, Ref) (io.ReadCloser, error)
	Stat(context.Context, domain.ID, domain.ID, Ref) (Ref, error)
	Delete(context.Context, domain.ID, domain.ID, Ref) error
}

type LocalStore struct {
	root     *os.Root
	maxBytes int64
}

func NewLocalStore(directory string, maxBytes int64) (*LocalStore, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("%w: artifact limit must be positive", domain.ErrInvalid)
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	// Persist the store root itself and every containing directory. This also
	// covers a freshly provisioned nested root, whose directory entries are not
	// made durable merely by syncing objects below it.
	abs, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	canonical, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, err
	}
	for current := canonical; ; current = filepath.Dir(current) {
		dir, err := os.Open(current)
		if err != nil {
			return nil, err
		}
		err = dir.Sync()
		_ = dir.Close()
		if err != nil {
			return nil, err
		}
		if filepath.Dir(current) == current {
			break
		}
	}
	r, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	return &LocalStore{root: r, maxBytes: maxBytes}, nil
}

func (s *LocalStore) Close() error { return s.root.Close() }

func (s *LocalStore) Put(ctx context.Context, tenant, run domain.ID, kind string, input io.Reader) (Ref, error) {
	ctx, cancel := dependency.Artifact(ctx)
	defer cancel()
	stop := interruptInputOnCancel(ctx, input)
	defer stop()
	var ref Ref
	err := WithPublication(ctx, s, func(locked context.Context) error {
		var err error
		ref, err = s.put(locked, tenant, run, kind, input)
		return err
	})
	return ref, err
}

func (s *LocalStore) put(ctx context.Context, tenant, run domain.ID, kind string, input io.Reader) (Ref, error) {
	if err := ctx.Err(); err != nil {
		return Ref{}, err
	}
	if err := tenant.Validate(); err != nil {
		return Ref{}, err
	}
	if err := run.Validate(); err != nil {
		return Ref{}, err
	}
	if kind == "" || input == nil {
		return Ref{}, domain.ErrInvalid
	}
	dir := path.Join(string(tenant), string(run))
	if err := s.root.MkdirAll(dir, 0700); err != nil {
		return Ref{}, err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return Ref{}, err
	}
	tmp := path.Join(dir, ".staging-"+hex.EncodeToString(nonce[:]))
	f, err := s.root.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return Ref{}, err
	}
	defer s.root.Remove(tmp)
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(f, h), io.LimitReader(contextReader{ctx, input}, s.maxBytes+1))
	if copyErr == nil && n > s.maxBytes {
		copyErr = fmt.Errorf("%w: artifact exceeds byte limit", domain.ErrCapacity)
	}
	if copyErr == nil {
		copyErr = f.Sync()
	}
	closeErr := f.Close()
	if err := ctx.Err(); err != nil {
		return Ref{}, err
	}
	if copyErr != nil {
		return Ref{}, copyErr
	}
	if closeErr != nil {
		return Ref{}, closeErr
	}
	digest := hex.EncodeToString(h.Sum(nil))
	key := path.Join(dir, digest)
	// Link publishes without overwriting an already-addressed object. Concurrent
	// identical writers converge; an existing object is verified before reuse.
	if err = s.root.Link(tmp, key); err != nil && !errors.Is(err, os.ErrExist) {
		return Ref{}, err
	}
	ref := Ref{TenantID: tenant, RunID: run, Kind: kind, ObjectKey: key, SHA256: digest, Size: n}
	if _, err = s.Stat(ctx, tenant, run, ref); err != nil {
		return Ref{}, err
	}
	d, err := s.root.Open(dir)
	if err != nil {
		return Ref{}, err
	}
	err = d.Sync()
	_ = d.Close()
	if err != nil {
		return Ref{}, err
	}
	// Newly created tenant/run directories must themselves be durable before
	// a database can publish a reference to a file below them.
	for _, parent := range []string{string(tenant), "."} {
		d, err := s.root.Open(parent)
		if err != nil {
			return Ref{}, err
		}
		err = d.Sync()
		_ = d.Close()
		if err != nil {
			return Ref{}, err
		}
	}
	return ref, nil
}

func (s *LocalStore) Open(ctx context.Context, tenant, run domain.ID, ref Ref) (io.ReadCloser, error) {
	ctx, cancel := dependency.Artifact(ctx)
	transferred := false
	defer func() {
		if !transferred {
			cancel()
		}
	}()
	if err := binding(tenant, run, ref); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := s.root.Open(ref.ObjectKey)
	if err != nil {
		return nil, err
	}
	reader := deadlineReader(ctx, cancel, f)
	defer func() {
		if !transferred {
			_ = reader.Close()
		}
	}()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != ref.Size || info.Size() > s.maxBytes {
		return nil, fmt.Errorf("%w: object type or length mismatch", domain.ErrConflict)
	}
	h := sha256.New()
	if _, err = io.Copy(h, reader); err != nil {
		return nil, err
	}
	if hex.EncodeToString(h.Sum(nil)) != ref.SHA256 {
		return nil, fmt.Errorf("%w: artifact digest mismatch", domain.ErrConflict)
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	transferred = true
	return reader, nil
}

func (s *LocalStore) Stat(ctx context.Context, tenant, run domain.ID, ref Ref) (Ref, error) {
	ctx, cancel := dependency.Artifact(ctx)
	defer cancel()
	f, err := s.Open(ctx, tenant, run, ref)
	if err != nil {
		return Ref{}, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, contextReader{ctx, f}); err != nil {
		return Ref{}, err
	}
	if hex.EncodeToString(h.Sum(nil)) != ref.SHA256 {
		return Ref{}, fmt.Errorf("%w: artifact digest mismatch", domain.ErrConflict)
	}
	return ref, nil
}

func (s *LocalStore) Delete(ctx context.Context, tenant, run domain.ID, ref Ref) error {
	ctx, cancel := dependency.Artifact(ctx)
	defer cancel()
	if err := binding(tenant, run, ref); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := s.root.Remove(ref.ObjectKey)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func binding(tenant, run domain.ID, ref Ref) error {
	if tenant.Validate() != nil || run.Validate() != nil || ref.TenantID != tenant || ref.RunID != run {
		return domain.ErrForbidden
	}
	digest, err := hex.DecodeString(ref.SHA256)
	if err != nil || len(digest) != sha256.Size || ref.Size < 0 || ref.ObjectKey != path.Join(string(tenant), string(run), ref.SHA256) {
		return domain.ErrInvalid
	}
	return nil
}

type contextReader struct {
	ctx context.Context
	io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}
