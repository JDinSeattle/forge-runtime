package artifact

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"golang.org/x/sys/unix"
)

type publicationKey struct{}
type publicationLock struct {
	store  *LocalStore
	active atomic.Bool
}

// WithPublication excludes collection until both durable bytes and their
// authoritative reference have committed. Pass the callback context to nested
// Put/publication operations; it must not outlive the callback. Locks are held
// by open file descriptions, so separate processes and store handles cooperate,
// and process death releases the lock. Other Store implementations must supply
// their own equivalent before enabling collection.
func WithPublication(ctx context.Context, store Store, publish func(context.Context) error) error {
	local, ok := store.(*LocalStore)
	if !ok {
		return publish(ctx)
	}
	if held, ok := ctx.Value(publicationKey{}).(*publicationLock); ok && held.store == local && held.active.Load() {
		return publish(ctx)
	}
	unlock, err := local.lockCollection(ctx, false)
	if err != nil {
		return err
	}
	defer unlock()
	held := &publicationLock{store: local}
	held.active.Store(true)
	defer held.active.Store(false)
	return publish(context.WithValue(ctx, publicationKey{}, held))
}

func (s *LocalStore) lockCollection(ctx context.Context, exclusive bool) (func(), error) {
	f, err := s.root.OpenFile(".publication.lock", os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		f.Close()
		return nil, fmt.Errorf("%w: publication lock must be a private regular file", domain.ErrInvalid)
	}
	mode := unix.LOCK_SH
	if exclusive {
		mode = unix.LOCK_EX
	}
	for {
		if err = ctx.Err(); err != nil {
			f.Close()
			return nil, err
		}
		err = unix.Flock(int(f.Fd()), mode|unix.LOCK_NB)
		if err == nil {
			return func() { _ = f.Close() }, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EINTR) {
			f.Close()
			return nil, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}

type CollectionOptions struct {
	MinAge time.Duration
	Limit  int
	Apply  bool
}

type CollectedObject struct {
	Key     string `json:"object_key"`
	Bytes   int64  `json:"bytes"`
	Staging bool   `json:"staging"`
}

type CollectionResult struct {
	Apply     bool              `json:"apply"`
	Cutoff    time.Time         `json:"cutoff"`
	Examined  int               `json:"examined"`
	Protected int               `json:"protected"`
	Objects   []CollectedObject `json:"objects"`
}

// Collect removes only old, exactly named objects/staging files under valid
// tenant/run paths. references is read AFTER obtaining the exclusive lock and
// must include every READY object and every recoverable runner journal pin.
// Failure to read either authority aborts before any deletion. All publishers
// must use WithPublication; this local protocol is not a distributed S3 lock.
func (s *LocalStore) Collect(ctx context.Context, options CollectionOptions, references func(context.Context) (map[string]bool, error)) (CollectionResult, error) {
	result := CollectionResult{Apply: options.Apply, Cutoff: time.Now().UTC().Add(-options.MinAge), Objects: []CollectedObject{}}
	if options.MinAge < time.Hour || options.Limit < 1 || options.Limit > 10000 || references == nil {
		return result, domain.ErrInvalid
	}
	unlock, err := s.lockCollection(ctx, true)
	if err != nil {
		return result, err
	}
	defer unlock()
	protected, err := references(ctx)
	if err != nil {
		return result, err
	}
	visited := 0
	err = fs.WalkDir(s.root.FS(), ".", func(key string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		visited++
		if visited > 1000000 {
			return fmt.Errorf("%w: collection scan limit", domain.ErrCapacity)
		}
		if key == "." {
			return nil
		}
		parts := strings.Split(key, "/")
		if entry.IsDir() {
			if len(parts) > 2 || domain.ID(entry.Name()).Validate() != nil {
				return fs.SkipDir
			}
			return nil
		}
		if len(parts) != 3 || !entry.Type().IsRegular() {
			return nil
		}
		staging := strings.HasPrefix(entry.Name(), ".staging-")
		name, size := entry.Name(), 32
		if staging {
			name, size = strings.TrimPrefix(name, ".staging-"), 16
		}
		decoded, err := hex.DecodeString(name)
		if err != nil || len(decoded) != size || hex.EncodeToString(decoded) != name {
			return nil
		}
		result.Examined++
		if protected[key] {
			result.Protected++
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.ModTime().Before(result.Cutoff) {
			return nil
		}
		if options.Apply {
			if err = s.root.Remove(key); err != nil {
				return err
			}
			// Persist each unlink before reporting it; a crash can only leave
			// an unreported deletion of an already proven orphan.
			dir, err := s.root.Open(path.Dir(key))
			if err != nil {
				return err
			}
			err = dir.Sync()
			_ = dir.Close()
			if err != nil {
				return err
			}
		}
		result.Objects = append(result.Objects, CollectedObject{Key: key, Bytes: info.Size(), Staging: staging})
		if len(result.Objects) == options.Limit {
			return fs.SkipAll
		}
		return nil
	})
	return result, err
}
