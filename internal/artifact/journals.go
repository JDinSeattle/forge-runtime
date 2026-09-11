package artifact

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"golang.org/x/sys/unix"
)

// RegisterJournal permanently records each runner reference authority sharing
// this local root. GC must not accept an unrelated empty journal, nor silently
// omit a second runner. Removing/rebinding this registration is an offline,
// coordinated restore operation; no age-based expiration is safe.
func RegisterJournal(ctx context.Context, store Store, filename, identity string) error {
	s, ok := store.(*LocalStore)
	if !ok {
		return nil
	}
	canonical, name, err := journalIdentity(filename)
	if err != nil {
		return err
	}
	if !validJournalID(identity) {
		return domain.ErrInvalid
	}
	body, err := json.Marshal(journalRegistration{JournalPath: canonical, JournalID: identity})
	if err != nil {
		return err
	}
	return WithPublication(ctx, store, func(context.Context) error {
		tmp := ".journal-staging-" + rand.Text()
		file, err := s.root.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY|unix.O_NOFOLLOW, 0600)
		if err != nil {
			return err
		}
		defer s.root.Remove(tmp)
		_, err = file.Write(append(body, '\n'))
		if err == nil {
			err = file.Sync()
		}
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if err = s.root.Link(tmp, name); err != nil && !os.IsExist(err) {
			return err
		}
		file, err = s.root.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW, 0)
		if err != nil {
			return err
		}
		raw, err := io.ReadAll(io.LimitReader(file, 32769))
		_ = file.Close()
		if err != nil {
			return err
		}
		if !matchesRegistration(raw, canonical, identity) {
			return fmt.Errorf("%w: journal registration mismatch", domain.ErrConflict)
		}
		dir, err := s.root.Open(".")
		if err != nil {
			return err
		}
		defer dir.Close()
		return dir.Sync()
	})
}

// CheckJournal must be called inside Collect's exclusive callback. The local
// operator command supports exactly one durable runner reference authority.
func (s *LocalStore) CheckJournal(ctx context.Context, filename, identity string) error {
	if !validJournalID(identity) {
		return domain.ErrInvalid
	}
	canonical, expected, err := journalIdentity(filename)
	if err != nil {
		return err
	}
	dir, err := s.root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	matched := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, err := dir.ReadDir(128)
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), ".runner-journal-") {
				continue
			}
			if matched || entry.Name() != expected || !entry.Type().IsRegular() {
				return fmt.Errorf("%w: collector must account for every registered runner journal", domain.ErrConflict)
			}
			f, err := s.root.OpenFile(entry.Name(), os.O_RDONLY|unix.O_NOFOLLOW, 0)
			if err != nil {
				return err
			}
			raw, err := io.ReadAll(io.LimitReader(f, 32769))
			_ = f.Close()
			if err != nil {
				return err
			}
			if !matchesRegistration(raw, canonical, identity) {
				return domain.ErrConflict
			}
			matched = true
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if !matched {
		return fmt.Errorf("%w: runner must register its upgraded journal before collection", domain.ErrConflict)
	}
	return nil
}

type journalRegistration struct {
	JournalPath string `json:"journal_path"`
	JournalID   string `json:"journal_id"`
}

func validJournalID(identity string) bool {
	raw, err := hex.DecodeString(identity)
	return err == nil && len(raw) == 32 && hex.EncodeToString(raw) == identity
}

func matchesRegistration(raw []byte, canonical, identity string) bool {
	if len(raw) > 32768 {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value journalRegistration
	if decoder.Decode(&value) != nil {
		return false
	}
	var extra any
	return decoder.Decode(&extra) == io.EOF && value.JournalPath == canonical && value.JournalID == identity
}

func journalIdentity(filename string) (canonical, name string, err error) {
	if !filepath.IsAbs(filename) || strings.ContainsAny(filename, "\r\n") {
		return "", "", domain.ErrInvalid
	}
	canonical, err = filepath.EvalSymlinks(filename)
	if err != nil {
		return "", "", err
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return "", "", err
	}
	if !info.Mode().IsRegular() || len(canonical) > 4095 {
		return "", "", domain.ErrInvalid
	}
	digest := sha256.Sum256([]byte(canonical))
	return canonical, ".runner-journal-" + hex.EncodeToString(digest[:]), nil
}
