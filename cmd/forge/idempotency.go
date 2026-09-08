package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type receipt struct {
	Key       string          `json:"key"`
	BodyHash  string          `json:"body_hash"`
	CreatedAt time.Time       `json:"created_at"`
	Response  json.RawMessage `json:"response,omitempty"`
}

func digest(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func (s settings) receipt(route string, body any, key string) (receipt, string, error) {
	if len(key) > 128 || strings.ContainsAny(key, "\r\n") {
		return receipt{}, "", errors.New("idempotency key must be at most 128 bytes without newlines")
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return receipt{}, "", err
	}
	scope := strings.TrimRight(s.endpoint, "/") + "\n" + s.tenant + "\n" + route + "\n"
	bodyHash := digest(append([]byte(scope), raw...))
	name := "auto-" + bodyHash
	if key != "" {
		name = "key-" + digest([]byte(scope+key))
	}
	if err = os.MkdirAll(s.stateDir, 0700); err != nil {
		return receipt{}, "", err
	}
	path := filepath.Join(s.stateDir, name+".json")
	if key == "" {
		key = "forge_" + rand.Text()
	}
	r := receipt{Key: key, BodyHash: bodyHash, CreatedAt: time.Now().UTC()}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		f, err = os.Open(path)
		if err != nil {
			return receipt{}, path, err
		}
		saved, readErr := readBounded(f, 64<<10)
		f.Close()
		if readErr != nil {
			return receipt{}, path, readErr
		}
		if err = json.Unmarshal(saved, &r); err != nil {
			return receipt{}, path, fmt.Errorf("receipt is incomplete; retry after the competing submit finishes: %w", err)
		}
		if r.BodyHash != bodyHash {
			return receipt{}, path, errors.New("saved idempotency key belongs to a different request body")
		}
		if r.Key == "" || r.CreatedAt.IsZero() {
			return receipt{}, path, errors.New("invalid saved receipt")
		}
		if len(r.Response) == 0 && time.Since(r.CreatedAt) > 23*time.Hour {
			return receipt{}, path, errors.New("unconfirmed receipt is older than 23 hours; reconcile the server result before choosing a new key")
		}
		return r, path, nil
	}
	if err != nil {
		return receipt{}, path, err
	}
	if err = json.NewEncoder(f).Encode(r); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return receipt{}, path, err
	}
	// Sync containing directories before dispatch: a durable file without its
	// directory entry is insufficient to recover the key after machine failure.
	for dir := filepath.Clean(s.stateDir); ; dir = filepath.Dir(dir) {
		if err = syncDirectory(dir); err != nil {
			return receipt{}, path, err
		}
		if filepath.Dir(dir) == dir {
			break
		}
	}
	return r, path, nil
}

func syncDirectory(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func completeReceipt(path string, r *receipt, response any) error {
	raw, err := json.Marshal(response)
	if err != nil {
		return err
	}
	r.Response = raw
	f, err := os.CreateTemp(filepath.Dir(path), ".receipt-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = json.NewEncoder(f).Encode(r); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
