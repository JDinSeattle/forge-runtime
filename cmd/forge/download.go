package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

func downloadCommand(s *settings) *cobra.Command {
	var output string
	var maxBytes int64
	cmd := &cobra.Command{Use: "download ARTIFACT_ID", Short: "Download and verify an immutable artifact without overwriting files", Args: oneID, RunE: func(cmd *cobra.Command, args []string) error {
		if output == "" || maxBytes <= 0 {
			return errors.New("--output and a positive --max-bytes are required")
		}
		if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
			if err == nil {
				return errors.New("output already exists; choose another path")
			}
			return err
		}
		client, err := s.client()
		if err != nil {
			return err
		}
		ctx, cancel := s.context(cmd.Context())
		defer cancel()
		response, err := client.DownloadArtifact(ctx, args[0])
		if err != nil {
			return err
		}
		if response.StatusCode != 200 {
			var ignored any
			return decodeResponse(response, &ignored, 200)
		}
		defer response.Body.Close()
		if response.ContentLength < 0 || response.ContentLength > maxBytes {
			return errors.New("artifact size is absent or exceeds --max-bytes")
		}
		etag := response.Header.Get("ETag")
		if len(etag) != 66 || etag[0] != '"' || etag[len(etag)-1] != '"' {
			return errors.New("artifact lacks a SHA-256 ETag")
		}
		expected := strings.Trim(etag, "\"")
		if _, err = hex.DecodeString(expected); err != nil {
			return errors.New("invalid artifact SHA-256 ETag")
		}
		f, err := os.CreateTemp(filepath.Dir(output), ".forge-download-*")
		if err != nil {
			return err
		}
		temp := f.Name()
		defer os.Remove(temp)
		defer f.Close()
		hash := sha256.New()
		n, err := io.Copy(io.MultiWriter(f, hash), io.LimitReader(response.Body, maxBytes+1))
		if err != nil {
			return err
		}
		if n > maxBytes || n != response.ContentLength {
			return errors.New("artifact size mismatch")
		}
		if hex.EncodeToString(hash.Sum(nil)) != expected {
			return errors.New("artifact checksum mismatch")
		}
		if err = f.Sync(); err != nil {
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
		// Link creates the final name exclusively, so a concurrent writer cannot
		// have its file replaced between the initial existence check and publish.
		if err = os.Link(temp, output); err != nil {
			return fmt.Errorf("cannot publish artifact: %w", err)
		}
		if err = syncDirectory(filepath.Dir(output)); err != nil {
			return err
		}
		return printJSON(cmd.OutOrStdout(), map[string]any{"artifact_id": args[0], "path": output, "sha256": expected, "bytes": n})
	}}
	cmd.Flags().StringVar(&output, "output", "", "Destination path (must not exist)")
	cmd.Flags().Int64Var(&maxBytes, "max-bytes", 64<<20, "Maximum accepted artifact size")
	return cmd
}
