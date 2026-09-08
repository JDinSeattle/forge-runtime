//go:build ignore

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {
	if err := generate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func generate() error {
	root, err := filepath.Abs("../../..")
	if err != nil {
		return err
	}
	protoc := os.Getenv("FORGE_PROTOC")
	if protoc == "" {
		protoc, err = exec.LookPath("protoc")
		if err != nil {
			return err
		}
	}
	version, err := exec.Command(protoc, "--version").Output()
	if err != nil || strings.TrimSpace(string(version)) != "libprotoc 36.1" {
		return fmt.Errorf("require protoc 36.1; got %q (%v)", strings.TrimSpace(string(version)), err)
	}
	args := []string{"--proto_path=.", "--proto_path=" + filepath.Join(filepath.Dir(filepath.Dir(protoc)), "include")}
	for _, tool := range []string{"protoc-gen-go", "protoc-gen-go-grpc"} {
		build := exec.Command("go", "tool", tool, "--version")
		build.Dir = root
		if output, err := build.CombinedOutput(); err != nil {
			return fmt.Errorf("build %s: %w: %s", tool, err, output)
		}
		locate := exec.Command("go", "tool", "-n", tool)
		locate.Dir = root
		output, err := locate.Output()
		if err != nil {
			return err
		}
		args = append(args, "--plugin="+tool+"="+strings.TrimSpace(string(output)))
	}
	outputDir := os.Getenv("FORGE_PROTO_OUTPUT")
	if outputDir == "" {
		outputDir = "."
	}
	args = append(args, "--go_out="+outputDir, "--go_opt=module=github.com/JDinSeattle/forge-runtime", "--go-grpc_out="+outputDir, "--go-grpc_opt=module=github.com/JDinSeattle/forge-runtime", "proto/runner/v1/runner.proto")
	cmd := exec.Command(protoc, args...)
	cmd.Dir = root
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}
