#!/usr/bin/env python3
"""Run the real pinned OpenTelemetry Collector and verify persisted trace topology."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

REPO = Path(__file__).absolute().parents[2]


def verify_delivery(endpoint, output):
    for attempt in range(100):
        try:
            request = urllib.request.Request(endpoint, data=b"{}", headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=1):
                break
        except (OSError, urllib.error.URLError):
            if attempt == 99:
                raise
            time.sleep(0.1)
    env = dict(os.environ, GOCACHE="/tmp/forge-runtime-gocache", GOPROXY="off", FORGE_RECOVERY_OTLP_ENDPOINT=endpoint, FORGE_RECOVERY_OTLP_OUTPUT=str(output / "collector-traces.jsonl"))
    result = subprocess.run(["go", "test", "./tests/review", "-run", "^TestRecoveryIndependentCollectorDelivery$", "-count=1", "-v"], cwd=REPO, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=90)
    (output / "test.log").write_text(result.stdout)
    print(result.stdout, end="")
    if result.returncode:
        raise RuntimeError("collector delivery test failed; raw evidence retained")


def process_collector(docker, image, image_ref, output):
    # Extraction creates a stopped, network-none container solely as an image
    # filesystem source. It never starts that container or changes networking.
    entrypoint = image.get("Config", {}).get("Entrypoint")
    if entrypoint != ["/otelcol-contrib"]:
        raise ValueError("unexpected official collector image entrypoint")
    cid = subprocess.check_output(docker + ["create", "--pull=never", "--network=none", image_ref], text=True).strip()
    try:
        info = subprocess.check_output(docker + ["inspect", cid], text=True)
        (output / "collector-extraction-container.json").write_text(info)
        with tempfile.TemporaryDirectory(prefix="forge-collector-") as temporary:
            binary = Path(temporary) / "otelcol-contrib"
            subprocess.run(docker + ["cp", cid + ":/otelcol-contrib", str(binary)], check=True)
            if binary.is_symlink() or not binary.is_file():
                raise ValueError("extracted collector is not a regular file")
            binary.chmod(0o700)
            hasher = hashlib.sha256()
            with binary.open("rb") as f:
                while block := f.read(1 << 20):
                    hasher.update(block)
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            # The real collector binds its own socket; no test receiver exists.
            config = (REPO / "scripts/recovery/collector.yaml").read_text().replace("0.0.0.0:4318", "127.0.0.1:" + str(port)).replace("/evidence/collector-traces.jsonl", json.dumps(str(output / "collector-traces.jsonl")))
            (output / "collector.yaml").write_text(config)
            metadata = {"mode": "ordinary-user-process", "image": image_ref, "id": image["Id"], "repo_digests": image["RepoDigests"], "binary_sha256": hasher.hexdigest(), "extraction_container_started": False, "docker_networking_modified": False}
            (output / "collector-image.json").write_text(json.dumps(metadata, indent=2) + "\n")
            with (output / "collector.log").open("x") as log:
                process = subprocess.Popen([str(binary), "--config=" + str(output / "collector.yaml")], stdout=log, stderr=subprocess.STDOUT, env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}, cwd=output)
                try:
                    verify_delivery("http://127.0.0.1:" + str(port) + "/v1/traces", output)
                finally:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
                    metadata["collector_pid"], metadata["collector_exit"] = process.pid, process.returncode
                    (output / "collector-image.json").write_text(json.dumps(metadata, indent=2) + "\n")
    finally:
        subprocess.run(docker + ["rm", cid], check=True, stdout=subprocess.DEVNULL)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker-host", required=True)
    parser.add_argument("--image", required=True, help="already pulled otel/opentelemetry-collector-contrib@sha256 digest")
    parser.add_argument("--output", required=True)
    parser.add_argument("--mode", choices=("process", "container"), default="process", help="process extracts but never starts the image container; works with bridge=none daemons")
    args = parser.parse_args()
    if os.geteuid() == 0:
        raise ValueError("collector harness runs as an ordinary host user, not root")
    if not args.docker_host.startswith("unix:///") or not re.fullmatch(r"otel/opentelemetry-collector-contrib@sha256:[0-9a-f]{64}", args.image):
        raise ValueError("explicit Unix Docker socket and official pinned collector digest required")
    output = Path(args.output).absolute()
    if output.resolve() != output:
        raise ValueError("evidence directory cannot contain symlinks")
    os.umask(0o077)
    output.mkdir(mode=0o700)
    shutil.copyfile(REPO / "scripts/recovery/collector.yaml", output / "collector.yaml")
    docker = ["docker", "--host", args.docker_host]
    image = json.loads(subprocess.check_output(docker + ["image", "inspect", args.image], text=True))[0]
    if args.mode == "process":
        process_collector(docker, image, args.image, output)
        print("Collector process stopped; raw evidence retained at " + str(output))
        return
    cid = subprocess.check_output(docker + ["run", "-d", "--pull=never", "--user=0:0", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges", "--memory=256m", "--pids-limit=64", "--publish=127.0.0.1::4318", "--mount", "type=bind,src=" + str(output) + ",dst=/evidence", "--mount", "type=bind,src=" + str(output / "collector.yaml") + ",dst=/etc/otelcol-contrib/config.yaml,readonly", args.image, "--config=/etc/otelcol-contrib/config.yaml"], text=True).strip()
    try:
        port = subprocess.check_output(docker + ["port", cid, "4318/tcp"], text=True).strip()
        if not re.fullmatch(r"127\.0\.0\.1:[0-9]+", port):
            raise ValueError("collector must publish only one loopback endpoint")
        endpoint = "http://" + port + "/v1/traces"
        (output / "collector-image.json").write_text(json.dumps({"image": args.image, "id": image["Id"], "repo_digests": image["RepoDigests"], "container_id": cid}, indent=2) + "\n")
        verify_delivery(endpoint, output)
    finally:
        subprocess.run(docker + ["stop", "--time=5", cid], check=False, stdout=subprocess.DEVNULL)
        logs = subprocess.run(docker + ["logs", cid], capture_output=True, text=True)
        (output / "collector.log").write_text(logs.stdout + logs.stderr)
        info = subprocess.run(docker + ["inspect", cid], capture_output=True, text=True)
        (output / "collector-container.json").write_text(info.stdout)
    print("Collector stopped; its container and evidence are retained at " + str(output))


if __name__ == "__main__":
    main()
