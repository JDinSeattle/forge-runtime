#!/usr/bin/env python3
"""Run production-component telemetry acceptance against an independent collector.
Only a stopped official image container is created for extraction. No Docker
networking, live service, mount, public schema or model endpoint is modified.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import shutil
import socket
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]

def run_test(endpoint, out):
    for _ in range(100):
        try:
            request=urllib.request.Request(endpoint, data=b"{}", headers={"Content-Type":"application/json"})
            with urllib.request.urlopen(request, timeout=1):
                break
        except OSError:
            time.sleep(.1)
    else:
        raise RuntimeError("collector did not become ready")
    env=dict(os.environ,FORGE_RECOVERY_OTLP_ENDPOINT=endpoint,FORGE_RECOVERY_OTLP_OUTPUT=str(out/"collector-traces.jsonl"),FORGE_TELEMETRY_OUTPUT=str(out))
    # Preserve the explicitly supplied shared build cache/toolchain settings.
    scratch=ROOT/"var/local/build-tmp"
    scratch.mkdir(parents=True,exist_ok=True)
    env["GOTMPDIR"]=str(scratch)
    command=["go","test","-race","./tests/review","-run","^TestReviewTelemetryProductionChain$","-count=1","-v"]
    with (out/"test.log").open("x") as log:
        child=subprocess.Popen(command,cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        try:
            code=child.wait(timeout=100)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid,signal.SIGTERM)
            try: child.wait(timeout=3)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid,signal.SIGKILL)
                child.wait(timeout=3)
            raise RuntimeError("acceptance timed out; own process group terminated")
    print((out/"test.log").read_text(),end="",flush=True)
    if code:
        raise RuntimeError(f"production telemetry acceptance failed: exit {code}; raw retained")

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--docker-host",required=True)
    p.add_argument("--image",required=True)
    p.add_argument("--output",required=True)
    a=p.parse_args()
    if os.geteuid()==0 or not a.docker_host.startswith("unix:///") or not re.fullmatch(r"otel/opentelemetry-collector-contrib@sha256:[0-9a-f]{64}",a.image):
        raise ValueError("ordinary user, explicit Unix daemon and pinned official collector image required")
    out=Path(a.output).absolute()
    if out.resolve()!=out: raise ValueError("symlink output path forbidden")
    os.umask(0o077);out.mkdir(mode=0o700)
    docker=["docker","--host",a.docker_host]
    info=json.loads(subprocess.check_output(docker+["image","inspect",a.image],text=True))[0]
    if info["Config"]["Entrypoint"]!=["/otelcol-contrib"]:raise ValueError("unexpected collector entrypoint")
    binary=ROOT/"var/local/collector/otelcol-contrib";binary.parent.mkdir(parents=True,exist_ok=True)
    cid=subprocess.check_output(docker+["create","--pull=never","--network=none",a.image],text=True).strip()
    try:
        (out/"extraction-container.json").write_text(subprocess.check_output(docker+["inspect",cid],text=True))
        subprocess.run(docker+["cp",cid+":/otelcol-contrib",str(binary)],check=True)
    finally:
        subprocess.run(docker+["rm",cid],check=True,stdout=subprocess.DEVNULL)
    if binary.is_symlink() or not binary.is_file():raise ValueError("invalid extracted binary")
    binary.chmod(0o700)
    with socket.socket() as s:
        s.bind(("127.0.0.1",0));port=s.getsockname()[1]
    config=(ROOT/"scripts/recovery/collector.yaml").read_text().replace("0.0.0.0:4318",f"127.0.0.1:{port}").replace("/evidence/collector-traces.jsonl",json.dumps(str(out/"collector-traces.jsonl")))
    (out/"collector.yaml").write_text(config)
    # Hash the actual source inputs before the test starts. Preserve changed
    # source bytes alongside their base commit; never archive local credentials.
    tracked=subprocess.check_output(["git","ls-files","--cached","--others","--exclude-standard","-z"],cwd=ROOT).split(b"\0")
    names=sorted({v.decode() for v in tracked if v})
    allowed=lambda name: name in ("go.mod","go.sum","scripts/recovery/collector.yaml") or name.startswith(("cmd/","internal/","db/","tests/review/","scripts/telemetry/"))
    inputs={}
    for name in names:
        if not allowed(name):continue
        source=ROOT/name
        if source.is_symlink() or not source.is_file() or not source.resolve().is_relative_to(ROOT):raise ValueError("invalid source input")
        inputs[name]=hashlib.sha256(source.read_bytes()).hexdigest()
    changed=set(subprocess.check_output(["git","diff","HEAD","--name-only","-z"],cwd=ROOT).split(b"\0"))
    changed.update(subprocess.check_output(["git","ls-files","--others","--exclude-standard","-z"],cwd=ROOT).split(b"\0"))
    for item in changed:
        if not item:continue
        name=item.decode()
        if name not in inputs:continue
        target=out/"source"/name;target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(ROOT/name,target)
    (out/"source-inputs.json").write_text(json.dumps(inputs,sort_keys=True,indent=2)+"\n")
    metadata={"image":a.image,"image_id":info["Id"],"binary_sha256":hashlib.sha256(binary.read_bytes()).hexdigest(),"container_started":False,"mode":"ordinary-user-process","producer_base":subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip()}
    with (out/"collector.log").open("x") as log:
        collector=subprocess.Popen([str(binary),"--config="+str(out/"collector.yaml")],cwd=out,env={"PATH":"/usr/bin:/bin","LANG":"C.UTF-8"},stdout=log,stderr=subprocess.STDOUT)
        try:run_test(f"http://127.0.0.1:{port}/v1/traces",out)
        finally:
            collector.terminate()
            try:collector.wait(timeout=5)
            except subprocess.TimeoutExpired:collector.kill();collector.wait(timeout=5)
            metadata.update(collector_pid=collector.pid,collector_exit=collector.returncode)
            (out/"collector-process.json").write_text(json.dumps(metadata,indent=2)+"\n")

if __name__=="__main__":main()
