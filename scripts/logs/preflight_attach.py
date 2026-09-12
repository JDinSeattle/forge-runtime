#!/usr/bin/env python3
"""Opt-in, no-mount Docker attach-before-start probe. Owns one fresh container."""
import argparse
import base64
import hashlib
import http.client
import json
import pathlib
import socket
import struct
import threading
import time
import uuid


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--socket", required=True)
    p.add_argument("--output", type=pathlib.Path, required=True)
    p.add_argument("--image", default="python@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36")
    a = p.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)
    name = "forge-e44-preflight-" + uuid.uuid4().hex
    evidence = {"name": name, "image": a.image, "passed": False, "events": [], "container_id": None}
    begin = time.monotonic_ns()

    def record(kind, **data):
        evidence["events"].append(dict(kind=kind, monotonic_ns=time.monotonic_ns()-begin, **data))

    class UnixHTTP(http.client.HTTPConnection):
        def connect(self):
            self.sock = socket.socket(socket.AF_UNIX)
            self.sock.settimeout(8)
            self.sock.connect(a.socket)

    def request(method, path, body=None):
        c = UnixHTTP("localhost", timeout=8)
        try:
            c.request(method, "/v1.51"+path, json.dumps(body) if body is not None else None, {"Content-Type":"application/json"})
            r = c.getresponse()
            payload = r.read(65536)
            assert len(payload)<65536
            if r.status>=300: raise RuntimeError(f"Docker {method} {path}: {r.status}: {payload[:512]!r}")
            return json.loads(payload) if payload else None
        finally:c.close()

    attached = None
    try:
        created = request("POST", "/containers/create?name="+name, {
            "Image": a.image, "User":"1000:1000", "AttachStdout":True,"AttachStderr":True,"Tty":False,
            "Labels":{"forge.e44.preflight":name}, "Cmd":["python","-I","-B","-c","import os; os.write(1,b'early-out\\x00'); os.write(2,b'early-err\\xff')"],
            "HostConfig":{"ReadonlyRootfs":True,"NetworkMode":"none","CapDrop":["ALL"],"SecurityOpt":["no-new-privileges"],"Memory":268435456,"MemorySwap":268435456,"NanoCpus":1000000000,"PidsLimit":64,"LogConfig":{"Type":"none"}}})
        cid = evidence["container_id"] = created["Id"]
        record("created", container_id=cid)
        attached = socket.socket(socket.AF_UNIX); attached.settimeout(8); attached.connect(a.socket)
        attached.sendall((f"POST /v1.51/containers/{cid}/attach?stream=1&stdout=1&stderr=1&stdin=0 HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: tcp\r\nContent-Length: 0\r\n\r\n").encode())
        reader = attached.makefile("rb")
        status = reader.readline(8192).decode().strip(); assert " 101 " in status, status
        total=0
        while True:
            line=reader.readline(8192);total+=len(line);assert total<16384 and line
            if line==b"\r\n":break
        record("attach_ack_before_start", status=status)
        streams={1:bytearray(),2:bytearray()}; errors=[]
        def drain():
            try:
                while True:
                    header=reader.read(8)
                    if not header:break
                    assert len(header)==8 and header[0] in streams and header[1:4]==b"\0\0\0"
                    n=struct.unpack(">I",header[4:])[0];assert n<=32768
                    data=reader.read(n);assert len(data)==n
                    streams[header[0]].extend(data); assert sum(map(len,streams.values()))<=32768
            except BaseException as err:errors.append(repr(err))
        thread=threading.Thread(target=drain);thread.start()
        record("start_begin");request("POST",f"/containers/{cid}/start");record("start_ack")
        stopped=request("POST",f"/containers/{cid}/wait?condition=not-running");thread.join(10)
        assert not thread.is_alive() and not errors,errors
        assert stopped["StatusCode"]==0
        assert bytes(streams[1])==b"early-out\0" and bytes(streams[2])==b"early-err\xff"
        state=request("GET",f"/containers/{cid}/json")
        assert state["HostConfig"]["LogConfig"]["Type"]=="none" and not state["State"]["Running"]
        evidence.update(streams={str(k):base64.b64encode(v).decode() for k,v in streams.items()}, log_config=state["HostConfig"]["LogConfig"], mounts=state["Mounts"], log_path=state.get("LogPath"), exit_code=stopped["StatusCode"])
        record("capture_complete");evidence["passed"]=True
    except BaseException as err:
        evidence["error"]=repr(err)
        raise
    finally:
        if attached:attached.close()
        cid=evidence["container_id"]
        if cid:
            try:
                state=request("GET",f"/containers/{cid}/json")
                assert state["Id"]==cid and state["Config"]["Labels"].get("forge.e44.preflight")==name
                if state["State"]["Running"]:request("POST",f"/containers/{cid}/kill");state=request("GET",f"/containers/{cid}/json")
                assert not state["State"]["Running"]
                request("DELETE",f"/containers/{cid}");record("owned_container_removed")
            except BaseException as err:evidence["cleanup_error"]=repr(err);evidence["passed"]=False
        evidence["script_sha256"]=hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()
        (a.output/"report.json").write_text(json.dumps(evidence,indent=2)+"\n")
        if not evidence["passed"]:raise RuntimeError("preflight failed; inspect retained report")


if __name__=="__main__":main()
