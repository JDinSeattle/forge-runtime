"""Pure local corpus and report primitives; importing this module performs no I/O."""
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess

HERE = Path(__file__).absolute().parent
REPO = HERE.parents[1]
CORPUS = HERE / "corpus"
CASES = ("py-utf8-chunks", "py-latest-records", "go-midpoint", "go-rune-rle")
VERSION = "eval-v2"
SOURCE_PREFIX = "evalv2_"


def go_environment():
    # Reuse the caller's cache (including CI setup-go), or Go's normal default.
    return dict(os.environ, GOPROXY="off")


def local_output(command, *, input=None, cwd=None, env=None, timeout=90):
    """Bound a local helper and its inherited descendants to one owned group."""
    process = subprocess.Popen(command, cwd=cwd, env=env, text=True,
                               stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
                               stdout=subprocess.PIPE, start_new_session=True)
    try:
        output, _ = process.communicate(input, timeout=timeout)
    except BaseException:
        # subprocess.run(timeout=...) kills only the direct child. go run also
        # starts compiler/linker children, so stop the group we created first.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.communicate()
        raise
    if process.returncode:
        raise subprocess.CalledProcessError(process.returncode, command, output=output)
    return output


def file_hash(path):
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1 << 20):
            hasher.update(chunk)
    return hasher.hexdigest()


def corpus_files(root=CORPUS):
    files = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        if path.is_symlink() or not (path.is_dir() or path.is_file()) or any(c in relative for c in "\t\r\n"):
            raise ValueError("nonregular or ambiguous corpus path")
        if path.is_file() and relative != "manifest.json":
            files.append({"path": relative, "sha256": file_hash(path), "size": path.stat().st_size, "executable": bool(path.stat().st_mode & 0o111)})
    return files


def corpus_digest(files):
    material = VERSION + "\n" + "".join(f"{row['path']}\t{row['sha256']}\t{row['size']}\t{int(row['executable'])}\n" for row in files)
    return hashlib.sha256(material.encode()).hexdigest()


def verify_corpus(root=CORPUS):
    manifest = json.loads((root / "manifest.json").read_text())
    files = corpus_files(root)
    if manifest.get("version") != VERSION or manifest.get("case_order") != list(CASES) or manifest.get("files") != files or manifest.get("sha256") != corpus_digest(files):
        raise ValueError("fixed evaluation corpus changed; do not relabel an adapted corpus as " + VERSION)
    return manifest


def save_json(path, value, replace=False):
    raw = (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode()
    temporary = path.with_name(path.name + ".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        if replace:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)
            temporary.unlink()
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def valid_id(value):
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value) is not None


def grade_command(case, suite):
    if case not in CASES or suite not in ("target", "regression"):
        raise ValueError("unknown grader selection")
    if case.startswith("py-"):
        return ["python", "-I", "-B", "/forge-tests/python-check.py", case, suite]
    return ["env", "-i", "PATH=/usr/local/go/bin:/usr/bin:/bin", "sh", "/forge-tests/go-check.sh", case, suite]
