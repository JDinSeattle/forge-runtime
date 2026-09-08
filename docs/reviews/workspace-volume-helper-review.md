# Independent workspace volume helper review

Reviewed 2026-09-07 by the independent review agent. This review covers the
provisioning helper source, not actual privileged execution or a human security
endorsement.

## Source reviewed

| File | SHA-256 |
| --- | --- |
| `scripts/workspace-volumes/common.py` | `bf50b9bfaef24a1d6b2980495a6d84baa68ab10b6184ba21b56a0b5e89229bc2` |
| `scripts/workspace-volumes/mount.py` | `bfa00fab40652ba84e18409f85fd56bc1c1285923cfea6295a3bd31a7130027a` |
| `scripts/workspace-volumes/volumes.py` | `683ebc5e3538ba65a701b4d7cad17d6e21d12cb1ab1cfc54835d2689718cd4bc` |

No blocking source issue was identified within the documented scope: an exact,
one-shot invocation approved by the trusted operator who owns this repository.
The source does not support an unrestricted elevated Python command rule. These
scripts and their imported module are user writable, and a malicious host owner
editing the approved program or filesystem image during execution is explicitly
outside this threat model. Task containers must have no access to those files.

The reviewed implementation fixes the allocation at four 256 MiB images and
derives all target paths from its own repository location. Preparation creates
new regular files exclusively, pins their descriptors, refuses existing-file
formatting, verifies identity, and writes a durable manifest. The root helper
requires isolated Python and the initial root namespace, uses fixed executables
with a clean environment and no shell, checks loop backing identity, and binds
mount targets through anchored descriptors. Recorded root-owned mount state is
required for reuse or teardown. Unexpected loops, mounts, ownership, remaining
checkout data, or another mount namespace cause refusal. Teardown uses ordinary
unmount and exact-device detach, with no forced unmount, global cleanup, or image
deletion.

## Validation and limits

The reviewer ran:

```sh
python3 -B -m unittest discover -s scripts/workspace-volumes -p 'test_*.py' -v
```

All 14 tests passed. They exercise temporary-file identities and mocked
format/mount command construction; this invocation did not format, attach, or
mount an image. Hashes above were rechecked after the test run.

The implementation owner subsequently reported successful image preparation
and, after an initial privilege-elevation block, four mounted volumes and a
rootless Docker daemon with cgroup v2. This reviewer did not perform or
independently verify an actual mount. Effective ENOSPC limits,
exclusive runner volume leases, nonroot UID mapping, container memory/PID/CPU
limits, and restart behavior remain separate acceptance requirements. See
[the design review](design-review.md), [ADR 0002](../adr/0002-workspace-storage.md),
and [the helper instructions](../../scripts/workspace-volumes/README.md).
