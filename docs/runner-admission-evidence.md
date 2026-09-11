# E47 — Runner admission before RPC drain

The runner executable now calls `Engine.BeginShutdown` as soon as its signal
context ends, before waiting for gRPC shutdown. Previously admission stayed open
through that wait because `Engine.Close` ran afterward. A handler waiting for a
workspace could therefore admit another operation during the drain window.

`BeginShutdown` sets the existing closed flag and cancels runner-side operation
contexts under the admission mutex. Authorization checks reject new requests and
recheck the flag after acquiring a workspace lock; dispatch also checks it before
registering its goroutine. The journal and exclusive pool locks remain open until
RPC handlers drain and `Close` finishes the operation goroutines. Shutdown records
an uncertain running outcome without requesting business cancellation. An already
dispatched external request can still become observable after this local boundary;
the flag is not an atomic fence at a remote Docker daemon.

## Recorded checks

- [Host checks](../benchmarks/results/runner-admission-e47-20260911-host/results.json):
  runner, runnerclient and runner executable race tests have 102 passing entries,
  three explicit opt-in skips and no failures. Vet passes. The progress audit's ten
  Python regression methods pass and are now included in CI.
- The new test starts one deterministic backend job, leaves a second request
  pending, begins shutdown twice and checks one reservation, one Start, zero
  Cancel calls, and the original operation retained as unknown. A successor opens
  the same SQLite journal and inspects the externally completed original job into
  one successful receipt without another Start.
- [Copy-only negative control](../benchmarks/results/runner-admission-e47-20260911-host/late-close-negative.log)
  makes early shutdown a no-op while retaining cancellation in `Close`, modeling
  the previous executable order. The same test fails at its five-second drain
  bound. It is a controlled ordering experiment, not an old executable SIGTERM run.
- [Input identity](../benchmarks/results/runner-admission-e47-20260911-host/identity.json)
  records 268 Go/module/SQL/protocol/workflow inputs unchanged across the checks,
  based on commit `826f88f` plus the captured working-tree patch and new test.
  The first [sandbox attempt](../benchmarks/results/runner-admission-e47-20260911/failure-scope.json)
  retains runnerclient socket-denial failures; the host rerun passes.
- [Independent review](../benchmarks/results/runner-admission-e47-independent-review/forge-e47-independent-review.json)
  checks the source, negative control, raw counts and manifests with no open P1/P2.

This is real SQLite/filesystem and deterministic `TestBackend` evidence. It does
not execute Docker or send an OS signal. Actual API SIGTERM is separate [E45](api-lifecycle-evidence.md);
the full worker/runner process scenario remains open under S16.4. No live service
or mounted workspace was changed by E47.

The preceding integrated commit `826f88f` passed
[GitHub Actions](https://github.com/JDinSeattle/forge-runtime/actions/runs/34659200602);
its [raw archive](../benchmarks/results/github-actions-826f88f/run.json) identifies
that exact commit. That CI run predates E47 and does not validate this new patch.
