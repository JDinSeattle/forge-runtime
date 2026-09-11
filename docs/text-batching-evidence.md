# E35 — Configurable and lossless text batching, 2026-09-11

This change adds operator-owned text delivery limits while preserving the default
100 ms / 16 KiB behavior. Text is split at UTF-8 rune boundaries, and every emitted
payload is checked against the actual 32 KiB JSON persistence limit. JSON escaping
can therefore produce several smaller events without changing their decoded
concatenation. The isolated acceptance passes real private-PG, configuration and both native F03 tests under race detection. This is delivery correctness, not model-quality evidence.

## Preserved original defects

The [before record](../benchmarks/results/text-batching-20260911/before/identity.json)
binds five unmodified source snapshots to commit
`a9208e79879d29373eb3964e098eff286aa6de82`. The
[standalone reproduction](../benchmarks/results/text-batching-20260911/before/repro.go.txt)
executes the original slicing/encoding algorithm without PostgreSQL, a provider
or a runner. Its [unchanged output](../benchmarks/results/text-batching-20260911/before/repro.log)
records:

- A valid 16,386-byte input ending in `你` becomes 16,392 decoded bytes with three
  replacement runes when a 16 KiB byte slice crosses that character.
- 16 KiB of `<` becomes a 98,366-byte JSON event, exceeding
  `Store.AppendWorkerEvent`'s 32 KiB payload limit even though the raw text fits.

The first is silent text corruption; the second reaches the existing consumer
failure path and cancels the provider. These are finite algorithm reproductions,
not claims of an observed deployed incident or an actual before-fix PG failure.

## Configuration and implementation

The deployment JSON may include:

```json
{
  "text_batch": {
    "interval_ns": 100000000,
    "max_bytes": 16384
  }
}
```

Each zero/omitted field gets its default. The interval must be 10 ms–1 s and the
raw-text size 4–16,384 bytes. Four bytes allow any one UTF-8 rune. This is operator
configuration, not a task, HTTP or repository setting. `configuration.Load`,
`Driver.RunWorker` and `Driver.Drive` validate it before accepting work. The worker
passes it explicitly to the Driver; existing configurations need no edit.

[TextBatchConfig and the sink](../internal/application/text_batch.go) keep at most
the configured number of raw pending bytes. When a rune does not fit, the existing
complete prefix flushes first. A flush marshals the actual payload, including
attempt identity and provisional status. If escaping exceeds the persistence
ceiling, a search over rune boundaries finds a fitting prefix. This bound covers
the payload, not the surrounding SSE event envelope or TCP frames.

Concurrent close calls share one stop/join/flush operation. Emit after successful
close is rejected. Any append error remains the first error, cancels the model
context, and is never retried by the sink because the commit outcome may be
unknown. [The model call](../internal/application/model.go) retains its existing
consumer-error precedence and independent complete-model artifact storage.
Invalid UTF-8 delta input is rejected rather than silently replaced.

## Verification scope

[Unit tests](../internal/application/text_batch_test.go) check default/bounded
configuration, early rejection before Store access, Chinese/emoji boundaries,
HTML/control/quote/backslash escaping, and exact reconstruction at four different
raw size limits. Eight concurrent close calls cannot duplicate a frame. Size,
timer and close append failures cancel once and never replay an uncertain write.
[Configuration tests](../internal/configuration/text_batch_test.go) exercise the
real JSON loader with omitted, explicit and invalid deployment policies.

The [private PostgreSQL cases](../internal/application/text_batch_integration_test.go)
separate these boundaries:

1. Timer-only text persists without a subsequent emit or close.
2. Large Unicode/escaped text persists through the size threshold and final close,
   then reconstructs exactly; another close adds no event.
3. With only the text sink mutex deliberately held, actual Store message and
   cancellation transactions commit ordered control events without a text flush.
   Cancellation then fences the pending provisional text and cancels its context.
   This is a Store control fixture, not an HTTP or slow-database-lock experiment.
4. The production Driver and FakeProvider retain the complete model turn in a
   separate actual local artifact; decoded PG deltas match that exact text.
   The repair uses SQLite/files and TestBackend, not a real container or model.

The [second actual run](../benchmarks/results/text-batching-20260911/actual-02-isolated/execution.json)
passes all three fixed race test binaries (application/configuration/benchmarks
exit codes `[0,0,0]`). Its application record contains three PG top-level tests,
four leaf cases, plus the unit checks; configuration cases pass separately.

| PG case in actual-02 | Retained observation |
| --- | --- |
| Timer, configured 40 ms | 21 decoded bytes in one text event, observed before close; first observation 56.412824 ms after emit began, including DB write/read and polling |
| Size and JSON encoding | 65,553 decoded bytes in eight text events; largest encoded payload exactly 32,768 bytes; byte-exact reconstruction and stable event prefix across close |
| Independent controls | Four ordered events: created, claimed, message-added, cancel-requested; zero text events while the sink mutex is held; both control calls return in 12.570067 ms, followed by fenced text and cancelled context |
| Complete-model artifact | 32,768 decoded bytes in eight text events among 46 ordered run events; text equals the separately loaded completed model turn, SHA-256 `88ad729f9ab2965a9d8ed7dcd24c68283cf938128ed910fdf91feceefc53ee4c` |

These are individual host-wall-clock observations, not percentiles or guarantees
that database/event observation finishes within the configured timer. The full
Driver case takes 1.35 s in the test log. Tests read an actual local model-response
artifact before fixture cleanup; the report retains its decoded turn and reference,
not the original transient object bytes. The author audit checks the retained
decoded content and event binding, not a newly re-read artifact file. PG fixtures
use administrator test connections and unique disposable schemas; they are not
API-role/RLS acceptance.

The unchanged native F03 cases then pass: OpenAI 8.44 s and Anthropic 8.42 s,
including natural request-deadline waits. Each has exactly one actual local native
HTTP request, one failed/incomplete attempt, zero effects, and five ledger phase
captures with event counts 10/11/11/11/11. Each preserves 9,216 reserved tokens and
10,240 synthetic microUSD; request concurrency expires once, then returns zero on
repeat. No model-response artifact or partial tool is executed. The negative
expense-refund oracle also passes. These are synthetic protocol/pricing fixtures
through the actual SDK/Driver/PG, not paid inference or containers.

The [author audit](../benchmarks/results/text-batching-20260911/author-audit.json)
reconstructs all PG text, checks ordered identities and encoded payload bounds,
reuses the retained F03 oracle, and verifies the fixed binaries and source scope.
The [manifest](../benchmarks/results/text-batching-20260911/author-manifest.json)
binds 186 files, including the [recomputation script](../benchmarks/results/text-batching-20260911/author-recompute.py)
and the unchanged F03 auditor. This is an author recheck of saved evidence, not
another OS execution. Separately, the root agent independently reviewed the
config validation, UTF-8/JSON prefix logic, concurrent close, uncertain-append
cancellation and Driver/worker wiring after actual-02, and reported no P1/P2
findings. That is a code-review record, not an additional experiment.

## Preserved execution failure and correction

[Actual-01](../benchmarks/results/text-batching-20260911/actual-01-isolated/execution.json)
has `[0,0,1]`: all PG/configuration cases pass, but both native cases fail at their
`go tool buildid` preflight before any PG/native stream activity. Their failed
reports remain unchanged. A separate credential-free reproduction retains the
[original stderr](../benchmarks/results/text-batching-20260911/launcher-go-env-preflight/before.stderr):
the minimal child environment had no GOCACHE, HOME or XDG_CACHE_HOME.

Only the Python launcher changes for actual-02. It inherits a named whitelist of
Go/cache/temp path settings, supplies a cache default and sets `GOENV=off`,
`GOTOOLCHAIN=local`, `GOPROXY=off`, `GOSUMDB=off`; it does not forward the entire
parent environment or provider credentials. The same buildid command now returns
zero, with [before/after records](../benchmarks/results/text-batching-20260911/launcher-go-env-preflight/after.json).
The new launcher SHA-256 is
`2ed65df87321af400b9ac7c0bd788b246b58a516da2d4a0de36329d0d88ffc79`, and its exact
[execution snapshot](../benchmarks/results/text-batching-20260911/actual-02-isolated/launcher.py)
is retained. All Go/SQL/module inputs and all four build02 binary hashes remain
unchanged; no Go recompile or fault-criterion change produced the passing repeat.

## Reproduction and source identity

The [acceptance launcher](../internal/application/text_batch_acceptance.py) has
separate build and run modes. It records pre/post Go/SQL/module hashes, refuses
source changes during build, keeps the race-instrumented test binaries, and checks
their hashes before execution. Run mode also rejects source drift because the
unchanged F03 harness captures source at execution. Only a private, owner-readable
environment file supplies the explicitly allowed loopback `/forge` test database;
its values are never written to evidence or passed as argv.

```sh
python3 internal/application/text_batch_acceptance.py build \
  --output benchmarks/results/text-batching-20260911/build-01
python3 internal/application/text_batch_acceptance.py run \
  --build-record benchmarks/results/text-batching-20260911/build-01 \
  --env-file var/local/review-database.env \
  --output benchmarks/results/text-batching-20260911/actual-01
```

Use fresh output directories; existing records are never overwritten. The first
[working-tree build](../benchmarks/results/text-batching-20260911/build-01/identity.json)
compiled successfully but was not used for the actual experiment. To avoid parallel
features changing source identity, the operator instead created an
[isolated checkout](../benchmarks/results/text-batching-20260911/isolated-build-preflight/scope.json)
of a9208e7 and copied only the nine owned files. Both actual runs use
[build-02-isolated](../benchmarks/results/text-batching-20260911/build-02-isolated/identity.json):
199 unchanged Go/SQL/module inputs, exactly eight batching Go files differing
from or added to that base, plus the separate Python launcher. The author audit
reconstructs all other inputs from the base commit. This supplies a strict
base-plus-change identity, not a new CI result or a clean commit containing the
uncommitted change.

The fixed application test SHA-256 is
`ffc9af70676381e704599778bde9aa742fc4a16153e63859ed6838bb1401da1c`;
the benchmark test is
`c4bae2db723e1b24da829f83e484e8a1097b25f4ef95e6a643ce68d80f750f57`.
All executable paths/hashes are recorded in build02 and still verified locally;
large binaries reside in the workspace temporary build directory, not in the
versioned evidence bundle. `TMPDIR`/`GOTMPDIR` selected workspace storage because
host `/tmp` capacity was limited. For an isolated repeat, run the same launcher
from that checkout and point `--build-record` at build02; the source gate and
binary verification stay enabled. The checked-in example above builds a new
record from whichever explicitly selected checkout contains the launcher.

The run mode executes both original F03 native interrupted-stream cases plus the
negative ledger oracle. Their HTTP servers produce synthetic native protocol
bytes locally; no paid provider request is made. Original F03 reports remain in
their fresh directories and are copied into this acceptance output. These checks
must continue to preserve unknown fees/tokens, withhold incomplete tools and
release only expired request concurrency; batching does not relax those rules.
