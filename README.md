# Forge Runtime

A Go coding-agent execution platform built around a specific failure question:
**after a worker disappears, did the command actually run?**

PostgreSQL owns task intent, worker leases, approvals and budget reservations.
A separate runner owns operation facts in a SQLite journal, bounded workspaces
and authenticated receipts. A timed-out request is inspected by the same
operation ID on the original runner before execution can continue.

This is an engineering project with reproducible failure tests, not a claim of
production adoption or prior employment. Current measured results and remaining
acceptance work are recorded in [the evidence ledger](docs/evidence.md).
See the [remaining acceptance boundaries](docs/open-acceptance.md) and
[Chinese interview case study](docs/portfolio-zh.md) when evaluating its scope.

```mermaid
flowchart LR
  CLI[Go CLI] -->|HTTP + resumable SSE| API[API / restricted DB role]
  API --> PG[(PostgreSQL)]
  W[Bounded workers] -->|claim / epoch / transaction| PG
  W -->|native streaming| LLM[OpenAI / Anthropic / Fake]
  W -->|typed gRPC + signed grants| R[Independent runner]
  R --> J[(SQLite operation journal)]
  R --> D[Rootless Docker / no network]
  D --> V[Exclusive bounded ext4 slot]
  R --> A[Immutable artifact files]
  W --> A
  API -->|authorize then stream| A
```

The repository contains five binaries:

| Binary | Responsibility |
| --- | --- |
| `forge` | Submission, watch/reconnect, approval, control and verified downloads |
| `forge-api` | Tenant authorization, idempotent HTTP admission, SSE and artifacts |
| `forge-worker` | Durable finite-state driver, quotas, model adapters and runner reconciliation |
| `forge-runner` | File tools, isolated processes, epoch fencing, SQLite receipts and fixed-volume leases |
| `forge-admin` | Migrations, operator bootstrap, token issuance and maintenance |

Key implementation decisions are explained in [ADR 0001](docs/adr/0001-receipt-first-runtime.md)
and [ADR 0002](docs/adr/0002-workspace-storage.md). The [implementation map](docs/implementation-map.md)
tracks the scope of the [SDE plan](docs/spec/SDE_IMPLEMENTATION_PLAN.md).

The runtime also supports a frozen, one-way [compatible model fallback](docs/provider-handoff-evidence.md),
a durable [closed-batch repetition limit](docs/no-progress-evidence.md), and
[API/worker/runner trace propagation](docs/telemetry-chain-evidence.md).
Their evidence distinguishes local native-protocol fixtures, real database and
journal behavior, and the still-pending paid-model and operational acceptance.

## Build and verify

Go 1.26 and Git are required; contract generation also requires protoc 36.1.
PostgreSQL integration tests require PostgreSQL 17;
the supplied development compose binds it to an allocated loopback port.

```bash
go build -o bin/ ./cmd/...
go test ./...
go test -race ./...
go vet ./...
sh api/check-generated.sh
sh db/check-generated.sh
sh proto/check-generated.sh
```

Default tests use deterministic provider/executor doubles and skip tests that
require an explicit database. `var/go.mod` is a module boundary that prevents Go
from scanning persistent Docker and workspace data.

```bash
docker compose -p forge-runtime -f deploy/compose/compose.yaml up -d postgres
docker compose -p forge-runtime -f deploy/compose/compose.yaml port postgres 5432
# Use the printed port, not a guessed or shared PostgreSQL instance.
export FORGE_DATABASE_URL='postgres://forge_admin:forge_dev_only@127.0.0.1:PORT/forge?sslmode=disable'
export FORGE_TEST_DATABASE_URL="$FORGE_DATABASE_URL"
export FORGE_REVIEW_DATABASE_URL="$FORGE_DATABASE_URL"
export FORGE_REVIEW_ALLOW_FIXTURES=1
go test ./...
go test -race ./...
```

Integration helpers require loopback `/forge`, create private schemas, and remove
only their own fixtures. Paid provider calls are absent from these suites.

## Local execution

The real process backend currently targets Linux with user namespaces, delegated
cgroup v2, rootless Docker and a mounted pool of bounded ext4 image files. It
refuses an ordinary unbounded host directory. The API and workers run without
host root; task containers use namespace UID/GID 1000, no capabilities, no
network, read-only root filesystems and explicit memory/PID/CPU limits.

1. Build the binaries and start the development PostgreSQL above.
2. Initialize a new private directory:

   ```bash
   bin/forge-admin -repo "$PWD" -state "$PWD/var/local" local-setup
   ```

   This applies migrations, creates separate nonowner API/worker database roles,
   registers three Python sources and one Go source, and writes private `api.env`,
   `worker.env`, `client.env`, signing key and JSON configuration. A repeated
   setup refuses to overwrite existing state. The worker has explicitly scoped
   cross-tenant service authority; the API role must pass the RLS deployment check.

3. Follow [fixed volume provisioning](scripts/workspace-volumes/README.md),
   including the narrow operator mount step. Mounting images is setup, not proof
   that container isolation works.
4. Follow the [runner setup guide](cmd/forge-runner/README.md) to start the
   dedicated rootless daemon, select a pinned image, derive verified volume
   identities with `configure-local.py`, and launch the mapped runner.
5. Start each service with its own environment file:

   ```bash
   (set -a; . var/local/api.env; set +a; exec bin/forge-api -config "$PWD/var/local/platform.json")
   (set -a; . var/local/worker.env; set +a; exec bin/forge-worker -config "$PWD/var/local/platform.json")
   ```

The API defaults to `127.0.0.1:8097`. Worker metrics default to
`127.0.0.1:8098` and runner metrics to `127.0.0.1:8099`;
override `FORGE_METRICS_LISTEN` for an additional worker and
pass a unique `-id`. Use a TLS reverse proxy for remote API access. Runner TCP
requires TLS 1.3 mutual authentication and configured peer identity; plaintext
TCP is not a deployment option.

## Repair and inspect

```bash
set -a; . var/local/client.env; set +a
bin/forge project create --name clamp --source clamp --profile python-clamp
# Copy the returned project ID and sources.clamp.hash from platform.json.
bin/forge run submit PROJECT_ID --task-file testdata/repairs/clamp/task.txt \
  --base SOURCE_HASH --config demo --idempotency-key clamp-example-1
bin/forge run watch RUN_ID
bin/forge run artifacts RUN_ID
```

`demo` uses deterministic fake model scripts with a known patch. The runner
still applies actual file changes and runs target/regression checks in a clean
container. The three fixtures exercise clamp bounds, TTL deadline equality and
touching closed intervals. They demonstrate platform behavior, not model quality.
An additional Go integer-division fixture uses a separate trusted grader that
builds and executes the candidate offline, including overflow and invalid-input
cases. Run it explicitly with `python3 scripts/demo-repairs.py --fixture go-ceil-div`.

`python3 scripts/demo-repairs.py --fixture all` drives these fixtures through
the real CLI, saves events and artifacts, and applies each downloaded patch to
an independent source copy. It requires the local services above and available
workspace slots. Completed workspaces remain retained until the explicit
[snapshot-before-release cleanup](docs/operations.md).

The final output is a standard patch checked against original file hashes. The
artifact list also includes the structured diff, model results, operation
receipts, baseline check and independent verification report. A completion text
alone cannot produce a verified result. Command execution requires approval
bound to the exact operation, canonical arguments, workspace revision and policy.

For real providers, add exact model IDs/capabilities and a versioned price policy
to the operator configuration, then set `OPENAI_API_KEY` or `ANTHROPIC_API_KEY`
only in the worker environment. No model alias, current price or usage total is
guessed. Native response state remains private persisted context, and incomplete
tool-call streams cannot dispatch effects. Text-only input admission uses a
conservative UTF-8 byte and framing bound against the frozen reservation; actual
billing comes from final usage, with unknown obligations retained for reconciliation.

## Failure semantics and operations

- Same HTTP idempotency key plus same body resolves to the same run. The CLI
  persists its mutation receipt before transmission and does not blindly retry.
- Worker updates require current owner, epoch, version and unexpired database
  lease at write time. Fixed-client workers claim only their configured runner.
- A previous dispatch with uncertain outcome remains uncertain until runner
  evidence settles it. An HTTP/RPC timeout does not mean business cancellation.
- Stop admission is persistent across runner restart. A late adoption or start
  cannot reopen a stopped workspace.
- SSE transport history can expire. Old cursors receive `410 reset_required`;
  the CLI fetches a consistent snapshot and resumes at `covered_seq`.
- `forge-admin -age 168h -keep 128 trim-events` trims only old terminal transport
  events. Exact reducer input/output audit snapshots and effect records remain.
- Graceful worker shutdown stops claiming and cancels its local contexts. It
  leaves remote operation facts available for takeover rather than declaring
  every interrupted task cancelled.

Back up PostgreSQL, the runner journal, its fixed-volume identity manifests,
retained workspace images and immutable artifact files as one recovery set.
Restoring only PostgreSQL cannot establish whether an external operation ran.
Stop only the Forge services before image-level backups; never format, prune or
force-unmount volumes to recover from an uncertain operation.

Metrics are scrape-based and traces can be exported via
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`. Neither traces nor metrics replace the
database cost ledger. Raw prompts, tool arguments, keys and file paths are not
Prometheus labels. See [telemetry mapping](internal/telemetry/README.md) and the
[scheduler measurement](benchmarks/README.md) for exact scope and limitations.

MCP, ACP, Temporal, Redis, Kubernetes, S3 and multi-node workspace migration are
documented extension boundaries. They are not claimed as implemented features.
