# Local artifact orphan retention

`forge-admin artifact-gc` collects old unpublished objects and abandoned staging
files in the single-runner local deployment. It defaults to a dry run, requires
an age of at least one hour, and handles at most 100 objects by default (maximum
10,000). It does not retire READY artifacts, journal receipts, or durable runner
pins. These still carry recovery evidence even when a run has stopped.

## Publication and collection protocol

Every local publisher holds a shared `.publication.lock` file lock from before
writing/deduplicating bytes until its authoritative reference commits. Worker
publications verify content and commit PostgreSQL metadata plus the event in
that scope. Runner operations persist a journal pin and their operation receipt
in that scope. Snapshot and stop responses also receive durable pins before
returning, protecting the response-to-PostgreSQL gap. Journal schema 4 introduces
`runner_artifacts`; older operation receipts remain protected through the
existing `operations.receipt_json` column.

Collection obtains the exclusive file lock first, then reads every PostgreSQL
artifact key and every runner reference. An API role without all-tenant
visibility is rejected. Missing/unreadable/corrupt journals, an unsupported
schema, or a second runner allocation abort collection before any deletion.
The root also records its runner journal in a durable `.runner-journal-*` file;
the command rejects an unrelated empty journal or an omitted second registered
authority. It binds both the canonical path and the durable random
`journal_identity.id`; recreating an empty SQLite file at the same path cannot
replace the original reference authority. Registration has no guessed expiration.

Only exact lowercase SHA-256 names or `.staging-<32 lowercase hex>` names under
valid tenant/run directories are considered. Symlinks, unknown names, fresh
files, READY keys, and journal-pinned keys survive. Unlinks are followed by parent
directory fsync. Cancellation or failure can leave a partially completed batch;
rerunning remains safe because every removed item was an unreferenced orphan.
The exclusive scan has a one-million-entry safety bound and the operator
command's 30-second deadline.

Deduplication does not make file age a publication lease. A publisher reusing
old bytes holds the same shared lock, and the collector reads references only
after that publisher releases it. Conversely, if collection wins first, a
delayed publisher must revalidate the bytes under the lock and fails without
creating a READY reference. Process death releases the OS lock. An object pinned
before a later journal transition fails remains protected for reconciliation.

## Operator use

Before the **first** collection after upgrading, quiesce the deployment and
restart the runner and all workers with the publication-lock implementation.
Verify runner journal schema 4 and resume the upgraded publishers. Schema
migration alone does not upgrade an already running worker. Never run GC while
old binaries or external direct writers can bypass this protocol.

Use the existing private operator database environment; do not paste credentials
into evidence logs. Pass the actual platform and runner configurations:

```sh
bin/forge-admin -config /absolute/path/platform.json \
  -runner-config /absolute/path/runner.json -age 168h -limit 100 artifact-gc

bin/forge-admin -config /absolute/path/platform.json \
  -runner-config /absolute/path/runner.json -age 168h -limit 100 -apply artifact-gc
```

The JSON result contains the cutoff, protected/examined counts, candidate keys,
sizes, staging classification, and whether deletion was requested. Repeat a dry
run or applied batch to continue scanning. There is no automatic deletion job.

This lock protocol applies to a cooperating local filesystem deployment. A
multi-runner or S3 implementation must account for all durable reference
authorities and supply a distributed publication/collection protocol first.
Coordinated restore must explicitly rebind journal registrations together with
the restored journal, artifact bytes, workspace images and their identities.
Changing one configuration path is insufficient.

## Verification

`internal/artifact` tests exercise an old-object deduplication race, exact-name
and age filtering, dry-run/batch restart behavior, failed reference reads,
cross-process exclusion, forced publisher process death, atomic concurrent
journal registration, and refusal of an omitted/unrelated authority.

`internal/retention` uses a private PostgreSQL schema and a WAL SQLite fixture to
exercise both race orderings, READY protection, runner-only protection, and
missing/corrupt/old-schema journal refusal. The application suite separately
executes the durable repair loop and model-output reuse under the new lock.
These are local correctness tests; they do not establish distributed object
store semantics or a retention policy for retiring recoverable references.

The [actual local operator rehearsal](../benchmarks/results/artifact-retention-20260911/report.json)
verified 101 distinct READY keys and 159 total PostgreSQL/journal keys before and
after collection. Its dry run selected exactly one synthetic orphan deliberately
aged eight days; the applied seven-day policy removed only that object. Every
original referenced object's hash and length still matched. The
[reproduction harness](../scripts/retention/check-local.py) records this fixture
and refuses to continue if the dry-run candidate set contains another object.
