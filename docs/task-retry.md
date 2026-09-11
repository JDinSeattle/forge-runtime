# Whole-task retry

`forge run submit PROJECT_ID --parent-run RUN_ID` creates a new run linked to a terminal parent. Supply the original task and immutable base, a current server configuration and the desired budget limits, using the ordinary submission flags. The API accepts `parent_run_id` in `POST /v1/projects/{id}/runs` and returns it in run/snapshot reads. A parent must belong to the authenticated tenant and the same project; its task and base must match exactly. Nonterminal parents or mismatched input return conflict; inaccessible parents return not found.

The child starts queued with version 1, zero model/tool/cost counters, no lease or prepared workspace, a fresh runtime deadline and its own operation identities. It carries the original accepted input snapshot byte-for-byte. It does not inherit the parent's mutable workspace, messages, approvals, effects, completed outputs or unknown provider reservations. Its requested configuration and budgets pass the same named-server limits as any submission. Existing parent charges and unresolved accounting remain attached to the parent. No job or model call is dispatched by the API.

The parent relationship and initial child snapshot/event are committed in the normal idempotent acceptance transaction. The parent remains terminal and unchanged. Retrying the same submission key returns the same child; changing its parent changes the request hash. Use a new explicit key for an intentionally separate retry of the same parent. The CLI's saved receipt includes the parent field and refuses a changed parent for an existing key. Requests without a parent retain their previous canonical hash input, including field order and omission behavior.

`run resume --version` remains the operation for reconciling the original nonterminal run on its original runner. It cannot reopen a terminal parent. Whole-task retry always starts from the registered immutable source, so it is not a way to continue an unknown write in a reused workspace.

The implementation uses the existing tenant-scoped `runs.parent_run_id` foreign key; no schema migration is needed. Terminal-parent validation and the insert share one transaction. The optional JSON fields are additive. Older clients can still submit ordinary runs; clients that reject unknown response fields must update before reading child runs.

Verification is control-plane scoped: real TCP HTTP, a nonowner/RLS PostgreSQL role, concurrent duplicate submissions, parent/input/counter checks, authorization failures and terminal resume rejection. The terminal parent is prepared with an explicit fixture stop receipt and no executor; this test does not claim container cancellation or model quality. The production cancellation/runner matrices provide separate evidence.

New ordinary submissions capture a versioned JSON admission record in the existing
`runs.input_snapshot` text column. It records the accepted task, project, immutable
base, registered source and profile in the same transaction as admission; the
configuration remains in `config_snapshot`. This is a record identifying the
registered input, not a copy of repository bytes or a mutable workspace. New
child runs copy the parent's recorded bytes. Existing empty/legacy references
are preserved without inventing historical input, and duplicate keys return
the existing run without reconstructing its snapshot. The server and Driver
still validate the registered source/base before execution.

The reserved `priority` field accepts integers -2 through 2 and defaults to 0.
`forge run submit --priority` stores this metadata; dispatch still rotates tenants
and chooses FIFO within a tenant. It does not confer authorization, preemption or
a waiting-time guarantee. Explicit zero and omission normalize identically in
server admission; a nonzero change with the same idempotency key conflicts.
Migration `00009_run_priority.sql` adds the bounded column with default zero,
so existing rows acquire the same baseline value. Apply migrations before
running the new API/worker binary; the previous binary remains compatible with
the additive column. No public fixture migration/deployment is claimed by the
private-schema tests.

The [retained admission tests](../benchmarks/results/run-admission-20260911/report.json)
pass under the race detector with actual loopback HTTP and private PostgreSQL.
They cover child/input identity, 12 concurrent same-key retries, authorization and
invalid parents, finite priority bounds in both API and SQL, explicit/default-zero
replay, and FIFO low-priority-before-high-priority claims. CLI receipt and the old
ordinary request hash regression also pass. Their fixture stop receipt has no
container execution, as described above. Earlier [retry-only evidence](../benchmarks/results/task-retry-20260911/report.json)
is retained separately; that command's CLI regex selected no CLI test, whereas
the later admission command explicitly ran the parent-receipt test.
