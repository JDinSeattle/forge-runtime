# HTTP contract and CLI

`openapi.yaml` is the OpenAPI 3.1 control-plane contract. It describes the actual
Go handlers, including server-owned config IDs, optional budget reductions,
immutable approval bindings, typed run state, structured errors, SSE recovery,
and artifact pagination/downloads. Authentication uses `Authorization: Bearer`
and `X-Forge-Tenant`; credentials are never query parameters.

The client and Go wire types are generated with the module-pinned oapi-codegen
v2.8.0. Do not edit `internal/httpcontract/client.gen.go` by hand.

```sh
sh api/generate.sh
sh api/check-generated.sh
go test ./cmd/forge ./tests/client
go build -o bin/forge ./cmd/forge
```

`check-generated.sh` regenerates into a temporary file and compares exact bytes.
`tests/client` compares documented routes to actual handlers, validates live Go
wire models against schemas, and checks generated-client round trips.

## CLI examples

Use the local development credentials created by the administrative bootstrap:

```sh
export FORGE_API_URL=http://127.0.0.1:8080
export FORGE_TENANT=your_tenant_id
# Set FORGE_TOKEN in your environment; never put it in URLs or command flags.

forge project create --name example --source fixture-python --profile python
forge run submit PROJECT_ID --task-file task.txt --base BASE_COMMIT --config demo
forge run watch RUN_ID
forge run snapshot RUN_ID
forge run message RUN_ID --text 'Keep the public API stable.'
forge run cancel RUN_ID
forge run resume RUN_ID --version REVIEWED_VERSION
forge run artifacts RUN_ID --limit 100
forge run download ARTIFACT_ID --output patch.diff

forge run approval APPROVAL_ID > reviewed-approval.json
# Review the pending effect in the run snapshot before choosing a decision.
forge run approve APPROVAL_ID --binding-file reviewed-approval.json --allow
```

Submit and message commands create a private, synced receipt before sending any
request. The default key is reused for identical endpoint/tenant/route/body input.
Uncertain HTTP results are not automatically retried. Rerun the unchanged command
to use its saved key. A confirmed local receipt returns the original resource
without creating another run. Supply a new explicit `--idempotency-key` when you
intend to create another identical run. Reusing an explicit key with changed
input fails locally. Unconfirmed receipts older than 23 hours require server
reconciliation because the current API retains idempotency records for 24 hours.

Receipts contain keys, request hashes, and small API results, not bearer tokens
or task text. `FORGE_STATE_DIR` overrides the default XDG state directory.

Watch prints complete JSON events, reconnects with `Last-Event-ID`, suppresses
duplicates, and resets from a fresh atomic snapshot after HTTP 410. It rejects
oversized/deep JSON and mismatched sequence/type/run identities, discards partial
frames, and stops at `run.finished`. A bounded reconnect budget prevents endless
failure loops; the diagnostic cursor can be supplied with `--after` later.

Downloads enforce a byte bound, Content-Length, and the SHA-256 ETag. They publish
only verified bytes to a new destination and never replace an existing file.
Redirects are disabled so bearer credentials cannot be forwarded to another host.
