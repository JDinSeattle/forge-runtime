# Provider boundary

`Provider.Stream` is a single request attempt. Both SDKs have retries disabled,
including a per-request override, so the application can persist retry budgets
and `not_before` times. HTTP status and `Retry-After` are normalized into `Error`;
Anthropic SSE errors arriving after HTTP 200 use the native error type.

The caller supplies exact model IDs and capabilities in a registry. No changing
model alias, token window, quota, price, or account access is inferred here.
The initial adapters support visible text and custom function tools. Advanced
capabilities are exposed as routing metadata, not implicitly enabled on requests.

## Commit boundary

Events carry run, step, attempt, and sequence identifiers. Text and argument
deltas are provisional, and an interrupted attempt emits `model.attempt_failed`.
Only a successful returned `ModelTurn` contains tool calls; persist the turn
before passing any call to the policy/execution layers. Tool argument completion
alone does not cross this boundary. Every call must pass all of:

- Declared tool identity; distinct call ID; bounded call count.
- Byte, depth, duplicate-key, and root-object checks.
- Offline JSON Schema validation; remote schema loading is disabled.
- Provider-specific terminal event and consistent final tool records.

Visible text, native events, continuation state, and the raw HTTP body have
separate size checks. The tool policy remains responsible for repository path,
command, authorization, and other application constraints.

## Usage and continuation

`TokenCount.Known` distinguishes observed zero from missing usage. `Usage.Final`
is only true after the entire response validates; retain conservative reservations
for interrupted attempts even if some counters were observed. This package does
not calculate costs or release shared quota reservations.

`NativeState.Raw` is a protected artifact, not user event data. When using it,
`ModelRequest.Messages` contains only new messages. OpenAI uses `store: false`,
requests encrypted reasoning, and replays original input plus raw output items
using the SDK's documented `param.Override`. Anthropic retains native content
blocks, thinking signatures, system instructions, and raw events. Unsupported
native block/delta types fail explicitly. Full canonical history without native
state is the route for cross-provider migration at a closed tool batch boundary.

SDK contracts are exercised against local `httptest` servers. They cover split
and interleaved tool JSON, continuation round trips, unknown native fields,
interrupted responses, size rejection, and retry/error behavior. They do not
assert live model availability or make paid model calls.

References used for the protocol implementation:

- https://developers.openai.com/api/docs/guides/function-calling
- https://platform.claude.com/docs/en/build-with-claude/streaming
- Locked native SDK source in `go.mod` (`openai-go/v3`, `anthropic-sdk-go`).
