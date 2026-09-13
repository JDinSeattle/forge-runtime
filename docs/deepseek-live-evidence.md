# DeepSeek real-provider evaluation — 2026-09-13

The authorized four-task batch completed collection using `deepseek-v4-flash`
through the native Responses adapter, with a $2 total ceiling and $0.50 per task.
The [raw report](../benchmarks/results/deepseek-live-20260913/report.json)
records **0/4 verified repairs**, 10 real provider dispatches, and four failed runs.
These are execution results from the sealed `ca6327c` version, not successful
model-quality evidence and not results from the subsequent quota fix.

| Task | Run duration (DB seconds) | Provider dispatches | Conservative exposure (USD) |
| --- | ---: | ---: | ---: |
| UTF-8 chunks | 63.022889 | 2 | 0.039322 |
| Latest records | 124.417252 | 4 | 0.078644 |
| Go midpoint | 69.143714 | 2 | 0.039322 |
| Go rune RLE | 69.321311 | 2 | 0.039322 |

All four defective baselines failed their target checks as expected. None reached
verified repair. Total conservative exposure is **$0.196610**. Actual billed cost
is unknown: all ten reservations remain unresolved. The known-cost subtotal of
zero is not a claim of free requests. The frozen price registration uses a
conservative ceiling with `exact_pricing=false`; successful response token counts
therefore do not establish exact invoice settlement. Reservations are retained.

Live execution exposed a concurrency bug: a durably completed model response
with unknown billing kept its request slot until the original deadline. With
one allowed request, the next attempt waited behind that slot and could reach
actual dispatch with less than a second remaining. The subsequent code fix
releases concurrency after durable response completion while preserving every
unknown token/cost reservation. Interrupted or unconfirmed requests retain the
existing conservative handling. This batch was not modified or resampled to
hide the original failures; the fix requires separate live validation. The affected application and quota
packages pass [real PostgreSQL tests](../benchmarks/results/deepseek-live-20260913/quota-fix-tests.log),
including unknown-reservation preservation, duplicate completion and later settlement.

The first launch stopped before project creation because the tenant member was
a developer. It had zero projects/runs/model attempts/fees and all services exited.
After explicit operator authorization, only this evaluation tenant membership
changed to admin. The original token, entitlement, quota window and allocations
were reused; the failed launch was retained locally.

The final [deployment record](../benchmarks/results/deepseek-live-20260913/deployment-result.json)
shows ordered API, worker and runner SIGTERM exits, each code 0. New API TCP
connections were refused after API exit. Final active runs, runner reservations,
allocations and request slots were zero. Collector `complete=true` means all four
results were collected; deployment `complete=false` correctly reflects unresolved
billing. This supplies actual ordered shutdown alongside E45/E47/E50 busy-component
proof, not a claim that billing or the four repairs succeeded.

Full run/event/artifact records remain in the local ignored evaluation directory.
The committed reports contain no API key. This finite fixture does not establish
pretraining-data exclusion, general benchmark performance, or vendor invoice accuracy.
