Execute the 2026-09-03 handoff (`git show e200425:artifacts/handoff-2026-09-03.md`) on branch `fix/reply-attribution` (cut from origin/main e200425). Order is fixed by the handoff: §1 first, then canonical step 1 tests, then step 2.

Constraints (from the handoff §7):
- Two known load-flaky tests (`delivery-crash.e2e` "inflight platform delivery is duplicate-labeled…", `broker-supervisor` "red-team G3-F3") may fail only under the full run; everything else must be green.
- Never `git stash pop` in this repo.
- `bun test` redirected prints no per-test lines; use `--reporter=junit --reporter-outfile=…` when needed.
- Fixture timestamps must derive from the clock (10-minute stale floor).
- The interim-speech `pre-tool` gate is deliberate; leave it alone.
- No watchdog/sweeper for wedged batches. Remove the batch model instead.
- `dispatched_at` (or an equivalent pre-send stamp) must survive any schema change if §1 relies on it. `accepted_at` is not a safe floor.
- Do not touch remote hosts or deploy; code + tests + commits on the branch only.

@goal: Fix off-by-one reply attribution (handoff §1)
In `packages/gateway/src/orchestrator/persona-session.ts`: (1) scope tail frames to the turn — drop any un-attributed transcript frame whose `ts` precedes the batch's `dispatched_at` minus ~2s clock skew so a cursorless resync replay can never become `bound.lastAssistantText`; frames with a matching `opRef` remain accepted. (2) Make the transcript authoritative at terminal — prefer `port.fetchAssistantSince({ notBeforeMs })` with `notBeforeMs = runtime startedAt ?? dispatched_at` over `bound.lastAssistantText`. Add a regression test that reproduces the mechanism (turn ends, tail re-attaches cursorless, resync replays a pre-turn transcript row without opRef, then terminal) and proves the current trigger's delivery carries the current turn's text, not the previous one. Existing targeted suites (tail-progress, terminal-idempotency.e2e, tail-liveness, persona-session tests) stay green. Commit on the branch.

@goal: Land canonical pipeline step 1 with rewritten tests (handoff §3 step 1)
Bring `feat/canonical-steer-pipeline` (b917425: remove the `nonSteerable` gag from ingestion; a failed steer escalates to a session rebind) onto `fix/reply-attribution` on top of the §1 fix. Rewrite — not patch — the three tests that encode the old contract: "broker death during a steer leaves the row unconsumed …", "slow tail keeps a status-terminal turn non-steerable …", "coverage audit leaves a failed steer pending …" so they assert the new contract (message always reaches the session; steer failure → new session + resume → fresh session with 24h channel context). Gateway test suite green except the two known load-flaky tests. Commit on the branch.

@goal: Delete the batch layer (handoff §3 step 2)
Remove the inbound batch model from the gateway: the `inboundBatch*` DB methods in `packages/gateway/src/store/db.ts`, the settle window, `expireStale`, holds, and all batch references in `persona-session.ts` / `server.ts`, replacing them with the canonical flow: discord stream → needs delivery? → steer into the correct session (send when idle) → reply from that session; message edits stream as an update of a `[MESSAGE POINTER]`. Add schema 19 dropping `batch_*`, `accepted_at`, `attributed_op_ref`, `terminal_delivery_id`; keep `dispatched_at` (or an equivalent pre-send stamp) because §1's floor depends on it. Messages queued behind a wedge must never be silently deleted (no 10-minute expiry of never-seen rows). Update migration drill tests, docs/architecture.md and docs/runbooks as affected. Gateway + adapter test suites green except the two known load-flaky tests. Commit on the branch.
