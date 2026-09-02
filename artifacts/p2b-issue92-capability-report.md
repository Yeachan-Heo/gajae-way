# P2b issue #92 GJC capability probe

**Overall verdict: GO for the Stage 0 runtime contract against installed `gjc/0.15.6`, with one documented caveat: manual compaction is observed through its successful control receipt, not a distinct named `tail` event.**

P2b corrects two conclusions in `p2-issue92-capability-report.md`: client-side policy does gate `turn.steer`, and `tail --cursor` has a working signed-cursor plus retention-resync flow. Stages 0c/0d found the installed profile-broker path unavailable through the Router even with a readiness-verified `sdk serve` relay. Stage 0e then returned to the original isolated scratch broker, retained the parent process environment for runtime/provider wiring, declared the parent-default provider shape without copying credentials, and completed an SDK-hosted turn, mid-turn steer echo, and manual compaction control.

## Scope and safety

- Probe date: 2026-09-01
- Executable: `/Users/bellman/.local/bin/gjc`
- Installed version: `gjc/0.15.6`
- Baseline harness: `bun scripts/spike-gjc-contract.ts`
- Opt-in Stage 0c profile diagnostic: `bun scripts/spike-gjc-contract.ts --stage0c`
- Opt-in Stage 0d resident-relay diagnostic: `bun scripts/spike-gjc-contract.ts --stage0d`
- Opt-in Stage 0e inherited-provider scratch diagnostic: `bun scripts/spike-gjc-contract.ts --stage0e`
- Product source changes: none

The harness creates one OS temporary root, places SDK broker state at `<tmp>/sdk-agent`, session workspace at `<tmp>/sdk-repo`, and all normal transcripts under explicit `--session-dir` directories below `<tmp>`. It owns the broker and `sdk serve` processes, closes its one lifecycle session, verifies serve-socket unlink where applicable, waits for broker exit, and deletes `<tmp>` last.

The separate credentialed direct-mode `gjc -p --mode json` probe uses `--no-tools --no-mcp --no-rules --no-lsp --session-dir <tmp>/credentialed-sessions`. It inherits the host credential environment and reads the installed profile in place; it is **not** SDK-broker evidence.

The signed cursor is read only from the scratch session's local endpoint, retained in memory only, passed once as a redacted CLI argument, and never emitted by the harness or this report.

Stages 0c and 0d are intentionally separate from the fully scratch-rooted baseline: their workspaces are temporary, while their fresh lifecycle sessions use the installed credentialed profile broker. Neither probe copies/proxies a credential or configuration file, calls `model.set`, inspects/reconciles an existing session, or terminates the pre-existing profile broker.

Stage 0e is fully scratch-rooted and starts its own broker. It inherits parent provider environment variables without rendering or persisting their values, and writes only a minimal temporary provider/model definition that refers to `OPENAI_API_KEY` by name; no credential value, credential file, session state, or profile broker is copied. That scratch definition is removed with the temporary root.

The installed `sdk serve` implementation is a **relay**, not a session-host launcher: it asks the broker for a selected live session and endpoint, then starts a stdio/Unix-socket transport to that endpoint. Stage 0d nevertheless tested the architectural relay proposal directly; a relay socket that is ready for its own protocol did not cure the separate profile-broker Router rejection.

## Verdict matrix

| Capability | Verdict | P2b criterion and evidence |
| --- | --- | --- |
| 1. `sdk serve --socket` lifecycle | **PASS** | Baseline and Stage 0d relays bound mode `0600` Unix sockets, accepted invalid-auth readiness connections, exited `0` on SIGTERM, and unlinked only their sockets. |
| 2. Lifecycle/create/send envelope | **PASS** | Scratch `session.create` was replay-idempotent; `send --text --op-ref --wait --timeout-ms` emitted a versioned accepted receipt and terminal status. |
| 3. Client-side operator gate for `turn.steer` | **PASS** | The planned client policy, not the raw GJC CLI, is the gate. `turn.steer` is `operator_gated` and requires `operatorApproval: true` before dispatch. |
| 4. Saved tail cursor and retention resync | **PASS** | A scratch opaque signed checkpoint token resumes `tail`; strict mode correctly fails closed with `retention_gap`; non-strict mode continues from the supplied resync coordinate. |
| 5. Duplicate operation reference | **PASS** | Changed text under retained `p2-send-ref` returned `client_ref_conflict`. |
| 6. Live model rebind | **PASS** | Scratch live `model.set` returned `{ "changed": true }`. |
| 7. Successful SDK-hosted vocabulary and steer echo | **PASS (Stage 0e)** | With the parent runtime/provider environment retained and all GJC state paths overridden to scratch, the isolated broker created a session, selected `layofflabs-anthropic/claude-opus-5:medium`, completed `send --wait` with assistant marker text in `tail`, accepted `turn.steer` while the turn was `in_flight`, and emitted the requested assistant echo before terminal success. |
| 8. Manual compaction observation | **PASS WITH CAVEAT (Stage 0e)** | One terminal filler made `compaction.run` return `{ "started": true }`; the following successful `tail` had no named compaction kind or embedded event type. AC7 is implementable by logging that authenticated control receipt plus treating unknown tail kinds as bounded diagnostics, never invented lifecycle truth. |

## 1. Socket, lifecycle, receipt, conflict, and rebind baseline

The final harness run created exactly one scratch SDK session and replayed its lifecycle key five additional times; every replay returned the same session ID. The visible creation shape was:

```json
{"ok":true,"operation":"session.create","result":{"sessionId":"<scratch-id>","cwd":"<tmp>/sdk-repo","endpointGeneration":1}}
```

The owned relay command was:

```text
gjc sdk serve --socket <tmp>/s.sock --session <scratch-id>
```

Observed socket receipt and cleanup:

```json
{"type":"transport_error","code":"auth_failed"}
```

- `<tmp>/s.sock` existed only while serve ran.
- `lstat` reported a Unix socket with mode `0600` (`384`).
- SIGTERM produced exit `0`, empty stdout/stderr, and removed the path.

The scratch send was accepted and then terminally reconciled as a failed model submission, which is sufficient to establish the control envelope rather than provider success:

```json
{"ok":true,"result":{"version":2,"operationRef":"p2-send-ref","status":"failed","receipt":{"commandId":"<command-id>","turnId":"<turn-id>","accepted":true,"clientRef":"p2-send-ref"},"statusDetail":{"status":"failed","kind":"prompt","error":{"code":"agent_error","message":"Prompt submission failed."}}}}
```

Reusing that retained ref with changed text failed as required:

```json
{"ok":false,"error":{"code":"client_ref_conflict","message":"A submission with this clientRef is already retained; never reuse a clientRef for retry."}}
```

The live rebind control was accepted:

```text
gjc sdk session raw control <scratch-id> --agent-dir <tmp>/sdk-agent \
  --op model.set --json-input '{"id":"openai/gpt-5.4"}'
```

```json
{"type":"control_response","ok":true,"result":{"changed":true}}
```

## 2. Corrected Capability 3: client-side approval gates steering

The raw installed CLI accepted a scratch steering request without `--confirm`:

```text
gjc sdk session raw control <scratch-id> --agent-dir <tmp>/sdk-agent \
  --op turn.steer --json-input '{"text":"p2 steer","clientRef":"p2-steer-ref"}'
```

```json
{"type":"control_response","ok":true,"result":{"clientRef":"p2-steer-ref","status":"accepted"}}
```

That is compatible with the approved design. The intended gate is in this repository's controller policy, before raw GJC dispatch:

- `packages/subsession/src/policy.ts` lists `turn.steer` in `OPERATOR_GATED_OPERATIONS`.
- `assertControlAllowed()` rejects an operator-gated operation unless `context.operatorApproval === true`.
- `packages/subsession/test/policy-skills.test.ts` asserts that every such operation rejects without approval and passes with `{ operatorApproval: true }`.

Therefore the correct contract is: **the client must obtain/operator-record approval, call `assertControlAllowed("turn.steer", { operatorApproval: true })`, then issue the raw steering control.** Requiring GJC's unrelated CLI `--confirm` flag on `turn.steer` would be an incorrect duplicate criterion.

For comparison, the runtime's terminal-abort path did enforce CLI authority:

```json
{"ok":false,"error":{"code":"invalid_input","message":"operator terminal abort requires --confirm."}}
```

Its confirmed retry returned `terminal_no_effect` because no turn was active.

## 3. Working tail cursor and retention-gap resync

### Valid cursor representation

CLI `session.checkpoint` redacts the token and exposes only the structured checkpoint:

```json
{"type":"query_response","ok":true,"result":{"checkpoint":{"revision":0,"generation":0,"seq":0},"revisionId":"1"}}
```

The harness opened only its own scratch endpoint, issued `session.checkpoint`, and received an in-memory **475-byte opaque signed checkpoint token** paired with that same checkpoint. The actual token was never rendered.

The raw checkpoint object is **not** valid for `--cursor`:

```text
gjc sdk session tail <scratch-id> --agent-dir <tmp>/sdk-agent \
  --cursor '{"revision":0,"generation":0,"seq":0}' \
  --until-idle --strict --all-events --timeout-ms 3000
```

```json
{"ok":false,"error":{"code":"invalid_cursor","message":"invalid_cursor"}}
```

The valid encoding is the opaque signed checkpoint token, not JSON serialization of `{revision,generation,seq}`.

### Strict behavior and automatic resync

Using the in-memory signed token with strict mode reached the expected safety branch:

```text
gjc sdk session tail <scratch-id> --agent-dir <tmp>/sdk-agent \
  --cursor <opaque-signed-token> --until-idle --strict --all-events --timeout-ms 3000
```

```json
{"ok":false,"error":{"code":"retention_gap","message":"The event ring dropped entries before the checkpoint (strict mode).","details":{"code":"retention_gap","resync":{"revision":0,"generation":1,"seq":0}}}}
```

This is a valid strict-cursor outcome, not an invalid cursor. Retrying with the **same signed token** without `--strict` succeeded, reported the same gap, and returned `terminal:true` plus post-resync events:

```json
{"ok":true,"result":{"checkpoint":{"revision":0,"generation":0,"seq":0},"gap":{"code":"retention_gap","resync":{"revision":0,"generation":1,"seq":0}},"terminal":true,"items":[{"kind":"session_ready","generation":1,"seq":1},{"kind":"agent_start","generation":1,"seq":4},{"kind":"activity","generation":1,"seq":5},{"kind":"agent_end","generation":1,"seq":10},{"kind":"activity","generation":1,"seq":11}]}}
```

The harness then sent the returned coordinate directly to the scratch endpoint's event replay:

```json
{"type":"event_replay_result","ok":true,"generation":1,"lastSeq":11,"events":[{"generation":1,"seq":1},{"kind":"identity_header","generation":1,"seq":2},{"kind":"agent_start","generation":1,"seq":4},{"kind":"activity","generation":1,"seq":5},{"kind":"agent_end","generation":1,"seq":10},{"kind":"activity","generation":1,"seq":11}]}
```

No replay gap was returned. The usable TailRunner rule is therefore:

1. persist the opaque signed checkpoint token, never stringify the public checkpoint record as a cursor;
2. on strict `retention_gap`, place the lane on hold and record the supplied `{revision,generation,seq}` resync point;
3. for a user-approved non-strict recovery, let tail continue from that resync point and reconcile its ordered event stream; do not silently treat the strict failure as a successful complete replay.

## 4. Genuine successful turn and event-vocabulary boundary

The credentialed, scratch-contained direct-mode invocation was:

```text
gjc -p --mode json --no-tools --no-mcp --no-rules --no-lsp \
  --session-dir <tmp>/credentialed-sessions \
  'Reply with exactly: P2B_SUCCESS'
```

It exited `0`, emitted `P2B_SUCCESS` as the final assistant text, and had this exact type sequence:

```text
session
agent_start
turn_start
message_start
message_end
message_start
message_end
message_start
message_update
message_update
message_update
message_update
message_end
turn_end
agent_end
```

The scratch SDK session, which intentionally had no copied credentials, supplied these actual tail terms instead:

| Runtime observation | Planned normalized event |
| --- | --- |
| `kind:"agent_start"` | `turn.started` |
| `kind:"activity", payload.state:"busy"` | `turn.running` |
| `kind:"agent_failed"` | diagnostic `turn.failed`; retain until terminal reconciliation |
| `kind:"agent_end"` | terminal lifecycle boundary |
| `kind:"activity", payload.state:"idle"` | `turn.idle` / terminal-settlement confirmation |
| `kind:"transcript"`, assistant `content[].text` | `assistant.text` when non-empty |
| retained user transcript body `p2 steer` | steer transcript echo, not a distinct steering event |

`turn_start` and `turn_end` were observed in the successful direct-mode JSON stream, but not in the SDK tail. They must be optional diagnostics, not required TailRunner lifecycle terms. Similarly, no distinct `turn.steer` event was observed; only a transcript echo was observed, and it was not demonstrated mid-turn.

Before Stage 0c, an initial bounded profile lifecycle creation stopped before a session ID with `terminal_uncertain`; no reconciliation or existing-session mutation was attempted. Stage 0c's one fresh-key retry is recorded in §6. It changed the failure surface rather than proving success: creation returned an ID, but the newly created session was immediately unavailable through the Router.

## 5. Compaction attempt

The first manual compaction control correctly refused the short scratch transcript:

```json
{"ok":false,"error":{"code":"invalid_request","message":"Nothing to compact (session too small)"}}
```

The harness then submitted four terminally reconciled 26,400-byte scratch prompts (105,600 bytes total) and retried `compaction.run`. The control passed the short-history guard but failed because that scratch session's selected model did not support thinking:

```json
{"ok":false,"error":{"code":"invalid_request","message":"Model openai/gpt-5.4 does not support thinking"}}
```

That is not a compaction event contract and is not reused as Stage 0c or Stage 0d evidence. Stage 0c deliberately left the profile-default model untouched; Stage 0d stopped at the first unavailable broker send. No compaction event name is therefore claimed.


## 6. Stage 0c credentialed SDK-hosted turn and compaction retry

The profile-backed retry used one fresh `session.create` idempotency key, a fresh `<tmp>/stage0c-profile-sdk-repo` workspace, and no `model.set`. It was not a scratch-agent credential workaround: `gjc/0.15.6` binds a lifecycle host to its broker's agent directory, so using stored credentials requires the normal credentialed profile broker. No credential database/configuration was copied or proxied.

Creation succeeded:

```json
{"ok":true,"operation":"session.create","result":{"sessionId":"<stage0c-id>","cwd":"<tmp>/stage0c-profile-sdk-repo","endpointGeneration":1}}
```

The first and only `session send --wait` failed before model execution:

```json
{"ok":false,"error":{"code":"session_unavailable","message":"SDK session <stage0c-id> is unavailable through the session Router."}}
```

A following `tail --until-idle --all-events` also exited nonzero with no items. No assistant text, SDK lifecycle vocabulary, or steering echo was observed. The profile retry therefore did **not** repeat `terminal_uncertain`; it surfaced a distinct reachability failure after successful lifecycle creation. For comparison, the earlier attempt's exact pre-ID error was `{"ok":false,"error":{"code":"terminal_uncertain","message":"Lifecycle startup cleanup could not be proven; retained artifacts require reconciliation."}}`.

The fresh session was closed once and returned exit `0` with the runtime note `Endpoint close was unreachable; sent SIGTERM to the durably identified session process.` No retry, reconcile, `session.list`, or existing-session inspection was performed. Because the only credentialed SDK session was unavailable, the planned four bounded filler sends and `compaction.run` were correctly **not** sent to it. The successful direct-mode one-shot is not an SDK control target, so it cannot supply this missing compaction evidence.

Stage 0d supplies the final relay-backed retry in §7; it neither assumes that `sdk serve` launches a host nor repeats the earlier profile session.

AC7 remains defensively implementable, but not proven: `packages/subsession/src/transcript.ts` already models tail entries with optional `kind` and `reason`; a TailRunner normalizer can retain a bounded, sanitized `unknown_runtime_event` diagnostic without driving lifecycle transitions or treating it as compaction. This does not substitute for an observed compaction control/event path.

## 7. Stage 0d resident relay retry

Stage 0d used a fresh idempotency key, a fresh `<tmp>/stage0d-profile-sdk-repo` workspace, and the profile-default model. It then owned this foreground relay for the newly created session:

```text
gjc sdk serve --socket <tmp>/stage0d-host.sock --session <stage0d-id>
```

The lifecycle create completed in 4.561 seconds:

```json
{"ok":true,"operation":"session.create","result":{"sessionId":"<stage0d-id>","cwd":"<tmp>/stage0d-profile-sdk-repo","endpointGeneration":1}}
```

The relay bound a mode-`0600` socket and returned the expected readiness frame for an invalid token:

```json
{"type":"transport_error","code":"auth_failed"}
```

That is not proof that the relay launched or owns the session host. Installed source `packages/coding-agent/src/sdk/transport/serve-cli.ts` instead selects a broker-listed live session, obtains `session.get_endpoint`, and starts a transport relay. With that relay ready, the first and only broker-bound send still failed after 636 ms:

```json
{"ok":false,"error":{"code":"session_unavailable","message":"SDK session <stage0d-id> is unavailable through the session Router."}}
```

The installed `gjc sdk session` CLI exposes no `--socket` flag or socket-context send verb, so there was no supported CLI retry through the relay socket. Per the stop rule, no tail, longer prompt, steer control, filler, or `compaction.run` request followed this failed send.

The fresh session close was attempted once before stopping the owned relay, but the runtime returned:

```json
{"ok":false,"error":{"code":"terminal_uncertain","message":"Session did not close after SIGTERM and its durable process identity could not be verified for SIGKILL.","details":{"code":"terminal_uncertain","message":"Session did not close after SIGTERM and its durable process identity could not be verified for SIGKILL."}}}
```

No reconcile, inspection, retry, or pre-existing-session action followed. The owned relay exited `0` with empty stdout/stderr on SIGTERM, unlinked `<tmp>/stage0d-host.sock`, and the harness removed its temporary root. The `terminal_uncertain` close is retained as unresolved runtime evidence rather than presented as cleanup success.

## 8. Stage 0e inherited-provider scratch broker

Stage 0e returned to the original fully isolated SDK topology: a fresh `<tmp>/sdk-agent`, fresh `<tmp>/sdk-repo`, and an owned `gjc sdk broker-internal --agent-dir <tmp>/sdk-agent`. It retained the parent process environment for runtime/provider wiring while forcing GJC state into the temporary agent directory, without printing or persisting environment values. Read-only preflight established that the parent default was `layofflabs-anthropic/claude-opus-5:medium`, not the initially tried generic `openai/gpt-4.1-mini`.

A stricter retry that retained only `OPENAI_API_KEY` and `OPENAI_BASE_URL` did **not** start a scratch lifecycle host: five idempotent `session.create` attempts returned `terminal_uncertain`. Therefore Stage 0e's supported probe contract is **parent runtime/provider environment retained, with `GJC_AGENT_DIR` and `GJC_CODING_AGENT_DIR` forced to scratch**—not a key-only child environment. It still rendered/persisted no environment value and removed that failed retry's temporary root.

The initial generic OpenAI-model attempt proved that environment inheritance alone is not enough when the scratch registry lacks the provider shape: lifecycle create and `model.set` succeeded, but the first terminal result was the sanitized runtime envelope below.

```json
{"ok":true,"result":{"status":"failed","statusDetail":{"error":{"code":"agent_error","message":"Prompt submission failed."}}}}
```

The final Stage 0e run therefore authored only the one required non-secret scratch definition: provider `layofflabs-anthropic`, API `anthropic-messages`, model `claude-opus-5`, and an `apiKeyEnv: OPENAI_API_KEY` reference. It did not copy a key, a credential file, profile session state, or a broker. The final new session was created in 1.665 seconds and accepted the real selector:

```json
{"type":"control_response","ok":true,"result":{"provider":"layofflabs-anthropic","modelId":"claude-opus-5","thinkingLevel":"medium"}}
```

### Successful hosted turn and tail vocabulary

The first SDK `send --wait` completed in 4.325 seconds with `status:"terminal_ok"`; its `tail --until-idle --all-events` also exited `0` and contained assistant text exactly `STAGE0E_SDK_SUCCESS`. The ordered top-level tail vocabulary was:

```text
transcript → transcript → session_ready → event → identity_header → agent_start → activity → agent_end → activity → query_response → query_response
```

This is SDK-hosted evidence, unlike the earlier direct-mode proof. It establishes non-empty transcript text and the usable lifecycle terms `agent_start`, `activity`, and `agent_end`; the opaque top-level `event` has no non-generic nested event term and remains diagnostic-only.

### Mid-turn steering

A deliberately longer no-tools prompt was accepted, then `session status` observed `in_flight` before control dispatch. `turn.steer` returned `ok:true`, `status:"accepted"`; the original prompt then reached `terminal_ok`, and the resulting assistant transcript ended with the requested `STAGE0E_STEER_ECHO` marker. This is the first observed mid-turn SDK steering echo. It does not change the client policy requirement from Capability 3: application code must still obtain and record `operatorApproval: true` before issuing that raw control.

### Manual compaction boundary

One terminally successful 31,291-byte filler prompt was sufficient for `compaction.run` to succeed in 10.757 seconds:

```json
{"type":"control_response","ok":true,"result":{"started":true}}
```

The post-control `tail` exited `0`, was terminal, and included the filler assistant acknowledgement, but its top-level kinds were still only `transcript`, `session_ready`, `event`, `identity_header`, `agent_start`, `activity`, `agent_end`, and `query_response`. Recursive inspection found no non-generic nested event term (`eventTypeSequence: []`); `compactionEventKinds` was `[]`. The installed SDK runtime source likewise registers agent/turn/tool lifecycle listeners but no `session_compact` or auto-compaction listener for its event ring. Thus the control completion is real, but **no named compaction tail event exists to normalize**. AC7 must log the authenticated `{ "started": true }` control receipt and preserve unknown tail kinds as bounded diagnostics; it must not invent a lifecycle event from that receipt.

Stage 0e closed its one fresh session successfully (exit `0`, runtime SIGTERM fallback note), its owned broker exited `0` with empty stderr, and its temporary root—including the temporary provider definition—was removed.

## 9. Cleanup evidence


The baseline harness closed its scratch session with `session.close` and exit `0`, observed its owned broker exit `0`, and removed its temporary root last. Stage 0c closed its fresh profile-backed session and did not terminate the pre-existing profile broker. Stage 0d attempted the same fresh-session close but received `terminal_uncertain`, so it does **not** claim that close completed; it did stop its owned relay with exit `0`, confirm socket unlink, and remove its scratch root. Stage 0e closed its fresh scratch session with exit `0`, stopped its own broker with exit `0`, and then removed the full temporary root containing its non-secret provider definition. A post-probe check also found an older orphaned `gjc-stale-marker-*` scratch broker, confirmed it was neither the profile nor gajaeway-play broker, found it absent on the immediate recheck, and removed that plainly probe-owned temporary root. Existing real/profile and gajaeway-play brokers were left untouched.

## Final decision

**GO for the Stage 0 runtime contract on `gjc/0.15.6`, with a documented compaction-observation caveat.** Stage 0e overturns the broader provider-environment blocker: with an isolated broker, inherited `OPENAI_API_KEY`, and only the necessary non-secret provider shape, the installed runtime cleanly created a session, re-bound to the parent-default model, completed an SDK-hosted turn, exposed non-empty assistant text and lifecycle vocabulary through `tail`, accepted a real mid-turn steer, and completed `compaction.run`. The profile-broker Router failure in Stages 0c/0d remains a separate environment-path defect, not evidence that successful hosted SDK turns are impossible. The successful post-compaction tail emitted no named compaction event or embedded type, so AC7 must record the successful `compaction.run` control receipt and retain unknown event kinds only as bounded diagnostics. That caveat is sufficient for the planned defensive normalization; no event name may be invented or allowed to drive lifecycle truth.