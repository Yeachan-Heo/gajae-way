# P2 issue #92 GJC capability probe

**Overall verdict: NO-GO.** The installed runtime supports the socket relay, lifecycle/send envelopes, duplicate `op-ref` conflict, and live `model.set`. It does **not** demonstrate operator-gated `turn.steer`, and its required `tail --cursor --until-idle --strict` path is blocked by a redacted checkpoint token plus an immediate `retention_gap`; the requested turn markers, non-empty assistant text, and compaction event name were not observed.

## Probe environment

- Probe date: 2026-09-01
- Executable: `/Users/bellman/.local/bin/gjc`
- Installed version: `gjc/0.15.6`
- Repeatable harness: `bun scripts/spike-gjc-contract.ts`
- Scratch root for this run: `/var/folders/yl/ndsjfsl95t781x9qyjrqyqc80000gn/T/gajaeway-gjc-spike-OKljBa` (removed after the run)
- SDK broker state: `<tmp>/sdk-agent`; SDK session workspace: `<tmp>/sdk-repo`

Every SDK command used `GJC_AGENT_DIR=<tmp>/sdk-agent` and `GJC_CODING_AGENT_DIR=<tmp>/sdk-agent`. The harness started `gjc sdk broker-internal --agent-dir <tmp>/sdk-agent` as a foreground-owned child, explicitly closed the scratch lifecycle session, terminated the broker, and then removed `<tmp>`. The final cleanup receipts were exit 0 for `session.close` and the broker.

The session ID below was `7fe02605-931b-472e-968c-a376b39cc488`. Paths in command listings are normalized as `<tmp>`; JSON blocks retain the emitted IDs and values.

## Verdict matrix

| Capability | Verdict | Explicit criterion |
| --- | --- | --- |
| 1. `sdk serve --socket` lifecycle | **PASS** | Requested path is a `0600` Unix socket, accepts a connection, exits 0 on SIGTERM, and removes only its socket path. |
| 2. Lifecycle create + `send --text --op-ref --wait --timeout-ms` envelope | **PASS** | Creation is idempotent and `send` accepts the exact flags and emits the versioned receipt/status envelope. |
| 3. `turn.steer` acceptance with an operator gate | **NO-GO** | `turn.steer` accepts without an operator confirmation. The observed operator gate applies to terminal `turn.abort`, not steering. |
| 4. `tail --cursor --until-idle --strict` vocabulary | **NO-GO** | No usable cursor was emitted, strict tail failed with `retention_gap`, and required `turn_start`/`turn_end`, non-empty assistant text, and a compaction event were not observed. |
| 5. Duplicate op-ref conflict | **PASS** | Reusing `p2-send-ref` with different text exits nonzero with `client_ref_conflict`. |
| 6. Live model rebind | **PASS** | `model.set` is accepted by the live session and returns `{"changed":true}`. |

## 1. `gjc sdk serve --socket` lifecycle

Commands run:

```text
$ gjc sdk session raw global --agent-dir <tmp>/sdk-agent --op session.create \
    --idempotency-key spike-external-key \
    --json-input '{"cwd":"<tmp>/sdk-repo"}'

$ gjc sdk serve --socket <tmp>/s.sock --session 7fe02605-931b-472e-968c-a376b39cc488
# SIGTERM sent by the harness after readiness
```

Observed lifecycle:

- Before start: no `*.sock` entry in `<tmp>`.
- During service: exactly `s.sock` existed at the requested path; `lstat` identified a Unix socket with mode `0600`.
- A well-behaved invalid-auth client connected and received this readiness/authentication receipt without exposing the endpoint credential:

```json
{"type":"transport_error","code":"auth_failed"}
```

- SIGTERM produced `exitCode: 0`, empty stdout/stderr, and `s.sock` was absent afterwards.

**PASS criterion met:** path discipline, listener readiness, clean process shutdown, and owned socket unlink were all directly observed.

## 2. SDK session creation and `send` envelope

Commands run:

```text
$ gjc sdk session raw global --agent-dir <tmp>/sdk-agent --op session.create \
    --idempotency-key spike-external-key \
    --json-input '{"cwd":"<tmp>/sdk-repo"}'

$ gjc sdk session send 7fe02605-931b-472e-968c-a376b39cc488 \
    --agent-dir <tmp>/sdk-agent \
    --text 'say exactly: p2-sdk-send' \
    --op-ref p2-send-ref --wait --timeout-ms 15000
```

Creation returned the same ID on the initial call and its identical replay:

```json
{"ok":true,"operation":"session.create","result":{"sessionId":"7fe02605-931b-472e-968c-a376b39cc488","cwd":"/var/folders/yl/ndsjfsl95t781x9qyjrqyqc80000gn/T/gajaeway-gjc-spike-OKljBa/sdk-repo","endpointGeneration":1}}
```

The send command exited 0 and emitted this envelope:

```json
{"ok":true,"result":{"version":2,"operationRef":"p2-send-ref","status":"failed","receipt":{"commandId":"48586c7d-2d0d-4760-8958-6f746e5f45fe","turnId":"43697d62-71b0-4abb-b3ed-0278fbc345c7","accepted":true,"clientRef":"p2-send-ref"},"statusDetail":{"status":"failed","kind":"prompt","commandId":"48586c7d-2d0d-4760-8958-6f746e5f45fe","turnId":"43697d62-71b0-4abb-b3ed-0278fbc345c7","clientRef":"p2-send-ref","acceptedAt":1788272816596,"startedAt":1788272816602,"terminalAt":1788272816629,"error":{"code":"agent_error","message":"Prompt submission failed."}}}}
```

The model execution failed in this isolated environment, but the acceptance receipt, operation reference, command/turn correlations, terminal status, and terminal error all followed the observable CLI envelope.

**PASS criterion met:** the exact invocation was accepted and the response shape is captured. A successful model answer is not required to establish this transport/receipt contract.

## 3. `turn.steer` invocation and operator approval

The exact accepted steering invocation was:

```text
$ gjc sdk session raw control 7fe02605-931b-472e-968c-a376b39cc488 \
    --agent-dir <tmp>/sdk-agent \
    --op turn.steer \
    --json-input '{"text":"p2 steer","clientRef":"p2-steer-ref"}'
```

Its acceptance receipt was:

```json
{"type":"control_response","id":"1f5bd4ba-7085-48b3-bfd6-c4e4c03eb3cf","ok":true,"result":{"sessionId":"7fe02605-931b-472e-968c-a376b39cc488","commandId":"9fd76576-91bf-48b4-9edd-1858bf216d09","turnId":"d6b9e5fb-a141-4f54-9760-e56de2152bce","clientRef":"p2-steer-ref","status":"accepted","acceptedAt":1788272818048}}
```

`turn.steer` did **not** require `--confirm`, so it is not operator-gated by this CLI. The observed operator-gated control was terminal `turn.abort`:

```text
$ gjc sdk session raw control 7fe02605-931b-472e-968c-a376b39cc488 \
    --agent-dir <tmp>/sdk-agent --op turn.abort \
    --idempotency-key p2-operator-no-confirm \
    --json-input '{"mode":"terminal","scope":"turn","operator":true}'
```

```json
{"ok":false,"error":{"code":"invalid_input","message":"operator terminal abort requires --confirm."}}
```

Adding `--confirm` was accepted (the scratch session had no active turn):

```text
$ gjc sdk session raw control 7fe02605-931b-472e-968c-a376b39cc488 \
    --agent-dir <tmp>/sdk-agent --op turn.abort \
    --idempotency-key p2-operator-confirm --confirm \
    --json-input '{"mode":"terminal","scope":"turn","operator":true}'
```

```json
{"type":"broker_response","id":"584230c1-e118-4a67-a539-ffacef33aadc","ok":true,"result":{"ok":true,"selection":"turn","turn":"no_active_turn","terminal":"terminal_no_effect"}}
```

**NO-GO criterion:** an operator-gated steering operation would need an approval requirement on the `turn.steer` invocation itself. The installed CLI instead gates terminal abort with all of: `operator:true` in input, `--confirm`, and `--idempotency-key`.

## 4. `tail` cursor, strictness, and event vocabulary

The checkpoint probe was:

```text
$ gjc sdk session raw query 7fe02605-931b-472e-968c-a376b39cc488 \
    --agent-dir <tmp>/sdk-agent --query session.checkpoint --json-input '{}'
```

It succeeded but emitted no usable `checkpointToken` in the CLI output:

```json
{"type":"query_response","id":"533f09d9-1a7e-4707-9d3a-9f34e03b7d16","ok":true,"result":{"checkpoint":{"revision":0,"generation":0,"seq":0},"revisionId":"1","issuedAt":1788272815952,"expiresAt":1788273715952}}
```

Consequently the direct cursor syntax was accepted but a sentinel token failed closed:

```text
$ gjc sdk session tail 7fe02605-931b-472e-968c-a376b39cc488 \
    --agent-dir <tmp>/sdk-agent --cursor p2-invalid-checkpoint-token \
    --until-idle --strict --all-events --timeout-ms 1500
```

```json
{"ok":false,"error":{"code":"invalid_cursor","message":"invalid_cursor","details":{"code":"invalid_cursor","message":"invalid_cursor"}}}
```

The required strict-tail command itself failed with a retention gap:

```text
$ gjc sdk session tail 7fe02605-931b-472e-968c-a376b39cc488 \
    --agent-dir <tmp>/sdk-agent --until-idle --strict --all-events --timeout-ms 3000
```

```json
{"ok":false,"error":{"code":"retention_gap","message":"The event ring dropped entries before the checkpoint (strict mode).","details":{"code":"retention_gap","resync":{"revision":4,"generation":1,"seq":0}}}}
```

The non-strict comparison command succeeded and reported `terminal:true` plus this gap shape:

```text
$ gjc sdk session tail 7fe02605-931b-472e-968c-a376b39cc488 \
    --agent-dir <tmp>/sdk-agent --until-idle --all-events --timeout-ms 3000
```

```json
{"checkpoint":{"revision":4,"generation":0,"seq":0},"gap":{"code":"retention_gap","resync":{"revision":4,"generation":1,"seq":0}},"terminal":true}
```

The compaction attempt used the real control surface:

```text
$ gjc sdk session raw control 7fe02605-931b-472e-968c-a376b39cc488 \
    --agent-dir <tmp>/sdk-agent --op compaction.run --json-input '{}' --timeout-ms 5000
```

```json
{"ok":false,"error":{"code":"invalid_request","message":"Nothing to compact (session too small)","details":{"code":"invalid_request","message":"Nothing to compact (session too small)"}}}
```

### Event-name table

The successful non-strict tail contained transcript rows and the event-ring rows below. “Not observed” means it was absent from the returned item list; it is not inferred from implementation source.

| Requested/observed term | Observation from installed binary | Status |
| --- | --- | --- |
| `session_ready` | `kind:"session_ready"`, generation 1, seq 1 | Observed |
| `identity_header` | `kind:"identity_header"`, generation 1, seq 2 | Observed |
| `agent_start` | Observed at seq 4 and seq 8 | Observed |
| `agent_failed` | Observed at seq 3 and seq 6; terminal error was `Prompt submission failed.` | Observed |
| `agent_end` | Observed at seq 10; the non-strict tail set `terminal:true` | Observed |
| idle marker | `kind:"activity"` with `payload.state:"idle"` at seq 7 and seq 11 | Observed |
| `turn_start` | No such item returned | Not observed |
| `turn_end` | No such item returned | Not observed |
| assistant text frame | Two `kind:"transcript"` rows had `role:"assistant"`, each with `content:[{"type":"text","text":""}]` | Only empty assistant rows observed |
| steer echo | A retained user transcript row contained `textSummary:"p2 steer"`; no distinct `turn.steer` event item was returned | Transcript echo only |
| compaction event name | `compaction.run` failed before compaction because the session was too small | Not observed |
| `retention_gap` | Present in both strict failure and non-strict `gap` with `{code,resync}` | Observed |

**NO-GO criterion:** a usable emitted cursor, successful strict tail, and all requested lifecycle/text/compaction vocabulary were required. The checkpoint token was not available through this CLI output, `--strict` failed closed with `retention_gap`, and the required names/text were incomplete.

## 5. Duplicate op-ref conflict

The duplicate reused `p2-send-ref` with different text:

```text
$ gjc sdk session send 7fe02605-931b-472e-968c-a376b39cc488 \
    --agent-dir <tmp>/sdk-agent \
    --text 'different p2-sdk-send' \
    --op-ref p2-send-ref --wait --timeout-ms 15000
```

It exited 1 with the required conflict code:

```json
{"ok":false,"error":{"code":"client_ref_conflict","message":"A submission with this clientRef is already retained; never reuse a clientRef for retry.","details":{"code":"client_ref_conflict","message":"A submission with this clientRef is already retained; never reuse a clientRef for retry."}}}
```

**PASS criterion met:** differing content under a retained duplicate operation reference is rejected as `client_ref_conflict`.

## 6. Live model rebind

Command run:

```text
$ gjc sdk session raw control 7fe02605-931b-472e-968c-a376b39cc488 \
    --agent-dir <tmp>/sdk-agent --op model.set \
    --json-input '{"id":"openai/gpt-5.4"}'
```

Receipt:

```json
{"type":"control_response","id":"f2b39505-4987-4940-8866-dd65b5815883","ok":true,"result":{"changed":true}}
```

For comparison, root help also advertises direct per-process model selection as `--model=<value>`. It is not the needed fallback in this runtime because a live session accepted `model.set` and reported `changed:true`.

**PASS criterion met:** a real live-session `model.set` control was accepted and confirmed a changed selection.

## Cleanup evidence

The harness closed the scratch session with:

```text
$ gjc sdk session raw global --agent-dir <tmp>/sdk-agent --op session.close \
    --idempotency-key spike-close-7fe02605-931b-472e-968c-a376b39cc488 \
    --json-input '{"sessionId":"7fe02605-931b-472e-968c-a376b39cc488","cwd":"<tmp>/sdk-repo"}'
```

It exited 0. The foreground broker also exited 0 with empty stderr; the serve process exited 0 and removed its owned socket; the scratch root was removed last.

## Final decision

Do not use the installed `gjc/0.15.6` SDK runtime as the Stage 0 basis for an operator-gated, strict-cursor, fully vocabulary-driven persistent session model. Capabilities 1, 2, 5, and 6 are usable as observed. Capability 3 lacks a steering-specific operator gate, and capability 4 fails the hard cursor/strict-tail/event-vocabulary criteria.
