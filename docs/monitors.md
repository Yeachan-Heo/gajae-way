# Monitors

A monitor turns an external or scheduled signal into a Gajae-authored event. It has a name, one trigger, a non-empty list of declared event types, an optional burst policy, and at most one output target. A cron is simply a monitor whose trigger is periodic.

## MonitorSpec and triggers

`gajaeway monitors add --json` accepts the `MonitorSpec` shape from `@gajae-gateway/protocol`:

```json
{
  "name": "weekday-review",
  "trigger": { "kind": "cron", "schedule": "30 8 * * 1-5" },
  "eventTypes": ["review.due"],
  "burstPolicy": "dedupe",
  "instruction": "Read the open review queue, pick the oldest item, and post a one-paragraph verdict.",
  "channelTarget": {
    "origin": { "platform": "discord", "kind": "dm", "conversationId": "owner", "peerId": "owner" }
  },
  "enabled": true
}
```

`instruction` is the per-monitor execution instruction. It is prepended to the guidance section of the authoring prompt, so the event session is told what to *do*, not just that an event fired. It is optional (at most 4000 characters); a monitor without one falls back to the built-in maintenance guidance for its event types, and with neither the session only writes a receipt note.

The four trigger kinds are:

```json
{ "kind": "cron", "schedule": "30 5 * * *" }
```

```json
{ "kind": "webhook", "route": "incoming" }
```

```json
{ "kind": "watcher", "root": "/absolute/path/to/watch", "debounceMs": 250 }
```

```json
{ "kind": "script", "command": ["/absolute/path/to/check"], "intervalMs": 60000 }
```

A cron schedule is evaluated in the gateway host's local time. Every scheduled slot is claimed exactly once in `monitor_slots` together with its event, whose `firedAt` is the exact slot time. After a restart or a suspended process, the monitor replays every slot due since its durable cursor (the newest slot it claimed or skipped, never earlier than its creation), within the `monitorCatchUp` age/count ceiling. Slots beyond that ceiling are counted and exposed by `monitors inspect` as `catchUp`, never silently lost.

For webhook monitors, the registry replaces the supplied route with a generated route token. The runtime receives it at `/hook/<token>`. Watcher roots must fall under configured `watcherRoots`; script commands must be inside configured `scriptRoot` and are checked by ActionGuard.

## Event sessions and propagation

Event types are declared at monitor creation; they are never inferred. A declared type uses its own monitor-event-type session (`monitor/eventtype/<event type>`). An undeclared type is deliberately routed to the single `monitor/eventtype/catch-all` session, preventing accidental mixing with a declared workflow.

The durable propagation path is:

1. **Admitted** — persist the incoming event before work begins and emit its systematic event record.
2. **Batched** — apply the monitor burst policy and assign a batch.
3. **Session selected** — choose the declared-type session or catch-all session.
4. **Authored** — ask Gajae for exactly one note per event, then persist each authored output.
5. **Memory queued** — create a durable `monitor-event` memory intent for the authored note.
6. **Delivered** — when a `channelTarget` exists, prepare and mark an outbound ledger delivery.
7. **Reconciled** — startup and periodic reconciliation replays unfinished admitted, dispatched, or failed events, and repairs authored events missing their memory intent.

The admission log occurs before propagation. Systematic state is held in the gateway database (`monitor_event` stages such as `admitted`, `batched`, `dispatched`, `authored`, and `failed`); the authored note is separately persisted and fed to the Markdown-memory closure queue. This dual logging preserves both operational history and human-readable memory.

## Session context: native compaction and the safety net

Monitor authoring shares one persistent broker-hosted `gjc` session per event-type origin, so a monitor session accumulates context fast: a 10-minute monitor authors about 144 turns a day.

Keeping that bounded is `gjc`'s job, not the gateway's. Persistent SDK sessions apply native auto-compaction, and the GJC SDK also exposes a `compaction.run` control action for an explicit request. The production incident behind issue #68 was not missing compaction but late compaction: the session overflowed with zero entries left, the authoring turn came back empty, and dispatch settled as `internal_error`. The host now runs gjc 0.15.6 with adaptive compaction (base 70%, floor 45%), which is the first line of defence.

The gateway's job is the second line: detecting that native compaction silently stopped working. A monitor that keeps answering is **never** rolled, whatever its turn count — the count is recorded and reported, and is purely observational. Instead the gateway classifies each authoring failure onto exactly three axes:

- **context** — empty response, a context-length rejection, or a zero-token completion. This is what a compaction failure looks like from outside, and it is the only class the roll decision reads.
- **executor** — the work around the turn failed: a worker timeout, an external tool failure, a lock that made the run skip. The model may never have been asked. Unrecognised runtime errors land here too: without positive evidence the safety net stays holstered.
- **protocol** — the answer arrived but broke the contract: unparseable JSON, wrong shape, missing/duplicate/unknown events. A non-empty answer proves the context still works, so this is the opposite of context evidence.

The executor class carries a sub-kind, reported as `lastExecutorReason`, because the operator action differs:

- `executor_timeout` — the worker hit its cap and nothing was left behind. The aside collection worker's 300s cap is the canonical case, recorded as `aside_timeout`.
- `executor_orphaned_external_work` — the child CLI process was killed on the wrapper's timeout but the external daemon's job kept running, so the next tick stacks a duplicate on top. Measured: aside exec collection took 7–13 minutes, the wrapper killed the child at 300/360s, the Aside daemon task stayed `running`, and 3 sessions and 10 tabs accumulated. Recorded as `orphaned_executor`, and it outranks the timeout match — the same message looks like a timeout, but "external work is still running" is the actionable part. The remedy is reclaiming that external job; the gateway therefore names the reason and leaves the session completely alone, with no streak, no compaction request and no roll.
- `executor_failed` — anything else on the executor side.

Executor and protocol failures are counted for the operator and can never arm a roll, however long they repeat. `lastExecutorReason` deliberately survives a roll: an unreclaimed external executor is not fixed by rolling a session.

The streak is strictly **consecutive** and strictly about the **current** session:

- Any non-empty answer resets it. An isolated `context_too_large` followed by a healthy turn is not a roll reason.
- A batch that reconcile replayed (`dispatch_attempts > 0`) carries an old payload, and a failure whose bound epoch is no longer the live epoch belongs to a session that is already gone. Both are recorded as `staleContextFailures` and dropped, so a dead session's failure record can never roll its successor.
- A roll clears the streak and the executor/protocol counters: a new epoch starts with a clean slate.

On a counted context-class failure the gateway asks for native compaction through `CompactionPort` — the single seam where compaction is requested. The default implementation reports `unavailable` and does nothing, because a fake success would disarm the safety net exactly when it is needed; the doc comment names the SDK `compaction.run` delegation as its wiring target. A roll fires only when consecutive context-class failures reach `monitorContextFailureRollThreshold` (default 2, restart-only) **and** the native-compaction request came back `unavailable`, `failed` or `skipped`. The reason is recorded as a structured code (`context_failures_native_compaction_unavailable` / `_failed` / `_skipped`) alongside the native-compaction result; the event row carries the `authoring_context_exhausted` failure code. Raw provider messages are never logged or persisted — they can carry secrets.

A roll bumps the session epoch (fresh gjc session and idempotency key) and injects a compact digest into that session's first authoring prompt: the monitor's standing instruction plus its most recent authored notes, each clipped and the whole digest capped. The digest is pure text assembly — no model call — so a roll never costs a turn. Continuity is carried by the digest, not by the discarded transcript.

The roll happens after the per-origin turn chain is taken and before the session is bound, so a batch can neither be stranded nor authored twice across the boundary; leases and fencing are untouched. All of it lives in one place (`packages/gateway/src/monitors/compaction.ts` plus the single `#rollSessionIfArmed` call site).

Persona sessions likewise do not rotate on a turn count. Native SDK compaction is observed, while `/new` remains the explicit epoch boundary.

## Persistent-session operator signals

Monitor authoring uses the same broker-bound `SessionPort` and its request/response convenience. The daemon emits grep-stable signals: `broker_restart generation=… backoffMs=…` for a supervised lifecycle replacement, `compaction_event sessionId=… originKey=…` when native compaction is observed, and `stall_alert originKey=… sessionId=… silentMs=…` for an alert-only silent running operation. A held retired batch is logged as `retired_hold originKey=… batchKey=…`; do not resend it manually. Status and tail evidence decide terminal state.

## Burst policies

Burst handling is per monitor and event type. The default is `coalesce`.

- `coalesce`: collect a short 250 ms batch and author each event together.
- `dedupe`: retain only distinct JSON payloads in that batch; duplicate arrivals are marked `deduped`.
- `serialize`: dispatch each event immediately, one submission at a time.
- `drop`: within an existing short batch, retain the newest event.

## Webhook ingress security

Webhook binding defaults to loopback. A non-loopback bind requires both `webhook.exposeNonLoopback: true` and authentication on **every** webhook monitor. The runtime supports bearer secrets and HMAC secrets read from a credential file. HMAC requests require `x-gajaeway-timestamp`, `x-gajaeway-nonce`, and `x-gajaeway-signature`; timestamps must be within five minutes and nonces cannot be replayed in that window. Bodies are limited to 256 KiB. Put exposed ingress behind your normal authenticated network boundary as well.

## CLI

```sh
gajaeway monitors add --json '{"name":"weekday-review","trigger":{"kind":"cron","schedule":"30 8 * * 1-5"},"eventTypes":["review.due"],"burstPolicy":"dedupe","enabled":true}'
gajaeway monitors list
gajaeway monitors list --json
gajaeway monitors list --fields id,name,schedule --limit 20 --offset 20
gajaeway monitors inspect <monitor-id>
gajaeway monitors test <monitor-id> --type review.due --payload '{"source":"manual"}'
```

`list` prints a one-line-per-monitor table (`id`, `name`, `schedule`, `events`, `target`, `enabled`); `--json` emits the raw monitor records. `--fields a,b,c` selects columns (an unknown name errors and lists the valid names) and `--limit N` / `--offset N` page the rows; both apply to `--json` as well. `sessions list` accepts the same flags. `inspect` returns the selected monitor and its recent event records. `test` submits an event and returns its `eventId`; omit `--type` to use the monitor’s first declared type. See [deployment](deployment.md) for `webhook`, `watcherRoots`, and `scriptRoot` configuration.
