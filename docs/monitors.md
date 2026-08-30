# Monitors

A monitor turns an external or scheduled signal into a Gajae-authored event. It has a name, one trigger, a non-empty list of declared event types, an optional burst policy, and at most one output target. A cron is simply a monitor whose trigger is periodic.

## MonitorSpec and triggers

`gajaeway monitors add --json` accepts the `MonitorSpec` shape from `@gajaeway/protocol`:

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

Monitor authoring shares one gjc session per event-type origin, and every turn is replayed by `gjc --resume`, so a monitor session accumulates context fast: a 10-minute monitor authors about 144 turns a day.

Keeping that bounded is gjc's job, not the gateway's. The non-interactive `-p --mode json` path goes through `AgentSession.prompt()` exactly like the interactive one, so native auto-compaction applies to authoring turns, and the GJC SDK also exposes a `compaction.run` control action for an explicit request. The production incident behind issue #68 was not missing compaction but late compaction: the session overflowed with zero entries left, the authoring turn came back empty, and dispatch settled as `internal_error`. The host now runs gjc 0.15.5 with adaptive compaction (base 70%, floor 45%), which is the first line of defence.

The gateway's job is the second line: detecting that native compaction silently stopped working. A monitor that keeps answering is **never** rolled, whatever its turn count — the count is recorded and reported, and is purely observational. Instead the gateway classifies each authoring failure:

- **context-class** — empty response, a context-length rejection, or a zero-token completion. This is what a compaction failure looks like from outside.
- **other** — malformed JSON, response-contract violations, bind and delivery errors. These say nothing about context size and never arm the safety net. A non-empty answer, even a malformed one, clears the context-failure streak.

On a context-class failure the gateway asks for native compaction through `CompactionPort` — the single seam where compaction is requested. The default implementation reports `unavailable` and does nothing, because a fake success would disarm the safety net exactly when it is needed; the doc comment names the SDK `compaction.run` delegation as its wiring target. A roll fires only when consecutive context-class failures reach `monitorContextFailureRollThreshold` (default 2, restart-only) **and** the native-compaction request came back `unavailable`, `failed` or `skipped`. The reason is recorded as a structured code (`context_failures_native_compaction_unavailable` / `_failed` / `_skipped`) alongside the native-compaction result; the event row carries the `authoring_context_exhausted` failure code. Raw provider messages are never logged or persisted — they can carry secrets.

A roll bumps the session epoch (fresh gjc session and idempotency key) and injects a compact digest into that session's first authoring prompt: the monitor's standing instruction plus its most recent authored notes, each clipped and the whole digest capped. The digest is pure text assembly — no model call — so a roll never costs a turn. Continuity is carried by the digest, not by the discarded transcript.

The roll happens after the per-origin turn chain is taken and before the session is bound, so a batch can neither be stranded nor authored twice across the boundary; leases and fencing are untouched. All of it lives in one place (`packages/gateway/src/monitors/compaction.ts` plus the single `#rollSessionIfArmed` call site).

The chat path keeps its own, separate rotation at 50 turns; monitor sessions have no turn ceiling at all.

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
gajaeway monitors inspect <monitor-id>
gajaeway monitors test <monitor-id> --type review.due --payload '{"source":"manual"}'
```

`list` returns monitor records. `inspect` returns the selected monitor and its recent event records. `test` submits an event and returns its `eventId`; omit `--type` to use the monitor’s first declared type. See [deployment](deployment.md) for `webhook`, `watcherRoots`, and `scriptRoot` configuration.
