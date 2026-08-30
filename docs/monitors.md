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

## Session health, native compaction, and the last-resort roll

Compaction is the runtime's job, and the gateway never implements its own summarising compaction — a second summary contract would drift from GJC's. The order of preference is fixed:

1. **Observe.** Every monitor authoring turn is counted on the session row, so growth is visible instead of invisible, and every authoring failure is classified: context-family (a zero-token/empty answer, or a runtime code such as `context_too_large`) versus everything else (a malformed response, a failed turn, a bind failure). Classification is by structured code and error type only, never by message wording.
2. **Ask the runtime to compact.** On context pressure the gateway invokes GJC's own compaction control action (`compaction.run`) through `CompactionPort` (`packages/gateway/src/orchestrator/compaction.ts`). The outcome is recorded per session as `compacted`, `skipped`, `failed`, or `unavailable`. A reported `compacted` clears the failure streak, so the session is left alone.
3. **Roll as a last resort.** Only when the native attempt left the session unrecovered (`failed`, `skipped`, or `unavailable`) *and* context-family failures have recurred to `monitorContextFailureThreshold` (default 2, range 1–10, restart-only) does the gateway roll the session epoch and seed the fresh session's first authoring prompt with a digest: the monitor's standing instruction plus its most recent authored notes, each clipped and the whole digest capped. The digest is pure string assembly over durable text — no model call.

Today the default port is `UnavailableCompactionPort`, which reports `unavailable` with code `no_control_channel_oneshot_cli`. That is the honest state of the integration: the gateway drives gjc as a one-shot `gjc --session <id> -p --mode json` process, whose flags are `--resume`, `--session-dir` and `--no-session`, so there is no channel to carry a control action. When the gateway gains an SDK/control session, a real implementation drops in behind the port and step 3 stops firing on `unavailable`.

Any turn that produces text clears the streak, so a monitor whose JSON keeps failing is never mistaken for one whose context is exhausted, and a healthy session is never rolled however many turns it accumulates.

The roll happens after the per-origin turn chain is taken and before the session is bound, so a batch can neither be stranded nor authored twice across the boundary; leases and fencing are untouched. Operator evidence is coded, never raw: each affected event gets an `authoring_context_exhausted` failure row, each native attempt logs `status=… code=…`, and the roll logs `reason=context_exhausted_after_native_<status>` with `native_compaction`, the streak, the dead epoch's turn count, and the digest size.

Background (issue #68): two production monitor sessions reached 14.7 MB and 13.9 MB with zero compaction entries, the last healthy turn running at ~918K context tokens. The host ran gjc 0.15.3 and adaptive compaction landed in 0.15.4; the model catalog also advertises a 372000-token window while observed provider usage passed 900K. The fallback lives in `packages/gateway/src/monitors/session-health.ts` plus the single `#rollIfNativeCompactionFailed` call site, and the native attempt in the single `#runNativeCompaction` call site.

The chat path keeps its own turn rotation at 50 turns; that path has no compaction contract to rely on.

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
