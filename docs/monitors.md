# Monitors

A monitor turns an external or scheduled signal into a Gajae-authored event. It has a name, one trigger, a non-empty list of declared event types, an optional burst policy, and at most one external channel-delivery target. A cron is simply a monitor whose trigger is periodic.

## Default memory monitors

By default, boot seeds `memory-canonicalize`, `memory-audit`, and `memory-debloat` once per database. To skip this before the first boot, put the following in `$GAJAEWAY_HOME/config.json`:

```json
{ "schemaVersion": 1, "memory": { "seedDefaultMonitors": false } }
```

This restart-required boolean skips only default creation, not the monitor runtime. It does not disable, delete, or otherwise change existing monitors (including previously seeded defaults and custom monitors). With a fresh database, no default monitors or seed markers are created, including on subsequent boots. Set it to `true` or omit it and restart to seed normally later. Existing seed markers still apply: removed defaults are never resurrected and disabled defaults stay disabled.

## MonitorSpec and triggers

`gajaeway monitors add --json` accepts the `MonitorSpec` shape from `@gajae-gateway/protocol`:

```json
{
  "name": "weekday-review",
  "trigger": { "kind": "cron", "schedule": "30 8 * * 1-5", "timezone": "Asia/Seoul" },
  "eventTypes": ["review.due"],
  "burstPolicy": "dedupe",
  "instruction": "Read the open review queue, pick the oldest item, and post a one-paragraph verdict.",
  "channelTarget": {
    "origin": { "platform": "discord", "kind": "channel", "conversationId": "1470204268933022023" },
    "mentionUserIds": ["1468532331001413743"]
  },
  "reportTo": { "platform": "discord", "kind": "channel", "conversationId": "1470204268933022023" },
  "enabled": true
}
```

`instruction` is the per-monitor execution instruction. The authoring prompt opens by telling the event session to do the work with its tools before replying, then gives the instruction as the `Task:` line, followed by the built-in maintenance guidance for its event types. The JSON `{eventId, note}` response contract comes last, and the note must report what was actually done and found with at least one concrete evidence marker not already in the event contract (a command result, changed-file path, commit SHA, issue/PR number, URL, or numeric result), not restate the task (#369, #632). The instruction is optional (at most 4000 characters); a monitor without one falls back to the built-in maintenance guidance, and with neither the prompt has no task line and the session only writes a receipt note. Write it as prose sentences, not a tag bundle: it reaches the authoring session verbatim.

Destination and pings are typed fields, never part of the event type or the instruction. `channelTarget.origin` is where authored notes are delivered; `channelTarget.mentionUserIds` (Discord snowflakes or Slack `U…`/`W…` ids; other platforms reject it) are prefixed to every delivered note as `<@id>` by the gateway, so the author never has to remember who to ping. Optional `reportTo` identifies a chat or loopback conversation whose persona session should also receive each reportable authored note as a durable internal report. Silent outputs and contract-only echoes suppressed by channel delivery are not injected. This is context injection, not a chat delivery: it lets the conversation session know what the separate monitor session did, without posting another message to the channel. `reportTo` may match `channelTarget.origin`; omitting it preserves the existing behavior. Monitor execution origins are not valid report targets. Keep `eventTypes` short, stable identifiers that are safe to group by.

`procedureFiles` (optional, at most 8) lists the procedure/doctrine files the monitor follows, relative to `$GAJAEWAY_HOME/workspace` (`memory/ops/...` reaches the memory corpus through the workspace link). The event-type session is long-lived and never re-reads files it saw at session start, so these files are re-read from disk at **every** firing and their current content goes into that firing's authoring prompt. A doctrine edit therefore takes effect on the very next firing, without a session roll or restart. Each file is inlined up to 24 KiB; a larger one is versioned and the session is told to read it from disk. Paths must stay inside the workspace (symlinks may resolve into the memory corpus only); a missing or escaping file is reported, not fatal.

Every event authored with declared procedure files records the versions it was given (`path`, `status`, `sha256`, `mtime`) in `monitor_events.procedure_json`, surfaced as `procedure` on `gajaeway monitors inspect` events, so you can tell a stale-procedure firing from a disobeyed rule after the fact.

`contextRollTokens` (optional integer, 10000 to 2000000) is a per-monitor observed-usage roll threshold, not a hard context limit. Usage is sampled after an answered authoring turn; a count at or above the threshold arms a roll for the next fresh dispatch. Unset by default, it never rolls on size. See the session-context trade-off below.

On update only, `contextRollTokens: null` clears the threshold; creating a monitor with `null` is invalid. Omitting the field from an update preserves its current value.

`overlap` decides what a new fire does while an earlier event of the same monitor still owes an authoring turn (stage `admitted`, `batched`, `dispatched`, or `failed` awaiting retry):

- `queue` (default) admits it behind the predecessor. A monitor slower than its cadence then stacks up and its later reports describe old state.
- `skip` records the fire as the terminal stage `skipped`, naming the in-flight predecessor (`skippedBy` in `monitors inspect`). It never authors, never retries, and writes no `monitor_failures` row: losing the slot is a scheduling outcome, not an error. A skipped cron slot stays claimed, so restart catch-up does not replay it. Use `skip` for watchdogs whose report is only worth reading while it is current.

`burstPolicy` is separate: it only merges fires that arrive within the same 250 ms admission window. It has no effect on a fire that lands while a previous turn is still running.

The four trigger kinds are:

```json
{ "kind": "cron", "schedule": "30 5 * * *", "timezone": "Asia/Seoul" }
```

Cron schedules use five fields (`minute hour day-of-month month day-of-week`) in the trigger's IANA `timezone`. If omitted, the gateway resolves its host-local timezone when the monitor is added and records that effective zone in the trigger. Set `timezone` explicitly when a schedule must keep the same wall-clock meaning across hosts. Schedule data is a separate RPC projection, not a `MonitorRecord` field. `monitor.list` returns `{ "monitors": [...], "schedules": { "<monitorId>": { "effectiveTimezone": "Asia/Seoul", "nextFireAt": { "local": "2026-08-28 08:30:00", "utc": "2026-08-27T23:30:00.000Z" } } } }`; `monitor.inspect` returns `{ "monitor": ..., "schedule": { "effectiveTimezone": "Asia/Seoul", "nextFireAt": { "local": "2026-08-28 08:30:00", "utc": "2026-08-27T23:30:00.000Z" } }, "recentEvents": [...] }`. A schedule with no upcoming cron fire has `nextFireAt: null`; non-cron schedules have `effectiveTimezone: null` and `nextFireAt: null`.

The scheduler evaluates absolute UTC minute slots against local wall-clock fields. A nonexistent spring-forward minute is skipped; a repeated fall-back minute fires once for each distinct UTC instant.

```json
{ "kind": "webhook", "route": "incoming" }
```

```json
{ "kind": "watcher", "root": "/absolute/path/to/watch", "debounceMs": 250 }
```

```json
{ "kind": "script", "command": ["/absolute/path/to/check"], "intervalMs": 60000 }
```

Cron slots use the trigger's configured IANA timezone and are claimed exactly once in `monitor_slots` together with their event; `firedAt` is the exact scheduled slot time. On startup and after a suspended tick, the gateway replays every due slot since that monitor's durable cursor (newest claimed or policy-skipped slot, never earlier than creation), not just the last hour and not as one coalesced event. `monitorCatchUp` limits replay to the newest `maxSlots` within `maxAgeMs`; older or over-limit slots are durably counted and logged as `monitor_slots_skipped`, and `gajaeway monitors inspect <id>` reports them under `catchUp`. See [deployment](deployment.md) for bounds and defaults.

For webhook monitors, the registry replaces the supplied route with a generated route token. The runtime receives it at `/hook/<token>`. Watcher roots must fall under configured `watcherRoots`; script commands must be inside configured `scriptRoot` and are checked by ActionGuard.

## Event sessions and propagation

Event types are declared at monitor creation; they are never inferred. Every session is owned by one monitor: a declared type uses that monitor's session for the type (`monitor/eventtype/<event type>/parent=<monitor id>`), and an undeclared type is routed to that monitor's catch-all session (`monitor/eventtype/catch-all/parent=<monitor id>`), so it cannot mix with a declared workflow. Two monitors that declare the same event type never share a session, its history, its instruction, or its failure domain: a poisoned session fails only the monitor that owns it. Rows keyed by the old event-type-only origin (`monitor/eventtype/<event type>`) are no longer bound after the upgrade. Their history is left unused, not re-keyed, because it already mixes monitors.

The durable propagation path is:

1. **Admitted** — persist the incoming event before work begins and emit its systematic event record.
2. **Batched** — apply the monitor burst policy and assign a batch.
3. **Session selected** — choose the monitor's declared-type session or its catch-all session.
4. **Authored** — ask Gajae for exactly one note per event, persist each authored output, and atomically enqueue it as an internal report to `reportTo` when configured.
5. **Memory queued** — create a durable `monitor-event` memory intent for the authored note.
6. **Delivered** — when a `channelTarget` exists, prepare and mark an outbound ledger delivery.
7. **Reconciled** — startup and periodic reconciliation replays unfinished admitted, dispatched, or failed events, and repairs authored events missing their memory intent. A `failed` event is reclaimed on a backoff measured from its last failure — immediately, then after 10 minutes, 1 hour, 4 hours and 12 hours — so the five-attempt budget spans ~17 hours and a slot survives a multi-hour dispatch outage. Only after that does it land on the terminal `failed_no_retry`. A `session_busy`, `session_host_unavailable`, or `gateway_shutdown` failure is not evidence of a dispatch outage, so it is reclaimed on the next sweep without waiting out the backoff; it still counts against the budget.

A gateway stop waits a bounded 10 seconds for in-flight authoring turns. A turn still running past that is failed as `gateway_shutdown` with the bound session id and stop time in `monitor_failures.detail`, its lease is released, and the event is re-dispatched by the next boot's reconcile rather than retried against a session whose host the stop orphaned.

The admission log occurs before propagation. Systematic state is held in the gateway database (`monitor_event` stages such as `admitted`, `batched`, `dispatched`, `authored`, `failed`, and the terminal `skipped`); the authored note is separately persisted and fed to the Markdown-memory closure queue. This dual logging preserves both operational history and human-readable memory.

The stale-event reaper only handles events still owing an authoring turn: `admitted`, `batched`, `dispatched`, or `failed`, with no authored output, live lease, or saved authoring-turn pointer. It marks eligible events older than 24 hours (measured from `updated_at`) `failed_no_retry`. Authored events awaiting memory or delivery, including delivery-pending events, are not reaped. Since age is based on `updated_at`, a retry resets the stale-age window; `firedAt` remains the original event/fire time.

## Session context: native compaction and the safety net

Monitor authoring shares one persistent broker-hosted `gjc` session per event-type origin, so a monitor session accumulates context fast: a 10-minute monitor authors about 144 turns a day.

Keeping that bounded is `gjc`'s job, not the gateway's. Persistent SDK sessions apply native auto-compaction, and the GJC SDK also exposes a `compaction.run` control action for an explicit request. The production incident behind issue #68 was not missing compaction but late compaction: the session overflowed with zero entries left, the authoring turn came back empty, and dispatch settled as `internal_error`. The host now runs gjc 0.15.6 with adaptive compaction (base 70%, floor 45%), which is the first line of defence.

The gateway's job is the second line: detecting that native compaction silently stopped working. A monitor that keeps answering is **never** rolled unless its owner set `contextRollTokens`. Turn count is recorded and reported, and is purely observational.

An armed size roll is persisted with its session epoch and observed token count, so it survives a gateway restart. It is restored only for the same live epoch and still revalidates the current threshold before rolling.

A size roll waits for saved authoring turns on the current session while any batch member remains open for ordinary retry. Retained terminal pointers do not delay a roll; given-up turns can still recover late results from their original session and operation.

After a size roll, retirement uses the broker's journaled `session.close`, not a gateway PID signal. This is a forced shutdown, not an idle-only operation: it preserves the transcript but aborts active work. No valid saved authoring pointer or live binding may name the session. Every close attempt, including one after a failed close, monitor removal or restart, requires positive idle evidence from three independent probes in this order: `turnRunning === false`, then `queueEmpty === true` when implemented, then an empty `runningJobs` list. Missing turn or job probes, active or unknown evidence, and probe errors hold the record without closing. An unimplemented optional queue probe is skipped. After the last await, one synchronous transaction rechecks ownership of the exact record, live binding and saved pointers, independently of monitor existence. It records `phase: "closing"` and `closeStartedAt` only as diagnostics for the latest issued attempt, never as authority for another close. The port's close is invoked without an intervening await.

Each fresh dispatch attempts at most two retirements for its origin, inside its existing dispatch lock. The gateway's 60-second monitor reconcile also sweeps retirement records across all origins, including removed monitors, attempting at most two records globally per pass. The retirement pass runs in the background without taking an origin lock, so a busy origin delays neither event recovery nor its own dispatch through the sweep. Both paths share the same synchronous final authorization, pointer and idle checks, and an in-process attempt guard prevents concurrent attempts on the same retirement record. They order records by oldest `lastAttemptAt`, then `retiredAt`, and persist each attempt for fairness. Reconcile sweeps do not overlap. After rotation, fresh authoring uses the live binding; harvest reads existing saved turns rather than creating new work on the retired session. Invalid logical counters are repaired and safe-integer saturation rebases their ordering. Retirement errors never fail authoring or reconciliation. Records survive monitor removal and restart. Every close failure keeps the record for a later pass and logs `error:<code>` (or `error:unknown` without a recognized code); nothing is deleted on inferred absence, including a reported `not_found`. Only a successful `close()` resolution deletes the record, and only if it is unchanged. A port without close support relinquishes the record as unsupported.

A record whose host never answers stays indefinitely, even after monitor removal or if the host independently died. The probes do not atomically reserve an idle host. A background job can finish and wake an autonomous turn between probes, or work can start between the last probe and the broker close, which may wait in the CLI pool. Such work is aborted by the forced close. Closing this known race requires an idle-fenced broker close, tracked in [Yeachan-Heo/gajae-code#7540](https://github.com/Yeachan-Heo/gajae-code/issues/7540). A later dispatch or reconcile retries the close call, not necessarily the shutdown effect. While the port retains an idempotency key, the broker may replay a stored terminal refusal or uncertainty even after the host recovers, and the retirement record remains. A new close operation requires the old intent to be released or pruned, and still requires fresh idle evidence and final authorization. Pending close intents currently become eligible for pruning after 24 hours; cleanup has no bounded completion guarantee.

An operator retry after a size roll checks the tagged saved operation on its original session before authoring fresh: readable results are harvested, and failed, running, unknown, or temporarily unreadable results are kept without resending. The tag enforces this policy through retry exhaustion and late-result recovery too; an unresolved event can reach `authoring_turn_unresolved` while retaining its pointer. Only `output_unavailable` with `definitiveLoss: true` (a matching operation identity, valid terminal timestamps, no content, and explicit `receiptState: "absent"`) or an exact durable busy refusal permits fresh authoring. All other unavailable results hold the pointer: `identity_mismatch`, `invalid_evidence`, `cancelled`, `incomplete_body`, and `output_unavailable` without definitive loss, including unsupported queries and session errors. A `missing` receipt remains pending, not definitive loss. Pointers from failure rolls retain the existing recovery behavior.

With `contextRollTokens` set, an answered fresh authoring turn whose observed session context is at or above the current threshold arms `context_budget`. The next fresh dispatch rechecks the current monitor configuration and persisted arm against the live epoch before starting a new session epoch seeded with the monitor digest described below. Clearing the threshold or raising it above the latest observed count disarms the roll, including for a dispatch already queued behind another turn. This is not a hard limit: context is sampled after the turn only when the field is set, and a stale reading can still cause a roll immediately after native compaction reduced the actual context. Harvesting an already accepted turn never samples context or arms a size roll; unavailable context usage leaves the answered turn successful without arming one. `lastContextTokens` is an internal diagnostic, cleared on roll, not an operator API projection. The field is unset by default, preserving native compaction and existing failure-recovery behavior. Lower carried context reduces cost per call, but loses in-session memory beyond the digest. A monitor whose procedure keeps state only in its session should persist that state on disk before opting in.

Busy, prior-run-busy, submission-wedged, and host-unavailable failure rolls take precedence over a pending size roll and do not wait on size-only saved-turn deferral. Failure rolls retain their separate immediate host-termination path. Removing a monitor clears its persisted size arms and digest handoffs for every declared and catch-all origin, but no longer abandons retired sessions: their records remain owned and eligible for reconciliation. Removal does not clear saved authoring pointers or live session bindings, which continue to hold closure when they name the retired session. Removal during an idle probe does not revoke retirement authorization; an already-issued close cannot be undone. Clearing `contextRollTokens` after a roll does not discard its retirement records: fresh dispatches and reconciliation still sweep them to finish cleanup of the earlier roll. Late answers and failures cannot arm rolls, and queued dispatches cannot consume a pending roll or recreate its handoff after removal.

Size-roll lifecycle messages are grep-stable:

```text
monitor session safety net armed for <originKey> (monitor <id>): reason=context_budget context_tokens=<tokens>/<threshold> turns=<turns>
monitor session size roll disarmed for <originKey> (monitor <id>): reason=context_budget context_tokens=<tokens|unknown>/<threshold|unset>
monitor session size roll deferred for <originKey> (monitor <id>): reason=context_budget saved_turns=<n>
monitor session rolled for <originKey> (monitor <id>): reason=context_budget native_compaction=<status> digest=<bytes>B.
monitor retired session close for <originKey>: session=<id> outcome=<closed|held:<reason>|error:<code>|unsupported>
```

For failure recovery, the gateway classifies each authoring failure onto exactly three axes:

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

On a counted context-class failure the gateway asks for native compaction through `CompactionPort`, the single seam where compaction is requested. The default implementation reports `unavailable` and does nothing, because a fake success would disarm the safety net exactly when it is needed; the doc comment names the SDK `compaction.run` delegation as its wiring target. A context-failure roll fires only when consecutive context-class failures reach `monitorContextFailureRollThreshold` (default 2, restart-only) **and** the native-compaction request came back `unavailable`, `failed` or `skipped`. The reason is recorded as a structured code (`context_failures_native_compaction_unavailable` / `_failed` / `_skipped`) alongside the native-compaction result; the event row carries the `authoring_context_exhausted` failure code. Raw provider messages are never logged or persisted because they can carry secrets.

A roll bumps the session epoch (fresh gjc session and idempotency key) and injects a compact digest into that session's first authoring prompt: the monitor's standing instruction plus its most recent authored notes, each clipped and the whole digest capped. Notes that ended `contract_unmet`, silent notes, and notes that only restate the instruction or event type are left out, so a fresh session never starts from an echo it would copy. The digest is pure text assembly — no model call — so a roll never costs a turn. Continuity is carried by the digest, not by the discarded transcript.

The digest handoff survives bind failure, gateway restart, and later epoch changes until the first answered turn on the live epoch.

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
gajaeway monitors add --json '{"name":"weekday-review","trigger":{"kind":"cron","schedule":"30 8 * * 1-5","timezone":"Asia/Seoul"},"eventTypes":["review.due"],"burstPolicy":"dedupe","enabled":true}'
gajaeway monitors update <monitor-id> --schedule '0 9 * * 1-5'
gajaeway monitors update <monitor-id> --enabled false
gajaeway monitors update <monitor-id> --schedule '0 9 * * 1-5' --enabled true
gajaeway monitors update <monitor-id> --json '{"instruction":"Review the open queue."}'
gajaeway monitors update <monitor-id> --json '{"contextRollTokens":100000}'
gajaeway monitors update <id> --json '{"contextRollTokens":null}'
gajaeway monitors list
gajaeway monitors list --json
gajaeway monitors list --fields id,name,schedule --limit 20 --offset 20
gajaeway monitors inspect <monitor-id>
gajaeway monitors test <monitor-id> --type review.due --payload '{"source":"manual"}'
gajaeway monitors test <monitor-id> --wait=60
gajaeway monitors settle <event-id>
gajaeway monitors retry <event-id>
```

`settle` marks an event that still owes authoring as `failed_no_retry`; it refuses an event with a live authoring lease and preserves any saved authoring-turn pointer. `retry` requeues a `failed` or `failed_no_retry` event for immediate reconciliation, provided it has no authored output or live lease. It resets the dispatch-attempt count and refreshes `updated_at` without changing `firedAt`, and also retains the saved authoring-turn pointer so an already accepted turn can be recovered safely. Both commands print JSON with `{ eventId, done, previousStage?, reason? }`; a result with `done: false` exits with status 1. Neither command reaps or retries authored output that is waiting on memory closure or delivery settlement.

`update` changes the existing monitor in place, preserving its identity and event history. Its `--json` value is a partial `MonitorSpec`: only supplied fields are merged into the current spec. Use either `--json` or the shorthand flags; `--schedule '<cron>'` and `--enabled true|false` may be used together. The enabled flag requires an explicit `true` or `false`. The command prints the resulting monitor ID as JSON. `list` prints a one-line-per-monitor table (`id`, `name`, `schedule`, `timezone`, `nextFire`, `events`, `target`, `reportTo`, `contextRollTokens`, `enabled`); `--json` emits `{ monitors, schedules }`, with `schedules` keyed by monitor ID. Each schedule projection contains `effectiveTimezone` and nullable `nextFireAt.local` / `nextFireAt.utc`; those fields are not added to the monitor record. `--fields a,b,c` selects columns (an unknown name errors and lists the valid names) and `--limit N` / `--offset N` page the rows; both apply to `--json` as well. `sessions list` accepts the same flags. `inspect` returns `{ monitor, schedule, recentEvents }`, with the effective zone in `schedule.effectiveTimezone` and next local/UTC times in `schedule.nextFireAt.local` and `schedule.nextFireAt.utc`. `test` submits an event and returns its `eventId`; omit `--type` to use the monitor’s first declared type. Add `--wait` to observe stage events without polling (30-second default), or `--wait=SECONDS` to choose a non-negative timeout up to 24 hours. It exits when delivery, no-delivery, or dispatch failure is observed; on timeout it returns the latest observed stage, such as `batched`, `authored`, `delivered`, or `failed`. See [deployment](deployment.md) for `webhook`, `watcherRoots`, and `scriptRoot` configuration.
