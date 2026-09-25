# Gajaeway v1 operator runbook

## Install and run

For production, compile standalone binaries — no source checkout, node_modules, or Bun install is needed on the host:

```sh
bun run build   # emits dist/gajaeway-gateway, dist/gajaeway-discord, dist/gajaeway-telegram, dist/gajaeway-slack, dist/gajaeway
dist/gajaeway-gateway daemon
```

The external `gjc` binary remains a runtime dependency. `GlobalGjcClient` connects to the same global-user executable, canonical agent directory, and broker as the operator's normal SDK; it does not own a private runtime. Verify `command -v gjc` in the normal user's shell and set the managed service's `GJC_EXECUTABLE` to that absolute executable path. Preserve that user's canonical profile environment, including `HOME` and any intentional GJC/PI config-directory or coding-agent-directory selection. No gateway-private profile overrides, copied model configuration, pinned shared settings, startup reaping, or global GC are permitted. GJC owns daemon startup and lifecycle; gateway stop closes only its own calls and relays, never the shared user daemon or session hosts. Configure providers in the user's canonical profile, not a gateway copy. For development, run straight from source: `bun packages/gateway/src/main.ts daemon`.

The CLI does not start the daemon: `gajaeway daemon run` prints the launcher command, and `gajaeway status` requires the daemon socket. Run the launcher under your service manager (systemd/launchd/container supervisor), keep its state directory private, and stop the service before an offline restore.

### macOS launchd deployment pitfalls (learned live)

- **TCC-protected paths hang launchd children silently.** A user LaunchAgent has no Desktop/Documents/Downloads consent, so a `WorkingDirectory` inside `~/Documents` makes Bun spin forever in `getcwd`, and a `gjc` binary (or symlink target) under a protected folder blocks its children inside `dyld` at `open()`. Deploy the repo clone, the `gjc` binary, and every state directory outside TCC-protected folders (for example under `~/gajaeway-play/`).
- **gjc sessions remember the cwd they were created with.** Sessions bound while the gateway ran from a protected path keep hanging after the move; bump each origin with `/new` so fresh sessions bind under the new working directory.
- **Model API keys are env-delivered.** gjc providers resolve `apiKeyEnv` names from the daemon's environment; a launchd job does not inherit your shell. Put the required key variables in the plist `EnvironmentVariables` and `chmod 600` the plist. A missing key fails every turn with `401 Invalid API key` — visible in the daemon log since non-protocol request failures are logged there.

## Configuration and credentials

`$GAJAEWAY_HOME/config.json` is JSON with this schema summary:

```json
{
  "schemaVersion": 1,
  "logVerbosity": "info",
  "socketPath": "/absolute/path/gateway.sock",
  "dbPath": "/absolute/path/gateway.db",
  "credentials": { "discord": { "credentialFile": "/absolute/path/discord-token" } },
  "channels": { "channel-id": { "engagement": "open", "audience": "human-only" } },
  "model": { "preset": "codex-medium" },
  "stallTimeoutMs": 120000,
  "work": { "maxLanes": 8, "idleRetireMs": 21600000 },
  "mentionAllowlist": ["owner-author-id"],
  "webhook": { "bind": "127.0.0.1", "port": 8080, "exposeNonLoopback": false },
  "watcherRoots": ["/absolute/path"],
  "scriptRoot": "/absolute/path",
  "monitorContextFailureRollThreshold": 2
}
```

Use schema version 1 only. Each credential is a file reference, never an inline secret or environment fallback; a credential file may be referenced by exactly one configured credential. Create secret files with restrictive ownership and mode, keep them outside version control, and rotate by replacing the file and restarting the service.

`model` accepts either a gjc model selector string such as `"openai/gpt-5.2"` or a preset object such as `{ "preset": "codex-medium" }`. The gateway applies it through the persistent session’s authenticated `model.set` control; presets are resolved by gjc from its merged built-in and `~/.gjc/agent/models.yml` profile catalog, so gjc retains its native availability checks, retry budgets, sticky selection, and fallback-chain behavior.

`work.maxLanes` (integer 1–256, default 8) caps bound worker lanes. `work.idleRetireMs` (integer 60000–604800000 milliseconds, default 21600000 / 6 hours) sets the idle retirement threshold checked by the 60-second sweep; terminal `done`/`aborted` lane jobs are also retired. Open attempts are never retired. Both settings are restart-required: edit `config.json` and restart the gateway; a config reload does not apply them.

For delegated long work, use `gajaeway work start <name> --cwd "<absolute_worktree_path>" "<task>"`, observe with `gajaeway work status <name>`, and send additional input to its open attempt with `gajaeway work steer <name> "<input>"`. Start returns an accepted receipt, not completion. Status is read-only: it does not bind, settle, refresh activity, or clear a hold. Steer never rebinds or starts a successor; `work steer acceptance uncertain` must not trigger an automatic retry with a new reference. An open attempt refuses another start/run even with `--resume`; steer or wait instead. `gajaeway work run` remains synchronous and response-only, never sending completion notifications. A caller timeout (`work wait timed out; attempt remains observable`), disconnect, or shutdown ends only the wait, not the worker attempt.

Only start accepts `--notify <originKey>`: the target is snapshotted once from the explicit target, otherwise the configured default owner, otherwise no target. Run and historical attempts never notify, including after recovery; run rejects notify. Settlement and pending notification admission commit atomically into the existing delivery ledger before broadcast. Restart uses existing ledger replay with its retention and ambiguous-duplicate warnings, not external exactly-once delivery. Enqueued does not mean acknowledged; durable suppressed/no-target decisions and delivery identities prevent recreation after ledger pruning.

Notices use `[lane <name>] completed|failed|attempt_ended: …`. Uncertainty or missing receipts say `attempt_ended` plus a safe reason while the true end state and operator hold remain intact; cancellation and incomplete outcomes are not success. A completed operation with unprovable output says `completed: output_unavailable`; synchronous run reports `work output unavailable` without changing known completion. An uncertain/incomplete outcome with unavailable output includes both reasons, never raw SDK exception text. A terminal reported with `receiptState=missing` keeps being read within the output budget. If a late final body proves present, the attempt settles `completed` once, with no resend. `attempt_ended: terminal_missing_receipt: final_response_missing opRef=<opRef>` means the SDK recorded no final response text for that op. It does not mean the worker's commits or PRs were lost. Verify the artifacts in the repository, then continue with `work start <name> --resume` or retire the lane. Do not edit the database or restart the finished work. Silence suppresses a notice only when proven from this attempt's complete original final content before prefixing/truncation; a summary token, neighboring reply, intermediate text, timestamp alone, or completed pagination cannot prove final output or silence. Silence evidence is durable before dependent tail checkpoints, so later missing output cannot resurrect a notice. Missing proof means unavailable, not assumed silence. Output retrieval is limited to three durable post-terminal reads (immediately, then after 1 second and 5 seconds), ending earlier for definitive unavailability or invalid attribution; this is not a worker-duration limit.

The ActionGuard notice is prompt guidance to use owned start/status/steer lanes and avoid raw `gjc` launches, not a new enforcement hook. The gateway shares the user's broker but controls only positively proven gateway-created sessions under the active database authority. A visible session ID alone is not ownership. It never adopts unrelated sessions, runs global GC, or uses `session.delete`. Host skill/script replacement, deployment, and process cleanup remain separate operational work.

`monitorContextFailureRollThreshold` (1–20, default 2) is the monitor safety net, not a turn ceiling: native gjc auto-compaction keeps monitor sessions bounded, and a monitor that keeps answering is never rolled however many turns it takes. The epoch rolls only after this many **consecutive** context-class authoring failures (empty response, context-length rejection, zero-token completion) attributable to the current session AND a native-compaction request that came back `unavailable`/`failed`/`skipped`; the new session's first prompt then carries a digest of the monitor's instruction and its recent authored notes. Any healthy answer resets the streak, and failures that are executor-class or protocol-class (malformed or off-contract answers), or that come from a reconcile-replayed stale event or a dead epoch, never count. Executor-class failures report a sub-kind: `aside_timeout` and other worker/tool/lock failures, and `orphaned_executor` for a child process killed on the wrapper's timeout while the external daemon's job kept running — that one means "reclaim the external executor", never "roll the session". See `docs/monitors.md`.

`turnTimeoutMs` was removed with the persistent-session cutover. Configuration containing it is rejected; use `stallTimeoutMs` for an alert-only tail silence threshold. It never kills or replaces a running SDK operation.

### Automatic reset after a recognized provider failure

A persona turn rejected with the exact provider HTTP 400 `Unknown parameter: 'input[N].status'.` or a recognized context-exhaustion rejection can retire its session for **the next message**. The gateway corroborates the failure against bounded, private, current-session transcript evidence; a generic `Prompt submission failed`, high context occupancy alone, authentication/rate-limit error, or quoted error in chat/tool output is not reset authority. Missing, malformed, ambiguous, or stale evidence leaves the ordinary failure path unchanged.

The failed trigger is settled once and **never automatically re-executed**: the installed SDK does not attest that the whole operation had no external effects. This remains true when the turn already executed tools, sent partial output, or accepted steers. Pending messages and uncertain steer attribution are preserved; automatic reset does not use `/new`'s pending-message discard behavior. Reset uses existing native SDK session operations through `SessionPort`, not a private broker, shared-daemon replacement, or resubmission of old accepted work. Only the currently owned failed session under the active authority may advance its epoch; retired failures cannot reset a replacement session. Unrelated user sessions and global configuration remain untouched.

The reset budget is durable: one consecutive automatic reset per origin, with per-trigger deduplication across restarts. Another failure on the fresh session does not start a reset loop. A healthy completed reply or explicit operator `/new` permits a later automatic reset. This is recovery from an unusable conversation, not a serializer fix: repeated `input[N].status` rejection requires correcting the upstream gjc provider request format, not repeated session creation. No preset change, deployment, or GC deletion is part of automatic reset.

### Steer delivery is not session replacement authority

A refused steer remains pending until the current turn finishes, then is sent on the same session. Missing, mismatched, or contradictory acceptance receipts remain uncertain; only a receipt carrying the exact requested `clientRef` can prove acceptance. A failed steer must never retire a running turn or mint another session. Only explicit operator reset or independently proven, narrowly classified irrecoverable session errors may authorize replacement.

Periodic recovery also visits origins whose only remaining work is an uncertain steer on an already-terminal turn. After gateway restart, it resolves that original session/op/clientRef without requiring another user message. A confirmed acceptance consumes the held row without a new send; a confirmed refusal releases it for the next turn on the same session. Unresolved outcomes remain held rather than being duplicated.

### Relay-owned turns

Every bound persona session has one resident `gjc sdk serve --stdio --session <id>` relay; it is the turn's only transport. The gateway writes SDK frames to its stdin (`hello`, then `turn.prompt` / `turn.steer` as `control_request`, `turn.result` as `query_request`) and, because the prompt was submitted on that connection, the host streams the turn's own content back on stdout: `agent_start`, every assistant `message_end` (delivered to the channel as mid-work speech the moment it arrives), `tool_execution_*` (presence), and `agent_end`, all stamped with the turn's `commandId`/`turnId`. There is no `gjc sdk session tail` polling, no cursor, no transcript re-read for a live turn, and no text de-duplication: a healthy turn logs no `tail_poll`, `tail_error` or `tail_frame_foreign` lines. A relay that dies mid-turn logs `tail_relay_lost session=… opRef=…` then `recovery_hold … reason=relay_lost_mid_turn`; the remaining mid-work speech of that turn is lost (host content is best-effort), the handle reopens with backoff, and the turn settles from `turn.result` plus its original-result body on a 250 ms–5 s recheck. `tail_stream_dead session=… reopens=6` means six consecutive sub-5-second relay deaths: check that `gjc sdk serve --stdio --session <id>` runs by hand under the gateway's environment before anything else.

Rollback path from this transport: the pre-relay binaries are kept beside the deployed ones as `<name>.prev-relay-<UTC stamp>` (`~/gajaeway-play/bin/` on the macOS host, `~/Workspace/gajae-way/dist/` on the systemd host). Follow the persistent-session binary rollback below; the `broker_tail_cursors` table is still present, so a pre-relay binary reads its (stale, harmless) cursors and resumes polling without a schema change.

## Adapters and engagement

Create separate credential files for Discord and Telegram tokens, then reference them as `credentials.discord` and `credentials.telegram`. Slack needs two: a bot token (`xoxb-…`) and a Socket Mode app-level token (`xapp-…`), referenced from `adapter-slack.json` as `botTokenFile` and `appTokenFile`; gateway channel policies for Slack are keyed `slack:<channelId>`. See the Slack section of the deployment guide for the app manifest (scopes, events, slash commands). Discord channel engagement is exactly `open`, `mention-open`, or `closed`; select `all`, `human-only`, or `bot-only` independently with `audience`. Omitted audience is safely `human-only`. `mention-open` wakes only for a real mention or a native reply to this bot, while `closed` also requires owner/allowlist authorization. Parent channel policy applies to Discord threads unless a thread entry overrides it. Verify adapter connectivity from its service logs and use `gajaeway sessions list` to confirm accepted traffic.

### Emoji reactions

Reactions work in both directions and are never turns. An inbound reaction (added or removed) is recorded as conversation context for the next engaged turn; it never wakes the persona on its own. Outbound, the persona can answer with a reaction instead of a message, and a reaction is settled through the same delivery ledger as a message: once a reaction has been *attempted*, its outcome is a ledger row, so a failure shows up in `gajaeway status` pending counts rather than silently.

A reaction refused *before* it is attempted is a different case and deliberately does not appear there. The gateway refuses one when the platform cannot express the emoji, when the per-turn or per-message cap is already spent, or when the same emoji is already on that message; no ledger row is created, so there is nothing for `gajaeway status` to show. Those are reported to the caller as an error on `chat.react`, and written to the daemon log as a `reaction skipped` or `reaction rejected` line when the persona asked for them with a `[REACT:…]` token. If a reaction seems to have gone missing, read the daemon log first and the pending counts second.

Operator prerequisites, per platform:

- **Both:** the adapter's own presence markers (⏳ 🔧 💭 ✍️, clock faces, effort digits) are reactions it places on the message it is answering and removes when the reply lands; they are disjoint from the persona's allowlist and are filtered out of inbound engagement.
- **Discord:** the adapter requests the `GuildMessageReactions` and `DirectMessageReactions` intents plus message/reaction/user partials. Without them Discord dispatches no reaction events at all, and reactions on messages posted before the last restart are dropped. Neither intent is privileged, so no portal approval is needed. Custom guild emoji are matched by name against the bounded allowlist and fall back to the unicode equivalent when the guild does not own one.
- **Slack:** the bot token needs `reactions:read` and `reactions:write`, and the app must subscribe to `reaction_added` / `reaction_removed`. Slack reacts by emoji *name*, so every allowlist entry is mapped (`👍` → `+1`, `✅` → `white_check_mark`, `🦞` → `lobster`) and the whole allowlist is deliverable. An `already_reacted` answer counts as delivered; any other API error is a definitive delivery failure. A reaction on a threaded message is recorded against the parent channel origin because Slack's reaction event carries no `thread_ts`.
- **Telegram:** inbound reactions require the bot to be an **administrator** in the chat, and the adapter must list `message_reaction` in `allowed_updates` (it does). Telegram never reports reactions set by bots. Outbound, Telegram accepts only its own 73 server-provided reaction emoji, so the three allowlist entries it cannot express (`✅`, `❌`, `🦞`) are never offered to the persona on a Telegram origin and are refused up front by `chat.react` with an error naming what Telegram does accept. If one reaches the adapter anyway — a redelivery recorded by an older build, say — it is reported as a definitive delivery failure rather than converted into a text message; that path is a backstop, not the normal one.

## Monitors

```sh
gajaeway monitors add --json '<MonitorSpec JSON>'
gajaeway monitors list
gajaeway monitors inspect <monitor-id>
gajaeway monitors test <monitor-id> --type changed --payload '{"source":"manual"}'
```

Declare event types at creation time. Use `inspect` to review recent event stages before retrying a trigger. Webhooks are dangerous when exposed beyond loopback: set an explicit non-loopback bind only when required, put it behind authenticated ingress, and require authentication at that ingress. Do not expose an unauthenticated webhook directly to the Internet.

## Memory

```sh
gajaeway memory audit
gajaeway memory search 'query terms'
```

Memory changes are durable intents, committed in the memory Git repository, and recorded in `memory-receipts.jsonl`. Audit before manual repair; search returns bounded excerpts. Investigate quarantined mutations rather than editing receipts or Git history to hide them.

## Runtime cycle

`gajaeway ops cycle` projects one operator-readable snapshot of where every runtime cycle stands: aggregate phase, per-origin session identity with epoch, durable inbound queue depth, delivery settlement, memory closure, and monitor settlement:

```sh
gajaeway ops cycle          # human-readable; exits 1 when any gate is present
gajaeway ops cycle --json   # typed OpsCycleResult for scripting; same exit contract
```

Phases are `idle`, `dispatching` (turn work accepted or claimed from the durable queue), `delivering` (unsettled ledger deliveries), `draining` (memory closure in flight or durable unsettled intents), and `degraded`. A session shown as `(rebinding)` has a bumped epoch with no bound session yet — the next turn rebinds it.

The command is fail-closed by contract. `gates:` names reasons the cycle is not healthy, including `stale_session_identity`, `delivery_settlement_unknown`, `memory_closure_blocked`, `monitor_settlement_failed`, `monitor_authoring_lost` (in the last 24h, two or more monitor event types whose latest slot exhausted retries with no authored output, or one type with two consecutive such slots — listed per type under `monitor authoring lost:`; a delivered slot of that type clears it; inspect `monitor_failures.detail` for the cause), `lane_capacity_exhausted`, and `inbound_starved` (a replayable pending trigger older than ten minutes with nothing in flight — a stuck actor, never a busy one); any gate forces exit code 1, so automation can never read a degraded runtime as idle. `lanes: {active, max}` reports bound worker-lane capacity. An unavailable daemon is a connection error, not a healthy report. The projection is read-only; durable SQLite rows and the delivery ledger remain the authority.

## Backup and restore drill

With the daemon running, take an online SQLite backup and validate it:

```sh
gajaeway ops integrity
gajaeway ops backup /absolute/backup/gateway.db
gajaeway ops redeliver <deliveryId>
gajaeway ops redeliver --since 2026-09-23T10:00:00.000Z
```

For restore, stop the service first. The CLI refuses restore while the gateway socket exists. It opens the backup read-only, refuses an empty database, and requires `PRAGMA integrity_check` to answer `ok` (the same acceptance the gateway applies at boot; a WAL-mode backup may gain empty `-wal`/`-shm` sidecars beside it, which are safe to delete; restore installs only the backup file itself, so a hand-made WAL-mode backup must be checkpointed first). It then resolves the live database from the same configuration the gateway reads (`dbPath` from `$GAJAEWAY_HOME/config.json`, otherwise `$GAJAEWAY_HOME/gateway.db`), copies that database to `<db>.pre-restore-<timestamp>`, then copies the backup over it. An unreadable or malformed `config.json` refuses the restore rather than guessing:

```sh
# stop the service and confirm its gateway.sock is gone
gajaeway ops restore /absolute/backup/gateway.db
# restart the service
gajaeway ops integrity
```

Practice this sequence on a disposable home directory before relying on it during an incident.

### Explicit broker-authority cutover

Changing to the global-user profile is an authority change, not an ordinary restart or automatic adoption of old private sessions. Perform cutover before any recovery against the target broker:

1. Stop external intake. While the existing gateway is running, take and validate an online backup with the integrity and backup commands above; retain it with the old binary and profile identity. Do not copy a live SQLite database as a substitute for online backup.
2. Stop the gateway with the host's service manager and disable automatic restart for the entire inspect/apply window; confirm its PID and socket are gone. On the systemd host reached on SSH port 24, use the systemd user service, not a macOS launchd command. Allow at least 30 seconds for shutdown; preserve `KillMode=process` wherever GJC hosts share the gateway cgroup. Do not stop or kill the shared user broker.
3. From the checkout with Bun available, inspect the stopped database using the administrative script below. `ABS` is the absolute gateway home and `CANONICAL` is the verified user's canonical absolute agent-directory path, not a symlink alias. Record `oldAuthority`, `targetAuthority`, and the census. Inspection is a consistent read, not exclusive ownership or proof that old work stopped.

   ```sh
   bun scripts/gjc-authority-cutover.ts inspect --home ABS --agent-dir CANONICAL
   ```

4. Apply with the same paths, the exact inspected `oldAuthority` JSON, an operator evidence reference, and a unique absolute backup destination whose canonical parent already exists. The example below applies only when inspection returned `oldAuthority: null`; otherwise replace `'null'` with the inspected JSON object, shell-quoted as one argument. `UNIQUEABS` must not exist and must not name the database or its sidecars.

   ```sh
   bun scripts/gjc-authority-cutover.ts apply --home ABS --agent-dir CANONICAL --expected-authority 'null' --quarantine --evidence 'operatorreference' --backup UNIQUEABS
   ```

   Both modes refuse a live gateway PID or socket. Apply acquires the same kernel-backed gateway-home lease used by boot, rechecks the stopped state and expected authority, and creates and integrity-checks a non-overwriting backup before opening the database for migration. Retain the returned backup receipt, census, and snapshot ID. A refusal is not permission to delete PID/socket records or bypass the lease.

   The transaction preserves immutable historical snapshots and quarantine records, retires old session identities, advances origin epochs, and installs the single active target authority. It neither controls old broker sessions nor settles or replays their work. Accepted or unproven old work remains non-replayable; old private held lanes do not automatically become global-user lanes. Old worker names remain reserved: use new names for new work, not `--resume`, a no-op run, or rewritten session IDs. The cutover does not authorize global configuration restore, host reaping, or session deletion.
5. Re-enable managed startup only after apply succeeds and the service's explicit `GJC_EXECUTABLE` matches the normal user's verified `command -v gjc`, with the same canonical user profile and agent directory. Start recovery only under that target authority. Verify a newly gateway-created session has exactly the same ID in the gateway and the normal user's SDK; compare unrelated sessions and configuration fingerprints with the pre-cutover record, and verify the same user daemon survives a subsequent gateway restart without resend of accepted work.

The script is the administrative caller of internal `inspectBrokerAuthority()` and `cutoverBrokerAuthority()` storage APIs; those API names are not gateway CLI verbs. The current deployment blocker is that SDK global model controls can modify the user's configuration. Successful cutover alone does not clear it: verification must prove configuration remains unchanged through session creation, model controls, and restart before deployment is accepted. Do not compensate with automatic global configuration restore. These are required observations, not a claim that production deployment or service verification has completed.

### Persistent-session binary rollback

A code revert is safe only after the current gateway has reconciled all current and retired turns. Stop intake, wait until `SELECT COUNT(*) FROM inbound_messages WHERE turn_role='trigger' AND turn_state IN ('bound','accepted')` is zero, verify `gajaeway ops integrity`, then stop the gateway in order. Shutdown releases gateway-home ownership and its own SDK connections, not the shared GJC daemon. Quarantined work is preserved evidence, not proof that an old operation stopped. Never remove old broker directories or rewrite authority to force a rollback. A pre-schema-19 binary needs the matching historical configuration and database backup; the current build rejects `settleWindowMs` and `debounceMs`. Restore only a binary/database/profile combination whose authority is understood, then verify a fresh positively owned session and its delivery. Refuse rollback if quiescence or authority cannot be proved.
### Persistent-session schema rollback

Do not down-mark a live database. Schema 19 rebuilt `inbound_messages` (the batch columns are gone; `turn_role`, `turn_epoch`, `turn_state`, `turn_op_ref`, `bound_session_id`, `dispatched_at` and `terminal_delivery_id` remain), so there is no in-place down-marker: rolling back to a pre-19 binary means restoring the backup taken before the upgrade. First stop external intake while the current gateway is still running, reconcile every current and retired turn until the nonterminal-trigger count above is zero, verify integrity, and only then swap binary and database together. If quiescence cannot be proved, refuse the rollback.

## Crash recovery

On adapter negotiation, and every 15 seconds while an adapter remains connected, the delivery ledger redelivers due unsettled output. Ambiguous prior delivery is visibly duplicate-labeled, so adapters must preserve that label. Five definitive failures or age beyond 24 hours expire an unsettled row; the gateway logs expiry, exposes expired counts and recent ids/origins in `gateway.status`, and sends an owner-target notice when configured. Requeue one eligible row with `gajaeway ops redeliver <deliveryId>`, or requeue rows updated since a timestamp with `gajaeway ops redeliver --since <iso>`. Confirmed rows are never requeued. Memory closure resumes durable intents and receipts successful Git closure; irrecoverable intent work is quarantined. Monitor reconciliation resumes admitted, dispatched, or failed events and repairs authored events whose memory closure is missing.

Worker recovery independently resumes observation of the saved operation/session identity before admitting conflicting worker mutations. It never resends the prompt, binds a replacement, or requires a no-op run to reconcile an open attempt. Proven-live `unknown` remains observable without settlement or overlap; it proves neither acceptance nor completion. `work send acceptance uncertain` likewise requires observation, not resubmission. Trusted terminal evidence can finish saved reconciliation without live reattachment. Without trusted terminal proof, unrecoverable dead/disowned/indeterminate authority is closed locally as `terminal_uncertain` and held for the operator; the local end timestamp is not proof of worker stoppage. Transient transport failure does not invent a worker deadline. Repository progress cannot clear sticky uncertainty/failure holds; `--resume` is an explicit continuation choice only after the prior attempt is reconciled, not a way around an open attempt.

## Troubleshooting

- **Socket missing:** start the out-of-band service and verify `socketPath`, directory permissions, and service logs.
- **Newer-schema refusal:** do not downgrade against that database. Restore a compatible backup or run a gateway that supports its schema.
- **Quarantined mutation:** inspect the intent payload, memory repository state, and Git error; repair the source condition, then use the documented recovery workflow rather than deleting the evidence.
- **Backup failure:** supply an absolute target path whose parent directory already exists; never target the live `gateway.db`.
- **Every turn on one origin fails with the same gjc `api_error` (for example "cannot restore Claude OAuth MCP tool alias"):** the persistent broker-hosted session may be poisoned. Send `/new` to create a fresh epoch; prior in-session context is lost by design. The daemon log carries the exact error and the conversation receives the `[turn failed]` notice.
- **Repeated structured recovery signals:** `recovery_hold` and `retired_hold` retain a durable turn; `stall_alert originKey=… sessionId=… silentMs=…` is alert-only. Broker-generation changes reflect observed GJC runtime changes, not permission for the gateway to replace the daemon. None of these signals permits a blind resend or abort. Within the same proven authority, use `/new` only to establish a fresh epoch for later traffic; accepted old turns retain their original recovery identity and remain fenced until status/tail proves a terminal result. `steer_failed … action=recover` means reconcile the current turn, not replace its session: an authoritative refusal leaves the message pending until that turn ends, then sends it on the same session. Ambiguous receipts and transport failures (`steer_ambiguous … action=replay`, `steer_hold … reason=transport_torn`) require observation and reconciliation with the same `clientRef`, never a new send or reference. Steer failures never authorize replacement, an epoch bump, or retirement; replacement requires explicit operator `/new` (or `/reset`) or a separate, narrowly proven irrecoverable session condition, not an inference from failed steering. Historical `session_rebound_after_steer_failure` logs identify removed unsafe behavior, not the current recovery policy.
- **Unaccepted retired turns versus authority quarantine:** within the same proven authority, independently retired turns whose sends never landed (`bound` row, op `unknown`, session dead or router-disowned) are released like current ones: `recovery_requeue_unaccepted … reason=router_disowned|retired_router_disowned|unknown_op_on_dead_session`, the trigger re-enters the queue under the current epoch (steered into a running replacement turn, or sent next), and the old lifecycle emits its final `chat.progress` so adapters stop showing "working…". A `retired_hold … reason=stall` that repeats every sweep for such a proven-unaccepted turn is a bug, not an operator hold. This rule never releases accepted or unproven work across an authority cutover: quarantined old turns and held lanes are never requeued or adopted by the global broker. Inspect the saved authority and cutover evidence before attempting recovery.
- **`lane_capacity` on work.start/work.run / `lane_capacity_exhausted` in ops.cycle:** read `gajaeway work jobs` (one line per lane: the name `work retire` accepts, job state, `session_id`, `last_activity_at`), inspect `gajaeway work status <name>`, then retire an eligible idle lane with `gajaeway work retire <name>`. A refusal names why: an open attempt, a corrupt job record (repair `lane_jobs.record_json` at its source), an attempt ended without trusted broker terminal/dead-session proof, or `session.close failed and the session is not proven gone` (the broker is unreachable or the host is still alive; retry after the broker recovers, never delete sessions by hand). Open attempts recover in the background after restart: do not submit a no-op run or use `--resume` to force them closed. Read-only status does not itself settle the attempt. A `terminal_uncertain` end timestamp alone cannot release the lane. `lane_close_failed … action=retained` is the close refusal in the log; `action=session_gone` means the binding was cleared because liveness proved the host dead. Raise `work.maxLanes` in `config.json` only with a gateway restart; it is restart-required, not reloadable. `lane_retired … reason=idle|job_done|job_aborted` lines are the 60-second sweep working as designed; open attempts remain excluded, and settlement refreshes the ordinary idle threshold.
- **Session bootstrap remains pending:** inspect `session.list` or `ops.cycle`. A pending projection means the current epoch has not completed a terminal successful turn yet; pre-success failures deliberately retry it. Repeated attempts carry the same origin+epoch bootstrap ID. Do not mark it complete manually or copy source bodies into the database. Repair unreadable memory files or rejected links at their source. In public/group channels, associated channel/project/task/handoff documents need an exact full `origin:`/`origin-key:` match and `bootstrap-safe: public` (or `bootstrap-visibility: public`); bare conversation IDs never qualify. Daily sections are admitted only when every stable-origin declaration is well formed and resolves to one consistent current-origin key. The configured `memory/` root may itself be a symlink, but every followed target must remain beneath that resolved memory root.
- **`inbound_starved` in ops.cycle / pending grows with `inflight=0` / every origin logs `persona actor … failed: session tail failed: session_unavailable` / `/new` replies but nothing follows:** the persona actor is bound to a session the broker has disowned and cannot progress. Since the 2026-09-15 fix the actor releases the turn itself (`recovery_requeue_unaccepted … reason=router_disowned`) and requeues the trigger under the current epoch; if you still see the actor line repeat every sweep on the current build, that is a new bug, not an operator hold. Do not hand-edit `inbound_messages`. Confirm the broker is live first: `python3 -c 'import json,os;d=json.load(open("$HOME/.gjc/agent/sdk/broker.json"));os.kill(d["pid"],0);print("alive",d["url"])'` — note that the shell builtin `kill -0` is NOT a reliable liveness probe in every environment; use `ps -p` or `os.kill` from Python.
- **`gateway_exit cause=broker_unreachable code=1 detail=live broker unreachable for …s: …`:** the broker's own discovery record said it was live (fresh heartbeat, pid alive) but this gateway could not reach it for 3 minutes, so the gateway exited on purpose for the service manager to restart it (#246). While such an outage lasts, `service_alive` carries `broker_unavailable_for=<s> reason=<cause>`. A dead or absent broker never triggers this exit. It stays at `observing without repair: discovery pid … is dead` / `discovery absent`, because restarting the gateway cannot bring the broker back. Fix the broker's lifecycle instead. If these exits repeat, the `detail` reason is the diagnosis.
- **`persona_bind_failed … attempts=N` with `session_create_epoch_rotated … reason=poisoned_create_key` on every attempt and the epoch column in the thousands:** an earlier build rotated the origin epoch on any `session.create` failure, including `broker_unavailable`. The current build rotates only on a classified rebindable code (`resource_gone`, `spawn_failed`, `terminal_uncertain`, `idempotency_conflict`, …) and retries the same key otherwise. Epochs already burned are harmless; nothing needs resetting. If rotations continue on the current build the log line now carries `code=…` — that code is the diagnosis.
- **`tail_error … request timed out` / `tail_stream_reopen` on every session and `gjc sdk session tail` measures 10s+ by hand:** the event stream relay is not running and every tail fell back to CLI polling, which saturates the four-slot command queue. `gjc sdk serve` takes no `--agent-dir` flag (exit 2 + usage on gjc 0.16.6); the gateway binds it by environment only, and boot preflight now fails closed with `stream relay rejected its argv` if the relay contract regresses. Verify with `pgrep -fa 'sdk serve --stdio'` — one relay per bound session is healthy; zero while sessions are bound is the fault.
- **`config.json is unreadable (...); refusing to start on defaults`:** the file exists but cannot be read (permissions, a directory in its place, or a symlink whose target is missing). The daemon exits non-zero rather than booting on defaults, because defaults would drop `mentionAllowlist` and open a mention-gated room. Fix the file, then start again; a reload in a running daemon keeps the previous config and reports the same diagnostic.
