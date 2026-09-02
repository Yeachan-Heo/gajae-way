# Gajaeway v1 operator runbook

## Install and run

For production, compile the one standalone binary — no source checkout, `node_modules`, or Bun install is needed on the host:

```sh
bun run build   # emits dist/gajaeway
dist/gajaeway daemon run
```

The external `gjc` binary remains a runtime dependency on `PATH`. The composite daemon owns one private gjc agent directory (`<home>/broker/<instanceId>/agent`); gjc's own daemon for that directory hosts the persistent sessions and is auto-started by the gateway's first `gjc sdk` probe. The supervisor observes daemon health and publishes a new generation when it recovers; it never spawns or kills the daemon. On every start the private agent directory is re-seeded from the operator SSOT `~/.gjc/agent` (`models.yml`, `model-presets/`, and `config.yml` operator keys; `steeringMode: all` / `interruptMode: wait` are pinned on top). Edit provider/model configuration in `~/.gjc/agent` only — never in the private directory, which is overwritten on boot. A missing SSOT `models.yml` fails boot. Before seeding, boot reaps everything the previous incarnation left in that private directory: gjc daemon/host/relay processes bound to it and lock tombstones from spawn races (`broker_reaped` log line). The operator's own `~/.gjc/agent` daemon is never touched, so restarting the gateway is always safe. For development, run straight from source: `bun packages/gateway/src/main.ts daemon`.

For development, run the composite daemon from source:

```sh
bun packages/gajaeway/src/main.ts daemon run
```

Client verbs such as `gajaeway status` connect to the resident daemon. Run `gajaeway daemon run` under your service manager (systemd, launchd, or a container supervisor), keep its state directory private, and stop the single service before an offline restore.

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
  "credentials": {
    "discord": { "credentialFile": "/absolute/path/discord-token" },
    "discordVoice": { "credentialFile": "/absolute/path/discord-voice-key" },
    "telegram": { "credentialFile": "/absolute/path/telegram-token" }
  },
  "adapters": {
    "discord": { "intents": [1, 512], "voice": { "languageCode": "ko", "voiceId": "voice-id" } },
    "telegram": {}
  },
  "channels": {
    "discord:channel-id": { "engagement": "open", "settleWindowMs": 500 },
    "telegram:chat-id": { "engagement": "open" }
  },
  "model": { "preset": "codex-medium" },
  "settleWindowMs": 1000,
  "stallTimeoutMs": 120000,
  "mentionAllowlist": ["owner-author-id"],
  "webhook": { "bind": "127.0.0.1", "port": 8080, "exposeNonLoopback": false },
  "watcherRoots": ["/absolute/path"],
  "scriptRoot": "/absolute/path",
  "monitorContextFailureRollThreshold": 2
}
```

Use schema version 1 only. Each credential is a file reference, never an inline secret or environment fallback; a credential file may be referenced by exactly one configured credential. Create secret files with restrictive ownership and mode, keep them outside version control, and rotate by replacing the file and restarting the single service. Presence of `adapters.discord` or `adapters.telegram` enables that adapter. `adapters` and `credentials` are restart-required, as is Discord recovery-channel enumeration: it is derived at daemon start from `discord:<id>` keys and Discord bare IDs. Use prefixed keys for new configuration; bare IDs remain Discord-only compatibility keys.

`model` accepts either a gjc model selector string such as `"openai/gpt-5.2"` or a preset object such as `{ "preset": "codex-medium" }`. The gateway applies it through the persistent session’s authenticated `model.set` control; presets are resolved by gjc from its merged built-in and `~/.gjc/agent/models.yml` profile catalog, so gjc retains its native availability checks, retry budgets, sticky selection, and fallback-chain behavior.

`monitorContextFailureRollThreshold` (1–20, default 2) is the monitor safety net, not a turn ceiling: native gjc auto-compaction keeps monitor sessions bounded, and a monitor that keeps answering is never rolled however many turns it takes. The epoch rolls only after this many **consecutive** context-class authoring failures (empty response, context-length rejection, zero-token completion) attributable to the current session AND a native-compaction request that came back `unavailable`/`failed`/`skipped`; the new session's first prompt then carries a digest of the monitor's instruction and its recent authored notes. Any healthy answer resets the streak, and failures that are executor-class or protocol-class (malformed or off-contract answers), or that come from a reconcile-replayed stale event or a dead epoch, never count. Executor-class failures report a sub-kind: `aside_timeout` and other worker/tool/lock failures, and `orphaned_executor` for a child process killed on the wrapper's timeout while the external daemon's job kept running — that one means "reclaim the external executor", never "roll the session". See `docs/monitors.md`.

`turnTimeoutMs` was removed with the persistent-session cutover. Configuration containing it is rejected; use `stallTimeoutMs` for an alert-only tail silence threshold. It never kills or replaces a running SDK operation.

## Adapters and engagement

Create separate credential files for Discord and Telegram tokens (and a Discord voice key when voice is enabled), then reference them through `credentials` and enable platforms through `adapters`. Configure group/channel engagement with `channels["discord:<id>"]` or `channels["telegram:<id>"]`; unconfigured group traffic remains mention-gated. Direct messages engage normally. Adapters report platform facts, while the gateway applies the current engagement gate. Verify accepted traffic with `gajaeway sessions list`.

### Emoji reactions

Reactions work in both directions and are never turns. An inbound reaction (added or removed) is recorded as conversation context for the next engaged turn; it never wakes the persona on its own. Outbound, the persona can answer with a reaction instead of a message, and a reaction is settled through the same delivery ledger as a message: once a reaction has been *attempted*, its outcome is a ledger row, so a failure shows up in `gajaeway status` pending counts rather than silently.

A reaction refused *before* it is attempted is a different case and deliberately does not appear there. The gateway refuses one when the platform cannot express the emoji, when the per-turn or per-message cap is already spent, or when the same emoji is already on that message; no ledger row is created, so there is nothing for `gajaeway status` to show. Those are reported to the caller as an error on `chat.react`, and written to the daemon log as a `reaction skipped` or `reaction rejected` line when the persona asked for them with a `[REACT:…]` token. If a reaction seems to have gone missing, read the daemon log first and the pending counts second.

Operator prerequisites, per platform:

- **Discord:** the adapter requests the `GuildMessageReactions` and `DirectMessageReactions` intents plus message/reaction/user partials. Without them Discord dispatches no reaction events at all, and reactions on messages posted before the last restart are dropped. Neither intent is privileged, so no portal approval is needed. Custom guild emoji are matched by name against the bounded allowlist and fall back to the unicode equivalent when the guild does not own one.
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

The command is fail-closed by contract. `gates:` names every reason the cycle is not healthy — `stale_session_identity`, `delivery_settlement_unknown`, `memory_closure_blocked`, `monitor_settlement_failed` — and any gate forces exit code 1, so automation can never read a degraded runtime as idle. An unavailable daemon is a connection error, not a healthy report. The projection is read-only; durable SQLite rows and the delivery ledger remain the authority.

## Backup and restore drill

With the daemon running, take an online SQLite backup and validate it:

```sh
gajaeway ops integrity
gajaeway ops backup /absolute/backup/gateway.db
```

For restore, stop the single service first. Confirm both the gateway socket and `$GAJAEWAY_HOME/gajaeway.pid` are gone. The CLI refuses restore while the socket exists. It validates the SQLite header, copies the current database to `gateway.db.pre-restore-<timestamp>`, then copies the backup into `$GAJAEWAY_HOME/gateway.db`:

```sh
# stop dev.gajaeway and confirm gateway.sock and gajaeway.pid are gone
gajaeway ops restore /absolute/backup/gateway.db
# restart dev.gajaeway
gajaeway ops integrity
```

Practice this sequence on a disposable home directory before relying on it during an incident.

### Single-binary rollback

A code revert replaces the one `gajaeway` binary and restarts the one service; it never starts platform components independently. Before a rollback, stop intake, wait until `batch_state IN ('settled','accepted')` is zero, verify `gajaeway ops integrity`, then stop the composite daemon so it releases its broker ownership lock in order (the gjc daemon itself is gjc-owned and is not killed). Start the reverted single-binary release only after any required schema rollback below succeeds, then smoke one adopted session and verify its delivery.

### Persistent-session schema rollback

Do not down-mark a live database. First stop external intake while the current daemon is still running, reconcile every current and retired batch until `batch_state IN ('settled','accepted')` is zero, and verify integrity. Take a backup before the transactional down-marker `DELETE FROM schema_migrations WHERE version = 17;`, then verify the v16 marker before starting the prior single-binary release. Version 17 is the one additive migration that adds the batch-to-session binding used to reattach retired holds after `/new`; it leaves the legacy inbound state column unchanged. If quiescence cannot be proved, refuse the down-step and restore the pre-cutover backup instead.

## Crash recovery

On startup, the delivery ledger redelivers unsettled output. Ambiguous prior delivery is visibly duplicate-labeled, so adapters must preserve that label. Memory closure resumes durable intents and receipts successful Git closure; irrecoverable intent work is quarantined. Monitor reconciliation resumes admitted, dispatched, or failed events and repairs authored events whose memory closure is missing.

## Troubleshooting

### Daemon and adapter log signals

The following grep-stable names identify the composite lifecycle and platform supervision: `gajaeway daemon starting pid=… home=`, `gajaeway daemon ready pid=… home= adapters=[…] admin=…`, `gajaeway daemon stopping reason=…`, `gajaeway daemon stopped status=…`, `gajaeway daemon stop forced`, `boot_aborted phase=…`, and `daemon_lock_reclaimed stale_pid=…`. Local replay is recorded as `local_port_open connection=… replayed=N`.

Adapter lifecycle signals are `adapter_started adapter=… generation=…`, `adapter_failed adapter=… generation=… reason=… restartInMs=…`, `adapter_escalated adapter=… failures=5`, `adapter_disposed_after_stop adapter=… generation=…`, `adapter_stopped adapter=… generation=…`, and `adapter_stop_timeout adapter=…`. Platform-specific signals are `telegram_update_rejected update_id=… code=…`, `telegram_poll_conflict`, and `discord_unrecoverable_close code=…`.

- **Socket missing:** confirm the single `dev.gajaeway` service is running, then verify `socketPath`, directory permissions, and the daemon log.
- **`adapter_escalated adapter=<name> failures=5`:** the fifth consecutive adapter failure exits the daemon with status 1 after four supervised retries. Repair the platform fault. To run degraded while investigating, remove `adapters.<name>` from `config.json`, validate it, and restart the single service.
- **`daemon lock … is unreadable or malformed`:** the lock is fail-closed. Remove `$GAJAEWAY_HOME/gajaeway.pid` by hand only after confirming no gajaeway daemon is running; otherwise correct the live owner or lock problem.
- **`adapter_stop_timeout adapter=<name>`:** the adapter did not stop within its ten-second deadline. Its local port is closed and shutdown continues; investigate the platform client or network call. A complete daemon shutdown has a thirty-second force deadline signalled by `gajaeway daemon stop forced`.
- **Newer-schema refusal:** do not downgrade against that database. Restore a compatible backup or run a daemon that supports its schema.
- **Quarantined mutation:** inspect the intent payload, memory repository state, and Git error; repair the source condition, then use the documented recovery workflow rather than deleting the evidence.
- **Backup failure:** supply an absolute target path whose parent directory already exists; never target the live `gateway.db`.
- **Every turn on one origin fails with the same gjc `api_error`:** the persistent broker-hosted session may be poisoned. Send `/new` to create a fresh epoch; prior in-session context is lost by design. The daemon log carries the exact error and the conversation receives the `[turn failed]` notice.
- **Repeated structured recovery signals:** `recovery_hold` and `retired_hold` retain a durable batch; `stall_alert originKey=… sessionId=… silentMs=…` is alert-only; `broker_restart generation=… backoffMs=…` records a supervised replacement. None permits a blind re-send or abort. Use `/new` only to establish a fresh epoch for later traffic; an accepted old batch remains fenced until status/tail proves a terminal result.
- **Session bootstrap remains pending:** inspect `sessions inspect <originKey-or-index>` or `gajaeway ops cycle`. A pending projection means the current epoch has not completed a terminal successful turn yet; pre-success failures deliberately retry it. Repeated attempts carry the same origin+epoch bootstrap ID. Do not mark it complete manually or copy source bodies into the database. Repair unreadable memory files or rejected links at their source. In public/group channels, associated channel/project/task/handoff documents need an exact full `origin:`/`origin-key:` match and `bootstrap-safe: public` (or `bootstrap-visibility: public`); bare conversation IDs never qualify. Daily sections are admitted only when every stable-origin declaration is well formed and resolves to one consistent current-origin key. The configured `memory/` root may itself be a symlink, but every followed target must remain beneath that resolved memory root.
- **`config.json is unreadable (...); refusing to start on defaults`:** the file exists but cannot be read (permissions, a directory in its place, or a symlink whose target is missing). The daemon exits non-zero rather than booting on defaults. Fix the file, then start the service again; a reload in a running daemon keeps the previous configuration and reports the same diagnostic.
