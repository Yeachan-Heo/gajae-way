# Gajaeway v1 operator runbook

## Install and run

For production, compile standalone binaries — no source checkout, node_modules, or Bun install is needed on the host:

```sh
bun run build   # emits dist/gajaeway-gateway, dist/gajaeway-discord, dist/gajaeway-telegram, dist/gajaeway
dist/gajaeway-gateway daemon
```

The external `gjc` binary remains a runtime dependency on PATH (the gateway spawns it per turn). For development, run straight from source: `bun packages/gateway/src/main.ts daemon`.

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
  "channels": { "channel-id": { "engagement": "open", "debounceMs": 500 } },
  "turnTimeoutMs": 900000,
  "model": { "preset": "codex-medium" },
  "debounceMs": 1000,
  "mentionAllowlist": ["owner-author-id"],
  "handoffTargets": { "gajae-way-dev": { "platform": "discord", "kind": "channel", "conversationId": "channel-id" } },
  "webhook": { "bind": "127.0.0.1", "port": 8080, "exposeNonLoopback": false },
  "watcherRoots": ["/absolute/path"],
  "scriptRoot": "/absolute/path"
}
```

Use schema version 1 only. Each credential is a file reference, never an inline secret or environment fallback; a credential file may be referenced by exactly one configured credential. Create secret files with restrictive ownership and mode, keep them outside version control, and rotate by replacing the file and restarting the service.

`model` accepts either a gjc model selector string such as `"openai/gpt-5.2"` or a preset object such as `{ "preset": "codex-medium" }`. Presets are resolved by gjc from its merged built-in and `~/.gjc/agent/models.yml` profile catalog. A preset's `model_mapping.default` may be an ordered selector array; gajaeway invokes the preset with `--mpreset`, so gjc retains its native availability checks, retry budgets, sticky selection, and fallback-chain behavior instead of the gateway attempting unsafe whole-turn retries.

`handoffTargets` binds the aliases a persona may hand work to: alias -> a full origin. Only declared aliases are offered to the persona in a turn, and an alias whose origin resolves to the current conversation is not offered at all. It is reloadable, so retargeting a room takes effect on the next turn. A `[HANDOFF:<target>]` naming anything else fails loudly in the source channel and wakes no other session.

## Adapters and engagement

Create separate credential files for Discord and Telegram tokens, then reference them as `credentials.discord` and `credentials.telegram`. Configure group/channel engagement with `channels.<conversation-id>.engagement: "open"`; unconfigured group traffic remains mention-gated. Direct messages engage normally. Verify adapter connectivity from its service logs and use `gajaeway sessions list` to confirm accepted traffic.

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

For restore, stop the service first. The CLI refuses restore while the gateway socket exists. It validates the SQLite header, copies the current database to `gateway.db.pre-restore-<timestamp>`, then copies the backup into `$GAJAEWAY_HOME/gateway.db`:

```sh
# stop the service and confirm its gateway.sock is gone
gajaeway ops restore /absolute/backup/gateway.db
# restart the service
gajaeway ops integrity
```

Practice this sequence on a disposable home directory before relying on it during an incident.

## Crash recovery

On startup, the delivery ledger redelivers unsettled output. Ambiguous prior delivery is visibly duplicate-labeled, so adapters must preserve that label. Memory closure resumes durable intents and receipts successful Git closure; irrecoverable intent work is quarantined. Monitor reconciliation resumes admitted, dispatched, or failed events and repairs authored events whose memory closure is missing.

## Troubleshooting

- **Socket missing:** start the out-of-band service and verify `socketPath`, directory permissions, and service logs.
- **Newer-schema refusal:** do not downgrade against that database. Restore a compatible backup or run a gateway that supports its schema.
- **Quarantined mutation:** inspect the intent payload, memory repository state, and Git error; repair the source condition, then use the documented recovery workflow rather than deleting the evidence.
- **Backup failure:** supply an absolute target path whose parent directory already exists; never target the live `gateway.db`.
- **Every turn on one origin fails with the same gjc `api_error` (for example "cannot restore Claude OAuth MCP tool alias"):** the bound gjc session transcript is poisoned and every resume replays the failure. Send `/new` to that conversation to rebind a fresh session; prior in-session context is lost by design. The daemon log carries the exact error; the conversation receives the `[turn failed]` notice.
- **Repeated `gateway session rebind` lines for one origin:** the gateway rebinds automatically when the runtime condemns a session key (`resource_gone`, `spawn_failed`, `terminal_uncertain`, `managed_append_identity_mismatch`). Each line carries `cause=<code>`, the epoch transition, and `lifetime=<N>` — the total this process has ever spent for that origin, which a completed unaided turn does NOT reset. A rising `lifetime` with the consecutive count stuck at `1/3` means a failure alternating with healthy turns: the epoch is still growing, so investigate the cause code rather than waiting for the cap. After three consecutive rebinds the turn fails with `rebind_cap_exceeded` and the conversation is told to send `/new`, which also restores the budget.
- **Session bootstrap remains pending:** inspect `session.list` or `ops.cycle`. A pending projection means the current epoch has not completed a terminal successful turn yet; pre-success failures deliberately retry it. Repeated attempts carry the same origin+epoch bootstrap ID. Do not mark it complete manually or copy source bodies into the database. Repair unreadable memory files or rejected links at their source. In public/group channels, associated channel/project/task/handoff documents need an exact full `origin:`/`origin-key:` match and `bootstrap-safe: public` (or `bootstrap-visibility: public`); bare conversation IDs never qualify. Daily sections are admitted only when every stable-origin declaration is well formed and resolves to one consistent current-origin key. The configured `memory/` root may itself be a symlink, but every followed target must remain beneath that resolved memory root.
- **`config.json is unreadable (...); refusing to start on defaults`:** the file exists but cannot be read (permissions, a directory in its place, or a symlink whose target is missing). The daemon exits non-zero rather than booting on defaults, because defaults would drop `mentionAllowlist` and open a mention-gated room. Fix the file, then start again; a reload in a running daemon keeps the previous config and reports the same diagnostic.
