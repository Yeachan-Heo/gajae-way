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
  "model": "opus",
  "debounceMs": 1000,
  "mentionAllowlist": ["owner-author-id"],
  "webhook": { "bind": "127.0.0.1", "port": 8080, "exposeNonLoopback": false },
  "watcherRoots": ["/absolute/path"],
  "scriptRoot": "/absolute/path"
}
```

Use schema version 1 only. Each credential is a file reference, never an inline secret or environment fallback; a credential file may be referenced by exactly one configured credential. Create secret files with restrictive ownership and mode, keep them outside version control, and rotate by replacing the file and restarting the service.

## Adapters and engagement

Create separate credential files for Discord and Telegram tokens, then reference them as `credentials.discord` and `credentials.telegram`. Configure group/channel engagement with `channels.<conversation-id>.engagement: "open"`; unconfigured group traffic remains mention-gated. Direct messages engage normally. Verify adapter connectivity from its service logs and use `gajaeway sessions list` to confirm accepted traffic.

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
- **`config.json is unreadable (...); refusing to start on defaults`:** the file exists but cannot be read (permissions, a directory in its place, or a symlink whose target is missing). The daemon exits non-zero rather than booting on defaults, because defaults would drop `mentionAllowlist` and open a mention-gated room. Fix the file, then start again; a reload in a running daemon keeps the previous configuration and reports the same diagnostic.
