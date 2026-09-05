# Deployment

## Production unit

Production runs the compiled standalone binaries. Build them on a machine with Bun:

```sh
bun run build
```

The result is:

```text
dist/gajaeway-gateway
dist/gajaeway-discord
dist/gajaeway-telegram
dist/gajaeway-admin
dist/gajaeway
```

Each binary requires its verb: `gajaeway-gateway daemon`, `gajaeway-admin serve`, and a subcommand for `gajaeway`. Invoked with no arguments they print usage on stderr and exit 2, so probing one never blocks. `gajaeway-discord` and `gajaeway-telegram` run in the foreground with no arguments; `gajaeway-discord --help` and `--version` answer without connecting, and a second `gajaeway-discord` refuses to boot while `$GAJAEWAY_HOME/adapter-discord.pid` names a live process.

A production host does not need a source checkout, `node_modules`, or Bun to run those binaries. It **does** need the external `gjc` executable on `PATH`: gateway startup owns one private gjc agent directory for the instance, and gjc's own daemon for that directory hosts the persistent sessions (auto-started on first use; requires gjc >= 0.16.1, the first release containing gajae-code PR #5208 and its authoritative tail revisions; revision-qualified tail item IDs are required for cross-turn delivery identity). Model-provider credentials are inherited from the gateway process environment; do not place them in the broker agent directory. Provider/model configuration lives in the operator SSOT `~/.gjc/agent`; the gateway seeds its private agent directory from it on every start.

## Home and configuration

`GAJAEWAY_HOME` selects the state directory; it defaults to `~/.gajaeway`. The gateway makes the home directory private (`0700`). A typical layout is:

```text
$GAJAEWAY_HOME/
  config.json
  adapter-discord.json
  adapter-telegram.json
  adapter-discord.pid        # single-instance lock, held by the running Discord adapter
  gateway.sock
  gateway.db
  workspace/                 # SOUL.md, AGENTS.md, USER.md; gjc working directory
  memory/                    # Markdown files and private Git repository
  broker/<instance-id>/agent/ # private broker-owned GJC state; not a credential store
  memory-receipts.jsonl
  secrets/
    discord-token
    telegram-token
```

Use `config.json` schema version 1. Every configured secret is a credential-file reference; a credential file may be referenced by only one configured credential.

```json
{
  "schemaVersion": 1,
  "logVerbosity": "info",
  "socketPath": "/Users/me/gajaeway/gateway.sock",
  "dbPath": "/Users/me/gajaeway/gateway.db",
  "credentials": {
    "discord": { "credentialFile": "/Users/me/gajaeway/secrets/discord-token" },
    "telegram": { "credentialFile": "/Users/me/gajaeway/secrets/telegram-token" }
  },
  "channels": { "discord-channel-id": { "engagement": "open" } },
  "handoffTargets": {
    "way-dev": { "platform": "discord", "kind": "channel", "conversationId": "discord-dev-channel" }
  },
  "webhook": { "bind": "127.0.0.1", "port": 8080, "exposeNonLoopback": false },
  "watcherRoots": ["/Users/me/automations"],
  "scriptRoot": "/Users/me/automations",
  "stallTimeoutMs": 120000
}
```

`socketPath`, `dbPath`, `logVerbosity`, credentials, channels, handoff targets, webhook, watcher roots, script root, and `stallTimeoutMs` are optional. `handoffTargets` maps a whitespace-free alias to a validated `OriginRef`; aliases are the stable names personas use in `[HANDOFF:<alias>]`. Socket and database paths default inside the home directory, `stallTimeoutMs` defaults to 120000 ms, and log verbosity defaults to `info`. `turnTimeoutMs` is rejected because persistent-session liveness is alert-only; `settleWindowMs`, `channels.*.settleWindowMs` and `maxInboundAgeMs` are rejected because every message is steered or sent immediately and nothing expires while queued.

A finalized terminal reply whose first line is `[HANDOFF:<target>]` is accepted by inserting one bounded, provenance-carrying inbound event into the target origin's durable queue. The target's own `PersonaSessionManager` actor then serializes or steers it; the source receives only a pointer (or a loud refusal), and target processing can continue after the source turn is complete. Handoff event ids are deterministic, so replayed source turns do not run the target twice. Relay provenance is context, not authority, and chains are capped at two hops with cycle refusal.


## Reloading configuration without a restart

A running gateway re-reads `config.json` on `SIGHUP` (`kill -HUP <pid>`) or on the `gateway.reloadConfig` verb; both run the same implementation, so the console and the signal behave identically.

The reload is fail-safe and reports exactly what it did:

- `changed` — fields applied live. `mentionAllowlist`, `channels`, `stallTimeoutMs`, `dmPolicy`, and `handoffTargets` are re-read at runtime; a change takes effect on the next actor event.
- `restartRequired` — fields you edited that are bound to a startup resource (`socketPath`, `dbPath`, `model`, `serviceTier`, `credentials`, `webhook`, `watcherRoots`, `scriptRoot`, `ownerTarget`, `monitorContextFailureRollThreshold`). They are reported and deliberately NOT applied; restart to pick them up.
- `ignored` — fields you edited that no code reads at all. `logVerbosity` is currently parsed but unconsumed, so editing it has no effect and no restart would give it one.
- On a parse or validation error, or when `config.json` is missing or unreadable, the reload fails, keeps the previous configuration untouched, and returns a diagnostic. A missing file never publishes defaults over live policy, because that would drop the mention allowlist and open a mention-gated room.

Every reload is logged with the trigger and all three field lists.

The Discord adapter has a separate `$GAJAEWAY_HOME/adapter-discord.json` because it reads its own token:

```json
{
  "tokenFile": "/Users/me/gajaeway/secrets/discord-token",
  "gatewaySocket": "/Users/me/gajaeway/gateway.sock",
  "channels": { "discord-channel-id": { "engagement": "open" } }
}
```

Keep token files out of version control and restrict their permissions. Replace a token file and restart the relevant service to rotate it.

## Service manager: launchd example

Install binaries and state outside macOS TCC-protected directories such as Desktop, Documents, and Downloads. A launchd process can hang when its working directory, `gjc`, or a symlink target lies there. Put the binaries, `gjc`, and `$GAJAEWAY_HOME` somewhere such as `~/gajaeway`.

A user agent can launch the gateway:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>dev.gajaeway.gateway</string>
  <key>ProgramArguments</key><array>
    <string>/Users/me/gajaeway/bin/gajaeway-gateway</string><string>daemon</string>
  </array>
  <key>WorkingDirectory</key><string>/Users/me/gajaeway</string>
  <key>EnvironmentVariables</key><dict>
    <key>GAJAEWAY_HOME</key><string>/Users/me/gajaeway/state</string>
    <key>PATH</key><string>/Users/me/gajaeway/bin:/usr/local/bin:/usr/bin:/bin</string>
    <key>YOUR_MODEL_KEY</key><string>replace-with-provider-key</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/Users/me/gajaeway/gateway.log</string>
  <key>StandardErrorPath</key><string>/Users/me/gajaeway/gateway.log</string>
</dict></plist>
```

Model keys used by `gjc` are inherited from the gateway environment. A launchd job does not inherit your shell, so provide the required model-key variables in `EnvironmentVariables` (or an equivalent protected secret mechanism) and protect the plist:

```sh
chmod 600 ~/Library/LaunchAgents/dev.gajaeway.gateway.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/dev.gajaeway.gateway.plist
```

Run the Discord and Telegram binaries as separate managed services after the gateway. The CLI is an on-demand client; it does not start the daemon.

### One gateway per home, owned by the service manager

The gateway daemon is always started and stopped by launchd or systemd; never start a second copy by hand while a managed one runs. On boot the daemon settles ownership of `$GAJAEWAY_HOME` **before** touching the socket, the database, or the broker lock, using `$GAJAEWAY_HOME/daemon.pid`:

- If the record names a live `gajaeway-gateway daemon` for the same home, the newcomer **waits** (up to 20s) for it to finish its ordered shutdown. It never signals that process - the service manager owns its lifecycle. If the predecessor is still alive at the deadline the newcomer exits 1 and the service manager retries under its own throttle.
- `daemon --only-new` skips the wait: any live same-home gateway is an immediate refusal (exit 1). Use it in scripts that must not disturb a running instance.
- A record whose process is dead, is not a gateway, or belongs to a different home is stale and is replaced.

Restart with the service manager's own verbs - `launchctl kickstart -k gui/$(id -u)/dev.gajaeway.gateway` or `systemctl --user restart gajaeway-gateway` - and give the ordered shutdown time to complete: set launchd `ExitTimeOut` (or systemd `TimeoutStopSec`) to at least 30s. Two gateways on one home is the failure mode this guards against: both supervise the same private gjc daemon and take turns retiring it.

## Troubleshooting

- **Socket missing:** verify the gateway service, configured socket path, parent permissions, and service log.
- **Every turn fails with an API error:** confirm `gjc` is on the service `PATH` and its model-key environment variables are present. If the log says a model was not found, use an explicit selector (`"model": "provider/model"`) or activate a gjc profile (`"model": { "preset": "profile-name" }`); profile default-role arrays retain gjc's native fallback-chain handling. A poisoned conversation session can be rebound with `/new`.
  Fast/priority processing is independent of the model selector: set `"serviceTier": "priority"` in gateway `config.json`. The gateway applies GJC `service_tier.set` once per persona session before its first prompt (OpenAI `service_tier=priority`; Anthropic fast speed where supported). Use `"model": { "preset": "gpt-heavy" }` to pin the model profile separately.
- **launchd hangs:** move the working directory, state, `gjc`, and symlink targets out of TCC-protected paths; then send `/new` to sessions created under the old location.
- **Webhook or monitor failure:** verify the gateway configuration and use `gajaeway monitors inspect <monitor-id>`.
- **Recovery or restore:** use the [operator runbook](runbooks/gajaeway-v1.md), especially its backup, restore, crash-recovery, and schema guidance.

Read [architecture](architecture.md) for delivery semantics and [memory](memory.md) for the private Markdown repository.
