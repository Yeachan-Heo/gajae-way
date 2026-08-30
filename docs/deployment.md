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
dist/gajaeway
```

A production host does not need a source checkout, `node_modules`, or Bun to run those binaries. It **does** need the external `gjc` executable on `PATH`: the gateway spawns `gjc` to create each session and for every turn.

## Home and configuration

`GAJAEWAY_HOME` selects the state directory; it defaults to `~/.gajaeway`. The gateway makes the home directory private (`0700`). A typical layout is:

```text
$GAJAEWAY_HOME/
  config.json
  adapter-discord.json
  adapter-telegram.json
  gateway.sock
  gateway.db
  workspace/                 # SOUL.md, AGENTS.md, USER.md; gjc working directory
  memory/                    # Markdown files and private Git repository
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
  "webhook": { "bind": "127.0.0.1", "port": 8080, "exposeNonLoopback": false },
  "watcherRoots": ["/Users/me/automations"],
  "scriptRoot": "/Users/me/automations"
}
```

`socketPath`, `dbPath`, `logVerbosity`, credentials, channels, webhook, watcher roots, and script root are optional. Socket and database paths default inside the home directory, and log verbosity defaults to `info`.

## Reloading configuration without a restart

A running gateway re-reads `config.json` on `SIGHUP` (`kill -HUP <pid>`) or on the `gateway.reloadConfig` verb; both run the same implementation, so the console and the signal behave identically.

The reload is fail-safe and reports exactly what it did:

- `changed` — fields applied live. Only `mentionAllowlist`, `channels`, and `debounceMs` are re-read at runtime; a change to one of these takes effect on the next turn.
- `restartRequired` — fields you edited that are bound to a startup resource (`socketPath`, `dbPath`, `turnTimeoutMs`, `model`, `credentials`, `webhook`, `watcherRoots`, `scriptRoot`, `ownerTarget`, `monitorContextFailureThreshold`). They are reported and deliberately NOT applied; restart to pick them up.
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

## Troubleshooting

- **Socket missing:** verify the gateway service, configured socket path, parent permissions, and service log.
- **Every turn fails with an API error:** confirm `gjc` is on the service `PATH` and its model-key environment variables are present. If the log says a model was not found, use an explicit selector (`"model": "provider/model"`) or activate a gjc profile (`"model": { "preset": "profile-name" }`); profile default-role arrays retain gjc's native fallback-chain handling. A poisoned conversation session can be rebound with `/new`.
- **launchd hangs:** move the working directory, state, `gjc`, and symlink targets out of TCC-protected paths; then send `/new` to sessions created under the old location.
- **Webhook or monitor failure:** verify the gateway configuration and use `gajaeway monitors inspect <monitor-id>`.
- **Recovery or restore:** use the [operator runbook](runbooks/gajaeway-v1.md), especially its backup, restore, crash-recovery, and schema guidance.

Read [architecture](architecture.md) for delivery semantics and [memory](memory.md) for the private Markdown repository.
