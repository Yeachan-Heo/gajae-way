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
dist/gajaeway-admin
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

`socketPath`, `dbPath`, `logVerbosity`, credentials, channels, webhook, watcher roots, and script root are optional. Socket and database paths default inside the home directory, and log verbosity defaults to `info`. `logVerbosity` is reloadable; changing socket or database paths requires a restart.

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

## Admin console

`dist/gajaeway-admin` is an optional operator surface: a small HTTP console over an already running gateway. Build it with the other binaries (`bun run build`), or run it from source with `bun packages/admin/src/main.ts`.

It is configured entirely by two environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `GAJAEWAY_SOCKET` | `/Users/bellman/gajaeway-play/discord-v1/gateway.sock` | Gateway unix socket to connect to |
| `GAJAEWAY_ADMIN_PORT` | `8788` | TCP port to listen on |

A `GAJAEWAY_ADMIN_PORT` that is not a plain decimal integer in `1..65535` refuses to start rather than falling back to the default. On startup the process logs one line with the resolved url and socket.

The `GAJAEWAY_SOCKET` default above is the operator's current deployment path, not a portable one — set it explicitly on any other host.

The console binds `127.0.0.1` only and has no authentication of its own. Do not put it on a non-loopback address; reach a remote host through an SSH tunnel instead. Because a loopback bind alone does not stop DNS rebinding, the console also pins the `Host` header: a request whose `Host` is not `127.0.0.1`, `localhost`, or `[::1]` is refused with `403` before any routing.

Read routes are GET-only and map onto existing gateway methods — `/api/status`, `/api/core`, `/api/sessions`, `/api/monitors`. Any other method on one of those four returns `405`. `/api/operations` answers `GET` with the allowlist itself and returns `404` for other methods.

Mutations go to `POST /api/mutations` only; every other method on that path, including a `GET`, is refused with `405`, so a mutation is never reachable by following a link. A request has to clear all three checks:

- an **allowlisted `operationId`** — `monitor.add`, `monitor.remove`, `monitor.test`, `ops.backup`, `ops.integrity`; anything else is `404`. `chat.send` is deliberately not allowlisted: making the bot speak from a dashboard is irreversible and public. That closes the direct path only — a `monitor.add` carrying a `channelTarget` can still cause a delivery, so treat monitor mutations as publicly visible.
- an **`actor`** carrying at least one letter or digit, otherwise `401`. It is a self-declared label, not an authenticated identity; the console cannot tell you who really called.
- a **`confirm`** field echoing the operation id exactly, otherwise `428`.

Every gate decision — allow or reject — is written to stdout as one `admin.audit <json>` line, so the service log is the audit trail. Capture it the same way you capture the gateway log. Two things are deliberately outside it: a request refused before the gate (`400` malformed body, `403` bad host, `405` wrong method) produces no line, and an `allowed` line records the authorization, not the gateway's result — a mutation that then fails at the socket still shows only `allowed`.

Run the console under the same service manager as the gateway with restart-on-exit. It probes the gateway every 30 seconds and exits non-zero when the connection is gone, because a gateway restart otherwise leaves it up and answering `502` forever.

## Troubleshooting

- **Socket missing:** verify the gateway service, configured socket path, parent permissions, and service log.
- **Every turn fails with an API error:** confirm `gjc` is on the service `PATH` and its model-key environment variables are present. A poisoned conversation session can be rebound with `/new`.
- **launchd hangs:** move the working directory, state, `gjc`, and symlink targets out of TCC-protected paths; then send `/new` to sessions created under the old location.
- **Webhook or monitor failure:** verify the gateway configuration and use `gajaeway monitors inspect <monitor-id>`.
- **Recovery or restore:** use the [operator runbook](runbooks/gajaeway-v1.md), especially its backup, restore, crash-recovery, and schema guidance.

Read [architecture](architecture.md) for delivery semantics and [memory](memory.md) for the private Markdown repository.
