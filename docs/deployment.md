# Deployment

## Production unit

Build the compiled production binary on a machine with Bun:

```sh
bun run build
```

The build output is exactly:

```text
dist/gajaeway
```

Install that file as `gajaeway` on the production host. It is the only executable; one long-lived `gajaeway daemon run` process composes the gateway, enabled platform adapters, and admin console.

| Invocation | Role |
|---|---|
| `gajaeway daemon run` | Starts the one composite daemon. |
| `gajaeway config check [path]` | Validates a configuration file without starting the daemon. |
| `gajaeway [--socket PATH] status|shutdown|chat|sessions …|ops …|memory …|monitors …|work …` | On-demand clients of the resident daemon. `--socket PATH` overrides the default client socket path. |
| `gajaeway --help`, `gajaeway --version` | Prints usage or the version without connecting. |

Normal shutdown exits 0. A boot failure or an adapter escalation exits 1. Invalid usage and a second daemon for the same home exit 2.

A production host does not need a source checkout, `node_modules`, or Bun to run the binary. It does need the external `gjc` executable on `PATH`: the daemon owns one private gjc agent directory for its instance, while gjc owns the persistent session daemon. Model-provider credentials are inherited from the daemon environment; keep provider/model configuration in the operator SSOT at `~/.gjc/agent`, not in the private broker directory.

## Home and configuration

`GAJAEWAY_HOME` selects the state directory and defaults to `~/.gajaeway`. The daemon makes the home directory private (`0700`). A typical layout is:

```text
$GAJAEWAY_HOME/
  config.json
  gajaeway.pid                  # daemon ownership lock
  gateway.sock                  # local client transport
  gateway.db
  admin-audit.jsonl             # admin request audit trail
  adapter-telegram-state.json   # durable Telegram update cursor
  adapters/
    discord/
      recovery-cursor.json
  workspace/                    # SOUL.md, AGENTS.md, USER.md; gjc working directory
  memory/                       # Markdown files and private Git repository
  broker/<instance-id>/agent/   # private broker-owned GJC state; not a credential store
  memory-receipts.jsonl
  secrets/
    discord-token
    discord-voice-key
    telegram-token
```

The ownership lock is `$GAJAEWAY_HOME/gajaeway.pid`. It is fail-closed: the daemon reclaims it only when its PID is provably dead. A readable lock naming a live process, an unreadable/malformed lock, and an unprovable PID all refuse startup.

Use schema version 1. Every secret is a credential-file reference, never an inline token; a credential file may be referenced by only one configured credential. This complete example enables both adapters and Discord voice:

```json
{
  "schemaVersion": 1,
  "logVerbosity": "info",
  "socketPath": "/Users/me/gajaeway/state/gateway.sock",
  "dbPath": "/Users/me/gajaeway/state/gateway.db",
  "credentials": {
    "discord": { "credentialFile": "/Users/me/gajaeway/secrets/discord-token" },
    "discordVoice": { "credentialFile": "/Users/me/gajaeway/secrets/discord-voice-key" },
    "telegram": { "credentialFile": "/Users/me/gajaeway/secrets/telegram-token" }
  },
  "adapters": {
    "discord": {
      "intents": [1, 512],
      "voice": {
        "languageCode": "ko",
        "voiceId": "voice-id",
        "speechSpeed": 1.2
      }
    },
    "telegram": {}
  },
  "channels": {
    "discord:123456789": { "engagement": "open", "settleWindowMs": 500 },
    "telegram:-100123456789": { "engagement": "open" }
  },
  "model": { "preset": "codex-medium" },
  "settleWindowMs": 2000,
  "stallTimeoutMs": 120000,
  "maxInboundAgeMs": 600000,
  "mentionAllowlist": ["owner-author-id"],
  "dmPolicy": "allowlist",
  "ownerTarget": {
    "origin": {
      "platform": "discord",
      "kind": "dm",
      "conversationId": "owner-dm",
      "peerId": "owner-author-id"
    }
  },
  "webhook": { "bind": "127.0.0.1", "port": 8080, "exposeNonLoopback": false },
  "watcherRoots": ["/Users/me/automations"],
  "scriptRoot": "/Users/me/automations",
  "monitorContextFailureRollThreshold": 2
}
```

### Adapters, credentials, and channels

Presence enables an adapter: `adapters.discord` requires `credentials.discord`; its optional `voice` section also requires `credentials.discordVoice`; and `adapters.telegram: {}` requires `credentials.telegram`. Credentials are read once from the same configuration snapshot used to boot the gateway.

Use `discord:<id>` and `telegram:<id>` channel keys for new configuration. A bare ID remains accepted as a Discord channel key, but never as a Telegram key. Platform adapters report observed facts such as mentions and bot authorship; the gateway applies the live channel engagement policy.

Both `adapters` and `credentials` are restart-required. A configuration reload can apply live policy fields, but it reports changes to either of those sections without applying them until the daemon restarts. Discord recovery channels are also enumerated at daemon start from `discord:<id>` keys and Discord bare IDs, so a channel change that must affect recovery needs a restart.

Other startup-bound fields are `socketPath`, `dbPath`, `model`, `webhook`, `watcherRoots`, `scriptRoot`, `ownerTarget`, and `monitorContextFailureRollThreshold`. `mentionAllowlist`, `channels`, `settleWindowMs`, `stallTimeoutMs`, `maxInboundAgeMs`, and `dmPolicy` are live-reloadable. A failed reload retains the previous configuration.

## Admin console

`GAJAEWAY_ADMIN_PORT` selects the loopback-only admin HTTP port. It defaults to `8788` and must be an integer from `1` through `65535`; an invalid environment value fails boot. Port `0` is a direct test seam only and is not accepted through the environment. Admin actions are recorded in `$GAJAEWAY_HOME/admin-audit.jsonl`.

## Service manager: launchd example

Install the binary, `gjc`, and the state directory outside macOS TCC-protected locations such as Desktop, Documents, and Downloads. A user LaunchAgent can use this single plist as `~/Library/LaunchAgents/dev.gajaeway.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>dev.gajaeway</string>
  <key>ProgramArguments</key><array>
    <string>/Users/me/gajaeway/bin/gajaeway</string>
    <string>daemon</string>
    <string>run</string>
  </array>
  <key>WorkingDirectory</key><string>/Users/me/gajaeway</string>
  <key>EnvironmentVariables</key><dict>
    <key>GAJAEWAY_HOME</key><string>/Users/me/gajaeway/state</string>
    <key>GAJAEWAY_ADMIN_PORT</key><string>8788</string>
    <key>PATH</key><string>/Users/me/gajaeway/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    <key>YOUR_MODEL_KEY</key><string>replace-with-provider-key</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/Users/me/gajaeway/gajaeway.log</string>
  <key>StandardErrorPath</key><string>/Users/me/gajaeway/gajaeway.log</string>
</dict></plist>
```

The `PATH` must resolve the external `gjc` program as well as the app binary. A launchd job does not inherit shell model-key variables, so put required provider variables in `EnvironmentVariables` (or an equivalent protected secret mechanism) and protect the plist accordingly.

## Migrating from the split binaries

| Before | After |
|---|---|
| Three or four launchd units: `dev.gajaeway-gateway`, `dev.gajaeway-discord`, `dev.gajaeway-telegram`, and optionally `dev.gajaeway-admin`. | One `dev.gajaeway` unit runs `gajaeway daemon run`. |
| Independent process lifecycles and restarts. | One daemon owns the gateway, admin console, enabled adapters, lock, signals, and exit status. |

Stop the old units in ingress-to-core order: the Discord adapter unit, the Telegram adapter unit, the admin unit when present, then the gateway unit. After all are stopped, remove the old plist files.

| Before | `config.json` now |
|---|---|
| `adapter-discord.json.tokenFile` | `credentials.discord.credentialFile` |
| `voice.apiKeyFile` | `credentials.discordVoice.credentialFile` |
| `intents` and `voice.*` | `adapters.discord.intents` and `adapters.discord.voice.*` |
| `channels` | `channels["discord:<id>"]` |
| `adapter-telegram.json.tokenFile` | `credentials.telegram.credentialFile` |
| `chats` | `channels["telegram:<id>"]` |
| `gatewaySocket` | Dropped; all in-process components use local ports. |

**Old-file list (remove only after the old units are stopped):**

- `adapter-discord.pid`

Preserve these state files and directories: `gateway.db`, `adapter-telegram-state.json`, `adapters/discord/recovery-cursor.json`, `memory/`, and `workspace/`.

1. Put the mapped configuration and credential files under the chosen home, then validate it:

   ```sh
   gajaeway config check
   ```

2. Load `dev.gajaeway` and let launchd start `gajaeway daemon run`; then verify a Discord or Telegram DM reaches the bot.
3. The rollback boundary is packaging and service configuration only: restore the archived old plists and binaries if needed. The database schema is unchanged, so do not down-migrate `gateway.db` for this migration.

## Troubleshooting

- **`adapter_escalated adapter=<name> failures=5` crash loop:** the supervisor retried four failures and the fifth terminated the daemon with exit 1 for launchd `KeepAlive`. Repair the credential or platform configuration. To operate degraded while investigating, remove `adapters.<name>` from `config.json`, validate, and restart.
- **`telegram_poll_conflict`:** another Telegram poller is using the same token. Stop the competing poller. HTTP 409 is logged and polling continues with transient backoff; it is not a fatal Telegram error.
- **Second-instance refusal (exit 2):** another daemon owns the home, or the ownership file cannot be safely interpreted. Do not delete `$GAJAEWAY_HOME/gajaeway.pid` unless no daemon is running; only a PID proven dead is reclaimed automatically.
- **`broker health probe miss N/3`:** gjc health did not answer within the ten-second probe timeout. After three consecutive misses the broker generation is fenced and recovery is awaited. Check the gjc executable, its environment, and the daemon log before restarting the service.
- **Every turn fails with an API error:** confirm `gjc` is on the service `PATH` and required model-key environment variables are present. A poisoned conversation session can be rebound with `/new`.
- **launchd hangs:** move the working directory, state directory, `gjc`, and symlink targets out of TCC-protected paths; then send `/new` to sessions created under the old location.
- **Webhook or monitor failure:** verify the configuration and use `gajaeway monitors inspect <monitor-id>`.
- **Recovery or restore:** use the [operator runbook](runbooks/gajaeway-v1.md), especially its backup, restore, crash-recovery, and schema guidance.

Read [architecture](architecture.md) for delivery semantics and [memory](memory.md) for the private Markdown repository.
