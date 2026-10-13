# Deployment

## Quick Start

### Option 1: Docker Compose (recommended for first-time users)

The fastest way to get running: a one-command setup experience with persistent volumes and healthchecks.

```sh
# Clone the repository
git clone https://github.com/Yeachan-Heo/gajae-way.git
cd gajae-way

# Run interactive setup (choose Discord, Slack, or Telegram)
docker compose run --rm -it setup gajaeway setup

# Start the gateway and adapters
docker compose up
```

Then send your bot a DM and start chatting. For details on environment variables and configuration, see [Docker Compose](#docker-compose) below.

### Option 2: Local Installation (Bun)

If you prefer a local installation, build the binaries and use the one-command `up` workflow:

```sh
# Requirements: Bun ≥1.4.0, gjc ≥0.16.0 on PATH
git clone https://github.com/Yeachan-Heo/gajae-way.git
cd gajae-way

bun install
bun run build

# One-command setup and status check
GAJAEWAY_HOME=~/.gajaeway ./dist/gajaeway up
```

The `gajaeway up` command:
- Prompts for credentials if `config.json` doesn't exist
- Validates tokens against each platform API
- Reports next steps (daemon start, service installation, etc.)

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
dist/gajaeway-slack
dist/gajaeway-admin
dist/gajaeway
```

Each adapter binary runs in the foreground. Discord and Slack `--help` and `--version` return without connecting. Every adapter acquires its own non-blocking home lease (`adapter-discord`, `adapter-slack`, or `adapter-telegram`) and refuses a second owner for the same `$GAJAEWAY_HOME`; no PID file decides ownership.

The release gateway does not use an arbitrary executable found on `PATH`. It reads the exact GJC version from the gateway package pin; the current pin is `0.18.11`. When installing a release or running `gajaeway update --force`, the updater installs that version under `$GAJAEWAY_HOME/gjc/<version>/bin/gjc`, verifies its reported version, and atomically flips `$GAJAEWAY_HOME/gjc/current` to it. The previous version is retained for rollback. The binaries need no source checkout or Bun to run; installing/updating the pinned GJC package uses Bun on the update host.

The gateway and broker host pass `GJC_SDK_BROKER_AUTOSTART=0` to GJC child processes. GJC 0.18.9 and later honor this attach-only contract: a client that finds no live broker fails with `broker_unavailable` instead of spawning, retiring, or restarting one, and `gjc doctor` refuses broker restarts. The gateway refuses any GJC older than 0.18.9. The broker service runs the public foreground command `gjc sdk broker run --agent-dir <gjc.agentDir>` as its child; the broker never detaches, a live incumbent makes it exit 1 without being touched, and its exit code propagates to the service manager.

## Upgrades

`gajaeway update` upgrades an installed binary release and serializes its installation with the `update` home lease:

```sh
gajaeway update --check                 # report only; touches nothing
gajaeway update                         # download, replace, and queue the stack restart
gajaeway update --no-restart            # replace binaries; restart later with ops restart-stack
gajaeway update --bin-dir /opt/gajaeway # when driving the update from elsewhere
```

The updater stages the platform release, backs up replaced binaries, reads the staged gateway package's GJC pin, installs and verifies that exact version in the private prefix, then installs service definitions. When a broker manifest exists and restart is enabled, it enables the broker unit on Linux and queues the restart supervisor. The supervisor restarts the broker, gateway, and connected services in order. `--force` reinstalls the current release and its pinned GJC version. `GAJAEWAY_UPDATE_REPO=owner/name` redirects release checks to a fork.

A source checkout upgrades through `git`, `bun run build`, and `gajaeway services repair`; when running under Bun, pass `--bin-dir DIR` to update. The release update requires Bun to install the pinned GJC package.

### macOS upgrade from 0.1.x to 0.2.0

When migrating the broker to its private GJC agent directory, keep the updated gateway stopped until provisioning and the authority cutover are complete:

```sh
gajaeway update --no-restart
gajaeway setup --broker-agent
GAJAEWAY_HOME=ABS gajaeway-gateway census
GAJAEWAY_HOME=ABS gajaeway-gateway cutover --agent-dir CANONICAL
gajaeway ops restart-stack
gajaeway ops restart-stack --status
```

Before census, stop the gateway service and disable managed startup for the entire cutover window; follow [Explicit broker-authority cutover](runbooks/gajaeway-v1.md#explicit-broker-authority-cutover) and retain its census and cutover evidence. The final restart loads a missing macOS LaunchAgent from its plist, including `dev.gajaeway.broker` once its provision manifest exists.

## Home and configuration

`GAJAEWAY_HOME` selects the state directory; it defaults to `~/.gajaeway`. The gateway makes it private (`0700`). A typical layout is:

```text
$GAJAEWAY_HOME/
  config.json
  adapter-discord.json
  adapter-telegram.json
  adapter-slack.json
  adapters/discord/recovery-cursor.json
  broker.env                 # optional protected environment for the broker service
  adapters/slack/recovery-cursor.json  # Slack missed-message watermarks
  attachments/telegram/<chat>/  # inbound Telegram photos/files (owner-only, pruned after 7 days)
  gateway.sock
  gateway.db                 # live SQLite WAL/SHM sidecars may also be present
  leases/
    gateway                   # permanent flock files; never unlink them
    broker
    update
    memory
    adapter-discord
    adapter-slack
    adapter-telegram
  broker/
    agent/                    # private pinned-GJC agent directory
    provision/
      manifest.json
      journal.json            # present while generation recovery is needed
      gen-*/                  # staged generation
      prev-*/                 # preserved generation
  gjc/
    current -> <version>
    <version>/bin/gjc
  backups/
    *.db
    *.open-work.json          # cutover evidence alongside its backup
    pre-restore-<id>/         # preserved database and sidecars
  restore.journal.json        # present while a restore is incomplete
  workspace/                 # SOUL.md, AGENTS.md, USER.md; GJC working directory
  memory/                    # Markdown files and private Git repository
  secrets/
    discord-token
    telegram-token
    slack-bot-token          # xoxb-…
    slack-app-token          # xapp-… (Socket Mode)
```

`restore.journal.json` and the broker provision journal are transient recovery records. A live database may have `gateway.db-wal` and `gateway.db-shm`; do not copy those files as a backup. The cutover export is written next to its `.db` backup.

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
  "channels": {
    "discord-channel-id": { "engagement": "open", "audience": "human-only" },
    "slack:C0123456789": { "engagement": "mention-open" }
  },
  "webhook": { "bind": "127.0.0.1", "port": 8080, "exposeNonLoopback": false },
  "watcherRoots": ["/Users/me/automations"],
  "scriptRoot": "/Users/me/automations",
  "stallTimeoutMs": 120000,
  "monitorCatchUp": { "maxSlots": 24, "maxAgeMs": 86400000 }
}
```

`socketPath`, `dbPath`, `logVerbosity`, credentials, channels, webhook, watcher roots, script root, `monitorCatchUp`, and `stallTimeoutMs` are optional. Socket and database paths default inside the home directory, `stallTimeoutMs` defaults to 120000 ms, and log verbosity defaults to `info`. `interimSpeech` is optional and restart-required. Mid-work assistant messages that are pure procedural narration ("let me check…", "채널 더 볼게요") or a near-repeat of the previous one are held back and logged as `gateway mid-work speech suppressed (<turn>, procedural|duplicate)`; everything else ships. `interimSpeech.maxPerTurn` caps delivered mid-work messages per turn (`0` delivers none) and `interimSpeech.minGapMs` spaces them; both are unlimited when unset. `turnTimeoutMs` is rejected because persistent-session liveness is alert-only; `settleWindowMs`, `channels.*.settleWindowMs` and `maxInboundAgeMs` are rejected because every message is steered or sent immediately and nothing expires while queued.

`monitorCatchUp` bounds cron replay after downtime: `maxSlots` is the newest due slots to admit (1–1000, default 24), and `maxAgeMs` excludes slots older than the lookback (60000–604800000 ms, default 86400000 / 24 hours). Refused slots advance the durable monitor cursor, are counted and logged, and appear in `gajaeway monitors inspect <id>` as `catchUp`; the setting is restart-required.

### Private broker runtime

`gjc.agentDir` is optional and must be absolute when set. Its default is `$GAJAEWAY_HOME/broker/agent`, the private directory owned by the broker service. The gateway uses the pinned GJC executable from `$GAJAEWAY_HOME/gjc/current/bin/gjc`; it does not fall back to a system executable or mutate the operator's source profile.

Provision the private directory with `gajaeway setup --broker-agent` while the broker is stopped. By default, the command reads the operator's `~/.gjc/agent` profile; `--from DIR` selects another source. It copies only the source allowlist: required `agent.db` credentials (captured with a read-only SQLite snapshot), optional `models.yml` and `config.yml`, and the supported `model-presets` files (`state.json`, `state.backup.json`, `control.json`). Symlinks and live broker URL settings are refused. Gajaeway reads the source for provisioning and drift checks but does not write it.

Provisioning uses the `broker` lease and journaled generations under `broker/provision/`. A staged `gen-*` generation is swapped into the private agent directory while the old files are held in `prev-*`; a manifest records the generation and entry fingerprints. If provisioning is interrupted, the broker host or next provisioning command recovers the journal under the same lease by finishing the manifest-committed generation or rolling back to the previous files.

Inspect the provision result with `gajaeway-gateway broker-agent status` (or `--json`). Entry states distinguish source changes, private-copy changes, divergence, and missing inputs. `gajaeway ops cycle` projects that drift under `brokerAgent`; it raises `broker_agent_unprovisioned` for an unprovisioned/interrupted state or a missing required `agent.db` copy. Review other drift before forcing a new snapshot, since a changed private copy may contain broker-refreshed state.

## Docker Compose

The repository includes a `compose.yaml` for containerized deployment with persistent volumes and health checks.

### First-time setup with Docker Compose

```sh
# Interactive setup (choose adapters and enter credentials)
docker compose run --rm -it setup gajaeway setup

# Or with environment variables (non-interactive)
docker compose run --rm -it \
  -e GAJAEWAY_DISCORD_TOKEN=$DISCORD_TOKEN \
  -e GAJAEWAY_OWNER_IDS=$YOUR_ID \
  setup gajaeway setup --from-env --adapters discord
```

### Start the services

```sh
# Start gateway and enabled adapters
docker compose up

# Start specific adapters only
docker compose --profile discord up          # Discord only
docker compose --profile slack up            # Slack only
docker compose --profile discord,slack up    # Discord + Slack
```

### Common commands

```sh
# Check configuration
docker compose exec gajaeway-gateway gajaeway setup --status

# View logs
docker compose logs -f gajaeway-gateway

# Access shell
docker compose exec gajaeway-gateway sh

# Restart adapters
docker compose restart gajaeway-discord
```

Configuration and state are stored in the `gajaeway_home` volume, which persists across container restarts.

### Model Credentials for GJC

The Docker image includes gjc (the AI coding agent) installed at `/usr/local/bin/gjc`. To enable gjc to access AI models (Claude, etc.), you must mount your gjc agent directory with credentials from the host:

**Option 1: Mount gjc agent directory** (recommended)

Edit `compose.yaml` and uncomment the gjc volume mount:

```yaml
services:
  gajaeway-gateway:
    volumes:
      - gajaeway_home:/home/gajaeway/.gajaeway
      # Uncomment to share model credentials with gjc inside container:
      - ${GJC_AGENT_DIR:-~/.gjc}:/home/gajaeway/.gjc:ro
```

Then start with:
```bash
GJC_AGENT_DIR=~/.gjc docker compose up
```

**Option 2: Mount credentials only** (if gjc config is elsewhere)

```yaml
services:
  gajaeway-gateway:
    volumes:
      - gajaeway_home:/home/gajaeway/.gajaeway
      # Mount just the credentials directory:
      - ${GJC_CREDENTIALS_PATH:-~/.gjc/credentials}:/home/gajaeway/.gjc/credentials:ro
```

**Option 3: Separate compose override file**

Create `compose.credentials.yaml`:

```yaml
services:
  gajaeway-gateway:
    volumes:
      - ~/.gjc:/home/gajaeway/.gjc:ro
  gajaeway-discord:
    volumes:
      - ~/.gjc:/home/gajaeway/.gjc:ro
  gajaeway-slack:
    volumes:
      - ~/.gjc:/home/gajaeway/.gjc:ro
  gajaeway-telegram:
    volumes:
      - ~/.gjc:/home/gajaeway/.gjc:ro
```

Then start with:
```bash
docker compose -f compose.yaml -f compose.credentials.yaml up
```

**Without model credentials**

If you do not mount credentials, gjc will fall back to configured models in the Docker container's isolated gjc config at `/home/gajaeway/.gjc`, but most users will want to reuse their host's gjc configuration with authenticated providers (see above).

**Read-only mounts**

All gjc mounts should be read-only (`:ro`) to prevent the container from accidentally modifying your host's credentials.

## Manual Configuration

If you prefer not to use the interactive `gajaeway setup` command, you can manually create the configuration files:

### 1. Create the home directory

```sh
mkdir -p $GAJAEWAY_HOME/secrets
chmod 0700 $GAJAEWAY_HOME
```

### 2. Create credential files

Store each credential in a separate file (0600 permissions):

```sh
# Discord
echo "YOUR_DISCORD_BOT_TOKEN" > $GAJAEWAY_HOME/secrets/discord-token
chmod 0600 $GAJAEWAY_HOME/secrets/discord-token

# Slack
echo "xoxb-YOUR-SLACK-BOT-TOKEN" > $GAJAEWAY_HOME/secrets/slack-bot-token
echo "xapp-YOUR-SLACK-APP-TOKEN" > $GAJAEWAY_HOME/secrets/slack-app-token
chmod 0600 $GAJAEWAY_HOME/secrets/slack-*

# Telegram
echo "123456:YOUR_TELEGRAM_BOT_TOKEN" > $GAJAEWAY_HOME/secrets/telegram-token
chmod 0600 $GAJAEWAY_HOME/secrets/telegram-token
```

### 3. Create config.json

```json
{
  "schemaVersion": 1,
  "logVerbosity": "info",
  "credentials": {
    "discord": { "credentialFile": "$GAJAEWAY_HOME/secrets/discord-token" }
  }
}
```

Save this as `$GAJAEWAY_HOME/config.json`.

### 4. Create adapter configurations

For each enabled adapter, create a configuration file:

**Discord** (`$GAJAEWAY_HOME/adapter-discord.json`):
```json
{
  "tokenFile": "$GAJAEWAY_HOME/secrets/discord-token",
  "intents": ["GUILDS", "GUILD_MESSAGES", "DIRECT_MESSAGES", "MESSAGE_CONTENT"]
}
```

**Slack** (`$GAJAEWAY_HOME/adapter-slack.json`):
```json
{
  "botTokenFile": "$GAJAEWAY_HOME/secrets/slack-bot-token",
  "appTokenFile": "$GAJAEWAY_HOME/secrets/slack-app-token",
  "socketMode": true
}
```

**Telegram** (`$GAJAEWAY_HOME/adapter-telegram.json`):
```json
{
  "tokenFile": "$GAJAEWAY_HOME/secrets/telegram-token",
  "pollingTimeout": 30
}
```

### 5. Start the gateway

```sh
dist/gajaeway-gateway daemon
```

Then start your adapters in separate terminals:

```sh
dist/gajaeway-discord
dist/gajaeway-slack
dist/gajaeway-telegram
```

## Slack adapter

`gajaeway-slack` connects over **Socket Mode**, so the host needs no public URL and no inbound firewall rule. It reads `$GAJAEWAY_HOME/adapter-slack.json`:

```json
{
  "botTokenFile": "secrets/slack-bot-token",
  "appTokenFile": "secrets/slack-app-token",
  "gatewaySocket": "/Users/me/gajaeway/gateway.sock",
  "channels": { "C0123456789": { "engagement": "open" } },
  "liveReplies": true
}
```

Both token files are credential-file references (relative paths resolve against the config directory) and are validated by prefix: the bot token must start with `xoxb-`, the app-level token with `xapp-`. A wrong prefix is a startup error naming the file, never the value. `gatewaySocket` defaults to `$GAJAEWAY_HOME/gateway.sock`. The adapter's own `channels` map only promotes an `open` channel's messages to a mention before they reach the gateway; the gateway's `config.json` `channels` map, keyed `slack:<channelId>`, remains the authority for mode and audience.

Create the Slack app from a manifest with:

- Socket Mode **enabled**, and an app-level token with the `connections:write` scope (`appTokenFile`).
- Bot token scopes: `app_mentions:read channels:history channels:read chat:write groups:history groups:read im:history im:read im:write mpim:history mpim:read reactions:read reactions:write users:read files:read commands`.
- Event subscriptions (bot events): `message.channels message.groups message.im message.mpim reaction_added reaction_removed`.
- Slash commands `/new`, `/reset`, `/restart`, and `/model` (any request URL; Socket Mode delivers them over the socket). Register `/model` even if you never plan to type it: Slack delivers a slash command to whichever workspace app registered its name, so if this app does not register `/model`, typing `/model` anywhere — including this bot's own DM — reaches whichever other app claimed it instead.

Invite the bot to every channel it should read; Slack delivers no history or events for channels the bot is not a member of.

Origins are `slack/dm/D…/peer=U…` for direct messages, `slack/channel/C…` for public, private, and multi-person channels, and `slack/thread/C…:<thread_ts>/parent=C…` for messages inside a thread. Platform message ids are `channel:ts` pairs because a Slack `ts` is unique only within its channel. Replies default into threads: a channel mention is answered in a thread rooted at the message that triggered it (the room stays readable and the conversation continues in that thread, which is its own session), a reply inside a thread stays in it, and a DM sent inside a thread is answered in that thread. `[REPLY:<channel:ts>]` overrides the target.

Presence is a reaction gradient on the message the persona is answering, not a posted or edited message: on acceptance the message gets ⏳; as the turn moves it becomes 🔧 (running a tool), 💭 (reading a result), ✍️ (writing); a clock face 🕐…🕛 advances once per minute and a digit 1️⃣ 2️⃣ 3️⃣ 5️⃣ 🔟 💯 tracks tool calls (or tokens when the runtime reports none). Each marker is swapped only when its bucket changes and at most once per 15 seconds, and every marker comes off when the reply lands. The tool's name and stated intent are never rendered in chat; they are shown in the admin console's live-work row. Outbound writes are paced per channel (about one per second; replies take priority over status edits), HTTP 429 is retried up to three times honouring `Retry-After` and then left as an ambiguous delivery for the next redelivery pass rather than recorded as a failure, and a reconnect after a blip shorter than five seconds does not repeat a catch-up pass that finished within the last minute. Reactions are mapped by Slack emoji name (`👍` → `+1`, `🦞` → `lobster`); the whole allowlist is deliverable, and the presence markers are disjoint from it so the two never collide. Inbound text is normalised (`<@U…>` mentions, `<#C…|name>`, links, `&amp;`) before it reaches the persona, and outbound Markdown is converted to Slack mrkdwn. Files and images arrive as `[image · name · size · url]` lines; the persona needs the bot token to fetch `url_private`. There is no voice transcription or spoken reply on Slack.
`liveReplies` (optional, default off) folds a turn's streaming output into one message instead of one message per part: the first mid-turn part posts the reply and every later part — including the terminal reply — is edited into it with `chat.update`, so a working persona reads as one growing answer rather than a burst of separate messages. A single-part turn posts exactly as before, and the feature never changes ledger semantics: each part is still its own delivery row, settled from the same outcomes. The edit is a full-text render of the applied parts, so ledger replays are idempotent — an already-applied part re-confirms without a platform call, an unapplied retry renders in its stored position, and only the fallback post path (entry sealed or lost) carries the `[recovered - may be a duplicate]` prefix. Folding degrades to ordinary posting, never loses content: a render past the message limit, a Slack refusal to edit (message deleted, permissions changed), or a turn that already ended seals the entry for the rest of the turn. It needs no extra scope (`chat.update` uses the same `chat:write` as posting).

After every socket connect and gateway reconnect the adapter backfills missed messages from `conversations.history`, keyed by `channel:ts`, so the gateway's durable dedupe makes overlap with live traffic safe. Coverage is bounded on purpose: only channels listed in `adapter-slack.json` `channels`, the 100 most recently active DMs seen live within 30 days, and threads the persona replied in during the last 7 days are revisited; a channel with no watermark is backfilled 24 hours deep, and an oversized gap drains across passes in bounded slices. Channels the bot cannot read are quarantined after three consecutive failures and probed again on the next connect. A message the gateway refuses three times for its own content is dead-lettered into `adapters/slack/recovery-cursor.json` (with a per-channel digest) rather than pinning the channel; a gateway outage never discards anything.

## Reloading configuration without a restart

A running gateway re-reads `config.json` on `SIGHUP` (`kill -HUP <pid>`) or on the `gateway.reloadConfig` verb; both run the same implementation, so the console and the signal behave identically.

The reload is fail-safe and reports exactly what it did:

- `changed` — fields applied live: `mentionAllowlist`, `channels`, `stallTimeoutMs`, `dmPolicy`, `botAudience`, and `handoffTargets` (alias -> chat `OriginRef` for `[HANDOFF:<alias>]` replies). Verify the next event through the path consuming the changed policy.
- `restartRequired` — fields bound to startup resources: `socketPath`, `dbPath`, `model`, `serviceTier`, `credentials`, `webhook`, `watcherRoots`, `scriptRoot`, `runtime`, `ownerTarget`, `monitorContextFailureRollThreshold`, `monitorCatchUp`, `work`, and `interimSpeech`. They are reported and deliberately NOT applied; restart to pick them up.
- `ignored` — fields you edited that no code reads at all. `logVerbosity` is currently parsed but unconsumed, so editing it has no effect and no restart would give it one.
- On a parse or validation error, or when `config.json` is missing or unreadable, the reload fails, keeps the previous configuration untouched, and returns a diagnostic. A missing file never publishes defaults over live policy, because that would drop the mention allowlist and open a mention-gated room.

Every reload is logged with the trigger and all three field lists.

The Discord adapter has a separate `$GAJAEWAY_HOME/adapter-discord.json` because it reads its own token:

```json
{
  "tokenFile": "/Users/me/gajaeway/secrets/discord-token",
  "gatewaySocket": "/Users/me/gajaeway/gateway.sock",
  "channels": { "discord-channel-id": { "engagement": "open", "audience": "human-only" } }
}
```

`engagement` selects how messages become turns: `open` admits them without addressing, `mention-open` requires a real mention or native reply to this bot, and `closed` requires both addressing and owner/allowlist authorization. `audience` independently restricts authors to `all`, `human-only`, or `bot-only`; `closed` ignores it. Omitting `audience` keeps the safe `human-only` default (humans follow the mode; bots stay on the closed gate).

Bot-authored turns admitted by a widened audience are unlimited in sequence by default. Set `botAudience.maxConsecutiveTurns` globally, or `channels.<key>.botAudienceMaxConsecutiveTurns`, to require a human message after N consecutive bot turns. The runaway guard is the rolling-minute rate limit `botAudience.maxTurnsPerWindow` (per-channel override `botAudienceMaxTurnsPerWindow`), default 30 bot admissions per conversation per minute; it is not reset by a human message. Both declines are logged with the origin's current counts and counted in `gateway.status` (`engagement.botAudienceDeclines`, `engagement.botAudienceRateLimited`).

On startup and gateway reconnect, the adapter backfills each configured Discord channel, its active threads, and recent archived threads whose last activity is within 30 days. DMs cannot be enumerated through Discord, so live DM ingress records a bounded recovery set before `chat.send`; the adapter retains at most 100 DM conversations and expires entries after 30 days. Progress is stored atomically in `$GAJAEWAY_HOME/adapters/discord/recovery-cursor.json`. A missing-access or deleted conversation keeps its watermark and is retried twice, then quarantined on the third unreadable result so healthy conversations can finish; a later reconnect probes it and re-admits it when history is readable again. Message-id dedupe in the gateway makes live/recovery races and restart resumes exactly-once.

Keep token files out of version control and restrict their permissions. Replace a token file and restart the relevant service to rotate it.

## Service manager: leases and broker unit

Keep binaries and state outside macOS TCC-protected directories such as Desktop, Documents, and Downloads. A launchd process can hang when its working directory or GJC executable is under a protected path. The generated definitions use the installed binaries and `$GAJAEWAY_HOME`; configure service secrets through protected environment or credential files as appropriate for the host.

`gajaeway services install --bin-dir DIR` writes service definitions for the broker, gateway, adapters, and admin. Linux systemd user units are named `gajaeway-broker.service` and `gajaeway-gateway.service`; the macOS LaunchAgent labels are `dev.gajaeway.broker` and `dev.gajaeway.gateway`. On Linux, the broker unit runs `gajaeway-gateway broker` and has `ConditionPathExists` for the broker provision manifest. Provision the private agent first with `gajaeway setup --broker-agent`; a systemd unit without a manifest does not start a broker. The broker host takes the `broker` lease for its lifetime, validates the provisioned directory and exact GJC pin, then launches the pinned executable against that private directory.

The gateway takes the `gateway` lease followed by the `memory` lease and holds both for its lifetime. Each adapter takes its own lease. All use the same kernel-backed, non-blocking flock implementation and permanent files under `$GAJAEWAY_HOME/leases`; a diagnostic owner record or service-manager PID is not the ownership authority. Offline census, cutover, and restore also require the `gateway` lease and refuse while the daemon holds it. The `update` lease serializes binary updates and the restart supervisor.

On Linux, the gateway unit has `Wants=` and `After=` for the broker unit; adapters and admin are bound to the gateway unit. The broker uses `KillMode=control-group` and a 30-second stop timeout. The gateway uses `KillMode=mixed` and `TimeoutStopSec=30s` so its own relays and commands stop with it; the broker is a different service and is not in the gateway unit's cgroup. A normal gateway restart does not restart the broker. On macOS, use the service manager definitions and the supervisor below; launchd does not propagate systemd dependencies.

```sh
gajaeway ops restart-stack
gajaeway ops restart-stack --status
```

The command returns after a detached supervisor is queued. The supervisor restarts the broker, gateway, then connected adapters and admin; on Linux it restarts the broker and gateway units and lets gateway dependencies restart clients, while on macOS it kickstarts each job in order and bootstraps the LaunchAgent plist when a job has not been loaded. It verifies process start and deployed binary; lease-backed services additionally require the lease owner's PID to match the service manager's main PID. The receipt is `$GAJAEWAY_HOME/restart-stack.json`; `--status` is successful only when it reports `ok` and exits 0.

An update-triggered restart stops the broker, runs journaled `broker-agent provision --mode update` under the broker lease, then starts the broker. If provisioning is unrecoverable, the restart receipt says to run `gajaeway setup --broker-agent`; inspect the provision status before restarting. On Linux, the service manager names the broker unit `gajaeway-broker.service`; on macOS the supervisor uses `dev.gajaeway.broker`.

`ops cycle` includes the private broker agent's status and source/copy drift. It also reports `broker_respawn_churn`, `broker_index_lock_blocked`, and `gjc_unverified_version` when those gates apply. Migration 33 (L4h), which removes `broker_quarantine` and `broker_cutovers`, remains pending.

## Speech gate

A persona told in its prompt to answer `[SILENT]` when it will not speak sometimes posts its reasoning instead ("not my lane, only recording this"). The speech gate makes that decision in code. It uses the same gate model as the kev value shadow and is off by default:

- `KEV_SHADOW_URL` (and `KEV_SHADOW_TOKEN` if the model needs one): the gate model. Without it, the gate and the shadow are both no-ops.
- `KEV_GATE_MODE=enforce`: turn the gate on for this gateway. Any other value, or no value, only measures (`kev-shadow` log lines).
- `KEV_GATE_ABSTAIN_UNDER` (default `0.35`): the cut for dropping a reply part as abstention narration.

It applies only to group-channel messages that are not addressed to the persona. DMs, @mentions, replies to the persona, threads it is already talking in, the owner (`ownerTarget` peer), and bot authors always bypass it. For gated messages:

1. A message the value score reads as `would-skip` opens no turn. It is still recorded as context, and the gateway logs `speech-gate skip`.
2. A reply part the gate model reads as not responding to the message is not delivered. The gateway logs `speech-gate drop` with the dropped text, so false drops can be audited.

If the gate model is unreachable, both steps fail open: the turn runs and the reply is delivered.

## Troubleshooting

- **Socket missing:** verify the gateway service, configured socket path, parent permissions, and service log.
- **Every turn fails with an API error:** confirm `gjc` is on the service `PATH` and its model-key environment variables are present. If the log says a model was not found, use an explicit selector (`"model": "provider/model"`) or activate a gjc profile (`"model": { "preset": "profile-name" }`); profile default-role arrays retain gjc's native fallback-chain handling. A poisoned conversation session can be rebound with `/new`.
  Fast/priority processing is independent of the model selector: set `"serviceTier": "priority"` in gateway `config.json`. The gateway applies GJC `service_tier.set` once per persona session before its first prompt (OpenAI `service_tier=priority`; Anthropic fast speed where supported). Use `"model": { "preset": "gpt-heavy" }` to pin the model profile separately.
- **launchd hangs:** move the working directory, state, `gjc`, and symlink targets out of TCC-protected paths; then send `/new` to sessions created under the old location.
- **Webhook or monitor failure:** verify the gateway configuration and use `gajaeway monitors inspect <monitor-id>`.
- **Recovery or restore:** use the [operator runbook](runbooks/gajaeway-v1.md), especially its backup, restore, crash-recovery, and schema guidance.

Read [architecture](architecture.md) for delivery semantics and [memory](memory.md) for the private Markdown repository.
