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
dist/gajaeway-slack
dist/gajaeway-admin
dist/gajaeway
```

On macOS, `bun run build` finishes with `scripts/sign-dist.sh`, which re-signs every binary with a local code-signing identity (identifiers `dev.gajaeway.<gateway|slack|discord|telegram|admin|cli>`) taken from `GAJAEWAY_CODESIGN_IDENTITY` or the first line of `~/.gajaeway/codesign-identity`. `bun build --compile` alone leaves ad-hoc signatures whose privacy grants (Documents, Desktop, Downloads, Full Disk Access) are pinned to one build hash, so every rebuild re-prompts and a persona session touching `~/Documents` blocks until someone clicks Allow. With a stable identity a grant survives rebuilds. Without a configured identity the script leaves the ad-hoc signatures in place.

Each binary requires its verb: `gajaeway-gateway daemon`, `gajaeway-admin serve`, and a subcommand for `gajaeway`. Invoked with no arguments they print usage on stderr and exit 2, so probing one never blocks. `gajaeway-discord`, `gajaeway-telegram`, and `gajaeway-slack` run in the foreground with no arguments; `gajaeway-discord --help` / `gajaeway-slack --help` and `--version` answer without connecting, and a second `gajaeway-discord` (or `gajaeway-slack`) refuses to boot while `$GAJAEWAY_HOME/adapter-discord.pid` (`adapter-slack.pid`) names a live process.

A production host does not need a source checkout, `node_modules`, or Bun to run those binaries. It does need the same global-user `gjc` executable and canonical agent directory/broker used by the operator's interactive SDK. Verify `command -v gjc` in that user's normal shell and set the service's `GJC_EXECUTABLE` explicitly to the verified absolute executable path. Run the service as that same user with the same canonical profile environment (`HOME`, and any intentional `GJC_CONFIG_DIR`/`PI_CONFIG_DIR` or `GJC_CODING_AGENT_DIR`/`PI_CODING_AGENT_DIR` selection). Do not introduce gateway-private overrides, copy model/provider configuration, or seed settings. The default profile is `~/.gjc/agent`; using the same executable with a different agent directory is not the same runtime. GJC owns its daemon; the gateway is an SDK client only. Credentials belong in the user's established protected environment, not in copied broker settings.

The gateway requires GJC 0.16.0 or newer. Its error-code handling is verified through the gjc minor named by `VERIFIED_GJC_THROUGH` (`packages/gateway/src/orchestrator/gjc-contract.ts`); a newer minor still runs but raises the `gjc_unverified_version` cycle gate until `GAJAEWAY_E2E_GJC=1 bun test packages/gateway/test/gjc-contract.e2e.test.ts` passes against it and the constant is raised. After upgrading gjc on a host, run that test there; the gateway ends a pre-upgrade `gjc` process that keeps killing the shared broker on its own (see the runbook). GJC 0.17.6 requires `--json` for machine-readable `sdk session` errors; the gateway adds it to those commands. The `sdk serve --stdio` relay arguments and environment binding are unchanged.
The pin travels with the binaries: the gateway package's own `package.json` (which `bun run build` copies to `dist/package.json`) is the only place an installed binary can read `gjc.version` from, and a release tarball must ship it beside the binaries. A compiled binary keeps the *build machine's* source path in `__filename`, so a binary carried to another host cannot read the pin from a checkout there and exits at boot naming a path that does not exist on it.

## Upgrades

`gajaeway update` upgrades a binary install in place from the project's GitHub releases — the same model `gjc update` uses — so a production host never needs the source checkout it was first installed from:

```sh
gajaeway update --check                 # report only; touches nothing
gajaeway update                         # download, replace, and queue the stack restart
gajaeway update --no-restart            # replace binaries; restart later with ops restart-stack
gajaeway update --bin-dir /opt/gajaeway # when driving the update from elsewhere
```

The update resolves the latest `vX.Y.Z` release, downloads this platform's `gajaeway-<platform>-<arch>.tar.gz`, stages and size-checks the binaries, backs the previous ones up into `<bin-dir>/backup-<version>-<timestamp>/`, replaces them in place, records the installed release in `<bin-dir>/.gajaeway-release`, and queues the existing `ops restart-stack` supervisor (gateway first, then adapters). A release carries `package.json` beside the binaries — the gateway cannot boot without the pinned gjc version in it. Only binaries the host already installed are replaced, plus the `gajaeway`/`gajaeway-gateway` core pair; a release never installs adapters the operator did not choose. One update runs at a time per bin dir (`.gajaeway-update.lock`; a lock naming a live pid refuses, a stale one is taken over), and `--force` reinstalls the current release.

A source checkout keeps upgrading through `git` + `bun run build` + `services repair`; `update` refuses to guess a bin dir when run under `bun`, so pass `--bin-dir` in that case. `GAJAEWAY_UPDATE_REPO=owner/name` redirects update checks at a fork's releases; the default comes from the package's declared repository.

## Home and configuration

`GAJAEWAY_HOME` selects the state directory; it defaults to `~/.gajaeway`. The gateway makes the home directory private (`0700`). A typical layout is:

```text
$GAJAEWAY_HOME/
  config.json
  adapter-discord.json
  adapter-telegram.json
  adapter-slack.json
  adapter-discord.pid        # single-instance lock, held by the running Discord adapter
  adapter-slack.pid          # same, for the Slack adapter
  adapters/slack/recovery-cursor.json  # Slack missed-message watermarks
  gateway.sock
  gateway.db
  workspace/                 # SOUL.md, AGENTS.md, USER.md; gjc working directory
  memory/                    # Markdown files and private Git repository
  memory-receipts.jsonl
  secrets/
    discord-token
    telegram-token
    slack-bot-token          # xoxb-…
    slack-app-token          # xapp-… (Socket Mode)
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

### GJC agent directory

The gateway communicates with the operator's shared GJC broker through its agent directory. The agent directory is resolved in this order:

1. Deployment seam: `options.broker.agentDir` (test/deployment override only).
2. `config.json` `gjc.agentDir` (must be an absolute path; optional).
3. `GJC_CODING_AGENT_DIR` or `PI_CODING_AGENT_DIR` environment variable (if set).
4. Established home authority: existing databases keep the agent directory they were created with (read from `broker_authority` table).
5. Default: `$GAJAEWAY_HOME/gjc-agent` for fresh deployments.

The gateway checks broker authority (based on the resolved agent directory) before running schema migrations. If the authority does not match an existing home's recorded authority, the boot fails with `authority_mismatch` and leaves the database schema unchanged, allowing offline corrective action.

**Existing homes and pre-#321 upgrade paths:** A home established against the shared `~/.gjc/agent` keeps that directory automatically on upgrade (step 4), with its sessions and lanes intact; setting `gjc.agentDir` or `GJC_CODING_AGENT_DIR` makes that choice explicit. Keeping the shared directory gives up the isolation #321 added: an interactive gjc upgrade can affect the gateway. To move an established home onto the isolated `$GAJAEWAY_HOME/gjc-agent`, use a deliberate cutover instead (copy provider auth and model presets into the new directory first). The operator runbook documents the `gjc-authority-cutover` script for explicit migration between agent directories, including quarantine disposition for durable history.

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
    <key>GJC_EXECUTABLE</key><string>/Users/me/gajaeway/bin/gjc</string>
    <key>HOME</key><string>/Users/me</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ExitTimeOut</key><integer>30</integer>
  <key>StandardOutPath</key><string>/Users/me/gajaeway/gateway.stdout.log</string>
  <key>StandardErrorPath</key><string>/Users/me/gajaeway/gateway.stderr.log</string>
</dict></plist>
```

### The service owns its own log sink

Each daemon writes its own rotating sink under `$GAJAEWAY_HOME`: `gateway.log`, `adapter-discord.log`, `adapter-slack.log`. Every line begins with an ISO-8601 UTC timestamp and a level, identical consecutive events collapse into one `x<N> (identical, first=… last=…)` line, the live file rotates to `.1`…`.5` at 10 MB, and an hourly `service_alive uptime=…` line makes log silence distinguishable from service silence.

The service manager's own stdout/stderr redirect must therefore NOT point at those paths - it would double-write every line. Point it at a separate file (as above) or leave it to the journal; the owned sink is the one to read during an incident.

This launchd example is macOS-specific, not a deployment command for the systemd host reached on SSH port 24. Before installing it, set its executable and profile paths to the verified interactive user's values. A launchd job does not inherit your shell: supply the same canonical profile environment and required model-key variables through a protected mechanism. An already-running shared GJC daemon retains its own environment; restarting the gateway does not rotate that daemon's credentials. Protect the plist:

```sh
chmod 600 ~/Library/LaunchAgents/dev.gajaeway.gateway.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/dev.gajaeway.gateway.plist
```

Run the Discord, Telegram, and Slack binaries as separate managed services after the gateway. `gajaeway services install --bin-dir DIR` writes definitions for the gateway, the Discord and Slack adapters, and the admin console; Telegram is run under your own service definition. The CLI is an on-demand client; it does not start the daemon. The definitions it writes follow the host: launchd plists in `~/Library/LaunchAgents` on macOS, systemd user units in `${XDG_CONFIG_HOME:-~/.config}/systemd/user` on Linux. `--platform darwin|linux` with `--launch-agents-dir`/`--unit-dir` generates for the other host.

### The adapters restart with the gateway

A gateway restart does not kill an adapter. The adapter reconnects and keeps serving the previous generation: nothing dies, nothing is lost, `delivery.pending` stays 0, and the only symptom is replies arriving a beat late. The service definitions therefore bind the stack together instead of relying on an operator following a restart order.

On systemd each adapter and the admin unit carry `BindsTo=`, `After=`, and `PartOf=gajaeway-gateway.service`, so `systemctl --user restart gajaeway-gateway` realigns the whole stack in one command, and `WantedBy=gajaeway-gateway.service` means enabling the gateway enables them. Only the gateway unit carries `KillMode=mixed` and `TimeoutStopSec=30s`; see below for why that cannot reach the shared GJC broker.

launchd has no `BindsTo`/`PartOf` equivalent, and `WatchPaths` does not restart an already-running job. Use the single entry point instead, on either host:

```sh
gajaeway ops restart-stack
gajaeway ops restart-stack --status
```

On Linux that is the one `systemctl --user restart gajaeway-gateway`; on macOS it kickstarts the gateway and then every dependent job, in order. It never uses `launchctl bootout`, which removes the job and leaves it with no automatic recovery.

The command returns as soon as it has queued the sequence. A detached supervisor carries it out: on macOS a process in a new session, on Linux a transient `systemd-run --user` unit outside the gateway cgroup. Restarting the gateway kills a persona turn that invoked it, but not the supervisor. After each command the supervisor checks every service against its deployed binary. The check reads the pid from the service manager and the start time from `ps`, and passes only when the process started after both the binary's modification time and the restart command. A service that fails is recorded as `stale` and the remaining labels are skipped. The receipt is `$GAJAEWAY_HOME/restart-stack.json`, and `--status` prints it. `--status` exits 0 only for a verified `ok`. A sequence whose supervisor died reports `interrupted`.

To diagnose a stack that is already mismatched, read `clients` in `gajaeway status`: every connected client reports its process `startedAt` and a `staleGeneration` flag that is true when the client process predates the running gateway process. That replaces comparing `ps -o lstart` by hand.

### One gateway per home, owned by the service manager

The gateway daemon is always started and stopped by launchd or systemd; never start a second copy by hand while a managed one runs. On boot the daemon settles ownership of `$GAJAEWAY_HOME` before touching its socket or database, using `$GAJAEWAY_HOME/daemon.pid`. This is gateway-home ownership, not ownership of the shared GJC broker:

- If the record names a live `gajaeway-gateway daemon` for the same home, the newcomer **waits** (up to 20s) for it to finish its ordered shutdown. It never signals that process - the service manager owns its lifecycle. If the predecessor is still alive at the deadline the newcomer exits 1 and the service manager retries under its own throttle.
- `daemon --only-new` skips the wait: any live same-home gateway is an immediate refusal (exit 1). Use it in scripts that must not disturb a running instance.
- A record whose process is dead, is not a gateway, or belongs to a different home is stale and is replaced.

Restart with the host's service manager: on the systemd deployment reached on SSH port 24, use `systemctl --user restart gajaeway-gateway`; on macOS launchd, use `launchctl kickstart -k gui/$(id -u)/dev.gajaeway.gateway`. These are alternatives, not consecutive steps. Give ordered shutdown at least 30 seconds with systemd `TimeoutStopSec` or launchd `ExitTimeOut`. Two gateways on one home can race over the database and delivery state even though neither owns the user broker.

### systemd: no gateway child outlives the unit

The gateway unit must use:

```ini
[Service]
KillMode=mixed
TimeoutStopSec=30s
```

`TimeoutStopSec` must be at least 30 seconds; the installed unit pins it. On SIGTERM/SIGINT the gateway stops admitting work, waits up to 10 seconds for in-flight monitor authoring turns, marks any that are still running as `gateway_shutdown` (failure detail names the session and the stop time; lease released so the next boot's reconcile re-dispatches them at once), and exits by itself within 25 seconds, logging `gateway_shutdown_timeout` if teardown could not settle. A stop that reaches the service manager's SIGKILL is therefore a defect, not the expected path.

`KillMode=mixed` sends SIGTERM to the gateway main process for its ordered shutdown, then SIGKILLs everything still left in the unit's cgroup. Without it, every `gjc sdk serve` relay and `gjc sdk session` command the gateway spawned outlived each stop, and the next start re-adopted them (`Found left-over process ... in control group while starting unit. Ignoring.`). Those orphans held gigabytes of memory for days and kept writing into state owned by the new incarnation (#183, #227). `KillMode=process` is what caused this, and it is not supported.

The shared GJC broker is not the gateway's child to kill. GJC autostarts it detached from whichever SDK command first finds it absent. When the broker is discovered to be absent or dead, the gateway sends a read-only `sdk session list --scope all` command to trigger GJC's normal autostart mechanism. This nudge command is identical to the one used during boot readiness probing and does not attempt to kill, restart, or replace the broker—it is a passive observe-only probe consistent with the gateway's client-only relationship to the shared broker. The nudge respects a 45-second backoff between attempts to avoid hammering a broker that is repeatedly failing.

When that autostart command is the gateway's, the broker and every session host it forks would otherwise land in the gateway cgroup. The gateway therefore checks each broker generation it observes. If that broker runs inside the gateway's own unit, the gateway moves it and its descendants into a transient `gajaeway-gjc-broker-<pid>.scope` and logs `broker_scope_released`. A broker started anywhere else is never touched. SDK turns outlive gateway restarts because they run in the broker's scope, not because the unit spares its children.

Upgrading a host that still has `KillMode=process` in the unit or in a drop-in: deploy this gateway first, and confirm `broker_scope_released` in its log (or `systemd-cgls --user-unit gajaeway-gateway.service` no longer listing `broker-internal`). Only then change the drop-in to `KillMode=mixed`, run `systemctl --user daemon-reload`, and restart. Changing the kill mode before the broker has been released stops the broker along with the gateway.

Normal gateway stop/start closes its own SDK calls and relays and reconnects as a client. It never kills the shared user broker, reaps old hosts, deletes discovery files, copies settings, or runs global session GC. A readiness failure is not permission to repair or replace the user's daemon. If the shared broker is unavailable at boot (for example while it clears a stale lock after a host reboot), the gateway does not exit: it retries the preflight and readiness steps with exponential backoff (1s doubling to 30s) for up to 10 minutes, logging one `gateway_boot_waiting_for_broker step=… attempt=… retry_in_ms=…` line per retry, and exits 1 only after that deadline. A wrong gjc version or a rejected relay argv is still fatal immediately. For a database changing broker authority, complete the explicit cutover in the runbook before starting recovery; changing service environment alone does not migrate old work.

The implemented administrative entry point requires the checkout and Bun. Stop the managed gateway and disable automatic restart throughout inspection and apply; keep the shared GJC daemon running. Use the verified absolute gateway home (`ABS`) and canonical absolute user agent directory (`CANONICAL`):

```sh
bun scripts/gjc-authority-cutover.ts inspect --home ABS --agent-dir CANONICAL
bun scripts/gjc-authority-cutover.ts apply --home ABS --agent-dir CANONICAL --expected-authority 'null' --quarantine --evidence 'operatorreference' --backup UNIQUEABS
```

Use `'null'` only when inspection reports `oldAuthority: null`; otherwise supply the exact inspected `oldAuthority` JSON as one shell-quoted argument. Supply an actual operator evidence reference and a unique, nonexistent absolute backup destination (`UNIQUEABS`) under an existing canonical parent. Both modes refuse a live gateway PID/socket. Apply holds the same kernel-backed home lease as gateway boot, revalidates authority, and creates an integrity-checked, non-overwriting backup before database migration. The runbook details the receipt and census to retain. Immutable old history stays quarantined without replay; old worker names remain reserved, so new work requires new names. Re-enable managed startup only after successful apply and executable/profile verification. Do not globally restore configuration, reap hosts, or delete sessions as part of cutover.

Deployment acceptance requires a newly gateway-created session to be visible through the normal user's SDK and the gateway with the exact same session ID. Record unrelated user session IDs and configuration fingerprints before deployment; verify they remain unchanged afterward. The current blocker is that SDK global model controls can modify user configuration: verify configuration invariance through session creation and model controls, not just executable/profile parity or a successful cutover. Do not mask a failure with automatic global configuration restore. Verify the existing user daemon survives a gateway restart and owned work remains observable without resend. These are required observations, not a claim that production is deployed or healthy.

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
