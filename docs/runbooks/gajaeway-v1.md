# Gajaeway v1 operator runbook

## Install and run

For production, compile standalone binaries — no source checkout, node_modules, or Bun install is needed on the host:

```sh
bun run build   # emits dist/gajaeway-gateway, dist/gajaeway-discord, dist/gajaeway-telegram, dist/gajaeway-slack, dist/gajaeway
dist/gajaeway-gateway daemon
```

Gajaeway runs the gateway package's pinned GJC version from `$GAJAEWAY_HOME/gjc/current/bin/gjc` against the private agent directory `$GAJAEWAY_HOME/broker/agent` (the default `gjc.agentDir`). The current pin is GJC 0.18.11. `gajaeway update` installs the pin in a versioned private prefix; the update host needs Bun for that package-install step, while the compiled service binaries need no Bun to run. `gajaeway setup --broker-agent` provisions the private directory from an allowlist, reading `~/.gjc/agent` as the default source without writing to it. Both gateway and broker GJC children receive `GJC_SDK_BROKER_AUTOSTART=0`; from 0.18.9 GJC clients are attach-only, so a missing broker surfaces as `broker_unavailable` and only the broker service (`gjc sdk broker run`) starts one. For development, run straight from source: `bun packages/gateway/src/main.ts daemon`.

The CLI does not start the daemon: `gajaeway daemon run` prints the launcher command, and `gajaeway status` requires the daemon socket. The status command prints the gateway status JSON, including functional readiness when reported. Run the launcher under your service manager (systemd/launchd/container supervisor), keep its state directory private, and stop the service before an offline restore.

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
  "sessions": { "maxLiveHosts": 8, "idleRetireMs": 21600000, "idleResetMs": 86400000 },
  "mentionAllowlist": ["owner-author-id"],
  "webhook": { "bind": "127.0.0.1", "port": 8080, "exposeNonLoopback": false },
  "watcherRoots": ["/absolute/path"],
  "scriptRoot": "/absolute/path",
  "monitorContextFailureRollThreshold": 2,
  "monitorCatchUp": { "maxSlots": 24, "maxAgeMs": 86400000 }
}
```

Use schema version 1 only. Each credential is a file reference, never an inline secret or environment fallback; a credential file may be referenced by exactly one configured credential. Create secret files with restrictive ownership and mode, keep them outside version control, and rotate by replacing the file and restarting the service.

`model` accepts either a GJC model selector string such as `"openai/gpt-5.2"` or a preset object such as `{ "preset": "codex-medium" }`. The gateway applies it through the persistent session's authenticated `model.set` control; presets resolve through the private `gjc.agentDir` profile, including its `models.yml` when present.

Per conversation, `/model` shows the effective selection and its source, `/model set <choice>` stores an override and rebinds the live session with `model.set`, and `/model clear` returns to `model`. Discord registers `/model show|set|clear`, and Slack delivers the same verb when its app registers the `/model` slash command; `set` autocompletes from `profiles:` names in the private agent directory's `models.yml` plus the configured model (verb `session.modelChoices`), and an unreadable file only empties the suggestions. Authorization is the same gate as `/new`.

`work.maxLanes` (integer 1–256, default 8) caps bound worker lanes. `work.idleRetireMs` (integer 60000–604800000 milliseconds, default 21600000 / 6 hours) sets the idle retirement threshold checked by the 60-second sweep; terminal `done`/`aborted` lane jobs are also retired. Open attempts are never retired. `work.allowNested` (boolean, default false) gates both work.start and work.run from lane sessions. All three settings are restart-required: edit `config.json` and restart the gateway; a config reload does not apply them.

`sessions.maxLiveHosts` (integer 1–256, default 8) caps concurrently live persona session hosts: each bound conversation origin runs one `gjc sdk session-host-internal` process of roughly 360 MB, so an unbounded count on a busy workspace exhausts memory and the broker answers `broker_unavailable` to everything. At the cap the least-recently-active idle host is ended before a new origin binds; when every host is busy the new turn is held (`persona_bind_hold … reason=host_capacity live=N max=M`, with one `[turn held]` notice in the channel) and dispatched as soon as a host frees. `sessions.idleRetireMs` (integer 60000–604800000 milliseconds, default 21600000 / 6 hours) ends hosts idle that long on the 60-second sweep (`persona_host_retired … reason=host_idle`). Ending a host keeps its session: the next turn resumes it with full history (`session_resumed … reason=idle_dead_binding`), and a host with background jobs is skipped. `sessions.idleResetMs` (optional integer 60000–2592000000 milliseconds; unset disables it) instead rotates quiet non-loopback, non-monitor session contexts like `/new`, so the next turn starts with fresh GJC history. It skips running turns and rows with no bound GJC session after an earlier reset, avoiding repeated no-op epoch changes. All three settings are reloadable; a lowered cap applies to the next bind and sweep and never kills a busy host.

For delegated long work, use `gajaeway work start <name> --cwd "<absolute_worktree_path>" "<task>"` for an existing checkout, or `gajaeway work start <name> --repo <absolute_path_or_git_url> [--base <ref>] "<task>"` to create a separate worktree. Relative `--repo` paths are resolved by the CLI; `--base` requires `--repo`. A repo-backed worktree must live outside the gateway service directory, is bound to the selected repository's push remote, and reports its repository, work branch, and baseline SHA in `work status`. Observe with `gajaeway work status <name>`, and send additional input to its open attempt with `gajaeway work steer <name> "<input>"`. Start returns an accepted receipt, not completion, and binds to the calling conversation (or the configured owner target if no known caller id is supplied). A settled start result is routed to its parent as an internal lane report turn, not as a human message. Lane parents receive reports through a durable inbox; an open parent attempt is steered, otherwise a new parent attempt is woken. Reports follow lane -> persona -> human. Runs remain response-only and never report; a run from a lane session is refused unless `work.allowNested=true`. Status is read-only: it does not bind, settle, refresh activity, or clear a hold. Steer never rebinds or starts a successor; `work steer acceptance uncertain` must not trigger an automatic retry with a new reference. An open attempt refuses another start/run even with `--resume`; steer or wait instead. A caller timeout (`work wait timed out; attempt remains observable`), disconnect, or shutdown ends only the wait, not the worker attempt.

`GJC_SESSION_ID` is forwarded as `callerSessionId`, an untrusted routing hint on the owner-trusted socket. Socket clients are owner-equivalent, so this is not a security boundary; the nested-lane check is a policy guard. If report admission is refused, exactly one `[lane ...]` fallback entry is committed to the parent origin's delivery ledger. The ledger row is durable once, but the physical post is at-least-once and may duplicate after a crash. `work jobs` shows pending, claimed, held, and undeliverable report counts as `reports=p:N c:N h:N u:N` when any are non-zero; held and undeliverable entries require operator attention.

Settled lane reports use `[lane <name>] completed|failed|attempt_ended: …` with bounded, safe content, never raw SDK exceptions. Cancellation and incomplete outcomes are not success. Uncertain or missing-receipt reports say `attempt_ended` plus the true safe reason while retaining the actual end state and hold. Completed work whose answer cannot be proven says `completed: output_unavailable`; synchronous run reports `work output unavailable` without rewriting known broker completion. Uncertain/incomplete outcomes with unavailable output include both the outcome reason and `output_unavailable`. A `receiptState=missing` status does not settle a lane until its matching `agent_end` or `agent_failed` lifecycle frame proves the turn ended; without that proof the attempt stays open/running. After lifecycle end, output retrieval continues within its budget because the SDK may attach the final body late. If a late final body proves present, the attempt settles `completed` once, with no resend. `attempt_ended: terminal_missing_receipt: final_response_missing opRef=<opRef>` means the SDK recorded no final response text. It does not mean the worker's commits or PRs were lost. Verify the artifacts in the repository, then continue with `work start <name> --resume` or retire the lane. Do not edit the database or restart the finished work. Silence suppresses a lane report only when proven from this attempt's complete original final content before prefixing/truncation; a summary token, neighboring reply, intermediate text, timestamp alone, or completed pagination cannot prove final output or silence. Silence evidence is durable before dependent tail checkpoints, so later missing output cannot resurrect a report. Missing proof means unavailable, not assumed silence. Output retrieval is limited to three durable post-terminal reads (immediately, then after 1 second and 5 seconds), ending earlier for definitive unavailability or invalid attribution; this is not a worker-duration limit.

The ActionGuard notice is prompt guidance to use owned start/status/steer lanes and avoid raw GJC launches, not a new enforcement hook. The gateway controls only positively proven gateway-created sessions under the active database authority. A visible session ID alone is not ownership. It never adopts unrelated sessions or runs global GC. Host skill/script replacement, deployment, and process cleanup remain separate operational work.

`monitorContextFailureRollThreshold` (1–20, default 2) is the monitor safety net, not a turn ceiling: native gjc auto-compaction keeps monitor sessions bounded, and a monitor that keeps answering is never rolled however many turns it takes. The epoch rolls only after this many **consecutive** context-class authoring failures (empty response, context-length rejection, zero-token completion) attributable to the current session AND a native-compaction request that came back `unavailable`/`failed`/`skipped`; the new session's first prompt then carries a digest of the monitor's instruction and its recent authored notes. Any healthy answer resets the streak, and failures that are executor-class or protocol-class (malformed or off-contract answers), or that come from a reconcile-replayed stale event or a dead epoch, never count. Executor-class failures report a sub-kind: `aside_timeout` and other worker/tool/lock failures, and `orphaned_executor` for a child process killed on the wrapper's timeout while the external daemon's job kept running — that one means "reclaim the external executor", never "roll the session". See `docs/monitors.md`.

`monitorCatchUp` (maxSlots 1–1000, default 24; maxAgeMs 60000–604800000, default 86400000) bounds durable cron replay after downtime. Refused slots advance the cursor, are logged as `monitor_slots_skipped`, and appear in `monitors inspect` as `catchUp`; changes require a restart.

`turnTimeoutMs` was removed with the persistent-session cutover. Configuration containing it is rejected; `stallTimeoutMs` remains the alert threshold for tail silence. A current persona turn is status-reconciled at the first stall. If the tail stays silent and status remains nonterminal for ten minutes after that stall, the gateway signals only its positively identified, gateway-owned session host and confirms that process identity has exited; it allows a bounded SIGTERM grace period, then rechecks operation status and escalates to SIGKILL if still authorized. If status becomes terminal after SIGTERM, escalation stops, the terminal operation follows normal reconciliation, and the signaled host remains counted but is not reused until its process identity exits. If host exit cannot be verified, cutoff is not authorized; a nonterminal accepted turn remains held, and an already-terminal turn may settle while its host continues to occupy capacity. After confirmed host exit, the gateway reads operation status again: terminal status follows normal reconciliation; nonterminal or `unknown` status fails the accepted trigger without replay, posts a cutoff notice, and releases queued inbound messages; an unreadable status remains held for recheck. Any resumed tail activity before termination resets the deadline. An unreadable or `unknown` status alone never authorizes cutoff; it does not prove the operation never terminalized, and an unverified host exit keeps the host counted and non-reusable.

### Automatic reset after a recognized provider failure

A persona turn rejected with the exact provider HTTP 400 `Unknown parameter: 'input[N].status'.` or a recognized context-exhaustion rejection can retire its session for **the next message**. The gateway corroborates the failure against bounded, private, current-session transcript evidence; a generic `Prompt submission failed`, high context occupancy alone, authentication/rate-limit error, or quoted error in chat/tool output is not reset authority. Missing, malformed, ambiguous, or stale evidence leaves the ordinary failure path unchanged.

The failed trigger is settled once and **never automatically re-executed**: the installed SDK does not attest that the whole operation had no external effects. This remains true when the turn already executed tools, sent partial output, or accepted steers. Pending messages and uncertain steer attribution are preserved; automatic reset does not use `/new`'s pending-message discard behavior. Reset uses existing native SDK session operations through `SessionPort`, not a private broker, shared-daemon replacement, or resubmission of old accepted work. Only the currently owned failed session under the active authority may advance its epoch; retired failures cannot reset a replacement session. Unrelated user sessions and global configuration remain untouched.

The reset budget is durable: one consecutive automatic reset per origin, with per-trigger deduplication across restarts. Another failure on the fresh session does not start a reset loop. A healthy completed reply or explicit operator `/new` permits a later automatic reset. This is recovery from an unusable conversation, not a serializer fix: repeated `input[N].status` rejection requires correcting the upstream gjc provider request format, not repeated session creation. No preset change, deployment, or GC deletion is part of automatic reset.

### Steer delivery is not session replacement authority

A refused steer remains pending until the current turn finishes, then is sent on the same session. Missing, mismatched, or contradictory acceptance receipts remain uncertain; only a receipt carrying the exact requested `clientRef` can prove acceptance. A failed steer must never retire a running turn or mint another session. Only explicit operator reset or independently proven, narrowly classified irrecoverable session errors may authorize replacement.

Periodic recovery also visits origins whose only remaining work is an uncertain steer on an already-terminal turn. After gateway restart, it resolves that original session/op/clientRef without requiring another user message. A confirmed acceptance consumes the held row without a new send; a confirmed refusal releases it for the next turn on the same session. Unresolved outcomes remain held rather than being duplicated.

### Relay-owned turns

Every bound persona session has one resident `gjc sdk serve --stdio --session <id>` relay; it is the turn's only transport. The gateway writes SDK frames to its stdin (`hello`, then `turn.prompt` / `turn.steer` as `control_request`, `turn.result` as `query_request`) and, because the prompt was submitted on that connection, the host streams the turn's own content back on stdout: `agent_start`, every assistant `message_end` (delivered to the channel as mid-work speech the moment it arrives), `tool_execution_*` (presence), and `agent_end`, all stamped with the turn's `commandId`/`turnId`. There is no `gjc sdk session tail` polling, no cursor, no transcript re-read for a live turn, and no text de-duplication: a healthy turn logs no `tail_poll`, `tail_error` or `tail_frame_foreign` lines. A relay that dies mid-turn logs `tail_relay_lost session=… opRef=…` then `recovery_hold … reason=relay_lost_mid_turn`; the remaining mid-work speech of that turn is lost (host content is best-effort), the handle reopens with backoff, and the turn settles from `turn.result` plus its original-result body on a 250 ms–5 s recheck. `tail_stream_dead session=… reopens=6` means six consecutive sub-5-second relay deaths: check that `gjc sdk serve --stdio --session <id>` runs by hand under the gateway's environment before anything else.

A message that arrives while a turn is running is steered into that turn, and its header tells the persona to answer it in its own mid-work reply before resuming the original request. That reply reaches the channel as mid-work speech, and the moment it is consumer-visible the turn's steer window closes (#167): a later message then starts its own turn instead of being folded into the same batch. This is what keeps a burst of messages from collapsing into one late answer.

### Replies from runtime-started runs

A persona can start background work inside a turn (an async `task` subagent, an async `bash` job, or a command folded into the background by a steer). When that work finishes after the turn has ended, gjc starts a run on its own to hand the persona the result. That run has no gateway submitter, so nothing it writes is streamed to the gateway and none of it reaches the channel. Before this was handled, such a reply was lost without a trace (the persona believed it had answered; the user saw nothing).

After every settled turn the gateway keeps that turn's relay open as a lifecycle-only observer for 2 minutes (`AGENT_RUN_GRACE_MS`), extended while the session reports running jobs, non-idle activity, an unfinished runtime-started run, or an unavailable probe, and never beyond 6 hours after the settle. An uncorrelated `agent_start` on the observer opens a runtime-started run; the next terminal closes it. About 3 seconds after it closes, if no jobs are still running, the gateway enqueues one internal notice (coalesced while undispatched) telling the persona that text from those runs was not delivered and asking it to send anything the user should see, or `[SILENT]`. The notice is an ordinary gateway-owned turn, so the answer goes through the normal delivery ledger. Delivery is best effort and costs one extra model turn: the persona restates its result rather than the gateway replaying the original text, and a run that starts and finishes entirely while the gateway is down, leaving no running jobs, is not reconstructed. Prefer `gajaeway work start` for delegation whose result must reach the channel.

Log lines: `agent_run_observer_open|closed origin=… session=… reason=…`, `agent_run_observer_lost` (warn), `agent_run_autonomous_end`, `agent_run_unattributed_end` (an uncorrelated terminal with no observed start; no notice), `agent_run_notice_deferred … jobs=N`, `agent_run_notice_coalesced`, `agent_run_notice_enqueued … messageId=agent-run/…`. To check a missing background reply, grep `gateway.log` for `agent_run_` on the origin, then look for the notice's `inbound_messages` row (`message_id LIKE 'agent-run/%'`) and its `deliveries` row.

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
gajaeway monitors update <monitor-id> --schedule '<cron>'
gajaeway monitors update <monitor-id> --enabled false
gajaeway monitors list
gajaeway monitors inspect <monitor-id>
gajaeway monitors test <monitor-id> --type changed --payload '{"source":"manual"}' --wait=60
```

Declare event types with `add`; use a partial JSON update to change them later. `update` changes a monitor in place, preserving its ID and event history; its `--enabled` option requires an explicit `true` or `false`. Use `test --wait[=SECONDS]` to observe a submitted event’s stage transitions over the event stream without repeatedly inspecting the database; use `inspect` for recent history before retrying a trigger. Webhooks are dangerous when exposed beyond loopback: set an explicit non-loopback bind only when required, put it behind authenticated ingress, and require authentication at that ingress. Do not expose an unauthenticated webhook directly to the Internet.

## Memory

```sh
gajaeway memory audit
gajaeway memory search 'query terms'
```

Memory changes are durable intents stored in the gateway database, committed in the private memory Git repository, and marked `receipted` in the intent row after the commit is verified by its `Gajaeway-Mutation-Id` trailer. There is no separate receipt file. Audit before manual repair; search returns bounded excerpts. Investigate quarantined mutations rather than editing intent state or Git history to hide them.

## Runtime cycle

`gajaeway ops cycle` projects one operator-readable snapshot of where every runtime cycle stands: aggregate phase, per-origin session identity with epoch, durable inbound queue depth, delivery settlement, memory closure, and monitor settlement. The result includes up to the five newest monitor dispatch failures with their event, code, and sanitized detail:

```sh
gajaeway ops cycle                                  # first 100 sessions; exits 1 when any gate is present
gajaeway ops cycle --offset 100                     # next session page
gajaeway ops cycle --limit 500 --offset 1000 --json # typed page and summary for scripting
```

The `sessions` details are paged (`--limit` 1–500, default 100; `--offset` defaults to 0). JSON includes `sessionsTotal` and `sessionsOffset`; aggregate counts, phase, and gates always cover the full home, not just the displayed session page. Older parameterless protocol clients receive an explicit error when the result has more than one page rather than a silently incomplete session list. An upgraded CLI rejects responses from a gateway that lacks pagination or monitor-failure diagnostics; restart the daemon after updating it.

Phases are `idle`, `dispatching` (turn work accepted or claimed from the durable queue), `delivering` (unsettled ledger deliveries), `draining` (memory closure in flight or durable unsettled intents), and `degraded`. A session shown as `(rebinding)` has a bumped epoch with no bound session yet — the next turn rebinds it.

`gajaeway ops cycle` is a read-only projection; any gate makes it exit 1. Its gates include session/delivery uncertainty, memory closure and `memory_git_index_locked`, monitor settlement, lane capacity, inbound starvation, GJC version verification, broker churn/index-lock conditions, and broker-agent provisioning. The JSON field `monitorFailures` exposes the five newest failure rows; details are sanitized and bounded when projected, including URLs and absolute paths retained in older records, while raw SDK payloads are not included. Use these details to diagnose dispatch failures without querying the database. The result also includes `brokerAgent` source/copy drift and `agentDisk` headroom. A `memory_git_index_locked` gate means Git reports the memory repository's index lock path; identify the Git process that owns it and let it finish rather than deleting the lock by age. For `broker_agent_unprovisioned` or broker-agent drift, inspect `gajaeway-gateway broker-agent status [--json]`. `setup --broker-agent` is a force provision, not a harmless status check; review drift and the pending-invitation queue before running it.

## Backup and restore drill

With the daemon running, take an online SQLite backup and validate it:

```sh
gajaeway ops integrity
gajaeway ops backup /absolute/backup/gateway.db
gajaeway ops redeliver <deliveryId>
gajaeway ops redeliver --since 2026-09-23T10:00:00.000Z
```

For restore, use `gajaeway-gateway restore <backup> [--abort]` after stopping the gateway service and disabling automatic restart. The restore command takes the gateway lease, validates a self-contained backup, stages it in the home, journals each preservation/install phase, preserves the live database and sidecars in `$GAJAEWAY_HOME/backups/pre-restore-<id>/`, installs the backup, and runs a post-check. The journal is `$GAJAEWAY_HOME/restore.journal.json`. If interrupted, repeat the command with the same backup to resume; `--abort` rolls back only before installation:

```sh
GAJAEWAY_HOME=/absolute/home gajaeway-gateway restore /absolute/backup/gateway.db
# For a pre-install interrupted restore, restore the original live set:
GAJAEWAY_HOME=/absolute/home gajaeway-gateway restore /absolute/backup/gateway.db --abort
```

Exit codes from `gajaeway-gateway restore` are: `0` restore completed; `1` refused (for example a held gateway lease, invalid backup, absent journal for `--abort`, or abort after install); `2` usage error; `3` rollback completed (explicit abort or automatic rollback of an unusable staged backup); `4` unrecoverable restore/journal state. If an incomplete restore journal exists, code 4 leaves it as evidence and normal startup remains blocked; do not remove it or replace files by hand. After a successful restore, start the service and verify `gajaeway ops integrity`. Practice on a disposable home first.

### Explicit broker-authority cutover

Changing the configured `gjc.agentDir` changes database authority; it is not a routine restart or adoption of sessions from another agent directory. The default target is `$GAJAEWAY_HOME/broker/agent`. Stop the gateway service and disable automatic restart for the entire census/cutover window. Keep the home private and ensure no restore journal is present.

1. Set `GAJAEWAY_HOME=ABS`, where `ABS` is the verified absolute home. Confirm `config.json` `gjc.agentDir` resolves to the intended absolute target; cutover requires the `--agent-dir` argument to resolve to that same path.
2. Run the read-only census and retain its JSON report. It includes the database authority and counts, integrity check, lease-owner diagnostics, and open quarantined work:

   ```sh
   GAJAEWAY_HOME=ABS gajaeway-gateway census
   ```

3. Review the current authority and open-work counts, then apply the explicit cutover:

   ```sh
   GAJAEWAY_HOME=ABS gajaeway-gateway cutover --agent-dir CANONICAL
   ```

   The offline command requires the gateway lease, validates the configured target, creates an integrity-checked `backups/pre-cutover-*.db`, writes the matching `.open-work.json` export, and records a cutover receipt. It preserves history and quarantines old accepted or unproven work; it does not stop broker processes or replay old work. Retain the census, backup, export, and receipt.

4. Treat the open-work export as an evidence inventory. Review its `inbound`, `work` (lane jobs, attempts, reports, invalid rows), and `monitors` entries against the external effects each may have caused. There is no import/replay command for this file. Resolve each item through the responsible system or operator workflow; do not bulk-replay it or rewrite session IDs. Only after cutover succeeds should the broker service and gateway be restarted against the configured private target.
The transaction preserves immutable historical snapshots and quarantine records, retires old session identities, advances origin epochs, and installs the single active target authority. It neither controls old broker sessions nor settles or replays their work. Accepted or unproven old work remains non-replayable; old private held lanes remain under their original authority and must not be converted to another authority's lanes. Old worker names remain reserved: use new names for new work, not `--resume`, a no-op run, or rewritten session IDs. The cutover does not authorize restoring shared configuration, host reaping, or session deletion. `ops cycle` treats an unbound origin as cutover history only while no inbound row, lane job, or monitor event for it exists outside quarantine and its `last_activity_at` is not after the cutover; any post-cutover admission evidence, including an inbound turn whose first bind fails, gates it again until it binds.
5. Re-enable managed startup only after apply succeeds and the executable path explicitly configured in the service unit matches the normal user's verified `command -v gjc`, with the same canonical user profile and agent directory. Start recovery only under that target authority. Verify a newly gateway-created session has exactly the same ID in the gateway and the normal user's SDK; compare unrelated sessions and configuration fingerprints with the pre-cutover record, and verify the same user daemon survives a subsequent gateway restart without resend of accepted work.

The database is currently schema 37 and still contains `broker_quarantine` and `broker_cutovers`. Preserve the cutover evidence and quarantine records during upgrades and authority changes.

### Persistent-session binary rollback

A code rollback is safe only after the current gateway has reconciled its open turns. Stop intake, verify `gajaeway ops integrity`, then stop the gateway service. Gateway shutdown releases its `gateway` and `memory` leases; it does not control the separately supervised broker service. Quarantined work is preserved evidence, not proof that an old operation stopped. Do not delete broker directories or rewrite authority to force a rollback. Restore only a binary/database combination whose schema and recorded authority are understood, then verify a fresh positively owned session and delivery. Refuse rollback if quiescence or authority cannot be proved.
### Persistent-session schema rollback

Do not down-mark a live database. Schema 19 rebuilt `inbound_messages` (the batch columns are gone; `turn_role`, `turn_epoch`, `turn_state`, `turn_op_ref`, `bound_session_id`, `dispatched_at` and `terminal_delivery_id` remain), so there is no in-place down-marker: rolling back to a pre-19 binary means restoring the backup taken before the upgrade. First stop external intake while the current gateway is still running, reconcile every current and retired turn until the nonterminal-trigger count above is zero, verify integrity, and only then swap binary and database together. If quiescence cannot be proved, refuse the rollback.

## Crash recovery

On adapter negotiation and every 15 seconds while connected, the delivery ledger redelivers due unsettled output. Ambiguous prior delivery is visibly duplicate-labeled. Failures store allowlisted error codes; five definitive failures or age beyond 24 hours expires an unsettled row. Requeue eligible rows with `gajaeway ops redeliver <deliveryId>` or `gajaeway ops redeliver --since <iso>`; confirmed rows are never requeued. Memory closure resumes durable intents and verifies successful Git closure from the commit trailer before marking the intent `receipted`; irrecoverable intent work is quarantined. Monitor reconciliation resumes admitted, dispatched, or failed events and repairs authored events whose memory closure is missing.

Worker recovery independently resumes observation of the saved operation/session identity before admitting conflicting worker mutations. It never resends the prompt, binds a replacement, or requires a no-op run to reconcile an open attempt. Proven-live `unknown` remains observable without settlement or overlap; it proves neither acceptance nor completion. `work send acceptance uncertain` likewise requires observation, not resubmission. Trusted terminal evidence can finish saved reconciliation without live reattachment. Without trusted terminal proof, unrecoverable dead/disowned/indeterminate authority is closed locally as `terminal_uncertain` and held for the operator; the local end timestamp is not proof of worker stoppage. Transient transport failure does not invent a worker deadline. Repository progress cannot clear sticky uncertainty/failure holds; `--resume` is an explicit continuation choice only after the prior attempt is reconciled, not a way around an open attempt.

## Troubleshooting

- **Socket missing:** start the out-of-band service and verify `socketPath`, directory permissions, and service logs.
- **Newer-schema refusal:** do not downgrade against that database. Restore a compatible backup or run a gateway that supports its schema.
- **Quarantined mutation:** inspect the intent payload, memory repository state, and Git error; repair the source condition, then use the documented recovery workflow rather than deleting the evidence.
- **Backup failure:** supply an absolute target path whose parent directory already exists; never target the live `gateway.db`.
- **Every turn on one origin fails with the same gjc `api_error` (for example "cannot restore Claude OAuth MCP tool alias"):** the persistent broker-hosted session may be poisoned. Send `/new` to create a fresh epoch; prior in-session context is lost by design. The daemon log carries the exact error and the conversation receives the `[turn failed]` notice.
- **Repeated structured recovery signals:** `recovery_hold` and `retired_hold` retain a durable turn; `stall_alert originKey=… sessionId=… silentMs=…` is alert-only. Broker-generation changes reflect observed GJC runtime changes, not permission for the gateway to replace the daemon. None of these signals permits a blind resend or abort. Within the same proven authority, use `/new` only to establish a fresh epoch for later traffic; accepted old turns retain their original recovery identity and remain fenced until status/tail proves a terminal result. `steer_failed … action=recover` means reconcile the current turn, not replace its session: an authoritative refusal leaves the message pending until that turn ends, then sends it on the same session. Ambiguous receipts and transport failures (`steer_ambiguous … action=replay`, `steer_hold … reason=transport_torn`) require observation and reconciliation with the same `clientRef`, never a new send or reference. Steer failures never authorize replacement, an epoch bump, or retirement; replacement requires explicit operator `/new` (or `/reset`) or a separate, narrowly proven irrecoverable session condition, not an inference from failed steering. Historical `session_rebound_after_steer_failure` logs identify removed unsafe behavior, not the current recovery policy.
- **Unaccepted retired turns versus authority quarantine:** an independently retired turn whose send never landed can be requeued only with positive evidence that it was unaccepted and the session is dead or disowned. Accepted or unproven work from a prior authority remains quarantined evidence and is never requeued into a different private agent. Inspect the saved authority and cutover receipt before recovery.
- **`lane_capacity` / `lane_capacity_exhausted`:** inspect `gajaeway work jobs` and `gajaeway work status <name>`, then use `gajaeway work retire <name>` only when its eligibility checks pass. An open attempt or uncertain terminal state is not cleared by a PID, transcript, or worktree change. Do not edit the database or delete session rows to free a lane; quarantined lanes remain evidence under their previous authority.
- **A lane is `running` but its output stopped:** inspect `gajaeway work status <name>` and `gajaeway ops cycle`. Recovery observes the saved operation and session identity; transport failures do not settle an attempt. Do not submit a duplicate prompt or start a replacement until the original attempt is reconciled.
- **Session bootstrap remains pending:** inspect `session.list` or `ops.cycle`. A pending projection means the current epoch has not completed a terminal successful turn yet; pre-success failures deliberately retry it. Repeated attempts carry the same origin+epoch bootstrap ID. Do not mark it complete manually or copy source bodies into the database. Repair unreadable memory files or rejected links at their source. In public/group channels, associated channel/project/task/handoff documents need an exact full `origin:`/`origin-key:` match and `bootstrap-safe: public` (or `bootstrap-visibility: public`); bare conversation IDs never qualify. Daily sections are admitted only when every stable-origin declaration is well formed and resolves to one consistent current-origin key. The configured `memory/` root may itself be a symlink, but every followed target must remain beneath that resolved memory root.
- **`inbound_starved` or stalled turns:** inspect `gajaeway ops cycle` and the session status/recovery fields. It is a gateway-owned session recovery condition; do not launch another GJC process or resend an operation with a new reference.
- **Broker service unavailable or churning:** inspect the service-manager status and logs, then read `gajaeway-gateway broker-agent status [--json]` and `gajaeway ops cycle`. `broker_respawn_churn` and `broker_index_lock_blocked` are gates, not permission to delete broker state or a lease file. The gateway and broker host set `GJC_SDK_BROKER_AUTOSTART=0`, so GJC clients never start a replacement broker: the broker unit is the only lifecycle authority, and `broker_unavailable` from a client means the service is down, not that a client should restart it.
- **Broker alive but not answering:** the broker host probes its own broker every 15s (fresh discovery heartbeat for the child pid plus a `session.get_endpoint` round trip, 10s bound). Until the broker first answers it gets 5 minutes (reboot recovery can be slow); after that, 90s of continuous failures makes it log `broker_host_unresponsive`, sends SIGTERM to the broker's private process group (including `session-host-internal` children), escalates to SIGKILL after 20s (`broker_host_stop_timeout`), waits for the group to exit, then exits 1 so the service manager restarts it. On macOS this terminates the broker's inherited SDK-host subprocesses together; Linux also has systemd's broader `KillMode=control-group` fence. Repeated replacement-broker `startup-deadline` failures or `persona_host_retired ... outcome=refused detail=inspect_failed: broker_unavailable` with `host_capacity` holds can indicate orphaned session hosts from an older broker host; check for stale `session-host-internal` processes and preserve the broker's `sdk/lifecycle-ledger.jsonl`. Do not kill unrelated ppid-1 processes. Expect about two minutes from wedge to a fresh broker, inside the gateway's 195s live-outage exit. Repeated `broker_host_unresponsive` lines are a GJC defect to report with the broker's `sdk/lifecycle-ledger.jsonl`, not a reason to disable the check.
- **`broker_agent_unprovisioned` or entry drift:** status reports provision state and per-entry source/copy differences. Review it before `gajaeway setup --broker-agent`, which force-provisions the allowlist and can replace private copies. Stop the broker service before provisioning. The current schema is 37; `broker_quarantine` and `broker_cutovers` remain broker-authority evidence.
- **`gjc_unverified_version`:** the detected version exceeds the gateway's verified error contract. Test the pinned release against the gateway contract before changing the pin; the current GJC package pin is 0.18.11.
- **`memory_git_index_locked`:** Git reported an existing `.git/index.lock`. Identify its owner; the closure queue retains the intent for recovery and `ops cycle` clears the gate when the lock is gone.
- **`config.json is unreadable (...); refusing to start on defaults`:** repair the file or its target before restarting. An unreadable configuration does not silently become defaults.
