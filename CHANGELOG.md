# Changelog

## [0.5.0] - 2026-11-11

### Deps
- **gateway**: Pin GJC 0.20.0 (#786)

### Fixes
- **gateway**: Allow forced retire of idle open attempts as operator abandonment (#779)
- **gateway**: Don't post failure notice when stall cutoff finds properly concluded reply (#769)
- **gateway**: Keep an accepted authoring turn whose status is unknown (#746)
- **gateway**: Escalate holds on live-but-unresponsive hosts and surface lane-report backlogs (#767)
- **gateway**: Escalate terminal_uncertain/ambiguous holds and dedupe alternating reasons (#773)
- **delivery**: Drop a malformed Discord reply target instead of losing the reply; validate reply targets and handle Discord 50035 errors (#763, #764)
- **adapter-slack**: Keep the native working status on DM turns (#754)

### Features
- **gateway**: Opt-in per-monitor session roll on observed context size; retire size-rolled sessions through the broker's journaled `session.close`, preserving transcripts but aborting active work; retirement records survive monitor removal and restart, and are swept on later fresh dispatches and the gateway's 60-second monitor reconcile (#738)
- **adapters**: Forward static PNG/JPEG attachments from Discord, Slack, and Telegram as durable native model inputs through the `chat.images` capability; persist inbound image rows in schema 42 and keep non-native formats as attachment descriptions (#775)
- **adapter-slack**: Complete command routing, interactions, and delivery lifecycle; route explicitly bot-addressed session commands through gateway authorization without creating a top-level mention thread; dispatch Socket Mode interactive envelopes, preserve panel origin and underscore-containing option ids (#777)
- **adapter-telegram**: Send persona Markdown as Telegram HTML, split without breaking tags or entities; syntax the renderer cannot place exactly stays literal text; a retried multi-chunk reply resumes after chunks already accepted and respects Telegram flood-control (#774)

## [0.4.4] - 2026-11-04

### Fixes
- **gateway**: Pin GJC 0.19.3, which bounds and rotates the session-index rejection audit log, exits a broker wedged on a stalled discovery write, and keeps publication liveness SIGKILL off a full stderr (#752)
- **docs**: Move an oversized pre-0.19.3 `index-audit.jsonl` aside before the first 0.19.3 start, since 0.19.0 re-read the whole log at host startup (#752)
- **cli**: `services install|repair` installs and verifies the pinned GJC from the checkout (or the manifest beside release binaries) before writing service definitions, and fails instead of falling through to another pin (#753)
- **gateway**: A lane started from a monitor turn reports to the monitor's channel target (#739)
- **gateway**: Keep the authoring-turn pointer through a lease-fenced update, retire it in the same transaction as the `dispatched` transition so a stale attempt cannot erase a successor's pointer, and retire it before throwing on an empty response (#743, #744)

## [0.4.3] - 2026-11-03

### Fixes
- **cli**: `gajaeway update` with `SHELL` unset no longer assumes `/bin/zsh`; it tries the passwd shell, `/bin/bash`, then `/bin/sh`, and falls back to the default PATH instead of exiting 1 after the release is installed (#734)
- **adapter-discord**: A reply with a refused `MEDIA:` attachment still sends its text plus a `(file not sent: …)` note, without exposing host paths (#730)
- **gateway**: An uncorrelated `agent_end` is no longer treated as owned terminal evidence (#733)

### CI
- Move darwin-arm64 runners off the retiring macos-14 image (#732)

## [0.4.2] - 2026-10-29

### Features
- **work**: Standing operator directives reach running and later lanes (`gajaeway work directive`) (#604)

### Fixes
- **gateway**: A held restart drain reports not-ready with its restart id; an inherited drain is held and reported at boot instead of silently fencing dispatch (#713)
- **gateway**: Escalate an unresolved hold on a retired epoch too, and bound status polling on escalated retired holds (#705, #715)
- **gateway**: Make `work.start` admission visible and idempotent across a caller timeout (#707)

## [0.4.1] - 2026-10-27

### Fixes
- **gateway**: Gate `ops cycle` and repair-step `monitor_failed` event isolation and require a submitted-turn repair base (#712)
- **gateway**: Suppress `broker_unavailable` when the broker is visible but unreachable and surface what caused the failed endpoint fetch (#714)

## [0.4.0] - 2026-10-26

### Features
- **gateway**: Ledger operator directives (`/directive`) with outcomes and durable routing; directives reach monitors (#694, #695)
- **gateway**: `gajaeway stop` waits for stalled-turn repair and drains turns (#710)
- **gateway**: Deliver a monitor's *inferred* outcome from a still-valid cached result when GJC cannot be reached (#706)

### Fixes
- **gateway**: Pin GJC 0.19.0 (#663)
- **gateway**: Announce stack restarts to active conversations ("restarting", then "back online" or "restart cancelled"), configurable via `restartNotice` (#662)
- **gateway**: Default daily memory de-bloat cron for Markdown memory files over 100KB (#651)
- **gateway**: Suggest gjc-listed models in `/model set` autocomplete (#647)
- **telegram**: Upload `MEDIA:` file deliveries with `sendPhoto`/`sendDocument` (#639)

### Fixes
- **gateway**: Preserve native operation authority across recovery and persist confirmed stalled-host exits (#652)
- **gateway**: Preserve a busy refusal across restart without replay (#644)
- **gateway**: Fence observer settlement and drain relay teardown (#640)
- **gateway**: Skip journaling no-op attempt updates, compact superseded `attempt_updated` rows, and report journal size in census (#648, #649, #650)
- **gateway**: Bound journal retention sweeps to O(batch) so compacting a multi-million-row journal never blocks the gateway (#671)
- **gateway**: Include the top-level error message in failure details (#657, #660)
- **memory**: Bound memory git by a deadline and never wait on Bun reporting its exit (#656)
- **telegram**: Key inbound messages by chat-scoped Telegram ids so reactions resolve (#655)
- **adapter-slack**: Answer an @mention that invited the bot into the channel (#645)
- **adapter-slack**: Do not open an empty thread for a root-message status line (#658)

## [0.3.4] - 2026-10-21

### Fixes
- **gateway**: Report force-ended checks cycles atomically so a parent receives exactly one cycle conclusion (#641)

## [0.3.3] - 2026-10-20

### Features
- **gateway**: `[SHUTUP]` mutes a thread durably until a mention or reply-to unmutes it (#625, #626)
- **gateway**: Ask a UI elicitation through chat with gateway capabilities advertised (#619, #620)

### Fixes
- **gateway**: Bound lane git commands and add an event-loop watchdog (#615, #616)
- **gateway**: Recover repeated monitor prior-run failures (#627, #629)
- **gateway**: Resolve awaiting-checks PRs by pushed head SHA (#628, #630)
- **gateway**: Reconcile missing and saved receipts with runtime evidence; fence native restarts (#635, #637)
- **gateway**: Observe worker activity after relay reconnect (#636)
- **gateway**: Keep Discord channel lane reports at root (#634)
- **gateway**: Replay the work journal on boot and drop caller session-create retries (#567, #568, #586, #597)
- **gateway**: Monitors execute first instead of re-planning (#369, #623)
- **monitors**: Detect paraphrased contract echoes (#632, #633)
- **cli**: Hermetic restart-stack and order-independent gjc-install tests (#621, #622, #638)
- **docs**: Align README with implemented setup and runtime (#631)

## [0.3.2] - 2026-10-18

### Features
- **gateway**: Block-form `HANDOFF`/`SEND` routing tokens anywhere in a reply, many per turn; `SEND`/`REPLY:<channel>` post cross-room (#614)
- **gateway**: Report functional readiness (#608)
- **gateway**: Intent-first broker operation journal and broker readiness wait for `work.start` (#568, #570)

### Fixes
- **gateway**: Pin GJC 0.18.12 (#617)
- **gateway**: Re-prompt the persona after runs the runtime started on its own (#612)
- **gateway**: Roll a monitor session after a wedged prompt submission (#611)
- **gateway**: Terminalize stalled persona turns (#596, #599)
- **gateway**: Drain persona turns before shutdown (#601, #605)
- **gateway**: Recover invalid model creates and drain restarts (#602, #607)
- **gateway**: Diagnose monitor dispatch failures (#600, #606)
- **gateway**: Fix work lane checks lookup and retirement (#595, #598)
- **broker**: Measure the watchdog window in awake time, not wall time (#610)
- **discord**: Keep fenced `MEDIA` examples as text (#613)
- **slack**: Preserve mrkdwn link syntax (#593, #594)

## [0.3.1] - 2026-10-15

### Fixes
- **gateway**: Reap SDK session hosts with the broker on watchdog restart so replacements do not crash-loop (#573, #587)
- **gateway**: Page `ops cycle` session details so large homes stay under the 1 MiB frame limit (#574, #585)
- **gateway**: Use BSD `ps -E` so the boot reaper runs on macOS (#583)
- **gateway**: Settle a removed monitor's waiting events and slots (#576)

### Features
- **gateway**: Name provider policy refusals in the delivered turn failure (#575)

## [0.3.0] - 2026-10-15

### Features
- **gateway**: Redact secrets from every chat-bound frame for enhanced security [sionic-gajae]
- **gateway**: [WAKE:<origin>] token wakes another conversation (#442)
- **gateway**: Auto-wake watched conversations (#442)
- **gateway**: Rotate idle persona sessions on dev
- **telegram**: Forward inbound photos and files so the persona can read them
- **discord**: Send explicit local file attachments
- **monitor**: Inject results into conversation sessions
- **gateway**: Durable work journal for crash recovery (#567) with schema v36
- **cli**: Install release package manifest on update

### Fixes
- **gateway**: Harden awaiting-checks recovery and verify checks against current remote head
- **gateway**: Restore putLaneReport idempotency for awaiting_checks consumption
- **gateway**: Hold lanes until checks settle and settle merged checks + aborted parents
- **gateway**: Prevent collateral quarantine of healthy lanes
- **gateway**: Avoid completion without check evidence
- **gateway**: Include CI log tails in repair retries
- **gateway**: Preserve census diagnostics for quarantined work
- **gateway**: Bound CI log redaction and report observed heads
- **gateway**: Base the cutover exemption on post-cutover admission evidence
- **gateway**: Treat origins unbound by a broker cutover as history in ops.cycle
- **protocol**: Treat a mentioned [SILENT] as prose, not a silence directive
- **cli**: Skip services with missing binaries (#523)
- **cli**: Bootstrap unloaded macOS LaunchAgents (#524)
- **discord**: Restrict outbound media paths
- **gateway**: Log and surface the cause of work lane bind failures (Fixes #379) (#387)

### Merged from main
- **gateway**: Service directory check treats embedded module root correctly (#579 bunfs fix)
- Back-merged v0.2.1-v0.2.3 hotfixes into dev

## [0.2.3] - Previous release
