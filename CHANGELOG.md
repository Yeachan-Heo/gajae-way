# Changelog

## [Unreleased]

### Fixes
- **adapter-slack**: Route explicitly bot-addressed session commands through gateway authorization without creating a top-level mention thread; dispatch Socket Mode interactive envelopes, preserve panel origin and underscore-containing option ids, and show duplicate-recovery warnings on panels. Unify live/recovery routing, retain edit origins and recovered parent authors, serialize complete delivery settlements per origin, classify partial writes and Slack server errors as uncertain, retain recovery cursors on local timeouts, and retry failed presence cleanup without treating it as confirmed.
- **gateway**: Preserve logical turn ids across streaming outputs and progress so Slack live replies update one message; terminal progress waits for queued Slack settlements and live edits before closing the reply.
- **monitors**: Retire size-rolled sessions through the broker's journaled `session.close`, a forced shutdown rather than an idle-only operation, preserving transcripts but aborting active work. Retirement records survive monitor removal and restart, and are swept on later fresh dispatches of their origin and by the gateway's 60-second monitor reconcile. The background sweep takes no origin lock, so a busy origin delays neither event recovery nor its own dispatch through the sweep. Inline retirement retains the existing dispatch lock. Both paths attempt at most two records per pass (globally across origins for reconcile), with persisted fair ordering, safe-integer counter repair, the same ownership, saved-pointer and idle checks, and an in-process guard against concurrent attempts on one record. Every attempt requires resolved saved operations, no saved pointer or live binding, positive idle evidence from three independent probes (turn, queue when implemented, then jobs), and synchronous final authorization immediately before invoking close. Active, unknown, throwing or missing required probes hold, including after failed closes or restart; diagnostic closing state grants no authority. Every close failure keeps the record for a later pass; nothing is deleted on inferred absence. Only a successful close removes an unchanged record; ports without close support relinquish unsupported records. Hosts that never answer retain their records indefinitely. Removing a monitor no longer abandons its retired sessions, but still clears its size arms and handoff rows. Clearing `contextRollTokens` still allows earlier roll cleanup. A background job can finish and wake an autonomous turn between probes, or work can start between the last probe and broker close, which may wait in the CLI pool. Such work is aborted by close. Fixing this known race requires an idle-fenced broker close, tracked in [Yeachan-Heo/gajae-code#7540](https://github.com/Yeachan-Heo/gajae-code/issues/7540). Retries may replay a stored refusal; cleanup has no bounded completion guarantee. Failure rolls retain immediate host termination.

### Features
- **gateway/images**: Forward static PNG/JPEG attachments from Discord, Slack, and Telegram as durable native model inputs through the `chat.images` capability; persist inbound image rows in schema 42 and keep non-native formats as attachment descriptions.
- **monitors**: Opt in to digest-seeded session rolls with per-monitor observed-usage threshold `contextRollTokens` to reduce carried-context cost; sampled after the turn only when configured, this is not a hard limit and a stale reading can still roll after native compaction. Persisted arms and handoffs survive restart, failure rolls take precedence, and internal diagnostics emit armed, disarmed, deferred, and rolled lifecycle logs. Size-retired saved results are harvested or held on their original session, and removing a monitor clears its size arms and handoffs while preserving pending retirement records.
- **adapter-telegram**: Send persona Markdown as Telegram HTML, split without breaking tags or entities; syntax the renderer cannot place exactly (images, link titles, unbalanced link parentheses, `_underscore_` emphasis) stays literal text. A retried multi-chunk reply resumes after the chunks Telegram already accepted, waits out short flood-control `retry_after` pauses, and expires once Telegram keeps refusing the same chunk (#654)

### Fixes
- **gateway**: Keep pending image-bearing messages out of text steering, include image payloads in prompt fingerprints, and validate exact relay frames before the durable send fence so oversized local prompts settle crash-safely as `prompt_too_large` with a visible failure notice instead of remaining uncertain.
- **adapters**: Preserve non-native image attachments, captions, sibling files, and GIF/WebP caption edits; restrict Slack image redirects, globally bound Slack/Discord image retrieval and decoding, and advance Telegram past durably recorded terminal protocol rejections while retaining transient retries.

## [0.4.2] - 2026-10-29

### Features
- **work**: Standing operator directives reach running and later lanes (`gajaeway work directive`) (#604)

### Fixes
- **gateway**: A held restart drain reports not-ready with its restart id; an inherited drain is held and reported at boot instead of silently fencing dispatch (#713)
- **gateway**: Escalate an unresolved hold on a retired epoch too, and bound status polling on escalated retired holds (#705, #715)
- **gateway**: Make `work.start` admission visible and idempotent across a caller timeout (#707)
- **gateway**: Settle `awaiting_checks` on `work.retire` so retired lanes cannot respawn (#716)
- **work**: Preserve SDK failure causes independently of receipts (#696)
- **monitors**: A failed event can no longer wedge an `overlap=skip` monitor; a running authoring turn is never aged out (#702)
- **adapter-slack**: Recover a closed socket and detect deaf event delivery (#710)

## [0.4.1] - 2026-10-27

### Fixes
- **gateway**: Requeue bound turns that never reached `turn.prompt` under a fresh op-ref; a CLI queue timeout is no longer treated as a broker outage (#675, #685)
- **gateway**: Bound unresolved recovery holds, fence escalated holds' inputs, and keep their late answers and escalation notices durable (#686)
- **gateway**: Detect busy-wait stalls and preserve a busy refusal only when the retry provably never reached the host (#687, #689)
- **gateway**: Alert the owner when an origin goes silent (#688)
- **gateway**: Keep an AGENTS.md update pending until a turn carrying it succeeds, and size the CLI pool from the effective host cap (#676)
- **gateway**: The schema 39 migration skips turns quarantined by a broker-authority cutover instead of failing boot

## [0.4.0] - 2026-10-26

### Features
- **gateway**: Support the GJC 0.19.0 runtime contracts and lifecycle fences; the pinned GJC is now 0.19.0 (#663)
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
