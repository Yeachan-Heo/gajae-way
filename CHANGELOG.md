# Changelog

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
