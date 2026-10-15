# Changelog

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
