# Migration Guide: Hermes / OpenClaw → gajae-way

This document guides operators migrating agent configurations and data from Hermes (NousResearch/hermes-agent) or OpenClaw to gajae-way.

## Overview

The `gajaeway migrate` command automates the migration of:

- **Personas and identity**: workspace configuration files
- **Memory and notes**: long-term curated memory, preserving timestamps
- **Channel/adapter configuration**: Discord, Slack, Telegram channel bindings and credentials
- **Schedules**: cron jobs and monitoring rules
- **Skills**: custom skills and workflows

Unmappable entries are reported (never dropped silently), and the process is idempotent—re-running produces the same outcome.

## Quick Start

### Detect Source Layout

```bash
gajaeway migrate [--source /path/to/source] --dry-run
```

The tool auto-detects whether the source is Hermes or OpenClaw and reports what will be migrated:

```
Detected hermes configuration at /path/to/source

Personas: 2
Schedules: 5
Memory entries: 12
Skills: 3
Channels: 2

[DRY RUN] Output would be written to:
  workspace/SOUL.md (persona)
  workspace/USER.md (persona)
  memory/imported/hermes/MEMORY.md (memory)
  gateway/config.json (channels)
  migration/monitors.json (monitors)
  …
```

### Apply Migration

```bash
gajaeway migrate /path/to/source --target ~/.gajaeway
```

The default target is `~/.gajaeway`. Files are written to the target home, and the migration is logged.

```
Migration complete. Output written to /home/user/.gajaeway
```

### Unmappable Entries

Some source items cannot be migrated automatically and are reported:

```
Unmapped entries (these will not be migrated):
  credentials:
    - .env OPENAI_API_KEY: provider/tool credential; gajae-way sessions use the shared gjc agent profile, so set it there if still needed
    - .env ANTHROPIC_API_KEY: setting has no gajae-way equivalent
  monitors:
    - cron/jobs.json job-name-1: cron expression … uses syntax the gateway cron trigger does not evaluate
```

Review these entries and add them manually if needed.

## Source Formats

### Hermes Agent

Hermes keeps its configuration under `~/.hermes` (or `$HERMES_HOME`):

```
~/.hermes/
  SOUL.md                     → workspace/SOUL.md
  AGENTS.md                   → workspace/AGENTS.md
  USER.md                     → workspace/USER.md
  memories/MEMORY.md          → memory/imported/hermes/MEMORY.md
  .env                        → gateway/config.json (credentials)
  config.yaml                 → gateway/config.json (channel policies)
  cron/jobs.json              → migration/monitors.json
  skills/<category>/<name>/   → workspace/.gjc/skills/<name>/
```

**Reference**: https://github.com/NousResearch/hermes-agent

### OpenClaw

OpenClaw configuration is held in `config.json` and `.env`:

```
./
  config.json                 → gateway/config.json, migration/monitors.json
  .env                        → gateway/config.json (credentials)
  skills/                     → workspace/.gjc/skills/
  memory/                     → memory/imported.md
```

## Mappings in Detail

### Personas and Identity

Hermes: `SOUL.md`, `AGENTS.md`, `USER.md` → gajae-way: `workspace/` directory

OpenClaw: `config.json` persona field → gajae-way: `persona.json`

These files define the agent's instructions and identity.

### Memory

Hermes and OpenClaw memory is imported into a custom memory axis (`memory/imported/`) to distinguish curated data from operational memory:

```
memory/
  imported/
    hermes/MEMORY.md     (preserved timestamps and structure)
    openclaw.md          (combined memory entries)
```

The gateway includes imported memory in audits and searches but tracks it separately.

### Channel Configuration and Credentials

Credentials (bot tokens, app tokens) are extracted and written to secret files with restricted permissions:

- Hermes `.env` → `gateway/secrets.env` (0600 permissions)
- OpenClaw `.env` → `gateway/secrets.env`

Channel policies (engagement mode, allowlist, home channel) are merged into:

```
gateway/config.json
```

**Supported channels**: Discord, Telegram, Slack

**Unmappable**: Multi-workspace tokens (comma-separated in Hermes `.env`) are not migrated.

### Monitors (Cron Jobs)

Hermes `cron/jobs.json` and OpenClaw `config.monitors` are converted to monitor specifications:

```
migration/monitors.json
```

The gateway imports these monitors once at boot. Monitors are idempotent: re-running the migration does not duplicate them.

**Supported schedules**:
- Cron expressions (standard 5-field syntax, no names, no `?`)
- Intervals that divide an hour or day evenly (e.g., every 15, 30, 60 minutes)

**Unmappable**: One-shot jobs, monitor pre-checks, and interval-based schedules that do not divide the day.

### Skills

Hermes: Custom skills from `~/.hermes/skills/` → `workspace/.gjc/skills/`

OpenClaw: Skills from `skills/` → `workspace/.gjc/skills/`

Bundled/built-in skills are not migrated (they ship with their respective runtimes).

## Safety Features

### Dry-Run Mode (Default)

All migration commands default to `--dry-run`, which reports what would be written without making changes:

```bash
gajaeway migrate /path/to/source --dry-run
```

Output is logged to stdout and includes the target files and their categories.

### Apply

To write files, use the default (no `--dry-run` flag) or explicitly run:

```bash
gajaeway migrate /path/to/source
```

### Non-Destructive

The migration **never modifies the source**. It always reads and imports.

### Overwrite Protection

If the target workspace already exists, the migration refuses to proceed unless `--force` is set:

```bash
gajaeway migrate /path/to/source --target ~/.gajaeway --force
```

When `--force` is used, files that would be overwritten are backed up with a timestamp:

```
gateway/config.json.backup.2026-10-12T23-10-00Z
```

### Idempotent Re-Run

Running the migration multiple times on the same source and target converges to the same state. Merged JSON files (e.g., monitor specifications) are not duplicated.

## Secrets and Security

Credentials and tokens are:

1. **Extracted** from source environment files
2. **Never logged** in the dry-run or apply output
3. **Redacted** in the plan/report output
4. **Written** to secret files with 0600 permissions
5. **Merged** (not overwritten) into gateway configuration

Credentials are never embedded in monitor instructions or other human-readable output.

## Troubleshooting

### "No Hermes or OpenClaw configuration detected"

Ensure the source directory contains:
- For Hermes: `config.yaml`, `memories/`, or `cron/jobs.json`
- For OpenClaw: `config.json`

### Monitor import fails

Check the dry-run output for warnings and unmapped entries. Some schedules (non-divisible intervals, one-shot jobs) cannot be migrated. Add them manually to `migration/monitors.json` if needed.

### Credentials not imported

Verify that bot token environment variables use the expected names:
- Hermes: `DISCORD_BOT_TOKEN`, `SLACK_BOT_TOKEN`, `TELEGRAM_BOT_TOKEN`, `SLACK_APP_TOKEN`
- OpenClaw: `DISCORD_TOKEN`, `SLACK_TOKEN`, `TELEGRAM_TOKEN`

### Multiple tokens (Hermes only)

Hermes allows comma-separated tokens for multi-workspace operation. gajae-way runs one bot per adapter, so these are unmapped. Manually select the primary workspace token.

## Next Steps

1. Run a dry-run to review the migration plan.
2. Back up your source (recommended).
3. Apply the migration.
4. Review the gateway configuration in `~/.gajaeway/gateway/config.json`.
5. Verify credentials in `~/.gajaeway/gateway/secrets.env`.
6. Start the gateway and test channel integrations.
7. Add any unmappable items manually.

## Questions or Issues?

Refer to the issue tracker: [Issue #311](https://github.com/Yeachan-Heo/gajae-way/issues/311)
