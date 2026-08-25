# gajae-way

`gajae-way` (**gajaeway**) is a personal-agent gateway in the spirit of Hermes
and OpenClaw: one Gajae persona living across many strictly isolated
[gajae-code (gjc)](https://github.com) sessions, reachable chat-first from
Discord and Telegram, with a filesystem-first Markdown memory, unified
event-driven monitors, and a versioned SDK protocol that exposes every gateway
action and event to third parties.

Pure Bun/TypeScript. No Rust core, no TUI, no fail-closed ceremony — durability
comes from `bun:sqlite` transaction discipline plus explicit, tested recovery
paths.

## Architecture

```text
Discord adapter  ─┐                        ┌─ gjc session per origin
Telegram adapter ─┼─ @gajaeway/sdk ─ UDS ──┤   (idempotent-key create,
owner CLI        ─┘   (protocol v1.0)      │    resume per turn)
third-party apps ─┘                        ├─ filesystem memory (git closure)
                     gateway daemon ───────┼─ monitors (cron/webhook/watcher/script)
                                           └─ SQLite: sessions, delivery ledger,
                                              systematic event log
```

- **One Gajae, many sessions.** Every conversational origin (Discord
  channel/thread/DM, Telegram chat/topic/DM, loopback REPL) maps to its own
  isolated gjc session. Transcripts never merge; cross-session context flows
  only through on-demand, bounded, source-cited recall and canonical memory.
- **SDK-first.** Adapters and the owner CLI are ordinary consumers of the
  public `@gajaeway/sdk` — anything they can do, a third party can do.
  Enforced by the `sdk-boundary-dogfood` and `sdk-coverage-inventory` CI gates.
- **Memory by doctrine.** Canonical memory is a Markdown tree under
  `$GAJAEWAY_HOME/memory` with a map-only `MEMORY.md`, BM25 retrieval, and a
  crash-proven intent → write → git commit → receipt closure ladder.
- **Monitors.** A cron is just a monitor with a periodic trigger. Cron,
  webhook, watcher, and script triggers feed a seven-stage propagation
  pipeline: log-before-propagate, declared event types (unknown → catch-all
  session), per-monitor burst policies, one Gajae-authored memory record per
  event, channel output through the delivery ledger.
- **Honest at-least-once delivery.** A durable ledger redelivers unconfirmed
  replies on boot; mid-send ambiguity is redelivered with a visible duplicate
  label, never silently resent.

## Packages

| Package | Role |
|---|---|
| `@gajaeway/protocol` | Wire profile v1.0: frames, negotiation, verbs, events, origin normalization |
| `@gajaeway/sdk` | Public client (Unix socket + stdio) |
| `@gajaeway/gateway` | The daemon: sessions, memory, monitors, delivery, guard |
| `@gajaeway/adapter-discord` | Discord surface (SDK-only consumer) |
| `@gajaeway/adapter-telegram` | Telegram surface (SDK-only, zero platform deps) |
| `@gajaeway/cli` | Owner CLI: chat REPL, sessions, monitors, memory, ops |
| `@gajaeway/conformance` | SDK boundary + coverage CI gates |

## Quick start

```sh
bun install
bun packages/gateway/src/main.ts daemon     # out-of-band launcher (foreground)
bun packages/cli/src/main.ts chat           # loopback REPL
bun packages/cli/src/main.ts status
```

Configuration lives in `$GAJAEWAY_HOME/config.json` (default `~/.gajaeway`);
secrets are credential-file references only. See
[`docs/runbooks/gajaeway-v1.md`](docs/runbooks/gajaeway-v1.md) for adapters,
monitors, memory operations, backup/restore, and crash-recovery behavior.

## Development

```sh
bun test packages/                          # full suite
GAJAEWAY_BENCH=1 bun test packages/gateway/bench    # retrieval benchmark gate
GAJAEWAY_STRESS=1 bun test packages/gateway/test/stress  # 60s burst stress
bunx biome check packages/
bunx tsc --noEmit -p tsconfig.json
```

Safety floors are unoverridable in every configuration: the unrecoverable
command blocklist and the path-scope guard (no recursive deletion of `$HOME`
itself or anything outside `$HOME` / `$GAJAEWAY_HOME`).
