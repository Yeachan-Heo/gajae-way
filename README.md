# gajae-way

**Your AI shouldn't live in a browser tab. It should live in your DMs.**

gajae-way is a runtime that turns an AI coding agent into a *resident persona* — one that sits in your Discord and Telegram, remembers you in plain Markdown, wakes itself up on a schedule, and keeps every conversation in its own private head. No hosted dashboard. No new place to check for ordinary conversation. You just talk to it where you already talk.

```text
Discord / Telegram ──> gajaeway daemon ──> gateway ──> your persona (gjc)
                              │
                    Markdown memory + scheduled/event monitors
```

Chat frontends are easy. What is hard is everything that happens when a real bot has to stay up for months: crashes mid-reply, two messages racing the same conversation, an agent that forgets who you are, an agent that confuses one room for another, an agent that answers a message you never sent. gajae-way is the boring, durable layer that handles those — so the persona on top can be interesting.

## Why this exists

- **One presence, many rooms — never one blur.** Every conversation is a validated origin (`platform/kind/conversationId`) with its own session and epoch. Your DM is not your team channel. `/new` rebinds *that* conversation and leaves the others untouched.
- **Memory you can read with `cat`.** Turns and monitor output land in `$GAJAEWAY_HOME/memory` as Markdown across a small set of canonical axes. It is your filesystem, your git history, your grep — not a vector blob you have to trust.
- **It acts without being asked.** Cron and event monitors give the persona its own turns, so it can canonicalize memory on a schedule or audit itself each morning while you sleep.
- **Delivery is ledgered, not hoped for.** Replies get a durable record before they go out. After a crash, an unsettled reply is reissued and *visibly labeled a duplicate* instead of quietly pretending nothing happened.
- **It knows when to shut up.** Direct messages follow your configured policy. Group traffic is mention-gated unless you explicitly open a channel — and an opened channel still lets the persona choose silence over noise.
- **Safety floors that config cannot unlock.** Unrecoverable commands and deletions outside your own home are refused at the runtime boundary, not left to prompt discipline.
- **One binary, one daemon.** `bun run build` emits `dist/gajaeway`. One `gajaeway daemon run` process owns the gateway, enabled Discord and Telegram adapters, and the admin console; production runs it under launchd or systemd.

## What it feels like

- Send a DM and talk normally. Your bot responds in that conversation’s own ongoing context.
- In a group, it stays out of the way until you mention it. You can explicitly open a configured channel for normal conversation.
- Send `/new` when you want a fresh start in that conversation. It confirms that a fresh session has started.
- On Discord, it shows a typing indicator while it is working.
- Replies are protected by a durable delivery record. After a crash, an unsettled reply may be sent again; when the earlier send was uncertain, it is visibly labeled as a duplicate rather than silently pretending it was not.
- Conversations and useful monitor output are captured under your own `$GAJAEWAY_HOME/memory` directory as readable Markdown, not hidden in a proprietary store.

For how memory, monitors, and the gateway work, see [the documentation](docs/).

## Start in five steps

1. Build the standalone binary on a machine with Bun:

   ```sh
   bun run build
   ```

   This creates exactly `dist/gajaeway`.

2. Choose a private `$GAJAEWAY_HOME` and create separate, permission-restricted credential files for each platform token (and a Discord voice key when voice is enabled).

3. Create `$GAJAEWAY_HOME/config.json` with `credentials` pointing at those files and `adapters` naming each platform to enable. Validate it before starting:

   ```sh
   dist/gajaeway config check
   ```

   See [deployment](docs/deployment.md) for the complete configuration and channel examples.

4. Install one launchd unit that runs the composite daemon:

   ```sh
   dist/gajaeway daemon run
   ```

   The host also needs the external `gjc` program on `PATH`; it supplies the AI runtime for every turn.

5. Send a DM to the enabled Discord or Telegram bot. The daemon receives platform traffic, operates the gateway, and delivers replies in the same process.

## Make it yours

Put `SOUL.md`, `AGENTS.md`, and `USER.md` in `$GAJAEWAY_HOME/workspace`. They are read for each turn and that workspace is also your persona’s working directory. Keep the home directory private: it contains configuration, the gateway database, your workspace, and memory.

- [Deployment guide](docs/deployment.md) — configuration, credentials, and service-manager setup
- [Memory guide](docs/memory.md) — readable memory and search
- [Monitor guide](docs/monitors.md) — scheduled and event-driven work
- [Architecture](docs/architecture.md) — protocol and reliability details
- [Operator runbook](docs/runbooks/gajaeway-v1.md) — recovery and troubleshooting

## Development

This repository is a Bun/TypeScript workspace. Build with `bun run build`; run tests with `bun test packages`. Production hosts run the compiled binary, not this source checkout. For development, run the composite daemon directly with `bun packages/gajaeway/src/main.ts daemon run`.
