# gajae-way

**gajae-way** gives you a persistent AI persona that lives where you already talk: Discord and Telegram DMs, channels, and threads. It can keep notes in plain Markdown files, react to scheduled work and outside events, and keep each conversation in its own private context.

It is meant to feel like talking to one helpful presence—not operating a dashboard.

```text
Discord / Telegram ──> gajaeway gateway ──> your persona (gjc)
                              │
                    Markdown memory + scheduled/event monitors
```

## What it feels like

- Send a DM and talk normally. Your bot responds in that conversation’s own ongoing context.
- In a group, it stays out of the way until you mention it. You can explicitly open a configured channel for normal conversation.
- Send `/new` when you want a fresh start in that conversation. It confirms that a fresh session has started.
- On Discord, it shows a typing indicator while it is working.
- Replies are protected by a durable delivery record. After a crash, an unsettled reply may be sent again; when the earlier send was uncertain, it is visibly labeled as a duplicate rather than silently pretending it was not.
- Conversations and useful monitor output are captured under your own `$GAJAEWAY_HOME/memory` directory as readable Markdown, not hidden in a proprietary store.

For how memory, monitors, and the gateway work, see [the documentation](docs/).

## Start in five steps

1. Build the standalone programs on a machine with Bun:

   ```sh
   bun run build
   ```

   This creates `dist/gajaeway-gateway`, `dist/gajaeway-discord`, `dist/gajaeway-telegram`, `dist/gajaeway`, and `dist/gajaeway-admin`.

2. Choose a private home directory and create `$GAJAEWAY_HOME/config.json` plus separate credential files. The gateway configuration references credential **files**, rather than storing secret values inline. See [deployment](docs/deployment.md) for the complete layout and examples.

3. Create `$GAJAEWAY_HOME/adapter-discord.json` with its own `tokenFile` reference for Discord. Create the analogous Telegram adapter configuration when using Telegram.

4. Run the gateway as a long-lived daemon under your service manager:

   ```sh
   dist/gajaeway-gateway daemon
   ```

   The host also needs the external `gjc` program on `PATH`; it supplies the AI runtime for every turn.

5. Start the Discord adapter, then send your bot a DM:

   ```sh
   dist/gajaeway-discord
   ```

   Start `dist/gajaeway-telegram` separately when using Telegram. `dist/gajaeway-admin` is an optional loopback-only web console over a running gateway — see [deployment](docs/deployment.md#admin-console).

## Make it yours

Put `SOUL.md`, `AGENTS.md`, and `USER.md` in `$GAJAEWAY_HOME/workspace`. They are read for each turn and that workspace is also your persona’s working directory. Keep the home directory private: it contains configuration, the gateway database, your workspace, and memory.

- [Deployment guide](docs/deployment.md) — configuration, credentials, and service-manager setup
- [Memory guide](docs/memory.md) — readable memory and search
- [Monitor guide](docs/monitors.md) — scheduled and event-driven work
- [Architecture](docs/architecture.md) — protocol and reliability details
- [Operator runbook](docs/runbooks/gajaeway-v1.md) — recovery and troubleshooting

## Development

This repository is a Bun/TypeScript workspace. Build with `bun run build`; run tests with `bun test packages`. Production hosts run the compiled binaries, not this source checkout.
