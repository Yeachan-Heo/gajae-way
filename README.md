<p align="center">
  <img src="assets/logo-horizontal.png" alt="gajae-way" width="640">
</p>

<p align="center"><sub>Mascot artwork is a derivative of <a href="https://github.com/Yeachan-Heo/gajae-code">gajae-code</a>'s branding — see <a href="assets/NOTICE.md">assets/NOTICE.md</a> for attribution.</sub></p>

<p align="center">
  <a href="https://github.com/Yeachan-Heo/gajae-way/actions/workflows/build.yml"><img alt="Build" src="https://github.com/Yeachan-Heo/gajae-way/actions/workflows/build.yml/badge.svg"></a>
  <a href="LICENSE"><img alt="License: AGPL-3.0" src="https://img.shields.io/badge/license-AGPL--3.0-blue.svg"></a>
  <img alt="Bun" src="https://img.shields.io/badge/bun-%3E%3D1.4.0-000000?logo=bun&logoColor=white">
  <img alt="Runtime" src="https://img.shields.io/badge/runtime-TypeScript-3178C6?logo=typescript&logoColor=white">
</p>

<p align="center"><strong>Your AI shouldn't live in a browser tab. It should live in your DMs.</strong></p>

gajae-way is a runtime that turns an AI coding agent into a *resident persona* — one that sits in your Discord, Telegram, and Slack, remembers you in plain Markdown, wakes itself up on a schedule, and keeps every conversation in its own private head. No new place to check for ordinary conversation — you just talk to it where you already talk. An optional local `gajaeway-admin` console exists for the operator, not for everyday chat.

```text
Discord / Telegram / Slack ──> gajaeway gateway ──> your persona (gjc)
                              │
                    Markdown memory + scheduled/event monitors
```

Chat frontends are easy. What is hard is everything that happens when a real bot has to stay up for months: crashes mid-reply, two messages racing the same conversation, an agent that forgets who you are, an agent that confuses one room for another, an agent that answers a message you never sent. gajae-way is the boring, durable layer that handles those — so the persona on top can be interesting.

## Why this exists

- **One presence, many rooms — never one blur.** Every conversation is a validated origin (`platform/kind/conversationId`) with its own session and epoch. Your DM is not your team channel. `/new` rebinds *that* conversation and leaves the others untouched.
- **Memory you can read with `cat`.** Turns and monitor output land in `$GAJAEWAY_HOME/memory` as Markdown across a small set of canonical axes. It is your filesystem, your git history, your grep — not a vector blob you have to trust.
- **It acts without being asked.** Cron and event monitors give the persona its own turns, so it can canonicalize memory on a schedule or audit itself each morning while you sleep.
- **Delivery is ledgered, not hoped for.** Replies get a durable record before they go out. After a crash, a send whose outcome was uncertain is reissued and *visibly labeled a duplicate* instead of quietly pretending nothing happened.
- **It knows when to shut up.** A `dmPolicy`-accepted DM engages without needing a mention. Group traffic is mention-gated unless you explicitly open a channel — and an opened channel still lets the persona choose silence over noise.
- **Safety floors gjc always sees.** Every persona turn carries non-configurable ActionGuard system guidance forbidding unrecoverable commands (recursive removal of `/`, filesystem formatting, raw device writes, fork bombs) and recursive deletion of `$HOME` itself or absolute paths outside `$HOME` and `$GAJAEWAY_HOME`; the gateway does not execute or intercept persona commands itself. A configured monitor script is separately checked against the same floors before it runs.
- **Standalone binaries, not a stack.** `bun run build` emits compiled executables. Production hosts run those under launchd/systemd; the source checkout stays on your laptop.

## What it feels like

- Send a DM that your `dmPolicy` accepts (`owner-only`, `allowlist`, or `open`) and talk normally. Your bot responds in that conversation's own ongoing context.
- In a group, it stays out of the way until you mention it. You can explicitly open a configured channel for normal conversation.
- Send `/new` when you want a fresh start in that conversation. It confirms that a fresh session has started.
- When the gateway accepts a DM or an addressed group turn (a real mention or native reply), Discord and Slack both show a reaction-gradient presence — phase markers, an advancing clock, and an effort digit (tool calls, falling back to output tokens) reacted directly onto the message it is answering; Discord also shows the platform's native typing indicator. Slack additionally promotes messages in an adapter-configured `open` channel to addressed; Discord does not treat `open`-channel messages as addressed without a real mention. Nothing is posted or edited to show progress. In a channel it merely overhears, nothing is shown until it actually replies.
- Replies are protected by a durable delivery record. After a crash, a send whose outcome was uncertain may be reissued; when it was, it is visibly labeled as a duplicate rather than silently pretending it was not.
- Conversations and useful monitor output are captured under your own `$GAJAEWAY_HOME/memory` directory as readable Markdown, not hidden in a proprietary store.

## Packages

The Bun workspace under `packages/` is divided by responsibility:

| Package | Responsibility |
|---|---|
| [`@gajaeway/protocol`](packages/protocol) | Versioned NDJSON frames, negotiation, verb/event catalogues, and canonical origins. |
| [`@gajaeway/sdk`](packages/sdk) | Client for the gateway's Unix-domain socket or stdio transport. |
| [`@gajaeway/cli`](packages/cli) | Owner commands over the gateway socket. |
| `@gajaeway/gateway` | Daemon: configuration, SQLite state, sessions, delivery, memory, monitors, and the `gjc` boundary. |
| `@gajaeway/adapter-discord` | Discord ingress and outbound delivery, including typing hints and the reaction-gradient presence. |
| `@gajaeway/adapter-telegram` | Telegram ingress and outbound delivery. |
| `@gajaeway/adapter-slack` | Slack ingress over Socket Mode, mrkdwn delivery, reactions, reaction-gradient working presence, and missed-message recovery. |

`@gajaeway/protocol`, `@gajaeway/sdk`, and `@gajaeway/cli` are npm-publishable as standalone packages — see [Publishing packages](#publishing-packages) below for the release workflow. The gateway, adapters, admin console, and `@gajaeway/subsession` stay private application code and ship only as the compiled binaries below; `@gajaeway/conformance` is a private, unshipped CI/test-only package.

For how memory, monitors, and the gateway work end to end, see [the documentation](docs/).

## Start in five steps

1. Build the standalone programs on a machine with Bun:

   ```sh
   bun run build
   ```

   This creates `dist/gajaeway-gateway`, `dist/gajaeway-discord`, `dist/gajaeway-telegram`, `dist/gajaeway-slack`, `dist/gajaeway-admin`, and `dist/gajaeway`.

2. Choose a private home directory and create `$GAJAEWAY_HOME/config.json` plus separate credential files. The gateway configuration references credential **files**, rather than storing secret values inline. See [deployment](docs/deployment.md) for the complete layout and examples.

3. Create `$GAJAEWAY_HOME/adapter-discord.json` with its own `tokenFile` reference for Discord. Create the analogous Telegram adapter configuration when using Telegram, or `adapter-slack.json` with `botTokenFile` and `appTokenFile` references when using Slack.

4. Run the gateway as a long-lived daemon under your service manager:

   ```sh
   dist/gajaeway-gateway daemon
   ```

   The host also needs the external `gjc` executable, either resolvable on `PATH` or pinned with an absolute `GJC_EXECUTABLE`; it supplies the AI runtime for every turn.

5. Start the Discord adapter, then send your bot a DM:

   ```sh
   dist/gajaeway-discord
   ```

   Start `dist/gajaeway-telegram` or `dist/gajaeway-slack` separately when using Telegram or Slack.

## Make it yours

Put `SOUL.md`, `AGENTS.md`, and `USER.md` in `$GAJAEWAY_HOME/workspace`. They are read for each turn and that workspace is also your persona's working directory. Keep the home directory private: it contains configuration, the gateway database, your workspace, and memory.

- [Deployment guide](docs/deployment.md) — configuration, credentials, and service-manager setup
- [Memory guide](docs/memory.md) — readable memory and search
- [Monitor guide](docs/monitors.md) — scheduled and event-driven work
- [Architecture](docs/architecture.md) — protocol and reliability details
- [Operator runbook](docs/runbooks/gajaeway-v1.md) — recovery and troubleshooting

## Development

This repository is a Bun/TypeScript workspace.

```sh
bun install
bun run build     # compiles gateway, adapters, admin, and CLI into standalone binaries under dist/
bun test packages # runs the full package test suite
```

Production hosts run the compiled binaries, not this source checkout.

### Publishing packages

`@gajaeway/protocol`, `@gajaeway/sdk`, and `@gajaeway/cli` are npm-publishable under the `@gajaeway` scope. Each has its own `build` step that bundles `src/` into a single Node-ESM-resolvable `dist/*.js` with Bun, then emits `.d.ts` declarations with `tsc` (`emitDeclarationOnly`), and a `prepublishOnly` script so a publish always ships current compiled output. The published tarball includes both `dist/` (resolved by `main`/`types`/the default export condition for non-Bun consumers) and `src/` (resolved by the `bun` export condition, so an installed Bun consumer imports source directly instead of the compiled build). `@gajaeway/protocol` has zero runtime dependencies. Publishing must go through Bun, not plain npm: `npm pack --dry-run` only lists a package's contents and does **not** rewrite dependencies — plain `npm pack`/`npm publish` leave `workspace:*` in the packed manifest, which a real npm consumer cannot resolve. `bun pm pack` and `bun publish` rewrite each published package's `workspace:*` dependency on `@gajaeway/protocol`/`@gajaeway/sdk` to the exact version being packed, producing a registry-valid manifest — verified by packing with `bun pm pack`, `npm install`-ing the tarballs into a scratch project, and running a real `node --input-type=module -e "import('@gajaeway/sdk')"` against the installed package (transcript: [`artifacts/ultragoal-npm-clean-install-verification.txt`](artifacts/ultragoal-npm-clean-install-verification.txt)).

`scripts/release-packages.ts` runs the whole release in the required dependency order (`protocol` → `sdk` → `cli`, since `sdk` and `cli` depend on the versions published before them):

```sh
bun run release:dry-run   # builds each package, then `bun pm pack`s it into dist-packed/ — never touches the registry
bun run release:publish   # builds each package, then `bun publish`s it (add --tag <name> for a non-latest dist-tag)
```

Inspect `dist-packed/*.tgz` from a dry run before ever running `release:publish`. `@gajaeway/sdk` and `@gajaeway/cli` depend on `@gajaeway/protocol`/`@gajaeway/sdk` being published first, which is why the script releases them in that order. Each package's `publishConfig` sets public npm access for the scoped name.

## License

Copyright (C) 2026 Yeachan Heo. Licensed under the [GNU Affero General Public License v3.0](LICENSE).
