# gajae-way

**Put your GJC coding agent in Discord/Slack/Telegram: it answers, remembers, and runs real coding work from chat.**

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

- **One presence, many rooms — never one blur.** Every conversation is a validated origin (`platform/kind/conversationId`) with its own session and epoch. Your DM is not your team channel. `/new` rebinds *that* conversation and leaves the others untouched. This separates conversation context, not tenants: the persona shares its workspace, memory, and host permissions across rooms.
- **Memory you can read with `cat`.** Turns and monitor output land in `$GAJAEWAY_HOME/memory` as Markdown across a small set of canonical axes. It is your filesystem, your git history, your grep — not a vector blob you have to trust.
- **It acts without being asked.** Cron and event monitors give the persona its own turns, so it can canonicalize memory on a schedule or audit itself each morning while you sleep.
- **Delivery is ledgered, not hoped for.** Replies get a durable record before they go out. After a crash, a send whose outcome was uncertain is reissued and *visibly labeled a duplicate* instead of quietly pretending nothing happened.
- **It knows when to shut up.** A `dmPolicy`-accepted DM engages without needing a mention. Group traffic is mention-gated unless you explicitly open a channel — and an opened channel still lets the persona choose silence over noise.
- **Safety floors gjc always sees.** Every persona turn carries non-configurable ActionGuard system guidance forbidding unrecoverable commands (recursive removal of `/`, filesystem formatting, raw device writes, fork bombs) and recursive deletion of `$HOME` itself or absolute paths outside `$HOME` and `$GAJAEWAY_HOME`; the gateway does not execute or intercept persona commands itself. A configured monitor script is separately checked against the same floors before it runs.
- **Standalone binaries, not a stack.** `bun run build` emits compiled executables. Production hosts run those under launchd/systemd; the source checkout stays on your laptop.

## What it feels like

- Send a DM that your `dmPolicy` accepts (`owner-only`, `allowlist`, or `open`) and talk normally. Your bot responds in that conversation's own ongoing context.
- In a group, it normally waits for a mention or a native reply to the bot. You can explicitly open a configured channel for normal conversation; author and audience policies still apply.
- Send `/new` when you want a fresh start in that conversation. It confirms that a fresh session has started.
- Discord shows reaction-gradient presence for gateway-engaged turns, including unmentioned turns in open channels, plus the platform's native typing indicator. Discord's `statusReactions` can select `static` or `off`. Slack shows reaction-gradient presence for accepted DMs and addressed group turns (including messages in an adapter-configured `open` channel), plus a native assistant status line (`<app> <status>`, via `assistant.threads.setStatus`) under the reply thread. The gradient uses phase markers, an advancing clock, and an effort digit (tool calls, falling back to output tokens) reacted directly onto the message it is answering — never a posted or edited status message. Presence stays up through mid-work messages while the turn keeps working, with stale-presence cleanup if progress stops.
- Replies are protected by a durable delivery record. After a crash, a send whose outcome was uncertain may be reissued; when it was, it is visibly labeled as a duplicate rather than silently pretending it was not.
- Discord replies can attach local files when the response contains a standalone `MEDIA:/absolute/path/to/file` line outside a code fence. Uploads are disabled unless `mediaDirectories` is configured in `$GAJAEWAY_HOME/adapter-discord.json`; files must resolve inside those directories. The adapter removes the directive from the visible text and sends the file natively with its caption; uploads are limited to 10 files and 25 MiB total per message.
- Conversations and useful monitor output are captured under your own `$GAJAEWAY_HOME/memory` directory as readable Markdown, not hidden in a proprietary store.

## Packages

The Bun workspace under `packages/` is divided by responsibility:

| Package | Responsibility |
|---|---|
| [`@gajae-gateway/protocol`](packages/protocol) | Versioned NDJSON frames, negotiation, verb/event catalogues, and canonical origins. |
| [`@gajae-gateway/sdk`](packages/sdk) | Client for the gateway's Unix-domain socket or stdio transport. |
| [`@gajae-gateway/cli`](packages/cli) | Owner commands over the gateway socket. |
| `@gajae-gateway/gateway` | Daemon: configuration, SQLite state, sessions, delivery, memory, monitors, and the `gjc` boundary. |
| `@gajae-gateway/adapter-discord` | Discord ingress and outbound delivery, including typing hints and the reaction-gradient presence. |
| `@gajae-gateway/adapter-telegram` | Telegram ingress and outbound delivery. |
| `@gajae-gateway/adapter-slack` | Slack ingress over Socket Mode, mrkdwn and file delivery, reactions, reaction-gradient working presence, opt-in live replies, and missed-message recovery. |
| `@gajae-gateway/admin` | Local operator web console. |
| `@gajae-gateway/home` | Shared home layout and process ownership leases. |
| `@gajae-gateway/log` | Shared structured logging. |
| `@gajae-gateway/subsession` | GJC session boundary. |
| `@gajae-gateway/conformance` | Protocol conformance checks for CI/tests. |

`@gajae-gateway/protocol`, `@gajae-gateway/sdk`, and `@gajae-gateway/cli` are npm-publishable as standalone packages — see [Publishing packages](#publishing-packages) below for the release workflow. The remaining packages are private: runtime code is bundled into the six application binaries; `@gajae-gateway/conformance` is CI/test-only, not a standalone binary.

For how memory, monitors, and the gateway work end to end, see [the documentation](docs/).

## Quick Start

**Local installation from source**

Requirements: Bun ≥1.4.0 and an authenticated GJC source profile at `~/.gjc/agent` (credentials and model configuration). The runtime uses the exact GJC version pinned in [`packages/gateway/package.json`](packages/gateway/package.json), currently 0.18.12, installed privately under `$GAJAEWAY_HOME/gjc`; a `gjc` on your PATH is not sufficient.

```sh
# Clone and enter the repository
git clone https://github.com/Yeachan-Heo/gajae-way.git
cd gajae-way

# Build the binaries
bun install
bun run build

export GAJAEWAY_HOME="$HOME/.gajaeway"

# Bootstrap the private, pinned GJC using the source checkout's installer
bun -e 'import { installPinnedGjc } from "./packages/cli/src/gjc-install.ts"; const { gjc } = await Bun.file("packages/gateway/package.json").json(); await installPinnedGjc({ home: process.env.GAJAEWAY_HOME!, version: gjc.version });'

# Validate platform credentials and write configuration (does not start services)
./dist/gajaeway setup

# Snapshot the authenticated source profile into the private broker agent directory
# Run this before starting the broker; --from DIR selects a different source profile
./dist/gajaeway setup --broker-agent

# Validate configuration before starting processes
./dist/gajaeway-gateway config check
```

Setup asks which adapters to configure and for your platform user ID(s). The default `dmPolicy` is `allowlist`: without any owner/allowlist IDs, all DMs are declined. Discord setup can infer the application owner; Slack and Telegram users should supply their own IDs. Complete each platform's app installation and permissions before chatting.

Start the broker, gateway, and each configured adapter in separate terminals, in that order. Set the same `GAJAEWAY_HOME` in every terminal and run from the checkout:

```sh
# Terminal 1: keep the broker running
export GAJAEWAY_HOME="$HOME/.gajaeway"
./dist/gajaeway-gateway broker

# Terminal 2: keep the gateway running
export GAJAEWAY_HOME="$HOME/.gajaeway"
./dist/gajaeway-gateway daemon

# Terminal 3: choose the adapter you configured (or use another terminal for each)
export GAJAEWAY_HOME="$HOME/.gajaeway"
./dist/gajaeway-discord    # alternatives: ./dist/gajaeway-slack or ./dist/gajaeway-telegram

# Another terminal: inspect readiness, then test a DM from an allowed user
export GAJAEWAY_HOME="$HOME/.gajaeway"
./dist/gajaeway status
./dist/gajaeway ops cycle --json
```

There is no dispatchable `gajaeway up` command. For persistent deployment, `gajaeway services install --bin-dir DIR` writes launchd/systemd definitions; installation does not load or start them. Use an absolute binary directory and follow the service-manager instructions in the [Deployment Guide](docs/deployment.md).

**Docker Compose: setup helper, not a complete runtime stack**

The checked-in Compose file can build the image and run platform credential setup:

```sh
docker compose build setup
docker compose run --rm -it setup gajaeway setup
docker compose run --rm --no-deps -T setup gajaeway setup --status
```

It is not a ready-to-chat deployment: it has no broker service, private pinned-GJC installation, or broker-agent provisioning step. The image's `/usr/local/bin/gjc` and `GJC_EXECUTABLE` setting do not replace the private executable required by the runtime. Model credentials must also be provisioned. Adapter services are profile-gated (`discord`, `slack`, `telegram`); setup writes their configuration but does not activate Compose profiles, so plain `docker compose up` starts no adapters. Use the local procedure above unless you supply the missing container orchestration.

## Getting a Discord Bot Token

When you run `gajaeway setup` and select Discord, you'll see step-by-step guidance for getting your bot token:

1. Go to [Discord Developer Portal](https://discord.com/developers/applications)
2. Click **New Application**, name it (e.g., "My gajae-way agent")
3. Go to **Bot** → **Add Bot**
4. Under **TOKEN**, click **Reset Token** → **Yes, do it!**
5. Click **Copy** to copy your bot token
6. **Important**: Scroll down to **GATEWAY INTENTS** and enable **MESSAGE CONTENT INTENT** (lets the bot read messages, not just user names)
7. Click **Save Changes**
8. Paste the token when prompted

After validation, you'll receive a ready-to-click **invite link** that you can share with others or click yourself to add the bot to your Discord server. The link includes the required permissions automatically.

## Make it yours

Put `SOUL.md`, `AGENTS.md`, and `USER.md` in `$GAJAEWAY_HOME/workspace`. `SOUL.md` and `USER.md` are read for each turn. gjc discovers `AGENTS.md` when a session starts; a later edit or deletion supersedes that project-context copy on the next turn. The workspace is also your persona's working directory. Keep the home directory private: it contains configuration, the gateway database, your workspace, and memory.

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

`@gajae-gateway/protocol`, `@gajae-gateway/sdk`, and `@gajae-gateway/cli` are npm-publishable under the `@gajae-gateway` scope. Each has its own `build` step that bundles `src/` into a single ESM `dist/*.js` with Bun, then emits `.d.ts` declarations with `tsc` (`emitDeclarationOnly`), and a `prepublishOnly` script that builds before publishing a package directory. Protocol and SDK target Node; the CLI targets Bun and requires Bun at runtime. The published tarball includes both `dist/` (resolved by `main` and the default export condition) and `src/` (resolved by the `bun` and `types` conditions; the `types` field also points to source, not the generated declarations). `@gajae-gateway/protocol` has zero runtime dependencies. Packing must go through Bun, not plain npm: `npm pack --dry-run` only lists a package's contents and does **not** rewrite dependencies — plain `npm pack`/`npm publish` of a package directory leave `workspace:*` in the packed manifest, which a real npm consumer cannot resolve. `bun pm pack` rewrites each published package's `workspace:*` dependency on `@gajae-gateway/protocol`/`@gajae-gateway/sdk` to the exact version being packed, producing a registry-valid manifest — verified by packing with `bun pm pack`, `npm install`-ing the tarballs into a scratch project, and running a real `node --input-type=module -e "import('@gajae-gateway/sdk')"` against the installed package (transcript: [`artifacts/ultragoal-npm-clean-install-verification.txt`](artifacts/ultragoal-npm-clean-install-verification.txt)).

`scripts/release-packages.ts` runs the whole release in the required dependency order (`protocol` → `sdk` → `cli`, since `sdk` and `cli` depend on the versions published before them):

```sh
bun run release:dry-run   # builds each package, then `bun pm pack`s it into dist-packed/ — never touches the registry
bun run release:publish   # builds and packs each package, then `npm publish`es that tarball (add --tag <name> for a non-latest dist-tag)
```

Inspect `dist-packed/*.tgz` from a dry run before ever running `release:publish`. Bun takes the rewritten version from the workspace entries in `bun.lock`, which `bun install` does not refresh after a `package.json` version bump; the script refuses any tarball that pins a sibling package to a version other than the one being released, so bump those `bun.lock` entries together with the manifests. The upload itself uses the npm CLI because only npm supports trusted publishing. `@gajae-gateway/sdk` and `@gajae-gateway/cli` depend on `@gajae-gateway/protocol`/`@gajae-gateway/sdk` being published first, which is why the script releases them in that order. Each package's `publishConfig` sets public npm access for the scoped name.

### CI/CD

[`build.yml`](.github/workflows/build.yml) runs on every push and pull request: Biome, `tsc --noEmit`, `bun test packages/`, and the frozen-corpus benchmark gate, each across macOS arm64, Linux x64, and Linux arm64. A separate Linux x64 job validates Compose, builds its setup image, and exercises `setup --status`; it does not test a live broker-backed chat deployment.

[`release.yml`](.github/workflows/release.yml) runs on a `v*.*.*` tag push (or manually via `workflow_dispatch`, with an optional `npm_tag` input for a non-`latest` dist-tag). Its `verify` matrix runs Biome, `tsc --noEmit`, and package tests on the same three platforms, but omits the build workflow's benchmark and Docker checks. Only on green does it publish `@gajae-gateway/protocol`, `@gajae-gateway/sdk`, and `@gajae-gateway/cli` to npm in dependency order via `bun run release:publish`, build and archive the six standalone binaries for each platform, and create a GitHub release with the archives attached and auto-generated notes. The publish job uses npm trusted publishing (OIDC), not a token: it grants `id-token: write` and installs current npm on Node 24. Each package must have a matching GitHub Actions trusted publisher configured on npmjs.com for repository `Yeachan-Heo/gajae-way` and workflow `release.yml`; that registry-side configuration is not established by this repository. npm >= 11.5.1 can exchange the job's OIDC token for a short-lived publish credential and attach provenance. No `NPM_TOKEN` secret is used. This workflow uses GitHub-hosted runners; renaming the workflow file requires updating the trusted publisher on all three packages.

## License

Copyright (C) 2026 Yeachan Heo. Licensed under the [GNU Affero General Public License v3.0](LICENSE).
