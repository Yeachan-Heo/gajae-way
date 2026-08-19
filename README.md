# gajae-way

`gajae-way` is a generic, profile-driven operator-agent runtime. It runs one
strict-resumed operator session against a configured corpus and exposes a local
JSON-RPC gateway. The bundled `gaebal-gajae` profile is an example, not a
product default: persona files, corpus paths, owner surfaces, policies, and
platform credentials all belong in deployment configuration.

> **Single-writer operator precondition — load-bearing:** Never run two
> gateways, or a gateway and a legacy bot, against the same corpus checkout or
> remote. `write_mode`, lock quarantine, and Git non-fast-forward protection
> are backstops; they do not make concurrent writers safe.

## Architecture

```text
                         Unix-domain JSON-RPC
way console (local owner)  -------------------------->  way daemon
way-discord adapter       -------------------------->  Bun + Rust single binary
Discord gateway/REST                                      |
                                                           +-- Rust: SQLite, journal,
                                                           |   lock FSM, RPC, registry
                                                           +-- TypeScript: strict resume,
                                                               profile injection, broker bridge
                                                                     |
                                                              gjc broker-owned,
                                                              isolated sessions
```

`way` owns durable state, the UDS endpoint, and the only v1 in-daemon corpus
lock holder. `way-discord` is a separate adapter process with no local
checkpoint; it reconnects to the daemon and settles delivery through the
server-owned journal/outbox. GJC sessions remain broker-owned and isolated;
the gateway resumes only the configured main session by its full transcript
fingerprint.

Durable gateway state is SQLite WAL with `synchronous=FULL`: journal events,
lease transitions, verification receipts, and consumer settlements acknowledge
only after their committing transaction crosses the power-loss durability
boundary. This does not replace backups of the corpus or strict-resume
transcript.

## Install, configure, bootstrap, operate

The release unit contains two compiled executables. Build them for the target
architecture before copying them to the Linux host:

```sh
bun scripts/build-native.ts && bun scripts/compile.ts
sudo install -m 0755 dist/way /usr/local/bin/way
sudo install -m 0755 dist/way-discord /usr/local/bin/way-discord
```

Create a dedicated service identity and protected configuration/state paths:

```sh
sudo useradd --system --home-dir /var/lib/gajae-way --create-home \
  --shell /usr/sbin/nologin gajae-way
sudo install -d -o root -g gajae-way -m 0750 /etc/gajae-way
sudo install -d -o root -g root -m 0700 /etc/gajae-way/credentials
sudo install -d -o gajae-way -g gajae-way -m 0700 /var/lib/gajae-way
sudo install -m 0644 ops/systemd/gajae-way.service /etc/systemd/system/gajae-way.service
sudo install -m 0644 ops/systemd/gajae-way-discord.service /etc/systemd/system/gajae-way-discord.service
```

Copy `ops/profiles/gaebal-gajae.example.toml` to
`/etc/gajae-way/profile.toml`, replace every example identity and path, then
make the profile readable by the service identity without making it writable:

```sh
sudo install -m 0640 -o root -g gajae-way ops/profiles/gaebal-gajae.example.toml \
  /etc/gajae-way/profile.toml
sudo install -m 0600 -o root -g root /path/to/discord-token \
  /etc/gajae-way/credentials/discord-token
sudo systemctl daemon-reload
```

Before installing or enabling the daemon, set `ReadWritePaths=` in the source
unit (or in `/etc/systemd/system/gajae-way.service` after installation) to
exactly match `[corpus].path` and `[corpus].workspace`; `/srv/gajae-way/...` is
only a placeholder. Run `sudo systemctl daemon-reload` after changing an
installed unit. Review the `ProtectHome=` comment when either path is under
`/home`.

Bootstrap is an explicit, one-time ceremony while the daemon is stopped:

```sh
sudo systemctl stop gajae-way-discord.service gajae-way.service
sudo -u gajae-way -H /usr/local/bin/way bootstrap --confirm \
  --state-dir /var/lib/gajae-way --profile /etc/gajae-way/profile.toml
```

Then start the gateway and adapter:

```sh
sudo systemctl enable --now gajae-way.service
sudo systemctl enable --now gajae-way-discord.service
```

Probe the running daemon, rather than only the executable version, with:

```sh
sudo -u gajae-way -H /usr/local/bin/way --health --state-dir /var/lib/gajae-way
```

The command queries the UDS and exits non-zero with `state: "unavailable"` if
the daemon cannot be reached. The operations runbook covers `way.status` and
full RPC monitoring.

## Owner console and adapter final gates

`way console` is the first-party, local owner acceptance surface. Run it as the
same service identity that owns the protected UDS socket:

```sh
sudo -u gajae-way -H /usr/local/bin/way console \
  --state-dir /var/lib/gajae-way --profile /etc/gajae-way/profile.toml
```

It reads `way.health` and `way.status` before accepting input, displays the
main-session resume state, journal cursor, lock/quarantine and write-mode
state, and reconciliation freshness, and refuses a failed-closed or unhealthy
daemon. Owner messages use the configured owner surface mapping; when a
profile has more than one owner surface, select one explicitly with
`--surface-id`. Replies, turn transitions, health changes, and workflow gates
are read and checkpointed through the daemon-owned journal, so the console
holds no local cursor or durable delivery state.

The local console is the first acceptance surface, not a replacement for chat
adapter validation. The configured live Discord route remains the final chat
adapter gate, and a future Telegram adapter is subject to the same final-gate
route drill after local-console acceptance succeeds.

Use the runbooks below for the complete operational procedure and incident
handling. Do not bootstrap a second state directory or start a second adapter
for the same configured route.

## Profile model

The loader creates a versioned, canonical SHA-256 identity/security projection.
Changes to these fields require `way profile approve --confirm` before strict
resume can continue:

- corpus path and workspace;
- ordered injection file list;
- restricted-file policy;
- owner surface mapping; and
- the `[operator]` or `[identity]` table.

Tunables are the hot-reload class and are deliberately outside that digest:
`[tunables]`, `[poll]`, `[ack]`, `[adapter]`/`[adapters]`, and `[policy]`.
They do not trigger `profile_drift`. In v1 there is no profile file watcher, so
a changed tunable takes effect on the affected process's next configuration load
or restart.

## Runbooks

- [Operations](ops/runbooks/operations.md) — install/configure/bootstrap/operate,
  local owner console, monitoring, audit, backup, and chat-adapter final-gate verification.
- [Bootstrap ceremony](ops/runbooks/bootstrap-ceremony.md) — atomic first-run
  identity binding and interrupted-ceremony recovery.
- [Profile approval](ops/runbooks/profile-approve.md) — intentional
  digest-bound identity/security changes.
- [Quarantine](ops/runbooks/quarantine.md) — lock-holder uncertainty and the
  mandatory manual Git-verification fence.
- [Legacy decommission](ops/runbooks/legacy-decommission.md) — disable legacy
  writers before this runtime becomes the single corpus writer.
