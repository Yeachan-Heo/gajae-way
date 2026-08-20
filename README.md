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
gajaeway console (local owner)  -------------------------->  gajaeway daemon
gajaeway-discord adapter       -------------------------->  Bun + Rust single binary
Discord gateway/REST                                      |
                                                           +-- Rust: SQLite, journal,
                                                           |   lock FSM, RPC, registry
                                                           +-- TypeScript: strict resume,
                                                               profile injection, broker bridge
                                                                     |
                                                              gjc broker-owned,
                                                              isolated sessions
```

`gajaeway` owns durable state, the UDS endpoint, and the only v1 in-daemon corpus
lock holder. `gajaeway-discord` is a separate adapter process with no local
checkpoint; it reconnects to the daemon and settles delivery through the
server-owned journal/outbox. GJC sessions remain broker-owned and isolated. The
gateway adopts only the exact operator-run `main` session selected in
`[main_session].session_id` (or by an exact bootstrap `--session-id`) and
strictly re-verifies its broker identity and locator on every daemon start; it
never creates, resumes, or falls back to another GJC session.

Durable gateway state is SQLite WAL with `synchronous=FULL`: journal events,
lease transitions, verification receipts, and consumer settlements acknowledge
only after their committing transaction crosses the power-loss durability
boundary. This does not replace backups of the corpus or broker-managed GJC
session evidence.

## Install, configure, bootstrap, operate

The release unit contains two compiled executables. Build them for the target
architecture before copying them to the Linux host:

```sh
bun scripts/build-native.ts && bun scripts/compile.ts
sudo install -m 0755 dist/gajaeway /usr/local/bin/gajaeway
sudo install -m 0755 dist/gajaeway-discord /usr/local/bin/gajaeway-discord
```

Create a dedicated service identity and protected configuration/state paths:

```sh
sudo useradd --system --home-dir /var/lib/gajaeway --create-home \
  --shell /usr/sbin/nologin gajaeway
sudo install -d -o root -g gajaeway -m 0750 /etc/gajaeway
sudo install -d -o root -g root -m 0700 /etc/gajaeway/credentials
sudo install -d -o gajaeway -g gajaeway -m 0700 /var/lib/gajaeway
sudo install -m 0644 ops/systemd/gajaeway.service /etc/systemd/system/gajaeway.service
sudo install -m 0644 ops/systemd/gajaeway-discord.service /etc/systemd/system/gajaeway-discord.service
```

Copy `ops/profiles/gaebal-gajae.example.toml` to
`/etc/gajaeway/profile.toml`, replace every example identity and path, then
make the profile readable by the service identity without making it writable:

```sh
sudo install -m 0640 -o root -g gajaeway ops/profiles/gaebal-gajae.example.toml \
  /etc/gajaeway/profile.toml
sudo install -m 0600 -o root -g root /path/to/discord-token \
  /etc/gajaeway/credentials/discord-token
sudo systemctl daemon-reload
```

Before installing or enabling the daemon, set `ReadWritePaths=` in the source
unit (or in `/etc/systemd/system/gajaeway.service` after installation) to
exactly match `[corpus].path` and `[corpus].workspace`; `/srv/gajaeway/...` is
only a placeholder. Run `sudo systemctl daemon-reload` after changing an
installed unit. Review the `ProtectHome=` comment when either path is under
`/home`.

Bootstrap is an explicit, one-time adoption ceremony while the daemon is
stopped. The operator first starts the real interactive `gjc` in the configured
corpus workspace; tmux is the recommended durable owner:

```sh
tmux new-session -s gajae-main -c /srv/gajaeway/workspace gjc
```

Discover the live `main` session, then record its exact ID in the profile:

```sh
gjc sdk session list
```

```toml
[main_session]
session_id = "SESSION_ID_FROM_GJC"
```

With the profile pin in place, adopt it through the broker CLI:

```sh
sudo systemctl stop gajaeway-discord.service gajaeway.service
sudo -u gajaeway -H /usr/local/bin/gajaeway bootstrap --confirm \
  --state-dir /var/lib/gajaeway --profile /etc/gajaeway/profile.toml
```

An operator may use `--session-id SESSION_ID_FROM_GJC` instead of the profile
pin; when both are supplied, they must match. Bootstrap verifies the exact live
external session and commits its identity. Every later `gajaeway serve` strictly
re-verifies that identity and fails closed rather than adopting a different
session.

Then start the gateway and adapter:

```sh
sudo systemctl enable --now gajaeway.service
sudo systemctl enable --now gajaeway-discord.service
```

Probe the running daemon, rather than only the executable version, with:

```sh
sudo -u gajaeway -H /usr/local/bin/gajaeway --health --state-dir /var/lib/gajaeway
```

The command queries the UDS and exits non-zero with `state: "unavailable"` if
the daemon cannot be reached. The operations runbook covers `way.status` and
full RPC monitoring.

## Owner console and adapter final gates

`gajaeway console` is the first-party, local owner acceptance surface. Run it as the
same service identity that owns the protected UDS socket:

```sh
sudo -u gajaeway -H /usr/local/bin/gajaeway console \
  --state-dir /var/lib/gajaeway --profile /etc/gajaeway/profile.toml
```

It reads `way.health` and `way.status` before accepting input, displays the
main-session resume state, journal cursor, lock/quarantine and write-mode
state, and reconciliation freshness, and refuses a failed-closed or unhealthy
daemon. Owner messages use the configured owner surface mapping; when a
profile has more than one owner surface, select one explicitly with
`--surface-id`. The gateway controls the adopted GJC session through the broker
CLI; replies, turn transitions, health changes, and gate notifications are read
and checkpointed through the daemon-owned journal, so the console holds no
local cursor or durable delivery state.

Gates are answered in the attached real GJC TUI, not through the gateway:
`tmux attach -t <session>` when tmux hosts it, or the terminal running `gjc`.
The console `/gate` command remains a capability probe for a future backend
with validated gate receipts; the external-host backend reports it as
unsupported. Replies finalized while the daemon is down are recovered from
durable delivery progress or represented by an explicit delivery-gap event,
never silently as a delivered assistant reply.

The local console is the first acceptance surface, not a replacement for chat
adapter validation. The configured live Discord route remains the final chat
adapter gate, and a future Telegram adapter is subject to the same final-gate
route drill after local-console acceptance succeeds.

### Main-session journal lifecycle payloads

`turn_start` and `turn_end` each represent one broker-observed GJC attempt, not
an individual provider/tool turn. Both rows use the same stable payload and
contain no provider message or assistant text:

```json
{"attempt_id":"<broker attempt ID>","generation":1,"lineage":"main"}
```

Final assistant text is published only in the finalized `assistant_message`
journal event. Consumers can therefore treat lifecycle rows as transition
identities and render reply text solely from `assistant_message`.

Use the runbooks below for the complete operational procedure and incident
handling. Do not bootstrap a second state directory or start a second adapter
for the same configured route.

## Profile model

The loader creates a versioned, canonical SHA-256 identity/security projection.
Changes to these fields require `gajaeway profile approve --confirm` before strict
resume can continue:

- corpus path and workspace;
- ordered injection file list;
- restricted-file policy;
- owner surface mapping;
- the `[operator]` or `[identity]` table; and
- `[main_session].session_id`.

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
