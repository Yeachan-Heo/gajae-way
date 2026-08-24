# Operations

## Operating invariant

Exactly one gateway may write a corpus at a time. Never run a second `gajaeway`
instance, or `gajaeway` alongside a legacy bot or outbound job, against the same
checkout or remote. Stop the other writer first; quarantine and Git
non-fast-forward checks are incident backstops, not a concurrency plan.

## Install, configure, bootstrap, start

1. Build and install the two release executables, create the `gajaeway` system
   user, and install both unit files as described in the repository README.
   Keep the service account home under `/var/lib/gajaeway`, not under `/home`.
2. Copy `ops/profiles/gaebal-gajae.example.toml` to
   `/etc/gajaeway/profile.toml`. Replace the corpus/workspace, injection
   files, `[main_session].session_id`, owner mapping, operator identity,
   Discord route, and all examples. Install it `root:gajaeway`, mode `0640`.
3. Edit `ReadWritePaths=` in `gajaeway.service` so it names the exact
   configured corpus and workspace. Reinstall the unit and run
   `sudo systemctl daemon-reload`. The state path is `/var/lib/gajaeway`; leave
   it in the list.
4. Put the Discord token in
   `/etc/gajaeway/credentials/discord-token`, owned by `root:root`, mode
   `0600`. `LoadCredential=` exposes it only to the adapter as a private file;
   never put a token in the profile, a shell history, or a unit environment.
5. The operator must first run the interactive `gjc` owner in the configured
   corpus workspace (tmux is the recommended durable owner) and use `gjc sdk session
   list` to identify its live `main` session. Record that exact ID in
   `[main_session].session_id`; see the [bootstrap ceremony](bootstrap-ceremony.md)
   for the selection and attachment procedure.
6. With both services stopped, adopt that live session:

   ```sh
   sudo systemctl stop gajaeway-discord.service gajaeway.service
   sudo -u gajaeway -H /usr/local/bin/gajaeway bootstrap --confirm \
     --state-dir /var/lib/gajaeway --profile /etc/gajaeway/profile.toml
   ```

   This command reads `[main_session].session_id`, broker-verifies the external
   session, and commits its identity. When an operator intentionally uses
   `--session-id` instead, it must identify the same session as any profile pin.
7. Start the daemon first, then its bound adapter:

   ```sh
   sudo systemctl enable --now gajaeway.service
   sudo systemctl enable --now gajaeway-discord.service
   ```

   The daemon sends `READY=1` after its UDS endpoint and fenced main-session
   host are installed. A busy restart can remain `state: "verifying"` while
   transcript verification is pending; status and observation RPCs are live,
   but main-session mutations remain refused until the first compatible complete
   tail promotes it to `running`. The adapter is `BindsTo=` the daemon and stops
   when it stops. It polls `way.health` first and does not connect to Discord or
   register inbound handlers until health is `status: "healthy", state: "running"`.
   Every inbound typing acknowledgement is sent only after the adapter observes
   `main.submit` return durable acceptance. `ack_budget_ms` bounds the typing
   request from that observed response, not from the inbound Discord dispatch:
   `main.submit` exposes no earlier accepted-claim response. A slow successful
   broker admission can therefore produce typing more than two seconds after
   Discord delivery; a fence, failed-closed transition, or rejected submit
   leaves the message unacknowledged rather than consuming it without delivery.

## Scrapeable metrics and their accepted exposure

`way.metrics` is answerable over the peer-credential-authenticated UDS and is
deliberately allowed even while the daemon is failed closed, because that is the
state in which telemetry matters most. Prefer this path: it is the only
authenticated one.

```sh
gajaeway_rpc way.metrics '{}'
```

An optional Prometheus text endpoint can be enabled with
`[tunables.metrics].http_enabled = true`. **It is disabled by default and it is
unauthenticated.** Read the following before enabling it.

- **What is exposed.** While the listener is enabled, *any local process running
  as any user* can scrape it. The exposition carries gateway state as counters,
  fail-closed duration, journal head and per-consumer lag, turn activity,
  follow-up queue depth, lock held/queue/stuck/quarantined flags, and scheduler
  counters.
- **What is not exposed.** Lock holder identity, session ids, lease ids, and
  free-text failure reasons are excluded from the HTTP exposition and this is
  asserted by test. Only the authenticated UDS path returns the richer
  `way.status` document.
- **Bind address.** The listener binds loopback only, and a non-loopback address
  is refused rather than honoured. There is no `bind_addr` profile key, so the
  address is not operator-configurable. An occupied port is refused rather than
  silently rebound.
- **Surface.** Only `GET /metrics` is served. Every other path and method
  returns a bare 404 with no request echo.
- **Recommendation.** Keep the endpoint disabled on multi-tenant or shared-login
  hosts, where "loopback" does not imply "private". On a single-tenant host,
  enabling it is reasonable.
- **Audit trail.** `[tunables.metrics]` is in the non-digest tunable class, so
  enabling the endpoint changes no profile digest and produces **no
  `profile approve` receipt**. The audit trail for "an operator chose to expose
  telemetry" is this runbook plus the profile file's modification time, not an
  approval record. That is a deliberate trade: loopback-only plus default-off is
  the actual control, and treating enablement as a ceremony would overstate it.

## Monitor the live UDS service

`gajaeway --health --state-dir /var/lib/gajaeway` queries the running daemon over
its UDS and exits non-zero with `status: "unhealthy", state: "unavailable",
reason: "daemon_unreachable"` when that daemon cannot be reached. Use it for a
concise liveness probe. Query the local UDS as the service user for `way.status`
and operational RPCs. In a shell entered by `sudo -u gajaeway -H -s`, define:

```sh
gajaeway_rpc() {
  python3 - "$1" "${2:-{}}" <<'PY'
import json, socket, sys
request = {"jsonrpc": "2.0", "id": "operations", "method": sys.argv[1], "params": json.loads(sys.argv[2])}
with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
    client.connect("/var/lib/gajaeway/rpc.sock")
    client.sendall((json.dumps(request) + "\n").encode())
    print(client.makefile("r", encoding="utf-8").readline().strip())
PY
}
```

Probe both liveness and operational state:

```sh
/usr/local/bin/gajaeway --health --state-dir /var/lib/gajaeway
gajaeway_rpc way.health '{}'
gajaeway_rpc way.status '{}'
systemctl --no-pager status gajaeway.service gajaeway-discord.service
journalctl -u gajaeway.service -u gajaeway-discord.service --since '15 minutes ago'
```

A normal health response is `status: "healthy", state: "running"`. A
failed-closed process first serves `status: "unhealthy", state:
`"failed_closed"` during its linger window and exits **78**. The daemon unit's
`RestartPreventExitStatus=78` intentionally stops automatic restart loops;
investigate the reported reason before an operator approves or repairs state.

## Local owner console

`gajaeway console` is the first-party local owner surface and must run as the service
identity that owns the UDS socket. It is a pure gateway RPC client: it has no
local checkpoint and does not open the broker, SDK, corpus, or SQLite state.

```sh
sudo -u gajaeway -H /usr/local/bin/gajaeway console \
  --state-dir /var/lib/gajaeway --profile /etc/gajaeway/profile.toml
```

Before presenting a prompt, the console calls `way.health` and `way.status` and
shows daemon state, strict-resume status, journal head cursor, lock state
(including `quarantined` and `write_mode`), and reconciliation freshness. It
refuses interactive input when the daemon is failed closed or unhealthy; repair
that condition first. Owner text is admitted only through `main.submit` with
the configured owner surface, and the displayed `delivered_as` result is the
gateway's authoritative admission decision. The gateway controls the adopted
GJC session through the broker CLI; the console does not open an SDK session.

Replies, turn state, health changes, and gate notifications use the named
`gajaeway-console` journal consumer; rendering precedes `consumer.commit`, so
a normal console restart resumes the server-owned checkpoint without replaying
settled output. If assistant replies finalized while the daemon was down, the
runtime either recovers them from durable delivery progress or emits an explicit
delivery-gap event. It never silently claims a missing reply was delivered.

For a profile with multiple configured owner surfaces, name the intended one:

```sh
sudo -u gajaeway -H /usr/local/bin/gajaeway console --surface-id OWNER_SURFACE_ID \
  --state-dir /var/lib/gajaeway --profile /etc/gajaeway/profile.toml
```

The external-host gateway cannot answer gates. `/gate GATE_ID EXPECTED_SESSION_ID JSON_ANSWER` remains a capability probe for a future
backend with validated gate receipts; the current backend returns
`{accepted:false, gate_state:"unsupported"}`. Answer the displayed gate in the
attached GJC TUI instead: `tmux attach -t <session>` when the tmux backend hosts
it, or the terminal that runs `gjc`.

## Audit journal and receipt evidence

The durable journal is exposed through `main.events.read`; begin at `1:0` only
for a bounded audit window and persist the returned `next_cursor` for the next
read:

```sh
gajaeway_rpc main.events.read '{"cursor":"1:0","limit":100}'
```

Use `journalctl` for process lifecycle evidence and correlate event cursors
with the SQLite journal. The state database uses WAL with `synchronous=FULL`:
a successful journal, receipt, lease, or consumer-settlement transaction is a
power-loss durability boundary, not merely a buffered acknowledgement. Do not
downgrade that pragma. In v1, profile approval (`profile_approved`) and
quarantine receipt recording/clearance (`lock_event`) carry durable audit
evidence; record the daemon-generated `receipt_id` and its event cursor with
the operator change record. A generic receipt API is a v2/P10 feature, so do
not claim that a v1 `receipt.list` command exists.

## Backup and integrity check

Install the standard `sqlite3` client on the host. Its `.backup` command takes
a consistent SQLite snapshot while the daemon remains live; do not copy only
the main database file and omit its WAL.

```sh
stamp=$(date -u +%Y%m%dT%H%M%SZ)
sudo install -d -o gajaeway -g gajaeway -m 0700 /var/backups/gajaeway
sudo -u gajaeway sqlite3 /var/lib/gajaeway/way-core.sqlite3 \
  ".backup '/var/backups/gajaeway/way-core-${stamp}.sqlite3'"
sudo sqlite3 /var/backups/gajaeway/way-core-${stamp}.sqlite3 'PRAGMA integrity_check;'
sudo install -m 0640 -o root -g gajaeway /etc/gajaeway/profile.toml \
  /var/backups/gajaeway/profile-${stamp}.toml
```

`PRAGMA integrity_check` must print `ok`. Back up the corpus according to its
own retention policy. The adopted GJC session and transcript are broker-managed;
there is no gateway-local SDK transcript file for `gajaeway` to copy. Retain the
session ID and broker-verified identity in the deployment record and use the broker's
retention procedure for its evidence. The SQLite database alone is not a
replacement for the corpus or broker-managed session evidence. Stop automation
and follow the quarantine runbook before any recovery that could change Git
history or reopen write authority.

## Chat adapter final-gate verification

The configured live Discord route is the final chat-adapter gate after local
console acceptance. A future Telegram adapter is subject to the same final-gate
route drill; neither chat adapter is accepted solely from local RPC fixtures.

Enable Discord **Direct Messages** and **Message Content** intents. With the
adapter unit running (so its systemd credential directory exists), verify the
configured credential and live gateway without sending a bot message:

```sh
sudo -u gajaeway -H env \
  GAJAEWAY_DISCORD_TOKEN_FILE=/run/credentials/gajaeway-discord.service/discord-token \
  /usr/local/bin/gajaeway-discord --check --state-dir /var/lib/gajaeway \
  --profile /etc/gajaeway/profile.toml
```

`gajaeway-discord --check` succeeds only after Discord `GET /users/@me` and the
running daemon's `way.health` succeed. Send one unique test DM to the configured
owner route. After the gateway accepts it, observe typing within `ack_budget_ms`
(two seconds by default); a slow broker admission can make typing arrive later
than two seconds after Discord delivery. Then observe one reply. Restart the
adapter after the reply and verify no ordinary repost occurs; a crash between
send and settlement can produce at most the documented, nonce-deduplicated retry.
Never start a second adapter for the same route.
## Known limitation: journal latency on continuously busy sessions

The credential-free broker CLI returns tail envelopes only when a terminal turn
state occurs inside the wait window. While the adopted persona works
continuously, finalized replies are visible immediately in the attached gjc TUI
(the primary owner surface), but journal consumers (Discord adapter, cockpit
transcript) receive them at the next terminal boundary. `way.status` stays live
throughout via instant context queries. If reply latency to adapters matters
for your deployment, keep turns bounded or wait for an upstream snapshot-tail
query.
