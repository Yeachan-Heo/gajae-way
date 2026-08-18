# Operations

## Operating invariant

Exactly one gateway may write a corpus at a time. Never run a second `way`
instance, or `way` alongside a legacy bot or outbound job, against the same
checkout or remote. Stop the other writer first; quarantine and Git
non-fast-forward checks are incident backstops, not a concurrency plan.

## Install, configure, bootstrap, start

1. Build and install the two release executables, create the `gajae-way` system
   user, and install both unit files as described in the repository README.
   Keep the service account home under `/var/lib/gajae-way`, not under `/home`.
2. Copy `ops/profiles/gaebal-gajae.example.toml` to
   `/etc/gajae-way/profile.toml`. Replace the corpus/workspace, injection
   files, owner mapping, operator identity, Discord route, and all examples.
   Install it `root:gajae-way`, mode `0640`.
3. Edit `ReadWritePaths=` in `gajae-way.service` so it names the exact
   configured corpus and workspace. Reinstall the unit and run
   `sudo systemctl daemon-reload`. The state path is
   `/var/lib/gajae-way`; leave it in the list.
4. Put the Discord token in
   `/etc/gajae-way/credentials/discord-token`, owned by `root:root`, mode
   `0600`. `LoadCredential=` exposes it only to the adapter as a private file;
   never put a token in the profile, a shell history, or a unit environment.
5. With both services stopped, perform the one-time bootstrap ceremony:

   ```sh
   sudo systemctl stop gajae-way-discord.service gajae-way.service
   sudo -u gajae-way -H /usr/local/bin/way bootstrap --confirm \
     --state-dir /var/lib/gajae-way --profile /etc/gajae-way/profile.toml
   ```

6. Start the daemon first, then its bound adapter:

   ```sh
   sudo systemctl enable --now gajae-way.service
   sudo systemctl enable --now gajae-way-discord.service
   ```

   The daemon sends `READY=1` only after strict resume and the UDS endpoint are
   usable. The adapter is `BindsTo=` the daemon and stops when it stops.

## Monitor the live UDS service

`way --health --state-dir /var/lib/gajae-way` queries the running daemon over
its UDS and exits non-zero with `status: "unhealthy", state: "unavailable",
reason: "daemon_unreachable"` when that daemon cannot be reached. Use it for a
concise liveness probe. Query the local UDS as the service user for `way.status`
and operational RPCs. In a shell entered by `sudo -u gajae-way -H -s`, define:

```sh
way_rpc() {
  python3 - "$1" "${2:-{}}" <<'PY'
import json, socket, sys
request = {"jsonrpc": "2.0", "id": "operations", "method": sys.argv[1], "params": json.loads(sys.argv[2])}
with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
    client.connect("/var/lib/gajae-way/rpc.sock")
    client.sendall((json.dumps(request) + "\n").encode())
    print(client.makefile("r", encoding="utf-8").readline().strip())
PY
}
```

Probe both liveness and operational state:

```sh
/usr/local/bin/way --health --state-dir /var/lib/gajae-way
way_rpc way.health '{}'
way_rpc way.status '{}'
systemctl --no-pager status gajae-way.service gajae-way-discord.service
journalctl -u gajae-way.service -u gajae-way-discord.service --since '15 minutes ago'
```

A normal health response is `status: "healthy", state: "running"`. A
failed-closed process first serves `status: "unhealthy", state:
`"failed_closed"` during its linger window and exits **78**. The daemon unit's
`RestartPreventExitStatus=78` intentionally stops automatic restart loops;
investigate the reported reason before an operator approves or repairs state.

## Audit journal and receipt evidence

The durable journal is exposed through `main.events.read`; begin at `1:0` only
for a bounded audit window and persist the returned `next_cursor` for the next
read:

```sh
way_rpc main.events.read '{"cursor":"1:0","limit":100}'
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
sudo install -d -o gajae-way -g gajae-way -m 0700 /var/backups/gajae-way
sudo -u gajae-way sqlite3 /var/lib/gajae-way/way-core.sqlite3 \
  ".backup '/var/backups/gajae-way/way-core-${stamp}.sqlite3'"
sudo sqlite3 /var/backups/gajae-way/way-core-${stamp}.sqlite3 'PRAGMA integrity_check;'
sudo install -m 0640 -o root -g gajae-way /etc/gajae-way/profile.toml \
  /var/backups/gajae-way/profile-${stamp}.toml
```

`PRAGMA integrity_check` must print `ok`. Back up the strict-resume transcript
location configured by the SDK and the corpus according to their own retention
policy as well; the SQLite database alone is not a replacement for the
transcript or corpus. Stop automation and follow the quarantine runbook before
any recovery that could change Git history or reopen write authority.

## Discord adapter verification

Enable Discord **Direct Messages** and **Message Content** intents. With the
adapter unit running (so its systemd credential directory exists), verify the
configured credential and live gateway without sending a bot message:

```sh
sudo -u gajae-way -H env \
  WAY_DISCORD_TOKEN_FILE=/run/credentials/gajae-way-discord.service/discord-token \
  /usr/local/bin/way-discord --check --state-dir /var/lib/gajae-way \
  --profile /etc/gajae-way/profile.toml
```

`way-discord --check` succeeds only after Discord `GET /users/@me` and the
running daemon's `way.health` succeed. Send one unique test DM to the configured
owner route, observe its typing acknowledgement within two seconds, then one
reply. Restart the adapter after the reply and verify no ordinary repost occurs;
a crash between send and settlement can produce at most the documented,
nonce-deduplicated retry. Never start a second adapter for the same route.