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

## Persona gateway tools (MCP stdio)

The operator-run persona may discover only four gateway-mediated tools through the
existing `gajaeway` binary: `way_status`, `way_surfaces`, `way_turn_origin`, and
`way_say`. These tools do not expose Discord credentials, a generic RPC proxy,
Git/lock controls, or gate/quarantine operations. `way_say` appends a durable,
surface-attributed `assistant_message` with `origin: "persona"` and never calls
`main.submit`, so it cannot create a self-feeding turn.

Configure the operator-run GJC session's corpus MCP entry with this server command,
for example in the corpus workspace's MCP configuration:

```json
{
  "mcpServers": {
    "gajaeway": {
      "command": "/usr/local/bin/gajaeway",
      "args": ["mcp", "--state-dir", "/var/lib/gajaeway", "--profile", "/etc/gajaeway/profile.toml"]
    }
  }
}
```

The command is equivalent to:

```sh
/usr/local/bin/gajaeway mcp \
  --state-dir /var/lib/gajaeway \
  --profile /etc/gajaeway/profile.toml
```

The MCP server is stdio JSON-RPC. The GJC process starts it as its child and reads
its tool list from `tools/list`; do not put a Discord token, REST endpoint, or a
second adapter in that configuration. The command must run as `gajaeway` (or the
same OS user that owns `/var/lib/gajaeway/rpc.sock`); connecting from another user
is intentionally refused by the filesystem-protected owner-only UDS.

`way_turn_origin` returns a surface only for one currently busy, unambiguous
admitted turn whose origin is proven by gateway admission. It fails with
`turn_origin_unavailable` for autonomous, idle, or ambiguous concurrent context.
`way_say` requires an exact configured surface and an idempotency key. It is capped
to 20 persona messages per 60 seconds and is replay-safe across retries; the
journal event is delivered by the existing outbox using its explicit `surface_id`.

The `way_say` journal frame is an `assistant_message` with this explicit marker
(and is therefore distinguishable from a broker turn reply):

```json
{
  "finalized": true,
  "origin": "persona",
  "persona_initiated": true,
  "text": "...",
  "surface_id": "configured-surface-id",
  "idempotency_key": "caller-key",
  "request_hash": "sha256-of-canonical-request"
}
```

The existing outbox consumes the frame only after `consumer.commit` can settle its
confirmed send. Its deterministic event/nonce dedupe and the gateway idempotency
record prevent a retried `way_say` from creating a second journal frame or post.

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

The configured live Discord route table is the final chat-adapter gate after local
console acceptance. A future Telegram adapter is subject to the same final-gate
route drill; neither chat adapter is accepted solely from local RPC fixtures.

Enable Discord **Direct Messages**, **Guild Messages**, and **Message Content** intents. A thread under a configured guild-channel route is resolved through a
cached read-only `GET /channels/{thread_id}` parent lookup; any unresolved or
unrouted channel remains unsubmitted. Guild-channel routes default to
`groupPolicy = "mention"`: only a direct bot user mention or a reply to the bot
is admitted. Use `groupPolicy = "open"` only for intentionally open channels;
the legacy `engagement = "mention"`/`"always"` key remains accepted, but
conflicting duplicate policy keys are rejected. DMs and routed threads are always
engaged. Bot-authored messages are allowed by default (`allowBots = true`), while
the adapter's own bot id is always ignored; set `allowBots = false` to block other
bots. `blocked_author_ids` drops listed users on every route, including DMs, before
admission. Drops are silent on Discord and rate-limited in adapter diagnostics.

Outbound Discord presentation is not journal state: markdown tables are rendered
as bullet lists, and when a reply contains multiple bare links each link is
wrapped in angle brackets to suppress embeds. Replies over 2,000 characters are
sent as ordered chunks with deterministic per-event/per-chunk nonces. The
consumer commits only after every chunk is confirmed; a restart may retry an
already-posted chunk with the same nonce, while Discord suppresses the duplicate.
Replies to accepted messages use `message_reference` when the trigger remains
available and degrade to a normal post when it has been deleted.
systemd credential directory exists), verify the configured credential and live
gateway without sending a bot message:

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

Telegram channels run in the same `gajaeway-discord` adapter release unit when
`[adapter.telegram].enabled = true`; `gajaeway-telegram` is only an alias to that
binary. Telegram `getMe` is checked with `--check`, and credentials come from a
root-owned token file or process-start credential environment. Long-polling
updates are persisted in `telegram-offset.json`; the offset advances only after
successful handler completion, so restart neither skips nor reprocesses updates.
Telegram outbound sends use durable `dedupe_key:chunk:index` records in that same
state file before commit, preventing duplicate posts despite Telegram lacking
Discord's enforce_nonce. Typing uses `sendChatAction` and follows the existing
acceptance-before-acknowledgement keepalive ordering.
## Recover a failed-closed gateway without discarding the journal

A gateway that cannot prove its own safety refuses to serve and records a durable
`FAILED_CLOSED` marker with a reason. That marker short-circuits strict resume, so
fixing the underlying cause is not by itself enough to bring the daemon back.

`gajaeway recover --confirm` is the explicit ceremony that clears exactly one
recorded reason, and only after re-proving from live evidence that its cause is
gone. It preserves the durable journal, tail checkpoint, transcript delivery
progress, consumer checkpoints, and admission records; it appends a
`failed_closed_recovered` receipt carrying the cleared reason, a receipt id, and
the re-verification evidence that authorized it.

```sh
sudo -u gajaeway -H /usr/local/bin/gajaeway recover --confirm \
  --state-dir /var/lib/gajaeway --profile /etc/gajaeway/profile.toml
```

Recoverable reasons, each gated on its own live re-verification:

| Reason | Re-verification required |
|---|---|
| `main_identity_mismatch` | The broker transcript must be an attested append-only extension of the persisted prefix. The identity re-bind lands in the same transaction that clears the marker. |
| `session_unavailable`, `turn_state_unavailable`, `tail_resync_unavailable` | A fresh broker verification must now succeed for the same session and locator. |
| `transcript_proof_persist_failed`, `tail_ring_rotation_write_failed`, `transcript_delivery_progress_write_failed` | Durable metadata must read back coherent, and the clearing transaction itself must succeed — a store still refusing writes fails recovery without clearing anything. |

Terminal reasons that recovery deliberately refuses:

- `main_admission_recovery_unprovable`, `main_admission_intent_invalid` — an
  unprovable admission outcome is never cleared by fiat, because doing so could
  resend or silently lose an owner command. Investigate, then re-adopt.
- `growth_intent_mismatch`, and any `main_identity_mismatch` whose transcript is
  not an append-only extension — rewritten, reordered, or truncated history.
- `metadata_invalid` / `metadata_missing` and any corrupt-durable-state reason.
- `profile_drift` — cleared by the profile-approval ceremony instead; recovery
  refuses and says so.

Anything not listed as recoverable fails closed on the classification itself.
Recovery never runs automatically on boot, never retries in a loop, and never
clears a reason the runtime cannot presently prove resolved. When recovery
refuses, the remaining remedy is the explicit re-adoption ceremony in
`bootstrap-ceremony.md`, which does reset journal history.

### Telegram surface notes

Forum topics are the Telegram thread analogue. A topic message's
`message_thread_id` becomes its routed channel id, so derived thread surfaces stay
numeric (`<parent-surface>/thread:<topic-id>`) and the parent supergroup is
resolved from the topic the adapter observed on ingress; replies and typing for a
topic-originated turn are sent back into that topic. A topic whose parent
supergroup is not an explicitly routed channel stays refused.

Reply-to-bot engagement needs no extra API call on Telegram: `reply_to_message`
carries the replied-to author, so the shared engagement policy resolves it from
ingress. Message reactions are NOT implemented for Telegram; the adapter's
`react` is intentionally a no-op there and reactions must not be relied on for
Telegram surfaces.

## Known limitation: journal latency on continuously busy sessions

The credential-free broker CLI returns tail envelopes only when a terminal turn
state occurs inside the wait window. While the adopted persona works
continuously, finalized replies are visible immediately in the attached gjc TUI
(the primary owner surface), but journal consumers (Discord adapter, cockpit
transcript) receive them at the next terminal boundary. `way.status` stays live
throughout via instant context queries. If reply latency to adapters matters
for your deployment, keep turns bounded or wait for an upstream snapshot-tail
query.
