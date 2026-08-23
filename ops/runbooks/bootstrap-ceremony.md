# Bootstrap ceremony

## Preconditions and the operator-owned GJC session

`gajaeway bootstrap --confirm` is an explicit adoption ceremony, not a session
creation path. It never creates, resumes, or takes ownership of a GJC session,
and the daemon never auto-bootstraps. Complete the profile first, including the
corpus/workspace, ordered injection files, restricted-file policy, owner
surfaces, operator identity, and the exact external main-session selection.

The operator starts an interactive `gjc` in the configured corpus workspace
before bootstrap. A durable tmux owner is recommended so the real owner TUI can
be reattached after a disconnect:

```sh
tmux new-session -s gajae-main -c /srv/gajaeway/workspace gjc
```

In another operator shell, discover the live session and select the exact live
`main` session for that workspace:

```sh
gjc sdk session list
```

Record that session in the deployment profile before the first bootstrap:

```toml
[main_session]
session_id = "SESSION_ID_FROM_GJC"
```

The normal bootstrap command then reads the profile selection. An operator may
instead pass `--session-id SESSION_ID_FROM_GJC`; when both are supplied, they
must match exactly. Do not select a recent, similarly named, or different
workspace session.

The corpus must have no other writer. Stop the gateway and adapter before
adopting the operator-owned session; this does not stop the operator's `gjc`
process:

```sh
sudo systemctl stop gajaeway-discord.service gajaeway.service
sudo -u gajaeway -H /usr/local/bin/gajaeway bootstrap --confirm \
  --state-dir /var/lib/gajaeway --profile /etc/gajaeway/profile.toml
```

## What is committed atomically

The exact `--confirm` command asks the credential-free broker CLI to verify the
selected external GJC session. It verifies that the session is live, is a
`main` session, and has the configured workspace/locator before committing the
external identity and resolved profile digest in one durable state transition.
It does not create a session, inject a bootstrap prompt, scan nonce-bearing
transcripts, or adopt a fallback session.

A successful command prints JSON with `state: "committed"`, `session_id`, and
the adoption nonce. Record the session ID, broker-verified identity/locator,
and profile digest in the deployment record. Do not hand-edit
`way-core.sqlite3` to repair bootstrap state.

## Strict resume and external-owner boundaries

Every `gajaeway serve` performs strict resume against the exact committed
external session. It re-verifies the broker identity, workspace locator, and
profile digest, and fails closed on an unavailable, ambiguous, mismatched, or
otherwise unverifiable identity. It never creates a local SDK session or falls
back to a recent session.

The operator remains the owner of the real GJC TUI. Attach with `tmux attach -t <session>` (for example, `tmux attach -t gajae-main`) when tmux hosts it, or use the terminal that runs `gjc`.
The gateway sends prompts, steers, and follow-ups only through the broker CLI;
The gateway is only the session's broker-CLI controller.
## Optional persona MCP discovery

After the gateway is healthy and before asking the persona to use tools, configure
its corpus/session MCP entry to launch the existing binary as the same OS user:

```sh
/usr/local/bin/gajaeway mcp \
  --state-dir /var/lib/gajaeway \
  --profile /etc/gajaeway/profile.toml
```

This is a stdio server, so the operator-run `gjc` owns the child process and the
server connects only to `/var/lib/gajaeway/rpc.sock`. The service account must be
the same user that owns the owner-only socket; no credentials are passed to the
persona. Verify `tools/list` contains exactly `way_status`, `way_surfaces`,
`way_turn_origin`, and `way_say`. If the socket is unavailable or owned by a
different user, tool calls fail rather than falling back to broker or platform
access.

The corpus MCP configuration may use this entry:

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
Gates are answered in that attached GJC TUI, not through the gateway. The
console `/gate` command remains a capability probe for a future backend with
validated gate receipts; the external-host backend honestly reports it as
unsupported.

Replies finalized while the daemon is down are recovered from durable delivery
progress or surfaced as an explicit delivery-gap event. A missing delivery is
never silently represented as a delivered assistant reply.

## Interrupted ceremony recovery

Do not delete adoption state or reset it by hand. If bootstrap was interrupted
while recording `CREATING` or `CREATED`, rerun the same explicit bootstrap
command with the same state directory and profile. Recovery re-verifies only
the exact session ID in the durable adoption intent:

- a verified identity is committed and strict resume proceeds; and
- an unavailable or mismatched identity fails closed for manual investigation.

There is no nonce transcript scan, local transcript-file identity, or
recent-session discovery recovery path. A failed-closed daemon serves its
reason through `way.health` during its configured linger window, writes
`health.json`, then exits 78. Correct the cause or use the profile-approval
ceremony when the reported reason is `profile_drift`; do not use systemd restart
loops as a repair mechanism.

## Start after commit

After a committed result, start only the configured units:

```sh
sudo systemctl start gajaeway.service
sudo systemctl start gajaeway-discord.service
```

Verify `way.health`, `way.status`, and the adapter through the operations
runbook before admitting production traffic.
