# Bootstrap ceremony

## Preconditions

`way bootstrap --confirm` is the only creation path for a main identity. The
daemon never auto-bootstraps. Complete profile configuration first, including
corpus/workspace, ordered injection files, restricted-file policy, owner
surfaces, and operator identity. Ensure the service account can access the
configured SDK/broker credentials without placing them in the profile.

The corpus must have no other writer. Stop the daemon and adapter before
opening its state directory:

```sh
sudo systemctl stop gajae-way-discord.service gajae-way.service
sudo -u gajae-way -H /usr/local/bin/way bootstrap --confirm \
  --state-dir /var/lib/gajae-way --profile /etc/gajae-way/profile.toml
```

The exact `--confirm` flag is mandatory. A successful command prints a JSON
object with `state: "committed"`, `session_id`, and the bootstrap `nonce`.
Record the session ID and profile digest from the profile in the deployment
record; do not hand-edit `way-core.sqlite3` to repair bootstrap state.

## What is committed atomically

The ceremony writes `CREATING { nonce, ts }` before SDK creation. Its first
nonce-bearing message is persisted in the transcript. After two stable
fingerprints, one durable transaction publishes the full transcript identity
(canonical path, session ID, device/inode, link count, size, timestamps, and
SHA-256) and the resolved profile digest. The next `way serve` can only resume
that exact identity.

## Interrupted ceremony recovery

Do not delete a partial transcript or reset state by hand. Start the daemon
once with the same profile and state directory; it scans the durable nonce:

- exactly one matching valid transcript is committed and strict resume proceeds;
- zero matching transcripts returns the state to `ABSENT`, so repeat the same
  explicit bootstrap command; and
- multiple matches or an invalid candidate fails closed for manual
  investigation.

A failed-closed daemon serves its reason through `way.health` during its
configured linger window, writes `health.json`, then exits 78. Correct the
cause or use the profile-approval ceremony when the reported reason is
`profile_drift`; do not use systemd restart loops as a repair mechanism.

## Start after commit

After a committed result, start only the configured units:

```sh
sudo systemctl start gajae-way.service
sudo systemctl start gajae-way-discord.service
```

Verify `way.health`, `way.status`, and the adapter through the operations
runbook before admitting production traffic.