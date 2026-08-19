# Profile digest approval

## When approval is required

Strict resume binds a versioned SHA-256 projection of identity/security fields.
Changing any of the following intentionally produces `profile_drift` until an
operator approves it:

- `[corpus] path` or `workspace`;
- ordered `[injection] files`;
- `[security.restricted_files]` (or the supported top-level equivalent);
- `[surfaces.owner]`, `[owner_surfaces]`, or `[owner_surface]`; and
- `[operator]` or `[identity]` values.

Do not approve an unexpected change. Treat it as possible profile or corpus
configuration tampering, restore the intended file, and investigate before
continuing.

## Approved change procedure

1. Preserve the old profile in the deployment/change record and review the
   exact planned identity/security change. Keep secrets out of both versions.
2. Stop the adapter so it cannot make new external effects while the profile is
   being changed. For a routine planned change, stop the daemon too:

   ```sh
   sudo systemctl stop gajaeway-discord.service gajaeway.service
   ```

3. Install the reviewed profile as `/etc/gajaeway/profile.toml`, retaining
   `root:gajaeway` ownership and mode `0640`.
4. Run the exact explicit approval command as the daemon identity:

   ```sh
   sudo -u gajaeway -H /usr/local/bin/gajaeway profile approve --confirm \
     --state-dir /var/lib/gajaeway --profile /etc/gajaeway/profile.toml
   ```

   The command first prints a secret-free projection diff. It then writes the
   next digest, projection, approval timestamp, receipt ID, and
   `profile_approved` journal event atomically. Store the emitted `receipt_id`
   and event cursor with the change record.
5. Start the gateway, check `way.health` and `way.status`, then start the
   adapter:

   ```sh
   sudo systemctl start gajaeway.service
   sudo systemctl start gajaeway-discord.service
   ```

During the bounded failed-closed linger window, the same command detects the
running UDS and sends its owner-authenticated `profile.approve` RPC instead of
opening SQLite directly. Run it as `gajaeway`; after the approval it is normal
for the old failed-closed process to exit 78, and the systemd unit will not loop
on that exit code.

## Mutable tunables

`[tunables]`, `[poll]`, `[ack]`, `[adapter]`/`[adapters]`, and `[policy]` are
excluded from the digest. Credential rotation and polling/acknowledgement
values therefore do not need profile approval. They are not an exemption from
change review or the single-writer rule. In v1 there is no profile file watcher:
restart the affected daemon or adapter to load a mutable configuration change.