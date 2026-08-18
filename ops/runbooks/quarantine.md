# Lock uncertainty quarantine

## When to use this flow

Use this procedure only when `gitlock.force_release` refuses with JSON-RPC
error **1207** (`lock_holder_unverified`): the old lock holder could still be
alive, so authority cannot safely transfer. This is not an availability retry.
Stop every gateway, adapter, legacy writer, and outbound job for the corpus
before taking action. The single-writer precondition remains in force during
the entire incident.

Run the UDS calls as `gajae-way`. The `way_rpc` shell helper in
[operations.md](operations.md#monitor-the-live-uds-service) sends the required
NDJSON JSON-RPC request. First obtain the current `lease_id` from:

```sh
way_rpc way.status '{}'
```

## Fence first, then verify Git

1. Attempt the normal proven-death release once. It must return error 1207 to
   enter this path; a successful release means this runbook does not apply.

   ```sh
   way_rpc gitlock.force_release '{"lease_id":"LEASE_ID","confirm":true,"idempotency_key":"force-LEASE_ID"}'
   ```

2. Deliberately acknowledge the uncertainty and invoke the quarantine override:

   ```sh
   way_rpc gitlock.quarantine_override '{"lease_id":"LEASE_ID","confirm":true,"acknowledge_unverified":true,"idempotency_key":"quarantine-LEASE_ID"}'
   ```

   The response must report `quarantined: true`. This operation turns the
   durable write fence off; confirm it through `way.status` before touching
   Git. Do **not** clear the fence merely because the old PID later disappears.

3. Stop the daemon and adapter after the fence is durable, then establish that
   no residual writer or push remains:

   ```sh
   sudo systemctl stop gajae-way-discord.service gajae-way.service
   systemctl --no-pager status gajae-way.service gajae-way-discord.service
   pgrep -af '(^|/)(way|way-discord)( |$)' || true
   ```

4. Perform and record manual Git verification from a clean operator shell. At
   minimum, inspect the configured corpus and remote, reconcile unexpected
   refs with the owner, and verify no push remains in flight:

   ```sh
   git -C /srv/gajae-way/corpus status --porcelain=v1
   git -C /srv/gajae-way/corpus fsck --full
   git -C /srv/gajae-way/corpus fetch --prune origin
   git -C /srv/gajae-way/corpus log --left-right --graph --cherry-pick origin/main...HEAD
   git --git-dir=/path/to/authoritative/remote.git fsck --full
   ```

   Replace every example path and branch with the configured corpus and remote.
   Investigate any unrecognized commit, non-fast-forward, dirty index, or
   residual process/push before continuing.

5. Create an operator-controlled manual verification receipt in the incident or
   change-management system. It must name the corpus, remote, verified refs,
   commands/evidence, time, and responsible operator. Keep the secret-free
   receipt identifier, for example `git-verify-20260818T120000Z-INC123`; the
   v1 daemon records this opaque ID in its durable lock event but cannot verify
   an external ticket system itself.

6. From the operator shell, restart only the gateway:

   ```sh
   sudo systemctl start gajae-way.service
   ```

   Then enter a `sudo -u gajae-way -H -s` shell, define the `way_rpc` helper as
   shown in the operations runbook, and confirm the gateway is healthy but
   still quarantined before binding the exact receipt ID to fence removal:

   ```sh
   way_rpc way.status '{}'
   way_rpc gitlock.clear_quarantine '{"verification_receipt_id":"git-verify-20260818T120000Z-INC123","confirm":true,"idempotency_key":"clear-INC123"}'
   way_rpc way.status '{}'
   ```

   The final status must report `quarantined: false`. Only then start the
   adapter: `sudo systemctl start gajae-way-discord.service`.

Retain the 1207 response, override response, manual receipt ID, before/after
status, Git evidence, and journal `lock_event` cursor in the incident record.
Any uncertainty after the fence is set is a reason to keep it set and escalate,
not to retry clearance.
