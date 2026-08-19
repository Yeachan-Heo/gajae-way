# Lock uncertainty quarantine

## When to use this flow

Use this procedure only when `gitlock.force_release` returns JSON-RPC error
**1207** (`lock_holder_unverified`): the existing holder may still be alive, so
write authority cannot transfer. This is not an availability retry. Never clear
or replace a lock holder based on a timeout alone.

The daemon must remain reachable long enough to record the durable quarantine
override. Stop external ingress, legacy writers, and outbound jobs **after**
the override below; do not stop the gateway before the UDS calls that establish
the fence. Run `gajaeway_rpc` as `gajaeway` using the helper in
[operations.md](operations.md#monitor-the-live-uds-service).

## 1. Detect the holder and set the durable write fence

Read the current lease and copy `lock.holder.lease_id` as `LEASE_ID`:

```sh
gajaeway_rpc way.status '{}'
```

Attempt normal proven-death release once. A successful release means this
runbook does not apply. Error 1207 means the holder is still alive or cannot be
proven dead:

```sh
gajaeway_rpc gitlock.force_release '{"lease_id":"LEASE_ID","confirm":true,"idempotency_key":"force-LEASE_ID"}'
```

Acknowledge the uncertainty and fence corpus writes:

```sh
gajaeway_rpc gitlock.quarantine_override '{"lease_id":"LEASE_ID","confirm":true,"acknowledge_unverified":true,"idempotency_key":"quarantine-LEASE_ID"}'
gajaeway_rpc way.status '{}'
```

The second response must show `lock.quarantined: true` and `write_mode: false`.
Do not continue merely because the old PID later disappears: the receipt and a
fresh runtime death proof are both still required.

## 2. Stop external ingress and prepare gateway-only recovery

The normal daemon unit has a `Wants=gajaeway-discord.service` edge so an
ordinary daemon restart restores the adapter. Quarantine recovery uses the
adapter unit's persistent, root-owned **no-ingress interlock**:
`ConditionPathExists=!/etc/gajaeway/recovery/no-discord-ingress`. Creating the
marker blocks every adapter start, including the daemon's `Wants=` edge and
automatic restart paths, until clearance removes it.

```sh
sudo install -d -o root -g root -m 0700 /etc/gajaeway/recovery
sudo install -o root -g root -m 0600 /dev/null /etc/gajaeway/recovery/no-discord-ingress
sudo systemctl stop gajaeway-discord.service gajaeway.service
if sudo systemctl is-active --quiet gajaeway-discord.service; then
  echo 'Discord ingress is still active; do not continue.' >&2
  exit 1
fi
systemctl --no-pager status gajaeway.service gajaeway-discord.service
pgrep -af '(^|/)(gajaeway|gajaeway-discord)( |$)' || true
```

The process inspection must show no residual gateway, adapter, legacy writer,
or in-flight closure process. If any remains, stop it under the incident
procedure and inspect again; it is false to attest `process_inspected` while a
recorded holder can still run.

## 3. Perform manual Git verification

From a clean operator shell, replace all example paths and branch names with
the configured corpus and authoritative remote. Reconcile any unrecognized
commit, non-fast-forward history, dirty index, residual process, or push before
recording a receipt.

```sh
git -C /srv/gajaeway/corpus status --porcelain=v1
git -C /srv/gajaeway/corpus fsck --full
git -C /srv/gajaeway/corpus fetch --prune origin
git -C /srv/gajaeway/corpus log --left-right --graph --cherry-pick origin/main...HEAD
git --git-dir=/path/to/authoritative/remote.git fsck --full
```

These checks map directly to the required daemon attestation fields:
`process_inspected`, `git_status_checked`, `git_log_checked`,
`git_fsck_checked`, and `remote_verified`. Set each field to `true` only after
its corresponding inspection succeeded. An external ticket or an operator-made
opaque receipt ID is not accepted for clearance.

## 4. Start the gateway-only recovery daemon and record the receipt

The root-owned no-ingress marker remains in place, so this `start` may satisfy
the daemon's normal `Wants=` edge but the adapter's condition rejects ingress:

```sh
sudo systemctl start gajaeway.service
sudo systemctl is-active --quiet gajaeway.service
test "$(sudo systemctl show --value --property=ActiveState gajaeway-discord.service)" = inactive
test "$(sudo systemctl show --value --property=ConditionResult gajaeway-discord.service)" = no
sudo -u gajaeway -H /usr/local/bin/gajaeway --health --state-dir /var/lib/gajaeway
```

Do not remove the marker during this recovery phase. With the gateway-only
recovery daemon healthy, confirm the fence, then record the completed checks.
In v1 the sole corpus lock name is the literal string `corpus`; it is not the
filesystem path.

```sh
gajaeway_rpc way.status '{}'
gajaeway_rpc gitlock.record_quarantine_receipt '{"lease_id":"LEASE_ID","corpus":"corpus","checks":{"process_inspected":true,"git_status_checked":true,"git_log_checked":true,"git_fsck_checked":true,"remote_verified":true},"idempotency_key":"quarantine-receipt-INCIDENT"}'
```

The RPC performs a fresh death proof and returns a daemon-generated
`receipt_id` in the form `git-verify-<32 lowercase hex>`, bound to the
quarantined lease, the `corpus` lock, and the holder process incarnation. It
rejects missing/false checks, a still-live holder, an unknown lease, and an
unbound receipt. Copy the returned `receipt_id` as `RECEIPT_ID`; do not invent
one.

## 5. Clear, verify write authority, then restore ingress

Clear exactly the generated receipt, then confirm the fence is open before the
adapter is allowed back:

```sh
gajaeway_rpc gitlock.clear_quarantine '{"verification_receipt_id":"RECEIPT_ID","confirm":true,"idempotency_key":"clear-INCIDENT"}'
gajaeway_rpc way.status '{}'
```

The final status must show `lock.quarantined: false` and `write_mode: true`.
That is the durable authority transition; the next normal authorized corpus
closure runs through the supervised executor and should be verified against the
remote rather than by creating an ad hoc probe commit.

Only after that status check succeeds, restore external ingress by removing the
root-owned interlock and starting the adapter:

```sh
sudo rm -f /etc/gajaeway/recovery/no-discord-ingress
sudo rmdir /etc/gajaeway/recovery 2>/dev/null || true
sudo systemctl start gajaeway-discord.service
sudo systemctl is-active --quiet gajaeway-discord.service
```

Retain the 1207 response, quarantine override, manual Git command evidence,
daemon-generated receipt ID, before/after `way.status`, and both `lock_event`
cursors in the incident record. Any uncertainty after the fence is set is a
reason to keep it set and escalate, not to retry clearance.