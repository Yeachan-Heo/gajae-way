# Profile digest approval

Identity/security profile changes (corpus path/workspace, injection order,
restricted-file policy, owner surfaces, or operator identity) intentionally
fail strict resume with `profile_drift` until an operator records approval:

```text
way profile approve --confirm --state-dir /var/lib/gajae-way --profile /etc/gajae-way/profile.toml
```

The command emits the secret-free projection diff and writes the new digest,
projection, approval receipt, and `profile_approved` journal event atomically.
It opens durable state directly while the daemon is down. During the bounded
failed-closed linger window it uses the owner-authenticated UDS RPC instead.
Poll intervals, acknowledgement budgets, policy tuning, and adapter credential
rotation remain outside this digest and do not require approval.
