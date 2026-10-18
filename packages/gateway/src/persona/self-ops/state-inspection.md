# State inspection

Prefer supported runtime surfaces first:

```sh
gajaeway status
gajaeway sessions list --json
gajaeway sessions inspect <origin-key-or-index>
gajaeway ops cycle --json
gajaeway memory audit
```

Use supported read-only projections: `gajaeway ops cycle|deliveries|integrity` while the gateway is online, and `gajaeway-gateway census` while it is stopped. Do not inspect the database file directly.

`gajaeway ops cycle --json` includes up to five recent monitor failures in `monitorFailures`, with bounded, sanitized cause details. Use those codes and details to diagnose dispatch failures; never query `monitor_failures` directly.

Do not edit ledger, session, or migration rows by hand to make an incident look resolved. Preserve the evidence and repair through the owning runtime operation.
