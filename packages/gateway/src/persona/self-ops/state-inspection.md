# State inspection

Prefer supported runtime surfaces first:

```sh
gajaeway status
gajaeway sessions list --json
gajaeway sessions inspect <origin-key-or-index>
gajaeway work status <name> --progress
gajaeway ops cycle --json
gajaeway memory audit
```

`sessions list` is paged by the gateway; follow its `next: --offset` hint instead of raising `--limit`. `work status <name> --progress` shows what a lane is doing from its transcript without reaching it; use `work steer` only to change what the lane does.
A tool status of `unknown` means a skipped transcript row may have contained its result; do not infer that the call is still running or aborted.

Use supported read-only projections: `gajaeway ops cycle|deliveries|integrity` while the gateway is online, and `gajaeway-gateway census` while it is stopped. Do not inspect the database file directly.

`gajaeway ops cycle --json` includes up to five recent monitor failures in `monitorFailures`, with bounded, sanitized cause details. Use those codes and details to diagnose dispatch failures; never query `monitor_failures` directly.

Do not edit ledger, session, or migration rows by hand to make an incident look resolved. Preserve the evidence and repair through the owning runtime operation.
