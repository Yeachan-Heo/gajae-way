# State inspection

Prefer supported runtime surfaces first:

```sh
gajaeway status
gajaeway sessions list --json
gajaeway sessions inspect <origin-key-or-index>
gajaeway ops cycle --json
gajaeway memory audit
```

When direct SQLite inspection is required, stop at read-only queries against `$GAJAEWAY_HOME/gateway.db`. Inspect schema before assuming column names. Useful state includes:

- unsettled delivery rows and their last error
- per-origin engagement and session epoch
- bootstrap byte count, truncation flag, and diagnostics
- schema version

Do not edit ledger, session, or migration rows by hand to make an incident look resolved. Preserve the evidence and repair through the owning runtime operation.
