# Configuration and restart verification

`SIGHUP` and the `gateway.reloadConfig` verb use the same fail-safe reload implementation. Read its result rather than inferring configuration from process age:

- `changed`: live fields are `mentionAllowlist`, `channels` (including `engagement` and `audience`), `stallTimeoutMs`, and `dmPolicy`. Verify behavior through the next event that consumes the policy.
- `restartRequired`: `socketPath`, `dbPath`, `model`, `serviceTier`, `credentials`, `webhook`, `watcherRoots`, `scriptRoot`, `runtime`, `ownerTarget`, `monitorContextFailureRollThreshold`, and `work` remain unchanged until restart.
- `ignored`: `logVerbosity` is parsed but has no consumer; neither reload nor restart gives it an effect.
- Invalid, missing, or unreadable configuration fails reload and leaves the current policy intact. `debounceMs` and `settleWindowMs` are rejected, including channel-level variants.

For a restart-required change:

1. Record the configuration file modification time and the intended field change.
2. Use the service manager or `gajaeway ops restart-stack`; do not start an unmanaged gateway. A normal gateway restart does not restart the broker service.
3. Allow at least 30 seconds for ordered shutdown. The gateway holds `gateway` then `memory`; the broker service independently holds `broker`. Each adapter holds its own `adapter-<platform>` lease.
4. Verify the new process start time is later than the configuration modification time, then verify effective behavior through the status or chat path consuming the field. A healthy PID alone proves neither configuration nor deployment success.

The gateway uses the pinned GJC executable and `gjc.agentDir`, which defaults to `homeLayout(home).brokerAgentDir` under `$GAJAEWAY_HOME/broker/agent`. The broker service owns that directory under the `broker` lease. `gajaeway setup --broker-agent` provisions an allowlisted snapshot from the source profile; it does not link the private directory to the source. Gajaeway never writes the source `~/.gjc/agent` from its setup or runtime paths.

The environment variable `GJC_SDK_BROKER_AUTOSTART=0` is set for GJC child processes. The gateway package pins GJC 0.18.9, which honors this attach-only contract: when the broker is down, GJC commands fail with `broker_unavailable` instead of starting a broker. Only the broker service starts the broker. Do not try to start one yourself; report the outage and let the service manager restart it.

A change to `gjc.agentDir` changes database authority and is not a routine restart. Stop the gateway, take a validated backup, then run `gajaeway-gateway census` and `gajaeway-gateway cutover --agent-dir <canonical>` offline. Restore with `gajaeway-gateway restore <backup> [--abort]`. Review the exported open-work evidence; the cutover quarantines old accepted or unproven work and does not replay it. Migration 33 (L4h), which removes the quarantine and cutover tables, remains pending.
