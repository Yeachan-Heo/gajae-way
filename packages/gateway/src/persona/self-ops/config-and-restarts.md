# Configuration and restart verification

`SIGHUP` and the `gateway.reloadConfig` verb use the same fail-safe reload implementation. Read its result rather than inferring configuration from process age:

- `changed`: live fields are `mentionAllowlist`, `channels` (including `engagement` and `audience`), `stallTimeoutMs`, `dmPolicy`, and `sessions`. Verify behavior through the next event that consumes the policy.
- `restartRequired`: `socketPath`, `dbPath`, `model`, `serviceTier`, `credentials`, `webhook`, `watcherRoots`, `scriptRoot`, `outboundFileRoots`, `runtime`, `ownerTarget`, `monitorContextFailureRollThreshold`, and `work` remain unchanged until restart.
- A model selector GJC cannot resolve returns `model_not_found` on bind. Correct `model` and retry; this failure does not rotate the session epoch or consume the poisoned-create recovery cap.
- `ignored`: `logVerbosity` is parsed but has no consumer; neither reload nor restart gives it an effect.
- `sessions.maxLiveHosts` (default 8, 1..256) caps concurrently live persona session hosts (`gjc sdk session-host-internal`, roughly 360 MB each). At the cap the least-recently-active idle host is ended before a new origin binds; when every host is busy the new turn is held (`persona_bind_hold ... reason=host_capacity`) and dispatched as soon as a host frees. `sessions.idleRetireMs` (default 6h, 60s..7d) ends hosts idle that long on the minute sweep. An ended host keeps its session: the next turn resumes it with history (`session_resumed ... reason=idle_dead_binding`). A lowered cap never kills a busy host.
- Invalid, missing, or unreadable configuration fails reload and leaves the current policy intact. `debounceMs` and `settleWindowMs` are rejected, including channel-level variants; there is no debounce window.

For a restart-required change:

1. Record the configuration file modification time and the intended field change.
2. Use `gajaeway ops restart-stack`; it fences new persona dispatches and waits for active-turn replies to be confirmed by adapters before restarting services. Do not start an unmanaged gateway. A gateway-only restart does not restart the broker service.
3. Allow at least 40 seconds for ordered shutdown plus the bounded persona reply drain (up to two minutes). Messages arriving during the drain stay queued. If an accepted turn remains active or its terminal reply is unconfirmed, the restart fails closed and no service is restarted; cancelling the drain resumes queued dispatches. The gateway holds `gateway` then `memory`; the broker service independently holds `broker`. Each adapter holds its own `adapter-<platform>` lease.
4. Verify the new process start time is later than the configuration modification time, then verify effective behavior through the status or chat path consuming the field. A healthy PID alone proves neither configuration nor deployment success.

The gateway uses the pinned GJC executable and `gjc.agentDir`, which defaults to `homeLayout(home).brokerAgentDir` under `$GAJAEWAY_HOME/broker/agent`. The broker service owns that directory under the `broker` lease. `gajaeway setup --broker-agent` provisions an allowlisted snapshot from the source profile; it does not link the private directory to the source. Gajaeway never writes the source `~/.gjc/agent` from its setup or runtime paths.

The environment variable `GJC_SDK_BROKER_AUTOSTART=0` is set for GJC child processes. The gateway package pins GJC 0.19.0; GJC 0.18.9 and later honor this attach-only contract: when the broker is down, GJC commands fail with `broker_unavailable` instead of starting a broker. Only the broker service starts the broker. Do not try to start one yourself; report the outage and let the service manager restart it.

A change to `gjc.agentDir` changes database authority and is not a routine restart. Stop the gateway, take a validated backup, then run `gajaeway-gateway census` and `gajaeway-gateway cutover --agent-dir <canonical>` offline. Restore with `gajaeway-gateway restore <backup> [--abort]`. Review the exported open-work evidence; the cutover quarantines old accepted or unproven work and does not replay it. The current schema is 35; the quarantine and cutover tables remain broker-authority evidence.
