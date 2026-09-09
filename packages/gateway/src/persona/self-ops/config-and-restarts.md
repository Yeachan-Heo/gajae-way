# Configuration and restart verification

`SIGHUP` and the `gateway.reloadConfig` verb use the same fail-safe reload implementation. Read its result rather than inferring configuration from process age:

- `changed`: live fields are `mentionAllowlist`, `channels` (including `engagement` and `audience`), `stallTimeoutMs`, and `dmPolicy`. Verify behavior through the next event that consumes the policy.
- `restartRequired`: `socketPath`, `dbPath`, `model`, `serviceTier`, `credentials`, `webhook`, `watcherRoots`, `scriptRoot`, `runtime`, `ownerTarget`, `monitorContextFailureRollThreshold`, and `work` remain unchanged until restart.
- `ignored`: `logVerbosity` is parsed but has no consumer; neither reload nor restart gives it an effect.
- Invalid, missing, or unreadable configuration fails reload and leaves the current policy intact. `debounceMs` and `settleWindowMs` are rejected, including channel-level variants; there is no debounce window.

For a restart-required change:

1. Record the configuration file modification time and the intended field change.
2. Restart out of turn through the actual host service manager. The systemd deployment reached on SSH port 24 uses `systemctl --user restart gajaeway-gateway`; only macOS launchd uses `launchctl kickstart -k gui/$(id -u)/dev.gajaeway.gateway`. Never start a second unmanaged gateway.
3. Allow at least 30 seconds for ordered shutdown. Preserve systemd `KillMode=process` while GJC daemon/session hosts share the gateway cgroup. Gateway shutdown closes only its own SDK calls and relays, never the shared user's broker or unrelated sessions.
4. Verify the new process start time is later than the configuration modification time, then verify effective behavior through the status or chat path consuming the field. A healthy PID alone proves neither configuration nor deployment success.

The service must use explicit `GJC_EXECUTABLE` matching the normal user's verified `command -v gjc`, and the same canonical user profile, agent directory, and broker. Do not copy provider/model configuration, apply private profile overrides, reap hosts, or run global session GC. Controls require positive gateway-created session ownership. Verify a newly owned session is visible in the gateway and normal user SDK with the exact same ID, unrelated sessions/configuration are unchanged, and the user daemon survives restart.

Changing broker authority is not a routine restart: stop intake, take a validated online backup, stop the service, and complete the explicit database authority census/cutover described in the operator runbook before recovery. Internal `inspectBrokerAuthority()` and `cutoverBrokerAuthority()` are storage APIs, not CLI verbs. Preserve old history and quarantine accepted or unproven old work; never replay private held lanes into the global broker. If no administrative caller is available, keep the service stopped rather than inventing a command or bypassing the authority boundary.
