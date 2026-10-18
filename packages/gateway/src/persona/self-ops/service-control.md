# Service control

Use the supervised command to realign the installed service stack:

```sh
gajaeway ops restart-stack
gajaeway ops restart-stack --status
```

The command acquires the `update` lease and returns after a detached supervisor is queued. It restarts the broker service, then the gateway, then its connected adapters and admin service. On Linux, systemd restarts the broker and gateway units; adapter/admin units follow the gateway dependency. On macOS, the supervisor kickstarts each managed job in that order. A gateway-only restart does not restart the broker.

Before changing services, the detached supervisor asks a live gateway to hold new persona dispatches and waits up to two minutes for accepted turns to finish and their terminal replies to be confirmed by the adapter. Messages arriving during the drain stay queued. If a turn remains active or its reply is not confirmed before the deadline, the restart fails closed, the dispatch hold is cancelled, and no service is restarted. A gateway that is already unavailable has no live turns to drain. The supervisor then checks that each service came up after the restart and deployed binary; lease-backed services must also have a lease owner whose PID matches the service manager's main PID. A failed check is recorded as `stale` and later steps are skipped. The receipt lives at `$GAJAEWAY_HOME/restart-stack.json`; `--status` reports the last sequence and its step diagnostics.

On the first `update` from a gateway that predates restart draining, the receipt warns that the stack restart relied on durable turn recovery. A manual restart fails closed when the live gateway cannot prepare a drain.

Do not chain service-manager restart commands by hand: dependency behavior differs between systemd and launchd. When run inline, the sequence stops at the gateway and leaves the adapters on the old binary. On macOS the supervisor runs `launchctl kickstart -k gui/$(id -u)/<label>` for each label. Never use `launchctl bootout`: it removes the job and can leave the bot offline with no automatic recovery. For an update-triggered restart, the supervisor stops the broker, runs journaled broker-agent provisioning in update mode under the broker lease, then starts the broker again. If provisioning is unrecoverable, follow the emitted `gajaeway setup --broker-agent` recovery instruction before restarting the broker. A PID change alone does not prove that the intended configuration or binary loaded; verify service health and a real request path.

A host without a service manager (a container with no `systemd-run`) cannot run the supervisor: the command fails at once, and `--status` reads `failed` with a `detail:` line instead of staying `queued`. Restart such a host through whatever supervises it (for example its container runtime); do not retry restart-stack there.
