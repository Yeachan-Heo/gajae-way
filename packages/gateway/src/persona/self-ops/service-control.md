# Service control

Use launchd kickstart, never bootout, for a managed restart:

```sh
launchctl kickstart -k gui/$(id -u)/dev.gajaeway.gateway
launchctl kickstart -k gui/$(id -u)/dev.gajaeway.adapter-discord
```

`launchctl bootout` removes the job and can leave the bot offline with no automatic recovery.

A restart invoked inside an inbound persona turn destroys that turn before its reply reaches the gateway. Queue the restart for an out-of-turn operator context, let the current turn finish, then execute it.

After restart, verify process start time, service health, and a real request path. A changed PID alone is not proof that the intended configuration loaded.
