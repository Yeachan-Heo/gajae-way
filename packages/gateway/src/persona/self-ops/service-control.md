# Service control

Restart the stack with the one supervised command, on either host:

```sh
gajaeway ops restart-stack
gajaeway ops restart-stack --status
```

The first command returns immediately. A detached supervisor then restarts the gateway followed by every adapter, in order. Restarting the gateway kills the turn that asked for the restart, but it does not kill the supervisor. After each service restarts, the supervisor checks that the new process started after its binary was last modified. A service that fails this check is reported as `stale` and the rest of the sequence is skipped. In a later turn, run `--status` to read the receipt stored in `$GAJAEWAY_HOME/restart-stack.json`. Treat the deploy as done only if `--status` prints `ok` and exits 0.

Do not chain the service-manager commands yourself. When run inline, the sequence stops at the gateway and leaves the adapters on the old binary. On macOS the supervisor runs `launchctl kickstart -k gui/$(id -u)/<label>` for each label. Never use `launchctl bootout`: it removes the job and can leave the bot offline with no automatic recovery.

A PID that changed does not prove the intended configuration loaded. After `ok`, still verify service health and a real request path.
