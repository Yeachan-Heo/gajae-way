# Configuration and restart verification

Channel policy fields such as `engagement` and `debounceMs` are read at gateway boot. `logVerbosity` is reloadable; do not assume other fields hot-reload.

For a boot-only change:

1. Record the configuration file modification time.
2. Restart the gateway out of turn with `launchctl kickstart -k`.
3. Verify the new process start time is later than the configuration modification time.
4. Verify the effective behavior through the status or chat path that consumes the changed field.

A live process whose start time predates the config file is still running old configuration, even when its PID looks healthy.
