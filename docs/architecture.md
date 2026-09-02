# Architecture

## Runtime layout

The Bun workspace is divided into small packages:

| Package | Responsibility |
|---|---|
| `@gajaeway/app` | Composition root and the only executable: `gajaeway`. |
| `@gajaeway/protocol` | Versioned NDJSON frames, negotiation, verb/event catalogues, and canonical origins. |
| `@gajaeway/sdk` | Client for the gateway’s Unix-domain socket or stdio transport. |
| `@gajaeway/gateway` | In-process runtime: configuration, SQLite state, sessions, delivery, memory, monitors, and the `gjc` boundary. |
| `@gajaeway/adapter-discord` | Discord ingress and outbound delivery, including typing hints. |
| `@gajaeway/adapter-telegram` | Telegram ingress and outbound delivery. |
| `@gajaeway/admin` | Loopback admin HTTP server and audit integration. |
| `@gajaeway/cli` | On-demand owner commands over the gateway socket. |

`bun run build` compiles `@gajaeway/app` into exactly `dist/gajaeway`. The app owns one private gjc agent directory at boot; gjc's per-agent-directory daemon hosts persistent sessions (auto-started by the first `gjc sdk` control call), while the gateway observes health and generation fencing rather than spawning a host process. Every persona, worker, and monitor operation uses the agent-dir-bound `SessionPort`; no per-turn `gjc --resume` process is spawned.

## Process model

### One owner and one configuration snapshot

`gajaeway daemon run` is one process. Its composition root acquires the fail-closed `$GAJAEWAY_HOME/gajaeway.pid` lock before loading configuration, installs `SIGINT`/`SIGTERM` handling, loads one immutable configuration snapshot, reads the referenced credential files once, then boots the gateway, admin server, and enabled adapters. The app—not an adapter or the admin server—owns the lock, configuration snapshot, signals, shutdown, and process exit status.

Exit status is monotonic. It begins at 0, and a boot failure or adapter escalation requests 1 before shutdown starts; a later signal or ordinary shutdown cannot lower that failure status. A clean resident shutdown remains 0. A second daemon for the same home is refused before boot with exit 2.

### Local ports and replay ordering

The admin server and every adapter generation receive a fresh `LocalGatewayPort`. They subscribe to delivery and progress events before calling `open()`. `open()` negotiates the local connection and replays unsettled deliveries only after those handlers exist, then logs `local_port_open connection=<label> replayed=N`. A stopped or failed generation never reuses its port, so replay cannot be delivered to a stale handler.

### Supervision and platform faults

The supervisor owns one live generation per enabled adapter. Its failure counter resets after 60 seconds of healthy uptime; otherwise it uses this restart table, with ±25% jitter on each delay:

| Consecutive failure | Action |
|---:|---|
| 1 | Restart after 1 second. |
| 2 | Restart after 2 seconds. |
| 3 | Restart after 4 seconds. |
| 4 | Restart after 8 seconds. |
| 5 | Log escalation and terminate the daemon with exit status 1 so launchd `KeepAlive` can restart it. |

Discord reports a fatal generation failure on login rejection or `shardDisconnect` close codes 4004 and 4010–4014. Telegram treats only HTTP 401 and 404 as fatal; HTTP 409 logs `telegram_poll_conflict` and keeps polling with transient backoff. Telegram's durable inbound key is `telegram:<botUserId>:update:<update_id>`, which prevents collisions with Discord message IDs and with a prior Telegram bot identity.

### Composite shutdown and log signals

Shutdown arms a 30-second force deadline immediately, logs `gajaeway daemon stopping reason=…`, and follows one order: (1) stop the supervisor to cancel pending restarts and abort in-flight starts; (2) stop the admin server and all live adapters in parallel, with a 10-second deadline for each adapter; (3) stop the gateway; (4) release the daemon lock. An adapter that exceeds its deadline logs `adapter_stop_timeout adapter=…` and its local port is still closed. The force deadline logs `gajaeway daemon stop forced`; completed shutdown logs `gajaeway daemon stopped status=…`.

Other grep-stable lifecycle signals are `gajaeway daemon starting pid=… home=`, `gajaeway daemon ready pid=… home= adapters=[…] admin=…`, `boot_aborted phase=…`, `daemon_lock_reclaimed stale_pid=…`, `local_port_handler_error connection=… event=… error=…`, `adapter_started adapter=… generation=…`, `adapter_failed adapter=… generation=… reason=… restartInMs=…`, `adapter_escalated adapter=… failures=5`, `adapter_disposed_after_stop adapter=… generation=…`, `adapter_stopped adapter=… generation=…`, `telegram_update_rejected update_id=… code=…`, and `discord_unrecoverable_close code=…`.

### Gateway-owned engagement

Adapters report facts—explicit mention/reply, group status, author identity and bot status, names, labels, and reply metadata—but never promote an open channel into a synthetic mention. The gateway evaluates those facts against the current configuration. Its `isAddressed` result drives the current-conversation notice: a human message in a currently open group channel is addressed even without an explicit mention, while an open-to-closed reload changes that notice on the next message without an adapter restart. Discord recovery-channel enumeration is the startup-bound exception described in deployment.

## Wire protocol and negotiation

Gateway clients exchange newline-delimited JSON frames over a Unix socket (or stdio). A frame has a profile version and a type: `hello`, `negotiated`, `request`, `response`, `event`, or `error`; an encoded frame is capped at 1 MiB.

A client must start with `hello`, listing `supportedVersions` and optionally `requiredCapabilities`. The server chooses the highest mutually supported profile version, currently from `0.1` and `1.0`, and returns its capability list. No common version produces `incompatible_profile_version`; a missing required capability produces `missing_required_capability`. Unknown optional fields are ignored, while required capabilities are explicit compatibility checks. Requests then invoke catalogued verbs and asynchronous events include `chat.message`, `monitor.event`, and `gateway.stopping`.

## Conversation identity and sessions

An origin is a validated, platform-neutral record: platform, kind, conversation ID, and (only where appropriate) parent or peer IDs. Platforms are `loopback`, `discord`, `telegram`, and `monitor`; kinds include DM, channel, thread, topic, loopback, and monitor event type. The only canonical identity string is `originKey`:

```text
platform/kind/conversationId[/parent=…][/peer=…]
```

It is opaque after creation—callers must not parse it back into fields. Each origin key has one persistent broker-hosted `gjc` session and an epoch. `/new` (and `/reset`) increments that origin’s epoch and binds the next turn to a new session, leaving other origins untouched. Session creation is idempotent and includes the gateway instance, normalized origin key, and epoch, so a crash around creation can safely retry while the broker remains live.

Inbound messages enter a durable per-origin mailbox actor. Its fixed-from-first settle window forms one batch with a deterministic caller operation reference and records the bound SDK session before send; an accepted receipt advances the batch independently of terminal completion. The actor serializes admission, steering, tail frames, broker-generation changes, `/model` live controls, and `/new` fencing. A single broker-owned logical tail observes activity and transcript frames, while `session status` remains terminal authority. Different origins work concurrently, but same-origin persona, worker, and monitor requests use the shared `SessionPort` lock. A terminal failure generates a visible, ledgered notice carrying the runtime's own error code and message, truncated but never erased, with only secrets redacted:

```text
[turn failed] spawn_failed: SDK startup did not complete before readiness cutoff. Send /new to rebind this conversation.
```

The same string goes to the daemon log, so a channel transcript is enough to triage without shell access. The `/new` hint appears only when a rebind is plausibly the remedy.

`/model` applies `model.set` to the existing session through that mailbox; it neither changes the epoch nor truncates the transcript, and each subsequent persona turn logs its effective model. `/new` is the explicit recovery boundary: it atomically advances the epoch, fences stale output, and discards only still-unbatched pending messages. Accepted prior-epoch batches retain their bound session identity as durable recovery holds, so a restarted actor can reattach status/tail until terminal reconciliation; the gateway never invents a replacement operation reference or aborts a running SDK turn.

### Persistent-session observability

The daemon writes grep-stable fields for the persistent-session control plane: `steer_delivered originKey=… opRef=… messageId=…`, `stall_alert originKey=… sessionId=… silentMs=…`, `compaction_event sessionId=… originKey=…`, `broker_restart generation=… backoffMs=…`, and `retired_hold originKey=… batchKey=…`. A stall is an operator alarm only; it never terminates a live operation. A compaction event is observed from a tail frame or authenticated control receipt, never initiated as persona-turn orchestration.

The persona preamble comes from `SOUL.md`, `AGENTS.md`, and `USER.md` in `$GAJAEWAY_HOME/workspace`; the same directory is passed to `gjc` as the session working directory.

The first attempted turn in every origin epoch also receives a trusted `Session bootstrap` system section. Its stable ID is derived from the opaque origin key and epoch. It carries current conversation metadata plus bounded navigation from `$GAJAEWAY_HOME/memory`; it is never placed in unread/user text. The serialized section is capped at 8 KiB by UTF-8 bytes, includes only whole sections, and names omissions explicitly. Source paths are realpath-confined to the resolved memory root, so a configured root symlink and in-root canonical symlinks work while traversal and escaped symlinks are rejected. Public/group turns require explicit full-origin association for channel/project/task/handoff material, exact-origin filtering for daily entries, and an explicit `bootstrap-safe: public` or `bootstrap-visibility: public` marker before associated document bodies are admitted. Included source material is delimited as reference data, never executable instruction text.

Bootstrap completion is durable in schema 14. An epoch is pending while `epoch > last_bootstrapped_epoch`; `/new` establishes pending state in the same session-row update that bumps the epoch. The gateway commits the projection only after a terminal text or intentional-silence success. A failure before terminal success retries with the same stable bootstrap ID. If an intermediate reply was already delivered before failure, the unread body is consumed to prevent a harmful answer replay, while the still-pending bootstrap is safely re-presented under that same idempotent ID on the next turn. Session and ops projections expose only epoch, applied time, included section names, byte count, truncation, and diagnostics—never source bodies.

## Delivery and crash semantics

Before an outbound reply is emitted, the gateway creates a durable ledger item. States are `pending`, `inflight`, `confirmed`, `failed_ambiguous`, and `expired`. Adapters mark an item inflight before attempting platform delivery and confirm it after success. A non-ambiguous failure returns to `pending` with exponential backoff; after three attempts it expires. An ambiguous failure stays `failed_ambiguous`.

At negotiated connection time, adapters receive unsettled deliveries created within the last 24 hours. This is deliberately at-least-once delivery: an `inflight` or `failed_ambiguous` item is reissued with `redelivered: true` and `duplicateWarning: true`. Platform adapters must display the duplicate warning. Confirmed or expired records are not replayed; old settled records are pruned after seven days.

## Engagement and safety floors

Loopback is trusted; direct-message and group engagement are evaluated by the gateway from the configured policy. Adapters provide raw engagement facts only. A human message in a configured `engagement: "open"` group can engage and is treated as addressed by the gateway; unconfigured group traffic remains mention-gated and policy-authorised.

The gateway gives `gjc` unoverridable ActionGuard guidance. It forbids unrecoverable commands such as recursive removal of `/`, filesystem formatting, raw device writes, and fork bombs. It also refuses recursive deletion of `$HOME` itself or absolute paths outside `$HOME` and `$GAJAEWAY_HOME`. These are floors, not a configurable permission bypass.

See [deployment](deployment.md), [memory](memory.md), and [monitors](monitors.md) for operational details.
