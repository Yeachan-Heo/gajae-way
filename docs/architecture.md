# Architecture

## Runtime layout

The Bun workspace is divided into small packages:

| Package | Responsibility |
|---|---|
| `@gajaeway/protocol` | Versioned NDJSON frames, negotiation, verb/event catalogues, and canonical origins. |
| `@gajaeway/sdk` | Client for the gateway’s Unix-domain socket or stdio transport. |
| `@gajaeway/gateway` | Daemon: configuration, SQLite state, sessions, delivery, memory, monitors, and the `gjc` boundary. |
| `@gajaeway/adapter-discord` | Discord ingress and outbound delivery, including typing hints. |
| `@gajaeway/adapter-telegram` | Telegram ingress and outbound delivery. |
| `@gajaeway/cli` | Owner commands over the gateway socket. |

`bun run build` compiles the gateway, Discord adapter, Telegram adapter, and CLI into standalone executables. At boot, the gateway takes ownership of one private gjc agent directory; gjc's own per-agent-dir daemon hosts the sessions (auto-started by the first `gjc sdk` command), and the gateway's `BrokerSupervisor` observes it (health, generation fencing) rather than spawning a host process. Every persona, worker, and monitor operation then uses the agent-dir-bound `SessionPort`; no per-turn `gjc --resume` process is spawned; `gjc sdk` commands are short-lived control calls against the daemon.

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

Loopback and DMs engage automatically. Group traffic engages only when the adapter reports a mention, or the configured channel is explicitly `engagement: "open"`. Unconfigured group traffic therefore remains mention-gated.

The gateway gives `gjc` unoverridable ActionGuard guidance. It forbids unrecoverable commands such as recursive removal of `/`, filesystem formatting, raw device writes, and fork bombs. It also refuses recursive deletion of `$HOME` itself or absolute paths outside `$HOME` and `$GAJAEWAY_HOME`. These are floors, not a configurable permission bypass.

See [deployment](deployment.md), [memory](memory.md), and [monitors](monitors.md) for operational details.
