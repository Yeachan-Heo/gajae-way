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

`bun run build` compiles the gateway, Discord adapter, Telegram adapter, and CLI into standalone executables. The gateway still invokes the external `gjc` executable for session creation and each turn.

## Wire protocol and negotiation

Gateway clients exchange newline-delimited JSON frames over a Unix socket (or stdio). A frame has a profile version and a type: `hello`, `negotiated`, `request`, `response`, `event`, or `error`; an encoded frame is capped at 1 MiB.

A client must start with `hello`, listing `supportedVersions` and optionally `requiredCapabilities`. The server chooses the highest mutually supported profile version, currently from `0.1` and `1.0`, and returns its capability list. No common version produces `incompatible_profile_version`; a missing required capability produces `missing_required_capability`. Unknown optional fields are ignored, while required capabilities are explicit compatibility checks. Requests then invoke catalogued verbs and asynchronous events include `chat.message`, `monitor.event`, and `gateway.stopping`.

## Conversation identity and sessions

An origin is a validated, platform-neutral record: platform, kind, conversation ID, and (only where appropriate) parent or peer IDs. Platforms are `loopback`, `discord`, `telegram`, and `monitor`; kinds include DM, channel, thread, topic, loopback, and monitor event type. The only canonical identity string is `originKey`:

```text
platform/kind/conversationId[/parent=…][/peer=…]
```

It is opaque after creation—callers must not parse it back into fields. Each origin key has one `gjc` session and an epoch. `/new` (and `/reset`) increments that origin’s epoch and binds the next turn to a new session, leaving other origins untouched. Session creation is idempotent and includes the gateway instance, normalized origin key, and epoch, so a crash around creation can safely retry.

Turns are serialized by origin key with `KeyedQueue`. Different conversations can work concurrently, but two messages for the same origin cannot race two `gjc --resume` processes. Each `gjc` child—creation or a turn—has a 300-second ceiling. A failing platform turn generates a visible, ledgered notice carrying the runtime's own error code and message, truncated but never erased, with only secrets redacted:

```text
[turn failed] spawn_failed: SDK startup did not complete before readiness cutoff. Send /new to rebind this conversation.
```

The same string goes to the daemon log, so a channel transcript is enough to triage without shell access. The `/new` hint appears only when a rebind is plausibly the remedy.

Four runtime codes—`resource_gone`, `spawn_failed`, `terminal_uncertain`, and turn-level `managed_append_identity_mismatch`—are permanent for the derived idempotency key and absent for a fresh one, so the gateway treats them as *rebindable*: it bumps and persists the epoch using the same reset semantics as `/new` and retries once, up to `DEFAULT_REBIND_CAP` (3) consecutive rebinds per origin, after which it fails explicitly rather than growing the epoch silently. Every rebind logs the causing code, both epochs, and a per-origin lifetime total. A completed turn that needed no rebind, or an explicit `/new`, restores the budget.

The persona preamble comes from `SOUL.md`, `AGENTS.md`, and `USER.md` in `$GAJAEWAY_HOME/workspace`; the same directory is passed to `gjc` as the session working directory.

The first attempted turn in every origin epoch also receives a trusted `Session bootstrap` system section. Its stable ID is derived from the opaque origin key and epoch. It carries current conversation metadata plus bounded navigation from `$GAJAEWAY_HOME/memory`; it is never placed in unread/user text. The serialized section is capped at 8 KiB by UTF-8 bytes, includes only whole sections, and names omissions explicitly. Source paths are realpath-confined to the resolved memory root, so a configured root symlink and in-root canonical symlinks work while traversal and escaped symlinks are rejected. Public/group turns require explicit full-origin association for channel/project/task/handoff material, exact-origin filtering for daily entries, and an explicit `bootstrap-safe: public` or `bootstrap-visibility: public` marker before associated document bodies are admitted. Included source material is delimited as reference data, never executable instruction text.

Bootstrap completion is durable in schema 14. An epoch is pending while `epoch > last_bootstrapped_epoch`; `/new`, automatic rebind, and turn-limit rotation therefore establish pending state in the same session-row update that bumps the epoch. The gateway commits the projection only after a terminal text or intentional-silence success. A failure before terminal success retries with the same stable bootstrap ID. If an intermediate reply was already delivered before failure, the unread body is consumed to prevent a harmful answer replay, while the still-pending bootstrap is safely re-presented under that same idempotent ID on the next turn. Session and ops projections expose only epoch, applied time, included section names, byte count, truncation, and diagnostics—never source bodies.

## Session-to-session handoff

Work surfaces in the wrong room. A reply whose FIRST LINE is `[HANDOFF:<target>]` is not a message but a handoff: the rest of the body is what the target conversation's session needs to know and do. The target resolves either to a `handoffTargets` alias (an operator-declared origin, the intended path) or to a conversation that already has a session, named by its canonical origin key or an unambiguous conversation ID. Nothing is ever constructed from a bare ID by guessing a platform and kind, because a plausible key nothing is bound to is exactly the silent drop this path must not have.

The target origin receives ONE durable inbound event whose body is a bounded payload: the relaying session's words, a bounded pure digest of the source conversation (no LLM call, hard byte caps, and dropped entries are stated), and provenance — source origin, source message, requester, timestamp, and the relay chain. It is drained by that origin's own queue under the existing per-origin `KeyedQueue` serialization, so the target session answers in the target channel like any other turn.

Authority does not travel. The payload states in words that nothing in it was said in the target room and that it grants no permission the target session does not already have, and the target's system notice repeats it, so a relayed request cannot read as an instruction that arrived locally.

The chain in the provenance is what makes the loop check local: the hop appends its own origin key, refuses any target already in the chain, and refuses a hop past `HANDOFF_DEPTH_CAP` (2). That is also what makes the nested target turn deadlock-free — no hop can wait on a turn lock its own call stack holds. The idempotency key is derived from the causal facts alone (source origin, source message, target origin), so a replayed or reconciled handoff collides on the durable inbound insert and the target turn runs exactly once.

The source room gets one pointer line naming where the work went, never the work or an excerpt of it; every refusal (unresolvable target, ambiguous target, cycle, depth, failed target turn, a second token in one turn) is delivered there as a loud `[handoff failed] <code>` notice. A dropped handoff is worse than no handoff, because the human believes the work moved.

## Delivery and crash semantics

Before an outbound reply is emitted, the gateway creates a durable ledger item. States are `pending`, `inflight`, `confirmed`, `failed_ambiguous`, and `expired`. Adapters mark an item inflight before attempting platform delivery and confirm it after success. A non-ambiguous failure returns to `pending` with exponential backoff; after three attempts it expires. An ambiguous failure stays `failed_ambiguous`.

At negotiated connection time, adapters receive unsettled deliveries created within the last 24 hours. This is deliberately at-least-once delivery: an `inflight` or `failed_ambiguous` item is reissued with `redelivered: true` and `duplicateWarning: true`. Platform adapters must display the duplicate warning. Confirmed or expired records are not replayed; old settled records are pruned after seven days.

## Engagement and safety floors

Loopback and DMs engage automatically. Group traffic engages only when the adapter reports a mention, or the configured channel is explicitly `engagement: "open"`. Unconfigured group traffic therefore remains mention-gated.

The gateway gives `gjc` unoverridable ActionGuard guidance. It forbids unrecoverable commands such as recursive removal of `/`, filesystem formatting, raw device writes, and fork bombs. It also refuses recursive deletion of `$HOME` itself or absolute paths outside `$HOME` and `$GAJAEWAY_HOME`. These are floors, not a configurable permission bypass.

See [deployment](deployment.md), [memory](memory.md), and [monitors](monitors.md) for operational details.
