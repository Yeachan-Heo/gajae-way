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

## The `gajaeway gjc` terminal entrypath

`gajaeway gjc` runs the native interactive `gjc` TUI as a gateway-managed persona session instead of a standalone one. The CLI calls the additive `session.attach` verb; the gateway classifies the operator's flags against a closed allowlist, takes an exclusive in-memory lease on the single managed origin `loopback/loopback/terminal`, binds or rotates that origin's session, creates that epoch's directory and pins it as the native session store, builds the persona preamble, and returns a fully assembled child `argv` plus the environment the child must run with. The CLI spawns that argv with inherited stdio and **waits**, so the socket—and therefore the lease—covers the whole life of the TUI. The gateway never proxies interactive turns.

**Argv ownership (ARCH-007, reworded).** The gateway's `GjcClient` is the sole assembler of `gjc` argv and the sole binder of managed sessions; the spawn site is no longer unique. A child spawned inside the daemon would inherit daemon stdio and could never be an interactive TUI, so the CLI spawns it in the operator's terminal — but only as an *opaque consumer* of the preassembled argv. Flag classification lives solely in `classifyGjcWrapperFlags`, assembly solely in `assembleGjcAttachArgv`, and the CLI must not classify, extend, or reorder what it receives.

The binder injects `--resume <bound session>` and the persona `--append-system-prompt`; it never emits `--system-prompt`, `-p`, `--print`, or `--mode`, so `gjc`'s native coding register survives by omission. Operator `--model`/`--mpreset` suppress the configured model so one argv never carries two. Session-selection and system-prompt flags are refused, and their values are consumed so they cannot leak through as positionals; unknown flags are refused fail-closed.

Session storage is **epoch-scoped**, and the mechanism was settled by running against real `gjc` rather than by reading flags. `--session-dir` is not injected — `session.create` goes through `gjc sdk session raw`, which rejects that flag, and pointing the TUI at a store the session was never written to made every launch die with `Session "<id>" not found`. Instead the gateway pins the **native state root** to `$GAJAEWAY_HOME/sessions/terminal/e<epoch>/agent` for both the `session.create` it issues and the child it hands back, so `gjc` writes its transcript inside that epoch's directory. Rotation gives the next epoch its own root and leaves the previous one intact.

That also removes a whole failure class structurally: because the gateway dictates the root to both sides, a daemon and a CLI can no longer resolve different stores. It travels as `SessionAttachResult.childEnv` plus `childEnvUnset`, and the CLI applies it as a **replacement** rather than a merge — a value-only patch could not clear a selector the operator's shell exports but the daemon does not, which is the ordinary production split.

`--worktree`/`-w` are **wrapper-owned**, not forwarded — a deliberate departure from literal flag pass-through, still pending ratification. Forwarding them does not work: native `gjc` enters a worktree it names itself (observed suffix `hazard-branch-f6ddf077`) *before* it resolves the session, so the session bound in the persona workspace reads as a different project and the TUI offers to *fork* it — which would split the persona's managed session. The gateway instead prepares the worktree itself with plain `git worktree add` at `<workspace>/.worktrees/<branch>`, binds the session in that directory, and spawns the child there with no worktree flag. Measured against real `gjc`: two sequential `gajaeway gjc --worktree <branch>` invocations resume the same native session with no fork prompt.

Because a native session belongs to the project it was created in, an epoch's cwd is fixed for that epoch's lifetime. The gateway records it as `bound-cwd` in the epoch's directory, alongside `bound-agent-dir` naming the native state root that epoch's session actually lives in, and an attach that would move the session elsewhere is refused with the remedy named (`pass the matching --worktree or use --new`) rather than being handed to `gjc` to surface as an interactive fork prompt. `--worktree` on a non-git workspace is refused for the same fail-closed reason.

Because a live TUI owns the bound session, that origin is **attach-only**: `chat.send` targeting it is refused with `invalid_params` before the `/new` branch, so the chat surface can neither drive a turn on it nor rotate its epoch. Wrapper-owned `--new` is the only rotation. A second concurrent invocation is refused with `session_lease_held` naming the holder, and the lease is released on every disconnect path plus daemon shutdown. Because the gateway releases the lease the instant a connection dies, the SDK exposes a one-shot `onTransportTerminal` notification; the CLI registers it before spawning and kills the child whenever the transport dies — including in the window before the child exists, where it declines to spawn at all.

One residual hazard is deliberately accepted rather than papered over: if the wrapper process itself is `SIGKILL`ed, it cannot run any handler, so the gateway frees the lease while the already-spawned TUI keeps running. A later `gajaeway gjc` then attaches to the same managed session alongside that orphan. Every case the wrapper can observe — abrupt transport loss, graceful daemon shutdown, forwarded signals, and a transport that dies before the child is even spawned — does terminate the child. Closing the `SIGKILL` case would require the child to watch for its parent's death, which the native runtime does not offer today; until then, `kill -9` on a `gajaeway gjc` wrapper should be followed by checking for a stray `gjc` process.

Deliberately deferred for this origin: delivery-ledger participation, per-turn memory capture, and turn-count epoch rotation — the gateway cannot observe native TUI turns, so claiming them would be dishonest. `ops cycle` therefore reports identifiers only (origin key, epoch, bound session id) and implies nothing about child liveness, PTY attachment, or delivery health.

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

## Delivery and crash semantics

Before an outbound reply is emitted, the gateway creates a durable ledger item. States are `pending`, `inflight`, `confirmed`, `failed_ambiguous`, and `expired`. Adapters mark an item inflight before attempting platform delivery and confirm it after success. A non-ambiguous failure returns to `pending` with exponential backoff; after three attempts it expires. An ambiguous failure stays `failed_ambiguous`.

At negotiated connection time, adapters receive unsettled deliveries created within the last 24 hours. This is deliberately at-least-once delivery: an `inflight` or `failed_ambiguous` item is reissued with `redelivered: true` and `duplicateWarning: true`. Platform adapters must display the duplicate warning. Confirmed or expired records are not replayed; old settled records are pruned after seven days.

## Engagement and safety floors

Loopback and DMs engage automatically. Group traffic engages only when the adapter reports a mention, or the configured channel is explicitly `engagement: "open"`. Unconfigured group traffic therefore remains mention-gated.

The gateway gives `gjc` unoverridable ActionGuard guidance. It forbids unrecoverable commands such as recursive removal of `/`, filesystem formatting, raw device writes, and fork bombs. It also refuses recursive deletion of `$HOME` itself or absolute paths outside `$HOME` and `$GAJAEWAY_HOME`. These are floors, not a configurable permission bypass.

See [deployment](deployment.md), [memory](memory.md), and [monitors](monitors.md) for operational details.
