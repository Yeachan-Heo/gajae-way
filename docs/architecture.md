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

Inbound messages enter a durable per-origin mailbox actor that applies one rule: if a turn is running, the message is steered into it (`turn.steer`); if the origin is idle, the message is sent as the next turn, immediately. There is no settle window, no coalescing and no age-based expiry: an unanswered row stays `pending` until a turn takes it. Each turn binds its trigger row with a deterministic caller operation reference, the SDK session and a `dispatched_at` stamp before the send; an accepted receipt advances the turn independently of terminal completion. A steer the session refuses never discards the message: if the turn has already ended the row becomes the next send; if the session is genuinely broken the epoch is bumped, a new session is bound (bootstrapped with recent channel context) and the row becomes that turn's prompt, while the replaced turn still delivers its own answer. Session binding happens before any prompt is attached to the row; if a bind epoch's session-create key becomes `terminal_uncertain`, three consecutive bind failures rotate the epoch automatically, preserving every pending row and deriving a fresh idempotency key instead of leaving the channel permanently dead. Model selection also happens before send: a `model.set` failure releases the bound row back to plain pending rather than creating an ambiguous hold; an unavailable session rotates the epoch immediately, while other model failures retry with capped backoff. After send, a `bound` operation whose status remains `unknown` is held once; if the session is positively dead or is live with an empty prompt queue on the second sweep, it is safely requeued on a fresh epoch. An `accepted` operation is never released by that heuristic because it may already have produced side effects. The actor serializes admission, steering, tail frames, broker-generation changes, `/model` live controls, and `/new` fencing. A message the user edits after ingestion is not a new message: the adapter forwards it as `chat.edit` and the gateway streams it into the same session as `[MESSAGE POINTER: <messageId>] …` with the new body - steered into the running turn or sent as the next one by the same rule - while the context ledger's copy of the original is rewritten to the new body. An edit of a message the gateway never ingested is ignored, and the same edit event delivered twice is one update. The adapter keeps an edit that could not reach the gateway (link down, request failed) in a bounded outbox and replays it on the next connect, because message-history recovery cannot reconstruct an edit of an id it already knows. A single broker-owned logical tail observes activity and transcript frames, while `session status` remains terminal authority. Different origins work concurrently, but same-origin persona, worker, and monitor requests use the shared `SessionPort` lock. A terminal failure generates a visible, ledgered notice carrying the runtime's own error code and message, truncated but never erased, with only secrets redacted:

```text
[turn failed] spawn_failed: SDK startup did not complete before readiness cutoff. Send /new to rebind this conversation.
```

The same string goes to the daemon log, so a channel transcript is enough to triage without shell access. The `/new` hint appears only when a rebind is plausibly the remedy.

`/model` applies `model.set` to the existing session through that mailbox; it neither changes the epoch nor truncates the transcript, and each subsequent persona turn logs its effective model. `/new` is the explicit recovery boundary: it atomically advances the epoch, fences stale output, and discards only pending messages not yet in a turn - the one path that completes an inbound row without answering it. Accepted prior-epoch turns retain their bound session identity as durable recovery holds, so a restarted actor can reattach status/tail until terminal reconciliation; the gateway never invents a replacement operation reference or aborts a running SDK turn.

### Persistent-session observability

The daemon writes grep-stable fields for the persistent-session control plane: `steer_delivered originKey=… opRef=… messageId=…`, `steer_failed origin=… message=… action=recover`, `session_rebound_after_steer_failure origin=… epoch=… nextEpoch=… opRef=…`, `tail_frame_pre_turn origin=… epoch=… session=…` (a replayed pre-turn transcript row fenced by the dispatch floor), `stall_alert originKey=… sessionId=… silentMs=…`, `compaction_event sessionId=… originKey=…`, `broker_restart generation=… backoffMs=…`, and `retired_hold originKey=… epoch=… opRef=…`. A stall is an operator alarm only; it never terminates a live operation. A compaction event is observed from a tail frame or authenticated control receipt, never initiated as persona-turn orchestration.

### Worker lanes

`work.run` binds a `work/task/<name>` origin hosted by the private broker in the coding register. Its optional `model` is a model ID or `{ "preset": "name" }`; the selection is applied at `session.create` and re-applied on each send. Admission is capped by `work.maxLanes` (default 8). A new name at capacity fails with `lane_capacity` carrying `{maxLanes, active, candidates[]}`, with candidates sorted idlest first; an already-bound name is always admitted by the capacity check.

`work.retire` closes the gjc session with `session.close` and bumps the origin epoch, so the next run creates a fresh session. Retirement is fail-closed on ownership: it refuses while the lane job has an open attempt, while the job record is corrupt, and while the last attempt ended in the ledger without broker proof (`attempt_ended` from a reaped wait, `terminal_uncertain` from a crash) until `status` reports the op terminal or the broker reports the session dead. A failed `session.close` releases the slot only when broker liveness proves the session gone; otherwise the binding is retained and the refusal names the close error, because an uncounted live worker is the exact failure the governor exists to prevent. The 60-second sweep nominates lanes idle for at least `work.idleRetireMs` (default 6 hours, measured from the last attempt's end) or whose lane job is `done` or `aborted`, and each nomination is re-proven under the lane lock against the session id it was selected with, so a lane reused or rebound meanwhile is left alone. `session.delete` is never used: gjc's cleanup fence is global, and one refused delete blocks every later `session.create`; retirement therefore bounds live worker hosts, not the saved-session index.

`ops.cycle` reports `lanes: {active, max}` and gates on `lane_capacity_exhausted` when the cap is reached. A retired lane keeps its `work/task/<name>` row unbound; that row is healthy only while a settled lane job (`attempt_ended`, `done`, `aborted`) vouches for it. The same unbound shape with no job (a failed first bind) or with a `running`/`awaiting_operator`/`stalled` job (a crash-left or held worker) stays a `stale_session_identity` gate. The ActionGuard system notice forbids direct `gjc` launches from persona turns; workers use gateway-owned lanes rather than launching unmanaged sessions.

Worker-lane logs are grep-stable:

- `lane_retired name=… session=… reason=operator|idle|job_done|job_aborted closed=…`
- `lane_close_failed name=… session=… detail=… action=retained|session_gone`

The persona preamble comes from `SOUL.md`, `AGENTS.md`, and `USER.md` in `$GAJAEWAY_HOME/workspace`; the same directory is passed to `gjc` as the session working directory.

The first attempted turn in every origin epoch also receives a trusted `Session bootstrap` system section. Its stable ID is derived from the opaque origin key and epoch. It carries current conversation metadata plus bounded navigation from `$GAJAEWAY_HOME/memory`; it is never placed in unread/user text. The serialized section is capped at 8 KiB by UTF-8 bytes. Identity is mandatory, today and yesterday receive bounded priority shares, and recoverable channel/rules/navigation pointers consume only the remainder. Daily sources alone may be labelled newest-entry excerpts and have a 4 MiB read ceiling; other sections remain whole. Excerpts, read-ceiling drops, and budget omissions all set truncation and are named explicitly. Source paths are realpath-confined to the resolved memory root, so a configured root symlink and in-root canonical symlinks work while traversal and escaped symlinks are rejected. Public/group turns require explicit full-origin association for channel/project/task/handoff material, exact-origin filtering for daily entries, and an explicit `bootstrap-safe: public` or `bootstrap-visibility: public` marker before associated document bodies are admitted. Included source material is delimited as reference data, never executable instruction text.

Bootstrap completion is durable in schema 14. An epoch is pending while `epoch > last_bootstrapped_epoch`; `/new` establishes pending state in the same session-row update that bumps the epoch. The gateway commits the projection only after a terminal text or intentional-silence success. A failure before terminal success retries with the same stable bootstrap ID. If an intermediate reply was already delivered before failure, the unread body is consumed to prevent a harmful answer replay, while the still-pending bootstrap is safely re-presented under that same idempotent ID on the next turn. Session and ops projections expose only epoch, applied time, included section names, byte count, truncation, and diagnostics—never source bodies.

## Delivery and crash semantics

Before an outbound reply is emitted, the gateway creates a durable ledger item. States are `pending`, `inflight`, `confirmed`, `failed_ambiguous`, and `expired`. Adapters mark an item inflight before attempting platform delivery and confirm it after success. A non-ambiguous failure returns to `pending` with exponential backoff; after three attempts it expires. An ambiguous failure stays `failed_ambiguous`.

At negotiated connection time, adapters receive unsettled deliveries created within the last 24 hours. This is deliberately at-least-once delivery: an `inflight` or `failed_ambiguous` item is reissued with `redelivered: true` and `duplicateWarning: true`. Platform adapters must display the duplicate warning. Confirmed or expired records are not replayed; old settled records are pruned after seven days.

## Engagement and safety floors

Loopback is trusted locally, while DMs use `dmPolicy`. Group traffic uses one gateway-owned channel policy: `engagement` is exactly `open`, `mention-open`, or `closed`, and `audience` is independently `all`, `human-only`, or `bot-only`. `open` admits matching-audience chatter; `mention-open` requires a real mention or native reply to this bot; `closed` ignores audience and requires both addressing and the existing owner/allowlist authorization. Omitted engagement remains closed. Omitted audience remains `human-only`, preserving historical `engagement: "open"` behavior: humans are open while bots fall back to the closed gate. Discord threads inherit a configured parent channel policy unless the thread has its own entry.

Discord drops self-authored messages before forwarding and the gateway durably deduplicates platform message IDs. Bot turns admitted through an explicitly widened audience are additionally capped at one consecutive turn per conversation; a human message resets that budget. This permits deliberate bot collaboration without an unbounded bot-to-bot reply loop.

The gateway gives `gjc` unoverridable ActionGuard guidance. It forbids unrecoverable commands such as recursive removal of `/`, filesystem formatting, raw device writes, and fork bombs. It also refuses recursive deletion of `$HOME` itself or absolute paths outside `$HOME` and `$GAJAEWAY_HOME`. These are floors, not a configurable permission bypass.

See [deployment](deployment.md), [memory](memory.md), and [monitors](monitors.md) for operational details.
