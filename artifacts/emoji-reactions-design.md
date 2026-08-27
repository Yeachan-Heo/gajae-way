# Emoji reactions — design spec for the runtime

Lane: `design/discord-reactions` (design half). Base: `origin/main @ a5124f4`.
Owner request, verbatim: **"이모지도좀 잘 쓰게 해라" / "디코 이모지 reply."**

This document is written to be implemented without re-deciding anything. Every decision is stated as a
decision, not an option. Where a platform fact could not be verified from primary docs it is labelled
**UNVERIFIED**; where a repo/product judgement had to be invented it is labelled **ASSUMPTION**.

**Ownership note.** The sibling lane `feat/reply-metadata` currently owns
`packages/adapter-discord`, `packages/adapter-telegram`, `packages/protocol/src/catalog.ts` and the
gateway turn header. Every delta below that lands in those files is written as a delta *for that lane
to apply*; this lane wrote no code. Files this design touches that are **not** contested:
`packages/gateway/src/server/server.ts`, `packages/gateway/src/delivery/delivery.ts`,
`packages/gateway/src/store/db.ts`, `packages/gateway/src/store/ledger.ts`,
`packages/gateway/src/config.ts`, `packages/gateway/src/orchestrator/gjc-client.ts` (persona base
note — overlaps "turn header" only if that lane reads it broadly; coordinate).

---

## 0. What exists today (the shape the design must fit)

Read before believing anything below:

- `packages/protocol/src/catalog.ts` — `VerbCatalogV01`, `EventCatalogV01`, `VERBS_V01`,
  `EVENTS_V01`, `EngagementContext`, `ChatMessagePayload`, `SILENCE_TOKENS` / `isSilenceToken`.
- `packages/gateway/src/server/server.ts` — `sendChat()` (admission + `contextRecord` +
  `inboundEnqueue`), `runInboundTurn()` (turn composition, `[BREAK]` split, `[REPLY:<id>]` prefix,
  silence-token gate at line 791, per-part `delivery.prepare`).
- `packages/gateway/src/delivery/delivery.ts` + `store/ledger.ts` — the delivery ledger:
  `pending → inflight → confirmed | failed_ambiguous | expired`, `fail()` increments `attempts` and
  expires at 3, `retryBackoffMs = 1000 * 2^(attempts-1)`, `redeliveries()` re-emits anything
  unconfirmed created within 24h and stamps `redelivered` (+ `duplicateWarning` for
  `inflight`/`failed_ambiguous`).
- `packages/gateway/src/engagement/policy.ts` — `decideEngagement()`: DMs always engage, groups need
  `channels[...] = { engagement: "open" }` or a mention from an allowlisted author.
- `packages/gateway/src/store/db.ts` — `conversation_context` ledger (`contextRecord`,
  `contextUnread`, `contextConsume`, `contextPrune`), `inbound_messages` queue,
  `LATEST_SCHEMA_VERSION = 8`.
- Adapters: `engagementForMessage` / `decideInbound` (Discord), `TelegramAdapter.handleUpdate`
  (Telegram, `getUpdates` with no `allowed_updates`), `settle*Delivery` + `deliveryFailureIsAmbiguous`
  in both.

Two structural facts drive most of this design:

1. **A turn is created only by `inboundEnqueue`.** Anything that never calls it can never wake the
   persona. That is the whole enforcement mechanism for decision 1.
2. **The `deliveries` table is the single source of outbound truth.** Everything the runtime pushes to
   a platform already has a confirm/fail path, retry budget, restart redelivery and a
   `gateway.status.delivery.pending` health number. A reaction that bypasses it is invisible to ops.

---

## 1. Decision — inbound reactions

**Decision: a reaction NEVER becomes a turn. It becomes exactly one `conversation_context` row, and
nothing else.** No `EngagementContext` field, no `inbound_messages` insert, no engagement
re-evaluation.

Adapters report it through a new verb `chat.reacted` (past tense: a platform fact that already
happened, not a request to act). The gateway handler for `chat.reacted`:

1. validates the origin (`validateOriginRef`) and the payload,
2. applies the inbound reaction quota (§4),
3. writes one `conversation_context` row with `kind = 'reaction'`,
4. returns `{ recorded: boolean }`,
5. **never** touches `inbound_messages`, `runtime.inbound`, `drainOrigin` or `decideEngagement`.

The persona sees reactions the next time it takes a turn *for another reason*, as part of the unread
diff that `runInboundTurn` already renders. That is exactly the right latency: a 👍 is context for the
next thing you say, not an event that deserves an answer.

**Why not an `EngagementContext` field.** `EngagementContext` is *admission metadata for one inbound
message*, computed by the adapter at message time and consumed by `decideEngagement`. A reaction
arrives asynchronously, minutes later, and refers to a *different* message; putting it on
`EngagementContext` would mean either a field that is stale for every real message or a synthetic
"engagement" object for a thing that is never admitted. Both are worse than a ledger row. The
engagement path stays untouched, which is also the cheapest way to guarantee decision 1 is true.

**Scope of what is recorded.** Every reaction change the adapter can see in a watched origin is
recorded, whether the target is one of our messages or not. `onOwnMessage` is a column-level flag, not
a filter: "someone 👍'd *my* message" and "someone 👍'd Bob's message" are both useful and cost one row.

**Reaction removal.** A removal means "the reactor withdrew that signal", and it is handled by
*state*, not by an extra event the persona has to reason about:

- If the matching add-row is still **unconsumed** (`consumed_at IS NULL`), the removal deletes it.
  The persona never saw it, so the net truth is "no reaction" and the unread diff must not show a
  reaction that no longer exists. New DB method `contextRetract(messageId): boolean`.
- If the add-row is already **consumed**, the persona has read it and may have acted on it. Deleting
  history is a lie, so the removal is recorded as its own row with `kind = 'reaction_removed'`.
- If there is no matching row at all (restart, pre-feature reaction, quota drop), the removal is
  dropped silently. Recording "X removed a reaction I never knew about" is noise.

Reaction *replacement* on Telegram (`message_reaction` carries `old_reaction` / `new_reaction` arrays,
verified) is decomposed by the adapter into removals for `old \ new` and adds for `new \ old`.

**Row encoding.** `conversation_context.message_id` is the primary key and `contextRecord` is
`INSERT … ON CONFLICT DO NOTHING`, which gives idempotency for free. Synthetic id:

```
rx!<targetMessageId>:<reactorId>:<emojiKey>
```

`rx!` is a deliberately un-mentionable prefix. `emojiKey` is the normalized emoji (§4) or
`c:<name>` for a custom emoji. `!` is not in the protocol's origin `SEGMENT_RE`, so the prefix can
never collide with a platform id in any other position of the system.

Body text (this is what the persona reads, so it is part of the contract):

- add, our message: `reacted 👍 to your message (msg:1234)`
- add, someone else's: `reacted 👍 to msg:1234 (author:5678)`
- removal: `removed their 👍 from your message (msg:1234)`

**Reply-target validation.** `[REPLY:<id>]` must reject any id starting with `rx!` — a synthetic
reaction row is not a platform message and threading to it would 404 into a ledger failure. The
gateway drops the directive and delivers the body as a plain message.

**Adapter wiring (Discord).** `MESSAGE_REACTION_ADD` / `MESSAGE_REACTION_REMOVE` require the
`GUILD_MESSAGE_REACTIONS (1 << 10)` and `DIRECT_MESSAGE_REACTIONS (1 << 13)` intents (verified). Add
both to `REQUIRED_INTENTS` in `packages/adapter-discord/src/main.ts`. Self-authored reactions are
dropped (`user_id === botUser.id`), mirroring `decideInbound`'s own-message rule. Partial (uncached)
reactions must be fetched or dropped, never reported with an empty emoji.

**Adapter wiring (Telegram).** `message_reaction` updates require the bot to be an administrator in
the chat **and** an explicit `allowed_updates` list containing `"message_reaction"` (verified — the
default list excludes it). `TelegramBotApi.getUpdates` must pass
`allowed_updates: ["message", "message_reaction"]`. Note the verified caveat: *"The update isn't
received for reactions set by bots."* Where the bot is not an admin, Telegram inbound reactions are
simply absent — that is a platform limitation, recorded as such, not an error.

---

## 2. Decision — outbound reactions

**Decision: outbound reactions are a first-class delivery in the existing ledger, carried by a new
event `chat.reaction`, settled with the existing `delivery.confirm` / `delivery.fail` verbs. No new
settlement verbs, no second ledger, no separate delivery class.**

Naming (deliberate, to keep logs readable): the **verb** `chat.reacted` is inbound/observation, the
**event** `chat.reaction` is outbound/intent. Verbs and events are separate namespaces in
`catalog.ts` and separate frame types on the wire, and past tense vs noun makes the direction obvious
in a log line.

**Target addressing.** A reaction targets `(origin, targetMessageId)`:

- `origin` is the canonical `OriginRef` of the conversation the turn is running in — never a
  cross-origin target. A persona turn may only react inside the origin it was invoked from. This is
  the same isolation rule as `chat.send`, and it removes an entire class of "react in a channel you
  were never engaged in" abuse.
- `targetMessageId` is a platform message id in that origin. Default: the id of the message that
  triggered the turn (`row.message_id` in `runInboundTurn`, already in scope).
- Validation at prepare time, in the gateway, all rejections silent-to-the-room:
  1. `origin.platform === "loopback"` → drop (there is no surface to react on).
  2. `targetMessageId` starts with `rx!` or fails `SEGMENT_RE` → drop.
  3. `targetMessageId` not present in this origin's `conversation_context` (as a `kind='message'`
     row) and not the triggering message id → drop. The persona may only react to messages it can
     actually see in this conversation's ledger; this stops hallucinated ids from becoming ledger
     failures.
  4. emoji not resolvable for this platform (§4/§5/§6) → drop.
  5. per-turn / per-message / per-origin quota exceeded (§4) → drop.
  A drop is a `console.warn` line plus no ledger row. Nothing is delivered and the room is unchanged.

**Ledger semantics, concretely in this repo.** `DeliveryService` gains `prepareReaction()` which is
`prepare()`'s twin: it mints `deliveryId = crypto.randomUUID()`, writes `deliveries` row via
`ledger.createPending({ deliveryId, turnId, originKey, payloadJson })`, and returns the payload. From
there the reaction inherits, unchanged:

- `markInflight()` before emitting the event;
- adapter calls `delivery.confirm` → `state = confirmed`;
- adapter calls `delivery.fail` → `attempts + 1`; `ambiguous: true` → `failed_ambiguous`, otherwise
  `pending` until `attempts >= 3` then `expired`;
- retry pacing `1s / 2s / 4s` from `retryBackoffMs`;
- restart redelivery from `listUndelivered(24h)`;
- it counts in `gateway.status.delivery.pending` and in `oldestPendingAgeMs`.

**One deliberate difference: reactions ignore `duplicateWarning`.** For a message, an ambiguous
outcome means the room may see the text twice, so `settleDiscordDelivery` /
`settleTelegramDelivery` prepend `[recovered - may be a duplicate]`. Setting the same reaction twice
is idempotent on both platforms (Discord `PUT .../reactions/:emoji/@me` is 204 either way; Telegram
`setMessageReaction` sets the full reaction list, verified), so a re-issued reaction is
indistinguishable from the first. Adapters MUST NOT decorate a redelivered reaction, and MUST NOT
convert it to text. `redelivered` / `duplicateWarning` are simply not read on the reaction path.

**Payload discrimination in the ledger.** `redeliveries()` currently `JSON.parse`s every
`payload_json` as a `ChatMessagePayload`. Reaction payloads carry `kind: "reaction"`;
`ChatMessagePayload` carries `kind: "message"`. `redeliveries()` returns a discriminated union and
`boot`/server re-emits `chat.message` or `chat.reaction` accordingly. Rows written before the field
existed have no `kind` and are all messages, so the parse defaults to `"message"` — this is the
discriminant's default, not a compatibility layer.

**Emission.** Same fan-out as `chat.message`: `for (const recipient of runtime.connections) if
(recipient.negotiated) recipient.write({ v: PROFILE_VERSION, type: "event", event: "chat.reaction",
payload })`. Adapters filter by `payload.origin.platform`, exactly as `settle*Delivery` already does.

---

## 3. Decision — reaction instead of, or in addition to, text

**Decision: the persona expresses a reaction with an inline directive `[REACT:<emoji>]`, parsed in
`runInboundTurn` at the same point and in the same style as the existing `[REPLY:<id>]` prefix. The
silence-token contract is untouched.**

Grammar (one reply body, `[BREAK]`-split into at most 5 parts, unchanged):

```
[REACT:👍]                        react to the triggering message
[REACT:👍@1234567890]             react to a specific message id in this origin
[REACT::shipit:]                  custom guild emoji by name (§5)
[REACT:👍] ok, on it              react AND say something
[REACT:👍][REACT:👀]              two reactions in one part (subject to §4 limits)
```

Parse rules, in order, per part (after `[BREAK]` split, before the existing `[REPLY:]` handling):

1. Consume any number of leading `[REACT:...]` directives (regex
   `/^\[REACT:([^\]\s@]+)(?:@([^\]\s]+))?\]\s*/`, applied repeatedly). Directives are only recognized
   at the start of a part; `[REACT:...]` in the middle of prose is literal text and is delivered as
   written (it will look silly, which is the correct feedback).
2. Each consumed directive becomes a candidate reaction, validated per §2/§4/§5/§6.
3. The remainder is the body. Then the existing `[REPLY:]` prefix logic runs on the remainder.
4. If the body is non-empty → a normal message delivery is prepared as today, **plus** the
   reaction deliveries. Reactions are emitted first, so the acknowledgement lands before the text.
5. If the body is empty and at least one reaction survived validation → the reactions are the entire
   output of that part. Critically, the existing `if (!body) continue;` must be evaluated *after*
   reaction extraction, or reaction-only parts would be silently dropped.
6. If the body is empty and no reaction survived → nothing is delivered for that part.

**Silence token, unchanged.** `isSilenceToken(text)` at line 791 still runs on the whole reply before
the `[BREAK]` split, and still returns early with no delivery and no daily capture. `[SILENT]` means
total silence, including no reaction. A reply of `[REACT:👍]` is *not* a silence token and is not
treated as one; it is a delivered acknowledgement. `[REACT:👍]\n[BREAK]\n[SILENT]` reacts and then
says nothing, because the per-part filter already drops silence-token parts. This ordering keeps
exactly one meaning per token: `[SILENT]` = "I produce nothing", `[REACT:x]` = "I acknowledge without
speaking".

**Memory / recall.** `addRecall` and the `daily_capture` intent currently store the raw reply text.
Raw directives in memory teach the persona to write directives at the wrong times. Both must store
the canonical rendering instead: `[REACT:👍] ok` → `reacted 👍 to msg:1234; ok`, and a reaction-only
reply → `reacted 👍 to msg:1234`. `updateActivity` / `addRecall` still run (they are above the
silence gate), so a reaction-only turn correctly counts as activity in this conversation.

**Persona instruction** (one clause added to `GENERIC_AGENT_SYSTEM_PROMPT` in
`packages/gateway/src/orchestrator/gjc-client.ts`, alongside the existing `[BREAK]` / `[REPLY:]`
clauses):

> In a busy room you can acknowledge without speaking: start a part with `[REACT:<emoji>]` to place
> an emoji reaction on the message you are answering (`[REACT:<emoji>@<msg id>]` to target another
> message). Reaction-only is the right answer for "ok / got it / agreed / noted" — prefer it over
> sending a one-word message. Allowed emoji: 👍 👎 ❤ 🔥 🎉 😁 🤔 👀 🙏 👌 💯 🤝 😢 🤯 ✍ 🫡. At most two
> reactions per turn. `[SILENT]` still means you produce nothing at all, reaction included.

---

## 4. Decision — emoji allowlist and safety

**The bounded set (P0).** One list, hard-coded in the protocol package, chosen as the intersection of
"universally available on Discord" and "in Telegram's server-provided reaction set" so a reaction
means the same thing on both platforms:

| emoji | codepoint | intended meaning |
| --- | --- | --- |
| 👍 | U+1F44D | ack / agree / done |
| 👎 | U+1F44E | disagree / no |
| ❤ | U+2764 | affection, owner-facing |
| 🔥 | U+1F525 | strong approval |
| 🎉 | U+1F389 | shipped / milestone |
| 😁 | U+1F601 | that was funny |
| 🤔 | U+1F914 | thinking / not sure yet |
| 👀 | U+1F440 | seen, looking into it |
| 🙏 | U+1F64F | thanks / please |
| 👌 | U+1F44C | fine as-is |
| 💯 | U+1F4AF | exactly right |
| 🤝 | U+1F91D | agreed, deal |
| 😢 | U+1F622 | bad news acknowledged |
| 🤯 | U+1F92F | genuinely surprising |
| ✍ | U+270D | noted / writing it down |
| 🫡 | U+1FAE1 | understood, acting on it |

All sixteen appear verbatim in Telegram's `ReactionTypeEmoji` list (verified, §Citations).

**Normalization.** Compare and store with `U+FE0F` (VS16) and `U+FE0E` stripped, so `❤️` and `❤`
are one entry. Telegram is sent the exact codepoint sequence from the table (no VS16); Discord is sent
the same sequence, URL-encoded (Discord requires URL encoding or the request fails with
`10014: Unknown Emoji` — verified). **ASSUMPTION:** Discord's reaction endpoint accepts the
VS16-less form of `❤`/`✍`; if a live drill shows otherwise, keep a per-platform emit form in the same
table rather than widening the allowlist.

**Anything not in the set is rejected.** There is no "well-known emoji" heuristic and no
model-supplied emoji passthrough. Rejection is silent to the room: the directive is stripped, any text
in the part still delivers, no ledger row is created, and the gateway logs
`reaction rejected: <token> not in allowlist`. Rationale: the alternative — telling the room "I tried
to react with X" — turns a safety stop into a message, which is exactly what a reaction is supposed
to avoid. The persona is told the exact allowlist in its system note, so rejection is a bug-catcher,
not a normal path.

**Limits (all gateway-side, at prepare time, so an adapter bug cannot bypass them):**

| limit | value | rationale |
| --- | --- | --- |
| reactions per turn | 2 | a turn is one thought; three emoji is spam |
| reactions per target message per turn | 1 | Telegram allows bots exactly one reaction per message (verified); keep both platforms symmetric |
| reactions per origin per rolling 60s | 6 | caps a reaction loop at a level a human would read as normal |
| reaction deliveries in flight per origin | 4 | bounds the queue if a platform is degraded |
| inbound reaction context rows per origin per rolling 60s | 20 | a reaction raid must not flood the unread diff; excess dropped, one warn log per window |
| ledger retry budget | existing: 3 attempts, 1s/2s/4s, then `expired` | reuse, do not special-case |

**Discord rate limiting.** Verified from primary docs: the **global limit is 50 requests per second
per bot**, per-route limits are discovered from the `X-RateLimit-*` response headers, a 429 carries
`retry_after`, and 10,000 invalid requests (401/403/429) per 10 minutes gets the IP Cloudflare-banned.
The current Rate Limits page contains **no** reaction-specific limit (zero occurrences of
"reaction"), and the Emoji resource page's "not the normal rate limit conventions … limited per guild"
note is about *emoji management* routes, not reactions.

**UNVERIFIED:** the widely repeated "1 reaction per 250 ms per channel" figure is not in the current
Discord documentation and MUST NOT be stated as a documented limit. Because of that, the adapter
imposes its own pacing rather than claiming a platform number: **reaction requests are serialized per
channel with a 250 ms minimum spacing** — a self-imposed conservative pace chosen to sit under any
plausible undocumented per-channel bucket, not a citation. On 429 the adapter honours
`retry_after` / `X-RateLimit-Reset-After` from the response, then reports
`delivery.fail({ ambiguous: false })` so the ledger's own 1s/2s/4s budget applies. 429 must never be
retried in a tight loop — the invalid-request ban is a real and documented consequence.

**Telegram rate limiting: UNVERIFIED.** The Bot API documentation states no numeric per-chat rate
limit for `setMessageReaction`. The adapter serializes reaction calls per chat at the same 250 ms
pace and treats `TelegramApiError` as a definitive (non-ambiguous) failure, consistent with the
existing `deliveryFailureIsAmbiguous` in `packages/adapter-telegram/src/main.ts`.

**Offensiveness.** Handled by construction, not by a filter: the allowlist has no middle finger, no
skull, no clown, no 💩 — all of which exist in Telegram's set and would eventually be used at the
worst possible moment. 👎 and 😢 are the only negative-valence entries and both are legible as
disagreement rather than insult. Extending the set is a doctrine change, not a config change:
the list lives in `packages/protocol/src/catalog.ts`, not in `gateway.config`.

---

## 5. Decision — custom guild emoji

**Decision: the persona names a custom emoji by name (`[REACT::shipit:]`); the Discord adapter
resolves the name to `name:id` at delivery time; unresolvable names degrade to a configured unicode
fallback and then to nothing. Raw `<:name:id>` and literal `:name:` never reach a platform.**

Discord requires custom reactions to be encoded as `name:id`, URL-encoded (verified). The `<:name:id>`
angle-bracket form is *message text* syntax, not reaction syntax, so it must never appear on the
reaction path at all.

**Resolution order (Discord adapter, per delivery):**

1. exact-name match in the guild emoji cache for `origin`'s guild;
2. case-insensitive match (single unambiguous hit only; two hits → treat as miss);
3. **UNVERIFIED:** application emoji (`List Application Emojis` exists as an endpoint, but the docs do
   not state that application emoji are usable as reactions). Implement this step behind a config
   flag, default off, and promote it only after a live drill confirms it;
4. configured unicode fallback: `discord.customEmoji[<name>] = "<allowlisted unicode emoji>"`;
5. drop the reaction entirely (warn log, no ledger row).

Steps 4 and 5 are the whole point: **there is no text degradation path for a custom emoji.** Never
post `<:shipit:123>`, never post `:shipit:`, never post "I wanted to react with shipit". A missing
custom emoji is a missing acknowledgement, which is invisible and harmless; a leaked colon-code is a
persona that looks broken in front of the room.

**Caching.** discord.js maintains the guild emoji cache, but keeping it *fresh* needs the
`GUILD_EXPRESSIONS (1 << 3)` intent (verified: it delivers `GUILD_EMOJIS_UPDATE`). Add it to
`REQUIRED_INTENTS`. On a cache miss for a name the adapter MAY do one `GET /guilds/{id}/emojis`
refresh per guild per 10 minutes before declaring a miss; more than that is a rate-limit hazard on
routes the docs explicitly describe as per-guild limited.

**DM and thread origins.** A DM has no guild, so every custom name misses and degrades at step 4/5.
A thread resolves against its parent guild.

**Custom emoji in message *text* (P1).** Two rules, applied by the gateway before
`delivery.prepare`:

- A bare colon-code `:name:` that is not inside a fenced code block is stripped from outbound text.
  It renders as literal `:name:` on both platforms and is the exact "looks broken" failure this
  section exists to prevent.
- `<a?:name:id>` is left intact for `platform === "discord"` (it renders) and replaced with the
  configured unicode fallback — or removed — for every other platform, where it renders as raw
  garbage.

**Telegram custom emoji: not supported.** Verified: a bot may set a custom emoji reaction only if it
is already present on the message or explicitly allowed by chat administrators, and premium custom
emoji are the normal case. That is too conditional to be worth a code path. `[REACT::name:]` on a
Telegram origin resolves through the fallback map (step 4) or drops (step 5).

---

## 6. Decision — Telegram parity

Verified platform facts (`setMessageReaction`):

- reaction is an array of `ReactionType`; **"as non-premium users, bots can set up to one reaction per
  message"**;
- bots cannot use paid reactions;
- **"Service messages of some types can't be reacted to"**;
- unicode reactions are restricted to a server-provided list — 73 emoji as of Bot API 10.3, containing
  all sixteen of our allowlist;
- `deleteMessageReaction` / `deleteAllMessageReactions` exist but require the `can_delete_messages`
  admin right and operate on *other people's* reactions — not needed here; the bot clears its own
  reaction by calling `setMessageReaction` with an empty/absent `reaction` list.

**Mapping.** Because the P0 allowlist was chosen as a subset of Telegram's list, the mapping is the
identity function: `emoji → [{ type: "emoji", emoji }]`. There is no translation table to get wrong,
and adding a Discord-only emoji later (P1) is what introduces the first unmappable case.

**Decision for an impossible reaction: a statically-known impossibility is never attempted; a
dynamically-discovered impossibility is a reported ledger failure. Never a text fallback, never an
untracked silent no-op.**

- **Statically known** (emoji not in the Telegram set, custom emoji with no fallback, loopback origin,
  quota exceeded): the gateway does not create a ledger row. Nothing is attempted, nothing is
  reported, one warn log. Manufacturing a failure row for something we knew in advance would poison
  `delivery.pending` — the ops health number — with noise.
- **Dynamically discovered** (bot is not permitted to react in this chat, message is a
  non-reactable service message, chat type does not support reactions, API 400): the attempt happens,
  fails, and the adapter reports `delivery.fail({ deliveryId, reason, ambiguous: false })`. The ledger
  burns its 3 attempts and lands on `expired`. The room sees nothing.

**Why not a text fallback.** The persona chose a reaction precisely because it did *not* want to
speak. Converting that into a message inverts the intent and reintroduces the failure mode decision 1
exists to prevent — an acknowledgement becoming chatter. **Why not a bare silent no-op.** The
`deliveries` table is this repo's only source of delivery truth; an attempt that fails and leaves no
row means an operator debugging "why did nothing happen" has nothing to read. The chosen split gives
both properties: the room stays quiet, and the ledger stays honest.

---

## 7. Decision — emoji in normal text (house style)

Six rules, small enough to paste into the persona documents (`SOUL.md` / `AGENTS.md`) and into the
`GENERIC_AGENT_SYSTEM_PROMPT` base note. Aim: emoji as **signal**, never decoration.

1. **At most one emoji per message, and only where it replaces words.** Status, verdict, or short
   acknowledgement. If the sentence reads the same with the emoji deleted, delete it.
2. **Never open a message with an emoji, and never use one as a bullet or heading marker.** Emoji
   bullets are the single clearest tell of machine-written text.
3. **Never more than one emoji-carrying part per `[BREAK]` series.** One turn, one emoji at most.
4. **Prefer a reaction over an emoji message.** If the whole content is "ok / got it / agreed /
   nice", send `[REACT:👍]`, not a message containing 👍. A one-emoji message is a notification for
   everyone in the room; a reaction is not.
5. **No emoji in:** numbers, facts, quotes, error reports, failure explanations, apologies, code
   blocks, or any reply to an urgent or angry message. Nothing undermines "the deploy failed" like a
   🙃 next to it.
6. **Allowlisted emoji only, and only real emoji.** Never `:colon_codes:`, never `<:name:id>`, never
   ASCII kaomoji, never an emoji the persona cannot see rendered.

Enforceable pieces (P1, gateway-side, cheap): strip non-fenced `:colon_codes:` from outbound text
(§5); warn-log any outbound message part containing more than 2 emoji codepoints, so drift is visible
in the logs instead of only in the room.

---

## 8. Protocol delta

Written in `catalog.ts` style, to be applied by the lane that owns `packages/protocol/src/catalog.ts`.

```ts
/**
 * Emoji reactions. Inbound reactions are observations, never turns: the
 * `chat.reacted` verb writes one conversation-context row and never touches the
 * inbound queue. Outbound reactions are ordinary ledger deliveries carried by
 * the `chat.reaction` event and settled with delivery.confirm / delivery.fail.
 */

/** The only unicode emoji the runtime may react with; subset of Telegram's server-provided set. */
export const REACTION_EMOJI = [
	"👍",
	"👎",
	"❤",
	"🔥",
	"🎉",
	"😁",
	"🤔",
	"👀",
	"🙏",
	"👌",
	"💯",
	"🤝",
	"😢",
	"🤯",
	"✍",
	"🫡",
] as const;
export type ReactionEmoji = (typeof REACTION_EMOJI)[number];

/** Variation selectors are decoration, not identity: ❤️ and ❤ are one reaction. */
export function normalizeReactionEmoji(text: string): string {
	return text.replace(/[\uFE0E\uFE0F]/g, "").trim();
}

export function isReactionEmoji(text: string): text is ReactionEmoji {
	return (REACTION_EMOJI as readonly string[]).includes(normalizeReactionEmoji(text));
}

/**
 * A reaction target: unicode from the allowlist, or a guild-scoped custom emoji
 * addressed by name. Ids are never authored by the persona — the adapter
 * resolves the name at delivery time (design §5).
 */
export type ReactionRef =
	| { readonly kind: "unicode"; readonly emoji: ReactionEmoji }
	| { readonly kind: "custom"; readonly name: string };

/** Inbound reaction change reported by an adapter. Never creates a turn. */
export interface ChatReactedParams {
	readonly origin: OriginRef;
	/** Platform message id the reaction was placed on. */
	readonly targetMessageId: string;
	readonly reaction: ReactionRef;
	/** "added" | "removed": a removal retracts an unread add, else records its own row. */
	readonly change: "added" | "removed";
	/** Platform-scoped id of the reacting account. */
	readonly reactorId: string;
	/** Per-surface display name of the reactor, same precedence as EngagementContext.authorName. */
	readonly reactorName?: string;
	/** True when the target message was authored by the agent account. */
	readonly onOwnMessage: boolean;
}

export interface ChatReactedResult {
	/** False when the reaction was dropped (quota, unknown target, no matching row to retract). */
	readonly recorded: boolean;
}

/** Outbound reaction intent: a ledger delivery, settled exactly like a message. */
export interface ChatReactionPayload {
	readonly kind: "reaction";
	readonly turnId: string;
	readonly origin: OriginRef;
	/** Platform message id to react to, validated against this origin's context ledger. */
	readonly targetMessageId: string;
	readonly reaction: ReactionRef;
	/** Ledger delivery id; adapters MUST settle it via delivery.confirm / delivery.fail. */
	readonly deliveryId: string;
	/** True when re-emitted from the ledger after a restart. Reactions are idempotent: never labelled. */
	readonly redelivered?: boolean;
}
```

Deltas to existing declarations:

```ts
export interface ChatMessagePayload {
	/** Ledger payload discriminant; absent in rows written before reactions existed. */
	readonly kind?: "message";
	// … unchanged fields
}

export interface VerbCatalogV01 {
	// … unchanged entries
	"chat.reacted": { params: ChatReactedParams; result: ChatReactedResult };
}

export interface EventCatalogV01 {
	// … unchanged entries
	"chat.reaction": ChatReactionPayload;
}

export const VERBS_V01 = [
	// … unchanged entries
	"chat.reacted",
] as const;
export const EVENTS_V01 = [
	"chat.message",
	"chat.progress",
	"chat.reaction",
	"gateway.stopping",
	"monitor.event",
] as const;
```

`packages/conformance/test/sdk-coverage-inventory.test.ts` enforces that the gateway implements
every `VERBS_V01` entry and references no uncatalogued name, so both additions are covered by an
existing gate the moment they land.

### Gateway-side deltas (not in the contested files)

```ts
// packages/gateway/src/delivery/delivery.ts
prepareReaction(
	turnId: string,
	origin: OriginRef,
	targetMessageId: string,
	reaction: ReactionRef,
): ChatReactionPayload;                    // mints deliveryId, writes a pending ledger row

redeliveries(): (ChatMessagePayload | ChatReactionPayload)[];   // discriminated on payload.kind

// packages/gateway/src/store/db.ts — schema migration 9 (LATEST_SCHEMA_VERSION 8 -> 9)
// ALTER TABLE conversation_context ADD COLUMN kind TEXT NOT NULL DEFAULT 'message';
// ALTER TABLE conversation_context ADD COLUMN target_message_id TEXT;
contextRecord(row: { …existing…; kind?: "message" | "reaction" | "reaction_removed"; targetMessageId?: string }): void;
contextRetract(messageId: string): boolean;          // deletes an unconsumed row; false if consumed/absent
contextHasMessage(originKey: string, messageId: string): boolean;   // reaction target validation
```

`contextUnread` also returns `kind`, so the turn header can render a reaction row as an observation
rather than as something to reply to.

### Config delta

```ts
// packages/gateway/src/config.ts
readonly reactions?: {
	/** Master switch; when false, [REACT:] directives are stripped and inbound reactions are still recorded. */
	readonly enabled?: boolean;              // default true
	/** name -> allowlisted unicode emoji, used when a custom guild emoji cannot be resolved (§5 step 4). */
	readonly customEmojiFallback?: Readonly<Record<string, string>>;
	/** Opt-in to §5 step 3 (application emoji as reactions) once verified. */
	readonly allowApplicationEmoji?: boolean; // default false
};
```

---

## 9. Adapter-by-adapter behaviour

| concern | Discord | Telegram | loopback |
| --- | --- | --- | --- |
| inbound reaction source | `MESSAGE_REACTION_ADD` / `MESSAGE_REACTION_REMOVE` gateway events | `message_reaction` update (`old_reaction`/`new_reaction` diffed into adds+removals) | none |
| inbound prerequisites | intents `GUILD_MESSAGE_REACTIONS (1<<10)` + `DIRECT_MESSAGE_REACTIONS (1<<13)`; partials fetched or dropped | bot must be chat **admin** and `getUpdates` must pass `allowed_updates: ["message","message_reaction"]`; bot-set reactions never delivered | — |
| inbound verb | `chat.reacted` | `chat.reacted` | — |
| can inbound create a turn | no | no | — |
| outbound API | `PUT /channels/{channel}/messages/{message}/reactions/{emoji}/@me` (emoji URL-encoded; custom as `name:id`) | `setMessageReaction(chat_id, message_id, reaction: [{type:"emoji",emoji}])` | none |
| outbound permissions | `READ_MESSAGE_HISTORY`, plus `ADD_REACTIONS` when nobody has used that emoji yet | reactions must be permitted in that chat for the bot | — |
| reactions per message | 1 (self-imposed, symmetry) | 1 (platform limit, verified) | — |
| custom emoji | resolved by name → `name:id`; fallback map; else dropped | unsupported → fallback map; else dropped | — |
| pacing | serialized per channel, ≥250 ms (self-imposed, §4) | serialized per chat, ≥250 ms (self-imposed) | — |
| failure classification | reuse `deliveryFailureIsAmbiguous`, **plus `10014` (Unknown Emoji) added to the permanent set** alongside `10003/10008/50001/50013` | `TelegramApiError` → not ambiguous; transport/timeout → ambiguous (existing rule) | — |
| redelivery after restart | re-issue, no duplicate label | re-issue, no duplicate label | — |
| text emoji handling | `<a?:name:id>` left intact | `<a?:name:id>` → fallback or removed | unchanged |

---

## 10. Degradation matrix

| situation | detected where | room sees | ledger | log |
| --- | --- | --- | --- | --- |
| emoji not in allowlist | gateway, prepare | text of the part if any, else nothing | no row | warn |
| custom name resolves in guild | adapter | reaction | confirmed | — |
| custom name missing, fallback configured | adapter | fallback unicode reaction | confirmed | info |
| custom name missing, no fallback | adapter | nothing (never `<:name:id>`, never `:name:`) | `fail(ambiguous:false)` → expires | warn |
| target message id unknown in this origin | gateway, prepare | text of the part if any | no row | warn |
| target id is a synthetic `rx!…` row | gateway, prepare | text of the part if any | no row | warn |
| loopback origin | gateway, prepare | nothing | no row | debug |
| per-turn / per-origin quota hit | gateway, prepare | nothing extra | no row | warn (once per window) |
| Telegram: emoji outside the 73-set | gateway, prepare (platform-aware) | nothing | no row | warn |
| Telegram: chat forbids reactions / service message | adapter, API 400 | nothing | `fail(ambiguous:false)` → expires | warn |
| Telegram: bot not admin (no inbound reactions) | nothing to detect | — | — | one startup info line |
| Discord: missing `ADD_REACTIONS` (50013) | adapter, API 403 | nothing | `fail(ambiguous:false)` → expires | warn |
| Discord: message deleted (10008) | adapter, API 404 | nothing | `fail(ambiguous:false)` → expires | info |
| 429 from either platform | adapter | delayed reaction or nothing | honour `retry_after`, then `fail(ambiguous:false)`; ledger retries 1s/2s/4s | warn |
| gateway restart mid-reaction | ledger redelivery | reaction appears once (idempotent) | pending → confirmed | — |
| reaction added then removed before the persona reads it | gateway, `contextRetract` | — | — | debug |
| reaction removed after the persona read it | gateway | — (context only) | — | — |
| inbound reaction flood | gateway quota | — | — | warn per window |
| `reactions.enabled = false` | gateway, prepare | `[REACT:]` stripped, text still delivered | no row | debug |

---

## 11. Test plan

All tests are unit tests in existing suites (`packages/gateway/test/`, `packages/protocol/test/`,
`packages/adapter-discord/test/`, `packages/adapter-telegram/test/`), using the mock `GjcPort` /
duck-typed adapter patterns already in `packages/gateway/test/server.test.ts`.

**protocol**
1. `isReactionEmoji` accepts every entry in `REACTION_EMOJI`.
2. `isReactionEmoji("❤️")` (with VS16) is true; `normalizeReactionEmoji` strips U+FE0F/U+FE0E.
3. `isReactionEmoji("💩")` is false — negative, and it is deliberately a Telegram-legal emoji.
4. `VERBS_V01` contains `chat.reacted`; `EVENTS_V01` contains `chat.reaction`.
5. `isSilenceToken` is unchanged for all `SILENCE_TOKENS` spellings, bracketed and not.

**gateway — inbound (the load-bearing negatives)**
6. **`chat.reacted` does not create a turn**: mock `GjcPort` whose `sendTurn` throws if called; after
   a `chat.reacted` request, `inboundPendingCount(originKey) === 0`, no `chat.message` event is
   emitted, and `sendTurn` was never called.
7. `chat.reacted` writes exactly one `conversation_context` row with `kind='reaction'` and the
   documented body text.
8. Duplicate `chat.reacted` (same target, reactor, emoji) is idempotent — still one row.
9. Removal of an **unconsumed** add deletes the row (`contextUnread` no longer returns it).
10. Removal of a **consumed** add inserts a `reaction_removed` row instead.
11. Removal with no matching row returns `{ recorded: false }` and writes nothing.
12. A recorded reaction row appears in the next turn's unread diff, and the turn that renders it is
    triggered by a *message*, never by the reaction.
13. Inbound reaction quota: the 21st reaction in a 60s window for one origin is dropped.
14. `chat.reacted` with an invalid origin → `invalid_params`.

**gateway — outbound**
15. `[REACT:👍]` alone produces a `chat.reaction` event with a `deliveryId` and **no** `chat.message`.
16. `[REACT:👍] ok, on it` produces both, reaction first, and the delivered text is exactly
    `ok, on it` (directive stripped).
17. `[REACT:👍@1234]` targets `1234`; the id must exist in this origin's context ledger.
18. Unknown target id → no reaction delivery, text still delivered.
19. `[REACT:...]` targeting an `rx!…` synthetic id → dropped; likewise `[REPLY:rx!…]` is rejected and
    the body still delivers.
20. **Disallowed emoji rejected**: `[REACT:💩] fine` delivers only `fine`, creates no ledger row.
21. Per-turn limit: three `[REACT:]` directives produce exactly two reaction deliveries.
22. Per-message limit: two directives on the same target produce one delivery.
23. Loopback origin: `[REACT:👍]` produces nothing and does not throw.
24. **Silence token still works**: reply `[SILENT]` → no `chat.message`, no `chat.reaction`, no
    daily-capture intent, and the turn is still recorded in recall.
25. `[REACT:👍]\n[BREAK]\n[SILENT]` → one reaction, zero messages.
26. `reactions.enabled = false` → directive stripped, text delivered, no reaction event.
27. `[REACT:` mid-sentence is literal text and is delivered verbatim.
28. Recall/daily-capture stores the canonical `reacted 👍 to msg:…` rendering, never the raw directive.

**gateway — ledger**
29. `prepareReaction` writes a `pending` row; `markInflight` then `delivery.confirm` → `confirmed`.
30. `delivery.fail({ambiguous:false})` three times → `expired`, and it counts in
    `gateway.status.delivery.pending` until then.
31. `redeliveries()` returns the reaction payload with `kind:"reaction"` and re-emits it as
    `chat.reaction`, while a legacy row with no `kind` re-emits as `chat.message`.

**adapter-discord**
32. Unicode reaction: correct URL-encoded route, then `delivery.confirm`.
33. Custom emoji resolves by exact name → `name:id` in the route.
34. **Missing custom emoji degrades**: no `send()` is ever called (no text posted), no `<:name:id>`
    or `:name:` appears in any outbound payload, and `delivery.fail` is reported.
35. Missing custom emoji with a configured fallback reacts with the fallback unicode emoji.
36. `10014` and `50013` are non-ambiguous failures; a thrown transport error is ambiguous.
37. A redelivered reaction is issued without the `[recovered - may be a duplicate]` label.
38. Self-authored reaction events are ignored (no `chat.reacted` request).
39. `MESSAGE_REACTION_ADD` on a message from another user maps to `onOwnMessage: false`.
40. Reaction requests to one channel are serialized with ≥250 ms spacing (fake timers).

**adapter-telegram**
41. Allowlisted emoji → `setMessageReaction` with `[{type:"emoji",emoji}]`, then `delivery.confirm`.
42. **Impossible reaction path**: `setMessageReaction` rejects with `TelegramApiError(400)` →
    `delivery.fail({ambiguous:false})`, **no `sendMessage` call at all** (no text fallback).
43. Custom-emoji reaction with no fallback → nothing sent, failure reported.
44. `message_reaction` update with `old_reaction`/`new_reaction` diffs into the right add/remove
    `chat.reacted` calls.
45. `getUpdates` passes `allowed_updates` including `message_reaction`.

**conformance**
46. `sdk-coverage-inventory` still passes: the gateway implements `chat.reacted` and emits
    `chat.reaction`, and no uncatalogued verb/event name is referenced.

---

## 12. P0 / P1 split

**P0 — the owner's request, end to end**
- `chat.reacted` verb; inbound reactions recorded as context rows; retract-on-unread-removal; the
  guarantee that no reaction creates a turn.
- `chat.reaction` event + `prepareReaction` in the existing ledger; confirm/fail/retry/redelivery.
- `[REACT:<emoji>]` / `[REACT:<emoji>@<id>]` parsing in `runInboundTurn`, ordered against `[BREAK]`,
  `[REPLY:]` and the silence gate.
- The 16-emoji allowlist, normalization, per-turn/per-message/per-origin quotas, silent rejection.
- Discord unicode reactions + intents + per-channel pacing + `10014` classification.
- Telegram unicode reactions (identity mapping), `allowed_updates`, impossible-reaction → ledger
  failure with no text fallback.
- Persona note: `[REACT:]` clause + house-style rules 1–6.
- Schema migration 9; tests 1–31, 32–33, 36–39, 41–42, 45–46.

**P1 — polish, once P0 is live**
- Custom guild emoji resolution, cache refresh, fallback map, `GUILD_EXPRESSIONS` intent (tests
  34–35, 43).
- Colon-code stripping and `<a?:name:id>` handling in outbound text (§5, §7).
- `>2 emoji` warn-log lint on outbound message parts.
- Telegram inbound `message_reaction` full diffing where the bot is an admin (test 44).
- Application-emoji resolution step, only after the UNVERIFIED question is answered by a live drill.
- A Discord-only extended emoji set, which is the first case that needs a real platform mapping table.

---

## 13. Assumptions and unverified items

**ASSUMPTIONS** (product/repo judgement, not platform facts)
- A1. Discord's reaction endpoint accepts `❤` and `✍` without VS16. If a drill disproves it, add a
  per-platform emit form to the allowlist table.
- A2. 2 reactions per turn / 6 per origin per 60s are the right numbers for a room a human reads.
  They are config-free constants on purpose; tune from live behaviour, not from taste.
- A3. Silent rejection of a disallowed emoji is better than telling the room. Any other choice
  converts a safety stop into chatter.
- A4. The persona will use `[REACT:]` at the right times because the base note tells it to prefer a
  reaction over a one-word message. If live behaviour shows over-reaction, tighten the note before
  adding runtime heuristics.
- A5. Reaction removals older than the context prune window are irrelevant; no separate retention.

**UNVERIFIED** (do not state these as limits)
- U1. "1 reaction per 250 ms per channel" on Discord is **not** in the current documentation. The
  250 ms pacing in this design is self-imposed, not cited.
- U2. Discord's per-message unique-emoji cap (commonly cited as 20) is not documented on the Message
  resource page; the design's 1-per-message rule makes it moot.
- U3. Whether Discord **application** emoji can be used as reactions (§5 step 3) — endpoints exist,
  reaction usability is not documented.
- U4. Telegram has no documented numeric rate limit for `setMessageReaction`.
- U5. Whether Telegram's 73-emoji reaction list is stable per-chat; the docs say "server-provided",
  and `ChatFullInfo.available_reactions` exists, so a chat could in principle allow fewer. The design
  does not query it; a 400 from a chat with a narrower set lands on the dynamic-failure path.

### Citations

- Discord — Rate Limits (global 50 req/s, `X-RateLimit-*`, `retry_after`, 10,000 invalid
  requests / 10 min): https://discord.com/developers/docs/topics/rate-limits
- Discord — Message resource, Create Reaction (`READ_MESSAGE_HISTORY`, `ADD_REACTIONS` when first,
  204, emoji must be URL-encoded, custom as `name:id`, `10014: Unknown Emoji`):
  https://discord.com/developers/docs/resources/message
- Discord — Gateway intents (`GUILD_MESSAGE_REACTIONS (1 << 10)`,
  `DIRECT_MESSAGE_REACTIONS (1 << 13)`, `GUILD_EXPRESSIONS (1 << 3)`):
  https://discord.com/developers/docs/events/gateway
- Discord — Gateway events, Message Reaction Add fields (`user_id`, `message_author_id`, `burst`,
  `type`): https://discord.com/developers/docs/events/gateway-events
- Discord — Emoji resource ("Routes for controlling emojis do not follow the normal rate limit
  conventions … limited on a per-guild basis"): https://discord.com/developers/docs/resources/emoji
- Telegram — `setMessageReaction` (one reaction per message for bots, no paid reactions, custom emoji
  only if already present or admin-allowed, some service messages not reactable):
  https://core.telegram.org/bots/api#setmessagereaction
- Telegram — `ReactionTypeEmoji` (the 73-emoji restricted set):
  https://core.telegram.org/bots/api#reactiontypeemoji
- Telegram — `Update.message_reaction` / `MessageReactionUpdated` (admin + explicit
  `allowed_updates`, not delivered for bot-set reactions, `old_reaction`/`new_reaction`):
  https://core.telegram.org/bots/api#messagereactionupdated
- Telegram — `getUpdates` `allowed_updates` (an explicit list is required to receive
  `message_reaction`): https://core.telegram.org/bots/api#getupdates

Telegram facts checked against Bot API 10.3 (2026-08-24) on 2026-08-27.
