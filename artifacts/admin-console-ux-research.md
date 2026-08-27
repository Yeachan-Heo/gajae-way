# Owner Console for a Personal AI Agent — UI/UX Research and Design Specification

**Mission:** `autoresearch`, mode `mixed` (web prior art + local codebase evidence).
**Subject:** replacement design for the `gajae-way` admin console.
**Date:** 2026-08-27 (Asia/Seoul).
**Branch:** `research/admin-ui-ux`. **Scope:** research artifact only, no product code.

---

## 0. Evidence base and honesty ledger

### 0.1 Local evidence actually read

| Path | What it established |
|---|---|
| `packages/admin/src/server.ts` | 4 read routes + 1 POST mutation route; injected `GatewayRequest`; no event stream, no session state |
| `packages/admin/src/gate.ts` | allowlist of 5 ops, `actor` required, `confirm === operationId` echo, optional `AuditSink` |
| `packages/admin/src/ui.ts` | one HTML string, three `<pre>` blocks, `setInterval(refresh, 5000)`, `JSON.stringify(..., null, 2)` |
| `packages/protocol/src/catalog.ts` | complete verb/event catalog for profile v0.1 (17 verbs, 4 events) |
| `packages/gateway/src/server/server.ts` | the real dispatch switch; `chat.progress` emission and throttling; `work.run` implementation |
| `packages/gateway/src/store/db.ts` | schema v8: `sessions`, `deliveries`, `recall_snippets`, `meta`, `memory_intents`, `monitors`, `monitor_events`, `authored_outputs`, `inbound_messages`, `conversation_context` |
| `packages/subsession/src/status.ts`, `lane.ts`, `index.ts` | `SupervisorOpState`, `requiresOperatorHold()`, lane identity, transcript paging |
| `packages/sdk/src/client.ts` | `on(event, handler)`, `onChatMessage`, `onChatProgress` — the client already multiplexes events |
| `docs/architecture.md`, `docs/monitors.md`, `docs/memory.md`, `README.md` | origins, engagement floors, delivery ledger states, monitor propagation stages, memory closure ladder |

### 0.2 Web sources fetched and used

- Google SRE Book, Ch. 6 *Monitoring Distributed Systems* — https://sre.google/sre-book/monitoring-distributed-systems/
- Temporal Web UI docs — https://docs.temporal.io/web-ui
- Argo CD Sync Options (`Prune=confirm`, `Delete=confirm`) — https://argo-cd.readthedocs.io/en/stable/user-guide/sync-options/
- GitHub "Deleting a repository" (Danger Zone ceremony) — https://docs.github.com/en/repositories/creating-and-managing-repositories/deleting-a-repository
- GitHub Actions "Manually running a workflow" (`workflow_dispatch` inputs) — https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow
- LangSmith observability concepts (run / trace / thread / trajectory) — https://docs.langchain.com/langsmith/observability-concepts
- LangGraph Interrupts (human-in-the-loop pause/resume) — https://docs.langchain.com/oss/python/langgraph/interrupts
- LangChain Agent Inbox (`HumanInterrupt` / `HumanResponse` schema) — https://github.com/langchain-ai/agent-inbox
- MDN, Using server-sent events (`retry:`, `id:`, `Last-Event-ID`, 6-connection cap on HTTP/1.1) — https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events

### 0.3 Blockers and assumptions — stated, not smuggled in as findings

- **Nielsen Norman Group articles were fetched but the article bodies did not render** through the reader for https://www.nngroup.com/articles/confirmation-dialog/ and https://www.nngroup.com/articles/progressive-disclosure/. Only page metadata was retrievable. I cite them for the claim carried in their own published abstract — confirmation dialogs must "reduce the risk that people automatically agree to a warning without realizing the consequences", and progressive disclosure "defers advanced or rarely used features to a secondary screen". Anything beyond those abstracts is **not** sourced from NN/g here.
- **Products asserted from prior knowledge, not verified in this session** (marked `[unverified]` at each use): Sidekiq Web, Home Assistant, Vercel deployment view, Datadog/Grafana dashboards, Slack thread UI, Linear inbox, Devin, OpenAI Operator, Cursor background agents, Uptime Kuma. Treat their specifics as design intuition, not as citations.
- **No runnable metric harness exists for this mission.** A UI design question has no deterministic `METRIC name=value` benchmark that would be honest. Phase-1 harness discipline is recorded as a caveat rather than faked with a synthetic number.
- **I did not run the current admin console against a live gateway.** Its behaviour is inferred from source, which is sufficient for the defects listed in §6 because each one is visible in the source itself.

---

## 1. Findings per research question

### Q1 — Canonical UI/UX patterns for consoles of long-running autonomous systems

**F1.1 — The run/execution object is the primary noun, not the machine.**
Temporal's Web UI is organised around a Workflow Execution: you land on a filtered list of executions, and a single execution opens onto History (Timeline / All / Compact / JSON), Pending Activities, Workers, Relationships, Call Stack, and Metadata (https://docs.temporal.io/web-ui). The critical detail is *Compact* view: "a logical grouping of Activities, Signals and Timers" — the raw event log exists, but it is the **fourth** tab, and JSON is the last. The default is a human summary.
*So what for us:* the primary noun is the **turn** (`turnId` + `origin`), not the daemon. Raw JSON must exist and must be the last tab, exactly inverting today's console where JSON is the only tab.

**F1.2 — A failure-shaped default view beats a neutral list.**
Temporal ships a *Task Failures* Saved View that surfaces workflows whose task failed or timed out, with a deliberate hysteresis: flagged after five consecutive task failures, unflagged on the first success, tunable via `numConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute`. The docs call this out explicitly — "This smart threshold filters out minor glitches while surfacing Workflows with genuine problems."
*So what for us:* the owner console needs a pre-baked "needs me" projection with a threshold, not a raw feed. Our equivalent flag conditions are enumerable from the runtime: `failed_ambiguous` deliveries, `expired` deliveries, `quarantined` memory intents, `monitor_events` stuck in `admitted`/`dispatched`/`failed`, and subsession states where `requiresOperatorHold()` is true.

**F1.3 — Trace / thread / trajectory is a solved taxonomy for agent surfaces.**
LangSmith separates three projections over the same data: a **trace** (tree of runs for one operation), a **thread** (sequence of traces for a multi-turn session, nesting intact), and a **trajectory** ("a flat, ordered list of messages … each appearing once, in the order it first appeared, with the nesting of runs removed") (https://docs.langchain.com/langsmith/observability-concepts). Their own table says when to reach for each: trace for debugging one operation, thread for cross-turn behaviour, trajectory for "reading what was exchanged … without the execution detail".
*So what for us:* this maps one-to-one onto our vocabulary. Trace = one turn's tool calls and progress. Thread = one `originKey` session across turns. Trajectory = the readable conversation projection. The console needs **all three**, and the owner's default must be the trajectory, because the owner reads, and only debugs when something broke.

**F1.4 — Explicit human-decision objects, delivered as an inbox.**
LangGraph's `interrupt()` suspends a graph, checkpoints state, and "waits indefinitely until you resume execution" (https://docs.langchain.com/oss/python/langgraph/interrupts). LangChain's Agent Inbox then renders those interrupts with a fixed schema: `HumanInterrupt { action_request { action, args }, config { allow_ignore, allow_respond, allow_edit, allow_accept }, description }` answered by `HumanResponse { type: accept|ignore|response|edit, args }` (https://github.com/langchain-ai/agent-inbox). Two things work here: the **agent declares which responses are legal**, so the UI cannot offer a decision the runtime cannot honour; and the description field is markdown intended to carry enough context to decide without leaving the item.
*So what for us:* our `SupervisorOpState` already has the two hold states — `terminal_uncertain` and `terminal_missing_receipt` — and `requiresOperatorHold()` already gates them. What is missing is (a) protocol exposure and (b) the `allow_*` affordance declaration. Adopt the Agent Inbox shape wholesale; it is the cheapest correct answer.

**F1.5 — Manual triggering must be a typed form, not a free-text params blob.**
GitHub Actions renders `workflow_dispatch` inputs as real form fields — "If the workflow requires input, fill in the fields" — with a branch selector and a documented 25-input ceiling (https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow).
*So what for us:* today's console has one `params (json)` text input for **all five** operations. `monitor.test` needs `{monitorId, eventType, payload}`; `ops.backup` needs `{path}`; `ops.integrity` needs nothing. Rendering one JSON box for all of them is the direct cause of every operator typo. Per-operation forms, derived from the allowlist entry.

**F1.6 — Symptom-first, cause-on-demand.**
Google SRE Ch. 6 is unambiguous: "Your monitoring system should address two questions: what's broken, and why? … it's better to spend much more effort on catching symptoms than causes". It also warns that "Signals that are collected, but not exposed in any prebaked dashboard nor used by any alert, are candidates for removal" (https://sre.google/sre-book/monitoring-distributed-systems/).
*So what for us:* the top of the console states symptoms in the owner's language ("2 replies stuck to Discord for 11 minutes"), and the cause (`state: failed_ambiguous, attempts: 3`) is one tap down. Conversely, every field we render must answer a question the owner actually asks — `schemaVersion` and `pid` fail this test and belong on a Diagnostics screen, not the home view.

**F1.7 — Stage machines render as a ladder, not a status word.**
Argo CD's sync panel does not print a status enum; it shows what was skipped and why, and it parks the operation in `Syncing` with a **"Confirm Pruning"** button when a resource is annotated `Prune=confirm` (https://argo-cd.readthedocs.io/en/stable/user-guide/sync-options/). The operation being visibly *incomplete and waiting for you* is the design.
*So what for us:* our monitor propagation is already a seven-rung ladder (`admitted → batched → dispatched → authored → memory queued → delivered → reconciled`, `docs/monitors.md`) and the DB stores the stage per event. Render it as a ladder with the current rung lit. Same for the delivery ledger (`pending → inflight → confirmed | failed_ambiguous | expired`) and the memory closure ladder (`queued → written → committed → receipted | quarantined`). Three ladders, one component. `[unverified]` this is also how Vercel renders build → deploy → assign-domain.

**F1.8 — Dependency-free, single-operator consoles are a proven shape.**
`[unverified]` Sidekiq Web and Uptime Kuma both ship a small server-rendered dashboard with no build step and a handful of pages; Home Assistant proves that a resident daemon aimed at one household needs a mobile-first card layout more than it needs a desktop grid.
*So what for us:* the zero-dependency constraint is not a handicap for this class of tool. It is the norm.

### Q2 — What the owner needs at a glance vs. on demand

**The first 5 seconds must answer exactly three questions, in this order:**

1. **Is it alive?** — one line, from `gateway.status`: uptime derived from `startedAt`, `profileVersion`, `sessions.active`. Not `pid`, not `schemaVersion`.
2. **Is it doing something right now?** — count of in-flight turns and what they are: origin label, elapsed, tool calls. Sourced from `chat.progress` (throttled: first event at 15 s, then every 15 s — `packages/gateway/src/server/server.ts`), terminated by `chat.message` with `final: true`.
3. **Does anything need me?** — a single integer badge, and if it is zero the console says so in words. This is the Temporal *Task Failures* idea (F1.2) with our own flag set.

Everything else is on demand. The information hierarchy:

```
TIER 0  (always visible, ~1 line each, no scrolling on a phone)
        alive · working-now count · attention count

TIER 1  (one scroll, home screen cards)
        Attention queue     — holds, ambiguous deliveries, quarantined intents, failed monitor events
        Live work           — in-flight turns and delegated work, with elapsed + evidence
        Recent conversation — last N exchanges across all origins, newest first

TIER 2  (one tap from a Tier-1 row)
        Session detail      — one origin: trajectory, engagement mode, epoch, pending inbound
        Monitor detail      — schedule, next fire, last 20 events with stage ladder
        Delivery detail     — one ledger item, attempts, age, duplicate-warning flag
        Work/subsession     — prompt, status, receipt state, hold reason

TIER 3  (deliberate navigation, never on the home screen)
        Memory browser · Audit log · Operations (backup/integrity) · Raw JSON inspector
```

**Rule:** anything an owner would look at less than once a week is Tier 3. Backup and integrity are Tier 3 despite being in today's allowlist. Memory search is Tier 3 because the memory corpus is already readable Markdown on disk (`docs/memory.md`) — the console is not the primary reader.

### Q3 — Representing in-flight autonomous work so the owner can trust it

Trust in an autonomous process comes from four things, and our runtime can supply three of them today.

**F3.1 — Elapsed against a known ceiling, not a spinner.** The gateway enforces a 300-second ceiling on every `gjc` child (`docs/architecture.md`). That makes a *bounded* progress representation possible: `2m 41s / 5m` with a determinate bar. A determinate bar against a real deadline is honest; a barber-pole spinner is not.

**F3.2 — Evidence of work, not assertion of work.** `ChatProgressPayload` already carries `toolCalls` and `outputTokens`. "14 tool calls, 3.2k tokens out" is evidence. "Working…" is a claim. Temporal makes the same distinction with its Pending Activities panel and its `__stack_trace` Call Stack query, which "shows each location where Workflow code is waiting" (https://docs.temporal.io/web-ui) — the UI proves the worker is blocked somewhere specific.

**F3.3 — Liveness must be able to expire.** The gateway heartbeats `chat.progress` every 15 s even when the `gjc` stream is silent. Therefore the console can assert a hard rule: **if no progress event for a turn has arrived in 45 s (3 missed heartbeats), the row degrades to `stalled?` with the last-known counters and a timestamp.** It does not keep animating. This is the single most important anti-fake-liveness mechanism available to us and it costs nothing, because the heartbeat already exists.

**F3.4 — The moment a decision is required must be a different visual class, not a different colour.** An Agent Inbox item is not a red log line; it is a card with the action name as its header, the arguments in a table, a markdown description, and only the buttons the runtime declared legal (https://github.com/langchain-ai/agent-inbox). Holds belong in the Attention queue at Tier 1, never inline in a timeline where they scroll away.

*Missing today:* there is **no protocol surface** for in-flight turns at all. `chat.progress` is broadcast to whoever happens to be connected; a console that connects at 12:04 knows nothing about the turn that started at 12:03. See gap **G1** in §8.

### Q4 — Making gated actions meaningful rather than habitual

The current gate is structurally sound and experientially worthless. Its three checks — allowlist, `actor`, `confirm === operationId` — all live in `gate.ts` and all are enforced server-side. The failure is in the UI: the operation id is **already visible in the adjacent `<select>`**, so "type the operation id" degrades to copy the string you are looking at. That is precisely the automatic agreement NN/g warns about in the abstract of https://www.nngroup.com/articles/confirmation-dialog/.

Four corrections, each with prior art:

**F4.1 — Confirm the *object*, not the *verb*.** GitHub's Danger Zone requires you to type the **repository name**, after two separate acknowledgement clicks, having read a warning that names the irreversible consequences (https://docs.github.com/en/repositories/creating-and-managing-repositories/deleting-a-repository). The typed token is the thing being destroyed, and it is not on screen next to the input.
*Applied:* `monitor.remove` should require typing the **monitor name** (`MonitorRecord.name`), not the string `monitor.remove`. `ops.backup` needs no echo at all — it is additive. The echo requirement should scale with blast radius, which means the allowlist entry needs a `severity` and a `confirmToken` field, not a uniform rule.

**F4.2 — Show the consequence before the button, computed live.** Argo CD parks the sync in `Syncing` and shows exactly which resources await confirmation before offering "Confirm Pruning" (https://argo-cd.readthedocs.io/en/stable/user-guide/sync-options/).
*Applied:* the confirm panel for `monitor.remove` renders the monitor's name, trigger, declared `eventTypes`, `channelTarget`, and its event count from the last 7 days — "this monitor has fired 43 times and posted to #ops" is what makes the decision real. That data is available from `monitor.inspect`.

**F4.3 — Typed forms per operation.** Per F1.5. Each allowlist entry declares its fields; the console renders them; the JSON blob disappears. This removes an entire error class (`{"monitorId":"m1"}` typed into the wrong operation) that the gate cannot catch, because the gate validates the *operation*, not the *params*.

**F4.4 — The audit trail must be visible in the console that produces it.** `AuditSink` is optional (`gate.ts: readonly audit?: AuditSink`) and nothing in `packages/gateway` or `packages/cli` wires one. An audit trail nobody can read is a compliance ornament. Show the last 20 attempts — allowed and rejected, with actor and reason — directly under the mutation panel. Seeing "3 rejected attempts, 1 allowed, yesterday 22:14" is what converts the gate from friction into a record.

**Explicit non-recommendation:** do **not** add `chat.send` to the allowlist. `gate.ts` documents the reason correctly — it is "the one mistake with an irreversible, public effect". If owner-reply-from-console is ever wanted, it needs a distinct verb restricted to origins the owner already participates in, with its own gate, not a line in the existing allowlist.

### Q5 — Conversation/timeline representation for a Discord/Telegram-native agent

**Do not mirror the chat. Project it.** Three reasons grounded in the runtime:

1. **Fidelity is unachievable and pretending otherwise is worse than not trying.** Discord and Telegram own threading, reactions, edits, attachments, and per-guild display names. Our DB stores `conversation_context (message_id, origin_key, author_id, author_name, body, received_at, consumed_at)` and `authored_outputs`. A "chat view" built on that will look like Discord and behave nothing like it. `[unverified]` this is the standard failure of chat-mirroring admin panels.
2. **The owner's question is different from a participant's question.** In the room, the owner reads to converse. In the console, the owner reads to audit: *did it answer, was it right, did it stay silent when it should have, did the reply actually land*. That is the LangSmith **trajectory** projection — "reading what was exchanged in the session, without the execution detail" (https://docs.langchain.com/langsmith/observability-concepts) — augmented with delivery outcome, which no chat client shows.
3. **The interesting events are invisible in the chat itself.** Four of them, all of which the gateway already computes and none of which a Discord mirror could render:
   - **Declined by engagement policy** — `ChatSendResult { turnId: null, engaged: false }` when a mention-gated group message was not addressed to the agent. The room shows nothing; the console must show "seen, not engaged".
   - **Silence** — `isSilenceToken()` suppresses outbound delivery while keeping the turn in the transcript (`catalog.ts`). The console shows "replied `[SILENT]` — nothing sent", which is *precisely* the behaviour an owner wants to verify.
   - **Redelivery** — `redelivered: true` with `duplicateWarning: true` after a crash. The console should show which messages the owner's contacts may have seen twice.
   - **Turn failure** — the ledgered `[turn failed]` notice (`docs/architecture.md`).

**Concrete projection — one row per exchange, newest first:**

```
14:02  #ops (guild "가재판")            ·  engaged: mention
       yeachanheo → "배포 로그 확인해줘"
       ↳ replied in 41s · 6 tools · delivered ✓ 14:03
14:00  DM yeachanheo                    ·  engaged: dm
       "굿모닝"
       ↳ [SILENT] · nothing sent
13:47  #random                          ·  seen, not engaged (no mention)
       someone → "..."
```

Tapping a row opens the session trajectory for that `originKey` plus the trace for that `turnId`. Collapsed by default, expandable — the progressive disclosure principle per the published abstract of https://www.nngroup.com/articles/progressive-disclosure/.

*Missing today:* no verb returns any of this. `session.list` gives four scalar fields per origin. See gaps **G2**, **G4**.

### Q6 — Real anti-patterns for this class of tool

| Anti-pattern | Why it fails | Evidence |
|---|---|---|
| **Raw JSON as the interface** | JSON is a serialisation, not a hierarchy. It gives every field equal visual weight, so `pid` competes with `oldestPendingAgeMs`. The reader must do the ranking the designer refused to do. | Temporal makes JSON the *fourth* History tab behind Timeline/All/Compact (https://docs.temporal.io/web-ui) |
| **Fixed-interval full-refresh polling** | Every tick replaces the DOM, so scroll position, text selection, and expanded state are destroyed; and the refresh rate becomes the truth resolution regardless of event rate. | Google SRE: choose resolution per signal, not one global rate (https://sre.google/sre-book/monitoring-distributed-systems/) |
| **Fake liveness** | An animation that runs whether or not data is arriving trains the operator to trust motion. When the stream dies, the console lies confidently. | SRE Ch. 6's entire symptom/cause framing depends on the dashboard reflecting reality |
| **Alert fatigue / undifferentiated red** | "When pages occur too frequently, employees second-guess, skim, or even ignore incoming alerts, sometimes even ignoring a 'real' page that's masked by the noise." | https://sre.google/sre-book/monitoring-distributed-systems/ (verbatim) |
| **Habituated confirmation** | A confirmation the operator can satisfy by reading the adjacent widget is a keystroke tax, not a decision point. | NN/g abstract: reduce the risk that people "automatically agree to a warning without realizing the consequences" (https://www.nngroup.com/articles/confirmation-dialog/) |
| **The dashboard nobody opens** | A console that only restates what the chat client already shows has no reason to be opened, so it is not open when it matters. | SRE Ch. 6: signals not on a prebaked dashboard or alert "are candidates for removal" — the inverse applies to whole dashboards |
| **Untyped free-text params** | Shifts schema validation onto the operator at the exact moment they are performing a destructive action. | GitHub Actions renders `workflow_dispatch` inputs as fields (https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow) |
| **Unbounded infinite log** | An append-only feed with no "needs me" projection makes the operator the filter. Temporal's answer was a thresholded, pre-baked failure view. | https://docs.temporal.io/web-ui (Task Failures View) |

### Q7 — The right technical shape under the constraints

**Decision: server-rendered HTML shell + one SSE stream + surgical DOM patching. No SPA, no framework, no build step.**

- **Server-rendered shell.** `renderIndex()` already proves the shape works; it needs to render *semantic* markup (`<dl>`, `<ol>`, `<details>`, `<progress>`, `<time datetime=...>`) instead of `<pre>`. First paint is complete and correct with JavaScript disabled — which also means the phone shows something useful on a slow connection before the stream opens.
- **SSE, not polling, not WebSocket.** SSE is the correct fit for a strictly one-way telemetry stream: `EventSource` reconnects automatically, honours a server-supplied `retry:` interval, and replays position via `id:` / `Last-Event-ID` (https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events). A WebSocket buys bidirectionality we do not want — mutations must go through the audited POST path, not an unlogged socket frame. Polling buys nothing and costs the flicker in Q6.
- **This is nearly free architecturally.** The gateway already broadcasts events to *every* negotiated connection (`for (const recipient of runtime.connections) if (recipient.negotiated) recipient.write(...)`, `server/server.ts`), and the SDK client already demultiplexes them via `on(event, handler)`. The admin process holds **one** persistent SDK connection and fans out to **N** browser `EventSource` clients. No new gateway transport.
- **Known SSE ceiling, and why it does not bite us.** Over HTTP/1.1 the browser caps concurrent connections at 6 per origin (MDN, above). Loopback single-operator use means 1–2 tabs. Mitigate anyway with one stream per page, not one per panel.
- **State model.** Snapshot-then-delta. On load, the server inlines a JSON snapshot into the HTML; the SSE stream then carries deltas keyed by a stable DOM id. The client patches individual nodes — it never re-renders a panel — so scroll, selection, and `<details>` state survive.
- **Mobile.** One column, CSS `grid` with `grid-template-columns: repeat(auto-fit, minmax(320px, 1fr))` so the desktop gets columns for free. Tap targets ≥ 44 px. Status conveyed by **glyph + word + position**, never colour alone — the constraint bans icon fonts, and Unicode geometric characters (`●○◐▲` and `✓ ✕ ⏸`) cost nothing and are screen-reader-legible when paired with text.
- **Theming.** `color-scheme: dark light` is already in place and is the right call. Add `light-dark()` for the handful of semantic colours (ok / warn / danger / muted) and stop there.
- **Total budget.** One HTML document, one `<style>` block, one `<script>` block. Realistically 1200–1800 lines across `ui.ts` split into per-panel render functions. Bun serves it; nothing is built.

---

## 2. Information architecture

### Screen 1 — `/` Home (the only screen that matters)

Priority order top to bottom. On a phone this is the scroll order; on desktop, Status is a sticky header and Attention/Live/Conversation become columns.

1. **Status bar** (sticky, ~1 line)
   `● alive 4d 6h · 7 sessions · 2 working · ⚠ 1 needs you · stream ✓ 14:02:31`
   The stream indicator is part of the status line, not a corner dot: when the SSE connection drops, the whole bar changes to `⚠ stream lost — showing data from 13:58`.

2. **Attention queue** — empty state is a *feature*: `Nothing needs you.` in plain text. Items, in severity order:
   - operator holds (`terminal_uncertain`, `terminal_missing_receipt`)
   - `failed_ambiguous` deliveries (a human may have seen a duplicate)
   - `quarantined` memory intents
   - `expired` deliveries (a reply never landed)
   - monitor events stuck in `admitted` / `dispatched` / `failed` past a threshold

3. **Live work** — one row per in-flight turn and per running delegated work item. Determinate `<progress>` against the 300 s ceiling, `toolCalls`, `outputTokens`, elapsed, and the stall rule from F3.3.

4. **Conversation** — the projection from Q5. Last 20 exchanges across all origins.

5. **Monitors** — compact: name, trigger summary, next fire, last outcome glyph. Not the full record.

6. **Footer links** to Tier-3 screens. Deliberately unglamorous.

### Screen 2 — `/session/<originKey>`
Header: platform/kind, channel + server labels, engagement mode, epoch, pending inbound count, session age. Body: trajectory (default) with a `trace` toggle per turn. Actions: none in P0.

### Screen 3 — `/monitor/<monitorId>`
Record, trigger, declared `eventTypes`, burst policy, channel target, enabled state. Last 20 events as stage ladders. Actions: test-fire (gated), disable (gated), remove (gated, name echo).

### Screen 4 — `/work/<id>`
Prompt, cwd, session key, status ladder, receipt state, transcript tail, hold reason and decision affordances when held.

### Screen 5 — `/ops` (Tier 3)
Backup, integrity, memory audit, delivery ledger table, memory intent table, audit log, gateway diagnostics (`pid`, `schemaVersion`, `capabilities`, `profileVersion`), raw JSON inspector for any protocol method.

---

## 3. Component inventory with exact protocol reads

`✅` = satisfiable today. `⚠️` = partially satisfiable. `❌` = blocked on a protocol gap (see §8).

| # | Component | Screen | Protocol read | Status |
|---|---|---|---|---|
| C1 | Status bar — alive, uptime, session count | Home | `gateway.status` → `startedAt`, `sessions.active`, `profileVersion` | ✅ |
| C2 | Status bar — delivery health chip | Home | `gateway.status` → `delivery.pending`, `delivery.oldestPendingAgeMs` | ✅ |
| C3 | Stream-health indicator | Home | client-side; SSE `open`/`error` + last-event timestamp | ✅ |
| C4 | Live work — in-flight turns | Home | `chat.progress` event (live) + terminated by `chat.message{final:true}` | ⚠️ live-only; no backfill on reconnect → **G1** |
| C5 | Live work — delegated work | Home | none — `work.run` is synchronous and returns only final text | ❌ **G7** |
| C6 | Attention — operator holds | Home | none — `SupervisorOpState` is library-internal | ❌ **G8** |
| C7 | Attention — ambiguous/expired deliveries | Home | `gateway.status` gives counts only; `deliveryRows()` exists in the DB, no verb | ❌ **G3** |
| C8 | Attention — quarantined memory intents | Home | `memoryIntentRows()` exists in the DB, no verb | ❌ **G5** |
| C9 | Attention — stuck monitor events | Home | `monitor.list` then `monitor.inspect` per monitor (N+1) | ⚠️ works, inefficient → **G6** |
| C10 | Conversation projection | Home | none — `conversation_context` + `authored_outputs` unexposed | ❌ **G2** |
| C11 | Session list | Home / Session | `session.list` → `origin`, `createdAt`, `lastActivityAt`, `epoch` | ⚠️ no labels, no unread, no last-turn outcome → **G4** |
| C12 | Session detail header | Session | `session.list` + missing per-origin detail | ⚠️ → **G4** |
| C13 | Session trajectory | Session | none | ❌ **G2** |
| C14 | Cross-session recall panel | Session | `session.recall` → `{query, limit, requestingOrigin}` | ✅ |
| C15 | Monitor list | Home / Monitor | `monitor.list` → `MonitorRecord[]` | ✅ |
| C16 | Monitor detail + event ladder | Monitor | `monitor.inspect` → `{monitor, recentEvents[]}` with `stage` | ✅ |
| C17 | Monitor next-fire time | Monitor | none — cron `schedule` is a string; no computed next-fire | ⚠️ compute client-side or → **G6** |
| C18 | Monitor test-fire | Monitor | `monitor.test` → `{monitorId, eventType?, payload?}` (gated) | ✅ |
| C19 | Monitor create | Monitor | `monitor.add` → `MonitorSpec` (gated) | ✅ |
| C20 | Monitor remove | Monitor | `monitor.remove` → `{monitorId}` (gated) | ✅ |
| C21 | Monitor enable/disable | Monitor | none — `enabled` column exists, no verb | ❌ **G6** |
| C22 | Memory search | Ops | `memory.search` → `{query, limit}` | ✅ |
| C23 | Memory audit | Ops | `memory.audit` → `{ok, issues[]}` (gated: slow) | ✅ |
| C24 | Backup | Ops | `ops.backup` → `{path}` (gated) | ✅ |
| C25 | Integrity check | Ops | `ops.integrity` → `{ok, detail}` (gated) | ✅ |
| C26 | Mutation console (typed forms) | Ops | `GET /api/operations` → allowlist; `POST /api/mutations` | ⚠️ needs `fields`/`severity`/`confirmToken` on `MutationOperation` |
| C27 | Audit log viewer | Ops | none — `AuditSink` is optional and unwired | ❌ **G9** |
| C28 | Gateway diagnostics | Ops | `gateway.status` → `pid`, `schemaVersion`, `capabilities` | ✅ |
| C29 | Raw JSON inspector | Ops | any read-route method, rendered last | ✅ |
| C30 | Shutdown | — | `gateway.shutdown` — **deliberately excluded from the console** | n/a |

**Immediate defect found while building this table:** `packages/admin/src/server.ts` maps `/api/core` to the method `gateway.core`. `gateway.core` is a **capability string** declared in `packages/protocol/src/version.ts`, not a verb — it does not appear in `VERBS_V01` and there is no `case "gateway.core"` in the gateway dispatch switch. Against a live gateway that route returns 502. `packages/admin/test/server.test.ts` asserts only that the route forwards the string to a stub, so the test passes and the route has never worked. Delete the route.

---

## 4. Interaction specs for the three hard cases

### 4.1 An operator hold needing a decision

**Trigger:** a subsession reaches `terminal_uncertain` or `terminal_missing_receipt`; `requiresOperatorHold()` returns true (`packages/subsession/src/status.ts`).

**Why it is hard:** the two states mean genuinely different things and the wrong merge destroys the decision. `terminal_uncertain` = the work may have completed but the outcome cannot be trusted. `terminal_missing_receipt` = the work claims completion but the evidence is absent. Rendering both as "failed" throws away the distinction the runtime went to trouble to compute.

**Presentation** — Agent Inbox card shape (https://github.com/langchain-ai/agent-inbox), rendered at Tier 1:

```
┌─────────────────────────────────────────────────────────┐
│ ⏸  DECISION NEEDED                        held for 22m  │
│ work/task/refactor-delivery                             │
│                                                          │
│ State  terminal_missing_receipt                          │
│ Means  The session reported completion but no receipt    │
│        was written. The work may or may not have landed. │
│                                                          │
│ Evidence                                                 │
│   session   0f3a…c21 (epoch 2)                           │
│   prompt    "…" [expand]                                 │
│   last out  "…" [expand]                                 │
│   receipt   absent                                       │
│   stop      —                                            │
│                                                          │
│ [ Accept as complete ]  [ Re-run ]  [ Ignore ]           │
└─────────────────────────────────────────────────────────┘
```

**Rules:**
1. **Held time is displayed and it grows.** A hold sitting for 22 minutes reads differently from one 4 seconds old. This is the honest counterpart to the stall rule in F3.3.
2. **The buttons are declared by the runtime, not by the UI.** Adopt `HumanInterruptConfig`'s `allow_accept / allow_respond / allow_edit / allow_ignore`. A hold whose state cannot be safely accepted must not render an Accept button.
3. **`Means` is a fixed sentence per state, written once by us.** The owner must not have to know the state machine to answer the question.
4. **Evidence is inline and collapsed**, never behind a link to another screen. A decision requiring navigation gets deferred, and a deferred hold is an unresolved hold.
5. **A hold decision is a gated mutation** (§4.2) with `severity: medium` — actor required, but no token echo. It is a judgement, not a destruction; adding ceremony here trains click-through.
6. **The card is dismissed by the runtime, not by the click.** Optimistic removal, then confirm against the next stream event; on failure the card returns with the error attached.

**Blocked on:** gaps **G8** (`subsession.list` / `subsession.resolve`) and **G7**.

### 4.2 A gated mutation

**Current flow:** pick from a select → type actor → type the operation id that is visible in the select → type JSON → click run → get a JSON blob.

**Specified flow** — three states in one panel, no modal (modals are hostile on a phone and are the classic habituation surface):

**State A — Compose.**
Operation picker. Below it, the fields **for that operation only**, derived from an extended allowlist entry:

```ts
type MutationOperation = {
  id: string;
  method: string;
  summary: string;
  severity: "low" | "medium" | "high";
  fields: readonly {
    name: string;
    label: string;
    kind: "text" | "monitor-ref" | "path" | "json";
    required: boolean;
  }[];
  /** Which value must be typed back to confirm. Absent for low severity. */
  confirmToken?: "operation-id" | "target-name";
};
```

`kind: "monitor-ref"` renders a select populated from `monitor.list`, so the owner picks "weekday-review", not a UUID.

**State B — Consequence + confirm.** Clicking Review (not Run) replaces the form with a rendered consequence, computed from live reads:

```
You are about to REMOVE a monitor.

  weekday-review
  cron  30 8 * * 1-5   (weekdays 08:30)
  emits review.due
  posts to discord/channel/1493635653441945762
  fired 43 times in the last 7 days, last 2026-08-27 08:30

This cannot be undone. The monitor and its schedule are deleted.
Its past events and authored notes are kept.

Type the monitor name to confirm:  [            ]
Actor:                             [            ]

              [ Cancel ]  [ Remove weekday-review ]
```

Per F4.1/F4.2. The typed token is the **monitor name**, and it is deliberately placed **above** the input, separated by the consequence text — you must scroll past the reason to reach the field. The action button restates the target. `ops.backup` (`severity: low`) skips State B entirely: one click, actor only.

**State C — Result + receipt.** Not a JSON dump. Outcome sentence, the audit entry that was written, and the refreshed affected panel:

```
✓ Removed weekday-review at 14:07:22
  audited: actor=형님 · operation=monitor.remove · allowed
  Monitors: 5 → 4
```

Rejections are equally specific and preserve the form: `428 — confirmation did not match. You typed "weekday review"; expected "weekday-review".`

**Server-side invariants (unchanged, and must stay unchanged):** the gate never trusts the client; `GET /api/mutations` remains 405 so no mutation is reachable by following a link; every attempt is audited before the method is dispatched. The redesign is entirely presentational plus the allowlist metadata — `MutationGate.evaluate()` keeps its exact semantics.

### 4.3 A long-running subsession

**Lifecycle as the owner experiences it:** started → running with evidence → possibly held → terminal. Today the console can see none of it, because `work.run` blocks the request until the turn finishes and returns only `{text, sessionKey}`.

**Specified row (Live work, Tier 1):**

```
▶ refactor-delivery                                    3m 12s
  work/task/refactor-delivery · /Users/…/gajae-way
  ▓▓▓▓▓▓▓▓▓▓░░░░░░░░  3:12 / 5:00
  14 tool calls · 3,240 tokens out · last event 6s ago
  [ open ]  [ cancel ]
```

**Rules:**
1. **Determinate against the real ceiling.** 300 s (`docs/architecture.md`). Past 80 % the bar changes glyph and the row reads `approaching timeout`.
2. **`last event Ns ago` is always visible.** Once it exceeds 45 s the bar stops animating and the row reads `⚠ no progress for 1m 20s — last known: 14 tool calls`. Per F3.3.
3. **Tool calls are the trust signal.** A run at 4 minutes with 0 tool calls and 0 tokens is a different situation from one with 14 and 3.2k, and the row must make that legible without opening anything.
4. **Terminal rows persist for 10 minutes** in a "recently finished" group with their outcome, then move to history. Work that vanishes the instant it finishes cannot be verified by an owner who was away from the desk.
5. **Cancel is a `severity: medium` gated mutation.** Actor, no echo.
6. **Opening the row** shows prompt, cwd, session key + epoch, full status ladder, transcript tail (`packages/subsession/src/transcript.ts` already pages this), and hold state.

**Blocked on:** **G7** (async `work.start` / `work.list` / `work.cancel` + a `work.progress` event) and **G8**.

---

## 5. Realtime strategy

### Decision

**One SSE endpoint, `GET /api/stream`, fed by a single persistent SDK connection held by the admin process, fanned out to browser clients. Snapshot inlined at page load; deltas over the stream; periodic reconciliation every 60 s.**

### Tradeoffs considered

| Option | Pros | Cons | Verdict |
|---|---|---|---|
| Polling every 5 s (current) | trivially simple; no connection state | flicker; DOM churn destroys scroll/selection/`<details>`; up to 5 s stale; N requests/tick × M panels; cannot express "an event happened" | **Rejected** — it is the current failure |
| Long-poll | works everywhere | reimplements SSE badly, with worse reconnect semantics | Rejected |
| WebSocket | bidirectional; binary | needs an upgrade handler, ping/pong keepalive, and hand-rolled reconnect+backoff; bidirectionality is a *liability* because mutations must stay on the audited POST path | Rejected |
| **SSE** | native `EventSource` auto-reconnect; server-controlled `retry:`; `id:` + `Last-Event-ID` resume; plain `text/event-stream`, zero deps; maps directly onto gateway events | one-way (correct here); 6-connection HTTP/1.1 cap (irrelevant at 1–2 loopback tabs); needs an explicit keepalive comment | **Chosen** |

Reference for every SSE mechanism named: https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events

### Wire shape

```
retry: 3000

: keepalive                       ← every 15s, prevents idle-timeout death

id: 1042
event: turn.progress
data: {"turnId":"…","origin":{…},"elapsedMs":192000,"toolCalls":14,"outputTokens":3240}

id: 1043
event: turn.final
data: {"turnId":"…","origin":{…},"text":"…","deliveryId":"…"}

id: 1044
event: monitor.event
data: {"eventId":"…","monitorId":"…","eventType":"review.due","stage":"authored"}

id: 1045
event: attention
data: {"count":1,"items":[…]}
```

The `id:` counter is admin-process-local and monotonic. `event: attention` is a **computed** projection the admin server derives — the browser must never assemble the attention queue itself, because that duplicates severity policy in two places.

### Fallback when the stream drops

Five behaviours, in order:

1. **Say so, immediately and unmistakably.** The status bar becomes `⚠ stream lost — showing data from 13:58`. Every live-work row freezes and is annotated with its last-known timestamp. **Nothing continues to animate.** This is the anti-fake-liveness rule from Q6 applied at the transport layer.
2. **Let `EventSource` reconnect.** It does this natively; `retry: 3000` sets the interval. Do not hand-roll a reconnect loop on top of it.
3. **Escalate on repeated failure.** After 3 failed reconnects, the admin server sends a larger `retry:` (15 s) on the next successful connect, and the UI offers an explicit `[ retry now ]` button. No infinite tight loop against a dead gateway.
4. **Reconcile on reconnect, do not replay.** `Last-Event-ID` tells the server how far behind the client is, but the admin process is **not** an event store — the gateway does not offer event replay (gap **G10**), and the admin process must not pretend to be a durable log. So on reconnect the server sends `event: snapshot` with a full recomputed state, and the client replaces wholesale. Correct-by-reconstruction beats a delta chain with a hole in it.
5. **Distinguish "admin lost the gateway" from "browser lost the admin".** These are different failures with different fixes and must read differently: `⚠ gateway unreachable — the daemon may be down` vs `⚠ console stream lost — reconnecting…`. The admin process knows which one it is; the browser must be told.

**Belt and braces:** a 60 s low-frequency reconciliation poll runs *even while the stream is healthy*, refreshing `gateway.status` + `session.list` + `monitor.list`. It costs three cheap reads a minute and it means a silently-wedged stream self-heals within a minute instead of lying indefinitely. This is not a fallback; it is a correctness backstop, and it is the one place where polling is the right tool.

---

## 6. Anti-patterns mapped to the current implementation

| # | Anti-pattern | Exactly where it lives today | The failure |
|---|---|---|---|
| A1 | Raw JSON as the interface | `ui.ts`: `show(id, JSON.stringify(body.result ?? body, null, 2))` into `<pre id="status">`, `<pre id="sessions">`, `<pre id="monitors">` | Zero information hierarchy. `pid` and `oldestPendingAgeMs` have identical visual weight. The owner performs the ranking the console refused to do. |
| A2 | Fixed-interval full refresh | `ui.ts`: `setInterval(refresh, 5000)` calling three `load()`s that overwrite `textContent` wholesale | Every 5 s: scroll jumps, text selection dies, and any expanded state is lost. On a phone the page becomes unusable while reading. |
| A3 | Fake liveness | `ui.ts`: `document.getElementById("clock").textContent = new Date().toISOString()` — the **browser's** clock, updated regardless of whether any fetch succeeded | The clock ticks confidently while the gateway is down. Individual panels do show `unreachable:`, but the header keeps asserting freshness. This is the exact lie SRE Ch. 6's symptom-orientation exists to prevent. |
| A4 | Habituated confirmation | `ui.ts`: `<select id="op">` lists every operation id, and `<input id="confirm" placeholder="type the operation id">` sits three fields away | The confirmation token is on screen, adjacent to the input. The gate in `gate.ts` is correct; the UI hands the operator the answer. |
| A5 | Untyped params | `ui.ts`: `<label>params (json)<br><input id="params" value="{}"></label>`, one field for all 5 operations | JSON schema validation is offloaded onto the operator during a destructive action. `monitor.remove` and `ops.backup` take entirely different params through the same box. |
| A6 | Invisible audit trail | `gate.ts`: `readonly audit?: AuditSink` — optional, and no production caller wires one anywhere in `packages/gateway` or `packages/cli` | The console claims "Every attempt is audited server-side" (`ui.ts`) while the default deployment audits to nothing and the console can display nothing. The claim is currently false. |
| A7 | Dead route shipped green | `server.ts`: `"/api/core": { method: "gateway.core" }`; `gateway.core` is a capability, not a verb | Returns 502 against a real gateway. `server.test.ts` asserts only stub forwarding, so CI is green on a route that has never worked. |
| A8 | Console not wired to anything | `startAdminServer` is exported from `packages/admin/src/index.ts` and referenced nowhere in `packages/gateway` or `packages/cli` | There is no supported way for the owner to *start* the console. A dashboard nobody can open is the terminal case of "dashboard nobody opens". |
| A9 | No attention projection | Nothing in `server.ts` or `ui.ts` computes severity | Everything is equally urgent, therefore nothing is. Temporal's answer (Task Failures View) is absent. |
| A10 | No conversation surface | No route touches `conversation_context` or `authored_outputs` | The console cannot answer the owner's most basic question — "what did it say?" — despite the rows being in the same SQLite file. |

---

## 7. Prioritised implementation plan

Each item is scoped to one implementation session. P0 ships a console worth opening **using only verbs that exist today**; nothing in P0 is blocked on a protocol gap.

### P0 — a console the owner will actually open (4 sessions)

**P0.1 — Mount it and fix what is broken.**
Wire `startAdminServer` into the gateway daemon behind a config flag (loopback bind, default off), so the console is reachable. Delete the `/api/core` route and its test. Wire a real `AuditSink` that appends JSONL to `$GAJAEWAY_HOME/admin-audit.jsonl`, and add `GET /api/audit` to read the tail. *Done when:* the daemon serves the console on a configured port and a rejected mutation appears in the audit file and in the API response.

**P0.2 — Replace the three `<pre>` blocks with semantic panels.**
Status bar (C1, C2, C28-lite), session list with humanised origins and relative times (C11), monitor list with trigger summaries and last-outcome glyph (C15). Delete `JSON.stringify` from the render path; keep a Tier-3 raw inspector (C29). Mobile-first single column, `auto-fit` grid for desktop, `light-dark()` semantics. *Done when:* nothing on the home screen is a JSON blob and the page is readable at 375 px wide.

**P0.3 — SSE stream and the end of polling.**
`GET /api/stream`; admin holds one persistent SDK client; forward `chat.message`, `chat.progress`, `monitor.event`, `gateway.stopping`. Snapshot inlined at load, deltas patch nodes by id. Full fallback ladder from §5, including honest stream-lost text and the 60 s reconciliation backstop. *Done when:* killing the gateway makes the status bar say `stream lost — showing data from HH:MM` within 5 s and nothing on the page keeps animating.

**P0.4 — Typed mutation console.**
Extend `MutationOperation` with `severity`, `fields`, `confirmToken`. Per-operation forms; `monitor-ref` picker from `monitor.list`. Three-state Compose → Consequence → Receipt flow per §4.2. `monitor.remove` requires the monitor **name**; `ops.backup` is one click. Audit tail rendered under the panel. `MutationGate.evaluate()` semantics unchanged. *Done when:* removing a monitor requires typing its name, the consequence panel shows its real trigger and 7-day event count, and the receipt shows the audit entry.

### P1 — trust in in-flight work (3 sessions)

**P1.1 — Live work panel** (C4) from `chat.progress` + `chat.message{final}`: determinate bar against 300 s, tool calls, output tokens, the 45 s stall rule, 10-minute persistence of finished rows. Ships live-only; backfill lands with **G1**.

**P1.2 — Attention queue v1** (C9 + the parts of C7 available from `gateway.status`): stuck monitor events via `monitor.list` + `monitor.inspect`, delivery pressure from `delivery.pending` / `oldestPendingAgeMs`. Severity ordering, `attention` SSE event computed server-side, plain-text empty state.

**P1.3 — Session and monitor detail screens** (C12, C16, C17, C18): stage ladders for monitor events, cron next-fire computed client-side, gated test-fire.

### P2 — the full owner surface (3 sessions, each gated on protocol work)

**P2.1 — Conversation projection** (C10, C13). Requires **G2**. The single highest-value addition once the verb exists: engagement decisions, silence, redelivery, and turn failures all become visible.

**P2.2 — Holds and delegated work** (C5, C6, §4.1, §4.3). Requires **G7** + **G8**.

**P2.3 — Ops screen** (C22–C25, C27, delivery and memory-intent tables). Requires **G3** + **G5** for the tables; the rest ships today.

---

## 8. Protocol gaps

Data the design needs that no current verb provides. Each names the storage that already exists, so none of these is a new subsystem.

| ID | Proposed verb / event | Params → result | Why it is needed | Backing storage today |
|---|---|---|---|---|
| **G1** | `turn.list` | `{}` → `{ turns: { turnId, origin, startedAt, elapsedMs, toolCalls, outputTokens, pendingInbound }[] }` | `chat.progress` is fire-and-forget to connected clients. A console that connects mid-turn, or reconnects after a drop, is blind to work in flight. This is the single largest gap: C4 is the "is it working right now" answer and today it is unanswerable on load. | in-memory turn state in `server.ts`; `inbound_messages` for queue depth |
| **G2** | `session.transcript` | `{ originKey, limit?, before? }` → `{ entries: { at, direction, authorName?, text, turnId?, deliveryState?, engaged, silenced, redelivered }[] }` | Blocks the entire conversation projection (C10, C13) — the owner's primary read. Must carry the four gateway-only facts: engagement decision, silence-token suppression, redelivery, turn failure. | `conversation_context`, `authored_outputs`, `deliveries` |
| **G3** | `delivery.list` | `{ state?, limit? }` → `{ deliveries: { deliveryId, turnId, origin, state, attempts, createdAt, updatedAt }[] }` | `gateway.status` gives only `pending` and `oldestPendingAgeMs`. A `failed_ambiguous` item means a human may have seen a duplicate — that is an attention item, and it is currently invisible. | `deliveries` table; `deliveryRows()` already implemented |
| **G4** | `session.inspect` | `{ originKey }` → `{ origin, labels: { channelLabel?, serverLabel? }, engagementMode, epoch, createdAt, lastActivityAt, pendingInbound, lastTurn: { at, ok, silenced } }` | `session.list` returns four scalars. The console cannot name a channel; it can only show `discord/channel/1493635653441945762`. **Note:** `channelLabel`/`serverLabel` arrive on `EngagementContext` but are only persisted inside `inbound_messages.engagement_json` — this gap needs a small storage change (denormalise the latest labels onto `sessions`), not only a verb. | `sessions`, `inbound_messages.engagement_json` |
| **G5** | `memory.intents` | `{ state?, limit? }` → `{ intents: { id, kind, state, createdAt, updatedAt }[] }` | `quarantined` is by design the state that means *a human must look* (`docs/memory.md` step 5). It is currently unobservable from any client. | `memory_intents`; `memoryIntentRows()` already implemented |
| **G6** | `monitor.update` | `{ monitorId, enabled?, burstPolicy?, channelTarget? }` → `{ monitor: MonitorRecord }` | The `enabled` column exists and `MonitorSpec.enabled` is in the type, but only `add` and `remove` are verbs. Pausing a noisy monitor requires delete-and-recreate, which loses `monitorId` and orphans its event history. Should also return a computed `nextFireAt` for cron triggers. | `monitors.enabled` |
| **G7** | `work.start` / `work.list` / `work.inspect` / `work.cancel` + `work.progress` event | `work.start {name, text, cwd?}` → `{workId, sessionKey}`; `work.list {}` → `{ work: { workId, name, state, startedAt, elapsedMs, toolCalls, outputTokens }[] }` | `work.run` blocks the caller for up to 300 s and returns only `{text, sessionKey}`. There is no id, no listing, no cancel, no progress. Delegated work is invisible to any observer that did not issue it — which is exactly the owner. §4.3 is unimplementable without this. | none — needs a `work_runs` table |
| **G8** | `subsession.list` / `subsession.resolve` | `list {}` → `{ items: { id, lane, opState, receiptState, heldSince, evidence, allow: { accept, respond, edit, ignore } }[] }`; `resolve { id, response: HumanResponse }` | `packages/subsession` computes `SupervisorOpState`, `requiresOperatorHold()`, and `judgeOperation()` entirely inside a library driven by the `gjc` CLI. Nothing crosses the gateway protocol. §4.1 — the highest-trust interaction in this whole design — is fully blocked. Adopt the Agent Inbox `HumanInterrupt`/`HumanResponse` shape (https://github.com/langchain-ai/agent-inbox). | none — needs subsession state to reach the gateway |
| **G9** | `audit.list` | `{ limit? }` → `{ entries: AuditEntry[] }` | The gate's `AuditSink` is optional and unwired; nothing can render "what did I approve, and what got rejected". P0.1 can implement this admin-locally (JSONL), but CLI-originated mutations would remain unaudited, so the durable answer belongs in the gateway. | none — needs an `audit` table or a gateway-owned JSONL |
| **G10** | event cursor: `id` on every event + `event.since {afterId}` | → `{ events: [...] }` | Unsettled **deliveries** are replayed to adapters on connect (24 h window, `docs/architecture.md`), but `chat.progress` and `monitor.event` are not replayable at all. §5 therefore reconciles by full snapshot on reconnect. If the gateway ever grows a bounded event cursor, the console can resume precisely instead. **Lowest priority** — the snapshot fallback is correct, just chattier. | none |

**Gaps ordered by value delivered per unit of work:** G2 → G1 → G3 → G7 → G8 → G5 → G4 → G6 → G9 → G10.

---

## 9. Verdict

The current console is not an under-featured console; it is a **debug endpoint with a stylesheet**. Three of its four read routes render `JSON.stringify` into `<pre>`, its fourth read route calls a method that does not exist, its confirmation control displays the answer next to the input, its audit sink is unwired in every production path, and nothing in the repository starts it. The owner's rejection is correct and the correct response is replacement, not iteration.

The design that replaces it is not exotic. It is Temporal's execution-first hierarchy, LangSmith's trace/thread/trajectory taxonomy, LangChain's Agent Inbox decision card, GitHub's object-name confirmation ceremony, Argo CD's consequence-before-confirm sync panel, and Google SRE's symptom-first, low-noise dashboard discipline — assembled inside one dependency-free HTML document served by Bun over a single SSE stream.

**Everything in P0 is buildable today against verbs that already exist.** The parts that require protocol work — the conversation projection, in-flight turn backfill, and operator holds — are precisely the parts that make the console worth trusting, and each maps onto storage the gateway already keeps. §8 names all ten, with their params, their results, and their backing tables.
