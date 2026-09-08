# Handoff 2026-09-08 — gateway-owned worker lanes: state, decision, next spec

Self-contained. Assumes no prior conversation context. Everything referenced is on `origin/main` or on the hosts named below.

## 1. What landed (done, deployed)

`main` `fc9fcdc` (merge of `feat/broker-cursor-limit`, commits `1cc73d1` feature + `3de9aea` close-route fix). Deployed to **gaebal-gajae** (`~/Workspace/gajae-way`, `bun run build`, `systemctl --user restart gajaeway-gateway`) on 2026-09-07 ~21:15 KST. `origin/main` has since moved (`e4d38cf` context-ceiling rotation, unrelated).

Gateway-owned worker lanes ("LaneGovernor"):

| Surface | Behavior |
|---|---|
| `config.work.maxLanes` (1..256, default 8), `config.work.idleRetireMs` (60s..7d, default 6h) | restart-required |
| `work.run {name,text,cwd?,resume?,model?}` | `model` = model id string or `{preset}`; applied at `session.create` (`modelPreset`) and re-applied on each send. Admission + bind run under a shared `runExclusive("work/admission")` lock inside the per-name lock. New name past cap → error `lane_capacity` with detail `{maxLanes, active, candidates[{name, idleMs(-1=unknown), state}]}` idlest first. Bound name always admitted. `last_activity_at` refreshed at attempt END (success and failure). |
| `work.retire {name}` → `{retired:true, sessionKey, sessionId, closed}` \| `{retired:false, sessionKey, reason}` | `session.close` via **`raw global`** (per-session `raw control` is `adapter_operation_prohibited` on gjc 0.16.3) then `database.rebindEpoch(originKey)`. Fail-closed: refuses on open attempt, corrupt/empty `lane_jobs.record_json`, last attempt ended `attempt_ended|terminal_uncertain|terminal_missing_receipt|failed` until broker `status` is `terminal_ok|failed` or (`unknown` and session dead/disowned), and on close failure unless `liveness` proves `live===false||disowned`. Never `session.delete`. |
| sweep (60s reconcile timer) | nominates lanes idle ≥ `idleRetireMs` or job `done|aborted`; `retire(name, reason, {sessionId, reason, now})` re-proves session id + predicate under the lane lock. |
| `work.jobs` rows | + `session_id`, `last_activity_at` |
| `ops.cycle` | + `lanes {active,max}`; gate `lane_capacity_exhausted` when `active >= max`; unbound `work/task/*` row is exempt from `stale_session_identity` only when a lane job in `attempt_ended|done|aborted` vouches for it (no job = failed first bind, unsettled job = crash-left → gated). |
| ActionGuard notice | forbids direct `gjc` launches from turns; routes to `work.run`/`work.retire`. Prompt instruction only, not command enforcement. |
| CLI | `gajaeway work run <name> [--cwd DIR] [--resume] [--model ID\|--preset NAME] "<text>"`, `work retire <name>`, `work jobs` (prints canonical name, i.e. `lane_key` minus `work-`). |

Files: `packages/gateway/src/orchestrator/lane-governor.ts` (new), `server.ts` (`work.run`/`work.retire`/`work.jobs`, `parseWorkModel`, timer), `ops/cycle.ts`, `config.ts` (`parseWork`), `orchestrator/session-port.ts` (`close`), `store/db.ts` (`workLaneRows`), `packages/protocol/src/{catalog,errors,index}.ts`, `packages/cli/src/main.ts`, `docs/architecture.md` "Worker lanes", `docs/runbooks/gajaeway-v1.md` triage entry. Tests: `packages/gateway/test/{lane-governor,lane-governor-redteam,cycle,server,session-port,config-reload,action-guard-floors}.test.ts`, `packages/cli/test/main.test.ts`. Review evidence: `artifacts/lane-governor-redteam-report.json`, `artifacts/lane-governor-final.junit.xml`.

Live verification on gaebal-gajae (2026-09-07 21:08–21:30 KST): `work run lane-smoke --preset muse-gpt` → `PONG` 19s; `work retire` → `closed=true`, `active` 2→0; with temporary `work:{maxLanes:2,idleRetireMs:60000}`: third name refused `lane_capacity`, existing name admitted, `ops cycle` gated `lane_capacity_exhausted`, sweep auto-retired both after ~130s. Config restored to defaults afterwards.

## 2. The architectural fact that decides the next step

Two brokers, fully isolated (verified 2026-09-07 21:23 KST on gaebal-gajae):

```
~/.gjc/agent                         ws://127.0.0.1:42669 pid 2397209   ← interactive gjc; persona's tmux lanes live here
~/.gajaeway/broker/b1942e24-…/agent  ws://127.0.0.1:46429 pid 2497566   ← gateway-private; persona/monitor/work lanes
```

Consequences:
- The 24 `gjc --mpreset` tmux processes (~24 GB RSS) on gaebal are **not** in the gateway broker. They cannot break it (the 2026-09-06 cursor exhaustion and 2026-09-07 `terminal_uncertain` fence were both inside the gateway broker, caused by persona epoch rotation, not by tmux lanes). The gateway also cannot count, cap, close, or see them.
- The lane governor only governs lanes created through `work.run`. On gaebal the persona currently creates lanes via `~/clawd/skills/gjc-session/{create,lane}.sh` (tmux + raw `gjc`), so the governor is idle there until that skill is replaced.
- The persona cannot replace that skill with `work.run` as-is, because `work.run` is **synchronous**: it blocks the caller until the worker turn ends (CLI waits up to 1h). A persona turn that starts a multi-day PR lane would hang on it. The tmux + `watch-turn.ts`/clawhip pattern exists precisely to get fire-and-forget + completion notification.

Keep the two brokers separate. Do not add code that touches `~/.gjc/agent` or kills foreign processes from the gateway.

Known gjc-side blocker, unrelated to this work: session-index GC deletes stay off (`PersonaSessionManager` `gcDeletes=false`) until gjc scopes `hasUncertainCleanupForSession` to the session it names (one refused `session.delete` currently fences every later `session.create`). Persona epoch rotation keeps growing the gateway broker index; only `session.close`+rotation frequency bounds it today.

## 3. Next spec: asynchronous worker lanes (`work.start` / `work.status` / `work.steer` + completion delivery)

Goal: make `work.run` usable from a persona turn so `~/clawd/skills/gjc-session` can be replaced entirely, putting every persona-spawned session under the governor.

### Verbs (protocol `packages/protocol/src/catalog.ts`, register in `VERBS_V01`)

- `work.start {name, text, cwd?, model?, resume?, notify?: OriginRef}` → `{ started: true, jobId, opRef, sessionKey, sessionId }` | `{ started: false, held: true, jobId, state, reason }`.
  Same validation as `work.run` (`name` regex, absolute `cwd`, `parseWorkModel`). Admission + bind under the existing `work/admission` lock. Appends the attempt (`appendAttempt`) and issues `sessionPort.send` (not `request`), returning as soon as the send receipt is accepted. Refuses with `lane_capacity` exactly like `work.run`. A name with an open attempt → error `invalid_params` "attempt already open; use work.steer or wait".
- `work.status {name}` → `{ jobId, state, sessionId, lastActivityAt, attempt: {opRef, startedAt, endedAt?, endState?} | null, op: PromptStatusBody | null }`. `op` is `sessionPort.status(...)` for the last attempt when the session is bound; pure read, no mutation.
- `work.steer {name, text}` → `{ steered: true, clientRef }`. Requires an open attempt; uses `sessionPort.steer`. Refusal from the session (`steerRefused`) → `{ steered: false, reason }`, never a rebind.
- `work.run` stays as the synchronous form (it is `work.start` + wait); factor the shared admission/bind/attempt code out of `server.ts` `work.run` into `lane-governor.ts` or a new `orchestrator/work-lane.ts` so the two verbs cannot drift.

### Completion observation and delivery

- A background observer per open attempt (reuse `TailRunner`/`attachTail` + `status` polling the way `PersonaSessionManager` does for persona turns; do not spawn a new mechanism). On terminal: `closeAttempt` (`completed`/`failed`/`attempt_ended` exactly as `work.run` does today, including `collectRepoFacts` reconciliation), refresh `last_activity_at`, persist.
- If `notify` was given: create a ledger delivery (`DeliveryService`, same path monitors use in `packages/gateway/src/monitors/propagate.ts` → `chat.message`) to that origin with a bounded body: `[lane <name>] <completed|failed|attempt_ended>: <first 1–2 KiB of assistant text or error code>`. Silence tokens apply. Default `notify` = `config.ownerTarget.origin` when set; otherwise no delivery, status only.
- Gateway restart: open attempts survive in `lane_jobs`; on boot, re-attach the observer for every open attempt whose session is still live (`liveness`), else close as `terminal_uncertain` (existing hold semantics). This replaces the `awaiting_operator` dance for the common case.

### Governor interaction

- `work.start` counts as activity; `attemptOpen=true` blocks retire/sweep exactly as now.
- After terminal, the lane is an ordinary idle lane: sweep retires it after `idleRetireMs`, or the persona calls `work.retire`.
- `lane_capacity` detail is already sufficient for the persona to pick a retire candidate.

### CLI

`gajaeway work start …` (same flags as `run` plus `--notify <originKey>`), `work status <name>`, `work steer <name> "<text>"`. Keep `work run`.

### Tests (acceptance)

- Socket: `work.start` returns before the scripted `onSend` completes; `work.status` shows `in_flight` then `terminal_ok`; completion delivery row appears for `notify`; `work.steer` reaches `port.steers`; cap and open-attempt refusals; restart re-attach (boot with an open attempt + live session → observer closes it when it completes; dead session → `terminal_uncertain`).
- Governor: open attempt from `work.start` blocks sweep/retire; after terminal, sweep retires at threshold.
- Existing suites stay green (`bun test packages`, ~1630 tests; `bunx tsc --noEmit -p tsconfig.json`).

### Then, on gaebal-gajae (host SOUL, not repo code)

1. Replace `~/clawd/skills/gjc-session/create.sh`/`lane.sh`/`prompt.sh`/`watch-turn.ts`/`sdk-control.ts` with `gajaeway work start --cwd <worktree> --preset <p> --notify <channel origin> "<prompt>"`, `work status`, `work steer`, `work retire`. Delete the "maximum parallelism, no numeric cap" doctrine in `SKILL.md`.
2. Let existing tmux lanes finish; `tmux kill-session` the 15 empty shells and the MERGED `gc-pr-5366-lane`; kill the `/tmp/gjc-*-test`/`gjc-supervisor-*` leftovers (1.5 days old on 2026-09-07).
3. Set `work.maxLanes` deliberately (start 8) and restart the gateway.

### Out of scope / later

- Command-level enforcement of "no raw gjc from a turn" needs a gjc bash-exec hook (ActionGuard P4 note in `guard/action-guard.ts`). Until then: doctrine + visibility. Optional: an `ops.cycle` observation-only gate counting foreign `gjc` processes/RSS on the host (measure, never kill).
- Turning `gcDeletes` on: blocked on gjc scoping its cleanup fence.

## 4. How to reproduce the current state without this document's author

```sh
git clone https://github.com/Yeachan-Heo/gajae-way && cd gajae-way && git checkout fc9fcdc   # or main
bun install && bunx tsc --noEmit -p tsconfig.json && bun test packages/gateway/test/lane-governor*.test.ts
# read: docs/architecture.md "Worker lanes"; docs/runbooks/gajaeway-v1.md "lane_capacity"; packages/gateway/src/orchestrator/lane-governor.ts
# live: ssh gaebal-gajae; cd ~/Workspace/gajae-way; ./dist/gajaeway ops cycle --json | jq .lanes; ./dist/gajaeway work jobs
```
