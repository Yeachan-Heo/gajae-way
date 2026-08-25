# Owner console UI and runtime cycle

This is the first vertical slice that makes the gajae-way daemon/adapter
lifecycle an explicit, testable contract. Durable authority is unchanged:
SQLite WAL, single-writer lock, exact-session adoption, fail-closed resume,
and server-owned journal/outbox checkpoints. The owner console still holds
no local cursor.

Grok-bot 0.18 is an architectural/UI reference only. Reused *patterns*:

- Project a UI phase from transport/readiness (`loading` / `connected` /
  `reconnecting` / `unreachable` / `fenced`) instead of inventing a second
  store.
- Keep send/admission ownership separate from post-accept acknowledgement.
- Treat restart as dropping uncommitted claims while replaying durable
  receipts (duplicate admission/commit is a no-op, not a second effect).

No grok-bot packaged assets, credentials, or proprietary renderer source
were copied.

## Operator UI model

`gajaeway console` remains the local owner surface. `/status` now renders
the existing daemon/main/journal/lock/reconcile/consumers block plus:

- `cycle:` phase, UI mode, whether owner input is allowed, identity verdict
- `identity:` expected profile pin vs adopted session id
- `settlement:` journal head, degradation, delivery gap, claimed/behind
  consumers, invariant flag
- `gates:` actionable operator work, `blocking:` or `advisory:`

The cockpit rail prefixes `GATEWAY COCKPIT` with a compact `cycle=<phase>`
badge. The full cycle, identity, settlement, and gates block remains in
`/status` so the transcript is not crowded.

### UI modes

| Mode | Meaning |
| --- | --- |
| `loading` | Strict resume/verification, or delivery not yet claimed |
| `connected` | Healthy daemon; owner input allowed |
| `fenced` | Failed-closed, stale identity, degraded, or shutting down |
| `unreachable` | Owner socket did not answer |
| `reconnecting` | Local journal consumer lost delivery after a connected session |

## Runtime cycle phases

Projection source: `way.health` + `way.status` + optional profile pin and
local delivery observation. Highest-priority exclusive phase:

```
unavailable
  -> bootstrap_required | verifying
  -> failed_closed | identity_stale | shutting_down | degraded
  -> quarantined
  -> settling (journal degraded, delivery gap, or consumer ahead of head)
  -> gating | turning | delivering
  -> idle
```

`bootstrap_required` is advisory. A healthy running daemon with no published
main-session id stays interactive, matching the existing console fence
(health/failed-closed only) plus the new stale-identity fence.

## Turn / settlement machine

Executable in `src/runtime-cycle.ts` as `applyTurnCycleEvent`:

```
idle -> ingress -> admission_claimed -> admission_accepted
    -> ack_sent (optional; only after accept)
    -> turning -> journaled -> outbox_claimed -> delivered
```

Invariants:

- Acknowledgement is illegal before durable `accept`.
- Duplicate `claim` / `accept` / `commit` with the same key is a replay.
- Restart drops uncommitted outbox claims and undelivered ingress; a durable
  admission claim or accepted receipt is preserved.
- A consumer cursor ahead of the journal head is `consumer_ahead_of_journal`
  and fences owner input when observed on `way.status`.

This is the cadence the adapter already implements: `main.submit` acceptance,
then ack/typing, then journal projection, then `consumer.commit`.

## Failure and recovery

| Condition | Operator affordance |
| --- | --- |
| Daemon unreachable | `start_daemon` |
| `profile_drift` fail-closed | `approve_profile` |
| Other fail-closed | `recover_fail_closed` (never systemd restart as repair) |
| Adopted id ≠ profile pin | `re_adopt_session` |
| Lock quarantined | `clear_quarantine` after Git verification |
| Journal gap / cursor ahead | `repair_journal_gap` |
| Stale reconcile | `wait_reconcile` |
| Open SDK gate | `answer_gate_in_gjc` in the attached gjc TUI |

Shutdown remains `host_disposed` / local delivery `stopping`. The console
does not mutate daemon state on exit; outstanding work stays at the
`gajaeway-console` checkpoint.

## Source contracts

- `src/runtime-cycle.ts` — projection + turn machine
- `src/console/console.ts` — `/status` and startup fence
- `src/console/tui/renderer.ts` — cockpit rail
- `test/unit/runtime-cycle.test.ts` — happy path, duplicate/restart,
  unavailable daemon, stale identity, delivery/ack settlement

Unresolved (not guessed here): whether a future backend should accept
validated gate receipts through `/gate`, and whether `way.status` should
publish open-gate counts so `gating` can be projected without a journal
tail. Today `gating` requires an explicit `openGateCount` observation.
