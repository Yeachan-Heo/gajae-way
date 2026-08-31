# Erratum: two withdrawn conclusions about the `gajaeway gjc` entrypath

**Status: one divergence withdrawn (AC-11). One interpretation change OPEN and unratified (AC-8).**

This document began as a record of two places where the implementation diverged from the SHA-pinned interview spec. Both conclusions turned out to be wrong, and both requirements are now met. It is kept, rather than deleted, because both were published and acted on — including one that briefly shipped a refusal for a flag the user had required. The reasoning error was the same each time: concluding *"this is impossible"* from *"the first approach I tried is unsafe"*.

| Item | First conclusion | Actual outcome |
|---|---|---|
| `fact-session-dir-layout` / AC-11 | epoch directory cannot hold native sessions | **met as written** — the gateway pins an epoch-scoped native state root |
| option parity / AC-8 (`--worktree`) | cannot be supported at all | **behavior delivered, but the literal observable differs** — see section 2; needs ratification |

| Field | Value |
|---|---|
| Spec artifact | `.gjc/_session-01a051d2-e660-76f9-9a04-945068b1fc60/specs/deep-interview-gajaeway-gjc-entrypath.md` |
| sha256 | `22cc52329812f9d7dc60636198bcc26473f8ce33cba3530eab28a431c6232a4f` |
| Companion erratum | `artifacts/gjc-entrypath-arch007-erratum.md` (ARCH-007 wording — that one IS a real supersession, authorized by user-confirmed reconciliation R1) |
| Approving authority needed | **None for AC-11.** AC-8 needs a user decision — see section 2. |

## 1. Session storage — WITHDRAWN: the epoch directory is the session store

**First conclusion (wrong):** that AC-11 could not be met, because the gateway binds sessions through `gjc sdk session raw global --op session.create`, which rejects `--session-dir`:

```
Unknown option '--session-dir'.
```

That rejection is real, and injecting the epoch directory as `--session-dir` did break every launch:

```
[Uncaught Exception] Error: Session "c70a0f1d-fcae-4b7e-aae7-1e3f3f70597f" not found.
```

**What was missed:** `--session-dir` is not the only way to place native session state. The store follows the native state root, which is environment-selected — something already proven by a separate experiment in which a daemon and CLI given different `GJC_CODING_AGENT_DIR` values could not see each other's session.

**Governing behavior:** the gateway pins an **epoch-scoped native state root** at `$GAJAEWAY_HOME/sessions/terminal/e<epoch>/agent`, for both the `session.create` it issues and the interactive child it hands back. Native gjc then writes its transcript under that root, inside the epoch directory. Measured live:

```
sessions/terminal/e0/agent/sessions/v2-<scope>/…_02fc31f1-f670-4eb0-a931-427f16c76a35.jsonl
sessions/terminal/e1/agent/sessions/v2-<scope>/…_5f02cb5e-aadd-4094-bdf1-eab3015c3cdc.jsonl
```

`e0` holds the session that two sequential invocations both resumed; `e1` holds the one bound after `--new`; both epoch directories and their contents survive rotation. **AC-11 and `fact-session-dir-layout` are satisfied.** `--session-dir` remains refused for operators, since redirecting the store would unbind the managed session.

This also removed an earlier defect structurally rather than by correction: because the gateway now *dictates* the state root to both sides, a daemon and CLI can no longer resolve different stores at all. The value/unset pair returned as `childEnv` / `childEnvUnset` keeps that override total, so a selector the operator's shell exports cannot survive it.

## 2. `--worktree` — WITHDRAWN: the flag is supported

**This section previously claimed AC-8 could not be satisfied. That claim is withdrawn.** It is left here rather than deleted because it was published, and because the reasoning error is worth recording: I concluded "no implementable path" after establishing only that *forwarding* the flag was unsafe, without testing whether the gateway could own the worktree itself.

**What was true:** forwarding `--worktree` is genuinely unsafe. Native gjc enters a worktree it names itself (observed `hazard-branch-f6ddf077`) before resolving the session, so the gateway-bound session reads as a different project and the TUI offers `Fork into current directory? [y/N]`, which would split the persona's managed session. A control run resuming the same session *without* the flag shows no prompt, so the hazard is specific to the flag. Evidence: `artifacts/gjc-entrypath-worktree-hazard-log.txt`.

**What was false:** that no alternative existed. `--worktree`/`-w` are now **wrapper-owned**. The gateway prepares the worktree itself with plain `git worktree add` at `<workspace>/.worktrees/<branch>` — standard git, not a reimplementation of gjc's worktree machinery — binds the session with that directory as its cwd, and spawns the child there with no worktree flag. Measured live: two sequential `gajaeway gjc --worktree probe-branch` invocations both resumed native session `baaa782d-d826-47ce-885e-258c2cb97f71` with no fork prompt (`artifacts/gjc-entrypath-scope-probe-log.txt`).

**Consequence for the contract — and this is an OPEN item, not a closed one.** The user-facing behavior is delivered: `gajaeway gjc --worktree <branch>` puts the operator in a managed worktree on that branch with their persona session bound there. But AC-8's literal observable is that `--worktree` appears **unchanged in the child argv**, and it deliberately does not: the gateway consumes it as wrapper-owned, and a regression test asserts its absence. Calling that "AC-8 satisfied" would be reading the criterion by intent while it is written by observable.

So this is an **interpretation change requiring ratification**, not a met criterion.

Five routes to the literal observable were measured and closed; a sixth is unmeasured and is the honest reason this needs a decision rather than more work. See `artifacts/gjc-entrypath-worktree-hazard-log.txt` sections A-G:

| route | outcome |
|---|---|
| A pin the store via `--session-dir` on the create path | `Unknown option` |
| B forward the flag from the persona workspace | different-project **fork prompt** |
| C same session, no flag (control) | clean resume |
| D forward the flag from the gateway-prepared worktree | **`branch_in_use` crash** |
| E pre-bind in the worktree gjc will choose | needs replicating gjc's generated suffix |
| F let gjc create worktree + session, adopt the id | blocked at first-run onboarding, no session created |
| G seed onboarding, then retry F | **unmeasured** — see below |
| H ask gjc for the worktree path | no such command exists in 0.15.5 |
| I gjc creates the worktree, gateway still binds inside it, flag retained | **promising but incomplete** — no `branch_in_use`, no fork prompt, but the launch stalled in first-run onboarding |

Route G is **not shown to be impossible**, and this document does not claim it is. What is known: it needs gjc's onboarding state seeded into every fresh epoch root, which is vendor-internal knowledge of exactly the kind ARCH-007 exists to keep out of this codebase; and it needs a two-phase attach that spawns without `--resume` and then adopts whatever session gjc creates, which inverts the locked invariant that `GjcClient` is the sole binder and that `session.attach` returns an already-bound session. Both attempts to measure it were blocked by an interactive wizard I could not reliably drive, and that is recorded as a failure to measure rather than a result.

Route I deserves the most attention of the unfinished ones, because it avoids both objections that closed E and G: gjc creates the worktree so nothing is replicated, and the gateway still binds the session itself so the sole-binder invariant holds. Resuming with the flag retained produced neither of the failure modes that closed B and D. It stalled only on first-run onboarding, and it would depend on a throwaway non-interactive warm-up invocation per worktree.

The options are therefore:

1. ratify the interpretation — the criterion is about the operator getting a worktree, which is delivered and verified;
2. fund route I — finish characterising the onboarding prerequisite and decide whether a per-worktree warm-up invocation is acceptable;
3. authorise route G, accepting a change to the binder invariant and a dependency on gjc's onboarding internals;
4. reject worktree support entirely;
5. reword AC-8 to state the wrapper-owned mechanism explicitly.

Until one is chosen, no artifact in this change set should be read as claiming AC-8 is met as written, and none does.

**One derived rule, which is new behavior rather than a spec divergence:** a native session belongs to the project it was created in, so an epoch's cwd is fixed for that epoch's lifetime. The gateway records it as `bound-cwd` in the epoch's artifact directory and refuses an attach that would move the session, naming the remedy (`pass the matching --worktree or use --new`). Refusing with an actionable message is the only honest option: silently rebinding would lose the conversation, and forwarding would surface an interactive fork prompt the operator cannot answer safely.

## Archived evidence

| Observation | Log |
|---|---|
| `gjc sdk session raw ... session.create` rejects `--session-dir`; resuming a gateway-bound session with `--worktree` offers to fork it (worktree `hazard-branch-f6ddf077`); and the same session resumes cleanly WITHOUT `--worktree` | `artifacts/gjc-entrypath-worktree-hazard-log.txt`, produced by `artifacts/gjc-entrypath-worktree-hazard-probe.sh` |
| Shipped refusal lands before any bind (`gateway row: none`); control and env-split same-session resume | `artifacts/gjc-entrypath-scope-probe-log.txt` |
| Default-path AC-17 drill | `artifacts/gjc-entrypath-drill-log.txt` |

## Scope

AC-11 and `fact-session-dir-layout` are met as written; nothing about them is amended. AC-8 remains **open**: its intent is delivered and its literal observable is not, which is a decision for the user rather than a claim this document can settle. Every other fact, non-goal, and acceptance criterion in the pinned spec stands, including the accepted non-goal on persona git-identity isolation.
