# `gajaeway gjc` live drill receipt (AC-17)

**Date:** 2026-08-31T05:50:42Z
**Result:** PASS — two sequential invocations resumed the same native gjc session; `--new` rotated the epoch while the previous epoch directory and its contents survived.

This is the only acceptance criterion whose *subject* is real native gjc. Most others (AC-1..AC-8, AC-10..AC-16, AC-18, AC-20, AC-21) run on the `GAJAEWAY_TEST_STUB_GJC` process seam. Two do not need it and were also confirmed against a real daemon: **AC-9** (daemon down: the CLI names the resolved socket path and never opens SQLite) and **AC-19** (the TTY gate refuses a piped stdin before connecting) — both were exercised with the stub env unset while establishing that a PTY was required for this drill at all.

## Environment

| Item | Value |
|---|---|
| `$GAJAEWAY_HOME` | `/tmp/gjc-live-drill-home` (throwaway; not the operator's home) |
| gjc | `gjc/0.15.5` |
| Daemon | `bun packages/gateway/src/main.ts daemon`, launched with `GAJAEWAY_TEST_STUB_GJC` **unset** |
| Socket | `/tmp/gjc-live-drill-home/gateway.sock` |
| Persona | `workspace/{SOUL,AGENTS,USER}.md` written into the drill home |
| TTY | PTY allocated with `script -q /dev/null`, because the entrypath refuses a non-TTY stdin (AC-19) |

No secrets are recorded here. The drill home was created fresh and the daemon was stopped at the end; the operator's own gateway (a separate long-running process) was never touched.

## Commands

```sh
# daemon, real runtime, no stub seam
env -u GAJAEWAY_TEST_STUB_GJC GAJAEWAY_HOME=$H bun packages/gateway/src/main.ts daemon

# each invocation, under a real PTY
env -u GAJAEWAY_TEST_STUB_GJC GAJAEWAY_HOME=$H \
  script -q /dev/null bun packages/cli/src/main.ts --socket $H/gateway.sock gjc [--new]

# identity read back from the gateway's own durable row
SELECT gjc_session_id, epoch FROM sessions WHERE origin_key = 'loopback/loopback/terminal';
```

## Turn 1 — first attach binds the terminal origin

1. `gajaeway gjc` under a PTY: exit code **0**. The native TUI started and, on exit, printed its own resume hint:
   `Resume this session with: gjc --resume 02fc31f1-f670-4eb0-a931-427f16c76a35`
2. Gateway row: `02fc31f1-f670-4eb0-a931-427f16c76a35 @e0`.
3. Gateway created `$GAJAEWAY_HOME/sessions/terminal/e0/` and pinned the native state root to `e0/agent`, so gjc wrote its transcript inside that epoch directory. (The `0700` mode is asserted by the integration test, not captured in this log.)
4. A marker file `e0/drill-marker.txt` was written by the drill so rotation could be proved later.

## Turn 2 — second attach resumes the SAME native session

1. `gajaeway gjc` again: exit code **0**.
2. The TUI printed the identical resume hint: `gjc --resume 02fc31f1-f670-4eb0-a931-427f16c76a35`.
3. Gateway row unchanged: `02fc31f1-f670-4eb0-a931-427f16c76a35 @e0`.

**This is the load-bearing assertion.** The session id is a real native id minted by `session.create`, not a formula, so matching it across two separate processes proves the managed binding persisted and the TUI genuinely resumed rather than starting fresh.

## Turn 3 — `--new` rotates the epoch and preserves history

1. `gajaeway gjc --new`: exit code **0**.
2. The TUI printed a **different** resume hint: `gjc --resume 5f02cb5e-aadd-4094-bdf1-eab3015c3cdc`.
3. Gateway row: `5f02cb5e-aadd-4094-bdf1-eab3015c3cdc @e1` — epoch incremented, new session bound.
4. Epoch directories after rotation: `sessions/terminal/e0` **and** `sessions/terminal/e1`.
5. `e0/drill-marker.txt` still present, contents intact (`drill-marker`); both `e0` and `e1` are listed after rotation. The log shows retention of that directory and file — it is not a byte-level audit of everything under `e0`.

## Gateway view

`gajaeway ops cycle` after the drill:

```
phase: idle
gates: none
sessions:
INDEX  ORIGIN                                      EPOCH  SESSION      PENDING  UNSETTLED  OLDEST
0      loopback/loopback/terminal                  1      5f02cb5e-aa  0        0          -
```

Identifiers only — origin key, epoch, bound session id. No lease holder, child pid, PTY state, or delivery health, as required.

## Epoch-scoped session storage (AC-11), as observed

The native transcripts land inside the epoch directories, which is what makes the epoch boundary real rather than bookkeeping:

```
sessions/terminal/e0/agent/sessions/v2-<scope>/…_02fc31f1-f670-4eb0-a931-427f16c76a35.jsonl
sessions/terminal/e1/agent/sessions/v2-<scope>/…_5f02cb5e-aadd-4094-bdf1-eab3015c3cdc.jsonl
```

`e0` holds the session both sequential invocations resumed; `e1` holds the one bound after `--new`; `e0` and its marker survived rotation. Session ids differ from the earlier run in this receipt because the drill was re-run after the storage mechanism was corrected; the invariants asserted are unchanged.

## Defects this drill caught

The first drill run **failed**, and the failure was a real product defect that no stub-seam test could have surfaced:

```
[Uncaught Exception] Error: Session "c70a0f1d-fcae-4b7e-aae7-1e3f3f70597f" not found.
```

The gateway created the session through `gjc sdk session raw global --op session.create`, which **rejects `--session-dir`**, so the session always lands in gjc's default managed scope. The binder was nevertheless injecting the epoch directory as `--session-dir`, pointing the TUI at a store the session had never been written to. Every invocation started and then died.

Notably the gateway half was already provably correct in that failing run: invocations 1 and 2 both resolved to the same id `c70a0f1d…`, and `--new` moved to `7eecd606…` at epoch 1. Only the child's view of the store was wrong.

**Fix:** the binder no longer injects `--session-dir`. Instead the gateway pins the **native state root** to `<epoch dir>/agent` for both the create it issues and the child it spawns, which puts the transcript inside the epoch directory and satisfies AC-11 as written. `--session-dir` remains refused for operators, since redirecting the store would unbind the managed session.

My first fix for this was weaker: I removed the injection and let both sides fall back to gjc's default scope, then recorded AC-11 as unsatisfiable. That was wrong — the store follows an environment-selected root, so the gateway can place it deliberately.

## Scope and evidence limits

Stated plainly so the receipt is not read as more than it is:

- Each TUI is stopped with `SIGTERM` after it has started and bound. Every invocation exited **0** and printed its own resume hint, which is gjc's normal shutdown output. The `SIGTERM` itself is not serialized in the raw log.
- The captured blocks are output **tails**, not complete transcripts.
- Session ids are read from the TUI's printed resume hint and from the gateway's `sessions` row; both are shown in the log.
- No interactive turn was driven. AC-17 asks for session identity across sequential invocations and epoch-directory retention, which is what is demonstrated.
- The drill covers the **default** invocation. `--worktree` and the environment split are covered separately below, with their own archived logs.

## Two further defects found by probing beyond the drill

Both were reproduced, then fixed, then re-verified live.

### `--worktree` — forwarding it would fork the managed session; the gateway owns it instead

With a correct fixture (a git repo whose `.gitignore` excludes `/.worktrees`), resuming a gateway-bound session with the flag forwarded reached:

```
GJC warming workspace
> Session found in different project: /private/tmp/.../workspace. Fork into current directory? [y/N]
```

Native gjc enters a worktree it names itself (observed `hazard-branch-f6ddf077`) before resolving the session, so the bound session reads as a *different project*. A control resuming that same session **without** the flag shows no prompt, which pins the hazard to the flag rather than the fixture. Evidence: `artifacts/gjc-entrypath-worktree-hazard-log.txt`.

My first conclusion — that this made `--worktree` unsupportable — was **wrong**, and worth recording as an error: I had only established that *forwarding* was unsafe, not that the gateway could not own the worktree itself. It can. `--worktree`/`-w` are now wrapper-owned: the gateway prepares `<workspace>/.worktrees/<branch>` with plain `git worktree add`, binds the session with that directory as its cwd, and spawns the child there with no worktree flag.

Verified live — two sequential `gajaeway gjc --worktree probe-branch` invocations both resumed native session `baaa782d-d826-47ce-885e-258c2cb97f71`, with the gateway-created worktree `probe-branch` (no generated suffix) and no fork prompt.

**AC-8 is delivered in behavior but OPEN as written.** The criterion's literal observable is that `--worktree` reaches the child argv unchanged; here it is deliberately consumed by the gateway. Forwarding it is what produces the fork prompt above, so the literal reading cannot be implemented safely. Whether the wrapper-owned mechanism satisfies AC-8 is a ratification question for the operator, recorded as a durable blocker and in `artifacts/gjc-entrypath-session-store-erratum.md` section 2. Nothing here claims the criterion is met as written.

Because a native session belongs to the project it was created in, an epoch's cwd is fixed for that epoch. The gateway records it as `bound-cwd` in the epoch's artifact directory and refuses an attach that would move the session, naming the remedy: `pass the matching --worktree or use --new`.

### Daemon and CLI could resolve different session stores — now pinned

A daemon and CLI given deliberately different `GJC_CODING_AGENT_DIR` values produced, on every invocation:

```
Error: Session "<id>" not found.
```

`session.create` runs in the daemon's environment while the TUI runs in the operator's, so the two could resolve different native state roots — a realistic split when the daemon runs under a service manager. The gateway now returns the state-root variables it resolved as `SessionAttachResult.childEnv` and the CLI applies them to the child.

The fix transmits the daemon's native-state decision **totally**: `childEnv` for the selectors it has, `childEnvUnset` for those it does not, applied by the CLI as a replacement rather than a merge. A merge was not sufficient — a selector the operator's shell exports but the daemon lacks would have survived and won, which is the ordinary production split (service-managed daemon, operator shell).

Re-verified with the mismatch still deliberately in place. Archived run: `artifacts/gjc-entrypath-scope-probe-log.txt`, produced by `artifacts/gjc-entrypath-scope-probe.sh`.

| Case | Observed |
|---|---|
| control (matching env) | both invocations resumed `e9735b9d-b085-4603-aed4-f63ec321b646` |
| envsplit (daemon `/tmp/gjc-scope-agentA`, CLI `/tmp/gjc-scope-agentB`) | both invocations resumed `994f60b6-213c-4d5d-a66d-5a1f0b8b2e72`, no `Session not found` |
| worktree (`--worktree probe-branch`) | both invocations resumed `baaa782d-d826-47ce-885e-258c2cb97f71` in the gateway-created worktree `probe-branch`, no fork prompt |

Session ids differ between runs of the probe because each run creates a fresh throwaway home. The invariant under test is that the two invocations *within* a run agree.

## Related records

| Artifact | Purpose |
|---|---|
| `artifacts/gjc-entrypath-drill-log.txt` | raw output of the default-path drill above |
| `artifacts/gjc-entrypath-scope-probe.sh` / `-log.txt` | control, env-split, and worktree-refusal probes |
| `artifacts/gjc-entrypath-session-store-erratum.md` | supersedes `fact-session-dir-layout`, narrows AC-11, and records the `--worktree` escalation |
| `artifacts/gjc-entrypath-arch007-erratum.md` | supersedes the ARCH-007 wording |
| `artifacts/gjc-entrypath-qa-report.json` | adversarial QA scenarios and statuses |
