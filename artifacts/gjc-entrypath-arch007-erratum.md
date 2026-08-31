# Erratum: ARCH-007 wording in the `gajaeway gjc` entrypath spec

**Status:** immutable erratum. This document expressly supersedes one established fact in a SHA-pinned interview record. It does not modify that record.

## Superseded source

| Field | Value |
|---|---|
| Artifact | `.gjc/_session-01a051d2-e660-76f9-9a04-945068b1fc60/specs/deep-interview-gajaeway-gjc-entrypath.md` |
| sha256 | `22cc52329812f9d7dc60636198bcc26473f8ce33cba3530eab28a431c6232a4f` |
| Superseded fact id | `fact-arch007-preserved` |
| Authority for this change | Post-consensus intent reconciliation **R1**, user-confirmed, recorded in `.gjc/_session-01a051d2-e660-76f9-9a04-945068b1fc60/plans/ralplan/01a051d2-e660-76f9-9a04-945068b1fc60/stage-06-post-interview.md` (sha256 `fe764e12bf860fa534a2d5aae200923d2546d79c89b692f86be04b05e9eb49ee`) |

The interview spec and the ralplan final receipt both pin the spec's digest as a binding input, so the record is deliberately left byte-identical. Rewriting it would invalidate that provenance chain.

## What the pinned fact says (now superseded)

> `fact-arch007-preserved` — GjcClient remains the only module that builds/spawns gjc argv, so GAJAEWAY_TEST_STUB_GJC keeps covering flag forwarding; the CLI never becomes a second vendor adapter.

The "and spawns" half of that sentence is no longer accurate.

## The governing formulation

> **`GjcClient` is the sole assembler of gjc argv and the sole binder of managed sessions. The spawn site may be more than one process.** The `gajaeway gjc` CLI spawns an interactive TTY child in the operator's terminal because a child spawned inside the daemon would inherit daemon stdio and could never be a TUI; that spawn is an **opaque consumer** of the preassembled `SessionAttachResult.argv`. Flag classification lives only in `classifyGjcWrapperFlags`, argv assembly only in `assembleGjcAttachArgv`, and the CLI must not classify, extend, or reorder what it receives. Under `GAJAEWAY_TEST_STUB_GJC=1` the only argv mutation the CLI may make is rewriting `argv[0]` to the test stub child.

## Why the change was unavoidable

Option A was the only viable topology. Attaching the TUI to daemon stdio (Option B) cannot satisfy AC-1/AC-10 and is the terminal-proxy the spec lists as a non-goal; letting the CLI assemble argv (Option C) would genuinely make it a second vendor adapter and would move flag policy outside the `GAJAEWAY_TEST_STUB_GJC` seam that tests it. `@gajaeway/cli` depends only on `@gajaeway/sdk` and `@gajaeway/protocol` and cannot import `GjcClient`. Passing a PTY fd over the socket would restore a unique spawn site but is the same non-goal proxy.

What the original fact was protecting — that flag, model, and session policy never fragments into a second implementation — is preserved in full, and is now enforced by test rather than by comment.

## Where the governing formulation is published

| Location | Role |
|---|---|
| `packages/gateway/src/orchestrator/gjc-client.ts` (comment above `GjcClient`) | normative, next to the code it constrains |
| `packages/cli/src/gjc.ts` (module comment) | mirrored at the spawn site |
| `docs/architecture.md` § "The `gajaeway gjc` terminal entrypath" | living product documentation |

## Drift enforcement (AC-20)

The invariant is machine-checked, not just documented:

- `packages/cli/test/gjc-flags.test.ts` — "the CLI source contains no copy of the gjc flag allowlist" fails if `packages/cli/src/gjc.ts` contains any of `--session-dir`, `--append-system-prompt`, `--system-prompt`, `--thinking`, `--worktree`, `--mpreset`, `--fork`, `--no-session` as classified code (comments are stripped before the check).
- `packages/gateway/test/gjc-attach-argv.test.ts` — the closed allowlist, refusal-with-value-consumption, blank-value, separator, and model-precedence rules are pinned at `classifyGjcWrapperFlags` / `assembleGjcAttachArgv`, i.e. inside the single owning module.
- `packages/cli/test/gjc-entrypath.e2e.test.ts` — the child's captured argv is asserted token by token: the binder's injections must be present, and wrapper-owned and refused flags must be absent. It does not diff the whole argv against the `session.attach` result, so it would catch a CLI-side injection of a known flag but not an arbitrary reordering.

## Scope

This erratum changes exactly one established fact's wording. Every other fact, non-goal, and acceptance criterion in the pinned spec stands unaltered, including the explicitly accepted non-goal that the persona reports a relocated repository's git state as its own while `--worktree` is active.
