# Memory

## Files first

Gajaeway’s canonical long-term memory is a private Git-backed Markdown tree at `$GAJAEWAY_HOME/memory`, not an opaque transcript database. The gateway creates it with mode `0700` and initializes its Git repository when needed.

```text
memory/
  MEMORY.md
  axes.json          # optional: deployment-registered custom axes
  daily/
  events/
  tasks/
  people/
  projects/
  channels/
  decisions/
  ops/
    rules/
    distillations/
    handoffs/
  reflections/
  .git/
```

`MEMORY.md` is strictly a navigation map: it contains generated pointers to canonical files, not long-form facts. Keep durable knowledge in axis files, then regenerate the map rather than turning `MEMORY.md` into a second store.

## Fresh-session bootstrap safety

The first attempted turn in each origin epoch receives a trusted, bounded navigation bootstrap. The gateway reads from the resolved `$GAJAEWAY_HOME/memory` root only. The memory root may itself be a symlink to the canonical corpus, and symlinks that resolve inside that canonical root remain supported; traversal, malformed percent escapes, absolute links, dangling targets, non-files, and targets outside the resolved memory root are rejected. Rejected sources produce compact diagnostic codes, never source bodies or raw operating-system error strings.

Association is canonical and exact. A channel/project/task/handoff document must declare `origin-key: <opaque originKey>` (the aliases `origin-id:` and `origin:` accept the same complete key; `origin:` may also contain a valid JSON `OriginRef` whose normalized key matches). A bare conversation ID is never sufficient because platforms, thread parents, and DM peers can share it. Public/group origins additionally require `bootstrap-safe: public` or `bootstrap-visibility: public` before an associated document body or pointer is eligible. Daily capture is parsed entry-by-entry and only entries whose full canonical origin matches the current origin are eligible; other-origin and DM entries are omitted. `MEMORY.md` and `ops/rules/index.md` contribute headings and validated Markdown pointers only, not prose bodies.

The complete `Session bootstrap` section is at most 8192 UTF-8 bytes. Sections have deterministic priority, are included whole, carry their logical source path and filesystem freshness timestamp, and are never sliced mid-line or mid-codepoint. Omitted section names/counts and optional-source failures appear in the bounded diagnostics section. Secret-shaped values are redacted, source material is explicitly delimited as reference data rather than instructions, and the database/admin/ops projections retain only epoch, applied time, included section names, byte count, truncation, and diagnostic codes.

## The axis registry

An axis is not a name in a list. It is a **descriptor** that tells every other part of the memory system how to treat its files, so map generation, audit, canonicalization and retrieval never test an axis by id:

| Field | Meaning |
|---|---|
| `id` | stable machine id; the map heading, audit messages and promotion targets key off it |
| `displayName` | human label rendered under the map heading |
| `root` | canonical root relative to the memory root; roots may not overlap |
| `nesting` | `flat` (entries directly under the root) or `nested` (subdirectories at any depth) |
| `partitions` | named subdirectories that partition a nested axis |
| `index` | `recent` (map the newest entries) or `tree` (map the whole hierarchy, grouped) |
| `layout` | `dated` (`[YYYY-MM/]YYYY-MM-DD.md` only) or `free` (any `.md` name) |
| `retrievalPriority` | higher wins when two documents score equally during recall |
| `orphanPolicy` | `partitioned` (a file must sit in a declared partition) or `any-depth` |
| `appendOnly` | entries are only ever appended to, never rewritten in place |
| `promotesTo` | axis ids this axis promotes durable material into; must be acyclic |

The built-in axes live in exactly one place, `packages/gateway/src/memory/registry.ts`. A deployment adds its own — or restates a built-in — without editing that module by writing `memory/axes.json`:

```json
{
  "version": 1,
  "axes": [
    {
      "id": "runbooks",
      "displayName": "Deployment runbooks",
      "root": "runbooks",
      "nesting": "nested",
      "partitions": ["staging", "production"],
      "index": "tree",
      "layout": "free",
      "retrievalPriority": 90,
      "orphanPolicy": "partitioned",
      "appendOnly": false,
      "promotesTo": ["ops"]
    }
  ]
}
```

Only `id` is required; every other field takes the documented default (`root` defaults to the id, `nesting` to `nested`, `index` to `recent`, `layout` to `free`, `orphanPolicy` to `any-depth`, `retrievalPriority` to `0`, `appendOnly` to whether the layout is dated). A registered axis is created, indexed, audited and searched by the same code as a built-in.

A declaration whose `id` names a built-in **overrides** it: stated fields win, omitted fields keep the built-in's value. The built-in set is a default, not a floor. This is how a deployment whose `ops/` tree grew its own partitions clears `axis_layout_violation` without moving a single file — `{ "id": "ops", "partitions": ["rules", "distillations", "handoffs", "incidents", "runbooks"] }` widens the partition list and leaves the axis's index, priority and policy exactly as shipped. Relaxing a built-in is possible but must be said out loud: `{ "id": "daily", "appendOnly": false }` drops the append-only guarantee and its `map_content_drift` check, and re-rooting the capture axis moves where turns are captured and read back.

The registry **fails closed**. Startup refuses to run, rather than quietly dropping an axis and orphaning everything under it, when a declaration is repeated twice, claims a root that contains or sits inside another axis's root, has a promotion target that is not registered, closes a promotion cycle, carries an unknown field, or holds any malformed value. Registration is also the *only* way a directory becomes canonical: writing into an unregistered directory never promotes it, and the audit keeps reporting it as an orphan.

### What each built-in takes and refuses

| Axis | Takes | Refuses |
|---|---|---|
| `daily` | append-only capture of what was said | curated fact |
| `events` | something that happened at a point in time | plans, rules |
| `tasks` | work still owed, with its state | a record of finished work |
| `people` | durable facts about a person | what they said once |
| `projects` | durable facts about an ongoing effort | its individual work items |
| `channels` | durable facts about a place we talk in | the messages sent there |
| `decisions` | what was chosen at a point in time and why: context, options, chosen, rationale, scope, timestamp, supersedes | rules for what to do next |
| `ops` | repeatable operating rules, runtime/session/tool procedure, principles distilled from failure, state the next executor picks up | raw transcript, secrets, dated small talk, one-off dumps |
| `reflections` | observed failure or drift, the invariant learned, why it matters, the concrete next action, its promotion target | facts about the world |

`ops` and `decisions` are the pair most easily confused: **`ops` constrains current behaviour before acting; `decisions` records what was chosen at a point in time and why.**

**`ops` is routable, not one growing file.** It is partitioned into `ops/rules/`, `ops/distillations/` and `ops/handoffs/`, indexed as a tree so a rule pack never scrolls off the newest-entries window, and ranked above raw capture during recall. A file directly under `ops/` is an `axis_layout_violation`: it belongs in a partition.

**`reflections` is dated and append-only.** Entries are `reflections/YYYY-MM-DD.md`, optionally sharded into `reflections/YYYY-MM/`, with several entries per day allowed inside a file. Per-subject files (`reflections/tone.md`) are rejected with `axis_layout_violation`; a subject view may exist only as a generated projection, never as a second authority. One layout, not both: a writer holding a subject file edits it in place, and the correction history that makes a reflection useful is exactly what gets overwritten. A reflection promotes onward — an operating invariant to `ops/rules`, a cross-incident learning to `ops/distillations`, a project-scoped or person/channel correction to that axis.

Adding an axis to an existing corpus is additive only. Startup creates the missing directories and regenerates the generated map; it never reads, moves, rewrites or deletes a file a human wrote, so a seven-axis corpus and an `ops/` tree written by hand before the axis existed both survive byte-identically.

Each completed chat turn and each authored monitor event is captured in the current UTC daily file. A capture records timestamp, origin, speaker (`author @ #channel | server`), user/event text, and reply text; each text field is bounded to 500 characters. This gives the canonicalization routine raw daily material to sort into the canonical axes.

The daily axis has two layers: flat `daily/YYYY-MM-DD.md` files are the gateway-written raw capture layer and must never be edited or moved, while persona-curated digests live in `daily/YYYY-MM/` subdirectories. Axis subdirectories are fully supported at any depth: map generation, audit and retrieval all walk an axis recursively.

There is exactly **one** traversal behind those three, and that is deliberate: a file the map serves must be a file the audit inspects, or an unpartitioned rule can be indexed and recalled while never being reported. It follows directory symlinks — an axis rooted at a symlink still gets audited — bounded by `realpath` bookkeeping so a symlink cycle terminates. Symlinked *files* are skipped, since they are a second name for content already indexed under its real path.

Startup is safe to call concurrently. Adapters, monitors and the closure queue all initialize, so `initializeMemory` serializes per memory root; otherwise concurrent cold starts race on `git init` and on the generated map. A malformed registry, an unwritable root, or a plain file where an axis directory belongs still fails closed, leaving the corpus untouched.

## Default maintenance monitors

These are seeded automatically on a database's first boot (removals are never resurrected); a monitor without its own channel target reports to the configured `ownerTarget`:

| Monitor | Local schedule | Event type | Burst policy | Target |
|---|---:|---|---|---|
| `memory-canonicalize` | every 6 hours (00:30/06:30/12:30/18:30) | `memory.canonicalize` | `dedupe` | `ownerTarget` fallback |
| `memory-audit` | 06:00 daily | `memory.audit` | `dedupe` | `ownerTarget` fallback |

Canonicalization reads daily captures, consolidates durable facts into the relevant canonical axes, and keeps `MEMORY.md` as a generated map. The audit then reports the structural state to the owner channel. Review changes as ordinary Markdown and Git changes; daily capture remains the source material rather than an invisible side channel.

## Closure ladder and recovery

Memory writes use a durable closure ladder so a reply does not have to wait for Git work:

1. Store an intent in the gateway database (the acceptance boundary).
2. Write the daily Markdown capture and regenerate `MEMORY.md`.
3. Commit the whole corpus in the memory repository with `Gajaeway-Mutation-Id: <id>` in the commit message. Staging is not limited to the capture axis: every registered axis is committed, so a reflection or an `ops` rule written by hand between two captures enters the same reviewable history instead of sitting untracked.
4. Append a receipt containing the mutation ID, commit hash, and timestamp to `$GAJAEWAY_HOME/memory-receipts.jsonl`.
5. On startup, resume queued, written, or committed intents. If the evidence cannot be recovered, mark the intent **quarantined**; do not erase the intent, receipt, or Git history to hide it.

This is why a crash can leave a repeatable recovery record instead of a half-explained note. The queue serializes closure work. It checks existing commits and receipts before repeating a step.

## Audit gate

Run the validator through the daemon:

```sh
gajaeway memory audit
```

It returns JSON issues and exits non-zero when memory is not structurally sound. Its diagnostic codes are:

- `map_dangling` — a `MEMORY.md` pointer does not exist.
- `unmapped_axis_dir` — an axis has no heading in the map.
- `long_form_map` — a map line carries more than 200 characters of prose. A link is measured by its text, not by its target, so a generated pointer to a deep path is navigation rather than long-form content.
- `duplicate_file_hash` — two Markdown files have identical content.
- `orphan_file` — Markdown exists outside every registered axis root (except `MEMORY.md`); register the axis in `axes.json` to make it canonical.
- `out_of_root_link` — a Markdown link or map pointer escapes the memory root. Percent-escapes are decoded first, so `%2e%2e/secret.md` is reported as an escape rather than a missing file.
- `axis_layout_violation` — a file breaks its own axis's layout policy: a dated axis given a per-subject name, a partitioned axis written to directly, or a flat axis given a subdirectory.
- `map_content_drift` — the map does not point to the newest entry of an append-only axis (`daily`, `reflections`).

Audit before manual repair. Repair the source Markdown or map generation problem, not the diagnostic evidence.

The audit is **read-only and takes no arguments**: there is no `--fix`, and passing one is refused rather than ignored, because a flag that silently degraded to a plain audit would look like a repair attempt that reproduced the failure. Repair is by hand and by policy — register the directory in `axes.json` (`orphan_file`), restate the axis with the partitions the corpus actually uses (`axis_layout_violation`), or move/merge the files — and then re-run the audit.

## Retrieval and quality checks

```sh
gajaeway memory search 'project decision'
```

Search reads `.md` pointers from `MEMORY.md`, then includes canonical axis files not already mapped. A pointer is followed only if it stays inside the memory root once decoded; a pointer that escapes is ignored, and one that no longer resolves is skipped, so recall degrades on a stale map instead of failing the query (the audit is what reports the staleness, as `map_dangling`). It ranks results with BM25-style term frequency, inverse document frequency, and document-length normalization; ties break on the axis's declared `retrievalPriority`, then on mapped ahead of unmapped. Results are bounded excerpts with path and score (default 10, maximum 50).

Maintain a small golden-query benchmark beside operational practice: representative queries should name the expected canonical file(s) and be checked after a reorganization. This is a human quality benchmark for retrieval relevance, not a replacement for `memory audit`’s structural gate.

See [monitors](monitors.md) for monitor mechanics and the [operator runbook](runbooks/gajaeway-v1.md) for recovery.
