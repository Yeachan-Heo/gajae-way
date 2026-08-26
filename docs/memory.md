# Memory

## Files first

Gajaeway’s canonical long-term memory is a private Git-backed Markdown tree at `$GAJAEWAY_HOME/memory`, not an opaque transcript database. The gateway creates it with mode `0700` and initializes its Git repository when needed.

```text
memory/
  MEMORY.md
  daily/
  events/
  tasks/
  people/
  projects/
  channels/
  decisions/
  .git/
```

These seven directories are the canonical homes: **daily**, **events**, **tasks**, **people**, **projects**, **channels**, and **decisions**. `MEMORY.md` is strictly a navigation map: it contains generated pointers to canonical files, not long-form facts. It lists up to 20 recent files per axis. Keep durable knowledge in axis files, then regenerate the map rather than turning `MEMORY.md` into a second store.

Each completed chat turn and each authored monitor event is captured in the current UTC daily file. A capture records timestamp, origin, speaker (`author @ #channel | server`), user/event text, and reply text; each text field is bounded to 500 characters. This gives the canonicalization routine raw daily material to sort into the seven axes.

The daily axis has two layers: flat `daily/YYYY-MM-DD.md` files are the gateway-written raw capture layer and must never be edited or moved, while persona-curated digests live in `daily/YYYY-MM/` subdirectories. Axis subdirectories are fully supported: the generated map scans axes recursively.

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
3. Commit `MEMORY.md` and `daily/` in the memory repository with `Gajaeway-Mutation-Id: <id>` in the commit message.
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
- `long_form_map` — a map line exceeds 200 characters.
- `duplicate_file_hash` — two Markdown files have identical content.
- `orphan_file` — Markdown exists outside the seven axis directories (except `MEMORY.md`).
- `out_of_root_link` — a Markdown link escapes the memory root.
- `map_content_drift` — the map does not point to the newest daily file.

Audit before manual repair. Repair the source Markdown or map generation problem, not the diagnostic evidence.

## Retrieval and quality checks

```sh
gajaeway memory search 'project decision'
```

Search reads safe `.md` pointers from `MEMORY.md`, then includes canonical axis files not already mapped. It ranks results with BM25-style term frequency, inverse document frequency, and document-length normalization; mapped files break score ties ahead of unmapped files. Results are bounded excerpts with path and score (default 10, maximum 50).

Maintain a small golden-query benchmark beside operational practice: representative queries should name the expected canonical file(s) and be checked after a reorganization. This is a human quality benchmark for retrieval relevance, not a replacement for `memory audit`’s structural gate.

See [monitors](monitors.md) for monitor mechanics and the [operator runbook](runbooks/gajaeway-v1.md) for recovery.
