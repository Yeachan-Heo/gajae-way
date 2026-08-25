# P0 GJC contract spike

**VERDICT: IDEMPOTENT-KEY: proven via `gjc sdk session raw global --op session.create --idempotency-key <origin-key>`.**

Probe date: 2026-08-25. Installed executable: `/Users/bellman/.local/bin/gjc`.

```text
$ which gjc
/Users/bellman/.local/bin/gjc
$ gjc --version
gjc/0.15.0
```

The repeatable probe is `bun scripts/spike-gjc-contract.ts`. It creates a new OS temporary directory, makes that directory the command cwd, passes `--session-dir` on every agent invocation, uses a minimal environment (`PATH`, `HOME`, `TERM`, `LANG`, `LC_ALL`, and the pre-existing agent configuration path), and removes the directory on completion. Provider-secret environment variables are not inherited. The intentionally bad-resume invocation additionally redirects `GJC_CODING_AGENT_DIR` to the temporary directory so its crash recorder cannot write under the normal agent directory.

## Evidence transcript

Paths below are normalized from one successful run; the probe deletes its temporary root after capturing the result.

### Session creation and isolated v2 storage

```text
$ gjc -p --mode json --no-tools --no-mcp --no-rules --no-lsp \
  --session-dir <tmp>/sessions "say exactly: spike-alpha"
exit=0 wall=2653ms

{"type":"session","version":5,"id":"01a037ea-db5a-7072-91a5-73ca624323e2",…}
{"type":"agent_start",…}
{"type":"turn_start",…}
```

The first NDJSON frame identifies the session ID. The requested `--session-dir` contained exactly:

```text
<tmp>/sessions/
├── 2026-08-25T07-55-33-594Z_01a037ea-db5a-7072-91a5-73ca624323e2.jsonl
└── 2026-08-25T07-55-33-594Z_01a037ea-db5a-7072-91a5-73ca624323e2/
```

Thus this explicit directory is the observed v2 session-storage directory for the run; session JSONL and its companion directory were confined to it. No default user session directory was inspected or used for session persistence.

### Resume and `--continue`

```text
$ gjc --resume 01a037ea-db5a-7072-91a5-73ca624323e2 -p --mode json \
  --no-tools --no-mcp --no-rules --no-lsp --session-dir <tmp>/sessions \
  "say exactly: spike-beta"
exit=0 wall=2031ms
{"type":"session","version":5,"id":"01a037ea-db5a-7072-91a5-73ca624323e2",…}

$ gjc --continue -p --mode json --no-tools --no-mcp --no-rules --no-lsp \
  --session-dir <tmp>/sessions "say exactly: spike-continue"
exit=0 wall=2230ms
{"type":"session","version":5,"id":"01a037ea-db5a-7072-91a5-73ca624323e2",…}
```

Both commands emitted the original ID, so resume continued the same saved transcript rather than making a fork. Re-running `--continue` against a fresh, shared session directory is therefore an effective deterministic single-session-per-directory mechanism. It is not the external-key contract selected for the gateway because directory naming and lifecycle ownership would have to be made durable separately.

Bogus IDs fail closed rather than silently creating a new session:

```text
$ gjc --resume 00000000-0000-0000-0000-000000000000 -p --mode json …
exit=1
[Uncaught Exception] Error: Session "00000000-0000-0000-0000-000000000000" not found.
```

This is a typed-by-behaviour nonzero failure, although the CLI renders it as an uncaught exception rather than a structured NDJSON error frame.

### SDK surface and atomic idempotent creation

```text
$ gjc sdk --help
… gjc sdk serve --stdio | --socket <path> [--session <id>];
  gjc sdk session list|inspect|send|status|tail …

$ gjc sdk session --help
FLAGS
  --idempotency-key=<value>  Caller idempotency key required for SDK lifecycle globals
```

The raw lifecycle operation accepts a caller key. Repeating it with the same temporary broker state, cwd, and key returned the same ID twice:

```text
$ gjc sdk session raw global --agent-dir <tmp>/sdk-agent --op session.create \
  --idempotency-key spike-external-key --json-input '{"cwd":"<tmp>/repo"}'
{"ok":true,"operation":"session.create","result":{"sessionId":"84a52b9e-d55d-43a5-8e37-1c51580e73ec","cwd":"<tmp>/repo","endpointGeneration":1}}

# identical command and key
{"ok":true,"operation":"session.create","result":{"sessionId":"84a52b9e-d55d-43a5-8e37-1c51580e73ec","cwd":"<tmp>/repo","endpointGeneration":1}}
```

Five concurrent `session.create` calls with one shared key all exited 0 and all returned `3b1987c1-005e-4654-a61d-bf34c7b2e0ff` (`uniqueSessionIds: 1`). This is direct evidence of atomic create-or-return-existing semantics under contention.

### One-shot concurrency

Five parallel, independently created `gjc -p` turns sharing the same `--session-dir` all exited 0, each produced an `agent_end` frame, and produced five distinct IDs:

```text
uniqueSessionIds: 5
sessionIds:
  01a037eb-1467-736e-b921-40e69b257add
  01a037eb-1467-7243-8d87-ebe2855006de
  01a037eb-1461-75bb-bbea-909a48c7dec9
  01a037eb-1456-70b8-aaea-2f1f3188733a
  01a037eb-145c-7143-95e0-63f462bb7f02
```

No lock contention, corrupted JSON, or storage race was observed in this five-process sample.

## Cost and latency

Observed process wall-clock was **2.65 s** for the fresh one-shot and **2.03 s** for the resumed turn (about **0.62 s** less on this run). The installed provider configuration returned an HTTP 401 after session initialization, so no successful model token usage or monetary cost was available; these figures measure CLI/session setup through provider failure, not end-to-end model generation latency. The script reports fresh and resumed timings on every run.

## P1 GjcPort strategy

Use **spawn-per-turn** in P1:

1. Map the durable `originKey` to the SDK lifecycle idempotency key and call `gjc sdk session raw global --op session.create --idempotency-key <originKey>` with the gateway scratch cwd. Persist the returned `sessionId` only after the operation succeeds.
2. For each message, spawn `gjc --resume <sessionId> -p --mode json --no-tools --session-dir <gateway-controlled-dir>` and translate its NDJSON output into the frozen gajaeway protocol frames.
3. Treat nonzero `--resume` as a typed port failure; never replace it with a new session.

This is the smallest P1 implementation with the empirically proven external-key contract and clear per-turn process ownership. `gjc sdk serve --socket <path> --session <id>` is a viable later optimization for persistent relay latency, but this spike did not establish its restart, multiplexing, or lifecycle-reconciliation behaviour; it should not be the P1 correctness dependency.
