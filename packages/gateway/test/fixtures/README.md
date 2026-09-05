# Fake gjc fixtures (version 1)

These are synthetic contract fixtures, not recordings of the pinned executable. Run a one-shot command with `bun packages/gateway/test/fixtures/fake-gjc.mjs sdk session inspect session-1 --repo /fixture`. No model, credentials, agent directory or persistent broker state is used.

`GAJAEWAY_FAKE_GJC_MODES` is a comma-separated list. Commas and escaped quotes inside `transcript:rows=<json>` are preserved. Selection is captured when `createFakeGjc()` is called. The daemon test seam applies only selected overrides, then executes its original code for unhandled operations. When the variable is unset/empty, the original daemon seam runs without awaiting an override; its original serialized output, counters, timers and inert spawn remain unchanged. Production `main.ts` does not import this seam.

## Envelopes

All CLI envelopes have exit code 0, JSON stdout and empty stderr, including structured `ok:false` errors. The subprocess adds a trailing newline. `S(x)` below means `{ "ok": true, "result": x }`; `Q(page)` means `{ "type": "query_response", "ok": true, "page": page }`.

| Mode | Command and exact payload |
| --- | --- |
| `cursor:<code>` | `sdk session tail ... --cursor ...` returns `{ok:false,error:{code}}`. Codes: `invalid_input`, `invalid_cursor`, `cursor_expired`, `snapshot_capacity_exceeded`. A tail without `--cursor` is not overridden. |
| `inspect:cwd-locator` | inspect: `S({session:{sessionId,live:false,saved:true,deleted:false,locator:{cwd,worktreeRoot:cwd,stateRoot:cwd+"/.gjc"}}})`; list (including `--scope`): `S({sessions:[row]})`. No `repo` field anywhere. `cwd` is `--repo` or process cwd. List id is `stub-session-1`; inspect echoes its id. |
| `inspect:cwd-mismatch` | Same row, but cwd/worktreeRoot are caller repo + `/fixture-other-repo`. Both locator modes start saved/non-live; resume on the same `createFakeGjc()` instance changes subsequent rows to `live:true`. |
| `status:unknown-forever` | Every status call: `S({operationRef,status:{status:"unknown"},summary:{completed:false}})`. No turn or transcript evidence. |
| `status:hang-30s` | Each status invocation waits 30,000 ms, then returns the unknown envelope above (or the selected unknown/content/failure projection). No timeout override is supplied. |
| `status:content` | Status: `S({operationRef,status:{status:"terminal_ok",outcome:{reason:"end_turn"}},summary:{completed:true},turn:{result:{kind:"prompt",status:"terminal_ok",content:{text:"AUTHORITATIVE",truncated:false}}}})`. |
| `status:content:truncated` | Same, with `content.truncated:true`. `GAJAEWAY_FAKE_GJC_CONTENT` replaces text, including Unicode/oversized text; fixture never silently truncates the supplied bytes. |
| `status:failed:<code>` | Status: `S({operationRef,status:{status:"failed",error:{code}},summary:{completed:true},turn:{result:{kind:"prompt",status:"failed",error:{code}}}})`. |
| `transcript:rows=<json>` | `raw query ... --query transcript.list`: `Q({items:[row],complete:false,continuationCursor})`; final page has `complete:true` and no continuation cursor. Empty rows return `items:[],complete:true`. Exactly one row per page, preserving input order and every field (`id`, `ts`, `revision`, body, parentId, etc.) without generating metadata. Cursor is base64url JSON `{connectionId,offset}`. A new command instance/CLI process rejects the continuation with `{ok:false,error:{code:"invalid_cursor"}}`; same serve connection accepts it. |
| `daemon:capacity-exhausted` | Every routed SDK session command: `{ok:false,error:{code:"invalid_input",message:"session.list cursor capacity is exhausted"}}`. `startCapacityExhaustedBroker()` provides a real loopback websocket: token-authenticated upgrade, first `{type:"broker_hello",protocolVersion:3}`, then `{type:"broker_response",id,ok:false,error:{code:"invalid_input",message:"session.list cursor capacity is exhausted"}}` per request. Caller must `stop()`. |
| `serve:bidirectional` | `sdk serve --stdio` reads NDJSON until EOF; `query_request`/`control_request` responses echo `id` and use `type:"query_response"`/`"control_response"`. Query field is `query`, control is `op` (also accepts `operation`), input is `input`, continuation is top-level `cursor`. One persistent command instance owns the connection. |
| `socket-down:<s>` | Adapter helper only; accepts `45` or `45s`. Before elapsed `s*1000`, `connect()` rejects an Error with `{code:"ECONNREFUSED",syscall:"connect",address:"fixture.sock"}`; at/after boundary it invokes the injected successful connect. No timers, real socket, or implicit delay. |

Precedence: capacity overrides all session commands; unknown status precedes failure/content; failure precedes content; mismatch precedes matching locator. Hang delays whichever status projection is selected. Other independent modes compose.

## CLI / serve support

The subprocess default is intentionally simpler than the existing stateful daemon seam: create returns `S({sessionId:"stub-session-1"})`; resume returns `S({sessionId,resumed:true})`; list is empty; inspect is live with `locator.repo`; send returns `S({sessionId,commandId:"stub-command-1"})`; status returns terminal_ok/end_turn; tail returns `S({items:[],terminal:false})`. This newly introduced subprocess has no pre-existing default behavior to preserve.

Both short create/resume and `raw global --op session.create` / `raw control ID --op session.resume` are supported. Model/profile/service-tier set return `S({changed:true})`; steer returns `S({status:"accepted"})`; close returns `S({closed:true})`; turn.prompt returns `S({sessionId,commandId:"stub-command-1",clientRef:input.clientRef})`.

`raw query --query turn.result` returns `{type:"query_response",ok:true,result:{kind:"prompt",status,content|error}}` using the selected status mode. `session.checkpoint` returns `{type:"query_response",ok:true,result:{checkpointToken:"fixture-checkpoint",revisionId:"fixture-revision-1"}}`. `queue.messages.list` and unconfigured `transcript.list` return `Q({items:[],complete:true})`; `session.last_assistant` returns `Q({items:[text],complete:true})` where text is `GAJAEWAY_TEST_STUB_REPLY` or `stub reply`. Unsupported ops fail explicitly with `unsupported_operation`/`stub_unsupported`.

## Exports and environment

- `fake-gjc.mjs`: `FIXTURE_VERSION`, `parseFakeModes(value?)`, `createFakeGjc({modes?,connectionId?,env?})`, `runFakeGjc(args,command?)`, `startCapacityExhaustedBroker({token?})`. A command returns `Promise<CliResult | undefined>`; undefined means no override, not success. New instances use random connection identities; pass a fixed identity only to deliberately model one connection.
- `fake-ps.ts`: `FakePsRow`, `renderFakePsTable(rows,tag)`, `fakePsTable(env?)`. Executable with Bun. `GAJAEWAY_FAKE_PS_ROWS` is JSON `{pid,ppid,command,startedAt?}[]`; `GAJAEWAY_FAKE_PS_TAG` defaults to `fixture`. Output is headerless `pid ppid lstart command GAJAEWAY_FAKE_PS_TAG=tag`, one row per line; default lstart is `Sun Sep  6 00:00:00 2026`. This is an injected table, never a claim about real process ownership.
- `packages/adapter-discord/test/fixtures/socket-down.ts`: `createSocketDownFixture({modes?,now?,connect?})` returns `{availableAtMs,attemptsAt,connect}`. Times are elapsed milliseconds relative to construction; injected clock enables virtual-time reconnect tests.
- Environment names: `GAJAEWAY_FAKE_GJC_MODES`, `GAJAEWAY_FAKE_GJC_CONTENT`, `GAJAEWAY_TEST_STUB_REPLY` (existing), `GAJAEWAY_FAKE_PS_ROWS`, `GAJAEWAY_FAKE_PS_TAG`.

Focused verification: `bun test packages/gateway/test/fixtures/fake-gjc.selfcheck.test.ts` (includes one real 30-second wait, bounded by a 35-second test timeout). All spawned processes and endpoint sockets are closed in finally blocks.
