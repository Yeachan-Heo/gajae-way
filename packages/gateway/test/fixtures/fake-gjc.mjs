#!/usr/bin/env bun
import { randomUUID } from "node:crypto";
import { createInterface } from "node:readline";

export const FIXTURE_VERSION = 1;
const capacityError = { code: "invalid_input", message: "session.list cursor capacity is exhausted" };
const cli = (envelope) => ({ exitCode: 0, stdout: JSON.stringify(envelope), stderr: "" });
const ok = (result) => cli({ ok: true, result });
const fail = (code) => cli({ ok: false, error: { code } });
const arg = (args, name) => (args.includes(name) ? args[args.indexOf(name) + 1] : undefined);

// Commas inside transcript JSON (including quoted strings) are not mode separators.
export function parseFakeModes(value = "") {
	let depth = 0;
	let quoted = false;
	let escaped = false;
	let start = 0;
	const modes = [];
	for (let i = 0; i < value.length; i++) {
		const c = value[i];
		if (quoted) {
			if (escaped) escaped = false;
			else if (c === "\\") escaped = true;
			else if (c === '"') quoted = false;
		} else if (c === '"') quoted = true;
		else if (c === "[" || c === "{") depth++;
		else if (c === "]" || c === "}") depth--;
		else if (c === "," && depth === 0) {
			modes.push(value.slice(start, i).trim());
			start = i + 1;
		}
	}
	if (quoted || depth !== 0) throw new Error("malformed fake modes JSON");
	return [...modes, value.slice(start).trim()].filter(Boolean);
}

export function createFakeGjc({ modes, connectionId = randomUUID(), env = process.env } = {}) {
	const selected = parseFakeModes(modes ?? env.GAJAEWAY_FAKE_GJC_MODES);
	const has = (mode) => selected.includes(mode);
	const prefixed = (prefix) => selected.find((mode) => mode.startsWith(prefix));
	let resumed = false;
	const content = {
		text: env.GAJAEWAY_FAKE_GJC_CONTENT ?? "AUTHORITATIVE",
		truncated: has("status:content:truncated"),
	};
	const failure = prefixed("status:failed:")?.slice("status:failed:".length);
	const turnResult = () =>
		failure
			? { kind: "prompt", status: "failed", error: { code: failure } }
			: { kind: "prompt", status: "terminal_ok", content };
	return async (rawArgs) => {
		const args = rawArgs.filter((_, i) => rawArgs[i] !== "--agent-dir" && rawArgs[i - 1] !== "--agent-dir");
		const command = args[2];
		const op = args.includes("--op") ? arg(args, "--op") : undefined;
		const query = args.includes("--query") ? arg(args, "--query") : undefined;
		if (args[0] !== "sdk" || args[1] !== "session") return undefined;
		if (has("daemon:capacity-exhausted")) return cli({ ok: false, error: capacityError });
		if (command === "tail" && args.includes("--cursor") && prefixed("cursor:"))
			return fail(prefixed("cursor:").slice(7));
		if (has("inspect:cwd-locator") || has("inspect:cwd-mismatch")) {
			if (command === "resume" || op === "session.resume") {
				resumed = true;
				return ok({ sessionId: args[3], resumed: true });
			}
			if (command === "inspect" || command === "list" || command === "--scope") {
				const repo = args.includes("--repo") ? arg(args, "--repo") : process.cwd();
				const cwd = has("inspect:cwd-mismatch") ? `${repo}/fixture-other-repo` : repo;
				const session = {
					sessionId: command === "inspect" ? args[3] : "stub-session-1",
					live: resumed,
					saved: true,
					deleted: false,
					locator: { cwd, worktreeRoot: cwd, stateRoot: `${cwd}/.gjc` },
				};
				return ok(command === "inspect" ? { session } : { sessions: [session] });
			}
		}
		if (command === "status") {
			if (has("status:hang-30s")) await Bun.sleep(30_000);
			if (has("status:unknown-forever"))
				return ok({ operationRef: args[4], status: { status: "unknown" }, summary: { completed: false } });
			if (failure || has("status:content") || has("status:content:truncated")) {
				// Real `session status` (gjc 0.16.3 runStatus) returns the turn.result
				// object AS `status`, so content/error live inside it.
				const result = turnResult();
				return ok({
					operationRef: args[4],
					status: {
						...result,
						terminalAt: Date.parse(env.GAJAEWAY_FAKE_GJC_TERMINAL_AT ?? "2026-09-05T00:10:00.000Z"),
						...(failure ? {} : { outcome: { reason: "end_turn" } }),
					},
					summary: { completed: true },
				});
			}
			if (has("status:hang-30s"))
				return ok({ operationRef: args[4], status: { status: "unknown" }, summary: { completed: false } });
		}
		if (query === "turn.result") return cli({ type: "query_response", ok: true, result: turnResult() });
		if (query === "session.checkpoint")
			return cli({
				type: "query_response",
				ok: true,
				result: { checkpointToken: "fixture-checkpoint", revisionId: "fixture-revision-1" },
			});
		if (query === "Q23" && prefixed("transcript:rows=")) {
			const rows = JSON.parse(prefixed("transcript:rows=").slice("transcript:rows=".length));
			const input = JSON.parse(arg(args, "--json-input"));
			const row = rows.find((candidate) => candidate.id === input.itemId);
			if (!row) return fail("invalid_input");
			const value = String(row[input.field] ?? "");
			const CHUNK = 64 * 1024;
			let byteOffset = 0;
			if (args.includes("--cursor")) {
				try {
					const cursor = JSON.parse(Buffer.from(arg(args, "--cursor"), "base64url").toString());
					if (cursor.connectionId !== connectionId || cursor.itemId !== input.itemId || cursor.field !== input.field)
						return fail("invalid_cursor");
					byteOffset = cursor.byteOffset;
				} catch {
					return fail("invalid_cursor");
				}
			}
			// Like the pinned runtime: byteOffset counts UTF-8 bytes and every chunk
			// ends on a code-point boundary (the live 262143-byte chunk was 87381 x 3).
			const bytes = Buffer.from(value, "utf8");
			let end = Math.min(bytes.length, byteOffset + CHUNK);
			while (end < bytes.length && end > byteOffset && (bytes[end] & 0xc0) === 0x80) end--;
			const safe = bytes.subarray(byteOffset, end);
			const body = safe.toString("utf8");
			const done = end >= bytes.length;
			return cli({
				type: "query_response",
				ok: true,
				page: {
					revision: "fixture-revision-1",
					items: [{ field: input.field, itemId: input.itemId, byteOffset, body, complete: done }],
					complete: done,
					...(!done
						? {
								continuationCursor: Buffer.from(
									JSON.stringify({
										connectionId,
										itemId: input.itemId,
										field: input.field,
										byteOffset: byteOffset + safe.length,
									}),
								).toString("base64url"),
							}
						: {}),
				},
			});
		}
		if (query === "transcript.list" && prefixed("transcript:rows=")) {
			const rows = JSON.parse(prefixed("transcript:rows=").slice("transcript:rows=".length));
			if (!Array.isArray(rows)) throw new Error("transcript rows must be an array");
			let offset = 0;
			if (args.includes("--cursor")) {
				try {
					const cursor = JSON.parse(Buffer.from(arg(args, "--cursor"), "base64url").toString());
					if (cursor.connectionId !== connectionId || !Number.isSafeInteger(cursor.offset) || cursor.offset < 0)
						return fail("invalid_cursor");
					offset = cursor.offset;
				} catch {
					return fail("invalid_cursor");
				}
			}
			const complete = offset + 1 >= rows.length;
			// `oversized:true` rows arrive as item_too_large placeholders with Q23
			// continuations per field, exactly like the pinned runtime (recordings).
			const project = (row) =>
				row.oversized
					? {
							id: row.id,
							error: { code: "item_too_large" },
							continuations: ["id", "role", "textSummary", "ts", "body"].map((field) => ({
								query: "Q23",
								resourceKind: "transcript",
								resourceId: "default",
								revision: "fixture-revision-1",
								itemId: row.id,
								field,
							})),
						}
					: row;
			return cli({
				type: "query_response",
				ok: true,
				page: {
					revision: "fixture-revision-1",
					items: rows.slice(offset, offset + 1).map(project),
					complete,
					...(!complete
						? {
								continuationCursor: Buffer.from(JSON.stringify({ connectionId, offset: offset + 1 })).toString(
									"base64url",
								),
							}
						: {}),
				},
			});
		}
		return undefined;
	};
}

export function startCapacityExhaustedBroker({ token = "fixture-token" } = {}) {
	const requests = [];
	const server = Bun.serve({
		hostname: "127.0.0.1",
		port: 0,
		fetch(request, server) {
			if (new URL(request.url).searchParams.get("token") !== token)
				return new Response("Unauthorized", { status: 401 });
			return server.upgrade(request) ? undefined : new Response("Upgrade Required", { status: 426 });
		},
		websocket: {
			open(socket) {
				socket.send(JSON.stringify({ type: "broker_hello", protocolVersion: 3 }));
			},
			message(socket, raw) {
				const frame = JSON.parse(String(raw));
				requests.push(frame);
				socket.send(JSON.stringify({ type: "broker_response", id: frame.id, ok: false, error: capacityError }));
			},
		},
	});
	return { url: `ws://127.0.0.1:${server.port}`, token, requests, stop: () => server.stop(true) };
}

export async function runFakeGjc(args, command = createFakeGjc()) {
	const overridden = await command(args);
	if (overridden) return overridden;
	if (args.includes("--version")) return { exitCode: 0, stdout: "gjc/0.16.3\n", stderr: "" };
	const op = args.includes("--op") ? arg(args, "--op") : args[2];
	const sessionId = args.includes("control") ? args[args.indexOf("control") + 1] : (args[3] ?? "stub-session-1");
	if (op === "session.create" || op === "create") return ok({ sessionId: "stub-session-1" });
	if (op === "session.resume" || op === "resume") return ok({ sessionId, resumed: true });
	if (args.includes("--query")) {
		const query = arg(args, "--query");
		if (["session.last_assistant", "queue.messages.list", "transcript.list"].includes(query))
			return cli({
				type: "query_response",
				ok: true,
				page: {
					items: query === "session.last_assistant" ? [process.env.GAJAEWAY_TEST_STUB_REPLY ?? "stub reply"] : [],
					complete: true,
				},
			});
		return fail("unsupported_operation");
	}
	if (args.includes("--op")) {
		if (["model.set", "model.profile.set", "service_tier.set"].includes(op)) return ok({ changed: true });
		if (op === "turn.prompt")
			return ok({
				sessionId,
				commandId: "stub-command-1",
				clientRef: JSON.parse(arg(args, "--json-input") ?? "{}").clientRef,
			});
		if (op === "turn.steer") return ok({ status: "accepted" });
		if (op === "session.close") return ok({ closed: true });
		return fail("unsupported_operation");
	}
	if (args[2] === "list" || args[2] === "--scope") return ok({ sessions: [] });
	if (args[2] === "inspect")
		return ok({
			session: {
				sessionId,
				live: true,
				deleted: false,
				locator: { repo: args.includes("--repo") ? arg(args, "--repo") : process.cwd() },
			},
		});
	if (args[2] === "send") return ok({ sessionId, commandId: "stub-command-1" });
	if (args[2] === "status")
		return ok({
			operationRef: args[4],
			status: { status: "terminal_ok", outcome: { reason: "end_turn" } },
			summary: { completed: true },
		});
	if (args[2] === "tail") return ok({ items: [], terminal: false });
	return fail("stub_unsupported");
}

if (import.meta.main) {
	const args = process.argv.slice(2);
	if (
		args.includes("serve") &&
		args.includes("--stdio") &&
		parseFakeModes(process.env.GAJAEWAY_FAKE_GJC_MODES).includes("serve:bidirectional")
	) {
		const command = createFakeGjc();
		for await (const line of createInterface({ input: process.stdin })) {
			const request = JSON.parse(line);
			const query = request.type === "query_request";
			const cliArgs = [
				"sdk",
				"session",
				"raw",
				query ? "query" : "control",
				request.sessionId ?? "stub-session-1",
				query ? "--query" : "--op",
				query ? request.query : (request.op ?? request.operation),
				"--json-input",
				JSON.stringify(request.input ?? {}),
				...(request.cursor ? ["--cursor", request.cursor] : []),
			];
			const result = JSON.parse((await runFakeGjc(cliArgs, command)).stdout);
			console.log(JSON.stringify({ ...result, type: query ? "query_response" : "control_response", id: request.id }));
		}
	} else {
		const result = await runFakeGjc(args);
		process.stdout.write(`${result.stdout}\n`);
		process.exitCode = result.exitCode;
	}
}
