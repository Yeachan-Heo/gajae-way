import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { GjcClient, type GjcPort } from "../src/orchestrator/gjc-client";
import { DEFAULT_REBIND_CAP, formatFailureNotice, GjcRuntimeError } from "../src/orchestrator/rebind";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";

let directory = "";
const testDirectories: string[] = [];
let server: GatewayServer | undefined;
afterEach(async () => {
	await server?.stop();
	server = undefined;
	for (const home of new Set([directory, ...testDirectories]))
		if (home) await rm(home, { recursive: true, force: true });
	testDirectories.length = 0;
	directory = "";
});

function fakeChild(stdout: string, stderr = "", exitCode = 0): ReturnType<typeof Bun.spawn> {
	return {
		stdout: new Response(stdout).body,
		stderr: new Response(stderr).body,
		exited: Promise.resolve(exitCode),
		kill: () => {},
	} as unknown as ReturnType<typeof Bun.spawn>;
}

function blockedChild(
	gate: Promise<void>,
	stdout: string,
	stderr: string,
	exitCode: number,
	onKill: () => void,
): ReturnType<typeof Bun.spawn> {
	const stream = new ReadableStream<Uint8Array>({
		start(controller) {
			void gate.then(() => {
				if (stdout) controller.enqueue(new TextEncoder().encode(stdout));
				controller.close();
			});
		},
	});
	return {
		stdout: stream,
		stderr: new Response(stderr).body,
		exited: gate.then(() => exitCode),
		kill: onKill,
	} as unknown as ReturnType<typeof Bun.spawn>;
}

function createFailure(code: string, message: string): string {
	return `${JSON.stringify({ ok: false, operation: "session.create", error: { code, message } })}\n`;
}
/** Every create fails rebindably: the workhorse for cap/hold assertions. */
const failingSpawn = (() =>
	fakeChild(createFailure("resource_gone", "session endpoint record is gone"), "", 1)) as unknown as typeof Bun.spawn;

function createSuccess(sessionId: string): string {
	return `${JSON.stringify({ ok: true, operation: "session.create", result: { sessionId } })}\n`;
}

function turnFailure(code: string, message: string): string {
	return `${JSON.stringify({ type: "error", error: { code, message } })}\n`;
}

function turnReply(text: string): string {
	return `${JSON.stringify({ type: "message_end", message: { role: "assistant", content: [{ type: "text", text }] } })}\n`;
}

async function makeDatabase(prefix: string): Promise<GatewayDatabase> {
	directory = await mkdtemp(join(tmpdir(), prefix));
	testDirectories.push(directory);
	return GatewayDatabase.open(join(directory, "gateway.db"));
}

test("mixed rebind and clean turns restore the cap without erasing lifetime history", async () => {
	const database = await makeDatabase("gajaeway-takeover-cap-");
	const logs: string[] = [];
	let created = 0;
	let turnPlan: boolean[] = [];
	const spawn = ((options: { cmd: string[] }) => {
		if (options.cmd.includes("session.create")) return fakeChild(createSuccess(`session-e${created++}`));
		if (turnPlan.shift()) {
			return fakeChild(turnFailure("managed_append_identity_mismatch", "identity moved"), "", 1);
		}
		return fakeChild(turnReply("clean"));
	}) as unknown as typeof Bun.spawn;
	const client = new GjcClient(database, 5_000, directory, undefined, {
		spawn,
		log: (line) => logs.push(line),
	});
	const current = async () => {
		const epoch = database.getSessionRecord("discord:dm:cap")?.epoch ?? 0;
		return (await client.ensureSession("discord:dm:cap", epoch)).sessionId;
	};
	try {
		// Rebind, prove the fresh binding healthy, and repeat. The clean turns clear
		// the consecutive budget, while lifetime keeps climbing.
		for (let round = 0; round < 2; round++) {
			turnPlan = [true, false];
			// The first attempt rebinds and its empty replay succeeds; the call is
			// therefore successful, but the rebind log/lifetime still records it.
			await expect(client.sendTurn(await current(), "failure")).resolves.toBe("clean");
			turnPlan = [false];
			await expect(client.sendTurn(await current(), "clean")).resolves.toBe("clean");
		}
		expect(database.getSessionRecord("discord:dm:cap")?.epoch).toBe(2);
		expect(logs.map((line) => line.match(/lifetime=(\d+)/)?.[1])).toEqual(["1", "2"]);
		expect(logs.every((line) => line.includes("rebind 1/3"))).toBe(true);

		// Three consecutive failures spend the restored budget. The fourth attempt
		// is refused and its operator-facing notice must point at /new.
		for (let attempt = 0; attempt < DEFAULT_REBIND_CAP; attempt++) {
			turnPlan = [true, true];
			await expect(client.sendTurn(await current(), "failure")).rejects.toMatchObject({
				code: "managed_append_identity_mismatch",
			});
		}
		turnPlan = [true, true];
		const capFailure = await client.sendTurn(await current(), "failure").catch((error: unknown) => error);
		expect(capFailure).toMatchObject({ name: "RebindCapExceededError", code: "rebind_cap_exceeded" });
		expect(formatFailureNotice(capFailure)).toContain("/new");
		expect(database.getSessionRecord("discord:dm:cap")?.epoch).toBe(5);
		expect(logs).toHaveLength(5);
	} finally {
		database.close();
	}
});

test("concurrent ensureSession calls share one in-flight rebind and one epoch binding", async () => {
	const database = await makeDatabase("gajaeway-takeover-concurrent-");
	const logs: string[] = [];
	const commands: string[][] = [];
	let createCalls = 0;
	let releaseFirst!: () => void;
	const firstGate = new Promise<void>((resolve) => {
		releaseFirst = resolve;
	});
	const spawn = ((options: { cmd: string[] }) => {
		commands.push(options.cmd);
		if (!options.cmd.includes("session.create")) throw new Error("unexpected turn spawn");
		createCalls++;
		if (createCalls === 1)
			return blockedChild(
				firstGate,
				createFailure("resource_gone", "session endpoint record is gone"),
				"",
				1,
				releaseFirst,
			);
		// With one shared bind there are exactly two spawns: the condemned e0
		// create and the post-rebind e1 create that both callers receive.
		if (createCalls === 2) return fakeChild(createSuccess("session-e1"));
		throw new Error(`unexpected extra spawn ${createCalls}`);
	}) as unknown as typeof Bun.spawn;
	const client = new GjcClient(database, 60_000, directory, undefined, {
		spawn,
		log: (line) => logs.push(line),
	});
	try {
		const first = client.ensureSession("discord:dm:race", 0);
		const second = client.ensureSession("discord:dm:race", 0);
		// Both callers are now parked on ONE in-flight bind: create #1 is blocked,
		// and no second spawn may exist while it is. Releasing the gate lets the
		// shared bind see resource_gone once, rebind once to e1, and hand BOTH
		// callers that same session.
		await Bun.sleep(10);
		expect(commands).toHaveLength(1);
		releaseFirst();
		const results = await Promise.all([first, second]);
		expect(new Set(results.map((result) => result.sessionId))).toEqual(new Set(["session-e1"]));
		expect(database.getSessionRecord("discord:dm:race")).toEqual({ sessionId: "session-e1", epoch: 1 });
		expect(commands.filter((command) => command.includes("session.create"))).toHaveLength(2);
		expect(logs).toHaveLength(1);
	} finally {
		database.close();
	}
}, 20_000);

test("message wording never rebinds an unrelated create or turn error code", async () => {
	for (const message of ["session endpoint record is gone", "SDK startup did not complete"]) {
		const database = await makeDatabase("gajaeway-takeover-prose-create-");
		const logs: string[] = [];
		let calls = 0;
		const spawn = ((_options: { cmd: string[] }) => {
			calls++;
			return fakeChild(createFailure("unsupported_state_version", message), "", 1);
		}) as unknown as typeof Bun.spawn;
		const client = new GjcClient(database, 5_000, directory, undefined, { spawn, log: (line) => logs.push(line) });
		try {
			await expect(client.ensureSession("discord:dm:prose-create")).rejects.toMatchObject({
				code: "unsupported_state_version",
			});
			expect(calls).toBe(1);
			expect(logs).toEqual([]);
			expect(database.getSessionRecord("discord:dm:prose-create")?.epoch ?? 0).toBe(0);
		} finally {
			database.close();
		}
	}

	for (const message of ["session endpoint record is gone", "SDK startup did not complete"]) {
		const database = await makeDatabase("gajaeway-takeover-prose-turn-");
		const logs: string[] = [];
		let calls = 0;
		const spawn = ((_options: { cmd: string[] }) => {
			calls++;
			return calls === 1
				? fakeChild(createSuccess("session-e0"))
				: fakeChild(turnFailure("unsupported_state_version", message), "", 1);
		}) as unknown as typeof Bun.spawn;
		const client = new GjcClient(database, 5_000, directory, undefined, { spawn, log: (line) => logs.push(line) });
		try {
			const { sessionId } = await client.ensureSession("discord:dm:prose-turn");
			await expect(client.sendTurn(sessionId, "hello")).rejects.toMatchObject({ code: "unsupported_state_version" });
			expect(calls).toBe(2);
			expect(logs).toEqual([]);
			expect(database.getSessionRecord("discord:dm:prose-turn")).toEqual({ sessionId: "session-e0", epoch: 0 });
		} finally {
			database.close();
		}
	}
});

test("redaction covers quoted headers, URL/camelCase keys, control splits, and hostile codes", () => {
	const bearer = "0123456789abcdef0123456789abcdef01234567";
	const cases = [
		`{"Authorization":"Bearer ${bearer}"}`,
		"GET /v1/items?apikey=secret1value23&limit=1 failed",
		"apiKey=camelSecretValue123",
		"apiKey=secret1\u0000value23",
	];
	for (const message of cases) {
		const notice = formatFailureNotice(new GjcRuntimeError("wrapped", { code: "spawn_failed", message }));
		expect(notice).toContain("[redacted]");
	}
	const notice = formatFailureNotice(
		new GjcRuntimeError("wrapped", { code: "spawn_failed", message: cases[0] as string }),
	);
	expect(notice).not.toContain(bearer);
	expect(
		formatFailureNotice(new GjcRuntimeError("wrapped", { code: "spawn_failed", message: cases[1] as string })),
	).not.toContain("secret1value23");
	expect(
		formatFailureNotice(new GjcRuntimeError("wrapped", { code: "spawn_failed", message: cases[2] as string })),
	).not.toContain("camelSecretValue123");
	expect(
		formatFailureNotice(new GjcRuntimeError("wrapped", { code: "spawn_failed", message: cases[3] as string })),
	).not.toContain("secret1value23");

	const hostile = `sk_live_${"z".repeat(60)}`;
	const hostileNotice = formatFailureNotice(
		new GjcRuntimeError("wrapped", { code: hostile, message: "startup failed" }),
	);
	expect(hostileNotice).toContain("[redacted]");
	expect(hostileNotice).not.toContain(hostile);
	expect(hostileNotice.length).toBeLessThan(120);
});

test("streamed assistant text causes a rebind without replay and reports turn_not_replayed", async () => {
	const database = await makeDatabase("gajaeway-takeover-replay-");
	const logs: string[] = [];
	const commands: string[][] = [];
	let call = 0;
	const spawn = ((options: { cmd: string[] }) => {
		commands.push(options.cmd);
		call++;
		if (call === 1) return fakeChild(createSuccess("session-e0"));
		if (call === 2)
			return fakeChild(
				`${turnReply("partial streamed")}${turnFailure("managed_append_identity_mismatch", "identity moved")}`,
				"",
				1,
			);
		return fakeChild(createSuccess("session-e1"));
	}) as unknown as typeof Bun.spawn;
	const client = new GjcClient(database, 5_000, directory, undefined, { spawn, log: (line) => logs.push(line) });
	const streamed: string[] = [];
	try {
		const { sessionId } = await client.ensureSession("discord:dm:side-effect");
		const error = await client
			.sendTurn(sessionId, "do the side effect", undefined, undefined, {
				onAssistantText: (text) => streamed.push(text),
			})
			.catch((failure: unknown) => failure);
		expect(streamed).toEqual(["partial streamed"]);
		expect(error).toMatchObject({ code: "turn_not_replayed" });
		expect(formatFailureNotice(error)).toContain("turn_not_replayed");
		expect(formatFailureNotice(error)).not.toContain("/new");
		expect(database.getSessionRecord("discord:dm:side-effect")).toEqual({ sessionId: "session-e1", epoch: 1 });
		expect(commands).toHaveLength(3);
		expect(commands.filter((command) => command.includes("--resume"))).toHaveLength(1);
		expect(logs).toHaveLength(1);
	} finally {
		database.close();
	}
});

interface Frame {
	readonly type?: string;
	readonly id?: string;
	readonly event?: string;
	readonly result?: Record<string, unknown>;
}

interface Client {
	readonly frames: Frame[];
	send(value: unknown): void;
	close(): void;
}

async function connect(socketPath: string): Promise<Client> {
	const frames: Frame[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line) as Frame);
			},
		},
	});
	return { frames, send: (value) => socket.write(`${JSON.stringify(value)}\n`), close: () => socket.end() };
}

async function waitForFrame(frames: readonly Frame[], predicate: (frame: Frame) => boolean): Promise<Frame> {
	for (let attempt = 0; attempt < 600; attempt++) {
		const found = frames.find(predicate);
		if (found) return found;
		await Bun.sleep(5);
	}
	throw new Error(`timed out waiting for frame: ${JSON.stringify(frames)}`);
}

test("overlapping SIGHUP and verb reloads stay whole while a turn is debouncing", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-takeover-reload-race-"));
	const configPath = join(directory, "config.json");
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath,
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		debounceMs: 250,
		mentionAllowlist: ["owner"],
		channels: { "discord:c1": { debounceMs: 250 } },
	};
	await Bun.write(
		configPath,
		JSON.stringify({
			schemaVersion: 1,
			debounceMs: 250,
			mentionAllowlist: ["owner"],
			channels: { "discord:c1": { debounceMs: 250 } },
		}),
	);
	const database = await GatewayDatabase.open(config.dbPath);
	let turnStarts = 0;
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		forgetRebinds: () => {},
		sendTurn: async () => {
			turnStarts++;
			return "reply";
		},
	};
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitForFrame(client.frames, (frame) => frame.type === "negotiated");
	const send = (id: string, origin: { platform: string; kind: string; conversationId: string }, authorId: string) =>
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "chat.send",
			params: {
				origin,
				text: "hello",
				messageId: id,
				engagement: { mentioned: true, group: true, authorId },
			},
		});
	const c1 = { platform: "discord", kind: "channel", conversationId: "c1" } as const;
	send("debounce", c1, "owner");
	await waitForFrame(client.frames, (frame) => frame.type === "response" && frame.id === "debounce");
	await Bun.sleep(20);
	expect(turnStarts).toBe(0);

	const nextConfig = {
		schemaVersion: 1,
		debounceMs: 0,
		mentionAllowlist: ["new-owner"],
		channels: { "discord:c1": { engagement: "open", debounceMs: 0 } },
	};
	await Bun.write(configPath, JSON.stringify(nextConfig));
	process.kill(process.pid, "SIGHUP");
	process.kill(process.pid, "SIGHUP");
	client.send({ v: "0.1", type: "request", id: "reload-valid", verb: "gateway.reloadConfig" });
	const validReload = await waitForFrame(
		client.frames,
		(frame) => frame.type === "response" && frame.id === "reload-valid",
	);
	expect(validReload.result?.ok).toBe(true);
	await Bun.sleep(40);

	// This pair distinguishes all four possible mentionAllowlist/channels mixes:
	// c1 stranger needs the new open channel, while c2 owner must be rejected by
	// the new allowlist. A half-applied object fails one of these probes.
	send("new-open", c1, "stranger");
	send("new-gated", { platform: "discord", kind: "channel", conversationId: "c2" }, "owner");
	const openResult = await waitForFrame(client.frames, (frame) => frame.type === "response" && frame.id === "new-open");
	const gatedResult = await waitForFrame(
		client.frames,
		(frame) => frame.type === "response" && frame.id === "new-gated",
	);
	expect(openResult.result?.engaged).toBe(true);
	expect(gatedResult.result?.engaged).toBe(false);

	// A malformed edit is the failure branch: it must be a complete diagnostic,
	// not an ok response with one reloadable field partially applied. The previous
	// whole config remains observable through the same two probes.
	await Bun.write(configPath, "{ not json");
	client.send({ v: "0.1", type: "request", id: "reload-invalid", verb: "gateway.reloadConfig" });
	const invalidReload = await waitForFrame(
		client.frames,
		(frame) => frame.type === "response" && frame.id === "reload-invalid",
	);
	expect(invalidReload.result?.ok).toBe(false);
	expect(Array.isArray(invalidReload.result?.diagnostics)).toBe(true);
	send("still-open", c1, "stranger");
	send("still-gated", { platform: "discord", kind: "channel", conversationId: "c2" }, "owner");
	const stillOpen = await waitForFrame(
		client.frames,
		(frame) => frame.type === "response" && frame.id === "still-open",
	);
	const stillGated = await waitForFrame(
		client.frames,
		(frame) => frame.type === "response" && frame.id === "still-gated",
	);
	expect(stillOpen.result?.engaged).toBe(true);
	expect(stillGated.result?.engaged).toBe(false);
	for (const frame of client.frames.filter((candidate) => candidate.id?.startsWith("reload-"))) {
		expect(typeof frame.result?.ok).toBe("boolean");
		if (frame.result?.ok === false) expect((frame.result.diagnostics as unknown[]).length).toBeGreaterThan(0);
	}
	client.close();
});

test("terminal_uncertain is logged once and the next code is classified independently", async () => {
	const database = await makeDatabase("gajaeway-takeover-terminal-");
	const logs: string[] = [];
	let call = 0;
	const spawn = ((_options: { cmd: string[] }) => {
		call++;
		if (call === 1) return fakeChild(createFailure("terminal_uncertain", "cleanup could not be proven"), "", 1);
		if (call === 2) return fakeChild(createSuccess("session-e1"));
		if (call === 3)
			return fakeChild(createFailure("resource_gone", "terminal uncertain wording is not the code"), "", 1);
		return fakeChild(createSuccess("session-e3"));
	}) as unknown as typeof Bun.spawn;
	const client = new GjcClient(database, 5_000, directory, undefined, { spawn, log: (line) => logs.push(line) });
	try {
		await expect(client.ensureSession("discord:dm:terminal")).resolves.toEqual({ sessionId: "session-e1" });
		database.withTransaction(() =>
			database.bumpEpoch(
				"discord:dm:terminal",
				JSON.stringify({ platform: "discord", kind: "dm", conversationId: "terminal", peerId: "p" }),
			),
		);
		const epoch = database.getSessionRecord("discord:dm:terminal")?.epoch ?? 0;
		await expect(client.ensureSession("discord:dm:terminal", epoch)).resolves.toEqual({ sessionId: "session-e3" });
		expect(logs.filter((line) => line.includes("cause=terminal_uncertain"))).toHaveLength(1);
		expect(logs[0]).toContain("cause=terminal_uncertain epoch 0 -> 1");
		expect(logs[1]).toContain("cause=resource_gone epoch 2 -> 3");
		expect(database.getSessionRecord("discord:dm:terminal")).toEqual({ sessionId: "session-e3", epoch: 3 });
	} finally {
		database.close();
	}
});

test("a shared bind that fails rejects every waiter identically and later retries cleanly", async () => {
	const database = await makeDatabase("gajaeway-takeover-shared-reject-");
	let failing = true;
	const spawn = ((options: { cmd: string[] }) => {
		if (!options.cmd.includes("session.create")) throw new Error("unexpected turn spawn");
		return failing
			? fakeChild(createFailure("terminal_uncertain", "cleanup could not be proven"), "", 1)
			: fakeChild(createSuccess("session-ok"));
	}) as unknown as typeof Bun.spawn;
	const client = new GjcClient(database, 5_000, directory, undefined, { spawn });
	try {
		const first = client.ensureSession("discord:dm:shared", 0);
		const second = client.ensureSession("discord:dm:shared", 0);
		const [firstOutcome, secondOutcome] = await Promise.all([
			first.then(
				(value) => `resolved:${value.sessionId}`,
				(error: unknown) => `rejected:${(error as Error).message}`,
			),
			second.then(
				(value) => `resolved:${value.sessionId}`,
				(error: unknown) => `rejected:${(error as Error).message}`,
			),
		]);
		expect(firstOutcome).toBe(secondOutcome);
		expect(firstOutcome).toContain("terminal_uncertain");
		// The finally reaper must have cleared the settled entry: the next call
		// performs a NEW bind instead of replaying the rejected promise.
		failing = false;
		await expect(client.ensureSession("discord:dm:shared", 0)).resolves.toMatchObject({ sessionId: "session-ok" });
	} finally {
		database.close();
	}
});

test("a code-only envelope with secret-bearing stderr never logs the raw fallback", async () => {
	const database = await makeDatabase("gajaeway-takeover-codeonly-");
	const leaked = "sk_live_0123456789abcdef012345";
	const envelope = `${JSON.stringify({ ok: false, operation: "session.create", error: { code: "resource_gone" } })}\n`;
	const spawn = (() => fakeChild(envelope, `auth failed for ${leaked}`, 1)) as unknown as typeof Bun.spawn;
	const client = new GjcClient(database, 5_000, directory, undefined, { spawn });
	try {
		const error = await client.ensureSession("discord:dm:codeonly").catch((failure: unknown) => failure);
		expect(error).toMatchObject({ code: "resource_gone" });
		expect(String((error as Error).message)).not.toContain(leaked);
		expect((error as GjcRuntimeError).runtimeMessage).not.toContain(leaked);
	} finally {
		database.close();
	}
});

test("rebindEpoch resets turn_count so a recovery near rotation keeps its fresh binding", async () => {
	const database = await makeDatabase("gajaeway-takeover-rotation-");
	try {
		database.rebindEpoch("discord:dm:rotation");
		expect(database.incrementTurnCount("discord:dm:rotation")).toBe(1);
		database.incrementTurnCount("discord:dm:rotation");
		database.incrementTurnCount("discord:dm:rotation");
		// The /new-equivalent reset semantics: a rebinding key starts its count at
		// zero again rather than inheriting the dead epoch's count.
		database.rebindEpoch("discord:dm:rotation");
		expect(database.incrementTurnCount("discord:dm:rotation")).toBe(1);
	} finally {
		database.close();
	}
});

test("a pretty-printed multiline credential array is redacted whole", () => {
	// The bracketed-list rule used to stop at the first newline, so a
	// pretty-printed array shipped its entries line by line.
	const notice = formatFailureNotice(
		new GjcRuntimeError("wrapped", {
			code: "spawn_failed",
			message: `{"secrets":[\n"wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY"\n]}`,
		}),
	);
	expect(notice).not.toContain("wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY");
	expect(notice).toContain("[redacted]");
});

test("the rebind budget survives a gateway restart via durable counters", async () => {
	const database = await makeDatabase("gajaeway-takeover-durable-");
	const logs: string[] = [];
	const spawn = (() =>
		fakeChild(createFailure("resource_gone", "session endpoint record is gone"), "", 1)) as unknown as typeof Bun.spawn;
	try {
		const first = new GjcClient(database, 5_000, directory, undefined, { spawn, log: (line) => logs.push(line) });
		for (let attempt = 0; attempt < DEFAULT_REBIND_CAP; attempt++)
			await expect(first.ensureSession("discord:dm:durable")).rejects.toMatchObject({ code: "resource_gone" });
		database.close();
		// "Restart": a fresh client over the same durable store must inherit the
		// spent budget — a restart is not an unauthorized cap reset.
		const reopened = await GatewayDatabase.open(join(directory!, "gateway.db"));
		try {
			const second = new GjcClient(reopened, 5_000, directory, undefined, { spawn, log: (line) => logs.push(line) });
			await expect(second.ensureSession("discord:dm:durable")).rejects.toMatchObject({
				name: "RebindCapExceededError",
			});
		} finally {
			reopened.close();
		}
	} finally {
		database.close();
	}
});

test("a corrupted durable counter fails CLOSED: rebinds blocked, no epoch consumed, /new restores", async () => {
	const database = await makeDatabase("gajaeway-takeover-corrupt-");
	const logs: string[] = [];
	const spawn = (() =>
		fakeChild(createFailure("resource_gone", "session endpoint record is gone"), "", 1)) as unknown as typeof Bun.spawn;
	try {
		// Corrupt the durable counter BEFORE any hydration, as a crash mid-write would.
		database.metaSet("rebind_budget:discord:dm:c6", "{not-json-at-all");
		const client = new GjcClient(database, 5_000, directory, undefined, { spawn, log: (line) => logs.push(line) });
		await expect(client.ensureSession("discord:dm:c6")).rejects.toMatchObject({
			name: "RebindCapExceededError",
			code: "rebind_cap_exceeded",
		});
		// Fail-closed means NO epoch bump and NO create beyond the first attempt.
		expect(database.getSessionRecord("discord:dm:c6")?.epoch ?? 0).toBe(0);
		expect(logs.some((line) => line.includes("CORRUPT"))).toBe(true);
		// The hold is operator-visible with a usable remedy.
		const held = await client.ensureSession("discord:dm:c6").catch((failure: unknown) => failure);
		expect(formatFailureNotice(held)).toContain("/new");
		// /new is the explicit operator rewrite of the corrupted counter.
		client.forgetRebinds("discord:dm:c6");
		await expect(client.ensureSession("discord:dm:c6")).rejects.toMatchObject({ code: "resource_gone" });
		expect(database.getSessionRecord("discord:dm:c6")?.epoch).toBe(1);
	} finally {
		database.close();
	}
});

test("a credential array whose closing bracket lies past the bound still redacts whole", () => {
	// G4-B3: the 600-char work bound must not become an egress boundary. An
	// unterminated or over-long array redacts the whole bounded window, so the
	// early entry cannot ride through even though the `]` never arrives in time.
	const padded = `{"secrets":[\n"wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY",\n${"x".repeat(700)}\n]}`;
	const notice = formatFailureNotice(new GjcRuntimeError("wrapped", { code: "spawn_failed", message: padded }));
	expect(notice).not.toContain("wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY");
	expect(notice).toContain("[redacted]");
});

test("an empty durable counter value is corruption, not absence", async () => {
	const database = await makeDatabase("gajaeway-takeover-empty-");
	try {
		database.metaSet("rebind_budget:discord:dm:empty", "");
		const client = new GjcClient(database, 5_000, directory, undefined, { spawn: failingSpawn });
		await expect(client.ensureSession("discord:dm:empty")).rejects.toMatchObject({
			name: "RebindCapExceededError",
		});
		expect(database.getSessionRecord("discord:dm:empty")?.epoch ?? 0).toBe(0);
	} finally {
		database.close();
	}
});

test("/new is the first operation against a corrupt counter and restores service on the first reset", async () => {
	const database = await makeDatabase("gajaeway-takeover-corrupt-new-");
	const logs: string[] = [];
	try {
		database.metaSet("rebind_budget:discord:dm:c7", "{broken");
		const client = new GjcClient(database, 5_000, directory, undefined, {
			spawn: failingSpawn,
			log: (line) => logs.push(line),
		});
		// The operator's very first action is /new — it must rewrite the corrupt
		// counter and lift the hold WITHOUT needing a second reset or restart.
		client.forgetRebinds("discord:dm:c7");
		await expect(client.ensureSession("discord:dm:c7")).rejects.toMatchObject({ code: "resource_gone" });
		expect(database.getSessionRecord("discord:dm:c7")?.epoch).toBe(1);
	} finally {
		database.close();
	}
});

test("a restart followed by clear preserves the durable lifetime audit total", async () => {
	const database = await makeDatabase("gajaeway-takeover-lifetime-");
	const logs: string[] = [];
	const spawn = (() =>
		fakeChild(createFailure("resource_gone", "session endpoint record is gone"), "", 1)) as unknown as typeof Bun.spawn;
	try {
		const first = new GjcClient(database, 5_000, directory, undefined, { spawn, log: (line) => logs.push(line) });
		await expect(first.ensureSession("discord:dm:life")).rejects.toMatchObject({ code: "resource_gone" });
		expect(logs[0]).toContain("lifetime=1");
		database.close();
		// "Restart": the fresh client hydrates {used:1, lifetime:1}; a proven-good
		// turn clears `used` but the NEXT rebind must log lifetime=2, not 1.
		const reopened = await GatewayDatabase.open(join(directory!, "gateway.db"));
		try {
			const second = new GjcClient(reopened, 5_000, directory, undefined, { spawn, log: (line) => logs.push(line) });
			second.forgetRebinds("discord:dm:life");
			await expect(second.ensureSession("discord:dm:life")).rejects.toMatchObject({ code: "resource_gone" });
			expect(logs[logs.length - 1]).toContain("lifetime=2");
		} finally {
			reopened.close();
		}
	} finally {
		database.close();
	}
});

test("a quoted ] inside a credential array does not end the redaction early", () => {
	// G5-B1: the scanner must track quotes/escapes — a `]` inside a quoted value
	// cannot close the array and let the later generic credential through.
	const message = `{"secrets":["safe]text","wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY"]}`;
	const notice = formatFailureNotice(new GjcRuntimeError("wrapped", { code: "spawn_failed", message }));
	expect(notice).not.toContain("wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY");
	expect(notice).toContain("[redacted]");
});

test("a generic credential AFTER the old work bound is still redacted", () => {
	// The scanner is linear (no work cap), so a later entry beyond any fixed
	// window is redacted because the array's true close is found.
	const padded = `{"secrets":["${"x".repeat(800)}","wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY"]}`;
	const notice = formatFailureNotice(new GjcRuntimeError("wrapped", { code: "spawn_failed", message: padded }));
	expect(notice).not.toContain("wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY");
});

test("an unterminated credential array redacts to the end of the diagnostic", () => {
	const notice = formatFailureNotice(
		new GjcRuntimeError("wrapped", {
			code: "spawn_failed",
			message: `{"secrets":["wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY"`,
		}),
	);
	expect(notice).not.toContain("wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY");
	expect(notice).toContain("[redacted]");
});

test("a nested array's ] does not end credential-array redaction early", () => {
	// G5-B1 final form: bracket DEPTH is tracked, so a valid nested array before
	// a later generic credential cannot terminate the redaction at the inner ].
	const message = `{"secrets":[["safe"],"wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY"]}`;
	const notice = formatFailureNotice(new GjcRuntimeError("wrapped", { code: "spawn_failed", message }));
	expect(notice).not.toContain("wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY");
	expect(notice).toContain("[redacted]");
});

test("malformed nesting redacts to the end of the diagnostic", () => {
	const notice = formatFailureNotice(
		new GjcRuntimeError("wrapped", {
			code: "spawn_failed",
			message: `{"secrets":[["open","wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY"] later text`,
		}),
	);
	expect(notice).not.toContain("wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY");
	expect(notice).toContain("[redacted]");
});
