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
