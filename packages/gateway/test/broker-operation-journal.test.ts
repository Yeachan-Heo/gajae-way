import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliResult, CliRunner } from "@gajae-gateway/subsession";
import { GjcCliUnavailableError } from "../src/orchestrator/broker";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner, type TailStreamSpawner } from "../src/orchestrator/tail-runner";
import { Store } from "../src/store";
import type { BrokerAuthority } from "../src/store/groups/bindings";
import { createOwnedSessionFixture, initializeTestBrokerAuthority, noRelay, scriptedRelay } from "./session-port.fake";

/**
 * Issue #568: every broker mutation is journaled intent-first, so a broker that
 * dies mid-operation and a gateway that restarts around it still apply each
 * operation once and hand the retry the original result. All state lives in a
 * per-test temp dir; no real broker or agent dir is touched.
 */

let home = "";
let database: Store | undefined;

afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

function ok(result: unknown): CliResult {
	return { exitCode: 0, stdout: JSON.stringify({ ok: true, result }), stderr: "" };
}

function required<T>(value: T | undefined, label: string): T {
	if (value === undefined) throw new Error(`missing ${label}`);
	return value;
}

/** A fake broker that keeps its own state across gateway restarts and can die right after applying an op. */
class FakeBroker {
	readonly sessions = new Map<string, string>();
	readonly closes = new Map<string, number>();
	readonly createKeys: string[] = [];
	readonly createInputs: string[] = [];
	readonly closeKeys: string[] = [];
	readonly closedSessionIds: string[] = [];
	readonly dead = new Set<string>();
	dieAfterApply = false;
	#sessions = 0;

	readonly cli: CliRunner = async (args) => {
		const key = args[args.indexOf("--idempotency-key") + 1] ?? "";
		if (args.includes("session.create")) {
			this.createKeys.push(key);
			this.createInputs.push(args[args.indexOf("--json-input") + 1] ?? "{}");
			const sessionId = this.sessions.get(key) ?? `broker-session-${++this.#sessions}`;
			this.sessions.set(key, sessionId);
			return this.#reply({ sessionId });
		}
		if (args.includes("session.close")) {
			this.closeKeys.push(key);
			// The broker's own idempotency: one key closes once.
			if (!this.closes.has(key)) {
				this.closes.set(key, this.closes.size + 1);
				const request = JSON.parse(args[args.indexOf("--json-input") + 1] ?? "{}") as { sessionId?: unknown };
				if (typeof request.sessionId === "string") this.closedSessionIds.push(request.sessionId);
			}
			return this.#reply({ closed: true, closeNumber: this.closes.get(key) });
		}
		if (args.includes("inspect")) {
			const sessionId = args[args.indexOf("inspect") + 1] ?? "";
			return ok({ session: { live: !this.dead.has(sessionId), deleted: this.dead.has(sessionId) } });
		}
		throw new Error(`unexpected command ${args.join(" ")}`);
	};

	#reply(result: unknown): CliResult {
		if (this.dieAfterApply) {
			this.dieAfterApply = false;
			throw new GjcCliUnavailableError("broker exited before replying");
		}
		return ok(result);
	}
}

async function openGateway(
	path: string,
	authority: BrokerAuthority,
	broker: FakeBroker,
	relay: TailStreamSpawner = noRelay,
) {
	database?.close();
	database = await Store.open(path);
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: broker.cli,
		tailRunner: new TailRunner({ stream: relay, repo: join(home, "workspace"), requestTimeoutMs: 50 }),
		sleep: async () => {},
	});
	return { database, port };
}

async function setup() {
	home = await mkdtemp(join(tmpdir(), "gajaeway-broker-journal-"));
	const path = join(home, "gateway.db");
	database = await Store.open(path);
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	return { path, authority, repo: join(home, "workspace") };
}

test("session.create interrupted after the broker applied it: the restarted gateway replays the same key and binds the one session", async () => {
	const { path, authority, repo } = await setup();
	const broker = new FakeBroker();
	let gateway = await openGateway(path, authority, broker);
	broker.dieAfterApply = true;
	await expect(gateway.port.bind({ originKey: "lane/a", epoch: 0, repo })).rejects.toThrow("broker_unavailable");
	expect(broker.sessions.size).toBe(1);
	// The intent survived the failure; the binding was never recorded.
	expect(
		gateway.database.bindings.brokerOperation(authority, "session.create", JSON.stringify(["lane/a", 0])),
	).toMatchObject({
		state: "pending",
		operationRef: broker.createKeys[0],
	});
	expect(gateway.database.bindings.getSessionRecord("lane/a")).toBeUndefined();

	gateway = await openGateway(path, authority, broker);
	const binding = await gateway.port.bind({ originKey: "lane/a", epoch: 0, repo });

	expect(broker.createKeys).toEqual([broker.createKeys[0]!, broker.createKeys[0]!]);
	expect(broker.sessions.size).toBe(1);
	expect(binding.sessionId).toBe(broker.sessions.get(broker.createKeys[0]!)!);
	expect(gateway.database.bindings.getSessionRecord("lane/a")).toMatchObject({
		sessionId: binding.sessionId,
		epoch: 0,
	});
	// Complete released the intent in the binding's transaction.
	expect(
		gateway.database.bindings.brokerOperation(authority, "session.create", JSON.stringify(["lane/a", 0])),
	).toBeUndefined();
});

test("session.create success without an ID retains its key so retry recovers the applied session", async () => {
	const { authority, repo } = await setup();
	const sessions = new Map<string, string>();
	const createKeys: string[] = [];
	let createAttempts = 0;
	const cli: CliRunner = async (args) => {
		if (args.includes("session.create")) {
			const key = args[args.indexOf("--idempotency-key") + 1] ?? "";
			createKeys.push(key);
			createAttempts++;
			const sessionId = sessions.get(key) ?? "sdk-created-once";
			sessions.set(key, sessionId);
			return createAttempts === 1 ? ok({}) : ok({ sessionId });
		}
		if (args.includes("inspect")) return ok({ session: { live: true, deleted: false, locator: { repo } } });
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const port = new BrokerSessionPort({
		database: database!,
		authority,
		cli,
		tailRunner: new TailRunner({ stream: noRelay, repo }),
		sleep: async () => {},
	});
	const scope = JSON.stringify(["lane/a", 0]);

	await expect(port.bind({ originKey: "lane/a", epoch: 0, repo })).rejects.toThrow(
		"session.create succeeded without a sessionId",
	);
	expect(database!.bindings.brokerOperation(authority, "session.create", scope)).toMatchObject({ state: "pending" });
	const binding = await port.bind({ originKey: "lane/a", epoch: 0, repo });

	expect(createKeys).toHaveLength(2);
	expect(createKeys[1]).toBe(createKeys[0]);
	expect(sessions.size).toBe(1);
	expect(binding.sessionId).toBe("sdk-created-once");
	expect(database!.bindings.brokerOperation(authority, "session.create", scope)).toBeUndefined();
});

test("a changed repository retires the pending create before binding a replacement session", async () => {
	const { path, authority, repo } = await setup();
	const broker = new FakeBroker();
	const gateway = await openGateway(path, authority, broker);
	broker.dieAfterApply = true;
	await expect(gateway.port.bind({ originKey: "lane/a", epoch: 0, repo })).rejects.toThrow("broker_unavailable");
	const originalKey = required(broker.createKeys[0], "original create key");
	const originalSessionId = required(broker.sessions.get(originalKey), "original session id");

	const changedRepo = join(home, "workspace-retargeted");
	const binding = await gateway.port.bind({ originKey: "lane/a", epoch: 0, repo: changedRepo });
	const replacementKey = required(broker.createKeys[2], "replacement create key");
	expect(broker.createKeys).toEqual([originalKey, originalKey, replacementKey]);
	expect(replacementKey).not.toBe(originalKey);
	expect(broker.closedSessionIds).toEqual([originalSessionId]);
	expect(binding.sessionId).toBe(required(broker.sessions.get(replacementKey), "replacement session id"));
	expect(binding.repo).toBe(changedRepo);
	expect(gateway.database.bindings.getSessionRecord("lane/a")).toMatchObject({
		sessionId: binding.sessionId,
		epoch: 0,
	});
	expect(broker.createInputs.map((input) => JSON.parse(input).cwd)).toEqual([repo, repo, changedRepo]);
	expect(
		gateway.database.bindings.brokerOperation(authority, "session.create", JSON.stringify(["lane/a", 0])),
	).toBeUndefined();
});

test("a changed startup model retires the pending create before certifying the replacement model", async () => {
	const { path, authority, repo } = await setup();
	const broker = new FakeBroker();
	const gateway = await openGateway(path, authority, broker);
	const originalModel = "anthropic/claude-sonnet";
	broker.dieAfterApply = true;
	await expect(gateway.port.bind({ originKey: "lane/a", epoch: 0, repo, model: originalModel })).rejects.toThrow(
		"broker_unavailable",
	);

	const binding = await gateway.port.bind({ originKey: "lane/a", epoch: 0, repo, model: "openai/gpt-4" });
	const originalKey = required(broker.createKeys[0], "original create key");
	const originalSessionId = required(broker.sessions.get(originalKey), "original session id");
	const replacementKey = required(broker.createKeys[2], "replacement create key");
	expect(broker.createKeys).toEqual([originalKey, originalKey, replacementKey]);
	expect(replacementKey).not.toBe(originalKey);
	expect(broker.closedSessionIds).toEqual([originalSessionId]);
	expect(JSON.parse(required(broker.createInputs[0], "original create request")).modelId).toBe(originalModel);
	expect(JSON.parse(required(broker.createInputs[2], "replacement create request")).modelId).toBe("openai/gpt-4");
	expect(binding.startupModelApplied).toBe(true);
	expect(binding.sessionId).toBe(required(broker.sessions.get(replacementKey), "replacement session id"));
});

test("a changed startup preset is created with the new preset after retiring the pending create", async () => {
	const { path, authority, repo } = await setup();
	const broker = new FakeBroker();
	const gateway = await openGateway(path, authority, broker);
	broker.dieAfterApply = true;
	await expect(gateway.port.bind({ originKey: "lane/a", epoch: 0, repo, model: { preset: "deep" } })).rejects.toThrow(
		"broker_unavailable",
	);

	const binding = await gateway.port.bind({ originKey: "lane/a", epoch: 0, repo, model: { preset: "fast" } });
	const originalKey = required(broker.createKeys[0], "original create key");
	const originalSessionId = required(broker.sessions.get(originalKey), "original session id");
	const replacementKey = required(broker.createKeys[2], "replacement create key");
	expect(broker.createKeys).toEqual([originalKey, originalKey, replacementKey]);
	expect(broker.closedSessionIds).toEqual([originalSessionId]);
	expect(JSON.parse(required(broker.createInputs[0], "original create request")).modelPreset).toBe("deep");
	expect(JSON.parse(required(broker.createInputs[2], "replacement create request")).modelPreset).toBe("fast");
	expect(binding.startupModelApplied).toBe(true);
	expect(binding.sessionId).toBe(required(broker.sessions.get(replacementKey), "replacement session id"));
});

test("a pending create outlives the intent age cutoff: a bind after a >24h outage replays its key, not a new session", async () => {
	const { path, authority, repo } = await setup();
	await createOwnedSessionFixture(database!, authority, { sessionId: "sdk-1", repo, originKey: "lane/c", epoch: 0 });
	const broker = new FakeBroker();
	let gateway = await openGateway(path, authority, broker);
	broker.dieAfterApply = true;
	await expect(gateway.port.bind({ originKey: "lane/a", epoch: 0, repo })).rejects.toThrow("broker_unavailable");
	const key = broker.createKeys[0]!;

	// The gateway returns 25h later; its first broker operation prunes the journal.
	database?.close();
	database = await Store.open(path);
	const later = new BrokerSessionPort({
		database,
		authority,
		cli: broker.cli,
		tailRunner: new TailRunner({ stream: noRelay, repo }),
		sleep: async () => {},
		now: () => Date.now() + 25 * 60 * 60_000,
	});
	gateway = { database, port: later };
	await gateway.port.close({ sessionId: "sdk-1", repo });
	expect(
		gateway.database.bindings.brokerOperation(authority, "session.create", JSON.stringify(["lane/a", 0])),
	).toMatchObject({ state: "pending", operationRef: key });

	const binding = await gateway.port.bind({ originKey: "lane/a", epoch: 0, repo });
	expect(broker.createKeys).toEqual([key, key]);
	expect(broker.sessions.size).toBe(1);
	expect(binding.sessionId).toBe(broker.sessions.get(key)!);
});

test("bind of a persisted binding surfaces an unavailable broker instead of reusing the session unchecked", async () => {
	const { authority, repo } = await setup();
	await createOwnedSessionFixture(database!, authority, { sessionId: "sdk-1", repo, originKey: "lane/a", epoch: 0 });
	const calls: string[] = [];
	const down: CliRunner = async (args) => {
		calls.push(args.join(" "));
		if (calls.length === 1) throw new GjcCliUnavailableError("broker restarting");
		return {
			exitCode: 1,
			stdout: JSON.stringify({ ok: false, error: { code: "broker_unavailable", message: "restarting" } }),
			stderr: "",
		};
	};
	const port = new BrokerSessionPort({
		database: database!,
		authority,
		cli: down,
		tailRunner: new TailRunner({ stream: noRelay, repo }),
		sleep: async () => {},
	});
	await expect(port.bind({ originKey: "lane/a", epoch: 0, repo })).rejects.toBeInstanceOf(GjcCliUnavailableError);
	await expect(port.bind({ originKey: "lane/a", epoch: 0, repo })).rejects.toBeInstanceOf(GjcCliUnavailableError);
	expect(calls).toEqual(["sdk session inspect sdk-1", "sdk session inspect sdk-1"]);
	// Neither a replacement create nor an epoch rotation happened.
	expect(database!.bindings.getSessionRecord("lane/a")).toMatchObject({ sessionId: "sdk-1", epoch: 0 });
});

test("bind preserves thrown and enveloped broker outages from saved-session resume liveness", async () => {
	const { authority, repo } = await setup();
	for (const [originKey, sessionId] of [
		["lane/throw", "sdk-throw"],
		["lane/envelope", "sdk-envelope"],
	] as const) {
		await createOwnedSessionFixture(database!, authority, { sessionId, repo, originKey, epoch: 0 });
	}
	const calls: string[] = [];
	const inspected = new Map<string, number>();
	const cli: CliRunner = async (args) => {
		calls.push(args.join(" "));
		if (!args.includes("inspect")) throw new Error(`unexpected command ${args.join(" ")}`);
		const sessionId = args[args.indexOf("inspect") + 1] ?? "";
		const count = (inspected.get(sessionId) ?? 0) + 1;
		inspected.set(sessionId, count);
		if (count === 1) return ok({ session: { live: false, deleted: false } });
		if (sessionId === "sdk-throw") throw new GjcCliUnavailableError("broker restarting");
		return {
			exitCode: 1,
			stdout: JSON.stringify({ ok: false, error: { code: "broker_unavailable", message: "supervisor restart" } }),
			stderr: "",
		};
	};
	const port = new BrokerSessionPort({
		database: database!,
		authority,
		cli,
		tailRunner: new TailRunner({ stream: noRelay, repo }),
		sleep: async () => {},
	});

	await expect(port.bind({ originKey: "lane/throw", epoch: 0, repo })).rejects.toBeInstanceOf(GjcCliUnavailableError);
	await expect(port.bind({ originKey: "lane/envelope", epoch: 0, repo })).rejects.toBeInstanceOf(
		GjcCliUnavailableError,
	);
	expect(calls).toEqual([
		"sdk session inspect sdk-throw",
		"sdk session inspect sdk-throw",
		"sdk session inspect sdk-envelope",
		"sdk session inspect sdk-envelope",
	]);
	expect(database!.bindings.getSessionRecord("lane/throw")).toMatchObject({ sessionId: "sdk-throw", epoch: 0 });
	expect(database!.bindings.getSessionRecord("lane/envelope")).toMatchObject({
		sessionId: "sdk-envelope",
		epoch: 0,
	});
});

test("cold-create readiness keeps the create intent until Router indexing catches up", async () => {
	const { authority, repo } = await setup();
	let creates = 0;
	let inspects = 0;
	const cli: CliRunner = async (args) => {
		if (args.includes("session.create")) {
			creates++;
			return ok({ sessionId: "cold-created-session" });
		}
		if (args.includes("inspect")) {
			inspects++;
			if (inspects === 1)
				return {
					exitCode: 1,
					stdout: JSON.stringify({ ok: false, error: { code: "broker_unavailable", message: "broker restarted" } }),
					stderr: "",
				};
			if (inspects === 2)
				return {
					exitCode: 1,
					stdout: JSON.stringify({ ok: false, error: { code: "session_unavailable", message: "not indexed yet" } }),
					stderr: "",
				};
			return ok({ session: { live: true, deleted: false } });
		}
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const port = new BrokerSessionPort({
		database: database!,
		authority,
		cli,
		tailRunner: new TailRunner({ stream: noRelay, repo }),
		sleep: async () => {},
	});

	await expect(port.bind({ originKey: "lane/cold", epoch: 0, repo })).rejects.toBeInstanceOf(GjcCliUnavailableError);
	expect(database!.bindings.getSessionRecord("lane/cold")).toMatchObject({
		sessionId: "cold-created-session",
		epoch: 0,
	});
	expect(
		database!.bindings.brokerOperation(authority, "session.create", JSON.stringify(["lane/cold", 0])),
	).toMatchObject({ state: "pending" });
	await expect(port.bind({ originKey: "lane/cold", epoch: 0, repo })).resolves.toMatchObject({
		sessionId: "cold-created-session",
		epoch: 0,
	});
	expect(creates).toBe(1);
	expect(inspects).toBe(3);
	expect(database!.bindings.getSessionRecord("lane/cold")).toMatchObject({ epoch: 0 });
	expect(
		database!.bindings.brokerOperation(authority, "session.create", JSON.stringify(["lane/cold", 0])),
	).toBeUndefined();
});

test("a changed startup model retires a pending cold binding before rotating its epoch", async () => {
	const { authority, repo } = await setup();
	const createKeys: string[] = [];
	const createInputs: string[] = [];
	const sessions = new Map<string, string>();
	const closed: string[] = [];
	let inspections = 0;
	const cli: CliRunner = async (args) => {
		if (args.includes("session.create")) {
			const key = args[args.indexOf("--idempotency-key") + 1] ?? "";
			createKeys.push(key);
			createInputs.push(args[args.indexOf("--json-input") + 1] ?? "{}");
			const sessionId = sessions.get(key) ?? `cold-session-${sessions.size + 1}`;
			sessions.set(key, sessionId);
			return ok({ sessionId });
		}
		if (args.includes("session.close")) {
			const request = JSON.parse(args[args.indexOf("--json-input") + 1] ?? "{}") as { sessionId?: string };
			if (request.sessionId) closed.push(request.sessionId);
			return ok({ closed: true });
		}
		if (args.includes("inspect")) {
			inspections++;
			if (inspections === 1)
				return {
					exitCode: 1,
					stdout: JSON.stringify({ ok: false, error: { code: "broker_unavailable", message: "broker restarted" } }),
					stderr: "",
				};
			return ok({ session: { live: true, deleted: false } });
		}
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const port = new BrokerSessionPort({
		database: database!,
		authority,
		cli,
		tailRunner: new TailRunner({ stream: noRelay, repo }),
		sleep: async () => {},
	});
	const originalModel = "anthropic/claude-sonnet";
	const changedRepo = join(home, "workspace-retargeted");

	await expect(
		port.bind({ originKey: "lane/cold-model", epoch: 0, repo, model: originalModel }),
	).rejects.toBeInstanceOf(GjcCliUnavailableError);
	const originalSessionId = required(sessions.get(createKeys[0]!), "original cold session id");
	const binding = await port.bind({
		originKey: "lane/cold-model",
		epoch: 0,
		repo: changedRepo,
		model: "openai/gpt-4",
	});

	expect(binding).toMatchObject({ epoch: 1 });
	expect(binding.sessionId).toBe(required(sessions.get(createKeys[1]!), "replacement cold session id"));
	expect(binding.repo).toBe(changedRepo);
	expect(createKeys).toHaveLength(2);
	expect(createKeys[1]).not.toBe(createKeys[0]);
	expect(closed).toEqual([originalSessionId]);
	expect(createInputs.map((input) => JSON.parse(input).modelId)).toEqual([originalModel, "openai/gpt-4"]);
	expect(createInputs.map((input) => JSON.parse(input).cwd)).toEqual([repo, changedRepo]);
	expect(inspections).toBe(2);
	expect(database!.bindings.getSessionRecord("lane/cold-model")).toMatchObject({
		epoch: 1,
		sessionId: binding.sessionId,
	});
});

test("resuming a session invalidates pending and completed closes before a later retirement", async () => {
	const { authority, repo } = await setup();
	const cases = [
		{ sessionId: "sdk-completed", originKey: "lane/completed", replyLost: false },
		{ sessionId: "sdk-pending", originKey: "lane/pending", replyLost: true },
	] as const;
	const transcriptDir = join(home, "agent", "sessions", "v2-scope");
	await mkdir(transcriptDir, { recursive: true });
	const live = new Map<string, boolean>();
	const closeAttempts = new Map<string, number>();
	const closeKeys = new Map<string, string[]>();
	for (const { sessionId, originKey } of cases) {
		await createOwnedSessionFixture(database!, authority, { sessionId, repo, originKey, epoch: 0 });
		await writeFile(join(transcriptDir, `2026-10-01T00-00-00-000Z_${sessionId}.jsonl`), "{}\n");
		live.set(sessionId, true);
		closeKeys.set(sessionId, []);
	}
	const cli: CliRunner = async (args) => {
		if (args.includes("session.close")) {
			const request = JSON.parse(args[args.indexOf("--json-input") + 1] ?? "{}") as { sessionId?: string };
			const sessionId = request.sessionId ?? "";
			closeKeys.get(sessionId)?.push(args[args.indexOf("--idempotency-key") + 1] ?? "");
			const attempts = (closeAttempts.get(sessionId) ?? 0) + 1;
			closeAttempts.set(sessionId, attempts);
			live.set(sessionId, false);
			if (sessionId === "sdk-pending" && attempts === 1)
				throw new GjcCliUnavailableError("broker restarted after close");
			return ok({ closed: true });
		}
		if (args.includes("session.resume")) {
			const request = JSON.parse(args[args.indexOf("--json-input") + 1] ?? "{}") as { sessionId?: string };
			expect(database!.bindings.brokerOperation(authority, "session.close", request.sessionId ?? "")).toBeUndefined();
			if (request.sessionId) live.set(request.sessionId, true);
			return ok({ resumed: true });
		}
		if (args.includes("inspect")) {
			const sessionId = args[args.indexOf("inspect") + 1] ?? "";
			return ok({
				session: {
					sessionId,
					live: live.get(sessionId) ?? false,
					deleted: false,
					locator: { repo },
					hostIncarnation: live.get(sessionId) ? "host-resumed" : "host-closed",
				},
			});
		}
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const port = new BrokerSessionPort({
		database: database!,
		authority,
		cli,
		tailRunner: new TailRunner({ stream: noRelay, repo }),
		sleep: async () => {},
	});

	for (const { sessionId, originKey, replyLost } of cases) {
		if (replyLost) await expect(port.close({ sessionId, repo })).rejects.toBeInstanceOf(GjcCliUnavailableError);
		else await port.close({ sessionId, repo });
		expect(database!.bindings.brokerOperation(authority, "session.close", sessionId)).toMatchObject({
			state: replyLost ? "pending" : "complete",
		});
		await expect(port.resume({ sessionId, repo, originKey, epoch: 0 })).resolves.toMatchObject({ sessionId });
		expect(database!.bindings.brokerOperation(authority, "session.close", sessionId)).toBeUndefined();
		await port.close({ sessionId, repo });
		const keys = closeKeys.get(sessionId) ?? [];
		expect(keys).toHaveLength(2);
		expect(keys[1]).not.toBe(keys[0]);
	}
});

test("resume removes a close intent before a broker-side resume can lose its reply", async () => {
	const { authority, repo } = await setup();
	const sessionId = "sdk-resume-reply-lost";
	const originKey = "lane/resume-reply-lost";
	await createOwnedSessionFixture(database!, authority, { sessionId, repo, originKey, epoch: 0 });
	const transcriptDir = join(home, "agent", "sessions", "v2-scope");
	await mkdir(transcriptDir, { recursive: true });
	await writeFile(join(transcriptDir, `2026-10-01T00-00-00-000Z_${sessionId}.jsonl`), "{}\n");
	database!.transaction((tx) =>
		database!.bindings.beginBrokerOperation(tx, authority, {
			operation: "session.close",
			scope: sessionId,
			operationRef: "gw-close-stale",
			requestJson: JSON.stringify({ sessionId }),
		}),
	);
	let live = false;
	let inspections = 0;
	let resumeAttempts = 0;
	const closeKeys: string[] = [];
	const cli: CliRunner = async (args) => {
		if (args.includes("session.resume")) {
			resumeAttempts++;
			expect(database!.bindings.brokerOperation(authority, "session.close", sessionId)).toBeUndefined();
			live = true;
			throw new GjcCliUnavailableError("broker restarted after applying resume");
		}
		if (args.includes("session.close")) {
			closeKeys.push(args[args.indexOf("--idempotency-key") + 1] ?? "");
			return ok({ closed: true });
		}
		if (args.includes("inspect")) {
			inspections++;
			if (inspections === 2)
				return {
					exitCode: 1,
					stdout: JSON.stringify({ ok: false, error: { code: "broker_unavailable", message: "broker restarting" } }),
					stderr: "",
				};
			return ok({
				session: {
					sessionId,
					live,
					deleted: false,
					locator: { repo },
					hostIncarnation: live ? "new-host" : "old-host",
				},
			});
		}
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const options = {
		database: database!,
		authority,
		cli,
		tailRunner: new TailRunner({ stream: noRelay, repo }),
		sleep: async () => {},
	};
	const input = { sessionId, repo, originKey, epoch: 0 };

	await expect(new BrokerSessionPort(options).resume(input)).rejects.toBeInstanceOf(GjcCliUnavailableError);
	expect(resumeAttempts).toBe(1);
	expect(database!.bindings.brokerOperation(authority, "session.close", sessionId)).toBeUndefined();
	await expect(new BrokerSessionPort(options).resume(input)).resolves.toMatchObject({ sessionId });
	await new BrokerSessionPort(options).close({ sessionId, repo });
	expect(resumeAttempts).toBe(1);
	expect(closeKeys).toHaveLength(1);
	expect(closeKeys[0]).not.toBe("gw-close-stale");
});

test("bind clears a stale pending close when inspection confirms a resumed host is live", async () => {
	const { authority, repo } = await setup();
	const sessionId = "sdk-live-after-resume";
	const originKey = "lane/live-after-resume";
	await createOwnedSessionFixture(database!, authority, { sessionId, repo, originKey, epoch: 0 });
	database!.transaction((tx) =>
		database!.bindings.beginBrokerOperation(tx, authority, {
			operation: "session.close",
			scope: sessionId,
			operationRef: "gw-close-stale",
			requestJson: JSON.stringify({ sessionId }),
		}),
	);
	const closeKeys: string[] = [];
	const cli: CliRunner = async (args) => {
		if (args.includes("inspect")) return ok({ session: { sessionId, live: true, deleted: false, locator: { repo } } });
		if (args.includes("session.close")) {
			closeKeys.push(args[args.indexOf("--idempotency-key") + 1] ?? "");
			return ok({ closed: true });
		}
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const port = new BrokerSessionPort({
		database: database!,
		authority,
		cli,
		tailRunner: new TailRunner({ stream: noRelay, repo }),
		sleep: async () => {},
	});

	await expect(port.bind({ originKey, epoch: 0, repo })).resolves.toMatchObject({ sessionId, epoch: 0 });
	expect(database!.bindings.brokerOperation(authority, "session.close", sessionId)).toBeUndefined();
	await port.close({ sessionId, repo });
	expect(closeKeys).toHaveLength(1);
	expect(closeKeys[0]).not.toBe("gw-close-stale");
});

test("a finished create is never served to a later bind after the lane's session died", async () => {
	const { path, authority, repo } = await setup();
	const broker = new FakeBroker();
	const gateway = await openGateway(path, authority, broker);
	const first = await gateway.port.bind({ originKey: "lane/a", epoch: 0, repo });
	broker.dead.add(first.sessionId);
	const second = await gateway.port.bind({ originKey: "lane/a", epoch: 0, repo });
	expect(broker.createKeys).toHaveLength(2);
	expect(broker.createKeys[1]).not.toBe(broker.createKeys[0]);
	expect(second.sessionId).not.toBe(first.sessionId);
	expect(second.epoch).toBe(1);
	const rows = new Database(path, { readonly: true });
	try {
		expect(rows.query("SELECT COUNT(*) AS n FROM broker_operations").get()).toEqual({ n: 0 });
	} finally {
		rows.close();
	}
});

test("turn.prompt accepted by the broker before the relay died is reconciled, not resent, and answers the same receipt", async () => {
	const { path, authority, repo } = await setup();
	await createOwnedSessionFixture(database!, authority, { sessionId: "sdk-1", repo, originKey: "lane/p", epoch: 0 });
	const accepted = new Map<string, { commandId: string; turnId: string }>();
	let dieOnPrompt = true;
	const relay = scriptedRelay((request) => {
		const clientRef = String(request.input.clientRef);
		if (request.operation === "turn.prompt") {
			if (accepted.has(clientRef)) return { ok: false, error: { code: "client_ref_conflict" } };
			const ids = { commandId: `cmd-${accepted.size + 1}`, turnId: `turn-${accepted.size + 1}` };
			accepted.set(clientRef, ids);
			if (dieOnPrompt) {
				dieOnPrompt = false;
				return new Promise(() => {});
			}
			return { ok: true, result: { accepted: true, clientRef, ...ids } };
		}
		if (request.operation === "turn.result") {
			const ids = accepted.get(clientRef);
			return { ok: true, result: ids ? { status: "in_flight", clientRef, ...ids } : { status: "unknown" } };
		}
		return { ok: false, error: { code: "unsupported_operation" } };
	});
	const broker = new FakeBroker();
	let gateway = await openGateway(path, authority, broker, relay.spawn);
	const send = { sessionId: "sdk-1", repo, text: "do the work", opRef: "gw-work-a-1" };
	await expect(gateway.port.send(send)).rejects.toThrow();
	expect(accepted.size).toBe(1);

	gateway = await openGateway(path, authority, broker, relay.spawn);
	const replayed = await gateway.port.send(send);
	expect(replayed).toMatchObject({
		sessionId: "sdk-1",
		operationRef: "gw-work-a-1",
		commandId: "cmd-1",
		turnId: "turn-1",
	});
	expect(accepted.size).toBe(1);
	const prompts = relay.requests.filter((request) => request.operation === "turn.prompt");
	expect(prompts).toHaveLength(1);

	const before = relay.requests.length;
	expect(await gateway.port.send(send)).toEqual(replayed);
	expect(relay.requests).toHaveLength(before);
	await expect(gateway.port.send({ ...send, text: "something else" })).rejects.toThrow("already used");
	// Same text, but a different preamble or model is a different turn: the
	// journal must refuse it rather than answer it with the stored receipt.
	await expect(gateway.port.send({ ...send, systemPreamble: "policy v2" })).rejects.toThrow("already used");
	await expect(gateway.port.send({ ...send, model: "anthropic/claude-opus" })).rejects.toThrow("already used");
	await expect(gateway.port.send({ ...send, model: { preset: "deep" } })).rejects.toThrow("already used");
	expect(relay.requests).toHaveLength(before);
});

test("a fresh prompt intent is released when relay attachment fails before submission", async () => {
	const { authority, repo } = await setup();
	await createOwnedSessionFixture(database!, authority, {
		sessionId: "sdk-1",
		repo,
		originKey: "lane/attach",
		epoch: 0,
	});
	let failFirstAttach = true;
	const relay = scriptedRelay((request) => {
		if (request.operation === "turn.prompt")
			return { ok: true, result: { accepted: true, clientRef: request.input.clientRef, commandId: "cmd-1" } };
		return { ok: false, error: { code: "unsupported_operation" } };
	});
	const stream: TailStreamSpawner = (sessionId) => {
		if (failFirstAttach) {
			failFirstAttach = false;
			throw new Error("relay attachment failed");
		}
		return relay.spawn(sessionId);
	};
	const port = new BrokerSessionPort({
		database: database!,
		authority,
		cli: async (args) => {
			throw new Error(`unexpected CLI command ${args.join(" ")}`);
		},
		tailRunner: new TailRunner({ stream, repo, requestTimeoutMs: 50 }),
		sleep: async () => {},
	});
	const send = { sessionId: "sdk-1", repo, text: "do the work", opRef: "gw-work-attach-1" };
	const scope = JSON.stringify([send.sessionId, send.opRef]);

	await expect(port.send(send)).rejects.toThrow("relay attachment failed");
	expect(database!.bindings.brokerOperation(authority, "turn.prompt", scope)).toBeUndefined();
	await expect(port.send(send)).resolves.toMatchObject({ operationRef: send.opRef, commandId: "cmd-1" });
	expect(relay.requests.filter((request) => request.operation === "turn.prompt")).toHaveLength(1);
	expect(database!.bindings.brokerOperation(authority, "turn.prompt", scope)).toMatchObject({ state: "complete" });
});

test("a fresh prompt intent is released when model selection fails before submission", async () => {
	const { authority, repo } = await setup();
	await createOwnedSessionFixture(database!, authority, {
		sessionId: "sdk-1",
		repo,
		originKey: "lane/model",
		epoch: 0,
	});
	let modelSetAttempts = 0;
	const cli: CliRunner = async (args) => {
		if (args.includes("model.set")) {
			modelSetAttempts++;
			if (modelSetAttempts === 1)
				return {
					exitCode: 1,
					stdout: JSON.stringify({ ok: false, error: { code: "broker_unavailable", message: "model control failed" } }),
					stderr: "",
				};
			return ok({ changed: true });
		}
		throw new Error(`unexpected CLI command ${args.join(" ")}`);
	};
	const relay = scriptedRelay((request) => {
		if (request.operation === "turn.prompt")
			return { ok: true, result: { accepted: true, clientRef: request.input.clientRef, commandId: "cmd-1" } };
		return { ok: false, error: { code: "unsupported_operation" } };
	});
	const port = new BrokerSessionPort({
		database: database!,
		authority,
		cli,
		tailRunner: new TailRunner({ stream: relay.spawn, repo, requestTimeoutMs: 50 }),
		sleep: async () => {},
	});
	const send = {
		sessionId: "sdk-1",
		repo,
		text: "do the work",
		opRef: "gw-work-model-1",
		model: "anthropic/claude-sonnet",
	} as const;
	const scope = JSON.stringify([send.sessionId, send.opRef]);

	await expect(port.send(send)).rejects.toThrow("broker_unavailable");
	expect(database!.bindings.brokerOperation(authority, "turn.prompt", scope)).toBeUndefined();
	expect(relay.requests.filter((request) => request.operation === "turn.prompt")).toHaveLength(0);
	await expect(port.send(send)).resolves.toMatchObject({ operationRef: send.opRef, commandId: "cmd-1" });
	expect(modelSetAttempts).toBe(2);
	expect(relay.requests.filter((request) => request.operation === "turn.prompt")).toHaveLength(1);
	expect(database!.bindings.brokerOperation(authority, "turn.prompt", scope)).toMatchObject({ state: "complete" });
});

test("an aged pending turn.prompt with unknown status stays uncertain and is never resent", async () => {
	const { path, authority, repo } = await setup();
	await createOwnedSessionFixture(database!, authority, { sessionId: "sdk-1", repo, originKey: "lane/q", epoch: 0 });
	let promptEffects = 0;
	const relay = scriptedRelay((request) => {
		const clientRef = String(request.input.clientRef);
		if (request.operation === "turn.prompt") {
			promptEffects++;
			if (promptEffects === 1) return new Promise(() => {});
			return { ok: true, result: { accepted: true, clientRef, commandId: "cmd-1", turnId: "turn-1" } };
		}
		if (request.operation === "turn.result") return { ok: true, result: { status: "unknown" } };
		return { ok: false, error: { code: "unsupported_operation" } };
	});
	const broker = new FakeBroker();
	let gateway = await openGateway(path, authority, broker, relay.spawn);
	const send = { sessionId: "sdk-1", repo, text: "do the work", opRef: "gw-work-q-1" };
	await expect(gateway.port.send(send)).rejects.toThrow();
	expect(promptEffects).toBe(1);
	database?.close();
	database = await Store.open(path);
	const later = new BrokerSessionPort({
		database,
		authority,
		cli: broker.cli,
		tailRunner: new TailRunner({ stream: relay.spawn, repo, requestTimeoutMs: 50 }),
		sleep: async () => {},
		now: () => Date.now() + 25 * 60 * 60_000,
	});
	await expect(later.send(send)).rejects.toThrow("status is unknown; refusing to resend");
	expect(promptEffects).toBe(1);
	expect(relay.requests.map((request) => request.operation)).toEqual(["turn.prompt", "turn.result"]);
	expect(
		database.bindings.brokerOperation(authority, "turn.prompt", JSON.stringify(["sdk-1", send.opRef])),
	).toMatchObject({ state: "pending", operationRef: send.opRef });
});

test("session.close interrupted after the broker applied it replays its key once, then answers from the journal", async () => {
	const { path, authority, repo } = await setup();
	await createOwnedSessionFixture(database!, authority, { sessionId: "sdk-1", repo, originKey: "lane/c", epoch: 0 });
	const broker = new FakeBroker();
	let gateway = await openGateway(path, authority, broker);
	broker.dieAfterApply = true;
	await expect(gateway.port.close({ sessionId: "sdk-1", repo })).rejects.toThrow("broker_unavailable");

	gateway = await openGateway(path, authority, broker);
	await gateway.port.close({ sessionId: "sdk-1", repo });
	expect(broker.closeKeys).toEqual([broker.closeKeys[0]!, broker.closeKeys[0]!]);
	expect(broker.closes.size).toBe(1);

	await gateway.port.close({ sessionId: "sdk-1", repo });
	expect(broker.closeKeys).toHaveLength(2);
	expect(gateway.database.bindings.brokerOperation(authority, "session.close", "sdk-1")).toMatchObject({
		state: "complete",
		resultJson: JSON.stringify({ closed: true, closeNumber: 1 }),
	});
});

// Store group: the bindings.ts broker-operation methods, driven directly.

const at = (offsetMs: number) => new Date(Date.now() + offsetMs).toISOString();

function journalRows(path: string): Array<{ operation: string; scope: string; state: string }> {
	const rows = new Database(path, { readonly: true });
	try {
		return rows
			.query("SELECT operation, scope, state FROM broker_operations ORDER BY operation, scope")
			.all() as Array<{ operation: string; scope: string; state: string }>;
	} finally {
		rows.close();
	}
}

test("beginBrokerOperation records the intent once and hands every later begin the original row", async () => {
	const { authority } = await setup();
	const store = database!;
	const first = store.transaction((tx) =>
		store.bindings.beginBrokerOperation(tx, authority, {
			operation: "turn.prompt",
			scope: "s/1",
			operationRef: "ref-1",
			requestJson: '{"prompt":"a"}',
		}),
	);
	expect(first).toEqual({
		operation: "turn.prompt",
		scope: "s/1",
		operationRef: "ref-1",
		requestJson: '{"prompt":"a"}',
		state: "pending",
		resumed: false,
	});
	const again = store.transaction((tx) =>
		store.bindings.beginBrokerOperation(tx, authority, {
			operation: "turn.prompt",
			scope: "s/1",
			operationRef: "ref-2",
			requestJson: '{"prompt":"b"}',
		}),
	);
	expect(again).toEqual({ ...first, resumed: true });
	// The same scope under another operation kind is a separate row.
	expect(
		store.transaction((tx) =>
			store.bindings.beginBrokerOperation(tx, authority, {
				operation: "session.close",
				scope: "s/1",
				operationRef: "ref-3",
				requestJson: "{}",
			}),
		).resumed,
	).toBe(false);
	const foreign = { canonicalAgentDir: "/elsewhere", identity: "gjc:/elsewhere" };
	expect(() =>
		store.transaction((tx) =>
			store.bindings.beginBrokerOperation(tx, foreign, {
				operation: "turn.prompt",
				scope: "s/9",
				operationRef: "ref-9",
				requestJson: "{}",
			}),
		),
	).toThrow();
	expect(store.bindings.brokerOperation(authority, "turn.prompt", "s/9")).toBeUndefined();
});

test("completeBrokerOperation stores the result only for the pending row of the same ref", async () => {
	const { authority } = await setup();
	const store = database!;
	const begin = () =>
		store.transaction((tx) =>
			store.bindings.beginBrokerOperation(tx, authority, {
				operation: "session.close",
				scope: "sdk-1",
				operationRef: "gw-close-1",
				requestJson: "{}",
			}),
		);
	begin();
	expect(() =>
		store.transaction((tx) =>
			store.bindings.completeBrokerOperation(tx, authority, "session.close", "sdk-1", "gw-close-other", "{}"),
		),
	).toThrow("is not pending");
	store.transaction((tx) =>
		store.bindings.completeBrokerOperation(tx, authority, "session.close", "sdk-1", "gw-close-1", '{"closed":true}'),
	);
	expect(store.bindings.brokerOperation(authority, "session.close", "sdk-1")).toEqual({
		operation: "session.close",
		scope: "sdk-1",
		operationRef: "gw-close-1",
		requestJson: "{}",
		state: "complete",
		resultJson: '{"closed":true}',
		resumed: true,
	});
	expect(begin()).toMatchObject({ state: "complete", resultJson: '{"closed":true}', resumed: true });
	// A completed row cannot be completed again with another answer.
	expect(() =>
		store.transaction((tx) =>
			store.bindings.completeBrokerOperation(tx, authority, "session.close", "sdk-1", "gw-close-1", '{"closed":false}'),
		),
	).toThrow("is not pending");
	expect(store.bindings.brokerOperation(authority, "session.close", "sdk-1")?.resultJson).toBe('{"closed":true}');
});

test("releaseBrokerOperation drops only the row of the exact ref, pending or complete", async () => {
	const { authority } = await setup();
	const store = database!;
	for (const [operation, scope, ref] of [
		["session.create", "c/1", "gw-bind-1"],
		["turn.prompt", "p/1", "ref-p"],
	] as const)
		store.transaction((tx) =>
			store.bindings.beginBrokerOperation(tx, authority, { operation, scope, operationRef: ref, requestJson: "{}" }),
		);
	store.transaction((tx) =>
		store.bindings.completeBrokerOperation(tx, authority, "turn.prompt", "p/1", "ref-p", '{"ok":1}'),
	);
	store.transaction((tx) => store.bindings.releaseBrokerOperation(tx, authority, "session.create", "c/1", "gw-bind-2"));
	expect(store.bindings.brokerOperation(authority, "session.create", "c/1")?.operationRef).toBe("gw-bind-1");
	store.transaction((tx) => store.bindings.releaseBrokerOperation(tx, authority, "session.create", "c/1", "gw-bind-1"));
	store.transaction((tx) => store.bindings.releaseBrokerOperation(tx, authority, "turn.prompt", "p/1", "ref-p"));
	expect(store.bindings.brokerOperation(authority, "session.create", "c/1")).toBeUndefined();
	expect(store.bindings.brokerOperation(authority, "turn.prompt", "p/1")).toBeUndefined();
	// After release the scope takes a fresh intent.
	expect(
		store.transaction((tx) =>
			store.bindings.beginBrokerOperation(tx, authority, {
				operation: "session.create",
				scope: "c/1",
				operationRef: "gw-bind-3",
				requestJson: "{}",
			}),
		),
	).toMatchObject({ operationRef: "gw-bind-3", resumed: false });
});

test("pruneBrokerOperations honors each cutoff and retains pending creates and prompts at any age", async () => {
	const { path, authority } = await setup();
	const store = database!;
	const begin = (operation: "session.create" | "turn.prompt" | "session.close", scope: string) =>
		store.transaction((tx) =>
			store.bindings.beginBrokerOperation(tx, authority, {
				operation,
				scope,
				operationRef: `ref-${scope}`,
				requestJson: "{}",
			}),
		);
	begin("session.create", "create-pending");
	begin("turn.prompt", "prompt-pending");
	begin("session.close", "close-pending");
	begin("turn.prompt", "prompt-done");
	begin("session.close", "close-done");
	for (const [operation, scope] of [
		["turn.prompt", "prompt-done"],
		["session.close", "close-done"],
	] as const)
		store.transaction((tx) =>
			store.bindings.completeBrokerOperation(tx, authority, operation, scope, `ref-${scope}`, "{}"),
		);
	const prune = (completedBefore: string, pendingBefore: string) =>
		store.transaction((tx) => store.bindings.pruneBrokerOperations(tx, { completedBefore, pendingBefore }));

	// Cutoffs before every row: nothing is old enough.
	expect(prune(at(-60_000), at(-60_000))).toBe(0);
	expect(journalRows(path)).toHaveLength(5);

	// Only the completed cutoff has passed: completed rows go, every pending intent stays.
	expect(prune(at(60_000), at(-60_000))).toBe(2);
	expect(journalRows(path)).toEqual([
		{ operation: "session.close", scope: "close-pending", state: "pending" },
		{ operation: "session.create", scope: "create-pending", state: "pending" },
		{ operation: "turn.prompt", scope: "prompt-pending", state: "pending" },
	]);

	// Only pending close intents expire; a prompt's missing reconciliation record never authorizes a resend.
	expect(prune(at(60_000), at(60_000))).toBe(1);
	expect(journalRows(path)).toEqual([
		{ operation: "session.create", scope: "create-pending", state: "pending" },
		{ operation: "turn.prompt", scope: "prompt-pending", state: "pending" },
	]);
	expect(prune(at(365 * 24 * 60 * 60_000), at(365 * 24 * 60 * 60_000))).toBe(0);
	expect(store.bindings.brokerOperation(authority, "session.create", "create-pending")).toMatchObject({
		state: "pending",
		operationRef: "ref-create-pending",
	});
});
