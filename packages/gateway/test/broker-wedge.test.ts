import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	BROKER_HEARTBEAT_TTL_MS,
	type BrokerLivenessVerdict,
	describeBindHold,
	judgeBrokerLiveness,
} from "../src/orchestrator/broker-liveness";
import {
	BIND_WEDGE_PROBE_STRIKES,
	type PersonaBindHoldInput,
	PersonaSessionManager,
} from "../src/orchestrator/persona-session";
import {
	BrokerSessionPort,
	MAX_POISONED_CREATE_ROTATIONS,
	type SessionBindInput,
} from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";
import { ScriptedSessionPort } from "./session-port.fake";

const ORIGIN = { platform: "discord", kind: "channel", conversationId: "broker-wedge" } as const;
const ORIGIN_KEY = "discord/channel/broker-wedge";
const REPO = "/tmp/gajaeway-broker-wedge-repo";
const NOW = Date.parse("2026-09-06T00:00:00.000Z");

const UNAVAILABLE = "gjc sdk request failed: unavailable";
const FROZEN_AT = Date.parse("2026-09-05T16:46:00.000Z");
const WEDGED: BrokerLivenessVerdict = {
	state: "wedged",
	reason: "pid_dead",
	pid: 1071147,
	heartbeatAt: FROZEN_AT,
};

const directories: string[] = [];
let database: GatewayDatabase | undefined;
let manager: PersonaSessionManager | undefined;

function discoveryBody(pid: number, heartbeatAt: number): Record<string, unknown> {
	return {
		protocolVersion: 3,
		host: "127.0.0.1",
		url: "ws://127.0.0.1:43123",
		token: "test-token",
		pid,
		heartbeatAt,
	};
}

async function writeDiscovery(home: string, body: unknown): Promise<string> {
	const path = join(home, "sdk", "broker.json");
	await mkdir(join(home, "sdk"), { recursive: true });
	await writeFile(path, JSON.stringify(body));
	return path;
}

async function eventually(predicate: () => boolean, message: string, attempts = 200): Promise<void> {
	for (let attempt = 0; attempt < attempts; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

async function closeFixtures(): Promise<void> {
	await manager?.stop();
	database?.close();
	manager = undefined;
	database = undefined;
	await Promise.all(directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
}

afterEach(closeFixtures);

test("judgeBrokerLiveness classifies absent, malformed, dead, stale, and live discovery records", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-broker-wedge-liveness-"));
	directories.push(home);
	const alive = () => true;
	const dead = () => false;

	expect(await judgeBrokerLiveness(join(home, "missing", "broker.json"), alive, NOW)).toEqual({ state: "absent" });
	const malformed = join(home, "malformed.json");
	await writeFile(malformed, "not json");
	expect(await judgeBrokerLiveness(malformed, alive, NOW)).toEqual({ state: "absent" });

	const deadPath = await writeDiscovery(home, discoveryBody(7001, NOW - BROKER_HEARTBEAT_TTL_MS - 1));
	expect(await judgeBrokerLiveness(deadPath, dead, NOW)).toEqual({
		state: "wedged",
		reason: "pid_dead",
		pid: 7001,
		heartbeatAt: NOW - BROKER_HEARTBEAT_TTL_MS - 1,
	});

	const stalePath = await writeDiscovery(home, discoveryBody(7002, NOW - BROKER_HEARTBEAT_TTL_MS - 1));
	expect(await judgeBrokerLiveness(stalePath, alive, NOW)).toEqual({
		state: "wedged",
		reason: "heartbeat_stale",
		pid: 7002,
		heartbeatAt: NOW - BROKER_HEARTBEAT_TTL_MS - 1,
	});

	const boundaryPath = await writeDiscovery(home, discoveryBody(7003, NOW - BROKER_HEARTBEAT_TTL_MS));
	expect(await judgeBrokerLiveness(boundaryPath, alive, NOW)).toEqual({
		state: "live",
		pid: 7003,
		heartbeatAt: NOW - BROKER_HEARTBEAT_TTL_MS,
	});
	await writeFile(boundaryPath, JSON.stringify(discoveryBody(7003, NOW - BROKER_HEARTBEAT_TTL_MS + 1)));
	expect(await judgeBrokerLiveness(boundaryPath, alive, NOW)).toEqual({
		state: "live",
		pid: 7003,
		heartbeatAt: NOW - BROKER_HEARTBEAT_TTL_MS + 1,
	});

	await writeFile(boundaryPath, JSON.stringify(discoveryBody(7003, NOW - BROKER_HEARTBEAT_TTL_MS - 1)));
	expect(await judgeBrokerLiveness(boundaryPath, alive, NOW)).toEqual({
		state: "wedged",
		reason: "heartbeat_stale",
		pid: 7003,
		heartbeatAt: NOW - BROKER_HEARTBEAT_TTL_MS - 1,
	});

	const invalidShape = await writeDiscovery(home, { pid: 7004, heartbeatAt: NOW });
	expect(await judgeBrokerLiveness(invalidShape, alive, NOW)).toEqual({ state: "absent" });
});

test("judgeBrokerLiveness absorbs pid probe failures as absent", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-broker-wedge-probe-"));
	directories.push(home);
	const path = await writeDiscovery(home, discoveryBody(7005, NOW));
	expect(await judgeBrokerLiveness(path, () => Promise.reject(new Error("probe failed")), NOW)).toEqual({
		state: "absent",
	});
});

test("describeBindHold names wedged, absent, live, and unknown broker causes", () => {
	const detail = UNAVAILABLE;
	const wedged = describeBindHold(WEDGED, detail, BIND_WEDGE_PROBE_STRIKES);
	expect(wedged.reason).toBe("broker_wedged");
	expect(wedged.notice).toContain("sdk unavailable / broker wedged since 2026-09-05T16:46:00.000Z");
	expect(wedged.notice).toContain("pid 1071147 is dead");
	expect(wedged.notice).toContain(`${BIND_WEDGE_PROBE_STRIKES} consecutive bind failures: ${UNAVAILABLE}`);
	expect(wedged.notice.startsWith("[turn held]")).toBe(true);
	expect(wedged.notice).not.toContain("Prompt submission failed");

	const stale = describeBindHold(
		{ state: "wedged", reason: "heartbeat_stale", pid: 9, heartbeatAt: FROZEN_AT },
		detail,
		BIND_WEDGE_PROBE_STRIKES + 1,
	);
	expect(stale.notice).toContain("pid 9 stopped heartbeating");

	const absent = describeBindHold({ state: "absent" }, detail, BIND_WEDGE_PROBE_STRIKES);
	expect(absent.reason).toBe("broker_discovery_absent");
	expect(absent.notice).toContain("broker discovery absent");

	const live = describeBindHold({ state: "live", pid: 9, heartbeatAt: FROZEN_AT }, detail, BIND_WEDGE_PROBE_STRIKES);
	expect(live.reason).toBe("sdk_unavailable");
	expect(live.notice).toContain("broker daemon is live");

	const unknown = describeBindHold(undefined, detail, BIND_WEDGE_PROBE_STRIKES);
	expect(unknown.reason).toBe("sdk_unavailable");
	expect(unknown.notice).toContain("liveness unknown");
});

class FailingBindPort extends ScriptedSessionPort {
	bindAttempts = 0;
	readonly bindEpochs: number[] = [];
	failure: string | undefined = UNAVAILABLE;

	constructor() {
		super({ onSend: (input, scripted) => scripted.complete(input.opRef, "recovered") });
	}

	async bind(input: SessionBindInput) {
		this.bindAttempts += 1;
		this.bindEpochs.push(input.epoch);
		if (this.failure) throw new Error(this.failure);
		return await super.bind(input);
	}
}

type RetryTimer = { readonly work: () => void; readonly delayMs: number };

async function setupPersona(verdicts: readonly BrokerLivenessVerdict[]) {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-broker-wedge-persona-"));
	directories.push(home);
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new FailingBindPort();
	const logs: string[] = [];
	const holds: PersonaBindHoldInput[] = [];
	const timers: RetryTimer[] = [];
	const terminal: string[] = [];
	let probes = 0;
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "broker-wedge-test",
		repo: REPO,
		setTimeout: (work, delayMs) => {
			const timer = { work, delayMs };
			timers.push(timer);
			return timer;
		},
		clearTimeout: () => {},
		brokerLiveness: async () => {
			const verdict = verdicts[Math.min(probes++, verdicts.length - 1)];
			if (!verdict) throw new Error("missing scripted broker verdict");
			return verdict;
		},
		onBindHold: (input) => {
			holds.push(input);
		},
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onTerminal: ({ text }) => {
				terminal.push(text);
			},
		}),
		log: (line) => logs.push(line),
	});
	const accepted = database.inboundEnqueue({
		messageId: "m-broker-wedge",
		originKey: ORIGIN_KEY,
		originRefJson: JSON.stringify(ORIGIN),
		body: "hello",
		receivedAt: new Date(NOW).toISOString(),
	});
	expect(accepted).toBe(true);
	return { port, logs, holds, timers, terminal, probes: () => probes };
}

async function failPersonaTimes(fixture: Awaited<ReturnType<typeof setupPersona>>, count: number): Promise<void> {
	for (let index = 0; index < count; index++) {
		const before = fixture.port.bindAttempts;
		if (before === 0) {
			const current = manager;
			if (!current) throw new Error("persona manager is not initialized");
			await current.notifyInbound(ORIGIN_KEY);
		} else {
			const timer = fixture.timers[fixture.timers.length - 1];
			if (!timer) throw new Error("persona retry timer is missing");
			timer.work();
		}
		await eventually(() => fixture.port.bindAttempts === before + 1, `bind ${before + 1} did not run`);
		await eventually(() => fixture.timers.length === before + 1, `retry ${before + 1} was not armed`);
	}
}

test("a wedged broker holds the pending trigger at the backoff ceiling and recovers without epoch rotation", async () => {
	const fixture = await setupPersona([WEDGED]);
	await failPersonaTimes(fixture, BIND_WEDGE_PROBE_STRIKES - 1);
	expect(fixture.probes()).toBe(0);
	expect(fixture.holds).toEqual([]);

	await failPersonaTimes(fixture, 1);
	expect(fixture.probes()).toBe(1);
	expect(fixture.holds).toHaveLength(1);
	const hold = fixture.holds[0];
	if (!hold) throw new Error("wedged bind hold was not emitted");
	expect(hold.originKey).toBe(ORIGIN_KEY);
	expect(hold.trigger.message_id).toBe("m-broker-wedge");
	expect(hold.reason).toBe("broker_wedged");
	expect(hold.verdict).toEqual(WEDGED);
	expect(hold.notice).toContain("sdk unavailable / broker wedged since 2026-09-05T16:46:00.000Z");
	expect(fixture.logs.some((line) => line.includes("reason=broker_wedged"))).toBe(true);
	expect(fixture.timers.map((timer) => timer.delayMs)).toEqual([2_000, 4_000, 8_000, 16_000, 60_000]);

	await failPersonaTimes(fixture, 3);
	expect(fixture.holds).toHaveLength(1);
	expect(fixture.probes()).toBe(1);
	expect(fixture.timers.slice(5).map((timer) => timer.delayMs)).toEqual([60_000, 60_000, 60_000]);
	expect(new Set(fixture.port.bindEpochs)).toEqual(new Set([0]));
	const currentDatabase = database;
	if (!currentDatabase) throw new Error("persona database was not initialized");
	expect(currentDatabase.getSessionRecord(ORIGIN_KEY)?.epoch ?? 0).toBe(0);
	expect(currentDatabase.inboundPendingOldest(ORIGIN_KEY)).toMatchObject({
		message_id: "m-broker-wedge",
		turn_state: null,
	});

	fixture.port.failure = undefined;
	const retry = fixture.timers.at(-1);
	if (!retry) throw new Error("recovery retry timer was not armed");
	retry.work();
	await eventually(() => fixture.port.sends.length === 1, "pending row was not dispatched after recovery");
	const send = fixture.port.sends[0];
	if (!send) throw new Error("recovered send was not recorded");
	expect(send.text).toBe("hello");
	await eventually(() => fixture.terminal.length === 1, "recovered turn did not complete");
	expect(currentDatabase.inboundPendingCount(ORIGIN_KEY)).toBe(0);
});

test("persona bind wedge probes after five identical failures and deduplicates the user notice", async () => {
	const fixture = await setupPersona([
		{ state: "wedged", reason: "pid_dead", pid: 7007, heartbeatAt: NOW - 10_000 },
		{ state: "absent" },
	]);
	await failPersonaTimes(fixture, BIND_WEDGE_PROBE_STRIKES);
	expect(fixture.probes()).toBe(1);
	expect(fixture.holds).toHaveLength(1);
	const firstHold = fixture.holds[0];
	expect(firstHold).toBeDefined();
	expect(firstHold?.originKey).toBe(ORIGIN_KEY);
	expect(firstHold?.trigger.message_id).toBe("m-broker-wedge");
	expect(firstHold?.verdict).toEqual({ state: "wedged", reason: "pid_dead", pid: 7007, heartbeatAt: NOW - 10_000 });
	expect(firstHold?.notice).toContain("broker wedged");
	expect(fixture.logs.filter((line) => line.startsWith("persona_bind_hold "))).toHaveLength(1);

	await failPersonaTimes(fixture, 2);
	expect(fixture.probes()).toBe(1);
	expect(fixture.holds).toHaveLength(1);

	fixture.port.failure = "gjc sdk request failed: timeout";
	await failPersonaTimes(fixture, 1);
	expect(fixture.probes()).toBe(1);
	expect(fixture.holds).toHaveLength(1);
	await failPersonaTimes(fixture, BIND_WEDGE_PROBE_STRIKES - 2);
	expect(fixture.probes()).toBe(1);
	expect(fixture.holds).toHaveLength(1);
	await failPersonaTimes(fixture, 1);
	expect(fixture.probes()).toBe(2);
	expect(fixture.logs.filter((line) => line.startsWith("persona_bind_hold "))).toHaveLength(2);
	expect(fixture.holds).toHaveLength(2);
	expect(fixture.holds[1]?.reason).toBe("broker_discovery_absent");
});

test("a live broker at the probe threshold stays sdk-unavailable and follows ordinary backoff", async () => {
	const fixture = await setupPersona([{ state: "live", pid: 1, heartbeatAt: NOW }]);
	await failPersonaTimes(fixture, BIND_WEDGE_PROBE_STRIKES + 1);
	expect(fixture.probes()).toBe(1);
	expect(fixture.holds).toHaveLength(1);
	expect(fixture.holds[0]?.reason).toBe("sdk_unavailable");
	expect(fixture.holds[0]?.notice).toContain("broker daemon is live");
	expect(fixture.timers.map((timer) => timer.delayMs)).toEqual([2_000, 4_000, 8_000, 16_000, 32_000, 60_000]);
	expect(fixture.logs.some((line) => line.includes("reason=sdk_unavailable"))).toBe(true);
});

test("poisoned create rotations stop at the per-origin cap and reset after a successful bind", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-broker-wedge-port-"));
	directories.push(home);
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	let createFailure = true;
	const errors: string[] = [];
	const originalError = console.error;
	console.error = (line: unknown) => errors.push(String(line));
	const run = async (args: readonly string[]) => {
		if (args.includes("session.create")) {
			if (createFailure)
				return {
					exitCode: 1,
					stdout: JSON.stringify({ ok: false, error: { code: "terminal_uncertain", message: "create failed" } }),
					stderr: "",
				};
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { sessionId: "created-after-reset" } }),
				stderr: "",
			};
		}
		if (args.includes("inspect"))
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { session: { live: true } } }), stderr: "" };
		throw new Error(`unexpected SDK call: ${args.join(" ")}`);
	};
	const port = new BrokerSessionPort({
		database,
		cli: run,
		instanceId: "broker-wedge-port",
		tailRunner: new TailRunner({ run, repo: REPO }),
		sleep: async () => {},
	});
	const originKey = "monitor/broker-wedge/rotation";
	try {
		for (let rotation = 0; rotation < MAX_POISONED_CREATE_ROTATIONS; rotation++) {
			const epoch = database.getSessionRecord(originKey)?.epoch ?? 0;
			await expect(port.bind({ originKey, epoch, repo: REPO })).rejects.toThrow("terminal_uncertain");
		}
		const epoch = database.getSessionRecord(originKey)?.epoch ?? 0;
		await expect(port.bind({ originKey, epoch, repo: REPO })).rejects.toThrow("terminal_uncertain");
		expect(errors.filter((line) => line.includes("session_create_rotation_capped"))).toHaveLength(1);
		expect(errors.some((line) => line.includes("reason=poisoned_create_key_capped"))).toBe(true);

		createFailure = false;
		const resetEpoch = database.rebindEpoch(originKey);
		await expect(port.bind({ originKey, epoch: resetEpoch, repo: REPO })).resolves.toMatchObject({
			sessionId: "created-after-reset",
		});

		createFailure = true;
		const nextEpoch = database.rebindEpoch(originKey);
		await expect(port.bind({ originKey, epoch: nextEpoch, repo: REPO })).rejects.toThrow("terminal_uncertain");
		expect(errors.filter((line) => line.includes("session_create_rotation_capped"))).toHaveLength(1);
	} finally {
		console.error = originalError;
	}
});
