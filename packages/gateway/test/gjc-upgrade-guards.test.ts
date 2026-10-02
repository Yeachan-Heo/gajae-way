import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajae-gateway/subsession";
import {
	BrokerSpawnerGuard,
	installFootprintMtimeMs,
	KILL_WINDOW_MS,
	parseElapsedSeconds,
	type SpawnerProcessProbe,
} from "../src/orchestrator/broker-spawner-guard";
import { isSessionGoneCode, isVerifiedGjcVersion } from "../src/orchestrator/gjc-contract";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";
import { noRelay } from "./session-port.fake";

const directories: string[] = [];
afterEach(async () => {
	for (const directory of directories.splice(0)) await rm(directory, { recursive: true, force: true });
});
async function tempDir(prefix: string): Promise<string> {
	const directory = await mkdtemp(join(tmpdir(), prefix));
	directories.push(directory);
	return directory;
}

test("the verified gjc range is inclusive of every patch of the ceiling minor and nothing newer", () => {
	expect(isVerifiedGjcVersion("0.16.0")).toBe(true);
	expect(isVerifiedGjcVersion("gjc/0.18.1")).toBe(true);
	expect(isVerifiedGjcVersion("0.18.99")).toBe(true);
	expect(isVerifiedGjcVersion("0.19.0")).toBe(false);
	expect(isVerifiedGjcVersion("1.0.0")).toBe(false);
	expect(isVerifiedGjcVersion("not a version")).toBe(false);
});

test("every gjc spelling of a dropped session is one session-gone code", () => {
	for (const code of ["session_unavailable", "endpoint_stale", "not_found"]) expect(isSessionGoneCode(code)).toBe(true);
	for (const code of ["broker_unavailable", "operation_failed", undefined, 3])
		expect(isSessionGoneCode(code)).toBe(false);
});

test("ps etime parses on both Linux and macOS shapes", () => {
	expect(parseElapsedSeconds("00:28")).toBe(28);
	expect(parseElapsedSeconds("1-08:41:58")).toBe(((24 + 8) * 60 + 41) * 60 + 58);
	expect(parseElapsedSeconds("4-00:59:20")).toBe((4 * 24 * 60 + 59) * 60 + 20);
	expect(parseElapsedSeconds("12:03:04")).toBe((12 * 60 + 3) * 60 + 4);
	expect(parseElapsedSeconds("garbage")).toBeUndefined();
});

test("the install footprint is the newest of the executable link, its target, and the package manifest", async () => {
	const root = await tempDir("gajaeway-install-");
	await mkdir(join(root, "pkg", "bin"), { recursive: true });
	const target = join(root, "pkg", "bin", "gjc.js");
	await writeFile(target, "");
	await writeFile(join(root, "pkg", "package.json"), "{}");
	await utimes(target, 1_000, 1_000);
	await utimes(join(root, "pkg", "package.json"), 5_000, 5_000);
	expect(await installFootprintMtimeMs(target)).toBe(5_000_000);
});

interface Scenario {
	readonly agentDir: string;
	readonly terminated: number[];
	readonly logs: string[];
	guard: BrokerSpawnerGuard;
	killBroker(pid: number, uptimeMs?: number): Promise<void>;
	now: number;
}

const INSTALLED_AT = Date.parse("2026-09-30T05:05:49Z");
const STALE_GJC = 3192617;
const EXECUTABLE = "/mnt/offloading/.bun/bin/gjc";

async function scenario(options: {
	spawnerStartedAt?: number;
	spawnerCommand?: string;
	terminateSucceeds?: boolean;
}): Promise<Scenario> {
	const agentDir = await tempDir("gajaeway-spawner-");
	await mkdir(join(agentDir, "sdk"), { recursive: true });
	const state: Scenario = {
		agentDir,
		terminated: [],
		logs: [],
		now: Date.parse("2026-09-30T09:20:00Z"),
		guard: undefined as unknown as BrokerSpawnerGuard,
		async killBroker(pid, uptimeMs = 28_200) {
			await writeFile(
				join(agentDir, "sdk", "broker.exit.json"),
				JSON.stringify({ version: 1, reason: "signal", signal: "SIGTERM", uptimeMs, pid, writtenAt: state.now }),
			);
			// The stale parent immediately spawns the next broker.
			await writeFile(join(agentDir, "sdk", "broker.json"), JSON.stringify({ pid: pid + 1 }));
			state.now += 30_000;
		},
	};
	const probe: SpawnerProcessProbe = {
		parentPid: async () => STALE_GJC,
		command: async () => options.spawnerCommand ?? `bun ${EXECUTABLE} --mpreset lunamaxxing-local`,
		startedAtMs: async () => options.spawnerStartedAt ?? Date.parse("2026-09-26T08:20:46Z"),
		terminate: (pid) => {
			state.terminated.push(pid);
			return options.terminateSucceeds ?? true;
		},
	};
	state.guard = new BrokerSpawnerGuard({
		agentDir,
		executable: EXECUTABLE,
		probe,
		installedAtMs: async () => INSTALLED_AT,
		now: () => state.now,
		log: (line) => state.logs.push(line),
	});
	return state;
}

test("a pre-upgrade gjc killing young brokers is terminated on the third short-lived kill", async () => {
	const s = await scenario({});
	await s.killBroker(100);
	expect(await s.guard.observe()).toEqual({ action: "none" });
	await s.killBroker(200);
	expect(await s.guard.observe()).toEqual({ action: "none" });
	await s.killBroker(300);
	expect(await s.guard.observe()).toEqual({ action: "terminated", spawnerPid: STALE_GJC });
	expect(s.terminated).toEqual([STALE_GJC]);
	expect(s.logs.some((line) => line.startsWith(`broker_stale_spawner_terminated pid=${STALE_GJC} `))).toBe(true);
});

test("the same exit record observed repeatedly counts once", async () => {
	const s = await scenario({});
	await s.killBroker(100);
	for (let i = 0; i < 5; i++) expect(await s.guard.observe()).toEqual({ action: "none" });
	expect(s.terminated).toEqual([]);
});

test("a spawner started after the install is never terminated, however often brokers die", async () => {
	const s = await scenario({ spawnerStartedAt: INSTALLED_AT + 60_000 });
	for (const pid of [100, 200, 300, 400]) {
		await s.killBroker(pid);
		await s.guard.observe();
	}
	expect(s.terminated).toEqual([]);
	expect(s.logs.filter((line) => line.includes("reason=spawner_not_older_than_install"))).toHaveLength(1);
});

test("a broker parent that is not the configured gjc is never terminated", async () => {
	const s = await scenario({ spawnerCommand: "/usr/lib/systemd/systemd --user" });
	for (const pid of [100, 200, 300]) {
		await s.killBroker(pid);
		await s.guard.observe();
	}
	expect(s.terminated).toEqual([]);
	expect(s.logs.some((line) => line.includes("reason=spawner_not_gjc"))).toBe(true);
});

test("clean exits and long-lived brokers are not kill-loop evidence", async () => {
	const s = await scenario({});
	await s.killBroker(100, 3_600_000);
	await s.guard.observe();
	await s.killBroker(200, 3_600_000);
	await s.guard.observe();
	await writeFile(
		join(s.agentDir, "sdk", "broker.exit.json"),
		JSON.stringify({ reason: "idle", uptimeMs: 1_000, pid: 300, writtenAt: s.now }),
	);
	expect(await s.guard.observe()).toEqual({ action: "none" });
	expect(s.terminated).toEqual([]);
});

test("kills older than the window expire before they can add up", async () => {
	const s = await scenario({});
	await s.killBroker(100);
	await s.guard.observe();
	await s.killBroker(200);
	await s.guard.observe();
	s.now += KILL_WINDOW_MS;
	await s.killBroker(300);
	expect(await s.guard.observe()).toEqual({ action: "none" });
	expect(s.terminated).toEqual([]);
});

/** The envelope gjc 0.18.1 prints for `sdk session inspect` of an id the broker no longer serves (gaebal-gajae, 2026-09-30). */
const INSPECT_ENDPOINT_STALE = JSON.stringify({
	schema: "gjc.command-error",
	version: 1,
	ok: false,
	command: ["sdk", "session", "inspect"],
	error: {
		code: "endpoint_stale",
		category: "unavailable",
		message: "The SDK endpoint is stale or unavailable.",
		retryability: "unknown",
		outcomeCertainty: "unknown",
		references: [{ kind: "sessionId", value: "old-session" }],
		nextSteps: [],
	},
});

test("gjc 0.18 endpoint_stale on inspect retires the saved session instead of reusing it", async () => {
	const home = await tempDir("gajaeway-stale-inspect-");
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = { canonicalAgentDir: home, identity: `gjc:${home}` };
	database.assertBrokerAuthority(authority, { initializeEmpty: true });
	const repo = join(home, "workspace");
	let dropped = false;
	let creates = 0;
	const run: CliRunner = async (args) => {
		if (args.includes("session.create")) {
			creates++;
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { sessionId: creates === 1 ? "old-session" : "new-session" } }),
				stderr: "",
			};
		}
		if (args.includes("inspect")) {
			const sessionId = args[args.indexOf("inspect") + 1];
			if (dropped && sessionId === "old-session") return { exitCode: 1, stdout: INSPECT_ENDPOINT_STALE, stderr: "" };
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { session: { sessionId, live: true } } }),
				stderr: "",
			};
		}
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "stale-inspect",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
		sleep: async () => {},
	});
	try {
		expect(await port.bind({ originKey: "discord/channel/c", epoch: 0, repo })).toMatchObject({
			sessionId: "old-session",
			epoch: 0,
		});
		dropped = true;
		expect(await port.liveness({ sessionId: "old-session", repo })).toEqual({ live: undefined, disowned: true });
		expect(await port.bind({ originKey: "discord/channel/c", epoch: 0, repo })).toMatchObject({
			sessionId: "new-session",
			epoch: 1,
		});
	} finally {
		database.close();
	}
});
