import { afterEach, expect, test } from "bun:test";
import { lstat, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajaeway/subsession";
import { bootGateway } from "../src/boot";
import { MIN_GJC_VERSION } from "../src/orchestrator/broker";
import {
	claimGatewayHome,
	GatewayAlreadyRunningError,
	pidFilePath,
	readPidRecord,
	releaseGatewayHome,
	type TakeoverPorts,
} from "../src/takeover";

const directories: string[] = [];
afterEach(async () => {
	await Promise.all(directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});
async function temporaryHome(prefix: string): Promise<string> {
	const home = await mkdtemp(join(tmpdir(), prefix));
	directories.push(home);
	return home;
}

/** A fake process table: which pids are alive, what they are running, and how many liveness polls happened. */
function fakePorts(table: Map<number, { alive: boolean; command: string }>) {
	const logs: string[] = [];
	let polls = 0;
	const ports: TakeoverPorts = {
		isPidAlive: (pid) => {
			polls++;
			return table.get(pid)?.alive === true;
		},
		commandOf: (pid) => table.get(pid)?.command,
		sleep: async () => {},
		log: (line) => {
			logs.push(line);
		},
	};
	return { ports, logs, polls: () => polls };
}

async function seedRecord(home: string, pid: number, recordHome = home): Promise<void> {
	await writeFile(
		pidFilePath(home),
		`${JSON.stringify({ pid, home: recordHome, startedAt: "2026-09-03T00:00:00.000Z" })}\n`,
	);
}

test("a live same-home gateway is waited out; the newcomer never signals it and records itself once it is gone", async () => {
	const home = await temporaryHome("gajaeway-takeover-wait-");
	const table = new Map([[4242, { alive: true, command: "/opt/gajaeway/bin/gajaeway-gateway daemon" }]]);
	const { ports, logs, polls } = fakePorts(table);
	await seedRecord(home, 4242);
	// The predecessor exits (service manager stopped it) after a few polls.
	const original = ports.isPidAlive;
	let seen = 0;
	const waiting: TakeoverPorts = {
		...ports,
		isPidAlive: (pid) => {
			if (pid === 4242 && ++seen >= 4) table.set(4242, { alive: false, command: "" });
			return original(pid);
		},
	};
	const predecessor = await claimGatewayHome(home, { onlyNew: false, waitMs: 5_000 }, waiting);
	expect(predecessor).toBe(4242);
	expect(logs.some((line) => line.startsWith("gateway_predecessor_live pid=4242"))).toBe(true);
	expect(logs.some((line) => line === "gateway_predecessor_exited pid=4242")).toBe(true);
	expect(polls()).toBeGreaterThanOrEqual(4);
	expect(await readPidRecord(home)).toMatchObject({ pid: process.pid, home });
});

test("a predecessor that outlives the wait makes the newcomer fail closed for the service manager to retry", async () => {
	const home = await temporaryHome("gajaeway-takeover-timeout-");
	const table = new Map([[4243, { alive: true, command: "gajaeway-gateway daemon --stdio" }]]);
	const { ports } = fakePorts(table);
	await seedRecord(home, 4243);
	await expect(claimGatewayHome(home, { onlyNew: false, waitMs: 300 }, ports)).rejects.toBeInstanceOf(
		GatewayAlreadyRunningError,
	);
	// The predecessor's record is untouched: it still owns the home.
	expect(await readPidRecord(home)).toMatchObject({ pid: 4243 });
});

test("--only-new refuses immediately when a live same-home gateway exists, without waiting or polling", async () => {
	const home = await temporaryHome("gajaeway-takeover-onlynew-");
	const table = new Map([[4244, { alive: true, command: "/x/gajaeway-gateway daemon" }]]);
	const { ports, polls } = fakePorts(table);
	await seedRecord(home, 4244);
	await expect(claimGatewayHome(home, { onlyNew: true }, ports)).rejects.toMatchObject({
		code: "gateway_already_running",
		pid: 4244,
	});
	expect(polls()).toBe(1);
	expect(await readPidRecord(home)).toMatchObject({ pid: 4244 });
});

test("stale records are replaced: dead pid, a live pid that is not a gateway, or a gateway for another home", async () => {
	const home = await temporaryHome("gajaeway-takeover-stale-");
	const otherHome = await temporaryHome("gajaeway-takeover-other-");
	const table = new Map([
		[1, { alive: false, command: "" }],
		[2, { alive: true, command: "/usr/bin/vim daemon.pid" }],
		[3, { alive: true, command: "/x/gajaeway-gateway daemon" }],
	]);
	const { ports, logs } = fakePorts(table);
	for (const [pid, recordHome] of [
		[1, home],
		[2, home],
		[3, otherHome],
	] as const) {
		await seedRecord(home, pid, recordHome);
		expect(await claimGatewayHome(home, { onlyNew: true }, ports)).toBeUndefined();
		expect(await readPidRecord(home)).toMatchObject({ pid: process.pid, home });
	}
	expect(logs.filter((line) => line.startsWith("gateway_pid_stale"))).toHaveLength(3);
	// A malformed record is stale too.
	await writeFile(pidFilePath(home), "{not json");
	expect(await claimGatewayHome(home, { onlyNew: true }, ports)).toBeUndefined();
});

test("release removes only this process's own record; a successor's record survives", async () => {
	const home = await temporaryHome("gajaeway-takeover-release-");
	const { ports } = fakePorts(new Map());
	await claimGatewayHome(home, { onlyNew: true }, ports);
	await releaseGatewayHome(home);
	await expect(lstat(pidFilePath(home))).rejects.toMatchObject({ code: "ENOENT" });
	await seedRecord(home, 9999);
	await releaseGatewayHome(home);
	expect(JSON.parse(await readFile(pidFilePath(home), "utf8"))).toMatchObject({ pid: 9999 });
});

test("boot settles home ownership before the socket or database exist, and releases it on ordered shutdown", async () => {
	const home = await temporaryHome("gajaeway-takeover-boot-");
	const command: CliRunner = async (args) =>
		args[0] === "--version"
			? { exitCode: 0, stdout: `gjc/${MIN_GJC_VERSION}\n`, stderr: "" }
			: { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessions: [] } }), stderr: "" };
	const table = new Map([[5151, { alive: true, command: "/x/gajaeway-gateway daemon" }]]);
	const { ports } = fakePorts(table);
	await seedRecord(home, 5151);
	// --only-new against a live predecessor: nothing else must have been created.
	await expect(
		bootGateway({
			home,
			onlyNew: true,
			takeover: ports,
			broker: { ssotAgentDir: null, command, healthProbe: async () => true, log: () => {} },
		}),
	).rejects.toBeInstanceOf(GatewayAlreadyRunningError);
	await expect(lstat(join(home, "gateway.sock"))).rejects.toMatchObject({ code: "ENOENT" });
	await expect(lstat(join(home, "gateway.db"))).rejects.toMatchObject({ code: "ENOENT" });
	expect(await readPidRecord(home)).toMatchObject({ pid: 5151 });

	// Predecessor gone: boot proceeds, owns the home, releases it on stop.
	table.set(5151, { alive: false, command: "" });
	const server = await bootGateway({
		home,
		takeover: ports,
		broker: { ssotAgentDir: null, command, healthProbe: async () => true, healthIntervalMs: 60_000, log: () => {} },
	});
	try {
		expect(await readPidRecord(home)).toMatchObject({ pid: process.pid, home });
		expect((await lstat(join(home, "gateway.sock"))).isSocket()).toBe(true);
	} finally {
		await server.stop("test shutdown");
	}
	await expect(lstat(pidFilePath(home))).rejects.toMatchObject({ code: "ENOENT" });
});
