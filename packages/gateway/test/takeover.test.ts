import { afterEach, expect, test } from "bun:test";
import { lstat, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajae-gateway/subsession";
import { bootGateway } from "../src/boot";
import { MIN_GJC_VERSION } from "../src/orchestrator/broker";
import {
	acquireGatewayHome,
	claimGatewayHome,
	GatewayAlreadyRunningError,
	HOME_LOCK_FILE,
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

test("only dead PID records are replaced; malformed and unrelated live ownership fail closed", async () => {
	const home = await temporaryHome("gajaeway-takeover-stale-");
	const { ports } = fakePorts(
		new Map([
			[1, { alive: false, command: "" }],
			[2, { alive: true, command: "/usr/bin/vim daemon.pid" }],
		]),
	);
	await seedRecord(home, 1);
	expect(await claimGatewayHome(home, { onlyNew: true }, ports)).toBeUndefined();
	await seedRecord(home, 2);
	await expect(claimGatewayHome(home, { onlyNew: true }, ports)).rejects.toBeInstanceOf(GatewayAlreadyRunningError);
	await writeFile(pidFilePath(home), "{not json");
	await expect(claimGatewayHome(home, { onlyNew: true }, ports)).rejects.toThrow("gateway_pid_indeterminate");
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
	const command: CliRunner = async (args) => {
		expect((await lstat(join(home, "workspace"))).isDirectory()).toBe(true);
		return args[0] === "--version"
			? { exitCode: 0, stdout: `gjc/${MIN_GJC_VERSION}\n`, stderr: "" }
			: { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessions: [] } }), stderr: "" };
	};
	const broker = {
		executable: "/test-only/gjc",
		agentDir: home,
		command,
		healthProbe: async () => true,
		discovery: async () => ({ pid: 1, url: "ws://127.0.0.1:1", token: "test-only", heartbeatAt: Date.now() }),
		healthIntervalMs: 60_000,
		log: () => {},
	};
	const table = new Map([[5151, { alive: true, command: "/x/gajaeway-gateway daemon" }]]);
	const { ports } = fakePorts(table);
	await seedRecord(home, 5151);
	// --only-new against a live predecessor: nothing else must have been created.
	await expect(
		bootGateway({
			home,
			onlyNew: true,
			takeover: ports,
			broker,
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
		broker,
	});
	try {
		expect(await readPidRecord(home)).toMatchObject({ pid: process.pid, home });
		expect((await lstat(join(home, "gateway.sock"))).isSocket()).toBe(true);
	} finally {
		await server.stop("test shutdown");
	}
	await expect(lstat(pidFilePath(home))).rejects.toMatchObject({ code: "ENOENT" });
});

test("exclusive leases refuse both boot and admin contenders and release only their own descriptor", async () => {
	const home = await temporaryHome("gajaeway-exclusive-");
	const first = await acquireGatewayHome(home);
	try {
		await expect(acquireGatewayHome(home)).rejects.toThrow("gateway_home_owned");
		await expect(bootGateway({ home })).rejects.toThrow("gateway_home_owned");
		await expect(lstat(join(home, "gateway.db"))).rejects.toMatchObject({ code: "ENOENT" });
	} finally {
		await first.release();
	}
	const second = await acquireGatewayHome(home);
	try {
		expect(second.token).not.toBe(first.token);
		await first.release();
		await expect(acquireGatewayHome(home)).rejects.toThrow("gateway_home_owned");
	} finally {
		await second.release();
	}
});

test("malformed, incomplete and live owner metadata are never stolen", async () => {
	const home = await temporaryHome("gajaeway-exclusive-malformed-");
	for (const raw of [
		"",
		"{bad",
		JSON.stringify({ pid: process.pid }),
		JSON.stringify({
			pid: process.pid,
			token: "other-owner",
			startedAt: new Date().toISOString(),
			state: "held",
		}),
	]) {
		await writeFile(join(home, HOME_LOCK_FILE), raw);
		await expect(acquireGatewayHome(home)).rejects.toThrow(
			raw.includes("other-owner") ? "gateway_home_owner_live" : "gateway_home_owner_indeterminate",
		);
		expect(await readFile(join(home, HOME_LOCK_FILE), "utf8")).toBe(raw);
	}
});

test("kernel releases a crashed holder without unlinking the lock inode; stale recovery has one winner", async () => {
	const home = await temporaryHome("gajaeway-exclusive-crash-");
	const modulePath = new URL("../src/takeover.ts", import.meta.url).pathname;
	const child = Bun.spawn(
		[
			process.execPath,
			"--eval",
			`
		import { acquireGatewayHome } from ${JSON.stringify(modulePath)};
		const lease = await acquireGatewayHome(${JSON.stringify(home)});
		console.log("held");
		await Bun.stdin.text();
		process.exit(lease.token ? 23 : 24);
	`,
		],
		{ stdin: "pipe", stdout: "pipe", stderr: "pipe" },
	);
	let inode: number | undefined;
	try {
		const reader = child.stdout.getReader();
		const ready = await reader.read();
		reader.releaseLock();
		expect(new TextDecoder().decode(ready.value)).toContain("held");
		inode = (await lstat(join(home, HOME_LOCK_FILE))).ino;
		await expect(acquireGatewayHome(home)).rejects.toThrow("gateway_home_owned");
	} finally {
		child.stdin.end();
		await child.exited;
	}
	expect(child.exitCode).toBe(23);
	const recovered = await acquireGatewayHome(home);
	try {
		expect((await lstat(join(home, HOME_LOCK_FILE))).ino).toBe(inode);
		await expect(acquireGatewayHome(home)).rejects.toThrow("gateway_home_owned");
	} finally {
		await recovered.release();
	}
});
