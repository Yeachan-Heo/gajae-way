import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { acquireLease, homeLayout, leaseHeld } from "@gajae-gateway/home";
import type { CliRunner } from "@gajae-gateway/subsession";
import { bootGateway } from "../src/boot";
import { readPinnedGjcVersion } from "../src/orchestrator/broker";
import { Store } from "../src/store";

const homes: string[] = [];
const SESSIONS = JSON.stringify({ ok: true, result: { sessions: [] } });

afterEach(async () => {
	await Promise.all(homes.splice(0).map((home) => rm(home, { recursive: true, force: true })));
});

async function temporaryHome(): Promise<string> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-boot-lease-"));
	homes.push(home);
	return home;
}

function brokerFor(home: string) {
	const command: CliRunner = async (args) => {
		if (args[0] === "--version") return { exitCode: 0, stdout: `gjc/${readPinnedGjcVersion()}\n`, stderr: "" };
		return { exitCode: 0, stdout: SESSIONS, stderr: "" };
	};
	return {
		executable: "/test-only/gjc",
		agentDir: home,
		command,
		healthProbe: async () => true,
		discovery: async () => ({ pid: 1, url: "ws://127.0.0.1:1", token: "test-only", heartbeatAt: Date.now() }),
		healthIntervalMs: 60_000,
		log: () => {},
	};
}

test("a second boot on the same home fails with the gateway lease held", async () => {
	const home = await temporaryHome();
	const server = await bootGateway({ home, broker: brokerFor(home) });
	try {
		await expect(bootGateway({ home, broker: brokerFor(home) })).rejects.toMatchObject({
			code: "lease_held",
			name: "gateway",
		});
	} finally {
		await server.stop("test shutdown");
	}
});

test("boot holds the memory lease and releases both gateway leases on close", async () => {
	const home = await temporaryHome();
	const server = await bootGateway({ home, broker: brokerFor(home) });
	try {
		expect(await leaseHeld(home, "memory")).toBe(true);
	} finally {
		await server.stop("test shutdown");
	}
	expect(await leaseHeld(home, "memory")).toBe(false);
	expect(await leaseHeld(home, "gateway")).toBe(false);
});

test("memory lease acquisition failure releases the gateway lease", async () => {
	const home = await temporaryHome();
	const memoryLease = await acquireLease(home, "memory");
	try {
		await expect(bootGateway({ home, broker: brokerFor(home) })).rejects.toMatchObject({
			code: "lease_held",
			name: "memory",
		});
		expect(await leaseHeld(home, "gateway")).toBe(false);
	} finally {
		await memoryLease.release();
	}
});

test("boot restores the outbox projection from the journal before starting recovery", async () => {
	const home = await temporaryHome();
	const path = join(home, "gateway.db");
	const database = await Store.open(path);
	const configuredAgentDir = homeLayout(home).brokerAgentDir;
	await mkdir(configuredAgentDir, { recursive: true, mode: 0o700 });
	const agentDir = await realpath(configuredAgentDir);
	const authority = { canonicalAgentDir: agentDir, identity: `gjc:${agentDir}` };
	database.transaction((tx) => database.bindings.initializeAuthority(tx, authority, false));
	database.transaction((tx) =>
		database.outbox.enqueue(tx, {
			deliveryId: "boot-replay-delivery",
			turnId: "boot-replay-turn",
			originKey: "discord/channel/boot-replay",
			payloadJson: JSON.stringify({ deliveryId: "boot-replay-delivery", text: "journaled" }),
		}),
	);
	const expected = database.outbox.projectionRows();
	database.close();
	const raw = new Database(path);
	raw.exec("DELETE FROM deliveries;");
	raw.close();

	const server = await bootGateway({ home, broker: brokerFor(home) });
	try {
		const replayed = await Store.open(path);
		try {
			expect(replayed.outbox.projectionRows()).toEqual(expected);
		} finally {
			replayed.close();
		}
	} finally {
		await server.stop("test shutdown");
	}
});
