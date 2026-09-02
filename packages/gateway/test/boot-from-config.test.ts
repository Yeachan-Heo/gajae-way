import { afterEach, expect, test } from "bun:test";
import { lstat, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajaeway/subsession";
import { bootGateway, bootGatewayFromConfig } from "../src/boot";
import { loadConfig } from "../src/config";
import { MIN_GJC_VERSION } from "../src/orchestrator/broker";

const directories: string[] = [];
afterEach(async () => {
	await Promise.all(directories.splice(0).map((d) => rm(d, { recursive: true, force: true })));
});

const HEALTHY = { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessions: [] } }), stderr: "" };
const command: CliRunner = async (args) =>
	args[0] === "--version" ? { exitCode: 0, stdout: `gjc/${MIN_GJC_VERSION}\n`, stderr: "" } : HEALTHY;
const broker = {
	ssotAgentDir: null,
	command,
	spawn: (() => {
		throw new Error("boot must not spawn a session host");
	}) as unknown as typeof Bun.spawn,
	healthProbe: async () => true,
	healthIntervalMs: 60_000,
	log: () => {},
};

async function home(): Promise<string> {
	const dir = await mkdtemp(join(tmpdir(), "gajaeway-boot-from-config-"));
	directories.push(dir);
	return dir;
}

test("bootGatewayFromConfig boots from the exact snapshot it is given and never re-reads config.json", async () => {
	const dir = await home();
	await writeFile(join(dir, "config.json"), JSON.stringify({ schemaVersion: 1, settleWindowMs: 123 }));
	const snapshot = await loadConfig({ home: dir });
	expect(snapshot.settleWindowMs).toBe(123);
	// Mutate the file after the snapshot was taken; the boot must not observe it.
	await writeFile(join(dir, "config.json"), JSON.stringify({ schemaVersion: 1, socketPath: join(dir, "other.sock") }));
	const server = await bootGatewayFromConfig(snapshot, { broker });
	try {
		expect((await lstat(snapshot.socketPath)).isSocket()).toBe(true);
		expect(await Bun.file(join(dir, "other.sock")).exists()).toBe(false);
	} finally {
		await server.stop("test");
	}
});

test("bootGateway is a thin wrapper: loadConfig then bootGatewayFromConfig, with shutdown passthrough", async () => {
	const dir = await home();
	await writeFile(join(dir, "config.json"), JSON.stringify({ schemaVersion: 1 }));
	const reasons: string[] = [];
	let stopper: (() => Promise<void>) | undefined;
	const server = await bootGateway({
		home: dir,
		broker,
		shutdown: async (reason) => {
			reasons.push(reason);
			await stopper?.();
		},
	});
	stopper = () => server.stop("composite");
	const port = server.attach("wrapper");
	await port.open();
	await port.request("gateway.shutdown");
	expect(reasons).toEqual(["gateway.shutdown verb"]);
	expect(await Bun.file(join(dir, "gateway.sock")).exists()).toBe(false);
});
