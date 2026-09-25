import { expect, test } from "bun:test";
import { createHmac } from "node:crypto";
import { chmod, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CATCH_ALL_EVENT_ORIGIN, originKey } from "@gajae-gateway/protocol";
import { cronMatches, startCron } from "../src/monitors/triggers/cron";
import { startScript } from "../src/monitors/triggers/script";
import { startWatcher } from "../src/monitors/triggers/watcher";
import { startWebhook } from "../src/monitors/triggers/webhook";

test("cron supports steps, lists, ranges, dow and injected clock", () => {
	const monday = new Date(2026, 0, 5, 10, 15);
	expect(cronMatches("*/5 10 1-10 1 1", monday)).toBe(true);
	expect(cronMatches("0,15,30 10 * * 1", monday)).toBe(true);
	let fired = 0;
	const stop = startCron(
		"15 10 * * 1",
		{
			cursor: () => new Date(2026, 0, 5, 10, 0),
			fire: () => {
				fired++;
				return true;
			},
			skipped: () => {},
		},
		{ now: () => monday, since: new Date(2026, 0, 5, 10, 0) },
	);
	stop();
	expect(fired).toBe(1);
});

test("webhook admits before 202, rejects unknown routes, replay and oversized bodies", async () => {
	const events: string[] = [];
	const server = startWebhook({
		port: 0,
		monitors: () => [
			{ monitorId: "m", route: "secret", eventType: "changed", auth: { kind: "hmac" as const, secret: "key" } },
		],
		submit: () => {
			events.push("admitted");
			return "event";
		},
	});
	try {
		const base = `http://127.0.0.1:${server.port}`;
		expect((await fetch(`${base}/hook/missing`, { method: "POST" })).status).toBe(404);
		const bytes = new TextEncoder().encode(JSON.stringify({ x: 1 }));
		const timestamp = Math.floor(Date.now() / 1000);
		const nonce = "nonce";
		const signature = createHmac("sha256", "key").update(`${timestamp}.${nonce}.`).update(bytes).digest("hex");
		const request = () =>
			fetch(`${base}/hook/secret`, {
				method: "POST",
				body: bytes,
				headers: {
					"x-gajaeway-timestamp": String(timestamp),
					"x-gajaeway-nonce": nonce,
					"x-gajaeway-signature": signature,
				},
			});
		expect((await request()).status).toBe(202);
		expect(events).toEqual(["admitted"]);
		expect((await request()).status).toBe(401);
		expect((await fetch(`${base}/hook/secret`, { method: "POST", body: new Uint8Array(256 * 1024 + 1) })).status).toBe(
			413,
		);
	} finally {
		server.stop(true);
	}
});

test("webhook refuses non-loopback exposure without explicit opt-in", () => {
	expect(() => startWebhook({ bind: "0.0.0.0", port: 0, monitors: () => [], submit: () => "event" })).toThrow();
});

test("watcher and script honor allowlists and ActionGuard", async () => {
	const root = await mkdtemp(join(tmpdir(), "gajaeway-trigger-"));
	const outside = await mkdtemp(join(tmpdir(), "gajaeway-outside-"));
	try {
		const paths: string[] = [];
		const stop = await startWatcher(root, [root], (path) => paths.push(path), 5);
		for (let i = 0; i < 100 && paths.length === 0; i++) {
			if (i % 10 === 0) await writeFile(join(root, `file-${i}`), "x");
			await Bun.sleep(20);
		}
		stop();
		expect(paths.length).toBeGreaterThanOrEqual(1);
		await expect(startWatcher(outside, [root], () => {})).rejects.toThrow();
		await symlink(join(outside, "target"), join(root, "link"));
		const script = join(root, "echo.sh");
		await writeFile(script, "#!/bin/sh\necho payload\n");
		await chmod(script, 0o755);
		const output: string[] = [];
		const stopScript = await startScript([script], 100_000, root, (text) => output.push(text));
		for (let i = 0; i < 100 && output.length === 0; i++) await Bun.sleep(20);
		stopScript();
		expect(output[0]).toContain("payload");
		await expect(startScript([script, "rm -rf /"], 1, root, () => {})).rejects.toThrow();
	} finally {
		await rm(root, { recursive: true, force: true });
		await rm(outside, { recursive: true, force: true });
	}
});

test("catch-all origin retains undeclared event identity", () => {
	expect(originKey(CATCH_ALL_EVENT_ORIGIN)).toBe("monitor/eventtype/catch-all");
});

test("default memory monitors seed once and respect operator removals", async () => {
	const { mkdtemp: mkTemp } = await import("node:fs/promises");
	const { tmpdir: tmp } = await import("node:os");
	const { join: joinPath } = await import("node:path");
	const { GatewayDatabase: Db } = await import("../src/store/db");
	const { MonitorRegistry: Registry } = await import("../src/monitors/registry");
	const { seedDefaultMonitors } = await import("../src/monitors/defaults");
	const dir = await mkTemp(joinPath(tmp(), "gajaeway-seed-"));
	const database = await Db.open(joinPath(dir, "gateway.db"));
	const registry = new Registry(database);
	expect(seedDefaultMonitors(registry, database)).toBe(2);
	const names = registry.list().map((monitor) => monitor.name);
	expect(names).toContain("memory-canonicalize");
	expect(names).toContain("memory-audit");
	// Second boot: flag prevents reseeding.
	expect(seedDefaultMonitors(registry, database)).toBe(0);
	// Operator removal survives later boots.
	const canonical = registry.list().find((monitor) => monitor.name === "memory-canonicalize");
	registry.remove(canonical?.monitorId ?? "");
	expect(seedDefaultMonitors(registry, database)).toBe(0);
	expect(registry.list().map((monitor) => monitor.name)).not.toContain("memory-canonicalize");
	database.close();
});
