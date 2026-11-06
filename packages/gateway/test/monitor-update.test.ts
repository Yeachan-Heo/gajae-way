import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MonitorRegistry } from "../src/monitors/registry";
import { startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { sessionPortFromScript } from "./session-port.fake";

test("monitor update changes a cron schedule without replacing identity or event history", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-update-"));
	let database: Store | undefined;
	try {
		database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const created = registry.add({
			name: "schedule-change",
			trigger: { kind: "cron", schedule: "*/30 * * * *" },
			eventTypes: ["schedule.changed"],
			enabled: false,
		});
		const before = registry.get(created.monitorId);
		if (!before) throw new Error("new monitor was not persisted");
		const eventId = crypto.randomUUID();
		database!.transaction((tx) =>
			database!.monitors.eventCreate(tx, {
				eventId,
				monitorId: created.monitorId,
				eventType: "schedule.changed",
				payloadJson: "{}",
				firedAt: "2026-09-25T10:00:00.000Z",
			}),
		);

		const updated = registry.update({ monitorId: created.monitorId, schedule: "0 */2 * * *" });

		expect(updated).toMatchObject({
			monitorId: before.monitorId,
			createdAt: before.createdAt,
			trigger: { kind: "cron", schedule: "0 */2 * * *" },
			enabled: false,
		});
		if (!updated) throw new Error("monitor update removed the monitor");
		expect(registry.list()).toEqual([updated]);
		expect(database.monitors.eventRows(created.monitorId, "newest", true).map((event) => event.event_id)).toEqual([
			eventId,
		]);
	} finally {
		database?.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("monitor.update RPC returns the stable ID and monitor.inspect reads the changed schedule", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-update-rpc-"));
	let database: Store | undefined;
	let server: Awaited<ReturnType<typeof startUnixServer>> | undefined;
	let socket: Awaited<ReturnType<typeof Bun.connect>> | undefined;
	try {
		database = await Store.open(join(home, "gateway.db"));
		const monitor = new MonitorRegistry(database).add({
			name: "rpc-schedule-change",
			trigger: { kind: "cron", schedule: "*/30 * * * *" },
			eventTypes: ["rpc.schedule.changed"],
			enabled: false,
		});
		const socketPath = join(home, "gateway.sock");
		server = await startUnixServer({
			config: {
				schemaVersion: 1,
				home,
				gjc: { agentDir: join(home, "agent") },
				configPath: join(home, "config.json"),
				socketPath,
				dbPath: join(home, "gateway.db"),
				logVerbosity: "info",
			},
			database,
			sessionPort: sessionPortFromScript({ respond: async () => "[]" }),
			onStop: () => {},
		});

		const frames: Array<Record<string, unknown>> = [];
		let buffered = "";
		const connectedSocket = await Bun.connect({
			unix: socketPath,
			socket: {
				data(_socket, data) {
					buffered += Buffer.from(data).toString("utf8");
					const lines = buffered.split("\n");
					buffered = lines.pop() ?? "";
					for (const line of lines) if (line) frames.push(JSON.parse(line));
				},
			},
		});
		socket = connectedSocket;
		const waitFor = async (predicate: (frame: Record<string, unknown>) => boolean, label: string) => {
			for (let attempt = 0; attempt < 400; attempt++) {
				const frame = frames.find(predicate);
				if (frame) return frame;
				await Bun.sleep(5);
			}
			throw new Error(`no ${label} response`);
		};
		connectedSocket.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
		await waitFor((frame) => frame.type === "negotiated", "negotiation");
		const request = async (id: string, verb: string, params: Record<string, unknown>) => {
			connectedSocket.write(`${JSON.stringify({ v: "0.1", type: "request", id, verb, params })}\n`);
			return await waitFor((frame) => frame.id === id, verb);
		};

		const update = await request("update", "monitor.update", {
			monitorId: monitor.monitorId,
			schedule: "0 */2 * * *",
			contextRollTokens: 100_000,
			reportTo: { platform: "discord", kind: "channel", conversationId: "report-room" },
		});
		expect(update).toMatchObject({ type: "response", result: { monitorId: monitor.monitorId } });
		const inspected = await request("inspect", "monitor.inspect", { monitorId: monitor.monitorId });
		expect(inspected).toMatchObject({
			type: "response",
			result: {
				monitor: {
					monitorId: monitor.monitorId,
					trigger: { kind: "cron", schedule: "0 */2 * * *" },
					contextRollTokens: 100_000,
					reportTo: { platform: "discord", kind: "channel", conversationId: "report-room" },
				},
			},
		});
		const listed = await request("list", "monitor.list", {});
		expect(listed).toMatchObject({
			result: { monitors: [{ monitorId: monitor.monitorId, contextRollTokens: 100_000 }] },
		});
		const cleared = await request("clear-budget", "monitor.update", {
			monitorId: monitor.monitorId,
			contextRollTokens: null,
		});
		expect(cleared).toMatchObject({ type: "response", result: { monitorId: monitor.monitorId } });
		const inspectedCleared = await request("inspect-cleared", "monitor.inspect", { monitorId: monitor.monitorId });
		expect(inspectedCleared).toMatchObject({ type: "response", result: { monitor: { monitorId: monitor.monitorId } } });
		expect((inspectedCleared.result as { monitor: Record<string, unknown> }).monitor).not.toHaveProperty(
			"contextRollTokens",
		);
		expect(database.monitors.rows()[0]?.context_roll_tokens).toBeNull();
	} finally {
		socket?.end();
		await server?.stop();
		database?.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("contextRollTokens validates bounds and round-trips set, omitted and cleared updates", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-context-roll-registry-"));
	const path = join(home, "gateway.db");
	let database: Store | undefined;
	try {
		database = await Store.open(path);
		let registry = new MonitorRegistry(database);
		const spec = {
			name: "context-roll",
			trigger: { kind: "cron", schedule: "0 */6 * * *" },
			eventTypes: ["context.tick"],
		} as const;
		for (const contextRollTokens of [0, 9999, 1.5, 2_000_001, "10000", null])
			expect(() => registry.add({ ...spec, contextRollTokens: contextRollTokens as number })).toThrow(
				"monitor contextRollTokens is invalid",
			);
		const monitor = registry.add({ ...spec, contextRollTokens: 10_000 });
		expect(registry.get(monitor.monitorId)?.contextRollTokens).toBe(10_000);
		expect(database.monitors.rows()[0]?.context_roll_tokens).toBe(10_000);
		for (const contextRollTokens of [0, 9999, 1.5, 2_000_001, "10000"])
			expect(() =>
				registry.update({ monitorId: monitor.monitorId, contextRollTokens: contextRollTokens as number }),
			).toThrow("monitor contextRollTokens is invalid");
		expect(registry.get(monitor.monitorId)?.contextRollTokens).toBe(10_000);
		expect(registry.update({ monitorId: monitor.monitorId, contextRollTokens: 2_000_000 })?.contextRollTokens).toBe(
			2_000_000,
		);
		expect(registry.update({ monitorId: monitor.monitorId, name: "renamed" })?.contextRollTokens).toBe(2_000_000);
		database.close();
		database = await Store.open(path);
		registry = new MonitorRegistry(database);
		expect(registry.get(monitor.monitorId)?.contextRollTokens).toBe(2_000_000);
		expect(
			registry.update({ monitorId: monitor.monitorId, contextRollTokens: undefined })?.contextRollTokens,
		).toBeUndefined();
		expect(database.monitors.rows()[0]?.context_roll_tokens).toBeNull();
		database.close();
		database = await Store.open(path);
		expect(new MonitorRegistry(database).get(monitor.monitorId)?.contextRollTokens).toBeUndefined();
		registry = new MonitorRegistry(database);
		registry.update({ monitorId: monitor.monitorId, contextRollTokens: 10_000 });
		expect(
			registry.update({ monitorId: monitor.monitorId, contextRollTokens: null })?.contextRollTokens,
		).toBeUndefined();
		expect(database.monitors.rows()[0]?.context_roll_tokens).toBeNull();
		database.close();
		database = await Store.open(path);
		expect(new MonitorRegistry(database).get(monitor.monitorId)?.contextRollTokens).toBeUndefined();
	} finally {
		database?.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("updating a webhook never replaces its generated route with a caller-chosen route", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-update-webhook-"));
	let database: Store | undefined;
	try {
		database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const original = registry.add({
			name: "webhook-update",
			trigger: { kind: "webhook", route: "caller-supplied" },
			eventTypes: ["webhook.changed"],
			enabled: false,
		});
		const before = registry.get(original.monitorId);
		if (before?.trigger.kind !== "webhook") throw new Error("expected persisted webhook trigger");
		expect(() => registry.update({ monitorId: original.monitorId, schedule: "0 */2 * * *" })).toThrow(
			"only cron monitors have a schedule",
		);
		expect(registry.get(original.monitorId)).toEqual(before);

		const updated = registry.update({
			monitorId: original.monitorId,
			trigger: { kind: "webhook", route: "replacement-route" },
		});
		if (updated?.trigger.kind !== "webhook") throw new Error("updated webhook was not persisted");

		expect(updated.trigger).toEqual({ kind: "webhook", route: before.trigger.route });
		expect(registry.list().map((entry) => entry.trigger)).toEqual([updated.trigger]);
	} finally {
		database?.close();
		await rm(home, { recursive: true, force: true });
	}
});
