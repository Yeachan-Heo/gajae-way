import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MONITOR_INSTRUCTION_MAX_LENGTH, MonitorRegistry } from "../src/monitors/registry";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";

import { ScriptedSessionPort } from "./session-port.fake";

/** Real registry + generic SessionPort harness honoring the JSON authoring contract. */
async function harness(directory: string) {
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	const registry = new MonitorRegistry(database);
	const prompts: string[] = [];
	const sessionPort = new ScriptedSessionPort({
		onSend: (input, scripted) => {
			prompts.push(input.text);
			scripted.complete(
				input.opRef,
				JSON.stringify(
					(JSON.parse(input.text.match(/\[.*\]$/s)![0]) as Array<{ eventId: string }>).map(({ eventId }) => ({
						eventId,
						note: "recorded",
					})),
				),
			);
		},
	});
	const pipeline = new MonitorPropagator({
		database,
		registry,
		sessionPort,
		memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
		delivery: new DeliveryService(new DeliveryLedger(database)),
		emit: () => {},
	});
	return { database, registry, pipeline, prompts };
}

test("a monitor's instruction reaches the authoring prompt and survives a store round-trip", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-instruction-"));
	try {
		const { database, registry, pipeline, prompts } = await harness(directory);
		const instruction = "Pull the deploy queue and post the oldest blocked release with its owner.";
		const monitor = registry.add({
			name: "deploy-watch",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["deploy.blocked"],
			instruction,
		});
		expect(monitor.instruction).toBe(instruction);
		// Round-trip through the monitors table, not just the in-memory record.
		expect(registry.get(monitor.monitorId)?.instruction).toBe(instruction);

		pipeline.submit(monitor.monitorId, "deploy.blocked", { release: "r-1" });
		await Bun.sleep(250);
		expect(prompts).toHaveLength(1);
		const prompt = prompts[0]!;
		expect(prompt).toContain(instruction);
		// The instruction lands in the guidance slot, directly after the header and
		// before the JSON-array response contract — which is unchanged.
		expect(prompt.startsWith(`Author monitor events. ${instruction} Respond ONLY with a JSON array`)).toBe(true);
		expect(prompt).toContain('{"eventId","note"}');
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a monitor without an instruction keeps the exact pre-existing prompt shape", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-no-instruction-"));
	try {
		const { database, registry, pipeline, prompts } = await harness(directory);
		const monitor = registry.add({
			name: "plain",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["changed"],
		});
		expect(monitor.instruction).toBeUndefined();
		expect(registry.get(monitor.monitorId)?.instruction).toBeUndefined();

		pipeline.submit(monitor.monitorId, "changed", { i: 1 });
		await Bun.sleep(250);
		expect(prompts).toHaveLength(1);
		// No instruction and no maintenance guidance for this event type: no
		// guidance segment at all.
		expect(prompts[0]!.startsWith("Author monitor events. Respond ONLY with a JSON array")).toBe(true);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("an instruction is prepended to the built-in maintenance guidance, not replacing it", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-both-"));
	try {
		const { database, registry, pipeline, prompts } = await harness(directory);
		const instruction = "Start from the newest daily capture only.";
		const monitor = registry.add({
			name: "canonicalize",
			trigger: { kind: "cron", schedule: "30 6 * * *" },
			eventTypes: ["memory.canonicalize"],
			instruction,
		});
		pipeline.submit(monitor.monitorId, "memory.canonicalize", {});
		await Bun.sleep(250);
		const prompt = prompts[0]!;
		expect(prompt).toContain(instruction);
		// Both present, instruction first.
		const maintenanceAt = prompt.indexOf("For memory.canonicalize events:");
		expect(maintenanceAt).toBeGreaterThan(prompt.indexOf(instruction));
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("instruction validation rejects non-strings and over-long text", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-invalid-"));
	try {
		const { database, registry } = await harness(directory);
		const base = {
			name: "bad",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["changed"],
		} as const;
		expect(() => registry.add({ ...base, instruction: 42 as never })).toThrow("monitor instruction must be a string");
		expect(() => registry.add({ ...base, instruction: "x".repeat(MONITOR_INSTRUCTION_MAX_LENGTH + 1) })).toThrow(
			/at most 4000 characters/,
		);
		expect(() => registry.add({ ...base, eventTypes: ["broken/type"] })).toThrow("is not a valid origin segment");
		// Exactly at the bound is accepted, and a blank instruction normalises away.
		expect(registry.add({ ...base, instruction: "x".repeat(MONITOR_INSTRUCTION_MAX_LENGTH) }).instruction).toHaveLength(
			MONITOR_INSTRUCTION_MAX_LENGTH,
		);
		expect(registry.add({ ...base, instruction: "   " }).instruction).toBeUndefined();
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("monitor.add refuses an event type that cannot be an executing-session origin", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-eventtype-"));
	try {
		const { registry } = await harness(directory);
		const base = { trigger: { kind: "cron", schedule: "0 6 * * *" } as const };
		// Live: a 469-character "event type" that was really the whole instruction
		// registered fine and then every event died at dispatch with OriginRefError.
		const instruction = `gajaeway.runtime-feedback.daily.gaebal.channel-1470204268933022023.${"review-last-24h-runtime-operation.".repeat(14)}`;
		expect(instruction.length).toBeGreaterThan(256);
		expect(() => registry.add({ name: "too-long", eventTypes: [instruction], ...base })).toThrow(
			/not a valid origin segment/,
		);
		expect(() => registry.add({ name: "bad-chars", eventTypes: ["daily report/v2"], ...base })).toThrow(
			/not a valid origin segment/,
		);
		expect(registry.add({ name: "ok", eventTypes: ["townhall.report.8h+mention-1"], ...base }).eventTypes).toEqual([
			"townhall.report.8h+mention-1",
		]);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
