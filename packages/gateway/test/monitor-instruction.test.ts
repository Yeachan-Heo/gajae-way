import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload } from "@gajae-gateway/protocol";
import { DeliveryService } from "../src/delivery/delivery";
import { isAuthoredEchoSuspected } from "../src/monitors/compaction";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MONITOR_INSTRUCTION_MAX_LENGTH, MonitorRegistry } from "../src/monitors/registry";
import { Store } from "../src/store";

import { ScriptedSessionPort } from "./session-port.fake";

/** Real registry + generic SessionPort harness honoring the JSON authoring contract. */
async function harness(directory: string) {
	const database = await Store.open(join(directory, "gateway.db"));
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
		delivery: new DeliveryService(database),
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

test("#180/#369: typed targets survive restart and echoed instructions retain appended findings", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-typed-target-"));
	try {
		const instruction =
			"Review the last 24 hours of runtime operation. Report actual friction, failures and latency with public-safe evidence only. Check existing issues before proposing a new one.";
		const channelTarget = {
			origin: { platform: "discord", kind: "channel", conversationId: "1470204268933022023" },
			mentionUserIds: ["1468532331001413743"],
		} as const;
		let monitorId: string;
		{
			const { database, registry } = await harness(directory);
			monitorId = registry.add({
				name: "runtime-feedback",
				trigger: { kind: "cron", schedule: "0 11 * * *" },
				eventTypes: ["gajaeway.runtime-feedback.daily"],
				instruction,
				channelTarget,
			}).monitorId;
			database.close();
		}
		// Gateway restart: a fresh database handle and registry over the same file.
		const database = await Store.open(join(directory, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const restored = registry.get(monitorId)!;
		expect(restored.instruction).toBe(instruction);
		expect(restored.eventTypes).toEqual(["gajaeway.runtime-feedback.daily"]);
		expect(restored.channelTarget).toEqual(channelTarget);

		const prompts: string[] = [];
		const delivered: ChatMessagePayload[] = [];
		const note = `${instruction} Release blocked.`;
		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: new ScriptedSessionPort({
				onSend: (input, scripted) => {
					prompts.push(input.text);
					const [event] = JSON.parse(input.text.match(/\[.*\]$/s)![0]) as Array<{ eventId: string }>;
					scripted.complete(input.opRef, JSON.stringify([{ eventId: event!.eventId, note }]));
				},
			}),
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			deliver: (payload) => delivered.push(payload),
		});
		pipeline.submit(monitorId, "gajaeway.runtime-feedback.daily", { at: "2026-09-07T11:00:00.000Z" });
		await Bun.sleep(250);
		expect(prompts[0]!.startsWith(`Author monitor events. ${instruction} Respond ONLY`)).toBe(true);
		expect(delivered).toHaveLength(1);
		expect(delivered[0]!.origin).toEqual(channelTarget.origin);
		// The mention comes from the typed field, not from the author remembering it.
		expect(delivered[0]!.text).toBe(`<@1468532331001413743> ${note}`);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("#369: contract-only notes are terminal and never delivered", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-contract-echo-"));
	try {
		const database = await Store.open(join(directory, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const instruction =
			"Review the last 24 hours of runtime operation. Report actual friction, failures and latency with public-safe evidence only.";
		const monitor = registry.add({
			name: "runtime-feedback",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["gajaeway.runtime-feedback.daily"],
			instruction,
			channelTarget: {
				origin: { platform: "discord", kind: "channel", conversationId: "1470204268933022023" },
				mentionUserIds: [],
			},
		});
		const delivered: ChatMessagePayload[] = [];
		const sessionPort = new ScriptedSessionPort({
			onSend: (input, scripted) => {
				const payload = input.text.match(/\[.*\]$/s)?.[0];
				if (!payload) throw new Error("monitor event payload is missing");
				const [event] = JSON.parse(payload) as Array<{ eventId: string }>;
				if (!event) throw new Error("monitor event is missing");
				scripted.complete(input.opRef, JSON.stringify([{ eventId: event.eventId, note: instruction }]));
			},
		});
		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			deliver: (payload) => delivered.push(payload),
		});

		pipeline.submit(monitor.monitorId, "gajaeway.runtime-feedback.daily", {});
		await Bun.sleep(250);

		expect(delivered).toHaveLength(0);
		const [event] = database.monitors.eventRows(monitor.monitorId);
		expect(event?.stage).toBe("contract_unmet");
		expect(event && database.monitors.failure(event.event_id)?.code).toBe("authored_echo_suspected");
		expect(database.monitors.openCount()).toBe(0);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("#369: reused event-tag text and copied numbers are not novel work evidence", () => {
	const instruction = "Review the last 24 hours and inspect issue #369.";
	expect(isAuthoredEchoSuspected(instruction, instruction)).toBe(true);
	expect(isAuthoredEchoSuspected(instruction, `What the run should do: ${instruction}`)).toBe(true);
	expect(isAuthoredEchoSuspected("runtime.feedback.daily", "Runtime feedback daily")).toBe(true);
	expect(isAuthoredEchoSuspected(instruction, `${instruction} #370`)).toBe(false);
	expect(
		isAuthoredEchoSuspected(
			"Review the last 24 hours of runtime activity and report blockers and failures.",
			"Review the last 24 hours of runtime activity and report blockers and failures. Release blocked.",
		),
	).toBe(false);
	expect(
		isAuthoredEchoSuspected(
			"Review the last 24 hours of runtime activity and report blockers and failures.",
			"Review the last 24 hours of runtime activity and report blockers and failures. Blockers remain.",
		),
	).toBe(false);
	expect(
		isAuthoredEchoSuspected(
			"Review the last 24 hours of runtime activity and report blockers and failures.",
			"Review the last 24 hours.",
		),
	).toBe(false);
});

test("#180: mention targets are validated as platform ids", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-mention-invalid-"));
	try {
		const { database, registry } = await harness(directory);
		const base = {
			name: "bad",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["changed"],
		} as const;
		const origin = { platform: "discord", kind: "channel", conversationId: "c1" } as const;
		for (const mentionUserIds of [["way-gajae>@everyone"], [""], "u1", [42]])
			expect(() =>
				registry.add({ ...base, channelTarget: { origin, mentionUserIds: mentionUserIds as never } }),
			).toThrow("monitor channelTarget.mentionUserIds");
		// A Slack id on a Discord target, and mentions on a platform without `<@id>`.
		expect(() => registry.add({ ...base, channelTarget: { origin, mentionUserIds: ["U0C2GSKTA6M"] } })).toThrow(
			"monitor channelTarget.mentionUserIds",
		);
		const telegram = { platform: "telegram", kind: "channel", conversationId: "-100" } as const;
		expect(() => registry.add({ ...base, channelTarget: { origin: telegram, mentionUserIds: ["1"] } })).toThrow(
			"not supported for telegram",
		);
		const slack = {
			origin: { platform: "slack", kind: "channel", conversationId: "C1" },
			mentionUserIds: ["U0C2GSKTA6M"],
		} as const;
		expect(registry.add({ ...base, channelTarget: slack }).channelTarget).toEqual(slack);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
