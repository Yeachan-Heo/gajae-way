import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload } from "@gajae-gateway/protocol";
import { DeliveryService } from "../src/delivery/delivery";
import { isAuthoredEchoSuspected } from "../src/monitors/compaction";
import {
	buildMonitorAuthoringPrompt,
	MONITOR_AUTHORING_HEADER,
	MONITOR_AUTHORING_REPORT_RULE,
	MonitorPropagator,
} from "../src/monitors/propagate";
import { MONITOR_INSTRUCTION_MAX_LENGTH, MonitorRegistry } from "../src/monitors/registry";
import { Store } from "../src/store";

import { ScriptedSessionPort } from "./session-port.fake";

const RUNTIME_FEEDBACK_INSTRUCTION =
	"Review the last 24 hours of runtime operation. Report actual friction, failures and latency with public-safe evidence only. Check existing issues before proposing a new one.";
const RUNTIME_FEEDBACK_EVENT_TYPE = "gajaeway.runtime-feedback.daily";

const CONTRACT_ONLY_DAILY_NOTES = [
	{
		date: "2026-10-12",
		note: `What the run should do: ${RUNTIME_FEEDBACK_INSTRUCTION} This review was scheduled for 2026-10-12T11:00Z.`,
	},
	{
		date: "2026-10-13",
		note: "The daily runtime-feedback review was scheduled for 2026-10-13T11:00Z to review the last 24 hours of runtime operation and report actual friction, failures and latency with public-safe evidence. It describes the event only and does not confirm the review ran or what it found. Check existing issues before proposing a new one.",
	},
	{
		date: "2026-10-14",
		note: "The daily runtime feedback review was scheduled for 2026-10-14T11:00Z to look back over the last 24 hours, describe operational friction, failures and delays, and check existing issues before proposing a new one. This note only describes the event and does not confirm that the review ran or what it found.",
	},
	{
		date: "2026-10-15",
		note: "Today's target is a retrospective of runtime activity for the previous 24 hours: identify friction, failures, and latency, then consider existing issues before raising another. The event was scheduled at 2026-10-15T11:00Z; this only describes the target and does not confirm work.",
	},
	{
		date: "2026-10-16",
		note: "A daily review was due at 2026-10-16T11:00Z to examine recent runtime operations, capture problems and slowdowns, and consider current issues before proposing a new issue. This note describes the assignment only; it does not confirm that anything ran or what was found.",
	},
	{
		date: "2026-10-17",
		note: "The scheduled review at 2026-10-17T11:00Z covers the last 24 hours of runtime operation: report actual friction, failures and latency using public-safe evidence; check existing issues before proposing a new one. This note only describes the task; it does not confirm that review ran or what it found.",
	},
	{
		date: "2026-10-18",
		note: "The daily runtime feedback review was scheduled for 2026-10-18T11:00Z to inspect the last 24 hours of operation, report friction, failures and latency with public-safe evidence, and check existing issues before suggesting another. This note only describes the event. It does not confirm that the review ran or say what it found.",
	},
] as const;

function runtimeFeedbackEventContext(date: string) {
	return {
		eventType: RUNTIME_FEEDBACK_EVENT_TYPE,
		eventId: "4afac2c1-bf4b-44e8-9db4-58c453764a8b",
		payload: { at: `${date}T11:00:00.000Z` },
		firedAt: `${date}T11:00:00.000Z`,
	};
}

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

test("the authoring prompt orders execution before the report, with literal wording (#369)", () => {
	const events = [{ eventId: "e-1", eventType: "deploy.blocked", payload: { release: "r-1" } }];
	const prompt = buildMonitorAuthoringPrompt({ guidance: "Pull the deploy queue.", events });
	const lines = prompt.split("\n");
	// Literal phrases, not the exported constants: the wording is the fix.
	expect(lines[0]).toContain("Do the work");
	expect(lines[0]).toContain("using your tools, before you reply");
	expect(lines[1]).toBe("Task: Pull the deploy queue.");
	expect(lines[2]).toContain("Report only after the work is done");
	expect(lines[2]).toContain("restates the task instead of reporting results is rejected");
	expect(lines[3]).toBe(
		`Respond ONLY with a JSON array containing exactly one {"eventId","note"} entry per event: ${JSON.stringify(events)}`,
	);
	expect(lines).toHaveLength(4);
	expect(prompt).not.toContain("Author monitor events");
});

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
		// The instruction is the task to execute, directly after the execute-first
		// header and before the report rule and JSON-array response contract (#369).
		expect(
			prompt.startsWith(
				`${MONITOR_AUTHORING_HEADER}\nTask: ${instruction}\n${MONITOR_AUTHORING_REPORT_RULE}\nRespond ONLY with a JSON array`,
			),
		).toBe(true);
		expect(prompt).toContain('{"eventId","note"}');
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a monitor without an instruction gets no task segment", async () => {
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
		// task segment at all.
		expect(
			prompts[0]!.startsWith(
				`${MONITOR_AUTHORING_HEADER}\n${MONITOR_AUTHORING_REPORT_RULE}\nRespond ONLY with a JSON array`,
			),
		).toBe(true);
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
		const note = `${instruction} Release blocked; issue #632 tracks the blocker.`;
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
		expect(prompts[0]!.startsWith(`${MONITOR_AUTHORING_HEADER}\nTask: ${instruction}\n`)).toBe(true);
		expect(delivered).toHaveLength(1);
		expect(delivered[0]!.origin).toEqual(channelTarget.origin);
		// The mention comes from the typed field, not from the author remembering it.
		expect(delivered[0]!.text).toBe(`<@1468532331001413743> ${note}`);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("#632: dated contract-only notes are terminal and never delivered", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-contract-echo-"));
	try {
		const database = await Store.open(join(directory, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const instruction = RUNTIME_FEEDBACK_INSTRUCTION;
		const monitor = registry.add({
			name: "runtime-feedback",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: [RUNTIME_FEEDBACK_EVENT_TYPE],
			instruction,
			channelTarget: {
				origin: { platform: "discord", kind: "channel", conversationId: "1470204268933022023" },
				mentionUserIds: [],
			},
		});
		const delivered: ChatMessagePayload[] = [];
		const noteFixture = CONTRACT_ONLY_DAILY_NOTES.find(({ date }) => date === "2026-10-18");
		if (!noteFixture) throw new Error("10-18 regression note is missing");
		const note = noteFixture.note;
		const sessionPort = new ScriptedSessionPort({
			onSend: (input, scripted) => {
				const payload = input.text.match(/\[.*\]$/s)?.[0];
				if (!payload) throw new Error("monitor event payload is missing");
				const [event] = JSON.parse(payload) as Array<{ eventId: string }>;
				if (!event) throw new Error("monitor event is missing");
				scripted.complete(input.opRef, JSON.stringify([{ eventId: event.eventId, note }]));
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

		pipeline.submit(monitor.monitorId, RUNTIME_FEEDBACK_EVENT_TYPE, {
			at: "2026-10-18T11:00:00.000Z",
		});
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
	).toBe(true);
	expect(
		isAuthoredEchoSuspected(
			"Review the last 24 hours of runtime activity and report blockers and failures.",
			"Review the last 24 hours of runtime activity and report blockers and failures. Blockers remain.",
		),
	).toBe(true);
	expect(
		isAuthoredEchoSuspected(
			"Review the last 24 hours of runtime activity and report blockers and failures.",
			"Review the last 24 hours.",
		),
	).toBe(false);
});

test("#632: seven paraphrased daily notes are detected against the stored instruction and event contract", () => {
	// The issue quotes the 10-18 ending verbatim; the other entries represent the reported restatement shapes.
	for (const { date, note } of CONTRACT_ONLY_DAILY_NOTES) {
		expect(isAuthoredEchoSuspected(RUNTIME_FEEDBACK_INSTRUCTION, note, runtimeFeedbackEventContext(date))).toBe(true);
	}
});

test("#632: event types and timestamps cannot dilute a short instruction echo", () => {
	const context = {
		eventType: "backup.daily",
		eventId: "4afac2c1-bf4b-44e8-9db4-58c453764a8b",
		payload: { at: "2026-10-18T11:00:00.000Z" },
		firedAt: "2026-10-18T11:00:00.000Z",
	};
	expect(isAuthoredEchoSuspected("Check backups.", "backup daily 2026-10-18T11:00Z", context)).toBe(true);
	expect(isAuthoredEchoSuspected("Check backups.", "Check backups.", context)).toBe(true);
	expect(
		isAuthoredEchoSuspected(
			"Inspect release logs.",
			"This note only describes the event. It does not confirm that the review ran.",
			{ eventType: "deployment.audit" },
		),
	).toBe(true);
});

test("#632: reports with novel commands, PRs, SHAs, URLs, paths, or numeric results remain deliverable", () => {
	const reports = [
		"Ran bun test packages/gateway/test/monitor-instruction.test.ts: 12 tests passed and 0 failed.",
		"Opened PR #640 to add the missing event payload to the contract gate.",
		"Applied the fix in commit c534283180.",
		"Review evidence: https://github.com/Yeachan-Heo/gajae-way/pull/632/checks.",
		"Changed packages/gateway/src/monitors/compaction.ts and verified the new verdict.",
		"The monitor scan found 3 failures and 0 unresolved warnings.",
	];
	const context = runtimeFeedbackEventContext("2026-10-18");
	for (const note of reports) {
		expect(isAuthoredEchoSuspected(RUNTIME_FEEDBACK_INSTRUCTION, note, context)).toBe(false);
	}
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
