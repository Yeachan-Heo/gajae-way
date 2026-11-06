import { expect, spyOn, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { monitorSessionOrigin, originKey } from "@gajae-gateway/protocol";
import { type CliRunner, GjcCliError, type StatusReport } from "@gajae-gateway/subsession";
import { DeliveryService } from "../src/delivery/delivery";
import {
	buildMonitorCompactionDigest,
	type CompactionPort,
	classifyAuthoringFailure,
	classifyExecutorFailure,
	decideSessionRoll,
	isAsideTimeoutFailure,
	isOrphanedExecutorFailure,
	MONITOR_BUSY_FAILURE_ROLL_THRESHOLD,
	MONITOR_CONTEXT_FAILURE_ROLL_THRESHOLD,
	MONITOR_DIGEST_MAX_LENGTH,
	MONITOR_DIGEST_MAX_NOTES,
	MONITOR_PRIOR_RUN_BUSY_FAILURE_ROLL_THRESHOLD,
	type NativeCompactionStatus,
	unavailableCompactionPort,
} from "../src/monitors/compaction";
import { MONITOR_AUTHORING_HEADER, MONITOR_AUTHORING_REPORT_RULE, MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import {
	BrokerSessionPort,
	LastAssistantUnreadableError,
	type SessionContextUsage,
	SessionRequestTimeoutError,
	SessionTerminalError,
} from "../src/orchestrator/session-port";
import { RelayHelloError, TailRunner } from "../src/orchestrator/tail-runner";
import { Store } from "../src/store";
import { BROKER_PROMPT_BUSY_REFUSAL } from "../src/store/groups/bindings";
import { MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS, MONITOR_EVENT_MAX_OPEN_AGE_MS } from "../src/store/groups/monitors";
import {
	attachTestBrokerOwnership,
	initializeTestBrokerAuthority,
	scriptedRelay,
	sessionPortFromScript,
} from "./session-port.fake";

/**
 * Issue #68, final design: native gjc auto-compaction is the primary defence
 * (the `-p --mode json` path goes through AgentSession.prompt()), and this
 * module is the SAFETY NET for the case where it silently stops working.
 *
 * So the load-bearing assertions here are as much about what does NOT happen —
 * a healthy monitor is never rolled, no matter how many turns it takes — as
 * about the last-resort roll itself.
 */

/** A stub port with a scripted status, recording the sessions it was asked about. */
function stubPort(status: NativeCompactionStatus): CompactionPort & { calls: string[] } {
	const calls: string[] = [];
	return {
		calls,
		run: async (sessionId: string) => {
			calls.push(sessionId);
			return { status };
		},
	};
}

/**
 * The terminal submission failure a session wedged on a prior agent run answers
 * with: the runtime took the prompt, no run ever started, and the cause is the
 * agent runtime itself (measured live: `Timed out waiting for prior agent run
 * to finish before prompting.` — memory.canonicalize, every slot from
 * 2026-10-13T21:30Z).
 */
function wedgedSubmissionReport(): StatusReport {
	return {
		operationRef: "gw-p-wedged-submission",
		status: {
			status: "failed",
			commandId: "command-wedged-submission",
			turnId: "turn-wedged-submission",
			terminalAt: Date.now(),
			error: { code: "internal", message: "Prompt submission failed." },
			outcome: {
				kind: "failed",
				code: "prompt_failed",
				message: "Prompt submission failed.",
				phase: "submission",
				category: "agent_runtime",
				provenance: "agent_failed",
				providerCode: "internal",
			},
		},
		summaryCompleted: true,
	};
}

async function realBrokerMonitorHarness(directory: string) {
	const database = await Store.open(join(directory, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(directory, "agent"));
	const repo = join(directory, "workspace");
	const control = { tokens: undefined as number | undefined, goneSession: "" };
	let creates = 0;
	const cli: CliRunner = async (args) => {
		if (args.includes("turn.result")) {
			const opRef = JSON.parse(args[args.indexOf("--json-input") + 1]!).clientRef as string;
			const text = output.get(opRef) ?? "";
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: {
						kind: "prompt",
						clientRef: opRef,
						status: "terminal_ok",
						receiptState: "present",
						startedAt: started.get(opRef),
						terminalAt: Date.now(),
						content: {
							version: 1,
							type: "text",
							text,
							byteLength: new TextEncoder().encode(text).length,
							truncated: false,
						},
					},
				}),
				stderr: "",
			};
		}
		if (args.includes("status")) {
			const opRef = args[args.indexOf("status") + 2]!;
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: {
						operationRef: opRef,
						status: {
							status: output.has(opRef) ? "terminal_ok" : "unknown",
							startedAt: started.get(opRef),
							terminalAt: Date.now(),
						},
						summary: { completed: output.has(opRef) },
					},
				}),
				stderr: "",
			};
		}
		if (args.includes("session.create"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { sessionId: `real-session-${creates++}` } }),
				stderr: "",
			};
		if (args.includes("inspect"))
			return {
				exitCode: 0,
				stdout: JSON.stringify(
					args.at(-1) === control.goneSession
						? { ok: false, error: { code: "session_unavailable", message: "saved session no longer exists" } }
						: {
								ok: true,
								result: { session: { sessionId: args.at(-1), live: true, deleted: false, locator: { cwd: repo } } },
							},
				),
				stderr: "",
			};
		throw new Error(`unexpected CLI command ${args.join(" ")}`);
	};
	const prompts: string[] = [];
	const output = new Map<string, string>();
	const started = new Map<string, number>();
	let lastResponse = "";
	const relay = scriptedRelay((request) => {
		if (request.operation === "turn.prompt") {
			prompts.push(String(request.input.text));
			lastResponse = echoNotes(String(request.input.text));
			output.set(String(request.input.clientRef), lastResponse);
			started.set(String(request.input.clientRef), Date.now());
			return { ok: true, result: { accepted: true, clientRef: request.input.clientRef } };
		}
		if (request.operation === "turn.result")
			return {
				ok: true,
				result: {
					kind: "prompt",
					clientRef: request.input.clientRef,
					status: "terminal_ok",
					receiptState: "present",
					startedAt: started.get(String(request.input.clientRef)),
					terminalAt: Date.now(),
					outcome: { reason: "end_turn", provenance: "agent" },
					content: {
						version: 1,
						type: "text",
						text: output.get(String(request.input.clientRef)) ?? "",
						truncated: false,
						byteLength: new TextEncoder().encode(output.get(String(request.input.clientRef)) ?? "").length,
					},
				},
			};
		if (request.operation === "session.last_assistant")
			return { ok: true, page: { items: [lastResponse], complete: true } };
		if (request.operation === "context.get")
			return {
				ok: true,
				page: {
					items:
						control.tokens === undefined ? [] : [{ usage: { percent: control.tokens / 200, contextWindow: 20_000 } }],
				},
			};
		throw new Error(`unexpected relay request ${request.operation}`);
	});
	const port = new BrokerSessionPort({
		database,
		authority,
		cli,
		tailRunner: new TailRunner({ stream: relay.spawn, repo }),
		sleep: async () => {},
	});
	const registry = new MonitorRegistry(database);
	const makePipeline = () =>
		new MonitorPropagator({
			database,
			registry,
			sessionPort: port,
			repo,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
		});
	return { database, authority, registry, port, relay, prompts, control, makePipeline };
}

/** Valid authoring response for the events carried in `prompt`. */
function echoNotes(prompt: string): string {
	// The event array is the tail after the response-contract marker, so a prompt
	// prefix (guidance, compaction digest) can never be misparsed.
	const marker = "entry per event: ";
	const payload = prompt.slice(prompt.indexOf(marker) + marker.length);
	return JSON.stringify(
		(JSON.parse(payload) as Array<{ eventId: string }>).map(({ eventId }) => ({
			eventId,
			note: `note for ${eventId}`,
		})),
	);
}

async function harness(
	directory: string,
	options: {
		contextFailureRollThreshold?: number;
		protocolFailureRollThreshold?: number;
		compaction?: CompactionPort;
		/** Per-turn response. Defaults to a valid note per claimed event. */
		respond?: (prompt: string, index: number) => string | Promise<string>;
		contextUsage?: (sessionId: string) => SessionContextUsage | undefined;
		sessionIdForBind?: (originKey: string, epoch: number) => string;
		/**
		 * When true for a session, the runtime refuses the prompt with the exact
		 * `busy` envelope the real port surfaces after its bounded busy wait: the
		 * turn never starts, so it is not recorded in `turns`.
		 */
		busy?: (sessionId: string) => boolean;
		/**
		 * When true for a session, the runtime takes the prompt but never starts an
		 * agent run: the exact terminal submission failure a wedged session answers
		 * with (`Timed out waiting for prior agent run to finish before prompting.`).
		 * Nothing was sent, so it is not recorded in `turns` either.
		 */
		wedgedSubmission?: (sessionId: string) => boolean;
		/** Propagator clock; reconcile reclaims a `failed` slot only after its retry backoff (#179). */
		now?: () => number;
	} = {},
) {
	const database = await Store.open(join(directory, "gateway.db"));
	const registry = new MonitorRegistry(database);
	const turns: Array<{ sessionId: string; prompt: string }> = [];
	const sessionPort = sessionPortFromScript({
		// One session id per epoch: exactly what a real bind does, so a roll is
		// observable as a new transcript.
		bind: async (key: string, epoch = 0) => ({
			sessionId: options.sessionIdForBind?.(key, epoch) ?? `event-session-e${epoch}`,
		}),
		respond: async (sessionId: string, text: string) => {
			const index = turns.length;
			turns.push({ sessionId, prompt: text });
			return options.respond ? options.respond(text, index) : echoNotes(text);
		},
	});
	const request = sessionPort.request.bind(sessionPort);
	sessionPort.request = async (input) => {
		if (options.busy?.(input.sessionId))
			throw new GjcCliError("gjc sdk turn.prompt reported failure", 0, "", { code: "busy" });
		if (options.wedgedSubmission?.(input.sessionId)) throw new SessionTerminalError(wedgedSubmissionReport());
		const result = await request(input);
		const contextUsage = options.contextUsage?.(input.sessionId);
		return { ...result, ...(contextUsage === undefined ? {} : { contextUsage }) };
	};
	const pipeline = new MonitorPropagator({
		database,
		registry,
		sessionPort,
		memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
		delivery: new DeliveryService(database),
		emit: () => {},
		...(options.compaction === undefined ? {} : { compaction: options.compaction }),
		...(options.contextFailureRollThreshold === undefined
			? {}
			: { contextFailureRollThreshold: options.contextFailureRollThreshold }),
		...(options.protocolFailureRollThreshold === undefined
			? {}
			: { protocolFailureRollThreshold: options.protocolFailureRollThreshold }),
		...(options.now ? { now: options.now } : {}),
	});
	return { database, registry, pipeline, turns, sessionPort };
}

async function retirementHarness(
	directory: string,
	now?: () => number,
	respond?: (prompt: string, index: number) => string | Promise<string>,
) {
	const origins = new Map<string, number>();
	const f = await harness(directory, {
		...(now ? { now } : {}),
		...(respond ? { respond } : {}),
		contextUsage: (sessionId) => (sessionId.endsWith("-e0") ? { percent: 60, tokens: 12_000 } : undefined),
		sessionIdForBind: (key, epoch) => {
			if (!origins.has(key)) origins.set(key, origins.size);
			const index = origins.get(key)!;
			return `event-session${index === 0 ? "" : `-${index}`}-e${epoch}`;
		},
	});
	attachTestBrokerOwnership(f.database, f.sessionPort, join(directory, "agent"));
	const monitor = f.registry.add({
		name: "retirement",
		trigger: { kind: "cron", schedule: "0 * * * *" },
		eventTypes: ["budget.tick"],
		burstPolicy: "serialize",
		contextRollTokens: 10_000,
	});
	const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
	const retiredKey = `monitor_retired_session:${sessionKey}:event-session-e0`;
	const jobs = spyOn(f.sessionPort, "runningJobs");
	const terminate = spyOn(f.sessionPort, "terminateHost");
	const close = spyOn(f.sessionPort, "close");
	const dispatch = () => f.pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
	return { ...f, monitor, sessionKey, retiredKey, jobs, terminate, close, dispatch };
}

async function failOtherMonitor(f: Awaited<ReturnType<typeof retirementHarness>>): Promise<string> {
	const other = f.registry.add({
		name: "other-recovery",
		trigger: { kind: "cron", schedule: "0 * * * *" },
		eventTypes: ["other.tick"],
		burstPolicy: "serialize",
	});
	const request = spyOn(f.sessionPort, "request").mockRejectedValueOnce(
		new GjcCliError("gjc sdk turn.prompt reported failure", 0, "", { code: "busy" }),
	);
	try {
		const event = await f.pipeline.submitAwaitable(other.monitorId, "other.tick", {});
		expect(f.database.monitors.failure(event)?.code).toBe("session_busy");
		expect(f.database.monitors.authoredOutput(event)).toBeUndefined();
		return event;
	} finally {
		request.mockRestore();
	}
}

for (const restart of [false, true]) {
	test(`a size-rolled host without saved pointers is retired on the next fresh dispatch${restart ? " after restart" : ""}`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-"));
		const f = await retirementHarness(directory);
		let active = f.pipeline;
		const log = spyOn(console, "info").mockImplementation(() => {});
		try {
			await f.dispatch();
			await f.dispatch();
			expect(f.terminate).not.toHaveBeenCalled();
			expect(f.database.bindings.getSessionRecord(f.sessionKey)).toMatchObject({
				epoch: 1,
				sessionId: "event-session-e1",
			});
			expect(JSON.parse(f.database.core.metaGet(f.retiredKey)!)).toEqual({
				sessionId: "event-session-e0",
				epoch: 0,
				retiredAt: expect.any(Number),
			});
			if (restart) {
				f.pipeline.dispose();
				active = new MonitorPropagator({
					database: f.database,
					registry: f.registry,
					sessionPort: f.sessionPort,
					memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
					delivery: new DeliveryService(f.database),
					emit: () => {},
				});
			}
			await active.submitAwaitable(f.monitor.monitorId, "budget.tick", {});
			expect(f.jobs).toHaveBeenCalledTimes(1);
			expect(f.close).toHaveBeenCalledTimes(1);
			expect(f.close.mock.calls[0]?.[0]).toEqual({ sessionId: "event-session-e0", repo: process.cwd() });
			expect(f.sessionPort.closes.map(({ sessionId }) => sessionId)).toEqual(["event-session-e0"]);
			expect(f.terminate).not.toHaveBeenCalled();
			expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
			expect(log.mock.calls).toContainEqual([
				`monitor retired session close for ${f.sessionKey}: session=event-session-e0 outcome=closed`,
			]);
		} finally {
			log.mockRestore();
			active.dispose();
			f.database.close();
			await rm(directory, { recursive: true, force: true });
		}
	});
}

test("a size-rolled host waits for its tagged pointer to be harvested before retirement", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-pointer-"));
	const f = await retirementHarness(directory);
	try {
		const fetchLastAssistant = f.sessionPort.fetchLastAssistant.bind(f.sessionPort);
		f.sessionPort.fetchLastAssistant = async () => {
			throw new LastAssistantUnreadableError("last_assistant_no_page");
		};
		const savedEvent = await f.dispatch();
		f.sessionPort.fetchLastAssistant = fetchLastAssistant;
		expect(f.pipeline.settle(savedEvent).done).toBe(true);
		await f.dispatch();
		await f.dispatch();
		const pointerKey = `monitor_authoring_turn:${savedEvent}`;
		expect(JSON.parse(f.database.core.metaGet(pointerKey)!)).toMatchObject({
			sessionId: "event-session-e0",
			sizeRolled: true,
		});
		await f.dispatch();
		expect(f.jobs).not.toHaveBeenCalled();
		expect(f.terminate).not.toHaveBeenCalled();
		expect(f.close).not.toHaveBeenCalled();
		expect(f.database.core.metaGet(f.retiredKey)).toBeDefined();
		expect(f.pipeline.retry(savedEvent).done).toBe(true);
		await f.pipeline.reconcile();
		await f.pipeline.retiredSessionSweepSettled();
		expect(f.database.monitors.authoredOutput(savedEvent)).toBe(`note for ${savedEvent}`);
		expect(f.database.core.metaGet(pointerKey)).toBeUndefined();
		// Reconcile's sweep saw the saved pointer before recovery harvested it.
		expect(f.terminate).not.toHaveBeenCalled();
		expect(f.close).not.toHaveBeenCalled();
		expect(f.database.core.metaGet(f.retiredKey)).toBeDefined();
		await f.dispatch();
		expect(f.close).toHaveBeenCalledTimes(1);
		expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

for (const unavailable of [false, true]) {
	test(`a size-rolled host waits for ${unavailable ? "an unreadable job probe" : "a running background job"} and retires once jobs drain`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-jobs-"));
		const f = await retirementHarness(directory);
		const log = spyOn(console, "info").mockImplementation(() => {});
		try {
			await f.dispatch();
			await f.dispatch();
			f.sessionPort.hostJobs.set(
				"event-session-e0",
				unavailable ? new Error("host unavailable") : [{ id: "job-1", type: "bash", label: "background" }],
			);
			await f.dispatch();
			expect(f.terminate).not.toHaveBeenCalled();
			expect(f.database.core.metaGet(f.retiredKey)).toBeDefined();
			expect(log.mock.calls).toContainEqual([
				`monitor retired session close for ${f.sessionKey}: session=event-session-e0 outcome=held:runningJobs_${unavailable ? "unavailable" : "busy"}`,
			]);
			f.sessionPort.hostJobs.set("event-session-e0", []);
			await f.dispatch();
			expect(f.jobs).toHaveBeenCalledTimes(2);
			expect(f.close).toHaveBeenCalledTimes(1);
			expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
		} finally {
			log.mockRestore();
			f.pipeline.dispose();
			f.database.close();
			await rm(directory, { recursive: true, force: true });
		}
	});
}

for (const code of [
	"endpoint_stale",
	"terminal_uncertain",
	"close_refused",
	"transport",
	"not_found",
	"operation_failed",
	"session_unavailable",
] as const) {
	test(`size-rolled session close failure keeps its record code=${code}`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-outcome-"));
		const f = await retirementHarness(directory);
		const log = spyOn(console, "info").mockImplementation(() => {});
		try {
			await f.dispatch();
			await f.dispatch();
			// parseEnvelope surfaces broker error payloads as GjcCliError.details, not error.code.
			f.sessionPort.closeOutcomes.set("event-session-e0", [
				code === "transport"
					? new GjcCliError("transport lost", 1, "")
					: new GjcCliError("gjc sdk session.close reported failure", 0, "", { code }),
			]);
			await f.dispatch();
			expect(f.close).toHaveBeenCalledTimes(1);
			expect(JSON.parse(f.database.core.metaGet(f.retiredKey)!)).toMatchObject({
				phase: "closing",
				closeStartedAt: expect.any(Number),
			});
			expect(log.mock.calls).toContainEqual([
				`monitor retired session close for ${f.sessionKey}: session=event-session-e0 outcome=error:${code === "transport" ? "unknown" : code}`,
			]);
			// A prior attempt grants no authority when its host can no longer answer.
			f.sessionPort.hostTurnRunning.set("event-session-e0", new Error("endpoint unavailable"));
			f.sessionPort.hostQueueEmpty.set("event-session-e0", new Error("endpoint unavailable"));
			f.sessionPort.hostJobs.set("event-session-e0", new Error("endpoint unavailable"));
			f.jobs.mockClear();
			await f.dispatch();
			expect(f.close).toHaveBeenCalledTimes(1);
			expect(f.jobs).toHaveBeenCalledTimes(1);
			expect(f.terminate).not.toHaveBeenCalled();
			expect(f.database.core.metaGet(f.retiredKey)).toBeDefined();
		} finally {
			log.mockRestore();
			f.pipeline.dispose();
			f.database.close();
			await rm(directory, { recursive: true, force: true });
		}
	});
}

test("size-rolled host retirement drops unsupported records without probing jobs", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-unsupported-"));
	const f = await retirementHarness(directory);
	const log = spyOn(console, "info").mockImplementation(() => {});
	try {
		await f.dispatch();
		await f.dispatch();
		Object.defineProperty(f.sessionPort, "close", { value: undefined });
		await f.dispatch();
		expect(f.jobs).not.toHaveBeenCalled();
		expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
		expect(log.mock.calls).toContainEqual([
			`monitor retired session close for ${f.sessionKey}: session=event-session-e0 outcome=unsupported`,
		]);
	} finally {
		log.mockRestore();
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("retired-session sweeping does not touch another origin's pending retirement", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-origin-"));
	const f = await retirementHarness(directory);
	try {
		await f.dispatch();
		await f.dispatch();
		const record = f.database.core.metaGet(f.retiredKey);
		const otherEvent = await f.pipeline.submitAwaitable(f.monitor.monitorId, "undeclared.tick", {});
		expect(f.database.monitors.authoredOutput(otherEvent)).toBe(`note for ${otherEvent}`);
		expect(f.terminate).not.toHaveBeenCalled();
		expect(f.jobs).not.toHaveBeenCalled();
		expect(f.database.core.metaGet(f.retiredKey)).toBe(record);
		await f.dispatch();
		expect(f.close).toHaveBeenCalledTimes(1);
		expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("retired-session sweeping ignores an inconsistent retirement record for the live binding", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-live-"));
	const f = await retirementHarness(directory);
	try {
		await f.dispatch();
		expect(f.database.bindings.getSessionRecord(f.sessionKey)?.sessionId).toBe("event-session-e0");
		// Deliberately inconsistent durable input exercises the defensive live guard.
		f.database.core.metaSet(
			f.retiredKey,
			JSON.stringify({ sessionId: "event-session-e0", epoch: 0, retiredAt: Date.now() }),
		);
		await f.dispatch();
		expect(f.terminate).not.toHaveBeenCalled();
		expect(f.jobs).not.toHaveBeenCalled();
		expect(f.database.core.metaGet(f.retiredKey)).toBeDefined();
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("retired-session sweeping ignores invalid authoring pointers but holds without a job probe", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-partial-"));
	const f = await retirementHarness(directory);
	try {
		await f.dispatch();
		await f.dispatch();
		f.database.core.metaSet("monitor_authoring_turn:malformed", "{invalid");
		f.database.core.metaSet("monitor_authoring_turn:partial", JSON.stringify({ sessionId: "event-session-e0" }));
		Object.defineProperty(f.sessionPort, "runningJobs", { value: undefined });
		await f.dispatch();
		expect(f.terminate).not.toHaveBeenCalled();
		expect(f.database.core.metaGet(f.retiredKey)).toBeDefined();
		Object.defineProperty(f.sessionPort, "runningJobs", { value: f.jobs });
		await f.dispatch();
		expect(f.close).toHaveBeenCalledTimes(1);
		expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("monitor removal preserves retired-session records for declared, catch-all and other monitor origins", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-remove-"));
	const f = await retirementHarness(directory);
	try {
		for (const eventType of ["budget.tick", "undeclared:tick"]) {
			await f.pipeline.submitAwaitable(f.monitor.monitorId, eventType, {});
			await f.pipeline.submitAwaitable(f.monitor.monitorId, eventType, {});
		}
		const other = f.registry.add({ ...f.monitor, name: "other", eventTypes: ["other:tick"] });
		await f.pipeline.submitAwaitable(other.monitorId, "other:tick", {});
		await f.pipeline.submitAwaitable(other.monitorId, "other:tick", {});
		const prefix = "monitor_retired_session:";
		const records = f.database.core.metaKeysInRange(prefix, `${prefix}\uffff`);
		expect(records).toHaveLength(3);
		const otherKey = `${prefix}${originKey(monitorSessionOrigin(other.monitorId, "other:tick"))}:event-session-2-e0`;
		expect(records).toContain(otherKey);
		expect(f.registry.remove(f.monitor.monitorId)).toBe(true);
		expect(f.database.core.metaKeysInRange(prefix, `${prefix}\uffff`)).toEqual(records);
		expect(f.terminate).not.toHaveBeenCalled();
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

for (const restart of [false, true]) {
	test(`reconcile closes a size-rolled host after monitor removal without another dispatch${restart ? " after database restart" : ""}`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-reconcile-remove-"));
		const f = await retirementHarness(directory);
		let database = f.database;
		let active = f.pipeline;
		try {
			await f.dispatch();
			await f.dispatch();
			expect(f.registry.remove(f.monitor.monitorId)).toBe(true);
			expect(database.core.metaGet(f.retiredKey)).toBeDefined();
			expect(database.bindings.getSessionRecord(f.sessionKey)?.sessionId).toBe("event-session-e1");
			if (restart) {
				active.dispose();
				database.close();
				database = await Store.open(join(directory, "gateway.db"));
				active = new MonitorPropagator({
					database,
					registry: new MonitorRegistry(database),
					sessionPort: f.sessionPort,
					memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
					delivery: new DeliveryService(database),
					emit: () => {},
				});
			}
			await active.reconcile();
			await active.retiredSessionSweepSettled();
			expect(f.sessionPort.closes.map(({ sessionId }) => sessionId)).toEqual(["event-session-e0"]);
			expect(f.jobs).toHaveBeenCalledTimes(1);
			expect(f.turns).toHaveLength(2);
			expect(f.terminate).not.toHaveBeenCalled();
			expect(database.core.metaGet(f.retiredKey)).toBeUndefined();
			expect(database.bindings.getSessionRecord(f.sessionKey)?.sessionId).toBe("event-session-e1");
		} finally {
			active.dispose();
			database.close();
			await rm(directory, { recursive: true, force: true });
		}
	});
}

test("reconcile holds a removed monitor's retired host while its saved authoring pointer survives", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-reconcile-pointer-"));
	const f = await retirementHarness(directory);
	try {
		const fetchLastAssistant = f.sessionPort.fetchLastAssistant.bind(f.sessionPort);
		f.sessionPort.fetchLastAssistant = async () => {
			throw new LastAssistantUnreadableError("last_assistant_no_page");
		};
		const savedEvent = await f.dispatch();
		f.sessionPort.fetchLastAssistant = fetchLastAssistant;
		expect(f.pipeline.settle(savedEvent).done).toBe(true);
		await f.dispatch();
		await f.dispatch();
		const pointerKey = `monitor_authoring_turn:${savedEvent}`;
		const pointer = f.database.core.metaGet(pointerKey);
		expect(JSON.parse(pointer!)).toMatchObject({ sessionId: "event-session-e0", sizeRolled: true });
		const binding = f.database.bindings.getSessionRecord(f.sessionKey);
		expect(f.registry.remove(f.monitor.monitorId)).toBe(true);
		expect(f.database.core.metaGet(pointerKey)).toBe(pointer);
		expect(f.database.bindings.getSessionRecord(f.sessionKey)).toEqual(binding);
		await f.pipeline.reconcile();
		await f.pipeline.retiredSessionSweepSettled();
		expect(f.close).not.toHaveBeenCalled();
		expect(f.jobs).not.toHaveBeenCalled();
		expect(f.database.core.metaGet(f.retiredKey)).toBeDefined();
		expect(f.database.core.metaGet(pointerKey)).toBe(pointer);
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

for (const probe of ["turnRunning", "queueEmpty", "runningJobs"] as const) {
	for (const evidence of ["busy", "unknown", "throws", "missing"] as const) {
		if (probe === "queueEmpty" && evidence === "missing") continue;
		test(`reconcile holds a removed monitor's retired host when ${probe} is ${evidence}`, async () => {
			const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-reconcile-probe-"));
			const f = await retirementHarness(directory);
			try {
				await f.dispatch();
				await f.dispatch();
				f.registry.remove(f.monitor.monitorId);
				const original = f.sessionPort[probe];
				Object.defineProperty(f.sessionPort, probe, {
					configurable: true,
					value:
						evidence === "missing"
							? undefined
							: async () => {
									if (evidence === "throws") throw new Error("host unavailable");
									if (evidence === "unknown") return undefined;
									return probe === "turnRunning" ? true : probe === "queueEmpty" ? false : [{ id: "busy" }];
								},
				});
				await f.pipeline.reconcile();
				await f.pipeline.retiredSessionSweepSettled();
				expect(f.close).not.toHaveBeenCalled();
				expect(f.database.core.metaGet(f.retiredKey)).toBeDefined();
				Object.defineProperty(f.sessionPort, probe, { value: original });
				await f.pipeline.reconcile();
				await f.pipeline.retiredSessionSweepSettled();
				expect(f.close).toHaveBeenCalledTimes(1);
				expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
			} finally {
				f.pipeline.dispose();
				f.database.close();
				await rm(directory, { recursive: true, force: true });
			}
		});
	}
}

test("reconcile attempts at most two retired hosts globally and fairly across removed monitor origins", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-reconcile-fair-"));
	let now = Date.now();
	const f = await retirementHarness(directory, () => now);
	try {
		const eventTypes = ["budget.tick", "second.tick", "third.tick"];
		f.registry.update({ monitorId: f.monitor.monitorId, eventTypes });
		for (const eventType of eventTypes) {
			await f.pipeline.submitAwaitable(f.monitor.monitorId, eventType, {});
			await f.pipeline.submitAwaitable(f.monitor.monitorId, eventType, {});
			now++;
		}
		const sessions = ["event-session-e0", "event-session-1-e0", "event-session-2-e0"];
		for (const sessionId of sessions) f.sessionPort.hostJobs.set(sessionId, new Error("host unavailable"));
		f.registry.remove(f.monitor.monitorId);
		const exclusive = spyOn(f.sessionPort, "runExclusive");
		for (const expected of [
			[sessions[0], sessions[1]],
			[sessions[2], sessions[0]],
			[sessions[1], sessions[2]],
		]) {
			f.jobs.mockClear();
			exclusive.mockClear();
			await f.pipeline.reconcile();
			await f.pipeline.retiredSessionSweepSettled();
			expect(f.jobs.mock.calls.map(([input]) => input.sessionId)).toEqual(expected);
			expect(exclusive).not.toHaveBeenCalled();
		}
		expect(f.close).not.toHaveBeenCalled();
		for (const sessionId of sessions) f.sessionPort.hostJobs.set(sessionId, []);
		for (const count of [2, 1]) {
			f.close.mockClear();
			await f.pipeline.reconcile();
			await f.pipeline.retiredSessionSweepSettled();
			expect(f.close).toHaveBeenCalledTimes(count);
		}
		expect(f.database.core.metaKeysInRange("monitor_retired_session:", "monitor_retired_session;")).toEqual([]);
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("a gated retirement probe permits its own saved-event recovery and later other-origin recovery", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-reconcile-overlap-"));
	let now = Date.now();
	const f = await retirementHarness(directory, () => now);
	let release = () => {};
	let sweep: Promise<void> | undefined;
	try {
		await f.dispatch();
		await f.dispatch();
		// Keep e0 until A has a real failed dispatch with a saved turn on e1.
		f.sessionPort.hostJobs.set("event-session-e0", [{ type: "bash", id: "busy", label: "busy" }]);
		const unreadable = spyOn(f.sessionPort, "fetchLastAssistant").mockRejectedValueOnce(
			new LastAssistantUnreadableError("last_assistant_no_page"),
		);
		const failedA = await f.dispatch();
		unreadable.mockRestore();
		expect(f.database.monitors.eventGet(failedA)?.stage).toBe("failed");
		expect(JSON.parse(f.database.core.metaGet(`monitor_authoring_turn:${failedA}`)!)).toMatchObject({
			sessionId: "event-session-e1",
		});
		now++;
		const failedB = await failOtherMonitor(f);
		expect(f.database.monitors.eventsOpen().map((row) => row.event_id)).toEqual([failedA, failedB]);
		now += 13 * 60 * 60_000;
		f.sessionPort.hostJobs.set("event-session-e0", []);
		f.jobs.mockClear();
		let entered = () => {};
		const inProbe = new Promise<void>((resolve) => {
			entered = resolve;
		});
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		const turnRunning = f.sessionPort.turnRunning.bind(f.sessionPort);
		const probe = spyOn(f.sessionPort, "turnRunning").mockImplementation(async (input) => {
			entered();
			await gate;
			return turnRunning(input);
		});
		const exclusive = spyOn(f.sessionPort, "runExclusive");
		sweep = f.pipeline.reconcile();
		await inProbe;
		const currentSweep = f.pipeline.retiredSessionSweepSettled();
		const recovered = await Promise.race([sweep.then(() => true), Bun.sleep(1_000).then(() => false)]);
		expect(recovered).toBe(true);
		expect(f.database.monitors.authoredOutput(failedA)).toBe(`note for ${failedA}`);
		expect(f.database.monitors.authoredOutput(failedB)).toBe(`note for ${failedB}`);
		await f.pipeline.reconcile();
		expect(f.pipeline.retiredSessionSweepSettled()).toBe(currentSweep);
		expect(probe).toHaveBeenCalledTimes(1);
		expect(exclusive.mock.calls.filter(([key]) => key === f.sessionKey)).toHaveLength(1);
		expect(f.jobs).not.toHaveBeenCalled();
		expect(f.close).not.toHaveBeenCalled();
		release();
		await sweep;
		await f.pipeline.retiredSessionSweepSettled();
		expect(f.close).toHaveBeenCalledTimes(1);
		expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
	} finally {
		release();
		await sweep;
		await f.pipeline.retiredSessionSweepSettled();
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("a rolled origin's gated authoring request cannot delay another monitor's failed-event recovery", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-reconcile-busy-origin-"));
	let release = () => {};
	let entered = () => {};
	const inRequest = new Promise<void>((resolve) => {
		entered = resolve;
	});
	const gate = new Promise<void>((resolve) => {
		release = resolve;
	});
	const f = await retirementHarness(directory, undefined, async (prompt, index) => {
		if (index === 1) {
			entered();
			await gate;
		}
		return echoNotes(prompt);
	});
	let dispatch: Promise<string> | undefined;
	let recovery: Promise<void> | undefined;
	try {
		await f.dispatch();
		dispatch = f.dispatch();
		await inRequest;
		expect(f.database.bindings.getSessionRecord(f.sessionKey)?.epoch).toBe(1);
		expect(f.database.core.metaGet(f.retiredKey)).toBeDefined();
		const failed = await failOtherMonitor(f);
		const exclusive = spyOn(f.sessionPort, "runExclusive");
		recovery = f.pipeline.reconcile();
		const recovered = await Promise.race([recovery.then(() => true), Bun.sleep(1_000).then(() => false)]);
		expect(recovered).toBe(true);
		expect(f.database.monitors.authoredOutput(failed)).toBe(`note for ${failed}`);
		expect(exclusive.mock.calls.filter(([key]) => key === f.sessionKey)).toHaveLength(0);
		await f.pipeline.retiredSessionSweepSettled();
		expect(f.jobs).toHaveBeenCalledTimes(1);
		expect(f.close).toHaveBeenCalledTimes(1);
		expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
		release();
		await dispatch;
		await f.pipeline.retiredSessionSweepSettled();
		expect(f.close).toHaveBeenCalledTimes(1);
		expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
	} finally {
		release();
		await dispatch;
		await recovery;
		await f.pipeline.retiredSessionSweepSettled();
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

for (const expire of [false, true]) {
	test(`drain ${expire ? "bounds its wait for" : "waits for"} a background retirement sweep and dispose prevents later closes`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-reconcile-drain-"));
		const f = await retirementHarness(directory);
		let release = () => {};
		let draining: Promise<void> | undefined;
		try {
			for (const eventType of ["budget.tick", "second.tick"]) {
				await f.pipeline.submitAwaitable(f.monitor.monitorId, eventType, {});
				await f.pipeline.submitAwaitable(f.monitor.monitorId, eventType, {});
			}
			let entered = () => {};
			const inProbe = new Promise<void>((resolve) => {
				entered = resolve;
			});
			const gate = new Promise<void>((resolve) => {
				release = resolve;
			});
			const turnRunning = f.sessionPort.turnRunning.bind(f.sessionPort);
			const probe = spyOn(f.sessionPort, "turnRunning").mockImplementation(async (input) => {
				entered();
				await gate;
				return turnRunning(input);
			});
			await f.pipeline.reconcile();
			await inProbe;
			const sweep = f.pipeline.retiredSessionSweepSettled();
			let settled = false;
			void sweep.then(() => {
				settled = true;
			});
			const started = performance.now();
			let drained = false;
			draining = f.pipeline.drain(expire ? 20 : 1_000).then(() => {
				drained = true;
			});
			await Bun.sleep(0);
			expect(drained).toBe(false);
			await f.pipeline.reconcile();
			expect(f.pipeline.retiredSessionSweepSettled()).toBe(sweep);
			expect(probe).toHaveBeenCalledTimes(1);
			if (expire) {
				await draining;
				expect(performance.now() - started).toBeLessThan(1_000);
				expect(settled).toBe(false);
			}
			release();
			await draining;
			await sweep;
			expect(settled).toBe(true);
			expect(probe).toHaveBeenCalledTimes(1);
			expect(f.close).not.toHaveBeenCalled();
			expect(f.database.core.metaKeysInRange("monitor_retired_session:", "monitor_retired_session;")).toHaveLength(2);
			await f.pipeline.reconcile();
			await f.pipeline.retiredSessionSweepSettled();
			expect(probe).toHaveBeenCalledTimes(1);
			expect(f.close).not.toHaveBeenCalled();
		} finally {
			release();
			await draining;
			await f.pipeline.retiredSessionSweepSettled();
			f.pipeline.dispose();
			f.database.close();
			await rm(directory, { recursive: true, force: true });
		}
	});
}

test("a gated retirement sweep resolves after drain times out and the real Store closes", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-closed-store-"));
	const f = await retirementHarness(directory);
	let release = () => {};
	let sweep: Promise<void> | undefined;
	let closed = false;
	const unhandled: unknown[] = [];
	const onUnhandled = (reason: unknown) => {
		unhandled.push(reason);
	};
	process.on("unhandledRejection", onUnhandled);
	try {
		await f.dispatch();
		await f.dispatch();
		let entered = () => {};
		const inProbe = new Promise<void>((resolve) => {
			entered = resolve;
		});
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		const turnRunning = f.sessionPort.turnRunning.bind(f.sessionPort);
		f.sessionPort.turnRunning = async (input) => {
			entered();
			await gate;
			return turnRunning(input);
		};
		await f.pipeline.reconcile();
		await inProbe;
		sweep = f.pipeline.retiredSessionSweepSettled();
		let settled = false;
		void sweep.then(() => {
			settled = true;
		});
		await f.pipeline.drain(20);
		expect(settled).toBe(false);
		f.database.close();
		closed = true;
		release();
		await sweep;
		await Bun.sleep(0);
		expect(settled).toBe(true);
		expect(unhandled).toEqual([]);
		expect(f.close).not.toHaveBeenCalled();
	} finally {
		release();
		await sweep;
		await f.pipeline.retiredSessionSweepSettled();
		process.off("unhandledRejection", onUnhandled);
		f.pipeline.dispose();
		if (!closed) f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

for (const backgroundFirst of [true, false]) {
	test(`inline and background retirement share one probe sequence and close when ${backgroundFirst ? "background" : "inline"} starts first`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-attempt-guard-"));
		const f = await retirementHarness(directory);
		let release = () => {};
		let recovery: Promise<void> | undefined;
		let dispatch: Promise<string> | undefined;
		try {
			await f.dispatch();
			await f.dispatch();
			let entered = () => {};
			const inProbe = new Promise<void>((resolve) => {
				entered = resolve;
			});
			const gate = new Promise<void>((resolve) => {
				release = resolve;
			});
			const order: string[] = [];
			const turnRunning = f.sessionPort.turnRunning.bind(f.sessionPort);
			const queueEmpty = f.sessionPort.queueEmpty.bind(f.sessionPort);
			const runningJobs = f.sessionPort.runningJobs.bind(f.sessionPort);
			f.sessionPort.turnRunning = async (input) => {
				order.push("turn");
				entered();
				await gate;
				return turnRunning(input);
			};
			f.sessionPort.queueEmpty = async (input) => {
				order.push("queue");
				return queueEmpty(input);
			};
			f.sessionPort.runningJobs = async (input) => {
				order.push("jobs");
				return runningJobs(input);
			};
			const exclusive = spyOn(f.sessionPort, "runExclusive");
			if (backgroundFirst) recovery = f.pipeline.reconcile();
			else dispatch = f.dispatch();
			await inProbe;
			if (backgroundFirst) {
				dispatch = f.dispatch();
				const authored = await Promise.race([dispatch.then(() => true), Bun.sleep(1_000).then(() => false)]);
				expect(authored).toBe(true);
			} else recovery = f.pipeline.reconcile();
			await recovery;
			expect(exclusive.mock.calls.map(([key]) => key)).toEqual([f.sessionKey]);
			expect(order).toEqual(["turn"]);
			expect(f.close).not.toHaveBeenCalled();
			release();
			const event = await dispatch;
			await f.pipeline.retiredSessionSweepSettled();
			expect(f.database.monitors.authoredOutput(event!)).toBe(`note for ${event}`);
			expect(f.turns).toHaveLength(3);
			expect(order).toEqual(["turn", "queue", "jobs"]);
			expect(f.close).toHaveBeenCalledTimes(1);
			expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
		} finally {
			release();
			await recovery;
			await dispatch;
			await f.pipeline.retiredSessionSweepSettled();
			f.pipeline.dispose();
			f.database.close();
			await rm(directory, { recursive: true, force: true });
		}
	});
}

test("reconcile contains retirement enumeration errors and retries on its next pass", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-reconcile-error-"));
	const f = await retirementHarness(directory);
	try {
		await f.dispatch();
		await f.dispatch();
		f.registry.remove(f.monitor.monitorId);
		const metaKeysInRange = f.database.core.metaKeysInRange.bind(f.database.core);
		const keys = spyOn(f.database.core, "metaKeysInRange").mockImplementation((start, end) => {
			if (start === "monitor_retired_session:") throw new Error("retirement scan failed");
			return metaKeysInRange(start, end);
		});
		try {
			await f.pipeline.reconcile();
			await f.pipeline.retiredSessionSweepSettled();
			expect(f.close).not.toHaveBeenCalled();
			expect(f.database.core.metaGet(f.retiredKey)).toBeDefined();
		} finally {
			keys.mockRestore();
		}
		await f.pipeline.reconcile();
		await f.pipeline.retiredSessionSweepSettled();
		expect(f.close).toHaveBeenCalledTimes(1);
		expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("reconcile keeps failed closes after removal and retries only with fresh idle evidence", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-reconcile-close-"));
	const f = await retirementHarness(directory);
	try {
		await f.dispatch();
		await f.dispatch();
		f.registry.remove(f.monitor.monitorId);
		f.sessionPort.closeOutcomes.set("event-session-e0", [new GjcCliError("transport lost", 1, "")]);
		await f.pipeline.reconcile();
		await f.pipeline.retiredSessionSweepSettled();
		expect(f.close).toHaveBeenCalledTimes(1);
		expect(JSON.parse(f.database.core.metaGet(f.retiredKey)!)).toHaveProperty("phase", "closing");
		f.sessionPort.hostTurnRunning.set("event-session-e0", undefined);
		await f.pipeline.reconcile();
		await f.pipeline.retiredSessionSweepSettled();
		expect(f.close).toHaveBeenCalledTimes(1);
		expect(f.database.core.metaGet(f.retiredKey)).toBeDefined();
		f.sessionPort.hostTurnRunning.set("event-session-e0", false);
		await f.pipeline.reconcile();
		await f.pipeline.retiredSessionSweepSettled();
		expect(f.close).toHaveBeenCalledTimes(2);
		expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("reconcile ignores malformed retirement keys without blocking valid removed-monitor cleanup", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-reconcile-key-"));
	const f = await retirementHarness(directory);
	try {
		await f.dispatch();
		await f.dispatch();
		f.registry.remove(f.monitor.monitorId);
		const raw = f.database.core.metaGet(f.retiredKey)!;
		const invalidKeys = ["monitor_retired_session:invalid:event-session-e0", `${f.retiredKey}:wrong-session`];
		for (const key of invalidKeys) f.database.core.metaSet(key, raw);
		await f.pipeline.reconcile();
		await f.pipeline.retiredSessionSweepSettled();
		expect(f.close).toHaveBeenCalledTimes(1);
		expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
		for (const key of invalidKeys) expect(f.database.core.metaGet(key)).toBe(raw);
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("a size roll without a persisted session ID does not create an empty retirement record", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-empty-"));
	const f = await harness(directory, { contextUsage: () => ({ percent: 60, tokens: 12_000 }) });
	try {
		const monitor = f.registry.add({
			name: "empty-binding",
			trigger: { kind: "cron", schedule: "0 * * * *" },
			eventTypes: ["budget.tick"],
			burstPolicy: "serialize",
			contextRollTokens: 10_000,
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
		await f.pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		expect(f.database.bindings.getSessionRecord(sessionKey)?.sessionId).toBe("");
		await f.pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		expect(f.database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
		expect(f.database.core.metaKeysInRange("monitor_retired_session:", "monitor_retired_session;")).toEqual([]);
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("monitor removal during the last retired-session probe still permits close", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-remove-probe-"));
	const f = await retirementHarness(directory);
	try {
		await f.dispatch();
		await f.dispatch();
		f.sessionPort.beforeRunningJobs = () => {
			f.registry.remove(f.monitor.monitorId);
		};
		await f.dispatch();
		expect(f.terminate).not.toHaveBeenCalled();
		expect(f.close).toHaveBeenCalledTimes(1);
		expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("size-rolled retirement waits for an autonomous turn even after jobs drain", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-autonomous-"));
	const f = await retirementHarness(directory);
	try {
		await f.dispatch();
		await f.dispatch();
		expect(f.database.core.metaKeysInRange("monitor_authoring_turn:", "monitor_authoring_turn;")).toEqual([]);
		f.sessionPort.hostTurnRunning.set("event-session-e0", true);
		await f.dispatch();
		expect(f.jobs).toHaveBeenCalledTimes(1);
		expect(f.terminate).not.toHaveBeenCalled();
		expect(f.close).not.toHaveBeenCalled();
		expect(f.database.core.metaGet(f.retiredKey)).toBeDefined();
		f.sessionPort.hostTurnRunning.set("event-session-e0", false);
		await f.dispatch();
		expect(f.close).toHaveBeenCalledTimes(1);
		expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

for (const evidence of ["undefined", "throws", "missing"] as const) {
	test(`size-rolled retirement holds when turnRunning ${evidence}`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-turn-"));
		const f = await retirementHarness(directory);
		try {
			await f.dispatch();
			await f.dispatch();
			if (evidence === "missing") Object.defineProperty(f.sessionPort, "turnRunning", { value: undefined });
			else
				f.sessionPort.hostTurnRunning.set(
					"event-session-e0",
					evidence === "throws" ? new Error("context unavailable") : undefined,
				);
			const event = await f.dispatch();
			expect(f.database.monitors.authoredOutput(event)).toBe(`note for ${event}`);
			expect(f.terminate).not.toHaveBeenCalled();
			expect(f.close).not.toHaveBeenCalled();
			expect(JSON.parse(f.database.core.metaGet(f.retiredKey)!)).not.toHaveProperty("phase");
		} finally {
			f.pipeline.dispose();
			f.database.close();
			await rm(directory, { recursive: true, force: true });
		}
	});
}

for (const evidence of [false, "throws", "missing"] as const) {
	test(`size-rolled retirement ${evidence === "missing" ? "allows an absent queue method" : `holds when queueEmpty is ${evidence}`}`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-queue-"));
		const f = await retirementHarness(directory);
		try {
			await f.dispatch();
			await f.dispatch();
			if (evidence === "missing") Object.defineProperty(f.sessionPort, "queueEmpty", { value: undefined });
			else
				f.sessionPort.hostQueueEmpty.set(
					"event-session-e0",
					evidence === "throws" ? new Error("queue unavailable") : false,
				);
			await f.dispatch();
			expect(f.close).toHaveBeenCalledTimes(evidence === "missing" ? 1 : 0);
			if (evidence !== "missing") {
				expect(f.database.core.metaGet(f.retiredKey)).toBeDefined();
				f.sessionPort.hostQueueEmpty.set("event-session-e0", true);
				await f.dispatch();
			}
			expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
		} finally {
			f.pipeline.dispose();
			f.database.close();
			await rm(directory, { recursive: true, force: true });
		}
	});
}

for (const change of ["remove", "replace_record", "saved_pointer", "live_binding"] as const) {
	test(`size-rolled close authorization ${change === "remove" ? "permits" : "refuses"} ${change} during the last job probe`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-guard-"));
		const f = await retirementHarness(directory);
		try {
			await f.dispatch();
			await f.dispatch();
			f.sessionPort.beforeRunningJobs = () => {
				if (change === "remove") f.registry.remove(f.monitor.monitorId);
				if (change === "replace_record") {
					const record = JSON.parse(f.database.core.metaGet(f.retiredKey)!);
					f.database.core.metaSet(f.retiredKey, JSON.stringify({ ...record, retiredAt: record.retiredAt + 1 }));
				}
				if (change === "saved_pointer")
					f.database.core.metaSet(
						"monitor_authoring_turn:late",
						JSON.stringify({
							sessionId: "event-session-e0",
							opRef: "late-operation",
							eventIds: ["late"],
						}),
					);
				if (change === "live_binding")
					spyOn(f.database.bindings, "getSessionRecord").mockReturnValue({ sessionId: "event-session-e0", epoch: 0 });
			};
			await f.dispatch();
			expect(f.terminate).not.toHaveBeenCalled();
			if (change === "remove") {
				expect(f.close).toHaveBeenCalledTimes(1);
				expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
			} else {
				expect(f.close).not.toHaveBeenCalled();
				expect(f.sessionPort.closes).toEqual([]);
				expect(JSON.parse(f.database.core.metaGet(f.retiredKey)!)).not.toHaveProperty("phase");
			}
		} finally {
			f.pipeline.dispose();
			f.database.close();
			await rm(directory, { recursive: true, force: true });
		}
	});
}

for (const probe of ["turnRunning", "queueEmpty", "runningJobs"] as const) {
	for (const evidence of ["undefined", "throws", "missing"] as const) {
		if (probe === "queueEmpty" && evidence === "missing") continue;
		test(`a previous close attempt holds when ${probe} ${evidence}`, async () => {
			const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-prior-probe-"));
			const f = await retirementHarness(directory);
			try {
				await f.dispatch();
				await f.dispatch();
				f.sessionPort.closeOutcomes.set("event-session-e0", [
					new GjcCliError("close failed", 0, "", { code: "operation_failed" }),
				]);
				await f.dispatch();
				expect(JSON.parse(f.database.core.metaGet(f.retiredKey)!)).toHaveProperty("phase", "closing");
				Object.defineProperty(f.sessionPort, probe, {
					value:
						evidence === "missing"
							? undefined
							: async () => {
									if (evidence === "throws") throw new Error("probe unavailable");
									return undefined;
								},
				});
				const event = await f.dispatch();
				expect(f.database.monitors.authoredOutput(event)).toBe(`note for ${event}`);
				expect(f.close).toHaveBeenCalledTimes(1);
				expect(f.database.core.metaGet(f.retiredKey)).toBeDefined();
			} finally {
				f.pipeline.dispose();
				f.database.close();
				await rm(directory, { recursive: true, force: true });
			}
		});
	}
}

test("a successful close does not delete a retirement record changed while close was awaited", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-close-changed-"));
	const f = await retirementHarness(directory);
	try {
		await f.dispatch();
		await f.dispatch();
		let changed: string | undefined;
		f.close.mockImplementation(async () => {
			await Promise.resolve();
			const record = JSON.parse(f.database.core.metaGet(f.retiredKey)!);
			changed = JSON.stringify({ ...record, retiredAt: record.retiredAt + 1 });
			f.database.core.metaSet(f.retiredKey, changed);
		});
		await f.dispatch();
		expect(f.close).toHaveBeenCalledTimes(1);
		expect(changed).toBeDefined();
		expect(f.database.core.metaGet(f.retiredKey)).toBe(changed);
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

for (const busy of ["turn", "queue", "jobs", "pointer", "binding"] as const) {
	test(`a previous close attempt still holds on positive ${busy} evidence`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-closing-"));
		const f = await retirementHarness(directory);
		try {
			await f.dispatch();
			await f.dispatch();
			f.sessionPort.closeOutcomes.set("event-session-e0", [
				new GjcCliError("close refused", 0, "", { code: "endpoint_stale" }),
			]);
			await f.dispatch();
			if (busy === "turn") {
				f.sessionPort.hostTurnRunning.set("event-session-e0", true);
				f.sessionPort.hostJobs.set("event-session-e0", new Error("endpoint unavailable"));
			}
			if (busy === "queue") f.sessionPort.hostQueueEmpty.set("event-session-e0", false);
			if (busy === "jobs") {
				f.sessionPort.hostTurnRunning.set("event-session-e0", new Error("context unavailable"));
				f.sessionPort.hostJobs.set("event-session-e0", [{ id: "new-job", type: "bash", label: "new" }]);
			}
			if (busy === "pointer")
				f.database.core.metaSet(
					"monitor_authoring_turn:late",
					JSON.stringify({
						sessionId: "event-session-e0",
						opRef: "late-op",
						eventIds: ["late"],
					}),
				);
			if (busy === "binding")
				spyOn(f.database.bindings, "getSessionRecord").mockReturnValue({ sessionId: "event-session-e0", epoch: 0 });
			await f.dispatch();
			expect(f.close).toHaveBeenCalledTimes(1);
			expect(JSON.parse(f.database.core.metaGet(f.retiredKey)!)).toHaveProperty("phase", "closing");
		} finally {
			f.pipeline.dispose();
			f.database.close();
			await rm(directory, { recursive: true, force: true });
		}
	});
}

test("reopened retirement requires fresh idle evidence before invoking close (propagator gate; final close success is scripted)", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-reopen-"));
	const f = await retirementHarness(directory);
	let active = f.pipeline;
	let database = f.database;
	try {
		await f.dispatch();
		await f.dispatch();
		f.sessionPort.closeOutcomes.set("event-session-e0", [
			new GjcCliError("close uncertain", 0, "", { code: "terminal_uncertain" }),
		]);
		await f.dispatch();
		expect(JSON.parse(f.database.core.metaGet(f.retiredKey)!)).toHaveProperty("phase", "closing");
		active.dispose();
		database.close();
		database = await Store.open(join(directory, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const port = sessionPortFromScript({
			bind: async (_key, epoch = 0) => ({ sessionId: `event-session-e${epoch}` }),
			respond: async (_sessionId, text) => echoNotes(text),
		});
		attachTestBrokerOwnership(database, port, join(directory, "agent"));
		port.setSessionState("event-session-e0", { live: true, repo: "" });
		port.setSessionState("event-session-e1", { live: true, repo: "" });
		port.hostJobs.set("event-session-e0", new Error("restarted endpoint unavailable"));
		port.hostTurnRunning.set("event-session-e0", new Error("restarted context unavailable"));
		port.hostQueueEmpty.set("event-session-e0", new Error("restarted queue unavailable"));
		const jobs = spyOn(port, "runningJobs");
		const turn = spyOn(port, "turnRunning");
		const queue = spyOn(port, "queueEmpty");
		const close = spyOn(port, "close");
		active = new MonitorPropagator({
			database,
			registry,
			sessionPort: port,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
		});
		const event = await active.submitAwaitable(f.monitor.monitorId, "budget.tick", {});
		expect(database.monitors.authoredOutput(event)).toBe(`note for ${event}`);
		expect(jobs).toHaveBeenCalledTimes(1);
		expect(turn).toHaveBeenCalledTimes(1);
		expect(queue).toHaveBeenCalledTimes(1);
		expect(close).not.toHaveBeenCalled();
		expect(database.core.metaGet(f.retiredKey)).toBeDefined();
		port.hostTurnRunning.set("event-session-e0", false);
		port.hostQueueEmpty.set("event-session-e0", true);
		port.hostJobs.set("event-session-e0", []);
		const idleEvent = await active.submitAwaitable(f.monitor.monitorId, "budget.tick", {});
		expect(database.monitors.authoredOutput(idleEvent)).toBe(`note for ${idleEvent}`);
		expect(jobs).toHaveBeenCalledTimes(2);
		expect(turn).toHaveBeenCalledTimes(2);
		expect(queue).toHaveBeenCalledTimes(2);
		expect(close).toHaveBeenCalledTimes(1);
		expect(port.closes.map(({ sessionId }) => sessionId)).toEqual(["event-session-e0"]);
		expect(database.core.metaGet(f.retiredKey)).toBeUndefined();
	} finally {
		active.dispose();
		database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("an independently dead retired host holds without non-signaling process-death evidence", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-dead-"));
	const f = await retirementHarness(directory);
	try {
		await f.dispatch();
		await f.dispatch();
		f.sessionPort.setSessionState("event-session-e0", { live: false });
		// No job fixture: the fake must fail for a dead host instead of inventing an empty list.
		expect(f.sessionPort.hostJobs.has("event-session-e0")).toBe(false);
		await expect(f.sessionPort.runningJobs({ sessionId: "event-session-e0", repo: "" })).rejects.toThrow(
			"host unavailable",
		);
		await f.dispatch();
		expect(f.terminate).not.toHaveBeenCalled();
		expect(f.sessionPort.closes).toEqual([]);
		expect(JSON.parse(f.database.core.metaGet(f.retiredKey)!)).not.toHaveProperty("phase");
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("retirement sweeps at most two records fairly and still authors every dispatch", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-bound-"));
	const f = await retirementHarness(directory);
	try {
		await f.dispatch();
		await f.dispatch();
		f.database.core.metaDelete(f.retiredKey);
		const sessions = ["old-d", "old-b", "old-e", "old-c", "old-a"];
		const prefix = `monitor_retired_session:${f.sessionKey}:`;
		for (const [index, sessionId] of sessions.entries()) {
			f.sessionPort.setSessionState(sessionId, { live: true });
			f.sessionPort.hostJobs.set(sessionId, new Error("temporarily unavailable"));
			f.database.core.metaSet(
				`${prefix}${sessionId}`,
				JSON.stringify({
					sessionId,
					epoch: 0,
					retiredAt: index + 1,
				}),
			);
		}
		// A frozen clock must not let the first two unavailable records starve the rest.
		f.pipeline.dispose();
		const now = Date.now();
		const active = new MonitorPropagator({
			database: f.database,
			registry: f.registry,
			sessionPort: f.sessionPort,
			now: () => now,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(f.database),
			emit: () => {},
		});
		try {
			for (let sweep = 0; sweep < 3; sweep++) {
				f.jobs.mockClear();
				const event = await active.submitAwaitable(f.monitor.monitorId, "budget.tick", {});
				expect(f.database.monitors.authoredOutput(event)).toBe(`note for ${event}`);
				expect(f.jobs).toHaveBeenCalledTimes(2);
				expect(f.jobs.mock.calls.map(([input]) => input.sessionId)).toEqual(
					sweep === 0 ? ["old-d", "old-b"] : sweep === 1 ? ["old-e", "old-c"] : ["old-a", "old-d"],
				);
			}
			expect(f.terminate).not.toHaveBeenCalled();
			for (const sessionId of sessions) {
				expect(JSON.parse(f.database.core.metaGet(`${prefix}${sessionId}`)!)).toHaveProperty("lastAttemptAt");
				f.sessionPort.hostJobs.set(sessionId, []);
			}
			for (const count of [2, 2, 1]) {
				f.close.mockClear();
				const event = await active.submitAwaitable(f.monitor.monitorId, "budget.tick", {});
				expect(f.database.monitors.authoredOutput(event)).toBe(`note for ${event}`);
				expect(f.close).toHaveBeenCalledTimes(count);
			}
			expect(f.database.core.metaKeysInRange(prefix, `${prefix}\uffff`)).toEqual([]);
		} finally {
			active.dispose();
		}
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("retirement probes turn then queue then jobs, catching a job launched as the turn ends", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-probe-order-"));
	const f = await retirementHarness(directory);
	try {
		await f.dispatch();
		await f.dispatch();
		const order: string[] = [];
		const turn = f.sessionPort.turnRunning.bind(f.sessionPort);
		const queue = f.sessionPort.queueEmpty.bind(f.sessionPort);
		f.sessionPort.turnRunning = async (input) => {
			order.push("turn");
			const running = await turn(input);
			// The turn has just ended, but its background work is still running.
			f.sessionPort.hostJobs.set(input.sessionId, [{ id: "launched-job", type: "bash", label: "background" }]);
			return running;
		};
		f.sessionPort.queueEmpty = async (input) => {
			order.push("queue");
			return await queue(input);
		};
		f.sessionPort.beforeRunningJobs = () => {
			order.push("jobs");
		};
		await f.dispatch();
		expect(order).toEqual(["turn", "queue", "jobs"]);
		expect(f.close).not.toHaveBeenCalled();
		expect(f.database.core.metaGet(f.retiredKey)).toBeDefined();
		f.sessionPort.turnRunning = turn;
		f.sessionPort.hostJobs.set("event-session-e0", []);
		await f.dispatch();
		expect(f.close).toHaveBeenCalledTimes(1);
		expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("clearing contextRollTokens after a size roll still sweeps its retirement record", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-cleared-"));
	const f = await retirementHarness(directory);
	try {
		await f.dispatch();
		await f.dispatch();
		expect(f.database.core.metaGet(f.retiredKey)).toBeDefined();
		f.registry.update({ monitorId: f.monitor.monitorId, contextRollTokens: null });
		const event = await f.dispatch();
		expect(f.database.monitors.authoredOutput(event)).toBe(`note for ${event}`);
		expect(f.close).toHaveBeenCalledTimes(1);
		expect(f.database.core.metaGet(f.retiredKey)).toBeUndefined();
	} finally {
		f.pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

for (const lastAttemptAt of [Number.MAX_SAFE_INTEGER + 1, Number.MAX_SAFE_INTEGER]) {
	test(`retirement repairs logical lastAttemptAt=${lastAttemptAt} without starving records`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retire-overflow-"));
		const f = await retirementHarness(directory);
		try {
			await f.dispatch();
			await f.dispatch();
			f.database.core.metaDelete(f.retiredKey);
			const sessions = ["overflow-a", "overflow-b", "overflow-c"];
			const prefix = `monitor_retired_session:${f.sessionKey}:`;
			for (const [index, sessionId] of sessions.entries()) {
				f.sessionPort.setSessionState(sessionId, { live: true });
				f.sessionPort.hostJobs.set(sessionId, new Error("unavailable"));
				f.database.core.metaSet(
					`${prefix}${sessionId}`,
					JSON.stringify({
						sessionId,
						epoch: 0,
						retiredAt: index + 1,
						lastAttemptAt,
					}),
				);
			}
			for (let attempt = 0; attempt < 2; attempt++) await f.dispatch();
			expect(new Set(f.jobs.mock.calls.map(([input]) => input.sessionId))).toEqual(new Set(sessions));
			for (const sessionId of sessions) {
				const repaired = JSON.parse(f.database.core.metaGet(`${prefix}${sessionId}`)!).lastAttemptAt;
				expect(Number.isSafeInteger(repaired)).toBe(true);
				expect(repaired).toBeLessThanOrEqual(Number.MAX_SAFE_INTEGER);
			}
			expect(f.close).not.toHaveBeenCalled();
		} finally {
			f.pipeline.dispose();
			f.database.close();
			await rm(directory, { recursive: true, force: true });
		}
	});
}

test("buildMonitorCompactionDigest carries the contract plus bounded recent notes", () => {
	const digest = buildMonitorCompactionDigest({
		monitorName: "deploy-watch",
		instruction: "Post the oldest blocked release with its owner.",
		notes: Array.from({ length: MONITOR_DIGEST_MAX_NOTES + 4 }, (_, index) => ({
			eventType: "deploy.blocked",
			firedAt: `2026-08-30T0${index}:00:00.000Z`,
			note: `note ${index}`,
		})),
	});
	expect(digest).toContain("Context compaction");
	expect(digest).toContain("Monitor: deploy-watch");
	expect(digest).toContain("Standing instruction: Post the oldest blocked release with its owner.");
	// Newest notes only, oldest dropped.
	expect(digest).toContain("note 0");
	expect(digest).toContain(`note ${MONITOR_DIGEST_MAX_NOTES - 1}`);
	expect(digest).not.toContain(`note ${MONITOR_DIGEST_MAX_NOTES}`);
	expect(digest.split("\n").filter((line) => line.startsWith("- 2026-"))).toHaveLength(MONITOR_DIGEST_MAX_NOTES);
	// A digest line must never look like the JSON array the turn has to answer with.
	expect(digest).not.toContain("[");
});

test("the digest stays under its ceiling even with maximal instruction and notes", () => {
	const digest = buildMonitorCompactionDigest({
		monitorName: "x".repeat(500),
		instruction: "i".repeat(4000),
		notes: Array.from({ length: MONITOR_DIGEST_MAX_NOTES }, (_, index) => ({
			eventType: "e".repeat(200),
			firedAt: "2026-08-30T00:00:00.000Z",
			note: "n".repeat(5000) + String(index),
		})),
	});
	expect(digest.length).toBeLessThanOrEqual(MONITOR_DIGEST_MAX_LENGTH);
	// The contract survives truncation; notes are what gets dropped.
	expect(digest).toContain("Context compaction");
	expect(digest).toContain("Standing instruction:");
});

test("a monitor without an instruction produces a digest with no instruction line", () => {
	const digest = buildMonitorCompactionDigest({ monitorName: "plain", instruction: undefined, notes: [] });
	expect(digest).not.toContain("Standing instruction");
	expect(digest).toContain("Recent authored notes: none yet.");
});

test("the default compaction port is honest about doing nothing", async () => {
	expect((await unavailableCompactionPort.run("event-session-e0")).status).toBe("unavailable");
});

test("authoring failures split across exactly three axes, and only context evidence is context", () => {
	for (const message of [
		"authoring response is empty",
		"session send failed: context_too_large",
		"maximum context length exceeded",
		"prompt is too long for this model",
		"provider returned a 0-token completion",
	])
		expect(classifyAuthoringFailure(new Error(message))).toBe("context");
	for (const message of [
		"authoring response is not an array",
		"Unexpected token < in JSON at position 0",
		"JSON Parse error: Unexpected identifier",
		"authoring response omits event abc",
		"authoring response duplicates event abc",
	])
		expect(classifyAuthoringFailure(new Error(message))).toBe("protocol");
	for (const message of [
		"aside collection worker timed out after 300s",
		"aside_timeout",
		"external tool failed: gh exited 1",
		"memory lock held, run skipped",
		"session bind failed: socket closed",
		"something nobody has seen before",
		"aside exec collection exceeded 360s; child killed but Aside daemon task still running",
	])
		expect(classifyAuthoringFailure(new Error(message))).toBe("executor");
	// The aside worker's cap is recognised as its own executor failure, and is
	// never allowed to look like context exhaustion even when the message also
	// mentions a long prompt.
	expect(isAsideTimeoutFailure(new Error("aside collection timed out after 300000ms"))).toBe(true);
	expect(isAsideTimeoutFailure(new Error("context_too_large"))).toBe(false);
	expect(classifyAuthoringFailure(new Error("aside collection worker timed out after 300s; prompt is too long"))).toBe(
		"executor",
	);
});

test("the executor class names its sub-kinds, with an orphaned external run outranking the timeout", () => {
	// The drill shape: the child died on the wrapper's cap, the daemon job did
	// not, and the next tick would stack another one.
	for (const message of [
		"aside exec collection exceeded 360s; child killed but Aside daemon task still running",
		"child process terminated at 300s, external work still running",
		"orphaned_executor: aside session left open",
		"wrapper killed the child; daemon job is still active",
	]) {
		expect(isOrphanedExecutorFailure(new Error(message))).toBe(true);
		expect(classifyExecutorFailure(new Error(message))).toBe("executor_orphaned_external_work");
		// Still executor class: the class axis stays at three.
		expect(classifyAuthoringFailure(new Error(message))).toBe("executor");
	}
	// A plain timeout with nothing left behind is a timeout, not an orphan.
	expect(isOrphanedExecutorFailure(new Error("aside collection worker timed out after 300000ms"))).toBe(false);
	expect(classifyExecutorFailure(new Error("aside collection worker timed out after 300000ms"))).toBe(
		"executor_timeout",
	);
	expect(classifyExecutorFailure(new Error("external tool failed: gh exited 1"))).toBe("executor_failed");
	// And an orphan report that also mentions context stays executor: a stacked
	// external job is not evidence about this session's context.
	expect(classifyAuthoringFailure(new Error("child killed but daemon task still running; prompt is too long"))).toBe(
		"executor",
	);
});

test("the roll decision needs both the failure streak and a non-successful native compaction", () => {
	const threshold = MONITOR_CONTEXT_FAILURE_ROLL_THRESHOLD;
	// Below the streak, nothing rolls — whatever native compaction reported.
	for (const nativeCompaction of ["unavailable", "failed", "skipped", "succeeded"] as const)
		expect(
			decideSessionRoll({ consecutiveContextFailures: threshold - 1, threshold, nativeCompaction }),
		).toBeUndefined();
	// At the streak, a native compaction that did not help names the reason.
	expect(decideSessionRoll({ consecutiveContextFailures: threshold, threshold, nativeCompaction: "unavailable" })).toBe(
		"context_failures_native_compaction_unavailable",
	);
	expect(decideSessionRoll({ consecutiveContextFailures: threshold, threshold, nativeCompaction: "failed" })).toBe(
		"context_failures_native_compaction_failed",
	);
	expect(decideSessionRoll({ consecutiveContextFailures: threshold, threshold, nativeCompaction: "skipped" })).toBe(
		"context_failures_native_compaction_skipped",
	);
	// Native compaction succeeded: the session was repaired, so it is kept.
	expect(
		decideSessionRoll({ consecutiveContextFailures: threshold * 10, threshold, nativeCompaction: "succeeded" }),
	).toBeUndefined();
});

test("a healthy 10-minute monitor is never rolled, however far past any turn threshold it runs", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-healthy-"));
	try {
		const compaction = stubPort("unavailable");
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			compaction,
		});
		const monitor = registry.add({
			name: "heartbeat",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["heartbeat.tick"],
			burstPolicy: "serialize",
			instruction: "Report the oldest unacknowledged alert.",
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "heartbeat.tick"));
		// 25 hours at one tick per 10 minutes — far past the 24 the old
		// implementation rolled on, and past the chat path's 50.
		const ticks = 150;
		const eventIds: string[] = [];
		for (let tick = 0; tick < ticks; tick += 1)
			eventIds.push(await pipeline.submitAwaitable(monitor.monitorId, "heartbeat.tick", { tick }));
		expect(turns).toHaveLength(ticks);
		// One session, one epoch: not a single roll while the monitor was answering.
		expect(new Set(turns.map((turn) => turn.sessionId))).toEqual(new Set(["event-session-e0"]));
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(0);
		expect(turns.some((turn) => turn.prompt.includes("Context compaction"))).toBe(false);
		// Native compaction was never even asked: nothing failed.
		expect(compaction.calls).toHaveLength(0);
		// Turn count is still recorded — it is the observational signal.
		expect(database.bindings.sessionTurnCount(sessionKey)).toBe(ticks);
		const state = pipeline.sessionSafetyState(sessionKey);
		expect(state.turns).toBe(ticks);
		expect(state.contextFailures).toBe(0);
		expect(state.lastRoll).toBeUndefined();
		// And every turn authored its event.
		for (const eventId of eventIds) expect(database.monitors.authoredOutput(eventId)).toBe(`note for ${eventId}`);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

for (const tokens of [10_000, 12_000]) {
	test(`context budget ${tokens} arms after an answer and rolls with a digest at the next fresh dispatch`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-context-budget-"));
		const log = spyOn(console, "error");
		try {
			const { database, registry, pipeline, turns } = await harness(directory, {
				contextUsage: (sessionId) => (sessionId === "event-session-e0" ? { percent: 50, tokens } : undefined),
			});
			const instruction = "Check the deploy queue and report the oldest item.";
			const monitor = registry.add({
				name: "context-budget",
				trigger: { kind: "cron", schedule: "0 */6 * * *" },
				eventTypes: ["budget.tick"],
				burstPolicy: "serialize",
				instruction,
				contextRollTokens: 10_000,
			});
			const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
			const first = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			expect(database.monitors.authoredOutput(first)).toBe(`note for ${first}`);
			expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(0);
			expect(pipeline.sessionSafetyState(sessionKey)).toMatchObject({
				pendingRoll: "context_budget",
				lastContextTokens: tokens,
				turns: 1,
			});
			expect(JSON.parse(database.core.metaGet(`monitor_context_budget_arm:${sessionKey}`)!)).toEqual({
				epoch: 0,
				tokens,
			});
			expect(log.mock.calls.filter(([line]) => String(line).includes("reason=context_budget"))).toEqual([
				[
					`monitor session safety net armed for ${sessionKey} (monitor ${monitor.monitorId}): reason=context_budget context_tokens=${tokens}/10000 turns=1`,
				],
			]);
			await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			expect(turns[1]?.sessionId).toBe("event-session-e1");
			expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
			expect(turns[1]?.prompt).toContain("Context compaction");
			expect(turns[1]?.prompt).toContain(`Standing instruction: ${instruction}`);
			expect(turns[1]?.prompt).toContain(`note for ${first}`);
			expect(pipeline.sessionSafetyState(sessionKey)).toMatchObject({
				pendingRoll: undefined,
				lastRoll: "context_budget",
				lastContextTokens: undefined,
				turns: 1,
			});
			expect(database.core.metaGet(`monitor_context_budget_arm:${sessionKey}`)).toBeUndefined();
			await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			expect(turns[2]?.sessionId).toBe("event-session-e1");
			database.close();
		} finally {
			log.mockRestore();
			await rm(directory, { recursive: true, force: true });
		}
	});
}

for (const restart of [false, true]) {
	for (const lateFailure of [false, true]) {
		test(`a size-tagged op ${lateFailure ? "reporting failed after #702 give-up" : "failed after a side effect through retry exhaustion"} keeps its pointer${restart ? " across restart" : " in memory"}`, async () => {
			const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-tagged-failed-evidence-"));
			try {
				let clock = Date.now();
				let tokens: number | undefined;
				let originalEventId = "";
				let sideEffects = 0;
				const { database, registry, pipeline, sessionPort } = await harness(directory, {
					now: () => clock,
					contextUsage: () => (tokens === undefined ? undefined : { percent: 60, tokens }),
					respond: (prompt, index) => {
						const response = echoNotes(prompt);
						const entries = JSON.parse(response) as Array<{ eventId: string }>;
						if (index === 0) originalEventId = entries[0]!.eventId;
						if (entries.some((entry) => entry.eventId === originalEventId)) sideEffects++;
						if (index === 0 && !lateFailure) throw new Error("counted side effect committed before operation failed");
						return response;
					},
				});
				attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
				const monitor = registry.add({
					name: "tagged-failed-evidence",
					trigger: { kind: "cron", schedule: "0 * * * *" },
					eventTypes: ["budget.tick"],
					burstPolicy: "serialize",
					contextRollTokens: 10_000,
				});
				const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
				const fetchLastAssistant = sessionPort.fetchLastAssistant.bind(sessionPort);
				if (lateFailure)
					sessionPort.fetchLastAssistant = async () => {
						throw new LastAssistantUnreadableError("last_assistant_no_page");
					};
				const eventA = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
				sessionPort.fetchLastAssistant = fetchLastAssistant;
				expect(sideEffects).toBe(1);
				const pointerKey = `monitor_authoring_turn:${eventA}`;
				const saved = JSON.parse(database.core.metaGet(pointerKey)!) as { sessionId: string; opRef: string };
				expect((await sessionPort.status({ ...saved, repo: directory })).status.status).toBe(
					lateFailure ? "terminal_ok" : "failed",
				);
				expect(pipeline.settle(eventA).done).toBe(true);
				tokens = 12_000;
				await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
				tokens = undefined;
				await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
				expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
				const tagged = database.core.metaGet(pointerKey);
				expect(JSON.parse(tagged!)).toEqual({ ...saved, sizeRolled: true });
				const status = sessionPort.status.bind(sessionPort);
				if (lateFailure)
					sessionPort.status = async (input) =>
						input.opRef === saved.opRef
							? { operationRef: saved.opRef, status: { status: "unknown" }, summaryCompleted: false }
							: status(input);
				const makePipeline = () =>
					new MonitorPropagator({
						database,
						registry,
						sessionPort,
						memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
						delivery: new DeliveryService(database),
						emit: () => {},
						now: () => clock,
					});
				let active = pipeline;
				if (restart) {
					active.dispose();
					active = makePipeline();
				}
				expect(active.retry(eventA).done).toBe(true);
				const requests = spyOn(sessionPort, "request");
				const sends = sessionPort.sends.length;
				for (let attempt = 0; attempt < MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS; attempt++) {
					clock += 13 * 60 * 60_000;
					await active.reconcile();
					expect(database.core.metaGet(pointerKey)).toBe(tagged);
					expect(requests).not.toHaveBeenCalled();
				}
				expect(database.monitors.eventGet(eventA)?.dispatch_attempts).toBe(MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS);
				clock += MONITOR_EVENT_MAX_OPEN_AGE_MS + 1;
				await active.reconcile();
				expect(database.monitors.eventGet(eventA)?.stage).toBe("failed_no_retry");
				expect(database.monitors.failure(eventA)?.code).toBe("authoring_turn_unresolved");
				expect(database.core.metaGet(pointerKey)).toBe(tagged);
				if (lateFailure) {
					sessionPort.fail(saved.opRef, "original operation later failed");
					sessionPort.status = status;
					expect((await sessionPort.status({ ...saved, repo: directory })).status.status).toBe("failed");
				}
				if (restart) {
					active.dispose();
					active = makePipeline();
				}
				for (let sweep = 0; sweep < 2; sweep++) {
					await active.reconcile();
					expect(database.core.metaGet(pointerKey)).toBe(tagged);
					expect(database.monitors.eventGet(eventA)?.stage).toBe("failed_no_retry");
				}
				expect(active.retry(eventA).done).toBe(true);
				await active.reconcile();
				expect(database.monitors.eventGet(eventA)?.stage).toBe("failed");
				expect(database.core.metaGet(pointerKey)).toBe(tagged);
				expect(database.monitors.authoredOutput(eventA)).toBeUndefined();
				expect(requests).not.toHaveBeenCalled();
				requests.mockRestore();
				expect(sessionPort.sends).toHaveLength(sends);
				expect(sessionPort.sends.filter((input) => input.opRef === saved.opRef)).toHaveLength(1);
				expect(sideEffects).toBe(1);
				expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
				active.dispose();
				database.close();
			} finally {
				await rm(directory, { recursive: true, force: true });
			}
		});
	}
}

for (const evidence of [
	"foreign clientRef",
	"foreign commandId",
	"foreign turnId",
	"missing clientRef",
	"missing commandId",
	"missing turnId",
	"malformed evidence",
	"incomplete body",
	"unsupported query",
	"foreign session",
] as const) {
	for (const restart of [false, true]) {
		test(`size-tagged ${evidence} holds through ordinary retry, exhaustion and #702 recovery${restart ? " across restart" : " in memory"}`, async () => {
			const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-tagged-output-evidence-"));
			let active: MonitorPropagator | undefined;
			let database: Store | undefined;
			try {
				let clock = Date.now();
				let tokens: number | undefined;
				const f = await harness(directory, {
					now: () => clock,
					contextUsage: () => (tokens === undefined ? undefined : { percent: 60, tokens }),
				});
				database = f.database;
				const { registry, pipeline, sessionPort } = f;
				active = pipeline;
				attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
				const monitor = registry.add({
					name: "tagged-output-evidence",
					trigger: { kind: "cron", schedule: "0 * * * *" },
					eventTypes: ["budget.tick"],
					burstPolicy: "serialize",
					contextRollTokens: 10_000,
				});
				const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
				const fetchLastAssistant = sessionPort.fetchLastAssistant.bind(sessionPort);
				sessionPort.fetchLastAssistant = async () => {
					throw new LastAssistantUnreadableError("last_assistant_no_page");
				};
				const event = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
				sessionPort.fetchLastAssistant = fetchLastAssistant;
				const key = `monitor_authoring_turn:${event}`;
				const saved = JSON.parse(database.core.metaGet(key)!) as { sessionId: string; opRef: string };
				expect(pipeline.settle(event).done).toBe(true);
				tokens = 12_000;
				await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
				tokens = undefined;
				await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
				const tagged = database.core.metaGet(key);
				expect(JSON.parse(tagged!)).toMatchObject({ sizeRolled: true });
				const result: Record<string, unknown> = {
					kind: "prompt",
					clientRef: saved.opRef,
					commandId: `command-${saved.opRef}`,
					turnId: `turn-${saved.opRef}`,
					status: "terminal_ok",
					receiptState: "absent",
					terminalAt: Date.now(),
				};
				for (const field of ["clientRef", "commandId", "turnId"] as const) {
					if (evidence === `foreign ${field}`) result[field] = `foreign-${field}`;
					if (evidence === `missing ${field}`) delete result[field];
				}
				if (evidence === "malformed evidence") result.terminalAt = "not-a-timestamp";
				if (evidence === "incomplete body") result.content = { truncated: true };
				sessionPort.setWorkerOutputFixture(saved.opRef, {
					exitCode: 0,
					stdout: JSON.stringify(
						evidence === "unsupported query" || evidence === "foreign session"
							? {
									ok: false,
									error: {
										code: evidence === "unsupported query" ? "unsupported_query" : "operation_not_session_owned",
									},
								}
							: { ok: true, result },
					),
					stderr: "",
				});
				const expectedCode =
					(evidence.startsWith("foreign ") && evidence !== "foreign session") || evidence.startsWith("missing ")
						? "identity_mismatch"
						: evidence === "malformed evidence"
							? "invalid_evidence"
							: evidence === "incomplete body"
								? "incomplete_body"
								: "output_unavailable";
				expect(
					await sessionPort.fetchWorkerOutput({
						...saved,
						repo: directory,
						notBeforeMs: Date.parse(database.monitors.eventGet(event)!.fired_at),
						terminalIdentity: (await sessionPort.status({ ...saved, repo: directory })).status,
					}),
				).toEqual({ status: "unavailable", code: expectedCode });
				const makePipeline = () =>
					new MonitorPropagator({
						database: f.database,
						registry,
						sessionPort,
						memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
						delivery: new DeliveryService(f.database),
						emit: () => {},
						now: () => clock,
					});
				if (restart) {
					active.dispose();
					active = makePipeline();
				}
				const sends = sessionPort.sendAttempts.length;
				const requests = spyOn(sessionPort, "request");
				try {
					expect(active.retry(event).done).toBe(true);
					for (let attempt = 0; attempt < MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS; attempt++) {
						clock += 13 * 60 * 60_000;
						await active.reconcile();
						expect(database.core.metaGet(key)).toBe(tagged);
						expect(database.monitors.eventGet(event)?.stage).toBe("failed");
						expect(requests).not.toHaveBeenCalled();
					}
					expect(database.monitors.eventGet(event)?.dispatch_attempts).toBe(MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS);
					clock += MONITOR_EVENT_MAX_OPEN_AGE_MS + 1;
					await active.reconcile();
					expect(database.monitors.failure(event)?.code).toBe("authoring_turn_unresolved");
					if (restart) {
						active.dispose();
						active = makePipeline();
					}
					for (let sweep = 0; sweep < 2; sweep++) {
						await active.reconcile();
						expect(database.monitors.eventGet(event)?.stage).toBe("failed_no_retry");
						expect(database.core.metaGet(key)).toBe(tagged);
					}
					expect(database.monitors.authoredOutput(event)).toBeUndefined();
					expect(requests).not.toHaveBeenCalled();
					expect(sessionPort.sendAttempts).toHaveLength(sends);
					expect(sessionPort.sendAttempts.filter((input) => input.opRef === saved.opRef)).toHaveLength(1);
					expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
				} finally {
					requests.mockRestore();
				}
			} finally {
				active?.dispose();
				database?.close();
				await rm(directory, { recursive: true, force: true });
			}
		});
	}
}

for (const restart of [false, true]) {
	test(`an untagged failed op from a failure roll retains origin/dev retry behavior${restart ? " across restart" : " in memory"}`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-untagged-failed-evidence-"));
		try {
			let originalEventId = "";
			let sideEffects = 0;
			const { database, registry, pipeline, sessionPort } = await harness(directory, {
				respond: (prompt, index) => {
					const response = echoNotes(prompt);
					const entries = JSON.parse(response) as Array<{ eventId: string }>;
					if (index === 0) originalEventId = entries[0]!.eventId;
					if (entries.some((entry) => entry.eventId === originalEventId)) sideEffects++;
					if (index === 0) throw new Error("counted side effect committed before operation failed");
					return response;
				},
			});
			attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
			const monitor = registry.add({
				name: "untagged-failed-evidence",
				trigger: { kind: "cron", schedule: "0 * * * *" },
				eventTypes: ["budget.tick"],
				burstPolicy: "serialize",
				contextRollTokens: 10_000,
			});
			const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
			const eventA = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			const key = `monitor_authoring_turn:${eventA}`;
			const pointer = database.core.metaGet(key);
			expect(pipeline.settle(eventA).done).toBe(true);
			const request = sessionPort.request.bind(sessionPort);
			sessionPort.request = async () => {
				throw new RelayHelloError(1, "stream_ended");
			};
			const failed = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			expect(pipeline.settle(failed).done).toBe(true);
			sessionPort.request = request;
			await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
			expect(database.core.metaGet(key)).toBe(pointer);
			expect(JSON.parse(pointer!)).not.toHaveProperty("sizeRolled");
			let active = pipeline;
			if (restart) {
				pipeline.dispose();
				active = new MonitorPropagator({
					database,
					registry,
					sessionPort,
					memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
					delivery: new DeliveryService(database),
					emit: () => {},
				});
			}
			const sends = sessionPort.sends.length;
			expect(active.retry(eventA).done).toBe(true);
			await active.reconcile();
			for (let attempt = 0; attempt < 100 && database.monitors.authoredOutput(eventA) === undefined; attempt++)
				await Bun.sleep(1);
			expect(database.monitors.authoredOutput(eventA)).toBe(`note for ${eventA}`);
			expect(sessionPort.sends).toHaveLength(sends + 1);
			expect(sessionPort.sends.at(-1)?.sessionId).toBe("event-session-e1");
			expect(sideEffects).toBe(2);
			expect(database.core.metaGet(key)).toBeUndefined();
			active.dispose();
			database.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});
}

for (const accepted of [false, true]) {
	test(`a ${accepted ? "previously accepted" : "fresh"} authoring op keeps its pointer on host-unavailable${accepted ? " during reattach" : " and replays once after the failure roll"}`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-pre-send-"));
		try {
			const { database, registry, pipeline, sessionPort } = await harness(directory);
			attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
			const monitor = registry.add({
				name: "pre-send",
				trigger: { kind: "cron", schedule: "0 * * * *" },
				eventTypes: ["budget.tick"],
				burstPolicy: "serialize",
			});
			const request = sessionPort.request.bind(sessionPort);
			if (accepted) {
				sessionPort.request = async (input) => {
					sessionPort.seedAcceptedSend(input, "in_flight");
					throw new SessionRequestTimeoutError(
						input.sessionId,
						input.opRef,
						await sessionPort.status({ sessionId: input.sessionId, repo: input.repo, opRef: input.opRef }),
					);
				};
			} else
				sessionPort.request = async () => {
					throw new RelayHelloError(1, "stream_ended");
				};
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			const key = `monitor_authoring_turn:${eventId}`;
			if (accepted) {
				const pointer = database.core.metaGet(key);
				expect(pointer).toBeDefined();
				sessionPort.request = async () => {
					throw new RelayHelloError(1, "stream_ended");
				};
				expect(pipeline.retry(eventId).done).toBe(true);
				await pipeline.reconcile();
				expect(database.core.metaGet(key)).toBe(pointer);
				expect(sessionPort.sends).toHaveLength(1);
				expect(database.monitors.eventGet(eventId)?.stage).toBe("failed");
			} else {
				const pointer = database.core.metaGet(key);
				expect(pointer).toBeDefined();
				expect(JSON.parse(pointer!)).not.toHaveProperty("sizeRolled");
				expect(sessionPort.sends).toHaveLength(0);
				sessionPort.request = request;
				await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
				expect(database.core.metaGet(key)).toBe(pointer);
				expect(pipeline.retry(eventId).done).toBe(true);
				await pipeline.reconcile();
				for (let i = 0; i < 100 && database.monitors.authoredOutput(eventId) === undefined; i++) await Bun.sleep(1);
				expect(database.monitors.authoredOutput(eventId)).toBe(`note for ${eventId}`);
				expect(sessionPort.sends.filter((input) => input.text.includes(eventId))).toHaveLength(1);
				expect(sessionPort.sends.at(-1)?.sessionId).toBe("event-session-e1");
			}
			sessionPort.request = request;
			database.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});
}

for (const reason of [
	"session_busy_stalled",
	"session_prior_run_busy_stalled",
	"session_submission_wedged",
	"session_host_unavailable",
] as const) {
	test(`${reason} supersedes a deferred context budget without tagging failure-roll pointers`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-failure-precedence-"));
		try {
			let tokens: number | undefined;
			const { database, registry, pipeline, sessionPort, turns } = await harness(directory, {
				contextUsage: () => (tokens === undefined ? undefined : { percent: 60, tokens }),
			});
			attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
			const monitor = registry.add({
				name: "precedence",
				trigger: { kind: "cron", schedule: "0 * * * *" },
				eventTypes: ["budget.tick"],
				burstPolicy: "serialize",
				contextRollTokens: 10_000,
			});
			const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
			const fetchLastAssistant = sessionPort.fetchLastAssistant.bind(sessionPort);
			sessionPort.fetchLastAssistant = async () => {
				throw new LastAssistantUnreadableError("last_assistant_no_page");
			};
			const eventA = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			sessionPort.fetchLastAssistant = fetchLastAssistant;
			const pointerKey = `monitor_authoring_turn:${eventA}`;
			const pointer = database.core.metaGet(pointerKey)!;
			const saved = JSON.parse(pointer);
			tokens = 12_000;
			await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			tokens = undefined;
			const request = sessionPort.request.bind(sessionPort);
			sessionPort.request = async () => {
				if (reason === "session_busy_stalled") throw new GjcCliError("busy", 0, "", { code: "busy" });
				if (reason === "session_prior_run_busy_stalled")
					throw new Error("prior run", {
						cause: Object.assign(new Error("sdk_prompt_terminal_failed"), {
							code: "sdk_prompt_terminal_failed",
							reason: "Timed out waiting for prior agent run to finish before prompting.",
						}),
					});
				if (reason === "session_submission_wedged") throw new SessionTerminalError(wedgedSubmissionReport());
				throw new RelayHelloError(1, "stream_ended");
			};
			const attempts = reason === "session_busy_stalled" || reason === "session_prior_run_busy_stalled" ? 2 : 1;
			for (let i = 0; i < attempts; i++) await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBe(reason);
			expect(JSON.parse(database.core.metaGet(pointerKey)!)).not.toHaveProperty("sizeRolled");
			sessionPort.request = request;
			const terminate = spyOn(sessionPort, "terminateHost");
			await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
			expect(pipeline.sessionSafetyState(sessionKey).lastRoll).toBe(reason);
			expect(terminate).toHaveBeenCalledTimes(reason === "session_host_unavailable" ? 0 : 1);
			if (reason !== "session_host_unavailable")
				expect(terminate.mock.calls[0]?.[0]).toMatchObject({ sessionId: "event-session-e0" });
			expect(database.core.metaKeysInRange("monitor_retired_session:", "monitor_retired_session;")).toEqual([]);
			expect(database.core.metaGet(`monitor_context_budget_arm:${sessionKey}`)).toBeUndefined();
			expect(database.core.metaGet(pointerKey)).toBe(pointer);
			// Untagged failure-roll pointers keep origin/dev's fresh-authoring path.
			const sends = sessionPort.sends.length;
			expect(pipeline.retry(eventA).done).toBe(true);
			await pipeline.reconcile();
			for (let i = 0; i < 100 && database.monitors.authoredOutput(eventA) === undefined; i++) await Bun.sleep(1);
			expect(database.monitors.authoredOutput(eventA)).toBe(`note for ${eventA}`);
			expect(sessionPort.sends.slice(sends).filter((input) => input.text.includes(eventA))).toHaveLength(1);
			expect(sessionPort.sends.filter((input) => input.opRef === saved.opRef)).toHaveLength(1);
			expect(turns.at(-1)?.sessionId).toBe("event-session-e1");
			database.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});
}

for (const raw of [undefined, "{invalid", '{"epoch":0,"tokens":"12000"}']) {
	test(`an in-memory size arm is disarmed when its persisted row is ${raw === undefined ? "missing" : raw}`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-memory-arm-validation-"));
		try {
			let tokens: number | undefined = 12_000;
			const { database, registry, pipeline, sessionPort, turns } = await harness(directory, {
				contextUsage: () => (tokens === undefined ? undefined : { percent: 60, tokens }),
			});
			attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
			const monitor = registry.add({
				name: "memory-arm-validation",
				trigger: { kind: "cron", schedule: "0 * * * *" },
				eventTypes: ["budget.tick"],
				burstPolicy: "serialize",
				contextRollTokens: 10_000,
			});
			const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
			const armKey = `monitor_context_budget_arm:${sessionKey}`;
			await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBe("context_budget");
			if (raw === undefined) database.core.metaDelete(armKey);
			else database.core.metaSet(armKey, raw);
			tokens = undefined;
			await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(0);
			expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBeUndefined();
			expect(database.core.metaGet(armKey)).toBeUndefined();
			expect(turns.at(-1)?.prompt).not.toContain("Context compaction");
			database.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});
}

for (const restart of [false, true]) {
	for (const highSample of [false, true]) {
		test(`a bind-owned epoch rotation ${highSample ? "refreshes a high" : "invalidates a stale"} size arm${restart ? " after restart" : " in memory"}`, async () => {
			const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-real-arm-fence-"));
			const f = await realBrokerMonitorHarness(directory);
			let pipeline = f.makePipeline();
			try {
				const monitor = f.registry.add({
					name: "bind-arm-fence",
					trigger: { kind: "cron", schedule: "0 * * * *" },
					eventTypes: ["budget.tick"],
					burstPolicy: "serialize",
					contextRollTokens: 10_000,
				});
				const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
				const armKey = `monitor_context_budget_arm:${sessionKey}`;
				const fetchLastAssistant = f.port.fetchLastAssistant.bind(f.port);
				f.port.fetchLastAssistant = async () => {
					throw new LastAssistantUnreadableError("last_assistant_no_page");
				};
				await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
				f.port.fetchLastAssistant = fetchLastAssistant;
				f.control.tokens = 12_000;
				await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
				expect(JSON.parse(f.database.core.metaGet(armKey)!)).toEqual({ epoch: 0, tokens: 12_000 });
				f.control.goneSession = "real-session-0";
				f.control.tokens = highSample ? 15_000 : undefined;
				await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
				expect(f.database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
				expect(JSON.parse(f.database.core.metaGet(armKey)!)).toEqual({
					epoch: highSample ? 1 : 0,
					tokens: highSample ? 15_000 : 12_000,
				});
				f.control.tokens = undefined;
				if (restart) {
					pipeline.dispose();
					pipeline = f.makePipeline();
				}
				await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
				expect(f.database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(highSample ? 2 : 1);
				expect(f.prompts.at(-1)?.includes("Context compaction")).toBe(highSample);
				expect(f.database.core.metaGet(armKey)).toBeUndefined();
			} finally {
				pipeline.dispose();
				f.database.close();
				await rm(directory, { recursive: true, force: true });
			}
		});
	}
}

for (const mode of ["arm", "handoff"] as const) {
	for (const declared of [false, true]) {
		test(`removing a monitor clears its ${mode} on the ${declared ? "declared" : "catch-all"} origin`, async () => {
			const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-remove-roll-state-"));
			try {
				const { database, registry, pipeline, sessionPort } = await harness(directory, {
					contextUsage: () => ({ percent: 60, tokens: 12_000 }),
				});
				attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
				const monitor = registry.add({
					name: "remove-roll-state",
					trigger: { kind: "cron", schedule: "0 * * * *" },
					eventTypes: ["budget.tick"],
					burstPolicy: "serialize",
					contextRollTokens: 10_000,
				});
				const eventType = declared ? "budget.tick" : "undeclared.tick";
				await pipeline.submitAwaitable(monitor.monitorId, eventType, {});
				const armKeys = database.core.metaKeysInRange("monitor_context_budget_arm:", "monitor_context_budget_arm;");
				expect(armKeys).toHaveLength(1);
				if (mode === "handoff") {
					sessionPort.bind = async () => {
						throw new Error("session bind failed");
					};
					await pipeline.submitAwaitable(monitor.monitorId, eventType, {});
					expect(database.core.metaKeysInRange("monitor_roll_handoff:", "monitor_roll_handoff;")).toHaveLength(1);
				}
				expect(registry.remove(monitor.monitorId)).toBe(true);
				expect(database.core.metaKeysInRange("monitor_context_budget_arm:", "monitor_context_budget_arm;")).toEqual([]);
				expect(database.core.metaKeysInRange("monitor_roll_handoff:", "monitor_roll_handoff;")).toEqual([]);
				pipeline.dispose();
				database.close();
			} finally {
				await rm(directory, { recursive: true, force: true });
			}
		});
	}
}

for (const pending of [false, true]) {
	test(`a late answer after monitor removal cannot ${pending ? "refresh" : "create"} its context budget arm`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-remove-late-answer-"));
		let release = () => {};
		let dispatch: Promise<string> | undefined;
		try {
			const { database, registry, pipeline, sessionPort } = await harness(directory, {
				contextUsage: () => ({ percent: 60, tokens: 12_000 }),
			});
			attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
			const monitor = registry.add({
				name: "remove-late-answer",
				trigger: { kind: "cron", schedule: "0 * * * *" },
				eventTypes: ["budget.tick"],
				burstPolicy: "serialize",
				contextRollTokens: 10_000,
			});
			if (pending) {
				const fetchLastAssistant = sessionPort.fetchLastAssistant.bind(sessionPort);
				sessionPort.fetchLastAssistant = async () => {
					throw new LastAssistantUnreadableError("last_assistant_no_page");
				};
				await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
				sessionPort.fetchLastAssistant = fetchLastAssistant;
				await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
				expect(
					database.core.metaKeysInRange("monitor_context_budget_arm:", "monitor_context_budget_arm;"),
				).toHaveLength(1);
			}
			let answered = () => {};
			const answer = new Promise<void>((resolve) => {
				answered = resolve;
			});
			const gate = new Promise<void>((resolve) => {
				release = resolve;
			});
			const request = sessionPort.request.bind(sessionPort);
			sessionPort.request = async (input) => {
				const result = await request(input);
				answered();
				await gate;
				return result;
			};
			dispatch = pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			await answer;
			expect(registry.remove(monitor.monitorId)).toBe(true);
			release();
			await dispatch;
			expect(database.core.metaKeysInRange("monitor_context_budget_arm:", "monitor_context_budget_arm;")).toEqual([]);
			expect(database.core.metaKeysInRange("monitor_roll_handoff:", "monitor_roll_handoff;")).toEqual([]);
			pipeline.dispose();
			database.close();
		} finally {
			release();
			await dispatch;
			await rm(directory, { recursive: true, force: true });
		}
	});
}

for (const removedBeforeFailure of [false, true]) {
	test(`removal ${removedBeforeFailure ? "before a late host failure" : "after a host failure armed a roll"} prevents a queued dispatch recreating roll state`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-remove-queued-failure-"));
		let release = () => {};
		let first: Promise<string> | undefined;
		let second: Promise<string> | undefined;
		let active: MonitorPropagator | undefined;
		let database: Store | undefined;
		try {
			let sample = true;
			const f = await harness(directory, {
				contextUsage: () => (sample ? { percent: 60, tokens: 12_000 } : undefined),
			});
			database = f.database;
			const { registry, pipeline, sessionPort } = f;
			active = pipeline;
			const monitor = registry.add({
				name: "remove-queued-failure",
				trigger: { kind: "cron", schedule: "0 * * * *" },
				eventTypes: ["budget.tick", "other.tick"],
				burstPolicy: "serialize",
				contextRollTokens: 10_000,
			});
			await pipeline.submitAwaitable(monitor.monitorId, "other.tick", {});
			await pipeline.submitAwaitable(monitor.monitorId, "undeclared.tick", {});
			sample = false;
			await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
			const rotate = spyOn(database.bindings, "rotateEpoch");
			const gate = new Promise<void>((resolve) => {
				release = resolve;
			});
			let entered = () => {};
			const inFlight = new Promise<void>((resolve) => {
				entered = resolve;
			});
			sessionPort.request = async () => {
				entered();
				if (removedBeforeFailure) await gate;
				throw new RelayHelloError(1, "stream_ended");
			};
			first = pipeline.submitAwaitable(monitor.monitorId, "budget.tick", { event: "A" });
			await inFlight;
			if (!removedBeforeFailure) {
				await first;
				expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBe("session_host_unavailable");
			}
			let enqueued = () => {};
			const queued = new Promise<void>((resolve) => {
				enqueued = resolve;
			});
			const runExclusive = sessionPort.runExclusive.bind(sessionPort);
			sessionPort.runExclusive = (key, work) => {
				const result = runExclusive(key, async () => {
					if (!removedBeforeFailure) await gate;
					return work();
				});
				enqueued();
				return result;
			};
			const bind = spyOn(sessionPort, "bind").mockImplementation(async () => {
				throw new Error("queued dispatch bind failed");
			});
			second = pipeline.submitAwaitable(monitor.monitorId, "budget.tick", { event: "B" });
			await queued;
			expect(registry.remove(monitor.monitorId)).toBe(true);
			release();
			await first;
			await second;
			expect(bind).toHaveBeenCalledTimes(1);
			expect(rotate).not.toHaveBeenCalled();
			expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(0);
			expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBeUndefined();
			expect(database.core.metaKeysInRange("monitor_context_budget_arm:", "monitor_context_budget_arm;")).toEqual([]);
			expect(database.core.metaKeysInRange("monitor_roll_handoff:", "monitor_roll_handoff;")).toEqual([]);
			bind.mockRestore();
			rotate.mockRestore();
		} finally {
			release();
			await first;
			await second;
			active?.dispose();
			database?.close();
			await rm(directory, { recursive: true, force: true });
		}
	});
}

test("real opted-out monitors never sample context and a harvested opted-in turn does not resample", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-real-context-optin-"));
	const f = await realBrokerMonitorHarness(directory);
	const pipeline = f.makePipeline();
	try {
		const monitor = f.registry.add({
			name: "sampling",
			trigger: { kind: "cron", schedule: "0 * * * *" },
			eventTypes: ["budget.tick"],
			burstPolicy: "serialize",
		});
		await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		expect(f.relay.requests.filter((request) => request.operation === "context.get")).toHaveLength(0);
		f.registry.update({ monitorId: monitor.monitorId, contextRollTokens: 10_000 });
		const fetchLastAssistant = f.port.fetchLastAssistant.bind(f.port);
		f.port.fetchLastAssistant = async () => {
			throw new LastAssistantUnreadableError("last_assistant_no_page");
		};
		const event = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		f.port.fetchLastAssistant = fetchLastAssistant;
		const samples = f.relay.requests.filter((request) => request.operation === "context.get").length;
		expect(pipeline.retry(event).done).toBe(true);
		await pipeline.reconcile();
		expect(f.database.monitors.authoredOutput(event)).toBe(`note for ${event}`);
		expect(f.relay.requests.filter((request) => request.operation === "context.get")).toHaveLength(samples);
		f.control.tokens = 12_000;
		await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		expect(f.relay.requests.filter((request) => request.operation === "context.get")).toHaveLength(samples + 1);
		expect(
			pipeline.sessionSafetyState(originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"))),
		).toMatchObject({ pendingRoll: "context_budget" });
	} finally {
		pipeline.dispose();
		f.database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

for (const restart of [false, true]) {
	for (const result of [
		"readable",
		"definitively absent",
		"temporarily absent",
		"running",
		"unknown",
		"throws",
		"pending journal",
		"exact busy refusal",
	] as const) {
		test(`retry of an operator-settled saved turn after a size roll uses its ${result} result${restart ? " after restart" : ""}`, async () => {
			const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-settled-budget-"));
			try {
				let observeUsage = false;
				const { database, registry, pipeline, sessionPort, turns } = await harness(directory, {
					contextUsage: () => (observeUsage ? { percent: 60, tokens: 12_000 } : undefined),
				});
				attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
				const monitor = registry.add({
					name: "settled-budget",
					trigger: { kind: "cron", schedule: "0 */6 * * *" },
					eventTypes: ["budget.tick"],
					burstPolicy: "serialize",
					contextRollTokens: 10_000,
				});
				const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
				const fetchLastAssistant = sessionPort.fetchLastAssistant.bind(sessionPort);
				sessionPort.fetchLastAssistant = async () => {
					throw new LastAssistantUnreadableError("last_assistant_no_page");
				};
				const eventA = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
				sessionPort.fetchLastAssistant = fetchLastAssistant;
				const pointerKey = `monitor_authoring_turn:${eventA}`;
				const pointer = database.core.metaGet(pointerKey);
				expect(pointer).toBeDefined();
				expect(pipeline.settle(eventA).done).toBe(true);
				observeUsage = true;
				await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
				observeUsage = false;
				let active = pipeline;
				if (restart) {
					pipeline.dispose();
					active = new MonitorPropagator({
						database,
						registry,
						sessionPort,
						memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
						delivery: new DeliveryService(database),
						emit: () => {},
					});
				}
				await active.submitAwaitable(monitor.monitorId, "budget.tick", {});
				expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
				expect(turns.at(-1)?.sessionId).toBe("event-session-e1");
				expect(turns.at(-1)?.prompt).toContain("Context compaction");
				const taggedPointer = JSON.stringify({ ...JSON.parse(pointer!), sizeRolled: true });
				expect(database.core.metaGet(pointerKey)).toBe(taggedPointer);
				if (restart) {
					active.dispose();
					active = new MonitorPropagator({
						database,
						registry,
						sessionPort,
						memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
						delivery: new DeliveryService(database),
						emit: () => {},
					});
				}
				expect(database.monitors.eventGet(eventA)?.stage).toBe("failed_no_retry");
				const saved = JSON.parse(pointer!) as { sessionId: string; opRef: string };
				const handoffKey = `monitor_roll_handoff:${sessionKey}`;
				expect(database.core.metaGet(handoffKey)).toBeUndefined();
				const handoff = database.core.metaGet(handoffKey);
				const fetchWorkerOutput = sessionPort.fetchWorkerOutput.bind(sessionPort);
				const status = sessionPort.status.bind(sessionPort);
				if (result === "definitively absent")
					sessionPort.setWorkerOutputFixture(saved.opRef, {
						exitCode: 0,
						stdout: JSON.stringify({
							ok: true,
							result: {
								kind: "prompt",
								clientRef: saved.opRef,
								commandId: `command-${saved.opRef}`,
								turnId: `turn-${saved.opRef}`,
								status: "terminal_ok",
								receiptState: "absent",
								terminalAt: Date.now(),
							},
						}),
						stderr: "",
					});
				if (result === "temporarily absent")
					sessionPort.fetchWorkerOutput = async (input) =>
						input.opRef === saved.opRef ? { status: "absent", code: "transport_error" } : fetchWorkerOutput(input);
				if (result === "running")
					sessionPort.status = async (input) =>
						input.opRef === saved.opRef
							? { operationRef: saved.opRef, status: { status: "in_flight" }, summaryCompleted: false }
							: status(input);
				if (result === "unknown" || result === "pending journal")
					sessionPort.status = async (input) =>
						input.opRef === saved.opRef
							? { operationRef: saved.opRef, status: { status: "unknown" }, summaryCompleted: false }
							: status(input);
				if (result === "throws")
					sessionPort.status = async (input) => {
						if (input.opRef === saved.opRef) throw new Error("status read unavailable");
						return status(input);
					};
				if (result === "pending journal" || result === "exact busy refusal") {
					const authority = database.bindings.brokerAuthority()!;
					const scope = JSON.stringify([saved.sessionId, saved.opRef]);
					database.transaction((tx) => {
						database.bindings.beginBrokerOperation(tx, authority, {
							operation: "turn.prompt",
							scope,
							operationRef: saved.opRef,
							requestJson: "{}",
						});
						if (result === "exact busy refusal")
							database.bindings.completeBrokerOperation(
								tx,
								authority,
								"turn.prompt",
								scope,
								saved.opRef,
								JSON.stringify(BROKER_PROMPT_BUSY_REFUSAL),
							);
					});
				}
				const sends = sessionPort.sendAttempts.length;
				const binds = sessionPort.binds.length;
				const outputReads = sessionPort.workerOutputReads.length;
				const requests = spyOn(sessionPort, "request");
				expect(active.retry(eventA).done).toBe(true);
				await active.reconcile();
				if (result === "definitively absent" || result === "exact busy refusal") {
					for (let attempt = 0; attempt < 100 && database.monitors.authoredOutput(eventA) === undefined; attempt++)
						await Bun.sleep(1);
					expect(requests).toHaveBeenCalledTimes(1);
					expect(sessionPort.sendAttempts).toHaveLength(sends + 1);
					expect(sessionPort.sendAttempts.at(-1)?.sessionId).toBe("event-session-e1");
					expect(database.monitors.authoredOutput(eventA)).toBe(`note for ${eventA}`);
				} else {
					expect(requests).not.toHaveBeenCalled();
					expect(sessionPort.sendAttempts).toHaveLength(sends);
					expect(sessionPort.binds).toHaveLength(binds);
					if (result !== "readable") {
						expect(database.monitors.eventGet(eventA)?.stage).toBe("failed");
						expect(database.monitors.failure(eventA)?.code).toBe("authoring_result_unreadable");
						expect(database.core.metaGet(pointerKey)).toBe(taggedPointer);
						expect(database.monitors.authoredOutput(eventA)).toBeUndefined();
						sessionPort.fetchWorkerOutput = fetchWorkerOutput;
						sessionPort.status = status;
						expect(active.retry(eventA).done).toBe(true);
						await active.reconcile();
					}
					expect(database.monitors.authoredOutput(eventA)).toBe(`note for ${eventA}`);
					expect(requests).not.toHaveBeenCalled();
					expect(sessionPort.sendAttempts).toHaveLength(sends);
					const reads = sessionPort.workerOutputReads.slice(outputReads);
					expect(reads).not.toHaveLength(0);
					expect(reads.every((input) => input.sessionId === saved.sessionId && input.opRef === saved.opRef)).toBe(true);
				}
				requests.mockRestore();
				expect(database.core.metaGet(pointerKey)).toBeUndefined();
				expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
				expect(database.core.metaGet(handoffKey)).toBe(handoff);
				active.dispose();
				database.close();
			} finally {
				await rm(directory, { recursive: true, force: true });
			}
		});
	}
}

test("a given-up saved turn does not defer a size roll and its late result is harvested on its original session", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-given-up-budget-"));
	try {
		let clock = Date.now();
		let observeUsage = false;
		const { database, registry, pipeline, sessionPort, turns } = await harness(directory, {
			now: () => clock,
			contextUsage: () => (observeUsage ? { percent: 60, tokens: 12_000 } : undefined),
		});
		attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
		const monitor = registry.add({
			name: "given-up-budget",
			trigger: { kind: "cron", schedule: "0 */6 * * *" },
			eventTypes: ["budget.tick"],
			burstPolicy: "serialize",
			contextRollTokens: 10_000,
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
		const fetchLastAssistant = sessionPort.fetchLastAssistant.bind(sessionPort);
		sessionPort.fetchLastAssistant = async () => {
			throw new LastAssistantUnreadableError("last_assistant_no_page");
		};
		const eventA = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		sessionPort.fetchLastAssistant = fetchLastAssistant;
		const pointerKey = `monitor_authoring_turn:${eventA}`;
		const pointer = database.core.metaGet(pointerKey);
		const saved = JSON.parse(pointer!) as { sessionId: string; opRef: string };
		const fetchWorkerOutput = sessionPort.fetchWorkerOutput.bind(sessionPort);
		sessionPort.fetchWorkerOutput = async (input) =>
			input.opRef === saved.opRef ? { status: "absent", code: "transport_error" } : fetchWorkerOutput(input);
		for (let retry = 0; retry < MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS; retry++) {
			clock += 13 * 60 * 60_000;
			await pipeline.reconcile();
		}
		clock += MONITOR_EVENT_MAX_OPEN_AGE_MS + 1;
		await pipeline.reconcile();
		expect(database.monitors.eventGet(eventA)?.stage).toBe("failed_no_retry");
		expect(database.monitors.failure(eventA)?.code).toBe("authoring_turn_unresolved");
		expect(database.core.metaGet(pointerKey)).toBe(pointer);
		observeUsage = true;
		await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		observeUsage = false;
		await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
		expect(turns.at(-1)?.prompt).toContain("Context compaction");
		expect(database.core.metaGet(pointerKey)).toBe(JSON.stringify({ ...JSON.parse(pointer!), sizeRolled: true }));
		sessionPort.fetchWorkerOutput = fetchWorkerOutput;
		const requests = spyOn(sessionPort, "request");
		const sends = sessionPort.sendAttempts.length;
		const statusReads = sessionPort.statusReads.length;
		const outputReads = sessionPort.workerOutputReads.length;
		await pipeline.reconcile();
		expect(requests).not.toHaveBeenCalled();
		requests.mockRestore();
		expect(sessionPort.sendAttempts).toHaveLength(sends);
		expect(database.monitors.authoredOutput(eventA)).toBe(`note for ${eventA}`);
		expect(database.core.metaGet(pointerKey)).toBeUndefined();
		const lateStatusReads = sessionPort.statusReads.slice(statusReads).filter((input) => input.opRef === saved.opRef);
		expect(lateStatusReads).not.toHaveLength(0);
		expect(lateStatusReads.every((input) => input.sessionId === saved.sessionId)).toBe(true);
		expect(sessionPort.workerOutputReads.slice(outputReads)).not.toHaveLength(0);
		expect(
			sessionPort.workerOutputReads
				.slice(outputReads)
				.every((input) => input.sessionId === saved.sessionId && input.opRef === saved.opRef),
		).toBe(true);
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("operator retry harvests its old session while a rolled epoch has no bound session after bind failure", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-unbound-retry-"));
	try {
		let observeUsage = false;
		const { database, registry, pipeline, sessionPort, turns } = await harness(directory, {
			contextUsage: () => (observeUsage ? { percent: 60, tokens: 12_000 } : undefined),
		});
		attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
		const monitor = registry.add({
			name: "unbound-retry",
			trigger: { kind: "cron", schedule: "0 */6 * * *" },
			eventTypes: ["budget.tick"],
			burstPolicy: "serialize",
			contextRollTokens: 10_000,
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
		const handoffKey = `monitor_roll_handoff:${sessionKey}`;
		const fetchLastAssistant = sessionPort.fetchLastAssistant.bind(sessionPort);
		sessionPort.fetchLastAssistant = async () => {
			throw new LastAssistantUnreadableError("last_assistant_no_page");
		};
		const eventA = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		sessionPort.fetchLastAssistant = fetchLastAssistant;
		const pointerKey = `monitor_authoring_turn:${eventA}`;
		const saved = JSON.parse(database.core.metaGet(pointerKey)!) as { sessionId: string; opRef: string };
		expect(pipeline.settle(eventA).done).toBe(true);
		observeUsage = true;
		await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		observeUsage = false;
		const bind = sessionPort.bind.bind(sessionPort);
		sessionPort.bind = async () => {
			throw new Error("session bind failed after rotation");
		};
		const failed = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		expect(database.monitors.failure(failed)?.code).toBe("session_bind_failed");
		expect(database.bindings.getSessionRecord(sessionKey)).toMatchObject({ epoch: 1, sessionId: "" });
		const handoff = database.core.metaGet(handoffKey);
		expect(JSON.parse(handoff!)).toEqual({ epoch: 1, reason: "context_budget" });
		expect(pipeline.settle(failed).done).toBe(true);
		const requests = spyOn(sessionPort, "request");
		const binds = spyOn(sessionPort, "bind");
		const sends = sessionPort.sendAttempts.length;
		const outputReads = sessionPort.workerOutputReads.length;
		expect(pipeline.retry(eventA).done).toBe(true);
		await pipeline.reconcile();
		expect(database.monitors.authoredOutput(eventA)).toBe(`note for ${eventA}`);
		expect(requests).not.toHaveBeenCalled();
		expect(binds).not.toHaveBeenCalled();
		expect(sessionPort.sendAttempts).toHaveLength(sends);
		expect(database.bindings.getSessionRecord(sessionKey)).toMatchObject({ epoch: 1, sessionId: "" });
		expect(database.core.metaGet(handoffKey)).toBe(handoff);
		expect(database.core.metaGet(pointerKey)).toBeUndefined();
		const reads = sessionPort.workerOutputReads.slice(outputReads);
		expect(reads).not.toHaveLength(0);
		expect(reads.every((input) => input.sessionId === saved.sessionId && input.opRef === saved.opRef)).toBe(true);
		requests.mockRestore();
		binds.mockRestore();
		sessionPort.bind = bind;
		await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		expect(turns.at(-1)?.sessionId).toBe("event-session-e1");
		expect(turns.at(-1)?.prompt).toContain("Context compaction");
		expect(database.core.metaGet(handoffKey)).toBeUndefined();
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

for (const allTerminal of [false, true]) {
	test(`a saved shared batch ${allTerminal ? "with all members terminal does not defer a size roll" : "with one open member defers until the reopened batch is harvested"}`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-shared-budget-"));
		try {
			let observeUsage = false;
			const { database, registry, pipeline, sessionPort, turns } = await harness(directory, {
				contextUsage: () => (observeUsage ? { percent: 60, tokens: 12_000 } : undefined),
			});
			attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
			const monitor = registry.add({
				name: "shared-budget",
				trigger: { kind: "cron", schedule: "0 */6 * * *" },
				eventTypes: ["budget.tick"],
				burstPolicy: "coalesce",
				contextRollTokens: 10_000,
			});
			const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
			const fetchLastAssistant = sessionPort.fetchLastAssistant.bind(sessionPort);
			sessionPort.fetchLastAssistant = async () => {
				throw new LastAssistantUnreadableError("last_assistant_no_page");
			};
			const first = pipeline.submit(monitor.monitorId, "budget.tick", { member: 1 });
			const second = pipeline.submit(monitor.monitorId, "budget.tick", { member: 2 });
			for (let attempt = 0; attempt < 100 && database.monitors.eventGet(second)?.stage !== "failed"; attempt++)
				await Bun.sleep(10);
			expect(database.monitors.eventGet(second)?.stage).toBe("failed");
			sessionPort.fetchLastAssistant = fetchLastAssistant;
			const pointer = database.core.metaGet(`monitor_authoring_turn:${first}`);
			expect(JSON.parse(pointer!).eventIds.toSorted()).toEqual([first, second].toSorted());
			expect(database.core.metaGet(`monitor_authoring_turn:${second}`)).toBe(pointer);
			expect(pipeline.settle(first).done).toBe(true);
			if (allTerminal) expect(pipeline.settle(second).done).toBe(true);
			registry.update({ monitorId: monitor.monitorId, burstPolicy: "serialize" });
			observeUsage = true;
			await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", { event: "B" });
			observeUsage = false;
			await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", { event: "C" });
			const expectedPointer = allTerminal ? JSON.stringify({ ...JSON.parse(pointer!), sizeRolled: true }) : pointer;
			expect(database.core.metaGet(`monitor_authoring_turn:${first}`)).toBe(expectedPointer);
			expect(database.core.metaGet(`monitor_authoring_turn:${second}`)).toBe(expectedPointer);
			if (allTerminal) {
				expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
				expect(turns.at(-1)?.prompt).toContain("Context compaction");
				const sends = sessionPort.sendAttempts.length;
				const requests = spyOn(sessionPort, "request");
				expect(pipeline.retry(first).done).toBe(true);
				await pipeline.reconcile();
				for (let attempt = 0; attempt < 100 && database.monitors.authoredOutput(second) === undefined; attempt++)
					await Bun.sleep(1);
				expect(database.monitors.authoredOutput(first)).toBe(`note for ${first}`);
				expect(database.monitors.authoredOutput(second)).toBe(`note for ${second}`);
				expect(requests).not.toHaveBeenCalled();
				requests.mockRestore();
				expect(sessionPort.sendAttempts).toHaveLength(sends);
				expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
			} else {
				expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(0);
				expect(turns.at(-1)?.prompt).not.toContain("Context compaction");
				expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBe("context_budget");
				const sends = sessionPort.sendAttempts.length;
				// Reopen the settled sibling through the operator API so existing
				// whole-batch recovery can harvest both original results together.
				expect(pipeline.retry(first).done).toBe(true);
				await pipeline.reconcile();
				expect(database.monitors.authoredOutput(first)).toBe(`note for ${first}`);
				expect(database.monitors.authoredOutput(second)).toBe(`note for ${second}`);
				expect(sessionPort.sendAttempts).toHaveLength(sends);
				await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
				expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
				expect(turns.at(-1)?.prompt).toContain("Context compaction");
			}
			database.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});
}

for (const scenario of ["retry", "fresh dispatch before retry", "restart"] as const) {
	test(`a size roll defers until a saved terminal authoring turn is harvested: ${scenario}`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-saved-turn-budget-"));
		const log = spyOn(console, "info");
		try {
			let clock = Date.now();
			let observeUsage = false;
			const { database, registry, pipeline, sessionPort, turns } = await harness(directory, {
				now: () => clock,
				contextUsage: () => (observeUsage ? { percent: 60, tokens: 12_000 } : undefined),
			});
			attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
			const monitor = registry.add({
				name: "saved-turn-budget",
				trigger: { kind: "cron", schedule: "0 */6 * * *" },
				eventTypes: ["budget.tick"],
				burstPolicy: "serialize",
				contextRollTokens: 10_000,
			});
			const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
			const armKey = `monitor_context_budget_arm:${sessionKey}`;
			const fetchLastAssistant = sessionPort.fetchLastAssistant.bind(sessionPort);
			sessionPort.fetchLastAssistant = async () => {
				throw new LastAssistantUnreadableError("last_assistant_no_page");
			};
			const eventA = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", { event: "A" });
			const pointerKey = `monitor_authoring_turn:${eventA}`;
			const pointer = database.core.metaGet(pointerKey);
			expect(pointer).toBeDefined();
			const saved = JSON.parse(pointer!) as { sessionId: string; opRef: string };
			expect(
				(await sessionPort.status({ sessionId: saved.sessionId, repo: "", opRef: saved.opRef })).status.status,
			).toBe("terminal_ok");
			expect(database.monitors.failure(eventA)?.code).toBe("authoring_result_unreadable");
			sessionPort.fetchLastAssistant = fetchLastAssistant;
			observeUsage = true;
			await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", { event: "B" });
			observeUsage = false;
			expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBe("context_budget");
			const arm = database.core.metaGet(armKey);
			expect(JSON.parse(arm!)).toEqual({ epoch: 0, tokens: 12_000 });
			let active = pipeline;
			if (scenario === "restart") {
				pipeline.dispose();
				active = new MonitorPropagator({
					database,
					registry,
					sessionPort,
					memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
					delivery: new DeliveryService(database),
					emit: () => {},
					now: () => clock,
				});
			}
			if (scenario === "fresh dispatch before retry") {
				const eventC = await active.submitAwaitable(monitor.monitorId, "budget.tick", { event: "C" });
				expect(database.monitors.authoredOutput(eventC)).toBe(`note for ${eventC}`);
				expect(turns.at(-1)?.sessionId).toBe("event-session-e0");
				expect(turns.at(-1)?.prompt).not.toContain("Context compaction");
				expect(database.core.metaGet(pointerKey)).toBe(pointer);
				expect(database.core.metaGet(armKey)).toBe(arm);
				expect(active.sessionSafetyState(sessionKey).pendingRoll).toBe("context_budget");
			}
			const requests = spyOn(sessionPort, "request");
			const sendsBeforeRetry = sessionPort.sendAttempts.length;
			clock += 10 * 60_000 + 1;
			await active.reconcile();
			expect(requests).not.toHaveBeenCalled();
			requests.mockRestore();
			expect(sessionPort.sendAttempts).toHaveLength(sendsBeforeRetry);
			expect(database.monitors.authoredOutput(eventA)).toBe(`note for ${eventA}`);
			expect(database.core.metaGet(pointerKey)).toBeUndefined();
			expect(database.core.metaGet(armKey)).toBe(arm);
			expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(0);
			expect(active.sessionSafetyState(sessionKey).pendingRoll).toBe("context_budget");
			expect(
				turns.every((turn) => turn.sessionId === "event-session-e0" && !turn.prompt.includes("Context compaction")),
			).toBe(true);
			expect(log.mock.calls.filter(([line]) => String(line).includes("monitor session size roll deferred"))).toEqual(
				Array.from({ length: scenario === "fresh dispatch before retry" ? 2 : 1 }, () => [
					`monitor session size roll deferred for ${sessionKey} (monitor ${monitor.monitorId}): reason=context_budget saved_turns=1`,
				]),
			);
			await active.submitAwaitable(monitor.monitorId, "budget.tick", { event: "after-harvest" });
			expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
			expect(turns.at(-1)?.sessionId).toBe("event-session-e1");
			expect(turns.at(-1)?.prompt).toContain("Context compaction");
			expect(turns.at(-1)?.prompt).toContain(`note for ${eventA}`);
			expect(database.core.metaGet(armKey)).toBeUndefined();
			active.dispose();
			database.close();
		} finally {
			log.mockRestore();
			await rm(directory, { recursive: true, force: true });
		}
	});
}

for (const scenario of [
	{ tokens: 9_000, threshold: 10_000, restart: true, rolls: false },
	{ tokens: 9_000, threshold: 10_000, restart: false, rolls: false },
	{ tokens: 15_000, threshold: 14_000, restart: true, rolls: true },
	{ tokens: 15_000, threshold: 16_000, restart: true, rolls: false },
]) {
	test(`a deferred size roll persists its latest ${scenario.tokens} token sample with threshold ${scenario.threshold}${scenario.restart ? " across restart" : " in the same process"}`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-arm-latest-sample-"));
		try {
			let clock = Date.now();
			let tokens: number | undefined;
			const { database, registry, pipeline, sessionPort, turns } = await harness(directory, {
				now: () => clock,
				contextUsage: () => (tokens === undefined ? undefined : { percent: 60, tokens }),
			});
			attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
			const monitor = registry.add({
				name: "latest-sample-budget",
				trigger: { kind: "cron", schedule: "0 */6 * * *" },
				eventTypes: ["budget.tick"],
				burstPolicy: "serialize",
				contextRollTokens: 10_000,
			});
			const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
			const armKey = `monitor_context_budget_arm:${sessionKey}`;
			const fetchLastAssistant = sessionPort.fetchLastAssistant.bind(sessionPort);
			sessionPort.fetchLastAssistant = async () => {
				throw new LastAssistantUnreadableError("last_assistant_no_page");
			};
			const eventA = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", { event: "A" });
			sessionPort.fetchLastAssistant = fetchLastAssistant;
			const pointerKey = `monitor_authoring_turn:${eventA}`;
			const pointer = database.core.metaGet(pointerKey);
			expect(pointer).toBeDefined();
			tokens = 12_000;
			await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", { event: "B" });
			expect(JSON.parse(database.core.metaGet(armKey)!)).toEqual({ epoch: 0, tokens: 12_000 });
			tokens = scenario.tokens;
			const eventC = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", { event: "C" });
			expect(database.monitors.authoredOutput(eventC)).toBe(`note for ${eventC}`);
			expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(0);
			expect(turns.at(-1)?.prompt).not.toContain("Context compaction");
			expect(database.core.metaGet(pointerKey)).toBe(pointer);
			expect(pipeline.sessionSafetyState(sessionKey)).toMatchObject({
				pendingRoll: "context_budget",
				lastContextTokens: scenario.tokens,
			});
			expect(JSON.parse(database.core.metaGet(armKey)!)).toEqual({ epoch: 0, tokens: scenario.tokens });
			tokens = undefined;
			registry.update({ monitorId: monitor.monitorId, contextRollTokens: scenario.threshold });
			let active = pipeline;
			if (scenario.restart) {
				pipeline.dispose();
				active = new MonitorPropagator({
					database,
					registry,
					sessionPort,
					memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
					delivery: new DeliveryService(database),
					emit: () => {},
					now: () => clock,
				});
			}
			const sends = sessionPort.sendAttempts.length;
			clock += 10 * 60_000 + 1;
			await active.reconcile();
			expect(sessionPort.sendAttempts).toHaveLength(sends);
			expect(database.monitors.authoredOutput(eventA)).toBe(`note for ${eventA}`);
			expect(database.core.metaGet(pointerKey)).toBeUndefined();
			expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(0);
			await active.submitAwaitable(monitor.monitorId, "budget.tick", { event: "after-harvest" });
			expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(scenario.rolls ? 1 : 0);
			expect(turns.at(-1)?.sessionId).toBe(scenario.rolls ? "event-session-e1" : "event-session-e0");
			expect(turns.at(-1)?.prompt.includes("Context compaction")).toBe(scenario.rolls);
			expect(database.core.metaGet(armKey)).toBeUndefined();
			expect(active.sessionSafetyState(sessionKey).pendingRoll).toBeUndefined();
			active.dispose();
			database.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});
}

for (const scenario of ["unchanged", "raised", "cleared", "operator epoch bump"] as const) {
	test(`a persisted context budget arm survives restart with ${scenario}`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-arm-restart-"));
		try {
			let observeUsage = true;
			const { database, registry, pipeline, sessionPort, turns } = await harness(directory, {
				contextUsage: () => (observeUsage ? { percent: 60, tokens: 12_000 } : undefined),
			});
			const monitor = registry.add({
				name: "persisted-budget-arm",
				trigger: { kind: "cron", schedule: "0 */6 * * *" },
				eventTypes: ["budget.tick"],
				burstPolicy: "serialize",
				instruction: "Check the deploy queue.",
				contextRollTokens: 10_000,
			});
			const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
			const armKey = `monitor_context_budget_arm:${sessionKey}`;
			const first = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBe("context_budget");
			expect(JSON.parse(database.core.metaGet(armKey)!)).toEqual({ epoch: 0, tokens: 12_000 });
			observeUsage = false;
			pipeline.dispose();
			if (scenario === "raised") registry.update({ monitorId: monitor.monitorId, contextRollTokens: 20_000 });
			if (scenario === "cleared") registry.update({ monitorId: monitor.monitorId, contextRollTokens: null });
			if (scenario === "operator epoch bump")
				database.transaction((tx) => database.bindings.rotateEpoch(tx, sessionKey));
			const restarted = new MonitorPropagator({
				database,
				registry,
				sessionPort,
				memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
				delivery: new DeliveryService(database),
				emit: () => {},
			});
			expect(restarted.sessionSafetyState(sessionKey).pendingRoll).toBeUndefined();
			const second = await restarted.submitAwaitable(monitor.monitorId, "budget.tick", {});
			expect(database.monitors.authoredOutput(second)).toBe(`note for ${second}`);
			expect(database.core.metaGet(armKey)).toBeUndefined();
			const expectedEpoch = scenario === "unchanged" || scenario === "operator epoch bump" ? 1 : 0;
			expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(expectedEpoch);
			expect(turns[1]?.sessionId).toBe(`event-session-e${expectedEpoch}`);
			if (scenario === "unchanged") {
				expect(turns[1]?.prompt).toContain("Context compaction");
				expect(turns[1]?.prompt).toContain(`note for ${first}`);
				expect(restarted.sessionSafetyState(sessionKey).lastRoll).toBe("context_budget");
			} else {
				expect(turns[1]?.prompt).not.toContain("Context compaction");
				expect(restarted.sessionSafetyState(sessionKey).lastRoll).toBeUndefined();
			}
			expect(restarted.sessionSafetyState(sessionKey).pendingRoll).toBeUndefined();
			await restarted.submitAwaitable(monitor.monitorId, "budget.tick", {});
			expect(turns[2]?.sessionId).toBe(`event-session-e${expectedEpoch}`);
			expect(turns[2]?.prompt).not.toContain("Context compaction");
			restarted.dispose();
			database.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});
}

for (const rawArm of [
	"{invalid",
	"null",
	'{"epoch":0}',
	'{"epoch":0,"tokens":"12000"}',
	'{"epoch":-1,"tokens":12000}',
]) {
	test(`a malformed persisted context budget arm ${rawArm} is deleted without rolling`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-malformed-arm-"));
		try {
			const { database, registry, pipeline, turns } = await harness(directory);
			const monitor = registry.add({
				name: "malformed-budget-arm",
				trigger: { kind: "cron", schedule: "0 */6 * * *" },
				eventTypes: ["budget.tick"],
				burstPolicy: "serialize",
				contextRollTokens: 10_000,
			});
			const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
			const armKey = `monitor_context_budget_arm:${sessionKey}`;
			await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			database.core.metaSet(armKey, rawArm);
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			expect(database.monitors.authoredOutput(eventId)).toBe(`note for ${eventId}`);
			expect(database.core.metaGet(armKey)).toBeUndefined();
			expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(0);
			expect(turns[1]?.sessionId).toBe("event-session-e0");
			expect(turns[1]?.prompt).not.toContain("Context compaction");
			expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBeUndefined();
			database.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});
}

for (const contextRollTokens of [undefined, null, 20_000]) {
	test(`an armed context budget is disarmed when its threshold is ${contextRollTokens === undefined ? "cleared" : contextRollTokens === null ? "cleared with null" : "raised"} before dispatch`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-budget-disarm-"));
		const log = spyOn(console, "info");
		try {
			const { database, registry, pipeline, turns } = await harness(directory, {
				contextUsage: () => ({ percent: 60, tokens: 12_000 }),
			});
			const monitor = registry.add({
				name: "disarm-budget",
				trigger: { kind: "cron", schedule: "0 */6 * * *" },
				eventTypes: ["budget.tick"],
				burstPolicy: "serialize",
				contextRollTokens: 10_000,
			});
			const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
			await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBe("context_budget");
			registry.update({ monitorId: monitor.monitorId, contextRollTokens });
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
			expect(database.monitors.authoredOutput(eventId)).toBe(`note for ${eventId}`);
			expect(turns.map((turn) => turn.sessionId)).toEqual(["event-session-e0", "event-session-e0"]);
			expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(0);
			expect(turns[1]?.prompt).not.toContain("Context compaction");
			expect(pipeline.sessionSafetyState(sessionKey)).toMatchObject({
				pendingRoll: undefined,
				lastRoll: undefined,
				lastContextTokens: 12_000,
			});
			expect(database.core.metaGet(`monitor_context_budget_arm:${sessionKey}`)).toBeUndefined();
			expect(log.mock.calls.filter(([line]) => String(line).includes("monitor session size roll disarmed"))).toEqual([
				[
					`monitor session size roll disarmed for ${sessionKey} (monitor ${monitor.monitorId}): reason=context_budget context_tokens=12000/${contextRollTokens ?? "unset"}`,
				],
			]);
			database.close();
		} finally {
			log.mockRestore();
			await rm(directory, { recursive: true, force: true });
		}
	});
}

for (const reason of ["context_budget", "context_failures_native_compaction_unavailable"] as const) {
	for (const failure of ["bind failure", "bind failure and restart", "request failure"] as const) {
		test(`${reason} digest handoff survives ${failure} until the first answered new-epoch turn`, async () => {
			const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-roll-handoff-"));
			try {
				let clock = Date.now();
				let arm = false;
				const { database, registry, pipeline, sessionPort, turns } = await harness(directory, {
					now: () => clock,
					contextFailureRollThreshold: 1,
					contextUsage: () => (arm && reason === "context_budget" ? { percent: 60, tokens: 12_000 } : undefined),
					respond: (prompt) => (arm && reason !== "context_budget" ? "" : echoNotes(prompt)),
				});
				const monitor = registry.add({
					name: "handoff",
					trigger: { kind: "cron", schedule: "0 */6 * * *" },
					eventTypes: ["handoff.tick"],
					burstPolicy: "serialize",
					instruction: "Check the deploy queue.",
					contextRollTokens: 10_000,
				});
				const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "handoff.tick"));
				const handoffKey = `monitor_roll_handoff:${sessionKey}`;
				const healthy = await pipeline.submitAwaitable(monitor.monitorId, "handoff.tick", {});
				arm = true;
				const armed = await pipeline.submitAwaitable(monitor.monitorId, "handoff.tick", {});
				expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBe(reason);
				if (reason !== "context_budget") expect(pipeline.settle(armed).done).toBe(true);
				arm = false;
				const bind = sessionPort.bind.bind(sessionPort);
				const request = sessionPort.request.bind(sessionPort);
				let fail = true;
				sessionPort.bind = async (input) => {
					if (fail && failure !== "request failure") {
						fail = false;
						throw new Error("session bind failed once");
					}
					return bind(input);
				};
				sessionPort.request = async (input) => {
					if (fail && failure === "request failure") {
						fail = false;
						expect(input.sessionId).toBe("event-session-e1");
						expect(input.text).toContain("Context compaction");
						throw new Error("session send failed once before acceptance");
					}
					return request(input);
				};
				const failed = await pipeline.submitAwaitable(monitor.monitorId, "handoff.tick", {});
				expect(database.monitors.failure(failed)?.code).toBe(
					failure === "request failure" ? "authoring_turn_failed" : "session_bind_failed",
				);
				expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
				expect(JSON.parse(database.core.metaGet(handoffKey)!)).toEqual({ epoch: 1, reason });
				expect(database.core.metaGet(`monitor_context_budget_arm:${sessionKey}`)).toBeUndefined();
				registry.update({ monitorId: monitor.monitorId, instruction: "Check the updated deploy queue." });
				let active = pipeline;
				if (failure === "bind failure and restart") {
					pipeline.dispose();
					active = new MonitorPropagator({
						database,
						registry,
						sessionPort,
						memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
						delivery: new DeliveryService(database),
						emit: () => {},
						now: () => clock,
					});
				}
				clock += 6 * 60 * 60_000;
				await active.reconcile();
				expect(database.monitors.authoredOutput(failed)).toBe(`note for ${failed}`);
				const recovered = turns.find((turn) => turn.sessionId === "event-session-e1");
				expect(recovered?.prompt).toContain("Context compaction");
				expect(recovered?.prompt).toContain("Standing instruction: Check the updated deploy queue.");
				expect(recovered?.prompt).toContain(`note for ${healthy}`);
				expect(database.core.metaGet(handoffKey)).toBeUndefined();
				await active.submitAwaitable(monitor.monitorId, "handoff.tick", {});
				expect(turns.at(-1)?.sessionId).toBe("event-session-e1");
				expect(turns.at(-1)?.prompt).not.toContain("Context compaction");
				active.dispose();
				database.close();
			} finally {
				await rm(directory, { recursive: true, force: true });
			}
		});
	}
}

test("an unanswered roll handoff survives an operator epoch bump until a live-epoch answer", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-stale-handoff-"));
	try {
		let arm = true;
		const { database, registry, pipeline, sessionPort, turns } = await harness(directory, {
			contextUsage: () => (arm ? { percent: 60, tokens: 12_000 } : undefined),
		});
		const monitor = registry.add({
			name: "stale-handoff",
			trigger: { kind: "cron", schedule: "0 */6 * * *" },
			eventTypes: ["handoff.tick"],
			burstPolicy: "serialize",
			contextRollTokens: 10_000,
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "handoff.tick"));
		const handoffKey = `monitor_roll_handoff:${sessionKey}`;
		await pipeline.submitAwaitable(monitor.monitorId, "handoff.tick", {});
		arm = false;
		const bind = sessionPort.bind.bind(sessionPort);
		sessionPort.bind = async () => {
			throw new Error("session bind failed once");
		};
		await pipeline.submitAwaitable(monitor.monitorId, "handoff.tick", {});
		expect(JSON.parse(database.core.metaGet(handoffKey)!)).toEqual({ epoch: 1, reason: "context_budget" });
		database.transaction((tx) => database.bindings.rotateEpoch(tx, sessionKey));
		sessionPort.bind = bind;
		await pipeline.submitAwaitable(monitor.monitorId, "handoff.tick", {});
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(2);
		expect(turns.at(-1)?.sessionId).toBe("event-session-e2");
		expect(turns.at(-1)?.prompt).toContain("Context compaction");
		expect(database.core.metaGet(handoffKey)).toBeUndefined();
		await pipeline.submitAwaitable(monitor.monitorId, "handoff.tick", {});
		expect(turns.at(-1)?.prompt).not.toContain("Context compaction");
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

for (const marker of ["{invalid", JSON.stringify({ epoch: 2, reason: "context_budget" })]) {
	test(`a ${marker === "{invalid" ? "malformed" : "future-epoch"} roll handoff is deleted without digest injection`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-invalid-handoff-"));
		try {
			const { database, registry, pipeline, turns } = await harness(directory);
			const monitor = registry.add({
				name: "invalid-handoff",
				trigger: { kind: "cron", schedule: "0 */6 * * *" },
				eventTypes: ["handoff.tick"],
				burstPolicy: "serialize",
			});
			const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "handoff.tick"));
			const handoffKey = `monitor_roll_handoff:${sessionKey}`;
			await pipeline.submitAwaitable(monitor.monitorId, "handoff.tick", {});
			database.core.metaSet(handoffKey, marker);
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "handoff.tick", {});
			expect(database.monitors.authoredOutput(eventId)).toBe(`note for ${eventId}`);
			expect(turns.at(-1)?.prompt).not.toContain("Context compaction");
			expect(database.core.metaGet(handoffKey)).toBeUndefined();
			expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(0);
			database.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});
}

test("a stale authoring answer cannot consume an unanswered handoff after an operator epoch bump", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-stale-answer-handoff-"));
	try {
		let arm = true;
		const { database, registry, pipeline, sessionPort, turns } = await harness(directory, {
			contextUsage: () => (arm ? { percent: 60, tokens: 12_000 } : undefined),
		});
		const monitor = registry.add({
			name: "stale-answer-handoff",
			trigger: { kind: "cron", schedule: "0 */6 * * *" },
			eventTypes: ["handoff.tick"],
			burstPolicy: "serialize",
			contextRollTokens: 10_000,
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "handoff.tick"));
		const handoffKey = `monitor_roll_handoff:${sessionKey}`;
		await pipeline.submitAwaitable(monitor.monitorId, "handoff.tick", {});
		arm = false;
		const request = sessionPort.request.bind(sessionPort);
		sessionPort.request = async (input) => {
			const result = await request(input);
			database.transaction((tx) => database.bindings.rotateEpoch(tx, sessionKey));
			return result;
		};
		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "handoff.tick", {});
		sessionPort.request = request;
		expect(database.monitors.authoredOutput(eventId)).toBe(`note for ${eventId}`);
		expect(JSON.parse(database.core.metaGet(handoffKey)!)).toEqual({ epoch: 1, reason: "context_budget" });
		await pipeline.submitAwaitable(monitor.monitorId, "handoff.tick", {});
		expect(turns.at(-1)?.sessionId).toBe("event-session-e2");
		expect(turns.at(-1)?.prompt).toContain("Context compaction");
		expect(database.core.metaGet(handoffKey)).toBeUndefined();
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a roll digest survives real BrokerSessionPort poisoned-create epoch recovery and is consumed once", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-broker-handoff-"));
	const database = await Store.open(join(directory, "gateway.db"));
	let pipeline: MonitorPropagator | undefined;
	try {
		const authority = initializeTestBrokerAuthority(database, join(directory, "agent"));
		const repo = join(directory, "workspace");
		let poison = false;
		let clock = Date.now();
		let creates = 0;
		const cli: CliRunner = async (args) => {
			if (args.includes("session.create")) {
				creates++;
				return {
					exitCode: 0,
					stdout: JSON.stringify(
						poison
							? { ok: false, error: { code: "resource_gone", message: "saved session no longer exists" } }
							: { ok: true, result: { sessionId: `broker-session-${creates}` } },
					),
					stderr: "",
				};
			}
			if (args.includes("inspect"))
				return {
					exitCode: 0,
					stdout: JSON.stringify({
						ok: true,
						result: { session: { sessionId: args.at(-1), live: true, deleted: false, locator: { cwd: repo } } },
					}),
					stderr: "",
				};
			throw new Error(`unexpected command ${args.join(" ")}`);
		};
		const prompts: string[] = [];
		let response = "";
		const relay = scriptedRelay((request) => {
			if (request.operation === "turn.prompt") {
				const text = String(request.input.text);
				prompts.push(text);
				response = echoNotes(text);
				return { ok: true, result: { accepted: true, clientRef: request.input.clientRef } };
			}
			if (request.operation === "turn.result")
				return {
					ok: true,
					result: {
						kind: "prompt",
						clientRef: request.input.clientRef,
						status: "terminal_ok",
						receiptState: "present",
						outcome: { reason: "end_turn", provenance: "agent" },
					},
				};
			if (request.operation === "session.last_assistant")
				return { ok: true, page: { items: [response], complete: true } };
			if (request.operation === "context.get")
				return {
					ok: true,
					page: { items: [{ usage: { percent: prompts.length === 1 ? 60 : 1, contextWindow: 20_000 } }] },
				};
			throw new Error(`unexpected relay request ${request.operation}`);
		});
		const sessionPort = new BrokerSessionPort({
			database,
			authority,
			cli,
			tailRunner: new TailRunner({ stream: relay.spawn, repo }),
			sleep: async () => {},
		});
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "broker-handoff",
			trigger: { kind: "cron", schedule: "0 */6 * * *" },
			eventTypes: ["handoff.tick"],
			burstPolicy: "serialize",
			contextRollTokens: 10_000,
		});
		pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			repo,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			now: () => clock,
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "handoff.tick"));
		const handoffKey = `monitor_roll_handoff:${sessionKey}`;
		const first = await pipeline.submitAwaitable(monitor.monitorId, "handoff.tick", {});
		expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBe("context_budget");
		poison = true;
		const failed = await pipeline.submitAwaitable(monitor.monitorId, "handoff.tick", {});
		expect(database.monitors.failure(failed)?.code).toBe("session_bind_failed");
		expect(creates).toBe(3);
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(2);
		expect(JSON.parse(database.core.metaGet(handoffKey)!)).toEqual({ epoch: 1, reason: "context_budget" });
		poison = false;
		clock += 6 * 60 * 60_000;
		await pipeline.reconcile();
		expect(database.monitors.authoredOutput(failed)).toBe(`note for ${failed}`);
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(2);
		expect(prompts[1]).toContain("Context compaction");
		expect(prompts[1]).toContain(`note for ${first}`);
		expect(database.core.metaGet(handoffKey)).toBeUndefined();
		await pipeline.submitAwaitable(monitor.monitorId, "handoff.tick", {});
		expect(prompts).toHaveLength(3);
		expect(prompts[2]).not.toContain("Context compaction");
	} finally {
		pipeline?.dispose();
		database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

test("a queued dispatch rechecks an armed context budget after waiting in runExclusive", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-queued-budget-"));
	let release = () => {};
	let blocker: Promise<void> | undefined;
	let dispatch: Promise<string> | undefined;
	try {
		const { database, registry, pipeline, sessionPort, turns } = await harness(directory, {
			contextUsage: () => ({ percent: 60, tokens: 12_000 }),
		});
		const monitor = registry.add({
			name: "queued-budget",
			trigger: { kind: "cron", schedule: "0 */6 * * *" },
			eventTypes: ["budget.tick"],
			burstPolicy: "serialize",
			contextRollTokens: 10_000,
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
		await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBe("context_budget");
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		let entered = () => {};
		const locked = new Promise<void>((resolve) => {
			entered = resolve;
		});
		blocker = sessionPort.runExclusive(sessionKey, async () => {
			entered();
			await gate;
		});
		await locked;
		let enqueue = () => {};
		const queued = new Promise<void>((resolve) => {
			enqueue = resolve;
		});
		const runExclusive = sessionPort.runExclusive.bind(sessionPort);
		sessionPort.runExclusive = (key, work) => {
			const result = runExclusive(key, work);
			enqueue();
			return result;
		};
		dispatch = pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		await queued;
		// The dispatch already read its MonitorRecord and is waiting on the real port's chain.
		expect(turns).toHaveLength(1);
		registry.update({ monitorId: monitor.monitorId, contextRollTokens: 20_000 });
		release();
		await blocker;
		const eventId = await dispatch;
		expect(database.monitors.authoredOutput(eventId)).toBe(`note for ${eventId}`);
		expect(turns.map((turn) => turn.sessionId)).toEqual(["event-session-e0", "event-session-e0"]);
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(0);
		expect(turns[1]?.prompt).not.toContain("Context compaction");
		expect(pipeline.sessionSafetyState(sessionKey)).toMatchObject({ pendingRoll: undefined, lastRoll: undefined });
		database.close();
	} finally {
		release();
		await blocker;
		await dispatch;
		await rm(directory, { recursive: true, force: true });
	}
});

test("context budget arming uses a threshold raised during the awaited authoring turn", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-live-budget-"));
	let release = () => {};
	let dispatch: Promise<string> | undefined;
	try {
		const { database, registry, pipeline, sessionPort, turns } = await harness(directory, {
			contextUsage: () => ({ percent: 60, tokens: 12_000 }),
		});
		const monitor = registry.add({
			name: "live-budget",
			trigger: { kind: "cron", schedule: "0 */6 * * *" },
			eventTypes: ["budget.tick"],
			burstPolicy: "serialize",
			contextRollTokens: 10_000,
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		let answer = () => {};
		const answered = new Promise<void>((resolve) => {
			answer = resolve;
		});
		const request = sessionPort.request.bind(sessionPort);
		sessionPort.request = async (input) => {
			const result = await request(input);
			answer();
			await gate;
			return result;
		};
		dispatch = pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		await answered;
		registry.update({ monitorId: monitor.monitorId, contextRollTokens: 20_000 });
		release();
		const eventId = await dispatch;
		expect(database.monitors.authoredOutput(eventId)).toBe(`note for ${eventId}`);
		expect(pipeline.sessionSafetyState(sessionKey)).toMatchObject({
			pendingRoll: undefined,
			lastContextTokens: 12_000,
		});
		await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		expect(turns.map((turn) => turn.sessionId)).toEqual(["event-session-e0", "event-session-e0"]);
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(0);
		expect(turns[1]?.prompt).not.toContain("Context compaction");
		database.close();
	} finally {
		release();
		await dispatch;
		await rm(directory, { recursive: true, force: true });
	}
});

for (const scenario of [
	{ name: "below threshold", threshold: 10_000, tokens: 9_999 },
	{ name: "field unset even with huge usage", threshold: undefined, tokens: 3_000_000 },
	{ name: "usage unavailable", threshold: 10_000, tokens: undefined },
]) {
	test(`context budget does not roll when ${scenario.name}, and authoring succeeds`, async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-no-budget-roll-"));
		try {
			const { database, registry, pipeline, turns } = await harness(directory, {
				contextUsage: () => (scenario.tokens === undefined ? undefined : { percent: 50, tokens: scenario.tokens }),
			});
			const monitor = registry.add({
				name: "no-budget-roll",
				trigger: { kind: "cron", schedule: "0 */6 * * *" },
				eventTypes: ["budget.tick"],
				burstPolicy: "serialize",
				...(scenario.threshold === undefined ? {} : { contextRollTokens: scenario.threshold }),
			});
			const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
			for (let tick = 0; tick < 2; tick++) {
				const eventId = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", { tick });
				expect(database.monitors.authoredOutput(eventId)).toBe(`note for ${eventId}`);
			}
			expect(turns.map((turn) => turn.sessionId)).toEqual(["event-session-e0", "event-session-e0"]);
			expect(turns.some((turn) => turn.prompt.includes("Context compaction"))).toBe(false);
			expect(pipeline.sessionSafetyState(sessionKey)).toMatchObject({
				pendingRoll: undefined,
				lastRoll: undefined,
				lastContextTokens: scenario.tokens,
			});
			database.close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});
}

test("harvesting an accepted authoring turn succeeds without observing usage or arming a context budget roll", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-harvest-budget-"));
	try {
		let clock = Date.now();
		const { database, registry, pipeline, sessionPort, turns } = await harness(directory, {
			contextUsage: () => ({ percent: 90, tokens: 90_000 }),
			now: () => clock,
		});
		const monitor = registry.add({
			name: "harvest-budget",
			trigger: { kind: "cron", schedule: "0 */6 * * *" },
			eventTypes: ["budget.tick"],
			burstPolicy: "serialize",
			contextRollTokens: 10_000,
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
		const request = sessionPort.request.bind(sessionPort);
		sessionPort.request = async (input) => {
			const result = await request(input);
			throw new SessionRequestTimeoutError(input.sessionId, input.opRef, result.status);
		};
		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		expect(database.monitors.failure(eventId)?.code).toBe("authoring_turn_failed");
		clock += 6 * 60 * 60_000;
		await pipeline.reconcile();
		expect(database.monitors.authoredOutput(eventId)).toBe(`note for ${eventId}`);
		expect(turns).toHaveLength(1);
		expect(sessionPort.sendAttempts).toHaveLength(1);
		expect(pipeline.sessionSafetyState(sessionKey)).toMatchObject({
			pendingRoll: undefined,
			lastContextTokens: undefined,
		});
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("an empty authoring answer cannot arm a context budget roll even when usage exceeds the threshold", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-empty-budget-"));
	try {
		const { database, registry, pipeline } = await harness(directory, {
			contextUsage: () => ({ percent: 90, tokens: 90_000 }),
			respond: () => "",
		});
		const monitor = registry.add({
			name: "empty-budget",
			trigger: { kind: "cron", schedule: "0 */6 * * *" },
			eventTypes: ["budget.tick"],
			burstPolicy: "serialize",
			contextRollTokens: 10_000,
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		expect(database.monitors.failure(eventId)?.code).toBe("authoring_context_exhausted");
		expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBeUndefined();
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a superseded authoring epoch cannot arm a context budget roll for its successor", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-stale-budget-"));
	try {
		let supersede = () => {};
		const { database, registry, pipeline } = await harness(directory, {
			contextUsage: () => ({ percent: 90, tokens: 90_000 }),
			respond: (prompt) => {
				supersede();
				return echoNotes(prompt);
			},
		});
		const monitor = registry.add({
			name: "superseded-budget",
			trigger: { kind: "cron", schedule: "0 */6 * * *" },
			eventTypes: ["budget.tick"],
			burstPolicy: "serialize",
			contextRollTokens: 10_000,
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "budget.tick"));
		supersede = () => {
			database.transaction((tx) => database.bindings.rotateEpoch(tx, sessionKey));
		};
		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "budget.tick", {});
		expect(database.monitors.authoredOutput(eventId)).toBe(`note for ${eventId}`);
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
		expect(pipeline.sessionSafetyState(sessionKey)).toMatchObject({
			pendingRoll: undefined,
			lastContextTokens: undefined,
		});
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("consecutive context failures with native compaction unavailable roll, and the digest carries continuity", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-rolls-"));
	try {
		const compaction = stubPort("unavailable");
		let empty = false;
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			compaction,
			// An empty authoring response is exactly what the production incident
			// looked like: the session overflowed and answered with nothing.
			respond: (prompt) => (empty ? "" : echoNotes(prompt)),
		});
		const instruction = "Summarise the deploy queue in one line.";
		const monitor = registry.add({
			name: "digest-carrier",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["digest.tick"],
			burstPolicy: "serialize",
			instruction,
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "digest.tick"));
		const healthyIds: string[] = [];
		for (let tick = 0; tick < 3; tick += 1)
			healthyIds.push(await pipeline.submitAwaitable(monitor.monitorId, "digest.tick", { tick }));
		// Pre-failure prompts carry no digest.
		for (const turn of turns) {
			expect(
				turn.prompt.startsWith(`${MONITOR_AUTHORING_HEADER}\nTask: ${instruction}\n${MONITOR_AUTHORING_REPORT_RULE}\n`),
			).toBe(true);
			expect(turn.prompt).not.toContain("Context compaction");
		}
		empty = true;
		const failedIds: string[] = [];
		for (let tick = 0; tick < 2; tick += 1)
			failedIds.push(await pipeline.submitAwaitable(monitor.monitorId, "digest.tick", { tick: 100 + tick }));
		// One failure is not enough; two in a row with an unavailable port arms the net.
		expect(compaction.calls).toEqual(["event-session-e0", "event-session-e0"]);
		for (const eventId of failedIds) {
			const row = database.monitors.eventRows(monitor.monitorId).find((candidate) => candidate.event_id === eventId);
			expect(row?.stage).toBe("failed");
			expect(database.monitors.failure(eventId)?.code).toBe("authoring_context_exhausted");
		}
		const armed = pipeline.sessionSafetyState(sessionKey);
		expect(armed.contextFailures).toBe(2);
		expect(armed.nativeCompaction).toBe("unavailable");
		expect(armed.pendingRoll).toBe("context_failures_native_compaction_unavailable");
		// Still epoch 0: the roll happens at the next dispatch boundary, never
		// mid-batch.
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(0);
		empty = false;
		await pipeline.submitAwaitable(monitor.monitorId, "digest.tick", { tick: 200 });
		const rolled = turns.at(-1)!;
		expect(rolled.sessionId).toBe("event-session-e1");
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
		expect(rolled.prompt).toContain("Context compaction");
		// The digest carries (a) the standing contract and (b) recent authored notes.
		expect(rolled.prompt).toContain(`Standing instruction: ${instruction}`);
		for (const eventId of healthyIds) expect(rolled.prompt).toContain(`note for ${eventId}`);
		// The response contract is still the last thing in the prompt.
		expect(rolled.prompt).toContain('Respond ONLY with a JSON array containing exactly one {"eventId","note"}');
		const rolledEventId = rolled.prompt.match(/"eventId":"([^"]+)"/)![1]!;
		expect(database.monitors.authoredOutput(rolledEventId)).toBe(`note for ${rolledEventId}`);
		// Armed state consumed exactly once: the next healthy turn does not roll again.
		const after = pipeline.sessionSafetyState(sessionKey);
		expect(after.pendingRoll).toBeUndefined();
		expect(after.lastRoll).toBe("context_failures_native_compaction_unavailable");
		expect(after.contextFailures).toBe(0);
		await pipeline.submitAwaitable(monitor.monitorId, "digest.tick", { tick: 201 });
		expect(turns.at(-1)!.sessionId).toBe("event-session-e1");
		expect(turns.at(-1)!.prompt).not.toContain("Context compaction");
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a compaction port that reports success keeps the session, however long the failure streak", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-native-ok-"));
	try {
		const compaction = stubPort("succeeded");
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			compaction,
			respond: () => "",
		});
		const monitor = registry.add({
			name: "native-ok",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["native.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "native.tick"));
		for (let tick = 0; tick < 6; tick += 1) await pipeline.submitAwaitable(monitor.monitorId, "native.tick", { tick });
		// Native compaction was asked every time and said it handled it, so the
		// safety net stays holstered.
		expect(compaction.calls).toHaveLength(6);
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(0);
		expect(turns.some((turn) => turn.prompt.includes("Context compaction"))).toBe(false);
		const state = pipeline.sessionSafetyState(sessionKey);
		expect(state.contextFailures).toBe(6);
		expect(state.nativeCompaction).toBe("succeeded");
		expect(state.pendingRoll).toBeUndefined();
		expect(state.lastRoll).toBeUndefined();
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a native compaction that fails outright rolls with its own structured reason", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-native-failed-"));
	try {
		let empty = true;
		const { database, registry, pipeline } = await harness(directory, {
			contextFailureRollThreshold: 1,
			compaction: stubPort("failed"),
			respond: (prompt) => (empty ? "" : echoNotes(prompt)),
		});
		const monitor = registry.add({
			name: "native-failed",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["nf.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "nf.tick"));
		await pipeline.submitAwaitable(monitor.monitorId, "nf.tick", { tick: 0 });
		expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBe("context_failures_native_compaction_failed");
		empty = false;
		await pipeline.submitAwaitable(monitor.monitorId, "nf.tick", { tick: 1 });
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
		expect(pipeline.sessionSafetyState(sessionKey).lastRoll).toBe("context_failures_native_compaction_failed");
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

// Reversal of a former invariant ("a protocol-class failure never rolls"), on
// measured evidence: on the jip-gajae host 2026-09-02 an off-contract monitor
// session failed 19/19 ticks in one day while a sibling monitor on the same
// gateway delivered fine. Nothing remediated it and the answer text may not be
// logged, so it was both unfixable and undiagnosable. A protocol failure still
// NEVER feeds the context streak and never requests compaction — it now has its
// own streak and its own coded reason instead.
test("a protocol-class failure names the violated rule and rolls only after its own streak", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-badjson-"));
	try {
		const compaction = stubPort("unavailable");
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			protocolFailureRollThreshold: 3,
			compaction,
			respond: () => "sure! here are your notes:",
		});
		const monitor = registry.add({
			name: "bad-json",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["bad.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "bad.tick"));
		const eventIds: string[] = [];
		for (let tick = 0; tick < 2; tick += 1)
			eventIds.push(await pipeline.submitAwaitable(monitor.monitorId, "bad.tick", { tick }));
		// Below its own threshold nothing is touched: a single off-contract answer
		// is not proof the session is broken.
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(0);
		let state = pipeline.sessionSafetyState(sessionKey);
		expect(state.protocolFailures).toBe(2);
		expect(state.pendingRoll).toBeUndefined();
		// The coded sub-kind is the whole diagnosis, because the offending answer
		// text is never logged.
		expect(state.lastProtocolReason).toBe("protocol_unparseable_json");
		eventIds.push(await pipeline.submitAwaitable(monitor.monitorId, "bad.tick", { tick: 2 }));
		// Every attempt is still reported as a contract failure, and context-class
		// machinery is never engaged.
		for (const eventId of eventIds) {
			expect(database.monitors.failure(eventId)?.code).toBe("authoring_response_invalid");
		}
		expect(compaction.calls).toHaveLength(0);
		expect(turns.some((turn) => turn.prompt.includes("Context compaction"))).toBe(false);
		state = pipeline.sessionSafetyState(sessionKey);
		expect(state.contextFailures).toBe(0);
		expect(state.executorFailures).toBe(0);
		expect(state.staleContextFailures).toBe(0);
		expect(state.pendingRoll).toBe("protocol_failures_off_contract");
		// The roll itself lands at the next dispatch boundary, never mid-batch.
		await pipeline.submitAwaitable(monitor.monitorId, "bad.tick", { tick: 3 });
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
		expect(pipeline.sessionSafetyState(sessionKey).lastRoll).toBe("protocol_failures_off_contract");
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("no event is lost or authored twice across a roll boundary", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-boundary-"));
	try {
		let empty = false;
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			compaction: stubPort("unavailable"),
			respond: (prompt) => (empty ? "" : echoNotes(prompt)),
		});
		const monitor = registry.add({
			name: "boundary",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["boundary.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "boundary.tick"));
		const healthy: string[] = [];
		for (let tick = 0; tick < 3; tick += 1)
			healthy.push(await pipeline.submitAwaitable(monitor.monitorId, "boundary.tick", { tick }));
		empty = true;
		const failed: string[] = [];
		for (let tick = 0; tick < 2; tick += 1)
			failed.push(await pipeline.submitAwaitable(monitor.monitorId, "boundary.tick", { tick: 100 + tick }));
		empty = false;
		// Dispatches straddling the roll, then the recovery sweep that re-authors
		// only the events that actually failed.
		for (let tick = 0; tick < 3; tick += 1)
			healthy.push(await pipeline.submitAwaitable(monitor.monitorId, "boundary.tick", { tick: 200 + tick }));
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
		await pipeline.reconcile();
		// No loss: every event, including the two that failed, ends with its note.
		for (const eventId of [...healthy, ...failed])
			expect(database.monitors.authoredOutput(eventId)).toBe(`note for ${eventId}`);
		// No duplicate: a successful event is requested exactly once, and a failed
		// one exactly twice (its failure plus its single recovery). Only the event
		// PAYLOAD counts as a request; a rolled session's digest legitimately
		// quotes older notes.
		for (const eventId of healthy)
			expect(turns.filter((turn) => turn.prompt.includes(`"eventId":"${eventId}"`))).toHaveLength(1);
		for (const eventId of failed)
			expect(turns.filter((turn) => turn.prompt.includes(`"eventId":"${eventId}"`))).toHaveLength(2);
		expect(database.monitors.eventRows(monitor.monitorId)).toHaveLength(healthy.length + failed.length);
		expect(turns).toHaveLength(healthy.length + failed.length * 2);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a monitor with no instruction has no task segment, and rolls with a digest that omits the instruction line", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-noinstr-"));
	try {
		let empty = false;
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 1,
			compaction: stubPort("unavailable"),
			respond: (prompt) => (empty ? "" : echoNotes(prompt)),
		});
		const monitor = registry.add({
			name: "plain",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["plain.tick"],
			burstPolicy: "serialize",
		});
		await pipeline.submitAwaitable(monitor.monitorId, "plain.tick", { tick: 0 });
		// No task segment and no digest for an instruction-less monitor.
		expect(
			turns[0]!.prompt.startsWith(
				`${MONITOR_AUTHORING_HEADER}\n${MONITOR_AUTHORING_REPORT_RULE}\nRespond ONLY with a JSON array`,
			),
		).toBe(true);
		empty = true;
		await pipeline.submitAwaitable(monitor.monitorId, "plain.tick", { tick: 1 });
		empty = false;
		await pipeline.submitAwaitable(monitor.monitorId, "plain.tick", { tick: 2 });
		const rolled = turns.at(-1)!;
		expect(rolled.prompt).toContain("Context compaction");
		expect(rolled.prompt).not.toContain("Standing instruction");
		expect(rolled.prompt.startsWith(`${MONITOR_AUTHORING_HEADER}\nContext compaction`)).toBe(true);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a rolled digest skips silent notes and instruction echoes, and fills its budget from older real notes (#369)", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-digest-echo-"));
	try {
		const instruction = "Publish the blog draft and verify the live page.";
		let mode: "real" | "echo" | "silent" | "empty" = "real";
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 1,
			compaction: stubPort("unavailable"),
			respond: (prompt) => {
				if (mode === "empty") return "";
				if (mode === "real") return echoNotes(prompt);
				const payload = prompt.slice(prompt.indexOf("entry per event: ") + "entry per event: ".length);
				const note = mode === "echo" ? instruction : "[SILENT]";
				return JSON.stringify(
					(JSON.parse(payload) as Array<{ eventId: string }>).map(({ eventId }) => ({ eventId, note })),
				);
			},
		});
		const monitor = registry.add({
			name: "blog",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["blog.tick"],
			burstPolicy: "serialize",
			instruction,
		});
		// One more legitimate note than the digest holds, oldest first.
		const realIds: string[] = [];
		for (let tick = 0; tick <= MONITOR_DIGEST_MAX_NOTES; tick += 1)
			realIds.push(await pipeline.submitAwaitable(monitor.monitorId, "blog.tick", { tick }));
		mode = "silent";
		await pipeline.submitAwaitable(monitor.monitorId, "blog.tick", { tick: 100 });
		// More echoes than digest slots, all newer than every legitimate note.
		mode = "echo";
		const echoIds: string[] = [];
		for (let tick = 0; tick <= MONITOR_DIGEST_MAX_NOTES; tick += 1)
			echoIds.push(await pipeline.submitAwaitable(monitor.monitorId, "blog.tick", { tick: 200 + tick }));
		// No delivery target: an echo settles as an ordinary undelivered note.
		for (const echoId of echoIds) expect(database.monitors.authoredOutput(echoId)).toBe(instruction);
		mode = "empty";
		await pipeline.submitAwaitable(monitor.monitorId, "blog.tick", { tick: 300 });
		mode = "real";
		await pipeline.submitAwaitable(monitor.monitorId, "blog.tick", { tick: 301 });
		const rolled = turns.at(-1)?.prompt ?? "";
		expect(rolled).toContain("Context compaction");
		expect(rolled).not.toContain(`blog.tick: ${instruction}`);
		expect(rolled).not.toContain("[SILENT]");
		// The echoes do not consume the budget: the newest legitimate notes fill it,
		// and the cap still holds.
		const kept = realIds.filter((id) => rolled.includes(`blog.tick: note for ${id}`));
		expect(kept).toEqual(realIds.slice(1));
		expect(kept).toHaveLength(MONITOR_DIGEST_MAX_NOTES);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a healthy answer resets the streak, so isolated context failures never roll", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-streak-reset-"));
	try {
		const compaction = stubPort("unavailable");
		let fail = false;
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			compaction,
			respond: (prompt) => {
				if (!fail) return echoNotes(prompt);
				throw new Error("session send failed: context_too_large");
			},
		});
		const monitor = registry.add({
			name: "flaky",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["flaky.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "flaky.tick"));
		// A single context_too_large, then a healthy turn — twice. The failures are
		// never consecutive, so nothing may arm.
		for (const tick of [0, 1, 2, 3]) {
			fail = tick % 2 === 0;
			await pipeline.submitAwaitable(monitor.monitorId, "flaky.tick", { tick });
			const midway = pipeline.sessionSafetyState(sessionKey);
			// Exactly one failure is on the books right after a failure, and the
			// following healthy answer wipes it.
			expect(midway.contextFailures).toBe(fail ? 1 : 0);
			expect(midway.pendingRoll).toBeUndefined();
		}
		// Both failures were treated as real context failures (native compaction
		// was asked each time) — they simply never became a streak.
		expect(compaction.calls).toHaveLength(2);
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(0);
		expect(turns.some((turn) => turn.prompt.includes("Context compaction"))).toBe(false);
		const state = pipeline.sessionSafetyState(sessionKey);
		expect(state.contextFailures).toBe(0);
		expect(state.lastRoll).toBeUndefined();
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a context failure on a replayed stale event does not feed the current session's streak", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-stale-"));
	try {
		const compaction = stubPort("unavailable");
		let mode: "ok" | "executor" | "context" = "executor";
		const { database, registry, pipeline, turns } = await harness(directory, {
			// 1 makes the assertion sharp: any COUNTED context failure would arm
			// immediately, so a still-disarmed net proves the stale failure was
			// dropped rather than merely under threshold.
			contextFailureRollThreshold: 1,
			compaction,
			respond: (prompt) => {
				if (mode === "ok") return echoNotes(prompt);
				if (mode === "executor") throw new Error("external tool failed: gh exited 1");
				throw new Error("session send failed: context_too_large");
			},
		});
		const monitor = registry.add({
			name: "stale",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["stale.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "stale.tick"));
		// Park an event at `failed` with an executor failure: retryable, and no
		// context evidence on the books.
		const staleId = await pipeline.submitAwaitable(monitor.monitorId, "stale.tick", { tick: 0 });
		expect(database.monitors.failure(staleId)?.code).toBe("executor_failed");
		expect(pipeline.sessionSafetyState(sessionKey).contextFailures).toBe(0);
		// Now the session starts answering with a context rejection, and reconcile
		// replays that old event into it.
		mode = "context";
		await pipeline.reconcile();
		const replayRow = database.monitors.eventRows(monitor.monitorId).find((row) => row.event_id === staleId);
		expect(replayRow?.dispatch_attempts).toBeGreaterThan(0);
		expect(database.monitors.failure(staleId)?.code).toBe("authoring_context_exhausted");
		// The replayed failure is recorded, but as stale: no streak, no arm, no roll.
		const afterReplay = pipeline.sessionSafetyState(sessionKey);
		expect(afterReplay.staleContextFailures).toBe(1);
		expect(afterReplay.contextFailures).toBe(0);
		expect(afterReplay.pendingRoll).toBeUndefined();
		// No session row is even written when every turn throws, so "epoch 0" is
		// the absence of a roll; the bound session id says the same thing directly.
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch ?? 0).toBe(0);
		expect(new Set(turns.map((turn) => turn.sessionId))).toEqual(new Set(["event-session-e0"]));
		// A native compaction is never requested on behalf of a stale event either.
		expect(compaction.calls).toHaveLength(0);
		// The same failure from the session's OWN fresh work still arms: the guard
		// narrows the trigger, it does not disable it.
		await pipeline.submitAwaitable(monitor.monitorId, "stale.tick", { tick: 1 });
		const afterFresh = pipeline.sessionSafetyState(sessionKey);
		expect(afterFresh.contextFailures).toBe(1);
		expect(afterFresh.pendingRoll).toBe("context_failures_native_compaction_unavailable");
		expect(compaction.calls).toEqual(["event-session-e0"]);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("an aside-worker timeout streak never rolls: an executor failure is not context evidence", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-aside-"));
	try {
		const compaction = stubPort("unavailable");
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			compaction,
			// The real shape: the aside collection worker gives up at its 300s cap.
			respond: () => {
				throw new Error("aside collection worker timed out after 300000ms");
			},
		});
		const monitor = registry.add({
			name: "aside",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["aside.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "aside.tick"));
		const eventIds: string[] = [];
		for (let tick = 0; tick < 5; tick += 1)
			eventIds.push(await pipeline.submitAwaitable(monitor.monitorId, "aside.tick", { tick }));
		// Its own failure code, never aggregated with context exhaustion.
		for (const eventId of eventIds) expect(database.monitors.failure(eventId)?.code).toBe("aside_timeout");
		expect(compaction.calls).toHaveLength(0);
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch ?? 0).toBe(0);
		expect(new Set(turns.map((turn) => turn.sessionId))).toEqual(new Set(["event-session-e0"]));
		expect(turns.some((turn) => turn.prompt.includes("Context compaction"))).toBe(false);
		const state = pipeline.sessionSafetyState(sessionKey);
		expect(state.executorFailures).toBe(5);
		expect(state.contextFailures).toBe(0);
		expect(state.protocolFailures).toBe(0);
		expect(state.pendingRoll).toBeUndefined();
		expect(state.lastRoll).toBeUndefined();
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

/** The measured drill message: child dead on the wrapper's cap, daemon job alive. */
const ORPHANED_MESSAGE =
	"aside exec collection exceeded 360s; child killed but Aside daemon task still running (3 sessions, 10 tabs)";

test("an orphaned-executor streak never rolls and never touches the context streak", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-orphaned-"));
	try {
		const compaction = stubPort("unavailable");
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			compaction,
			respond: () => {
				throw new Error(ORPHANED_MESSAGE);
			},
		});
		const monitor = registry.add({
			name: "orphan",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["orphan.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "orphan.tick"));
		const eventIds: string[] = [];
		for (let tick = 0; tick < 5; tick += 1)
			eventIds.push(await pipeline.submitAwaitable(monitor.monitorId, "orphan.tick", { tick }));
		// Its own code, never aggregated with the plain timeout or with context.
		for (const eventId of eventIds) expect(database.monitors.failure(eventId)?.code).toBe("orphaned_executor");
		// The session is left completely alone: no compaction request, no roll.
		expect(compaction.calls).toHaveLength(0);
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch ?? 0).toBe(0);
		expect(new Set(turns.map((turn) => turn.sessionId))).toEqual(new Set(["event-session-e0"]));
		expect(turns.some((turn) => turn.prompt.includes("Context compaction"))).toBe(false);
		const state = pipeline.sessionSafetyState(sessionKey);
		expect(state.contextFailures).toBe(0);
		expect(state.orphanedExecutorFailures).toBe(5);
		expect(state.executorFailures).toBe(5);
		expect(state.protocolFailures).toBe(0);
		// The operator gets a coded reason, which is the whole point of this class.
		expect(state.lastExecutorReason).toBe("executor_orphaned_external_work");
		expect(state.pendingRoll).toBeUndefined();
		expect(state.lastRoll).toBeUndefined();
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("interleaved orphaned-executor and context failures advance only the context streak", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-orphan-mixed-"));
	try {
		const compaction = stubPort("unavailable");
		let mode: "orphaned" | "context" = "orphaned";
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			compaction,
			respond: () => {
				if (mode === "orphaned") throw new Error(ORPHANED_MESSAGE);
				throw new Error("session send failed: context_too_large");
			},
		});
		const monitor = registry.add({
			name: "mixed",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["mixed.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "mixed.tick"));
		// orphaned, context, orphaned, context: the orphans neither advance nor
		// reset the streak, so the two context failures still reach the threshold.
		const expectedContextStreak = [0, 1, 1, 2];
		const sequence: Array<"orphaned" | "context"> = ["orphaned", "context", "orphaned", "context"];
		for (const [index, step] of sequence.entries()) {
			mode = step;
			await pipeline.submitAwaitable(monitor.monitorId, "mixed.tick", { tick: index });
			expect(pipeline.sessionSafetyState(sessionKey).contextFailures).toBe(expectedContextStreak[index]);
		}
		const state = pipeline.sessionSafetyState(sessionKey);
		expect(state.orphanedExecutorFailures).toBe(2);
		expect(state.executorFailures).toBe(2);
		// Native compaction was requested for the context failures only.
		expect(compaction.calls).toHaveLength(2);
		// The threshold was reached by context evidence alone, so the net is armed.
		expect(state.pendingRoll).toBe("context_failures_native_compaction_unavailable");
		// Still not rolled: that happens at the next dispatch boundary.
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch ?? 0).toBe(0);
		expect(new Set(turns.map((turn) => turn.sessionId))).toEqual(new Set(["event-session-e0"]));
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a session that stays busy across dispatches is rolled so the next slot lands on a live session (#263)", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-busy-"));
	try {
		const compaction = stubPort("unavailable");
		// The live shape: the epoch-0 session is wedged on a turn it never
		// finishes and refuses every prompt with `busy`; a fresh session answers.
		let clock = Date.now();
		const { database, registry, pipeline, turns, sessionPort } = await harness(directory, {
			contextFailureRollThreshold: 1,
			compaction,
			busy: (sessionId) => sessionId === "event-session-e0",
			now: () => clock,
		});
		const monitor = registry.add({
			name: "threads",
			trigger: { kind: "cron", schedule: "0 * * * *" },
			eventTypes: ["threads.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "threads.tick"));
		expect(MONITOR_BUSY_FAILURE_ROLL_THRESHOLD).toBe(2);
		const first = await pipeline.submitAwaitable(monitor.monitorId, "threads.tick", { tick: 0 });
		expect(database.monitors.failure(first)?.code).toBe("session_busy");
		let state = pipeline.sessionSafetyState(sessionKey);
		expect(state.busyFailures).toBe(1);
		expect(state.pendingRoll).toBeUndefined();
		// Reconcile replays the failed slot into the same session and it is busy
		// again. A replay still counts: the refusal is about the session, not the
		// payload, and this is exactly the retry loop that burned every slot.
		await pipeline.reconcile();
		expect(database.monitors.failure(first)?.code).toBe("session_busy");
		state = pipeline.sessionSafetyState(sessionKey);
		expect(state.busyFailures).toBe(2);
		expect(state.pendingRoll).toBe("session_busy_stalled");
		// Not context evidence, not contract evidence: nothing else moved.
		expect(state.contextFailures).toBe(0);
		expect(state.staleContextFailures).toBe(0);
		expect(state.executorFailures).toBe(0);
		expect(state.protocolFailures).toBe(0);
		expect(compaction.calls).toHaveLength(0);
		expect(turns).toHaveLength(0);
		// The next slot rolls at the dispatch boundary, binds a fresh session and
		// delivers, instead of aiming at the stalled session again.
		const next = await pipeline.submitAwaitable(monitor.monitorId, "threads.tick", { tick: 1 });
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
		state = pipeline.sessionSafetyState(sessionKey);
		expect(state.lastRoll).toBe("session_busy_stalled");
		expect(state.busyFailures).toBe(0);
		expect(database.monitors.eventRows(monitor.monitorId).find((row) => row.event_id === next)?.stage).toBe(
			"authored_no_delivery",
		);
		expect(turns.map((turn) => turn.sessionId)).toEqual(["event-session-e1"]);
		// The stalled host is ended so it stops occupying the runtime.
		await Bun.sleep(0);
		expect(sessionPort.closes.map((entry) => entry.sessionId)).toEqual(["event-session-e0"]);
		// The stranded slot is replayed into the live session too, once its
		// retry backoff has elapsed (#179: the second retry waits 10 minutes).
		clock += 10 * 60_000 + 1;
		await pipeline.reconcile();
		expect(database.monitors.eventRows(monitor.monitorId).find((row) => row.event_id === first)?.stage).toBe(
			"authored_no_delivery",
		);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a wedged prompt submission rolls the session so the next slot lands on a live session", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-wedged-"));
	try {
		// The live shape: the epoch-0 session takes every prompt and answers with a
		// terminal submission failure (a prior run or its pre-prompt compaction
		// still holds the agent); a fresh session answers normally.
		const { database, registry, pipeline, turns, sessionPort } = await harness(directory, {
			wedgedSubmission: (sessionId) => sessionId === "event-session-e0",
		});
		const monitor = registry.add({
			name: "canonicalize",
			trigger: { kind: "cron", schedule: "30 */6 * * *" },
			eventTypes: ["memory.canonicalize"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "memory.canonicalize"));
		const first = await pipeline.submitAwaitable(monitor.monitorId, "memory.canonicalize", { slot: 0 });
		expect(database.monitors.failure(first)?.code).toBe("session_submission_wedged");
		let state = pipeline.sessionSafetyState(sessionKey);
		expect(state.pendingRoll).toBe("session_submission_wedged");
		// Nothing was sent to the agent, so the failure is not executor noise and
		// nothing else moved — and the roll itself waits for the dispatch boundary.
		expect(state.executorFailures).toBe(0);
		expect(state.contextFailures).toBe(0);
		expect(turns).toHaveLength(0);
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch ?? 0).toBe(0);
		// The next slot rolls at the dispatch boundary, binds a fresh session and
		// delivers, instead of aiming at the wedged session again.
		const next = await pipeline.submitAwaitable(monitor.monitorId, "memory.canonicalize", { slot: 1 });
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
		state = pipeline.sessionSafetyState(sessionKey);
		expect(state.lastRoll).toBe("session_submission_wedged");
		expect(database.monitors.eventRows(monitor.monitorId).find((row) => row.event_id === next)?.stage).toBe(
			"authored_no_delivery",
		);
		expect(turns.map((turn) => turn.sessionId)).toEqual(["event-session-e1"]);
		// The wedged session's host is ended so it stops occupying the runtime.
		await Bun.sleep(0);
		expect(sessionPort.closes.map((entry) => entry.sessionId)).toEqual(["event-session-e0"]);
		// The stranded slot replays into the live session: nothing was sent, so the
		// #179 retry backoff must not apply to this code.
		await pipeline.reconcile();
		expect(database.monitors.eventRows(monitor.monitorId).find((row) => row.event_id === first)?.stage).toBe(
			"authored_no_delivery",
		);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("repeated prior-run-busy terminal failures roll within a bounded streak and replay events once", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-prior-run-busy-"));
	try {
		const { database, registry, pipeline, turns, sessionPort } = await harness(directory);
		const monitor = registry.add({
			name: "prior-run-busy",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["busy.tick"],
			burstPolicy: "serialize",
			channelTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "loopback" } },
		});
		const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "busy.tick"));
		const priorRunReason = "Timed out waiting for prior agent run to finish before prompting.";
		const attempts: Array<{ sessionId: string; eventIds: string[] }> = [];
		const originalRequest = sessionPort.request.bind(sessionPort);
		sessionPort.request = async (input) => {
			const marker = "entry per event: ";
			const payload = input.text.slice(input.text.lastIndexOf(marker) + marker.length);
			const eventIds = (JSON.parse(payload) as Array<{ eventId: string }>).map(({ eventId }) => eventId);
			attempts.push({ sessionId: input.sessionId, eventIds });
			if (input.sessionId === "event-session-e0") {
				const cause = Object.assign(new Error("sdk_prompt_terminal_failed"), {
					code: "sdk_prompt_terminal_failed",
					reason: priorRunReason,
				});
				throw new Error("monitor prompt terminal failure", { cause });
			}
			return originalRequest(input);
		};

		expect(MONITOR_PRIOR_RUN_BUSY_FAILURE_ROLL_THRESHOLD).toBe(2);
		const failed: string[] = [];
		for (let index = 0; index < MONITOR_PRIOR_RUN_BUSY_FAILURE_ROLL_THRESHOLD; index += 1) {
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "busy.tick", { tick: index });
			failed.push(eventId);
			expect(database.monitors.failure(eventId)?.code).toBe("session_submission_wedged");
			const state = pipeline.sessionSafetyState(sessionKey);
			expect(state.priorRunBusyFailures).toBe(index + 1);
			expect(state.pendingRoll).toBe(
				index + 1 === MONITOR_PRIOR_RUN_BUSY_FAILURE_ROLL_THRESHOLD ? "session_prior_run_busy_stalled" : undefined,
			);
			expect(database.bindings.getSessionRecord(sessionKey)?.epoch ?? 0).toBe(0);
		}

		const recovered = await pipeline.submitAwaitable(monitor.monitorId, "busy.tick", { tick: "after-roll" });
		expect(database.bindings.getSessionRecord(sessionKey)?.epoch).toBe(1);
		expect(pipeline.sessionSafetyState(sessionKey)).toMatchObject({
			lastRoll: "session_prior_run_busy_stalled",
			priorRunBusyFailures: 0,
		});
		expect(database.monitors.eventRows(monitor.monitorId).find((row) => row.event_id === recovered)?.stage).toBe(
			"authored",
		);
		await Bun.sleep(0);
		expect(sessionPort.closes.map(({ sessionId }) => sessionId)).toEqual(["event-session-e0"]);

		// The known terminal failure proves nothing was sent, so its rows replay
		// immediately on the fresh session. Every durable event is authored once.
		await pipeline.reconcile();
		for (const eventId of [...failed, recovered]) {
			expect(database.monitors.authoredOutput(eventId)).toBe(`note for ${eventId}`);
			expect(database.monitors.eventRows(monitor.monitorId).find((row) => row.event_id === eventId)?.stage).toBe(
				"authored",
			);
		}
		for (const eventId of failed)
			expect(attempts.filter((attempt) => attempt.eventIds.includes(eventId))).toHaveLength(2);
		expect(attempts.filter((attempt) => attempt.eventIds.includes(recovered))).toHaveLength(1);
		expect(attempts.map(({ sessionId }) => sessionId)).toEqual([
			"event-session-e0",
			"event-session-e0",
			"event-session-e1",
			"event-session-e1",
			"event-session-e1",
		]);
		expect(turns.map(({ sessionId }) => sessionId)).toEqual([
			"event-session-e1",
			"event-session-e1",
			"event-session-e1",
		]);
		const deliveries = database.outbox.rows().map((row) => JSON.parse(row.payload_json) as { text: string });
		for (const eventId of [...failed, recovered])
			expect(deliveries.filter(({ text }) => text.includes(`note for ${eventId}`))).toHaveLength(1);
		expect(database.monitors.eventRows(monitor.monitorId)).toHaveLength(failed.length + 1);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
