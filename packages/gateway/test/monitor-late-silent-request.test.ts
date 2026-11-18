import { expect, spyOn, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type OriginRef, originKey } from "@gajae-gateway/protocol";
import type { CliResult, CliRunner } from "@gajae-gateway/subsession";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { Store } from "../src/store";
import {
	initializeTestBrokerAuthority,
	type ScriptedRelayReply,
	type ScriptedRelayRequest,
	scriptedRelay,
} from "./session-port.fake";

const target: OriginRef = { platform: "discord", kind: "channel", conversationId: "report-room" };
const notifications: string[] = [];

function eventsFromPrompt(text: string): Array<{ eventId: string }> {
	const match = text.match(/\[.*\]$/s);
	if (!match) throw new Error("prompt has no event array");
	return JSON.parse(match[0]) as Array<{ eventId: string }>;
}

/**
 * A real BrokerSessionPort as boot.ts builds it: the broker's CLI runner plus a
 * resident-relay TailRunner. A relay the request opened is always closed by the
 * time request() returns, so every propagator follow-up read goes over the CLI.
 */
async function brokerPortHarness(
	directory: string,
	relay: (request: ScriptedRelayRequest) => ScriptedRelayReply | Promise<ScriptedRelayReply>,
	cliTranscript: (args: readonly string[]) => CliResult | undefined = () => undefined,
) {
	const database = await Store.open(join(directory, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(directory, "agent"));
	const repo = join(directory, "workspace");
	const cli: CliRunner = async (args) => {
		const extra = cliTranscript(args);
		if (extra) return extra;
		if (args.includes("session.create"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { sessionId: "broker-session-1" } }),
				stderr: "",
			};
		if (args.includes("inspect"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: { session: { sessionId: args.at(-1), live: true, deleted: false, locator: { cwd: repo } } },
				}),
				stderr: "",
			};
		throw new Error(`unexpected command ${args.join(" ").slice(0, 90)}`);
	};
	const sessionPort = new BrokerSessionPort({
		database,
		authority,
		cli,
		tailRunner: new TailRunner({
			stream: (sessionId) => scriptedRelay(relay).spawn(sessionId),
			repo,
		}),
	});
	return { database, sessionPort, repo };
}

function turnFixture(): { outputs: Map<string, string>; started: Map<string, number> } {
	return { outputs: new Map(), started: new Map() };
}

/** The turn.prompt / turn.result / session.last_assistant half of the host, shared by both tests. */
function hostTurnHandlers(
	realAnswer: string,
	outputs: Map<string, string>,
	started: Map<string, number>,
	onAnswer: (answer: string) => void,
): (request: ScriptedRelayRequest) => ScriptedRelayReply {
	return (request) => {
		if (request.operation === "turn.prompt") {
			const clientRef = String(request.input.clientRef);
			const answer = JSON.stringify(
				eventsFromPrompt(String(request.input.text)).map(({ eventId }) => ({ eventId, note: realAnswer })),
			);
			outputs.set(clientRef, answer);
			started.set(clientRef, Date.now());
			onAnswer(answer);
			return { ok: true, result: { accepted: true, clientRef } };
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
						text: outputs.get(String(request.input.clientRef)) ?? "",
						truncated: false,
						byteLength: Buffer.byteLength(outputs.get(String(request.input.clientRef)) ?? ""),
					},
				},
			};
		throw new Error(`unexpected relay request ${request.operation}`);
	};
}

/**
 * Issue #809 regression on the REAL MonitorPropagator request path
 * (dispatch -> SessionPort.request -> parse -> author), through a real
 * BrokerSessionPort. The authoring turn answers correctly; before the monitor
 * selects the response, an async-result wake-up appends `[SILENT]`, which
 * becomes the session's LAST assistant message. `requestResult.assistant`
 * (session.last_assistant) then reads the marker and would author `[SILENT]`;
 * the turn-scoped transcript read must return the real answer instead, so the
 * event is authored with it and its monitor report is enqueued.
 */
test("a late [SILENT] after the authoring answer never replaces the monitor response (request path)", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-late-silent-request-"));
	let pipeline: MonitorPropagator | undefined;
	try {
		const realAnswer = "Deploy finished: 3 services rolled, 0 failures.";
		const { outputs, started } = turnFixture();
		// The host transcript. The late wake-up's `[SILENT]` row lands right
		// after the authoring answer, exactly as in the production race.
		const transcript: Array<{ ts: string; body: string }> = [];
		let silentRowAppended = false;
		const { database, sessionPort, repo } = await brokerPortHarness(
			directory,
			(request) => {
				const turn = hostTurnHandlers(realAnswer, outputs, started, (answer) => {
					transcript.push({ ts: new Date().toISOString(), body: answer });
					// The wake-up appends its silent row while the port sits between
					// the terminal status and the final assistant read.
					transcript.push({ ts: new Date().toISOString(), body: "[SILENT]" });
					silentRowAppended = true;
				});
				if (request.operation === "session.last_assistant")
					return { ok: true, page: { items: [transcript.at(-1)?.body ?? ""], complete: true } };
				return turn(request);
			},
			(args) => {
				if (args.includes("query") && args.includes("turn.result")) {
					// Handle turn.result query: find the clientRef in the args
					const atIdx = args.indexOf("--json-input");
					if (atIdx < 0) return undefined;
					const inputIdx = atIdx + 1;
					if (inputIdx >= args.length) return undefined;
					let input: { clientRef?: string } | undefined;
					try {
						input = JSON.parse(args[inputIdx]);
					} catch {
						return undefined;
					}
					const clientRef = input?.clientRef;
					if (!clientRef) return undefined;
					const answer = outputs.get(clientRef);
					const startTime = started.get(clientRef);
					if (answer === undefined || startTime === undefined) return undefined;
					return {
						exitCode: 0,
						stdout: JSON.stringify({
							ok: true,
							result: {
								kind: "prompt",
								clientRef,
								status: "terminal_ok",
								startedAt: startTime,
								terminalAt: Date.now(),
								outcome: { reason: "end_turn", provenance: "agent" },
								content: {
									version: 1,
									type: "text",
									text: answer,
									truncated: false,
									byteLength: Buffer.byteLength(answer),
								},
								receiptState: "present",
							},
						}),
						stderr: "",
					};
				}
				if (!args.includes("transcript.list")) return undefined;
				return {
					exitCode: 0,
					stdout: JSON.stringify({
						page: {
							items: transcript.map((row) => ({ role: "assistant", ts: row.ts, body: row.body })),
							complete: true,
						},
					}),
					stderr: "",
				};
			},
		);
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "late-silent-watch",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["late.silent"],
			burstPolicy: "serialize",
			reportTo: target,
		});
		notifications.length = 0;
		pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			repo,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			notifyInbound: async (key) => {
				notifications.push(key);
			},
		});

		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "late.silent", { run: 1 });

		// The late silent row landed before the response was selected.
		expect(silentRowAppended).toBe(true);
		// The event is authored with the REAL monitor answer — never the marker.
		expect(database.monitors.authoredOutput(eventId)).toBe(realAnswer);
		expect(database.monitors.eventGet(eventId)?.stage).toBe("authored_no_delivery");

		// The monitor report was enqueued for its reportTo conversation and the
		// owner was woken: a [SILENT] authorship would have suppressed both.
		const report = database.inbox.pendingOldest(originKey(target));
		expect(report).toMatchObject({
			message_id: `monitor-report:${eventId}`,
			source: "lane_report",
			origin_key: originKey(target),
			state: "pending",
		});
		expect(report?.body.endsWith(realAnswer)).toBe(true);
		expect(notifications).toEqual([originKey(target)]);
	} finally {
		pipeline?.dispose();
		await pipeline?.drain();
		await rm(directory, { recursive: true, force: true });
	}
});

/**
 * A failed turn-scoped transcript read is a structured failure with a logged
 * warning — never a silent slide back into the session's last assistant
 * message, which the late wake-up has already replaced with `[SILENT]`.
 */
test("a failed turn-scoped transcript read is a structured warning, not a silent last-assistant fallback", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-late-silent-tear-"));
	let pipeline: MonitorPropagator | undefined;
	try {
		const realAnswer = "Backup verified: snapshot consistent.";
		const { outputs, started } = turnFixture();
		const { database, sessionPort, repo } = await brokerPortHarness(
			directory,
			(request) => {
				const turn = hostTurnHandlers(realAnswer, outputs, started, () => {});
				if (request.operation === "session.last_assistant")
					// The hazard in full: by the time the final read runs, the
					// session's last assistant message is the wake-up's marker.
					return { ok: true, page: { items: ["[SILENT]"], complete: true } };
				if (request.operation === "turn.result")
					// The op's terminal receipt exists but its body never attached
					// (a late agent_end, #248): the invocation-owned read is
					// structured-absent, not authorable.
					return {
						ok: true,
						result: {
							kind: "prompt",
							clientRef: request.input.clientRef,
							status: "terminal_ok",
							receiptState: "absent",
							outcome: { reason: "end_turn", provenance: "agent" },
						},
					};
				if (request.operation === "transcript.list")
					// The transcript surface is torn entirely (CLI variant below).
					return { ok: false, error: { code: "relay_torn", message: "surface torn" } };
				return turn(request);
			},
			(args) => {
				if (args.includes("query") && args.includes("turn.result")) {
					// The CLI recovery transport after a torn relay is torn as well
					// (the whole result surface is down): the port's error reaches
					// #responseFromTurn instead of an empty-but-valid answer.
					throw new Error("broker turn.result surface unavailable");
				}
				if (!args.includes("transcript.list")) return undefined;
				// The CLI recovery transport after a torn relay is torn as well.
				throw new Error("broker transcript surface unavailable");
			},
		);
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "late-silent-tear",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["late.silent"],
			burstPolicy: "serialize",
			reportTo: target,
		});
		pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			repo,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
		});

		const warnings = spyOn(console, "error").mockImplementation(() => {});
		let eventId: string;
		try {
			eventId = await pipeline.submitAwaitable(monitor.monitorId, "late.silent", { run: 1 });
			// Verify that the error was logged (either "read failed" or "unavailable"):
			expect(warnings.mock.calls.some(([line]) => String(line).includes("monitor response"))).toBe(true);
		} finally {
			warnings.mockRestore();
		}
		// Unreadable is a recorded failure with evidence, never a guessed answer:
		// the pre-fix behaviour would have authored `[SILENT]` here.
		expect(database.monitors.authoredOutput(eventId)).toBeUndefined();
		expect(database.monitors.failure(eventId)?.code).toBe("authoring_result_unreadable");
		expect(database.inbox.pendingOldest(originKey(target))).toBeUndefined();
	} finally {
		pipeline?.dispose();
		await pipeline?.drain();
		await rm(directory, { recursive: true, force: true });
	}
});

/**
 * Late [SILENT] after a valid answer in the same operation: the operation's
 * own result is kept, never replaced by a [SILENT] that lands after it. This
 * verifies the fix correctly binds to the operation and reads from turn.result.
 */
test("late [SILENT] after valid answer in same operation keeps the answer", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-keep-answer-"));
	let pipeline: MonitorPropagator | undefined;
	try {
		const realAnswer = "Task completed successfully: 42 items processed.";
		const { outputs, started } = turnFixture();
		// Transcript simulates a [SILENT] arriving after the real answer
		const transcript: Array<{ ts: string; body: string }> = [];
		const { database, sessionPort, repo } = await brokerPortHarness(
			directory,
			(request) => {
				const turn = hostTurnHandlers(realAnswer, outputs, started, (answer) => {
					transcript.push({ ts: new Date().toISOString(), body: answer });
					// A late [SILENT] from an async result arrives after recording the answer
					setTimeout(() => {
						transcript.push({ ts: new Date().toISOString(), body: "[SILENT]" });
					}, 50);
				});
				if (request.operation === "session.last_assistant")
					// Returns the latest (the [SILENT]) to show the hazard the fix prevents
					return { ok: true, page: { items: [transcript.at(-1)?.body ?? ""], complete: true } };
				return turn(request);
			},
			(args) => {
				if (args.includes("query") && args.includes("turn.result")) {
					const atIdx = args.indexOf("--json-input");
					if (atIdx < 0) return undefined;
					const inputIdx = atIdx + 1;
					if (inputIdx >= args.length) return undefined;
					let input: { clientRef?: string } | undefined;
					try {
						input = JSON.parse(args[inputIdx]);
					} catch {
						return undefined;
					}
					const clientRef = input?.clientRef;
					if (!clientRef) return undefined;
					const answer = outputs.get(clientRef);
					const startTime = started.get(clientRef);
					if (answer === undefined || startTime === undefined) return undefined;
					return {
						exitCode: 0,
						stdout: JSON.stringify({
							ok: true,
							result: {
								kind: "prompt",
								clientRef,
								status: "terminal_ok",
								startedAt: startTime,
								terminalAt: Date.now(),
								outcome: { reason: "end_turn", provenance: "agent" },
								content: {
									version: 1,
									type: "text",
									text: answer,
									truncated: false,
									byteLength: Buffer.byteLength(answer),
								},
								receiptState: "present",
							},
						}),
						stderr: "",
					};
				}
				if (!args.includes("transcript.list")) return undefined;
				return {
					exitCode: 0,
					stdout: JSON.stringify({
						page: {
							items: transcript.map((row) => ({ role: "assistant", ts: row.ts, body: row.body })),
							complete: true,
						},
					}),
					stderr: "",
				};
			},
		);
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "keep-answer-watch",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["keep.answer"],
			burstPolicy: "serialize",
			reportTo: target,
		});
		pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			repo,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
		});

		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "keep.answer", { run: 1 });

		// Allow time for the late [SILENT] to arrive
		await new Promise((resolve) => setTimeout(resolve, 100));

		// The event must be authored with the real answer, not the late [SILENT]
		expect(database.monitors.authoredOutput(eventId)).toBe(realAnswer);
		expect(database.monitors.eventGet(eventId)?.stage).toBe("authored_no_delivery");
	} finally {
		pipeline?.dispose();
		await pipeline?.drain();
		await rm(directory, { recursive: true, force: true });
	}
});

/**
 * snowflake CHANGES_REQUESTED on #842 (5e3d7ffcc): a PRIOR event's spoken
 * response poisons the transcript for the CURRENT turn — its row sits
 * timestamped after the current event's fired_at (and within the 2s slack
 * before startedAt) while the current op's own turn.result holds the valid
 * answer for the CURRENT event, and the last assistant message is `[SILENT]`.
 * The timestamp-floor + preferSpoken transcript read picked the prior row,
 * the consumer then rejected the mismatched event IDs and retried. The
 * op-ref-bound read must author the current event with its own answer on the
 * first dispatch — through the REAL request path, no store-method faking.
 */
test("a prior event's spoken response is never selected for the current silent turn", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-prior-spoken-"));
	let pipeline: MonitorPropagator | undefined;
	try {
		const priorAnswer = "Prior answer for the EARLIER event only.";
		const realAnswer = "Current answer for THIS event.";
		const { outputs, started } = turnFixture();
		// The current event's own valid answer, stored under its op-ref: the
		// invocation-owned turn.result body the host reports.
		const transcript: Array<{ ts: string; body: string }> = [];
		const { database, sessionPort, repo } = await brokerPortHarness(
			directory,
			(request) => {
				const turn = hostTurnHandlers(realAnswer, outputs, started, (answer) => {
					const entries = JSON.parse(answer) as Array<{ eventId: string }>;
					// The host transcript keeps the PRIOR event's spoken answer as its
					// newest row, stamped after the current event's fired_at and inside
					// the 2s slack: the timestamp-floor heuristic would pick it.
					const priorEntry = JSON.stringify([{ eventId: entries[0]?.eventId, note: priorAnswer }]);
					transcript.push({ ts: new Date(Date.now() + 1500).toISOString(), body: priorEntry });
					transcript.push({ ts: new Date(Date.now() + 1600).toISOString(), body: "[SILENT]" });
				});
				if (request.operation === "session.last_assistant")
					// The session's last assistant message is the wake-up's marker.
					return { ok: true, page: { items: ["[SILENT]"], complete: true } };
				return turn(request);
			},
			(args) => {
				// Pre-#809-fix sources have no op-scoped CLI read: 3c5885f22 selects a
				// transcript row (its fetchAssistantSince picks the prior event's
				// spoken answer via timestamp-floor + preferSpoken) and 031a61f7b falls
				// back to requestResult.assistant (the last message, the `[SILENT]`
				// marker). Both must fail this test, so the op-owned CLI read answers
				// only on sources that actually perform it.
				if (args.includes("transcript.list")) {
					// The timestamp-floor + preferSpoken transcript read sees the
					// poisoned transcript: the prior event's spoken answer is the
					// newest spoken row, followed by the `[SILENT]` marker.
					return {
						exitCode: 0,
						stdout: JSON.stringify({
							page: {
								items: transcript.map((row) => ({ role: "assistant", ts: row.ts, body: row.body })),
								complete: true,
							},
						}),
						stderr: "",
					};
				}
				// The CLI recovery transport cannot serve `turn.result` (hosts whose
				// durable-result surface is relay-only): the op-owned read must ride
				// the request's own relay. A source without the relay-borne read
				// (031a61f7b) falls back to `requestResult.assistant` — the last
				// message, the `[SILENT]` marker — and fails this test; 3c5885f22 has
				// no op-scoped read at all and picks the poisoned transcript row.
				if (args.includes("query") && args.includes("turn.result"))
					throw new Error("CLI turn.result surface unavailable");
				return undefined;
			},
		);
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "prior-spoken-watch",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["prior.spoken"],
			burstPolicy: "serialize",
			reportTo: target,
		});
		pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			repo,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			notifyInbound: async (key) => {
				notifications.push(key);
			},
		});

		notifications.length = 0;
		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "prior.spoken", { run: 1 });

		// The transcript poisoning actually happened on the request path.
		expect(transcript.some((row) => row.body.includes(priorAnswer))).toBe(true);
		expect(transcript.at(-1)?.body).toBe("[SILENT]");
		// The current event is authored with ITS OWN answer on the first dispatch —
		// never the prior event's spoken response (a mismatched-ID retry would
		// surface as authoring_response_invalid, not authorship).
		expect(database.monitors.authoredOutput(eventId)).toBe(realAnswer);
		expect(database.monitors.failure(eventId)).toBeUndefined();
		expect(database.monitors.eventGet(eventId)?.stage).toBe("authored_no_delivery");
		const report = database.inbox.pendingOldest(originKey(target));
		expect(report?.body.endsWith(realAnswer)).toBe(true);
		expect(notifications).toEqual([originKey(target)]);
	} finally {
		pipeline?.dispose();
		await pipeline?.drain();
		await rm(directory, { recursive: true, force: true });
	}
});
