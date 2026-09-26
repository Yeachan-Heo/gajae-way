import { afterEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GjcCliError } from "@gajae-gateway/subsession";
import { PersonaSessionManager, personaTurnOpRef } from "../src/orchestrator/persona-session";
import { formatFailureNotice } from "../src/orchestrator/rebind";
import type { TailAttachInput } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, ScriptedSessionPort, steerRefused } from "./session-port.fake";

let home = "";
let database: GatewayDatabase | undefined;
let manager: PersonaSessionManager | undefined;
let latestOpRef = "";

afterEach(async () => {
	await manager?.stop();
	database?.close();
	manager = undefined;
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
	latestOpRef = "";
});

const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "persona" } as const;
const KEY = "loopback/loopback/persona";

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 200; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

function enqueue(messageId: string, body: string, source: "platform" | "lane_report" = "platform"): void {
	const accepted = database?.inboundEnqueue({
		messageId,
		originKey: KEY,
		originRefJson: JSON.stringify(ORIGIN),
		body,
		source,
	});
	expect(accepted).toBe(true);
}

const fixtureBindings = new WeakMap<ScriptedSessionPort, Pick<ScriptedSessionPort, "bind" | "resume">>();

/** Reattach reopened databases without retaining wrappers over a closed database. */
function registerFixtureBindings(port: ScriptedSessionPort): void {
	const original = fixtureBindings.get(port) ?? { bind: port.bind.bind(port), resume: port.resume.bind(port) };
	fixtureBindings.set(port, original);
	port.bind = original.bind;
	port.resume = original.resume;
	attachTestBrokerOwnership(database!, port, join(home, "agent"));
}

async function harness(
	port: ScriptedSessionPort,
	hooks: {
		terminal?: (text: string) => void;
		retired?: () => void;
		released?: (opRef: string) => void;
		failure?: (message: string) => void;
		failureError?: (error: Error) => void;
	} = {},
	log?: (line: string) => void,
	extra: { brokerGeneration?: () => number } = {},
) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-persona-session-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	registerFixtureBindings(port);
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		...(log ? { log } : {}),
		...extra,
		onTurnStart: ({ trigger, turn }) => {
			latestOpRef = turn.opRef;
			return {
				text: trigger.body,
				onTerminal: ({ text }) => hooks.terminal?.(text),
				onFailure: ({ error }) => {
					hooks.failure?.(error.message);
					hooks.failureError?.(error);
				},
				onRetired: () => hooks.retired?.(),
				onReleased: ({ turn: released }) => hooks.released?.(released.opRef),
			};
		},
	});
}

test("actor immediately dispatches durable inbound with one deterministic caller op-ref, then completes on tail terminal", async () => {
	const port = new ScriptedSessionPort({
		onSend: (input, scripted) => scripted.complete(input.opRef, "persona reply"),
	});
	const terminal: string[] = [];
	await harness(port, { terminal: (text) => terminal.push(text) });
	enqueue("m-1", "hello");
	await manager?.notifyInbound(KEY);
	await eventually(() => terminal.length === 1, "tail terminal did not reach lifecycle");

	expect(port.sends).toHaveLength(1);
	const send = port.sends[0]!;
	expect(send.text).toBe("hello");
	expect(send.opRef).toBe(latestOpRef);
	expect(send.opRef).toMatch(/^gw-p-[0-9a-f]{32}$/);
	expect(terminal).toEqual(["persona reply"]);
	expect(database?.inboundPendingCount(KEY)).toBe(0);
	expect(database?.inboundTurnRow(latestOpRef)).toMatchObject({ state: "done", turn_state: "done" });
});

for (const outcome of ["complete", "incomplete", "in_flight"] as const)
	test(`recovery does not require a tail for proven terminal status (${outcome})`, async () => {
		const port = new ScriptedSessionPort();
		await harness(port);
		enqueue("recover-without-tail", "execute only once");
		await manager!.notifyInbound(KEY);
		const send = port.sends[0]!;
		await manager!.stop();
		if (outcome !== "in_flight") port.seedOperation(send.opRef, send.sessionId, "terminal_ok", "original result");
		if (outcome === "incomplete") {
			const report = await port.status(send);
			port.setWorkerOutputFixture(send.opRef, {
				exitCode: 0,
				stderr: "",
				stdout: JSON.stringify({
					ok: true,
					result: {
						...report.status,
						kind: "prompt",
						clientRef: send.opRef,
						content: {
							version: 1,
							type: "text",
							text: "partial",
							byteLength: 7,
							truncated: true,
						},
					},
				}),
			});
		}
		let attaches = 0;
		port.attachTail = async () => {
			attaches++;
			throw new Error("protocol_error: broker tail timeout");
		};
		const terminals: string[] = [];
		const logs: string[] = [];
		manager = new PersonaSessionManager({
			database: database!,
			port,
			instanceId: "instance-test",
			repo: join(home, "workspace"),
			log: (line) => logs.push(line),
			onTurnStart: ({ trigger }) => ({
				text: trigger.body,
				onTerminal: ({ text }) => {
					terminals.push(text);
				},
			}),
		});
		await manager.recover();
		await manager.reconcile(KEY);
		expect(port.sends).toHaveLength(1);
		if (outcome === "in_flight") {
			expect(attaches).toBeGreaterThan(0);
			expect(port.workerOutputReads).toHaveLength(0);
			expect(logs.some((line) => line.includes("protocol_error"))).toBe(true);
		} else {
			expect(attaches).toBe(0);
			expect(port.workerOutputReads.length).toBeGreaterThan(0);
			expect(
				port.workerOutputReads.every((read) => read.opRef === send.opRef && read.sessionId === send.sessionId),
			).toBe(true);
		}
		expect(terminals).toEqual(outcome === "complete" ? ["original result"] : []);
		expect(database!.inboundTurnRow(send.opRef)?.turn_state).toBe(outcome === "complete" ? "done" : "accepted");
		if (outcome === "incomplete") expect(logs.some((line) => line.includes("reason=incomplete_body"))).toBe(true);
	});

for (const answer of ["owned original answer", ""])
	test(`terminal recovery reads only the accepted original operation (empty=${answer === ""})`, async () => {
		const port = new ScriptedSessionPort();
		const terminal: string[] = [];
		await harness(port, { terminal: (text) => terminal.push(text) });
		enqueue("original", "execute once");
		await manager!.notifyInbound(KEY);
		const send = port.sends[0]!;
		port.fetchAssistantSince = async () => {
			throw new Error("transcript.list returned no page items");
		};
		let latestReads = 0;
		port.fetchLastAssistant = async () => {
			latestReads++;
			return { text: "WRONG_LATEST_OPERATION", pages: 1, complete: true };
		};
		port.seedOperation(send.opRef, send.sessionId, "terminal_ok", answer);
		port.seedOperation("unrelated-newer-op", send.sessionId, "terminal_ok", "WRONG_LATEST_OPERATION");
		await manager!.reconcile(KEY);
		await eventually(() => terminal.length === 1, "original output was not recovered");
		await manager!.reconcile(KEY);
		expect(terminal).toEqual([answer]);
		expect(latestReads).toBe(0);
		expect(port.workerOutputReads).toHaveLength(1);
		expect(port.workerOutputReads[0]).toMatchObject({ sessionId: send.sessionId, opRef: send.opRef });
		expect(port.workerOutputReads[0]!.notBeforeMs).toBeGreaterThanOrEqual(
			Date.parse(database!.inboundTurnDispatchedAt(send.opRef)!),
		);
		expect(database!.inboundTurnRow(send.opRef)?.turn_state).toBe("done");
		expect(port.sends).toHaveLength(1);
	});

for (const evidence of ["missing", "truncated", "mismatch"] as const)
	test(`terminal recovery holds ${evidence} original content without substituting the latest answer`, async () => {
		const port = new ScriptedSessionPort();
		const terminal: string[] = [];
		const logs: string[] = [];
		await harness(port, { terminal: (text) => terminal.push(text) }, (line) => logs.push(line));
		enqueue("held-original", "execute once");
		await manager!.notifyInbound(KEY);
		const send = port.sends[0]!;
		port.seedOperation(send.opRef, send.sessionId, "terminal_ok", "owned answer");
		const status = (await port.status(send)).status;
		port.setWorkerOutputFixture(send.opRef, {
			exitCode: 0,
			stderr: "",
			stdout: JSON.stringify({
				ok: true,
				result: {
					...status,
					kind: "prompt",
					clientRef: evidence === "mismatch" ? "wrong-operation" : send.opRef,
					...(evidence === "missing"
						? {}
						: {
								content: {
									version: 1,
									type: "text",
									text: "WRONG_LATEST_OPERATION",
									byteLength: new TextEncoder().encode("WRONG_LATEST_OPERATION").length,
									truncated: evidence === "truncated",
								},
							}),
				},
			}),
		});
		let alternateReads = 0;
		port.fetchLastAssistant = async () => {
			alternateReads++;
			return { text: "WRONG_LATEST_OPERATION", pages: 1, complete: true };
		};
		port.fetchAssistantSince = port.fetchLastAssistant;
		await manager!.reconcile(KEY);
		await eventually(() => port.workerOutputReads.length > 0, "original result was not queried");
		await manager!.reconcile(KEY);
		expect(terminal).toEqual([]);
		expect(alternateReads).toBe(0);
		const reason =
			evidence === "missing" ? "output_pending" : evidence === "truncated" ? "incomplete_body" : "identity_mismatch";
		expect(logs.some((line) => line.includes(`reason=${reason}`))).toBe(true);
		expect(database!.inboundTurnRow(send.opRef)?.turn_state).toBe("accepted");
		expect(port.sends).toHaveLength(1);
	});

test("stop fences a pending original-result callback before delivery or database mutation", async () => {
	const port = new ScriptedSessionPort();
	const terminal: string[] = [];
	await harness(port, { terminal: (text) => terminal.push(text) });
	enqueue("stop-original", "execute once");
	await manager!.notifyInbound(KEY);
	const send = port.sends[0]!;
	const fetch = port.fetchWorkerOutput.bind(port);
	let release!: () => void;
	const blocked = new Promise<void>((resolve) => {
		release = resolve;
	});
	let entered = false;
	port.fetchWorkerOutput = async (input) => {
		// Capture proven evidence before stop, so the actor must perform its own fence.
		const result = await fetch(input);
		entered = true;
		await blocked;
		return result;
	};
	port.seedOperation(send.opRef, send.sessionId, "terminal_ok", "late original answer");
	await manager!.reconcile(KEY);
	await eventually(() => entered, "original result read did not begin");
	const before = database!.inboundTurnRow(send.opRef);
	const stopping = manager!.stop();
	release();
	await stopping;
	expect(terminal).toEqual([]);
	expect(database!.inboundTurnRow(send.opRef)).toEqual(before);
	expect(port.sends).toHaveLength(1);
});

test("a message admitted while a persistent turn is running becomes an operator-gated steer", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("m-1", "first");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "initial persistent send did not start");
	expect(port.sends).toHaveLength(1);
	enqueue("m-2", "correction");
	await manager?.notifyInbound(KEY);

	expect(port.steers).toHaveLength(1);
	expect(port.steers[0]).toMatchObject({ sessionId: port.sends[0]!.sessionId });
	expect(port.steers[0]!.text.endsWith("\ncorrection")).toBe(true);
	expect(database?.inboundTurnRows(latestOpRef)).toEqual(
		expect.arrayContaining([
			expect.objectContaining({
				message_id: "m-2",
				turn_role: "steer",
				turn_state: "done",
				turn_op_ref: latestOpRef,
			}),
		]),
	);
	port.complete(port.sends[0]!.opRef, "done");
	await eventually(
		() => database?.inboundTurnRow(latestOpRef)?.turn_state === "done",
		"accepted turn did not reconcile terminal",
	);
});

// gajae-code redacts a post-start failure's message to one fixed sentence, so
// the runtime code is the entire diagnosis. Six lost turns on one host produced
// six identical code-free lines (#244); the code is what tells them apart.
test("a post-start prompt failure delivers the runtime's code and logs its bounded classifiers", async () => {
	const port = new ScriptedSessionPort();
	const notices: string[] = [];
	const logs: string[] = [];
	await harness(port, { failureError: (error) => notices.push(formatFailureNotice(error)) }, (line) => logs.push(line));
	enqueue("post-start-failure", "work that failed after it started");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	port.fail(first.opRef, "Agent run failed after execution started.", {
		code: "prompt_failed",
		outcome: { kind: "failed", phase: "post_start", category: "agent_runtime", provenance: "agent_failed" },
	});
	await eventually(() => notices.length === 1, "post-start failure did not reach the lifecycle");
	expect(notices[0]).toBe("[turn failed] prompt_failed: Agent run failed after execution started.");
	// `prompt_failed` is not rebindable: a new session does not fix a runtime fault.
	expect(notices[0]).not.toContain("/new");
	expect(logs.find((line) => line.startsWith("terminal_failure "))).toContain(
		"code=prompt_failed provider_code=unknown phase=post_start category=agent_runtime provenance=agent_failed",
	);
});

// #210: a bash call blocked on an interactive auth prompt (`op whoami`) timed
// out and took the whole turn with it. The notice said only the redacted
// sentence, and the reply the persona had already written was discarded.
test("a turn that fails with a tool still running names the tool and hands over the answer it already wrote", async () => {
	let now = 1_000_000;
	const port = new ScriptedSessionPort();
	const failures: { notice: string; recovered?: string }[] = [];
	const logs: string[] = [];
	const frames: string[] = [];
	home = await mkdtemp(join(tmpdir(), "gajaeway-persona-session-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	registerFixtureBindings(port);
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		log: (line) => logs.push(line),
		now: () => now,
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onFrame: ({ frame }) => {
				frames.push(frame.rawKind);
			},
			onFailure: ({ error, recoveredText }) => {
				failures.push({ notice: formatFailureNotice(error), ...(recoveredText ? { recovered: recoveredText } : {}) });
			},
		}),
	});
	const probes: number[] = [];
	port.fetchAssistantSince = async (input) => {
		probes.push(input.notBeforeMs);
		return { text: "the answer written before the tool hung", pages: 1, complete: true };
	};
	enqueue("tool-timeout", "check the 1password session");
	await manager.notifyInbound(KEY);
	const first = port.sends[0]!;
	port.emitTool(first.sessionId, { toolName: "bash", args: { command: "op whoami" } });
	await eventually(() => frames.includes("tool_execution_start"), "tool start did not reach the actor");
	now += 300_000;
	port.fail(first.opRef, "Agent run failed after execution started.", { code: "prompt_failed" });
	await eventually(() => failures.length === 1, "failure did not reach the lifecycle");

	expect(failures[0]!.notice).toBe(
		"[turn failed] prompt_failed: Agent run failed after execution started. (a bash call had been running for 300s without finishing)",
	);
	expect(failures[0]!.recovered).toBe("the answer written before the tool hung");
	expect(probes).toHaveLength(1);
	expect(logs.find((line) => line.startsWith("terminal_failure "))).toContain(
		"code=prompt_failed provider_code=unknown phase=unknown category=unknown provenance=unknown open_tool=bash open_tool_elapsed_ms=300000",
	);
	expect(logs.some((line) => line.startsWith("failed_turn_answer_recovered "))).toBe(true);
});

test("a failed turn whose answer already reached the tail neither re-reads the transcript nor blames a finished tool", async () => {
	const port = new ScriptedSessionPort();
	const failures: { notice: string; recovered?: string }[] = [];
	home = await mkdtemp(join(tmpdir(), "gajaeway-persona-session-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	registerFixtureBindings(port);
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onFailure: ({ error, recoveredText }) => {
				failures.push({ notice: formatFailureNotice(error), ...(recoveredText ? { recovered: recoveredText } : {}) });
			},
		}),
	});
	let probes = 0;
	port.fetchAssistantSince = async () => {
		probes++;
		return { text: "must not be re-delivered", pages: 1, complete: true };
	};
	enqueue("visible-then-fail", "work");
	await manager.notifyInbound(KEY);
	const first = port.sends[0]!;
	port.emitTool(first.sessionId, { toolName: "bash" });
	port.emitToolEnd(first.sessionId, "bash");
	port.emitAssistant(first.sessionId, "already shown", "shown", first.opRef);
	port.fail(first.opRef, "Agent run failed after execution started.", { code: "prompt_failed" });
	await eventually(() => failures.length === 1, "failure did not reach the lifecycle");
	expect(failures[0]).toEqual({ notice: "[turn failed] prompt_failed: Agent run failed after execution started." });
	expect(probes).toBe(0);
});

test("a rebindable post-start code keeps its /new hint, and a codeless failure still has a diagnosis", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const notices: string[] = [];
	await harness(port, { failureError: (error) => notices.push(formatFailureNotice(error)) });
	enqueue("rebindable-failure", "first");
	await manager!.notifyInbound(KEY);
	port.fail(port.sends[0]!.opRef, "Agent run failed after execution started.", { code: "spawn_failed" });
	await eventually(() => notices.length === 1, "rebindable failure did not reach the lifecycle");
	expect(notices[0]).toBe(
		"[turn failed] spawn_failed: Agent run failed after execution started. Send /new to rebind this conversation.",
	);

	enqueue("codeless-failure", "second");
	await manager!.notifyInbound(KEY);
	const second = port.sends[1]!;
	// A child killed mid-write reports neither code nor message.
	port.fail(second.opRef, "");
	await eventually(() => notices.length === 2, "codeless failure did not reach the lifecycle");
	expect(notices[1]).toBe("[turn failed] session status failed");
});

test("failed notice must persist before reset completion and may retry without replaying the prompt", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	let notices = 0;
	await harness(port, {
		failure: () => {
			notices++;
			if (notices === 1) throw new Error("notice persistence interrupted");
		},
	});
	enqueue("failed-notice", "work that must not run twice");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	port.setFailedTurnEvidence(first.sessionId, "unsupported_input_status");
	port.fail(first.opRef);
	await eventually(() => notices === 1, "notice callback did not run");
	expect(database!.inboundTurnRow(first.opRef)?.turn_state).toBe("accepted");
	expect(database!.getSessionRecord(KEY)?.epoch).toBe(0);
	await manager!.tick(KEY);
	await eventually(() => manager!.state(KEY) === "idle", "retrying notice did not settle failure");
	expect(notices).toBe(2);
	expect(port.sends).toHaveLength(1);
	expect(database!.inboundTurnRow(first.opRef)?.turn_state).toBe("done");
	expect(database!.getSessionRecord(KEY)?.epoch).toBe(1);
});

test("reset consumes only the failed trigger context, preserving unrelated unread input", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	for (const messageId of ["failed-context", "unrelated-context"]) {
		database!.contextRecord({ messageId, originKey: KEY, body: messageId, receivedAt: new Date().toISOString() });
	}
	enqueue("failed-context", "failed-context");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	port.setFailedTurnEvidence(first.sessionId, "unsupported_input_status");
	port.fail(first.opRef);
	await eventually(() => manager!.state(KEY) === "idle", "failed trigger did not settle");
	expect(database!.contextDiagnostics(KEY).unread).toBe(1);
	expect(database!.contextWindow(KEY, "later-message").rows.map((row) => row.message_id)).toEqual([
		"unrelated-context",
	]);
});

// Exact failure recovery completes the trigger and resets only the NEXT session.
for (const reason of ["unsupported_input_status", "context_exhausted"] as const)
	test(`${reason} resets next without replay, including tools and visible output`, async () => {
		const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
		const failures: string[] = [];
		const released: string[] = [];
		await harness(port, {
			failure: (message) => {
				expect(database!.getSessionRecord(KEY)?.epoch).toBe(0);
				failures.push(message);
			},
			released: (opRef) => released.push(opRef),
		});
		enqueue("failed", "original work");
		await manager!.notifyInbound(KEY);
		const first = port.sends[0]!;
		port.emitTool(first.sessionId);
		port.emitToolEnd(first.sessionId, "tool");
		port.emitAssistant(first.sessionId, "partial answer", "partial", first.opRef);
		port.setFailedTurnEvidence(first.sessionId, reason);
		port.fail(first.opRef, "failed once");
		await eventually(() => manager!.state(KEY) === "idle", "failed turn did not settle");
		expect(port.sends).toHaveLength(1);
		expect(database!.inboundTurnRow(first.opRef)).toMatchObject({ state: "done", turn_state: "done" });
		expect(failures).toEqual(["failed once"]);
		expect(database!.getSessionRecord(KEY)?.epoch).toBe(1);
		expect(released).toEqual([]);
		port.fail(first.opRef, "duplicate callback");
		await manager!.tick(KEY);
		expect(failures).toHaveLength(1);
		enqueue("next", "new user message");
		await manager!.notifyInbound(KEY);
		expect(port.sends.map((send) => send.text)).toEqual(["original work", "new user message"]);
		expect(port.sends[1]!.sessionId).toBe("session-e1");
		expect(port.sends[1]!.opRef).toBe(personaTurnOpRef("instance-test", KEY, 1, "next"));
	});

for (const restart of [false, true])
	test(`origin reset cap stops repeated fresh-session failures (restart=${restart})`, async () => {
		const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
		const failures: string[] = [];
		await harness(port, { failure: (message) => failures.push(message) });
		enqueue("first", "first failed message");
		await manager!.notifyInbound(KEY);
		const first = port.sends[0]!;
		port.setFailedTurnEvidence(first.sessionId, "unsupported_input_status");
		port.fail(first.opRef);
		await eventually(() => manager!.state(KEY) === "idle", "first reset did not settle");
		if (restart) {
			await manager!.stop();
			database!.close();
			database = await GatewayDatabase.open(join(home, "gateway.db"));
			registerFixtureBindings(port);
			manager = new PersonaSessionManager({
				database,
				port,
				instanceId: "instance-test",
				repo: join(home, "workspace"),
				onTurnStart: ({ trigger }) => ({
					text: trigger.body,
					onFailure: ({ error }) => {
						failures.push(error.message);
					},
				}),
			});
			await manager.recover();
			expect(port.sends).toHaveLength(1);
		}
		enqueue("second", "different failed message");
		await manager!.notifyInbound(KEY);
		const second = port.sends[1]!;
		port.setFailedTurnEvidence(second.sessionId, "unsupported_input_status");
		port.fail(second.opRef, "second failure");
		await eventually(() => manager!.state(KEY) === "idle", "capped failure did not settle");
		expect(database!.getSessionRecord(KEY)?.epoch).toBe(1);
		expect(port.sends).toHaveLength(2);
		expect(failures[1]).toBe("second failure");
		expect(database!.inboundTurnRow(second.opRef)?.turn_state).toBe("done");
	});

for (const held of [false, true])
	test(`reset preserves steer attribution and unrelated pending input (held=${held})`, async () => {
		const port = new ScriptedSessionPort({
			onBind: (input) => `session-e${input.epoch}`,
			onSteer: () => {
				if (held) throw new Error("unknown transport outcome");
			},
		});
		await harness(port);
		enqueue("failed", "original");
		await manager!.notifyInbound(KEY);
		const first = port.sends[0]!;
		enqueue("steer", "additional accepted or uncertain work");
		await manager!.notifyInbound(KEY);
		enqueue("pending", "unrelated next input");
		port.setFailedTurnEvidence(first.sessionId, "unsupported_input_status");
		port.fail(first.opRef);
		await eventually(() => port.sends.length === 2, "pending input was not dispatched after reset");
		await manager!.tick(KEY);
		expect(port.sends.map((send) => send.text)).toEqual(["original", "unrelated next input"]);
		expect(port.sends[1]!.sessionId).toBe("session-e1");
		expect(database!.inboundTurnRows(first.opRef).find((row) => row.message_id === "steer")).toMatchObject({
			turn_role: "steer",
			turn_op_ref: first.opRef,
			turn_state: held ? "bound" : "done",
		});
		if (held) expect(database!.inboundSteersHeld(first.opRef).map((row) => row.message_id)).toEqual(["steer"]);
	});

for (const evidence of ["missing", "throws"] as const)
	test(`429 never resets with ${evidence} evidence`, async () => {
		class MissingEvidencePort extends ScriptedSessionPort {
			override async failedTurnEvidence(input: Parameters<ScriptedSessionPort["failedTurnEvidence"]>[0]) {
				if (evidence === "throws") throw new Error("unavailable diagnostics");
				return super.failedTurnEvidence(input);
			}
		}
		const port = new MissingEvidencePort();
		const failures: string[] = [];
		await harness(port, { failure: (message) => failures.push(message) });
		enqueue("rate-limit", "original");
		await manager!.notifyInbound(KEY);
		const first = port.sends[0]!;
		port.fail(first.opRef, "429 rate limited");
		await eventually(() => manager!.state(KEY) === "idle", "ordinary failure did not settle");
		expect(database!.getSessionRecord(KEY)?.epoch).toBe(0);
		expect(port.sends).toHaveLength(1);
		expect(failures).toEqual(["429 rate limited"]);
	});

for (const missing of ["start", "terminal", "nan", "reversed", "future", "op-ref"] as const)
	test(`reset rejects untrustworthy ${missing} status coordinates`, async () => {
		class InvalidStatusPort extends ScriptedSessionPort {
			override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
				const report = await super.status(input);
				if (report.status.status !== "failed") return report;
				return {
					...report,
					operationRef: missing === "op-ref" ? "other-op" : report.operationRef,
					status: {
						...report.status,
						startedAt: missing === "start" ? undefined : missing === "nan" ? Number.NaN : report.status.startedAt,
						terminalAt:
							missing === "terminal"
								? undefined
								: missing === "reversed"
									? 1
									: missing === "future"
										? Date.now() + 60_000
										: report.status.terminalAt,
					},
				};
			}
		}
		const port = new InvalidStatusPort();
		await harness(port);
		enqueue("invalid-status", "fail closed");
		await manager!.notifyInbound(KEY);
		const first = port.sends[0]!;
		port.setFailedTurnEvidence(first.sessionId, "unsupported_input_status");
		port.fail(first.opRef);
		await eventually(() => manager!.state(KEY) === "idle", "failure did not settle");
		expect(port.failureEvidenceProbes).toEqual([]);
		expect(database!.getSessionRecord(KEY)?.epoch).toBe(0);
	});

test("retired exact failure never resets the replacement binding", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	await harness(port);
	enqueue("old", "old");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	await manager!.reset(KEY, JSON.stringify(ORIGIN));
	enqueue("current", "current");
	await manager!.notifyInbound(KEY);
	port.setFailedTurnEvidence(first.sessionId, "unsupported_input_status");
	port.fail(first.opRef);
	await manager!.tick(KEY);
	expect(port.failureEvidenceProbes).toEqual([]);
	expect(port.sends.map((send) => send.text)).toEqual(["old", "current"]);
	expect(database!.getSessionRecord(KEY)?.epoch).toBe(1);
});

test("/new with nothing in flight ends the previous session's host at once", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const logs: string[] = [];
	await harness(port, undefined, (line) => logs.push(line));
	enqueue("first", "first");
	await manager!.notifyInbound(KEY);
	port.complete(port.sends[0]!.opRef, "answered");
	await manager!.tick(KEY);
	expect(port.closes).toEqual([]);
	// Idle now. /new rotates the epoch; the old host has no reason to live.
	await manager!.reset(KEY, JSON.stringify(ORIGIN));
	await manager!.tick(KEY);
	// The just-completed turn is still tracked as retired at /new time, so the
	// host is ended by that turn's reconcile rather than by reset() itself -
	// either way, exactly once, and only after nothing can still need it.
	expect(logs.filter((l) => l.includes("retired_session_host") && l.includes("outcome=terminated"))).toHaveLength(1);
	expect(port.closes.map((c) => c.sessionId)).toEqual(["session-e0"]);
	// The replacement is untouched and answers the next message.
	enqueue("second", "second");
	await manager!.notifyInbound(KEY);
	expect(port.sends.at(-1)?.sessionId).toBe("session-e1");
	expect(port.closes).toHaveLength(1);
});

test("/new with a turn in flight ends the old host only after that turn is reconciled", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const logs2: string[] = [];
	await harness(port, undefined, (line) => logs2.push(line));
	enqueue("slow", "slow");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	await manager!.reset(KEY, JSON.stringify(ORIGIN));
	await manager!.tick(KEY);
	// The retired turn may still deliver its answer: the host must stay up for it.
	expect(port.closes).toEqual([]);
	port.complete(first.opRef, "late answer");
	for (let i = 0; i < 4; i++) await manager!.tick(KEY);
	expect(port.closes.map((c) => c.sessionId)).toEqual(["session-e0"]);
});

test("a session the broker disowned mid-send has its host ended after the rebind", async () => {
	class DisowningPort extends ScriptedSessionPort {
		override async send(input: Parameters<ScriptedSessionPort["send"]>[0]) {
			if (input.sessionId === "session-e0")
				throw Object.assign(new Error("session_unavailable"), { code: "session_unavailable" });
			return await super.send(input);
		}
	}
	const port = new DisowningPort({
		onBind: (input) => `session-e${input.epoch}`,
		onSend: (input, scripted) => scripted.complete(input.opRef, "answered on the replacement"),
	});
	const logs3: string[] = [];
	await harness(port, undefined, (line) => logs3.push(line));
	enqueue("gone", "gone");
	await manager!.notifyInbound(KEY);
	for (let i = 0; i < 4; i++) await manager!.tick(KEY);
	expect(logs3.some((l) => l.startsWith("persona_send_session_gone"))).toBe(true);
	expect(logs3.some((l) => l.includes("retired_session_host") && l.includes("reason=session_gone"))).toBe(true);
	// The broker said the old session is gone; its host is ended anyway (it can
	// be disowned and still running), and the message lands on the replacement.
	expect(port.closes.map((c) => c.sessionId)).toContain("session-e0");
	expect(port.sends.at(-1)?.sessionId).toBe("session-e1");
});

for (const exact of [false, true])
	test(`recovered failed terminal grace never resends its trigger (exact=${exact})`, async () => {
		const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
		await harness(port);
		enqueue("recover", "original");
		await manager!.notifyInbound(KEY);
		const first = port.sends[0]!;
		await manager!.stop();
		port.seedOperation(first.opRef, first.sessionId, "failed");
		let attaches = 0;
		port.attachTail = async () => {
			attaches++;
			throw new Error("protocol_error: terminal recovery must not attach");
		};
		if (exact) port.setFailedTurnEvidence(first.sessionId, "unsupported_input_status");
		manager = new PersonaSessionManager({
			database: database!,
			port,
			instanceId: "instance-test",
			repo: join(home, "workspace"),
		});
		await manager.recover();
		await manager.tick(KEY);
		await eventually(() => manager!.state(KEY) === "idle", "recovered terminal did not settle");
		expect(port.sends).toHaveLength(1);
		expect(attaches).toBe(0);
		expect(database!.inboundTurnRow(first.opRef)?.turn_state).toBe("done");
		expect(database!.getSessionRecord(KEY)?.epoch).toBe(exact ? 1 : 0);
	});

for (const resetBy of ["healthy", "new"] as const)
	test(`${resetBy} clears the consecutive origin cap`, async () => {
		const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
		await harness(port);
		enqueue("failed-1", "first");
		await manager!.notifyInbound(KEY);
		port.setFailedTurnEvidence(port.sends[0]!.sessionId, "unsupported_input_status");
		port.fail(port.sends[0]!.opRef);
		await eventually(() => manager!.state(KEY) === "idle", "first reset incomplete");
		if (resetBy === "healthy") {
			enqueue("healthy", "healthy");
			await manager!.notifyInbound(KEY);
			port.complete(port.sends[1]!.opRef, "healthy answer");
			await eventually(() => manager!.state(KEY) === "idle", "healthy completion incomplete");
		} else await manager!.reset(KEY, JSON.stringify(ORIGIN));
		const before = database!.getSessionRecord(KEY)!.epoch;
		enqueue("failed-2", "second");
		await manager!.notifyInbound(KEY);
		const last = port.sends.at(-1)!;
		port.setFailedTurnEvidence(last.sessionId, "unsupported_input_status");
		port.fail(last.opRef);
		await eventually(() => manager!.state(KEY) === "idle", "second failure incomplete");
		expect(database!.getSessionRecord(KEY)?.epoch).toBe(before + 1);
	});

for (const stop of ["cancelled", "refusal"] as const)
	test(`${stop} terminal does not clear the origin reset cap`, async () => {
		class StopPort extends ScriptedSessionPort {
			override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
				const report = await super.status(input);
				return report.status.status === "terminal_ok"
					? { ...report, status: { ...report.status, outcome: { reason: stop } } }
					: report;
			}
		}
		const port = new StopPort({ onBind: (input) => `session-e${input.epoch}` });
		await harness(port);
		enqueue("first", "first");
		await manager!.notifyInbound(KEY);
		port.setFailedTurnEvidence(port.sends[0]!.sessionId, "unsupported_input_status");
		port.fail(port.sends[0]!.opRef);
		await eventually(() => manager!.state(KEY) === "idle", "initial failure incomplete");
		enqueue("stopped", "stopped");
		await manager!.notifyInbound(KEY);
		port.complete(port.sends[1]!.opRef, "stopped output");
		await eventually(() => manager!.state(KEY) === "idle", "stopped completion incomplete");
		enqueue("third", "third");
		await manager!.notifyInbound(KEY);
		port.setFailedTurnEvidence(port.sends[2]!.sessionId, "unsupported_input_status");
		port.fail(port.sends[2]!.opRef);
		await eventually(() => manager!.state(KEY) === "idle", "capped failure incomplete");
		expect(database!.getSessionRecord(KEY)?.epoch).toBe(1);
	});

test("broker generation change during evidence lookup fences reset", async () => {
	let generation = 0;
	class GenerationPort extends ScriptedSessionPort {
		override async failedTurnEvidence(input: Parameters<ScriptedSessionPort["failedTurnEvidence"]>[0]) {
			const evidence = await super.failedTurnEvidence(input);
			generation++;
			return evidence;
		}
	}
	const port = new GenerationPort();
	await harness(port, {}, undefined, { brokerGeneration: () => generation });
	enqueue("generation", "fenced");
	await manager!.notifyInbound(KEY);
	port.setFailedTurnEvidence(port.sends[0]!.sessionId, "unsupported_input_status");
	port.fail(port.sends[0]!.opRef);
	await eventually(() => manager!.state(KEY) === "idle", "failure incomplete");
	expect(database!.getSessionRecord(KEY)?.epoch).toBe(0);
});

for (const value of ["1", "garbage", "-1", ""])
	test(`malformed or spent origin cap ${JSON.stringify(value)} denies atomic reset`, async () => {
		const port = new ScriptedSessionPort();
		await harness(port);
		enqueue("cap", "original");
		await manager!.notifyInbound(KEY);
		const first = port.sends[0]!;
		const key = `failed-turn-reset-cap:${createHash("sha256")
			.update(JSON.stringify([KEY]))
			.digest("hex")}`;
		database!.metaSet(key, value);
		const before = database!.inboundTurnRow(first.opRef);
		expect(
			database!.inboundFailedTurnReset({
				originKey: KEY,
				epoch: 0,
				sessionId: first.sessionId,
				opRef: first.opRef,
				triggerMessageId: "cap",
			}),
		).toBeUndefined();
		expect(database!.inboundTurnRow(first.opRef)).toEqual(before);
		expect(database!.getSessionRecord(KEY)?.epoch).toBe(0);
	});

test("atomic reset rolls completion and markers back when epoch advancement fails", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("rollback", "original");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	const input = {
		originKey: KEY,
		epoch: 0,
		sessionId: first.sessionId,
		opRef: first.opRef,
		triggerMessageId: "rollback",
	};
	const before = database!.inboundTurnRow(first.opRef);
	const original = database!.rebindEpoch.bind(database!);
	database!.rebindEpoch = () => {
		throw new Error("injected transaction failure");
	};
	expect(() => database!.inboundFailedTurnReset(input)).toThrow("injected transaction failure");
	expect(database!.inboundTurnRow(first.opRef)).toEqual(before);
	expect(database!.getSessionRecord(KEY)?.epoch).toBe(0);
	database!.rebindEpoch = original;
	expect(database!.inboundFailedTurnReset(input)).toBe(1);
	expect(database!.inboundTurnRow(first.opRef)?.turn_state).toBe("done");
	expect(database!.inboundFailedTurnReset(input)).toBeUndefined();
});

test("bounded shutdown reconciliation leaves a nonterminal accepted turn durable", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("m-1", "still running");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "accepted turn did not start before shutdown");
	await manager?.drain(0);
	expect(manager?.state(KEY)).toBe("turn-running");
	expect(database?.inboundTurnRow(latestOpRef)).toMatchObject({ state: "pending", turn_state: "accepted" });
});

test("stop drains an admitted generation callback and fences queued and delayed tail callbacks", async () => {
	const port = new ScriptedSessionPort();
	let tailInput: TailAttachInput | undefined;
	const attach = port.attachTail.bind(port);
	port.attachTail = async (input) => {
		tailInput = input;
		return attach(input);
	};
	const terminal: string[] = [];
	const logs: string[] = [];
	await harness(port, { terminal: (text) => terminal.push(text) }, (line) => logs.push(line));
	enqueue("stop-generation", "keep accepted work durable");
	await manager!.notifyInbound(KEY);
	await manager!.drain(0);
	expect(logs.some((line) => line.startsWith("shutdown_hold "))).toBe(true);
	const before = database!.inboundTurnRow(latestOpRef);
	let release!: () => void;
	const blocked = new Promise<void>((resolve) => {
		release = resolve;
	});
	let entered!: () => void;
	const started = new Promise<void>((resolve) => {
		entered = resolve;
	});
	let statusCalls = 0;
	const status = port.status.bind(port);
	port.status = async (input) => {
		statusCalls++;
		entered();
		await blocked;
		return status(input);
	};
	const generation = manager!.onBrokerGeneration(1);
	await started;
	const queuedGeneration = manager!.onBrokerGeneration(2);
	const queuedTail = tailInput!.onStall?.({
		sessionId: port.sends[0]!.sessionId,
		brokerGeneration: 0,
		elapsedMs: 100_000,
	});
	let stopped = false;
	const stopping = manager!.stop().then(() => {
		stopped = true;
	});
	await Promise.resolve();
	expect(stopped).toBe(false);
	release();
	await Promise.all([generation, queuedGeneration, queuedTail, stopping]);
	expect(statusCalls).toBe(1);
	expect(database!.inboundTurnRow(latestOpRef)).toEqual(before);
	database!.close();
	database = undefined;
	const callbacks = tailInput!;
	await manager!.onBrokerGeneration(3);
	await manager!.tick(KEY);
	await manager!.reconcile(KEY);
	await callbacks.onStall?.({ sessionId: port.sends[0]!.sessionId, brokerGeneration: 0, elapsedMs: 100_000 });
	// A saved callback can outlive the tail handle and database.
	await callbacks.onFrame?.({
		kind: "message_end",
		rawKind: "message_end",
		payload: { role: "assistant", opRef: latestOpRef },
		assistantText: "late answer",
		steerEcho: false,
		idle: false,
	});
	await callbacks.onRelayLost?.({ sessionId: port.sends[0]!.sessionId, brokerGeneration: 0 });
	expect(statusCalls).toBe(1);
	expect(port.sends).toHaveLength(1);
	expect(port.steers).toHaveLength(0);
	expect(terminal).toEqual([]);
});

test("/new retires an accepted turn, fences its late output, and preserves turn recovery until terminal", async () => {
	const port = new ScriptedSessionPort();
	let retired = 0;
	const terminal: string[] = [];
	await harness(port, { retired: () => retired++, terminal: (text) => terminal.push(text) });
	enqueue("m-1", "old turn");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "accepted turn did not start before reset");
	const first = port.sends[0]!;
	await manager?.reset(KEY, JSON.stringify(ORIGIN));

	expect(retired).toBe(1);
	expect(database?.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(database?.inboundTurnRow(latestOpRef)).toMatchObject({ state: "pending", turn_state: "accepted" });
	port.complete(first.opRef, "stale output");
	await manager!.tick(KEY);
	await eventually(
		() => database?.inboundTurnRow(latestOpRef)?.turn_state === "done",
		"retired turn did not reconcile",
	);
	expect(terminal).toEqual([]);
});

/** The gjc 0.17.x envelope for a session the broker stopped serving, as the CLI parser surfaces it. */
function endpointStale(): GjcCliError {
	return new GjcCliError('gjc sdk session status reported failure: {"code":"endpoint_stale"}', 0, "", {
		code: "endpoint_stale",
		category: "unavailable",
		message: "The SDK endpoint is stale or unavailable.",
	});
}

/** A broker restart dropped this session: every status read fails endpoint_stale and the host is gone. */
class DroppedSessionPort extends ScriptedSessionPort {
	readonly dropped = new Set<string>();
	override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
		if (this.dropped.has(input.sessionId)) throw endpointStale();
		return await super.status(input);
	}
	override async liveness(input: Parameters<ScriptedSessionPort["liveness"]>[0]) {
		if (this.dropped.has(input.sessionId)) return { live: false, disowned: false };
		return await super.liveness(input);
	}
}

test("a /new-retired turn whose session a broker restart dropped is closed instead of re-checked forever", async () => {
	const port = new DroppedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const logs: string[] = [];
	await harness(port, {}, (line) => logs.push(line));
	enqueue("m-1", "old turn");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "turn did not start");
	const old = port.sends[0]!;
	await manager?.reset(KEY, JSON.stringify(ORIGIN));
	expect(database?.inboundTurnRow(old.opRef)).toMatchObject({ state: "pending", turn_state: "accepted" });

	port.dropped.add(old.sessionId);
	await manager!.tick(KEY);
	await eventually(() => database?.inboundTurnRow(old.opRef)?.turn_state === "done", "retired turn was not closed");
	expect(logs.some((line) => line.startsWith("retired_turn_closed") && line.includes(old.opRef))).toBe(true);
	// Recorded as a discarded turn, not as an answered-without-delivery one.
	expect(database?.inboundTurnRow(old.opRef)?.terminal_delivery_id).toBe(JSON.stringify({ none: "retired" }));
	// Closed, never re-sent: the discarded prompt is not replayed into the new session.
	expect(port.sends).toHaveLength(1);
	const holdsAfter = logs.filter((line) => line.startsWith("recovery_hold") && line.includes(old.opRef)).length;
	await manager!.tick(KEY);
	expect(logs.filter((line) => line.startsWith("recovery_hold") && line.includes(old.opRef)).length).toBe(holdsAfter);
});

test("an ACCEPTED live turn whose session answers endpoint_stale and is not live is closed with a notice, never re-sent", async () => {
	const port = new DroppedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const logs: string[] = [];
	const failures: string[] = [];
	await harness(port, { failure: (message) => failures.push(message) }, (line) => logs.push(line));
	enqueue("m-1", "question");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "turn did not start");
	const first = port.sends[0]!;
	expect(database?.inboundTurnRow(first.opRef)?.turn_state).toBe("accepted");

	port.dropped.add(first.sessionId);
	await manager!.tick(KEY);
	await eventually(
		() => logs.some((line) => line.startsWith("accepted_turn_closed") && line.includes(first.opRef)),
		`endpoint_stale was not classified as session_unavailable:\n${logs.join("\n")}`,
	);
	// The model may already have run and acted: the trigger is closed, never re-sent.
	expect(database?.inboundTurnRow(first.opRef)).toMatchObject({ state: "done", turn_state: "done" });
	expect(port.sends).toHaveLength(1);
	expect(logs.some((line) => line.startsWith("recovery_requeue_unaccepted"))).toBe(false);
	// The owner is told the turn was cut off instead of waiting on silence.
	expect(failures).toHaveLength(1);
	expect(failures[0]).toContain("session_unavailable");
	// The next message gets a fresh session instead of the dead one.
	expect(database?.getSessionRecord(KEY)?.epoch).toBe(1);
	enqueue("m-2", "next question");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 2, "next message was not dispatched");
	expect(port.sends[1]!.text).toBe("next question");
	expect(port.sends[1]!.sessionId).not.toBe(first.sessionId);
});

test("an ACCEPTED turn whose status is endpoint_stale but whose liveness is unanswerable is held, never closed or re-sent", async () => {
	class UnknownLivenessPort extends DroppedSessionPort {
		override async liveness(input: Parameters<ScriptedSessionPort["liveness"]>[0]) {
			if (this.dropped.has(input.sessionId)) return { live: undefined, disowned: false };
			return await super.liveness(input);
		}
	}
	const port = new UnknownLivenessPort({ onBind: (input) => `session-e${input.epoch}` });
	const logs: string[] = [];
	await harness(port, {}, (line) => logs.push(line));
	enqueue("m-1", "question");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "turn did not start");
	const first = port.sends[0]!;
	port.dropped.add(first.sessionId);
	await manager!.tick(KEY);
	await eventually(
		() => logs.some((line) => line.startsWith("recovery_hold") && line.includes(first.opRef)),
		"unanswerable liveness did not hold the turn",
	);
	expect(database?.inboundTurnRow(first.opRef)).toMatchObject({ state: "pending", turn_state: "accepted" });
	expect(port.sends).toHaveLength(1);
	expect(logs.some((line) => line.startsWith("accepted_turn_closed"))).toBe(false);
});

test("a dead-session close whose notice fails to persist leaves the turn open and retries the notice; it is never lost or duplicated", async () => {
	const port = new DroppedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const logs: string[] = [];
	let failNotice = true;
	const notices: string[] = [];
	await harness(
		port,
		{
			failure: (message) => {
				if (failNotice) throw new Error("ledger write failed");
				notices.push(message);
			},
		},
		(line) => logs.push(line),
	);
	enqueue("m-1", "question");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "turn did not start");
	const first = port.sends[0]!;
	port.dropped.add(first.sessionId);

	await manager!.tick(KEY).catch(() => {});
	// Persisting the notice failed: the trigger is NOT closed and the binding is kept.
	expect(database?.inboundTurnRow(first.opRef)).toMatchObject({ state: "pending", turn_state: "accepted" });
	expect(database?.getSessionRecord(KEY)?.epoch).toBe(0);
	expect(logs.some((line) => line.startsWith("accepted_turn_closed"))).toBe(false);

	failNotice = false;
	await manager!.tick(KEY);
	await eventually(() => database?.inboundTurnRow(first.opRef)?.turn_state === "done", "retry did not close the turn");
	expect(notices).toHaveLength(1);
	expect(database?.getSessionRecord(KEY)?.epoch).toBe(1);
	// A further sweep finds it closed: no second notice, no resend.
	await manager!.tick(KEY);
	expect(notices).toHaveLength(1);
	expect(port.sends).toHaveLength(1);
});

test("a retired stalled turn terminates its producer and summarizes discarded frames", async () => {
	const port = new ScriptedSessionPort({
		onBind: (input) => `session-e${input.epoch}`,
		onSend: (input, scripted) => {
			if (input.text === "new turn") scripted.complete(input.opRef, "replacement reply");
		},
	});
	const terminal: string[] = [];
	const logs: string[] = [];
	await harness(port, { terminal: (text) => terminal.push(text) }, (line) => logs.push(line));
	enqueue("m-1", "old turn");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "accepted turn did not start before retired stall");
	const send = port.sends[0]!;
	const opRef = latestOpRef;
	await manager?.reset(KEY, JSON.stringify(ORIGIN));
	enqueue("m-2", "new turn");
	await manager!.notifyInbound(KEY);
	await eventually(() => port.sends.length === 2 && terminal.length === 1, "replacement turn did not settle");
	const replacement = port.sends[1]!;
	expect(replacement.sessionId).not.toBe(send.sessionId);
	expect(database!.getSessionRecord(KEY)?.sessionId).toBe(replacement.sessionId);
	await eventually(() => port.tailsOf(send.sessionId).length === 1, "retired turn did not reattach its tail");
	const tail = port.tailsOf(send.sessionId)[0]!;
	for (let index = 0; index < 3; index++)
		tail.emit({
			kind: "message_end",
			rawKind: "message_end",
			eventId: `stale-${index}`,
			commandId: `command-${opRef}`,
			turnId: `turn-${opRef}`,
			payload: { role: "assistant", content: [{ text: "discarded answer" }] },
			assistantText: "discarded answer",
			steerEcho: false,
			idle: false,
		});
	await eventually(
		() => logs.some((line) => line.startsWith(`stale_output originKey=${KEY}`) && line.includes("action=start")),
		"discarded output start was not summarized",
	);
	expect(logs.filter((line) => line.startsWith(`stale_output originKey=${KEY}`))).toHaveLength(1);
	await Bun.sleep(10);
	port.emitStall(send.sessionId);
	await eventually(
		() => logs.some((line) => line.startsWith(`stale_output originKey=${KEY}`) && line.includes("action=stop")),
		"discarded output stop was not summarized",
	);
	await eventually(
		() =>
			logs.some((line) => line.includes("retired_session_host") && line.includes("reason=stall outcome=terminated")),
		"stalled retired host was not terminated",
	);
	await Bun.sleep(80);
	expect(port.tailsOf(send.sessionId)).toHaveLength(0);
	expect(port.closes.map((close) => close.sessionId)).toEqual([send.sessionId]);
	expect(await port.inspect({ sessionId: send.sessionId, repo: join(home, "workspace") })).toMatchObject({
		live: false,
	});
	expect(await port.inspect({ sessionId: replacement.sessionId, repo: join(home, "workspace") })).toMatchObject({
		live: true,
	});
	expect(database!.inboundTurnRow(opRef)?.turn_state).toBe("accepted");
	expect(port.sends).toHaveLength(2);
	expect(
		logs.filter((line) => line.includes(`retired_hold originKey=${KEY}`) && line.includes("reason=stall")),
	).toHaveLength(1);
	port.complete(send.opRef, "must remain fenced");
	await manager!.tick(KEY);
	await eventually(
		() => database?.inboundTurnRow(opRef)?.turn_state === "done",
		"retired hold did not reconcile terminal",
	);
	const staleLogs = logs.filter((line) => line.startsWith(`stale_output originKey=${KEY}`));
	expect(staleLogs).toHaveLength(2);
	expect(staleLogs[0]).toContain("action=start");
	expect(staleLogs[1]).toMatch(/action=stop count=3 first=\S+ last=\S+ reason=stall/);
	expect(
		logs.filter((line) => line.startsWith("retired_session_host ") && line.includes(`session=${send.sessionId}`)),
	).toHaveLength(1);
	const retiredHostLogIndex = logs.findIndex(
		(line) => line.startsWith("retired_session_host ") && line.includes(`session=${send.sessionId}`),
	);
	const retiredHoldLogIndex = logs.findIndex(
		(line) => line.includes(`retired_hold originKey=${KEY}`) && line.includes("reason=stall"),
	);
	expect(retiredHostLogIndex).toBeLessThan(retiredHoldLogIndex);
	expect(logs.slice(retiredHoldLogIndex + 1).some((line) => line.includes(`session=${send.sessionId}`))).toBe(false);
	expect(terminal).toEqual(["replacement reply"]);
});

/** A torn initial send used to exercise recovery of persisted unaccepted turns. */
class GhostSendPort extends ScriptedSessionPort {
	ghostOpRef: string | undefined;
	ghostSessionId: string | undefined;
	ghostDead = false;

	constructor() {
		super({ onBind: (input) => `session-e${input.epoch}` });
	}

	override async send(input: Parameters<ScriptedSessionPort["send"]>[0]) {
		if (this.ghostOpRef === undefined) {
			this.ghostOpRef = input.opRef;
			this.ghostSessionId = input.sessionId;
			this.sendAttempts.push(input);
			throw new GjcCliError("gjc sdk session send reported failure", 0, "", {
				code: "timeout",
				message: "SDK session Router startup timed out.",
			});
		}
		return await super.send(input);
	}

	override async steer(input: Parameters<ScriptedSessionPort["steer"]>[0]) {
		if (input.sessionId === this.ghostSessionId)
			throw steerRefused("session is unavailable through the session Router");
		await super.steer(input);
	}

	override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
		if (input.opRef === this.ghostOpRef && this.ghostDead)
			throw Object.assign(new Error("session_unavailable"), { code: "session_unavailable" });
		return await super.status(input);
	}

	async liveness(input: { sessionId: string; repo: string }) {
		return { live: !(input.sessionId === this.ghostSessionId && this.ghostDead), disowned: false };
	}
}

test("a refused running steer waits without rotating or retrying, then sends on the same session", async () => {
	const port = new ScriptedSessionPort({
		onSteer: () => {
			throw steerRefused();
		},
	});
	await harness(port);
	enqueue("m-1", "first");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	enqueue("m-2", "next turn");
	await manager!.notifyInbound(KEY);
	for (let tick = 0; tick < 3; tick++) await manager!.tick(KEY);
	expect(port.steers).toHaveLength(1);
	expect(port.binds).toHaveLength(1);
	expect(port.sends).toHaveLength(1);
	expect(database!.getSessionRecord(KEY)?.epoch).toBe(0);
	expect(database!.inboundPendingOldest(KEY)?.message_id).toBe("m-2");
	port.complete(first.opRef, "first answer");
	await eventually(() => port.sends.length === 2, "pending message did not dispatch after terminal");
	expect(port.sends[1]).toMatchObject({ sessionId: first.sessionId, text: "next turn" });
	expect(port.binds).toHaveLength(1);
	expect(database!.getSessionRecord(KEY)?.epoch).toBe(0);
	port.complete(port.sends[1]!.opRef, "second answer");
	await eventually(() => database!.inboundPendingCount(KEY) === 0, "input was lost or left pending");
});

for (const details of [{ code: "receipt_identity_mismatch" }, { code: "unknown_receipt", refused: true }])
	test(`ambiguous ${details.code} stays attributed across restart and terminal without a new operation`, async () => {
		let accepted = false;
		const port = new ScriptedSessionPort({
			onSteer: () => {
				if (!accepted) throw new GjcCliError("uncertain receipt", 0, "", details);
			},
		});
		await harness(port);
		enqueue("m-1", "first");
		await manager!.notifyInbound(KEY);
		const first = port.sends[0]!;
		enqueue("m-2", "held input");
		await manager!.notifyInbound(KEY);
		const clientRef = port.steers[0]!.clientRef;
		expect(database!.inboundSteersHeld(first.opRef).map((row) => row.message_id)).toEqual(["m-2"]);
		await manager!.stop();
		manager = new PersonaSessionManager({
			database: database!,
			port,
			instanceId: "instance-test",
			repo: join(home, "workspace"),
			onTurnStart: ({ trigger }) => ({ text: trigger.body }),
		});
		await manager.recover();
		await manager.tick(KEY);
		port.complete(first.opRef, "first answer");
		await manager.tick(KEY);
		await eventually(() => database!.inboundTurnRow(first.opRef)?.turn_state === "done", "terminal not recovered");
		await manager.tick(KEY);
		expect(database!.inboundSteersHeld(first.opRef).map((row) => row.message_id)).toEqual(["m-2"]);
		expect(database!.inboundPendingOldest(KEY)).toBeUndefined();
		expect(port.sends).toHaveLength(1);
		expect(port.binds).toHaveLength(1);
		expect(database!.getSessionRecord(KEY)?.epoch).toBe(0);
		expect(new Set(port.steers.map((steer) => steer.clientRef))).toEqual(new Set([clientRef]));
		accepted = true;
		await manager.tick(KEY);
		expect(database!.inboundTurnRows(first.opRef).find((row) => row.message_id === "m-2")).toMatchObject({
			turn_state: "done",
			turn_role: "steer",
		});
		expect(port.sends).toHaveLength(1);
	});

/**
 * Live 2026-09-05 (every main cutover from a schema-16 home): a BOUND trigger
 * on disk pointed at a session the new broker has never heard of. Status came
 * back adoptable, and the tail attach then threw
 * `session tail failed: session_unavailable`, which crashed the actor's recovery
 * and wedged the origin ("already has a nonterminal turn") until the row was
 * hand-edited.
 */
test("startup recovery releases a bound turn whose tail attach is disowned instead of wedging the origin", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("m-1", "orphaned by cutover");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "turn did not start");
	const orphan = port.sends[0]!;
	const opRef = latestOpRef;
	await manager?.stop();
	// The row as the old runtime left it: bound, never acknowledged.
	database?.inboundTurnRequeue(opRef);
	database?.inboundBindTurn({ messageId: "m-1", originKey: KEY, epoch: 0, opRef, sessionId: orphan.sessionId });
	expect(database?.inboundTurnRow(opRef)).toMatchObject({ turn_state: "bound" });

	const logs: string[] = [];
	class DisowningTailPort extends ScriptedSessionPort {
		override async attachTail(input: Parameters<ScriptedSessionPort["attachTail"]>[0]) {
			if (input.sessionId === orphan.sessionId) throw new Error("session tail failed: session_unavailable");
			return await super.attachTail(input);
		}
	}
	const disowning = new DisowningTailPort({ onBind: (input) => `fresh-e${input.epoch}` });
	registerFixtureBindings(disowning);
	disowning.seedOperation(opRef, orphan.sessionId, "in_flight");
	// inspect/status still describe the session as live and the op as running
	// (live shape: the id is indexed but the tail router disowns it).
	disowning.setSessionState(orphan.sessionId, { repo: join(home, "workspace"), live: true, deleted: false });
	manager = new PersonaSessionManager({
		database: database!,
		port: disowning,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		log: (line) => logs.push(line),
		onTurnStart: ({ trigger }) => ({ text: trigger.body }),
	});
	await manager.recover();
	expect(logs.filter((line) => line.startsWith("recovery_turn_failed"))).toEqual([]);
	expect(logs.some((line) => line.includes(`opRef=${opRef}`) && line.includes("reason=tail_attach_disowned"))).toBe(
		true,
	);
	await eventually(() => disowning.sends.length === 1, "released trigger was not re-dispatched");
	expect(disowning.sends[0]!.text).toBe("orphaned by cutover");
	expect(disowning.sends[0]!.sessionId).not.toBe(orphan.sessionId);
	expect(database?.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(manager.state(KEY)).toBe("turn-running");
});

test("startup recovery releases a retired bound turn the broker disowns instead of holding it forever", async () => {
	const port = new GhostSendPort();
	await harness(port);
	enqueue("m-1", "torn send");
	await manager?.notifyInbound(KEY);
	const ghostOpRef = port.ghostOpRef!;
	// Only an explicit operator reset retires this still-unresolved turn.
	await manager?.reset(KEY, JSON.stringify(ORIGIN));
	enqueue("m-2", "replacement turn");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "replacement turn did not start");
	const replacement = port.sends[0]!;
	port.complete(replacement.opRef, "answered");
	await eventually(
		() => database?.inboundTurnRow(replacement.opRef)?.turn_state === "done",
		"replacement did not close",
	);
	// Exactly the live shape: epoch rotated, ghost trigger still bound on a dead session.
	expect(database?.inboundTurnRow(ghostOpRef)).toMatchObject({ state: "pending", turn_state: "bound", turn_epoch: 0 });
	expect(database?.getSessionRecord(KEY)?.epoch).toBe(1);
	await manager?.stop();

	port.ghostDead = true;
	const logs: string[] = [];
	const released: string[] = [];
	manager = new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		log: (line) => logs.push(line),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onReleased: ({ turn }) => {
				released.push(turn.opRef);
			},
		}),
	});
	await manager.recover();
	expect(
		logs.some((line) => line.includes(`opRef=${ghostOpRef}`) && line.includes("reason=retired_router_disowned")),
	).toBe(true);
	// Never adopted, so no lifecycle to release; the row simply became the next turn under the current epoch.
	expect(released).toEqual([]);
	await eventually(() => port.sends.length === 2, "released ghost trigger was not re-dispatched");
	expect(port.sends[1]!.text).toBe("torn send");
	expect(port.sends[1]!.opRef).not.toBe(ghostOpRef);
	expect(database?.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(logs.filter((line) => line.includes(`opRef=${ghostOpRef}`) && line.startsWith("recovery_hold"))).toEqual([]);
});

test("startup recovery reconstructs an accepted durable turn and reconciles status plus turn.result", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("m-1", "recover me");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "accepted turn did not start before restart");
	const oldManager = manager!;
	const opRef = port.sends[0]!.opRef;
	await oldManager.stop();
	const terminal: string[] = [];
	manager = new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onTerminal: ({ text }) => {
				terminal.push(text);
			},
		}),
	});
	await manager.recover();
	port.complete(opRef, "recovered reply");
	await manager.tick(KEY);
	await eventually(() => terminal.length === 1, "recovered actor did not deliver terminal output");
	expect(terminal).toEqual(["recovered reply"]);
	expect(port.workerOutputReads.some((input) => input.opRef === opRef)).toBe(true);
	expect(database?.inboundTurnRow(opRef)).toMatchObject({ state: "done", turn_state: "done" });
});

test("turn op-refs are SDK-safe even when platform ids contain unsafe bytes", async () => {
	const opRef = personaTurnOpRef("instance", "discord/channel/room", 4, "message id / with spaces");
	expect(opRef).toMatch(/^gw-p-[0-9a-f]{32}$/);
});

test("notifyInbound immediately starts a turn while idle", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("m-1", "live policy");
	await manager?.notifyInbound(KEY);

	expect(manager?.state(KEY)).toBe("turn-running");
	expect(port.sends).toEqual([expect.objectContaining({ text: "live policy", opRef: latestOpRef })]);
});

test("lane_report steer uses lane framing, not user framing", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("human-trigger", "first");
	await manager!.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "initial persona turn did not start");
	enqueue("lane-report-steer", "[lane child] completed: result", "lane_report");
	await manager!.notifyInbound(KEY);
	await eventually(() => port.steers.length === 1, "lane report was not steered into the running turn");
	expect(port.steers[0]?.text).toBe(
		"[Internal lane report that arrived while you were working. Absorb it; mention it to the conversation only if useful.]\n\n[lane child] completed: result",
	);
	expect(port.steers[0]?.text).not.toContain("Additional message from the user");
	expect(database!.inboundTurnRows(latestOpRef)).toEqual(
		expect.arrayContaining([
			expect.objectContaining({ message_id: "lane-report-steer", source: "lane_report", turn_role: "steer" }),
		]),
	);
});

test("/new keeps pending lane_report rows while discarding platform rows", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("platform-before-new", "old human", "platform");
	enqueue("internal-before-new", "[lane child] attempt_ended", "lane_report");
	await manager!.reset(KEY, JSON.stringify(ORIGIN), "2030-09-01T00:00:00.000Z");
	await eventually(() => port.sends.length === 1, "pending lane report was not admitted after /new");
	expect(database!.inboundTurnRow(latestOpRef)).toMatchObject({
		message_id: "internal-before-new",
		source: "lane_report",
		state: "pending",
	});
	expect(port.sends[0]?.text).toBe("[lane child] attempt_ended");
});

test("admissionHold reports a quarantined nonterminal persona turn", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("quarantined-trigger", "original request");
	await manager!.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "persona turn did not start");
	const authority = database!.inspectBrokerAuthority().authority;
	if (!authority) throw new Error("fixture broker authority missing");
	database!.cutoverBrokerAuthority({
		expectedAuthority: authority,
		targetAuthority: { canonicalAgentDir: join(home, "next-agent"), identity: "next-owner" },
		evidence: "test quarantine for admission hold",
		disposition: "quarantine",
	});
	expect(manager!.admissionHold(KEY)).toBe("quarantined_turn");
});
test("two messages 50ms apart start one turn and steer the second", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	const now = Date.now();
	expect(
		database?.inboundEnqueue({
			messageId: "m-1",
			originKey: KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body: "first fragment",
			receivedAt: new Date(now).toISOString(),
		}),
	).toBe(true);
	expect(
		database?.inboundEnqueue({
			messageId: "m-2",
			originKey: KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body: "second fragment",
			receivedAt: new Date(now + 50).toISOString(),
		}),
	).toBe(true);

	await manager?.notifyInbound(KEY);

	expect(manager?.state(KEY)).toBe("turn-running");
	expect(port.sends).toEqual([expect.objectContaining({ text: "first fragment", opRef: latestOpRef })]);
	expect(port.steers).toEqual([
		expect.objectContaining({
			sessionId: port.sends[0]!.sessionId,
			text: expect.stringMatching(/^\[Additional message[^\n]*\]\nsecond fragment$/),
		}),
	]);
	expect(database?.inboundTurnRows(latestOpRef)).toEqual([
		expect.objectContaining({
			message_id: "m-1",
			state: "pending",
			turn_role: "trigger",
			turn_state: "accepted",
		}),
		expect.objectContaining({
			message_id: "m-2",
			state: "done",
			turn_role: "steer",
			turn_state: "done",
			turn_op_ref: latestOpRef,
		}),
	]);

	port.complete(latestOpRef, "done");
	await eventually(
		() =>
			database?.inboundTurnRows(latestOpRef).every((row) => row.state === "done" && row.turn_state === "done") === true,
		"turn rows did not complete after terminal tail evidence",
	);
});

test("recovery and stop never scan or delete unrelated shared broker sessions", async () => {
	class SharedPort extends ScriptedSessionPort {
		readonly index = [
			{ sessionId: "unrelated-saved", live: false, lastActivityMs: 0 },
			{ sessionId: "unrelated-live", live: true, lastActivityMs: 0 },
			{ sessionId: "unrelated-unknown-age", live: false, lastActivityMs: undefined },
		];
		indexScans = 0;
		readonly deleted: string[] = [];
		async listSessions() {
			this.indexScans++;
			return this.index.map((row) => ({
				...row,
				cwd: "/shared-workspace",
				sessionPath: `/shared-sessions/${row.sessionId}.jsonl`,
			}));
		}
		async deleteSession(input: { sessionId: string }) {
			this.deleted.push(input.sessionId);
			return { deleted: true as const };
		}
	}
	const port = new SharedPort();
	const logs: string[] = [];
	await harness(port, {}, (line) => logs.push(line));
	enqueue("m-1", "recover my pending message");
	await manager!.recover();
	await eventually(() => port.sends.length === 1, "pending recovery did not dispatch");
	const send = port.sends[0]!;
	expect(port.indexScans).toBe(0);
	expect(port.deleted).toEqual([]);
	await manager!.stop();
	expect(port.indexScans).toBe(0);
	expect(port.deleted).toEqual([]);

	manager = new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		log: (line) => logs.push(line),
	});
	await manager.recover();
	expect(manager.state(KEY)).toBe("turn-running");
	expect(port.sends).toHaveLength(1);
	port.complete(send.opRef, "recovered reply");
	await manager.tick(KEY);
	await eventually(
		() => database?.inboundTurnRow(send.opRef)?.turn_state === "done",
		"recovered turn did not complete",
	);
	await manager.stop();
	expect(port.indexScans).toBe(0);
	expect(port.deleted).toEqual([]);
	expect(logs.some((line) => line.startsWith("session_gc"))).toBe(false);
});
