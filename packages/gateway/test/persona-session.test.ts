import { afterEach, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GjcCliError } from "@gajae-gateway/subsession";
import {
	AGENT_RUN_GRACE_MS,
	AGENT_RUN_MAX_MS,
	freshTurnAttempt,
	PERSONA_COMPACTION_TOKENS,
	PERSONA_PREAMBLE_REFRESH_SENDS,
	type PersonaFailureInput,
	PersonaSessionManager,
	personaTurnOpRef,
	resetFailedTurn,
	retryInboundTurn,
	STALLED_TURN_TERMINAL_TIMEOUT_MS,
} from "../src/orchestrator/persona-session";
import { formatFailureNotice } from "../src/orchestrator/rebind";
import {
	ModelNotSelectedError,
	type SessionCompactionStatus,
	type SessionContextUsage,
} from "../src/orchestrator/session-port";
import type { TailAttachInput, TailFrame } from "../src/orchestrator/tail-runner";
import { BrokerAuthorityError, Store } from "../src/store";
import { BROKER_PROMPT_BUSY_REFUSAL } from "../src/store/groups/bindings";
import { attachTestBrokerOwnership, ScriptedSessionPort, steerRefused } from "./session-port.fake";

let home = "";
let database: Store | undefined;
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

async function establishStoredSession(port: ScriptedSessionPort, messageId: string) {
	enqueue(messageId, "seed turn");
	await manager!.notifyInbound(KEY);
	const stored = port.sends.at(-1)!;
	port.complete(stored.opRef, "seed answer");
	await manager!.tick(KEY);
	await eventually(
		() => database!.inbox.turnRow(stored.opRef)?.turn_state === "done",
		"seed turn did not settle before the stored session was exercised",
	);
	return stored;
}

function trackRecoveryCalls(): () => number {
	let calls = 0;
	const recover = manager!.recover.bind(manager!);
	Object.defineProperty(manager, "recover", {
		value: async () => {
			calls++;
			await recover();
		},
	});
	return () => calls;
}

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 200; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

function enqueue(messageId: string, body: string, source: "platform" | "lane_report" = "platform"): void {
	const accepted = database?.transaction((tx) =>
		database?.inbox.enqueue(tx, {
			messageId,
			originKey: KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body,
			source,
		}),
	);
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
		terminal?: (text: string, alreadyPosted?: boolean) => void;
		retired?: () => void;
		released?: (opRef: string) => void;
		failure?: (message: string, forceFailureNotice?: boolean) => void;
		failureError?: (error: Error) => void;
		modelRefusal?: (input: PersonaFailureInput) => void;
		onFrame?: (frame: TailFrame) => boolean | void;
		contextMessageIds?: readonly string[];
	} = {},
	log?: (line: string) => void,
	extra: {
		brokerGeneration?: () => number;
		submissionRetryDelayMs?: number;
		onSloEvent?: (code: "persona_bind_failed", at: number) => void;
		onBindFailure?: (input: { originKey: string; detail: string; at: number }) => void;
		onSessionReady?: (input: { originKey: string; at: number }) => void;
		maxLiveHosts?: number;
		hostIdleRetireMs?: number;
		setTimeout?: (work: () => void, delayMs: number) => unknown;
		clearTimeout?: (timer: unknown) => void;
		now?: () => number;
	} = {},
) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-persona-session-"));
	database = await Store.open(join(home, "gateway.db"));
	registerFixtureBindings(port);
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		submissionRetryDelayMs: 1,
		...(log ? { log } : {}),
		...extra,
		onTurnStart: ({ trigger, turn }) => {
			latestOpRef = turn.opRef;
			return {
				text: trigger.body,
				...(hooks.contextMessageIds ? { contextMessageIds: new Set(hooks.contextMessageIds) } : {}),
				...(hooks.onFrame ? { onFrame: (input) => hooks.onFrame?.(input.frame) } : {}),
				onTerminal: ({ text, alreadyPosted }) => hooks.terminal?.(text, alreadyPosted),
				onFailure: ({ error, forceFailureNotice }) => {
					hooks.failure?.(error.message, forceFailureNotice);
					hooks.failureError?.(error);
				},
				...(hooks.modelRefusal ? { onModelRefusal: hooks.modelRefusal } : {}),
				onRetired: () => hooks.retired?.(),
				onReleased: ({ turn: released }) => hooks.released?.(released.opRef),
			};
		},
	});
}

/** Settles one inbound whose every send fails; returns the final (retried) send. */
async function settleFailedInbound(port: ScriptedSessionPort, messageId: string) {
	enqueue(messageId, messageId);
	const activeManager = manager;
	const activeDatabase = database;
	if (!activeManager || !activeDatabase) throw new Error("test harness did not initialize");
	await activeManager.notifyInbound(KEY);
	const sendsFor = () => port.sends.filter((send) => send.text === messageId);
	await eventually(() => {
		const last = sendsFor().at(-1);
		return (
			last !== undefined &&
			activeDatabase.inbox.turnRow(last.opRef)?.turn_state === "done" &&
			activeManager.state(KEY) === "idle"
		);
	}, `failed turn ${messageId} did not settle`);
	return sendsFor().at(-1)!;
}

function seedPersonaPrompt(
	send: { sessionId: string; opRef: string },
	result: "busy" | "pending" | "accepted" = "busy",
) {
	const store = database!;
	const authority = store.bindings.authority!;
	const scope = JSON.stringify([send.sessionId, send.opRef]);
	store.transaction((tx) => {
		store.bindings.beginBrokerOperation(tx, authority, {
			operation: "turn.prompt",
			scope,
			operationRef: send.opRef,
			requestJson: "{}",
		});
		if (result !== "pending")
			store.bindings.completeBrokerOperation(
				tx,
				authority,
				"turn.prompt",
				scope,
				send.opRef,
				JSON.stringify(result === "busy" ? BROKER_PROMPT_BUSY_REFUSAL : { operationRef: send.opRef }),
			);
	});
	return () => store.bindings.brokerOperation(authority, "turn.prompt", scope);
}

test("retryInboundTurn removes only the abandoned persona busy proof and rolls it back with its owner", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("retry-busy-proof", "retry this turn");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	const proof = seedPersonaPrompt(first);
	const before = database!.inbox.turnRow(first.opRef);
	expect(() =>
		database!.transaction((tx) => {
			expect(retryInboundTurn(database!, tx, first.opRef)).toBe(1);
			expect(proof()).toBeUndefined();
			throw new Error("rollback busy release");
		}),
	).toThrow("rollback busy release");
	expect(database!.inbox.turnRow(first.opRef)).toEqual(before);
	expect(proof()).toMatchObject({ state: "complete", resultJson: JSON.stringify(BROKER_PROMPT_BUSY_REFUSAL) });
	expect(database!.transaction((tx) => retryInboundTurn(database!, tx, first.opRef))).toBe(1);
	expect(database!.inbox.pendingOldest(KEY)).toMatchObject({ turn_state: null, bound_session_id: null });
	expect(proof()).toBeUndefined();
});

for (const result of ["busy", "pending", "accepted"] as const)
	test(`persona terminal completion releases only normalized busy proof (${result})`, async () => {
		const port = new ScriptedSessionPort();
		await harness(port);
		enqueue(`terminal-proof-${result}`, "finish this turn");
		await manager!.notifyInbound(KEY);
		const first = port.sends[0]!;
		const proof = seedPersonaPrompt(first, result);
		await manager!.tick(KEY);
		expect(database!.inbox.turnRow(first.opRef)?.turn_state).toBe("accepted");
		expect(proof()).toBeDefined();
		port.complete(first.opRef, "finished");
		await manager!.tick(KEY);
		await eventually(() => database!.inbox.turnRow(first.opRef)?.turn_state === "done", "terminal did not settle");
		if (result === "busy") expect(proof()).toBeUndefined();
		else expect(proof()).toMatchObject({ state: result === "pending" ? "pending" : "complete" });
	});

test("standalone persona retry uses the owned binding repo rather than the manager repo", async () => {
	await harness(new ScriptedSessionPort());
	const store = database!;
	const sessionId = "standalone-owned-session";
	const opRef = "standalone-owned-ref";
	store.transaction((tx) =>
		store.bindings.recordOwnedBinding(tx, {
			authority: store.bindings.authority!,
			sessionId,
			originKey: KEY,
			epoch: 0,
			repo: join(home, "different-owned-repo"),
		}),
	);
	enqueue("standalone-retry", "standalone owned repo");
	store.transaction((tx) =>
		store.inbox.bindTurn(tx, { messageId: "standalone-retry", originKey: KEY, epoch: 0, opRef, sessionId }),
	);
	const proof = seedPersonaPrompt({ sessionId, opRef });
	expect(store.transaction((tx) => retryInboundTurn(store, tx, opRef))).toBe(1);
	expect(proof()).toBeUndefined();
});

for (const mismatch of ["session", "origin", "epoch", "ref", "authority"] as const)
	test(`persona retry retains busy proof with mismatched ${mismatch}`, async () => {
		const port = new ScriptedSessionPort();
		await harness(port);
		enqueue(`mismatch-${mismatch}`, "retry without deleting unrelated proof");
		await manager!.notifyInbound(KEY);
		const first = port.sends[0]!;
		const authority = database!.bindings.authority!;
		const proof = seedPersonaPrompt({
			sessionId: mismatch === "session" ? "unrelated-session" : first.sessionId,
			opRef: mismatch === "ref" ? "unrelated-ref" : first.opRef,
		});
		if (mismatch === "origin" || mismatch === "epoch") {
			database!.transaction((tx) => {
				database!.inbox.turnRequeue(tx, first.opRef);
				if (mismatch === "epoch") database!.bindings.rotateEpoch(tx, KEY);
				if (mismatch === "origin")
					database!.inbox.enqueue(tx, {
						messageId: "other-origin-trigger",
						originKey: "loopback/loopback/other",
						originRefJson: JSON.stringify({ ...ORIGIN, conversationId: "other" }),
						body: "unrelated origin",
					});
				database!.inbox.bindTurn(tx, {
					messageId: mismatch === "origin" ? "other-origin-trigger" : `mismatch-${mismatch}`,
					originKey: mismatch === "origin" ? "loopback/loopback/other" : KEY,
					epoch: mismatch === "epoch" ? 1 : 0,
					opRef: first.opRef,
					sessionId: first.sessionId,
				});
			});
		}
		if (mismatch === "authority")
			database!.transaction((tx) =>
				database!.bindings.setAuthority(tx, { ...authority, identity: "revoked-persona-owner" }),
			);
		try {
			database!.transaction((tx) => retryInboundTurn(database!, tx, first.opRef));
			expect(proof()).toBeDefined();
		} finally {
			if (mismatch === "authority") database!.transaction((tx) => database!.bindings.setAuthority(tx, authority));
		}
	});

test("ambiguous busy send retains its identity and proof despite an unobserved transcript answer", async () => {
	class BusySendPort extends ScriptedSessionPort {
		queueEmptyReads = 0;

		async queueEmpty(): Promise<boolean> {
			this.queueEmptyReads++;
			return true;
		}

		override async send(input: Parameters<ScriptedSessionPort["send"]>[0]): ReturnType<ScriptedSessionPort["send"]> {
			this.sendAttempts.push(input);
			seedPersonaPrompt(input);
			throw new GjcCliError("busy", 0, "", { code: "busy", message: "busy" });
		}
	}
	const port = new BusySendPort();
	await harness(port);
	enqueue("bound-busy-proof", "do not abandon an unresolved send");
	await manager!.notifyInbound(KEY);
	const first = port.sendAttempts[0]!;
	const authority = database!.bindings.authority!;
	const proof = () =>
		database!.bindings.brokerOperation(authority, "turn.prompt", JSON.stringify([first.sessionId, first.opRef]));
	expect(database!.inbox.turnRow(first.opRef)?.turn_state).toBe("bound");
	expect(proof()).toBeDefined();
	port.setActivity(first.sessionId, "idle");
	port.appendAssistant(first.sessionId, "recovered transcript answer");
	await manager!.tick(KEY);
	expect(port.queueEmptyReads).toBe(0);
	expect(database!.inbox.turnRow(first.opRef)).toMatchObject({
		state: "pending",
		turn_state: "bound",
		turn_op_ref: first.opRef,
		bound_session_id: first.sessionId,
		turn_epoch: 0,
	});
	expect(proof()).toBeDefined();
	expect(port.sendAttempts).toHaveLength(1);
});

test("persona terminal failed completion CAS retains its busy proof", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("failed-completion-cas", "hold on failed CAS");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	const proof = seedPersonaPrompt(first);
	const complete = database!.inbox.turnComplete.bind(database!.inbox);
	let attempts = 0;
	database!.inbox.turnComplete = () => {
		attempts++;
		return 0;
	};
	try {
		port.complete(first.opRef, "finished but not committed");
		await manager!.tick(KEY);
		await eventually(() => attempts > 0, "terminal completion CAS was not attempted");
		expect(database!.inbox.turnRow(first.opRef)?.turn_state).toBe("accepted");
		expect(proof()).toBeDefined();
	} finally {
		database!.inbox.turnComplete = complete;
	}
});

test("persona failure completion releases its abandoned busy proof", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("failed-busy-proof", "fail this turn");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	const proof = seedPersonaPrompt(first);
	port.fail(first.opRef, "terminal failure");
	await manager!.tick(KEY);
	await eventually(() => database!.inbox.turnRow(first.opRef)?.turn_state === "done", "failure did not settle");
	expect(proof()).toBeUndefined();
});

test("persona bind failures are emitted to the SLO recorder", async () => {
	const failures: Array<{ code: string; at: number }> = [];
	const readinessFailures: Array<{ originKey: string; detail: string; at: number }> = [];
	const port = new ScriptedSessionPort({
		onBind: () => {
			throw new Error("broker unavailable");
		},
	});
	await harness(port, {}, undefined, {
		onSloEvent: (code, at) => failures.push({ code, at }),
		onBindFailure: (input) => readinessFailures.push(input),
	});
	enqueue("bind-failure", "exercise the failed bind path");
	await manager!.notifyInbound(KEY);
	await eventually(() => failures.length > 0, "bind failure did not reach the SLO recorder");
	expect(failures[0]?.code).toBe("persona_bind_failed");
	expect(Number.isFinite(failures[0]?.at)).toBe(true);
	expect(readinessFailures[0]).toMatchObject({ originKey: KEY, detail: "broker unavailable" });
	expect(Number.isFinite(readinessFailures[0]?.at)).toBe(true);
});

test("readiness observes bind failures even when the durable SLO recorder throws", async () => {
	const readinessFailures: Array<{ originKey: string; detail: string; at: number }> = [];
	await harness(new ScriptedSessionPort(), {}, undefined, {
		onSloEvent: () => {
			throw new Error("SLO storage unavailable");
		},
		onBindFailure: (input) => readinessFailures.push(input),
	});

	expect(() => manager!.recordBindFailure(KEY, "broker unavailable")).toThrow("SLO storage unavailable");
	expect(readinessFailures).toHaveLength(1);
	expect(readinessFailures[0]).toMatchObject({ originKey: KEY, detail: "broker unavailable" });
});

test("actor immediately dispatches durable inbound with one deterministic caller op-ref, then completes on tail terminal", async () => {
	const port = new ScriptedSessionPort({
		onSend: (input, scripted) => scripted.complete(input.opRef, "persona reply"),
	});
	const terminal: string[] = [];
	const sessionCreateSuccesses: Array<{ originKey: string; at: number }> = [];
	await harness(port, { terminal: (text) => terminal.push(text) }, undefined, {
		onSessionReady: (input) => sessionCreateSuccesses.push(input),
	});
	enqueue("m-1", "hello");
	await manager?.notifyInbound(KEY);
	await eventually(() => terminal.length === 1, "tail terminal did not reach lifecycle");

	expect(port.sends).toHaveLength(1);
	const send = port.sends[0]!;
	expect(send.text).toBe("hello");
	expect(send.opRef).toBe(latestOpRef);
	expect(send.opRef).toMatch(/^gw-p-[0-9a-f]{32}$/);
	expect(terminal).toEqual(["persona reply"]);
	expect(sessionCreateSuccesses[0]?.originKey).toBe(KEY);
	expect(Number.isFinite(sessionCreateSuccesses[0]?.at)).toBe(true);
	expect(database?.inbox.pendingCount(KEY)).toBe(0);
	expect(database?.inbox.turnRow(latestOpRef)).toMatchObject({ state: "done", turn_state: "done" });

	enqueue("m-2", "hello again");
	await manager?.notifyInbound(KEY);
	await eventually(() => terminal.length === 2, "reused session turn did not complete");
	expect(port.sends).toHaveLength(2);
	expect(sessionCreateSuccesses).toHaveLength(2);
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
		expect(database!.inbox.turnRow(send.opRef)?.turn_state).toBe(outcome === "complete" ? "done" : "accepted");
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
			Date.parse(database!.inbox.turnDispatchedAt(send.opRef)!),
		);
		expect(database!.inbox.turnRow(send.opRef)?.turn_state).toBe("done");
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
		expect(database!.inbox.turnRow(send.opRef)?.turn_state).toBe("accepted");
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
	const before = database!.inbox.turnRow(send.opRef);
	const stopping = manager!.stop();
	release();
	await stopping;
	expect(terminal).toEqual([]);
	expect(database!.inbox.turnRow(send.opRef)).toEqual(before);
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
	expect(database?.inbox.turnRows(latestOpRef)).toEqual(
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
		() => database?.inbox.turnRow(latestOpRef)?.turn_state === "done",
		"accepted turn did not reconcile terminal",
	);
});

test("a held turn in recovery_hold must not absorb new inbound as steers; new inbound remains pending for a fresh trigger", async () => {
	// Regression test for the bug where held ops absorbed all new inbound as steers,
	// preventing mentions and new messages from ever reaching the user (2026-10-05).
	const port = new GhostSendPort();
	const logs: string[] = [];
	await harness(port, {}, (line) => logs.push(line));

	enqueue("first-msg", "initial message");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sendAttempts.length === 1, "first send attempt did not start");
	const firstOpRef = port.sendAttempts[0]!.opRef;

	// The first send failed with ambiguous error (timeout)
	expect(port.sends).toHaveLength(0);
	await manager!.tick(KEY);

	// Wait for recovery_hold to be logged with operation_state_unknown
	await eventually(
		() => logs.some((line) => line.includes("recovery_hold") && line.includes("operation_state_unknown")),
		"turn did not enter recovery_hold",
	);

	// New message arrives while the turn is held
	enqueue("second-msg", "mentioned message");
	await manager?.notifyInbound(KEY);

	// With the fix: the second message should NOT be absorbed as a steer.
	// It remains pending so it can start a fresh trigger later.
	await Bun.sleep(50);

	// Verify: no steers (the message was not absorbed into the held turn)
	expect(port.steers).toHaveLength(0);
	// Only the first failed attempt, no new send yet
	expect(port.sends).toHaveLength(0);
	// The second message should still be pending in the database
	const pending = database?.inbox.pendingOldest(KEY);
	expect(pending?.message_id).toBe("second-msg");
	// It should not have been associated with the first turn
	expect(pending?.turn_op_ref).toBeNull();
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

	await manager!.tick(KEY);
	await eventually(() => notices.length === 1, "post-start failure did not reach the lifecycle");
	expect(notices[0]).toBe("[turn failed] prompt_failed: Agent run failed after execution started.");
	expect(port.sends).toHaveLength(1);
	// `prompt_failed` is not rebindable: a new session does not fix a runtime fault.
	expect(notices[0]).not.toContain("/new");
	const terminalLine = logs.find((line) => line.startsWith("terminal_failure "));
	expect(terminalLine).toContain(
		"code=prompt_failed provider_code=prompt_failed phase=post_start category=agent_runtime provenance=agent_failed",
	);
	expect(terminalLine).toContain("cause=Agent run failed after execution started");
	// The redacted cause joins gajae-code's local `SDK invocation failed` record only by these ids.
	expect(terminalLine).toContain(`command=command-${first.opRef} turn=turn-${first.opRef}`);
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
	database = await Store.open(join(home, "gateway.db"));
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
	const terminalLine2 = logs.find((line) => line.startsWith("terminal_failure "));
	expect(terminalLine2).toContain(
		"code=prompt_failed provider_code=prompt_failed phase=unknown category=unknown provenance=unknown",
	);
	expect(terminalLine2).toContain("cause=Agent run failed after execution started");
	expect(terminalLine2).toContain("open_tool=bash open_tool_elapsed_ms=300000");
	expect(logs.some((line) => line.startsWith("failed_turn_answer_recovered "))).toBe(true);
});

test("a failed turn whose answer already reached the tail neither re-reads the transcript nor blames a finished tool", async () => {
	const port = new ScriptedSessionPort();
	const failures: { notice: string; recovered?: string }[] = [];
	home = await mkdtemp(join(tmpdir(), "gajaeway-persona-session-"));
	database = await Store.open(join(home, "gateway.db"));
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

test("post-start failure captures real error message in terminal_failure log (#408)", async () => {
	const port = new ScriptedSessionPort();
	const notices: string[] = [];
	const logs: string[] = [];
	await harness(port, { failureError: (error) => notices.push(formatFailureNotice(error)) }, (line) => logs.push(line));
	enqueue("boom-failure", "work that throws an error");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	// Failure with error.code and outcome with failureCauseDiagnostic to test real cause capture (#408)
	port.fail(first.opRef, "boom", {
		code: "execution_error",
		outcome: {
			kind: "failed",
			phase: "post_start",
			category: "agent_runtime",
			provenance: "agent_failed",
			failureCauseDiagnostic: "Error boom", // Real cause from SDK (#408)
		},
	});
	await eventually(() => notices.length === 1, "error failure did not reach the lifecycle");
	expect(notices[0]).toBe("[turn failed] execution_error: boom");
	// Verify that the real error cause from SDK is captured in terminal_failure log
	const terminalLine = logs.find((line) => line.startsWith("terminal_failure "));
	expect(terminalLine).toContain(
		"code=execution_error provider_code=execution_error phase=post_start category=agent_runtime provenance=agent_failed",
	);
	// Should use failureCauseDiagnostic when available (#408)
	expect(terminalLine).toContain("cause=Error boom");
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
	expect(database!.inbox.turnRow(first.opRef)?.turn_state).toBe("accepted");
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	await manager!.tick(KEY);
	await eventually(() => manager!.state(KEY) === "idle", "retrying notice did not settle failure");
	expect(notices).toBe(2);
	expect(port.sends).toHaveLength(1);
	expect(database!.inbox.turnRow(first.opRef)?.turn_state).toBe("done");
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
});

test("reset consumes only the failed trigger context, preserving unrelated unread input", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	for (const messageId of ["failed-context", "unrelated-context"]) {
		database!.transaction((tx) =>
			database!.inbox.contextRecord(tx, {
				messageId,
				originKey: KEY,
				body: messageId,
				receivedAt: new Date().toISOString(),
			}),
		);
	}
	enqueue("failed-context", "failed-context");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	port.setFailedTurnEvidence(first.sessionId, "unsupported_input_status");
	port.fail(first.opRef);
	await eventually(() => manager!.state(KEY) === "idle", "failed trigger did not settle");
	expect(database!.inbox.contextDiagnostics(KEY).unread).toBe(1);
	expect(
		database!
			.transaction((tx) => database!.inbox.contextWindow(tx, KEY, "later-message"))
			.rows.map((row) => row.message_id),
	).toEqual(["unrelated-context"]);
});

// Exact failure recovery completes the trigger and resets only the NEXT session.
for (const reason of ["unsupported_input_status", "context_exhausted"] as const)
	test(`${reason} resets next without replay, including tools and visible output`, async () => {
		const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
		const failures: string[] = [];
		const released: string[] = [];
		await harness(port, {
			failure: (message) => {
				expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
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
		expect(database!.inbox.turnRow(first.opRef)).toMatchObject({ state: "done", turn_state: "done" });
		expect(failures).toEqual(["failed once"]);
		expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
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

test("provider quota exhaustion gives a safe notice and does not reset or rebind the session", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const notices: string[] = [];
	const logs: string[] = [];
	await harness(port, { failureError: (error) => notices.push(formatFailureNotice(error)) }, (line) => logs.push(line));
	const activeManager = manager;
	const activeDatabase = database;
	if (!activeManager || !activeDatabase) throw new Error("test harness did not initialize");
	enqueue("provider-quota", "work");
	await activeManager.notifyInbound(KEY);
	const first = port.sends[0];
	if (!first) throw new Error("quota turn was not dispatched");
	port.setFailedTurnEvidence(first.sessionId, "provider_quota_exhausted");
	port.fail(first.opRef, '402 "Grok Build usage balance exhausted"');
	await eventually(() => activeManager.state(KEY) === "idle", "quota failure did not settle");

	expect(notices).toEqual([
		"[turn failed] provider_quota_exhausted: model provider quota/billing is exhausted (HTTP 402); switch the model preset",
	]);
	expect(notices.join("\n")).not.toContain("Grok Build");
	expect(logs).toContain(`failed_turn_classified origin=${KEY} opRef=${first.opRef} reason=provider_quota_exhausted`);
	expect(logs.some((line) => line.startsWith("session_reset_after_failed_turn "))).toBe(false);
	expect(port.sends).toHaveLength(1);
	expect(activeDatabase.bindings.getSessionRecord(KEY)).toMatchObject({ epoch: 0, sessionId: first.sessionId });
	expect(activeDatabase.inbox.turnRow(first.opRef)).toMatchObject({ state: "done", turn_state: "done" });
});

test("a provider policy refusal names its category, keeps provider prose private and does not reset the session", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const notices: string[] = [];
	const logs: string[] = [];
	await harness(port, { failureError: (error) => notices.push(formatFailureNotice(error)) }, (line) => logs.push(line));
	const activeManager = manager;
	const activeDatabase = database;
	if (!activeManager || !activeDatabase) throw new Error("test harness did not initialize");
	enqueue("provider-refusal", "summarize this exploit write-up");
	await activeManager.notifyInbound(KEY);
	const first = port.sends[0];
	if (!first) throw new Error("refused turn was not dispatched");
	port.setFailedTurnEvidence(first.sessionId, "provider_refusal", undefined, "cyber");
	// 2026-10-13 shape: gajae-code redacts the post-start failure to its fixed sentence.
	port.fail(first.opRef, "Agent run failed after execution started.", {
		code: "agent_error",
		outcome: {
			code: "agent_error",
			providerCode: "untrusted_safety_stop",
			phase: "post_start",
			category: "unknown",
			provenance: "agent_failed",
		},
	});
	await eventually(() => activeManager.state(KEY) === "idle", "refusal did not settle");

	expect(notices).toEqual([
		"[turn failed] provider_refusal: the model provider refused this request under its usage policy (cyber); rephrase or narrow the request",
	]);
	expect(logs).toContain(`failed_turn_classified origin=${KEY} opRef=${first.opRef} reason=provider_refusal`);
	expect(logs.some((line) => line.startsWith("session_reset_after_failed_turn "))).toBe(false);
	expect(logs.some((line) => line.startsWith("agent_error_retry "))).toBe(false);
	expect(port.sends).toHaveLength(1);
	expect(activeDatabase.bindings.getSessionRecord(KEY)).toMatchObject({ epoch: 0, sessionId: first.sessionId });
	expect(activeDatabase.inbox.turnRow(first.opRef)).toMatchObject({ state: "done", turn_state: "done" });
});

// A submission failure that is not a wedged agent run (that one resets on the
// first failure, see WEDGED_SUBMISSION) resets only when it repeats.
const REPEATABLE_SUBMISSION = {
	code: "internal",
	phase: "submission",
	category: "provider_transport",
	provenance: "agent_failed",
};

test("a provider rate limit names its reset instant and is neither resent nor reset, even at submission", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const notices: string[] = [];
	const logs: string[] = [];
	await harness(port, { failureError: (error) => notices.push(formatFailureNotice(error)) }, (line) => logs.push(line));
	const activeManager = manager;
	const activeDatabase = database;
	if (!activeManager || !activeDatabase) throw new Error("test harness did not initialize");
	enqueue("provider-rate-limit", "work");
	await activeManager.notifyInbound(KEY);
	const first = port.sends[0];
	if (!first) throw new Error("rate-limited turn was not dispatched");
	port.setFailedTurnEvidence(first.sessionId, "provider_rate_limited", 7_501_000);
	const before = Date.now();
	// A submission-phase failure is otherwise resent once; a rate limit would only fail again.
	port.fail(first.opRef, '429 {"type":"error","error":{"type":"rate_limit_error"}}', {
		code: "error",
		outcome: { code: "error", phase: "submission", category: "provider_transport", provenance: "agent_failed" },
	});
	const after = Date.now();
	await eventually(() => activeManager.state(KEY) === "idle", "rate-limit failure did not settle");

	expect(notices).toHaveLength(1);
	const match = /^\[turn failed\] provider_rate_limited: model provider rate limit reached; retry after (\S+)$/.exec(
		notices[0] ?? "",
	);
	expect(match).not.toBeNull();
	const retryAt = Date.parse(match?.[1] ?? "");
	expect(retryAt).toBeGreaterThanOrEqual(before + 7_501_000);
	expect(retryAt).toBeLessThanOrEqual(after + 7_501_000);
	expect(notices.join("\n")).not.toContain("rate_limit_error");
	expect(logs).toContain(`failed_turn_classified origin=${KEY} opRef=${first.opRef} reason=provider_rate_limited`);
	expect(logs.some((line) => line.startsWith("session_reset_after_failed_turn "))).toBe(false);
	expect(port.sends).toHaveLength(1);
	expect(activeDatabase.bindings.getSessionRecord(KEY)).toMatchObject({ epoch: 0, sessionId: first.sessionId });
	expect(activeDatabase.inbox.turnRow(first.opRef)).toMatchObject({ state: "done", turn_state: "done" });
});

test("two consecutive internal submission failures reset the next inbound to a new epoch", async () => {
	const port = new ScriptedSessionPort({
		onBind: (input) => `session-e${input.epoch}`,
		onSend: (input, scripted) =>
			scripted.fail(input.opRef, "Prompt submission failed.", {
				code: "internal",
				outcome: REPEATABLE_SUBMISSION,
			}),
	});
	const logs: string[] = [];
	await harness(port, {}, (line) => logs.push(line));

	const first = await settleFailedInbound(port, "submission-1");
	const second = await settleFailedInbound(port, "submission-2");
	expect(first.sessionId).toBe("session-e0");
	expect(second.sessionId).toBe(first.sessionId);
	expect(database?.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(logs).toContain(
		`session_reset_after_failed_turn origin=${KEY} epoch=0 nextEpoch=1 opRef=${second.opRef} reason=repeated_submission_failure`,
	);

	const third = await settleFailedInbound(port, "submission-3");
	expect(third).toMatchObject({ sessionId: "session-e1", text: "submission-3" });
});

test("an agent-runtime submission failure without startedAt resets its session once after the safe retry", async () => {
	const port = new ScriptedSessionPort({
		onBind: (input) => `session-e${input.epoch}`,
		onSend: (input, scripted) =>
			scripted.fail(input.opRef, "Prompt submission failed.", {
				code: "internal",
				outcome: { code: "internal", phase: "submission", category: "agent_runtime", provenance: "agent_failed" },
				omitStartedAt: true,
			}),
	});
	const logs: string[] = [];
	await harness(port, {}, (line) => logs.push(line));

	const first = await settleFailedInbound(port, "submission-1");
	const second = await settleFailedInbound(port, "submission-2");
	expect(first.sessionId).toBe("session-e0");
	expect(second.sessionId).toBe("session-e1");
	expect(database?.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(logs).toContain(
		`session_reset_after_failed_turn origin=${KEY} epoch=0 nextEpoch=1 opRef=${first.opRef} reason=submission_wedged`,
	);
	expect(logs).not.toContain(
		`session_reset_after_failed_turn origin=${KEY} epoch=1 nextEpoch=2 opRef=${second.opRef} reason=submission_wedged`,
	);
	expect(logs.some((line) => line.startsWith("failed_turn_reset_capped ") && line.includes(second.opRef))).toBe(true);

	const third = await settleFailedInbound(port, "submission-3");
	expect(third).toMatchObject({ sessionId: "session-e1", text: "submission-3" });
});

test("a healthy turn clears consecutive internal submission failures", async () => {
	const port = new ScriptedSessionPort({
		onBind: (input) => `session-e${input.epoch}`,
		onSend: (input, scripted) => {
			if (input.text === "healthy") scripted.complete(input.opRef, "healthy reply");
			else
				scripted.fail(input.opRef, "Prompt submission failed.", {
					code: "internal",
					outcome: REPEATABLE_SUBMISSION,
				});
		},
	});
	const logs: string[] = [];
	await harness(port, {}, (line) => logs.push(line));
	const activeManager = manager;
	const activeDatabase = database;
	if (!activeManager || !activeDatabase) throw new Error("test harness did not initialize");
	const first = await settleFailedInbound(port, "submission-before-healthy");
	const healthy = "healthy";
	enqueue(healthy, healthy);
	await activeManager.notifyInbound(KEY);
	const successful = port.sends.at(-1);
	if (!successful) throw new Error("healthy turn was not dispatched");
	await eventually(
		() => activeDatabase.inbox.turnRow(successful.opRef)?.turn_state === "done" && activeManager.state(KEY) === "idle",
		"healthy turn did not settle",
	);
	const last = await settleFailedInbound(port, "submission-after-healthy");

	expect(successful.sessionId).toBe(first.sessionId);
	expect(last.sessionId).toBe(first.sessionId);
	expect(activeDatabase.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	expect(logs.some((line) => line.startsWith("session_reset_after_failed_turn "))).toBe(false);
});

test("repeated submission failures bypass the reset cap to recover from stuck sessions (#477)", async () => {
	const port = new ScriptedSessionPort({
		onBind: (input) => `session-e${input.epoch}`,
		onSend: (input, scripted) =>
			scripted.fail(input.opRef, "Prompt submission failed.", {
				code: "internal",
				outcome: REPEATABLE_SUBMISSION,
			}),
	});
	const logs: string[] = [];
	await harness(port, {}, (line) => logs.push(line));

	const sends: (typeof port.sends)[number][] = [];
	for (let index = 0; index < 6; index++) sends.push(await settleFailedInbound(port, `stuck-submission-${index + 1}`));

	expect(sends.slice(0, 2).map((send) => send.sessionId)).toEqual(["session-e0", "session-e0"]);
	expect(sends.slice(2, 4).map((send) => send.sessionId)).toEqual(["session-e1", "session-e1"]);
	expect(sends.slice(4, 6).map((send) => send.sessionId)).toEqual(["session-e2", "session-e2"]);
	expect(database?.bindings.getSessionRecord(KEY)?.epoch).toBe(3);
	expect(logs.filter((line) => line.startsWith("failed_turn_reset_capped "))).toEqual([]);
});

const SUBMISSION_BUSY = {
	code: "internal",
	outcome: { code: "internal", phase: "submission", category: "agent_runtime", provenance: "agent_failed" },
} as const;

test("#424: a prompt that failed in its submission phase is re-sent once and answered without a failure notice", async () => {
	let sends = 0;
	const port = new ScriptedSessionPort({
		onSend: (input, scripted) => {
			sends++;
			if (sends === 1) scripted.fail(input.opRef, "Prompt submission failed.", SUBMISSION_BUSY);
			else scripted.complete(input.opRef, "answer after retry");
		},
	});
	const failures: string[] = [];
	const terminal: string[] = [];
	const logs: string[] = [];
	await harness(
		port,
		{ failure: (message) => failures.push(message), terminal: (text) => terminal.push(text) },
		(line) => logs.push(line),
	);
	enqueue("busy-1", "hello while busy");
	await manager!.notifyInbound(KEY);
	await eventually(() => terminal.length === 1 && manager!.state(KEY) === "idle", "retried turn was not answered");

	expect(terminal).toEqual(["answer after retry"]);
	expect(failures).toEqual([]);
	expect(port.sends).toHaveLength(2);
	const [first, retry] = port.sends;
	expect(retry!.opRef).not.toBe(first!.opRef);
	expect(retry!.sessionId).toBe(first!.sessionId);
	expect(retry!.text).toBe("hello while busy");
	expect(database!.inbox.turnRow(retry!.opRef)).toMatchObject({ state: "done", turn_state: "done" });
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	expect(logs.filter((line) => line.startsWith("submission_retry "))).toHaveLength(1);
});

test("#424: a prompt that fails its submission phase twice gets exactly one failure notice", async () => {
	const port = new ScriptedSessionPort({
		onSend: (input, scripted) => scripted.fail(input.opRef, "Prompt submission failed.", SUBMISSION_BUSY),
	});
	const failures: string[] = [];
	await harness(port, { failure: (message) => failures.push(message) });
	const last = await settleFailedInbound(port, "busy-twice");

	expect(port.sends.map((send) => send.text)).toEqual(["busy-twice", "busy-twice"]);
	expect(failures).toHaveLength(1);
	expect(database!.inbox.turnRow(last.opRef)).toMatchObject({ state: "done", turn_state: "done" });
});

test("#424: a failure outside the submission phase is reported without a re-send", async () => {
	const port = new ScriptedSessionPort({
		onSend: (input, scripted) =>
			scripted.fail(input.opRef, "model error", {
				code: "internal",
				outcome: { code: "internal", phase: "model", category: "agent_runtime", provenance: "agent_failed" },
			}),
	});
	const failures: string[] = [];
	await harness(port, { failure: (message) => failures.push(message) });
	await settleFailedInbound(port, "model-failure");

	expect(port.sends).toHaveLength(1);
	expect(failures).toHaveLength(1);
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
			database = await Store.open(join(home, "gateway.db"));
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
		expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
		expect(port.sends).toHaveLength(2);
		expect(failures[1]).toBe("second failure");
		expect(database!.inbox.turnRow(second.opRef)?.turn_state).toBe("done");
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
		expect(database!.inbox.turnRows(first.opRef).find((row) => row.message_id === "steer")).toMatchObject({
			turn_role: "steer",
			turn_op_ref: first.opRef,
			turn_state: held ? "bound" : "done",
		});
		if (held) expect(database!.inbox.steersHeld(first.opRef).map((row) => row.message_id)).toEqual(["steer"]);
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
		expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
		expect(port.sends).toHaveLength(1);
		expect(failures).toEqual(["429 rate limited"]);
	});

const WEDGED_SUBMISSION = {
	phase: "submission",
	provenance: "agent_failed",
	category: "agent_runtime",
	providerCode: "internal",
};

test("a wedged submission resets the next binding once without transcript evidence", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	// gjc reports a never-started prompt without startedAt.
	port.omitStartedAt = true;
	const failures: string[] = [];
	const logs: string[] = [];
	await harness(port, { failure: (message) => failures.push(message) }, (line) => logs.push(line));
	enqueue("wedged", "first");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	port.fail(first.opRef, "Prompt submission failed.", { code: "internal", outcome: WEDGED_SUBMISSION });
	// #424 re-sends a never-started prompt once on the same session; only a wedge that survives it resets.
	await eventually(() => port.sends.length === 2, "wedged submission was not re-sent");
	const retry = port.sends[1]!;
	expect(retry).toMatchObject({ text: "first", sessionId: "session-e0" });
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	expect(failures).toEqual([]);
	port.fail(retry.opRef, "Prompt submission failed.", { code: "internal", outcome: WEDGED_SUBMISSION });
	await eventually(() => manager!.state(KEY) === "idle", "wedged submission did not settle");
	expect(port.failureEvidenceProbes).toEqual([]);
	expect(failures).toEqual(["internal: Prompt submission failed."]);
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(logs.some((line) => line.includes("reason=submission_wedged"))).toBe(true);
	enqueue("next", "second");
	await manager!.notifyInbound(KEY);
	expect(port.sends.map((send) => send.text)).toEqual(["first", "first", "second"]);
	expect(port.sends[2]!.sessionId).toBe("session-e1");
	// The cap holds until a healthy turn: a second wedge on the fresh session stays put.
	port.fail(port.sends[2]!.opRef, "Prompt submission failed.", { code: "internal", outcome: WEDGED_SUBMISSION });
	await eventually(() => port.sends.length === 4, "capped wedge was not re-sent");
	port.fail(port.sends[3]!.opRef, "Prompt submission failed.", { code: "internal", outcome: WEDGED_SUBMISSION });
	await eventually(() => manager!.state(KEY) === "idle", "capped wedge did not settle");
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
});

for (const [label, detail] of [
	["aborted submission", { code: "aborted", outcome: { ...WEDGED_SUBMISSION, providerCode: "aborted" } }],
	[
		"provider transport submission",
		{ code: "provider_down", outcome: { ...WEDGED_SUBMISSION, category: "provider_transport" } },
	],
	["post-start agent failure", { code: "internal", outcome: { ...WEDGED_SUBMISSION, phase: "post_start" } }],
] as const)
	test(`${label} never resets without transcript evidence`, async () => {
		const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
		await harness(port);
		enqueue("kept", "original");
		await manager!.notifyInbound(KEY);
		port.fail(port.sends[0]!.opRef, "failed", detail);
		// A submission-phase failure is re-sent once (#424); fail the re-send the same way.
		if (detail.outcome.phase === "submission") {
			await eventually(() => port.sends.length === 2, "submission failure was not re-sent");
			port.fail(port.sends[1]!.opRef, "failed", detail);
		}
		await eventually(() => manager!.state(KEY) === "idle", "failure did not settle");
		expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
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
		expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
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
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
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

test("#41: ending a retired host that still runs an async sub-lane leaves a jobs-lost record naming it", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const logs: string[] = [];
	await harness(port, undefined, (line) => logs.push(line));
	enqueue("review", "start a background review");
	await manager!.notifyInbound(KEY);
	// The turn ends normally while the sub-lane it started keeps running in the host.
	port.hostJobs.set("session-e0", [{ id: "0-ArchitectReview", type: "task", label: "review #61" }]);
	port.complete(port.sends[0]!.opRef, "started the review");
	await manager!.tick(KEY);
	await manager!.reset(KEY, JSON.stringify(ORIGIN));
	await manager!.tick(KEY);
	expect(port.closes.map((c) => c.sessionId)).toEqual(["session-e0"]);
	const lost = logs.filter((l) => l.startsWith("retired_session_host_jobs_lost"));
	expect(lost).toHaveLength(1);
	expect(lost[0]).toContain("session=session-e0");
	expect(lost[0]).toContain("count=1");
	expect(lost[0]).toContain("task:0-ArchitectReview:review #61");
});

test("#41: an unreadable job list is recorded as unknown, never as no jobs", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const logs: string[] = [];
	await harness(port, undefined, (line) => logs.push(line));
	enqueue("first", "first");
	await manager!.notifyInbound(KEY);
	port.hostJobs.set("session-e0", new Error("broker_unavailable"));
	port.complete(port.sends[0]!.opRef, "answered");
	await manager!.tick(KEY);
	await manager!.reset(KEY, JSON.stringify(ORIGIN));
	await manager!.tick(KEY);
	const lost = logs.filter((l) => l.startsWith("retired_session_host_jobs_lost"));
	expect(lost).toHaveLength(1);
	expect(lost[0]).toContain("count=unknown");
	expect(lost[0]).toContain("broker_unavailable");
});

test("#41: a retired host with no running jobs ends without a jobs-lost record", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const logs: string[] = [];
	await harness(port, undefined, (line) => logs.push(line));
	enqueue("first", "first");
	await manager!.notifyInbound(KEY);
	port.complete(port.sends[0]!.opRef, "answered");
	await manager!.tick(KEY);
	await manager!.reset(KEY, JSON.stringify(ORIGIN));
	await manager!.tick(KEY);
	expect(port.closes.map((c) => c.sessionId)).toEqual(["session-e0"]);
	expect(logs.some((l) => l.startsWith("retired_session_host_jobs_lost"))).toBe(false);
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

for (const restart of [false, true])
	for (const code of ["session_unavailable", "endpoint_stale", "not_found"] as const)
		test(`post-admission endpoint loss retains native operation (restart=${restart}, code=${code})`, async () => {
			class AdmissionThenLossPort extends ScriptedSessionPort {
				unknown = true;
				terminations = 0;
				override async send(input: Parameters<ScriptedSessionPort["send"]>[0]) {
					const receipt = await super.send(input);
					if (this.sends.length === 1)
						throw new GjcCliError("gjc sdk turn.prompt reported failure", 0, "", {
							code,
							message: "Broker endpoint lost after native admission.",
						});
					return receipt;
				}
				override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
					const report = await super.status(input);
					return this.unknown ? { ...report, status: { status: "unknown" as const }, summaryCompleted: false } : report;
				}
				override async terminateHost(input: Parameters<ScriptedSessionPort["terminateHost"]>[0]) {
					this.terminations++;
					return await super.terminateHost(input);
				}
				async nativeStatus(input: Parameters<ScriptedSessionPort["status"]>[0]) {
					return await super.status(input);
				}
			}
			const port = new AdmissionThenLossPort({ onBind: (input) => `session-e${input.epoch}` });
			const terminal: string[] = [];
			const failures: string[] = [];
			await harness(port, { terminal: (text) => terminal.push(text), failure: (text) => failures.push(text) });
			enqueue("admitted", "execute exactly once");
			await manager!.notifyInbound(KEY);
			const first = port.sends[0]!;
			if (restart) {
				await manager!.stop();
				database!.close();
				database = await Store.open(join(home, "gateway.db"));
				registerFixtureBindings(port);
				manager = new PersonaSessionManager({
					database,
					port,
					instanceId: "instance-test",
					repo: join(home, "workspace"),
					onTurnStart: ({ trigger }) => ({
						text: trigger.body,
						onTerminal: ({ text }) => {
							terminal.push(text);
						},
						onFailure: ({ error }) => {
							failures.push(error.message);
						},
					}),
				});
			}
			enqueue("admitted-queued", "remain queued", "lane_report");
			await manager!.notifyInbound(KEY);
			for (let sweep = 0; sweep < 3; sweep++) {
				await manager!.recover();
				await manager!.tick(KEY);
			}
			expect(
				(await port.nativeStatus({ sessionId: first.sessionId, repo: join(home, "workspace"), opRef: first.opRef }))
					.status.status,
			).toBe("in_flight");
			expect(database!.inbox.turnRow(first.opRef)).toMatchObject({
				state: "pending",
				turn_state: "bound",
				turn_op_ref: first.opRef,
				bound_session_id: first.sessionId,
				turn_epoch: 0,
				terminal_delivery_id: null,
			});
			expect(database!.bindings.getSessionRecord(KEY)).toMatchObject({ epoch: 0, sessionId: first.sessionId });
			expect(port.sends).toHaveLength(1);
			expect(port.binds).toHaveLength(1);
			expect(port.resumes).toHaveLength(0);
			expect(port.closes).toHaveLength(0);
			expect(port.terminations).toBe(0);
			expect(terminal).toEqual([]);
			expect(failures).toEqual([]);
			port.unknown = false;
			port.complete(first.opRef, "exact admitted native result");
			await manager!.recover();
			await manager!.tick(KEY);
			await eventually(
				() => database!.inbox.turnRow(first.opRef)?.turn_state === "done",
				"admitted native terminal did not settle",
			);
			await manager!.recover();
			await manager!.tick(KEY);
			expect(terminal).toEqual(["exact admitted native result"]);
			expect(failures).toEqual([]);
			expect(port.sends.filter((send) => send.opRef === first.opRef)).toHaveLength(1);
			expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
		});

/** Structured prompt-command error used to exercise the ambiguous post-send path. */
function sessionNotFound(): GjcCliError {
	return new GjcCliError("gjc sdk turn.prompt reported failure", 0, "", {
		code: "session_not_found",
		message: "The broker does not know this session.",
	});
}

for (const acceptedBeforeError of [false, true] as const)
	test(`structured session_not_found after send stays ambiguous (acceptedBeforeError=${acceptedBeforeError})`, async () => {
		const acceptedPrompts: string[] = [];
		class StructuredSessionNotFoundPort extends ScriptedSessionPort {
			rejectStructuredNotFound = false;

			override async send(input: Parameters<ScriptedSessionPort["send"]>[0]) {
				if (this.rejectStructuredNotFound && input.text === "possibly accepted") {
					if (acceptedBeforeError) await super.send(input);
					else this.sendAttempts.push(input);
					throw sessionNotFound();
				}
				return await super.send(input);
			}
		}
		const port = new StructuredSessionNotFoundPort({
			onBind: (input) => `session-e${input.epoch}`,
			onSend: (input) => {
				if (input.text === "possibly accepted") acceptedPrompts.push(input.text);
			},
		});
		const logs: string[] = [];
		await harness(port, {}, (line) => logs.push(line));
		await establishStoredSession(port, `seed-session-not-found-${acceptedBeforeError}`);
		port.rejectStructuredNotFound = true;
		const recoveryCalls = trackRecoveryCalls();
		const text = "possibly accepted";
		enqueue(`session-not-found-${acceptedBeforeError}`, text);
		await manager!.notifyInbound(KEY);
		if (acceptedBeforeError)
			await eventually(() => acceptedPrompts.length === 1, "accepted prompt was not recorded before the command error");

		const attempted = port.sendAttempts.filter((attempt) => attempt.text === text);
		expect(attempted).toHaveLength(1);
		expect(port.sends.filter((send) => send.text === text)).toHaveLength(acceptedBeforeError ? 1 : 0);
		expect(acceptedPrompts).toHaveLength(acceptedBeforeError ? 1 : 0);
		expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
		expect(port.binds).toHaveLength(1);
		expect(recoveryCalls()).toBe(0);
		expect(logs.some((line) => line.startsWith("persona_send_ambiguous") && line.includes(attempted[0]!.opRef))).toBe(
			true,
		);
		expect(logs.some((line) => line.startsWith("send_session_disowned action=inline_rebind"))).toBe(false);
		expect(database!.inbox.turnRow(attempted[0]!.opRef)?.turn_state).toBe(acceptedBeforeError ? "accepted" : "bound");
	});

for (const proof of [
	{ name: "broker reports not live", result: { live: false, disowned: false } },
	{ name: "broker reports disowned", result: { live: undefined, disowned: true } },
])
	test(`plain tail attach failure rebinds the stored session when ${proof.name}`, async () => {
		class LivenessProvenAttachPort extends ScriptedSessionPort {
			storedSessionId: string | undefined;

			override async attachTail(input: Parameters<ScriptedSessionPort["attachTail"]>[0]) {
				if (input.sessionId === this.storedSessionId) throw new Error("host hello did not arrive");
				return await super.attachTail(input);
			}

			override async liveness(input: Parameters<ScriptedSessionPort["liveness"]>[0]) {
				if (input.sessionId === this.storedSessionId) return proof.result;
				return await super.liveness(input);
			}
		}
		const port = new LivenessProvenAttachPort({ onBind: (input) => `session-e${input.epoch}` });
		const logs: string[] = [];
		await harness(port, {}, (line) => logs.push(line));
		const stored = await establishStoredSession(port, `attach-${proof.name.replaceAll(" ", "-")}`);
		for (const tail of port.tailsOf(stored.sessionId)) await tail.close();
		port.storedSessionId = stored.sessionId;
		const recoveryCalls = trackRecoveryCalls();
		enqueue(`retry-${proof.name.replaceAll(" ", "-")}`, "one delivery");
		await manager!.notifyInbound(KEY);
		await eventually(() => port.sends.length === 2, "liveness-proven dead binding did not dispatch on replacement");

		expect(port.sends[1]).toMatchObject({ sessionId: "session-e1", text: "one delivery" });
		expect(
			port.sendAttempts.filter((attempt) => attempt.text === "one delivery").map((attempt) => attempt.sessionId),
		).toEqual(["session-e1"]);
		expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
		expect(recoveryCalls()).toBe(0);
		expect(logs.some((line) => line.startsWith("recovery_requeue_unaccepted"))).toBe(false);
		expect(
			logs.some(
				(line) =>
					line.startsWith("send_session_disowned action=inline_rebind stage=attach") && line.includes("nextEpoch=1"),
			),
		).toBe(true);
	});

for (const behavior of ["no_probe", "live", "unknown", "failed_probe", "attach_authority", "probe_authority"] as const)
	test(`plain tail attach failure does not rebind without broker proof (${behavior})`, async () => {
		class UnprovenAttachPort extends ScriptedSessionPort {
			storedSessionId: string | undefined;

			override async attachTail(input: Parameters<ScriptedSessionPort["attachTail"]>[0]) {
				if (input.sessionId === this.storedSessionId) {
					if (behavior === "attach_authority") throw new BrokerAuthorityError("authority_mismatch");
					throw new Error("host hello did not arrive");
				}
				return await super.attachTail(input);
			}

			override async liveness(input: Parameters<ScriptedSessionPort["liveness"]>[0]) {
				if (input.sessionId === this.storedSessionId) {
					if (behavior === "failed_probe") throw new Error("liveness transport failed");
					if (behavior === "probe_authority") throw new BrokerAuthorityError("authority_mismatch");
					if (behavior === "live") return { live: true, disowned: false };
					return { live: undefined, disowned: false };
				}
				return await super.liveness(input);
			}
		}
		const port = new UnprovenAttachPort({ onBind: (input) => `session-e${input.epoch}` });
		const logs: string[] = [];
		await harness(port, {}, (line) => logs.push(line));
		const stored = await establishStoredSession(port, `unproven-${behavior}`);
		for (const tail of port.tailsOf(stored.sessionId)) await tail.close();
		port.storedSessionId = stored.sessionId;
		if (behavior === "no_probe") Object.defineProperty(port, "liveness", { value: undefined });
		enqueue(`unproven-retry-${behavior}`, "do not replay");
		const failure = await manager!.notifyInbound(KEY).catch((error: unknown) => error);

		if (behavior === "attach_authority" || behavior === "probe_authority")
			expect(failure).toBeInstanceOf(BrokerAuthorityError);
		else {
			// No prompt was sent (#675): the trigger returns to pending for a backoff retry.
			expect(failure).toBeUndefined();
			expect(database!.inbox.pendingOldest(KEY)).toMatchObject({
				message_id: `unproven-retry-${behavior}`,
				turn_state: null,
			});
			expect(logs.some((line) => line.startsWith("send_pre_send_requeue stage=attach"))).toBe(true);
		}
		expect(port.sends).toHaveLength(1);
		expect(port.binds).toHaveLength(1);
		expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
		expect(logs.some((line) => line.startsWith("send_session_disowned action=inline_rebind"))).toBe(false);
	});

test("session_not_found text without the broker error code remains ambiguous and is never replayed", async () => {
	class AmbiguousSessionTextPort extends ScriptedSessionPort {
		rejectAmbiguous = false;

		override async send(input: Parameters<ScriptedSessionPort["send"]>[0]) {
			if (!this.rejectAmbiguous) return await super.send(input);
			this.sendAttempts.push(input);
			throw new GjcCliError("turn.prompt failed: session_not_found", 0, "", { code: "timeout" });
		}
	}
	const port = new AmbiguousSessionTextPort({ onBind: (input) => `session-e${input.epoch}` });
	const logs: string[] = [];
	await harness(port, {}, (line) => logs.push(line));
	enqueue("ambiguous-session-seed", "seed turn");
	await manager!.notifyInbound(KEY);
	const stored = port.sends[0]!;
	port.complete(stored.opRef, "seed answer");
	await manager!.tick(KEY);
	await eventually(() => database!.inbox.turnRow(stored.opRef)?.turn_state === "done", "seed turn did not settle");
	port.rejectAmbiguous = true;
	enqueue("ambiguous-session-text", "do not duplicate");
	await manager!.notifyInbound(KEY);

	expect(port.sendAttempts).toHaveLength(2);
	expect(port.sends).toHaveLength(1);
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	expect(logs.some((line) => line.startsWith("persona_send_ambiguous"))).toBe(true);
	expect(logs.some((line) => line.startsWith("send_session_disowned action=inline_rebind"))).toBe(false);
});

for (const authorityFailure of ["direct", "probe"] as const)
	test(`stale held steer propagates BrokerAuthorityError from ${authorityFailure}`, async () => {
		let mode: "initial" | "direct" | "probe" = "initial";
		class AuthorityFailurePort extends ScriptedSessionPort {
			override async steer(input: Parameters<ScriptedSessionPort["steer"]>[0]) {
				this.steers.push(input);
				if (mode === "direct") throw new BrokerAuthorityError("authority_mismatch");
				throw new Error("unknown transport outcome");
			}

			override async liveness(input: Parameters<ScriptedSessionPort["liveness"]>[0]) {
				if (mode === "probe") throw new BrokerAuthorityError("authority_mismatch");
				return await super.liveness(input);
			}
		}
		const port = new AuthorityFailurePort({ onBind: (input) => `session-e${input.epoch}` });
		await harness(port);
		enqueue(`held-steer-authority-${authorityFailure}`, "original");
		await manager!.notifyInbound(KEY);
		const first = port.sends[0]!;
		enqueue(`held-steer-${authorityFailure}`, "uncertain steer");
		await manager!.notifyInbound(KEY);
		expect(database!.inbox.steersHeld(first.opRef).map((row) => row.message_id)).toEqual([
			`held-steer-${authorityFailure}`,
		]);

		port.complete(first.opRef, "answer");
		await manager!.tick(KEY);
		await eventually(
			() => database!.inbox.turnRow(first.opRef)?.turn_state === "done",
			"trigger did not become terminal before the stale steer replay",
		);
		mode = authorityFailure;
		const priorSteers = port.steers.length;
		const failure = await manager!.tick(KEY).catch((error: unknown) => error);

		expect(failure).toBeInstanceOf(BrokerAuthorityError);
		expect(port.steers).toHaveLength(priorSteers + 1);
		expect(port.sends).toHaveLength(1);
		expect(port.binds).toHaveLength(1);
		expect(database!.inbox.steersHeld(first.opRef).map((row) => row.message_id)).toEqual([
			`held-steer-${authorityFailure}`,
		]);
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
		expect(database!.inbox.turnRow(first.opRef)?.turn_state).toBe("done");
		expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(exact ? 1 : 0);
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
		const before = database!.bindings.getSessionRecord(KEY)!.epoch;
		enqueue("failed-2", "second");
		await manager!.notifyInbound(KEY);
		const last = port.sends.at(-1)!;
		port.setFailedTurnEvidence(last.sessionId, "unsupported_input_status");
		port.fail(last.opRef);
		await eventually(() => manager!.state(KEY) === "idle", "second failure incomplete");
		expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(before + 1);
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
		expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
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
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
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
		database!.core.metaSet(key, value);
		const before = database!.inbox.turnRow(first.opRef);
		expect(
			database!.transaction((tx) =>
				resetFailedTurn(database!, tx, {
					originKey: KEY,
					epoch: 0,
					sessionId: first.sessionId,
					opRef: first.opRef,
					triggerMessageId: "cap",
				}),
			),
		).toBeUndefined();
		expect(database!.inbox.turnRow(first.opRef)).toEqual(before);
		expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	});

test("atomic reset rolls completion and markers back when epoch advancement fails", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("rollback", "original");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	const proof = seedPersonaPrompt(first);
	const input = {
		originKey: KEY,
		epoch: 0,
		sessionId: first.sessionId,
		opRef: first.opRef,
		triggerMessageId: "rollback",
	};
	const before = database!.inbox.turnRow(first.opRef);
	const original = database!.bindings.rotateEpoch.bind(database!.bindings);
	database!.bindings.rotateEpoch = () => {
		throw new Error("injected transaction failure");
	};
	expect(() => database!.transaction((tx) => resetFailedTurn(database!, tx, input))).toThrow(
		"injected transaction failure",
	);
	expect(database!.inbox.turnRow(first.opRef)).toEqual(before);
	expect(proof()).toMatchObject({ state: "complete", resultJson: JSON.stringify(BROKER_PROMPT_BUSY_REFUSAL) });
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	database!.bindings.rotateEpoch = original;
	expect(database!.transaction((tx) => resetFailedTurn(database!, tx, input))).toBe(1);
	expect(database!.inbox.turnRow(first.opRef)?.turn_state).toBe("done");
	expect(proof()).toBeUndefined();
	expect(database!.transaction((tx) => resetFailedTurn(database!, tx, input))).toBeUndefined();
});

test("bounded shutdown reconciliation leaves a nonterminal accepted turn durable", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("m-1", "still running");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "accepted turn did not start before shutdown");
	await manager?.drain(0);
	expect(manager?.state(KEY)).toBe("turn-running");
	expect(database?.inbox.turnRow(latestOpRef)).toMatchObject({ state: "pending", turn_state: "accepted" });
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
	const before = database!.inbox.turnRow(latestOpRef);
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
	expect(database!.inbox.turnRow(latestOpRef)).toEqual(before);
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
	expect(database?.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(database?.inbox.turnRow(latestOpRef)).toMatchObject({ state: "pending", turn_state: "accepted" });
	port.complete(first.opRef, "stale output");
	await manager!.tick(KEY);
	await eventually(() => database?.inbox.turnRow(latestOpRef)?.turn_state === "done", "retired turn did not reconcile");
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

/** A broker restart dropped its registry entry, without proving native producer exit. */
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

test("a /new-retired turn survives broker registry loss until its original native terminal", async () => {
	const port = new DroppedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const logs: string[] = [];
	await harness(port, {}, (line) => logs.push(line));
	enqueue("m-1", "old turn");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "turn did not start");
	const old = port.sends[0]!;
	await manager?.reset(KEY, JSON.stringify(ORIGIN));
	expect(database?.inbox.turnRow(old.opRef)).toMatchObject({ state: "pending", turn_state: "accepted" });
	const closesBeforeLoss = port.closes.length;

	port.dropped.add(old.sessionId);
	await manager!.tick(KEY);
	expect(database?.inbox.turnRow(old.opRef)).toMatchObject({
		state: "pending",
		turn_state: "accepted",
		turn_op_ref: old.opRef,
		bound_session_id: old.sessionId,
		turn_epoch: 0,
		terminal_delivery_id: null,
	});
	expect(database?.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(port.closes).toHaveLength(closesBeforeLoss);
	expect(logs.some((line) => line.startsWith("retired_turn_closed") && line.includes(old.opRef))).toBe(false);
	expect(port.sends).toHaveLength(1);
	port.dropped.delete(old.sessionId);
	expect(
		(await port.status({ sessionId: old.sessionId, repo: join(home, "workspace"), opRef: old.opRef })).status.status,
	).toBe("in_flight");
	port.complete(old.opRef, "original retired result");
	await manager!.tick(KEY);
	await eventually(
		() => database?.inbox.turnRow(old.opRef)?.turn_state === "done",
		"retired native terminal did not settle",
	);
	expect(database?.inbox.turnRow(old.opRef)?.terminal_delivery_id).toBe(JSON.stringify({ none: "retired" }));
	expect(port.sends).toHaveLength(1);
});

for (const restart of [false, true])
	for (const status of ["unknown", "unavailable"] as const)
		for (const registry of ["nonlive", "disowned"] as const)
			test(`accepted registry loss retains native operation (restart=${restart}, status=${status}, registry=${registry})`, async () => {
				class RegistryLossPort extends ScriptedSessionPort {
					lost = false;
					terminations = 0;
					override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
						if (this.lost && status === "unavailable") throw endpointStale();
						const report = await super.status(input);
						return this.lost ? { ...report, status: { status: "unknown" as const }, summaryCompleted: false } : report;
					}
					override async liveness(input: Parameters<ScriptedSessionPort["liveness"]>[0]) {
						if (this.lost)
							return { live: registry === "nonlive" ? false : undefined, disowned: registry === "disowned" };
						return await super.liveness(input);
					}
					override async terminateHost(input: Parameters<ScriptedSessionPort["terminateHost"]>[0]) {
						this.terminations++;
						return await super.terminateHost(input);
					}
					async nativeStatus(input: Parameters<ScriptedSessionPort["status"]>[0]) {
						return await super.status(input);
					}
				}
				const port = new RegistryLossPort({ onBind: (input) => `session-e${input.epoch}` });
				const terminal: string[] = [];
				const failures: string[] = [];
				await harness(port, { terminal: (text) => terminal.push(text), failure: (text) => failures.push(text) });
				enqueue("registry-original", "execute exactly once");
				await manager!.notifyInbound(KEY);
				await eventually(() => port.sends.length === 1, "accepted turn did not start");
				const first = port.sends[0]!;
				expect(database!.inbox.turnRow(first.opRef)?.turn_state).toBe("accepted");
				port.lost = true;
				if (restart) {
					await manager!.stop();
					database!.close();
					database = await Store.open(join(home, "gateway.db"));
					registerFixtureBindings(port);
					manager = new PersonaSessionManager({
						database,
						port,
						instanceId: "instance-test",
						repo: join(home, "workspace"),
						onTurnStart: ({ trigger }) => ({
							text: trigger.body,
							onTerminal: ({ text }) => {
								terminal.push(text);
							},
							onFailure: ({ error }) => {
								failures.push(error.message);
							},
						}),
					});
				}
				enqueue("registry-queued", "do not dispatch while original runs", "lane_report");
				await manager!.notifyInbound(KEY);
				for (let sweep = 0; sweep < 3; sweep++) {
					await manager!.recover();
					await manager!.tick(KEY);
				}
				expect(
					(await port.nativeStatus({ sessionId: first.sessionId, repo: join(home, "workspace"), opRef: first.opRef }))
						.status.status,
				).toBe("in_flight");
				expect(database!.inbox.turnRow(first.opRef)).toMatchObject({
					state: "pending",
					turn_state: "accepted",
					turn_op_ref: first.opRef,
					bound_session_id: first.sessionId,
					turn_epoch: 0,
					terminal_delivery_id: null,
				});
				expect(database!.bindings.getSessionRecord(KEY)).toMatchObject({ epoch: 0, sessionId: first.sessionId });
				expect(port.sends).toHaveLength(1);
				expect(port.binds).toHaveLength(1);
				expect(port.resumes).toHaveLength(0);
				expect(port.terminations).toBe(0);
				expect(port.closes).toHaveLength(0);
				expect(terminal).toEqual([]);
				expect(failures).toEqual([]);
				port.lost = false;
				port.complete(first.opRef, "exact original native output");
				await manager!.recover();
				await manager!.tick(KEY);
				await eventually(
					() => database!.inbox.turnRow(first.opRef)?.turn_state === "done",
					"same native terminal did not settle",
				);
				await manager!.recover();
				await manager!.tick(KEY);
				expect(terminal).toEqual(["exact original native output"]);
				expect(failures).toEqual([]);
				expect(port.sends.filter((send) => send.opRef === first.opRef)).toHaveLength(1);
				expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
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
	expect(database?.inbox.turnRow(first.opRef)).toMatchObject({ state: "pending", turn_state: "accepted" });
	expect(port.sends).toHaveLength(1);
	expect(logs.some((line) => line.startsWith("accepted_turn_closed"))).toBe(false);
});

test("broker registry loss never fabricates a cutoff notice even when its persistence would fail", async () => {
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
	// Registry loss cannot even attempt a cutoff notice or complete the trigger.
	expect(database?.inbox.turnRow(first.opRef)).toMatchObject({ state: "pending", turn_state: "accepted" });
	expect(database?.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	expect(logs.some((line) => line.startsWith("accepted_turn_closed"))).toBe(false);

	failNotice = false;
	await manager!.tick(KEY);
	expect(database?.inbox.turnRow(first.opRef)).toMatchObject({ state: "pending", turn_state: "accepted" });
	expect(notices).toHaveLength(0);
	expect(database?.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	port.dropped.delete(first.sessionId);
	port.complete(first.opRef, "original native result");
	await manager!.tick(KEY);
	await eventually(
		() => database?.inbox.turnRow(first.opRef)?.turn_state === "done",
		"original native terminal did not settle",
	);
	await manager!.tick(KEY);
	expect(notices).toHaveLength(0);
	expect(port.sends).toHaveLength(1);
});

/**
 * The 2026-09-29 wedge: an ACCEPTED turn whose operation never settles on a
 * session the broker reports live and idle. `stale` answers status
 * endpoint_stale (case A: host force-ended mid-tool); `pending` reports
 * terminal_ok whose original output is never provable (case B: receipt missing).
 */
class WedgedSessionPort extends ScriptedSessionPort {
	readonly stale = new Set<string>();
	readonly pending = new Set<string>();
	livenessFails = false;
	override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
		if (this.stale.has(input.sessionId)) throw endpointStale();
		return await super.status(input);
	}
	override async fetchWorkerOutput(input: Parameters<ScriptedSessionPort["fetchWorkerOutput"]>[0]) {
		if (this.pending.has(input.opRef)) return { status: "absent", code: "output_pending" } as const;
		return await super.fetchWorkerOutput(input);
	}
	override async liveness(input: Parameters<ScriptedSessionPort["liveness"]>[0]) {
		if (this.livenessFails) throw new Error("inspect timed out");
		return await super.liveness(input);
	}
}

async function wedgedHarness(port: WedgedSessionPort) {
	const clock = { offsetMs: 0 };
	const logs: string[] = [];
	const terminal: string[] = [];
	const recorded: string[] = [];
	const failures: string[] = [];
	await harness(
		port,
		{
			terminal: (text, alreadyPosted) => (alreadyPosted ? recorded : terminal).push(text),
			failure: (message) => failures.push(message),
		},
		(line) => logs.push(line),
		{ now: () => Date.now() + clock.offsetMs },
	);
	enqueue("m-1", "question");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "turn did not start");
	const first = port.sends[0]!;
	expect(database?.inbox.turnRow(first.opRef)?.turn_state).toBe("accepted");
	return { clock, logs, terminal, recorded, failures, first };
}

const MINUTE = 60_000;
const OLD_IDLE_REGISTRY_AGE_MS = 10 * MINUTE;

for (const lostAck of [false, true])
	test(`a stall retains unknown operation tracking (lostAck=${lostAck})`, async () => {
		class UnknownObservationPort extends ScriptedSessionPort {
			unknown = true;
			override async send(input: Parameters<ScriptedSessionPort["send"]>[0]) {
				const receipt = await super.send(input);
				if (lostAck) throw new Error("ACK lost after native admission");
				return receipt;
			}
			override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
				const report = await super.status(input);
				return this.unknown ? { ...report, status: { status: "unknown" as const } } : report;
			}
		}
		const port = new UnknownObservationPort();
		const terminal: string[] = [];
		await harness(port, { terminal: (text) => terminal.push(text) });
		enqueue("stalled-original", "execute once");
		await manager!.notifyInbound(KEY);
		const first = port.sends[0]!;
		await manager!.tick(KEY);
		port.emitStall(first.sessionId);
		await manager!.reconcile(KEY);
		expect(manager!.state(KEY)).toBe("turn-running");
		expect(port.tailsOf(first.sessionId)).toHaveLength(1);
		expect(port.closes).toHaveLength(0);
		expect(database!.inbox.turnRow(first.opRef)).toMatchObject({
			state: "pending",
			turn_state: lostAck ? "bound" : "accepted",
			bound_session_id: first.sessionId,
			turn_epoch: 0,
		});
		port.unknown = false;
		port.complete(first.opRef, "original result after observation recovered");
		await manager!.tick(KEY);
		await eventually(
			() => database!.inbox.turnRow(first.opRef)?.turn_state === "done",
			"stall orphaned original operation",
		);
		expect(terminal).toEqual(["original result after observation recovered"]);
		expect(port.sends).toHaveLength(1);
	});

test("an unknown observation followed by native terminal before the first stall delivers the original answer", async () => {
	class UnknownObservationPort extends ScriptedSessionPort {
		unknown = true;
		override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
			const report = await super.status(input);
			return this.unknown ? { ...report, status: { status: "unknown" as const } } : report;
		}
	}
	const port = new UnknownObservationPort();
	const terminal: string[] = [];
	await harness(port, { terminal: (text) => terminal.push(text) });
	enqueue("terminal-before-stall", "execute once");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	await manager!.reconcile(KEY);
	port.unknown = false;
	port.seedOperation(first.opRef, first.sessionId, "terminal_ok", "original answer without a terminal frame");
	port.emitStall(first.sessionId);
	await manager!.reconcile(KEY);
	await eventually(
		() => database!.inbox.turnRow(first.opRef)?.turn_state === "done",
		"original terminal remained held",
	);
	expect(terminal).toEqual(["original answer without a terminal frame"]);
	expect(database!.inbox.turnRow(first.opRef)?.terminal_delivery_id).not.toBe(JSON.stringify({ none: "retired" }));
	expect(database!.bindings.getSessionRecord(KEY)).toMatchObject({ epoch: 0, sessionId: first.sessionId });
	expect(port.sends).toHaveLength(1);
});

for (const outcome of ["refused", "terminating", "error", "resumed_activity"] as const)
	test(`old idle registry does not close accepted work (${outcome})`, async () => {
		const port = new WedgedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
		const { clock, terminal, failures, first } = await wedgedHarness(port);
		port.appendAssistant(first.sessionId, "intermediate transcript, not a receipt");
		port.setActivity(first.sessionId, "idle");
		let terminationAttempts = 0;
		port.terminateHost = async () => {
			terminationAttempts++;
			if (outcome === "error") throw new Error("termination transport failed");
			if (outcome === "terminating") return { outcome: "terminating", pid: 123 };
			return { outcome: "refused", reason: "process_identity_changed" };
		};
		if (outcome === "resumed_activity") {
			const read = port.fetchAssistantSince.bind(port);
			port.fetchAssistantSince = async (input) => {
				const answer = await read(input);
				await Promise.resolve();
				port.emitAssistant(first.sessionId, "native work resumed", "resumed", first.opRef);
				port.setActivity(first.sessionId, "tool", Date.now());
				return answer;
			};
		}
		clock.offsetMs = OLD_IDLE_REGISTRY_AGE_MS + MINUTE;
		await manager!.tick(KEY);
		expect(database!.inbox.turnRow(first.opRef)).toMatchObject({
			state: "pending",
			turn_state: "accepted",
			turn_op_ref: first.opRef,
			bound_session_id: first.sessionId,
			turn_epoch: 0,
		});
		expect(database!.bindings.getSessionRecord(KEY)).toMatchObject({ epoch: 0, sessionId: first.sessionId });
		expect(terminal).toEqual([]);
		expect(failures).toEqual([]);
		expect(terminationAttempts).toBe(0);
		expect(port.sends).toHaveLength(1);
	});

test("case A: stale status and old idle registry retain accepted identity until native completion", async () => {
	const port = new WedgedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const { clock, logs, terminal, failures, first } = await wedgedHarness(port);
	port.stale.add(first.sessionId);
	port.appendAssistant(first.sessionId, "the answer to a steer");
	port.setActivity(first.sessionId, "idle");

	// Before the bound: the ordinary hold, nothing released or killed.
	for (const offset of [0, 5 * MINUTE, OLD_IDLE_REGISTRY_AGE_MS - MINUTE]) {
		clock.offsetMs = offset;
		await manager!.tick(KEY);
	}
	expect(logs.some((line) => line.includes("reason=status_unavailable") && line.includes(first.opRef))).toBe(true);
	expect(logs.some((line) => line.startsWith("wedged_turn_released"))).toBe(false);
	expect(database?.inbox.turnRow(first.opRef)).toMatchObject({ state: "pending", turn_state: "accepted" });
	expect(port.closes).toHaveLength(0);
	expect(terminal).toEqual([]);

	clock.offsetMs = OLD_IDLE_REGISTRY_AGE_MS + MINUTE;
	await manager!.tick(KEY);
	expect(database!.inbox.turnRow(first.opRef)).toMatchObject({
		state: "pending",
		turn_state: "accepted",
		turn_op_ref: first.opRef,
		bound_session_id: first.sessionId,
		turn_epoch: 0,
	});
	expect(terminal).toEqual([]);
	expect(failures).toEqual([]);
	expect(port.closes).toHaveLength(0);
	expect(port.sends).toHaveLength(1);
	expect(database?.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	port.stale.delete(first.sessionId);
	port.complete(first.opRef, "native final answer");
	await manager!.tick(KEY);
	await eventually(() => database!.inbox.turnRow(first.opRef)?.turn_state === "done", "native terminal did not settle");
	expect(terminal).toEqual(["native final answer"]);
	enqueue("m-2", "next question");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 2, "next message was not dispatched");
	expect(port.sends[1]!.text).toBe("next question");
	expect(port.sends[1]!.sessionId).toBe(first.sessionId);
});

test("case A with an empty transcript and old idle registry remains held without a cut-off notice", async () => {
	const port = new WedgedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const { clock, logs, terminal, failures, first } = await wedgedHarness(port);
	port.stale.add(first.sessionId);
	port.setActivity(first.sessionId, "idle");
	clock.offsetMs = OLD_IDLE_REGISTRY_AGE_MS + MINUTE;
	await manager!.tick(KEY);
	expect(database!.inbox.turnRow(first.opRef)).toMatchObject({
		state: "pending",
		turn_state: "accepted",
		turn_op_ref: first.opRef,
		bound_session_id: first.sessionId,
	});
	expect(logs.some((line) => line.startsWith("accepted_turn_closed"))).toBe(false);
	expect(terminal).toEqual([]);
	expect(failures).toEqual([]);
	expect(port.closes).toHaveLength(0);
	expect(port.sends).toHaveLength(1);
});

test("in_flight status is not overridden by old idle registry or transcript text", async () => {
	const port = new WedgedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const { clock, logs, terminal, first } = await wedgedHarness(port);
	port.appendAssistant(first.sessionId, "answered");
	port.setActivity(first.sessionId, "idle");
	clock.offsetMs = OLD_IDLE_REGISTRY_AGE_MS + MINUTE;
	await manager!.tick(KEY);
	expect(database!.inbox.turnRow(first.opRef)).toMatchObject({
		state: "pending",
		turn_state: "accepted",
		turn_op_ref: first.opRef,
		bound_session_id: first.sessionId,
	});
	expect(logs.some((line) => line.startsWith("accepted_turn_closed"))).toBe(false);
	expect(terminal).toEqual([]);
	expect(port.closes).toHaveLength(0);
	expect(port.sends).toHaveLength(1);
});

async function caseB(port: WedgedSessionPort) {
	const fixture = await wedgedHarness(port);
	const { first, logs } = fixture;
	port.pending.add(first.opRef);
	port.seedOperation(first.opRef, first.sessionId, "terminal_ok", "");
	port.setActivity(first.sessionId, "idle");
	await manager!.tick(KEY);
	await eventually(
		() => logs.some((line) => line.includes("reason=no_assistant_text_for_terminal") && line.includes(first.opRef)),
		`terminal_ok with pending output was not held:\n${logs.join("\n")}`,
	);
	return fixture;
}

test("case B: terminal_ok with pending output holds despite spoken and silent transcript rows, then uses exact output", async () => {
	const port = new WedgedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const { clock, logs, terminal, failures, first } = await caseB(port);
	port.appendAssistant(first.sessionId, "the real answer");
	port.appendAssistant(first.sessionId, "[SILENT]");

	clock.offsetMs = OLD_IDLE_REGISTRY_AGE_MS - MINUTE;
	await manager!.tick(KEY);
	expect(logs.some((line) => line.startsWith("wedged_turn_released"))).toBe(false);
	expect(database?.inbox.turnRow(first.opRef)).toMatchObject({ state: "pending", turn_state: "accepted" });

	clock.offsetMs = OLD_IDLE_REGISTRY_AGE_MS + MINUTE;
	await manager!.tick(KEY);
	expect(database!.inbox.turnRow(first.opRef)).toMatchObject({
		state: "pending",
		turn_state: "accepted",
		turn_op_ref: first.opRef,
		bound_session_id: first.sessionId,
	});
	expect(terminal).toEqual([]);
	expect(failures).toEqual([]);
	// A healthy host is not killed and the binding is kept.
	expect(port.closes).toHaveLength(0);
	expect(database?.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	expect(logs.some((line) => line.startsWith("accepted_turn_closed"))).toBe(false);
	port.pending.delete(first.opRef);
	port.seedOperation(first.opRef, first.sessionId, "terminal_ok", "exact original result");
	await manager!.tick(KEY);
	await eventually(() => database!.inbox.turnRow(first.opRef)?.turn_state === "done", "exact output did not settle");
	expect(terminal).toEqual(["exact original result"]);
	expect(port.sends).toHaveLength(1);
});

test("case B with only [SILENT] in the transcript remains held without a silent receipt", async () => {
	const port = new WedgedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const { clock, logs, terminal, failures, first } = await caseB(port);
	port.appendAssistant(first.sessionId, "[SILENT]");
	clock.offsetMs = OLD_IDLE_REGISTRY_AGE_MS + MINUTE;
	await manager!.tick(KEY);
	expect(database!.inbox.turnRow(first.opRef)).toMatchObject({
		state: "pending",
		turn_state: "accepted",
		turn_op_ref: first.opRef,
		bound_session_id: first.sessionId,
	});
	expect(terminal).toEqual([]);
	expect(failures).toEqual([]);
	expect(logs.some((line) => line.startsWith("accepted_turn_closed"))).toBe(false);
	expect(port.closes).toHaveLength(0);
});

test("an unrelated confirmed delivery cannot settle a terminal operation with pending output", async () => {
	const port = new WedgedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const { clock, logs, terminal, recorded, failures, first } = await caseB(port);
	port.appendAssistant(first.sessionId, "already posted");
	database!.transaction((tx) => {
		database!.outbox.create(tx, {
			deliveryId: "delivery-earlier",
			turnId: "turn-earlier",
			originKey: KEY,
			payloadJson: JSON.stringify({ origin: ORIGIN, role: "assistant", text: "already posted", final: true }),
		});
		database!.outbox.confirm(tx, "delivery-earlier");
	});
	clock.offsetMs = OLD_IDLE_REGISTRY_AGE_MS + MINUTE;
	await manager!.tick(KEY);
	expect(database!.inbox.turnRow(first.opRef)).toMatchObject({
		state: "pending",
		turn_state: "accepted",
		turn_op_ref: first.opRef,
		bound_session_id: first.sessionId,
	});
	expect(terminal).toEqual([]);
	expect(recorded).toEqual([]);
	expect(failures).toEqual([]);
	expect(logs.some((line) => line.startsWith("accepted_turn_closed"))).toBe(false);
});

test("case B with no transcript remains held instead of inferring output failure from elapsed time", async () => {
	const port = new WedgedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const { clock, logs, terminal, failures, first } = await caseB(port);
	clock.offsetMs = OLD_IDLE_REGISTRY_AGE_MS + MINUTE;
	await manager!.tick(KEY);
	expect(database!.inbox.turnRow(first.opRef)).toMatchObject({
		state: "pending",
		turn_state: "accepted",
		turn_op_ref: first.opRef,
		bound_session_id: first.sessionId,
	});
	expect(terminal).toEqual([]);
	expect(failures).toEqual([]);
	expect(logs.some((line) => line.startsWith("accepted_turn_closed"))).toBe(false);
	expect(port.closes).toHaveLength(0);
	expect(port.sends).toHaveLength(1);
});

test("a live session that is busy (not idle) past the bound is never released", async () => {
	const port = new WedgedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const { clock, logs, terminal, first } = await wedgedHarness(port);
	port.stale.add(first.sessionId);
	port.appendAssistant(first.sessionId, "mid-work line");
	port.setActivity(first.sessionId, "tool", Date.now());
	for (const offset of [
		OLD_IDLE_REGISTRY_AGE_MS + MINUTE,
		3 * OLD_IDLE_REGISTRY_AGE_MS,
		12 * OLD_IDLE_REGISTRY_AGE_MS,
	]) {
		clock.offsetMs = offset;
		await manager!.tick(KEY);
	}
	expect(logs.some((line) => line.startsWith("wedged_turn_released"))).toBe(false);
	expect(database?.inbox.turnRow(first.opRef)).toMatchObject({ state: "pending", turn_state: "accepted" });
	expect(port.closes).toHaveLength(0);
	expect(terminal).toEqual([]);
});

test("a silent in-flight turn is failed after the stall deadline and releases a queued lane report", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const clock = { now: Date.now() };
	const logs: string[] = [];
	const failures: Array<{ message: string; forceNotice: boolean | undefined }> = [];
	let interimSeen = false;
	await harness(
		port,
		{
			failure: (message, forceNotice) => failures.push({ message, forceNotice }),
			onFrame: (frame) => {
				if (!frame.assistantText) return;
				interimSeen = true;
				return true;
			},
		},
		(line) => logs.push(line),
		{ now: () => clock.now },
	);
	enqueue("stalled-trigger", "start the lane");
	await manager!.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "persona turn did not start");
	const first = port.sends[0]!;
	port.emitAssistant(first.sessionId, "The lane is running", "lane-running", first.opRef);
	await eventually(() => interimSeen, "mid-work frame was not observed before the stall");
	await manager!.reconcile(KEY);
	enqueue("stalled-lane-report", "[lane child] complete", "lane_report");
	await manager!.notifyInbound(KEY);
	expect(port.steers).toHaveLength(0);
	expect(database!.inbox.pendingCount(KEY)).toBe(2);
	port.setActivity(first.sessionId, "tool", clock.now);

	const readsBeforeStall = port.statusReads.length;
	port.emitStall(first.sessionId);
	await eventually(() => port.statusReads.length > readsBeforeStall, "stall did not check operation status");
	expect(database!.inbox.turnRow(first.opRef)?.turn_state).toBe("accepted");
	expect(port.closes).toHaveLength(0);

	clock.now += STALLED_TURN_TERMINAL_TIMEOUT_MS - 1;
	await manager!.tick(KEY);
	expect(database!.inbox.turnRow(first.opRef)?.turn_state).toBe("accepted");
	expect(port.closes).toHaveLength(0);
	expect(failures).toEqual([]);

	clock.now += 1;
	await manager!.tick(KEY);
	await eventually(() => database!.inbox.turnRow(first.opRef)?.turn_state === "done", "stalled turn did not close");
	await eventually(() => port.sends.length === 2, "queued lane report did not start after cutoff");
	expect(database!.inbox.turnRow(first.opRef)?.terminal_delivery_id).toBe(JSON.stringify({ none: "turn_failed" }));
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(port.closes.map((close) => close.sessionId)).toEqual([first.sessionId]);
	expect(port.sends[1]?.text).toContain("[lane child] complete");
	expect(failures).toEqual([
		{
			message: expect.stringContaining("no terminal event before the stall deadline"),
			forceNotice: true,
		},
	]);
	expect(logs.some((line) => line.startsWith("stalled_turn_cutoff ") && line.includes(first.opRef))).toBe(true);
	port.complete(port.sends[1]!.opRef, "report received");
	await eventually(() => database!.inbox.pendingCount(KEY) === 0, "lane report did not settle after cutoff");
});

function stalledExitProofKey(opRef: string): string {
	return `stalled-turn-exit:${createHash("sha256")
		.update(JSON.stringify([opRef]))
		.digest("hex")}`;
}

/** Crash after positive exit, but before the failure receipt/completion transaction. */
async function durableExitFixture(outcome: "terminated" | "already_gone" | "async") {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const clock = { now: 10_000 };
	const timers = new Map<object, { work: () => void; delay: number }>();
	const timing = {
		now: () => clock.now,
		setTimeout: (work: () => void, delay: number) => {
			const id = {};
			timers.set(id, { work, delay });
			return id;
		},
		clearTimeout: (id: unknown) => {
			timers.delete(id as object);
		},
	};
	let noticeAttempts = 0;
	await harness(
		port,
		{
			failure: () => {
				noticeAttempts++;
				throw new Error("injected crash before durable failure receipt");
			},
		},
		undefined,
		timing,
	);
	enqueue("durable-exit-original", "execute exactly once");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	port.setActivity(first.sessionId, "tool", clock.now);
	port.emitStall(first.sessionId);
	await manager!.reconcile(KEY);
	clock.now += STALLED_TURN_TERMINAL_TIMEOUT_MS;
	let terminationAttempts = 0;
	port.terminateHost = async (input) => {
		terminationAttempts++;
		if (input.shouldTerminate) expect(await input.shouldTerminate()).toBe(true);
		if (outcome === "async" && terminationAttempts === 1) return { outcome: "terminating", pid: 123 };
		return outcome === "already_gone" ? { outcome: "already_gone" } : { outcome: "terminated", pid: 123 };
	};
	await manager!.tick(KEY).catch(() => {});
	if (outcome === "async") {
		expect(noticeAttempts).toBe(0);
		expect(database!.core.metaGet(stalledExitProofKey(first.opRef))).toBeUndefined();
		const recheck = [...timers].find(([, timer]) => timer.delay === 1_000);
		expect(recheck).toBeDefined();
		timers.delete(recheck![0]);
		recheck![1].work();
		await manager!.reconcile(KEY).catch(() => {});
	}
	expect(noticeAttempts).toBeGreaterThan(0);
	expect(database!.inbox.turnRow(first.opRef)).toMatchObject({
		state: "pending",
		turn_state: "accepted",
		bound_session_id: first.sessionId,
		turn_epoch: 0,
	});
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	const proofKey = stalledExitProofKey(first.opRef);
	const proof = database!.core.metaGet(proofKey);
	expect(proof, "confirmed exit must be durable before onFailure can throw").toBeDefined();
	const failures: string[] = [];
	const terminal: string[] = [];
	const status = port.status.bind(port);
	async function reopen(observation: "unknown" | "unavailable" | "terminal" = "unknown") {
		// stop() only tears down actor resources: no drain or completion is allowed here.
		await manager!.stop();
		expect(database!.inbox.turnRow(first.opRef)?.turn_state).toBe("accepted");
		database!.close();
		database = await Store.open(join(home, "gateway.db"));
		registerFixtureBindings(port);
		port.status = async (input) => {
			if (input.opRef !== first.opRef || observation === "terminal") return await status(input);
			if (observation === "unavailable") throw endpointStale();
			return { ...(await status(input)), status: { status: "unknown" as const } };
		};
		manager = new PersonaSessionManager({
			database,
			port,
			instanceId: "instance-test",
			repo: join(home, "workspace"),
			...timing,
			onTurnStart: ({ trigger }) => ({
				text: trigger.body,
				onFailure: ({ error }) => {
					failures.push(error.message);
				},
				onTerminal: ({ text }) => {
					terminal.push(text);
				},
			}),
		});
	}
	return {
		port,
		first,
		proofKey,
		proof: proof!,
		failures,
		terminal,
		reopen,
		terminationAttempts: () => terminationAttempts,
	};
}

for (const outcome of ["terminated", "already_gone", "async"] as const)
	for (const observation of ["unknown", "unavailable"] as const)
		test(`durable stalled-host exit survives pre-settlement Store reopen (${outcome}, ${observation})`, async () => {
			const f = await durableExitFixture(outcome);
			const attempts = f.terminationAttempts();
			const resumes = f.port.resumes.length;
			const binds = f.port.binds.length;
			await f.reopen(observation);
			await manager!.recover();
			await manager!.tick(KEY);
			expect(database!.inbox.turnRow(f.first.opRef)).toMatchObject({
				turn_state: "done",
				terminal_delivery_id: JSON.stringify({ none: "turn_failed" }),
			});
			expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
			expect(database!.core.metaGet(f.proofKey)).toBeUndefined();
			await manager!.recover();
			await manager!.tick(KEY);
			expect(f.failures).toHaveLength(1);
			expect(f.terminal).toEqual([]);
			expect(f.terminationAttempts()).toBe(attempts);
			expect(f.port.resumes).toHaveLength(resumes);
			expect(f.port.binds).toHaveLength(binds);
			expect(f.port.sends).toHaveLength(1);
			enqueue("durable-exit-next", "new work after confirmed exit");
			await manager!.notifyInbound(KEY);
			expect(f.port.sends).toHaveLength(2);
			expect(f.port.sends[1]).toMatchObject({ sessionId: "session-e1", text: "new work after confirmed exit" });
		});

test("durable stalled-host exit retry preserves native submission retry and consumes the old proof after Store reopen", async () => {
	const f = await durableExitFixture("terminated");
	await manager!.stop();
	f.port.fail(f.first.opRef, "Prompt submission failed.", SUBMISSION_BUSY);
	await f.reopen("terminal");
	await manager!.recover();
	expect(database!.inbox.pendingOldest(KEY)).toMatchObject({
		message_id: "durable-exit-original",
		state: "pending",
		turn_state: null,
		turn_op_ref: null,
		turn_epoch: null,
		bound_session_id: null,
	});
	expect(database!.inbox.turnRow(f.first.opRef)).toBeUndefined();
	expect(freshTurnAttempt(database!, KEY, 0, "durable-exit-original")).toBe(1);
	expect(database!.core.metaGet(f.proofKey)).toBeUndefined();
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	expect(f.failures).toEqual([]);
	expect(f.terminal).toEqual([]);
	expect(f.port.sends).toHaveLength(1);
});

test("durable stalled-host exit retry rolls back proof, trigger and attempt together before commit", async () => {
	const f = await durableExitFixture("terminated");
	await manager!.stop();
	const before = database!.inbox.turnRow(f.first.opRef);
	expect(freshTurnAttempt(database!, KEY, 0, "durable-exit-original")).toBe(0);
	expect(() =>
		database!.transaction((tx) => {
			expect(retryInboundTurn(database!, tx, f.first.opRef)).toBe(1);
			expect(database!.inbox.turnRow(f.first.opRef)).toBeUndefined();
			expect(freshTurnAttempt(database!, KEY, 0, "durable-exit-original")).toBe(1);
			expect(database!.core.metaGet(f.proofKey)).toBeUndefined();
			throw new Error("rollback exit retry");
		}),
	).toThrow("rollback exit retry");
	expect(database!.inbox.turnRow(f.first.opRef)).toEqual(before);
	expect(freshTurnAttempt(database!, KEY, 0, "durable-exit-original")).toBe(0);
	expect(database!.core.metaGet(f.proofKey)).toBe(f.proof);
	expect(database!.transaction((tx) => retryInboundTurn(database!, tx, f.first.opRef))).toBe(1);
	expect(database!.inbox.pendingOldest(KEY)).toMatchObject({
		state: "pending",
		turn_state: null,
		turn_op_ref: null,
		bound_session_id: null,
	});
	expect(freshTurnAttempt(database!, KEY, 0, "durable-exit-original")).toBe(1);
	expect(database!.core.metaGet(f.proofKey)).toBeUndefined();
});

for (const mismatch of [
	"opRef",
	"sessionId",
	"epoch",
	"repo",
	"originKey",
	"authority",
	"version",
	"outcome",
	"malformed",
] as const)
	test(`durable stalled-host exit retry preserves ${mismatch} proof`, async () => {
		const f = await durableExitFixture("terminated");
		await manager!.stop();
		const proof = JSON.parse(f.proof);
		if (mismatch === "authority") proof.authority = { ...proof.authority, identity: "foreign-broker" };
		else if (mismatch === "version" || mismatch === "epoch") proof[mismatch] = 99;
		else if (mismatch !== "malformed") proof[mismatch] = "foreign-or-unconfirmed";
		const poisoned = mismatch === "malformed" ? "{broken-json" : JSON.stringify(proof);
		database!.transaction(() => database!.core.metaSet(f.proofKey, poisoned));
		expect(database!.transaction((tx) => retryInboundTurn(database!, tx, f.first.opRef))).toBe(1);
		expect(database!.inbox.turnRow(f.first.opRef)).toBeUndefined();
		expect(database!.core.metaGet(f.proofKey)).toBe(poisoned);
	});

test("durable stalled-host exit retry preserves proof after persisted broker authority changes", async () => {
	const f = await durableExitFixture("terminated");
	await manager!.stop();
	const authority = database!.bindings.authority!;
	database!.transaction((tx) => database!.bindings.setAuthority(tx, { ...authority, identity: "replacement-broker" }));
	expect(database!.transaction((tx) => retryInboundTurn(database!, tx, f.first.opRef))).toBe(1);
	expect(database!.inbox.turnRow(f.first.opRef)).toBeUndefined();
	expect(database!.core.metaGet(f.proofKey)).toBe(f.proof);
});

test("durable stalled-host exit retry rejects quarantine without changing proof, trigger or attempt", async () => {
	const f = await durableExitFixture("terminated");
	await manager!.stop();
	const before = database!.inbox.turnRow(f.first.opRef);
	const authority = database!.bindings.authority!;
	database!.transaction((tx) =>
		database!.bindings.cutoverBrokerAuthority(tx, {
			expectedAuthority: authority,
			targetAuthority: { ...authority, identity: "cutover-broker" },
			evidence: "test-only verified cutover snapshot",
			disposition: "quarantine",
		}),
	);
	expect(database!.bindings.isQuarantined("inbound", "durable-exit-original")).toBe(true);
	expect(() => database!.transaction((tx) => retryInboundTurn(database!, tx, f.first.opRef))).toThrow(
		BrokerAuthorityError,
	);
	expect(database!.inbox.turnRow(f.first.opRef)).toEqual(before);
	expect(freshTurnAttempt(database!, KEY, 0, "durable-exit-original")).toBe(0);
	expect(database!.core.metaGet(f.proofKey)).toBe(f.proof);
});

test("durable stalled-host exit retry preserves proof when requeue changes zero rows", async () => {
	const f = await durableExitFixture("terminated");
	await manager!.stop();
	const before = database!.inbox.turnRow(f.first.opRef);
	const requeue = database!.inbox.turnRequeue;
	let calls = 0;
	database!.inbox.turnRequeue = (_tx, opRef) => {
		calls++;
		expect(opRef).toBe(f.first.opRef);
		return 0;
	};
	try {
		expect(database!.transaction((tx) => retryInboundTurn(database!, tx, f.first.opRef))).toBe(1);
	} finally {
		database!.inbox.turnRequeue = requeue;
	}
	expect(calls).toBe(1);
	expect(database!.inbox.turnRow(f.first.opRef)).toEqual(before);
	expect(database!.core.metaGet(f.proofKey)).toBe(f.proof);
});

test("durable stalled-host exit yields to exact original readable native terminal and consumes proof", async () => {
	const f = await durableExitFixture("terminated");
	f.port.seedOperation(f.first.opRef, f.first.sessionId, "terminal_ok", "exact original output after crash");
	await f.reopen("terminal");
	await manager!.recover();
	await manager!.tick(KEY);
	await manager!.recover();
	expect(f.terminal).toEqual(["exact original output after crash"]);
	expect(f.failures).toEqual([]);
	expect(database!.inbox.turnRow(f.first.opRef)?.turn_state).toBe("done");
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	expect(database!.core.metaGet(f.proofKey)).toBeUndefined();
	expect(f.port.sends).toHaveLength(1);
});

for (const mismatch of [
	"opRef",
	"sessionId",
	"epoch",
	"repo",
	"originKey",
	"authority",
	"version",
	"outcome",
	"malformed",
] as const)
	test(`durable stalled-host exit rejects ${mismatch} proof after Store reopen`, async () => {
		const f = await durableExitFixture("terminated");
		const proof = JSON.parse(f.proof);
		if (mismatch === "authority") proof.authority = { ...proof.authority, identity: "foreign-broker" };
		else if (mismatch === "version" || mismatch === "epoch") proof[mismatch] = 99;
		else if (mismatch !== "malformed") proof[mismatch] = "foreign-or-unconfirmed";
		const poisoned = mismatch === "malformed" ? "{broken-json" : JSON.stringify(proof);
		database!.transaction(() => database!.core.metaSet(f.proofKey, poisoned));
		const attempts = f.terminationAttempts();
		await f.reopen("unavailable");
		await manager!.recover();
		await manager!.tick(KEY);
		expect(database!.inbox.turnRow(f.first.opRef)?.turn_state).toBe("accepted");
		expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
		expect(database!.core.metaGet(f.proofKey)).toBe(poisoned);
		expect(f.failures).toEqual([]);
		expect(f.terminal).toEqual([]);
		expect(f.terminationAttempts()).toBe(attempts);
		expect(f.port.sends).toHaveLength(1);
	});

for (const outcome of ["refused", "terminating", "throw"] as const)
	test(`durable stalled-host exit never persists an unconfirmed ${outcome} outcome`, async () => {
		const port = new ScriptedSessionPort();
		const clock = { now: 10_000 };
		const failures: string[] = [];
		await harness(
			port,
			{
				failure: (message) => {
					failures.push(message);
				},
			},
			undefined,
			{
				now: () => clock.now,
				setTimeout: () => ({}),
				clearTimeout: () => {},
			},
		);
		enqueue("durable-exit-unconfirmed", "do not infer producer death");
		await manager!.notifyInbound(KEY);
		const first = port.sends[0]!;
		port.setActivity(first.sessionId, "tool", clock.now);
		port.emitStall(first.sessionId);
		await manager!.reconcile(KEY);
		clock.now += STALLED_TURN_TERMINAL_TIMEOUT_MS;
		let attempts = 0;
		port.terminateHost = async (input) => {
			attempts++;
			expect(await input.shouldTerminate?.()).toBe(true);
			if (outcome === "throw") throw new Error("termination outcome unavailable");
			return outcome === "terminating"
				? { outcome: "terminating", pid: 123 }
				: { outcome: "refused", reason: "process_identity_changed" };
		};
		await manager!.tick(KEY);
		expect(attempts).toBe(1);
		expect(database!.core.metaGet(stalledExitProofKey(first.opRef))).toBeUndefined();
		expect(database!.inbox.turnRow(first.opRef)?.turn_state).toBe("accepted");
		expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
		expect(failures).toEqual([]);
	});

test("durable stalled-host exit rejects proof when persisted broker authority changes", async () => {
	const f = await durableExitFixture("terminated");
	await f.reopen("unavailable");
	const authority = database!.bindings.authority!;
	database!.transaction((tx) => database!.bindings.setAuthority(tx, { ...authority, identity: "replacement-broker" }));
	await manager!.recover().catch(() => {});
	await manager!.tick(KEY).catch(() => {});
	expect(database!.inbox.turnRow(f.first.opRef)?.turn_state).toBe("accepted");
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	expect(database!.core.metaGet(f.proofKey)).toBe(f.proof);
	expect(f.failures).toEqual([]);
	expect(f.port.sends).toHaveLength(1);
});

test("durable stalled-host exit stays quarantined after broker cutover", async () => {
	const f = await durableExitFixture("terminated");
	await f.reopen("unavailable");
	const authority = database!.bindings.authority!;
	database!.transaction((tx) =>
		database!.bindings.cutoverBrokerAuthority(tx, {
			expectedAuthority: authority,
			targetAuthority: { ...authority, identity: "cutover-broker" },
			evidence: "test-only verified cutover snapshot",
			disposition: "quarantine",
		}),
	);
	expect(database!.bindings.isQuarantined("inbound", "durable-exit-original")).toBe(true);
	await manager!.recover();
	await manager!.tick(KEY);
	expect(database!.inbox.turnRow(f.first.opRef)?.turn_state).toBe("accepted");
	expect(database!.core.metaGet(f.proofKey)).toBe(f.proof);
	expect(f.failures).toEqual([]);
	expect(f.terminal).toEqual([]);
	expect(f.port.sends).toHaveLength(1);
});

test("durable stalled-host exit completion, rotation and proof consumption roll back together", async () => {
	const f = await durableExitFixture("already_gone");
	await f.reopen();
	const rotate = database!.bindings.rotateEpoch.bind(database!.bindings);
	database!.bindings.rotateEpoch = () => {
		throw new Error("injected exit rotation failure");
	};
	await manager!.recover().catch(() => {});
	await manager!.tick(KEY).catch(() => {});
	expect(database!.inbox.turnRow(f.first.opRef)?.turn_state).toBe("accepted");
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	expect(database!.core.metaGet(f.proofKey)).toBe(f.proof);
	database!.bindings.rotateEpoch = rotate;
	await manager!.recover();
	await manager!.tick(KEY);
	expect(database!.inbox.turnRow(f.first.opRef)?.turn_state).toBe("done");
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(database!.core.metaGet(f.proofKey)).toBeUndefined();
});

test("durable stalled-host exit from reset-retired epoch cannot rotate or close its replacement", async () => {
	const f = await durableExitFixture("terminated");
	await manager!.stop();
	database!.transaction((tx) => database!.bindings.rotateEpoch(tx, KEY));
	await f.reopen();
	await manager!.recover();
	await manager!.tick(KEY);
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
	enqueue("durable-exit-replacement", "replacement epoch");
	await manager!.notifyInbound(KEY);
	const replacement = f.port.sends[1]!;
	expect(replacement).toMatchObject({ sessionId: "session-e1", text: "replacement epoch" });
	await manager!.recover();
	await manager!.tick(KEY);
	expect(database!.inbox.turnRow(replacement.opRef)?.turn_state).toBe("accepted");
	expect(database!.bindings.getSessionRecord(KEY)).toMatchObject({ epoch: 1, sessionId: replacement.sessionId });
	expect(f.port.sends.filter((send) => send.opRef === f.first.opRef)).toHaveLength(1);
});

test("durable stalled-host exit confirms reset-retired original before cleanup and survives reopen", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const clock = { now: 10_000 };
	const timers = new Map<object, { work: () => void; delay: number }>();
	const timing = {
		now: () => clock.now,
		setTimeout: (work: () => void, delay: number) => {
			const id = {};
			timers.set(id, { work, delay });
			return id;
		},
		clearTimeout: (id: unknown) => {
			timers.delete(id as object);
		},
	};
	const failures: string[] = [];
	await harness(
		port,
		{
			failure: (message) => {
				failures.push(message);
			},
		},
		undefined,
		timing,
	);
	enqueue("retired-confirm-original", "original producer");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	const tail = port.tailsOf(first.sessionId)[0]!;
	port.setActivity(first.sessionId, "tool", clock.now);
	port.emitStall(first.sessionId);
	await manager!.reconcile(KEY);
	clock.now += STALLED_TURN_TERMINAL_TIMEOUT_MS;
	let confirmed = false;
	let attempts = 0;
	port.terminateHost = async (input) => {
		attempts++;
		expect(input.sessionId).toBe(first.sessionId);
		if (!confirmed) {
			expect(await input.shouldTerminate?.()).toBe(true);
			return { outcome: "terminating", pid: 123 };
		}
		return { outcome: "already_gone" };
	};
	await manager!.tick(KEY);
	await manager!.reset(KEY, JSON.stringify(ORIGIN));
	enqueue("retired-confirm-replacement", "replacement remains running");
	await manager!.notifyInbound(KEY);
	// A signalled-but-unconfirmed host intentionally holds replacement admission.
	expect(port.sends).toHaveLength(1);
	const proofKey = stalledExitProofKey(first.opRef);
	const close = tail.close.bind(tail);
	let interrupted = false;
	tail.close = async () => {
		interrupted = true;
		expect(database!.core.metaGet(proofKey), "retired confirmation must precede cleanup").toBeDefined();
		throw new Error("injected retired cleanup crash");
	};
	const status = port.status.bind(port);
	port.status = async (input) =>
		input.opRef === first.opRef
			? { ...(await status(input)), status: { status: "unknown" as const } }
			: await status(input);
	confirmed = true;
	const recheck = [...timers].find(([, timer]) => timer.delay === 1_000);
	expect(recheck).toBeDefined();
	timers.delete(recheck![0]);
	recheck![1].work();
	await manager!.reconcile(KEY).catch(() => {});
	expect(database!.core.metaGet(proofKey), "reset must not discard original positive exit").toBeDefined();
	expect(interrupted).toBe(true);
	expect(database!.inbox.turnRow(first.opRef)?.turn_state).toBe("accepted");
	tail.close = close;
	await manager!.stop();
	database!.close();
	database = await Store.open(join(home, "gateway.db"));
	registerFixtureBindings(port);
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		...timing,
	});
	await manager!.recover();
	await manager!.tick(KEY);
	await manager!.recover();
	expect(database!.inbox.turnRow(first.opRef)?.turn_state).toBe("done");
	expect(database!.core.metaGet(proofKey)).toBeUndefined();
	await manager!.notifyInbound(KEY);
	const replacement = port.sends[1]!;
	expect(replacement).toMatchObject({ sessionId: "session-e1", text: "replacement remains running" });
	await manager!.recover();
	await manager!.tick(KEY);
	expect(database!.bindings.getSessionRecord(KEY)).toMatchObject({ epoch: 1, sessionId: replacement.sessionId });
	expect(database!.inbox.turnRow(replacement.opRef)?.turn_state).toBe("accepted");
	expect(attempts).toBe(2);
	expect(failures).toEqual([]);
	expect(port.sends).toHaveLength(2);
});

for (const foreignStatus of ["unknown", "in_flight"] as const)
	test(`durable stalled-host exit holds direct foreign ${foreignStatus} status until original terminal`, async () => {
		const port = new ScriptedSessionPort();
		const clock = { now: 10_000 };
		const failures: string[] = [];
		const terminal: string[] = [];
		await harness(
			port,
			{
				failure: (message) => {
					failures.push(message);
				},
				terminal: (text) => {
					terminal.push(text);
				},
			},
			undefined,
			{ now: () => clock.now, setTimeout: () => ({}), clearTimeout: () => {} },
		);
		enqueue("foreign-confirm-original", "original output only");
		await manager!.notifyInbound(KEY);
		const first = port.sends[0]!;
		port.setActivity(first.sessionId, "tool", clock.now);
		port.emitStall(first.sessionId);
		await manager!.reconcile(KEY);
		clock.now += STALLED_TURN_TERMINAL_TIMEOUT_MS;
		let exited = false;
		const status = port.status.bind(port);
		port.status = async (input) => {
			const report = await status(input);
			return exited ? { ...report, operationRef: "foreign-operation", status: { status: foreignStatus } } : report;
		};
		port.terminateHost = async (input) => {
			expect(await input.shouldTerminate?.()).toBe(true);
			exited = true;
			return { outcome: "terminated", pid: 123 };
		};
		await manager!.tick(KEY);
		expect(exited).toBe(true);
		expect(database!.inbox.turnRow(first.opRef)?.turn_state).toBe("accepted");
		expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
		expect(database!.core.metaGet(stalledExitProofKey(first.opRef))).toBeDefined();
		expect(failures).toEqual([]);
		port.status = status;
		port.seedOperation(first.opRef, first.sessionId, "terminal_ok", "exact original terminal after foreign report");
		await manager!.tick(KEY);
		await manager!.tick(KEY);
		expect(terminal).toEqual(["exact original terminal after foreign report"]);
		expect(failures).toEqual([]);
		expect(database!.core.metaGet(stalledExitProofKey(first.opRef))).toBeUndefined();
		expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
		expect(port.sends).toHaveLength(1);
	});

test("a terminal operation found after host exit is not failed", async () => {
	const port = new ScriptedSessionPort();
	const clock = { now: Date.now() };
	const failures: Array<{ message: string; forceNotice: boolean | undefined }> = [];
	await harness(port, { failure: (message, forceNotice) => failures.push({ message, forceNotice }) }, undefined, {
		now: () => clock.now,
	});
	enqueue("terminal-before-cutoff", "finish the current turn");
	await manager!.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "persona turn did not start");
	const send = port.sends[0]!;
	port.setActivity(send.sessionId, "tool", clock.now);
	const readsBeforeStall = port.statusReads.length;
	port.emitStall(send.sessionId);
	await eventually(() => port.statusReads.length > readsBeforeStall, "stall did not check operation status");

	clock.now += STALLED_TURN_TERMINAL_TIMEOUT_MS;
	port.beforeTerminateHost = ({ sessionId }) => {
		port.seedOperation(send.opRef, sessionId, "terminal_ok", "completed during host inspection");
		port.setSessionState(sessionId, { live: false });
	};
	await manager!.tick(KEY);
	expect(database!.inbox.turnRow(send.opRef)?.turn_state).toBe("accepted");
	expect(port.closes).toHaveLength(0);
	expect(failures).toEqual([]);

	port.complete(send.opRef, "the operation completed before the cutoff");
	await eventually(
		() => database!.inbox.turnRow(send.opRef)?.turn_state === "done",
		"terminal operation was not reconciled",
	);
	expect(port.closes).toHaveLength(0);
	expect(failures).toEqual([]);
});

test("tail activity that resumes after the stall deadline cancels the cutoff", async () => {
	const port = new ScriptedSessionPort();
	const clock = { now: Date.now() };
	let assistantFrames = 0;
	await harness(
		port,
		{
			onFrame: (frame) => {
				if (!frame.assistantText) return;
				assistantFrames++;
				return true;
			},
		},
		undefined,
		{ now: () => clock.now },
	);
	enqueue("stall-resumed", "continue working");
	await manager!.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "persona turn did not start");
	const send = port.sends[0]!;
	port.setActivity(send.sessionId, "tool", clock.now);
	const readsBeforeStall = port.statusReads.length;
	port.emitStall(send.sessionId);
	await eventually(() => port.statusReads.length > readsBeforeStall, "stall did not check operation status");

	clock.now += STALLED_TURN_TERMINAL_TIMEOUT_MS;
	port.beforeRunningJobs = async ({ sessionId }) => {
		port.emitAssistant(sessionId, "The tool is still making progress", "resumed-work", send.opRef);
		await Bun.sleep(5);
	};
	await manager!.tick(KEY);
	await eventually(() => assistantFrames === 1, "resumed tail frame was not delivered");
	await manager!.reconcile(KEY);
	expect(database!.inbox.turnRow(send.opRef)?.turn_state).toBe("accepted");
	expect(port.closes).toHaveLength(0);

	port.complete(send.opRef, "finished normally");
	await eventually(() => database!.inbox.turnRow(send.opRef)?.turn_state === "done", "resumed turn did not finish");
	expect(port.closes).toHaveLength(0);
});

test("resumed activity clears an unknown-status recovery hold before a later stall", async () => {
	class UnknownStatusPort extends ScriptedSessionPort {
		unknown = false;

		override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
			const report = await super.status(input);
			return this.unknown ? { ...report, status: { status: "unknown" as const }, summaryCompleted: false } : report;
		}
	}
	const port = new UnknownStatusPort();
	let assistantFrames = 0;
	await harness(port, {
		onFrame: (frame) => {
			if (frame.assistantText) assistantFrames++;
		},
	});
	enqueue("unknown-stall-trigger", "keep working");
	await manager!.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "persona turn did not start");
	const first = port.sends[0]!;
	port.unknown = true;

	let statusReads = port.statusReads.length;
	port.emitStall(first.sessionId);
	await eventually(() => port.statusReads.length > statusReads, "first stall did not reconcile unknown status");
	enqueue("unknown-stall-queued", "wait for the current turn", "lane_report");
	await manager!.notifyInbound(KEY);
	port.emitAssistant(first.sessionId, "The operation resumed", "unknown-resumed", first.opRef);
	await eventually(() => assistantFrames === 1, "resumed tail activity was not delivered");

	statusReads = port.statusReads.length;
	port.emitStall(first.sessionId);
	await eventually(() => port.statusReads.length > statusReads, "second stall did not reconcile the current turn");
	expect(database!.inbox.turnRow(first.opRef)?.turn_state).toBe("accepted");
	expect(port.sends).toHaveLength(1);
	expect(port.closes).toHaveLength(0);
	expect(database!.inbox.pendingCount(KEY)).toBe(2);
});

test("an uncorrelated idle broadcast does not retire a turn with stalled-status polling", async () => {
	class UnknownStatusPort extends ScriptedSessionPort {
		unknown = false;

		override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
			const report = await super.status(input);
			return this.unknown ? { ...report, status: { status: "unknown" as const }, summaryCompleted: false } : report;
		}
	}
	const port = new UnknownStatusPort();
	let idleFrameSeen = false;
	await harness(port, {
		onFrame: (frame) => {
			if (frame.idle && frame.rawKind === "activity") idleFrameSeen = true;
		},
	});
	enqueue("idle-unknown-stall-trigger", "keep working");
	await manager!.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "persona turn did not start");
	const first = port.sends[0]!;
	port.unknown = true;

	let statusReads = port.statusReads.length;
	port.emitStall(first.sessionId);
	await eventually(() => port.statusReads.length > statusReads, "first stall did not reconcile unknown status");
	enqueue("idle-unknown-stall-lane", "remain queued", "lane_report");
	await manager!.notifyInbound(KEY);

	statusReads = port.statusReads.length;
	for (const tail of port.tailsOf(first.sessionId))
		tail.emit({ kind: "activity", rawKind: "activity", payload: { state: "idle" }, steerEcho: false, idle: true });
	await eventually(() => idleFrameSeen, "uncorrelated idle broadcast was not delivered");
	await manager!.reconcile(KEY);
	expect(port.statusReads.length).toBeGreaterThan(statusReads);

	statusReads = port.statusReads.length;
	port.emitStall(first.sessionId);
	await eventually(() => port.statusReads.length > statusReads, "second stall did not reconcile the current turn");
	await manager!.reconcile(KEY);
	expect(database!.inbox.turnRow(first.opRef)?.turn_state).toBe("accepted");
	expect(port.sends).toHaveLength(1);
	expect(port.closes).toHaveLength(0);
	expect(database!.inbox.pendingCount(KEY)).toBe(2);
});

test("an unanswerable liveness probe is not wedge evidence: the turn stays held", async () => {
	class UnanswerablePort extends WedgedSessionPort {
		override async liveness(input: Parameters<ScriptedSessionPort["liveness"]>[0]) {
			if (this.stale.has(input.sessionId)) return { live: undefined, disowned: false };
			return await super.liveness(input);
		}
	}
	// Case A shape: status unreadable, liveness unanswerable.
	const port = new UnanswerablePort({ onBind: (input) => `session-e${input.epoch}` });
	const { clock, logs, first } = await wedgedHarness(port);
	port.stale.add(first.sessionId);
	port.appendAssistant(first.sessionId, "answer");
	port.setActivity(first.sessionId, "idle");
	clock.offsetMs = 3 * OLD_IDLE_REGISTRY_AGE_MS;
	await manager!.tick(KEY);
	expect(logs.some((line) => line.startsWith("wedged_turn_released"))).toBe(false);
	expect(database?.inbox.turnRow(first.opRef)).toMatchObject({ state: "pending", turn_state: "accepted" });
	expect(port.closes).toHaveLength(0);
});

test("a throwing liveness probe on a terminal_ok turn with pending output keeps it held", async () => {
	const port = new WedgedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const { clock, logs, terminal, first } = await caseB(port);
	port.appendAssistant(first.sessionId, "the real answer");
	port.livenessFails = true;
	clock.offsetMs = 3 * OLD_IDLE_REGISTRY_AGE_MS;
	await manager!.tick(KEY);
	expect(logs.some((line) => line.startsWith("wedged_turn_released"))).toBe(false);
	expect(database?.inbox.turnRow(first.opRef)).toMatchObject({ state: "pending", turn_state: "accepted" });
	expect(terminal).toEqual([]);
	// A recovered liveness probe still cannot supply the operation's output.
	port.livenessFails = false;
	clock.offsetMs = 3 * OLD_IDLE_REGISTRY_AGE_MS + MINUTE;
	await manager!.tick(KEY);
	expect(database!.inbox.turnRow(first.opRef)).toMatchObject({
		state: "pending",
		turn_state: "accepted",
		turn_op_ref: first.opRef,
	});
	expect(terminal).toEqual([]);
	port.pending.delete(first.opRef);
	port.seedOperation(first.opRef, first.sessionId, "terminal_ok", "original output restored");
	await manager!.tick(KEY);
	await eventually(
		() => database!.inbox.turnRow(first.opRef)?.turn_state === "done",
		"restored original output did not settle",
	);
	expect(terminal).toEqual(["original output restored"]);
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
	expect(database!.bindings.getSessionRecord(KEY)?.sessionId).toBe(replacement.sessionId);
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
	expect(database!.inbox.turnRow(opRef)?.turn_state).toBe("accepted");
	expect(port.sends).toHaveLength(2);
	expect(
		logs.filter((line) => line.includes(`retired_hold originKey=${KEY}`) && line.includes("reason=stall")),
	).toHaveLength(1);
	port.complete(send.opRef, "must remain fenced");
	await manager!.tick(KEY);
	await eventually(
		() => database?.inbox.turnRow(opRef)?.turn_state === "done",
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
for (const restart of [false, true])
	for (const evidence of ["empty_live_queue", "dead_session", "transcript_only", "tail_disowned"] as const)
		test(`lost ACK retains exact operation (restart=${restart}, evidence=${evidence})`, async () => {
			class LostAckPort extends ScriptedSessionPort {
				unknown = true;
				tailDisownedStatus = false;
				override async attachTail(input: Parameters<ScriptedSessionPort["attachTail"]>[0]) {
					if (this.sends.length > 0 && this.unknown && evidence === "tail_disowned")
						throw new Error("session tail failed: session_unavailable");
					return await super.attachTail(input);
				}
				override async send(input: Parameters<ScriptedSessionPort["send"]>[0]) {
					const receipt = await super.send(input);
					if (this.sends.length === 1) throw new Error("ACK lost after native admission");
					return receipt;
				}
				override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
					if (this.unknown && evidence === "dead_session") throw endpointStale();
					const report = await super.status(input);
					return this.unknown && !this.tailDisownedStatus
						? { ...report, status: { status: "unknown" as const } }
						: report;
				}
				override async liveness(input: Parameters<ScriptedSessionPort["liveness"]>[0]) {
					if (this.unknown && evidence === "dead_session") return { live: false, disowned: true };
					return await super.liveness(input);
				}
				async queueEmpty() {
					return true;
				}
			}
			const port = new LostAckPort({ onBind: (input) => `session-e${input.epoch}` });
			await harness(port);
			enqueue("lost-ack", "execute exactly once");
			await manager!.notifyInbound(KEY);
			const first = port.sends[0]!;
			if (evidence === "transcript_only") {
				port.appendAssistant(first.sessionId, "not an operation receipt");
				port.setActivity(first.sessionId, "idle");
			}
			if (restart) {
				await manager!.stop();
				database!.close();
				database = await Store.open(join(home, "gateway.db"));
				registerFixtureBindings(port);
				manager = new PersonaSessionManager({
					database,
					port,
					instanceId: "instance-test",
					repo: join(home, "workspace"),
				});
				port.tailDisownedStatus = evidence === "tail_disowned";
			}
			for (let sweep = 0; sweep < 3; sweep++) {
				if (restart) await manager!.recover();
				await manager!.tick(KEY);
			}
			expect(port.sends).toHaveLength(1);
			expect(port.binds).toHaveLength(1);
			expect(database!.inbox.turnRow(first.opRef)).toMatchObject({
				state: "pending",
				turn_state: restart && evidence === "tail_disowned" ? "accepted" : "bound",
				turn_op_ref: first.opRef,
				bound_session_id: first.sessionId,
				turn_epoch: 0,
			});
			expect(database!.bindings.getSessionRecord(KEY)).toMatchObject({ epoch: 0, sessionId: first.sessionId });
			// Same-operation native terminal proof still settles the original prompt.
			port.unknown = false;
			expect(
				(await port.status({ sessionId: first.sessionId, repo: join(home, "workspace"), opRef: first.opRef })).status
					.status,
			).toBe("in_flight");
			port.complete(first.opRef, "original result");
			await manager!.recover();
			await manager!.tick(KEY);
			await eventually(
				() => database!.inbox.turnRow(first.opRef)?.turn_state === "done",
				"original native terminal did not settle",
			);
			expect(port.sends).toHaveLength(1);
		});

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

class ModelNotSelectedPort extends ScriptedSessionPort {
	repaired = false;
	modelFailure: Error | undefined;

	constructor(options: ConstructorParameters<typeof ScriptedSessionPort>[0] = {}) {
		super({
			...options,
			onBind: options.onBind ?? ((input) => `session-e${input.epoch}`),
		});
	}

	override async send(input: Parameters<ScriptedSessionPort["send"]>[0]) {
		if (!this.repaired) {
			this.sendAttempts.push(input);
			throw new ModelNotSelectedError(input.opRef);
		}
		return await super.send(input);
	}

	override async setModel(input: Parameters<ScriptedSessionPort["setModel"]>[0]) {
		if (this.modelFailure) throw this.modelFailure;
		const receipt = await super.setModel(input);
		this.repaired = true;
		return receipt;
	}
}

class UnprovenModelNotSelectedPort extends ScriptedSessionPort {
	readonly error: Error;

	constructor(error: Error) {
		super({ onBind: (input) => `session-e${input.epoch}` });
		this.error = error;
	}

	override async send(input: Parameters<ScriptedSessionPort["send"]>[0]): Promise<never> {
		this.sendAttempts.push(input);
		throw this.error;
	}
}

test("authenticated model_not_selected refusal requeues plain pending, notices once, and pauses hot-loop dispatch", async () => {
	const port = new ModelNotSelectedPort();
	const notices: PersonaFailureInput[] = [];
	const noticeStates: Array<string | null | undefined> = [];
	const logs: string[] = [];
	await harness(
		port,
		{
			modelRefusal: (input) => {
				notices.push(input);
				noticeStates.push(database!.inbox.pendingOldest(KEY)?.state);
			},
		},
		(line) => logs.push(line),
	);
	enqueue("model-not-selected", "keep this trigger pending");
	await manager!.notifyInbound(KEY);
	const refusedOpRef = latestOpRef;
	expect(database!.inbox.turnRow(refusedOpRef)).toBeUndefined();
	expect(database!.inbox.pendingOldest(KEY)).toMatchObject({
		message_id: "model-not-selected",
		state: "pending",
		turn_role: null,
		turn_state: null,
		turn_op_ref: null,
		bound_session_id: null,
		dispatched_at: null,
		terminal_delivery_id: null,
	});
	expect(freshTurnAttempt(database!, KEY, 0, "model-not-selected")).toBe(1);
	expect(port.sendAttempts).toHaveLength(1);
	expect(port.sends).toHaveLength(0);
	expect(manager!.state(KEY)).toBe("idle");
	expect(database!.inbox.nonterminalTurns(KEY)).toEqual([]);
	expect(database!.bindings.getSessionRecord(KEY)).toMatchObject({ epoch: 0, sessionId: "session-e0" });
	expect(notices).toHaveLength(1);
	expect(noticeStates).toEqual(["pending"]);
	expect(notices[0]?.error).toMatchObject({
		code: "model_not_selected",
		runtimeMessage: "request was not admitted because no model is selected",
	});
	expect(`${formatFailureNotice(notices[0]!.error)} Send /model <id> or /new`).toBe(
		"[turn failed] model_not_selected: request was not admitted because no model is selected Send /model <id> or /new",
	);

	enqueue("later-message", "wait behind the paused refusal");
	await manager!.notifyInbound(KEY);
	for (let sweep = 0; sweep < 3; sweep++) {
		await manager!.tick(KEY);
		await manager!.recover();
	}
	expect(port.sendAttempts).toHaveLength(1);
	expect(port.steers).toHaveLength(0);
	expect(database!.inbox.pendingOldest(KEY)?.message_id).toBe("model-not-selected");
	expect(database!.inbox.pendingCount(KEY)).toBe(2);
	expect(notices).toHaveLength(1);
	expect(
		logs.some((line) => line.includes(`opRef=${refusedOpRef}`) && line.includes("reason=model_not_selected")),
	).toBe(true);
	const blockedDispatches = logs.filter(
		(line) => line.includes("persona_dispatch_blocked") && line.includes("reason=model_not_selected"),
	);
	expect(blockedDispatches.length).toBeGreaterThan(0);
	expect(blockedDispatches.every((line) => line.includes("message=model-not-selected"))).toBe(true);
	expect(blockedDispatches[0]).toContain("message=model-not-selected");
	expect(blockedDispatches[0]).not.toContain("keep this trigger pending");
});

test("failed refusal-notice persistence propagates while the pause prevents retries", async () => {
	const port = new ModelNotSelectedPort();
	let noticeAttempts = 0;
	const released: string[] = [];
	const logs: string[] = [];
	await harness(
		port,
		{
			released: (opRef) => released.push(opRef),
			modelRefusal: () => {
				noticeAttempts++;
				expect(database!.inbox.pendingOldest(KEY)?.message_id).toBe("notice-persist-failure");
				throw new Error("notice persistence failed");
			},
		},
		(line) => logs.push(line),
	);
	enqueue("notice-persist-failure", "do not lose the refusal notice");
	await expect(manager!.notifyInbound(KEY)).rejects.toThrow("notice persistence failed");
	const refusedOpRef = latestOpRef;
	port.repaired = true;
	for (let sweep = 0; sweep < 3; sweep++) {
		await manager!.notifyInbound(KEY);
		await manager!.tick(KEY);
	}
	expect(database!.inbox.turnRow(refusedOpRef)).toBeUndefined();
	expect(database!.inbox.pendingOldest(KEY)?.message_id).toBe("notice-persist-failure");
	expect(manager!.state(KEY)).toBe("idle");
	expect(port.sendAttempts).toHaveLength(1);
	expect(port.sends).toHaveLength(0);
	expect(noticeAttempts).toBe(1);
	expect(released).toEqual([refusedOpRef]);
	expect(logs.some((line) => line.includes("notice persistence failed"))).toBe(true);
	expect(
		logs.filter((line) => line.includes("persona_dispatch_blocked") && line.includes("reason=model_not_selected")),
	).toHaveLength(3);
});

test("tail-close failure keeps the refusal pending and still persists its notice", async () => {
	const port = new ModelNotSelectedPort();
	const attach = port.attachTail.bind(port);
	let failCloseOnce = true;
	port.attachTail = async (input) => {
		const tail = await attach(input);
		const close = tail.close.bind(tail);
		tail.close = async () => {
			if (failCloseOnce) {
				failCloseOnce = false;
				throw new Error("tail close failed");
			}
			await close();
		};
		return tail;
	};
	const notices: PersonaFailureInput[] = [];
	const released: string[] = [];
	const logs: string[] = [];
	await harness(
		port,
		{
			released: (opRef) => released.push(opRef),
			modelRefusal: (input) => {
				notices.push(input);
				expect(database!.inbox.pendingOldest(KEY)?.message_id).toBe("tail-close-failure");
			},
		},
		(line) => logs.push(line),
	);
	enqueue("tail-close-failure", "keep the refusal trigger pending");
	await expect(manager!.notifyInbound(KEY)).rejects.toThrow("tail close failed");
	const refusedOpRef = latestOpRef;
	expect(database!.inbox.pendingOldest(KEY)).toMatchObject({ message_id: "tail-close-failure", state: "pending" });
	expect(database!.bindings.getSessionRecord(KEY)).toMatchObject({ epoch: 0, sessionId: "session-e0" });
	expect(manager!.state(KEY)).toBe("idle");
	expect(notices).toHaveLength(1);
	expect(released).toEqual([refusedOpRef]);
	expect(
		logs.some((line) => line.includes(`opRef=${refusedOpRef}`) && line.includes("reason=model_not_selected")),
	).toBe(true);
});

test("successful /model resumes the refused trigger on its existing session", async () => {
	const port = new ModelNotSelectedPort({
		onSend: (input, scripted) => scripted.complete(input.opRef, "fresh reply"),
	});
	const terminal: string[] = [];
	const notices: PersonaFailureInput[] = [];
	await harness(port, { terminal: (text) => terminal.push(text), modelRefusal: (input) => notices.push(input) });
	enqueue("refused-before-model", "replay after model selection");
	await manager!.notifyInbound(KEY);
	const refusedOpRef = latestOpRef;
	await manager!.rebindModel(KEY, "provider/model");
	await eventually(() => terminal.length === 1, "successful model selection did not resume the pending trigger");
	expect(port.models).toContainEqual({
		sessionId: "session-e0",
		repo: join(home, "workspace"),
		selection: "provider/model",
	});
	expect(port.sendAttempts).toHaveLength(2);
	expect(port.sends).toHaveLength(1);
	expect(port.sends[0]).toMatchObject({ sessionId: "session-e0", text: "replay after model selection" });
	expect(port.sends[0]?.opRef).not.toBe(refusedOpRef);
	expect(terminal).toEqual(["fresh reply"]);
	expect(database!.inbox.turnRow(port.sends[0]!.opRef)).toMatchObject({ state: "done", turn_state: "done" });
	expect(database!.bindings.getSessionRecord(KEY)).toMatchObject({ epoch: 0, sessionId: "session-e0" });
	expect(notices).toHaveLength(1);
	expect(port.steers).toHaveLength(0);
});
// The refusal's retryInboundTurn already advanced the shared per-trigger
// retry ordinal that arms the one automatic submission-phase re-send, so the
// turn resumed by /model reports its own submission failure instead of
// spending a second send on the same trigger.
test("an accepted turn resumed after an authenticated refusal reports its submission failure without a re-send", async () => {
	const port = new ModelNotSelectedPort({
		onSend: (input, scripted) =>
			scripted.fail(input.opRef, "Prompt submission failed.", { code: "internal", outcome: REPEATABLE_SUBMISSION }),
	});
	const terminal: string[] = [];
	const failures: string[] = [];
	const notices: PersonaFailureInput[] = [];
	const logs: string[] = [];
	await harness(
		port,
		{
			terminal: (text) => terminal.push(text),
			failure: (message) => failures.push(message),
			modelRefusal: (input) => notices.push(input),
		},
		(line) => logs.push(line),
	);
	enqueue("refused-then-submission", "refusal spends the shared retry budget");
	await manager!.notifyInbound(KEY);
	const refusedOpRef = latestOpRef;
	expect(freshTurnAttempt(database!, KEY, 0, "refused-then-submission")).toBe(1);
	expect(port.sendAttempts).toHaveLength(1);
	expect(port.sends).toHaveLength(0);
	expect(notices).toHaveLength(1);

	await manager!.rebindModel(KEY, "provider/model");
	await eventually(
		() => failures.length === 1 && manager!.state(KEY) === "idle",
		"resumed submission failure did not settle through the ordinary failure path",
	);

	expect(port.models).toContainEqual({
		sessionId: "session-e0",
		repo: join(home, "workspace"),
		selection: "provider/model",
	});
	expect(port.sendAttempts).toHaveLength(2);
	expect(port.sends).toHaveLength(1);
	const accepted = port.sends[0]!;
	expect(accepted.opRef).not.toBe(refusedOpRef);
	expect(accepted).toMatchObject({ sessionId: "session-e0", text: "refusal spends the shared retry budget" });
	expect(failures).toEqual(["internal: Prompt submission failed."]);
	expect(terminal).toEqual([]);
	expect(database!.inbox.turnRow(accepted.opRef)).toMatchObject({ state: "done", turn_state: "done" });
	expect(freshTurnAttempt(database!, KEY, 0, "refused-then-submission")).toBe(1);
	expect(database!.inbox.pendingCount(KEY)).toBe(0);
	expect(database!.bindings.getSessionRecord(KEY)).toMatchObject({ epoch: 0, sessionId: "session-e0" });
	expect(notices).toHaveLength(1);
	expect(
		logs.some(
			(line) =>
				line.startsWith("terminal_failure ") &&
				line.includes(`opRef=${accepted.opRef}`) &&
				line.includes("phase=submission"),
		),
	).toBe(true);
	expect(logs.filter((line) => line.startsWith("submission_retry "))).toHaveLength(0);
});

test("failed /model keeps the pending refusal paused", async () => {
	const port = new ModelNotSelectedPort();
	port.modelFailure = new Error("selection unavailable");
	const notices: PersonaFailureInput[] = [];
	await harness(port, { modelRefusal: (input) => notices.push(input) });
	enqueue("failed-model-selection", "must stay pending");
	await manager!.notifyInbound(KEY);
	const refusedOpRef = latestOpRef;
	await expect(manager!.rebindModel(KEY, "provider/model")).rejects.toThrow("selection unavailable");
	port.modelFailure = undefined;
	port.repaired = true;
	enqueue("after-failed-model", "also waits");
	await manager!.notifyInbound(KEY);
	for (let sweep = 0; sweep < 3; sweep++) await manager!.tick(KEY);
	expect(database!.inbox.pendingOldest(KEY)?.message_id).toBe("failed-model-selection");
	expect(database!.inbox.turnRow(refusedOpRef)).toBeUndefined();
	expect(port.models).toEqual([]);
	expect(port.sendAttempts).toHaveLength(1);
	expect(port.sends).toHaveLength(0);
	expect(port.steers).toHaveLength(0);
	expect(notices).toHaveLength(1);
});

test("process restart clears the in-memory refusal pause and retries the pending message", async () => {
	const port = new ModelNotSelectedPort({
		onSend: (input, scripted) => scripted.complete(input.opRef, "only reply"),
	});
	const terminal: string[] = [];
	const notices: PersonaFailureInput[] = [];
	await harness(port, { terminal: (text) => terminal.push(text), modelRefusal: (input) => notices.push(input) });
	enqueue("restart-model-refusal", "retry after restart");
	await manager!.notifyInbound(KEY);
	const rejectedOpRef = latestOpRef;
	expect(database!.inbox.pendingOldest(KEY)?.message_id).toBe("restart-model-refusal");
	await manager!.stop();
	manager = undefined;
	port.repaired = true;
	database!.close();
	database = await Store.open(join(home, "gateway.db"));
	registerFixtureBindings(port);
	manager = new PersonaSessionManager({
		database,
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
	await eventually(() => terminal.length === 1, "released trigger did not deliver");
	expect(port.sendAttempts).toHaveLength(2);
	expect(port.sends).toHaveLength(1);
	expect(port.sends[0]!.opRef).not.toBe(rejectedOpRef);
	expect(port.sends[0]).toMatchObject({ sessionId: "session-e0", text: "retry after restart" });
	expect(terminal).toEqual(["only reply"]);
	expect(database.inbox.pendingCount(KEY)).toBe(0);
	expect(database.bindings.getSessionRecord(KEY)).toMatchObject({ epoch: 0, sessionId: "session-e0" });
	expect(notices).toHaveLength(1);
});

test("/new discards the paused trigger and admits fresh input in the next epoch", async () => {
	const port = new ModelNotSelectedPort({
		onSend: (input, scripted) => scripted.complete(input.opRef, "new session reply"),
	});
	const terminal: string[] = [];
	await harness(port, { terminal: (text) => terminal.push(text) });
	enqueue("discard-refused", "discard this refused prompt");
	await manager!.notifyInbound(KEY);
	const refusedOpRef = latestOpRef;
	await manager!.reset(KEY, JSON.stringify(ORIGIN), new Date(Date.now() + 1_000).toISOString());
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(database!.inbox.turnRow(refusedOpRef)).toBeUndefined();
	expect(database!.inbox.pendingOldest(KEY)).toBeUndefined();
	port.repaired = true;
	enqueue("fresh-after-new", "fresh traffic after /new");
	await manager!.notifyInbound(KEY);
	await eventually(() => terminal.length === 1, "fresh post-/new traffic did not dispatch");
	expect(port.sendAttempts).toHaveLength(2);
	expect(port.sends).toHaveLength(1);
	expect(port.sends[0]).toMatchObject({ sessionId: "session-e1", text: "fresh traffic after /new" });
	expect(port.sends[0]?.opRef).not.toBe(refusedOpRef);
	expect(terminal).toEqual(["new session reply"]);
	expect(database!.inbox.pendingCount(KEY)).toBe(0);
});

for (const fixture of [
	{
		name: "transport failure with the exact code",
		error: new GjcCliError("relay disconnected before the prompt reply", 0, "", { code: "model_not_selected" }),
	},
	{ name: "diagnostic text only", error: new GjcCliError("failed", 0, "", { message: "model_not_selected" }) },
	{
		name: "nested code",
		error: new GjcCliError("failed", 0, "", { detail: { code: "model_not_selected" } }),
	},
	{ name: "malformed code", error: new GjcCliError("failed", 0, "", { code: 42 }) },
	{ name: "plain error text", error: new Error("model_not_selected") },
	{ name: "refusal for a different op-ref", error: new ModelNotSelectedError("different-op-ref") },
] as const)
	test(`${fixture.name} is not retried as an authenticated model refusal`, async () => {
		const port = new UnprovenModelNotSelectedPort(fixture.error);
		const notices: PersonaFailureInput[] = [];
		const logs: string[] = [];
		await harness(port, { modelRefusal: (input) => notices.push(input) }, (line) => logs.push(line));
		enqueue("unproven-model-refusal", "never duplicate uncertain input");
		await manager!.notifyInbound(KEY);
		const opRef = latestOpRef;
		await manager!.tick(KEY);
		await manager!.recover();
		expect(database!.inbox.turnRow(opRef)).toMatchObject({ state: "pending", turn_state: "bound" });
		expect(manager!.state(KEY)).toBe("turn-running");
		expect(port.sendAttempts).toHaveLength(1);
		expect(port.sends).toHaveLength(0);
		expect(notices).toHaveLength(0);
		expect(logs.some((line) => line.includes(`persona_send_ambiguous origin=${KEY} opRef=${opRef}`))).toBe(true);
	});

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
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	expect(database!.inbox.pendingOldest(KEY)?.message_id).toBe("m-2");
	port.complete(first.opRef, "first answer");
	await eventually(() => port.sends.length === 2, "pending message did not dispatch after terminal");
	expect(port.sends[1]).toMatchObject({ sessionId: first.sessionId, text: "next turn" });
	expect(port.binds).toHaveLength(1);
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	port.complete(port.sends[1]!.opRef, "second answer");
	await eventually(() => database!.inbox.pendingCount(KEY) === 0, "input was lost or left pending");
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
		expect(database!.inbox.steersHeld(first.opRef).map((row) => row.message_id)).toEqual(["m-2"]);
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
		await eventually(() => database!.inbox.turnRow(first.opRef)?.turn_state === "done", "terminal not recovered");
		await manager.tick(KEY);
		expect(database!.inbox.steersHeld(first.opRef).map((row) => row.message_id)).toEqual(["m-2"]);
		expect(database!.inbox.pendingOldest(KEY)).toBeUndefined();
		expect(port.sends).toHaveLength(1);
		expect(port.binds).toHaveLength(1);
		expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
		expect(new Set(port.steers.map((steer) => steer.clientRef))).toEqual(new Set([clientRef]));
		accepted = true;
		await manager.tick(KEY);
		expect(database!.inbox.turnRows(first.opRef).find((row) => row.message_id === "m-2")).toMatchObject({
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
test("startup recovery preserves a bound operation whose running status is known but tail attach is disowned", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("m-1", "orphaned by cutover");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "turn did not start");
	const orphan = port.sends[0]!;
	const opRef = latestOpRef;
	await manager?.stop();
	// The row as the old runtime left it: bound, never acknowledged.
	database?.transaction((tx) => database && retryInboundTurn(database, tx, opRef));
	database?.transaction((tx) =>
		database?.inbox.bindTurn(tx, { messageId: "m-1", originKey: KEY, epoch: 0, opRef, sessionId: orphan.sessionId }),
	);
	database!.transaction((tx) => database!.inbox.markSendAttempted(tx, opRef));
	expect(database?.inbox.turnRow(opRef)).toMatchObject({ turn_state: "bound" });

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
	expect(logs.some((line) => line.startsWith("recovery_turn_failed") && line.includes(opRef))).toBe(true);
	expect(disowning.sends).toHaveLength(0);
	expect(database!.inbox.turnRow(opRef)).toMatchObject({
		state: "pending",
		turn_state: "accepted",
		turn_op_ref: opRef,
		bound_session_id: orphan.sessionId,
		turn_epoch: 0,
	});
	expect(database?.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	// Native status/result recovery does not require an unavailable tail.
	disowning.seedOperation(opRef, orphan.sessionId, "terminal_ok", "original operation result");
	await manager.recover();
	expect(database!.inbox.turnRow(opRef)?.turn_state).toBe("done");
	expect(disowning.sends).toHaveLength(0);
});

test("startup recovery preserves a retired ambiguous bound turn after the broker disowns its session", async () => {
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
		() => database?.inbox.turnRow(replacement.opRef)?.turn_state === "done",
		"replacement did not close",
	);
	// Exactly the live shape: epoch rotated, ghost trigger still bound on a dead session.
	expect(database?.inbox.turnRow(ghostOpRef)).toMatchObject({ state: "pending", turn_state: "bound", turn_epoch: 0 });
	expect(database?.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
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
		logs.some((line) => line.includes(`opRef=${ghostOpRef}`) && line.includes("reason=operation_state_unknown")),
	).toBe(true);
	expect(released).toEqual([]);
	expect(port.sends).toHaveLength(1);
	expect(database!.inbox.turnRow(ghostOpRef)).toMatchObject({
		state: "pending",
		turn_state: "bound",
		turn_op_ref: ghostOpRef,
		bound_session_id: port.ghostSessionId,
		turn_epoch: 0,
	});
	expect(database?.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
});

test("a tail attach that fails before any send releases the lifecycle it created", async () => {
	class HelloLostOncePort extends ScriptedSessionPort {
		failures = 0;
		override async attachTail(input: TailAttachInput) {
			if (this.failures === 0) {
				this.failures += 1;
				throw new Error("host hello did not arrive");
			}
			return await super.attachTail(input);
		}
	}
	const port = new HelloLostOncePort();
	const released: string[] = [];
	await harness(port, { released: (opRef) => released.push(opRef) });
	enqueue("hello-lost", "reply after the relay recovers");
	await manager?.notifyInbound(KEY).catch(() => undefined);

	expect(port.failures).toBe(1);
	expect(port.sends).toEqual([]);
	expect(released).toEqual([latestOpRef]);
	// Nothing was sent (#675): the trigger is plain pending again, not stranded bound.
	expect(database?.inbox.turnRow(latestOpRef)).toBeUndefined();
	expect(database?.inbox.pendingOldest(KEY)).toMatchObject({ message_id: "hello-lost", turn_state: null });
});

/**
 * 가8: a gjc session host closes itself after 30 idle minutes. The broker keeps
 * the saved session (live=false, deleted=false; canonical normalization consumes
 * `locator.cwd`). This test double returns undefined for closed-host inspect.
 * `staleLive` models a
 * broker that still reports the closed host live, so the send reaches the dead
 * host and fails with `host hello did not arrive`.
 */
class IdleClosedHostPort extends ScriptedSessionPort {
	closed = new Set<string>();
	staleLive = false;
	deleted = false;
	resumeCalls = 0;

	override async inspect(input: { sessionId: string; repo: string }) {
		if (this.closed.has(input.sessionId)) {
			this.inspections.push(input);
			return undefined;
		}
		return await super.inspect(input);
	}

	override async liveness(input: { sessionId: string; repo: string }) {
		if (!this.closed.has(input.sessionId)) return await super.liveness(input);
		return {
			live: this.staleLive,
			disowned: false,
			workspace: input.repo,
			...(this.deleted ? { deleted: true } : {}),
		};
	}

	override async attachTail(input: TailAttachInput) {
		if (this.closed.has(input.sessionId)) throw new Error("host hello did not arrive");
		return await super.attachTail(input);
	}

	override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
		if (this.closed.has(input.sessionId))
			throw Object.assign(new Error("session_unavailable"), { code: "session_unavailable" });
		return await super.status(input);
	}

	override async resume(input: { sessionId: string; repo: string; originKey: string; epoch: number }) {
		this.resumeCalls += 1;
		const binding = await super.resume(input);
		this.closed.delete(input.sessionId);
		return binding;
	}
}

async function idleClosedHarness() {
	const port = new IdleClosedHostPort({ onBind: (input) => `session-e${input.epoch}` });
	const logs: string[] = [];
	const terminal: string[] = [];
	await harness(port, { terminal: (text) => terminal.push(text) }, (line) => logs.push(line));
	enqueue("before-idle", "earlier conversation");
	await manager!.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "first turn did not start");
	const first = port.sends[0]!;
	port.complete(first.opRef, "earlier answer");
	await eventually(() => terminal.length === 1, "first turn did not finish");
	// Thirty idle minutes later the host is gone.
	port.closed.add(first.sessionId);
	for (const tail of port.tailsOf(first.sessionId)) await tail.close();
	return { port, logs, terminal, sessionId: first.sessionId };
}

test("가8: the first message after the host closed itself resumes the same session before sending", async () => {
	const { port, logs, terminal, sessionId } = await idleClosedHarness();
	enqueue("after-idle", "결론?");
	await manager!.notifyInbound(KEY);
	await eventually(() => port.sends.length === 2, "message after idle was not sent");
	expect(port.resumeCalls).toBe(1);
	expect(port.sends[1]).toMatchObject({ sessionId, text: "결론?" });
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	expect(logs.some((line) => line.startsWith("session_resumed") && line.includes(`session=${sessionId}`))).toBe(true);
	port.complete(port.sends[1]!.opRef, "이어서 답");
	await eventually(() => terminal.length === 2, "resumed turn did not deliver");
	expect(terminal).toEqual(["earlier answer", "이어서 답"]);
});

test("가8: an attach failure after a closed host requeues the trigger without resending or resuming", async () => {
	const { port, logs, terminal, sessionId } = await idleClosedHarness();
	port.staleLive = true;
	enqueue("after-idle", "작업다됨?");
	await manager!.notifyInbound(KEY).catch(() => undefined);
	expect(port.sends).toHaveLength(1);
	const opRef = latestOpRef;
	expect(database!.inbox.turnRow(opRef)).toBeUndefined();

	await manager!.recover();
	await manager!.tick(KEY);
	expect(port.sends).toHaveLength(1);
	expect(port.resumeCalls).toBe(0);
	// No prompt was sent (#675): the trigger is back to plain pending, never bound forever.
	expect(database!.inbox.turnRow(opRef)).toBeUndefined();
	expect(database!.inbox.pendingOldest(KEY)).toMatchObject({ message_id: "after-idle", turn_state: null });
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	expect(logs.some((line) => line.startsWith("recovery_requeue_unaccepted"))).toBe(false);
	expect(terminal).toEqual(["earlier answer"]);
	expect(database!.inbox.pendingCount(KEY)).toBe(1);
});

test("가8: a failed resume cannot authorize a resend or a resume", async () => {
	const { port, logs, sessionId } = await idleClosedHarness();
	port.staleLive = true;
	port.failResume(sessionId);
	enqueue("after-idle", "결론");
	await manager!.notifyInbound(KEY).catch(() => undefined);
	const opRef = latestOpRef;
	await manager!.recover();
	await manager!.tick(KEY);
	expect(port.sends).toHaveLength(1);
	expect(port.resumeCalls).toBe(0);
	expect(logs.some((line) => line.startsWith("recovery_requeue_unaccepted"))).toBe(false);
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	// No prompt was sent (#675): the trigger is back to plain pending, never bound forever.
	expect(database!.inbox.turnRow(opRef)).toBeUndefined();
	expect(database!.inbox.pendingOldest(KEY)).toMatchObject({ message_id: "after-idle", turn_state: null });
});

test("가8: a deleted saved session is never resumed", async () => {
	const { port, sessionId } = await idleClosedHarness();
	port.staleLive = true;
	port.deleted = true;
	enqueue("after-idle", "로컬에놔둬");
	await manager!.notifyInbound(KEY).catch(() => undefined);
	const opRef = latestOpRef;
	await manager!.recover();
	await manager!.tick(KEY);
	expect(port.sends).toHaveLength(1);
	expect(port.resumeCalls).toBe(0);
	expect(database!.bindings.getSessionRecord(KEY)?.epoch).toBe(0);
	// No prompt was sent (#675): the trigger is back to plain pending, never bound forever.
	expect(database!.inbox.turnRow(opRef)).toBeUndefined();
	expect(database!.inbox.pendingOldest(KEY)).toMatchObject({ message_id: "after-idle", turn_state: null });
});

test("가8: a deleted closed binding is not resumed before the send either", async () => {
	const { port } = await idleClosedHarness();
	port.deleted = true;
	enqueue("after-idle", "다음");
	await manager!.notifyInbound(KEY).catch(() => undefined);
	expect(port.resumeCalls).toBe(0);
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
	expect(database?.inbox.turnRow(opRef)).toMatchObject({ state: "done", turn_state: "done" });
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
		"[Internal report that arrived while you were working. Absorb it; mention it to the conversation only if useful.]\n\n[lane child] completed: result",
	);
	expect(port.steers[0]?.text).not.toContain("Additional message from the user");
	expect(database!.inbox.turnRows(latestOpRef)).toEqual(
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
	expect(database!.inbox.turnRow(latestOpRef)).toMatchObject({
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
	database!.transaction((tx) =>
		database!.bindings.cutoverBrokerAuthority(tx, {
			expectedAuthority: authority,
			targetAuthority: { canonicalAgentDir: join(home, "next-agent"), identity: "next-owner" },
			evidence: "test quarantine for admission hold",
			disposition: "quarantine",
		}),
	);
	expect(manager!.admissionHold(KEY)).toBe("quarantined_turn");
});
test("two messages 50ms apart start one turn and steer the second", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	const now = Date.now();
	expect(
		database?.transaction((tx) =>
			database?.inbox.enqueue(tx, {
				messageId: "m-1",
				originKey: KEY,
				originRefJson: JSON.stringify(ORIGIN),
				body: "first fragment",
				receivedAt: new Date(now).toISOString(),
			}),
		),
	).toBe(true);
	expect(
		database?.transaction((tx) =>
			database?.inbox.enqueue(tx, {
				messageId: "m-2",
				originKey: KEY,
				originRefJson: JSON.stringify(ORIGIN),
				body: "second fragment",
				receivedAt: new Date(now + 50).toISOString(),
			}),
		),
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
	expect(database?.inbox.turnRows(latestOpRef)).toEqual([
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
			database?.inbox.turnRows(latestOpRef).every((row) => row.state === "done" && row.turn_state === "done") === true,
		"turn rows did not complete after terminal tail evidence",
	);
});

test("a pending message already rendered into the turn's unread context is closed, not steered twice", async () => {
	// Live 2026-09-27: rows requeued by recovery were both listed as unread in the
	// next turn's prompt AND steered into that turn, so every message was answered twice.
	const port = new ScriptedSessionPort();
	const lines: string[] = [];
	await harness(port, { contextMessageIds: ["m-1", "m-2", "m-3"] }, (line) => lines.push(line));
	const now = Date.now();
	for (const [index, id] of ["m-1", "m-2", "m-3", "m-4"].entries())
		expect(
			database?.transaction((tx) =>
				database?.inbox.enqueue(tx, {
					messageId: id,
					originKey: KEY,
					originRefJson: JSON.stringify(ORIGIN),
					body: `body ${id}`,
					receivedAt: new Date(now + index * 10).toISOString(),
				}),
			),
		).toBe(true);

	await manager?.notifyInbound(KEY);

	expect(manager?.state(KEY)).toBe("turn-running");
	expect(port.sends).toEqual([expect.objectContaining({ text: "body m-1", opRef: latestOpRef })]);
	// Only the message that was NOT in the prompt's unread context is steered.
	expect(port.steers).toEqual([expect.objectContaining({ text: expect.stringMatching(/\nbody m-4$/) })]);
	expect(database?.inbox.pendingOldest(KEY)).toBeUndefined();
	for (const id of ["m-2", "m-3"])
		expect(database?.inbox.turnRows(latestOpRef).find((row) => row.message_id === id)).toMatchObject({
			state: "done",
			turn_role: "steer",
			turn_state: "done",
		});
	expect(lines.filter((line) => line.startsWith("steer_skip "))).toHaveLength(2);

	// A later tick must not re-offer the closed rows.
	await manager?.notifyInbound(KEY);
	expect(port.steers).toHaveLength(1);
	port.complete(latestOpRef, "done");
});

test("PR #337: an edit row for a message in unread context IS still steered despite the original being in context", async () => {
	// Regression: edits carry new text and must be steered even when the original message is in the prompt.
	const port = new ScriptedSessionPort();
	const lines: string[] = [];
	await harness(port, { contextMessageIds: ["original-msg"] }, (line) => lines.push(line));
	const now = Date.now();
	expect(
		database?.transaction((tx) =>
			database?.inbox.enqueue(tx, {
				messageId: "trigger-msg",
				originKey: KEY,
				originRefJson: JSON.stringify(ORIGIN),
				body: "Start working",
				receivedAt: new Date(now).toISOString(),
			}),
		),
	).toBe(true);
	expect(
		database?.transaction((tx) =>
			database?.inbox.enqueue(tx, {
				messageId: "edit-msg",
				originKey: KEY,
				originRefJson: JSON.stringify(ORIGIN),
				body: "Actually, do this instead",
				receivedAt: new Date(now + 50).toISOString(),
			}),
		),
	).toBe(true);

	await manager?.notifyInbound(KEY);

	expect(manager?.state(KEY)).toBe("turn-running");
	expect(port.sends).toEqual([expect.objectContaining({ text: "Start working", opRef: latestOpRef })]);
	// The edit is steered despite the original message being in context.
	const sendSession = port.sends[0]?.sessionId;
	expect(sendSession).toBeDefined();
	expect(port.steers).toEqual([
		expect.objectContaining({
			sessionId: sendSession,
			text: expect.stringMatching(/\nActually, do this instead$/),
		}),
	]);
	expect(database?.inbox.turnRows(latestOpRef).find((row) => row.message_id === "edit-msg")).toMatchObject({
		state: "done",
		turn_role: "steer",
		turn_state: "done",
	});
	expect(lines.filter((line) => line.startsWith("steer_skip "))).toHaveLength(0);
	port.complete(latestOpRef, "done");
});

test("PR #337: after turn terminal and restart, an in-context message is never sent as trigger nor steered", async () => {
	// Regression: in-context messages closed as steers during the turn should not be re-sent as triggers after restart.
	const port = new ScriptedSessionPort();
	const logs: string[] = [];
	await harness(port, { contextMessageIds: ["in-context-msg"] }, (line) => logs.push(line));
	const now = Date.now();
	expect(
		database?.transaction((tx) =>
			database?.inbox.enqueue(tx, {
				messageId: "trigger",
				originKey: KEY,
				originRefJson: JSON.stringify(ORIGIN),
				body: "Trigger",
				receivedAt: new Date(now).toISOString(),
			}),
		),
	).toBe(true);
	expect(
		database?.transaction((tx) =>
			database?.inbox.enqueue(tx, {
				messageId: "in-context-msg",
				originKey: KEY,
				originRefJson: JSON.stringify(ORIGIN),
				body: "In context",
				receivedAt: new Date(now + 50).toISOString(),
			}),
		),
	).toBe(true);

	await manager?.notifyInbound(KEY);
	const triggerOpRef = latestOpRef;
	const sendCount = port.sends.length;
	const steerCount = port.steers.length;

	// The in-context message is closed as a steer of this turn.
	expect(database?.inbox.turnRows(triggerOpRef).find((row) => row.message_id === "in-context-msg")).toMatchObject({
		state: "done",
		turn_role: "steer",
		turn_state: "done",
	});

	// Complete the turn.
	port.complete(triggerOpRef, "reply");
	await eventually(() => database?.inbox.turnRow(triggerOpRef)?.turn_state === "done", "turn did not terminal");

	// Stop the manager.
	await manager?.stop();

	// Restart: fresh manager on the same database.
	manager = new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
		}),
	});

	await manager.recover();
	await manager.tick(KEY);

	// After restart: no new sends or steers for the closed message.
	expect(port.sends).toHaveLength(sendCount);
	expect(port.steers).toHaveLength(steerCount);
	// The inbound row is still done.
	expect(database?.inbox.turnRow(triggerOpRef)).toMatchObject({ state: "done", turn_state: "done" });
	const contextRow = database?.inbox.turnRows(triggerOpRef).find((row) => row.message_id === "in-context-msg");
	expect(contextRow).toMatchObject({ state: "done", turn_role: "steer", turn_state: "done" });
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
	await eventually(() => database?.inbox.turnRow(send.opRef)?.turn_state === "done", "recovered turn did not complete");
	await manager.stop();
	expect(port.indexScans).toBe(0);
	expect(port.deleted).toEqual([]);
	expect(logs.some((line) => line.startsWith("session_gc"))).toBe(false);
});

/** Reports a scripted context usage of a 1M-token window, like `context.get` on the turn's relay. */
class ContextUsagePort extends ScriptedSessionPort {
	percent: number | undefined = 1;
	readonly failing = new Set<string>();
	autoComplete = true;
	compactionStatus: SessionCompactionStatus = "succeeded";
	/** How many sends had been made when each compaction ran. */
	readonly compactedAfterSends: number[] = [];

	constructor() {
		super({
			onSend: (input, port) => {
				if (this.failing.has(input.text)) port.fail(input.opRef, "post-start failure");
				else if (this.autoComplete) port.complete(input.opRef, "ok");
			},
		});
	}

	async contextUsage(): Promise<SessionContextUsage | undefined> {
		return this.percent === undefined ? undefined : { percent: this.percent, tokens: this.percent * 10_000 };
	}

	override async runCompaction(): Promise<{ readonly status: SessionCompactionStatus }> {
		this.compactedAfterSends.push(this.sends.length);
		if (this.compactionStatus === "succeeded") this.percent = undefined;
		return { status: this.compactionStatus };
	}
}

const PERSONA = "## SOUL.md\npersona rules";

async function preambleHarness(
	port: ScriptedSessionPort,
	lifecycle: { persona: () => string; bootstrap?: () => string | undefined },
	log?: (line: string) => void,
): Promise<void> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-persona-preamble-"));
	database = await Store.open(join(home, "gateway.db"));
	registerFixtureBindings(port);
	manager = new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		...(log ? { log } : {}),
		onTurnStart: ({ trigger }) => {
			const bootstrap = lifecycle.bootstrap?.();
			return {
				text: trigger.body,
				systemPreamble: lifecycle.persona(),
				...(bootstrap ? { sessionBootstrap: bootstrap } : {}),
			};
		},
	});
}

async function sendTurn(port: ScriptedSessionPort, body: string): Promise<string | undefined> {
	const before = port.sends.length;
	enqueue(`m-${before}-${body}`, body);
	await manager!.notifyInbound(KEY);
	await eventually(
		() => port.sends.length === before + 1 && manager!.state(KEY) === "idle",
		`turn ${body} did not settle`,
	);
	return port.sends[before]!.systemPreamble;
}

test("persona preamble rides the first send only while the context keeps growing", async () => {
	const port = new ContextUsagePort();
	let bootstrap: string | undefined = "bootstrap once";
	await preambleHarness(port, { persona: () => PERSONA, bootstrap: () => bootstrap });

	expect(await sendTurn(port, "first")).toBe(`${PERSONA}\n\nbootstrap once`);
	bootstrap = undefined;
	port.percent = 2;
	expect(await sendTurn(port, "second")).toBeUndefined();
	port.percent = 2;
	expect(await sendTurn(port, "same usage")).toBeUndefined();
	bootstrap = "bootstrap without persona";
	port.percent = 3;
	expect(await sendTurn(port, "bootstrap only")).toBe("bootstrap without persona");
	expect(new Set(port.sends.map((send) => send.sessionId)).size).toBe(1);
});

test("persona preamble is resent after the context shrank (compaction)", async () => {
	const port = new ContextUsagePort();
	await preambleHarness(port, { persona: () => PERSONA });
	// gjc compacted on its own: the actor's compaction line is never reached.
	port.percent = 15;
	expect(await sendTurn(port, "first")).toBe(PERSONA);
	port.percent = 15.5;
	expect(await sendTurn(port, "grown")).toBeUndefined();
	port.percent = 4;
	expect(await sendTurn(port, "after compaction")).toBe(PERSONA);
	port.percent = 4.5;
	expect(await sendTurn(port, "grown again")).toBeUndefined();
	expect(port.compactedAfterSends).toEqual([]);
});

test("persona preamble is resent when its text changes", async () => {
	const port = new ContextUsagePort();
	let persona = PERSONA;
	await preambleHarness(port, { persona: () => persona });
	expect(await sendTurn(port, "first")).toBe(PERSONA);
	persona = `${PERSONA}\nnew rule`;
	port.percent = 2;
	expect(await sendTurn(port, "edited")).toBe(persona);
	port.percent = 3;
	expect(await sendTurn(port, "unchanged")).toBeUndefined();
});

test("persona preamble is resent on every send while context usage is unreadable", async () => {
	const port = new ContextUsagePort();
	port.percent = undefined;
	await preambleHarness(port, { persona: () => PERSONA });
	expect(await sendTurn(port, "first")).toBe(PERSONA);
	expect(await sendTurn(port, "second")).toBe(PERSONA);
});

test("a failed turn makes the next send carry the persona preamble again", async () => {
	const port = new ContextUsagePort();
	port.failing.add("fails");
	await preambleHarness(port, { persona: () => PERSONA });
	expect(await sendTurn(port, "first")).toBe(PERSONA);
	port.percent = 2;
	expect(await sendTurn(port, "fails")).toBeUndefined();
	port.percent = 2;
	expect(await sendTurn(port, "after failure")).toBe(PERSONA);
	expect(new Set(port.sends.map((send) => send.sessionId)).size).toBe(1);
});

test("a provider retry re-sends a preamble that the failed call may not have committed", async () => {
	const port = new ContextUsagePort();
	port.autoComplete = false;
	port.percent = 5;
	await preambleHarness(port, { persona: () => PERSONA });

	enqueue("provider-preamble-retry", "retry with persona");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0];
	if (!first) throw new Error("initial preamble-bearing request was not dispatched");
	expect(first.systemPreamble).toBe(PERSONA);
	port.fail(first.opRef, "Provider unavailable", {
		code: "provider_unavailable",
		outcome: { kind: "failed", phase: "post_start", category: "provider_transport", provenance: "agent_failed" },
	});

	await new Promise((resolve) => setTimeout(resolve, 1_500));
	await manager!.tick(KEY);
	const retry = port.sends[1];
	if (!retry) throw new Error("provider retry was not dispatched");
	expect(retry.systemPreamble).toBe(PERSONA);
	port.complete(retry.opRef, "completed after provider retry");
	await manager!.tick(KEY);
	await eventually(() => manager!.state(KEY) === "idle", "provider retry did not settle");
});

test("persona preamble is refreshed after the bounded number of sends without it", async () => {
	const port = new ContextUsagePort();
	await preambleHarness(port, { persona: () => PERSONA });
	const carried: boolean[] = [];
	for (let send = 0; send <= PERSONA_PREAMBLE_REFRESH_SENDS; send++) {
		// Growing, but below the compaction line throughout.
		port.percent = 1 + send * 0.5;
		carried.push((await sendTurn(port, `turn ${send}`)) === PERSONA);
	}
	expect(port.compactedAfterSends).toEqual([]);
	expect(carried.map((value, index) => (value ? index : -1)).filter((index) => index >= 0)).toEqual([
		0,
		PERSONA_PREAMBLE_REFRESH_SENDS,
	]);
});

/** Percent of the fake 1M-token window at which the actor compacts. */
const COMPACTION_PERCENT = PERSONA_COMPACTION_TOKENS / 10_000;

test("a session at the compaction line is compacted before the send, which carries the persona again", async () => {
	const port = new ContextUsagePort();
	const logs: string[] = [];
	await preambleHarness(port, { persona: () => PERSONA }, (line) => logs.push(line));
	port.percent = 5;
	expect(await sendTurn(port, "first")).toBe(PERSONA);
	port.percent = COMPACTION_PERCENT - 0.01;
	expect(await sendTurn(port, "just under the line")).toBeUndefined();
	expect(port.compactedAfterSends).toEqual([]);

	port.percent = COMPACTION_PERCENT;
	expect(await sendTurn(port, "at the line")).toBe(PERSONA);
	expect(port.compactedAfterSends).toEqual([2]);
	expect(
		logs.some(
			(line) =>
				line.startsWith("persona_compaction ") &&
				line.includes(`tokens=${PERSONA_COMPACTION_TOKENS}`) &&
				line.includes("status=succeeded"),
		),
	).toBe(true);
	expect(logs.some((line) => line.startsWith("persona_preamble ") && line.includes("reason=compacted"))).toBe(true);

	// Regrowing from the compacted size rides the copy the compacted send put back.
	port.percent = 3;
	expect(await sendTurn(port, "after compaction")).toBeUndefined();
	expect(port.compactedAfterSends).toEqual([2]);
	expect(new Set(port.sends.map((send) => send.sessionId)).size).toBe(1);
});

test("a compaction that does not succeed still sends the turn on the same session", async () => {
	const port = new ContextUsagePort();
	const logs: string[] = [];
	await preambleHarness(port, { persona: () => PERSONA }, (line) => logs.push(line));
	port.percent = 5;
	expect(await sendTurn(port, "first")).toBe(PERSONA);
	port.compactionStatus = "failed";
	port.percent = COMPACTION_PERCENT + 5;
	expect(await sendTurn(port, "compaction fails")).toBeUndefined();
	port.compactionStatus = "unavailable";
	port.percent = COMPACTION_PERCENT + 6;
	expect(await sendTurn(port, "compaction unavailable")).toBeUndefined();
	expect(port.compactedAfterSends).toEqual([1, 2]);
	expect(port.sends.map((send) => send.text)).toEqual(["first", "compaction fails", "compaction unavailable"]);
	expect(new Set(port.sends.map((send) => send.sessionId)).size).toBe(1);
	expect(
		logs.filter((line) => line.startsWith("persona_compaction ")).map((line) => line.match(/status=\w+/)?.[0]),
	).toEqual(["status=failed", "status=unavailable"]);
});

test("messages arriving while the actor compacts wait for the compacted send instead of reaching the session", async () => {
	let releaseCompaction!: () => void;
	const compactionGate = new Promise<void>((resolve) => {
		releaseCompaction = resolve;
	});
	class GatedCompactionPort extends ContextUsagePort {
		override async runCompaction(): Promise<{ readonly status: SessionCompactionStatus }> {
			const result = super.runCompaction();
			await compactionGate;
			return await result;
		}
	}
	const port = new GatedCompactionPort();
	await preambleHarness(port, { persona: () => PERSONA });
	port.percent = COMPACTION_PERCENT + 1;
	enqueue("m-full", "lands after compaction");
	void manager!.notifyInbound(KEY);
	await eventually(() => port.compactedAfterSends.length === 1, "compaction did not start");
	enqueue("m-during", "arrives during compaction");
	void manager!.notifyInbound(KEY);
	await Bun.sleep(20);
	expect(port.sends).toHaveLength(0);
	expect(port.steers).toHaveLength(0);

	releaseCompaction();
	await eventually(
		() => port.sends.length + port.steers.length === 2 && manager!.state(KEY) === "idle",
		"queued messages did not follow the compacted send",
	);
	expect(port.sends[0]).toMatchObject({ text: "lands after compaction", systemPreamble: PERSONA });
	expect(port.compactedAfterSends).toEqual([0]);
});

test("status-settled predecessor frames cannot affect its reused relay's delayed successor", async () => {
	const port = new ScriptedSessionPort();
	const terminal: string[] = [];
	await harness(port, {
		terminal: (text) => {
			terminal.push(text);
		},
	});
	enqueue("status-predecessor", "first question");
	await manager!.notifyInbound(KEY);
	const first = port.sends[0]!;
	const tail = port.tailsOf(first.sessionId)[0]!;
	const emit = tail.emit.bind(tail);
	// The status/result path proves A terminal while its content/terminal never
	// arrives. Keep the same live relay so the successor truly reuses it.
	tail.emit = () => {};
	port.complete(first.opRef, "first status answer");
	await manager!.tick(KEY);
	await eventually(() => manager!.state(KEY) === "idle", "status predecessor did not settle");
	tail.emit = emit;
	let entered = () => {};
	let release = () => {};
	const started = new Promise<void>((resolve) => {
		entered = resolve;
	});
	const resume = new Promise<void>((resolve) => {
		release = resolve;
	});
	Object.defineProperty(port, "contextUsage", {
		value: async () => {
			entered();
			await resume;
			return undefined;
		},
	});
	enqueue("status-successor", "second question");
	const dispatch = manager!.notifyInbound(KEY);
	await started;
	port.emitAssistant(first.sessionId, "late first answer", "old-event", first.opRef);
	port.completeWithoutAnswerFrame(first.opRef, "first status answer");
	release();
	await dispatch;
	await Bun.sleep(10);
	const second = port.sends[1]!;
	expect(port.tailsOf(first.sessionId)[0]).toBe(tail);
	expect(database!.inbox.turnRow(second.opRef)!.turn_state).toBe("accepted");
	expect(terminal).toHaveLength(1);
	port.complete(second.opRef, "second answer");
	await eventually(() => manager!.state(KEY) === "idle", "successor did not finish");
	expect(terminal).toEqual(["first status answer", "second answer"]);
});

async function agentObserverFixture() {
	let now = 10_000;
	const logs: string[] = [];
	const noticeTimers = new Set<() => void>();
	const port = new ScriptedSessionPort();
	await harness(port, {}, (line) => logs.push(line), {
		now: () => now,
		maxLiveHosts: 1,
		hostIdleRetireMs: AGENT_RUN_MAX_MS + 1,
		setTimeout: (work, delay) => {
			if (delay === 3_000) {
				noticeTimers.add(work);
				return work;
			}
			return setTimeout(work, delay);
		},
		clearTimeout: (timer) => {
			if (typeof timer === "function") noticeTimers.delete(timer as () => void);
			else clearTimeout(timer as ReturnType<typeof setTimeout>);
		},
	});
	enqueue("observer-seed", "start background work");
	await manager!.notifyInbound(KEY);
	const send = port.sends[0]!;
	port.complete(send.opRef, "working");
	await eventually(() => manager!.state(KEY) === "idle", "seed did not settle");
	port.setActivity(send.sessionId, "idle", now);
	database!.transaction((tx) => database!.bindings.touch(tx, KEY, JSON.stringify(ORIGIN)));
	const end = () => {
		const tail = port.tailsOf(send.sessionId)[0]!;
		tail.emit({ kind: "agent_start", rawKind: "agent_start", payload: {}, idle: false, steerEcho: false });
		tail.emit({ kind: "agent_end", rawKind: "agent_end", payload: {}, idle: true, steerEcho: false });
	};
	return {
		port,
		send,
		logs,
		end,
		noticeTimers,
		advance: (ms: number) => {
			now += ms;
		},
		sweep: async () => await manager!.sweepIdleHosts(now),
		fireNotice: () => {
			const work = [...noticeTimers];
			noticeTimers.clear();
			for (const run of work) run();
		},
	};
}

for (const outcome of ["terminated", "already_gone"] as const)
	test(`confirmed stalled-host exit (${outcome}) closes an active observer before unavailable post-stop probes`, async () => {
		const f = await agentObserverFixture();
		const active = manager!;
		enqueue("joined-turn", "continue the work");
		await active.notifyInbound(KEY);
		const joined = f.port.sends[1]!;
		const tail = f.port.tailsOf(joined.sessionId)[0]!;
		expect(joined.sessionId).toBe(f.send.sessionId);
		f.end();
		await eventually(() => f.noticeTimers.size === 1, "notice debounce was not armed");
		const queuedDebounce = [...f.noticeTimers][0]!;
		tail.emit({ kind: "agent_start", rawKind: "agent_start", payload: {}, idle: false, steerEcho: false });
		await active.reconcile(KEY);
		f.port.setActivity(joined.sessionId, "tool", 10_000);
		f.port.emitStall(joined.sessionId);
		await active.reconcile(KEY);
		f.advance(STALLED_TURN_TERMINAL_TIMEOUT_MS);
		let exited = false;
		let releases = 0;
		const hostFreed = active.hostFreed.bind(active);
		active.hostFreed = () => {
			expect(
				f.logs.some((line) => line.includes("agent_run_observer_closed") && line.includes("reason=host_terminated")),
			).toBe(true);
			releases++;
			hostFreed();
		};
		const status = f.port.status.bind(f.port);
		f.port.status = async (input) => {
			if (exited) throw new Error("post-stop status unavailable");
			return await status(input);
		};
		f.port.terminateHost = async (input) => {
			expect(await input.shouldTerminate?.()).toBe(true);
			exited = true;
			// Receipt occurs inside the cutoff mailbox entry; its lifecycle callback
			// runs later, after confirmed-exit cleanup and the failed status read.
			tail.emit({ kind: "agent_end", rawKind: "agent_end", payload: {}, idle: true, steerEcho: false });
			f.port.hostJobs.set(joined.sessionId, new Error("post-stop jobs unavailable"));
			f.port.liveness = async () => {
				throw new Error("post-stop liveness unavailable");
			};
			return outcome === "terminated" ? { outcome, pid: 40_001 } : { outcome };
		};
		let attaches = 0;
		const attach = f.port.attachTail.bind(f.port);
		f.port.attachTail = async (input) => {
			attaches++;
			return await attach(input);
		};
		await active.tick(KEY);
		await active.reconcile(KEY);
		expect(exited).toBe(true);
		expect(f.logs.some((line) => line.startsWith("stalled_turn_post_stop_status_unavailable "))).toBe(true);
		expect(database!.inbox.turnRow(joined.opRef)).toMatchObject({
			turn_state: "done",
			terminal_delivery_id: JSON.stringify({ none: "turn_failed" }),
		});
		expect(releases).toBe(1);
		expect(f.noticeTimers.size).toBe(0);
		queuedDebounce();
		await active.reconcile(KEY);
		await tail.close();
		await f.sweep();
		expect(attaches).toBe(0);
		expect(f.port.tailsOf(joined.sessionId)).toHaveLength(0);
		expect(f.logs.filter((line) => line.startsWith("agent_run_notice_enqueued "))).toHaveLength(0);
		expect(database!.inbox.hasPendingAgentRunNotice(KEY)).toBe(false);
		expect(f.port.sends).toHaveLength(2);
		expect(f.port.steers).toHaveLength(0);
		expect(releases).toBe(1);
	});

for (const guard of ["notice", "sweep"] as const)
	test(`${guard} rejects an observer on a terminating host with unavailable probes`, async () => {
		const f = await agentObserverFixture();
		const active = manager!;
		enqueue("terminating-joined-turn", "continue working");
		await active.notifyInbound(KEY);
		const joined = f.port.sends[1]!;
		const tail = f.port.tailsOf(joined.sessionId)[0]!;
		tail.emit({ kind: "agent_start", rawKind: "agent_start", payload: {}, idle: false, steerEcho: false });
		await active.reconcile(KEY);
		f.port.setActivity(joined.sessionId, "tool", 10_000);
		f.port.emitStall(joined.sessionId);
		await active.reconcile(KEY);
		f.advance(STALLED_TURN_TERMINAL_TIMEOUT_MS);
		let stopping = false;
		const status = f.port.status.bind(f.port);
		f.port.status = async (input) => {
			if (stopping) throw new Error("shutdown status unavailable");
			return await status(input);
		};
		f.port.terminateHost = async (input) => {
			expect(await input.shouldTerminate?.()).toBe(true);
			stopping = true;
			tail.emit({ kind: "agent_end", rawKind: "agent_end", payload: {}, idle: true, steerEcho: false });
			f.port.hostJobs.set(joined.sessionId, new Error("shutdown jobs unavailable"));
			f.port.liveness = async () => {
				throw new Error("shutdown liveness unavailable");
			};
			return { outcome: "terminating", pid: 40_001 };
		};
		await active.tick(KEY);
		await active.reconcile(KEY);
		expect(f.noticeTimers.size).toBe(1);
		let attaches = 0;
		const attach = f.port.attachTail.bind(f.port);
		f.port.attachTail = async (input) => {
			attaches++;
			return await attach(input);
		};
		if (guard === "notice") {
			f.fireNotice();
			await active.reconcile(KEY);
		} else await tail.close();
		await f.sweep();
		expect(
			f.logs.some((line) => line.includes("agent_run_observer_closed") && line.includes("reason=host_terminated")),
		).toBe(true);
		expect(attaches).toBe(0);
		expect(f.noticeTimers.size).toBe(0);
		expect(database!.inbox.hasPendingAgentRunNotice(KEY)).toBe(false);
		expect(f.logs.some((line) => line.startsWith("agent_run_notice_enqueued "))).toBe(false);
		expect(active.liveHostCount()).toBe(1);
		expect(f.port.sends).toHaveLength(2);
	});

test("every settled turn retains its relay through grace even without running jobs", async () => {
	const f = await agentObserverFixture();
	expect(f.port.tailsOf(f.send.sessionId)).toHaveLength(1);
	f.advance(AGENT_RUN_GRACE_MS - 1);
	await f.sweep();
	expect(f.port.tailsOf(f.send.sessionId)).toHaveLength(1);
	f.advance(1);
	await f.sweep();
	expect(f.port.tailsOf(f.send.sessionId)).toHaveLength(0);
	expect(
		f.logs.some((line) => line.includes("agent_run_observer_closed") && line.includes("reason=grace_expired")),
	).toBe(true);
});

test("observer with background jobs protects an otherwise idle host against capacity retirement", async () => {
	const f = await agentObserverFixture();
	f.port.hostJobs.set(f.send.sessionId, [{ id: "async-job", type: "task", label: "background work" }]);
	const other = "loopback/loopback/other-persona";
	database!.transaction((tx) =>
		database!.inbox.enqueue(tx, {
			messageId: "other-host",
			originKey: other,
			originRefJson: JSON.stringify({ ...ORIGIN, conversationId: "other-persona" }),
			body: "new work",
		}),
	);
	await manager!.notifyInbound(other);
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.tailsOf(f.send.sessionId)).toHaveLength(1);
	expect(f.logs.some((line) => line.startsWith("persona_host_retired "))).toBe(false);
});

test("pending notice decision prevents grace retirement until its debounce finishes", async () => {
	const f = await agentObserverFixture();
	f.end();
	await eventually(() => f.noticeTimers.size === 1, "autonomous end did not schedule notice");
	f.advance(AGENT_RUN_GRACE_MS);
	await f.sweep();
	expect(f.port.tailsOf(f.send.sessionId)).toHaveLength(1);
	f.fireNotice();
	await eventually(() => f.port.sends.length === 2, "notice did not dispatch");
	expect(f.port.sends[1]!.text).toContain("[Gateway notice]");
});

test("running jobs and unavailable reports extend grace only up to the six-hour cap", async () => {
	const f = await agentObserverFixture();
	f.port.hostJobs.set(f.send.sessionId, [{ id: "async", type: "task", label: "background work" }]);
	f.advance(AGENT_RUN_GRACE_MS * 2);
	await f.sweep();
	expect(f.port.tailsOf(f.send.sessionId)).toHaveLength(1);
	f.port.hostJobs.set(f.send.sessionId, new Error("jobs unavailable"));
	f.advance(AGENT_RUN_GRACE_MS * 2);
	await f.sweep();
	expect(f.port.tailsOf(f.send.sessionId)).toHaveLength(1);
	f.advance(AGENT_RUN_MAX_MS);
	await f.sweep();
	expect(f.port.tailsOf(f.send.sessionId)).toHaveLength(0);
});

test("capacity-evicted observer warns and sweep reattaches within grace", async () => {
	const f = await agentObserverFixture();
	await f.port.tailsOf(f.send.sessionId)[0]!.close();
	expect(f.logs.some((line) => line.startsWith("agent_run_observer_lost "))).toBe(true);
	await f.sweep();
	expect(f.port.tailsOf(f.send.sessionId)).toHaveLength(1);
	expect(f.logs.some((line) => line.includes("agent_run_observer_open") && line.includes("reason=reattach"))).toBe(
		true,
	);
});

test("an autonomous run starting with no jobs retains observation past grace and ends in one notice", async () => {
	const f = await agentObserverFixture();
	f.port
		.tailsOf(f.send.sessionId)[0]!
		.emit({ kind: "agent_start", rawKind: "agent_start", payload: {}, idle: false, steerEcho: false });
	// Drain the tail's serialized callback before evaluating expiry; even a stale
	// idle liveness report must not override an observed unfinished autonomous run.
	await Bun.sleep(10);
	f.advance(AGENT_RUN_GRACE_MS * 2);
	await f.sweep();
	expect(f.port.tailsOf(f.send.sessionId)).toHaveLength(1);
	expect(f.logs.some((line) => line.startsWith("agent_run_observer_closed "))).toBe(false);
	f.end();
	await eventually(() => f.noticeTimers.size === 1, "no completion notice timer");
	f.fireNotice();
	await eventually(() => f.port.sends.length === 2, "no gateway notice turn");
	expect(f.logs.filter((line) => line.startsWith("agent_run_notice_enqueued "))).toHaveLength(1);
});

test("an active autonomous observer lost across its terminal infers completion from idle liveness", async () => {
	const f = await agentObserverFixture();
	const tail = f.port.tailsOf(f.send.sessionId)[0]!;
	tail.emit({ kind: "agent_start", rawKind: "agent_start", payload: {}, idle: false, steerEcho: false });
	await Bun.sleep(10);
	await tail.close();
	f.port.setActivity(f.send.sessionId, "idle");
	await f.sweep();
	expect(f.noticeTimers.size).toBe(1);
	f.fireNotice();
	await eventually(() => f.port.sends.length === 2, "lost terminal did not produce a notice");
	expect(f.logs.filter((line) => line.startsWith("agent_run_notice_enqueued "))).toHaveLength(1);
});

test("a start missed during observer loss is inferred from working liveness without a gateway owner", async () => {
	const f = await agentObserverFixture();
	await f.port.tailsOf(f.send.sessionId)[0]!.close();
	f.port.setActivity(f.send.sessionId, "working");
	await f.sweep();
	const recovered = f.port.tailsOf(f.send.sessionId)[0]!;
	expect(recovered.running).toBe(false);
	f.port.setActivity(f.send.sessionId, "idle");
	recovered.emit({ kind: "agent_end", rawKind: "agent_end", payload: {}, idle: true, steerEcho: false });
	await eventually(() => f.noticeTimers.size === 1, "inferred start did not attribute the end");
	f.fireNotice();
	await eventually(() => f.port.sends.length === 2, "inferred run produced no notice");
	expect(f.logs.filter((line) => line.startsWith("agent_run_notice_enqueued "))).toHaveLength(1);
});

test("notice enqueue refusal retains its batch for retry", async () => {
	const f = await agentObserverFixture();
	const enqueue = database!.inbox.enqueue.bind(database!.inbox);
	database!.inbox.enqueue = () => false;
	f.end();
	await eventually(() => f.noticeTimers.size === 1, "no notice timer");
	f.fireNotice();
	await eventually(() => f.logs.some((line) => line.includes("reason=notice_failed")), "refusal was not reported");
	expect(f.logs.some((line) => line.startsWith("agent_run_notice_enqueued "))).toBe(false);
	database!.inbox.enqueue = enqueue;
	await f.sweep();
	await eventually(() => f.port.sends.length === 2, "failed notice was not retried");
	expect(f.logs.filter((line) => line.startsWith("agent_run_notice_enqueued "))).toHaveLength(1);
});

test("persistent notice enqueue refusal cannot retain observer or retirement guards past the hard cap", async () => {
	const f = await agentObserverFixture();
	f.port.liveness = async () => {
		throw new Error("liveness temporarily unavailable");
	};
	const enqueueNotice = database!.inbox.enqueue.bind(database!.inbox);
	database!.inbox.enqueue = () => false;
	f.end();
	await eventually(() => f.noticeTimers.size === 1, "no notice timer");
	f.fireNotice();
	await eventually(
		() => f.logs.some((line) => line.includes("reason=notice_failed")),
		"timer refusal was not reported",
	);
	f.advance(AGENT_RUN_GRACE_MS);
	await f.sweep();
	expect(f.logs.filter((line) => line.startsWith("agent_run_notice_failed "))).toHaveLength(1);
	expect(f.port.tailsOf(f.send.sessionId)).toHaveLength(1);
	f.advance(AGENT_RUN_MAX_MS);
	await f.sweep();
	expect(f.logs.filter((line) => line.startsWith("agent_run_notice_failed "))).toHaveLength(2);
	expect(f.port.tailsOf(f.send.sessionId)).toHaveLength(0);
	expect(f.noticeTimers.size).toBe(0);
	expect(
		f.logs.some((line) => line.startsWith("agent_run_observer_closed ") && line.includes("reason=grace_expired")),
	).toBe(true);
	database!.inbox.enqueue = enqueueNotice;
	// A completed but undeliverable notice must no longer hold host admission.
	database!.transaction((tx) =>
		database!.inbox.enqueue(tx, {
			messageId: "after-refusal",
			originKey: "loopback/loopback/after-refusal",
			originRefJson: JSON.stringify({ ...ORIGIN, conversationId: "after-refusal" }),
			body: "new origin",
		}),
	);
	await manager!.notifyInbound("loopback/loopback/after-refusal");
	expect(f.port.sends).toHaveLength(2);
	expect(f.port.closes.some((close) => close.sessionId === f.send.sessionId)).toBe(true);
});

test("working or unavailable liveness extends observation without running jobs", async () => {
	const f = await agentObserverFixture();
	f.port.setActivity(f.send.sessionId, "working");
	f.advance(AGENT_RUN_GRACE_MS * 2);
	await f.sweep();
	expect(f.port.tailsOf(f.send.sessionId)).toHaveLength(1);
	f.port.liveness = async () => {
		throw new Error("liveness unavailable");
	};
	f.advance(AGENT_RUN_GRACE_MS * 2);
	await f.sweep();
	expect(f.port.tailsOf(f.send.sessionId)).toHaveLength(1);
	f.advance(AGENT_RUN_MAX_MS);
	await f.sweep();
	expect(f.port.tailsOf(f.send.sessionId)).toHaveLength(0);
});

test("reset cancels a pending notice and closes the observer", async () => {
	const f = await agentObserverFixture();
	f.end();
	await eventually(() => f.noticeTimers.size === 1, "no notice timer");
	await manager!.reset(KEY, JSON.stringify(ORIGIN));
	expect(f.noticeTimers.size).toBe(0);
	expect(f.port.tailsOf(f.send.sessionId)).toHaveLength(0);
	expect(f.logs.some((line) => line.includes("agent_run_observer_closed") && line.includes("reason=reset"))).toBe(true);
});

test("external epoch rotation fences the observer before it can enqueue a notice", async () => {
	const f = await agentObserverFixture();
	f.end();
	await eventually(() => f.noticeTimers.size === 1, "no notice timer");
	database!.transaction((tx) => database!.bindings.rotateEpoch(tx, KEY));
	f.fireNotice();
	await eventually(() => f.port.tailsOf(f.send.sessionId).length === 0, "rotated observer did not close");
	expect(f.logs.some((line) => line.startsWith("agent_run_notice_enqueued "))).toBe(false);
});

test("P2-2: escalated hold with interleaved successful status reads does not close on endpoint_stale", async () => {
	class InterleavedSuccessPort extends WedgedSessionPort {
		readCount = 0;
		override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
			this.readCount++;
			// Pattern: stale (1) -> success (2) -> stale (3) -> success (4) -> stale (5) -> should NOT close
			const mod = this.readCount % 2;
			if (mod === 1) throw endpointStale();
			return { operationRef: input.opRef, status: { status: "unknown" }, summaryCompleted: false };
		}
		override async liveness(input: Parameters<ScriptedSessionPort["liveness"]>[0]) {
			return { live: false, disowned: false }; // Dead host to trigger escalation
		}
	}
	const port = new InterleavedSuccessPort();
	const logs: string[] = [];
	await harness(port, {}, (line) => logs.push(line), { unresolvedHoldMs: 50 });

	enqueue("m-1", "question");
	await manager!.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "send did not occur");
	const send = port.sends[0]!;

	// Advance time to trigger escalation
	await Bun.sleep(60);
	await manager!.tick(KEY);
	await eventually(
		() => logs.some((line) => line.includes("unresolved_hold_escalated")),
		"escalation did not occur",
	);

	// Now perform multiple status reads with interleaved success
	// Reads 1,3,5 are stale, 2,4 are success -> after 3 stale with successes between, should NOT close
	for (let i = 0; i < 5; i++) {
		await manager!.tick(KEY);
		await Bun.sleep(5);
	}

	// The turn should still be held, not closed as 'retired'
	expect(database!.inbox.turnRow(send.opRef)?.turn_state).not.toBe("done");
	const closedLogs = logs.filter((line) => line.includes("retired_turn_closed"));
	expect(closedLogs.length).toBe(0);
});

test("P2-3: restart with already-escalated bound turn adopts it into retired reconciliation", async () => {
	class DropOnDemandPort extends DroppedSessionPort {
		override async liveness(input: Parameters<ScriptedSessionPort["liveness"]>[0]) {
			if (this.dropped.has(input.sessionId)) return { live: false, disowned: false };
			return await super.liveness(input);
		}
	}
	const port = new DropOnDemandPort({ onBind: (input) => `session-e${input.epoch}` });
	const logs: string[] = [];
	await harness(port, {}, (line) => logs.push(line), { unresolvedHoldMs: 50 });

	enqueue("m-1", "question");
	await manager!.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "send did not occur");
	const send = port.sends[0]!;

	// Make broker unavailable to cause status reads to fail
	port.dropped.add(send.sessionId);

	// Trigger hold escalation by waiting and advancing time
	await Bun.sleep(60);
	await manager!.tick(KEY);
	await eventually(
		() => logs.some((line) => line.includes("unresolved_hold_escalated")),
		"escalation did not occur",
	);

	// Verify escalation marker was persisted
	const escalatedKey = `escalated-hold:${send.opRef}`;
	const hasMarker = database!.core.metaGet(escalatedKey) !== undefined;
	expect(hasMarker).toBe(true);

	// Now restart the manager (simulating process restart)
	const oldManager = manager;
	await oldManager!.stop();
	const oldDatabase = database;
	const oldHome = home;

	// Reopen database with same storage
	database = await Store.open(join(oldHome, "gateway.db"));
	registerFixtureBindings(port);
	const newLogs: string[] = [];
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "instance-test",
		repo: join(oldHome, "workspace"),
		log: (line) => newLogs.push(line),
		unresolvedHoldMs: 50,
	});

	// Run recovery which should adopt the bound escalated turn
	await manager!.recover();
	await manager!.tick(KEY);

	// The escalation marker should still exist (not re-sent)
	expect(database!.core.metaGet(escalatedKey)).not.toBeUndefined();

	// The turn should still be tracked and not abandoned
	const turnRow = database!.inbox.turnRow(send.opRef);
	expect(turnRow).toBeDefined();
	expect(turnRow!.turn_state).not.toBe("done");

	// Logs should show recovery adopted it, not that it was lost
	const recoveryHoldLogs = newLogs.filter(
		(line) => line.includes("recovery_hold") && line.includes(send.opRef),
	);
	expect(recoveryHoldLogs.length).toBeGreaterThan(0);
});

test("P2-1: endpoint_stale errors alone do not close an escalated hold without terminal evidence", async () => {
	class OnlyStalePort extends DroppedSessionPort {
		override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
			if (this.dropped.has(input.sessionId)) throw endpointStale();
			return await super.status(input);
		}
	}
	const port = new OnlyStalePort({ onBind: (input) => `session-e${input.epoch}` });
	const logs: string[] = [];
	await harness(port, {}, (line) => logs.push(line), { unresolvedHoldMs: 50 });

	enqueue("m-1", "question");
	await manager!.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "send did not occur");
	const send = port.sends[0]!;

	// Drop session to get endpoint_stale
	port.dropped.add(send.sessionId);

	// Trigger escalation
	await Bun.sleep(60);
	await manager!.tick(KEY);
	await eventually(
		() => logs.some((line) => line.includes("unresolved_hold_escalated")),
		"escalation did not occur",
	);

	// Perform multiple ticks to read status repeatedly - should hit stale errors
	for (let i = 0; i < 10; i++) {
		await manager!.tick(KEY);
		await Bun.sleep(5);
	}

	// The turn should remain held, not closed as 'retired' on stale errors alone
	const turnRow = database!.inbox.turnRow(send.opRef);
	expect(turnRow?.turn_state).not.toBe("done");

	const closedLogs = logs.filter((line) => line.includes("retired_turn_closed"));
	expect(closedLogs.length).toBe(0);
});

test("a notice during a gateway turn steers its gateway framing without lane-report wording", async () => {
	const f = await agentObserverFixture();
	enqueue("platform-successor", "current user question");
	await manager!.notifyInbound(KEY);
	f.end();
	await eventually(() => f.noticeTimers.size === 1, "no notice timer");
	f.fireNotice();
	await eventually(() => f.port.steers.length === 1, "notice was not steered");
	expect(f.port.steers[0]!.text).toContain("[Gateway notice]");
	expect(f.port.steers[0]!.text).not.toContain("Internal lane report");
});
