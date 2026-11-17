import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CATCH_ALL_EVENT_ORIGIN, monitorSessionOrigin, originKey } from "@gajae-gateway/protocol";
import { GjcCliError } from "@gajae-gateway/subsession";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import type { SessionRequestResult } from "../src/orchestrator/session-port";
import { Store } from "../src/store";
import { BROKER_PROMPT_BUSY_REFUSAL } from "../src/store/groups/bindings";
import { MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS } from "../src/store/groups/monitors";
import { initializeTestBrokerAuthority, sessionPortFromScript } from "./session-port.fake";

const SESSION_ID = "test-session-187";
const OP_REF = "gw-m-issue-187";

let home = "";
let database: Store | undefined;
let propagator: MonitorPropagator | undefined;

afterEach(async () => {
	await propagator?.drain();
	propagator = undefined;
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

async function fixture(
	options: {
		eventType?: string;
		declaredEventTypes?: string[];
		eventCount?: number;
		persistBinding?: boolean;
		contextFailureRollThreshold?: number;
		brokerOwnership?: boolean;
		oldOwnedRepo?: boolean;
		burstPolicy?: "serialize" | "coalesce";
	} = {},
) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retry-idempotency-"));
	const store = await Store.open(join(home, "gateway.db"));
	database = store;
	const authority = options.brokerOwnership ? initializeTestBrokerAuthority(store, join(home, "agent")) : undefined;
	const registry = new MonitorRegistry(store);
	const eventType = options.eventType ?? "test.retry";
	const declaredEventTypes = options.declaredEventTypes ?? [eventType];
	const monitor = registry.add({
		name: "retry-test",
		trigger: { kind: "cron", schedule: "* * * * *" },
		eventTypes: declaredEventTypes,
		burstPolicy: options.burstPolicy ?? "serialize",
	});
	const sessionOrigin = monitorSessionOrigin(
		monitor.monitorId,
		monitor.eventTypes.includes(eventType) ? eventType : CATCH_ALL_EVENT_ORIGIN.conversationId,
	);
	const sessionOriginKey = originKey(sessionOrigin);
	const eventIds = Array.from({ length: options.eventCount ?? 1 }, () => crypto.randomUUID());
	const eventId = eventIds[0];
	if (!eventId) throw new Error("fixture requires at least one monitor event");
	const pointerKey = `monitor_authoring_turn:${eventId}`;
	const pointer = JSON.stringify({ sessionId: SESSION_ID, opRef: OP_REF, eventIds });
	let bindSessionId = SESSION_ID;
	const sessionPort = sessionPortFromScript({
		bind: async () => ({ sessionId: bindSessionId }),
		respond: async () => "[]",
	});
	store.transaction((tx) => {
		if (options.persistBinding !== false && !authority) store.bindings.bindAtEpoch(tx, sessionOriginKey, SESSION_ID, 0);
		if (authority)
			store.bindings.recordOwnedBinding(tx, {
				authority,
				sessionId: SESSION_ID,
				originKey: sessionOriginKey,
				epoch: 0,
				repo: options.oldOwnedRepo ? join(home, "old-repo") : home,
			});
		for (const currentId of eventIds) {
			store.monitors.eventCreate(tx, {
				eventId: currentId,
				monitorId: monitor.monitorId,
				eventType,
				payloadJson: "{}",
				firedAt: new Date().toISOString(),
			});
			store.monitors.eventUpdate(tx, currentId, "failed");
			for (let attempt = 0; attempt < MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS; attempt++)
				store.monitors.eventIncrementAttempts(tx, currentId);
			store.core.metaSet(`monitor_authoring_turn:${currentId}`, pointer);
		}
	});
	const pipeline = new MonitorPropagator({
		database: store,
		registry,
		sessionPort,
		memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
		delivery: new DeliveryService(store),
		emit: () => {},
		repo: home,
		...(options.contextFailureRollThreshold === undefined
			? {}
			: { contextFailureRollThreshold: options.contextFailureRollThreshold }),
	});
	propagator = pipeline;
	return {
		authority,
		eventId,
		eventIds,
		monitor,
		pipeline,
		pointer,
		pointerKey,
		sessionOriginKey,
		sessionPort,
		setBindSessionId: (sessionId: string) => {
			bindSessionId = sessionId;
		},
		store,
	};
}

function countStatusCalls(port: ReturnType<typeof sessionPortFromScript>): () => number {
	let count = 0;
	const status = port.status.bind(port);
	port.status = async (input) => {
		count++;
		return status(input);
	};
	return () => count;
}

test("issue #187: an exhausted event stays recoverable while its catch-all authoring turn is in flight", async () => {
	const state = await fixture({ eventType: "test.undeclared", declaredEventTypes: ["test.declared"] });
	state.sessionPort.seedOperation(OP_REF, SESSION_ID, "in_flight");
	const statusCalls = countStatusCalls(state.sessionPort);

	await state.pipeline.reconcile();

	expect(statusCalls()).toBe(1);
	expect(state.store.monitors.eventGet(state.eventId)?.stage).toBe("failed");
	expect(state.store.core.metaGet(state.pointerKey)).toBe(state.pointer);
	expect(state.sessionPort.sends).toHaveLength(0);
});

test("issue #187: an unknown turn status preserves the event and authoring-turn pointer", async () => {
	const state = await fixture();
	const statusCalls = countStatusCalls(state.sessionPort);

	await state.pipeline.reconcile();

	expect(statusCalls()).toBe(1);
	expect(state.store.monitors.eventGet(state.eventId)?.stage).toBe("failed");
	expect(state.store.core.metaGet(state.pointerKey)).toBe(state.pointer);
});

test("issue #187: an unreadable turn status is not treated as proof the turn stopped", async () => {
	const state = await fixture();
	state.sessionPort.status = async () => {
		throw new Error("status transport unavailable");
	};

	await state.pipeline.reconcile();

	expect(state.store.monitors.eventGet(state.eventId)?.stage).toBe("failed");
	expect(state.store.core.metaGet(state.pointerKey)).toBe(state.pointer);
});

test("issue #187: a completed exhausted turn is resumed and its authored result is retained", async () => {
	const state = await fixture({
		eventType: "test.undeclared",
		declaredEventTypes: ["test.declared"],
		eventCount: 2,
		contextFailureRollThreshold: 1,
	});
	const originalRequest = state.sessionPort.request.bind(state.sessionPort);
	state.sessionPort.request = async (input) =>
		({
			receipt: { sessionId: input.sessionId, operationRef: input.opRef } as never,
			status: {
				operationRef: input.opRef,
				status: { status: "terminal_ok" },
				summaryCompleted: true,
			},
			assistant: { text: "", pages: 1, complete: true },
		}) satisfies SessionRequestResult;
	const contextFailureId = await state.pipeline.submitAwaitable(state.monitor.monitorId, "test.undeclared", {});
	state.sessionPort.request = originalRequest;
	expect(state.pipeline.sessionSafetyState(state.sessionOriginKey).pendingRoll).toBe(
		"context_failures_native_compaction_unavailable",
	);
	state.store.transaction((tx) => state.store.monitors.eventUpdate(tx, contextFailureId, "failed_no_retry"));
	const rotatedSessionId = "test-session-187-rotated";
	const rotatedEpoch = state.store.transaction((tx) => state.store.bindings.rotateEpoch(tx, state.sessionOriginKey));
	state.store.transaction((tx) =>
		state.store.bindings.bindAtEpoch(tx, state.sessionOriginKey, rotatedSessionId, rotatedEpoch),
	);
	state.setBindSessionId(rotatedSessionId);
	const bindCountBeforeRecovery = state.sessionPort.binds.length;
	const output = JSON.stringify(
		state.eventIds.map((eventId, index) => ({ eventId, note: `completed result ${index + 1}` })),
	);
	state.sessionPort.seedAcceptedSend(
		{ sessionId: SESSION_ID, repo: home, text: "previous authoring prompt", opRef: OP_REF },
		"terminal_ok",
		output,
	);

	await state.pipeline.reconcile();

	expect(state.eventIds.map((eventId) => state.store.monitors.authoredOutput(eventId))).toEqual([
		"completed result 1",
		"completed result 2",
	]);
	expect(state.eventIds.map((eventId) => state.store.monitors.eventGet(eventId)?.stage)).toEqual([
		"authored_no_delivery",
		"authored_no_delivery",
	]);
	expect(state.sessionPort.sends.map((send) => send.opRef)).toEqual([OP_REF]);
	// A completed saved turn is harvested from its own result: no send is even attempted (#702).
	expect(state.sessionPort.sendAttempts.map((send) => send.opRef)).toEqual([OP_REF]);
	expect(state.sessionPort.binds).toHaveLength(bindCountBeforeRecovery);
	expect(state.pipeline.sessionSafetyState(state.sessionOriginKey).pendingRoll).toBe(
		"context_failures_native_compaction_unavailable",
	);
	expect(state.store.bindings.getSessionRecord(state.sessionOriginKey)).toEqual({
		sessionId: rotatedSessionId,
		epoch: rotatedEpoch,
	});
	expect(state.eventIds.map((eventId) => state.store.core.metaGet(`monitor_authoring_turn:${eventId}`))).toEqual([
		undefined,
		undefined,
	]);
});

test("an exhausted event is terminalized when its recorded authoring turn failed", async () => {
	const state = await fixture({ persistBinding: false });
	state.sessionPort.seedOperation(OP_REF, SESSION_ID, "failed");

	await state.pipeline.reconcile();

	expect(state.store.monitors.eventGet(state.eventId)?.stage).toBe("failed_no_retry");
	expect(state.store.core.metaGet(state.pointerKey)).toBeUndefined();
});

function busyMarker(state: Awaited<ReturnType<typeof fixture>>, opRef = OP_REF) {
	const authority = state.authority;
	if (!authority) throw new Error("busy marker requires owned broker fixture");
	const scope = JSON.stringify([SESSION_ID, opRef]);
	state.store.transaction((tx) => {
		state.store.bindings.beginBrokerOperation(tx, authority, {
			operation: "turn.prompt",
			scope,
			operationRef: opRef,
			requestJson: JSON.stringify({ sessionId: SESSION_ID, opRef }),
		});
		state.store.bindings.completeBrokerOperation(
			tx,
			authority,
			"turn.prompt",
			scope,
			opRef,
			JSON.stringify(BROKER_PROMPT_BUSY_REFUSAL),
		);
	});
	return () => state.store.bindings.brokerOperation(authority, "turn.prompt", scope);
}

async function dispatchedBusyTurn() {
	const state = await fixture({ brokerOwnership: true });
	state.store.transaction((tx) => {
		state.store.monitors.eventUpdate(tx, state.eventId, "failed_no_retry");
		state.store.core.metaDelete(state.pointerKey);
	});
	const request = state.sessionPort.request.bind(state.sessionPort);
	let marker: ReturnType<typeof busyMarker> | undefined;
	let busyOpRef = "";
	state.sessionPort.request = async (input) => {
		if (!marker) {
			busyOpRef = input.opRef;
			marker = busyMarker(state, input.opRef);
			throw new GjcCliError("busy", 0, "", { code: "busy" });
		}
		return request(input);
	};
	const eventId = await state.pipeline.submitAwaitable(state.monitor.monitorId, "test.retry", {});
	if (!marker) throw new Error("dispatch did not reach busy request");
	return { ...state, busyEventId: eventId, busyOpRef, marker };
}

test("monitor retry abandons its durable busy-refused pointer and releases the old marker", async () => {
	const state = await dispatchedBusyTurn();
	expect(state.marker()).toMatchObject({ resultJson: JSON.stringify(BROKER_PROMPT_BUSY_REFUSAL) });
	expect(state.store.core.metaGet(`monitor_authoring_turn:${state.busyEventId}`)).toContain(state.busyOpRef);

	await state.pipeline.reconcile();

	expect(state.marker()).toBeUndefined();
	expect(state.sessionPort.sendAttempts.some((send) => send.opRef !== state.busyOpRef)).toBe(true);
});

test("monitor in-flight reuse retains the busy marker while the authoring pointer is unresolved", async () => {
	const state = await dispatchedBusyTurn();
	state.sessionPort.seedOperation(state.busyOpRef, SESSION_ID, "in_flight");
	state.sessionPort.request = async () => {
		throw new Error("request transport unavailable");
	};

	await state.pipeline.reconcile();

	expect(state.marker()).toBeDefined();
	expect(state.store.core.metaGet(`monitor_authoring_turn:${state.busyEventId}`)).toContain(state.busyOpRef);
});

test("terminalizing one shared-batch event retains its sibling pointer and busy marker", async () => {
	const state = await fixture({ brokerOwnership: true, eventCount: 2 });
	const marker = busyMarker(state);
	state.sessionPort.seedOperation(OP_REF, SESSION_ID, "failed");
	const siblingId = state.eventIds[1]!;
	state.store.transaction((tx) => state.store.monitors.eventUpdate(tx, siblingId, "failed_no_retry"));

	await state.pipeline.reconcile();

	expect(state.store.core.metaGet(state.pointerKey)).toBeUndefined();
	expect(state.store.core.metaGet(`monitor_authoring_turn:${siblingId}`)).toBe(state.pointer);
	expect(marker()).toBeDefined();
});

test("pointer abandonment and marker release roll back together", async () => {
	const state = await fixture({ brokerOwnership: true });
	const marker = busyMarker(state);
	state.sessionPort.seedOperation(OP_REF, SESSION_ID, "failed");
	const release = state.store.bindings.abandonBrokerPromptBusyRefusal.bind(state.store.bindings);
	state.store.bindings.abandonBrokerPromptBusyRefusal = (tx, input) => {
		expect(release(tx, input)).toBe(1);
		throw new Error("injected abandonment failure");
	};

	await expect(state.pipeline.reconcile()).rejects.toThrow("injected abandonment failure");

	expect(state.store.core.metaGet(state.pointerKey)).toBe(state.pointer);
	expect(marker()).toBeDefined();
	expect(state.store.monitors.eventGet(state.eventId)?.stage).toBe("failed");
});

test("a replacement pointer written during status lookup survives the old raw CAS", async () => {
	const state = await fixture({ brokerOwnership: true });
	const marker = busyMarker(state);
	state.sessionPort.seedOperation(OP_REF, SESSION_ID, "failed");
	const status = state.sessionPort.status.bind(state.sessionPort);
	const replacement = JSON.stringify({ sessionId: SESSION_ID, opRef: "replacement", eventIds: [state.eventId] });
	state.sessionPort.status = async () => {
		state.store.transaction(() => state.store.core.metaSet(state.pointerKey, replacement));
		return status({ sessionId: SESSION_ID, repo: home, opRef: OP_REF });
	};

	await state.pipeline.reconcile();

	expect(marker()).toBeDefined();
	expect(state.store.core.metaGet(state.pointerKey)).toBe(replacement);
	expect(state.store.monitors.eventGet(state.eventId)?.stage).toBe("failed");
});

test("retry batch mismatch forgets only its own pointer, not an abandoned batch sibling", async () => {
	const state = await dispatchedBusyTurn();
	const pointerKey = `monitor_authoring_turn:${state.busyEventId}`;
	const shared = JSON.stringify({
		sessionId: SESSION_ID,
		opRef: state.busyOpRef,
		eventIds: [state.busyEventId, state.eventId],
	});
	state.store.transaction(() => {
		state.store.core.metaSet(pointerKey, shared);
		state.store.core.metaSet(state.pointerKey, shared);
	});

	await state.pipeline.reconcile();

	expect(state.store.core.metaGet(state.pointerKey)).toBe(shared);
	expect(state.marker()).toBeDefined();
});

test("successful authoring clears the captured pointer and its busy marker atomically", async () => {
	const state = await fixture({ brokerOwnership: true });
	const marker = busyMarker(state);
	state.sessionPort.seedAcceptedSend(
		{ sessionId: SESSION_ID, repo: home, text: "previous prompt", opRef: OP_REF },
		"terminal_ok",
		JSON.stringify([{ eventId: state.eventId, note: "completed recovery work" }]),
	);

	await state.pipeline.reconcile();

	expect(state.store.monitors.authoredOutput(state.eventId)).toBe("completed recovery work");
	expect(state.store.core.metaGet(state.pointerKey)).toBeUndefined();
	expect(marker()).toBeUndefined();
});

test("terminal abandonment uses the old owned epoch rather than the replacement binding", async () => {
	const state = await fixture({ brokerOwnership: true });
	const marker = busyMarker(state);
	const authority = state.authority!;
	state.store.transaction((tx) => {
		const epoch = state.store.bindings.rotateEpoch(tx, state.sessionOriginKey);
		state.store.bindings.recordOwnedBinding(tx, {
			authority,
			sessionId: "replacement-session",
			originKey: state.sessionOriginKey,
			epoch,
			repo: home,
		});
	});
	state.sessionPort.seedOperation(OP_REF, SESSION_ID, "failed");

	await state.pipeline.reconcile();

	expect(state.store.core.metaGet(state.pointerKey)).toBeUndefined();
	expect(marker()).toBeUndefined();
	expect(state.store.bindings.getSessionRecord(state.sessionOriginKey)).toEqual({
		sessionId: "replacement-session",
		epoch: 1,
	});
});

test("terminal abandonment uses the old owned repo rather than the current monitor repo", async () => {
	const state = await fixture({ brokerOwnership: true, oldOwnedRepo: true });
	const marker = busyMarker(state);
	const authority = state.authority!;
	state.store.transaction((tx) => {
		const epoch = state.store.bindings.rotateEpoch(tx, state.sessionOriginKey);
		state.store.bindings.recordOwnedBinding(tx, {
			authority,
			sessionId: "replacement-session",
			originKey: state.sessionOriginKey,
			epoch,
			repo: home,
		});
	});
	state.sessionPort.seedOperation(OP_REF, SESSION_ID, "failed");

	await state.pipeline.reconcile();

	expect(state.store.bindings.assertOwnedSession(SESSION_ID, join(home, "old-repo"), authority).repo).toBe(
		join(home, "old-repo"),
	);
	expect(state.store.core.metaGet(state.pointerKey)).toBeUndefined();
	expect(marker()).toBeUndefined();
	expect(state.store.bindings.getSessionRecord(state.sessionOriginKey)).toEqual({
		sessionId: "replacement-session",
		epoch: 1,
	});
});

for (const retainSibling of [false, true]) {
	test(`pending batch overwrite releases old busy proof only after every owner is gone (retain sibling=${retainSibling})`, async () => {
		const state = await fixture({ brokerOwnership: true, burstPolicy: "coalesce" });
		state.store.transaction((tx) => {
			state.store.monitors.eventUpdate(tx, state.eventId, "failed_no_retry");
			state.store.core.metaDelete(state.pointerKey);
		});
		const marker = busyMarker(state);
		const release = state.store.bindings.abandonBrokerPromptBusyRefusal.bind(state.store.bindings);
		const releases: number[] = [];
		state.store.bindings.abandonBrokerPromptBusyRefusal = (tx, input) => {
			const changed = release(tx, input);
			releases.push(changed);
			return changed;
		};
		let requested!: () => void;
		const requestReached = new Promise<void>((resolve) => {
			requested = resolve;
		});
		let markerAtRequest = false;
		state.sessionPort.request = async () => {
			markerAtRequest = marker() !== undefined;
			requested();
			throw new Error("hold the new pointer for inspection");
		};
		const submitted = [1, 2, 3].map((index) => state.pipeline.submit(state.monitor.monitorId, "test.retry", { index }));
		// Dispatch claims database order (newest first), not burst admission order.
		const claimedIds = state.store.monitors.eventsByIds(submitted).map((row) => row.event_id);
		expect(claimedIds).toHaveLength(3);
		const first = claimedIds[0]!;
		const second = claimedIds[1]!;
		const third = claimedIds[2]!;
		const prior = JSON.stringify({
			sessionId: SESSION_ID,
			opRef: OP_REF,
			eventIds: retainSibling ? [second, third, state.eventId] : [second, third],
		});
		state.store.transaction(() => {
			state.store.core.metaSet(`monitor_authoring_turn:${second}`, prior);
			state.store.core.metaSet(`monitor_authoring_turn:${third}`, prior);
			if (retainSibling) state.store.core.metaSet(state.pointerKey, prior);
		});

		await requestReached;
		await state.pipeline.drain();

		expect(releases).toEqual(retainSibling ? [0, 0] : [0, 1]);
		expect(markerAtRequest).toBe(retainSibling);
		const pending = state.store.core.metaGet(`monitor_authoring_turn:${first}`);
		expect(pending).toBeDefined();
		expect(state.store.core.metaGet(`monitor_authoring_turn:${second}`)).toBe(pending);
		expect(state.store.core.metaGet(`monitor_authoring_turn:${third}`)).toBe(pending);
		if (retainSibling) expect(state.store.core.metaGet(state.pointerKey)).toBe(prior);
	});
}

test("pending pointer rewrite preserves busy proof for the same session and operation", async () => {
	const state = await fixture({ brokerOwnership: true, burstPolicy: "coalesce" });
	state.store.transaction((tx) => {
		state.store.monitors.eventUpdate(tx, state.eventId, "failed_no_retry");
		state.store.core.metaDelete(state.pointerKey);
	});
	const marker = busyMarker(state);
	state.sessionPort.seedOperation(OP_REF, SESSION_ID, "in_flight");
	let requested!: () => void;
	const requestReached = new Promise<void>((resolve) => {
		requested = resolve;
	});
	let markerAtRequest = false;
	let requestedOpRef = "";
	state.sessionPort.request = async (input) => {
		markerAtRequest = marker() !== undefined;
		requestedOpRef = input.opRef;
		requested();
		throw new Error("hold the reused operation for inspection");
	};
	const submitted = [1, 2].map((index) => state.pipeline.submit(state.monitor.monitorId, "test.retry", { index }));
	const claimedIds = state.store.monitors.eventsByIds(submitted).map((row) => row.event_id);
	expect(claimedIds).toHaveLength(2);
	const first = claimedIds[0]!;
	const second = claimedIds[1]!;
	const prior = JSON.stringify({ sessionId: SESSION_ID, opRef: OP_REF, eventIds: [first, second] });
	const differentEncoding = JSON.stringify({ eventIds: [second], opRef: OP_REF, sessionId: SESSION_ID });
	state.store.transaction(() => {
		state.store.core.metaSet(`monitor_authoring_turn:${first}`, prior);
		state.store.core.metaSet(`monitor_authoring_turn:${second}`, differentEncoding);
	});

	await requestReached;
	await state.pipeline.drain();

	expect(requestedOpRef).toBe(OP_REF);
	expect(markerAtRequest).toBe(true);
	expect(marker()).toBeDefined();
	expect(state.store.core.metaGet(`monitor_authoring_turn:${second}`)).toBe(prior);
});
