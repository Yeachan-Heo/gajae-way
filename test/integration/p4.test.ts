import { AsyncLocalStorage } from "node:async_hooks";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";

import { BrokerCli } from "../../src/broker/cli";
import { createMainAdmissionHandler, reconcilePendingMainAdmissions } from "../../src/main-session/admission";
import { createMainSessionHost } from "../../src/main-session/host";
import { strictResumeMainSession } from "../../src/main-session/resume";
import { createExternalHostSupervisor } from "../../src/main-session/supervisor";

import { canonicalJson } from "../../src/main-session/gates";
import { connectEventually, createExternalGateway, eventually, type ExternalGateway } from "../helpers/external-gateway";
import { ManagedProcessRegistry } from "../helpers/managed-process";
import { FakeBrokerFixture } from "../helpers/main-session";


const gatewayScope = new AsyncLocalStorage<ExternalGateway[]>();
const managedProcesses = new ManagedProcessRegistry();

afterEach(async () => {
	await managedProcesses.reapAll();
});

type ExternalTestBody = () => void | Promise<void>;
const externalTestLockPath = path.join(os.tmpdir(), `gajaeway-p4-external-test-${process.pid}.lock`);

async function acquireExternalTestLock(): Promise<() => void> {
	const deadline = Date.now() + 55_000;
	for (;;) {
		try {
			const descriptor = fs.openSync(externalTestLockPath, "wx");
			return () => {
				fs.closeSync(descriptor);
				fs.rmSync(externalTestLockPath, { force: true });
			};
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
			if (Date.now() >= deadline) throw new Error("Timed out waiting for the external gateway test lock.");
			await Bun.sleep(10);
		}
	}
}



function externalTest(name: string, body: ExternalTestBody, timeoutMs?: number): void {
	test(name, async () => {
		let releaseLock: (() => void) | undefined;
		const gateways: ExternalGateway[] = [];
		try {
			releaseLock = await acquireExternalTestLock();
			await gatewayScope.run(gateways, body);
		} finally {
			for (const gateway of gateways.splice(0)) await gateway.stop();
			releaseLock?.();
		}
	}, Math.max(timeoutMs ?? 0, 60_000));
}

async function hosted(options: Parameters<typeof createExternalGateway>[0] = {}): Promise<ExternalGateway> {
	const gateway = await createExternalGateway({
		knownSurfaces: [{ id: "guest", platform: "test", kind: "channel" }],
		...options,
	});
	const gateways = gatewayScope.getStore();
	if (!gateways) throw new Error("hosted() must run inside externalTest().");
	gateways.push(gateway);
	return gateway;
}

function rpcError(response: Awaited<ReturnType<ExternalGateway["client"]["request"]>>) {
	if (!response.error) throw new Error(`Expected an RPC error, got ${JSON.stringify(response)}`);
	return response.error;
}

async function waitForBusyStatus(gateway: ExternalGateway): Promise<Awaited<ReturnType<ExternalGateway["client"]["request"]>>> {
	const deadline = Date.now() + 2_000;
	for (;;) {
		const response = await gateway.client.request("way.status", {});
		if ((response.result as { turn_state?: unknown } | undefined)?.turn_state === "busy") return response;
		if (Date.now() >= deadline) throw new Error("main.submit did not project busy turn state within 2000 ms.");
		await Bun.sleep(10);
	}
}


async function settleHeld(gateway: ExternalGateway, opRef: string, text: string): Promise<void> {
	await eventually(
		() => (gateway.core.journalRead("1:0", 100).events.some(event => event.kind === "turn_start") ? true : undefined),
		`held operation ${opRef} did not publish its lifecycle start before teardown`,
		15_000,
	);
	const terminalEventsBefore = gateway.core.journalRead("1:0", 100).events.filter(event => event.kind === "turn_end").length;
	gateway.fixture.complete(opRef, { text });
	await eventually(
		() => (gateway.core.journalRead("1:0", 100).events.filter(event => event.kind === "turn_end").length > terminalEventsBefore ? true : undefined),
		`held operation ${opRef} did not settle before fixture teardown`,
		15_000,
	);
}


externalTest("main.submit admits a held external turn before the bridge timeout", async () => {
	const gateway = await hosted();
	gateway.fixture.holdNextTurn();

	const response = await gateway.client.request(
		"main.submit",
		{ text: "held owner prompt", surface_id: "owner", idempotency_key: "held-owner" },
		{ timeoutMs: 1_000 },
	);

	const opRef = (response.result as { op_ref: string }).op_ref;
	expect(response.error).toBeUndefined();
	expect(response.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
	expect(opRef).toEqual(expect.any(String));
	expect(gateway.fixture.commands()).toEqual([expect.objectContaining({ operation: "turn.prompt", text: "held owner prompt" })]);
	expect(gateway.core.rpcBridgeStats().timeouts).toBe(0);
	expect(gateway.core.journalRead("1:0", 100).events.filter(event => event.kind === "assistant_message")).toEqual([]);
	const busyStatus = await waitForBusyStatus(gateway);
	expect(busyStatus.result).toMatchObject({ turn_state: "busy" });
	await settleHeld(gateway, opRef, "settled after bridge timing assertion");

});

externalTest("pending busy adoption binds its first complete tail as an unprojected ring boundary before admission", async () => {
	const fixture = new FakeBrokerFixture();
	let gatewayOwned = false;
	try {
		fixture.holdNextTurn();
		const operatorBroker = new BrokerCli({ executable: fixture.executable, environment: fixture.environment() });
		await operatorBroker.sendPrompt(fixture.sessionId, "operator turn already in flight", "operator-busy");
		fixture.setNoEnvelopeWhileBusy();

		const gateway = await hosted({ fixture, tailTimeoutMs: 50, adoptionTailTimeoutMs: 50 });
		gatewayOwned = true;
		expect(gateway.state.read()).toMatchObject({ transcriptProof: "pending", tailCheckpoint: undefined });

		const pendingStatus = await gateway.client.request("way.status", {});
		expect(pendingStatus.result).toMatchObject({ transcript_proof: "pending" });
		const refused = await gateway.client.request("main.submit", {
			text: "must remain fenced",
			surface_id: "owner",
			idempotency_key: "proof-pending-fence",
		});
		expect(rpcError(refused)).toEqual(expect.objectContaining({ code: 1003, message: "transcript_proof_pending" }));
		expect(gateway.fixture.commands().filter(command => command.text === "must remain fenced")).toEqual([]);
		expect((await gateway.client.request("main.events.read", { cursor: "1:0" })).error).toBeUndefined();

		gateway.fixture.complete("operator-busy", { text: "operator boundary reached" });
		await eventually(
			() => (gateway.state.read().transcriptProof === "proven" ? true : undefined),
			"the first complete tail did not bind the pending transcript proof",
		);
		expect(gateway.state.read().tailCheckpoint).toEqual({ revision: 4, generation: 1, seq: 5 });
		const boundaryEvents = gateway.core.journalRead("1:0", 100).events;
		expect(boundaryEvents.filter(event => event.kind === "tail_adoption_start")).toHaveLength(1);
		expect(JSON.parse(boundaryEvents.find(event => event.kind === "tail_adoption_start")?.payloadJson ?? "{}")).toEqual({
			checkpoint: { revision: 4, generation: 1, seq: 5 },
		});
		expect(boundaryEvents.filter(event => event.kind === "turn_start" || event.kind === "turn_end" || event.kind === "assistant_message")).toEqual([]);
		expect((await gateway.client.request("way.status", {})).result).toMatchObject({ transcript_proof: "proven", turn_state: "idle" });

		gateway.fixture.holdNextTurn();
		const accepted = await gateway.client.request("main.submit", {
			text: "admission opens after proof",
			surface_id: "owner",
			idempotency_key: "proof-promoted-admission",
		});
		expect(accepted.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
		const opRef = (accepted.result as { op_ref: string }).op_ref;
		await eventually(
			() => gateway.core.journalRead("1:0", 100).events.some(event => event.kind === "turn_start"),
			"the envelope after the adoption boundary did not project lifecycle evidence",
		);
		gateway.fixture.complete(opRef, { text: "proof-bound roundtrip" });
		await eventually(
			() =>
				gateway.core
					.journalRead("1:0", 100)
					.events.some(event => event.kind === "assistant_message" && event.payloadJson.includes("proof-bound roundtrip"))
					? true
					: undefined,
			"proof-bound admission did not round-trip through the external session",
		);
	} finally {
		if (!gatewayOwned) fixture.dispose();
	}
}, 30_000);

externalTest("successive attempt generations project once and settle as ordered tail coordinates", async () => {
	const gateway = await hosted({ tailTimeoutMs: 50 });
	const rounds = [
		{ text: "first generation", idempotencyKey: "generation-one", reply: "first generation reply", generation: 1 },
		{ text: "second generation", idempotencyKey: "generation-two", reply: "second generation reply", generation: 2 },
	] as const;

	for (const round of rounds) {
		gateway.fixture.holdNextTurn();
		const accepted = await gateway.client.request("main.submit", {
			text: round.text,
			surface_id: "owner",
			idempotency_key: round.idempotencyKey,
		});
		expect(accepted.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
		const opRef = (accepted.result as { op_ref: string }).op_ref;
		await waitForBusyStatus(gateway);
		gateway.fixture.complete(opRef, { text: round.reply });
		await eventually(
			() =>
				gateway.core.journalRead("1:0", 100).events.some(event => event.kind === "assistant_message" && event.payloadJson.includes(round.reply))
					? true
					: undefined,
			`generation ${round.generation} transcript reply did not project`,
		);
		await eventually(() => (gateway.host.turnState === "idle" ? true : undefined), `generation ${round.generation} did not settle`);
		expect(gateway.state.read().tailCheckpoint).toMatchObject({ generation: round.generation, seq: 5 });
	}

	const lifecycle = gateway.core.journalRead("1:0", 100).events.filter(event => event.kind === "turn_start" || event.kind === "turn_end");
	const starts = lifecycle.filter(event => event.kind === "turn_start").map(event => JSON.parse(event.payloadJson));
	const ends = lifecycle.filter(event => event.kind === "turn_end").map(event => JSON.parse(event.payloadJson));
	expect(starts.map(payload => payload.generation)).toEqual([1, 2]);
	expect(ends.map(payload => payload.generation)).toEqual([1, 2]);
	for (const round of rounds) {
		expect(
			gateway.core.journalRead("1:0", 100).events.filter(event => event.kind === "assistant_message" && event.payloadJson.includes(round.reply)),
		).toHaveLength(1);
	}
	expect(gateway.state.read()).toMatchObject({
		bootstrapState: "COMMITTED",
		failedClosedReason: undefined,
		tailCheckpoint: { generation: 2, seq: 5 },
		tailRingRotationCount: 0,
	});
	expect(gateway.core.journalRead("1:0", 100).events.filter(event => event.kind === "tail_ring_rotation")).toHaveLength(0);
	expect(gateway.host.degraded).toBe(false);
}, 30_000);

externalTest("busy ring rotation across an attempt-generation boundary resyncs without losing transcript delivery", async () => {
	const gateway = await hosted({ tailTimeoutMs: 50 });
	gateway.fixture.setNoEnvelopeWhileBusy();
	gateway.fixture.holdNextTurn();
	const primer = await gateway.client.request("main.submit", {
		text: "advance the first attempt generation",
		surface_id: "owner",
		idempotency_key: "rotation-generation-primer",
	});
	const primerOpRef = (primer.result as { op_ref: string }).op_ref;
	expect(primer.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
	await waitForBusyStatus(gateway);
	gateway.fixture.complete(primerOpRef, { text: "first-generation terminal reply" });
	await eventually(
		() =>
			gateway.state.read().tailCheckpoint?.generation === 1 && gateway.state.read().tailCheckpoint?.seq === 5
				? true
				: undefined,
		"first attempt did not establish its generation-one tail watermark",
	);
	await eventually(() => (gateway.host.turnState === "idle" ? true : undefined), "first attempt did not settle before the generation-boundary rotation");

	gateway.fixture.holdNextTurn();
	const first = await gateway.client.request("main.submit", {
		text: "rotate this busy ring across generation boundary",
		surface_id: "owner",
		idempotency_key: "busy-ring-rotation",
	});
	const firstOpRef = (first.result as { op_ref: string }).op_ref;
	expect(first.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
	await waitForBusyStatus(gateway);
	gateway.fixture.rotateRingDuringNextCompletion();
	gateway.fixture.complete(firstOpRef, { text: "final reply after busy ring rotation" });

	await eventually(
		() => (gateway.state.read().tailRingRotationCount === 1 ? true : undefined),
		"busy-turn ring rotation was not durably recorded",
	);
	const rotation = await eventually(
		() => gateway.core.journalRead("1:0", 100).events.find(event => event.kind === "tail_ring_rotation"),
		"busy-turn ring rotation was not journaled",
	);
	expect(JSON.parse(rotation.payloadJson)).toEqual({
		prior_watermark: { revision: 4, generation: 1, seq: 5 },
		resync_point: { revision: 6, generation: 2, seq: 5 },
	});
	const filteredRotation = await gateway.client.request("main.events.read", {
		cursor: "1:0",
		kinds: ["tail_ring_rotation"],
	});
	expect((filteredRotation.result as { events: Array<{ kind: string; payload: unknown }> }).events).toEqual([
		expect.objectContaining({ kind: "tail_ring_rotation", payload: JSON.parse(rotation.payloadJson) }),
	]);
	await eventually(
		() =>
			gateway.core
				.journalRead("1:0", 100)
				.events.some(event => event.kind === "assistant_message" && event.payloadJson.includes("final reply after busy ring rotation"))
				? true
				: undefined,
		"transcript delivery did not project the terminal reply after ring rotation",
	);
	const settled = await gateway.client.request("way.status", {});
	expect(settled.result).toMatchObject({ turn_state: "idle", tail_ring_rotation_count: 1 });
	expect(gateway.state.read().bootstrapState).toBe("COMMITTED");
	expect(gateway.state.read().failedClosedReason).toBeUndefined();
	expect(gateway.host.degraded).toBe(false);

	gateway.fixture.holdNextTurn();
	const second = await gateway.client.request("main.submit", {
		text: "admission still works after cross-generation rotation",
		surface_id: "owner",
		idempotency_key: "post-ring-rotation",
	});
	expect(second.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
	const secondOpRef = (second.result as { op_ref: string }).op_ref;
	gateway.fixture.complete(secondOpRef, { text: "post-rotation roundtrip" });
	await eventually(
		() =>
			gateway.core
				.journalRead("1:0", 100)
				.events.some(event => event.kind === "assistant_message" && event.payloadJson.includes("post-rotation roundtrip"))
				? true
				: undefined,
		"post-rotation admission did not round-trip",
	);
}, 30_000);

externalTest("restart replays the exact verification tail after a down-time ring rotation without rebaselining transcript delivery", async () => {
	const gateway = await hosted({ tailTimeoutMs: 50 });
	gateway.fixture.setNoEnvelopeWhileBusy();
	gateway.fixture.holdNextTurn();
	const submitted = await gateway.client.request("main.submit", {
		text: "complete while daemon is absent and rotate the ring",
		surface_id: "owner",
		idempotency_key: "restart-rotation-delivery",
	});
	const opRef = (submitted.result as { op_ref: string }).op_ref;
	expect(submitted.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
	await waitForBusyStatus(gateway);
	await gateway.host.dispose();
	gateway.fixture.rotateRingDuringNextCompletion();
	gateway.fixture.complete(opRef, { text: "reply recovered from verification-tail delivery" });

	const restartedSupervisor = createExternalHostSupervisor({
		broker: new BrokerCli({ executable: gateway.fixture.executable, environment: gateway.fixture.environment() }),
		workspace: gateway.fixture.workspace,
		tailTimeoutMs: 50,
		commandTimeoutMs: 1_000,
	});
	let restartedHost: ReturnType<typeof createMainSessionHost> | undefined;
	try {
		const resumed = await strictResumeMainSession({ profile: gateway.profile, state: gateway.state, supervisor: restartedSupervisor });
		expect(resumed.verificationTail).toMatchObject({ retentionGap: true, resyncCheckpoint: { generation: 1, seq: 5 } });
		restartedHost = createMainSessionHost({
			supervisor: restartedSupervisor,
			identity: resumed.identity,
			state: gateway.state,
			journal: {
				journalAppend: (kind, payloadJson) => gateway.core.journalAppend(kind, payloadJson),
				journalAppendAtTailCheckpoint: (kind, payloadJson, expected, checkpoint) =>
					gateway.state.appendTailProjection(expected, checkpoint, kind, payloadJson),
				journalAppendTranscriptProjection: (kind, payloadJson, expectedTail, checkpoint, expectedDelivery, nextDelivery) =>
					gateway.state.appendTranscriptProjection(expectedTail, checkpoint, expectedDelivery, nextDelivery, kind, payloadJson),
				setRpcHealth: (state, reason) => gateway.core.setRpcHealth(state, reason),
				setMainSessionStatus: (turnState, followUpQueueDepth, verificationState) =>
					gateway.core.setMainSessionStatus(turnState, followUpQueueDepth, verificationState),
				setJournalDegraded: degraded => gateway.core.setJournalDegraded(degraded),
			},
			initialTurnState: resumed.turnState,
			initialFollowUpQueueDepth: resumed.followUpQueueDepth,
			initialVerificationState: resumed.verificationState,
			...(resumed.verificationTail === undefined ? {} : { verificationTail: resumed.verificationTail }),
			...(resumed.growthIntent === undefined ? {} : { recoveredGrowthIntent: resumed.growthIntent }),
		});
		await eventually(
			() =>
				gateway.core
					.journalRead("1:0", 100)
					.events.find(event => event.kind === "assistant_message" && event.payloadJson.includes("reply recovered from verification-tail delivery")),
			"restart did not project the transcript suffix from its verification tail",
		);
		await eventually(
			() => (gateway.state.read().tailRingRotationCount === 1 ? true : undefined),
			"verification-tail retention rotation was not counted",
		);
		const journal = gateway.core.journalRead("1:0", 100).events;
		const rotations = journal.filter(event => event.kind === "tail_ring_rotation");
		expect(rotations).toHaveLength(1);
		expect(JSON.parse(rotations[0]?.payloadJson ?? "{}")).toEqual({
			prior_watermark: { revision: 2, generation: 1, seq: 0 },
			resync_point: { revision: 4, generation: 1, seq: 5 },
		});
		expect(journal.some(event => event.kind === "transcript_delivery_gap")).toBe(false);
		const filtered = await gateway.client.request("main.events.read", {
			cursor: "1:0",
			kinds: ["tail_ring_rotation", "assistant_message", "transcript_delivery_gap"],
		});
		expect((filtered.result as { events: Array<{ kind: string }> }).events.map(event => event.kind)).toEqual([
			"tail_ring_rotation",
			"assistant_message",
		]);
	} finally {
		await restartedHost?.dispose();
		await restartedSupervisor.dispose();
	}
}, 30_000);

externalTest("a transcript prefix break remains a fail-closed authority violation", async () => {
	const gateway = await hosted();
	gateway.fixture.holdNextTurn();
	const accepted = await gateway.client.request("main.submit", {
		text: "force a transcript prefix check",
		surface_id: "owner",
		idempotency_key: "transcript-prefix-break",
	});
	const opRef = (accepted.result as { op_ref: string }).op_ref;
	await waitForBusyStatus(gateway);
	gateway.fixture.replaceTranscriptEntry(`${gateway.fixture.sessionId}:transcript:0`, {
		type: "session",
		id: "tampered-external-session",
	});
	gateway.fixture.complete(opRef, { text: "must not be accepted as append-only growth" });
	await eventually(
		() => (gateway.state.read().bootstrapState === "FAILED_CLOSED" ? true : undefined),
		"transcript prefix break did not fail closed",
	);
	expect(gateway.state.read()).toMatchObject({ bootstrapState: "FAILED_CLOSED", failedClosedReason: "growth_intent_mismatch", transcriptDeliveryGapCount: 1 });
	const status = await gateway.client.request("way.status", {});
	expect(status.result).toMatchObject({
		state: "failed_closed",
		reason: "growth_intent_mismatch",
		transcript_delivery_gap_detected: true,
		transcript_delivery_gap_count: 1,
	});
	const gaps = await gateway.client.request("main.events.read", {
		cursor: "1:0",
		kinds: ["transcript_delivery_gap"],
	});
	expect((gaps.result as { events: Array<{ kind: string; payload: unknown }> }).events).toEqual([
		expect.objectContaining({ kind: "transcript_delivery_gap", payload: expect.objectContaining({ reason: "transcript_delivery_unprovable" }) }),
	]);
}, 30_000);

externalTest("main.submit returns delivered_as before a held external assistant is journaled", async () => {
	const gateway = await hosted();
	gateway.fixture.holdNextTurn();

	const response = await gateway.client.request("main.submit", {
		text: "response before assistant",
		surface_id: "owner",
		idempotency_key: "response-before-assistant",
	});
	const opRef = (response.result as { op_ref: string }).op_ref;
	expect(response.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
	expect(gateway.core.journalRead("1:0", 100).events.filter(event => event.kind === "assistant_message")).toEqual([]);

	await eventually(
		() => (gateway.core.journalRead("1:0", 100).events.some(event => event.kind === "turn_start") ? true : undefined),
		"held external turn did not publish its lifecycle start before completion",
		15_000,
	);
	gateway.fixture.complete(opRef, { text: "journaled after admission" });
	const assistant = await eventually(
		() =>
			gateway.core
				.journalRead("1:0", 100)
				.events.find(event => event.kind === "assistant_message" && JSON.parse(event.payloadJson).text === "journaled after admission"),
		"held assistant output was not journaled",
		15_000,
	);
	expect(JSON.parse(assistant.payloadJson)).toMatchObject({ finalized: true, text: "journaled after admission" });
}, 20_000);

externalTest("main.submit replays a pending admission identically without double-sending", async () => {
	const gateway = await hosted();
	gateway.fixture.holdNextTurn();
	const request = { text: "pending once", surface_id: "owner", idempotency_key: "pending-once" };

	const first = await gateway.client.request("main.submit", request, { timeoutMs: 1_000 });
	const replay = await gateway.client.request("main.submit", request, { timeoutMs: 1_000 });

	expect(first.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
	expect(replay.result).toEqual(first.result);
	expect(gateway.fixture.commands().filter(command => command.operation === "turn.prompt" && command.text === "pending once")).toHaveLength(1);
	await settleHeld(gateway, (first.result as { op_ref: string }).op_ref, "settled after idempotent replay assertion");

});

externalTest("main.submit preserves an ambiguous post-acceptance claim for reconciliation without a second send", async () => {
	let interrupted = false;
	const gateway = await hosted({
		afterBrokerAcceptedBeforeFinalize: () => {
			interrupted = true;
			throw new Error("simulated kill after broker acceptance");
		},
	});
	gateway.fixture.holdNextTurn();
	const request = { text: "claim before send", surface_id: "owner", idempotency_key: "pre-effect-recovery" };
	const interruptedResponse = await gateway.client.request("main.submit", request);
	expect(interrupted).toBe(true);
	expect(interruptedResponse.error).toBeDefined();
	expect(gateway.fixture.commands().filter(command => command.operation === "turn.prompt" && command.text === request.text)).toHaveLength(1);
	const [pending] = gateway.core.mainAdmissionOperationsPending();
	expect(pending).toMatchObject({ scope: "main.submit", key: request.idempotency_key });
	const intent = JSON.parse(pending?.intentJson ?? "{}") as { op_ref?: unknown; delivered_as?: unknown };
	if (
		typeof intent.op_ref !== "string" ||
		(intent.delivered_as !== "prompt" && intent.delivered_as !== "steer" && intent.delivered_as !== "follow_up")
	) {
		throw new Error("pending main admission intent was malformed");
	}

	await gateway.host.dispose();
	const restartedSupervisor = createExternalHostSupervisor({
		broker: new BrokerCli({ executable: gateway.fixture.executable, environment: gateway.fixture.environment() }),
		workspace: gateway.fixture.workspace,
		tailTimeoutMs: 500,
		commandTimeoutMs: 1_000,
	});
	try {
		const resumed = await strictResumeMainSession({ profile: gateway.profile, state: gateway.state, supervisor: restartedSupervisor });
		expect(resumed.recoveredGrowthIntent).toBe(true);
		await reconcilePendingMainAdmissions(gateway.core, restartedSupervisor);
		const replay = await createMainAdmissionHandler(
			{
				turnState: "idle",
				async admit() {
					throw new Error("replay must not invoke the broker target");
				},
			},
			gateway.profile,
			gateway.core,
		)(request);
		expect(replay).toEqual({ accepted: true, op_ref: intent.op_ref, delivered_as: intent.delivered_as });
		expect(gateway.core.mainAdmissionOperationsPending()).toEqual([]);
		expect(gateway.fixture.commands().filter(command => command.operation === "turn.prompt" && command.text === request.text)).toHaveLength(1);
	} finally {
		await restartedSupervisor.dispose();
	}
});

externalTest("an accepted operation with a lost receipt preserves growth authorization across restart without resend", async () => {
	const gateway = await hosted({ tailTimeoutMs: 50 });
	gateway.fixture.setNoEnvelopeWhileBusy();
	gateway.fixture.holdNextTurn();
	gateway.fixture.suppressNextAdmissionReceipt();
	const request = { text: "accepted before receipt loss", surface_id: "owner", idempotency_key: "lost-receipt-growth" };
	const interrupted = await gateway.client.request("main.submit", request);
	expect(rpcError(interrupted)).toMatchObject({ code: -32603, message: "bridge_exception", data: { reason: "turn_admission_failed" } });
	expect(gateway.fixture.commands().filter(command => command.operation === "turn.prompt" && command.text === request.text)).toHaveLength(1);
	const [pending] = gateway.core.mainAdmissionOperationsPending();
	const intent = JSON.parse(pending?.intentJson ?? "{}") as { op_ref?: unknown };
	if (typeof intent.op_ref !== "string") throw new Error("lost receipt admission intent was malformed");
	expect(gateway.state.read().growthIntent).toBeDefined();
	expect(gateway.host.admissionFenceReason).toBe("admission_recovery_pending");
	const fenced = await gateway.client.request("main.submit", {
		text: "must remain fenced while receipt acceptance is ambiguous",
		surface_id: "owner",
		idempotency_key: "lost-receipt-fenced",
	});
	expect(rpcError(fenced)).toMatchObject({ code: 1003, message: "admission_recovery_pending" });

	await gateway.host.dispose();
	gateway.fixture.complete(intent.op_ref, { text: "recovered after accepted receipt loss" });
	const restartedSupervisor = createExternalHostSupervisor({
		broker: new BrokerCli({ executable: gateway.fixture.executable, environment: gateway.fixture.environment() }),
		workspace: gateway.fixture.workspace,
		tailTimeoutMs: 50,
		commandTimeoutMs: 1_000,
	});
	let restartedHost: ReturnType<typeof createMainSessionHost> | undefined;
	try {
		const resumed = await strictResumeMainSession({ profile: gateway.profile, state: gateway.state, supervisor: restartedSupervisor });
		expect(resumed.recoveredGrowthIntent).toBe(true);
		await reconcilePendingMainAdmissions(gateway.core, restartedSupervisor);
		restartedHost = createMainSessionHost({
			supervisor: restartedSupervisor,
			identity: resumed.identity,
			state: gateway.state,
			journal: {
				journalAppend: (kind, payloadJson) => gateway.core.journalAppend(kind, payloadJson),
				journalAppendAtTailCheckpoint: (kind, payloadJson, expected, checkpoint) =>
					gateway.state.appendTailProjection(expected, checkpoint, kind, payloadJson),
				journalAppendTranscriptProjection: (kind, payloadJson, expectedTail, checkpoint, expectedDelivery, nextDelivery) =>
					gateway.state.appendTranscriptProjection(expectedTail, checkpoint, expectedDelivery, nextDelivery, kind, payloadJson),
				setRpcHealth: (state, reason) => gateway.core.setRpcHealth(state, reason),
				setMainSessionStatus: (turnState, followUpQueueDepth, verificationState) =>
					gateway.core.setMainSessionStatus(turnState, followUpQueueDepth, verificationState),
				setJournalDegraded: degraded => gateway.core.setJournalDegraded(degraded),
			},
			initialTurnState: resumed.turnState,
			initialFollowUpQueueDepth: resumed.followUpQueueDepth,
			initialVerificationState: resumed.verificationState,
			...(resumed.verificationTail === undefined ? {} : { verificationTail: resumed.verificationTail }),
			...(resumed.growthIntent === undefined ? {} : { recoveredGrowthIntent: resumed.growthIntent }),
		});
		await eventually(
			() =>
				gateway.core
					.journalRead("1:0", 100)
					.events.some(event => event.kind === "assistant_message" && event.payloadJson.includes("recovered after accepted receipt loss"))
					? true
					: undefined,
			"lost-receipt recovery did not project the accepted transcript suffix",
		);
		await eventually(() => (gateway.state.read().growthIntent === undefined ? true : undefined), "lost-receipt recovery did not settle durable growth authorization");
		expect(gateway.state.read()).toMatchObject({ bootstrapState: "COMMITTED", failedClosedReason: undefined });
		expect(gateway.core.mainAdmissionOperationsPending()).toEqual([]);
		expect(gateway.fixture.commands().filter(command => command.operation === "turn.prompt" && command.text === request.text)).toHaveLength(1);
	} finally {
		await restartedHost?.dispose();
		await restartedSupervisor.dispose();
	}
}, 30_000);

externalTest("terminal tail evidence finalizes fast same-process lost-receipt claims before their send failures and never accumulates rows", async () => {
	const gateway = await hosted({ tailTimeoutMs: 50, commandTimeoutMs: 3_000 });
	const settleLostReceipt = async (ordinal: number): Promise<void> => {
		const request = {
			text: `fast accepted without receipt ${ordinal}`,
			surface_id: "owner",
			idempotency_key: `fast-same-process-lost-receipt-${ordinal}`,
		};
		gateway.fixture.suppressNextAdmissionReceiptAfterTerminalTail();
		const interrupted = await gateway.client.request("main.submit", request, { timeoutMs: 5_000 });
		expect(rpcError(interrupted)).toMatchObject({ code: -32603, message: "bridge_exception", data: { reason: "turn_admission_failed" } });
		await eventually(
			() => (gateway.core.mainAdmissionOperationsPending().length === 0 ? true : undefined),
			"terminal evidence did not finalize the fast same-process admission claim",
		);
		await eventually(
			() => (gateway.host.admissionFenceReason === undefined && gateway.state.read().growthIntent === undefined ? true : undefined),
			"terminal evidence did not settle the fast receipt-loss admission",
		);
		const commands = gateway.fixture.commands().filter(command => command.operation === "turn.prompt" && command.text === request.text);
		expect(commands).toHaveLength(1);
		if (typeof commands[0]?.opRef !== "string") throw new Error("fast same-process lost-receipt admission did not retain its operation reference");
		const replay = await gateway.client.request("main.submit", request);
		expect(replay.result).toEqual({ accepted: true, op_ref: commands[0].opRef, delivered_as: "prompt" });
		expect(gateway.fixture.commands().filter(command => command.operation === "turn.prompt" && command.text === request.text)).toHaveLength(1);
	};

	await settleLostReceipt(1);
	await settleLostReceipt(2);
	expect(gateway.core.mainAdmissionOperationsPending()).toEqual([]);
	expect(gateway.state.read()).toMatchObject({ bootstrapState: "COMMITTED", failedClosedReason: undefined });
}, 30_000);

externalTest("a definitive broker rejection abandons only its claim while ambiguous accepted claims still reconcile without resend", async () => {
	const gateway = await hosted();
	const rejectedRequest = { text: "retry after definite rejection", surface_id: "owner", idempotency_key: "definite-rejection" };
	gateway.fixture.rejectNextTurn("broker_rejected", "fixture rejected this admission");
	const rejected = await gateway.client.request("main.submit", rejectedRequest);
	expect(rpcError(rejected)).toMatchObject({ code: -32603, message: "bridge_exception", data: { reason: "turn_admission_failed" } });
	await eventually(() => (gateway.state.read().growthIntent === undefined ? true : undefined), "definitive rejection did not clear its growth intent");
	expect(gateway.host.admissionFenceReason).toBeUndefined();
	expect(gateway.core.mainAdmissionOperationsPending()).toEqual([]);

	gateway.fixture.holdNextTurn();
	const retry = await gateway.client.request("main.submit", rejectedRequest);
	expect(retry.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
	await settleHeld(gateway, (retry.result as { op_ref: string }).op_ref, "same key retried after broker rejection");

	gateway.fixture.holdNextTurn();
	const unrelated = await gateway.client.request("main.submit", {
		text: "unrelated key remains usable",
		surface_id: "owner",
		idempotency_key: "unrelated-after-rejection",
	});
	expect(unrelated.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
	await settleHeld(gateway, (unrelated.result as { op_ref: string }).op_ref, "unrelated key after broker rejection");
	expect(gateway.core.mainAdmissionOperationsPending()).toEqual([]);

	await gateway.host.dispose();
	const restartedSupervisor = createExternalHostSupervisor({
		broker: new BrokerCli({ executable: gateway.fixture.executable, environment: gateway.fixture.environment() }),
		workspace: gateway.fixture.workspace,
		tailTimeoutMs: 500,
		commandTimeoutMs: 1_000,
	});
	try {
		const resumed = await strictResumeMainSession({ profile: gateway.profile, state: gateway.state, supervisor: restartedSupervisor });
		expect(resumed.verificationState).toBe("verified");
		expect(gateway.state.read()).toMatchObject({ bootstrapState: "COMMITTED", failedClosedReason: undefined });
	} finally {
		await restartedSupervisor.dispose();
	}
});

externalTest("main.submit rejects conflicting reuse of a pending idempotency key with 1500", async () => {
	const gateway = await hosted();
	gateway.fixture.holdNextTurn();
	const accepted = await gateway.client.request("main.submit", {
		text: "first meaning",
		surface_id: "owner",
		idempotency_key: "conflicting-key",
	});
	expect(accepted.result).toMatchObject({ accepted: true });

	const conflict = await gateway.client.request("main.submit", {
		text: "different meaning",
		surface_id: "owner",
		idempotency_key: "conflicting-key",
	});
	expect(rpcError(conflict)).toEqual(expect.objectContaining({ code: 1500, message: "idempotency_conflict" }));
	expect(gateway.fixture.commands().filter(command => command.opRef === (accepted.result as { op_ref: string }).op_ref)).toHaveLength(1);
	await settleHeld(gateway, (accepted.result as { op_ref: string }).op_ref, "settled after conflict assertion");

});

externalTest("main.submit rejects an unknown surface with 1300", async () => {
	const gateway = await hosted();
	const response = await gateway.client.request("main.submit", {
		text: "unknown route",
		surface_id: "not-configured",
		idempotency_key: "unknown-surface",
	});
	expect(rpcError(response)).toEqual(expect.objectContaining({ code: 1300, message: "unknown_surface" }));
	expect(gateway.fixture.commands()).toEqual([]);
});

externalTest("main.submit rejects empty text before broker admission", async () => {
	const gateway = await hosted();
	const response = await gateway.client.request("main.submit", {
		text: "   ",
		surface_id: "owner",
		idempotency_key: "empty-text",
	});
	expect(rpcError(response)).toEqual(expect.objectContaining({ code: -32602, message: "text must be a non-empty string." }));
	expect(gateway.fixture.commands()).toEqual([]);
});

externalTest("main.submit rejects a quarantined surface with 1302", async () => {
	const gateway = await hosted({ isSurfaceQuarantined: surface => surface.id === "owner" });
	const response = await gateway.client.request("main.submit", {
		text: "quarantined owner input",
		surface_id: "owner",
		idempotency_key: "quarantined-owner",
	});
	expect(rpcError(response)).toEqual(expect.objectContaining({ code: 1302, message: "session_quarantined" }));
	expect(gateway.fixture.commands()).toEqual([]);
});

externalTest("non-owner admission is follow_up while the external main session is idle or busy", async () => {
	const gateway = await hosted();
	const idle = await gateway.client.request("main.submit", {
		text: "idle guest message",
		surface_id: "guest",
		idempotency_key: "guest-idle",
	});
	expect(idle.result).toMatchObject({ accepted: true, delivered_as: "follow_up" });

	gateway.fixture.holdNextTurn();
	const owner = await gateway.client.request("main.submit", {
		text: "hold owner turn",
		surface_id: "owner",
		idempotency_key: "owner-held-for-guest",
	});
	expect(owner.result).toMatchObject({ delivered_as: "prompt" });
	const busy = await gateway.client.request("main.submit", {
		text: "busy guest message",
		surface_id: "guest",
		idempotency_key: "guest-busy",
	});
	expect(busy.result).toMatchObject({ accepted: true, delivered_as: "follow_up" });
	expect(gateway.fixture.commands().filter(command => command.operation === "turn.follow_up").map(command => command.text)).toEqual([
		"idle guest message",
		"busy guest message",
	]);
	await settleHeld(gateway, (owner.result as { op_ref: string }).op_ref, "settled after guest follow-up assertion");
});

externalTest("post-admission external failure degrades health and closes the terminal journal attempt", async () => {
	const gateway = await hosted();
	gateway.fixture.holdNextTurn();
	const response = await gateway.client.request("main.submit", {
		text: "fails after acceptance",
		surface_id: "owner",
		idempotency_key: "fails-after-acceptance",
	});
	const opRef = (response.result as { op_ref: string }).op_ref;
	expect(response.result).toMatchObject({ accepted: true, delivered_as: "prompt" });

	gateway.fixture.complete(opRef, { failure: true });
	await eventually(() => (gateway.host.degraded ? true : undefined), "host did not degrade after terminal broker failure");
	await eventually(
		() => (gateway.core.journalRead("1:0", 100).events.some(event => event.kind === "turn_end") ? true : undefined),
		"terminal failure did not close the journal attempt",
	);

	expect((await gateway.client.request("way.health", {})).result).toMatchObject({
		status: "unhealthy",
		state: "degraded",
		reason: "turn_execution_failed",
	});
	const terminalEvents = gateway.core
		.journalRead("1:0", 100)
		.events.filter(event => event.kind === "turn_start" || event.kind === "assistant_message" || event.kind === "turn_end");
	expect(terminalEvents.map(event => event.kind)).toEqual(["turn_start", "turn_end"]);
	expect(gateway.state.read().growthIntent).toBeUndefined();
});

externalTest("main.events.read projects overlapping external lifecycle pairs as one attempt", async () => {
	const gateway = await hosted();
	gateway.fixture.holdNextTurn();
	const response = await gateway.client.request("main.submit", {
		text: "deduplicated lifecycle",
		surface_id: "owner",
		idempotency_key: "deduplicated-lifecycle",
	});
	const opRef = (response.result as { op_ref: string }).op_ref;
	await eventually(
		() => (gateway.core.journalRead("1:0", 100).events.some(event => event.kind === "turn_start") ? true : undefined),
		"overlapping lifecycle did not publish its start pair before completion",
		15_000,
	);
	gateway.fixture.complete(opRef, { text: "one final answer" });
	await eventually(
		() => (gateway.core.journalRead("1:0", 100).events.filter(event => event.kind === "turn_end").length === 1 ? true : undefined),
		"external lifecycle did not settle",
		15_000,
	);

	const events = await gateway.client.request("main.events.read", {
		cursor: "1:0",
		kinds: ["turn_start", "assistant_message", "turn_end"],
	});
	const rows = (events.result as { events: Array<{ kind: string; payload: Record<string, unknown> }> }).events;
	expect(rows.map(row => row.kind)).toEqual(["turn_start", "assistant_message", "turn_end"]);
	expect(rows[0]?.payload).toEqual({ attempt_id: `${gateway.fixture.sessionId}:${opRef}`, generation: 1, lineage: "main" });
	expect(rows[1]?.payload).toMatchObject({ finalized: true, text: "one final answer" });
	expect(rows[2]?.payload).toEqual({ attempt_id: `${gateway.fixture.sessionId}:${opRef}`, generation: 1, lineage: "main" });
}, 20_000);

externalTest("main.gate.answer reports unsupported and durably replays that honest broker limitation", async () => {
	const gateway = await hosted();
	gateway.host.gates.observeOpen({ gateId: "broker-cannot-answer", expectedSessionId: gateway.fixture.sessionId });
	const request = {
		gate_id: "broker-cannot-answer",
		expected_session_id: gateway.fixture.sessionId,
		answer: { approved: true },
		idempotency_key: "broker-gate-unsupported",
	};

	const first = await gateway.client.request("main.gate.answer", request);
	const replayClient = await connectEventually(gateway.socketPath);
	try {
		const replay = await replayClient.request("main.gate.answer", request);
		expect(first.result).toEqual({ accepted: false, gate_state: "unsupported" });
		expect(replay.result).toEqual(first.result);
		expect(
			gateway.core.idempotencyReplay({
				scope: "main.gate.answer",
				key: request.idempotency_key,
				requestJson: canonicalJson(request),
			}),
		).toEqual(expect.objectContaining({ replayed: true, responseJson: canonicalJson({ accepted: false, gate_state: "unsupported" }) }));
	} finally {
		replayClient.close();
	}
});

externalTest("process tail exhaustion keeps the RPC socket serving while every health surface reports degraded", async () => {
	const fixture = new FakeBrokerFixture();
	const corpus = path.join(fixture.root, "corpus");
	const profilePath = path.join(fixture.root, "process-health-profile.toml");
	const processStateRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gajaeway-tail-health-"));
	const stateDirectory = path.join(processStateRoot, "state");
	const socketPath = path.join(stateDirectory, "rpc.sock");
	const environment = {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
	};
	fs.mkdirSync(corpus, { recursive: true });
	fs.writeFileSync(
		profilePath,
		`[corpus]
path = "${corpus}"
workspace = "${fixture.workspace}"

[injection]
files = []

[main_session]
session_id = "${fixture.sessionId}"

[surfaces.owner]
id = "owner"
platform = "test"
kind = "dm"
`,
	);
	let daemon: ReturnType<typeof managedProcesses.spawnDaemon> | undefined;
	let client: Awaited<ReturnType<typeof connectEventually>> | undefined;
	try {
		const bootstrap = Bun.spawn({
			cmd: ["bun", "src/main.ts", "bootstrap", "--confirm", "--state-dir", stateDirectory, "--profile", profilePath],
			cwd: process.cwd(),
			env: environment,
			stdout: "pipe",
			stderr: "pipe",
		});
		const [exitCode, stdout, stderr] = await Promise.all([
			bootstrap.exited,
			new Response(bootstrap.stdout).text(),
			new Response(bootstrap.stderr).text(),
		]);
		if (exitCode !== 0) throw new Error(`process health bootstrap failed (${exitCode}): ${stderr || stdout}`);

		// strictResumeMainSession consumes the one successful tail; the host then
		// starts with an exhausted tail path while startup is still publishing.
		fixture.crashTailsAfter(1, 50);
		daemon = managedProcesses.spawnDaemon({
			cmd: ["bun", "src/main.ts", "serve", "--state-dir", stateDirectory, "--profile", profilePath],
			cwd: process.cwd(),
			env: environment,
			stderr: "pipe",
		});
		try {
			client = await connectEventually(socketPath);
		} catch (error) {
			if (daemon.exitCode !== null) {
				const stderr = await new Response(daemon.stderr).text();
				throw new Error(`process health daemon exited before exposing RPC (${daemon.exitCode}): ${stderr}`);
			}
			throw error;
		}
		let degraded: Record<string, unknown> | undefined;
		for (let attempt = 0; attempt < 1_400; attempt += 1) {
			const health = await client.request("way.health", {});
			if ((health.result as { state?: unknown } | undefined)?.state === "degraded") {
				degraded = health.result as Record<string, unknown>;
				break;
			}
			await Bun.sleep(25);
		}
		expect(degraded).toMatchObject({ status: "unhealthy", state: "degraded", reason: "tail_unavailable" });

		const status = await client.request("way.status", {});
		expect(status.result).toMatchObject({ status: "unhealthy", state: "degraded", reason: "tail_unavailable" });

		let fileHealth: unknown;
		for (let attempt = 0; attempt < 100; attempt += 1) {
			try {
				fileHealth = JSON.parse(fs.readFileSync(path.join(stateDirectory, "health.json"), "utf8"));
				if ((fileHealth as { state?: unknown } | undefined)?.state === "degraded") break;
			} catch {
				// The asynchronous health-file writer may not have replaced the baseline yet.
			}
			await Bun.sleep(25);
		}
		expect(fileHealth).toMatchObject({ status: "unhealthy", state: "degraded", reason: "tail_unavailable" });
	} finally {
		client?.close();
		if (daemon) await managedProcesses.stopDaemon(daemon);
		fixture.dispose();
		fs.rmSync(processStateRoot, { recursive: true, force: true });
	}
}, 45_000);