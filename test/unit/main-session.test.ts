import * as fs from "node:fs";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { BrokerCli, BrokerDtoParseError, parseSessionCheckpoint } from "../../src/broker/cli";
import { createMainAdmissionHandler, MainAdmissionRecoveryError } from "../../src/main-session/admission";
import { BootstrapError, bootstrapMainSession } from "../../src/main-session/bootstrap";
import { createMainSessionHost } from "../../src/main-session/host";
import { recoverFailedClosedGateway } from "../../src/main-session/recover";
import { ResumeError, strictResumeMainSession } from "../../src/main-session/resume";
import { compareTailCheckpoints, GatewayStateError, GatewayStateStore } from "../../src/main-session/state";
import { createExternalHostSupervisor, type HostSupervisor, type SupervisorTailEvents } from "../../src/main-session/supervisor";
import { loadWayProfile } from "../../src/profile";
import { durableTestJournal, FakeBrokerFixture, MemoryGatewayMeta } from "../helpers/main-session";

const fixtures: FakeBrokerFixture[] = [];

afterEach(() => {
	for (const fixture of fixtures.splice(0)) fixture.dispose();
});

function fixtureProfile(fixture: FakeBrokerFixture, options: { readonly sessionId?: string } = {}) {
	const corpus = path.join(fixture.root, "corpus");
	fs.mkdirSync(corpus, { recursive: true });
	const profilePath = path.join(fixture.root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[corpus]
path = "${corpus}"
workspace = "${fixture.workspace}"

[injection]
files = []

[main_session]
session_id = "${options.sessionId ?? fixture.sessionId}"

[surfaces.owner]
id = "owner"
platform = "test"
kind = "dm"

[operator]
id = "operator-1"
`,
	);
	return loadWayProfile(profilePath);
}

function supervisor(fixture: FakeBrokerFixture) {
	return createExternalHostSupervisor({
		broker: new BrokerCli({ executable: fixture.executable, environment: fixture.environment() }),
		workspace: fixture.workspace,
		tailTimeoutMs: 100,
		adoptionTailTimeoutMs: 100,
		// Unit uses the real fake-broker child process; 5s prevents cold spawn timeout under contention.
		commandTimeoutMs: 5_000,
	});
}

async function eventually(predicate: () => boolean, message: string, timeoutMs = 5_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (predicate()) return;
		await Bun.sleep(10);
	}
	throw new Error(message);
}

function recordingDurableJournal(state: GatewayStateStore, events: Array<{ kind: string; payloadJson: string }>) {
	return durableTestJournal(state, {
		journalAppend: (kind, payloadJson) => events.push({ kind, payloadJson }),
		onTranscriptProjection: (kind, payloadJson) => events.push({ kind, payloadJson }),
	});
}


async function bootstrapFixture(fixture: FakeBrokerFixture) {
	const meta = new MemoryGatewayMeta();
	const state = new GatewayStateStore(meta);
	const profile = fixtureProfile(fixture);
	const adoption = supervisor(fixture);
	try {
		const committed = await bootstrapMainSession({
			confirm: true,
			profile,
			state,
			supervisor: adoption,
			sessionId: fixture.sessionId,
		});
		return { meta, state, profile, committed };
	} finally {
		await adoption.dispose();
	}
}


test.serial("bootstrap adopts and persists the exact live external identity without creating a GJC session", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { meta, state, committed } = await bootstrapFixture(fixture);

	expect(committed.identity).toMatchObject({
		version: 1,
		sessionId: fixture.sessionId,
		locator: { repo: fixture.workspace, stateRoot: path.join(fixture.workspace, ".gjc", "state") },
		transcript: { entryCount: 2, sha256: expect.stringMatching(/^[a-f0-9]{64}$/) },
	});
	expect(state.read()).toMatchObject({
		bootstrapState: "COMMITTED",
		mainIdentity: { sessionId: fixture.sessionId },
		tailCheckpoint: { revision: 2, generation: 1, seq: 0 },
		transcriptDeliveryProgress: { lastEntryId: `${fixture.sessionId}:transcript:1` },
		transcriptProof: "proven",
	});
	expect(meta.events).toEqual([
		{ kind: "tail_adoption_start", payloadJson: JSON.stringify({ checkpoint: { revision: 2, generation: 1, seq: 0 } }) },
	]);
	expect(fixture.commands()).toEqual([]);
});

test("tail checkpoint ordering is lexicographic by generation then sequence, independent of revision", () => {
	expect(compareTailCheckpoints({ revision: 999, generation: 2, seq: 0 }, { revision: 0, generation: 1, seq: 999 })).toBeGreaterThan(0);
	expect(compareTailCheckpoints({ revision: 1, generation: 2, seq: 4 }, { revision: 999, generation: 2, seq: 5 })).toBeLessThan(0);
	expect(compareTailCheckpoints({ revision: 1, generation: 2, seq: 5 }, { revision: 999, generation: 2, seq: 5 })).toBe(0);
});

test("missing transcript_proof metadata is corrupt durable state, not an inferred legacy proof", () => {
	const meta = new MemoryGatewayMeta();
	const state = new GatewayStateStore(meta);
	meta.values.delete("transcript_proof");
	expect(() => state.read()).toThrow(GatewayStateError);
	try {
		state.read();
		throw new Error("missing transcript_proof unexpectedly read as a legacy proof");
	} catch (error) {
		expect(error).toMatchObject({ reason: "metadata_missing" } satisfies Partial<GatewayStateError>);
	}
	meta.values.set("transcript_proof", "null");
	expect(() => state.read()).toThrow(GatewayStateError);
	try {
		state.read();
		throw new Error("null transcript_proof unexpectedly read as a legacy proof");
	} catch (error) {
		expect(error).toMatchObject({ reason: "metadata_invalid" } satisfies Partial<GatewayStateError>);
	}
});

test.serial("bootstrap commits a pending proof when the bounded tail has no envelope", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const meta = new MemoryGatewayMeta();
	const state = new GatewayStateStore(meta);
	const profile = fixtureProfile(fixture);
	const adoption = supervisor(fixture);
	fixture.timeoutNextTails(50);
	try {
		await expect(bootstrapMainSession({ confirm: true, profile, state, supervisor: adoption, sessionId: fixture.sessionId })).resolves.toMatchObject({
			kind: "committed",
		});
		const durable = state.read();
		expect(durable).toMatchObject({
			bootstrapState: "COMMITTED",
			mainIdentity: { sessionId: fixture.sessionId },
			transcriptProof: "pending",
			tailCheckpoint: undefined,
		});
		expect(durable.mainIdentity?.transcript).toBeUndefined();
		expect(durable.transcriptDeliveryProgress).toBeUndefined();
		expect(meta.events).toEqual([]);
	} finally {
		await adoption.dispose();
	}
});
test.serial("bootstrap refuses only when its immediate session-checkpoint liveness query is unavailable", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const meta = new MemoryGatewayMeta();
	const state = new GatewayStateStore(meta);
	const profile = fixtureProfile(fixture);
	const adoption = supervisor(fixture);
	fixture.setQueryUnavailable("session.checkpoint");
	try {
		await expect(
			bootstrapMainSession({ confirm: true, profile, state, supervisor: adoption, sessionId: fixture.sessionId }),
		).rejects.toMatchObject({ reason: "session_checkpoint_unavailable" } satisfies Partial<BootstrapError>);
		expect(state.read()).toMatchObject({ bootstrapState: "CREATING", mainIdentity: undefined });
	} finally {
		await adoption.dispose();
	}
});

test.serial("a later complete verification tail promotes a pending proof", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const meta = new MemoryGatewayMeta();
	const state = new GatewayStateStore(meta);
	const profile = fixtureProfile(fixture);
	const adoption = supervisor(fixture);
	fixture.timeoutNextTails(1);
	try {
		await bootstrapMainSession({ confirm: true, profile, state, supervisor: adoption, sessionId: fixture.sessionId });
		expect(state.read().transcriptProof).toBe("pending");
	} finally {
		await adoption.dispose();
	}
	const resumedSupervisor = supervisor(fixture);
	try {
		const resumed = await strictResumeMainSession({ profile, state, supervisor: resumedSupervisor });
		expect(resumed.identity.transcript).toBeDefined();
		expect(state.read()).toMatchObject({ transcriptProof: "proven", transcriptDeliveryGapCount: 1, transcriptDeliveryProgress: { fingerprint: { entryCount: 2 } } });
		expect(meta.events).toContainEqual(
			expect.objectContaining({
				kind: "transcript_delivery_gap",
				payloadJson: expect.stringContaining('"adoption":"pending_proof_promotion"'),
			}),
		);
	} finally {
		await resumedSupervisor.dispose();
	}
});



test.serial("strict resume records a visible delivery gap when pending-proof promotion covers a post-commit reply", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { meta, state, profile, committed } = await bootstrapFixture(fixture);
	const legacyIdentity = { ...committed.identity } as { transcript?: unknown } & Record<string, unknown>;
	delete legacyIdentity.transcript;
	meta.values.set("main_identity", JSON.stringify(legacyIdentity));
	meta.values.set("transcript_delivery_progress", "null");
	meta.values.set("transcript_proof", "pending");
	meta.values.set("tail_checkpoint", "null");
	fixture.appendTranscript({ type: "message", role: "assistant", content: "reply finalized after pending adoption" });
	const resumedSupervisor = supervisor(fixture);
	try {
		const resumed = await strictResumeMainSession({ profile, state, supervisor: resumedSupervisor });
		expect(resumed.identity.transcript).toBeDefined();
		expect(state.read()).toMatchObject({
			mainIdentity: { transcript: { entryCount: 3 } },
			transcriptDeliveryGapCount: 1,
			transcriptDeliveryProgress: { lastEntryId: `${fixture.sessionId}:transcript:2`, fingerprint: { entryCount: 3 } },
			tailCheckpoint: { revision: 3, generation: 1, seq: 0 },
		});
		const gap = meta.events.find(event => event.kind === "transcript_delivery_gap");
		expect(JSON.parse(gap?.payloadJson ?? "{}")).toMatchObject({
			reason: "transcript_delivery_progress_missing",
			adoption: "pending_proof_promotion",
			available_through_entry_id: `${fixture.sessionId}:transcript:2`,
		});
		expect(meta.events.some(event => event.kind === "assistant_message")).toBe(false);
	} finally {
		await resumedSupervisor.dispose();
	}
});

test.serial("strict resume fails closed when the exact adopted external session disappears", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	fixture.setLive(false);
	const resumed = supervisor(fixture);
	try {
		await expect(strictResumeMainSession({ profile, state, supervisor: resumed })).rejects.toMatchObject({
			reason: "session_unavailable",
		} satisfies Partial<ResumeError>);
		expect(state.read()).toMatchObject({ bootstrapState: "FAILED_CLOSED", failedClosedReason: "session_unavailable" });
	} finally {
		await resumed.dispose();
	}
});

test.serial("scripted broker CLI fixture covers inspect, send, status, tail, and overlapping SDK lifecycle frames", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const broker = new BrokerCli({ executable: fixture.executable, environment: fixture.environment() });
	const rows = await broker.listSessions();
	expect(rows.sessions).toHaveLength(1);
	expect((await broker.inspectSession(fixture.sessionId)).sessionId).toBe(fixture.sessionId);
	expect((await broker.sessionMetadata(fixture.sessionId)).kind).toBe("main");
	expect(await broker.sessionCheckpoint(fixture.sessionId)).toEqual({ revision: 2, generation: 0, seq: 0 });
	const receipt = await broker.sendPrompt(fixture.sessionId, "fixture command", "fixture-op");
	expect(receipt).toMatchObject({ sessionId: fixture.sessionId, operation: "turn.prompt", operationRef: "fixture-op" });
	expect(await broker.turnStatus(fixture.sessionId, "fixture-op")).toMatchObject({ status: "terminal_ok", completed: true });
	const tail = await broker.tailSession(fixture.sessionId, { repo: fixture.workspace, allEvents: true, untilIdle: true, strict: true });
	expect(tail.items.filter(item => item.kind === "agent_start")).toHaveLength(2);
	expect(tail.items.map(item => item.kind)).not.toContain("message_end");
	expect(tail.items.filter(item => item.kind === "transcript").every(item => typeof item.id === "string" && item.id.length > 0)).toBe(true);
});

test.serial("session.checkpoint accepts decorative query fields but rejects malformed authority", () => {
	// Real envelope observed live: {type:"query_response", id, ok, result:{checkpoint, revisionId, issuedAt, expiresAt}}.
	const envelope = JSON.stringify({
		type: "query_response",
		id: "decorative-id",
		trace: "ignored",
		ok: true,
		result: {
			checkpoint: { revision: 7, generation: 3, seq: 11, label: "ignored" },
			revisionId: "decorative",
			issuedAt: 1787246519373,
			expiresAt: 1787247419373,
		},
	});
	expect(parseSessionCheckpoint(envelope)).toEqual({ revision: 7, generation: 3, seq: 11 });
	expect(() => parseSessionCheckpoint(JSON.stringify({ ok: true, result: { checkpoint: { revision: 7, generation: 3, seq: "11" } } }))).toThrow(
		BrokerDtoParseError,
	);
});

test.serial("scripted broker tail mirrors cursorless rotation gaps and checkpoint records", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	fixture.appendTailEvent("agent_start", { type: "agent_start", sessionId: fixture.sessionId });
	fixture.rotateTailThrough(1);
	const broker = new BrokerCli({ executable: fixture.executable, environment: fixture.environment() });
	const tail = await broker.tailSession(fixture.sessionId, { repo: fixture.workspace, allEvents: true, untilIdle: true });
	expect(tail.checkpoint).toEqual({ revision: 2, generation: 1, seq: 1 });
	expect(tail.gap).toEqual({
		code: "retention_gap",
		missing: { from: 0, to: 1 },
		resync: { revision: 2, generation: 1, seq: 1 },
	});
	await expect(
		broker.tailSession(fixture.sessionId, { repo: fixture.workspace, cursor: "not-a-signed-broker-checkpoint" }),
	).rejects.toMatchObject({ code: "invalid_cursor" });
});


test.serial("external identity disappearance is durable failed-closed host state, not a silent degraded continuation", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	const resumedSupervisor = supervisor(fixture);
	const resumed = await strictResumeMainSession({ profile, state, supervisor: resumedSupervisor });
	const host = createMainSessionHost({
		supervisor: resumedSupervisor,
		identity: resumed.identity,
		state,
		journal: durableTestJournal(state),
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
	});
	try {
		fixture.setLive(false);
		await expect(host.waitForFatalFailure()).resolves.toMatchObject({ reason: "session_unavailable" });
		expect(state.read()).toMatchObject({ bootstrapState: "FAILED_CLOSED", failedClosedReason: "session_unavailable" });
	} finally {
		await host.dispose();
	}
});

test.serial("a terminal tail snapshot that predates admission cannot settle the growth window", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, committed } = await bootstrapFixture(fixture);
	const staleTail = Promise.withResolvers<SupervisorTailEvents>();
	let tailCalls = 0;
	const controlledSupervisor: HostSupervisor = {
		async discover() {
			return {
				identity: committed.identity,
				transcriptEntries: [],
				initialRingCheckpoint: state.read().tailCheckpoint,
				transcriptProof: "proven",
				turnState: "idle" as const,
				followUpQueueDepth: 0,
			};
		},
		async verify() {
			return {
				identity: committed.identity,
				transcriptEntries: [],
				initialRingCheckpoint: state.read().tailCheckpoint,
				transcriptProof: "proven",
				turnState: "idle" as const,
				followUpQueueDepth: 0,
			};
		},
		async sendPrompt(_text, opRef) {
			return { sessionId: fixture.sessionId, operation: "turn.prompt", operationRef: opRef };
		},
		async sendSteer(_text, opRef) {
			return { sessionId: fixture.sessionId, operation: "turn.steer", operationRef: opRef };
		},
		async followUp(_text, opRef) {
			return { sessionId: fixture.sessionId, operation: "turn.follow_up", operationRef: opRef };
		},
		async operationStatus(opRef) {
			return { operationRef: opRef, status: "in_flight", completed: false, detail: {} };
		},
		async tailEvents() {
			tailCalls += 1;
			return await staleTail.promise;
		},
		async turnState() {
			return { turnState: "idle" as const, followUpQueueDepth: 0 };
		},
		async dispose() {},
	};
	const host = createMainSessionHost({
		supervisor: controlledSupervisor,
		identity: committed.identity,
		state,
		journal: durableTestJournal(state),
	});
	try {
		await eventually(() => tailCalls === 1, "host did not start its first tail poll");
		await host.admit("prompt", "admit while an old terminal tail is in flight", "stale-tail-admission");
		staleTail.resolve({
			identity: committed.identity,
			transcriptEntries: [],
			events: [],
			terminal: true,
			complete: true,
			retentionGap: false,
			checkpoint: state.read().tailCheckpoint,
		});
		await Bun.sleep(20);

		expect(host.turnState).toBe("busy");
		expect(state.read().growthIntent).toBeDefined();
	} finally {
		await host.dispose();
	}
});

test.serial("strict resume fails closed on unapproved profile drift before adopting the external session", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state } = await bootstrapFixture(fixture);
	const profilePath = path.join(fixture.root, "profile.toml");
	fs.writeFileSync(profilePath, fs.readFileSync(profilePath, "utf8").replace("files = []", 'files = ["DRIFT.md"]'));
	const resumed = supervisor(fixture);
	try {
		await expect(strictResumeMainSession({ profile: loadWayProfile(profilePath), state, supervisor: resumed })).rejects.toMatchObject({
			reason: "profile_drift",
		} satisfies Partial<ResumeError>);
		expect(state.read()).toMatchObject({ bootstrapState: "FAILED_CLOSED", failedClosedReason: "profile_drift" });
	} finally {
		await resumed.dispose();
	}
});

test.serial("strict resume fails closed when the profile selects a different external session", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state } = await bootstrapFixture(fixture);
	const mismatchedProfile = fixtureProfile(fixture, { sessionId: "other-operator-session" });
	state.approveProfile(mismatchedProfile, "profile-session-mismatch-receipt", 1);
	const resumed = supervisor(fixture);
	try {
		await expect(strictResumeMainSession({ profile: mismatchedProfile, state, supervisor: resumed })).rejects.toMatchObject({
			reason: "profile_session_mismatch",
		} satisfies Partial<ResumeError>);
		expect(state.read()).toMatchObject({ bootstrapState: "FAILED_CLOSED", failedClosedReason: "profile_session_mismatch" });
	} finally {
		await resumed.dispose();
	}
});

test.serial("strict resume absorbs attested autonomous append-only transcript growth without a growth intent", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { meta, state, profile } = await bootstrapFixture(fixture);
	const before = state.read();
	const priorEntryCount = before.mainIdentity?.transcript?.entryCount ?? 0;
	// The adopted persona is a live agent: it keeps working while the daemon is
	// down, so its transcript legitimately grows with no growth intent open.
	fixture.appendTranscript({ type: "message", role: "user", content: "autonomous growth while the daemon was down" });
	fixture.appendTranscript({ type: "message", role: "assistant", content: "autonomous reply while the daemon was down" });
	const resumed = supervisor(fixture);
	try {
		const result = await strictResumeMainSession({ profile, state, supervisor: resumed });
		expect(result.verificationState).toBe("verified");
		const after = state.read();
		expect(after.bootstrapState).toBe("COMMITTED");
		expect(after.failedClosedReason).toBeUndefined();
		// The durable identity re-bound to the observed transcript...
		expect(after.mainIdentity?.transcript?.entryCount).toBe(priorEntryCount + 2);
		// ...and the absorption is visible in the journal with its entry accounting.
		const absorbed = meta.events.filter(event => event.kind === "main_identity_growth_absorbed");
		expect(absorbed).toHaveLength(1);
		expect(JSON.parse(absorbed[0]?.payloadJson ?? "{}")).toEqual({
			reason: "autonomous_append_only_growth",
			prior_entry_count: priorEntryCount,
			observed_entry_count: priorEntryCount + 2,
			absorbed_entries: 2,
		});
		// Absorbing growth must NOT imply the new entries were delivered: delivery
		// progress stays where it was so the suffix is still projected or gapped.
		expect(after.transcriptDeliveryProgress).toEqual(before.transcriptDeliveryProgress);
	} finally {
		await resumed.dispose();
	}
});

test.serial("strict resume still fails closed when the external transcript prefix is rewritten rather than extended", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	// Not append-only: the persisted prefix no longer reproduces, so this is a
	// rewritten/truncated history and absence of attestation is never permission.
	fixture.rotateTranscriptPast(`${fixture.sessionId}:transcript:1`);
	const resumed = supervisor(fixture);
	try {
		await expect(strictResumeMainSession({ profile, state, supervisor: resumed })).rejects.toMatchObject({
			reason: "main_identity_mismatch",
		} satisfies Partial<ResumeError>);
		expect(state.read()).toMatchObject({ bootstrapState: "FAILED_CLOSED", failedClosedReason: "main_identity_mismatch" });
	} finally {
		await resumed.dispose();
	}
});

test.serial("host surface-attributes overlapping agent and turn lifecycle tail events", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	const resumedSupervisor = supervisor(fixture);
	const resumed = await strictResumeMainSession({ profile, state, supervisor: resumedSupervisor });
	const journal: Array<{ kind: string; payloadJson: string }> = [];
	const host = createMainSessionHost({
		supervisor: resumedSupervisor,
		identity: resumed.identity,
		state,
		journal: recordingDurableJournal(state, journal),
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
	});
	try {
		fixture.holdNextTurn();
		await host.admit("prompt", "deduplicate this attempt", "overlap-attempt", undefined, undefined, "owner");
		await eventually(
			() => journal.filter(event => event.kind === "turn_start").length === 1,
			"tail lifecycle did not observe the overlapping start pair",
			15_000,
		);
		fixture.complete("overlap-attempt", { text: "one terminal response" });
		await eventually(
			() => journal.filter(event => event.kind === "turn_end").length === 1,
			"tail lifecycle did not reach turn_end",
			20_000,
		);

		expect(journal.map(event => event.kind)).toEqual(["turn_start", "assistant_message", "turn_end"]);
		expect(JSON.parse(journal[0]?.payloadJson ?? "{}"))
			.toEqual({ attempt_id: `${fixture.sessionId}:overlap-attempt`, generation: 1, lineage: "main", surface_id: "owner" });
		expect(JSON.parse(journal[1]?.payloadJson ?? "{}"))
			.toEqual(expect.objectContaining({ finalized: true, text: "one terminal response", surface_id: "owner" }));
		expect(JSON.parse(journal[2]?.payloadJson ?? "{}"))
			.toEqual({ attempt_id: `${fixture.sessionId}:overlap-attempt`, generation: 1, lineage: "main", surface_id: "owner" });
	} finally {
		await host.dispose();
	}
}, 45_000);

test.serial("host journals finalized assistant messages from stable transcript entries", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	const resumedSupervisor = supervisor(fixture);
	const resumed = await strictResumeMainSession({ profile, state, supervisor: resumedSupervisor });
	const journal: Array<{ kind: string; payloadJson: string }> = [];
	const host = createMainSessionHost({
		supervisor: resumedSupervisor,
		identity: resumed.identity,
		state,
		journal: recordingDurableJournal(state, journal),
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
	});
	try {
		fixture.holdNextTurn();
		await host.admit("prompt", "produce a transcript-only final", "transcript-final");
		await eventually(
			() => journal.some(event => event.kind === "turn_start"),
			"held external turn did not start",
		);
		fixture.complete("transcript-final", { text: "final external answer" });
		await eventually(
			() => journal.filter(event => event.kind === "assistant_message").length === 1,
			"finalized transcript message was not projected",
			10_000,
		);

		const assistants = journal.filter(event => event.kind === "assistant_message");
		expect(assistants).toHaveLength(1);
		expect(JSON.parse(assistants[0]?.payloadJson ?? "{}"))
			.toEqual(expect.objectContaining({ finalized: true, text: "final external answer" }));
	} finally {
		await host.dispose();
	}
}, 15_000);

test.serial("host survives transient broker tail failures with bounded retry and recovers", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	const resumedSupervisor = supervisor(fixture);
	const resumed = await strictResumeMainSession({ profile, state, supervisor: resumedSupervisor });
	const journal: Array<{ kind: string; payloadJson: string }> = [];
	const host = createMainSessionHost({
		supervisor: resumedSupervisor,
		identity: resumed.identity,
		state,
		journal: recordingDurableJournal(state, journal),
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
	});
	try {
		fixture.holdNextTurn();
		await host.admit("prompt", "recover through transient tail failures", "transient-recovery-message");
		// Three consecutive CLI deaths (spawn pressure / nonzero exits), then healthy.
		fixture.crashNextTails(3);
		fixture.complete("transient-recovery-message", { text: "survived transient transport failure" });
		await eventually(
			() => journal.filter(event => event.kind === "assistant_message").length === 1,
			"host did not recover from transient tail failures",
			20_000,
		);
		expect(JSON.parse(journal.find(event => event.kind === "assistant_message")?.payloadJson ?? "{}"))
			.toEqual(expect.objectContaining({ finalized: true, text: "survived transient transport failure" }));
		// The host must still be usable: no failed-closed marker was recorded.
		expect(state.read().failedClosedReason).toBeUndefined();
	} finally {
		await host.dispose();
	}
}, 30_000);

test.serial("host fails closed when broker tail failures exhaust the bounded retry budget", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	const resumedSupervisor = supervisor(fixture);
	const resumed = await strictResumeMainSession({ profile, state, supervisor: resumedSupervisor });
	const journal: Array<{ kind: string; payloadJson: string }> = [];
	const healthReports: Array<{ state: string; reason: string }> = [];
	const host = createMainSessionHost({
		supervisor: resumedSupervisor,
		identity: resumed.identity,
		state,
		journal: durableTestJournal(state, {
			journalAppend: (kind, payloadJson) => journal.push({ kind, payloadJson }),
			onTranscriptProjection: (kind, payloadJson) => journal.push({ kind, payloadJson }),
			setRpcHealth: (healthState, reason) => healthReports.push({ state: healthState, reason }),
		}),
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
	});
	try {
		// More consecutive failures than the retry budget (8) tolerates.
		fixture.crashNextTails(50);
		await eventually(
			() => healthReports.some(report => report.state === "degraded" && report.reason === "tail_unavailable"),
			"host did not report degraded health after exhausting the retry budget",
			60_000,
		);
	} finally {
		await host.dispose();
	}
}, 90_000);


test.serial("bootstrap ring checkpoint suppresses retained gap-free pre-adoption lifecycle history", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	fixture.appendTailEvent("agent_start", { type: "agent_start", sessionId: fixture.sessionId });
	const { state, profile } = await bootstrapFixture(fixture);
	const resumedSupervisor = supervisor(fixture);
	const resumed = await strictResumeMainSession({ profile, state, supervisor: resumedSupervisor });
	const journal: Array<{ kind: string; payloadJson: string }> = [];
	const host = createMainSessionHost({
		supervisor: resumedSupervisor,
		identity: resumed.identity,
		state,
		journal: recordingDurableJournal(state, journal),
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
	});
	try {
		expect(state.read().tailCheckpoint).toMatchObject({ generation: 1, seq: 1 });
		await Bun.sleep(300);
		expect(journal).toEqual([]);
		fixture.appendTailEvent("agent_start", { type: "agent_start", sessionId: fixture.sessionId });
		await eventually(
			() => journal.filter(event => event.kind === "turn_start").length === 1,
			"post-adoption lifecycle event was not projected",
		);
	} finally {
		await host.dispose();
	}
});

test.serial("adoption-start retention gap records its resync checkpoint and projects subsequent events", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	fixture.appendTailEvent("agent_start", { type: "agent_start", sessionId: fixture.sessionId });
	fixture.rotateTailThrough(1);
	const { meta, state, profile } = await bootstrapFixture(fixture);
	const resumedSupervisor = supervisor(fixture);
	const resumed = await strictResumeMainSession({ profile, state, supervisor: resumedSupervisor });
	const journal: Array<{ kind: string; payloadJson: string }> = [];
	const host = createMainSessionHost({
		supervisor: resumedSupervisor,
		identity: resumed.identity,
		state,
		journal: recordingDurableJournal(state, journal),
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
	});
	try {
		await eventually(() => state.read().tailCheckpoint?.seq === 1, "host did not record the adoption-start resync checkpoint");
		expect(meta.events).toContainEqual({
			kind: "tail_adoption_start",
			payloadJson: JSON.stringify({ checkpoint: { revision: 2, generation: 1, seq: 1 } }),
		});

		fixture.holdNextTurn();
		await host.admit("prompt", "project after adoption resync", "after-adoption-resync");
		fixture.complete("after-adoption-resync", { text: "projected after adoption resync" });
		await eventually(
			() => journal.some(event => event.payloadJson.includes("projected after adoption resync")),
			"transcript after adoption resync was not projected",
		);
		expect(state.read().tailCheckpoint).toMatchObject({ generation: 1, seq: expect.any(Number) });
	} finally {
		await host.dispose();
	}
});

test.serial("an established busy-turn ring rotation journals its resync and continues from transcript delivery", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { meta, state, profile } = await bootstrapFixture(fixture);
	const resumedSupervisor = supervisor(fixture);
	const resumed = await strictResumeMainSession({ profile, state, supervisor: resumedSupervisor });
	const journal: Array<{ kind: string; payloadJson: string }> = [];
	const host = createMainSessionHost({
		supervisor: resumedSupervisor,
		identity: resumed.identity,
		state,
		journal: recordingDurableJournal(state, journal),
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
	});
	try {
		await eventually(() => state.read().tailCheckpoint?.seq === 0, "host did not establish its initial checkpoint");
		fixture.setNoEnvelopeWhileBusy();
		fixture.holdNextTurn();
		await host.admit("prompt", "rotate the held ring", "ring-rotation");
		await eventually(() => state.read().growthIntent !== undefined, "held admission did not open a growth window");
		fixture.rotateRingDuringNextCompletion();
		fixture.complete("ring-rotation", { text: "delivered after ring rotation" });
		await eventually(() => state.read().tailRingRotationCount === 1, "ring rotation was not durably recorded");
		await eventually(
			() => journal.some(event => event.kind === "assistant_message" && event.payloadJson.includes("delivered after ring rotation")),
			"transcript delivery did not project the terminal reply after ring rotation",
		);
		expect(meta.events).toContainEqual({
			kind: "tail_ring_rotation",
			payloadJson: JSON.stringify({
				prior_watermark: { revision: 2, generation: 1, seq: 0 },
				resync_point: { revision: 4, generation: 1, seq: 5 },
			}),
		});
		expect(state.read()).toMatchObject({ bootstrapState: "COMMITTED", failedClosedReason: undefined, tailRingRotationCount: 1 });
		expect(host.degraded).toBe(false);
		expect(host.turnState).toBe("idle");
	} finally {
		await host.dispose();
	}
});

test.serial("a failed journal append leaves the tail checkpoint behind so the scripted broker event replays", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	const firstSupervisor = supervisor(fixture);
	const first = await strictResumeMainSession({ profile, state, supervisor: firstSupervisor });
	const failedHost = createMainSessionHost({
		supervisor: firstSupervisor,
		identity: first.identity,
		state,
		journal: durableTestJournal(state, {
			journalAppend: () => {
				throw new Error("scripted journal interruption");
			},
		}),
		initialTurnState: first.turnState,
		initialFollowUpQueueDepth: first.followUpQueueDepth,
	});
	try {
		await eventually(() => state.read().tailCheckpoint?.seq === 0, "host did not establish its initial checkpoint");
		fixture.appendTailEvent("agent_start", { type: "agent_start", sessionId: fixture.sessionId });
		await eventually(() => failedHost.degraded, "scripted journal failure did not degrade the first host");
		expect(state.read().tailCheckpoint).toMatchObject({ generation: 1, seq: 0 });
	} finally {
		await failedHost.dispose();
	}

	const replaySupervisor = supervisor(fixture);
	const replayed = await strictResumeMainSession({ profile, state, supervisor: replaySupervisor });
	const journal: Array<{ kind: string; payloadJson: string }> = [];
	const replayHost = createMainSessionHost({
		supervisor: replaySupervisor,
		identity: replayed.identity,
		state,
		journal: recordingDurableJournal(state, journal),
		initialTurnState: replayed.turnState,
		initialFollowUpQueueDepth: replayed.followUpQueueDepth,
	});
	try {
		await eventually(
			() => journal.some(event => event.kind === "turn_start"),
			"restarted host did not replay the uncheckpointed broker event",
		);
		expect(state.read().tailCheckpoint).toMatchObject({ generation: 1, seq: 1 });
	} finally {
		await replayHost.dispose();
	}
});

test.serial("a reply finalized while the daemon is down is recovered from durable transcript delivery progress", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { meta, state, profile } = await bootstrapFixture(fixture);
	const firstSupervisor = supervisor(fixture);
	const first = await strictResumeMainSession({ profile, state, supervisor: firstSupervisor });
	const firstHost = createMainSessionHost({
		supervisor: firstSupervisor,
		identity: first.identity,
		state,
		journal: durableTestJournal(state),
		initialTurnState: first.turnState,
		initialFollowUpQueueDepth: first.followUpQueueDepth,
		initialVerificationState: first.verificationState,
	});
	try {
		fixture.setNoEnvelopeWhileBusy();
		fixture.holdNextTurn();
		await firstHost.admit("prompt", "finish while daemon is down", "down-recovery");
		await eventually(() => state.read().growthIntent !== undefined, "growth intent was not durable before daemon stop");
	} finally {
		await firstHost.dispose();
	}
	fixture.rotateRingDuringNextCompletion();
	fixture.complete("down-recovery", { text: "recovered after daemon downtime" });

	const restartedSupervisor = supervisor(fixture);
	const restarted = await strictResumeMainSession({ profile, state, supervisor: restartedSupervisor });
	const journal: Array<{ kind: string; payloadJson: string }> = [];
	const restartedHost = createMainSessionHost({
		supervisor: restartedSupervisor,
		identity: restarted.identity,
		state,
		journal: recordingDurableJournal(state, journal),
		initialTurnState: restarted.turnState,
		initialFollowUpQueueDepth: restarted.followUpQueueDepth,
		initialVerificationState: restarted.verificationState,
		...(restarted.verificationTail === undefined ? {} : { verificationTail: restarted.verificationTail }),
		...(restarted.growthIntent === undefined ? {} : { recoveredGrowthIntent: restarted.growthIntent }),
	});
	try {
		expect(restarted.verificationState).toBe("verified");
		expect(restarted.verificationTail).toMatchObject({ retentionGap: true, resyncCheckpoint: { generation: 1, seq: 5 } });
		expect(restarted.recoveredGrowthIntent).toBe(true);
		await eventually(
			() => journal.some(event => event.kind === "assistant_message" && event.payloadJson.includes("recovered after daemon downtime")),
			"reply finalized while down was not recovered",
		);
		await eventually(() => state.read().tailRingRotationCount === 1, "verification-tail ring rotation was not durably recorded");
		expect(meta.events).toContainEqual({
			kind: "tail_ring_rotation",
			payloadJson: JSON.stringify({
				prior_watermark: { revision: 2, generation: 1, seq: 0 },
				resync_point: { revision: 4, generation: 1, seq: 5 },
			}),
		});
		expect(journal.some(event => event.kind === "transcript_delivery_gap")).toBe(false);
		await eventually(() => state.read().growthIntent === undefined, "recovered growth intent was not finalized after transcript delivery");
	} finally {
		await restartedHost.dispose();
	}
});

test.serial("an admitted opaque assistant suffix surface-attributes its delivery gap instead of silently advancing", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { meta, state, profile } = await bootstrapFixture(fixture);
	const resumedSupervisor = supervisor(fixture);
	const resumed = await strictResumeMainSession({ profile, state, supervisor: resumedSupervisor });
	const host = createMainSessionHost({
		supervisor: resumedSupervisor,
		identity: resumed.identity,
		state,
		journal: {
			journalAppend: () => undefined,
			journalAppendAtTailCheckpoint: (kind, payloadJson, expected, checkpoint) => state.appendTailProjection(expected, checkpoint, kind, payloadJson),
			journalAppendTranscriptProjection: (kind, payloadJson, expectedTail, checkpoint, expectedDelivery, nextDelivery) =>
				state.appendTranscriptProjection(expectedTail, checkpoint, expectedDelivery, nextDelivery, kind, payloadJson),
		},
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
		initialVerificationState: resumed.verificationState,
		...(resumed.verificationTail === undefined ? {} : { verificationTail: resumed.verificationTail }),
	});
	try {
		fixture.holdNextTurn();
		await host.admit("prompt", "hold while an opaque assistant suffix appears", "opaque-suffix", undefined, undefined, "owner");
		await eventually(() => state.read().growthIntent !== undefined, "held operation did not open growth intent");
		fixture.appendTranscript({
			type: "message",
			role: "assistant",
			content: [{ type: "opaque_output", value: "must not silently rebaseline" }],
			turnId: "turn:opaque-suffix",
		});
		await eventually(
			() => meta.events.some(event => event.kind === "transcript_delivery_gap"),
			"opaque assistant transcript suffix did not journal a delivery gap",
		);
		const gap = meta.events.find(event => event.kind === "transcript_delivery_gap");
		expect(JSON.parse(gap?.payloadJson ?? "{}")).toMatchObject({
			reason: "transcript_delivery_unprovable",
			unprojectable_entry_id: `${fixture.sessionId}:transcript:2`,
			surface_id: "owner",
		});
		expect(state.read()).toMatchObject({ transcriptDeliveryGapCount: 1, transcriptDeliveryProgress: { lastEntryId: `${fixture.sessionId}:transcript:2` } });
		expect(meta.events.some(event => event.kind === "assistant_message")).toBe(false);
	} finally {
		await host.dispose();
	}
});

test.serial("an unprovable transcript suffix journals a durable delivery gap instead of silently baselining", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, committed } = await bootstrapFixture(fixture);
	const checkpoint = state.read().tailCheckpoint!;
	const controlled: HostSupervisor = {
		async discover() {
			return { identity: committed.identity, transcriptEntries: [], initialRingCheckpoint: checkpoint, transcriptProof: "proven", turnState: "idle", followUpQueueDepth: 0 };
		},
		async verify() {
			return { identity: committed.identity, transcriptEntries: [], initialRingCheckpoint: checkpoint, transcriptProof: "proven", turnState: "idle", followUpQueueDepth: 0 };
		},
		async sendPrompt() {
			throw new Error("not used");
		},
		async sendSteer() {
			throw new Error("not used");
		},
		async followUp() {
			throw new Error("not used");
		},
		async operationStatus(opRef) {
			return { operationRef: opRef, status: "unknown", completed: false, detail: {} };
		},
		async tailEvents() {
			return {
				identity: committed.identity,
				transcriptEntries: [],
				events: [],
				terminal: true,
				complete: true,
				retentionGap: false,
				checkpoint,
			};
		},
		async turnState() {
			return { turnState: "idle" as const, followUpQueueDepth: 0 };
		},
		async dispose() {},
	};
	const journal: Array<{ kind: string; payloadJson: string }> = [];
	const host = createMainSessionHost({
		supervisor: controlled,
		identity: committed.identity,
		state,
		journal: recordingDurableJournal(state, journal),
	});
	try {
		await eventually(
			() => journal.some(event => event.kind === "transcript_delivery_gap"),
			"unprovable transcript suffix did not produce a delivery-gap journal event",
		);
		expect(JSON.parse(journal.find(event => event.kind === "transcript_delivery_gap")?.payloadJson ?? "{}"))
			.toMatchObject({ reason: "transcript_delivery_unprovable", delivered_through_entry_id: `${fixture.sessionId}:transcript:1` });
		expect(state.read().transcriptDeliveryProgress).toMatchObject({ fingerprint: { entryCount: 0 } });
	} finally {
		await host.dispose();
	}
});

test.serial("a rotating external transcript window emits delivery-gap before fail-closed identity loss", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	const resumedSupervisor = supervisor(fixture);
	const resumed = await strictResumeMainSession({ profile, state, supervisor: resumedSupervisor });
	const journal: Array<{ kind: string; payloadJson: string }> = [];
	const host = createMainSessionHost({
		supervisor: resumedSupervisor,
		identity: resumed.identity,
		state,
		journal: recordingDurableJournal(state, journal),
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
	});
	try {
		fixture.rotateTranscriptPast(`${fixture.sessionId}:transcript:1`);
		await eventually(
			() => journal.some(event => event.kind === "transcript_delivery_gap"),
			"rotating transcript window did not emit a delivery gap",
		);
		await expect(host.waitForFatalFailure()).resolves.toMatchObject({ reason: "main_identity_mismatch" });
		expect(state.read()).toMatchObject({ bootstrapState: "FAILED_CLOSED", failedClosedReason: "main_identity_mismatch" });
	} finally {
		await host.dispose();
	}
});

test.serial("a recovered busy growth window remains open across restart until terminal transcript delivery", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	const firstSupervisor = supervisor(fixture);
	const first = await strictResumeMainSession({ profile, state, supervisor: firstSupervisor });
	const firstHost = createMainSessionHost({
		supervisor: firstSupervisor,
		identity: first.identity,
		state,
		journal: durableTestJournal(state),
		initialTurnState: first.turnState,
		initialFollowUpQueueDepth: first.followUpQueueDepth,
		initialVerificationState: first.verificationState,
	});
	try {
		fixture.setNoEnvelopeWhileBusy();
		fixture.holdNextTurn();
		await firstHost.admit("prompt", "stay busy across restart", "busy-restart");
		await eventually(() => firstHost.turnState === "busy", "first host did not observe its busy operation");
	} finally {
		await firstHost.dispose();
	}

	const restartedSupervisor = supervisor(fixture);
	const restarted = await strictResumeMainSession({ profile, state, supervisor: restartedSupervisor });
	const journal: Array<{ kind: string; payloadJson: string }> = [];
	const readinessReports: Array<{ state: string; reason: string }> = [];
	const restartedHost = createMainSessionHost({
		supervisor: restartedSupervisor,
		identity: restarted.identity,
		state,
		journal: durableTestJournal(state, {
			journalAppend: (kind, payloadJson) => journal.push({ kind, payloadJson }),
			onTranscriptProjection: (kind, payloadJson) => journal.push({ kind, payloadJson }),
			setRpcHealth: (healthState, reason) => readinessReports.push({ state: healthState, reason }),
		}),
		initialTurnState: restarted.turnState,
		initialFollowUpQueueDepth: restarted.followUpQueueDepth,
		initialVerificationState: restarted.verificationState,
		...(restarted.growthIntent === undefined ? {} : { recoveredGrowthIntent: restarted.growthIntent }),
	});
	try {
		expect(restarted.recoveredGrowthIntent).toBe(true);
		expect(restarted.verificationState).toBe("pending");
		expect(restartedHost.mutationReadinessReason).toBe("transcript_verification_pending");
		await expect(restartedHost.admit("steer", "must stay fenced", "busy-restart-fenced")).rejects.toMatchObject({ reason: "transcript_verification_pending" });
		expect(restarted.turnState).toBe("busy");
		expect(state.read().growthIntent).toBeDefined();
		fixture.complete("busy-restart", { text: "busy turn settled after restart" });
		await eventually(
			() => journal.some(event => event.kind === "assistant_message" && event.payloadJson.includes("busy turn settled after restart")),
			"recovered busy turn did not deliver its terminal transcript",
		);
		await eventually(() => restartedHost.mutationReadinessReason === undefined, "restarted host did not promote its complete-tail verification");
		expect(readinessReports).toContainEqual({ state: "running", reason: "transcript_verified" });
		await eventually(() => state.read().growthIntent === undefined, "growth intent was not cleared after terminal delivery");
		expect(state.read().bootstrapState).toBe("COMMITTED");
	} finally {
		await restartedHost.dispose();
	}
});

test.serial("pending-proof persistence failure leaves delivery unadvanced, emits degraded health, and fails closed", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const meta = new MemoryGatewayMeta();
	const state = new GatewayStateStore(meta);
	const profile = fixtureProfile(fixture);
	fixture.timeoutNextTails(2);
	const adoption = supervisor(fixture);
	try {
		await bootstrapMainSession({ confirm: true, profile, state, supervisor: adoption, sessionId: fixture.sessionId });
	} finally {
		await adoption.dispose();
	}
	const resumedSupervisor = supervisor(fixture);
	const resumed = await strictResumeMainSession({ profile, state, supervisor: resumedSupervisor });
	const healthReports: Array<{ state: string; reason: string }> = [];
	meta.failNextGatewayMetaTransaction();
	const host = createMainSessionHost({
		supervisor: resumedSupervisor,
		identity: resumed.identity,
		state,
		journal: durableTestJournal(state, { setRpcHealth: (healthState, reason) => healthReports.push({ state: healthState, reason }) }),
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
		initialVerificationState: resumed.verificationState,
	});
	try {
		await eventually(() => host.mutationReadinessReason === "transcript_proof_persist_failed", "pending-proof persistence failure did not fence mutations");
		expect(state.read()).toMatchObject({
			bootstrapState: "FAILED_CLOSED",
			failedClosedReason: "transcript_proof_persist_failed",
			transcriptProof: "pending",
			transcriptDeliveryProgress: undefined,
		});
		expect(meta.events.some(event => event.kind === "transcript_delivery_gap")).toBe(false);
		expect(healthReports).toContainEqual({ state: "degraded", reason: "transcript_proof_persist_failed" });
		expect(fixture.commands()).toEqual([]);
		expect(fixture.admissionAttempts()).toEqual([]);
	} finally {
		await host.dispose();
	}
});

test.serial("a retention gap without a resync checkpoint fails closed without advancing delivery", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, committed } = await bootstrapFixture(fixture);
	const checkpoint = state.read().tailCheckpoint!;
	const deliveryBefore = state.read().transcriptDeliveryProgress;
	let sends = 0;
	const controlled: HostSupervisor = {
		async discover() {
			return { identity: committed.identity, transcriptEntries: [], initialRingCheckpoint: checkpoint, transcriptProof: "proven", turnState: "idle" as const, followUpQueueDepth: 0 };
		},
		async verify() {
			return { identity: committed.identity, transcriptEntries: [], initialRingCheckpoint: checkpoint, transcriptProof: "proven", turnState: "idle" as const, followUpQueueDepth: 0 };
		},
		async sendPrompt() {
			sends += 1;
			throw new Error("not used");
		},
		async sendSteer() {
			sends += 1;
			throw new Error("not used");
		},
		async followUp() {
			sends += 1;
			throw new Error("not used");
		},
		async operationStatus(opRef) {
			return { operationRef: opRef, status: "unknown", completed: false, detail: {} };
		},
		async tailEvents() {
			return {
				identity: committed.identity,
				transcriptEntries: [],
				events: [],
				terminal: true,
				complete: true,
				retentionGap: true,
				checkpoint,
			};
		},
		async turnState() {
			return { turnState: "idle" as const, followUpQueueDepth: 0 };
		},
		async dispose() {},
	};
	const healthReports: Array<{ state: string; reason: string }> = [];
	const host = createMainSessionHost({
		supervisor: controlled,
		identity: committed.identity,
		state,
		journal: durableTestJournal(state, { setRpcHealth: (healthState, reason) => healthReports.push({ state: healthState, reason }) }),
	});
	try {
		await eventually(() => host.mutationReadinessReason === "tail_resync_unavailable", "missing ring resync did not fence mutations");
		expect(state.read()).toMatchObject({ bootstrapState: "FAILED_CLOSED", failedClosedReason: "tail_resync_unavailable" });
		expect(state.read().transcriptDeliveryProgress).toEqual(deliveryBefore);
		expect(healthReports).toContainEqual({ state: "degraded", reason: "tail_resync_unavailable" });
		expect(sends).toBe(0);
	} finally {
		await host.dispose();
	}
});

test.serial("a ring-rotation metadata write failure fences mutations without advancing the ring or delivery", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { meta, state, committed } = await bootstrapFixture(fixture);
	const checkpoint = state.read().tailCheckpoint!;
	const resyncCheckpoint = { revision: checkpoint.revision + 1, generation: checkpoint.generation, seq: checkpoint.seq + 1 };
	const deliveryBefore = state.read().transcriptDeliveryProgress;
	let sends = 0;
	const controlled: HostSupervisor = {
		async discover() {
			return { identity: committed.identity, transcriptEntries: [], initialRingCheckpoint: checkpoint, transcriptProof: "proven", turnState: "idle" as const, followUpQueueDepth: 0 };
		},
		async verify() {
			return { identity: committed.identity, transcriptEntries: [], initialRingCheckpoint: checkpoint, transcriptProof: "proven", turnState: "idle" as const, followUpQueueDepth: 0 };
		},
		async sendPrompt() {
			sends += 1;
			throw new Error("not used");
		},
		async sendSteer() {
			sends += 1;
			throw new Error("not used");
		},
		async followUp() {
			sends += 1;
			throw new Error("not used");
		},
		async operationStatus(opRef) {
			return { operationRef: opRef, status: "unknown", completed: false, detail: {} };
		},
		async tailEvents() {
			return {
				identity: committed.identity,
				transcriptEntries: [],
				events: [],
				terminal: true,
				complete: true,
				retentionGap: true,
				checkpoint: resyncCheckpoint,
				resyncCheckpoint,
			};
		},
		async turnState() {
			return { turnState: "idle" as const, followUpQueueDepth: 0 };
		},
		async dispose() {},
	};
	const healthReports: Array<{ state: string; reason: string }> = [];
	meta.failNextGatewayMetaTransaction();
	const host = createMainSessionHost({
		supervisor: controlled,
		identity: committed.identity,
		state,
		journal: durableTestJournal(state, { setRpcHealth: (healthState, reason) => healthReports.push({ state: healthState, reason }) }),
	});
	try {
		await eventually(() => host.mutationReadinessReason === "tail_ring_rotation_write_failed", "ring-rotation write failure did not fence mutations");
		expect(state.read()).toMatchObject({ bootstrapState: "COMMITTED", tailCheckpoint: checkpoint, tailRingRotationCount: 0 });
		expect(state.read().transcriptDeliveryProgress).toEqual(deliveryBefore);
		expect(meta.events.some(event => event.kind === "tail_ring_rotation")).toBe(false);
		expect(healthReports).toContainEqual({ state: "degraded", reason: "tail_ring_rotation_write_failed" });
		expect(sends).toBe(0);
	} finally {
		await host.dispose();
	}
});

test.serial("a failed definitive-rejection claim abandonment degrades and fences the host without a resend", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	const resumedSupervisor = supervisor(fixture);
	const resumed = await strictResumeMainSession({ profile, state, supervisor: resumedSupervisor });
	const deliveryBefore = state.read().transcriptDeliveryProgress;
	const healthReports: Array<{ state: string; reason: string }> = [];
	const host = createMainSessionHost({
		supervisor: resumedSupervisor,
		identity: resumed.identity,
		state,
		journal: durableTestJournal(state, { setRpcHealth: (healthState, reason) => healthReports.push({ state: healthState, reason }) }),
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
		initialVerificationState: resumed.verificationState,
	});
	let pending = false;
	const idempotency = {
		mainAdmissionOperationClaim() {
			pending = true;
			return { claimed: true };
		},
		mainAdmissionOperationFinalize(input: { readonly responseJson: string }) {
			return { responseJson: input.responseJson };
		},
		mainAdmissionOperationRecordAttemptIds() {},
		mainAdmissionOperationAbandon() {
			throw new Error("scripted durable claim abandonment failure");
		},
		mainAdmissionOperationsPending() {
			return pending ? [{ scope: "main.submit", key: "claim-abandon-failure", requestJson: "{}", intentJson: "{}" }] : [];
		},
	};
	const submit = createMainAdmissionHandler(host, profile, idempotency);
	try {
		fixture.rejectNextTurn();
		await expect(
			submit({ text: "definitively reject then fail durable abandonment", surface_id: "owner", idempotency_key: "claim-abandon-failure" }),
		).rejects.toMatchObject({ reason: "main_admission_claim_abandon_failed" } satisfies Partial<MainAdmissionRecoveryError>);
		expect(host.mutationReadinessReason).toBe("main_admission_claim_abandon_failed");
		expect(healthReports).toContainEqual({ state: "degraded", reason: "main_admission_claim_abandon_failed" });
		expect(state.read().transcriptDeliveryProgress).toEqual(deliveryBefore);
		expect(idempotency.mainAdmissionOperationsPending()).toHaveLength(1);
		expect(fixture.admissionAttempts().filter(attempt => attempt.text === "definitively reject then fail durable abandonment")).toHaveLength(1);
		expect(fixture.commands()).toEqual([]);
	} finally {
		await host.dispose();
	}
});

test.serial("recovery clears an attested-growth identity mismatch while preserving the journal", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { meta, state, profile } = await bootstrapFixture(fixture);
	const before = state.read();
	// Autonomous growth, then a marker recorded the way the pre-G010 runtime did.
	fixture.appendTranscript({ type: "message", role: "user", content: "growth before the marker" });
	state.markFailedClosed("main_identity_mismatch");
	const journalRowsBefore = meta.events.length;
	const recovering = supervisor(fixture);
	try {
		const result = await recoverFailedClosedGateway({ profile, state, supervisor: recovering, confirm: true });
		expect(result.clearedReason).toBe("main_identity_mismatch");
		const after = state.read();
		expect(after.bootstrapState).toBe("COMMITTED");
		expect(after.failedClosedReason).toBeUndefined();
		// Durable history preserved: nothing was reset, only appended to.
		expect(meta.events.length).toBeGreaterThan(journalRowsBefore);
		expect(after.tailCheckpoint).toEqual(before.tailCheckpoint);
		expect(after.transcriptDeliveryProgress).toEqual(before.transcriptDeliveryProgress);
		const receipt = meta.events.filter(event => event.kind === "failed_closed_recovered");
		expect(receipt).toHaveLength(1);
		expect(JSON.parse(receipt[0]?.payloadJson ?? "{}")).toMatchObject({
			cleared_reason: "main_identity_mismatch",
			receipt_id: expect.any(String),
			verification_evidence: expect.stringContaining("append-only growth"),
		});
	} finally {
		await recovering.dispose();
	}
});

test.serial("recovery refuses a rewritten transcript prefix and leaves the gateway terminal", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	fixture.rotateTranscriptPast(`${fixture.sessionId}:transcript:1`);
	state.markFailedClosed("main_identity_mismatch");
	const recovering = supervisor(fixture);
	try {
		await expect(recoverFailedClosedGateway({ profile, state, supervisor: recovering, confirm: true })).rejects.toMatchObject({
			reason: "reason_not_recoverable",
		});
		expect(state.read()).toMatchObject({ bootstrapState: "FAILED_CLOSED", failedClosedReason: "main_identity_mismatch" });
	} finally {
		await recovering.dispose();
	}
});

test.serial("recovery refuses an unprovable admission outcome as terminal by classification", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	state.markFailedClosed("main_admission_recovery_unprovable");
	const recovering = supervisor(fixture);
	try {
		await expect(recoverFailedClosedGateway({ profile, state, supervisor: recovering, confirm: true })).rejects.toMatchObject({
			reason: "reason_not_recoverable",
		});
		expect(state.read()).toMatchObject({ bootstrapState: "FAILED_CLOSED", failedClosedReason: "main_admission_recovery_unprovable" });
	} finally {
		await recovering.dispose();
	}
});

test.serial("recovery redirects profile drift to the profile-approval ceremony", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	state.markFailedClosed("profile_drift");
	const recovering = supervisor(fixture);
	try {
		await expect(recoverFailedClosedGateway({ profile, state, supervisor: recovering, confirm: true })).rejects.toMatchObject({
			reason: "use_profile_approval",
		});
		expect(state.read()).toMatchObject({ bootstrapState: "FAILED_CLOSED", failedClosedReason: "profile_drift" });
	} finally {
		await recovering.dispose();
	}
});

test.serial("recovery refuses while the original transient condition still holds", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	state.markFailedClosed("session_unavailable");
	// verify() inspects the session rather than tailing, so unavailability must be
	// expressed as the session no longer being live.
	fixture.setLive(false);
	const recovering = supervisor(fixture);
	try {
		await expect(recoverFailedClosedGateway({ profile, state, supervisor: recovering, confirm: true })).rejects.toMatchObject({
			reason: "recovery_verification_unavailable",
		});
		expect(state.read()).toMatchObject({ bootstrapState: "FAILED_CLOSED", failedClosedReason: "session_unavailable" });
	} finally {
		await recovering.dispose();
	}
});

test.serial("recovery clears a transient broker reason once live verification succeeds", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	state.markFailedClosed("session_unavailable");
	const recovering = supervisor(fixture);
	try {
		const result = await recoverFailedClosedGateway({ profile, state, supervisor: recovering, confirm: true });
		expect(result.clearedReason).toBe("session_unavailable");
		expect(result.evidence).toContain("live broker verification");
		expect(state.read()).toMatchObject({ bootstrapState: "COMMITTED", failedClosedReason: undefined });
	} finally {
		await recovering.dispose();
	}
});

test.serial("recovery requires explicit confirmation", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	state.markFailedClosed("session_unavailable");
	const recovering = supervisor(fixture);
	try {
		await expect(recoverFailedClosedGateway({ profile, state, supervisor: recovering, confirm: false })).rejects.toMatchObject({
			reason: "recovery_not_confirmed",
		});
		expect(state.read().bootstrapState).toBe("FAILED_CLOSED");
	} finally {
		await recovering.dispose();
	}
});

test("recovery of a write-failure reason is refused when the durable store still rejects writes", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const meta = new MemoryGatewayMeta();
	const state = new GatewayStateStore(meta);
	const profile = fixtureProfile(fixture);
	const adoption = supervisor(fixture);
	try {
		await bootstrapMainSession({ confirm: true, profile, state, supervisor: adoption, sessionId: fixture.sessionId });
	} finally {
		await adoption.dispose();
	}
	state.markFailedClosed("transcript_delivery_progress_write_failed");
	// The clearing CAS transaction is the write proof: a store that still refuses
	// writes must fail recovery rather than reporting a cleared marker.
	meta.failNextGatewayMetaTransaction();
	const recovering = supervisor(fixture);
	try {
		await expect(recoverFailedClosedGateway({ profile, state, supervisor: recovering, confirm: true })).rejects.toMatchObject({
			reason: "recovery_write_failed",
		});
		expect(state.read()).toMatchObject({
			bootstrapState: "FAILED_CLOSED",
			failedClosedReason: "transcript_delivery_progress_write_failed",
		});
		expect(meta.events.some(event => event.kind === "failed_closed_recovered")).toBe(false);
	} finally {
		await recovering.dispose();
	}
});
