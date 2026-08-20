import * as fs from "node:fs";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { BrokerCli } from "../../src/broker/cli";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { createMainSessionHost } from "../../src/main-session/host";
import { ResumeError, strictResumeMainSession } from "../../src/main-session/resume";
import { GatewayStateStore } from "../../src/main-session/state";
import { createExternalHostSupervisor, type HostSupervisor, type SupervisorTailEvents } from "../../src/main-session/supervisor";
import { loadWayProfile } from "../../src/profile";
import { FakeBrokerFixture, MemoryGatewayMeta } from "../helpers/main-session";

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
		commandTimeoutMs: 1_000,
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


async function bootstrapFixture(fixture: FakeBrokerFixture) {
	const state = new GatewayStateStore(new MemoryGatewayMeta());
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
		return { state, profile, committed };
	} finally {
		await adoption.dispose();
	}
}


test.serial("bootstrap adopts and persists the exact live external identity without creating a GJC session", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, committed } = await bootstrapFixture(fixture);

	expect(committed.identity).toMatchObject({
		version: 1,
		sessionId: fixture.sessionId,
		locator: { repo: fixture.workspace, stateRoot: path.join(fixture.workspace, ".gjc", "state") },
		transcript: { entryCount: 2, sha256: expect.stringMatching(/^[a-f0-9]{64}$/) },
	});
	expect(state.read()).toMatchObject({ bootstrapState: "COMMITTED", mainIdentity: { sessionId: fixture.sessionId } });
	expect(fixture.commands()).toEqual([]);
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
	const receipt = await broker.sendPrompt(fixture.sessionId, "fixture command", "fixture-op");
	expect(receipt).toMatchObject({ sessionId: fixture.sessionId, operation: "turn.prompt", operationRef: "fixture-op" });
	expect(await broker.turnStatus(fixture.sessionId, "fixture-op")).toMatchObject({ status: "terminal_ok", completed: true });
	const tail = await broker.tailSession(fixture.sessionId, { repo: fixture.workspace, allEvents: true, untilIdle: true, strict: true });
	expect(tail.items.filter(item => item.kind === "agent_start")).toHaveLength(2);
	expect(tail.items.map(item => item.kind)).toContain("message_end");
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
		journal: { journalAppend: () => undefined },
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
			return { identity: committed.identity, turnState: "idle", followUpQueueDepth: 0 };
		},
		async verify() {
			return { identity: committed.identity, turnState: "idle", followUpQueueDepth: 0 };
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
		journal: { journalAppend: () => undefined },
	});
	try {
		await eventually(() => tailCalls === 1, "host did not start its first tail poll");
		await host.admit("prompt", "admit while an old terminal tail is in flight", "stale-tail-admission");
		staleTail.resolve({
			identity: committed.identity,
			transcriptEntries: [],
			events: [],
			terminal: true,
			retentionGap: false,
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

test.serial("strict resume fails closed when an external transcript grew without a durable growth intent", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const { state, profile } = await bootstrapFixture(fixture);
	fixture.appendTranscript({ type: "message", role: "user", content: "out-of-band transcript growth" });
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

test.serial("host projects overlapping agent and turn lifecycle tail events into one journal attempt", async () => {
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
		journal: { journalAppend: (kind, payloadJson) => journal.push({ kind, payloadJson }) },
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
	});
	try {
		fixture.holdNextTurn();
		await host.admit("prompt", "deduplicate this attempt", "overlap-attempt");
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
			.toEqual({ attempt_id: `${fixture.sessionId}:overlap-attempt`, generation: 1, lineage: "main" });
		expect(JSON.parse(journal[1]?.payloadJson ?? "{}"))
			.toEqual(expect.objectContaining({ finalized: true, text: "one terminal response" }));
		expect(JSON.parse(journal[2]?.payloadJson ?? "{}"))
			.toEqual({ attempt_id: `${fixture.sessionId}:overlap-attempt`, generation: 1, lineage: "main" });
	} finally {
		await host.dispose();
	}
}, 45_000);

test.serial("host journals only finalized assistant messages from external tail events", async () => {
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
		journal: { journalAppend: (kind, payloadJson) => journal.push({ kind, payloadJson }) },
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
	});
	const finalMessage = {
		role: "assistant",
		content: [{ type: "text", text: "final external answer" }],
		responseId: "finalized-message-id",
		timestamp: 1_700_000_001_000,
	};
	try {
		fixture.appendTailEvent("message_update", {
			type: "message_update",
			message: { ...finalMessage, content: [{ type: "text", text: "partial external answer" }] },
		});
		fixture.appendTailEvent("message_end", { type: "message_end", message: finalMessage });
		fixture.appendTailEvent("message_end", { type: "message_end", message: finalMessage });
		await eventually(
			() => journal.filter(event => event.kind === "assistant_message").length === 1,
			"finalized assistant message was not projected",
			10_000,
		);

		const assistants = journal.filter(event => event.kind === "assistant_message");
		expect(assistants).toHaveLength(1);
		expect(JSON.parse(assistants[0]?.payloadJson ?? "{}"))
			.toEqual({ finalized: true, text: "final external answer", message_id: "finalized-message-id", timestamp: 1_700_000_001_000 });
		expect(journal.some(event => event.payloadJson.includes("partial external answer"))).toBe(false);
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
		journal: { journalAppend: (kind, payloadJson) => journal.push({ kind, payloadJson }) },
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
	});
	try {
		// Three consecutive CLI deaths (spawn pressure / nonzero exits), then healthy.
		fixture.crashNextTails(3);
		fixture.appendTailEvent("message_end", {
			type: "message_end",
			message: {
				role: "assistant",
				content: [{ type: "text", text: "survived transient transport failure" }],
				responseId: "transient-recovery-message",
				timestamp: 1_700_000_002_000,
			},
		});
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
		journal: {
			journalAppend: (kind, payloadJson) => journal.push({ kind, payloadJson }),
			setRpcHealth: (healthState, reason) => healthReports.push({ state: healthState, reason }),
		},
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
