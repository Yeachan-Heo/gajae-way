import * as fs from "node:fs";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { BrokerCli } from "../../src/broker/cli";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { createMainSessionHost } from "../../src/main-session/host";
import { ResumeError, strictResumeMainSession } from "../../src/main-session/resume";
import { GatewayStateStore } from "../../src/main-session/state";
import { createExternalHostSupervisor } from "../../src/main-session/supervisor";
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


test("bootstrap adopts and persists the exact live external identity without creating a GJC session", async () => {
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

test("strict resume fails closed when the exact adopted external session disappears", async () => {
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

test("scripted broker CLI fixture covers inspect, send, status, tail, and overlapping SDK lifecycle frames", async () => {
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


test("external identity disappearance is durable failed-closed host state, not a silent degraded continuation", async () => {
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
