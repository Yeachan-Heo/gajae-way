import * as fs from "node:fs";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { BrokerCli } from "../../src/broker/cli";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { approveProfile, previewProfileApproval } from "../../src/main-session/profile-approval";
import { GatewayStateStore } from "../../src/main-session/state";
import { createExternalHostSupervisor } from "../../src/main-session/supervisor";
import { loadWayProfile } from "../../src/profile";
import { FakeBrokerFixture, MemoryGatewayMeta } from "../helpers/main-session";

const fixtures: FakeBrokerFixture[] = [];

afterEach(() => {
	for (const fixture of fixtures.splice(0)) fixture.dispose();
});

function profilePath(fixture: FakeBrokerFixture, injection = '["SOUL.md", "USER.md"]'): string {
	const corpus = path.join(fixture.root, "corpus");
	fs.mkdirSync(corpus, { recursive: true });
	const output = path.join(fixture.root, "profile.toml");
	fs.writeFileSync(
		output,
		`[corpus]
path = "${corpus}"
workspace = "${fixture.workspace}"

[injection]
files = ${injection}

[main_session]
session_id = "${fixture.sessionId}"

[surfaces.owner]
id = "owner"
platform = "test"
kind = "dm"
`,
	);
	return output;
}

test("profile approval records an explicit external-session-bound projection change", async () => {
	const fixture = new FakeBrokerFixture();
	fixtures.push(fixture);
	const state = new GatewayStateStore(new MemoryGatewayMeta());
	const pathBefore = profilePath(fixture);
	const supervisor = createExternalHostSupervisor({
		broker: new BrokerCli({ executable: fixture.executable, environment: fixture.environment() }),
		workspace: fixture.workspace,
	});
	try {
		await bootstrapMainSession({
			confirm: true,
			profile: loadWayProfile(pathBefore),
			state,
			supervisor,
			sessionId: fixture.sessionId,
		});
		const changedPath = profilePath(fixture, '["USER.md", "SOUL.md"]');
		const changed = loadWayProfile(changedPath);
		const preview = previewProfileApproval(state, changed);
		expect(preview.changes).toContainEqual(expect.objectContaining({ path: "projection.injection_files" }));
		const approved = approveProfile(state, changed, true, { now: () => 123, receiptId: () => "approval-1" });
		expect(approved.receiptId).toBe("approval-1");
		expect(state.read()).toMatchObject({ bootstrapState: "COMMITTED", profileDigest: changed.digest.sha256 });
	} finally {
		await supervisor.dispose();
	}
});
