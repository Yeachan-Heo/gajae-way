import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { expect, test } from "bun:test";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { strictResumeMainSession } from "../../src/main-session/resume";
import { createPublishedSdk, type PublishedSdkOptions } from "../../src/main-session/sdk";

import { GatewayStateStore } from "../../src/main-session/state";
import { loadWayProfile } from "../../src/profile";
import { MemoryGatewayMeta } from "../helpers/main-session";

const runIntegration = Bun.env.WAY_SDK_INTEGRATION === "1";

function fixtureModel(baseUrl: string): NonNullable<PublishedSdkOptions["model"]> {
	return {
		id: "way-bootstrap-fixture",
		name: "Way Bootstrap Fixture",
		provider: "openai",
		api: "openai-completions",
		baseUrl,
		reasoning: false,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 32_768,
		maxTokens: 1_024,
	};
}

function startFixtureModelServer(): { server: ReturnType<typeof Bun.serve>; requestCount: () => number } {
	let requests = 0;
	const server = Bun.serve({
		hostname: "127.0.0.1",
		port: 0,
		fetch(request) {
			if (new URL(request.url).pathname !== "/v1/chat/completions") return new Response("not found", { status: 404 });
			requests += 1;
			const events = [
				{
					id: "chatcmpl-way-bootstrap",
					object: "chat.completion.chunk",
					created: 0,
					model: "way-bootstrap-fixture",
					choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }],
				},
				{
					id: "chatcmpl-way-bootstrap",
					object: "chat.completion.chunk",
					created: 0,
					model: "way-bootstrap-fixture",
					choices: [{ index: 0, delta: { content: "bootstrap acknowledged" }, finish_reason: null }],
				},
				{
					id: "chatcmpl-way-bootstrap",
					object: "chat.completion.chunk",
					created: 0,
					model: "way-bootstrap-fixture",
					choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
				},
			];
			return new Response(`${events.map(event => `data: ${JSON.stringify(event)}`).join("\n\n")}\n\ndata: [DONE]\n\n`, {
				headers: { "content-type": "text/event-stream" },
			});
		},
	});
	return { server, requestCount: () => requests };
}

(runIntegration ? test : test.skip)("published SDK create and openExistingStrict resume path", async () => {
	const fixture = startFixtureModelServer();
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-sdk-integration-"));
	try {
		const corpus = path.join(root, "corpus");
		const workspace = path.join(root, "workspace");
		fs.mkdirSync(corpus);
		fs.mkdirSync(workspace);
		const profilePath = path.join(root, "profile.toml");
		fs.writeFileSync(
			profilePath,
			`[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = []

[surfaces.owner]
id = "sdk-integration-owner"
platform = "test"
kind = "dm"
`,
		);
		const profile = loadWayProfile(profilePath);
		const state = new GatewayStateStore(new MemoryGatewayMeta());
		const sdk = createPublishedSdk({
			model: fixtureModel(new URL("/v1", fixture.server.url).toString()),
			sessionDirectory: path.join(root, "sdk-sessions"),
		});
		const created = await bootstrapMainSession({ confirm: true, profile, state, sdk });
		const resumed = await strictResumeMainSession({ profile, state, sdk });
		expect(resumed.identity.sessionId).toBe(created.identity.sessionId);
		expect(fixture.requestCount()).toBeGreaterThan(0);
		await resumed.session.dispose();
	} finally {
		fixture.server.stop(true);
		fs.rmSync(root, { recursive: true, force: true });
	}
}, 180_000);
