import * as fs from "node:fs";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { BrokerCli } from "../../src/broker/cli";
import { runWayConsole, type ConsoleTerminal } from "../../src/console/console";
import { createMainAdmissionHandler } from "../../src/main-session/admission";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { createMainSessionHost, type MainSessionHost } from "../../src/main-session/host";
import { strictResumeMainSession } from "../../src/main-session/resume";
import { GatewayStateStore } from "../../src/main-session/state";
import { createExternalHostSupervisor } from "../../src/main-session/supervisor";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { loadWayProfile } from "../../src/profile";
import { createRpcBridge, RpcBridgeException, type RpcBridgeHandler } from "../../src/rpc-bridge";
import { FakeBrokerFixture } from "../helpers/main-session";

class ScriptedTerminal implements ConsoleTerminal {
	readonly writes: string[] = [];
	readonly #lines: string[];
	#closed = false;

	constructor(lines: readonly string[]) {
		this.#lines = [...lines];
	}

	async writeTrusted(text: string): Promise<void> {
		this.writes.push(text);
	}

	async readLine(): Promise<string | undefined> {
		const next = this.#lines.shift();
		if (next === "/quit") await Bun.sleep(250);
		return next;
	}

	close(): void {
		this.#closed = true;
	}

	get closed(): boolean {
		return this.#closed;
	}
}

interface Gateway {
	readonly fixture: FakeBrokerFixture;
	readonly core: WayCoreHandle;
	readonly host: MainSessionHost;
	readonly stateDir: string;
	readonly profilePath: string;
	stop(): Promise<void>;
}

const gateways: Gateway[] = [];

afterEach(async () => {
	for (const gateway of gateways.splice(0)) await gateway.stop();
});

async function gateway(): Promise<Gateway> {
	const fixture = new FakeBrokerFixture();
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
session_id = "${fixture.sessionId}"

[surfaces.owner]
id = "owner"
platform = "test"
kind = "dm"
`,
	);
	const profile = loadWayProfile(profilePath);
	const stateDir = path.join(fixture.root, "state");
	const core = loadWayCore().WayCore.open(stateDir);
	const state = new GatewayStateStore(core);
	const supervisor = createExternalHostSupervisor({
		broker: new BrokerCli({ executable: fixture.executable, environment: fixture.environment() }),
		workspace: fixture.workspace,
	});
	await bootstrapMainSession({ confirm: true, profile, state, supervisor, sessionId: fixture.sessionId });
	const resumed = await strictResumeMainSession({ profile, state, supervisor });
	const host = createMainSessionHost({
		supervisor,
		identity: resumed.identity,
		state,
		journal: core,
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
	});
	const submit = createMainAdmissionHandler(host, profile, core, { newOpRef: () => "console-op" });
	const handler: RpcBridgeHandler = async (method, params) => {
		if (method === "main.submit") return await submit(params);
		if (method === "main.gate.answer") throw new RpcBridgeException(1103, "gate_answer_unsupported");
		throw new RpcBridgeException(-32601, `method not found: ${method}`);
	};
	core.startRpcServer(path.join(stateDir, "rpc.sock"), createRpcBridge(core, handler));
	core.setRpcHealth("running");
	const output: Gateway = {
		fixture,
		core,
		host,
		stateDir,
		profilePath,
		async stop() {
			await host.dispose();
			core.shutdownRpcServer();
			fixture.dispose();
		},
	};
	gateways.push(output);
	return output;
}

test("console consumes the UDS gateway surface and renders external finalized output after admission", async () => {
	const active = await gateway();
	const terminal = new ScriptedTerminal(["console external prompt", "/quit"]);
	await runWayConsole(
		{ stateDir: active.stateDir, profilePath: active.profilePath },
		[],
		{ terminal, idempotencyKey: () => "console-key", statusPollMs: 25, exitDrainMs: 500 },
	);
	const rendered = terminal.writes.join("");
	expect(rendered).toContain("Gateway cockpit ready");
	expect(rendered).toContain("Delivered as: prompt");
	expect(rendered).toContain("Assistant:\nack");
	expect(active.fixture.commands()).toEqual([expect.objectContaining({ operation: "turn.prompt", text: "console external prompt" })]);
	expect(active.core.consumerCursor("gajaeway-console")).not.toBe("1:0");
	expect(terminal.closed).toBe(true);
});
