import * as fs from "node:fs";
import * as path from "node:path";
import { BrokerCli } from "../../src/broker/cli";
import { createMainAdmissionHandler } from "../../src/main-session/admission";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { createMainGateAnswerHandler } from "../../src/main-session/gates";
import {
	createMainSessionHost,
	parseMainSessionAdmissionAttributions,
	type MainSessionHost,
	type MainSessionJournal,
} from "../../src/main-session/host";
import { strictResumeMainSession } from "../../src/main-session/resume";
import { GatewayStateStore } from "../../src/main-session/state";
import { GatewayToolController, GatewayToolError } from "../../src/main-session/tools";
import { createExternalHostSupervisor, type ExternalHostSupervisor } from "../../src/main-session/supervisor";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { loadWayProfile, type OwnerSurface, type WayProfile } from "../../src/profile";
import { createRpcBridge, RpcBridgeException, type RpcBridgeHandler } from "../../src/rpc-bridge";
import { RpcClient } from "./rpc-client";
import { FakeBrokerFixture } from "./main-session";

export interface ExternalGatewaySurface {
	readonly id: string;
	readonly platform: string;
	readonly kind: string;
	/** Declared redaction class; defaults to `conversation` for known surfaces. */
	readonly sessionKind?: string;
}

export interface ExternalGatewayOptions {
	readonly fixture?: FakeBrokerFixture;
	readonly ownerSurface?: ExternalGatewaySurface;
	readonly knownSurfaces?: readonly ExternalGatewaySurface[];
	readonly journal?: MainSessionJournal;
	readonly isSurfaceQuarantined?: (surface: OwnerSurface) => boolean;
	readonly newOpRef?: () => string;
	readonly afterBrokerAcceptedBeforeFinalize?: () => void | Promise<void>;
	readonly tailTimeoutMs?: number;
	readonly adoptionTailTimeoutMs?: number;
	readonly commandTimeoutMs?: number;
}

export interface ExternalGateway {
	readonly fixture: FakeBrokerFixture;
	readonly core: WayCoreHandle;
	readonly state: GatewayStateStore;
	readonly profile: WayProfile;
	readonly profilePath: string;
	readonly stateDirectory: string;
	readonly socketPath: string;
	readonly supervisor: ExternalHostSupervisor;
	readonly host: MainSessionHost;
	readonly client: RpcClient;
	stop(): Promise<void>;
}

function profileToml(
	fixture: FakeBrokerFixture,
	ownerSurface: ExternalGatewaySurface,
	knownSurfaces: readonly ExternalGatewaySurface[],
): string {
	const known = knownSurfaces
		.map(
			surface => `\n[[surfaces.known]]\nid = "${surface.id}"\nplatform = "${surface.platform}"\nkind = "${surface.kind}"\nsession_kind = "${surface.sessionKind ?? "conversation"}"\n`,
		)
		.join("");
	return `[corpus]\npath = "${path.join(fixture.root, "corpus")}"\nworkspace = "${fixture.workspace}"\n\n[injection]\nfiles = []\n\n[main_session]\nsession_id = "${fixture.sessionId}"\n\n[surfaces.owner]\nid = "${ownerSurface.id}"\nplatform = "${ownerSurface.platform}"\nkind = "${ownerSurface.kind}"\nsession_kind = "main"\n${known}`;
}

export async function connectEventually(socketPath: string): Promise<RpcClient> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (fs.existsSync(socketPath)) {
			try {
				return await RpcClient.connect(socketPath);
			} catch {
				// The native listener can exist before it begins accepting connections.
			}
		}
		await Bun.sleep(10);
	}
	throw new Error(`RPC socket did not become available: ${socketPath}`);
}

export async function eventually<T>(read: () => T | undefined, message: string, timeoutMs = 20_000): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		const value = read();
		if (value !== undefined) return value;
		if (Date.now() >= deadline) throw new Error(message);
		await Bun.sleep(10);
	}
}

/** Starts a full external-session gateway through the production broker CLI boundary. */
export async function createExternalGateway(options: ExternalGatewayOptions = {}): Promise<ExternalGateway> {
	const fixture = options.fixture ?? new FakeBrokerFixture();
	const ownerSurface = options.ownerSurface ?? { id: "owner", platform: "test", kind: "dm", sessionKind: "main" };
	const knownSurfaces = options.knownSurfaces ?? [];
	const corpus = path.join(fixture.root, "corpus");
	fs.mkdirSync(corpus, { recursive: true });
	const profilePath = path.join(fixture.root, "profile.toml");
	fs.writeFileSync(profilePath, profileToml(fixture, ownerSurface, knownSurfaces));
	const profile = loadWayProfile(profilePath);
	const stateDirectory = path.join(fixture.root, "state");
	const core = loadWayCore().WayCore.open(stateDirectory);
	core.registryConfigureSurfaces(
		profile.knownSurfaces.map(surface => ({
			surfaceId: surface.id,
			platform: surface.platform,
			kind: surface.kind,
			isOwnerSurface: profile.ownerSurfaces.some(owner => owner.id === surface.id),
		})),
	);
	const state = new GatewayStateStore(core);
	const supervisor = createExternalHostSupervisor({
		broker: new BrokerCli({ executable: fixture.executable, environment: fixture.environment() }),
		workspace: fixture.workspace,
		adoptionTailTimeoutMs: options.adoptionTailTimeoutMs ?? options.tailTimeoutMs ?? 500,
		tailTimeoutMs: options.tailTimeoutMs ?? 500,
		// External tests spawn a real fixture CLI for every broker operation; 1s is below cold child-process startup under contention.
		commandTimeoutMs: options.commandTimeoutMs ?? 5_000,
	});
	await bootstrapMainSession({ confirm: true, profile, state, supervisor, sessionId: fixture.sessionId });
	const resumed = await strictResumeMainSession({ profile, state, supervisor });
	const journal: MainSessionJournal =
		options.journal ??
		{
			journalAppend: (kind, payloadJson) => core.journalAppend(kind, payloadJson),
			journalAppendAtTailCheckpoint: (kind, payloadJson, expected, checkpoint) => {
				state.appendTailProjection(expected, checkpoint, kind, payloadJson);
			},
			journalAppendTranscriptProjection: (kind, payloadJson, expectedTail, checkpoint, expectedDelivery, nextDelivery) => {
				state.appendTranscriptProjection(expectedTail, checkpoint, expectedDelivery, nextDelivery, kind, payloadJson);
			},
			setRpcHealth: (healthState, reason) => core.setRpcHealth(healthState, reason),
			setMainSessionStatus: (turnState, followUpQueueDepth, verificationState) => core.setMainSessionStatus(turnState, followUpQueueDepth, verificationState),
			setJournalDegraded: degraded => core.setJournalDegraded(degraded),
		};

	const host = createMainSessionHost({
		supervisor,
		identity: resumed.identity,
		state,
		journal,
		initialTurnState: resumed.turnState,
		initialFollowUpQueueDepth: resumed.followUpQueueDepth,
		initialVerificationState: resumed.verificationState,
		...(resumed.verificationTail === undefined ? {} : { verificationTail: resumed.verificationTail }),
		...(resumed.growthIntent === undefined ? {} : { recoveredGrowthIntent: resumed.growthIntent }),
		initialAdmissionAttributions: parseMainSessionAdmissionAttributions(core.mainAdmissionAttributions()),
	});
	const gatewayTools = new GatewayToolController({ core, profile, host });
	const { submitFromRpc: submit } = createMainAdmissionHandler(host, profile, core, {
		newOpRef: options.newOpRef,
		isSurfaceQuarantined: options.isSurfaceQuarantined,
		afterBrokerAcceptedBeforeFinalize: options.afterBrokerAcceptedBeforeFinalize,
		mutationReadinessReason: () => host.mutationReadinessReason,
	});
	const answer = createMainGateAnswerHandler(host, core);
	const callGatewayTool = async <T>(call: () => T | Promise<T>): Promise<T> => {
		try {
			return await call();
		} catch (error) {
			if (error instanceof GatewayToolError) throw new RpcBridgeException(error.code, error.message, error.data);
			throw error;
		}
	};
	const handler: RpcBridgeHandler = async (method, params) => {
		if (method === "main.submit") return await submit(params);
		if (method === "main.say") return await callGatewayTool(() => gatewayTools.say(params));
		if (method === "main.turn.origin") return await callGatewayTool(() => gatewayTools.turnOrigin());
		if (method === "main.surfaces") return await callGatewayTool(() => gatewayTools.listSurfaces());
		if (method === "main.gate.answer") return await answer(params);
		throw new RpcBridgeException(-32601, `method not found: ${method}`);
	};
	const socketPath = path.join(stateDirectory, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core, handler));
	core.setRpcHealth("running");
	const client = await connectEventually(socketPath);
	let stopped = false;
	return {
		fixture,
		core,
		state,
		profile,
		profilePath,
		stateDirectory,
		socketPath,
		supervisor,
		host,
		client,
		async stop(): Promise<void> {
			if (stopped) return;
			stopped = true;
			client.close();
			await host.dispose();
			core.shutdownRpcServer();
			fixture.dispose();
		},
	};
}
