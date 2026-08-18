import * as fs from "node:fs";
import type { ContextFile } from "./inject";
import type { SessionFingerprint } from "./state";

/** Marker persisted in the first bootstrap transcript entry and used for orphan recovery. */
export function bootstrapNonceMarker(nonce: string): string {
	return `[[way-bootstrap-nonce:${nonce}]]`;
}

export class StrictSdkOpenError extends Error {
	readonly reason: string;

	constructor(reason: string) {
		super(`SDK strict session open failed: ${reason}`);
		this.name = "StrictSdkOpenError";
		this.reason = reason;
	}
}

export interface HostedSdkGate {
	readonly gateId: string;
	readonly sessionId: string;
	readonly expiresAt?: number;
	readonly payload: unknown;
}

export type HostedSdkGateResolution = "resolved" | "already_resolved" | "expired" | "not_found" | "rejected";

export class HostedSdkGateError extends Error {
	readonly reason: "not_found" | "expired";

	constructor(reason: "not_found" | "expired") {
		super(reason);
		this.name = "HostedSdkGateError";
		this.reason = reason;
	}
}


export interface HostedSdkSession {
	readonly sessionFile: string;
	readonly sessionId: string;
	subscribe(listener: (event: unknown) => void): () => void;
	subscribeGates(listener: (gate: HostedSdkGate) => void): () => void;
	prompt(text: string): Promise<void>;
	steer(text: string): Promise<void>;
	followUp(text: string): Promise<void>;
	followUpQueueDepth(): number;
	answerGate(gateId: string, answer: unknown, idempotencyKey: string): Promise<HostedSdkGateResolution>;
	/** Sends the nonce-bearing first user message and waits for the SDK turn. */
	sendBootstrapMessage(nonce: string): Promise<void>;
	dispose(): Promise<void>;
}

export interface CreateSdkSessionInput {
	readonly workspace: string;
	readonly contextFiles: readonly ContextFile[];
}

export interface ResumeSdkSessionInput extends CreateSdkSessionInput {
	readonly identity: SessionFingerprint;
}

/**
 * The only SDK seam used by P3. Unit tests provide a deterministic file-backed
 * double here; production goes through the pinned published package below.
 */
export interface MainSessionSdk {
	createNew(input: CreateSdkSessionInput): Promise<HostedSdkSession>;
	openExistingStrict(input: ResumeSdkSessionInput): Promise<HostedSdkSession>;
	findBootstrapNonceCandidates(workspace: string, nonce: string): Promise<readonly string[]>;
}

type RealSdkModule = typeof import("@gajae-code/coding-agent");
type RealCreateAgentSessionOptions = NonNullable<Parameters<RealSdkModule["createAgentSession"]>[0]>;
type RealAgentSession = Awaited<ReturnType<RealSdkModule["createAgentSession"]>>["session"];
type RealSessionManager = NonNullable<RealCreateAgentSessionOptions["sessionManager"]>;

export interface PublishedSdkOptions {
	/** Optional explicit model for controlled embedding/integration environments. */
	readonly model?: RealCreateAgentSessionOptions["model"];
	/** Optional explicit transcript directory for controlled embedding environments. */
	readonly sessionDirectory?: string;
}



async function loadSdk(): Promise<RealSdkModule> {
	return await import("@gajae-code/coding-agent");
}


function requireSessionFile(session: { sessionFile?: string; sessionId: string }): string {
	if (!session.sessionFile) throw new Error("The SDK returned a non-file-backed session.");
	return session.sessionFile;
}

function adaptSession(session: RealAgentSession): HostedSdkSession {
	const sessionFile = requireSessionFile(session);
	return {
		sessionFile,
		sessionId: session.sessionId,
		subscribe: listener => session.subscribe(listener),
		subscribeGates: listener => {
			const emitter = session.getWorkflowGateEmitter();
			if (!emitter?.onGateEmitted) return () => undefined;
			return emitter.onGateEmitted(gate => {
				listener({ gateId: gate.gate_id, sessionId: session.sessionId, payload: gate });
			});
		},
		prompt: text => session.prompt(text),
		steer: text => session.steer(text),
		followUp: text => session.followUp(text),
		followUpQueueDepth: () => session.pendingMessageCounts.followUp,
		async answerGate(gateId, answer, idempotencyKey) {
			const emitter = session.getWorkflowGateEmitter();
			if (
				!emitter?.resolveGate ||
				!emitter.lookupCompletedResolution ||
				!emitter.prepareTerminalization ||
				!emitter.clearPreparedTerminalization
			) {
				throw new HostedSdkGateError("not_found");
			}
			const response = { gate_id: gateId, answer, idempotency_key: idempotencyKey };
			let completed = emitter.lookupCompletedResolution(response);
			if (completed.kind === "completed") return "already_resolved";
			if (completed.kind === "accepted_incomplete") {
				if (!emitter.recoverAcceptedGates) throw new HostedSdkGateError("not_found");
				await emitter.recoverAcceptedGates();
				completed = emitter.lookupCompletedResolution(response);
				if (completed.kind === "completed") return "already_resolved";
				if (completed.kind === "accepted_incomplete") throw new Error("Workflow gate resolution is uncertain.");
			}
			if (!emitter.prepareTerminalization(gateId, "not_published")) {
				if (emitter.listPendingGates?.().some(gate => gate.gate_id === gateId)) return "rejected";
				return "already_resolved";
			}
			try {
				const resolution = await emitter.resolveGate(response);
				if (resolution.status === "rejected") {
					emitter.clearPreparedTerminalization(gateId);
					return "rejected";
				}
				return "resolved";
			} catch (error) {
				const code = (error as { code?: unknown }).code;
				if (code === "unknown_gate") throw new HostedSdkGateError("not_found");
				if (code === "already_resolved") return "already_resolved";
				const stillPending = emitter.listPendingGates?.().some(gate => gate.gate_id === gateId) === true;
				if (stillPending) emitter.clearPreparedTerminalization(gateId);
				else emitter.quarantineGate?.(gateId);
				throw error;
			}
		},
		sendBootstrapMessage: nonce => session.prompt(bootstrapNonceMarker(nonce)),
		dispose: () => session.dispose(),
	};
}

function resumeIdentity(identity: SessionFingerprint): {
	canonicalPath: string;
	sessionId: string;
	dev: bigint;
	ino: bigint;
	nlink: bigint;
	size: number;
	mtimeMs: number;
	mtimeNs: bigint;
	ctimeNs: bigint;
	sha256: string;
} {
	return {
		canonicalPath: identity.canonicalPath,
		sessionId: identity.sessionId,
		dev: BigInt(identity.device),
		ino: BigInt(identity.inode),
		nlink: BigInt(identity.nlink),
		size: identity.size,
		mtimeMs: identity.mtimeMs,
		mtimeNs: BigInt(identity.mtimeNs),
		ctimeNs: BigInt(identity.ctimeNs),
		sha256: identity.sha256,
	};
}

async function createWithManager(
	sdk: RealSdkModule,
	workspace: string,
	contextFiles: readonly ContextFile[],
	sessionManager: RealSessionManager,
	options: PublishedSdkOptions,
): Promise<HostedSdkSession> {
	const created = await sdk.createAgentSession({
		cwd: workspace,
		hasUI: false,
		enableLsp: false,
		contextFiles: [...contextFiles],
		sessionManager,
		...(options.model ? { model: options.model } : {}),
	});
	return adaptSession(created.session);
}


/** Production adapter for @gajae-code/coding-agent@0.14.0. */
export function createPublishedSdk(options: PublishedSdkOptions = {}): MainSessionSdk {

	return {
		async createNew(input) {
			const sdk = await loadSdk();
			const manager = options.sessionDirectory
				? sdk.SessionManager.create(input.workspace, sdk.SessionManager.explicitDestination(options.sessionDirectory))
				: sdk.SessionManager.create(input.workspace);
			return await createWithManager(sdk, input.workspace, input.contextFiles, manager, options);


		},
		async openExistingStrict(input) {
			const sdk = await loadSdk();
			const opened = await sdk.SessionManager.openExistingStrict(
				resumeIdentity(input.identity),
				options.sessionDirectory ? sdk.SessionManager.explicitDestination(options.sessionDirectory) : undefined,
			);
			if (opened.kind !== "opened") throw new StrictSdkOpenError(opened.reason);
			return await createWithManager(sdk, input.workspace, input.contextFiles, opened.manager, options);
		},
		async findBootstrapNonceCandidates(workspace, nonce) {
			const sdk = await loadSdk();
			const marker = bootstrapNonceMarker(nonce);
			const sessions = await sdk.SessionManager.list(workspace);
			const matches: string[] = [];
			for (const session of sessions) {
				try {
					if (fs.readFileSync(session.path, "utf8").includes(marker)) matches.push(session.path);
				} catch {
					// Strict recovery only accepts files that can be fingerprinted after this scan.
				}
			}
			return matches;
		},
	};
}
