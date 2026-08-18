import * as fs from "node:fs";
import * as path from "node:path";
import {
	bootstrapNonceMarker,
	type CreateSdkSessionInput,
	type HostedSdkGate,
	type HostedSdkSession,
	type MainSessionSdk,
	type ResumeSdkSessionInput,
} from "../../src/main-session/sdk";
import {
	fingerprintSessionFile,
	sameFingerprint,
	type GatewayMetaBackend,
	type GatewayMetaReadOutput,
	type GatewayMetaTransactionInput,
	type GatewayMetaTransactionOutput,
} from "../../src/main-session/state";

const defaults: Record<string, string> = {
	bootstrap_state: "ABSENT",
	bootstrap_intent: "null",
	main_identity: "null",
	growth_intent: "null",
	profile_digest: "null",
	profile_digest_version: "0",
	profile_projection: "null",
	profile_tunables_revision: "0",
	profile_approved_at: "null",
	profile_approval_receipt: "null",
	failed_closed_reason: "null",
};

export class MemoryGatewayMeta implements GatewayMetaBackend {
	readonly values = new Map(Object.entries(defaults));
	readonly events: Array<{ kind: string; payloadJson: string }> = [];

	gatewayMetaRead(keys: readonly string[]): GatewayMetaReadOutput {
		return { entries: keys.map(key => ({ key, value: this.values.get(key) })) };
	}

	gatewayMetaTransaction(input: GatewayMetaTransactionInput): GatewayMetaTransactionOutput {
		for (const expected of input.expected) {
			if (this.values.get(expected.key) !== expected.value) return { applied: false };
		}
		for (const put of input.puts) this.values.set(put.key, put.value);
		for (const key of input.deletes) this.values.delete(key);
		if (input.eventKind && input.eventPayloadJson) {
			this.events.push({ kind: input.eventKind, payloadJson: input.eventPayloadJson });
			return { applied: true, cursor: `1:${this.events.length}` };
		}
		return { applied: true };
	}
}

interface DoubleSessionRecord {
	readonly id: string;
	readonly file: string;
	readonly listeners: Set<(event: unknown) => void>;
	readonly gateListeners: Set<(gate: HostedSdkGate) => void>;
	readonly gates: Map<string, { expiresAt?: number; state: "open" | "resolved" | "expired" }>;
	followUpQueueDepth: number;
}

export interface FileSdkDoubleOptions {
	/** Simulates the SDK's deferred initial JSONL flush for timeout coverage. */
	readonly persistBootstrapTranscript?: boolean;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function writeLine(file: string, value: unknown): void {
	fs.appendFileSync(file, `${JSON.stringify(value)}\n`);
}


/**
 * Deterministic file-backed SDK double. Like the published SDK, `createNew`
 * retains the initial session in memory and only creates JSONL after the first
 * nonce-bearing turn has an assistant response.
 */
export class FileSdkDouble implements MainSessionSdk {
	readonly openedIdentities: ResumeSdkSessionInput["identity"][] = [];
	readonly createdInputs: CreateSdkSessionInput[] = [];
	readonly createdSessionFiles: string[] = [];
	readonly #persistBootstrapTranscript: boolean;
	#nextId = 1;
	readonly #sessions = new Map<string, DoubleSessionRecord>();

	constructor(options: FileSdkDoubleOptions = {}) {
		this.#persistBootstrapTranscript = options.persistBootstrapTranscript ?? true;
	}

	async createNew(input: CreateSdkSessionInput): Promise<HostedSdkSession> {
		this.createdInputs.push(input);
		const directory = path.join(input.workspace, ".way-test-sessions");
		fs.mkdirSync(directory, { recursive: true });
		const canonicalDirectory = fs.realpathSync.native(directory);
		const id = `session-${this.#nextId++}`;
		const file = path.join(canonicalDirectory, `${id}.jsonl`);
		const record: DoubleSessionRecord = {
			id,
			file,
			listeners: new Set(),
			gateListeners: new Set(),
			gates: new Map(),
			followUpQueueDepth: 0,
		};
		this.#sessions.set(record.file, record);
		this.createdSessionFiles.push(record.file);
		return this.hosted(record);
	}

	async openExistingStrict(input: ResumeSdkSessionInput): Promise<HostedSdkSession> {
		const observed = fingerprintSessionFile(input.identity.canonicalPath);
		if (!sameFingerprint(observed, input.identity)) throw new Error("strict identity mismatch");
		this.openedIdentities.push(input.identity);
		const record = this.#sessions.get(input.identity.canonicalPath);
		if (!record) throw new Error("unknown strict session");
		return this.hosted(record);
	}

	async findBootstrapNonceCandidates(_workspace: string, nonce: string): Promise<readonly string[]> {
		const marker = bootstrapNonceMarker(nonce);
		return [...this.#sessions.values()]
			.filter(record => {
				try {
					return fs.readFileSync(record.file, "utf8").includes(marker);
				} catch {
					return false;
				}
			})
			.map(record => record.file);
	}

	async createOrphan(workspace: string, nonce: string): Promise<string> {
		const session = await this.createNew({ workspace, contextFiles: [] });
		await session.sendBootstrapMessage(nonce);
		return session.sessionFile;
	}

	appendRaw(file: string, value: unknown): void {
		writeLine(file, value);
	}

	emitEvent(file: string, event: unknown): void {
		const record = this.#sessions.get(file);
		if (!record) throw new Error("unknown strict session");
		this.emit(record, event);
	}

	openGate(file: string, gateId: string, options: { expiresAt?: number; payload?: unknown } = {}): void {
		const record = this.#sessions.get(file);
		if (!record) throw new Error("unknown strict session");
		record.gates.set(gateId, { expiresAt: options.expiresAt, state: "open" });
		const gate: HostedSdkGate = {
			gateId,
			sessionId: record.id,
			expiresAt: options.expiresAt,
			payload: options.payload ?? { type: "workflow_gate", gate_id: gateId, session_id: record.id },
		};
		for (const listener of [...record.gateListeners]) listener(gate);
	}

	private emit(record: DoubleSessionRecord, event: unknown): void {
		if (isRecord(event) && event.type === "turn_start" && record.followUpQueueDepth > 0) record.followUpQueueDepth -= 1;
		for (const listener of [...record.listeners]) listener(event);
	}

	private hosted(record: DoubleSessionRecord): HostedSdkSession {
		return {
			sessionFile: record.file,
			sessionId: record.id,
			subscribe: listener => {
				record.listeners.add(listener);
				return () => record.listeners.delete(listener);
			},
			subscribeGates: listener => {
				record.gateListeners.add(listener);
				return () => record.gateListeners.delete(listener);
			},
			prompt: async text => {
				if (!fs.existsSync(record.file)) throw new Error("session transcript has not persisted its first assistant message");
				this.emit(record, { type: "turn_start" });
				writeLine(record.file, { type: "message", role: "user", content: text });
				this.emit(record, { type: "message_update", assistantMessageEvent: { type: "text_delta", delta: "ack" } });
				this.emit(record, { type: "turn_end" });
			},
			steer: async text => {
				if (!fs.existsSync(record.file)) throw new Error("session transcript has not persisted its first assistant message");
				writeLine(record.file, { type: "message", role: "user", content: text, delivery: "steer" });
				this.emit(record, { type: "message_update", assistantMessageEvent: { type: "text_delta", delta: "steered" } });
			},
			followUp: async () => {
				record.followUpQueueDepth += 1;
			},
			followUpQueueDepth: () => record.followUpQueueDepth,
			answerGate: async (gateId, _answer, _idempotencyKey) => {
				const gate = record.gates.get(gateId);
				if (!gate) return "not_found";
				if (gate.state === "expired" || (gate.expiresAt !== undefined && gate.expiresAt <= Date.now())) {
					gate.state = "expired";
					return "expired";
				}
				if (gate.state === "resolved") return "already_resolved";
				gate.state = "resolved";
				this.emit(record, { type: "gate_resolved", gate_id: gateId, session_id: record.id });
				return "resolved";
			},
			sendBootstrapMessage: async nonce => {
				if (!this.#persistBootstrapTranscript) return;
				fs.writeFileSync(record.file, `${JSON.stringify({ type: "session", id: record.id })}\n`);
				writeLine(record.file, { type: "message", role: "user", content: bootstrapNonceMarker(nonce) });
				writeLine(record.file, { type: "message", role: "assistant", content: "bootstrap persisted" });
			},
			dispose: async () => undefined,
		};
	}
}
