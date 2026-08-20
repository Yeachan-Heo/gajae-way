import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type {
	GatewayMetaBackend,
	GatewayMetaReadOutput,
	GatewayMetaTransactionInput,
	GatewayMetaTransactionOutput,
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

interface FixtureOperation {
	readonly opRef: string;
	readonly operation: "turn.prompt" | "turn.steer" | "turn.follow_up";
	readonly text: string;
	failure?: boolean;
	responseText?: string;
	generation?: number;
	completed?: boolean;
}

interface FixtureSession {
	row: Record<string, unknown>;
	metadata: Record<string, unknown>;
	context: { isStreaming: boolean; followupQueueDepth: number };
	transcript: unknown[];
	events: Array<Record<string, unknown>>;
	operations: Record<string, FixtureOperation>;
	nextSeq: number;
	nextGeneration: number;
	gap?: { readonly code: "retention_gap"; readonly missing?: { readonly from: number; readonly to: number } };
	holdNext?: boolean;
	failNext?: boolean;
	responseText?: string;
	commandLog: Array<Record<string, unknown>>;
}

interface FixtureState {
	indexSeq: number;
	sessions: Record<string, FixtureSession>;
}

export interface FakeBrokerFixtureOptions {
	readonly workspace?: string;
	readonly sessionId?: string;
	readonly live?: boolean;
	readonly kind?: string;
	readonly responseText?: string;
}

/**
 * Deterministic external-session fixture consumed through the real spawn-only
 * `BrokerCli`. It never mirrors a HostSupervisor in-process: all tests cross
 * the same command envelopes production uses.
 */
export class FakeBrokerFixture {
	readonly root: string;
	readonly statePath: string;
	readonly executable: string;
	readonly workspace: string;
	readonly sessionId: string;

	constructor(options: FakeBrokerFixtureOptions = {}) {
		this.root = fs.mkdtempSync(path.join(os.tmpdir(), "gajaeway-broker-fixture-"));
		const workspace = path.resolve(options.workspace ?? path.join(this.root, "workspace"));
		fs.mkdirSync(workspace, { recursive: true });
		this.workspace = fs.realpathSync.native(workspace);
		this.sessionId = options.sessionId ?? "external-main-session";
		this.statePath = path.join(this.root, "broker-state.json");
		this.executable = path.resolve(import.meta.dir, "../fixtures/fake-broker-cli.mjs");
		const now = 1_700_000_000_000;
		const session: FixtureSession = {
			row: {
				sessionId: this.sessionId,
				locator: { repo: this.workspace, stateRoot: path.join(this.workspace, ".gjc", "state") },
				endpointGeneration: 1,
				pid: process.pid,
				live: options.live ?? true,
				deleted: false,
				indexSeq: 1,
				hostIncarnation: "fixture:1",
				activity: { state: "idle", at: now },
				lastHeartbeatAt: now,
				identityProvenance: "composite",
			},
			metadata: { sessionId: this.sessionId, name: "fixture-main", cwd: this.workspace, kind: options.kind ?? "main" },
			context: { isStreaming: false, followupQueueDepth: 0 },
			transcript: [
				{ type: "session", id: this.sessionId },
				{ type: "message", role: "assistant", content: "operator-owned session ready", responseId: "bootstrap:assistant", timestamp: now },
			],
			events: [],
			operations: {},
			nextSeq: 0,
			nextGeneration: 1,
			responseText: options.responseText,
			commandLog: [],
		};
		this.write({ indexSeq: 1, sessions: { [this.sessionId]: session } });
	}

	environment(base: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv {
		return { ...base, GAJAEWAY_BROKER_FIXTURE_STATE: this.statePath };
	}

	read(): FixtureState {
		return JSON.parse(fs.readFileSync(this.statePath, "utf8")) as FixtureState;
	}

	update(mutator: (session: FixtureSession, state: FixtureState) => void): void {
		const state = this.read();
		const session = state.sessions[this.sessionId];
		if (!session) throw new Error("fixture session disappeared");
		mutator(session, state);
		this.write(state);
	}

	holdNextTurn(): void {
		this.update(session => {
			session.holdNext = true;
		});
	}

	failNextTurn(): void {
		this.update(session => {
			session.failNext = true;
		});
	}

	setLive(live: boolean): void {
		this.update(session => {
			session.row.live = live;
		});
	}

	setRetentionGap(): void {
		this.update(session => {
			session.gap = { code: "retention_gap" };
		});
	}

	setLocator(locator: { repo?: string; stateRoot?: string }): void {
		this.update(session => {
			const current = session.row.locator as { repo: string; stateRoot: string };
			session.row.locator = { repo: locator.repo ?? current.repo, stateRoot: locator.stateRoot ?? current.stateRoot };
		});
	}

	complete(opRef: string, options: { readonly failure?: boolean; readonly text?: string } = {}): void {
		this.update(session => {
			const operation = session.operations[opRef];
			if (!operation || operation.completed) throw new Error(`fixture operation ${opRef} is not held`);
			operation.completed = true;
			if (options.failure) operation.failure = true;
			if (options.text !== undefined) operation.responseText = options.text;
			const scope = { attemptId: `${this.sessionId}:${opRef}`, generation: operation.generation ?? 1, lineage: "main" };
			const append = (kind: string, payload: Record<string, unknown>) => {
				session.nextSeq += 1;
				session.events.push({ kind, id: `${this.sessionId}:${session.nextSeq}`, generation: 1, seq: session.nextSeq, payload });
			};
			if (operation.failure) {
				append("agent_failed", {
					type: "agent_failed",
					sessionId: this.sessionId,
					error: { code: "fixture_failure", message: "fixture injected turn failure" },
					scope,
				});
			} else {
				const text = operation.responseText ?? session.responseText ?? (operation.operation === "turn.steer" ? "steered" : "ack");
				const timestamp = 1_700_000_000_000 + session.nextSeq;
				const responseId = `${this.sessionId}:assistant:${opRef}`;
				const message = { role: "assistant", content: [{ type: "text", text }], responseId, timestamp };
				session.transcript.push({ type: "message", role: "assistant", content: text, responseId, timestamp });
				append("message_end", { type: "message_end", message, scope });
				append("turn_end", { type: "turn_end", message, toolResults: [], scope });
				append("agent_end", { type: "agent_end", messages: [message], stopReason: "completed", scope });
			}
			if (!operation.failure && operation.operation !== "turn.follow_up") {
				for (const queued of Object.values(session.operations)) {
					if (queued.operation !== "turn.follow_up" || queued.completed) continue;
					queued.completed = true;
					const queuedScope = {
						attemptId: `${this.sessionId}:${queued.opRef}`,
						generation: queued.generation ?? 1,
						lineage: "main",
					};
					append("agent_start", { type: "agent_start", sessionId: this.sessionId, scope: queuedScope });
					append("turn_start", { type: "turn_start", sessionId: this.sessionId, scope: queuedScope });
					append("agent_start", { type: "agent_start", sessionId: this.sessionId, scope: queuedScope });
					const queuedText = queued.responseText ?? session.responseText ?? "ack";
					const queuedTimestamp = 1_700_000_000_000 + session.nextSeq;
					const queuedResponseId = `${this.sessionId}:assistant:${queued.opRef}`;
					const queuedMessage = {
						role: "assistant",
						content: [{ type: "text", text: queuedText }],
						responseId: queuedResponseId,
						timestamp: queuedTimestamp,
					};
					session.transcript.push({ type: "message", role: "user", content: queued.text, delivery: queued.operation });
					session.transcript.push({ type: "message", role: "assistant", content: queuedText, responseId: queuedResponseId, timestamp: queuedTimestamp });
					append("message_end", { type: "message_end", message: queuedMessage, scope: queuedScope });
					append("turn_end", { type: "turn_end", message: queuedMessage, toolResults: [], scope: queuedScope });
					append("agent_end", { type: "agent_end", messages: [queuedMessage], stopReason: "completed", scope: queuedScope });
				}
			}
			session.context.isStreaming = false;
			session.context.followupQueueDepth = 0;
		});
	}

	commands(): readonly Record<string, unknown>[] {
		return this.read().sessions[this.sessionId]?.commandLog ?? [];
	}

	dispose(): void {
		fs.rmSync(this.root, { recursive: true, force: true });
	}

	private write(state: FixtureState): void {
		fs.writeFileSync(this.statePath, `${JSON.stringify(state)}\n`);
	}
}
