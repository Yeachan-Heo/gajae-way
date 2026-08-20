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
	tail_checkpoint: "null",
	tail_ring_rotation_count: "0",
	transcript_delivery_gap_count: "0",
	transcript_delivery_progress: "null",
	transcript_proof: "pending",

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
	completionRequested?: { readonly failure?: boolean; readonly text?: string };
}

interface FixtureTranscriptEntry {
	readonly id: string;
	readonly payload: unknown;
}

interface FixtureTailCoordinate {
	readonly generation: number;
	readonly seq: number;
}

interface FixtureSession {
	row: Record<string, unknown>;
	metadata: Record<string, unknown>;
	context: { isStreaming: boolean; followupQueueDepth: number };
	transcript: FixtureTranscriptEntry[];
	events: Array<Record<string, unknown>>;
	operations: Record<string, FixtureOperation>;
	tailCheckpoint: FixtureTailCoordinate;
	nextGeneration: number;
	nextSequenceByGeneration: Record<string, number>;
	nextEventId: number;
	nextTranscriptId: number;
	gap?: {
		readonly code: "retention_gap";
		readonly missing?: { readonly from: number; readonly to: number };
		readonly resync?: { readonly revision: number; readonly generation: number; readonly seq: number };
	};
	retentionFloor?: FixtureTailCoordinate;

	holdNext?: boolean;
	failNext?: boolean;
	rejectNext?: { readonly code: string; readonly message: string };
	suppressNextReceipt?: boolean;
	responseText?: string;
	tailTimeoutWhileBusy?: boolean;
	rotateRingDuringNextCompletion?: boolean;
	clearRetentionGapAfterTail?: boolean;
	unavailableQueries?: string[];
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
	readonly noEnvelopeWhileBusy?: boolean;
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
				{ id: `${this.sessionId}:transcript:0`, payload: { type: "session", id: this.sessionId } },
				{
					id: `${this.sessionId}:transcript:1`,
					payload: { type: "message", role: "assistant", content: "operator-owned session ready", responseId: "bootstrap:assistant", timestamp: now },
				},
			],
			events: [],
			operations: {},
			tailCheckpoint: { generation: 1, seq: 0 },
			nextGeneration: 1,
			nextSequenceByGeneration: {},
			nextEventId: 0,
			nextTranscriptId: 2,

			responseText: options.responseText,
			tailTimeoutWhileBusy: options.noEnvelopeWhileBusy,
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

	/** Rotates retained lifecycle frames when the next held turn completes; the following tail consumes the one-shot gap. */
	rotateRingDuringNextCompletion(): void {
		this.update(session => {
			session.rotateRingDuringNextCompletion = true;
		});
	}

	/** Makes `tail --until-idle` return tail_timeout while the fixture is streaming. */
	setNoEnvelopeWhileBusy(enabled = true): void {
		this.update(session => {
			session.tailTimeoutWhileBusy = enabled;
		});
	}

	/** Makes one raw query unavailable so adoption snapshot failures stay testable. */
	setQueryUnavailable(query: "session.checkpoint" | "context.get", unavailable = true): void {
		this.update(session => {
			const unavailableQueries = new Set(session.unavailableQueries ?? []);
			if (unavailable) unavailableQueries.add(query);
			else unavailableQueries.delete(query);
			session.unavailableQueries = [...unavailableQueries];
		});
	}

	crashNextTails(count: number): void {
		this.update(session => {
			(session as { crashNextTailCount?: number }).crashNextTailCount = count;
		});
	}

	timeoutNextTails(count: number): void {
		this.update(session => {
			(session as { timeoutNextTailCount?: number }).timeoutNextTailCount = count;
		});
	}

	/** Allows a strict-resume tail before exhausting the host's retry budget. */
	crashTailsAfter(successfulTails: number, count: number): void {
		if (!Number.isSafeInteger(successfulTails) || successfulTails < 0 || !Number.isSafeInteger(count) || count < 0) {
			throw new Error("tail crash counts must be non-negative safe integers");
		}
		this.update(session => {
			(session as { crashTailAfterCount?: number }).crashTailAfterCount = successfulTails;
			(session as { crashNextTailCount?: number }).crashNextTailCount = count;
		});
	}

	failNextTurn(): void {
		this.update(session => {
			session.failNext = true;
		});
	}

	/** Makes the next admission receive an explicit broker `ok:false` rejection envelope. */
	rejectNextTurn(code = "broker_rejected", message = "fixture broker rejected the admission"): void {
		this.update(session => {
			session.rejectNext = { code, message };
		});
	}

	/** Accepts the next operation durably but exits before returning its receipt. */
	suppressNextAdmissionReceipt(): void {
		this.update(session => {
			session.suppressNextReceipt = true;
		});
	}

	setLive(live: boolean): void {
		this.update(session => {
			session.row.live = live;
		});
	}

	/** Forces a resynchronizable gap without inventing a broker cursor token. */
	setRetentionGap(): void {
		this.update(session => {
			const resync = session.retentionFloor ?? session.tailCheckpoint;
			session.gap = {
				code: "retention_gap",
				resync: { revision: session.transcript.length, generation: resync.generation, seq: resync.seq },
			};
		});
	}

	/** Simulates event-ring rotation through the supplied sequence in the current generation. */
	rotateTailThrough(sequence: number): void {
		if (!Number.isSafeInteger(sequence) || sequence < 0) throw new Error("rotation sequence must be a non-negative safe integer");
		this.update(session => {
			const candidate: FixtureTailCoordinate = {
				generation: session.tailCheckpoint.generation,
				seq: Math.min(sequence, session.tailCheckpoint.seq),
			};
			const existing = session.retentionFloor;
			const floor =
				existing && (existing.generation > candidate.generation || (existing.generation === candidate.generation && existing.seq >= candidate.seq))
					? existing
					: candidate;
			session.retentionFloor = floor;
			session.events = session.events.filter(event => {
				const generation = event.generation;
				const eventSequence = event.seq;
				return (
					typeof generation !== "number" ||
					typeof eventSequence !== "number" ||
					generation > floor.generation ||
					(generation === floor.generation && eventSequence > floor.seq)
				);
			});
			session.gap = undefined;
		});
	}

	/** Simulates transcript-window rotation while retaining only entries after the supplied stable id. */
	rotateTranscriptPast(entryId: string): void {
		this.update(session => {
			const index = session.transcript.findIndex(entry => entry.id === entryId);
			if (index < 0) throw new Error(`fixture transcript entry ${entryId} is not retained`);
			session.transcript = session.transcript.slice(index + 1);
		});
	}

	setLocator(locator: { repo?: string; stateRoot?: string }): void {
		this.update(session => {
			const current = session.row.locator as { repo: string; stateRoot: string };
			session.row.locator = { repo: locator.repo ?? current.repo, stateRoot: locator.stateRoot ?? current.stateRoot };
		});
	}

	/** Adds an externally observed transcript entry without routing it through gajaeway. */
	appendTranscript(entry: unknown): void {
		this.update(session => {
			const id = `${this.sessionId}:transcript:${session.nextTranscriptId}`;
			session.nextTranscriptId += 1;
			session.transcript.push({ id, payload: entry });
		});
	}

	/** Rewrites retained transcript evidence for prefix-attestation failure drills. */
	replaceTranscriptEntry(entryId: string, payload: unknown): void {
		this.update(session => {
			const index = session.transcript.findIndex(entry => entry.id === entryId);
			if (index < 0) throw new Error(`fixture transcript entry ${entryId} is not retained`);
			session.transcript[index] = { id: entryId, payload };
		});
	}

	/** Adds one broker-tail event for projection tests in the current ring generation. */
	appendTailEvent(kind: string, payload: Record<string, unknown>): void {
		this.update(session => {
			const generation = session.tailCheckpoint.generation;
			const key = String(generation);
			const seq = (session.nextSequenceByGeneration[key] ?? 0) + 1;
			session.nextSequenceByGeneration[key] = seq;
			session.nextEventId += 1;
			session.tailCheckpoint = { generation, seq };
			session.events.push({
				kind,
				id: `${this.sessionId}:${generation}:${seq}:${session.nextEventId}`,
				generation,
				seq,
				payload,
			});
		});
	}


	/** Requests completion; the fixture CLI performs the actual transition on its next broker command. */
	complete(opRef: string, options: { readonly failure?: boolean; readonly text?: string } = {}): void {
		this.update(session => {
			const operation = session.operations[opRef];
			if (!operation || operation.completed || operation.completionRequested) throw new Error(`fixture operation ${opRef} is not held`);
			operation.completionRequested = options;
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
