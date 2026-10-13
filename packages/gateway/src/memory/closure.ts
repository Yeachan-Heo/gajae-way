import { originKey, validateOriginRef } from "@gajae-gateway/protocol";
import type { Store } from "../store";
import { appendDaily, CorpusWriter, initializeMemory, MemoryGitLockedError, memoryGit } from "./doctrine";

export interface DailyCaptureMutation {
	readonly kind: "daily_capture" | "monitor-event";
	readonly originRefJson: string;
	readonly userText: string;
	readonly replyText: string;
}
export type MemoryMutation = DailyCaptureMutation;
type Intent = {
	id: string;
	kind: string;
	payload_json: string;
	state: "queued" | "written" | "committed" | "receipted" | "quarantined";
	attempts: number;
	quarantine_reason: string | null;
};
export type RecoveryReport = {
	queued: number;
	written: number;
	committed: number;
	receipted: number;
	quarantined: number;
};

export class MemoryClosureQueue {
	readonly #database: Store;
	readonly #home: string;
	#tail: Promise<void> = Promise.resolve();
	#depth = 0;
	/** Intents whose processing failed in this process and were left for boot recovery. */
	failures = 0;
	#initializing: Promise<RecoveryReport> | undefined;
	readonly recovery: RecoveryReport = { queued: 0, written: 0, committed: 0, receipted: 0, quarantined: 0 };
	/** Test hook: invoked after appendDaily, before commit (for #341 regression test). */
	readonly #afterWrite?: () => Promise<void>;

	constructor(database: Store, home: string, options?: { afterWrite?: () => Promise<void> }) {
		this.#database = database;
		this.#home = home;
		this.#afterWrite = options?.afterWrite;
	}

	get queueDepth(): number {
		return this.#depth;
	}

	initialize(): Promise<RecoveryReport> {
		return this.enqueueWork(() => this.#ensureInitialized());
	}

	/** Adds a memory writer transaction behind all previously queued intents and writes. */
	enqueueWork<T>(work: () => Promise<T>): Promise<T> {
		this.#depth++;
		const result = this.#tail.then(work);
		this.#tail = result.then(
			() => {
				this.#depth--;
			},
			() => {
				this.#depth--;
			},
		);
		return result;
	}

	#ensureInitialized(): Promise<RecoveryReport> {
		if (!this.#initializing) {
			// A failed initialization is not cached: now that a fault no longer exits
			// the process, a cached rejection would fail every later intent until restart.
			this.#initializing = this.#initialize().catch((error: unknown) => {
				this.#initializing = undefined;
				throw error;
			});
		}
		return this.#initializing;
	}

	async #initialize(): Promise<RecoveryReport> {
		await initializeMemory(this.#home);
		for (const intent of this.#database.memory.openRows()) {
			if (intent.state === "receipted") continue;
			if (intent.state === "quarantined") {
				const reason = intent.quarantine_reason ?? "reason unavailable (legacy quarantined intent)";
				if (!intent.quarantine_reason)
					this.#database.transaction((tx) => this.#database.memory.quarantine(tx, intent.id, reason));
				continue;
			}
			try {
				await this.#recover(intent);
			} catch (error) {
				if (error instanceof MemoryGitLockedError) {
					this.failures++;
					console.error(`memory intent ${intent.id} failed; left for boot recovery: ${error.name}: ${error.message}`);
					continue;
				}
				const reason = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
				this.#database.transaction((tx) => this.#database.memory.quarantine(tx, intent.id, reason));
				this.recovery.quarantined++;
				console.warn(
					`memory_intent_quarantined id=${this.#logField(intent.id)} kind=${this.#logField(intent.kind)} origin=${this.#originKey(intent)} reason=${this.#logField(reason)}`,
				);
			}
		}
		return this.recovery;
	}

	enqueue(mutation: MemoryMutation): string {
		const id = crypto.randomUUID();
		// The SQLite insert is the acceptance boundary; work starts only after it returns.
		this.#database.transaction((tx) =>
			this.#database.memory.createIntent(tx, { id, kind: mutation.kind, payloadJson: JSON.stringify(mutation) }),
		);
		this.#kill("after-intent");
		this.#schedule(id);
		return id;
	}

	/**
	 * Idempotent admission for intents whose DB row was already written
	 * atomically by another writer (e.g. `store.monitors.authorFenced` plus a memory-intent write):
	 * schedules the EXISTING intent for processing without inserting a
	 * duplicate row, so same-run closure still happens while the atomic crash
	 * boundary is preserved. Safe to call multiple times for the same id —
	 * each call re-reads current state and skips terminal intents.
	 */
	enqueueExistingId(id: string): void {
		this.#schedule(id);
	}

	/** Worker failures stay contained and retain non-terminal DB state for a later retry. */
	#schedule(id: string): void {
		const attemptsAtSchedule = this.#database.memory.get(id)?.attempts ?? 0;
		void this.enqueueWork(async () => {
			try {
				await this.#ensureInitialized();
				const intent = this.#database.memory.get(id);
				if (
					intent &&
					intent.attempts === attemptsAtSchedule &&
					intent.state !== "receipted" &&
					intent.state !== "quarantined"
				)
					await this.#process(intent);
			} catch (error) {
				this.failures++;
				const reason = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
				console.error(`memory intent ${id} failed; left for boot recovery: ${reason}`);
			}
		});
	}

	async drain(): Promise<void> {
		await this.#tail;
	}

	getWriter(root: string): CorpusWriter {
		return new CorpusWriter(root);
	}

	async #recover(intent: Intent): Promise<void> {
		if (intent.state === "queued") this.recovery.queued++;
		if (intent.state === "written") this.recovery.written++;
		if (intent.state === "committed") this.recovery.committed++;
		await this.#process(intent);
		this.recovery.receipted++;
	}

	async #process(intent: Intent): Promise<void> {
		this.#database.transaction((tx) => this.#database.memory.beginAttempt(tx, intent.id));
		const root = await initializeMemory(this.#home);
		const mutation = this.#parse(intent);
		let state = intent.state;

		// appendDaily regenerates the map; the enclosing FIFO task keeps its file
		// write and later commit together with all other memory writes.
		if (state === "queued") {
			await appendDaily(root, mutation.originRefJson, mutation.userText, mutation.replyText, intent.id);
			this.#database.transaction((tx) => this.#database.memory.update(tx, intent.id, "written"));
			state = "written";
			this.#kill("after-write");
			if (this.#afterWrite) await this.#afterWrite();
		}

		const existing = await this.#commitFor(root, intent.id);
		if (!existing) {
			if (state !== "written") throw new Error(`memory intent ${intent.id} lacks recoverable written evidence`);
			const writer = this.getWriter(root);
			const trailer = `Gajaeway-Mutation-Id: ${intent.id}`;
			await memoryGit(root, ["add", "--all", "."]);
			const intentCommit = await writer.commit("Memory mutation", trailer);

			// If nothing was staged, content must already be in HEAD (recovery case).
			if (!intentCommit) {
				const found = await writer.findCommitByTrailer(`intent-id: ${intent.id}`);
				if (!found) throw new Error(`memory intent ${intent.id} content not found`);
			}

			this.#database.transaction((tx) => this.#database.memory.update(tx, intent.id, "committed"));
			this.#kill("after-commit");
		}

		this.#database.transaction((tx) => this.#database.memory.update(tx, intent.id, "receipted"));
	}

	#parse(intent: Intent): DailyCaptureMutation {
		if (intent.kind !== "daily_capture" && intent.kind !== "monitor-event")
			throw new Error(`unsupported memory intent ${intent.kind}`);
		const value = JSON.parse(intent.payload_json) as DailyCaptureMutation;
		if (
			!value ||
			typeof value.originRefJson !== "string" ||
			typeof value.userText !== "string" ||
			typeof value.replyText !== "string"
		)
			throw new Error(`invalid memory intent ${intent.id}`);
		return value;
	}

	async #commitFor(root: string, id: string): Promise<string | undefined> {
		try {
			const output = await memoryGit(root, ["log", "--format=%H", "--grep", `Gajaeway-Mutation-Id: ${id}`]);
			return output.split("\n").find(Boolean);
		} catch (error) {
			if (error instanceof Error && error.message.includes("does not have any commits")) return undefined;
			throw error;
		}
	}

	#originKey(intent: Intent): string {
		try {
			const payload = JSON.parse(intent.payload_json) as { originRefJson?: unknown };
			if (typeof payload.originRefJson !== "string") return "unknown";
			return originKey(validateOriginRef(JSON.parse(payload.originRefJson) as never));
		} catch {
			return "unknown";
		}
	}

	#logField(value: string): string {
		return JSON.stringify(value).slice(1, -1);
	}

	#kill(point: string): void {
		if (process.env.GAJAEWAY_MEMORY_KILL_POINT === point) process.kill(process.pid, "SIGKILL");
	}
}
