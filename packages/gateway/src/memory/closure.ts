import { appendFile, readFile } from "node:fs/promises";
import { join } from "node:path";
import { originKey, validateOriginRef } from "@gajae-gateway/protocol";
import type { GatewayDatabase } from "../store/db";
import { appendDaily, initializeMemory, memoryGit } from "./doctrine";

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
	readonly #database: GatewayDatabase;
	readonly #home: string;
	#tail: Promise<void> = Promise.resolve();
	#depth = 0;
	/** Intents whose processing failed in this process and were left for boot recovery. */
	failures = 0;
	#initializing: Promise<RecoveryReport> | undefined;
	readonly recovery: RecoveryReport = { queued: 0, written: 0, committed: 0, receipted: 0, quarantined: 0 };

	constructor(database: GatewayDatabase, home: string) {
		this.#database = database;
		this.#home = home;
	}

	get queueDepth(): number {
		return this.#depth;
	}

	initialize(): Promise<RecoveryReport> {
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
		for (const intent of this.#database.memoryIntentRows()) {
			if (intent.state === "receipted") continue;
			if (intent.state === "quarantined") {
				const reason = intent.quarantine_reason ?? "reason unavailable (legacy quarantined intent)";
				if (!intent.quarantine_reason) this.#database.memoryIntentQuarantine(intent.id, reason);
				await this.#ensureQuarantineReceipt(intent.id, reason);
				continue;
			}
			try {
				await this.#recover(intent);
			} catch (error) {
				const reason = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
				this.#database.memoryIntentQuarantine(intent.id, reason);
				this.recovery.quarantined++;
				console.warn(
					`memory_intent_quarantined id=${this.#logField(intent.id)} kind=${this.#logField(intent.kind)} origin=${this.#originKey(intent)} reason=${this.#logField(reason)}`,
				);
				await this.#ensureQuarantineReceipt(intent.id, reason);
			}
		}
		return this.recovery;
	}

	enqueue(mutation: MemoryMutation): string {
		const id = crypto.randomUUID();
		// The SQLite insert is the acceptance boundary; work starts only after it returns.
		this.#database.memoryIntentCreate({ id, kind: mutation.kind, payloadJson: JSON.stringify(mutation) });
		this.#kill("after-intent");
		this.#schedule(id);
		return id;
	}

	/**
	 * Idempotent admission for intents whose DB row was already written
	 * atomically by another writer (e.g. monitorEventFencedAuthorWithIntent):
	 * schedules the EXISTING intent for processing without inserting a
	 * duplicate row, so same-run closure still happens while the atomic crash
	 * boundary is preserved. Safe to call multiple times for the same id —
	 * each call re-reads current state and skips terminal intents.
	 */
	enqueueExistingId(id: string): void {
		this.#schedule(id);
	}

	/**
	 * The worker boundary. A memory fault (a lost map rename #227, a failed git
	 * commit #192) must never escape: nothing awaits `#tail` until shutdown, so a
	 * rejection here was an unhandled rejection that exited the whole gateway, and
	 * a rejected tail would also have skipped every later intent. The failed
	 * intent keeps its non-terminal durable state, so boot recovery retries it.
	 */
	#schedule(id: string): void {
		this.#depth++;
		this.#tail = this.#tail.then(async () => {
			try {
				await this.initialize();
				const intent = this.#database.memoryIntentRows().find((row) => row.id === id);
				if (intent && intent.state !== "receipted" && intent.state !== "quarantined") await this.#process(intent);
			} catch (error) {
				this.failures++;
				console.error(
					`memory intent ${id} failed; left for boot recovery: ${error instanceof Error ? error.message : String(error)}`,
				);
			} finally {
				this.#depth--;
			}
		});
	}

	async drain(): Promise<void> {
		await this.#tail;
	}

	async #recover(intent: Intent): Promise<void> {
		if (intent.state === "queued") this.recovery.queued++;
		if (intent.state === "written") this.recovery.written++;
		if (intent.state === "committed") this.recovery.committed++;
		await this.#process(intent);
		this.recovery.receipted++;
	}

	async #process(intent: Intent): Promise<void> {
		this.#database.memoryIntentBeginAttempt(intent.id);
		const root = await initializeMemory(this.#home);
		const mutation = this.#parse(intent);
		let state = intent.state;
		if (state === "queued") {
			await appendDaily(root, mutation.originRefJson, mutation.userText, mutation.replyText);
			this.#database.memoryIntentUpdate(intent.id, "written");
			state = "written";
			this.#kill("after-write");
		}
		const existing = await this.#commitFor(root, intent.id);
		let commit = existing;
		if (!commit) {
			if (state !== "written") throw new Error(`memory intent ${intent.id} lacks recoverable written evidence`);
			// Stage the whole corpus, not just the capture axis. Every registered axis —
			// built-in, custom, or an axis a human curated by hand — is memory, so a
			// reflection or an ops rule written between two captures would otherwise stay
			// permanently untracked and drop out of the Git history the doctrine promises
			// to review. Naming axes here would also silently miss any axis added later.
			await memoryGit(root, ["add", "--all", "."]);
			await memoryGit(root, ["commit", "-m", `Memory mutation\n\nGajaeway-Mutation-Id: ${intent.id}`]);
			commit = await this.#commitFor(root, intent.id);
			if (!commit) throw new Error(`memory intent ${intent.id} commit trailer missing`);
			this.#database.memoryIntentUpdate(intent.id, "committed");
			state = "committed";
			this.#kill("after-commit");
		}
		if (state !== "receipted") {
			if (!(await this.#hasReceipt(intent.id))) {
				await appendFile(
					join(this.#home, "memory-receipts.jsonl"),
					`${JSON.stringify({ id: intent.id, commit, at: new Date().toISOString() })}\n`,
					"utf8",
				);
			}
			this.#database.memoryIntentUpdate(intent.id, "receipted");
		}
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

	async #hasReceipt(id: string): Promise<boolean> {
		try {
			return (await readFile(join(this.#home, "memory-receipts.jsonl"), "utf8"))
				.split("\n")
				.some((line) => line && (JSON.parse(line) as { id?: string }).id === id);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
			throw error;
		}
	}

	async #ensureQuarantineReceipt(id: string, reason: string): Promise<void> {
		if (await this.#hasQuarantineReceipt(id)) return;
		await appendFile(
			join(this.#home, "memory-receipts.jsonl"),
			`${JSON.stringify({ id, state: "quarantined", reason, at: new Date().toISOString() })}\n`,
			"utf8",
		);
	}

	async #hasQuarantineReceipt(id: string): Promise<boolean> {
		try {
			return (await readFile(join(this.#home, "memory-receipts.jsonl"), "utf8")).split("\n").some((line) => {
				if (!line) return false;
				const receipt = JSON.parse(line) as { id?: string; state?: string };
				return receipt.id === id && receipt.state === "quarantined";
			});
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
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
