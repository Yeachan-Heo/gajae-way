import { appendFile, readFile } from "node:fs/promises";
import { join } from "node:path";
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
		if (!this.#initializing) this.#initializing = this.#initialize();
		return this.#initializing;
	}

	async #initialize(): Promise<RecoveryReport> {
		await initializeMemory(this.#home);
		for (const intent of this.#database.memoryIntentRows()) {
			if (intent.state === "receipted" || intent.state === "quarantined") continue;
			try {
				await this.#recover(intent);
			} catch {
				this.#database.memoryIntentUpdate(intent.id, "quarantined");
				this.recovery.quarantined++;
			}
		}
		return this.recovery;
	}

	enqueue(mutation: MemoryMutation): string {
		const id = crypto.randomUUID();
		// The SQLite insert is the acceptance boundary; work starts only after it returns.
		this.#database.memoryIntentCreate({ id, kind: mutation.kind, payloadJson: JSON.stringify(mutation) });
		this.#kill("after-intent");
		this.#depth++;
		this.#tail = this.#tail.then(async () => {
			try {
				await this.initialize();
				const intent = this.#database.memoryIntentRows().find((row) => row.id === id);
				if (intent && intent.state !== "receipted" && intent.state !== "quarantined") await this.#process(intent);
			} finally {
				this.#depth--;
			}
		});
		return id;
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
			await memoryGit(root, ["add", "MEMORY.md", "daily"]);
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

	#kill(point: string): void {
		if (process.env.GAJAEWAY_MEMORY_KILL_POINT === point) process.kill(process.pid, "SIGKILL");
	}
}
