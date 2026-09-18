import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { type OriginRef, originKey } from "@gajae-gateway/protocol";

interface PersistedRoute {
	readonly chatId: string;
	readonly messageThreadId?: number;
}

interface PersistedState {
	readonly updateId?: number;
	readonly topics?: Readonly<Record<string, { readonly parentId: string; readonly messageThreadId: number }>>;
	readonly routes?: Readonly<Record<string, PersistedRoute>>;
}

export interface TelegramReplyRoute extends PersistedRoute {}

/** Durable data Telegram cannot resolve again through its Bot API after restart. */
export class TelegramAdapterState {
	#updateId: number | undefined;
	#topics = new Map<string, { parentId: string; messageThreadId: number }>();
	#routes = new Map<string, TelegramReplyRoute>();

	private constructor(readonly path: string) {}

	static async load(home: string): Promise<TelegramAdapterState> {
		const state = new TelegramAdapterState(join(home, "adapter-telegram-state.json"));
		try {
			const parsed = JSON.parse(await readFile(state.path, "utf8")) as PersistedState;
			if (Number.isSafeInteger(parsed.updateId) && (parsed.updateId as number) >= 0) state.#updateId = parsed.updateId;
			for (const [key, topic] of Object.entries(parsed.topics ?? {})) {
				if (typeof topic.parentId === "string" && Number.isSafeInteger(topic.messageThreadId))
					state.#topics.set(key, { ...topic });
			}
			for (const [key, route] of Object.entries(parsed.routes ?? {})) {
				if (
					typeof route.chatId === "string" &&
					(route.messageThreadId === undefined || Number.isSafeInteger(route.messageThreadId))
				)
					state.#routes.set(key, { ...route });
			}
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT")
				throw new Error(`Unable to read Telegram adapter state at ${state.path}`);
		}
		return state;
	}

	get updateId(): number | undefined {
		return this.#updateId;
	}

	async acceptUpdate(updateId: number): Promise<boolean> {
		if (!Number.isSafeInteger(updateId)) throw new Error("Telegram update_id must be a safe integer");
		if (this.#updateId !== undefined && updateId <= this.#updateId) return false;
		this.#updateId = updateId;
		await this.save();
		return true;
	}

	async rememberOrigin(origin: OriginRef, messageThreadId?: number): Promise<void> {
		const route: TelegramReplyRoute = {
			chatId: origin.parentId ?? origin.conversationId,
			...(messageThreadId === undefined ? {} : { messageThreadId }),
		};
		this.#routes.set(originKey(origin), route);
		if (origin.kind === "topic") {
			if (messageThreadId === undefined || !origin.parentId)
				throw new Error(`Telegram topic ${origin.conversationId} lacks reply routing data`);
			this.#topics.set(origin.conversationId, { parentId: origin.parentId, messageThreadId });
		}
		await this.save();
	}

	routeFor(origin: OriginRef): TelegramReplyRoute | undefined {
		const known = this.#routes.get(originKey(origin));
		if (known) return known;
		if (origin.kind !== "topic") return undefined;
		const topic = this.#topics.get(origin.conversationId);
		return topic ? { chatId: topic.parentId, messageThreadId: topic.messageThreadId } : undefined;
	}

	async save(): Promise<void> {
		await mkdir(dirname(this.path), { recursive: true });
		const payload: PersistedState = {
			...(this.#updateId === undefined ? {} : { updateId: this.#updateId }),
			topics: Object.fromEntries(this.#topics),
			routes: Object.fromEntries(this.#routes),
		};
		const temporary = `${this.path}.${process.pid}.${crypto.randomUUID()}.tmp`;
		await writeFile(temporary, `${JSON.stringify(payload)}\n`, { mode: 0o600 });
		await rename(temporary, this.path);
	}
}
