import { createHash } from "node:crypto";
import {
	type ChatMessagePayload,
	isChatPlatform,
	type OriginRef,
	originKey,
	validateOriginRef,
} from "@gajae-gateway/protocol";
import type { RestartNoticeConfig, RestartNoticePhase } from "../config";
import { buildDeliveryPayload, type DeliveryService } from "../delivery/delivery";
import type { Store } from "../store";
import type { WriteTx } from "../store/tx";

/** A conversation counts as active when a person spoke in it this recently, or a turn is still in flight. */
export const RESTART_NOTICE_ACTIVE_WINDOW_MS = 30 * 60_000;

const RECORD_PREFIX = "restart_notice:";

/** Durable record of one restart's announcement; deleted with the closing notice. */
interface RestartNoticeRecord {
	readonly restartId: string;
	/** Process generation that announced it: a different generation closing it means the gateway did restart. */
	readonly bootId: string;
	readonly announcedAt: string;
	readonly origins: readonly string[];
}

export interface RestartNoticeOptions {
	readonly database: Store;
	readonly delivery: DeliveryService;
	/** Read at every notice, so a config reload applies to the next restart. */
	readonly enabled: () => boolean;
	/** Chat text for one phase; read at every notice so a config reload applies. */
	readonly text: (phase: RestartNoticePhase) => string;
	/** Hands committed notices to connected adapters; unconnected adapters receive them on their connect sweep. */
	readonly broadcast: (payloads: readonly ChatMessagePayload[]) => void;
	/** Restart id this process inherited as an already-held drain at boot; it was announced by its predecessor. */
	readonly inheritedRestartId?: string;
	readonly bootId?: string;
	readonly now?: () => number;
	readonly log?: (line: string, level?: "info" | "warn" | "error") => void;
}

/**
 * Chat-visible restart lifecycle. A restart drain announces itself once to
 * every active conversation, and the announcement is closed exactly once:
 * "back online" when a later process generation releases it, "cancelled" when
 * the announcing process releases it itself because no restart happened.
 * Notices are ordinary ledger deliveries with deterministic ids, so they take
 * the same adapter, redaction, and settlement path as every other message.
 */
export class RestartNotices {
	readonly #database: Store;
	readonly #delivery: DeliveryService;
	readonly #enabled: () => boolean;
	readonly #text: RestartNoticeOptions["text"];
	readonly #broadcast: RestartNoticeOptions["broadcast"];
	readonly #inheritedRestartId: string | undefined;
	readonly #bootId: string;
	readonly #now: () => number;
	readonly #log: NonNullable<RestartNoticeOptions["log"]>;

	constructor(options: RestartNoticeOptions) {
		this.#database = options.database;
		this.#delivery = options.delivery;
		this.#enabled = options.enabled;
		this.#text = options.text;
		this.#broadcast = options.broadcast;
		this.#inheritedRestartId = options.inheritedRestartId;
		this.#bootId = options.bootId ?? crypto.randomUUID();
		this.#now = options.now ?? Date.now;
		this.#log = options.log ?? ((line, level) => console[level ?? "info"](line));
	}

	/** The drain for `restartId` just took the dispatch hold. Announces at most once per restart id. */
	restarting(restartId: string): void {
		if (restartId === this.#inheritedRestartId || !this.#enabled()) return;
		const since = new Date(this.#now() - RESTART_NOTICE_ACTIVE_WINDOW_MS).toISOString();
		const targets = this.#database.inbox.activeConversationOrigins(since).flatMap((row) => {
			const origin = chatOrigin(row.origin_ref_json);
			// A muted thread asked for silence; a restart is not a reason to break it.
			return origin && originKey(origin) === row.origin_key && !this.#database.inbox.isThreadMuted(row.origin_key)
				? [origin]
				: [];
		});
		const text = this.#text("restarting");
		const payloads = this.#database.transaction((tx) => {
			if (this.#database.core.metaGet(recordKey(restartId)) !== undefined) return [];
			const record: RestartNoticeRecord = {
				restartId,
				bootId: this.#bootId,
				announcedAt: new Date(this.#now()).toISOString(),
				origins: targets.map((origin) => JSON.stringify(origin)),
			};
			this.#database.core.metaSet(recordKey(restartId), JSON.stringify(record));
			return this.#persist(tx, restartId, "restarting", targets, text);
		});
		if (payloads.length > 0 || targets.length === 0)
			this.#log(`restart_notice_sent phase=restarting id=${restartId} origins=${payloads.length}`);
		this.#broadcast(payloads);
	}

	/** The drain was released without this process exiting for it. */
	released(restartId: string): void {
		const record = this.#record(restartId);
		if (!record) return;
		this.#close(record, record.bootId === this.#bootId ? "cancelled" : "online");
	}

	/** A supervisor's hold moved to an owner restart: the open announcement now belongs to that restart. */
	transferred(restartId: string, nextRestartId: string): void {
		const record = this.#record(restartId);
		if (!record) return;
		this.#database.transaction(() => {
			this.#database.core.metaDelete(recordKey(restartId));
			if (this.#database.core.metaGet(recordKey(nextRestartId)) === undefined)
				this.#database.core.metaSet(recordKey(nextRestartId), JSON.stringify({ ...record, restartId: nextRestartId }));
		});
	}

	/**
	 * Boot: announcements left by an earlier process generation are closed as
	 * online, except the inherited drain, which stays open until its supervisor
	 * releases it (the stack is not ready while dispatch is still held).
	 */
	resumeAfterBoot(): void {
		const end = `${RECORD_PREFIX}\uffff`;
		for (const key of this.#database.core.metaKeysInRange(RECORD_PREFIX, end)) {
			const record = this.#record(key.slice(RECORD_PREFIX.length));
			if (!record || record.restartId === this.#inheritedRestartId || record.bootId === this.#bootId) continue;
			this.#close(record, "online");
		}
	}

	#close(record: RestartNoticeRecord, phase: "online" | "cancelled"): void {
		const enabled = this.#enabled();
		const text = this.#text(phase);
		const origins = record.origins.flatMap((json) => {
			const origin = chatOrigin(json);
			return origin ? [origin] : [];
		});
		const payloads = this.#database.transaction((tx) => {
			// `record` was read before this write lock. Another gateway process on the
			// same DB (an old generation still releasing its drain while the
			// replacement boots) may have closed it since; the delete is the one claim.
			if (this.#database.core.metaGet(recordKey(record.restartId)) === undefined) return undefined;
			this.#database.core.metaDelete(recordKey(record.restartId));
			return enabled ? this.#persist(tx, record.restartId, phase, origins, text) : [];
		});
		if (payloads === undefined) {
			this.#log(`restart_notice_skipped phase=${phase} id=${record.restartId} reason=already_closed`);
			return;
		}
		this.#log(`restart_notice_sent phase=${phase} id=${record.restartId} origins=${payloads.length}`);
		this.#broadcast(payloads);
	}

	#persist(
		tx: WriteTx,
		restartId: string,
		phase: RestartNoticePhase,
		origins: readonly OriginRef[],
		text: string,
	): ChatMessagePayload[] {
		const payloads: ChatMessagePayload[] = [];
		for (const origin of origins) {
			const key = originKey(origin);
			const deliveryId = restartNoticeDeliveryId(restartId, key, phase);
			// Not a turn reply: `final: false` leaves any in-flight turn's working status alone.
			const payload = buildDeliveryPayload(deliveryId, origin, text, deliveryId, undefined, false);
			if (payload && this.#delivery.persistInTransaction(tx, payload)) payloads.push(payload);
		}
		return payloads;
	}

	#record(restartId: string): RestartNoticeRecord | undefined {
		const raw = this.#database.core.metaGet(recordKey(restartId));
		if (raw === undefined) return undefined;
		try {
			const parsed = JSON.parse(raw) as Partial<RestartNoticeRecord>;
			if (
				parsed.restartId === restartId &&
				typeof parsed.bootId === "string" &&
				Array.isArray(parsed.origins) &&
				parsed.origins.every((origin) => typeof origin === "string")
			)
				return parsed as RestartNoticeRecord;
		} catch {
			// fall through: an unreadable record cannot be closed truthfully
		}
		this.#log(`restart notice record unreadable id=${restartId}; dropped without a notice`, "warn");
		this.#database.core.metaDelete(recordKey(restartId));
		return undefined;
	}
}

function recordKey(restartId: string): string {
	return `${RECORD_PREFIX}${restartId}`;
}

function chatOrigin(json: string): OriginRef | undefined {
	try {
		const origin = validateOriginRef(JSON.parse(json) as OriginRef);
		return isChatPlatform(origin.platform) ? origin : undefined;
	} catch {
		return undefined;
	}
}

export function restartNoticeDeliveryId(restartId: string, key: string, phase: RestartNoticePhase): string {
	return `gw-r-${createHash("sha256").update(`${restartId}|${key}|${phase}`).digest("hex").slice(0, 32)}`;
}

/** Persona name substituted for `{name}` when `restartNotice.personaName` is unset. */
export const DEFAULT_RESTART_NOTICE_NAME = "Gajaeway";

/** Neutral third-person defaults; `restartNotice.messages` replaces any of them per phase. */
export const DEFAULT_RESTART_NOTICE_MESSAGES: { readonly [phase in RestartNoticePhase]: string } = {
	restarting: "⚠️ {name} is restarting. Messages sent meanwhile are kept and answered after restart.",
	online: "♻️ {name} is back online.",
	cancelled: "↩️ {name}'s restart was cancelled. Queued messages are being answered now.",
};

/** The chat text for `phase`: the configured template or the default, with `{name}` filled in. */
export function restartNoticeText(config: RestartNoticeConfig | undefined, phase: RestartNoticePhase): string {
	const template = config?.messages?.[phase] ?? DEFAULT_RESTART_NOTICE_MESSAGES[phase];
	return template.replaceAll("{name}", config?.personaName ?? DEFAULT_RESTART_NOTICE_NAME);
}
