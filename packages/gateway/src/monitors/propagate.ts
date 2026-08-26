import { CATCH_ALL_EVENT_ORIGIN, eventTypeOrigin, type MonitorEventRecord, originKey } from "@gajaeway/protocol";
import type { DeliveryService } from "../delivery/delivery";
import type { MemoryClosureQueue } from "../memory/closure";
import type { GjcPort } from "../orchestrator/gjc-client";
import type { GatewayDatabase } from "../store/db";
import type { MonitorRegistry } from "./registry";

/** Product-level semantics for the seeded maintenance events (generic, persona-independent). */
const MAINTENANCE_GUIDANCE: Record<string, string | undefined> = {
	"memory.canonicalize":
		"For memory.canonicalize events: read the memory tree's daily/ captures, append the durable facts into their canonical axis files (people/ projects/ decisions/ events/ tasks/ channels/), keep every original byte (never delete or summarize-replace, move stray root files into an axis directory with their raw content preserved), and make new files reachable from MEMORY.md. The gateway commits; you only write files.",
	"memory.audit":
		"For memory.audit events: run the memory validator (gajaeway memory audit) and put a one-line pass report or the failure diagnostics plus your repair attempt into the note.",
};

export class MonitorPropagator {
	readonly #database: GatewayDatabase;
	readonly #registry: MonitorRegistry;
	readonly #gjc: GjcPort;
	readonly #memory: MemoryClosureQueue;
	readonly #delivery: DeliveryService;
	readonly #emit: (event: MonitorEventRecord) => void;
	#batches = new Map<string, { eventIds: string[]; timer: ReturnType<typeof setTimeout> }>();
	constructor(options: {
		database: GatewayDatabase;
		registry: MonitorRegistry;
		gjc: GjcPort;
		memory: MemoryClosureQueue;
		delivery: DeliveryService;
		emit: (event: MonitorEventRecord) => void;
	}) {
		this.#database = options.database;
		this.#registry = options.registry;
		this.#gjc = options.gjc;
		this.#memory = options.memory;
		this.#delivery = options.delivery;
		this.#emit = options.emit;
	}
	submit(monitorId: string, eventType: string, payload: unknown): string {
		const monitor = this.#registry.get(monitorId);
		if (!monitor?.enabled) throw new Error("unknown or disabled monitor");
		if (typeof eventType !== "string" || !eventType) throw new Error("event type is required");
		const eventId = crypto.randomUUID();
		const firedAt = new Date().toISOString();
		this.#database.withTransaction(() =>
			this.#database.monitorEventCreate({
				eventId,
				monitorId,
				eventType,
				payloadJson: JSON.stringify(payload ?? null),
				firedAt,
			}),
		);
		this.#emit({ eventId, monitorId, eventType, firedAt, stage: "admitted" });
		const key = `${monitorId}\u0000${eventType}`;
		if (monitor.burstPolicy === "serialize") {
			void this.#dispatch([eventId]);
			return eventId;
		}
		const previous = this.#batches.get(key);
		if (previous) {
			if (monitor.burstPolicy === "drop") previous.eventIds.splice(0, previous.eventIds.length, eventId);
			else if (monitor.burstPolicy === "dedupe") {
				const seen = previous.eventIds.some(
					(id) =>
						this.#database.monitorEventRows().find((row) => row.event_id === id)?.payload_json ===
						JSON.stringify(payload ?? null),
				);
				if (!seen) previous.eventIds.push(eventId);
				else this.#database.monitorEventUpdate(eventId, "batched", "deduped");
			} else previous.eventIds.push(eventId);
			return eventId;
		}
		const batch = {
			eventIds: [eventId],
			timer: setTimeout(() => {
				this.#batches.delete(key);
				void this.#dispatch(batch.eventIds);
			}, 250),
		};
		this.#batches.set(key, batch);
		return eventId;
	}
	async reconcile(): Promise<void> {
		// Replay oldest-first: recovery must re-author events in the order they fired.
		for (const row of this.#database.monitorEventRows(undefined, "oldest")) {
			const output = this.#database.authoredOutput(row.event_id);
			const hasMemory = this.#database
				.memoryIntentRows()
				.some((intent) => intent.kind === "monitor-event" && intent.payload_json.includes(row.event_id));
			if (output && !hasMemory) this.#author(row.event_id, output);
			else if (!output && (row.stage === "admitted" || row.stage === "dispatched" || row.stage === "failed"))
				await this.#dispatch([row.event_id]);
		}
	}
	async #dispatch(eventIds: string[]): Promise<void> {
		const rows = this.#database.monitorEventRows().filter((row) => eventIds.includes(row.event_id));
		if (!rows.length) return;
		const monitor = this.#registry.get(rows[0]?.monitor_id);
		if (!monitor) return;
		const batchId = crypto.randomUUID();
		this.#database.withTransaction(() => {
			for (const row of rows) this.#database.monitorEventUpdate(row.event_id, "batched", batchId);
		});
		const declared = new Set(monitor.eventTypes);
		const sessionOrigin = declared.has(rows[0]?.event_type)
			? eventTypeOrigin(rows[0]?.event_type)
			: CATCH_ALL_EVENT_ORIGIN;
		try {
			const { sessionId } = await this.#gjc.ensureSession(
				originKey(sessionOrigin),
				this.#database.getSessionRecord(originKey(sessionOrigin))?.epoch ?? 0,
			);
			const guidance = rows
				.map((row) => MAINTENANCE_GUIDANCE[row.event_type])
				.filter((entry, index, all) => entry && all.indexOf(entry) === index)
				.join(" ");
			const prompt = `Author monitor events.${guidance ? ` ${guidance}` : ""} Respond ONLY with a JSON array containing exactly one {"eventId","note"} entry per event: ${JSON.stringify(rows.map((row) => ({ eventId: row.event_id, eventType: row.event_type, payload: JSON.parse(row.payload_json) })))}`;
			const response = await this.#gjc.sendTurn(sessionId, prompt);
			this.#database.withTransaction(() => {
				for (const row of rows) this.#database.monitorEventUpdate(row.event_id, "dispatched", batchId);
			});
			const authored = JSON.parse(response) as Array<{ eventId?: unknown; note?: unknown }>;
			if (!Array.isArray(authored)) throw new Error("authoring response is not an array");
			for (const entry of authored)
				if (typeof entry.eventId === "string" && eventIds.includes(entry.eventId) && typeof entry.note === "string")
					this.#author(entry.eventId, entry.note);
			if (monitor.channelTarget) {
				const delivery = this.#delivery.prepare(
					batchId,
					monitor.channelTarget.origin,
					authored
						.filter(
							(entry): entry is { eventId: string; note: string } =>
								typeof entry.eventId === "string" && typeof entry.note === "string",
						)
						.map((entry) => entry.note)
						.join("\n"),
				);
				if (delivery) this.#delivery.markInflight(delivery.deliveryId as string);
			}
		} catch {
			this.#database.withTransaction(() => {
				for (const row of rows) this.#database.monitorEventUpdate(row.event_id, "failed", batchId);
			});
		}
	}
	#author(eventId: string, note: string): void {
		const row = this.#database.monitorEventRows().find((candidate) => candidate.event_id === eventId);
		if (!row) return;
		this.#database.withTransaction(() => {
			this.#database.authoredOutputCreate(eventId, note);
			this.#database.monitorEventUpdate(eventId, "authored");
		});
		this.#memory.enqueue({
			kind: "monitor-event",
			originRefJson: JSON.stringify(eventTypeOrigin(row.event_type)),
			userText: `Monitor event ${eventId}: ${row.event_type}`,
			replyText: note,
		});
		this.#emit({
			eventId,
			monitorId: row.monitor_id,
			eventType: row.event_type,
			firedAt: row.fired_at,
			stage: "authored",
		});
	}
}
