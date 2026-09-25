import {
	eventTypeOrigin,
	type MonitorRecord,
	type MonitorSpec,
	type MonitorUpdateParams,
	OriginRefError,
	type TriggerSpec,
	validateOriginRef,
} from "@gajae-gateway/protocol";
import type { GatewayDatabase } from "../store/db";

const BURST_POLICIES = new Set(["coalesce", "dedupe", "serialize", "drop"]);
const SERVICE_TIERS = new Set(["none", "auto", "default", "flex", "scale", "priority", "openai-only", "claude-only"]);
const MONITOR_UPDATE_FIELDS = new Set([
	"name",
	"trigger",
	"eventTypes",
	"burstPolicy",
	"channelTarget",
	"instruction",
	"model",
	"serviceTier",
	"enabled",
]);
/** Upper bound on a per-monitor authoring instruction, in characters. */
export const MONITOR_INSTRUCTION_MAX_LENGTH = 4000;

export class MonitorRegistry {
	readonly #database: GatewayDatabase;
	constructor(database: GatewayDatabase) {
		this.#database = database;
	}
	add(spec: MonitorSpec): MonitorRecord {
		validateSpec(spec);
		const trigger = spec.trigger.kind === "webhook" ? { ...spec.trigger, route: crypto.randomUUID() } : spec.trigger;
		const instruction = spec.instruction?.trim() || undefined;
		const record: MonitorRecord = {
			...spec,
			trigger,
			monitorId: crypto.randomUUID(),
			burstPolicy: spec.burstPolicy ?? "coalesce",
			enabled: spec.enabled ?? true,
			instruction,
			createdAt: new Date().toISOString(),
		};
		this.#database.withTransaction(() =>
			this.#database.monitorCreate({
				id: record.monitorId,
				name: record.name,
				triggerJson: JSON.stringify(record.trigger),
				eventTypesJson: JSON.stringify(record.eventTypes),
				burstPolicy: record.burstPolicy,
				channelTargetJson: record.channelTarget ? JSON.stringify(record.channelTarget) : null,
				enabled: record.enabled,
				instruction: instruction ?? null,
				modelJson: record.model ? JSON.stringify(record.model) : null,
				serviceTier: record.serviceTier ?? null,
			}),
		);
		return record;
	}
	list(): MonitorRecord[] {
		const records: MonitorRecord[] = [];
		for (const row of this.#database.monitorRows()) {
			try {
				records.push(rowToRecord(row));
			} catch {
				console.error(`monitor registry ignored invalid persisted record: monitor ${row.monitor_id}`);
			}
		}
		return records;
	}
	get(monitorId: string): MonitorRecord | undefined {
		return this.list().find((monitor) => monitor.monitorId === monitorId);
	}
	update(params: MonitorUpdateParams): MonitorRecord | undefined {
		if (!params || typeof params !== "object" || typeof params.monitorId !== "string" || !params.monitorId)
			throw new Error("monitorId is required");
		const { monitorId, schedule, ...patch } = params;
		for (const field of Object.keys(patch))
			if (!MONITOR_UPDATE_FIELDS.has(field)) throw new Error(`unknown monitor update field: ${field}`);
		if (schedule !== undefined && typeof schedule !== "string") throw new Error("monitor schedule must be a string");
		if (schedule !== undefined && patch.trigger !== undefined)
			throw new Error("use either schedule or trigger when updating a monitor");
		if (schedule === undefined && Object.keys(patch).length === 0) throw new Error("monitor update requires a change");

		return this.#database.withTransaction(() => {
			const current = this.get(monitorId);
			if (!current) return undefined;
			let trigger = current.trigger;
			if (patch.trigger !== undefined) {
				if (!patch.trigger || typeof patch.trigger !== "object") throw new Error("invalid monitor trigger");
				trigger =
					patch.trigger.kind === "webhook"
						? {
								...patch.trigger,
								route: current.trigger.kind === "webhook" ? current.trigger.route : crypto.randomUUID(),
							}
						: patch.trigger;
			}
			if (schedule !== undefined) {
				if (current.trigger.kind !== "cron") throw new Error("only cron monitors have a schedule");
				if (!schedule.trim()) throw new Error("monitor schedule is required");
				trigger = { ...current.trigger, schedule };
			}
			const instruction = Object.hasOwn(patch, "instruction")
				? patch.instruction?.trim() || undefined
				: current.instruction;
			const updated: MonitorRecord = { ...current, ...patch, trigger, instruction };
			if (typeof updated.enabled !== "boolean") throw new Error("monitor enabled must be a boolean");
			validateSpec(updated);
			const persisted = this.#database.monitorUpdate({
				id: current.monitorId,
				name: updated.name,
				triggerJson: JSON.stringify(updated.trigger),
				eventTypesJson: JSON.stringify(updated.eventTypes),
				burstPolicy: updated.burstPolicy,
				channelTargetJson: updated.channelTarget ? JSON.stringify(updated.channelTarget) : null,
				enabled: updated.enabled,
				instruction: instruction ?? null,
				modelJson: updated.model ? JSON.stringify(updated.model) : null,
				serviceTier: updated.serviceTier ?? null,
			});
			return persisted ? updated : undefined;
		});
	}
	remove(monitorId: string): boolean {
		return this.#database.withTransaction(() => this.#database.monitorDelete(monitorId));
	}
}

function rowToRecord(row: ReturnType<GatewayDatabase["monitorRows"]>[number]): MonitorRecord {
	const record: MonitorRecord = {
		monitorId: row.monitor_id,
		name: row.name,
		trigger: JSON.parse(row.trigger_json),
		eventTypes: JSON.parse(row.event_types_json),
		burstPolicy: row.burst_policy as MonitorRecord["burstPolicy"],
		channelTarget: row.channel_target_json ? JSON.parse(row.channel_target_json) : null,
		enabled: Boolean(row.enabled),
		instruction: row.instruction ?? undefined,
		model: row.model_json ? JSON.parse(row.model_json) : undefined,
		serviceTier: (row.service_tier as MonitorRecord["serviceTier"]) ?? undefined,
		createdAt: row.created_at,
	};
	validateSpec(record);
	return record;
}

export function validateSpec(spec: MonitorSpec): void {
	if (!spec || typeof spec.name !== "string" || !spec.name.trim()) throw new Error("monitor name is required");
	if (
		!Array.isArray(spec.eventTypes) ||
		!spec.eventTypes.length ||
		spec.eventTypes.some((type) => typeof type !== "string" || !type)
	)
		throw new Error("monitor eventTypes must be a non-empty string list");
	// An event type becomes the conversationId of its executing session's origin.
	// Admit only what that origin can carry, or the monitor is registered but
	// every event dies at dispatch with OriginRefError (live: a 469-character
	// "event type" that was really the whole instruction, gaebal, 2026-09-05).
	for (const type of spec.eventTypes) {
		try {
			validateOriginRef(eventTypeOrigin(type));
		} catch (error) {
			if (error instanceof OriginRefError)
				throw new Error(
					`monitor eventType ${JSON.stringify(type.slice(0, 64))}${type.length > 64 ? "…" : ""} is not a valid origin segment (1-256 chars of A-Z a-z 0-9 _ . : @ + -); put instructions in "instruction", not the event type`,
				);
			throw error;
		}
	}
	if (spec.burstPolicy && !BURST_POLICIES.has(spec.burstPolicy)) throw new Error("invalid monitor burstPolicy");
	validateTrigger(spec.trigger);
	if (spec.channelTarget) validateOriginRef(spec.channelTarget.origin);
	if (spec.model !== undefined) {
		const valid =
			(typeof spec.model === "string" && spec.model.trim() !== "") ||
			(typeof spec.model === "object" &&
				spec.model !== null &&
				!Array.isArray(spec.model) &&
				Object.keys(spec.model).length === 1 &&
				typeof spec.model.preset === "string" &&
				spec.model.preset.trim() !== "");
		if (!valid) throw new Error("monitor model must be a non-empty selector or contain only preset");
	}
	if (spec.serviceTier !== undefined && !SERVICE_TIERS.has(spec.serviceTier))
		throw new Error("monitor serviceTier is invalid");
	if (spec.instruction !== undefined) {
		if (typeof spec.instruction !== "string") throw new Error("monitor instruction must be a string");
		if (spec.instruction.length > MONITOR_INSTRUCTION_MAX_LENGTH)
			throw new Error(`monitor instruction must be at most ${MONITOR_INSTRUCTION_MAX_LENGTH} characters`);
	}
}
function validateTrigger(trigger: TriggerSpec): void {
	if (!trigger || typeof trigger !== "object") throw new Error("monitor trigger is required");
	if (trigger.kind === "cron" && typeof trigger.schedule === "string") return;
	if (trigger.kind === "webhook" && typeof trigger.route === "string") return;
	if (trigger.kind === "watcher" && typeof trigger.root === "string") return;
	if (
		trigger.kind === "script" &&
		Array.isArray(trigger.command) &&
		trigger.command.length &&
		trigger.command.every((arg) => typeof arg === "string") &&
		Number.isFinite(trigger.intervalMs) &&
		trigger.intervalMs > 0
	)
		return;
	throw new Error("invalid monitor trigger");
}
