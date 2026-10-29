import {
	eventTypeOrigin,
	isChatPlatform,
	type MonitorChannelTarget,
	type MonitorInvalidRecord,
	type MonitorRecord,
	type MonitorSpec,
	type MonitorUpdateParams,
	OriginRefError,
	type TriggerSpec,
	validateOriginRef,
} from "@gajae-gateway/protocol";
import type { Store } from "../store";
import type { MonitorCronState } from "../store/groups/monitors";
import { validateProcedureFiles } from "./procedure";
import { compileCron, isCronScheduleRunnable, nextCronFire, validateCronSchedule } from "./triggers/cron";

const monitorCronMetaKey = (monitorId: string) => `monitor_cron:${monitorId}`;
const monitorReportToMetaKey = (monitorId: string) => `monitor_report_to:${monitorId}`;

const BURST_POLICIES = new Set(["coalesce", "dedupe", "serialize", "drop"]);
const OVERLAP_POLICIES = new Set(["queue", "skip"]);
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
	"reportTo",
	"enabled",
	"procedureFiles",
]);
/** Upper bound on a per-monitor authoring instruction, in characters. */
export const MONITOR_INSTRUCTION_MAX_LENGTH = 4000;

export class MonitorRegistry {
	readonly #database: Store;
	constructor(database: Store) {
		this.#database = database;
	}
	add(spec: MonitorSpec): MonitorRecord {
		validateSpec(spec);
		if (spec.trigger.kind === "cron") validateCronSchedule(spec.trigger.schedule, spec.trigger.timezone);
		const trigger: TriggerSpec =
			spec.trigger.kind === "webhook"
				? { ...spec.trigger, route: crypto.randomUUID() }
				: spec.trigger.kind === "cron"
					? { ...spec.trigger, timezone: spec.trigger.timezone ?? localTimezone() }
					: spec.trigger;
		const instruction = spec.instruction?.trim() || undefined;
		const procedureFiles = spec.procedureFiles?.length ? spec.procedureFiles.map((file) => file.trim()) : undefined;
		const record: MonitorRecord = {
			...spec,
			trigger,
			monitorId: crypto.randomUUID(),
			burstPolicy: spec.burstPolicy ?? "coalesce",
			overlap: spec.overlap ?? "queue",
			enabled: spec.enabled ?? true,
			instruction,
			procedureFiles,
			createdAt: new Date().toISOString(),
		};
		this.#database.transaction((tx) => {
			this.#database.monitors.create(tx, {
				id: record.monitorId,
				name: record.name,
				triggerJson: JSON.stringify(record.trigger),
				eventTypesJson: JSON.stringify(record.eventTypes),
				burstPolicy: record.burstPolicy,
				overlap: record.overlap,
				channelTargetJson: record.channelTarget ? JSON.stringify(record.channelTarget) : null,
				enabled: record.enabled,
				instruction: instruction ?? null,
				modelJson: record.model ? JSON.stringify(record.model) : null,
				serviceTier: record.serviceTier ?? null,
				procedureFilesJson: procedureFiles ? JSON.stringify(procedureFiles) : null,
			});
			if (record.reportTo)
				this.#database.core.metaSet(monitorReportToMetaKey(record.monitorId), JSON.stringify(record.reportTo));
		});
		return record;
	}
	list(): MonitorRecord[] {
		return this.#scan({ validateCron: false }).records;
	}
	/**
	 * Stored monitors that do not run: rows that no longer load (they are left out of `list()`
	 * and can only be removed) and loadable cron monitors whose schedule fails write-time
	 * validation (still in `list()`, repairable with `update --schedule`).
	 */
	invalid(): MonitorInvalidRecord[] {
		return this.#scan({ validateCron: true }).invalid;
	}
	/**
	 * Monitors that cannot be started: rows that no longer load and cron monitors whose
	 * schedule cannot be compiled or never fires (matcher cannot execute). Unlike invalid(),
	 * this excludes cron schedules that fail write-time policy but can still run
	 * (e.g., a monitor saved with '5/1' before strict validation was added).
	 * Used at runtime to distinguish genuinely not-startable from stricter-policy-rejected.
	 */
	notStartable(): MonitorInvalidRecord[] {
		return this.#scan({ validateCron: true, matcherCompilable: true }).invalid;
	}
	#scan(options?: { validateCron?: boolean; matcherCompilable?: boolean }): {
		records: MonitorRecord[];
		invalid: MonitorInvalidRecord[];
	} {
		const validateCron = options?.validateCron ?? false;
		const matcherCompilable = options?.matcherCompilable ?? false;
		const records: MonitorRecord[] = [];
		const invalid: MonitorInvalidRecord[] = [];
		for (const row of this.#database.monitors.rows()) {
			let record: MonitorRecord;
			try {
				record = rowToRecord(row, this.#database.core.metaGet(monitorReportToMetaKey(row.monitor_id)));
			} catch (error) {
				invalid.push({ monitorId: row.monitor_id, name: row.name, reason: errorReason(error), loadable: false });
				continue;
			}
			records.push(record);
			if (validateCron && record.trigger.kind === "cron") {
				let reason: string | null = null;
				if (matcherCompilable) {
					// At runtime, skip ONLY schedules that:
					// 1. Cannot be compiled by the matcher, OR
					// 2. Never fire, OR
					// 3. Have out-of-range values (even if matcher handles them)
					if (!isCronScheduleRunnable(record.trigger.schedule, record.trigger.timezone)) {
						// Get the actual error message
						try {
							compileCron(record.trigger.schedule, record.trigger.timezone);
							const nextFire = nextCronFire(
								record.trigger.schedule,
								new Date(),
								record.trigger.timezone ?? new Intl.DateTimeFormat().resolvedOptions().timeZone,
							);
							if (nextFire === null) {
								reason = "cron schedule never fires (no date matches day-of-month, month and day-of-week together)";
							}
						} catch (error) {
							reason = errorReason(error);
						}
					} else {
						// Check for out-of-range field values. Even though the matcher might handle them,
						// we skip schedules with out-of-range values to prevent user errors.
						try {
							validateCronSchedule(record.trigger.schedule, record.trigger.timezone);
						} catch (error) {
							const errorMsg = errorReason(error);
							// Only skip if it's an out-of-range error. Skip step-syntax errors like '5/1'.
							if (errorMsg.includes("out of range") || errorMsg.includes("five fields")) {
								reason = errorMsg;
							}
						}
					}
				} else {
					// Check strict write-time policy (for invalid() report)
					try {
						validateCronSchedule(record.trigger.schedule, record.trigger.timezone);
					} catch (error) {
						reason = errorReason(error);
					}
				}
				if (reason !== null) {
					invalid.push({ monitorId: record.monitorId, name: record.name, reason, loadable: true });
				}
			}
		}
		return { records, invalid };
	}
	get(monitorId: string): MonitorRecord | undefined {
		const row = this.#database.monitors.row(monitorId);
		if (!row) return undefined;
		try {
			return rowToRecord(row, this.#database.core.metaGet(monitorReportToMetaKey(row.monitor_id)));
		} catch {
			return undefined;
		}
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

		return this.#database.transaction((tx) => {
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
			if (patch.procedureFiles !== undefined) validateProcedureFiles(patch.procedureFiles);
			const procedureFiles = Object.hasOwn(patch, "procedureFiles")
				? patch.procedureFiles?.length
					? patch.procedureFiles.map((file) => file.trim())
					: undefined
				: current.procedureFiles;
			const updated: MonitorRecord = { ...current, ...patch, trigger, instruction, procedureFiles };
			if (typeof updated.enabled !== "boolean") throw new Error("monitor enabled must be a boolean");
			validateSpec(updated);
			// A changed schedule is checked like a new one; an unchanged stored one is left as it is
			// (so an operator can still disable or rename a monitor saved before this check).
			if (updated.trigger.kind === "cron" && (schedule !== undefined || patch.trigger !== undefined))
				validateCronSchedule(updated.trigger.schedule, updated.trigger.timezone);
			const persisted = this.#database.monitors.update(tx, {
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
				procedureFilesJson: procedureFiles ? JSON.stringify(procedureFiles) : null,
			});
			if (!persisted) return undefined;
			const reportToKey = monitorReportToMetaKey(current.monitorId);
			if (updated.reportTo) this.#database.core.metaSet(reportToKey, JSON.stringify(updated.reportTo));
			else this.#database.core.metaDelete(reportToKey);
			return updated;
		});
	}
	/**
	 * Removes the monitor together with everything that would otherwise keep
	 * firing in its name: the cron cursor, its claimed slots, and every event
	 * still waiting for a dispatcher. Left in place, each of those events was
	 * picked up later, found its monitor gone and ended as
	 * `monitor dispatch rejected missing or invalid monitor` at error level
	 * (2026-10-14: five such lines for one removed work-nudge monitor). An
	 * event whose turn is already in flight keeps its own terminal disposition.
	 */
	remove(monitorId: string): boolean {
		return this.#database.transaction((tx) => {
			const deleted = this.#database.monitors.delete(tx, monitorId);
			this.#database.core.metaDelete(monitorCronMetaKey(monitorId));
			this.#database.core.metaDelete(monitorReportToMetaKey(monitorId));
			if (!deleted) return false;
			for (const event of this.#database.monitors.eventsAwaitingDispatch(monitorId)) {
				this.#database.monitors.eventTerminalFail(
					tx,
					event.event_id,
					"monitor_removed",
					"monitor removed before the event was dispatched",
				);
			}
			this.#database.monitors.slotsDelete(tx, monitorId);
			return true;
		});
	}

	cronCursor(monitorId: string): string | undefined {
		const claimed = this.#database.monitors.slotCursor(monitorId);
		const skipped = this.cronState(monitorId)?.cursor;
		if (!claimed) return skipped;
		if (!skipped) return claimed;
		return Date.parse(skipped) > Date.parse(claimed) ? skipped : claimed;
	}

	cronState(monitorId: string): MonitorCronState | undefined {
		const raw = this.#database.core.metaGet(monitorCronMetaKey(monitorId));
		return raw === undefined ? undefined : (JSON.parse(raw) as MonitorCronState);
	}

	recordCronSkip(
		monitorId: string,
		skip: { count: number; oldest: string; newest: string },
		recordedAt: string,
	): MonitorCronState {
		return this.#database.transaction(() => {
			const previous = this.cronState(monitorId);
			const state: MonitorCronState = {
				cursor: skip.newest,
				skippedTotal: (previous?.skippedTotal ?? 0) + skip.count,
				lastSkip: { ...skip, recordedAt },
			};
			this.#database.core.metaSet(monitorCronMetaKey(monitorId), JSON.stringify(state));
			return state;
		});
	}
}

function rowToRecord(row: ReturnType<Store["monitors"]["rows"]>[number], reportToJson?: string): MonitorRecord {
	const trigger = JSON.parse(row.trigger_json) as TriggerSpec;
	const record: MonitorRecord = {
		monitorId: row.monitor_id,
		name: row.name,
		trigger:
			trigger.kind === "cron" && trigger.timezone === undefined ? { ...trigger, timezone: localTimezone() } : trigger,
		eventTypes: JSON.parse(row.event_types_json),
		burstPolicy: row.burst_policy as MonitorRecord["burstPolicy"],
		overlap: row.overlap as MonitorRecord["overlap"],
		channelTarget: row.channel_target_json ? JSON.parse(row.channel_target_json) : null,
		enabled: Boolean(row.enabled),
		instruction: row.instruction ?? undefined,
		model: row.model_json ? JSON.parse(row.model_json) : undefined,
		serviceTier: (row.service_tier as MonitorRecord["serviceTier"]) ?? undefined,
		procedureFiles: row.procedure_files_json ? JSON.parse(row.procedure_files_json) : undefined,
		reportTo: reportToJson ? JSON.parse(reportToJson) : undefined,
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
	if (spec.overlap !== undefined && !OVERLAP_POLICIES.has(spec.overlap))
		throw new Error('monitor overlap must be "queue" or "skip"');
	validateTrigger(spec.trigger);
	if (spec.channelTarget) {
		validateOriginRef(spec.channelTarget.origin);
		validateMentionUserIds(spec.channelTarget);
	}
	if (spec.reportTo !== undefined && spec.reportTo !== null) {
		validateOriginRef(spec.reportTo);
		if (!isChatPlatform(spec.reportTo.platform) && spec.reportTo.platform !== "loopback")
			throw new Error("monitor reportTo must target a chat or loopback conversation origin");
	}
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
	if (spec.procedureFiles !== undefined) validateProcedureFiles(spec.procedureFiles);
}
/** `<@id>` renders as a ping on Discord (numeric snowflake) and Slack (`U…`/`W…`). */
const MENTION_ID_BY_PLATFORM: Partial<Record<string, RegExp>> = {
	discord: /^\d{1,32}$/,
	slack: /^[UW][A-Z0-9]{1,31}$/,
};
function validateMentionUserIds(target: MonitorChannelTarget): void {
	const ids: unknown = target.mentionUserIds;
	if (ids === undefined) return;
	const pattern = MENTION_ID_BY_PLATFORM[target.origin.platform];
	if (!pattern) throw new Error(`monitor channelTarget.mentionUserIds is not supported for ${target.origin.platform}`);
	if (!Array.isArray(ids) || ids.some((id) => typeof id !== "string" || !pattern.test(id)))
		throw new Error(`monitor channelTarget.mentionUserIds must be a list of ${target.origin.platform} user ids`);
}
function validateTrigger(trigger: TriggerSpec): void {
	if (!trigger || typeof trigger !== "object") throw new Error("monitor trigger is required");
	if (trigger.kind === "cron" && typeof trigger.schedule === "string") {
		if (trigger.timezone !== undefined) {
			if (typeof trigger.timezone !== "string" || !trigger.timezone.trim())
				throw new Error("monitor cron timezone must be a non-empty IANA timezone");
			try {
				new Intl.DateTimeFormat("en-US", { timeZone: trigger.timezone }).format();
			} catch {
				throw new Error(`monitor cron timezone ${JSON.stringify(trigger.timezone)} is not a valid IANA timezone`);
			}
		}
		return;
	}
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

function localTimezone(): string {
	const timezone = new Intl.DateTimeFormat().resolvedOptions().timeZone;
	if (!timezone) throw new Error("gateway local IANA timezone is unavailable");
	return timezone;
}

function errorReason(error: unknown): string {
	return (error instanceof Error ? error.message : String(error)).replace(/\s+/g, " ").slice(0, 300);
}
