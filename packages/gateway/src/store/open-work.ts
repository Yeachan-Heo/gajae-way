import { createHash } from "node:crypto";
import type { OriginRef } from "@gajae-gateway/protocol";
import { parseLaneJobRecord, type LaneJobRecord } from "@gajae-gateway/subsession";

export type OpenWorkScope = "replayable" | "quarantined" | "all";

export interface OpenWorkRows {
	readonly inbound: readonly {
		message_id: string;
		origin_key: string;
		origin_ref_json: string;
		body: string;
		source: string;
		state: string;
		turn_role: string | null;
		turn_state: string | null;
		turn_op_ref: string | null;
		bound_session_id: string | null;
		received_at: string;
		cutover_id: string | null;
	}[];
	readonly laneJobs: readonly {
		job_id: string;
		lane_key: string;
		branch: string;
		worktree_path: string;
		state: string;
		record_json: string;
		cutover_id: string | null;
	}[];
	readonly attempts: readonly {
		op_ref: string;
		job_id: string;
		lane_key: string;
		session_id: string;
		version: number;
		settled_at: string | null;
		delivery_id: string;
		record_json: string;
		cutover_id: string | null;
	}[];
	readonly laneReports: readonly {
		report_id: string;
		parent_name: string;
		child_name: string;
		state: string;
		body: string;
		cutover_id: string | null;
	}[];
	readonly monitors: readonly {
		event_id: string;
		monitor_id: string;
		monitor_name: string | null;
		event_type: string;
		stage: string;
		batch_id: string | null;
		fired_at: string;
		cutover_id: string | null;
	}[];
}

export interface OpenWorkRead {
	readOpenWorkRows(scope: OpenWorkScope): OpenWorkRows;
	workAttemptGet(opRef: string): unknown;
}

export interface OpenInboundWork {
	readonly messageId: string;
	readonly originKey: string;
	readonly originRef: OriginRef | null;
	readonly body: string;
	readonly source: string;
	readonly state: string;
	readonly turnRole: string | null;
	readonly turnState: string | null;
	readonly turnOpRef: string | null;
	readonly boundSessionId: string | null;
	readonly receivedAt: string;
	readonly cutoverId?: string;
}

export interface OpenLaneJob {
	readonly jobId: string;
	readonly laneKey: string;
	readonly branch: string;
	readonly worktreePath: string;
	readonly state: string;
	readonly openAttempts: LaneJobRecord["attempts"];
	readonly cutoverId?: string;
}

export interface OpenWorkAttempt {
	readonly opRef: string;
	readonly jobId: string;
	readonly laneKey: string;
	readonly sessionId: string;
	readonly sendPhase: string;
	readonly cutoverId?: string;
}

export interface OpenLaneReport {
	readonly reportId: string;
	readonly parentName: string;
	readonly childName: string;
	readonly state: string;
	readonly body: string;
	readonly cutoverId?: string;
}

export interface InvalidOpenWork {
	readonly kind: "lane_job" | "work_attempt_runtime";
	readonly jobId: string;
	readonly opRef?: string;
	readonly laneKey?: string;
	readonly stateColumn: string;
	readonly assertion: string;
	readonly recordJson: string;
	readonly recordSha256: string;
	readonly cutoverId?: string;
}

export interface OpenMonitorEvent {
	readonly eventId: string;
	readonly monitorId: string;
	readonly monitorName: string | null;
	readonly eventType: string;
	readonly stage: string;
	readonly batchId: string | null;
	readonly firedAt: string;
	readonly cutoverId?: string;
}

export interface OpenWorkReport {
	readonly inbound: readonly OpenInboundWork[];
	readonly work: {
		readonly laneJobs: readonly OpenLaneJob[];
		readonly attempts: readonly OpenWorkAttempt[];
		readonly laneReports: readonly OpenLaneReport[];
		readonly invalid: readonly InvalidOpenWork[];
	};
	readonly monitors: readonly OpenMonitorEvent[];
}

const OPEN_MONITOR_STAGES = new Set(["delivered", "authored_no_delivery", "failed_no_retry", "skipped"]);

function sha256(value: string): string {
	return createHash("sha256").update(value).digest("hex");
}

function optionalCutoverId(cutoverId: string | null): { cutoverId?: string } {
	return cutoverId === null ? {} : { cutoverId };
}

function assertionOf(error: unknown): string {
	if (typeof error === "object" && error !== null && "assertion" in error) {
		const assertion = (error as { assertion?: unknown }).assertion;
		if (typeof assertion === "string") return assertion;
	}
	return error instanceof Error ? error.message : String(error);
}

export function collectOpenWork(read: OpenWorkRead, options: { scope: OpenWorkScope }): OpenWorkReport {
	const rows = read.readOpenWorkRows(options.scope);
	const inbound: OpenInboundWork[] = rows.inbound.map((row) => {
		let originRef: OriginRef | null = null;
		try {
			originRef = JSON.parse(row.origin_ref_json) as OriginRef;
		} catch {}
		return {
			messageId: row.message_id,
			originKey: row.origin_key,
			originRef,
			body: row.body,
			source: row.source,
			state: row.state,
			turnRole: row.turn_role,
			turnState: row.turn_state,
			turnOpRef: row.turn_op_ref,
			boundSessionId: row.bound_session_id,
			receivedAt: row.received_at,
			...optionalCutoverId(row.cutover_id),
		};
	});
	const laneJobs: OpenLaneJob[] = [];
	const attempts: OpenWorkAttempt[] = [];
	const laneReports: OpenLaneReport[] = rows.laneReports.map((row) => ({
		reportId: row.report_id,
		parentName: row.parent_name,
		childName: row.child_name,
		state: row.state,
		body: row.body,
		...optionalCutoverId(row.cutover_id),
	}));
	const invalid: InvalidOpenWork[] = [];

	for (const row of rows.laneJobs) {
		let record: LaneJobRecord;
		try {
			record = parseLaneJobRecord(row.record_json);
			if (record.jobId !== row.job_id) throw new Error("lane job id matches database column");
		} catch (error) {
			invalid.push({
				kind: "lane_job",
				jobId: row.job_id,
				laneKey: row.lane_key,
				stateColumn: row.state,
				assertion: assertionOf(error),
				recordJson: row.record_json,
				recordSha256: sha256(row.record_json),
				...optionalCutoverId(row.cutover_id),
			});
			continue;
		}
		const openAttempts = record.attempts.filter((attempt) => attempt.endedAt === undefined);
		if (openAttempts.length > 0 || !["done", "aborted"].includes(record.state)) {
			laneJobs.push({
				jobId: row.job_id,
				laneKey: row.lane_key,
				branch: record.lane.branch,
				worktreePath: record.lane.worktreePath,
				state: record.state,
				openAttempts,
				...optionalCutoverId(row.cutover_id),
			});
		}
	}

	for (const row of rows.attempts) {
		let runtime: unknown;
		try {
			runtime = read.workAttemptGet(row.op_ref);
		} catch (error) {
			invalid.push({
				kind: "work_attempt_runtime",
				jobId: row.job_id,
				opRef: row.op_ref,
				laneKey: row.lane_key,
				stateColumn: row.settled_at === null ? "open" : "settled",
				assertion: assertionOf(error),
				recordJson: row.record_json,
				recordSha256: sha256(row.record_json),
				...optionalCutoverId(row.cutover_id),
			});
			continue;
		}
		if (!runtime || row.settled_at !== null) continue;
		const value = runtime as { opRef: string; jobId: string; laneKey: string; sessionId: string; sendPhase: string };
		attempts.push({
			opRef: value.opRef,
			jobId: value.jobId,
			laneKey: value.laneKey,
			sessionId: value.sessionId,
			sendPhase: value.sendPhase,
			...optionalCutoverId(row.cutover_id),
		});
	}

	return {
		inbound,
		work: { laneJobs, attempts, laneReports, invalid },
		monitors: rows.monitors
			.filter((row) => !OPEN_MONITOR_STAGES.has(row.stage))
			.map((row) => ({
				eventId: row.event_id,
				monitorId: row.monitor_id,
				monitorName: row.monitor_name,
				eventType: row.event_type,
				stage: row.stage,
				batchId: row.batch_id,
				firedAt: row.fired_at,
				...optionalCutoverId(row.cutover_id),
			})),
	};
}
