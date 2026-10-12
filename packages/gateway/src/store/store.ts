import { parseLaneJobRecord } from "@gajae-gateway/subsession";
import { BindingsGroup, type BrokerAuthority } from "./groups/bindings";
import { InboxGroup, RETENTION_BATCH_ROWS } from "./groups/inbox";
import { OutboxGroup } from "./groups/outbox";
import { WorkAttemptStateError, WorkGroup } from "./groups/work";
import { MonitorsGroup } from "./groups/monitors";
import { MemoryGroup } from "./groups/memory";
import { StoreCore, type StoreOpenOptions } from "./core";
import { TABLE_OWNER, type OwnedTable } from "./ownership";
import type { WriteTx } from "./tx";

export const HISTORY_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
export const CONFIRMED_DELIVERY_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;

export interface RetentionSweepResult {
	readonly deleted: Readonly<Record<string, number>>;
	readonly more: boolean;
}

export interface BrokerAuthorityCensus {
	readonly authority: BrokerAuthority | null;
	readonly populated: boolean;
	readonly openInbound: number;
	readonly openWork: number;
	readonly openMonitors: number;
}

/** Owner groups sharing one transaction boundary and one SQLite connection. */
export class Store {
	readonly core: StoreCore;
	readonly bindings: BindingsGroup;
	readonly outbox: OutboxGroup;
	readonly inbox: InboxGroup;
	readonly work: WorkGroup;
	readonly monitors: MonitorsGroup;
	readonly memory: MemoryGroup;

	constructor(core: StoreCore) {
		this.core = core;
		this.bindings = new BindingsGroup(core);
		this.outbox = new OutboxGroup(core);
		this.inbox = new InboxGroup(core);
		this.work = new WorkGroup(core);
		this.monitors = new MonitorsGroup(core);
		this.memory = new MemoryGroup(core);
	}

	static async open(path: string, options: StoreOpenOptions = {}): Promise<Store> {
		return new Store(await StoreCore.open(path, options));
	}

	static async openReadonly(path: string): Promise<Store> {
		return new Store(await StoreCore.openReadonly(path));
	}

	static async openAdministrative(path: string): Promise<Store> {
		return new Store(await StoreCore.openAdministrative(path));
	}

	get schemaVersion(): number {
		return this.core.schemaVersion;
	}

	get instanceId(): string {
		return this.core.instanceId;
	}

	transaction<T>(fn: (tx: WriteTx) => T): T {
		return this.core.transaction(fn);
	}

	inspectBrokerAuthority(): BrokerAuthorityCensus {
		let populated = false;
		for (const table of Object.keys(TABLE_OWNER) as OwnedTable[]) {
			if (table !== "schema_migrations" && this.core.tableHasRows(table)) populated = true;
		}
		try {
			for (const row of this.bindings.sessionEpochs()) {
				if (!Number.isSafeInteger(row.epoch) || row.epoch < 0 || row.epoch >= Number.MAX_SAFE_INTEGER)
					throw new Error("invalid session epoch");
			}
		} catch (error) {
			if (!(error as Error).message.includes("no such table")) throw error;
		}
		let openWork = 0;
		try {
			for (const row of this.work.laneJobHistoryRows()) {
				const job = parseLaneJobRecord(row.record_json);
				if (job.jobId !== row.job_id) throw new WorkAttemptStateError();
				if (
					!this.bindings.isQuarantined("work", job.jobId) &&
					(job.attempts.some((attempt) => attempt.endedAt === undefined) || !["done", "aborted"].includes(job.state))
				)
					openWork++;
			}
			for (const row of this.work.workAttemptRows()) {
				const runtime = this.work.workAttemptGet(row.op_ref)!;
				if (runtime.settledAt === null && !this.bindings.isQuarantined("work", runtime.jobId)) openWork++;
			}
			openWork += this.work.laneReportOpenCount();
		} catch (error) {
			if (!(error as Error).message.includes("no such table")) throw error;
		}
		let openInbound = 0;
		try {
			openInbound = this.inbox.openCount();
		} catch (error) {
			if (!(error as Error).message.includes("no such table")) throw error;
		}
		let openMonitors = 0;
		try {
			openMonitors = this.monitors.openCount();
		} catch (error) {
			if (!(error as Error).message.includes("no such table")) throw error;
		}
		return { authority: this.bindings.authority, populated, openInbound, openWork, openMonitors };
	}

	sweep(now = new Date(), batch = RETENTION_BATCH_ROWS): RetentionSweepResult {
		const cutoffs = {
			historyCutoff: new Date(now.getTime() - HISTORY_RETENTION_MS).toISOString(),
			deliveryCutoff: new Date(now.getTime() - CONFIRMED_DELIVERY_RETENTION_MS).toISOString(),
			batch,
		};
		return this.transaction((tx) => {
			const deleted: Record<string, number> = {};
			let more = false;
			const note = (table: string, count: number) => {
				deleted[table] = count;
				if (count >= batch) more = true;
			};
			note("inbound_messages", this.inbox.sweep(tx, cutoffs));
			const monitors = this.monitors.sweep(tx, cutoffs);
			note("monitor_events", monitors.events);
			note("monitor_slots", monitors.slots);
			note("deliveries", this.outbox.sweep(tx, cutoffs));
			note("lane_reports", this.work.sweep(tx, cutoffs));
			return { deleted, more };
		});
	}

	close(): void {
		this.core.close();
	}
}
