import { homeLayout, LEASE_NAMES, readLeaseOwner } from "@gajae-gateway/home";
import type { GatewayConfig } from "../config";
import { assertNoRestoreJournal, withGatewayLease } from "../offline";
import { Store } from "../store";
import type { BrokerAuthority } from "../store/groups/bindings";

export interface CensusAuthorityCounts {
	readonly authority: BrokerAuthority | null;
	readonly populated: boolean;
	readonly openInbound: number;
	readonly openWork: number;
	readonly openMonitors: number;
	readonly inspectionError?: string;
}

export interface CensusReport {
	readonly schemaVersion: number;
	readonly authority: BrokerAuthority | null;
	readonly authorityCounts: CensusAuthorityCounts;
	readonly tableCounts: Readonly<Record<string, number>>;
	readonly quickCheck: string;
	readonly leaseOwners: Readonly<Record<string, Awaited<ReturnType<typeof readLeaseOwner>>>>;
	readonly openQuarantined: Readonly<
		Record<
			string,
			{
				readonly inbound: number;
				readonly laneJobs: number;
				readonly attempts: number;
				readonly laneReports: number;
				readonly invalid: number;
				readonly monitors: number;
			}
		>
	>;
	readonly journalRowCount: number;
	readonly journalByteSize: number;
}

function addCutoverCount(
	counts: Map<
		string,
		{ inbound: number; laneJobs: number; attempts: number; laneReports: number; invalid: number; monitors: number }
	>,
	cutoverId: string | undefined,
	key: "inbound" | "laneJobs" | "attempts" | "laneReports" | "invalid" | "monitors",
): void {
	if (!cutoverId) return;
	const count = counts.get(cutoverId) ?? {
		inbound: 0,
		laneJobs: 0,
		attempts: 0,
		laneReports: 0,
		invalid: 0,
		monitors: 0,
	};
	count[key]++;
	counts.set(cutoverId, count);
}

function diagnostic(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

function authoritySummary(database: Store): CensusAuthorityCounts {
	try {
		return database.inspectBrokerAuthority();
	} catch (error) {
		const replayable = database.core.collectOpenWork("replayable");
		const tables = database.core.tableRowCounts();
		return {
			authority: database.bindings.authority,
			populated: Object.entries(tables).some(
				([name, count]) => !["schema_migrations", "broker_authority"].includes(name) && count > 0,
			),
			openInbound: replayable.inbound.length,
			openWork:
				replayable.work.laneJobs.length +
				replayable.work.attempts.length +
				replayable.work.laneReports.length +
				replayable.work.invalid.length,
			openMonitors: replayable.monitors.length,
			inspectionError: diagnostic(error),
		};
	}
}
export async function runCensus(config: GatewayConfig): Promise<CensusReport> {
	return withGatewayLease(config.home, async () => {
		await assertNoRestoreJournal(config.home);
		const database = await Store.openReadonly(homeLayout(config.home).db);
		try {
			const authorityCounts = authoritySummary(database);
			const work = database.core.collectOpenWork("quarantined");
			const quarantineCounts = new Map<
				string,
				{
					inbound: number;
					laneJobs: number;
					attempts: number;
					laneReports: number;
					invalid: number;
					monitors: number;
				}
			>();
			for (const row of work.inbound) addCutoverCount(quarantineCounts, row.cutoverId, "inbound");
			for (const row of work.work.laneJobs) addCutoverCount(quarantineCounts, row.cutoverId, "laneJobs");
			for (const row of work.work.attempts) addCutoverCount(quarantineCounts, row.cutoverId, "attempts");
			for (const row of work.work.laneReports) addCutoverCount(quarantineCounts, row.cutoverId, "laneReports");
			for (const row of work.work.invalid) addCutoverCount(quarantineCounts, row.cutoverId, "invalid");
			for (const row of work.monitors) addCutoverCount(quarantineCounts, row.cutoverId, "monitors");
			const leaseOwners = Object.fromEntries(
				await Promise.all(LEASE_NAMES.map(async (name) => [name, await readLeaseOwner(config.home, name)] as const)),
			);
			// Measure work_journal row count and byte size
			const journalDb = database.core.groupDb("journal");
			const journalStats = journalDb.prepare("SELECT COUNT(*) as cnt FROM work_journal").get() as { cnt: number };
			const journalRowCount = journalStats.cnt;

			// Measure journal byte size: sum of row_json lengths + estimated overhead per row
			// Each row has ~80 bytes overhead (seq INTEGER, entity TEXT, entity_key TEXT, event_type TEXT, recorded_at TEXT)
			const sizeStats = journalDb
				.prepare("SELECT COALESCE(SUM(LENGTH(row_json)), 0) as json_size FROM work_journal")
				.get() as { json_size: number };
			const journalByteSize = (sizeStats?.json_size ?? 0) + journalRowCount * 80;

			const report: CensusReport = {
				schemaVersion: database.schemaVersion,
				authority: authorityCounts.authority,
				authorityCounts,
				tableCounts: database.core.tableRowCounts(),
				quickCheck: database.core.quickCheckDetail(),
				leaseOwners,
				openQuarantined: Object.fromEntries(
					[...quarantineCounts.entries()].sort(([left], [right]) => left.localeCompare(right)),
				),
				journalRowCount,
				journalByteSize,
			};
			console.log(JSON.stringify(report, null, 2));
			return report;
		} finally {
			database.close();
		}
	});
}
