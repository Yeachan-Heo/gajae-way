import { chmod, lstat, mkdir, realpath } from "node:fs/promises";
import { hostname } from "node:os";
import { join } from "node:path";
import { homeLayout } from "@gajae-gateway/home";
import type { GatewayConfig } from "../config";
import { assertNoRestoreJournal, withGatewayLease } from "../offline";
import { Store } from "../store";
import type { StoreCore } from "../store/core";
import type { BrokerAuthority } from "../store/groups/bindings";
import { writeBackup } from "./backup";
import { writeOpenWorkExport } from "./open-work-export";
export interface CutoverReceipt {
	readonly snapshotId: string;
	readonly oldAuthority: BrokerAuthority | null;
	readonly targetAuthority: BrokerAuthority;
	readonly backup: { readonly path: string; readonly sha256: string };
	readonly openWorkExport: {
		readonly path: string;
		readonly sha256: string;
		readonly counts: ReturnType<typeof writeOpenWorkExport>["counts"];
	};
}

function diagnostic(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

async function prepareBackupsDir(home: string): Promise<string> {
	const path = homeLayout(home).backupsDir;
	await mkdir(path, { recursive: true, mode: 0o700 });
	const info = await lstat(path);
	if (!info.isDirectory() || info.isSymbolicLink()) throw new Error(`backup_directory_invalid path=${path}`);
	await chmod(path, 0o700);
	return path;
}

export async function runCutover(input: {
	readonly config: GatewayConfig;
	readonly agentDir: string;
}): Promise<number> {
	const { config, agentDir } = input;
	try {
		return await withGatewayLease(config.home, async () => {
			await assertNoRestoreJournal(config.home);
			const targetPath = await realpath(agentDir);
			if (!config.gjc?.agentDir || targetPath !== config.gjc.agentDir) {
				throw new Error("cutover_config_mismatch: --agent-dir must resolve to config.gjc.agentDir");
			}
			const targetAuthority: BrokerAuthority = {
				canonicalAgentDir: targetPath,
				identity: `gjc:${targetPath}`,
			};
			const database = await Store.openAdministrative(homeLayout(config.home).db);
			try {
				const recordedAuthority = database.bindings.authority;
				if (
					recordedAuthority !== null &&
					recordedAuthority.canonicalAgentDir === targetAuthority.canonicalAgentDir &&
					recordedAuthority.identity === targetAuthority.identity
				)
					throw new Error("cutover_already_targets_agent_dir");
				if (database.bindings.hasCutoverTarget(targetAuthority)) throw new Error("cutover_target_reused");

				const backupsDir = await prepareBackupsDir(config.home);
				const stamp = new Date().toISOString().replace(/[:.]/g, "-");
				const backupPath = join(backupsDir, `pre-cutover-${stamp}.db`);
				const backup = await writeBackup(database.core, backupPath);
				let openWork: ReturnType<StoreCore["collectOpenWork"]>;
				let openWorkExport: ReturnType<typeof writeOpenWorkExport>;
				try {
					openWork = database.core.collectOpenWork("replayable");
					openWorkExport = writeOpenWorkExport(backup.path, {
						...openWork,
						reason: "cutover",
						schemaVersion: database.schemaVersion,
						authority: recordedAuthority,
					});
					console.info(
						`open_work_exported reason=cutover path=${openWorkExport.path} sha256=${openWorkExport.sha256} counts=${JSON.stringify(openWorkExport.counts)}`,
					);
				} catch (error) {
					console.error(`open_work_export_failed ${diagnostic(error)}`);
					return 1;
				}
				let census: ReturnType<Store["inspectBrokerAuthority"]>;
				try {
					census = database.inspectBrokerAuthority();
				} catch (error) {
					census = {
						authority: recordedAuthority,
						populated: true,
						openInbound: openWork.inbound.length,
						openWork:
							openWork.work.laneJobs.length +
							openWork.work.attempts.length +
							openWork.work.laneReports.length +
							openWork.work.invalid.length,
						openMonitors: openWork.monitors.length,
					};
					console.error(`cutover_census_invalid ${diagnostic(error)}`);
				}
				const report = openWork;
				console.log(
					JSON.stringify(
						{ schemaVersion: database.schemaVersion, authority: recordedAuthority, counts: census, openWork: report },
						null,
						2,
					),
				);
				const at = new Date().toISOString();
				const evidence = {
					host: hostname(),
					at,
					backup: backup.path,
					sha256: backup.sha256,
					census,
					openWorkExport,
				};
				const snapshotId = database.transaction((tx) =>
					database.bindings.cutoverBrokerAuthority(tx, {
						expectedAuthority: recordedAuthority,
						targetAuthority,
						evidence: JSON.stringify(evidence),
						disposition: "quarantine",
					}),
				);
				const receipt: CutoverReceipt = {
					snapshotId,
					oldAuthority: recordedAuthority,
					targetAuthority,
					backup: { path: backup.path, sha256: backup.sha256 },
					openWorkExport,
				};
				console.log(JSON.stringify({ event: "cutover_receipt", ...receipt }, null, 2));
				return 0;
			} finally {
				database.close();
			}
		});
	} catch (error) {
		console.error(diagnostic(error));
		return 1;
	}
}
