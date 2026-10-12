import { createHash, randomUUID } from "node:crypto";
import { chmodSync, closeSync, fsyncSync, lstatSync, openSync, readSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import type { BrokerAuthority } from "../store/groups/bindings";
import type { OpenWorkReport } from "../store/open-work";

export interface OpenWorkExportInput extends OpenWorkReport {
	readonly reason: "cutover" | "migration-33";
	readonly schemaVersion: number;
	readonly authority: BrokerAuthority | null;
	readonly createdAt?: string;
}

export interface OpenWorkExportCounts {
	readonly inbound: number;
	readonly work: {
		readonly laneJobs: number;
		readonly attempts: number;
		readonly laneReports: number;
		readonly invalid: number;
	};
	readonly monitors: number;
}

export interface OpenWorkExportReceipt {
	readonly path: string;
	readonly sha256: string;
	readonly counts: OpenWorkExportCounts;
}

function hash(bytes: Uint8Array): string {
	return createHash("sha256").update(bytes).digest("hex");
}

function hashFile(path: string): string {
	const fd = openSync(path, "r");
	const bytes = Buffer.allocUnsafe(64 * 1024);
	const digest = createHash("sha256");
	try {
		for (;;) {
			const count = readSync(fd, bytes, 0, bytes.length, null);
			if (count === 0) break;
			digest.update(bytes.subarray(0, count));
		}
		return digest.digest("hex");
	} finally {
		closeSync(fd);
	}
}

function syncDirectory(path: string): void {
	const fd = openSync(path, "r");
	try {
		fsyncSync(fd);
	} finally {
		closeSync(fd);
	}
}

function assertAbsent(path: string): void {
	try {
		lstatSync(path);
		throw new Error(`open_work_export_exists path=${path}`);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}
}

function cutoverIds(report: OpenWorkReport): string[] {
	const ids = new Set<string>();
	const remember = (row: { readonly cutoverId?: string }) => {
		if (row.cutoverId) ids.add(row.cutoverId);
	};
	for (const row of report.inbound) remember(row);
	for (const row of report.work.laneJobs) remember(row);
	for (const row of report.work.attempts) remember(row);
	for (const row of report.work.laneReports) remember(row);
	for (const row of report.work.invalid) remember(row);
	for (const row of report.monitors) remember(row);
	return [...ids].sort();
}

export function writeOpenWorkExport(backupPath: string, report: OpenWorkExportInput): OpenWorkExportReceipt {
	const backup = resolve(backupPath);
	if (!backup.endsWith(".db")) throw new Error("open_work_export backup path must end in .db");
	const outputPath = backup.slice(0, -3) + ".open-work.json";
	const directory = dirname(outputPath);
	assertAbsent(outputPath);
	const backupSha256 = hashFile(backup);
	const counts: OpenWorkExportCounts = {
		inbound: report.inbound.length,
		work: {
			laneJobs: report.work.laneJobs.length,
			attempts: report.work.attempts.length,
			laneReports: report.work.laneReports.length,
			invalid: report.work.invalid.length,
		},
		monitors: report.monitors.length,
	};
	const document = {
		kind: "gajaeway.open-work-export",
		version: 1,
		reason: report.reason,
		createdAt: report.createdAt ?? new Date().toISOString(),
		schemaVersion: report.schemaVersion,
		authority: report.authority,
		backup: { path: backup, sha256: backupSha256 },
		cutoverIds: cutoverIds(report),
		counts,
		inbound: report.inbound,
		work: report.work,
		monitors: report.monitors,
	};
	const bytes = Buffer.from(`${JSON.stringify(document, null, 2)}\n`);
	const temporary = `${outputPath}.tmp-${randomUUID()}`;
	try {
		writeFileSync(temporary, bytes, { mode: 0o600, flag: "w" });
		chmodSync(temporary, 0o600);
		const fd = openSync(temporary, "r+");
		try {
			fsyncSync(fd);
		} finally {
			closeSync(fd);
		}
		renameSync(temporary, outputPath);
		syncDirectory(directory);
	} catch (error) {
		try {
			unlinkSync(temporary);
		} catch {}
		throw error;
	}
	return { path: outputPath, sha256: hash(bytes), counts };
}
