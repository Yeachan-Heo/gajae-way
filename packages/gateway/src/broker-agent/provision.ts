import { randomUUID } from "node:crypto";
import { chmod, copyFile, lstat, mkdir, mkdtemp, open, readFile, readdir, realpath, rename, rm, writeFile } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, resolve, sep } from "node:path";
import type { Lease } from "@gajae-gateway/home";
import { homeLayout } from "@gajae-gateway/home";
import { defaultPidAlive } from "../orchestrator/broker";
import { judgeBrokerLiveness } from "../orchestrator/broker-liveness";
import {
	BROKER_AGENT_ALLOWLIST,
	type BrokerAgentAllowlistEntry,
	brokerAgentEntryExists,
	brokerAgentEntryPath,
	fingerprintBrokerAgentEntry,
	hasLiveBrokerUrl,
	resolveBrokerAgentDataSource,
	resolveBrokerAgentSource,
	sourceEntryRootSync,
	sourceEntryStat,
} from "./allowlist";
import { assertAgentDb, snapshotAgentDb } from "./agent-db";
import { brokerAgentDrift, type BrokerAgentEntryState } from "./drift";

export type BrokerAgentErrorCode =
	| "broker_agent_unprovisioned"
	| "broker_agent_unrecoverable"
	| "broker_agent_invalid_target"
	| "broker_agent_live_link"
	| "broker_still_running"
	| "agent_source_missing"
	| "agent_source_live_link"
	| "broker_agent_symlink"
	| "broker_agent_invalid_entry"
	| "broker_agent_failed_restored"
	| "broker_lease_required"

export class BrokerAgentError extends Error {
	constructor(
		readonly code: BrokerAgentErrorCode,
		message: string,
	) {
		super(`${code}${message ? ` ${message}` : ""}`);
		this.name = "BrokerAgentError";
	}
}

export type BrokerAgentProvisionMode = "force" | "update";
export type BrokerProvisionFault =
	| "after_staging"
	| "after_journal"
	| `after_old_${string}`
	| `after_new_${string}`
	| "before_manifest"
	| "after_manifest";

export interface ProvisionBrokerAgentOptions {
	readonly home: string;
	readonly agentDir: string;
	readonly source: string;
	readonly mode: BrokerAgentProvisionMode;
	readonly lease: Lease;
	/** Fault seam used by process-crash tests; production callers leave this unset. */
	readonly fault?: (point: BrokerProvisionFault) => void | Promise<void>;
}

export interface BrokerAgentProvisionResult {
	readonly status: "ok" | "nothing_to_do";
	readonly genId?: string;
	readonly entries: readonly { readonly name: BrokerAgentAllowlistEntry["name"]; readonly kind: string; readonly state: BrokerAgentEntryState }[];
}

interface FileIdentity {
	readonly dev: number;
	readonly ino: number;
	readonly size: number;
}
interface JournalEntry {
	readonly original: Record<string, FileIdentity | null>;
	readonly stagedIdentity?: FileIdentity;
}
interface ProvisionJournal {
	readonly version: 1;
	readonly genId: string;
	readonly phase: "swap";
	readonly selected: readonly string[];
	readonly stagedDir: string;
	readonly prevDir: string;
	readonly entries: Record<string, JournalEntry>;
}
interface ManifestEntry {
	readonly sourceFingerprint: string;
	readonly copyFingerprint: string;
	readonly sourceStat: { readonly size: number; readonly mtimeNs: string };
	readonly copiedAt: string;
}
interface BrokerAgentManifest {
	readonly version: 2;
	readonly genId: string;
	readonly source: string;
	readonly dataSource: string;
	readonly agentDir: string;
	readonly provisionedAt: string;
	readonly by: string;
	readonly entries: Record<string, ManifestEntry>;
}

const DB_FILES = ["agent.db", "agent.db-wal", "agent.db-shm"] as const;
const DIRECTORY_MODE = 0o700;
const FILE_MODE = 0o600;

export async function provisionBrokerAgent(options: ProvisionBrokerAgentOptions): Promise<BrokerAgentProvisionResult> {
	assertBrokerLease(options.lease);
	const home = resolve(options.home);
	const agentDir = resolve(options.agentDir);
	if (!isAbsolute(options.agentDir)) throw new BrokerAgentError("broker_agent_invalid_target", "agentDir must be absolute");
	const source = resolveBrokerAgentSource(options.source);
	const sourceReal = await realpath(source).catch(() => {
		throw new BrokerAgentError("agent_source_missing", `source=${source}`);
	});
	const homeReal = await realpath(home);
	const targetReal = await realpath(agentDir).catch(() => agentDir);
	const dataSource = resolveBrokerAgentDataSource(source);
	const dataSourceReal = await realpath(dataSource).catch(() => sourceReal);
	if (
		pathsOverlap(sourceReal, targetReal) ||
		pathsOverlap(sourceReal, homeReal) ||
		pathsOverlap(dataSourceReal, targetReal) ||
		pathsOverlap(dataSourceReal, homeReal)
	)
		throw new BrokerAgentError("agent_source_live_link", "source overlaps broker home or agent directory");
	const sourceConfig = join(source, "config.yml");
	try {
		if (hasLiveBrokerUrl(await readFile(sourceConfig, "utf8")))
			throw new BrokerAgentError("agent_source_live_link", "config.yml sets a live broker URL");
	} catch (error) {
		if (error instanceof BrokerAgentError) throw error;
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}
	const sourcePresence = new Map<string, boolean>();
	for (const entry of BROKER_AGENT_ALLOWLIST) {
		const root = sourceEntryRootSync(entry, source, dataSource);
		let present: boolean;
		try {
			present = await brokerAgentEntryExists(entry, root);
		} catch (error) {
			throw brokerAgentEntryRefusal(error);
		}
		sourcePresence.set(entry.name, present);
		if (entry.required && !present) throw new BrokerAgentError("agent_source_missing", `entry=${entry.name}`);
	}
	const layout = homeLayout(home);
	await assertNoSymlinkAncestors(agentDir);
	await assertNoSymlinkAncestors(layout.brokerProvisionDir);
	if (targetReal !== agentDir) throw new BrokerAgentError("broker_agent_invalid_target", "agentDir resolves through a symlink");
	if (await pathExists(agentDir)) await assertAgentDirectory(agentDir);
	else await ensureAgentDirectory(agentDir);
	await assertAgentDirectory(agentDir);
	await validateTargetEntries(agentDir);
	await ensureProvisionDirectory(home);
	const live = await judgeBrokerLiveness(join(agentDir, "sdk", "broker.json"), defaultPidAlive);
	if (live.state === "live") throw new BrokerAgentError("broker_still_running", `pid=${live.pid}`);
	await recoverProvision(home, agentDir, options.lease);
	await readManifest(layout.brokerProvisionManifest);

	const drift = await brokerAgentDrift({ home, agentDir, source });
	const selected = BROKER_AGENT_ALLOWLIST.filter((entry) => {
		if (!sourcePresence.get(entry.name)) return false;
		if (options.mode === "force") return true;
		const state = drift.entries.find((item) => item.name === entry.name)?.state;
		return state === "source_changed" || state === "missing_copy";
	});
	const selectedNames = new Set(selected.map((entry) => entry.name));
	const resultEntries: BrokerAgentProvisionResult["entries"] = BROKER_AGENT_ALLOWLIST.map((entry) => ({
		name: entry.name,
		kind: entry.kind,
		state: selectedNames.has(entry.name) ? "in_sync" : drift.entries.find((item) => item.name === entry.name)?.state ?? "absent",
	}));
	if (selected.length === 0) {
		console.info(`broker_agent_sync copied=0 kept=${resultEntries.length} exit=0`);
		return { status: "nothing_to_do", entries: resultEntries };
	}

	const stagedDir = await mkdtemp(join(layout.brokerProvisionDir, "gen-"));
	const genId = basename(stagedDir).slice("gen-".length);
	const prevDir = join(layout.brokerProvisionDir, `prev-${genId}`);
	const stagedMetadata = new Map<string, { sourceFingerprint: string; copyFingerprint: string; sourceStat: ManifestEntry["sourceStat"] }>();
	let journalPersisted = false;
	try {
		for (const entry of selected) {
			const sourceRoot = sourceEntryRootSync(entry, source, dataSource);
			const sourcePath = brokerAgentEntryPath(entry, sourceRoot);
			const stagePath = brokerAgentEntryPath(entry, stagedDir);
			await stageEntry(entry, sourcePath, stagePath);
			const copyFingerprint = await fingerprintBrokerAgentEntry(entry, stagePath);
			const sourceFingerprint =
				entry.form === "sqlite" ? copyFingerprint : await fingerprintBrokerAgentEntry(entry, sourcePath);
			const sourceStat = await sourceEntryStat(entry, sourceRoot);
			stagedMetadata.set(entry.name, { sourceFingerprint, copyFingerprint, sourceStat });
		}
		await syncDirectory(stagedDir);
		await options.fault?.("after_staging");

		const journalEntries: Record<string, JournalEntry> = {};
		for (const entry of selected) {
			const original: Record<string, FileIdentity | null> = {};
			for (const name of targetComponents(entry)) original[name] = await identityOrNull(join(agentDir, name));
			const stagedIdentity = await identityOrNull(brokerAgentEntryPath(entry, stagedDir));
			if (!stagedIdentity) throw new BrokerAgentError("broker_agent_unrecoverable", `staged entry disappeared: ${entry.name}`);
			journalEntries[entry.name] = { original, stagedIdentity };
		}
		await mkdir(prevDir, { mode: DIRECTORY_MODE });
		await chmod(prevDir, DIRECTORY_MODE);
		const journal: ProvisionJournal = {
			version: 1,
			genId,
			phase: "swap",
			selected: selected.map((entry) => entry.name),
			stagedDir,
			prevDir,
			entries: journalEntries,
		};
		await writeJsonAtomic(layout.brokerProvisionDir, join(layout.brokerProvisionDir, "journal.json"), journal);
		journalPersisted = true;
		await options.fault?.("after_journal");

		for (const entry of selected) {
			for (const name of targetComponents(entry)) {
				if (!journalEntries[entry.name]?.original[name]) continue;
				await rename(join(agentDir, name), join(prevDir, name));
				await syncDirectory(agentDir);
				await syncDirectory(prevDir);
				await options.fault?.(`after_old_${name}`);
			}
			await rename(brokerAgentEntryPath(entry, stagedDir), brokerAgentEntryPath(entry, agentDir));
			await syncDirectory(stagedDir);
			await syncDirectory(agentDir);
			await options.fault?.(`after_new_${entry.name}`);
		}
		await assertAgentDb(join(agentDir, "agent.db"));
		await options.fault?.("before_manifest");

		const previous = await readManifest(layout.brokerProvisionManifest);
		const entries: Record<string, ManifestEntry> = { ...(previous?.entries ?? {}) };
		for (const entry of selected) {
			const metadata = stagedMetadata.get(entry.name);
			if (!metadata) throw new BrokerAgentError("broker_agent_unrecoverable", `staged metadata missing: ${entry.name}`);
			entries[entry.name] = { ...metadata, copiedAt: new Date().toISOString() };
		}
		const manifest: BrokerAgentManifest = {
			version: 2,
			genId,
			source: sourceReal,
			dataSource: dataSourceReal,
			agentDir: targetReal,
			provisionedAt: new Date().toISOString(),
			by: process.env.USER ?? "gajaeway",
			entries,
		};
		await writeJsonAtomic(layout.brokerProvisionDir, layout.brokerProvisionManifest, manifest);
		await options.fault?.("after_manifest");
		await finishCommit(layout.brokerProvisionDir, journal);
		logProvisioned(genId, manifest.by, selected.map((entry) => entry.name), BROKER_AGENT_ALLOWLIST.length - selected.length);
		return { status: "ok", genId, entries: resultEntries };
	} catch (error) {
		if (!journalPersisted) {
			await rm(stagedDir, { recursive: true, force: true }).catch(() => {});
			await rm(prevDir, { recursive: true, force: true }).catch(() => {});
			throw asBrokerAgentError(error);
		}
		try {
			const outcome = await recoverProvision(home, agentDir, options.lease);
			const manifest = await readManifest(layout.brokerProvisionManifest);
			if (manifest?.genId === genId || outcome === "finished") {
				logProvisioned(genId, manifest?.by ?? process.env.USER ?? "gajaeway", selected.map((entry) => entry.name), BROKER_AGENT_ALLOWLIST.length - selected.length);
				return { status: "ok", genId, entries: resultEntries };
			}
			throw new BrokerAgentError("broker_agent_failed_restored", errorMessage(error));
		} catch (recoveryError) {
			if (recoveryError instanceof BrokerAgentError && recoveryError.code === "broker_agent_failed_restored") throw recoveryError;
			throw new BrokerAgentError("broker_agent_unrecoverable", `cause=${errorMessage(error)} recovery=${errorMessage(recoveryError)}`);
		}
	}
}

export async function recoverProvision(home: string, agentDir: string, lease: Lease): Promise<"none" | "rolled_back" | "finished"> {
	assertBrokerLease(lease);
	const layout = homeLayout(resolve(home));
	const journalPath = join(layout.brokerProvisionDir, "journal.json");
	try {
		await assertNoSymlinkAncestors(layout.brokerProvisionDir);
	} catch (error) {
		throw new BrokerAgentError("broker_agent_unrecoverable", `provision path invalid: ${errorMessage(error)}`);
	}
	let journalInfo;
	try {
		journalInfo = await lstat(journalPath);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") {
			await cleanupOrphanProvisionDirectories(layout.brokerProvisionDir);
			console.info("broker_agent_recovered phase=none");
			return "none";
		}
		throw new BrokerAgentError("broker_agent_unrecoverable", `journal stat failed: ${errorMessage(error)}`);
	}
	if (journalInfo.isSymbolicLink() || !journalInfo.isFile() || journalInfo.nlink !== 1 || (journalInfo.mode & 0o777) !== FILE_MODE)
		throw new BrokerAgentError("broker_agent_unrecoverable", "journal must be a regular 0600 file with one link");
	let raw: string;
	try {
		raw = await readFile(journalPath, "utf8");
	} catch (error) {
		throw new BrokerAgentError("broker_agent_unrecoverable", `journal read failed: ${errorMessage(error)}`);
	}
	let journal: ProvisionJournal;
	try {
		journal = parseJournal(JSON.parse(raw), layout.brokerProvisionDir);
		await assertProvisionJournalDirectory(journal.stagedDir);
		await assertProvisionJournalDirectory(journal.prevDir);
	} catch (error) {
		throw new BrokerAgentError("broker_agent_unrecoverable", `journal invalid: ${errorMessage(error)}`);
	}
	const manifest = await readManifest(layout.brokerProvisionManifest).catch((error) => {
		throw new BrokerAgentError("broker_agent_unrecoverable", `manifest read failed: ${errorMessage(error)}`);
	});
	if (manifest?.genId === journal.genId) {
		try {
			await assertAgentDb(join(resolve(agentDir), "agent.db"));
			await finishCommit(layout.brokerProvisionDir, journal);
			console.info("broker_agent_recovered phase=finished");
			return "finished";
		} catch (error) {
			throw new BrokerAgentError("broker_agent_unrecoverable", `committed generation invalid: ${errorMessage(error)}`);
		}
	}
	try {
		await rollbackJournal(resolve(agentDir), layout.brokerProvisionDir, journal);
		if (manifest) await assertAgentDb(join(resolve(agentDir), "agent.db"));
		console.info("broker_agent_recovered phase=rolled_back");
		return "rolled_back";
	} catch (error) {
		throw new BrokerAgentError("broker_agent_unrecoverable", `rollback failed: ${errorMessage(error)}`);
	}
}

function logProvisioned(genId: string, by: string, copied: readonly string[], kept: number): void {
	console.info(`broker_agent_provisioned genId=${genId} by=${by} entries=${copied.join(",")}`);
	console.info(`broker_agent_sync copied=${copied.length} kept=${kept} exit=0`);
}

export async function assertAgentDirectory(agentDir: string): Promise<void> {
	let info;
	try {
		info = await lstat(agentDir);
	} catch (error) {
		throw new BrokerAgentError("broker_agent_invalid_target", `agentDir unavailable: ${(error as NodeJS.ErrnoException).code ?? "unknown"}`);
	}
	if (info.isSymbolicLink() || !info.isDirectory() || (info.mode & 0o777) !== DIRECTORY_MODE)
		throw new BrokerAgentError("broker_agent_invalid_target", "agentDir must be a real directory with mode 0700");
	const canonical = await realpath(agentDir);
	if (canonical !== resolve(agentDir)) throw new BrokerAgentError("broker_agent_invalid_target", "agentDir resolves through a symlink");
}

export function assertBrokerLease(lease: Lease): void {
	if (
		!lease ||
		typeof lease !== "object" ||
		lease.name !== "broker" ||
		typeof lease.token !== "string" ||
		typeof lease.path !== "string" ||
		typeof lease.release !== "function"
	)
		throw new BrokerAgentError("broker_lease_required", "a held broker lease is required");
}

async function ensureAgentDirectory(agentDir: string): Promise<void> {
	try {
		await mkdir(agentDir, { mode: DIRECTORY_MODE });
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "EEXIST") {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
			await mkdir(dirname(agentDir), { recursive: true, mode: DIRECTORY_MODE });
			await mkdir(agentDir, { mode: DIRECTORY_MODE });
		}
	}
	await assertAgentDirectory(agentDir);
}
async function pathExists(path: string): Promise<boolean> {
	try {
		await lstat(path);
		return true;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
		throw error;
	}
}

async function assertNoSymlinkAncestors(path: string): Promise<void> {
	let current = resolve(path);
	for (;;) {
		try {
			if ((await lstat(current)).isSymbolicLink())
				throw new BrokerAgentError("broker_agent_invalid_target", `symlink path component=${current}`);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		}
		const parent = dirname(current);
		if (parent === current) return;
		current = parent;
	}
}

async function assertProvisionJournalDirectory(path: string): Promise<void> {
	try {
		const info = await lstat(path);
		if (info.isSymbolicLink() || !info.isDirectory() || (info.mode & 0o777) !== DIRECTORY_MODE)
			throw new BrokerAgentError("broker_agent_unrecoverable", `journal directory invalid path=${path}`);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}
}

async function cleanupOrphanProvisionDirectories(provisionDir: string): Promise<void> {
	let names: string[];
	try {
		names = await readdir(provisionDir);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
		throw new BrokerAgentError("broker_agent_unrecoverable", `provision directory read failed: ${errorMessage(error)}`);
	}
	let removed = false;
	for (const name of names) {
		if (!/^(?:gen|prev)-[A-Za-z0-9_-]{6}$/.test(name)) continue;
		const path = join(provisionDir, name);
		const info = await lstat(path);
		if (info.isSymbolicLink() || !info.isDirectory() || (info.mode & 0o777) !== DIRECTORY_MODE)
			throw new BrokerAgentError("broker_agent_unrecoverable", `orphan generation invalid path=${path}`);
		await rm(path, { recursive: true });
		removed = true;
	}
	if (removed) await syncDirectory(provisionDir);
}

async function validateTargetEntries(agentDir: string): Promise<void> {
	try {
		for (const entry of BROKER_AGENT_ALLOWLIST) await brokerAgentEntryExists(entry, agentDir);
	} catch (error) {
		throw brokerAgentEntryRefusal(error);
	}
	for (const name of DB_FILES) await identityOrNull(join(agentDir, name));
}

function brokerAgentEntryRefusal(error: unknown): BrokerAgentError {
	if (error instanceof BrokerAgentError) return error;
	const message = error instanceof Error ? error.message : String(error);
	if (message.startsWith("broker_agent_symlink")) return new BrokerAgentError("broker_agent_symlink", message);
	if (message.startsWith("broker_agent_invalid_entry")) return new BrokerAgentError("broker_agent_invalid_entry", message);
	throw error;
}


async function ensureProvisionDirectory(home: string): Promise<void> {
	const layout = homeLayout(home);
	await mkdir(layout.brokerProvisionDir, { recursive: true, mode: DIRECTORY_MODE });
	await chmod(dirname(layout.brokerProvisionDir), DIRECTORY_MODE);
	await chmod(layout.brokerProvisionDir, DIRECTORY_MODE);
}

function pathsOverlap(left: string, right: string): boolean {
	const a = resolve(left);
	const b = resolve(right);
	return a === b || a.startsWith(`${b}${sep}`) || b.startsWith(`${a}${sep}`);
}

async function stageEntry(entry: BrokerAgentAllowlistEntry, sourcePath: string, stagePath: string): Promise<void> {
	if (entry.form === "sqlite") {
		snapshotAgentDb(sourcePath, stagePath);
		await chmod(stagePath, FILE_MODE);
		await syncFile(stagePath);
		return;
	}
	if (entry.form === "group") {
		await mkdir(stagePath, { mode: DIRECTORY_MODE });
		await chmod(stagePath, DIRECTORY_MODE);
		for (const name of entry.files) {
			const sourceFile = join(sourcePath, name);
			try {
				const info = await lstat(sourceFile);
				if (info.isSymbolicLink() || !info.isFile()) throw new BrokerAgentError("broker_agent_symlink", `entry=${entry.name}/${name}`);
				await copyFile(sourceFile, join(stagePath, name));
				await chmod(join(stagePath, name), FILE_MODE);
				await syncFile(join(stagePath, name));
			} catch (error) {
				if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
			}
		}
		if ((await readdir(stagePath)).length === 0)
			throw new BrokerAgentError("agent_source_missing", `entry=${entry.name}`);
		await syncDirectory(stagePath);
		return;
	}
	const info = await lstat(sourcePath);
	if (info.isSymbolicLink() || !info.isFile()) throw new BrokerAgentError("broker_agent_symlink", `entry=${entry.name}`);
	await copyFile(sourcePath, stagePath);
	await chmod(stagePath, FILE_MODE);
	await syncFile(stagePath);
}

function targetComponents(entry: BrokerAgentAllowlistEntry): readonly string[] {
	return entry.name === "agent.db" ? DB_FILES : [entry.name];
}

async function identityOrNull(path: string): Promise<FileIdentity | null> {
	try {
		const info = await lstat(path);
		if (info.isSymbolicLink() || (!info.isFile() && !info.isDirectory()))
			throw new BrokerAgentError("broker_agent_invalid_entry", `path=${path}`);
		if (info.isFile() && info.nlink !== 1) throw new BrokerAgentError("broker_agent_invalid_entry", `hard_link=${path}`);
		return { dev: info.dev, ino: info.ino, size: info.size };
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
		throw error;
	}
}

async function rollbackJournal(agentDir: string, provisionDir: string, journal: ProvisionJournal): Promise<void> {
	await validateRollbackJournal(agentDir, journal);
	for (const name of journal.selected) {
		const entry = entryByName(name);
		const record = journal.entries[name];
		if (!entry || !record) throw new BrokerAgentError("broker_agent_unrecoverable", `unknown journal entry=${name}`);
		for (const component of targetComponents(entry)) {
			const targetPath = join(agentDir, component);
			const previousPath = join(journal.prevDir, component);
			const targetIdentity = await identityOrNull(targetPath);
			const oldIdentity = record.original[component] ?? null;
			const stageIdentity = component === entry.name ? record.stagedIdentity : undefined;
			if (targetIdentity && sameIdentity(targetIdentity, stageIdentity)) await rm(targetPath, { recursive: true, force: true });
			if (oldIdentity && (await identityOrNull(previousPath))) await rename(previousPath, targetPath);
		}
		const stagePath = brokerAgentEntryPath(entry, journal.stagedDir);
		if (await identityOrNull(stagePath)) await rm(stagePath, { recursive: true, force: true });
	}
	await syncDirectory(agentDir);
	await syncDirectoryIfExists(journal.prevDir);
	await syncDirectoryIfExists(journal.stagedDir);
	await rm(journal.stagedDir, { recursive: true, force: true });
	await rm(journal.prevDir, { recursive: true, force: true });
	await rm(join(provisionDir, "journal.json"), { force: true });
	await syncDirectory(provisionDir);
}

async function validateRollbackJournal(agentDir: string, journal: ProvisionJournal): Promise<void> {
	for (const name of journal.selected) {
		const entry = entryByName(name);
		const record = journal.entries[name];
		if (!entry || !record || !record.original || typeof record.original !== "object")
			throw new BrokerAgentError("broker_agent_unrecoverable", `unknown journal entry=${name}`);
		for (const component of targetComponents(entry)) {
			if (!Object.hasOwn(record.original, component))
				throw new BrokerAgentError("broker_agent_unrecoverable", `journal identity missing entry=${component}`);
			const oldIdentity = record.original[component] ?? null;
			const previousIdentity = await identityOrNull(join(journal.prevDir, component));
			const targetIdentity = await identityOrNull(join(agentDir, component));
			const stageIdentity = component === entry.name ? record.stagedIdentity : undefined;
			if (oldIdentity && previousIdentity && !sameIdentity(oldIdentity, previousIdentity))
				throw new BrokerAgentError("broker_agent_unrecoverable", `previous identity mismatch entry=${component}`);
			if (!oldIdentity && previousIdentity)
				throw new BrokerAgentError("broker_agent_unrecoverable", `unexpected previous entry=${component}`);
			if (oldIdentity && !previousIdentity && !sameIdentity(oldIdentity, targetIdentity))
				throw new BrokerAgentError("broker_agent_unrecoverable", `original missing entry=${component}`);
			if (targetIdentity && !sameIdentity(targetIdentity, oldIdentity) && !sameIdentity(targetIdentity, stageIdentity))
				throw new BrokerAgentError("broker_agent_unrecoverable", `unexpected target entry=${component}`);
			if (oldIdentity && previousIdentity && targetIdentity && sameIdentity(targetIdentity, oldIdentity))
				throw new BrokerAgentError("broker_agent_unrecoverable", `duplicate original entry=${component}`);
			if (component === entry.name) {
				const stagedIdentity = await identityOrNull(brokerAgentEntryPath(entry, journal.stagedDir));
				if (stagedIdentity && !sameIdentity(stagedIdentity, stageIdentity))
					throw new BrokerAgentError("broker_agent_unrecoverable", `staged identity mismatch entry=${name}`);
				if (stagedIdentity && targetIdentity && sameIdentity(stagedIdentity, targetIdentity))
					throw new BrokerAgentError("broker_agent_unrecoverable", `duplicate staged entry=${name}`);
			}
		}
	}
}

async function finishCommit(provisionDir: string, journal: ProvisionJournal): Promise<void> {
	const previousDir = join(provisionDir, "previous");
	const prevExists = await identityOrNull(journal.prevDir);
	const previousExists = await identityOrNull(previousDir);
	if (prevExists) {
		await rm(previousDir, { recursive: true, force: true });
		await rename(journal.prevDir, previousDir);
		await syncDirectory(provisionDir);
	} else if (!previousExists) {
		await mkdir(previousDir, { mode: DIRECTORY_MODE });
		await chmod(previousDir, DIRECTORY_MODE);
		await syncDirectory(provisionDir);
	}
	await rm(journal.stagedDir, { recursive: true, force: true });
	await rm(join(provisionDir, "journal.json"), { force: true });
	await syncDirectory(provisionDir);
}

async function writeJsonAtomic(directory: string, destination: string, value: unknown): Promise<void> {
	const temporary = `${destination}.${randomUUID()}.tmp`;
	try {
		await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { mode: FILE_MODE });
		await chmod(temporary, FILE_MODE);
		await syncFile(temporary);
		await rename(temporary, destination);
		await syncDirectory(directory);
	} finally {
		await rm(temporary, { force: true }).catch(() => {});
	}
}

async function readManifest(path: string): Promise<BrokerAgentManifest | undefined> {
	let info;
	try {
		info = await lstat(path);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		throw new BrokerAgentError("broker_agent_unrecoverable", `manifest stat failed: ${errorMessage(error)}`);
	}
	if (info.isSymbolicLink() || !info.isFile() || info.nlink !== 1 || (info.mode & 0o777) !== FILE_MODE)
		throw new BrokerAgentError("broker_agent_unrecoverable", "manifest must be a regular 0600 file with one link");
	try {
		const value: unknown = JSON.parse(await readFile(path, "utf8"));
		if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("not an object");
		const manifest = value as Partial<BrokerAgentManifest>;
		if (
			manifest.version !== 2 ||
			typeof manifest.genId !== "string" ||
			!manifest.entries ||
			typeof manifest.entries !== "object" ||
			Array.isArray(manifest.entries)
		)
			throw new Error("invalid manifest fields");
		return manifest as BrokerAgentManifest;
	} catch (error) {
		throw new BrokerAgentError("broker_agent_unrecoverable", `manifest invalid: ${errorMessage(error)}`);
	}
}

function parseJournal(value: unknown, provisionDir: string): ProvisionJournal {
	if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("not an object");
	const journal = value as Partial<ProvisionJournal>;
	if (
		journal.version !== 1 ||
		journal.phase !== "swap" ||
		typeof journal.genId !== "string" ||
		!/^[A-Za-z0-9_-]{6}$/.test(journal.genId) ||
		!Array.isArray(journal.selected) ||
		journal.selected.length === 0 ||
		new Set(journal.selected).size !== journal.selected.length ||
		!journal.entries ||
		typeof journal.entries !== "object" ||
		Array.isArray(journal.entries)
	)
		throw new Error("invalid journal fields");
	const expectedStage = join(provisionDir, `gen-${journal.genId}`);
	const expectedPrevious = join(provisionDir, `prev-${journal.genId}`);
	if (journal.stagedDir !== expectedStage || journal.prevDir !== expectedPrevious) throw new Error("journal paths do not match generation");
	for (const name of journal.selected) {
		if (typeof name !== "string" || !entryByName(name) || !journal.entries[name])
			throw new Error(`unknown journal entry=${String(name)}`);
	}
	return journal as ProvisionJournal;
}

function entryByName(name: string): BrokerAgentAllowlistEntry | undefined {
	return BROKER_AGENT_ALLOWLIST.find((entry) => entry.name === name);
}

function sameIdentity(left: FileIdentity | null | undefined, right: FileIdentity | null | undefined): boolean {
	return left !== null && left !== undefined && right !== null && right !== undefined && left.dev === right.dev && left.ino === right.ino && left.size === right.size;
}

async function syncFile(path: string): Promise<void> {
	const handle = await open(path, "r+");
	try {
		await handle.sync();
	} finally {
		await handle.close();
	}
}

async function syncDirectory(path: string): Promise<void> {
	const handle = await open(path, "r");
	try {
		await handle.sync();
	} finally {
		await handle.close();
	}
}
async function syncDirectoryIfExists(path: string): Promise<void> {
	try {
		await syncDirectory(path);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}
}


function asBrokerAgentError(error: unknown): BrokerAgentError {
	if (error instanceof BrokerAgentError) return error;
	return new BrokerAgentError("broker_agent_failed_restored", errorMessage(error));
}

function errorMessage(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
