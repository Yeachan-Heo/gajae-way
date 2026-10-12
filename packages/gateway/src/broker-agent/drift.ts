import { existsSync, lstatSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { homeLayout } from "@gajae-gateway/home";
import {
	BROKER_AGENT_ALLOWLIST,
	type BrokerAgentAllowlistEntry,
	type BrokerAgentEntryName,
	brokerAgentEntryExistsSync,
	brokerAgentEntryPath,
	fingerprintBrokerAgentEntrySync,
	resolveBrokerAgentDataSource,
	resolveBrokerAgentSource,
	sourceEntryRootSync,
} from "./allowlist";

export type BrokerAgentEntryState =
	| "in_sync"
	| "source_changed"
	| "copy_changed"
	| "diverged"
	| "missing_source"
	| "missing_copy"
	| "absent";
export type BrokerAgentStatus = "provisioned" | "unprovisioned" | "interrupted";

export interface BrokerAgentDriftEntry {
	readonly name: BrokerAgentEntryName;
	readonly kind: string;
	readonly state: BrokerAgentEntryState;
}

export interface BrokerAgentDrift {
	readonly status: BrokerAgentStatus;
	readonly provisionedAt: string | null;
	readonly genId: string | null;
	readonly source: string | null;
	readonly entries: readonly BrokerAgentDriftEntry[];
}

interface ManifestEntry {
	readonly sourceFingerprint?: unknown;
	readonly copyFingerprint?: unknown;
}
interface Manifest {
	readonly version?: unknown;
	readonly genId?: unknown;
	readonly source?: unknown;
	readonly dataSource?: unknown;
	readonly provisionedAt?: unknown;
	readonly entries?: unknown;
}

const credentialFingerprintCache = new Map<string, { readonly key: string; readonly fingerprint: string }>();

/** Async public boundary; the file and SQLite snapshot reads are synchronous and read-only. */
export async function brokerAgentDrift(options: {
	readonly home: string;
	readonly agentDir: string;
	readonly source: string;
}): Promise<BrokerAgentDrift> {
	return readBrokerAgentDrift(options);
}

/** Synchronous projection used by the already-synchronous runtime-cycle snapshot. */
export function readBrokerAgentDrift(options: {
	readonly home: string;
	readonly agentDir: string;
	readonly source?: string;
}): BrokerAgentDrift {
	try {
		const layout = homeLayout(options.home);
		const manifest = readManifest(layout.brokerProvisionManifest);
		const source = options.source ?? (typeof manifest?.source === "string" ? manifest.source : resolveBrokerAgentSource());
		const dataSource =
			options.source === undefined && typeof manifest?.dataSource === "string"
				? manifest.dataSource
				: resolveBrokerAgentDataSource(source);
		const interrupted = existsSync(join(layout.brokerProvisionDir, "journal.json"));
		const entries = BROKER_AGENT_ALLOWLIST.map((entry) =>
			observeEntry(entry, source, dataSource, options.agentDir, manifest?.entries),
		);
		if (interrupted || (existsSync(layout.brokerProvisionManifest) && !manifest))
			return {
				status: "interrupted",
				provisionedAt: typeof manifest?.provisionedAt === "string" ? manifest.provisionedAt : null,
				genId: typeof manifest?.genId === "string" ? manifest.genId : null,
				source: typeof manifest?.source === "string" ? manifest.source : null,
				entries,
			};
		if (!manifest)
			return { status: "unprovisioned", provisionedAt: null, genId: null, source: null, entries };
		return {
			status: "provisioned",
			provisionedAt: typeof manifest.provisionedAt === "string" ? manifest.provisionedAt : null,
			genId: typeof manifest.genId === "string" ? manifest.genId : null,
			source: typeof manifest.source === "string" ? manifest.source : null,
			entries,
		};
	} catch {
		return { status: "interrupted", provisionedAt: null, genId: null, source: null, entries: [] };
	}
}

function readManifest(path: string): Manifest | undefined {
	try {
		const info = lstatSync(path);
		if (info.isSymbolicLink() || !info.isFile() || info.nlink !== 1 || (info.mode & 0o777) !== 0o600) return undefined;
		const value: unknown = JSON.parse(readFileSync(path, "utf8"));
		if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
		const manifest = value as Manifest;
		if (
			manifest.version !== 2 ||
			typeof manifest.genId !== "string" ||
			!manifest.entries ||
			typeof manifest.entries !== "object" ||
			Array.isArray(manifest.entries)
		)
			return undefined;
		return manifest;
	} catch {
		return undefined;
	}
}

function observeEntry(
	entry: BrokerAgentAllowlistEntry,
	source: string,
	dataSource: string,
	agentDir: string,
	manifestEntries: unknown,
): BrokerAgentDriftEntry {
	const sourceRoot = sourceEntryRootSync(entry, source, dataSource);
	const sourceExists = safeExists(entry, sourceRoot);
	const copyExists = safeExists(entry, agentDir);
	if (!sourceExists && !copyExists) return { name: entry.name, kind: entry.kind, state: entry.required ? "missing_copy" : "absent" };
	if (!copyExists) return { name: entry.name, kind: entry.kind, state: "missing_copy" };
	if (!sourceExists) return { name: entry.name, kind: entry.kind, state: "missing_source" };
	const sourcePath = brokerAgentEntryPath(entry, sourceRoot);
	const copyPath = brokerAgentEntryPath(entry, agentDir);
	const currentSource = currentFingerprint(entry, sourcePath);
	const currentCopy = currentFingerprint(entry, copyPath);
	if (currentSource === currentCopy) return { name: entry.name, kind: entry.kind, state: "in_sync" };
	const records = manifestEntries as Record<string, ManifestEntry> | undefined;
	const record = records?.[entry.name];
	const sourceUnchanged = typeof record?.sourceFingerprint === "string" && currentSource === record.sourceFingerprint;
	const copyUnchanged = typeof record?.copyFingerprint === "string" && currentCopy === record.copyFingerprint;
	if (sourceUnchanged && !copyUnchanged) return { name: entry.name, kind: entry.kind, state: "copy_changed" };
	if (copyUnchanged && !sourceUnchanged) return { name: entry.name, kind: entry.kind, state: "source_changed" };
	if (sourceUnchanged && copyUnchanged) return { name: entry.name, kind: entry.kind, state: "in_sync" };
	if (record && sourceUnchanged !== copyUnchanged) return { name: entry.name, kind: entry.kind, state: sourceUnchanged ? "copy_changed" : "source_changed" };
	return { name: entry.name, kind: entry.kind, state: "diverged" };
}

function safeExists(entry: BrokerAgentAllowlistEntry, root: string): boolean {
	try {
		return brokerAgentEntryExistsSync(entry, root);
	} catch {
		return false;
	}
}

function currentFingerprint(entry: BrokerAgentAllowlistEntry, path: string): string {
	if (entry.form !== "sqlite") return fingerprintBrokerAgentEntrySync(entry, path);
	const key = credentialCacheKey(path);
	const cached = credentialFingerprintCache.get(path);
	if (cached?.key === key) return cached.fingerprint;
	const fingerprint = fingerprintBrokerAgentEntrySync(entry, path);
	credentialFingerprintCache.set(path, { key, fingerprint });
	if (credentialFingerprintCache.size > 64) {
		const oldest = credentialFingerprintCache.keys().next().value;
		if (oldest !== undefined) credentialFingerprintCache.delete(oldest);
	}
	return fingerprint;
}

function credentialCacheKey(path: string): string {
	return [path, statSignature(path), statSignature(`${path}-wal`)].join("|");
}

function statSignature(path: string): string {
	try {
		const info = statSync(path, { bigint: true });
		if (!info.isFile()) return "not-file";
		return `${info.size}:${info.mtimeNs}`;
	} catch (error) {
		return (error as NodeJS.ErrnoException).code === "ENOENT" ? "absent" : "unreadable";
	}
}
