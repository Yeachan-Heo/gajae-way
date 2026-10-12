import { createHash } from "node:crypto";
import { lstatSync, readFileSync, readdirSync, statSync } from "node:fs";
import { lstat as lstatAsync, readFile as readFileAsync, readdir as readdirAsync, stat as statAsync } from "node:fs/promises";
import { homedir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import { credentialFingerprint } from "./agent-db";

export const BROKER_AGENT_ALLOWLIST = [
	{ name: "agent.db", kind: "credentials", required: true, form: "sqlite" },
	{ name: "models.yml", kind: "model-config", required: false, form: "file" },
	{ name: "config.yml", kind: "model-config", required: false, form: "file" },
	{
		name: "model-presets",
		kind: "model-presets",
		required: false,
		form: "group",
		files: ["state.json", "state.backup.json", "control.json"],
	},
] as const;

export type BrokerAgentAllowlistEntry = (typeof BROKER_AGENT_ALLOWLIST)[number];
export type BrokerAgentEntryName = BrokerAgentAllowlistEntry["name"];

export const LIVE_BROKER_ENV_KEYS = ["GJC_AUTH_BROKER_URL", "GJC_AUTH_BROKER_TOKEN"] as const;
export const BROKER_ENV_PROTECTED_KEYS = [
	...LIVE_BROKER_ENV_KEYS,
	"GJC_CODING_AGENT_DIR",
	"GJC_SDK_BROKER_AUTOSTART",
] as const;
const AUTH_BROKER_URL_KEY = "auth.broker.url";

export function resolveBrokerAgentSource(from?: string, homeDirectory = homedir()): string {
	return from === undefined ? join(homeDirectory, ".gjc", "agent") : resolve(from);
}

export function resolveBrokerAgentDataSource(
	source: string,
	options: { readonly homeDirectory?: string; readonly xdgDataHome?: string; readonly platform?: NodeJS.Platform } = {},
): string {
	const homeDirectory = options.homeDirectory ?? homedir();
	if (resolve(source) !== resolve(resolveBrokerAgentSource(undefined, homeDirectory))) return source;
	const platform = options.platform ?? process.platform;
	if (platform !== "linux" && platform !== "darwin") return source;
	// gjc follows `$XDG_DATA_HOME/gjc` only when the variable is set; it has no
	// `~/.local/share` fallback (that directory holds gjc's installer releases).
	const dataHome = options.xdgDataHome ?? process.env.XDG_DATA_HOME;
	if (!dataHome || !isAbsolute(dataHome)) return source;
	const candidate = join(dataHome, "gjc");
	try {
		if (statSync(candidate).isDirectory()) return candidate;
	} catch {
		// The source agent directory remains authoritative when the XDG store is absent.
	}
	return source;
}

export function brokerAgentEntryPath(entry: BrokerAgentAllowlistEntry, root: string): string {
	return join(root, entry.name);
}

export async function brokerAgentEntryExists(entry: BrokerAgentAllowlistEntry, root: string): Promise<boolean> {
	const path = brokerAgentEntryPath(entry, root);
	try {
		const info = await lstatAsync(path);
		if (info.isSymbolicLink()) throw new Error(`broker_agent_symlink entry=${entry.name}`);
		if (entry.form === "group") {
			if (!info.isDirectory()) throw new Error(`broker_agent_invalid_entry entry=${entry.name}`);
			for (const name of entry.files) {
				try {
					const member = await lstatAsync(join(path, name));
					if (member.isSymbolicLink()) throw new Error(`broker_agent_symlink entry=${entry.name}/${name}`);
					if (!member.isFile()) throw new Error(`broker_agent_invalid_entry entry=${entry.name}/${name}`);
				} catch (error) {
					if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
				}
			}
			return (await readdirAsync(path)).some((name) => entry.files.includes(name as (typeof entry.files)[number]));
		}
		if (!info.isFile()) throw new Error(`broker_agent_invalid_entry entry=${entry.name}`);
		return true;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
		throw error;
	}
}

export function brokerAgentEntryExistsSync(entry: BrokerAgentAllowlistEntry, root: string): boolean {
	const path = brokerAgentEntryPath(entry, root);
	try {
		const info = lstatSync(path);
		if (info.isSymbolicLink()) throw new Error(`broker_agent_symlink entry=${entry.name}`);
		if (entry.form === "group") {
			if (!info.isDirectory()) throw new Error(`broker_agent_invalid_entry entry=${entry.name}`);
			return readdirSync(path).some((name) => {
				if (!entry.files.includes(name as (typeof entry.files)[number])) return false;
				const member = lstatSync(join(path, name));
				if (member.isSymbolicLink()) throw new Error(`broker_agent_symlink entry=${entry.name}/${name}`);
				if (!member.isFile()) throw new Error(`broker_agent_invalid_entry entry=${entry.name}/${name}`);
				return true;
			});
		}
		if (!info.isFile()) throw new Error(`broker_agent_invalid_entry entry=${entry.name}`);
		return true;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
		throw error;
	}
}

function entryBytesSync(entry: BrokerAgentAllowlistEntry, path: string): Array<{ name: string; bytes: Buffer }> {
	if (entry.form === "group") {
		return entry.files
			.filter((name) => {
				try {
					return lstatSync(join(path, name)).isFile();
				} catch (error) {
					if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
					throw error;
				}
			})
			.map((name) => ({ name, bytes: readFileSync(join(path, name)) }));
	}
	return [{ name: entry.name, bytes: readFileSync(path) }];
}

async function entryBytes(entry: BrokerAgentAllowlistEntry, path: string): Promise<Array<{ name: string; bytes: Buffer }>> {
	if (entry.form === "group") {
		const names = (await readdirAsync(path)).filter((name) => entry.files.includes(name as (typeof entry.files)[number]));
		return Promise.all(names.map(async (name) => ({ name, bytes: await readFileAsync(join(path, name)) })));
	}
	return [{ name: entry.name, bytes: await readFileAsync(path) }];
}

export function fingerprintBrokerAgentEntrySync(entry: BrokerAgentAllowlistEntry, path: string): string {
	if (entry.form === "sqlite") return credentialFingerprint(path);
	return fingerprintBytes(entryBytesSync(entry, path));
}

export async function fingerprintBrokerAgentEntry(entry: BrokerAgentAllowlistEntry, path: string): Promise<string> {
	if (entry.form === "sqlite") return credentialFingerprint(path);
	return fingerprintBytes(await entryBytes(entry, path));
}

function fingerprintBytes(files: readonly { readonly name: string; readonly bytes: Uint8Array }[]): string {
	const hash = createHash("sha256");
	for (const file of [...files].sort((a, b) => a.name.localeCompare(b.name))) {
		const name = Buffer.from(file.name, "utf8");
		const size = Buffer.alloc(8);
		size.writeBigUInt64BE(BigInt(file.bytes.byteLength));
		hash.update(name);
		hash.update(Buffer.from([0]));
		hash.update(size);
		hash.update(file.bytes);
	}
	return hash.digest("hex");
}


export function sourceEntryRootSync(entry: BrokerAgentAllowlistEntry, source: string, dataSource: string): string {
	return entry.name === "agent.db" ? dataSource : source;
}


export function sourceEntryStat(entry: BrokerAgentAllowlistEntry, root: string): Promise<{
	readonly size: number;
	readonly mtimeNs: string;
}> {
	const path = brokerAgentEntryPath(entry, root);
	return (async () => {
		if (entry.form === "group") {
			const members = await entryBytes(entry, path);
			const mtimes = await Promise.all(
				entry.files.map((name) => statAsync(join(path, name), { bigint: true }).catch(() => undefined)),
			);
			return {
				size: members.reduce((sum, member) => sum + member.bytes.byteLength, 0),
				mtimeNs: mtimes.filter((info) => info !== undefined).map((info) => info.mtimeNs.toString()).join(","),
			};
		}
		const info = await statAsync(path, { bigint: true });
		return { size: Number(info.size), mtimeNs: info.mtimeNs.toString() };
	})();
}

export function hasLiveBrokerUrl(content: string): boolean {
	try {
		const value: unknown = JSON.parse(content);
		if (value && typeof value === "object") {
			const auth = (value as Record<string, unknown>).auth;
			if (auth && typeof auth === "object") {
				const broker = (auth as Record<string, unknown>).broker;
				if (broker && typeof broker === "object") {
					const url = (broker as Record<string, unknown>).url;
					if (typeof url === "string" && url.trim()) return true;
				}
			}
		}
	} catch {
		// GJC's config is YAML; continue with its nested-key form below.
	}
	const stack: Array<{ indent: number; key: string }> = [];
	for (const rawLine of content.split(/\r?\n/)) {
		const line = stripComment(rawLine);
		const match = /^(\s*)([A-Za-z0-9_.-]+)\s*:\s*(.*?)\s*$/.exec(line);
		if (!match) continue;
		const indent = match[1]?.length ?? 0;
		const key = (match[2] ?? "").replace(/^['"]|['"]$/g, "");
		const value = (match[3] ?? "").replace(/^['"]|['"]$/g, "").trim();
		while (stack.length > 0 && (stack.at(-1)?.indent ?? -1) >= indent) stack.pop();
		if (key === AUTH_BROKER_URL_KEY && value && value !== "null" && value !== "~") return true;
		if (key === "url" && value && value !== "null" && value !== "~" && stack.map((item) => item.key).join(".") === "auth.broker")
			return true;
		if (!value) stack.push({ indent, key });
	}
	return false;
}

function stripComment(line: string): string {
	let quote = "";
	for (let index = 0; index < line.length; index++) {
		const char = line[index];
		if ((char === "'" || char === '"') && line[index - 1] !== "\\") quote = quote === char ? "" : quote || char;
		if (char === "#" && !quote && (index === 0 || /\s/.test(line[index - 1] ?? ""))) return line.slice(0, index);
	}
	return line;
}
