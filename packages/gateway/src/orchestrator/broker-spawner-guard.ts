import { lstat, readFile, realpath, stat } from "node:fs/promises";
import { dirname, join } from "node:path";

/**
 * Ends a gjc process that predates the installed gjc and keeps killing the
 * shared broker.
 *
 * Measured on gaebal-gajae 2026-09-30: gjc was upgraded to 0.18.1 under an
 * interactive `gjc` session started four days earlier. That process spawned
 * brokers on the new install, but it compared their published
 * `packageGeneration` with its own in-memory (old) version, never accepted
 * them, and SIGTERMed each one when its 29 s discovery budget expired. The
 * broker was replaced 100+ times in an hour (`broker.exit.json`:
 * `reason=signal`, `uptimeMs≈28200`), every gateway turn failed with
 * `broker_unavailable`, and it only stopped when the old process was ended.
 *
 * Termination is limited to a process proven to be that spawner: it is the
 * live broker's parent, it runs the configured gjc executable, it started
 * before that executable was installed, and the broker has been signalled
 * dead young at least {@link MIN_SHORT_LIVED_KILLS} times within
 * {@link KILL_WINDOW_MS}. Anything less is logged and left alone. The shared
 * broker and daemon are never touched; an ended session is durable and
 * resumes on the new gjc.
 */
export const KILL_WINDOW_MS = 5 * 60_000;
export const MIN_SHORT_LIVED_KILLS = 3;
/** A broker signalled before this uptime never became a usable incumbent. */
export const SHORT_LIVED_BROKER_MS = 60_000;

export interface SpawnerProcessProbe {
	parentPid(pid: number): Promise<number | undefined>;
	command(pid: number): Promise<string | undefined>;
	startedAtMs(pid: number): Promise<number | undefined>;
	/** Sends SIGTERM; false when the process could not be signalled. */
	terminate(pid: number): boolean;
}

export interface BrokerSpawnerGuardOptions {
	readonly agentDir: string;
	readonly executable: string;
	readonly probe?: SpawnerProcessProbe;
	/** Newest modification time of the installed gjc; defaults to the executable's install footprint. */
	readonly installedAtMs?: () => Promise<number | undefined>;
	readonly now?: () => number;
	readonly log?: (line: string) => void;
	/** Gateway-side processes that are never candidates. */
	readonly protectedPids?: readonly number[];
}

export type SpawnerVerdict =
	| { readonly action: "none" }
	| { readonly action: "held"; readonly spawnerPid: number | undefined; readonly reason: string }
	| { readonly action: "terminated"; readonly spawnerPid: number };

interface ExitRecord {
	readonly pid: number;
	readonly writtenAt: number;
}

export class BrokerSpawnerGuard {
	readonly #agentDir: string;
	readonly #executable: string;
	readonly #probe: SpawnerProcessProbe;
	readonly #installedAtMs: () => Promise<number | undefined>;
	readonly #now: () => number;
	readonly #log: (line: string) => void;
	readonly #protected: ReadonlySet<number>;
	/** Broker pid -> the time its short-lived signal exit was recorded. */
	readonly #kills = new Map<number, number>();
	readonly #reported = new Set<string>();

	constructor(options: BrokerSpawnerGuardOptions) {
		this.#agentDir = options.agentDir;
		this.#executable = options.executable;
		this.#probe = options.probe ?? psProbe;
		this.#installedAtMs = options.installedAtMs ?? (() => installFootprintMtimeMs(options.executable));
		this.#now = options.now ?? Date.now;
		this.#log = options.log ?? ((line) => console.error(line));
		this.#protected = new Set([process.pid, process.ppid, 0, 1, ...(options.protectedPids ?? [])]);
	}

	/** Called on every broker generation change. */
	async observe(): Promise<SpawnerVerdict> {
		const now = this.#now();
		const exit = await this.#readShortLivedSignalExit();
		if (exit && now - exit.writtenAt <= KILL_WINDOW_MS) this.#kills.set(exit.pid, exit.writtenAt);
		for (const [pid, at] of this.#kills) if (now - at > KILL_WINDOW_MS) this.#kills.delete(pid);
		if (this.#kills.size < MIN_SHORT_LIVED_KILLS) return { action: "none" };

		const brokerPid = await this.#readBrokerPid();
		const spawnerPid = brokerPid === undefined ? undefined : await this.#probe.parentPid(brokerPid);
		if (spawnerPid === undefined || this.#protected.has(spawnerPid)) return this.#hold(spawnerPid, "spawner_unknown");
		const command = await this.#probe.command(spawnerPid);
		const executable = await realpath(this.#executable).catch(() => this.#executable);
		if (!command || !(command.includes(this.#executable) || command.includes(executable)))
			return this.#hold(spawnerPid, "spawner_not_gjc");
		const [startedAt, installedAt] = await Promise.all([this.#probe.startedAtMs(spawnerPid), this.#installedAtMs()]);
		if (startedAt === undefined || installedAt === undefined) return this.#hold(spawnerPid, "age_unknown");
		if (startedAt >= installedAt) return this.#hold(spawnerPid, "spawner_not_older_than_install");
		const kills = this.#kills.size;
		if (!this.#probe.terminate(spawnerPid)) return this.#hold(spawnerPid, "signal_failed");
		this.#kills.clear();
		this.#log(
			`broker_stale_spawner_terminated pid=${spawnerPid} startedAt=${new Date(startedAt).toISOString()} gjcInstalledAt=${new Date(installedAt).toISOString()} brokerKills=${kills} windowMs=${KILL_WINDOW_MS}`,
		);
		return { action: "terminated", spawnerPid };
	}

	#hold(spawnerPid: number | undefined, reason: string): SpawnerVerdict {
		const key = `${spawnerPid ?? "-"}|${reason}`;
		if (!this.#reported.has(key)) {
			this.#reported.add(key);
			this.#log(
				`broker_churn brokerKills=${this.#kills.size} windowMs=${KILL_WINDOW_MS} spawner=${spawnerPid ?? "-"} action=held reason=${reason}`,
			);
		}
		return { action: "held", spawnerPid, reason };
	}

	async #readShortLivedSignalExit(): Promise<ExitRecord | undefined> {
		const record = await readJson(join(this.#agentDir, "sdk", "broker.exit.json"));
		if (
			record?.reason !== "signal" ||
			typeof record.uptimeMs !== "number" ||
			record.uptimeMs >= SHORT_LIVED_BROKER_MS ||
			!Number.isSafeInteger(record.pid) ||
			typeof record.writtenAt !== "number"
		)
			return undefined;
		return { pid: record.pid as number, writtenAt: record.writtenAt };
	}

	async #readBrokerPid(): Promise<number | undefined> {
		const record = await readJson(join(this.#agentDir, "sdk", "broker.json"));
		return Number.isSafeInteger(record?.pid) && (record?.pid as number) > 1 ? (record?.pid as number) : undefined;
	}
}

async function readJson(path: string): Promise<Record<string, unknown> | undefined> {
	try {
		const value: unknown = JSON.parse(await readFile(path, "utf8"));
		return typeof value === "object" && value !== null && !Array.isArray(value)
			? (value as Record<string, unknown>)
			: undefined;
	} catch {
		return undefined;
	}
}

/**
 * Newest mtime among the executable link, its target, and the nearest
 * package.json above the target: a global `bun install -g` replaces the link
 * and package tree, a compiled install replaces the binary itself.
 */
export async function installFootprintMtimeMs(executable: string): Promise<number | undefined> {
	const times: number[] = [];
	const push = async (read: Promise<{ mtimeMs: number }>) => {
		try {
			times.push((await read).mtimeMs);
		} catch {}
	};
	await push(lstat(executable));
	const target = await realpath(executable).catch(() => undefined);
	if (target) {
		await push(stat(target));
		let directory = dirname(target);
		for (let depth = 0; depth < 4; depth++) {
			const before = times.length;
			await push(stat(join(directory, "package.json")));
			if (times.length > before) break;
			directory = dirname(directory);
		}
	}
	return times.length > 0 ? Math.max(...times) : undefined;
}

async function psField(pid: number, field: string): Promise<string | undefined> {
	try {
		const proc = Bun.spawn(["ps", "-o", `${field}=`, "-p", String(pid)], {
			stdout: "pipe",
			stderr: "ignore",
			env: { ...process.env, LC_ALL: "C" },
		});
		const out = (await new Response(proc.stdout).text()).trim();
		await proc.exited;
		return out.length > 0 ? out : undefined;
	} catch {
		return undefined;
	}
}

/** `ps -o etime` is `[[dd-]hh:]mm:ss` on both Linux and macOS. */
export function parseElapsedSeconds(etime: string): number | undefined {
	const match = /^(?:(\d+)-)?(?:(\d+):)?(\d+):(\d+)$/.exec(etime.trim());
	if (!match) return undefined;
	const [, days, hours, minutes, seconds] = match;
	return ((Number(days ?? 0) * 24 + Number(hours ?? 0)) * 60 + Number(minutes)) * 60 + Number(seconds);
}

const psProbe: SpawnerProcessProbe = {
	async parentPid(pid) {
		const value = Number(await psField(pid, "ppid"));
		return Number.isSafeInteger(value) && value > 0 ? value : undefined;
	},
	command: (pid) => psField(pid, "command"),
	async startedAtMs(pid) {
		const etime = await psField(pid, "etime");
		const elapsed = etime === undefined ? undefined : parseElapsedSeconds(etime);
		return elapsed === undefined ? undefined : Date.now() - elapsed * 1000;
	},
	terminate(pid) {
		try {
			process.kill(pid, "SIGTERM");
			return true;
		} catch {
			return false;
		}
	},
};
