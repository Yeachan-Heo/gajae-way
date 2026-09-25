#!/usr/bin/env bun
import { Database } from "bun:sqlite";
import { copyFile, lstat, readFile, stat } from "node:fs/promises";
import { isAbsolute, join } from "node:path";
import type {
	MonitorRecord,
	MonitorSpec,
	OpsCycleResult,
	OriginRef,
	WorkJobsResult,
	WorkRetireResult,
	WorkRunResult,
	WorkStartParams,
	WorkStartResult,
	WorkStatusResult,
	WorkSteerResult,
} from "@gajae-gateway/protocol";
import { LOOPBACK_ORIGIN, originKey, parseOriginKey } from "@gajae-gateway/protocol";
import { GajaewayClient } from "@gajae-gateway/sdk";
import {
	columnNames,
	type ListOptions,
	MONITOR_COLUMNS,
	parseListOptions,
	renderList,
	SESSION_COLUMNS,
	type SessionListRow,
} from "./list";
import {
	effectiveRestartState,
	type LaunchRestartOptions,
	launchRestartStack,
	type RunRestartOptions,
	readRestartReceipt,
	renderRestartReceipt,
	runRestartStack,
} from "./restart-stack";
import { type InstallServicesOptions, installServices, type ServicePlatform, serviceUsage } from "./services";

export function socketPath(home = process.env.GAJAEWAY_HOME): string {
	return `${home ?? `${process.env.HOME ?? "~"}/.gajaeway`}/gateway.sock`;
}

/**
 * Every dispatchable top-level subcommand. An empty argv used to fall through
 * the switch into the shared catch, which only *sets* an exit code — a probe
 * with no arguments has to terminate before `main` opens a gateway socket,
 * never after, so the list is checked up front.
 */
export const COMMANDS = [
	"status",
	"shutdown",
	"chat",
	"daemon",
	"sessions",
	"ops",
	"memory",
	"monitors",
	"work",
	"services",
] as const;

export const CLI_USAGE =
	"usage: gajaeway [--socket PATH] status|shutdown|chat|daemon run|sessions list [--json] [--fields a,b,c] [--limit N] [--offset N]|sessions inspect <originKey-or-index>|memory audit|memory search <query>|monitors ...|work run|start <name> [--cwd DIR] [--resume] [--model ID|--preset NAME] [--notify originKey (start only)] <text>|work status <name>|work steer <name> <text>|work retire <name>|work jobs|ops backup <path>|ops redeliver <deliveryId>|ops redeliver --since <iso>|ops cycle [--json]|ops integrity|ops restore <backupPath>|ops restart-stack [--status]|services install|repair --bin-dir DIR [--launch-agents-dir DIR] [--unit-dir DIR] [--platform darwin|linux] (work run waits for a response; caller timeout does not end the attempt)";

/** Usage errors exit 2, as `gajaeway-gateway` does; 1 stays a runtime failure. */
export const USAGE_EXIT_CODE = 2;

/** The usage text when `command` cannot be dispatched, undefined when it can. */
export function usageFor(command: string | undefined): string | undefined {
	return command !== undefined && (COMMANDS as readonly string[]).includes(command) ? undefined : CLI_USAGE;
}

export function parseArgs(args: string[]): { command?: string; rest: string[]; socket: string } {
	let socket = socketPath();
	const rest: string[] = [];
	for (let i = 0; i < args.length; i++) {
		if (args[i] === "--socket") socket = args[++i] ?? socket;
		else rest.push(args[i]);
	}
	return { command: rest[0], rest: rest.slice(1), socket };
}

function gatewayHome(): string {
	return process.env.GAJAEWAY_HOME ?? `${process.env.HOME ?? "~"}/.gajaeway`;
}

export interface MainOptions {
	readonly services?: Pick<InstallServicesOptions, "loginPathRunner" | "writeFile">;
	/** Test seams for `ops restart-stack`; the real path spawns the service manager. */
	readonly restartStack?: {
		readonly launch?: Omit<LaunchRestartOptions, "home">;
		readonly run?: Omit<RunRestartOptions, "home" | "id">;
	};
}

export type ServicesAction = "install" | "repair";

export interface ParsedServicesArgs {
	readonly action: ServicesAction;
	readonly binDir: string;
	readonly launchAgentsDir?: string;
	readonly unitDir?: string;
	readonly platform?: ServicePlatform;
}

export function parseServicesArgs(args: readonly string[]): ParsedServicesArgs {
	const action = args[0];
	if (action !== "install" && action !== "repair") throw new Error(serviceUsage());
	let binDir: string | undefined;
	let launchAgentsDir: string | undefined;
	let unitDir: string | undefined;
	let platform: ServicePlatform | undefined;
	for (let i = 1; i < args.length; i++) {
		const flag = args[i];
		if (flag === "--bin-dir" || flag === "--launch-agents-dir" || flag === "--unit-dir") {
			const value = args[++i];
			if (value === undefined || value.length === 0 || value.startsWith("--"))
				throw new Error(`${flag} expects a non-empty DIR`);
			if (flag === "--bin-dir") binDir = value;
			else if (flag === "--unit-dir") unitDir = value;
			else launchAgentsDir = value;
		} else if (flag === "--platform") {
			const value = args[++i];
			if (value !== "darwin" && value !== "linux") throw new Error("--platform expects darwin or linux");
			platform = value;
		} else throw new Error(`unknown option: ${flag}`);
	}
	if (binDir === undefined) throw new Error("services requires --bin-dir DIR");
	return {
		action,
		binDir,
		...(launchAgentsDir === undefined ? {} : { launchAgentsDir }),
		...(unitDir === undefined ? {} : { unitDir }),
		...(platform === undefined ? {} : { platform }),
	};
}

interface ParsedMonitorUpdateArgs {
	readonly monitorId: string;
	readonly params: Record<string, unknown>;
}

const MONITOR_UPDATE_USAGE =
	"usage: gajaeway monitors update <id> (--json '<partial MonitorSpec JSON>'|--schedule '<cron>' [--enabled true|false]|--enabled true|false [--schedule '<cron>'])";

function parseMonitorUpdateArgs(args: readonly string[]): ParsedMonitorUpdateArgs {
	const [monitorId, ...options] = args;
	if (!monitorId || monitorId.startsWith("--") || options.length === 0) throw new Error(MONITOR_UPDATE_USAGE);

	let jsonPatch: Partial<MonitorSpec> | undefined;
	let schedule: string | undefined;
	let enabled: boolean | undefined;
	for (let i = 0; i < options.length; i++) {
		const option = options[i];
		if (option === "--json") {
			const value = options[++i];
			if (jsonPatch !== undefined || value === undefined || value.startsWith("--"))
				throw new Error(MONITOR_UPDATE_USAGE);
			let parsed: unknown;
			try {
				parsed = JSON.parse(value);
			} catch {
				throw new Error(MONITOR_UPDATE_USAGE);
			}
			if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) throw new Error(MONITOR_UPDATE_USAGE);
			jsonPatch = parsed as Partial<MonitorSpec>;
		} else if (option === "--schedule") {
			const value = options[++i];
			if (schedule !== undefined || !value || value.startsWith("--")) throw new Error(MONITOR_UPDATE_USAGE);
			schedule = value;
		} else if (option === "--enabled") {
			const value = options[++i];
			if (enabled !== undefined || (value !== "true" && value !== "false")) throw new Error(MONITOR_UPDATE_USAGE);
			enabled = value === "true";
		} else throw new Error(MONITOR_UPDATE_USAGE);
	}

	if (jsonPatch !== undefined && (schedule !== undefined || enabled !== undefined))
		throw new Error(MONITOR_UPDATE_USAGE);
	if (jsonPatch !== undefined) return { monitorId, params: { ...jsonPatch, monitorId } };
	return {
		monitorId,
		params: {
			monitorId,
			...(schedule === undefined ? {} : { schedule }),
			...(enabled === undefined ? {} : { enabled }),
		},
	};
}

/**
 * Operator runtime-cycle view (`gajaeway ops cycle`).
 *
 * Rendering contract:
 * - The aggregate phase line always prints.
 * - Any gate prints under `gates:` and forces exit code 1 — a gated cycle is
 *   never reported as healthy, so scripting cannot mistake it for idle.
 * - `--json` emits the typed OpsCycleResult verbatim; the exit-code contract
 *   is identical.
 */
export function renderCycle(cycle: OpsCycleResult): string[] {
	const lines: string[] = [];
	lines.push(`phase: ${cycle.phase}`);
	if (cycle.gates.length > 0) lines.push(`gates: ${cycle.gates.join(", ")}`);
	else lines.push("gates: none");
	lines.push(`generatedAt: ${cycle.generatedAt}`);
	lines.push(`instance: ${cycle.instanceId}`);
	lines.push(`inbound: pending=${cycle.pendingInbound} inflight=${cycle.inFlightInbound}`);
	lines.push(
		`context: unread=${cycle.contextDiff.unread} expired=${cycle.contextDiff.expired} truncated=${cycle.contextDiff.truncated} omitted_oldest=${cycle.contextDiff.omittedOldestAt ?? "-"} omitted_newest=${cycle.contextDiff.omittedNewestAt ?? "-"}`,
	);
	lines.push(
		`deliveries: pending=${cycle.deliveries.pending} inflight=${cycle.deliveries.inflight} confirmed=${cycle.deliveries.confirmed} failed_ambiguous=${cycle.deliveries.failedAmbiguous} expired=${cycle.deliveries.expired}`,
	);
	lines.push(
		`memory: queued=${cycle.memoryIntents.queued} written=${cycle.memoryIntents.written} committed=${cycle.memoryIntents.committed} receipted=${cycle.memoryIntents.receipted} quarantined=${cycle.memoryIntents.quarantined}${cycle.memoryClosing ? " (closing)" : ""}`,
	);
	if (cycle.agentDisk) {
		const { path, freeBytes, totalBytes } = cycle.agentDisk;
		const gib = (bytes: number) => (bytes / 1024 ** 3).toFixed(1);
		lines.push(
			freeBytes === null || totalBytes === null
				? `agent_disk: ${path} unobservable`
				: `agent_disk: ${path} free=${gib(freeBytes)}GiB total=${gib(totalBytes)}GiB`,
		);
	}
	// Always rendered: an empty subsystem must be distinguishable from an absent one.
	lines.push(
		`monitors: ${cycle.monitorEvents.length ? cycle.monitorEvents.map((m) => `${m.stage}=${m.count}`).join(" ") : "none"}`,
	);
	if (cycle.monitorAuthoringLost.length > 0)
		lines.push(
			`monitor authoring lost: ${cycle.monitorAuthoringLost.map((m) => `${m.eventType}=${m.consecutive} (last ${m.lastFiredAt})`).join(" ")}`,
		);
	if (cycle.sessions.length > 0) {
		lines.push("sessions:");
		lines.push("INDEX  ORIGIN                                      EPOCH  SESSION      PENDING  UNSETTLED  OLDEST");
		for (const [index, session] of cycle.sessions.entries()) {
			const sessionId = session.sessionId === "" ? "(rebinding)" : session.sessionId.slice(0, 11);
			const oldest =
				session.oldestUnsettledAgeMs === null ? "-" : `${Math.round(session.oldestUnsettledAgeMs / 1000)}s`;
			lines.push(
				`${String(index).padEnd(6)} ${session.originKey.padEnd(43)} ${String(session.epoch).padEnd(6)} ${sessionId.padEnd(12)} ${String(session.pendingInbound).padEnd(8)} ${String(session.unsettledDeliveries).padEnd(10)} ${oldest}`,
			);
		}
	}
	return lines;
}

/**
 * Exit-code contract for `gajaeway ops cycle`: 0 only when the projection is
 * healthy. Extracted so the automation-facing contract is pinned by tests.
 */
export function cycleExitCode(cycle: OpsCycleResult): number {
	return cycle.gates.length > 0 ? 1 : 0;
}

/**
 * The same acceptance the gateway applies at boot (`PRAGMA integrity_check`
 * must answer `ok`), run read-only against the backup before it replaces
 * anything. A 16-byte header match is not a database: a truncated or
 * bit-flipped file passes it, gets copied into place, and the next boot
 * fails while the restore already reported success.
 *
 * An empty file is also refused: SQLite treats a zero-length file as a valid
 * empty database and `integrity_check` answers `ok` for it, but restoring it
 * would boot a gateway with every table freshly created and nothing in them.
 *
 * Opening a WAL-mode backup read-only may leave empty `-wal`/`-shm` sidecars
 * next to the backup; `ops backup` writes delete-mode files, which gain none.
 */
export function verifyBackupIntegrity(backupPath: string): void {
	let database: Database;
	try {
		database = new Database(backupPath, { readonly: true });
	} catch (error) {
		throw new Error(
			`Backup is not readable: ${backupPath} (${error instanceof Error ? error.message : String(error)})`,
		);
	}
	let detail: string;
	let pageCount: number;
	try {
		pageCount = database.query<{ page_count: number }, []>("PRAGMA page_count").get()?.page_count ?? 0;
		detail =
			database.query<{ integrity_check: string }, []>("PRAGMA integrity_check").get()?.integrity_check ?? "unknown";
	} catch (error) {
		throw new Error(
			`Backup is not a readable SQLite database: ${backupPath} (${error instanceof Error ? error.message : String(error)})`,
		);
	} finally {
		database.close();
	}
	if (pageCount === 0) throw new Error(`Backup is an empty SQLite database: ${backupPath}`);
	if (detail !== "ok") throw new Error(`Backup failed SQLite integrity_check: ${backupPath} (${detail})`);
}

/**
 * The database the gateway actually opens: `config.json` `dbPath` when set,
 * otherwise `<home>/gateway.db` — the same file-based resolution as the
 * gateway's `loadConfig`. An absent `config.json` means defaults, exactly as
 * at boot. A present-but-unreadable one (EACCES, EISDIR, or a dangling symlink,
 * which reports ENOENT while the entry exists) refuses the restore instead of
 * guessing, as boot refuses to start: copying a backup over the wrong file
 * reports success while the live database stays broken.
 *
 * `schemaVersion` is deliberately not checked: `dbPath` is a version-independent
 * string, and disaster recovery must not be blocked by an unrelated schema bump.
 */
export async function restoreTargetPath(home = gatewayHome()): Promise<string> {
	const configPath = join(home, "config.json");
	let raw: string | undefined;
	try {
		raw = await readFile(configPath, "utf8");
	} catch (error) {
		const code = (error as NodeJS.ErrnoException).code;
		const entryExists = await lstat(configPath).then(
			() => true,
			() => false,
		);
		if (code !== "ENOENT" || entryExists) throw new Error(`Cannot read ${configPath}; refusing restore.`);
	}
	if (raw === undefined) return join(home, "gateway.db");
	let parsed: unknown;
	try {
		parsed = JSON.parse(raw);
	} catch {
		throw new Error(`Cannot parse ${configPath}; refusing restore.`);
	}
	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed))
		throw new Error(`Invalid configuration ${configPath}: config must be an object; refusing restore.`);
	const dbPath = (parsed as { dbPath?: unknown }).dbPath;
	if (dbPath === undefined) return join(home, "gateway.db");
	if (typeof dbPath !== "string" || dbPath.length === 0)
		throw new Error(`Invalid configuration ${configPath}: dbPath must be a non-empty string; refusing restore.`);
	return dbPath;
}

export async function restoreDatabase(socket: string, backupPath: string): Promise<void> {
	if (!isAbsolute(backupPath)) throw new Error("ops restore requires an absolute backup path");
	try {
		await stat(socket);
		throw new Error(`Refusing restore: gateway socket ${socket} exists. Stop the daemon before restoring.`);
	} catch (error) {
		if (error instanceof Error && error.message.startsWith("Refusing restore:")) throw error;
		if ((error as NodeJS.ErrnoException).code !== "ENOENT")
			throw new Error(`Cannot verify gateway socket ${socket}; refusing restore.`);
	}
	verifyBackupIntegrity(backupPath);
	const databasePath = await restoreTargetPath();
	const preservedPath = `${databasePath}.pre-restore-${new Date().toISOString().replace(/[:.]/g, "-")}`;
	await copyFile(databasePath, preservedPath);
	await copyFile(backupPath, databasePath);
	console.log(`Validated SQLite backup: ${backupPath}`);
	console.log(`Copied current database to: ${preservedPath}`);
	console.log(`Restored backup to: ${databasePath}`);
}

const CHAT_TURN_TIMEOUT_MS = 30 * 60_000;

async function chat(socket: string): Promise<void> {
	let client: GajaewayClient;
	try {
		client = await GajaewayClient.connectSocket(socket);
	} catch {
		console.error(`Unable to connect to gateway socket ${socket}. Start the daemon out-of-band first.`);
		process.exitCode = 1;
		return;
	}
	const turnWaiters = new Map<string, () => void>();
	client.onChatMessage((message) => {
		console.log(message.text);
		if (message.final) turnWaiters.get(message.turnId)?.();
	});
	const sendAndWait = async (text: string): Promise<void> => {
		const { turnId } = await client.chatSend(LOOPBACK_ORIGIN, text);
		if (turnId === null) {
			console.error("(message was not engaged)");
			return;
		}
		// The gateway's own request wait is 30 minutes; a max-effort reasoning
		// turn routinely runs past two. Match the server so the REPL does not
		// abandon a turn the gateway is still delivering.
		await new Promise<void>((resolve) => {
			const timer = setTimeout(() => {
				turnWaiters.delete(turnId);
				console.error(`(turn timed out after ${CHAT_TURN_TIMEOUT_MS / 60_000}m)`);
				resolve();
			}, CHAT_TURN_TIMEOUT_MS);
			turnWaiters.set(turnId, () => {
				clearTimeout(timer);
				turnWaiters.delete(turnId);
				resolve();
			});
		});
	};
	const reader = Bun.stdin.stream().getReader();
	const decoder = new TextDecoder();
	let buffer = "";
	process.stdout.write("> ");
	try {
		while (true) {
			const { value, done } = await reader.read();
			if (done) break;
			buffer += decoder.decode(value, { stream: true });
			const lines = buffer.split(/\r?\n/);
			buffer = lines.pop() ?? "";
			for (const line of lines) {
				if (line === "/quit") return;
				if (line.trim()) await sendAndWait(line);
				process.stdout.write("> ");
			}
		}
	} finally {
		await client.close();
	}
}

export async function main(args = process.argv.slice(2), options: MainOptions = {}): Promise<void> {
	const parsed = parseArgs(args);
	const usage = usageFor(parsed.command);
	if (usage !== undefined) {
		console.error(usage);
		process.exit(USAGE_EXIT_CODE);
	}
	try {
		switch (parsed.command) {
			case "status": {
				const client = await GajaewayClient.connectSocket(parsed.socket);
				try {
					console.log(JSON.stringify(await client.status()));
				} finally {
					await client.close();
				}
				break;
			}
			case "shutdown": {
				const client = await GajaewayClient.connectSocket(parsed.socket);
				try {
					await client.shutdown();
				} finally {
					await client.close();
				}
				break;
			}
			case "chat":
				await chat(parsed.socket);
				break;
			case "daemon":
				if (parsed.rest[0] === "run")
					console.log("Launch the gateway out-of-band with: bun packages/gateway/src/main.ts daemon");
				else throw new Error("usage: gajaeway daemon run");
				break;
			// Flag validation happens before the socket connect so a bad
			// `--fields`/`--limit` fails fast without a running gateway.
			case "sessions": {
				const listOptions =
					parsed.rest[0] === "list" ? parseListOptions(parsed.rest.slice(1), SESSION_COLUMNS) : undefined;
				const client = await GajaewayClient.connectSocket(parsed.socket);
				try {
					const result = await client.request<{
						sessions: Array<{
							origin: Parameters<typeof originKey>[0];
							epoch: number;
							createdAt: string;
							lastActivityAt: string | null;
							bootstrap: {
								epoch: number;
								pending: boolean;
								appliedAt: string | null;
								includedSections: string[];
								byteCount: number;
								truncated: boolean;
								diagnostics: string[];
							};
						}>;
					}>("session.list");
					const [command, selector] = parsed.rest;
					if (command === "list") {
						const options = listOptions as ListOptions;
						for (const line of renderList(SESSION_COLUMNS, result.sessions satisfies SessionListRow[], options, {
							key: "sessions",
							result,
						}))
							console.log(line);
					} else if (command === "inspect" && selector) {
						const index = Number(selector);
						const session = Number.isInteger(index)
							? result.sessions[index]
							: result.sessions.find((candidate) => originKey(candidate.origin) === selector);
						if (!session) throw new Error(`Unknown session: ${selector}`);
						console.log(`origin: ${originKey(session.origin)}`);
						console.log(`epoch: ${session.epoch}`);
						console.log(`createdAt: ${session.createdAt}`);
						console.log(`lastActivityAt: ${session.lastActivityAt ?? "-"}`);
						console.log(`bootstrapEpoch: ${session.bootstrap.epoch}`);
						console.log(`bootstrapPending: ${session.bootstrap.pending}`);
						console.log(`bootstrapAppliedAt: ${session.bootstrap.appliedAt ?? "-"}`);
						console.log(`bootstrapSections: ${session.bootstrap.includedSections.join(", ") || "-"}`);
						console.log(`bootstrapBytes: ${session.bootstrap.byteCount}`);
						console.log(`bootstrapTruncated: ${session.bootstrap.truncated}`);
						console.log(`bootstrapDiagnostics: ${session.bootstrap.diagnostics.join(", ") || "-"}`);
					} else
						throw new Error(
							`usage: gajaeway sessions list [--json] [--fields ${columnNames(SESSION_COLUMNS).join(",")}] [--limit N] [--offset N]|inspect <originKey-or-index>`,
						);
				} finally {
					await client.close();
				}
				break;
			}
			case "ops": {
				const [command, path] = parsed.rest;
				if (command === "restore" && path) {
					await restoreDatabase(parsed.socket, path);
					break;
				}
				if (command === "restart-stack") {
					// Deliberately socket-free: the reason to run this is a gateway that
					// has to come back, so it must not need the gateway to answer first.
					// It also runs detached: restarting the gateway kills a persona turn that
					// invoked it, which must not end the sequence half-applied (issue #54).
					const usage = "usage: gajaeway ops restart-stack [--status]";
					const home = gatewayHome();
					const flag = parsed.rest[1];
					if (flag === "--run" && parsed.rest.length === 3 && parsed.rest[2]) {
						const receipt = await runRestartStack({
							...options.restartStack?.run,
							home,
							id: parsed.rest[2],
						});
						for (const line of renderRestartReceipt(receipt)) console.log(line);
						if (receipt.state !== "ok") process.exitCode = 1;
					} else if (flag === "--status" && parsed.rest.length === 2) {
						const receipt = await readRestartReceipt(home);
						if (receipt === undefined) throw new Error("no restart-stack receipt");
						for (const line of renderRestartReceipt(receipt)) console.log(line);
						// Only a completed, verified sequence exits 0; queued or running is not yet success.
						if (effectiveRestartState(receipt) !== "ok") process.exitCode = 1;
					} else if (parsed.rest.length === 1) {
						const { receipt, supervisorPid } = await launchRestartStack({ ...options.restartStack?.launch, home });
						console.log(`restart-stack ${receipt.id}: queued (supervisor pid ${supervisorPid})`);
						console.log("read the outcome with: gajaeway ops restart-stack --status");
					} else throw new Error(usage);
					break;
				}
				let redeliverParams: { deliveryId: string } | { since: string } | undefined;
				if (command === "redeliver") {
					if (parsed.rest.length === 2 && path && path !== "--since") redeliverParams = { deliveryId: path };
					else if (
						parsed.rest.length === 3 &&
						path === "--since" &&
						parsed.rest[2] &&
						Number.isFinite(Date.parse(parsed.rest[2]))
					)
						redeliverParams = { since: parsed.rest[2] };
					else throw new Error("usage: gajaeway ops redeliver <deliveryId>|--since <iso>");
				}
				const client = await GajaewayClient.connectSocket(parsed.socket);
				try {
					if (command === "backup" && path) console.log(JSON.stringify(await client.request("ops.backup", { path })));
					else if (redeliverParams) console.log(JSON.stringify(await client.request("ops.redeliver", redeliverParams)));
					else if (command === "integrity") console.log(JSON.stringify(await client.request("ops.integrity")));
					else if (command === "cycle") {
						const cycle = await client.opsCycle();
						if (parsed.rest[1] === "--json") console.log(JSON.stringify(cycle));
						else if (parsed.rest[1]) throw new Error("usage: gajaeway ops cycle [--json]");
						else for (const line of renderCycle(cycle)) console.log(line);
						// Fail-closed: a gated/degraded cycle exits non-zero even though the
						// request itself succeeded, so scripts can never read it as healthy.
						process.exitCode = cycleExitCode(cycle);
					} else
						throw new Error(
							"usage: gajaeway ops backup <path>|redeliver <deliveryId>|redeliver --since <iso>|cycle [--json]|integrity|restore <backupPath>|restart-stack [--status]",
						);
				} finally {
					await client.close();
				}
				break;
			}
			case "memory": {
				const usage = "usage: gajaeway memory audit|memory autolink|memory search <query>";
				// An unrecognised argument is refused before the socket is opened rather
				// than ignored: a run of `memory audit --fix` that silently degraded to a
				// plain audit would read as a repair attempt that reproduced the failure.
				if ((parsed.rest[0] === "audit" || parsed.rest[0] === "autolink") && parsed.rest.length > 1)
					throw new Error(`${usage} (unknown argument: ${parsed.rest[1]})`);
				const client = await GajaewayClient.connectSocket(parsed.socket);
				try {
					if (parsed.rest[0] === "audit") {
						const result = await client.request<{ ok: boolean; issues: unknown[] }>("memory.audit");
						console.log(JSON.stringify(result.issues));
						if (!result.ok) process.exitCode = 1;
					} else if (parsed.rest[0] === "autolink") {
						console.log(JSON.stringify(await client.request("memory.autolink")));
					} else if (parsed.rest[0] === "search" && parsed.rest.slice(1).join(" ")) {
						console.log(
							JSON.stringify(await client.request("memory.search", { query: parsed.rest.slice(1).join(" ") })),
						);
					} else throw new Error(usage);
				} finally {
					await client.close();
				}
				break;
			}
			case "monitors": {
				const listOptions =
					parsed.rest[0] === "list" ? parseListOptions(parsed.rest.slice(1), MONITOR_COLUMNS) : undefined;
				const [command, ...args] = parsed.rest;
				const update = command === "update" ? parseMonitorUpdateArgs(args) : undefined;
				const client = await GajaewayClient.connectSocket(parsed.socket);
				try {
					if (command === "add" && args[0] === "--json" && args[1])
						console.log(JSON.stringify(await client.request("monitor.add", JSON.parse(args[1]))));
					else if (command === "update" && update)
						console.log(JSON.stringify(await client.request<{ monitorId: string }>("monitor.update", update.params)));
					else if (command === "list") {
						const options = listOptions as ListOptions;
						const result = await client.request<{ monitors: MonitorRecord[] }>("monitor.list");
						for (const line of renderList(MONITOR_COLUMNS, result.monitors, options, {
							key: "monitors",
							result,
						}))
							console.log(line);
					} else if (command === "inspect" && args[0])
						console.log(JSON.stringify(await client.request("monitor.inspect", { monitorId: args[0] })));
					else if (command === "remove" && args[0])
						console.log(JSON.stringify(await client.request("monitor.remove", { monitorId: args[0] })));
					else if (command === "test" && args[0]) {
						let eventType: string | undefined;
						let payload: unknown = {};
						for (let i = 1; i < args.length; i++) {
							if (args[i] === "--type") eventType = args[++i];
							else if (args[i] === "--payload") payload = JSON.parse(args[++i] ?? "");
						}
						console.log(
							JSON.stringify(
								await client.request("monitor.test", {
									monitorId: args[0],
									...(eventType ? { eventType } : {}),
									payload,
								}),
							),
						);
					} else
						throw new Error(
							`usage: gajaeway monitors add --json '<MonitorSpec json>'|update <id> (--json '<partial MonitorSpec JSON>'|--schedule '<cron>' [--enabled true|false]|--enabled true|false [--schedule '<cron>'])|list [--json] [--fields ${columnNames(MONITOR_COLUMNS).join(",")}] [--limit N] [--offset N]|inspect <id>|remove <id>|test <id> [--type T] [--payload J]`,
						);
				} finally {
					await client.close();
				}
				break;
			}
			case "work": {
				const [command, ...args] = parsed.rest;
				const usage =
					'usage: gajaeway work run|start <name> [--cwd DIR] [--resume] [--model ID|--preset NAME] [--notify originKey (start only)] "<task text>"|status <name>|steer <name> <text>|retire <name>|jobs';
				if (command === "status" || command === "steer") {
					const name = args[0];
					const text = args.slice(1).join(" ").trim();
					if (
						!name ||
						!/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(name) ||
						(command === "status" ? args.length !== 1 : !text || args.slice(1).some((arg) => arg.startsWith("--")))
					)
						throw new Error(usage);
					const client = await GajaewayClient.connectSocket(parsed.socket);
					try {
						if (command === "status") {
							console.log(JSON.stringify(await client.request<WorkStatusResult>("work.status", { name })));
						} else {
							const result = await client.request<WorkSteerResult>("work.steer", { name, text });
							console.log(result.steered ? `steered: ${result.clientRef}` : `not steered: ${result.reason}`);
							if (!result.steered) process.exitCode = 1;
						}
					} finally {
						await client.close();
					}
					break;
				}
				if (command === "retire" || command === "jobs") {
					if (command === "retire" ? args.length !== 1 || !args[0] || args[0].startsWith("--") : args.length !== 0)
						throw new Error(usage);
					const client = await GajaewayClient.connectSocket(parsed.socket);
					try {
						if (command === "retire") {
							const result = await client.request<WorkRetireResult>("work.retire", { name: args[0] });
							console.log(
								result.retired
									? `retired: ${result.sessionKey} session=${result.sessionId} closed=${result.closed}`
									: `not retired: ${result.reason}`,
							);
						} else {
							const result = await client.request<WorkJobsResult>("work.jobs");
							for (const job of result.jobs) {
								// lane_key is the stored `work-<name>`; print the name work.retire accepts.
								const name = job.lane_key.startsWith("work-") ? job.lane_key.slice("work-".length) : job.lane_key;
								const state = job.quarantined
									? `HELD: quarantined reason=${job.reason} historical_state=${job.state}`
									: job.state;
								// HEAD is the progress signal that survives a dead op (issue #67).
								const head = job.last_commit
									? `${job.last_commit.sha.slice(0, 7)}@${job.last_commit.committed_at} ${JSON.stringify(job.last_commit.subject)}`
									: "-";
								console.log(
									`${name} ${state} session=${job.session_id || "-"} accepted=${job.accepted_at || "-"} op=${job.attempt?.op_ref || "-"} last=${job.last_activity_at || "-"} head=${head} ${job.worktree_path}`,
								);
							}
						}
					} finally {
						await client.close();
					}
					break;
				}
				if (command !== "run" && command !== "start") throw new Error(usage);
				const name = args[0];
				let cwd: string | undefined;
				let resume = false;
				let model: string | { preset: string } | undefined;
				let notify: OriginRef | undefined;
				const textParts: string[] = [];
				for (let i = 1; i < args.length; i++) {
					const arg = args[i];
					if (arg === "--cwd" || arg === "--model" || arg === "--preset" || arg === "--notify") {
						const value = args[++i];
						if (!value?.trim() || value.startsWith("--")) throw new Error(usage);
						if (arg === "--cwd") {
							if (cwd !== undefined || !isAbsolute(value)) throw new Error(usage);
							cwd = value;
						} else if (arg === "--notify") {
							if (command !== "start" || notify !== undefined) throw new Error(usage);
							try {
								notify = parseOriginKey(value);
							} catch {
								throw new Error(`${usage}\ninvalid --notify originKey`);
							}
						} else {
							if (model !== undefined) throw new Error(`${usage}\n--model and --preset are mutually exclusive`);
							model = arg === "--preset" ? { preset: value } : value;
						}
					} else if (arg === "--resume") resume = true;
					else if (arg?.startsWith("--")) throw new Error(usage);
					else textParts.push(arg as string);
				}
				const text = textParts.join(" ").trim();
				if (!name || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(name) || !text) throw new Error(usage);
				// This is a caller response-wait budget, never an attempt deadline.
				// Timeout/disconnect leaves the attempt observable via work.status.
				const client = await GajaewayClient.connectSocket(parsed.socket, {
					requestTimeoutMs: command === "run" ? 3_600_000 : 120_000,
				});
				try {
					const params: WorkStartParams = {
						name,
						text,
						...(cwd ? { cwd } : {}),
						...(resume ? { resume: true } : {}),
						...(model === undefined ? {} : { model }),
						...(notify === undefined ? {} : { notify }),
					};
					if (command === "start") {
						const result = await client.request<WorkStartResult>("work.start", params);
						if (result.started) {
							console.log(
								`started: ${result.sessionKey} session=${result.sessionId} job=${result.jobId} op=${result.opRef}`,
							);
						} else {
							console.log(`HELD: ${result.reason}\njob: ${result.jobId} state: ${result.state}`);
							process.exitCode = 1;
						}
					} else {
						const result = await client.request<WorkRunResult>("work.run", params);
						if (result.held) {
							console.log(`HELD: ${result.reason}\njob: ${result.jobId} state: ${result.state}`);
							process.exitCode = 1;
						} else console.log(result.text);
					}
				} finally {
					await client.close();
				}
				break;
			}
			case "services": {
				const service = parseServicesArgs(parsed.rest);
				const written = await installServices({
					binDir: service.binDir,
					...(service.launchAgentsDir === undefined ? {} : { launchAgentsDir: service.launchAgentsDir }),
					...(service.unitDir === undefined ? {} : { unitDir: service.unitDir }),
					...(service.platform === undefined ? {} : { platform: service.platform }),
					env: process.env,
					...options.services,
				});
				console.log(`services ${service.action}: wrote ${written.length} service definitions`);
				for (const definition of written) console.log(definition);
				break;
			}
			default:
				throw new Error(CLI_USAGE);
		}
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exitCode = 1;
	}
}

if (import.meta.main) await main();
