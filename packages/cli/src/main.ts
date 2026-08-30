import { copyFile, stat } from "node:fs/promises";
import { isAbsolute, join } from "node:path";
import type { OpsCycleResult } from "@gajaeway/protocol";
import { LOOPBACK_ORIGIN, originKey } from "@gajaeway/protocol";
import { GajaewayClient } from "@gajaeway/sdk";

export function socketPath(home = process.env.GAJAEWAY_HOME): string {
	return `${home ?? `${process.env.HOME ?? "~"}/.gajaeway`}/gateway.sock`;
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

function printSessions(
	sessions: Array<{
		origin: Parameters<typeof originKey>[0];
		epoch: number;
		createdAt: string;
		lastActivityAt: string | null;
		bootstrap: { readonly pending: boolean; readonly byteCount: number; readonly truncated: boolean };
	}>,
): void {
	console.log(
		"INDEX  ORIGIN                                      EPOCH  BOOTSTRAP        CREATED AT                 LAST ACTIVITY AT",
	);
	for (const [index, session] of sessions.entries())
		console.log(
			`${String(index).padEnd(6)} ${originKey(session.origin).padEnd(43)} ${String(session.epoch).padEnd(6)} ${(session.bootstrap.pending ? "pending" : `${session.bootstrap.byteCount}B${session.bootstrap.truncated ? "/trunc" : ""}`).padEnd(16)} ${session.createdAt.padEnd(26)} ${session.lastActivityAt ?? "-"}`,
		);
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
	// Always rendered: an empty subsystem must be distinguishable from an absent one.
	lines.push(
		`monitors: ${cycle.monitorEvents.length ? cycle.monitorEvents.map((m) => `${m.stage}=${m.count}`).join(" ") : "none"}`,
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
	let header: Uint8Array;
	try {
		header = new Uint8Array(await Bun.file(backupPath).slice(0, 16).arrayBuffer());
	} catch {
		throw new Error(`Backup is not readable: ${backupPath}`);
	}
	if (new TextDecoder().decode(header) !== "SQLite format 3\u0000")
		throw new Error(`Backup is not a SQLite database: ${backupPath}`);
	const databasePath = join(gatewayHome(), "gateway.db");
	const preservedPath = `${databasePath}.pre-restore-${new Date().toISOString().replace(/[:.]/g, "-")}`;
	await copyFile(databasePath, preservedPath);
	await copyFile(backupPath, databasePath);
	console.log(`Validated SQLite backup: ${backupPath}`);
	console.log(`Copied current database to: ${preservedPath}`);
	console.log(`Restored backup to: ${databasePath}`);
}

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
		await new Promise<void>((resolve) => {
			const timer = setTimeout(() => {
				turnWaiters.delete(turnId);
				console.error("(turn timed out after 120s)");
				resolve();
			}, 120_000);
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

export async function main(args = process.argv.slice(2)): Promise<void> {
	const parsed = parseArgs(args);
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
			case "sessions": {
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
						if (selector === "--json") console.log(JSON.stringify(result));
						else if (!selector) printSessions(result.sessions);
						else throw new Error("usage: gajaeway sessions list [--json]");
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
					} else throw new Error("usage: gajaeway sessions list [--json]|inspect <originKey-or-index>");
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
				const client = await GajaewayClient.connectSocket(parsed.socket);
				try {
					if (command === "backup" && path) console.log(JSON.stringify(await client.request("ops.backup", { path })));
					else if (command === "integrity") console.log(JSON.stringify(await client.request("ops.integrity")));
					else if (command === "cycle") {
						const cycle = await client.opsCycle();
						if (parsed.rest[1] === "--json") console.log(JSON.stringify(cycle));
						else if (parsed.rest[1]) throw new Error("usage: gajaeway ops cycle [--json]");
						else for (const line of renderCycle(cycle)) console.log(line);
						// Fail-closed: a gated/degraded cycle exits non-zero even though the
						// request itself succeeded, so scripts can never read it as healthy.
						process.exitCode = cycleExitCode(cycle);
					} else throw new Error("usage: gajaeway ops backup <path>|cycle [--json]|integrity|restore <backupPath>");
				} finally {
					await client.close();
				}
				break;
			}
			case "memory": {
				const client = await GajaewayClient.connectSocket(parsed.socket);
				try {
					if (parsed.rest[0] === "audit") {
						const result = await client.request<{ ok: boolean; issues: unknown[] }>("memory.audit");
						console.log(JSON.stringify(result.issues));
						if (!result.ok) process.exitCode = 1;
					} else if (parsed.rest[0] === "search" && parsed.rest.slice(1).join(" ")) {
						console.log(
							JSON.stringify(await client.request("memory.search", { query: parsed.rest.slice(1).join(" ") })),
						);
					} else throw new Error("usage: gajaeway memory audit|search <query>");
				} finally {
					await client.close();
				}
				break;
			}
			case "monitors": {
				const client = await GajaewayClient.connectSocket(parsed.socket);
				try {
					const [command, ...args] = parsed.rest;
					if (command === "add" && args[0] === "--json" && args[1])
						console.log(JSON.stringify(await client.request("monitor.add", JSON.parse(args[1]))));
					else if (command === "list") console.log(JSON.stringify(await client.request("monitor.list")));
					else if (command === "inspect" && args[0])
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
							"usage: gajaeway monitors add --json '<MonitorSpec json>'|list|inspect <id>|remove <id>|test <id> [--type T] [--payload J]",
						);
				} finally {
					await client.close();
				}
				break;
			}
			case "work": {
				const [command, ...args] = parsed.rest;
				if (command !== "run") throw new Error('usage: gajaeway work run <name> [--cwd DIR] [--resume] "<task text>"');
				const name = args[0];
				let cwd: string | undefined;
				let resume = false;
				const textParts: string[] = [];
				for (let i = 1; i < args.length; i++) {
					if (args[i] === "--cwd") cwd = args[++i];
					else if (args[i] === "--resume") resume = true;
					else textParts.push(args[i] as string);
				}
				const text = textParts.join(" ").trim();
				if (!name || !text) throw new Error('usage: gajaeway work run <name> [--cwd DIR] "<task text>"');
				// Worker turns are long agentic runs: the request waits as long as the
				// gateway's own inactivity ceiling allows, not the default 30s.
				const client = await GajaewayClient.connectSocket(parsed.socket, { requestTimeoutMs: 3_600_000 });
				try {
					const result = await client.request<
						| { held: true; jobId: string; state: string; reason: string }
						| { held: false; text: string; sessionKey: string; jobId: string; opRef: string }
					>("work.run", {
						name,
						text,
						...(cwd ? { cwd } : {}),
						...(resume ? { resume: true } : {}),
					});
					if (result.held) {
						console.log(`HELD: ${result.reason}\njob: ${result.jobId} state: ${result.state}`);
					} else {
						console.log(result.text);
					}
				} finally {
					await client.close();
				}
				break;
			}
			default:
				throw new Error(
					"usage: gajaeway [--socket PATH] status|shutdown|chat|daemon run|sessions list [--json]|sessions inspect <originKey-or-index>|memory audit|memory search <query>|monitors ...|work run <name> [--cwd DIR] <text>|ops backup <path>|ops cycle [--json]|ops integrity|ops restore <backupPath>",
				);
		}
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exitCode = 1;
	}
}

if (import.meta.main) await main();
