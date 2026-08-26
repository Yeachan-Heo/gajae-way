import { copyFile, stat } from "node:fs/promises";
import { isAbsolute, join } from "node:path";
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
	}>,
): void {
	console.log("INDEX  ORIGIN                                      EPOCH  CREATED AT                 LAST ACTIVITY AT");
	for (const [index, session] of sessions.entries())
		console.log(
			`${String(index).padEnd(6)} ${originKey(session.origin).padEnd(43)} ${String(session.epoch).padEnd(6)} ${session.createdAt.padEnd(26)} ${session.lastActivityAt ?? "-"}`,
		);
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
					else throw new Error("usage: gajaeway ops backup <path>|integrity|restore <backupPath>");
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
				if (command !== "run") throw new Error('usage: gajaeway work run <name> [--cwd DIR] "<task text>"');
				const name = args[0];
				let cwd: string | undefined;
				const textParts: string[] = [];
				for (let i = 1; i < args.length; i++) {
					if (args[i] === "--cwd") cwd = args[++i];
					else textParts.push(args[i] as string);
				}
				const text = textParts.join(" ").trim();
				if (!name || !text) throw new Error('usage: gajaeway work run <name> [--cwd DIR] "<task text>"');
				// Worker turns are long agentic runs: the request waits as long as the
				// gateway's own inactivity ceiling allows, not the default 30s.
				const client = await GajaewayClient.connectSocket(parsed.socket, { requestTimeoutMs: 3_600_000 });
				try {
					const result = await client.request<{ text: string; sessionKey: string }>("work.run", {
						name,
						text,
						...(cwd ? { cwd } : {}),
					});
					console.log(result.text);
				} finally {
					await client.close();
				}
				break;
			}
			default:
				throw new Error(
					"usage: gajaeway [--socket PATH] status|shutdown|chat|daemon run|sessions list [--json]|sessions inspect <originKey-or-index>|memory audit|memory search <query>|monitors ...|work run <name> [--cwd DIR] <text>|ops backup <path>|ops integrity|ops restore <backupPath>",
				);
		}
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exitCode = 1;
	}
}

if (import.meta.main) await main();
