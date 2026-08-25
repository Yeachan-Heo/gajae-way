import { LOOPBACK_ORIGIN } from "@gajaeway/protocol";
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
							"usage: gajaeway monitors add --json '<MonitorSpec json>'|list|inspect <id>|test <id> [--type T] [--payload J]",
						);
				} finally {
					await client.close();
				}
				break;
			}
			default:
				throw new Error(
					"usage: gajaeway [--socket PATH] status|shutdown|chat|daemon run|memory audit|memory search <query>",
				);
		}
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exitCode = 1;
	}
}

if (import.meta.main) await main();
