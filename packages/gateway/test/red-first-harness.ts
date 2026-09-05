import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PersonaSessionManager, type PersonaSessionManagerOptions } from "../src/orchestrator/persona-session";
import { GatewayDatabase } from "../src/store/db";
import { ScriptedSessionPort } from "./session-port.fake";

/**
 * Shared harness for the red-first scenarios. It lives in the package test tree so
 * a red file can move here unchanged when its increment lands; `red-first/` re-exports it.
 */
export const FIXTURE_VERSION = "red-first-v1";
export const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "red-first" } as const;
export const KEY = "loopback/loopback/red-first";

export async function harness(port = new ScriptedSessionPort(), options: Partial<PersonaSessionManagerOptions> = {}) {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-red-first-"));
	const repo = join(home, "workspace");
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const logs: string[] = [];
	const deliveries: { text: string }[] = [];
	const manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "red-first",
		repo,
		log: (line) => logs.push(line),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onTerminal: ({ text }) => {
				deliveries.push({ text });
			},
			// Failure notices are deliveries too (Q1: operation_lost is always visible).
			onFailure: ({ error }) => {
				deliveries.push({ text: `[turn failed] ${error.message}` });
			},
		}),
		...options,
	});
	return {
		home,
		repo,
		database,
		manager,
		port,
		logs,
		deliveries,
		enqueue(messageId: string) {
			database.inboundEnqueue({ messageId, originKey: KEY, originRefJson: JSON.stringify(ORIGIN), body: messageId });
		},
		async close() {
			await manager.stop();
			database.close();
			await rm(home, { recursive: true, force: true });
		},
	};
}

export async function eventually(predicate: () => boolean, message: string, timeoutMs = 2000) {
	const end = Date.now() + timeoutMs;
	while (!predicate() && Date.now() < end) await Bun.sleep(5);
	if (!predicate()) throw new Error(message);
}
