import { expect, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { GlobalGjcClient } from "../src/orchestrator/broker";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { type TailFrame, TailRunner } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";

const liveTest = process.env.GAJAEWAY_E2E_GJC === "1" ? test : test.skip;
const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "relay-live" } as const;
const KEY = "loopback/loopback/relay-live";

async function eventually(predicate: () => boolean, message: string, timeoutMs = 180_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (predicate()) return;
		await Bun.sleep(100);
	}
	expect(predicate(), message).toBe(true);
}

/**
 * The relay-owned turn against a real gjc broker: one persona turn that
 * speaks, runs a tool, and speaks again must produce exactly one interim
 * message, tool activity, and one terminal answer - through the gateway's own
 * actor - with zero `gjc sdk session tail` spawns and no poll diagnostics.
 */
liveTest(
	"a real persona turn streams mid-work speech, tool activity and the final answer over its owned relay",
	async () => {
		const executable = process.env.GJC_EXECUTABLE;
		const agentDir = process.env.GJC_CODING_AGENT_DIR;
		if (!executable || !agentDir || !isAbsolute(executable) || !isAbsolute(agentDir))
			throw new Error("live test requires explicit absolute GJC_EXECUTABLE and GJC_CODING_AGENT_DIR");
		const home = await mkdtemp(join(tmpdir(), "gajaeway-relay-live-"));
		const repo = join(home, "workspace");
		await mkdir(repo, { mode: 0o700 });
		expect(await Bun.spawn(["git", "init", "--quiet", repo], { stdout: "ignore", stderr: "pipe" }).exited).toBe(0);
		const database = await GatewayDatabase.open(join(home, "gateway.db"));
		const canonicalAgentDir = await realpath(agentDir);
		const authority = { canonicalAgentDir, identity: `gjc:${canonicalAgentDir}` };
		database.assertBrokerAuthority(authority, { initializeEmpty: true });
		const broker = new GlobalGjcClient({ executable, agentDir, cwd: repo, healthIntervalMs: 60_000 });
		const cliCalls: string[][] = [];
		const logs: string[] = [];
		const frames: TailFrame[] = [];
		const interim: string[] = [];
		const terminal: string[] = [];
		let manager: PersonaSessionManager | undefined;
		let sessionId: string | undefined;
		try {
			await broker.preflight();
			await broker.start();
			const port = new BrokerSessionPort({
				database,
				authority,
				instanceId: database.instanceId,
				cli: async (args, options) => {
					cliCalls.push([...args]);
					return await broker.cli(args, options);
				},
				tailRunner: new TailRunner({ stream: (id) => broker.openStream(id), repo }),
			});
			manager = new PersonaSessionManager({
				database,
				port,
				instanceId: database.instanceId,
				repo,
				onTurnStart: ({ trigger, sessionId: bound }) => {
					sessionId = bound;
					return {
						text: trigger.body,
						onFrame: ({ frame }) => {
							frames.push(frame);
							if (frame.assistantText && !frame.steerEcho) interim.push(frame.assistantText);
						},
						onTerminal: ({ text }) => {
							terminal.push(text);
						},
					};
				},
				log: (line) => logs.push(line),
			});
			expect(
				database.inboundEnqueue({
					messageId: "relay-live-1",
					originKey: KEY,
					originRefJson: JSON.stringify(ORIGIN),
					body: [
						"Do exactly this, in order, and nothing else:",
						"1. Reply with exactly one short sentence: RELAY_LIVE_INTERIM.",
						`2. Run the bash tool with the command: ls ${repo}`,
						"3. Reply with exactly one short sentence: RELAY_LIVE_FINAL.",
					].join("\n"),
					receivedAt: new Date().toISOString(),
				}),
			).toBe(true);
			await manager.notifyInbound(KEY);
			await eventually(() => terminal.length === 1, `turn did not settle; logs:\n${logs.slice(-40).join("\n")}`);
			// Final answer came from the relay's last assistant message, not a
			// transcript re-read: the terminal path logged tail evidence as present.
			expect(terminal[0]).toContain("RELAY_LIVE_FINAL");
			expect(
				logs.some((line) => line.includes("terminal_status_reconciled") && line.includes("tail_evidence=unavailable")),
			).toBe(false);
			// Mid-work speech: the first sentence, delivered before the tool ran;
			// the final sentence is the last frame's text (the terminal path claims
			// its slot). No third message, no duplicate.
			const speech = interim.map((text) => text.trim());
			expect(speech.some((text) => text.includes("RELAY_LIVE_INTERIM"))).toBe(true);
			expect(speech.filter((text) => text.includes("RELAY_LIVE_INTERIM"))).toHaveLength(1);
			expect(speech.filter((text) => text.includes("RELAY_LIVE_FINAL"))).toHaveLength(1);
			const kinds = frames.map((frame) => frame.rawKind);
			expect(kinds).toContain("tool_execution_start");
			expect(kinds).toContain("tool_execution_end");
			expect(kinds.at(-1)).toBe("agent_end");
			const toolStart = frames.find((frame) => frame.rawKind === "tool_execution_start")!;
			expect(toolStart.payload.toolName).toBe("bash");
			expect(kinds.indexOf("tool_execution_start")).toBeGreaterThan(
				frames.findIndex((frame) => frame.assistantText?.includes("RELAY_LIVE_INTERIM")),
			);
			// Every frame was attributed by the host to this one turn.
			const correlations = new Set(frames.map((frame) => `${frame.commandId}/${frame.turnId}`));
			expect(correlations.size).toBe(1);
			// The whole turn used the relay: no tail poll, no CLI send/status.
			expect(
				cliCalls.filter((args) => args.includes("tail") || args.includes("send") || args.includes("status")),
			).toEqual([]);
			expect(
				logs.filter((line) => /^tail_poll|^tail_error|^tail_frame_pre_floor|^tail_frame_duplicate/.test(line)),
			).toEqual([]);
			expect(logs.filter((line) => line.startsWith("tail_frame_foreign"))).toEqual([]);
		} finally {
			await manager?.stop();
			if (sessionId) {
				try {
					await broker.cli(
						["sdk", "session", "raw", "control", sessionId, "--op", "session.close", "--json-input", "{}"],
						{
							timeoutMs: 15_000,
						},
					);
				} catch {
					/* best-effort scratch cleanup */
				}
			}
			await broker.stop();
			database.close();
			await rm(home, { recursive: true, force: true });
		}
	},
	300_000,
);

liveTest(
	"the real serve CLI's refusal for an unknown session rejects attach as session_unavailable, not a hello timeout",
	async () => {
		const executable = process.env.GJC_EXECUTABLE;
		const agentDir = process.env.GJC_CODING_AGENT_DIR;
		if (!executable || !agentDir) throw new Error("live test requires GJC_EXECUTABLE and GJC_CODING_AGENT_DIR");
		const home = await mkdtemp(join(tmpdir(), "gajaeway-relay-refusal-"));
		const repo = join(home, "workspace");
		await mkdir(repo, { mode: 0o700 });
		const broker = new GlobalGjcClient({ executable, agentDir, cwd: repo, healthIntervalMs: 60_000 });
		try {
			await broker.preflight();
			await broker.start();
			const runner = new TailRunner({ stream: (id) => broker.openStream(id), repo });
			const startedAt = Date.now();
			const error = await runner
				.attach({ sessionId: "00000000-0000-4000-8000-00000000dead", brokerGeneration: 1, repo })
				.catch((e: unknown) => e);
			expect(error).toBeInstanceOf(Error);
			expect((error as { code?: string }).code).toBe("session_unavailable");
			// The refusal envelope is read from the CLI's stderr; no 15 s hello wait.
			expect(Date.now() - startedAt).toBeLessThan(10_000);
		} finally {
			await broker.stop();
			await rm(home, { recursive: true, force: true });
		}
	},
	60_000,
);
