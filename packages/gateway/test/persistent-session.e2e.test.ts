import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { isTerminalStatus } from "@gajaeway/subsession";
import { BrokerSupervisor } from "../src/orchestrator/broker";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";

const enabled =
	process.env.GAJAEWAY_E2E_GJC === "1" &&
	typeof process.env.OPENAI_API_KEY === "string" &&
	process.env.OPENAI_API_KEY.length > 0 &&
	Bun.which("gjc") !== null;
const liveTest = enabled ? test : test.skip;

const scratchModels = [
	"providers:",
	"  layofflabs-anthropic:",
	"    baseUrl: https://api.layofflabs.com/v1",
	"    apiKeyEnv: OPENAI_API_KEY",
	"    api: anthropic-messages",
	"    auth: apiKey",
	"    models:",
	"      - id: claude-opus-5",
	"        reasoning: true",
	"        thinking:",
	"          minLevel: minimal",
	"          maxLevel: xhigh",
	"          mode: anthropic-adaptive",
	"          defaultLevel: medium",
	"          levels: [minimal, low, medium, high, xhigh]",
	"        input: [text]",
	"        contextWindow: 1000000",
	"        maxTokens: 128000",
	"",
].join("\n");

async function waitForTerminal(
	port: BrokerSessionPort,
	sessionId: string,
	repo: string,
	opRef: string,
	timeoutMs = 120_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		const status = await port.status({ sessionId, repo, opRef });
		if (isTerminalStatus(status.status.status)) {
			expect(status.status.status).toBe("terminal_ok");
			return;
		}
		await Bun.sleep(250);
	}
	throw new Error("live SDK operation did not reach a terminal status within its bounded E2E window");
}

function assistantTextsFromTail(stdout: string): readonly string[] {
	const parsed: unknown = JSON.parse(stdout);
	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed) || (parsed as { ok?: unknown }).ok !== true)
		throw new Error("live SDK tail did not return a successful envelope");
	const result = (parsed as { result?: unknown }).result;
	if (typeof result !== "object" || result === null || Array.isArray(result)) throw new Error("live SDK tail did not return a result object");
	const items = (result as { items?: unknown }).items;
	if (!Array.isArray(items)) throw new Error("live SDK tail did not return items");
	return items.flatMap((item) => {
		if (typeof item !== "object" || item === null || Array.isArray(item)) return [];
		const record = item as { kind?: unknown; payload?: unknown };
		if (record.kind !== "transcript" || typeof record.payload !== "object" || record.payload === null || Array.isArray(record.payload)) return [];
		const payload = record.payload as { role?: unknown; content?: unknown };
		if (payload.role !== "assistant" || !Array.isArray(payload.content)) return [];
		return payload.content.flatMap((block) =>
			typeof block === "string"
				? [block]
				: typeof block === "object" && block !== null && !Array.isArray(block) && typeof (block as { text?: unknown }).text === "string"
					? [(block as { text: string }).text]
					: [],
		);
	});
}

liveTest(
	"real gjc-hosted scratch session accepts sends, a mid-turn steer, tail evidence, and duplicate-op-ref rejection",
	async () => {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-persistent-e2e-"));
		const repo = join(home, "workspace");
		await mkdir(repo, { recursive: true, mode: 0o700 });
		const database = await GatewayDatabase.open(join(home, "gateway.db"));
		const broker = new BrokerSupervisor({
			home,
			instanceId: database.instanceId,
			cwd: repo,
			healthIntervalMs: 60_000,
		});
		let sessionId: string | undefined;
		try {
			// gjc hosts sessions via its own per-agent-dir daemon (auto-started by the first sdk command):
			// the latter is only a relay. The supervisor inherits provider variables while
			// forcing all GJC state into this disposable agent directory.
			await mkdir(broker.agentDir, { recursive: true, mode: 0o700 });
			await writeFile(join(broker.agentDir, "models.yml"), scratchModels, { mode: 0o600 });
			await broker.preflight();
			await broker.start();
			expect(broker.generation).toBe(1);

			const port = new BrokerSessionPort({
				database,
				cli: broker.cli,
				instanceId: database.instanceId,
				tailRunner: new TailRunner({ run: broker.cli, stream: (sessionId) => broker.openStream(sessionId), repo, pollIntervalMs: 250 }),
			});
			const binding = await port.bind({ originKey: "loopback/loopback/persistent-e2e", epoch: 0, repo });
			sessionId = binding.sessionId;
			await port.setModel({ sessionId, repo, selection: "layofflabs-anthropic/claude-opus-5" });

			const firstRef = `gw-e2e-first-${crypto.randomUUID()}`;
			const first = await port.request({
				sessionId,
				repo,
				originKey: "loopback/loopback/persistent-e2e",
				text: "Reply with exactly PERSISTENT_E2E_FIRST.",
				opRef: firstRef,
				waitTimeoutMs: 120_000,
				pollMs: 250,
			});
			expect(first.assistant.text).toContain("PERSISTENT_E2E_FIRST");
			await expect(
				port.send({ sessionId, repo, text: "This duplicate must be rejected.", opRef: firstRef }),
			).rejects.toMatchObject({ name: "OpRefRejectedError", code: "client_ref_conflict" });

			const steerRef = `gw-e2e-steer-${crypto.randomUUID()}`;
			const steerMarker = "PERSISTENT_E2E_STEER_MARKER";
			const receipt = await port.send({
				sessionId,
				repo,
				text:
					"Without using tools, write a detailed 1,800-word explanation of persistent-session recovery. Use multiple sections and do not finish early; end with PERSISTENT_E2E_BASELINE.",
				opRef: steerRef,
			});
			expect(receipt.operationRef).toBe(steerRef);

			const inFlightDeadline = Date.now() + 15_000;
			let inFlight = false;
			while (Date.now() < inFlightDeadline) {
				const status = await port.status({ sessionId, repo, opRef: steerRef });
				if (status.status.status === "in_flight") {
					inFlight = true;
					break;
				}
				if (isTerminalStatus(status.status.status)) break;
				await Bun.sleep(250);
			}
			expect(inFlight).toBe(true);
			await port.steer({
				sessionId,
				repo,
				text: `End the answer with exactly ${steerMarker}.`,
				clientRef: `gw-e2e-control-${crypto.randomUUID()}`,
			});
			await waitForTerminal(port, sessionId, repo, steerRef);
			const assistant = await port.fetchLastAssistant({ sessionId, repo });
			expect(assistant.text).toContain(steerMarker);
			// A historical strict attach correctly reports retention_gap on this runtime;
			// this probe reads its explicit non-strict resync solely as E2E evidence,
			// while status remains the terminal authority in the production port.
			const tailResult = await broker.cli([
				"sdk",
				"session",
				"tail",
				sessionId,
				"--until-idle",
				"--all-events",
				"--timeout-ms",
				"30000",
			]);
			expect(tailResult.exitCode).toBe(0);
			expect(assistantTextsFromTail(tailResult.stdout).some((text) => text.includes(steerMarker))).toBe(true);
		} finally {
			if (sessionId) {
				try {
					await broker.cli([
						"sdk",
						"session",
						"raw",
						"global",
						"--op",
						"session.close",
						"--idempotency-key",
						`gw-e2e-close-${crypto.randomUUID()}`,
						"--json-input",
						JSON.stringify({ sessionId, cwd: repo }),
					]);
				} catch {
					// The scratch host is still stopped below; close uncertainty must not hide
					// the original assertion failure or retain a broker owned by this test.
				}
			}
			try {
				await broker.stop();
			} finally {
				database.close();
				await rm(home, { recursive: true, force: true });
			}
		}
	},
	180_000,
);
