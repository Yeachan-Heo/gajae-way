import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajaeway/subsession";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";

test("tail-free monitor request reaches terminal status and last_assistant even when tail history is malformed", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-tail-free-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const calls: string[][] = [];
	let statuses = 0;
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		if (args.includes("tail")) throw new Error("Cannot key a positioned tail item without its authoritative revision.");
		if (args.includes("send"))
			return {
				exitCode: 1,
				stdout: "",
				stderr: "transport tore after prompt acceptance",
			};
		if (args.includes("status")) {
			statuses++;
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: {
						operationRef: "gw-m-test",
						status: { status: "terminal_ok" },
						summary: { completed: true },
					},
				}),
				stderr: "",
			};
		}
		if (args.includes("session.last_assistant"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, page: { items: ['[{"eventId":"event","note":"ok"}]'], complete: true } }),
				stderr: "",
			};
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const port = new BrokerSessionPort({
		database,
		cli: run,
		instanceId: "monitor-tail-free",
		tailRunner: new TailRunner({ run, repo: join(home, "workspace") }),
	});
	try {
		const result = await port.request({
			sessionId: "monitor-session",
			repo: join(home, "workspace"),
			originKey: "monitor/eventtype/memory.canonicalize",
			text: "author",
			opRef: "gw-m-test",
			observeTail: false,
			pollMs: 0,
		});
		expect(result.assistant.text).toContain('"note":"ok"');
		expect(result.receipt).toMatchObject({ sessionId: "monitor-session", operationRef: "gw-m-test" });
		expect(statuses).toBe(1);
		expect(calls.some((args) => args.includes("tail"))).toBe(false);
	} finally {
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});
