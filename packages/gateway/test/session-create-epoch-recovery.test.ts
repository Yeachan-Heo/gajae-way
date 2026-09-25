import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliResult, CliRunner } from "@gajae-gateway/subsession";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";
import { noRelay } from "./session-port.fake";

test("a poisoned session-create key advances one epoch and retries with a fresh key", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-create-epoch-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = { canonicalAgentDir: home, identity: `gjc:${home}` };
	database.assertBrokerAuthority(authority, { initializeEmpty: true });
	let creates = 0;
	const inputs: Array<Record<string, unknown>> = [];
	const run: CliRunner = async (args) => {
		if (args.includes("session.create")) {
			creates++;
			inputs.push(JSON.parse(args[args.indexOf("--json-input") + 1]!) as Record<string, unknown>);
			if (creates <= 5)
				return {
					exitCode: 1,
					stdout: JSON.stringify({
						ok: false,
						error: { code: "terminal_uncertain", message: "startup did not complete" },
					}),
					stderr: "",
				};
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { sessionId: "fresh-session" } }),
				stderr: "",
			};
		}
		if (args.includes("inspect"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { session: { sessionId: "fresh-session", live: true } } }),
				stderr: "",
			};
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "create-epoch",
		tailRunner: new TailRunner({ stream: noRelay, repo: join(home, "workspace") }),
		sleep: async () => {},
	});
	try {
		const binding = await port.bind({
			originKey: "monitor/eventtype/memory.canonicalize",
			epoch: 0,
			repo: join(home, "workspace"),
			model: { preset: "gpt-heavy" },
		});
		expect(creates).toBe(6);
		expect(binding).toMatchObject({ sessionId: "fresh-session", epoch: 1, startupModelApplied: true });
		expect(database.getSessionRecord("monitor/eventtype/memory.canonicalize")).toMatchObject({
			epoch: 1,
			sessionId: "fresh-session",
		});
		expect(inputs.every((input) => input.modelPreset === "gpt-heavy")).toBe(true);
	} finally {
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("generic and malformed create failures do not mint a new idempotency key or rotate the epoch", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-create-generic-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = { canonicalAgentDir: home, identity: `gjc:${home}` };
	database.assertBrokerAuthority(authority, { initializeEmpty: true });
	const failures: Array<{ originKey: string; code?: string; result: CliResult }> = [
		{
			originKey: "create-operation-failed",
			code: "operation_failed",
			result: {
				exitCode: 1,
				stdout: JSON.stringify({
					schema: "gjc.command-error",
					version: 1,
					ok: false,
					command: ["sdk", "session", "raw", "global"],
					error: {
						code: "operation_failed",
						message: "The requested operation failed.",
						category: "operation",
						outcomeCertainty: "unknown",
						retryability: "unknown",
						references: [],
						nextSteps: [],
					},
					complete: true,
					evidence: { status: "inline" },
					continuation: null,
				}),
				stderr: "",
			},
		},
		{
			originKey: "create-endpoint-stale",
			code: "endpoint_stale",
			result: {
				exitCode: 1,
				stdout: JSON.stringify({ ok: false, error: { code: "endpoint_stale", message: "Session endpoint is stale." } }),
				stderr: "",
			},
		},
		{
			originKey: "create-session-unavailable",
			code: "session_unavailable",
			result: {
				exitCode: 1,
				stdout: JSON.stringify({
					ok: false,
					error: {
						code: "session_unavailable",
						message: "SDK session s-1 is unavailable through the session Router.",
					},
				}),
				stderr: "",
			},
		},
		{
			originKey: "create-malformed-json",
			result: { exitCode: 1, stdout: "{ malformed", stderr: "" },
		},
		{
			originKey: "create-unstructured-error",
			result: { exitCode: 1, stdout: "", stderr: "operation failed before producing an envelope" },
		},
		{
			originKey: "create-nonzero-ok-true",
			result: {
				exitCode: 1,
				stdout: JSON.stringify({ ok: true, result: { sessionId: "must-not-be-bound" } }),
				stderr: "",
			},
		},
	];
	let activeFailure = failures[0]!;
	let createCalls = 0;
	const idempotencyKeys: string[] = [];
	const run: CliRunner = async (args) => {
		if (!args.includes("session.create")) throw new Error(`unexpected command ${args.join(" ")}`);
		createCalls++;
		idempotencyKeys.push(args[args.indexOf("--idempotency-key") + 1]!);
		return activeFailure.result;
	};
	const repo = join(home, "workspace");
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "create-generic",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	});
	try {
		for (const failure of failures) {
			activeFailure = failure;
			createCalls = 0;
			idempotencyKeys.length = 0;
			let lastError: unknown;
			for (let attempt = 0; attempt < 2; attempt++) {
				const error = await port
					.bind({ originKey: failure.originKey, epoch: 0, repo })
					.catch((caught: unknown) => caught);
				expect(error).toBeInstanceOf(Error);
				lastError = error;
			}
			expect(createCalls).toBe(2);
			expect(idempotencyKeys).toHaveLength(2);
			expect(idempotencyKeys[1]).toBe(idempotencyKeys[0]);
			expect(database.getSessionRecord(failure.originKey)).toBeUndefined();
			expect(database.metaGet(`create_rotation:${failure.originKey}`)).toBeUndefined();
			if (failure.code) expect(lastError).toMatchObject({ name: "GjcCliError", details: { code: failure.code } });
		}
	} finally {
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});
