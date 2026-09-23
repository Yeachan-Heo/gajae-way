import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type CliResult, type CliRunner, GjcCliError } from "@gajae-gateway/subsession";
import { BrokerSessionPort, parseWorkerOutputResponse, type WorkerOutputInput } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";
import {
	createOwnedSessionFixture,
	initializeTestBrokerAuthority,
	noRelay,
	type ScriptedRelayReply,
	type ScriptedRelayRequest,
	ScriptedSessionPort,
	scriptedRelay,
	steerRefused,
} from "./session-port.fake";

const floor = 1_800_000_000_000;
const input: WorkerOutputInput = {
	sessionId: "worker-session",
	repo: "/tmp/worker-repo",
	opRef: "gw-worker-attempt-1",
	notBeforeMs: floor,
	terminalIdentity: { commandId: "command-1", turnId: "turn-1" },
};
const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
	for (const close of cleanup.splice(0)) await close();
});

function response(envelope: unknown, exitCode = 0): CliResult {
	return { exitCode, stdout: JSON.stringify(envelope), stderr: "" };
}

/** Installed SDK TurnResultPage and TurnResultContent, not guessed transcript identity fields. */
function terminal(text = "original answer", overrides: Record<string, unknown> = {}) {
	return {
		kind: "prompt",
		status: "terminal_ok",
		clientRef: input.opRef,
		commandId: "command-1",
		turnId: "turn-1",
		startedAt: floor,
		terminalAt: floor + 1_000,
		receiptState: "present",
		content: {
			version: 1,
			type: "text",
			text,
			byteLength: new TextEncoder().encode(text).length,
			truncated: false,
		},
		...overrides,
	};
}

async function broker(
	run: CliRunner,
	relay?: (request: ScriptedRelayRequest) => ScriptedRelayReply | Promise<ScriptedRelayReply>,
): Promise<BrokerSessionPort> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-work-output-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	cleanup.push(async () => {
		database.close();
		await rm(home, { recursive: true, force: true });
	});
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	await createOwnedSessionFixture(database, authority, {
		sessionId: input.sessionId,
		repo: input.repo,
		originKey: "work/output",
		epoch: 0,
	});
	return new BrokerSessionPort({
		authority,
		database,
		cli: run,
		instanceId: "output-test",
		tailRunner: new TailRunner({
			stream: relay ? scriptedRelay(relay).spawn : noRelay,
			repo: input.repo,
			stallTimeoutMs: 1_000,
		}),
		now: () => floor + 2_000,
	});
}

/** A steer answered by the session's relay: the envelope IS the control_response body. */
function steerRelay(envelope: () => unknown) {
	return (request: ScriptedRelayRequest): ScriptedRelayReply => {
		expect(request.type).toBe("control_request");
		expect(request.operation).toBe("turn.steer");
		return envelope() as ScriptedRelayReply;
	};
}

function parse(result: unknown, overrides: Partial<WorkerOutputInput> = {}) {
	return parseWorkerOutputResponse({ ...input, ...overrides }, response({ ok: true, result }), floor + 2_000);
}

test("worker output uses the actual operation-owned single-page query and preserves full original Unicode body", async () => {
	const text = `  ${"가나다".repeat(700)}\nlast line\n`;
	const calls: string[][] = [];
	const port = await broker(async (args, options) => {
		calls.push([...args]);
		expect(args).toEqual([
			"sdk",
			"session",
			"raw",
			"query",
			input.sessionId,
			"--query",
			"turn.result",
			"--json-input",
			JSON.stringify({ kind: "prompt", clientRef: input.opRef }),
		]);
		expect(options?.timeoutMs).toBe(15_000);
		return response({ ok: true, result: terminal(text) });
	});
	const output = await port.fetchWorkerOutput(input);
	expect(output).toEqual({
		status: "proven",
		text,
		observedAtMs: floor + 2_000,
		provenance: {
			source: "turn.result",
			fullness: "original",
			sessionId: input.sessionId,
			repo: input.repo,
			opRef: input.opRef,
			clientRef: input.opRef,
			commandId: "command-1",
			turnId: "turn-1",
			terminalAt: floor + 1_000,
			contentVersion: 1,
			byteLength: new TextEncoder().encode(text).length,
		},
	});
	expect(calls).toHaveLength(1);
});

test("a neighboring prior reply less than two seconds before the floor is never current output", () => {
	expect(parse(terminal("neighbor", { startedAt: floor - 1_500, terminalAt: floor - 1 }))).toEqual({
		status: "unavailable",
		code: "invalid_evidence",
	});
	expect(parse(terminal("neighbor", { clientRef: "previous-operation" }))).toEqual({
		status: "unavailable",
		code: "identity_mismatch",
	});
});

test("after-floor transcript timestamps and completed traversal do not prove attribution or finality", () => {
	const transcript = { role: "assistant", ts: new Date(floor + 10).toISOString(), body: "ambiguous current text" };
	expect(
		parseWorkerOutputResponse(input, response({ ok: true, page: { items: [transcript], complete: true } }), floor + 20),
	).toEqual({ status: "unavailable", code: "invalid_evidence" });
	expect(parse(terminal("text", { clientRef: undefined }))).toEqual({
		status: "unavailable",
		code: "identity_mismatch",
	});
	expect(parse(terminal("text", { turnId: "another-turn" }))).toEqual({
		status: "unavailable",
		code: "identity_mismatch",
	});
});

test("summary-only, incomplete and malformed original content cannot become output or silence", () => {
	expect(parse(terminal("", { content: undefined, textSummary: "." }))).toEqual({
		status: "unavailable",
		code: "invalid_evidence",
	});
	const full = terminal("actual answer");
	expect(parse({ ...full, content: { ...full.content, truncated: true } })).toEqual({
		status: "unavailable",
		code: "incomplete_body",
	});
	for (const patch of [{ truncated: undefined }, { byteLength: 1 }, { type: "summary" }, { version: 2 }]) {
		expect(parse({ ...full, content: { ...full.content, ...patch } })).toEqual({
			status: "unavailable",
			code: "invalid_evidence",
		});
	}
	expect(parse(terminal("original", { textSummary: "." }))).toMatchObject({ status: "proven", text: "original" });
	expect(parse(terminal(" . \n"))).toMatchObject({ status: "proven", text: " . \n" });
});

test("exact current clientRef is supported without fabricating optional command or turn ids", () => {
	const output = parse(terminal("current", { commandId: undefined, turnId: undefined }), {
		terminalIdentity: undefined,
	});
	expect(output.status).toBe("proven");
	if (output.status !== "proven") throw new Error("expected proof");
	expect(output.provenance.clientRef).toBe(input.opRef);
	expect("turnId" in output.provenance).toBe(false);
	expect("commandId" in output.provenance).toBe(false);
	expect(parse(terminal("current", { commandId: undefined }))).toEqual({
		status: "unavailable",
		code: "identity_mismatch",
	});
});

test("absence is retryable, affirmative missing authority is unavailable, and transport diagnostics stay private", async () => {
	expect(parse({ status: "unknown" })).toEqual({ status: "absent", code: "output_pending" });
	expect(parse(terminal("", { status: "in_flight", content: undefined }))).toEqual({
		status: "absent",
		code: "output_pending",
	});
	expect(parse(terminal("", { content: undefined, receiptState: "unknown" }))).toEqual({
		status: "absent",
		code: "output_pending",
	});
	expect(parse(terminal("", { content: undefined, receiptState: "missing" }))).toEqual({
		status: "unavailable",
		code: "output_unavailable",
	});
	for (const code of ["session_unavailable", "operation_not_session_owned", "resource_gone", "unsupported_query"]) {
		expect(
			parseWorkerOutputResponse(input, response({ ok: false, error: { code, message: "private" } }, 1), floor),
		).toEqual({ status: "unavailable", code: "output_unavailable" });
	}
	const port = await broker(async () => {
		throw new Error("secret endpoint or diagnostic");
	});
	expect(await port.fetchWorkerOutput(input)).toEqual({ status: "absent", code: "transport_error" });
});

test("pre-cancel and stale generation perform no query; cancellation of an in-flight read ignores late output", async () => {
	let calls = 0;
	let finish!: (result: CliResult) => void;
	const port = await broker(async () => {
		calls++;
		return await new Promise<CliResult>((resolve) => {
			finish = resolve;
		});
	});
	const alreadyAborted = new AbortController();
	alreadyAborted.abort();
	expect(await port.fetchWorkerOutput({ ...input, signal: alreadyAborted.signal })).toEqual({
		status: "unavailable",
		code: "cancelled",
	});
	expect(await port.fetchWorkerOutput({ ...input, isCurrent: () => false })).toEqual({
		status: "unavailable",
		code: "cancelled",
	});
	expect(calls).toBe(0);
	const controller = new AbortController();
	const pending = port.fetchWorkerOutput({ ...input, signal: controller.signal });
	expect(calls).toBe(1);
	controller.abort();
	expect(await pending).toEqual({ status: "unavailable", code: "cancelled" });
	finish(response({ ok: true, result: terminal() }));
	let current = true;
	const stale = port.fetchWorkerOutput({ ...input, isCurrent: () => current });
	current = false;
	finish(response({ ok: true, result: terminal() }));
	expect(await stale).toEqual({ status: "unavailable", code: "cancelled" });
});

test("fake accepts send while response is delayed and supports production raw output proof fixtures", async () => {
	let release!: () => void;
	const gate = new Promise<void>((resolve) => {
		release = resolve;
	});
	const port = new ScriptedSessionPort({
		onSend: async (sent, fake) => {
			await gate;
			fake.complete(sent.opRef, "delayed original");
		},
	});
	const attempt = { ...input, notBeforeMs: Date.now(), terminalIdentity: undefined };
	const receipt = await port.send({ ...attempt, text: "work" });
	expect(receipt.operationRef).toBe(input.opRef);
	expect((await port.status(input)).status.status).toBe("in_flight");
	expect(await port.fetchWorkerOutput(attempt)).toEqual({ status: "absent", code: "output_pending" });
	release();
	await gate;
	expect(await port.fetchWorkerOutput(attempt)).toMatchObject({ status: "proven", text: "delayed original" });
	port.setWorkerOutputFixture(input.opRef, response({ ok: true, result: terminal("fixture original") }));
	expect(await port.fetchWorkerOutput(input)).toMatchObject({ status: "proven", text: "fixture original" });
	port.setWorkerOutputFixture(
		input.opRef,
		response({ ok: true, page: { items: [{ textSummary: "." }], complete: true } }),
	);
	expect(await port.fetchWorkerOutput(input)).toEqual({ status: "unavailable", code: "invalid_evidence" });
});

test("production steer accepts the observed SDK 0.16.3 raw control response without an accepted boolean", async () => {
	const clientRef = "probe-terminal-steer-20260909";
	const observed = {
		type: "control_response",
		ok: true,
		result: {
			sessionId: input.sessionId,
			commandId: "command-1",
			turnId: "turn-1",
			clientRef,
			status: "accepted",
			acceptedAt: 1788932429645,
		},
	};
	const port = await broker(
		async () => {
			throw new Error("steer must not spawn a CLI");
		},
		steerRelay(() => observed),
	);
	await expect(
		port.steer({ sessionId: input.sessionId, repo: input.repo, text: "steer", clientRef }),
	).resolves.toBeUndefined();
});

test("production steer requires structured acceptance, preserves caller clientRef, and rejects nested refusal", async () => {
	let envelope: unknown = { ok: true, result: { accepted: true, status: "accepted", clientRef: "caller-steer-ref" } };
	const port = await broker(
		async () => {
			throw new Error("steer must not spawn a CLI");
		},
		(request) => {
			expect(request).toEqual({
				type: "control_request",
				operation: "turn.steer",
				input: { text: "steer", clientRef: "caller-steer-ref" },
			});
			return envelope as ScriptedRelayReply;
		},
	);
	const steer = { sessionId: input.sessionId, repo: input.repo, text: "steer", clientRef: "caller-steer-ref" };
	await port.steer(steer);
	for (const refusal of [
		{ ok: false, error: { code: "busy", message: "refused" } },
		{ ok: true, result: { accepted: false, status: "rejected", error: { code: "busy" } } },
		{ ok: true, result: { accepted: true, status: "rejected" } },
		{ ok: true, result: { accepted: true, ok: false } },
		{ ok: true, result: { accepted: true, clientRef: "another-steer" } },
		{ ok: true, result: {} },
	]) {
		envelope = refusal;
		await expect(port.steer(steer)).rejects.toBeInstanceOf(GjcCliError);
	}
	const fake = new ScriptedSessionPort({
		onSteer: () => {
			throw steerRefused();
		},
	});
	await expect(fake.steer(steer)).rejects.toBeInstanceOf(GjcCliError);
	expect(fake.steers[0]?.clientRef).toBe("caller-steer-ref");
});

test("completed fake status reflects original operation evidence without inventing SDK IDs", async () => {
	for (const finish of ["complete", "completeWithoutAnswerFrame"] as const) {
		const port = new ScriptedSessionPort();
		const before = Date.now();
		const receipt = await port.send({ sessionId: input.sessionId, repo: input.repo, opRef: input.opRef, text: "work" });
		expect((await port.status(input)).status.receiptState).toBeUndefined();
		port[finish](input.opRef, "original answer");
		const report = await port.status(input);
		expect(report.status).toMatchObject({
			status: "terminal_ok",
			clientRef: input.opRef,
			receiptState: "present",
			outcome: { reason: "end_turn" },
		});
		expect(report.status.startedAt).toBeGreaterThanOrEqual(before);
		expect(report.status.terminalAt).toBeGreaterThanOrEqual(report.status.startedAt!);
		// The host names the turn on accept and reports the same identity at terminal.
		expect(report.status.commandId).toBe(receipt.commandId!);
		expect(report.status.turnId).toBe(receipt.turnId!);
		expect(await port.fetchWorkerOutput({ ...input, notBeforeMs: before, terminalIdentity: undefined })).toMatchObject({
			status: "proven",
			text: "original answer",
		});
		port.omitStartedAt = true;
		expect((await port.status(input)).status.startedAt).toBeUndefined();
	}
	const failed = new ScriptedSessionPort();
	failed.seedOperation(input.opRef, input.sessionId);
	failed.fail(input.opRef);
	expect((await failed.status(input)).status.receiptState).toBeUndefined();
	expect((await failed.status(input)).status.outcome).toBeUndefined();
});

test("steer authoritative refusal or matching rejection is marked while malformed acceptance stays uncertain", async () => {
	let envelope: unknown;
	const port = await broker(
		async () => {
			throw new Error("steer must not spawn a CLI");
		},
		steerRelay(() => envelope),
	);
	const steer = { sessionId: input.sessionId, repo: input.repo, text: "steer", clientRef: "caller-ref" };
	for (const rejected of [
		{ ok: true, result: { accepted: false, status: "rejected", clientRef: "caller-ref" } },
		{ ok: false, error: { code: "busy" } },
	]) {
		envelope = rejected;
		const error = await port.steer(steer).then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(error).toBeInstanceOf(GjcCliError);
		expect((error as GjcCliError).details).toMatchObject({ refused: true });
	}
	for (const result of [
		undefined,
		null,
		{},
		{ accepted: "true" },
		{ accepted: true, status: "unknown" },
		{ accepted: "true", status: "accepted" },
		{ status: "accepted", clientRef: "other-operation" },
		{ accepted: false, clientRef: "other-operation" },
		{ accepted: false, error: { code: "new_sdk_rejection" } },
		{ accepted: false, error: { code: "busy" } },
		{ status: "rejected" },
		{ accepted: false, status: "rejected" },
		{ accepted: false, clientRef: "caller-ref" },
		{ status: "rejected", clientRef: "caller-ref" },
		{ accepted: false, status: "rejected", clientRef: "other-operation" },
		{ accepted: true, status: "rejected", clientRef: "caller-ref" },
		{ accepted: false, status: "accepted", clientRef: "caller-ref" },
	]) {
		envelope = { ok: true, result };
		const error = await port.steer(steer).then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(error).toBeInstanceOf(GjcCliError);
		expect((error as GjcCliError).details).toEqual({ code: "receipt_identity_mismatch" });
	}
	envelope = { ok: false, error: { code: "terminal_uncertain" } };
	const uncertain = await port.steer(steer).then(
		() => undefined,
		(error: unknown) => error,
	);
	expect((uncertain as GjcCliError).details).toEqual({ code: "terminal_uncertain" });
});
