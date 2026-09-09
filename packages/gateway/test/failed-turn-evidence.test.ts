import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rename, rm, symlink, truncate, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type FailedTurnEvidenceInput, readFailedTurnEvidence } from "../src/orchestrator/failed-turn-evidence";

type Row = Record<string, unknown>;
let directory: string;
let path: string;
let input: FailedTurnEvidenceInput;
const start = 1_800_000_000_000;
const rawError = "400 Unknown parameter: 'input[117].status'.\nRequest diagnostic: private-provider-detail";

beforeEach(async () => {
	directory = await mkdtemp(join(tmpdir(), "failed-turn-evidence-"));
	await mkdir(join(directory, "sessions", "bucket"), { recursive: true });
	path = join(directory, "sessions", "bucket", "2026-09-09_session-id.jsonl");
	input = { sessionId: "session-id", repo: directory, startedAtMs: start, terminalAtMs: start + 1000 };
});

afterEach(async () => {
	await rm(directory, { recursive: true, force: true });
});

function message(id: string, parentId: string | null, role: string, offset: number, fields: Row = {}): Row {
	return {
		type: "message",
		id,
		parentId,
		timestamp: new Date(start + offset + 10).toISOString(),
		message: { role, timestamp: start + offset, content: [], ...fields },
	};
}
function prompt(): Row {
	return message("user", null, "user", -100, { content: [{ type: "text", text: "Please help" }] });
}
function failure(parentId = "user", fields: Row = {}): Row {
	return message("error", parentId, "assistant", 100, {
		stopReason: "error",
		errorStatus: 400,
		errorMessage: rawError,
		...fields,
	});
}
async function save(rows: Row[] = [prompt(), failure()], header: Row = {}): Promise<void> {
	await writeFile(
		path,
		[
			{
				type: "session",
				version: 5,
				id: input.sessionId,
				cwd: directory,
				timestamp: new Date(start - 10000).toISOString(),
				...header,
			},
			...rows,
		]
			.map((row) => JSON.stringify(row))
			.join("\n") + "\n",
	);
}
const evidence = () => readFailedTurnEvidence(directory, input);

test("exact private provider 400 returns only the current cause without diagnostics", async () => {
	await save([
		prompt(),
		failure("user", {
			transportFailure: {
				status: 400,
				providerCode: "unknown_parameter",
				openaiErrorCode: "unknown_parameter",
				anthropicErrorType: "invalid_request_error",
			},
		}),
	]);
	expect(await evidence()).toEqual({ reason: "unsupported_input_status" });
	expect(JSON.stringify(await evidence())).not.toContain("private-provider-detail");
});

test("explicit provider context code and measured prompt-too-long errors", async () => {
	await save([
		prompt(),
		failure("user", {
			errorMessage: "provider rejected context",
			transportFailure: { providerCode: "context_length_exceeded" },
		}),
	]);
	expect(await evidence()).toEqual({ reason: "context_exhausted" });
	await save([prompt(), failure("user", { errorMessage: "400 prompt is too long: 200001 tokens > 200000 maximum" })]);
	expect(await evidence()).toEqual({ reason: "context_exhausted" });
});

for (const errorStatus of [400, 413]) {
	test(`HTTP ${errorStatus} request_too_large is not context exhaustion`, async () => {
		await save([
			prompt(),
			failure("user", {
				errorStatus,
				errorMessage: `${errorStatus} request body exceeds maximum byte size`,
				transportFailure: { status: errorStatus, providerCode: "request_too_large" },
			}),
		]);
		expect(await evidence()).toBeUndefined();
	});
}

test("user and tool prose are never provider evidence", async () => {
	await save([
		prompt(),
		message("tool", "user", "toolResult", 100, {
			errorMessage: rawError,
			errorStatus: 400,
			content: [{ type: "text", text: rawError }],
		}),
	]);
	expect(await evidence()).toBeUndefined();
	await save([message("user", null, "user", 0, { errorMessage: rawError, errorStatus: 400 })]);
	expect(await evidence()).toBeUndefined();
});

for (const fields of [
	{ errorStatus: 429 },
	{ errorStatus: 401 },
	{ stopReason: "stop" },
	{ stopReason: "refusal" },
	{ errorMessage: "quoted: " + rawError },
	{ errorMessage: "400 bad request" },
	{ errorMessage: "400 Unknown parameter: 'input[1].content'." },
	{ errorMessage: "context at 100%" },
	{ errorStatus: undefined },
	{ transportFailure: { providerCode: "untrusted_safety_stop" } },
]) {
	test(`does not recognize unrelated provider failure ${JSON.stringify(fields)}`, async () => {
		await save([prompt(), failure("user", fields)]);
		expect(await evidence()).toBeUndefined();
	});
}

test("historical model start is never attached via skew or persistence", async () => {
	const old = failure();
	(old.message as Row).timestamp = start - 1;
	await save([prompt(), old]);
	expect(await evidence()).toBeUndefined();
	await save([prompt(), failure()]);
	input.startedAtMs += 2000;
	input.terminalAtMs += 2000;
	expect(await evidence()).toBeUndefined();
});

test("prompt skew is limited and assistant persistence must be inside operation", async () => {
	await save([message("user", null, "user", -2001), failure()]);
	expect(await evidence()).toBeUndefined();
	const late = failure();
	late.timestamp = new Date(input.terminalAtMs + 1).toISOString();
	await save([prompt(), late]);
	expect(await evidence()).toBeUndefined();
});

test("prior tool and assistant activity do not obscure the current failure cause", async () => {
	for (const content of [
		[{ type: "toolCall", id: "call", name: "write", arguments: {} }],
		[{ type: "thinking", thinking: "plan" }],
		[{ type: "text", text: "hello" }],
	]) {
		await save([
			prompt(),
			message("prior", "user", "assistant", 0, { content }),
			message("tool", "prior", "toolResult", 20),
			failure("tool"),
		]);
		expect(await evidence()).toEqual({ reason: "unsupported_input_status" });
		await save([prompt(), failure("user", { content })]);
		expect(await evidence()).toEqual({ reason: "unsupported_input_status" });
	}
});

test("steers do not obscure the current failure cause", async () => {
	await save([prompt(), message("steer", "user", "user", 0), failure("steer")]);
	expect(await evidence()).toEqual({ reason: "unsupported_input_status" });
});

test("latest assistant wins rather than an earlier error", async () => {
	await save([
		prompt(),
		failure(),
		message("later", "error", "assistant", 200, { stopReason: "stop", content: [{ type: "text", text: "done" }] }),
	]);
	expect(await evidence()).toBeUndefined();
});

test("current mixed branches and missing ancestry are uncertain", async () => {
	await save([prompt(), message("side", "user", "assistant", 0), failure()]);
	expect(await evidence()).toBeUndefined();
	await save([prompt(), failure("missing")]);
	expect(await evidence()).toBeUndefined();
});

test("missing or malformed chronology and partial records fail closed", async () => {
	for (const timestamp of [undefined, "not-a-date", new Date(start - 20000).toISOString()]) {
		await save([prompt(), { ...failure(), timestamp }]);
		expect(await evidence()).toBeUndefined();
	}
	await writeFile(path, "{broken}\n");
	expect(await evidence()).toBeUndefined();
	await save();
	await truncate(path, 20);
	expect(await evidence()).toBeUndefined();
});

test("file, line, and entry caps reject rather than infer from truncated history", async () => {
	await save();
	await truncate(path, 16 * 1024 * 1024 + 1);
	expect(await evidence()).toBeUndefined();
	await save([prompt(), failure("user", { errorMessage: "x".repeat(1024 * 1024) })]);
	expect(await evidence()).toBeUndefined();
	await writeFile(path, "{}\n".repeat(100001));
	expect(await evidence()).toBeUndefined();
});

test("header identity, version and canonical repo are required", async () => {
	for (const header of [{ id: "other" }, { version: 4 }, { cwd: tmpdir() }, { type: "other" }]) {
		await save(undefined, header);
		expect(await evidence()).toBeUndefined();
	}
	await save();
	const alias = join(directory, "alias");
	await symlink(directory, alias);
	input.repo = alias;
	expect(await evidence()).toEqual({ reason: "unsupported_input_status" });
});

test("unsafe ids, duplicate candidates and symlink escapes fail closed", async () => {
	await save();
	for (const sessionId of ["../session-id", "a/b", "", "a\\b", "*"]) {
		expect(await readFailedTurnEvidence(directory, { ...input, sessionId })).toBeUndefined();
	}
	const duplicate = join(directory, "sessions", "bucket", "other_session-id.jsonl");
	await writeFile(duplicate, "");
	expect(await evidence()).toBeUndefined();
	await rm(duplicate);
	const privateFile = join(directory, "private.jsonl");
	await rename(path, privateFile);
	await symlink(privateFile, path);
	expect(await evidence()).toBeUndefined();
	await rm(path);
	await rename(privateFile, path);
	await rename(join(directory, "sessions", "bucket"), join(directory, "outside"));
	await symlink(join(directory, "outside"), join(directory, "sessions", "bucket"));
	expect(await evidence()).toBeUndefined();
});

test("anchored provider context JSON envelope is recognized", async () => {
	await save([
		prompt(),
		failure("user", {
			errorMessage:
				'400 {"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 200001 tokens > 200000 maximum"}}',
		}),
	]);
	expect(await evidence()).toEqual({ reason: "context_exhausted" });
});

test("later turns invalidate current failure evidence", async () => {
	await save([prompt(), failure(), message("next-user", "error", "user", 2000)]);
	expect(await evidence()).toBeUndefined();
});

test("real Anthropic context incident accepts generic outer error and ignores diagnostic suffix", async () => {
	await save([
		prompt(),
		failure("user", {
			transportFailure: { providerCode: "error", anthropicErrorType: "error" },
			errorMessage:
				'400 {"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 1042682 tokens > 1000000 maximum"},"request_id":"private-request-id"}\nraw-http-request=private-request\nnote: private-note',
		}),
	]);
	expect(await evidence()).toEqual({ reason: "context_exhausted" });
});

test("explicit context codes require HTTP 400 or 413", async () => {
	for (const errorStatus of [undefined, 401, 403, 429, 500]) {
		await save([
			prompt(),
			failure("user", { errorStatus, transportFailure: { providerCode: "context_length_exceeded" } }),
		]);
		expect(await evidence()).toBeUndefined();
	}
	await save([
		prompt(),
		failure("user", { errorStatus: 413, transportFailure: { providerCode: "context_length_exceeded" } }),
	]);
	expect(await evidence()).toEqual({ reason: "context_exhausted" });
});

test("generic error codes require exact measured context envelope on first line", async () => {
	for (const errorMessage of [
		"400 prompt is too long: 1042682 tokens > 1000000 maximum",
		rawError,
		'400 {"type":"error","error":{"type":"invalid_request_error","message":"request failed"}}\nprompt is too long: 1042682 tokens > 1000000 maximum',
		'400 {"type":"error","error":{"type":"authentication_error","message":"prompt is too long: 1042682 tokens > 1000000 maximum"}}',
		'400 {"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 1 tokens > 1000000 maximum"}}',
		"400 {malformed}\nnote: diagnostic",
	]) {
		await save([
			prompt(),
			failure("user", { transportFailure: { providerCode: "error", anthropicErrorType: "error" }, errorMessage }),
		]);
		expect(await evidence()).toBeUndefined();
	}
});
