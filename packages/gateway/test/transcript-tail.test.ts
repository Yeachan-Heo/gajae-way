import { afterEach, beforeEach, expect, test } from "bun:test";
import { appendFile, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WORK_PROGRESS_INTENT_MAX_CHARS, WORK_PROGRESS_TEXT_MAX_CHARS } from "@gajae-gateway/protocol";
import { SecretGuard } from "../src/guard/secret-guard";
import { readTranscriptTail } from "../src/orchestrator/transcript-tail";

type Row = Record<string, unknown>;
let directory: string;
let path: string;
const start = Date.parse("2026-11-15T10:00:00.000Z");
const noRedact = (text: string) => text;

beforeEach(async () => {
	directory = await mkdtemp(join(tmpdir(), "transcript-tail-"));
	path = join(directory, "2026-11-15_session.jsonl");
});
afterEach(async () => {
	await rm(directory, { recursive: true, force: true });
});

let seq = 0;
function entry(role: string, offsetMs: number, fields: Row): Row {
	seq++;
	return {
		type: "message",
		id: `e${seq}`,
		parentId: seq === 1 ? null : `e${seq - 1}`,
		timestamp: new Date(start + offsetMs).toISOString(),
		message: { role, timestamp: start + offsetMs, ...fields },
	};
}
const user = (offsetMs: number, text: string) => entry("user", offsetMs, { content: [{ type: "text", text }] });
const assistant = (offsetMs: number, ...content: Row[]) => entry("assistant", offsetMs, { content });
const text = (value: string): Row => ({ type: "text", text: value });
const call = (id: string, name: string, intent?: string): Row => ({
	type: "toolCall",
	id,
	name,
	arguments: { command: "ls", ...(intent === undefined ? {} : { _i: intent }) },
});
const result = (offsetMs: number, toolCallId: string, isError = false, output = "done") =>
	entry("toolResult", offsetMs, { toolCallId, toolName: "bash", isError, content: [{ type: "text", text: output }] });
const header = (): Row => ({
	type: "session",
	version: 5,
	id: "session",
	cwd: "/w",
	timestamp: new Date(start).toISOString(),
});
const lines = (rows: Row[]) => rows.map((row) => `${JSON.stringify(row)}\n`).join("");

test("reads recent texts and tool calls with ok/error status, intents and the turn start", async () => {
	await writeFile(
		path,
		lines([
			header(),
			user(0, "fix the bug"),
			assistant(1000, text("Looking at the failing test."), call("c1", "read", "Reading test file")),
			result(2000, "c1"),
			assistant(3000, call("c2", "bash", "Running unit tests")),
			result(4000, "c2", true, "1 failed"),
			assistant(5000, text("One test fails; fixing the parser.")),
		]),
	);
	const tail = await readTranscriptTail(path, { limit: 5, redact: noRedact });
	expect(tail.turnStartedAt).toBe(new Date(start).toISOString());
	expect(tail.toolInFlight).toBeNull();
	expect(tail.complete).toBe(true);
	expect(tail.skippedRows).toBe(0);
	expect(tail.toolCalls).toEqual([
		{ name: "read", intent: "Reading test file", at: new Date(start + 1000).toISOString(), status: "ok" },
		{ name: "bash", intent: "Running unit tests", at: new Date(start + 3000).toISOString(), status: "error" },
	]);
	expect(tail.assistantTexts.map((item) => item.text)).toEqual([
		"Looking at the failing test.",
		"One test fails; fixing the parser.",
	]);
});

test("a tool call without a saved result and nothing newer is in flight; a superseded one has no result", async () => {
	await writeFile(
		path,
		lines([
			header(),
			user(0, "go"),
			assistant(1000, call("aborted", "bash", "Old attempt")),
			assistant(2000, text("Retrying differently."), call("live", "bash", "Running the build")),
		]),
	);
	const tail = await readTranscriptTail(path, { limit: 5, redact: noRedact });
	expect(tail.toolInFlight).toEqual({
		name: "bash",
		intent: "Running the build",
		at: new Date(start + 2000).toISOString(),
		status: "running",
	});
	expect(tail.toolCalls.map((item) => [item.intent, item.status])).toEqual([
		["Old attempt", "no_result"],
		["Running the build", "running"],
	]);
});

test("a partial last line (a row still being written) is ignored, not parsed or counted", async () => {
	const complete = lines([header(), user(0, "go"), assistant(1000, call("c1", "bash", "Long job"))]);
	const partial = JSON.stringify(result(2000, "c1")).slice(0, 40);
	await writeFile(path, complete + partial);
	const tail = await readTranscriptTail(path, { limit: 5, redact: noRedact });
	expect(tail.toolInFlight?.intent).toBe("Long job");
	expect(tail.skippedRows).toBe(0);
	// Once the row is finished, the same call reads as settled.
	await writeFile(path, complete + lines([result(2000, "c1")]));
	const settled = await readTranscriptTail(path, { limit: 5, redact: noRedact });
	expect(settled.toolInFlight).toBeNull();
	expect(settled.toolCalls[0]?.status).toBe("ok");
});

test("a huge transcript is read from the end only, bounded and fast", async () => {
	// ~40 MB of earlier turns, rows of ~300 KB with multi-byte text crossing chunk boundaries.
	const filler = "가재".repeat(50_000);
	const old: Row[] = [header()];
	for (let i = 0; i < 130; i++) {
		old.push(user(i * 10, `turn ${i}`));
		old.push(assistant(i * 10 + 1, text(`old ${i} ${filler}`), call(`old-${i}`, "bash", `old ${i}`)));
		old.push(result(i * 10 + 2, `old-${i}`));
	}
	await writeFile(path, lines(old));
	await appendFile(
		path,
		lines([
			user(100_000, "current task"),
			assistant(100_001, text("Working on the current task."), call("now", "bash", "Running the slow suite")),
		]),
	);
	const size = (await stat(path)).size;
	expect(size).toBeGreaterThan(36 * 1024 * 1024);
	const began = performance.now();
	const tail = await readTranscriptTail(path, { limit: 1, redact: noRedact });
	const elapsed = performance.now() - began;
	expect(tail.bytes).toBe(size);
	expect(tail.complete).toBe(true);
	expect(tail.scannedBytes).toBeLessThan(1024 * 1024);
	expect(elapsed).toBeLessThan(1000);
	expect(tail.turnStartedAt).toBe(new Date(start + 100_000).toISOString());
	expect(tail.toolInFlight?.intent).toBe("Running the slow suite");
	expect(tail.assistantTexts.map((item) => item.text)).toEqual(["Working on the current task."]);

	// Asking for more than the window holds stops at the scan bound instead of reading 40 MB.
	const deep = await readTranscriptTail(path, { limit: 20, redact: noRedact, maxScanBytes: 2 * 1024 * 1024 });
	expect(deep.scannedBytes).toBe(2 * 1024 * 1024);
	expect(deep.complete).toBe(false);
	expect(deep.assistantTexts.length).toBeGreaterThan(1);
	// Multi-byte text that crossed 256 KiB chunk boundaries decodes intact.
	expect(deep.assistantTexts.at(-2)?.text.startsWith("old 129 가재가재")).toBe(true);
	expect(deep.assistantTexts.at(-2)?.text).not.toContain("\ufffd");
});

test("rows larger than the row bound are skipped and counted without hiding their neighbours", async () => {
	await writeFile(
		path,
		lines([
			header(),
			user(0, "go"),
			assistant(1000, call("big", "read", "Reading a huge file")),
			result(2000, "big", false, "x".repeat(600_000)),
			assistant(3000, text("Read it.")),
		]),
	);
	const tail = await readTranscriptTail(path, { limit: 5, redact: noRedact, maxRowBytes: 100_000 });
	expect(tail.skippedRows).toBe(1);
	expect(tail.assistantTexts.map((item) => item.text)).toEqual(["Read it."]);
	expect(tail.toolCalls).toEqual([
		{ name: "read", intent: "Reading a huge file", at: new Date(start + 1000).toISOString(), status: "no_result" },
	]);
	expect(tail.turnStartedAt).toBe(new Date(start).toISOString());
});

test("malformed rows are skipped and counted", async () => {
	await writeFile(
		path,
		`${lines([header(), user(0, "go")])}{not json\n${lines([assistant(1000, text("still here"))])}`,
	);
	const tail = await readTranscriptTail(path, { limit: 5, redact: noRedact });
	expect(tail.skippedRows).toBe(1);
	expect(tail.assistantTexts.map((item) => item.text)).toEqual(["still here"]);
});

test("limit and per-item truncation bound the result", async () => {
	const rows: Row[] = [header(), user(0, "go")];
	for (let i = 0; i < 30; i++)
		rows.push(assistant(i + 1, text(`step ${i} ${"y".repeat(5_000)}`), call(`c${i}`, "bash", "z".repeat(1_000))));
	await writeFile(path, lines(rows));
	const tail = await readTranscriptTail(path, { limit: 3, redact: noRedact });
	expect(tail.assistantTexts).toHaveLength(3);
	expect(tail.toolCalls).toHaveLength(3);
	expect(tail.assistantTexts.map((item) => item.text.slice(0, 7))).toEqual(["step 27", "step 28", "step 29"]);
	for (const item of tail.assistantTexts) {
		expect(item.truncated).toBe(true);
		expect(item.text.length).toBe(WORK_PROGRESS_TEXT_MAX_CHARS);
		expect(item.text.endsWith("…")).toBe(true);
	}
	for (const item of tail.toolCalls) expect(item.intent?.length).toBe(WORK_PROGRESS_INTENT_MAX_CHARS);
	expect(Buffer.byteLength(JSON.stringify(tail))).toBeLessThan(8 * 1024);
});

test("the outbound secret redaction is applied to texts and intents before truncation", async () => {
	const key = `sk-ant-${"A1b2C3d4".repeat(6)}`;
	const envSecret = "Zq9vW3xY7tP2mK5nR8sL";
	await writeFile(
		path,
		lines([
			header(),
			user(0, "go"),
			assistant(1000, text(`Using ${key} and ${envSecret} now.`), call("c1", "bash", `export TOKEN=${envSecret}`)),
		]),
	);
	const guard = new SecretGuard({ files: [], envNames: [], env: { DEPLOY_TOKEN: envSecret } }, { log: () => {} });
	const tail = await readTranscriptTail(path, { limit: 5, redact: (value) => guard.redact(value).text });
	const serialized = JSON.stringify(tail);
	expect(serialized).not.toContain(key);
	expect(serialized).not.toContain(envSecret);
	expect(tail.assistantTexts[0]?.text).toBe("Using [REDACTED:anthropic_key] and [REDACTED:known_secret] now.");
	expect(tail.toolCalls[0]?.intent).toBe("export TOKEN=[REDACTED:known_secret]");
});

test("reading never changes the transcript", async () => {
	await writeFile(path, lines([header(), user(0, "go"), assistant(1000, call("c1", "bash", "Working"))]));
	const before = await stat(path);
	const bytes = await readFile(path);
	for (let i = 0; i < 5; i++) await readTranscriptTail(path, { limit: 5, redact: noRedact });
	const after = await stat(path);
	expect(await readFile(path)).toEqual(bytes);
	expect(after.size).toBe(before.size);
	expect(after.mtimeMs).toBe(before.mtimeMs);
	expect(after.ino).toBe(before.ino);
});

test("a missing transcript throws instead of inventing progress", async () => {
	await expect(readTranscriptTail(join(directory, "missing.jsonl"), { limit: 5, redact: noRedact })).rejects.toThrow();
});
