import { constants } from "node:fs";
import { lstat, open, readdir, realpath } from "node:fs/promises";
import { dirname, join } from "node:path";

export type FailedTurnEvidence = {
	readonly reason: "unsupported_input_status" | "context_exhausted";
};

export type FailedTurnEvidenceInput = {
	sessionId: string;
	repo: string;
	startedAtMs: number;
	terminalAtMs: number;
};

type Row = Record<string, unknown>;
type Entry = { id: string; parentId: string | null; time: number; message?: Row; messageTime?: number };
const MAX_BYTES = 16 * 1024 * 1024;
const MAX_LINES = 100_000;
const MAX_LINE_BYTES = 1024 * 1024;

function record(value: unknown): Row | undefined {
	return value !== null && typeof value === "object" && !Array.isArray(value) ? (value as Row) : undefined;
}

function time(value: unknown): number | undefined {
	const result = typeof value === "number" ? value : typeof value === "string" ? Date.parse(value) : NaN;
	return Number.isFinite(result) && result >= 0 ? result : undefined;
}

function reason(message: Row): FailedTurnEvidence["reason"] | undefined {
	if (message.role !== "assistant" || message.stopReason !== "error") return undefined;
	const facts = record(message.transportFailure);
	const status = message.errorStatus ?? facts?.status;
	if (status !== 400 && status !== 413) return undefined;
	if ([message.errorStatus, facts?.status].some((value) => value !== undefined && value !== status)) return undefined;
	const codes = [facts?.providerCode, facts?.openaiErrorCode, facts?.anthropicErrorType, message.errorCode];
	const knownCodes = [
		"context_length_exceeded",
		"request_too_large",
		"invalid_request_error",
		"unknown_parameter",
		"unsupported_parameter",
	];
	if (codes.some((code) => code !== undefined && code !== "error" && !knownCodes.includes(String(code))))
		return undefined;
	// Diagnostics after the first line are private and never participate in classification.
	const firstLine = typeof message.errorMessage === "string" ? message.errorMessage.split(/\r?\n/, 1)[0]! : "";
	let contextText = firstLine;
	let contextEnvelope = false;
	if (/^(400|413) \{/.test(firstLine)) {
		if (Number(firstLine.slice(0, 3)) !== status) return undefined;
		const envelope = record(JSON.parse(firstLine.slice(4)));
		const providerError = record(envelope?.error);
		if (
			envelope?.type !== "error" ||
			providerError?.type !== "invalid_request_error" ||
			typeof providerError.message !== "string"
		)
			return undefined;
		contextText = providerError.message;
		contextEnvelope = true;
	}
	const measured = /^(?:(?:400|413) )?prompt is too long: (\d+) tokens? > (\d+) maximum\.?$/i.exec(contextText);
	const measuredContext = measured !== null && Number(measured[1]) > Number(measured[2]);
	// Generic outer "error" facts are not authority without the exact inner context envelope.
	if (codes.includes("error")) return contextEnvelope && measuredContext ? "context_exhausted" : undefined;
	if (message.errorStatus === 400 && /^400 Unknown parameter: 'input\[\d+\]\.status'\.$/.test(firstLine))
		return "unsupported_input_status";
	if (codes.includes("context_length_exceeded") || measuredContext) return "context_exhausted";
	return undefined;
}

/** Private saved-transcript evidence only. Never returns provider text or filesystem errors. */
export async function readFailedTurnEvidence(
	agentDir: string | undefined,
	input: FailedTurnEvidenceInput,
): Promise<FailedTurnEvidence | undefined> {
	try {
		if (!agentDir || !/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(input.sessionId)) return undefined;
		if (
			!Number.isFinite(input.startedAtMs) ||
			!Number.isFinite(input.terminalAtMs) ||
			input.startedAtMs < 0 ||
			input.terminalAtMs < input.startedAtMs
		)
			return undefined;
		const root = await realpath(agentDir);
		const sessions = join(root, "sessions");
		if ((await lstat(sessions)).isSymbolicLink() || (await realpath(sessions)) !== sessions) return undefined;
		const candidates: string[] = [];
		const buckets = await readdir(sessions, { withFileTypes: true });
		if (buckets.length > MAX_LINES) return undefined;
		let scanned = 0;
		for (const bucket of buckets) {
			if (bucket.isSymbolicLink()) return undefined;
			if (!bucket.isDirectory()) continue;
			const directory = join(sessions, bucket.name);
			if ((await realpath(directory)) !== directory) return undefined;
			const files = await readdir(directory, { withFileTypes: true });
			scanned += files.length;
			if (scanned > MAX_LINES) return undefined;
			for (const file of files) {
				if (!file.name.endsWith(`_${input.sessionId}.jsonl`)) continue;
				if (!file.isFile() || file.isSymbolicLink()) return undefined;
				candidates.push(join(directory, file.name));
			}
		}
		if (candidates.length !== 1) return undefined;
		const path = candidates[0]!;
		if ((await realpath(path)) !== path) return undefined;
		const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
		let text: string;
		try {
			const before = await handle.stat();
			if (!before.isFile() || before.size > MAX_BYTES || before.size === 0) return undefined;
			const buffer = Buffer.alloc(before.size + 1);
			let length = 0;
			while (length < buffer.length) {
				const result = await handle.read(buffer, length, buffer.length - length, length);
				if (!result.bytesRead) break;
				length += result.bytesRead;
			}
			const after = await handle.stat();
			const named = await lstat(path);
			if (
				length !== before.size ||
				after.size !== before.size ||
				after.mtimeMs !== before.mtimeMs ||
				after.ctimeMs !== before.ctimeMs ||
				named.dev !== before.dev ||
				named.ino !== before.ino ||
				named.isSymbolicLink() ||
				(await realpath(path)) !== path ||
				(await realpath(dirname(path))) !== dirname(path)
			)
				return undefined;
			text = new TextDecoder("utf-8", { fatal: true }).decode(buffer.subarray(0, length));
		} finally {
			await handle.close();
		}
		if (!text.endsWith("\n")) return undefined;
		const lines = text.slice(0, -1).split("\n");
		if (lines.length > MAX_LINES || lines.some((line) => !line || Buffer.byteLength(line) > MAX_LINE_BYTES))
			return undefined;
		const rows = lines.map((line) => record(JSON.parse(line)));
		const header = rows.shift();
		if (
			!header ||
			header.type !== "session" ||
			header.version !== 5 ||
			header.id !== input.sessionId ||
			typeof header.cwd !== "string" ||
			(await realpath(header.cwd)) !== (await realpath(input.repo))
		)
			return undefined;
		let previousTime = time(header.timestamp);
		if (previousTime === undefined) return undefined;
		const entries: Entry[] = [];
		let previousMessageTime = 0;
		const byId = new Map<string, Entry>();
		for (const row of rows) {
			if (
				!row ||
				typeof row.id !== "string" ||
				!row.id ||
				byId.has(row.id) ||
				(row.parentId !== null && (typeof row.parentId !== "string" || !byId.has(row.parentId)))
			)
				return undefined;
			const persisted = time(row.timestamp);
			if (persisted === undefined || persisted < previousTime) return undefined;
			previousTime = persisted;
			const message = row.type === "message" ? record(row.message) : undefined;
			const messageTime = message ? time(message.timestamp) : undefined;
			if (row.type === "message" && (!message || messageTime === undefined || messageTime > persisted))
				return undefined;
			if (messageTime !== undefined) {
				if (messageTime < previousMessageTime) return undefined;
				previousMessageTime = messageTime;
			}
			const entry: Entry = {
				id: row.id,
				parentId: row.parentId as string | null,
				time: persisted,
				message,
				messageTime,
			};
			entries.push(entry);
			byId.set(entry.id, entry);
		}
		const ancestry: Entry[] = [];
		let cursor = entries.at(-1);
		while (cursor) {
			ancestry.push(cursor);
			cursor = cursor.parentId === null ? undefined : byId.get(cursor.parentId);
		}
		ancestry.reverse();
		const active = new Set(ancestry);
		const nearStart = input.startedAtMs - 2000;
		if (entries.some((entry) => entry.time >= nearStart && !active.has(entry))) return undefined;
		const failed = ancestry.findLast((entry) => entry.message?.role === "assistant");
		if (
			!failed?.message ||
			failed.messageTime === undefined ||
			failed.messageTime < input.startedAtMs ||
			failed.time > input.terminalAtMs
		)
			return undefined;
		const failureReason = reason(failed.message);
		if (!failureReason) return undefined;
		const turn = ancestry.filter((entry) => entry.time >= nearStart);
		const prompts = turn.filter(
			(entry) =>
				entry.message?.role === "user" &&
				entry.messageTime !== undefined &&
				entry.messageTime >= nearStart &&
				entry.messageTime <= input.startedAtMs + 2000,
		);
		if (!prompts.length) return undefined;
		const prompt = prompts[0]!;
		if (prompt.time > failed.time || prompt.messageTime! > failed.messageTime) return undefined;
		const current = ancestry.slice(ancestry.indexOf(prompt));
		if (
			current.some(
				(entry) => entry.time > input.terminalAtMs || (entry.message && entry.messageTime! > input.terminalAtMs),
			)
		)
			return undefined;
		if (ancestry.slice(ancestry.indexOf(failed) + 1).some((entry) => entry.message)) return undefined;
		return { reason: failureReason };
	} catch {
		return undefined;
	}
}
