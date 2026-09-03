import { expect, test } from "bun:test";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ReconnectingGateway } from "../src/main";

const SELF = { id: "self-bot" };
const ORIGIN = { platform: "discord", kind: "channel", conversationId: "chan-1" } as const;
const ENGAGEMENT = { mentioned: true, group: true, authorId: "human-1" };

type Request = {
	readonly verb: string;
	readonly messageId: string | undefined;
	readonly text: string | undefined;
};

type Client = {
	request(verb: string, params?: unknown): Promise<unknown>;
};

type Deferred<T> = {
	readonly promise: Promise<T>;
	resolve(value: T): void;
	reject(error: unknown): void;
};

function deferred<T>(): Deferred<T> {
	let resolve!: (value: T) => void;
	let reject!: (error: unknown) => void;
	const promise = new Promise<T>((nextResolve, nextReject) => {
		resolve = nextResolve;
		reject = nextReject;
	});
	return { promise, resolve, reject };
}

function requestRecord(verb: string, params: unknown): Request {
	const edit = params as { messageId?: string; text?: string };
	return { verb, messageId: edit.messageId, text: edit.text };
}

function client(port: Client) {
	return { ...port, onChatMessage: () => () => {} } as never;
}

/** Mirrors the focused adapter test helper, with an absent initial client for a down link. */
function gateway(initialClient?: Client): ReconnectingGateway {
	return new ReconnectingGateway(
		"socket",
		{ channels: { fetch: async () => undefined } },
		{ tokenFile: "token", token: "redacted", configPath: "config", channels: {} } as never,
		undefined,
		undefined,
		join(tmpdir(), `gajaeway-edit-outbox-redteam-${crypto.randomUUID()}.json`),
		() => SELF,
		initialClient ? client(initialClient) : undefined,
		async () => {},
	);
}

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

async function settleFlush(): Promise<void> {
	await Bun.sleep(10);
}

test("H1: the down-link edit outbox keeps 256 newest distinct messages and names the dropped oldest", async () => {
	const errors: string[] = [];
	const originalError = console.error;
	const originalLog = console.log;
	console.error = (line: unknown) => errors.push(String(line));
	console.log = () => {};
	try {
		const gw = gateway();
		for (let index = 0; index < 257; index++) gw.sendEdit(`m-${index}`, ORIGIN, `edit ${index}`, ENGAGEMENT);
		await settleFlush();

		expect(gw.pendingEdits).toHaveLength(256);
		expect(gw.pendingEdits[0]).toMatchObject({ messageId: "m-1", text: "edit 1" });
		expect(gw.pendingEdits.at(-1)).toMatchObject({ messageId: "m-256", text: "edit 256" });
		expect(gw.pendingEdits.some((edit) => edit.messageId === "m-0")).toBe(false);
		expect(errors).toContain("Discord edit outbox full; dropped the oldest queued edit (message m-0).");
	} finally {
		console.error = originalError;
		console.log = originalLog;
	}
});

test("H2: a newer same-message edit arriving while v1 is awaiting acknowledgement is flushed after v1", async () => {
	const requests: Request[] = [];
	const v1Started = deferred<void>();
	const releaseV1 = deferred<void>();
	const gw = gateway({
		request: async (verb, params) => {
			const recorded = requestRecord(verb, params);
			requests.push(recorded);
			if (recorded.text === "v1") {
				v1Started.resolve();
				await releaseV1.promise;
			}
			return { engaged: true };
		},
	});

	gw.sendEdit("m1", ORIGIN, "v1", ENGAGEMENT);
	await v1Started.promise;
	gw.sendEdit("m1", ORIGIN, "v2", ENGAGEMENT);
	releaseV1.resolve();
	await settleFlush();

	// RED-TEAM FINDING: the in-flight flush snapshots only v1. Its successful acknowledgement preserves v2, but no successor flush is started after #editFlush clears.
	expect(requests).toEqual([
		{ verb: "chat.edit", messageId: "m1", text: "v1" },
		{ verb: "chat.edit", messageId: "m1", text: "v2" },
	]);
	expect(gw.pendingEdits).toEqual([]);
});

test("H3: a partial replay failure retains the failed tail, stops at it, and replays it after adoptClient", async () => {
	const requests: Request[] = [];
	const logs: string[] = [];
	const originalLog = console.log;
	const originalError = console.error;
	console.log = (line: unknown) => logs.push(String(line));
	console.error = () => {};
	try {
		const gw = gateway();
		gw.sendEdit("m1", ORIGIN, "first", ENGAGEMENT);
		gw.sendEdit("m2", ORIGIN, "second", ENGAGEMENT);
		gw.sendEdit("m3", ORIGIN, "third", ENGAGEMENT);
		await settleFlush();
		expect(gw.pendingEdits.map((edit) => edit.messageId)).toEqual(["m1", "m2", "m3"]);
		expect(logs.some((line) => line.includes("Discord adapter gateway reconnecting in"))).toBe(true);

		gw.adoptClient(
			client({
				request: async (verb, params) => {
					const recorded = requestRecord(verb, params);
					requests.push(recorded);
					if (recorded.messageId === "m2") throw new Error("second replay rejected");
					return { engaged: true };
				},
			}),
		);
		await eventually(() => requests.length === 2, "partial replay did not reach the second request");
		await settleFlush();
		expect(requests).toEqual([
			{ verb: "chat.edit", messageId: "m1", text: "first" },
			{ verb: "chat.edit", messageId: "m2", text: "second" },
		]);
		expect(gw.pendingEdits.map((edit) => [edit.messageId, edit.text])).toEqual([
			["m2", "second"],
			["m3", "third"],
		]);

		gw.adoptClient(
			client({
				request: async (verb, params) => {
					requests.push(requestRecord(verb, params));
					return { engaged: true };
				},
			}),
		);
		await eventually(
			() => requests.length === 4 && gw.pendingEdits.length === 0,
			"adopted client did not replay the failed tail",
		);
		expect(requests).toEqual([
			{ verb: "chat.edit", messageId: "m1", text: "first" },
			{ verb: "chat.edit", messageId: "m2", text: "second" },
			{ verb: "chat.edit", messageId: "m2", text: "second" },
			{ verb: "chat.edit", messageId: "m3", text: "third" },
		]);
	} finally {
		console.log = originalLog;
		console.error = originalError;
	}
});

test("H4: same-tick edits for different messages use one serial flush and issue both requests", async () => {
	const requests: Request[] = [];
	const firstStarted = deferred<void>();
	const releaseFirst = deferred<void>();
	let inFlight = 0;
	let greatestInFlight = 0;
	const gw = gateway({
		request: async (verb, params) => {
			const recorded = requestRecord(verb, params);
			requests.push(recorded);
			inFlight++;
			greatestInFlight = Math.max(greatestInFlight, inFlight);
			try {
				if (recorded.messageId === "m1") {
					firstStarted.resolve();
					await releaseFirst.promise;
				}
				return { engaged: true };
			} finally {
				inFlight--;
			}
		},
	});

	gw.sendEdit("m1", ORIGIN, "first", ENGAGEMENT);
	gw.sendEdit("m2", ORIGIN, "second", ENGAGEMENT);
	await firstStarted.promise;
	releaseFirst.resolve();
	await settleFlush();

	// RED-TEAM FINDING: sendEdit(m2) observes the active m1 flush and returns; the m1 snapshot is exhausted without a follow-up pass, leaving m2 indefinitely queued.
	expect(requests).toEqual([
		{ verb: "chat.edit", messageId: "m1", text: "first" },
		{ verb: "chat.edit", messageId: "m2", text: "second" },
	]);
	expect(greatestInFlight).toBe(1);
	expect(gw.pendingEdits).toEqual([]);
});
