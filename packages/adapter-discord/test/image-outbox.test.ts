import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { mkdir, mkdtemp, readFile, rm, stat, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ImageOutbox, type PendingImage } from "../src/image-outbox";

describe("ImageOutbox", () => {
	let directory: string;
	let path: string;

	beforeEach(async () => {
		directory = await mkdtemp(join(tmpdir(), "discord-image-outbox-"));
		path = join(directory, "outbox.json");
	});

	afterEach(async () => {
		await rm(directory, { recursive: true, force: true });
	});

	test("round trips an opaque image request across restart with restrictive permissions", async () => {
		const bytes = Buffer.from([0, 1, 2, 127, 128, 254, 255]);
		const entry: PendingImage = {
			messageId: "123456789012345678",
			channelId: "channel-1",
			request: {
				image: { filename: "image.png", data: bytes.toString("base64") },
				prompt: "preserve this request exactly",
			},
		};

		await new ImageOutbox(path).put(entry);
		expect((await stat(path)).mode & 0o077).toBe(0);

		const loaded = await new ImageOutbox(path).load();
		expect(loaded).toEqual([entry]);
		const request = loaded[0]?.request as typeof entry.request;
		expect(request).toEqual(entry.request);
		expect(Buffer.from((request as { image: { data: string } }).image.data, "base64")).toEqual(bytes);
		expect(await readFile(path, "utf8")).toContain(bytes.toString("base64"));
	});

	test("serializes concurrent writes without losing entries", async () => {
		const outbox = new ImageOutbox(path);
		const entries: PendingImage[] = Array.from({ length: 32 }, (_, index) => ({
			messageId: String(123456789012345678n + BigInt(index)),
			channelId: `channel-${index}`,
			request: { index },
		}));

		await Promise.all(entries.map((entry) => outbox.put(entry)));
		const loaded = await new ImageOutbox(path).load();
		expect(loaded).toHaveLength(entries.length);
		expect(new Map(loaded.map((entry) => [entry.messageId, entry]))).toEqual(
			new Map(entries.map((entry) => [entry.messageId, entry])),
		);
	});

	test("removing an entry retains other entries across restart", async () => {
		const outbox = new ImageOutbox(path);
		const removed: PendingImage = { messageId: "123456789012345679", channelId: "channel" };
		const retained: PendingImage = { messageId: "123456789012345680", channelId: "channel" };
		await outbox.put(removed);
		await outbox.put(retained);
		await outbox.remove(removed.messageId);

		expect(await new ImageOutbox(path).load()).toEqual([retained]);
	});

	test("terminal state survives restart", async () => {
		const entry: PendingImage = {
			messageId: "123456789012345681",
			channelId: "channel",
			request: { image: "AAEC/w==" },
			terminal: true,
		};
		await new ImageOutbox(path).put(entry);

		expect(await new ImageOutbox(path).load()).toEqual([entry]);
	});

	test("drops one corrupt record but keeps readable intents", async () => {
		const valid: PendingImage = { messageId: "123456789012345683", channelId: "channel" };
		await writeFile(path, JSON.stringify([valid, { messageId: "not-a-snowflake", channelId: "channel" }]));
		expect(await new ImageOutbox(path).load()).toEqual([valid]);
	});

	test("prunes expired completion tombstones but retains pending and recent ones", async () => {
		const now = Date.now();
		const expired = "123456789012345685";
		const recent = "123456789012345686";
		const pending: PendingImage = { messageId: "123456789012345684", channelId: "channel" };
		await writeFile(
			path,
			JSON.stringify([
				{ messageId: expired, channelId: "channel", completed: true, completedAt: now - 8 * 24 * 60 * 60 * 1000 },
				{ messageId: recent, channelId: "channel", completed: true, completedAt: now - 60_000 },
				pending,
			]),
		);
		const loaded = await new ImageOutbox(path).load();
		expect(loaded).toHaveLength(2);
		expect(loaded).toContainEqual({
			messageId: recent,
			channelId: "channel",
			completed: true,
			completedAt: now - 60_000,
		});
		expect(loaded).toContainEqual(pending);
		expect(await readFile(path, "utf8")).not.toContain(expired);
	});

	test("fails closed for malformed JSON and invalid state", async () => {
		for (const contents of ["{", "null"]) {
			await writeFile(path, contents);
			const outbox = new ImageOutbox(path);
			await expect(outbox.load()).rejects.toThrow();
			expect(await readFile(path, "utf8")).toBe(contents);
		}
	});

	test("propagates filesystem errors and recovers after repair", async () => {
		const parent = join(directory, "blocked");
		await writeFile(parent, "not a directory");
		const outbox = new ImageOutbox(join(parent, "outbox.json"));
		const entry: PendingImage = { messageId: "123456789012345682", channelId: "channel" };

		await expect(outbox.put(entry)).rejects.toThrow();
		await unlink(parent);
		await mkdir(parent);
		await outbox.put(entry);
		expect(await new ImageOutbox(join(parent, "outbox.json")).load()).toEqual([entry]);
	});

	test("a put enforces the tombstone bound without waiting for a recovery load", async () => {
		const outbox = new ImageOutbox(path);
		const expired = "123456789012345687";
		await outbox.put({
			messageId: expired,
			channelId: "channel",
			completed: true,
			completedAt: Date.now() - 8 * 24 * 60 * 60 * 1000,
		});
		const pending: PendingImage = { messageId: "123456789012345688", channelId: "channel" };
		await outbox.put(pending);
		expect(await readFile(path, "utf8")).not.toContain(expired);
		expect(await outbox.load()).toEqual([pending]);
	});

	test("ages terminal rows after the history rewalk window", async () => {
		const { snowflakeFromTimestamp } = await import("../src/recovery");
		const old = String(snowflakeFromTimestamp(Date.now() - 31 * 24 * 60 * 60 * 1000));
		const recent = String(snowflakeFromTimestamp(Date.now() - 60_000));
		await writeFile(
			path,
			JSON.stringify([
				{
					messageId: old,
					channelId: "channel",
					terminal: true,
					terminalAt: Date.now() - 31 * 24 * 60 * 60 * 1000,
					audited: true,
				},
				{ messageId: recent, channelId: "channel", terminal: true, terminalAt: Date.now() - 60_000, audited: true },
			]),
		);
		expect(await new ImageOutbox(path).load()).toEqual([
			{ messageId: recent, channelId: "channel", terminal: true, terminalAt: expect.any(Number), audited: true },
		]);
	});

	test("refuses a new unresolved intent past the admission bound", async () => {
		const base = 123456789012345600n;
		const entries = Array.from({ length: 1_000 }, (_, index) => ({
			messageId: String(base + BigInt(index)),
			channelId: "channel",
		}));
		await writeFile(path, JSON.stringify(entries));
		const outbox = new ImageOutbox(path);
		await expect(outbox.put({ messageId: String(base + 2_000n), channelId: "channel" })).rejects.toThrow(
			"unresolved intents",
		);
		// An existing intent and a terminal row are still admitted.
		await outbox.put({ ...entries[0]!, failureAt: Date.now() });
		await outbox.put({ messageId: String(base + 2_001n), channelId: "channel", terminal: true, audited: true });
	});
});
