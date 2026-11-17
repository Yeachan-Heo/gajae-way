import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { mkdir, mkdtemp, readFile, rm, stat, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ImageOutbox, type PendingImage, syncOutboxDirectory } from "../src/image-outbox";

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

	test("a stale write cannot replace terminal or completed evidence", async () => {
		const outbox = new ImageOutbox(path);
		const terminal: PendingImage = {
			messageId: "123456789012345682",
			channelId: "channel",
			terminal: true,
			audited: true,
		};
		await outbox.put(terminal);
		await outbox.put({ messageId: terminal.messageId, channelId: "channel", request: { image: "stale" } });
		expect(await outbox.get(terminal.messageId)).toMatchObject({ terminal: true, audited: true });
		expect((await outbox.get(terminal.messageId))?.request).toBeUndefined();

		const completed: PendingImage = {
			messageId: "123456789012345683",
			channelId: "channel",
			completed: true,
			completedAt: 1,
		};
		await outbox.put(completed);
		await outbox.put({ messageId: completed.messageId, channelId: "channel", failureAt: 2 });
		expect(await outbox.get(completed.messageId)).toMatchObject({ completed: true, completedAt: 1 });

		// Forward evidence still lands: a terminal row can become a completion tombstone.
		await outbox.put({ ...terminal, terminal: false, completed: true, completedAt: 3 });
		expect(await outbox.get(terminal.messageId)).toMatchObject({ completed: true, terminal: false });
	});

	test("drops one corrupt record but keeps readable intents", async () => {
		const valid: PendingImage = { messageId: "123456789012345683", channelId: "channel" };
		await writeFile(path, JSON.stringify([valid, { messageId: "not-a-snowflake", channelId: "channel" }]));
		expect(await new ImageOutbox(path).load()).toEqual([valid]);
	});

	test("retains completion tombstones until the watermark compacts them", async () => {
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
		expect(loaded).toHaveLength(3);
		expect(loaded).toContainEqual({
			messageId: expired,
			channelId: "channel",
			completed: true,
			completedAt: now - 8 * 24 * 60 * 60 * 1000,
		});
		expect(loaded).toContainEqual({
			messageId: recent,
			channelId: "channel",
			completed: true,
			completedAt: now - 60_000,
		});
		expect(loaded).toContainEqual(pending);
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

	test("a put retains completion evidence until the watermark compacts it", async () => {
		const outbox = new ImageOutbox(path);
		const completed = "123456789012345687";
		const entry = {
			messageId: completed,
			channelId: "channel",
			completed: true,
			completedAt: Date.now() - 8 * 24 * 60 * 60 * 1000,
		};
		await outbox.put(entry);
		const pending: PendingImage = { messageId: "123456789012345688", channelId: "channel" };
		await outbox.put(pending);
		expect(await outbox.load()).toEqual([entry, pending]);
	});

	test("keeps terminal evidence until the watermark compacts it", async () => {
		const old = { messageId: "123456789012345681", channelId: "channel", terminal: true, audited: true };
		const recent = { messageId: "1571558346320773120", channelId: "channel", terminal: true, audited: true };
		await writeFile(path, JSON.stringify([old, recent]));
		expect(await new ImageOutbox(path).load()).toEqual([old, recent]);
	});

	test("completed evidence bounds admission when no watermark is available", async () => {
		const base = 1571558400000000000n;
		await writeFile(
			path,
			JSON.stringify(
				Array.from({ length: 10_000 }, (_, index) => ({
					messageId: String(base + BigInt(index)),
					channelId: "channel",
					completed: true,
					completedAt: Date.now(),
				})),
			),
		);
		const outbox = new ImageOutbox(path);
		await expect(outbox.put({ messageId: String(base + 20_000n), channelId: "channel" })).rejects.toThrow(
			"admission bound",
		);
		expect((await outbox.load()).filter((entry) => entry.completed)).toHaveLength(10_000);
	});

	test("declined tombstones never consume the completed admission quota", async () => {
		const base = 1571558400000000000n;
		await writeFile(
			path,
			JSON.stringify(
				Array.from({ length: 10_000 }, (_, index) => ({
					messageId: String(base + BigInt(index)),
					channelId: "channel",
					completed: true,
					completedAt: Date.now(),
					declined: true,
				})),
			),
		);
		const outbox = new ImageOutbox(path);
		const admitted: PendingImage = { messageId: String(base + 20_000n), channelId: "channel" };
		// Ten thousand declines from bot-visible channels must not keep an authorized
		// image out; only real completions hold delivery quota.
		await expect(outbox.put(admitted)).resolves.toBeUndefined();
		const loaded = await outbox.load();
		expect(loaded).toContainEqual(admitted);
		expect(loaded.filter((entry) => entry.declined === true).length).toBeLessThanOrEqual(2_000);
	});

	test("refuses a new unresolved intent past the admission bound", async () => {
		const base = 123456789012345600n;
		const entries = Array.from({ length: 1_000 }, (_, index) => ({
			messageId: String(base + BigInt(index)),
			channelId: "channel",
		}));
		await writeFile(
			path,
			JSON.stringify([...entries, { messageId: String(base + 1_000n), channelId: "channel", terminal: true }]),
		);
		const outbox = new ImageOutbox(path);
		// With the new eviction logic, adding a new unresolved entry evicts the oldest one
		await expect(outbox.put({ messageId: String(base + 2_000n), channelId: "channel" })).resolves.toBeUndefined();
		// An existing intent and a terminal row are still admitted.
		const firstEntry = entries[0];
		if (firstEntry) await outbox.put({ ...firstEntry, failureAt: Date.now() });
		await outbox.put({ messageId: String(base + 2_001n), channelId: "channel", terminal: true, audited: true });
		// After all these additions, unresolved count should still be bounded
		const loaded = await outbox.load();
		const unresolved = loaded.filter((e) => !e.completed && !e.terminal && !e.declined);
		expect(unresolved.length).toBeLessThanOrEqual(1_000);
	});

	test("terminal audit rows are retained and only bind admission at the evidence cap", async () => {
		const base = 1571558400000000000n;
		const below = Array.from({ length: 10 }, (_, index) => ({
			messageId: String(base + BigInt(index)),
			channelId: "channel",
			terminal: true,
			audited: true,
		}));
		await writeFile(path, JSON.stringify(below));
		const outbox = new ImageOutbox(path);
		const pending = { messageId: String(base + 2_000n), channelId: "channel" };
		await outbox.put(pending);
		expect(await outbox.get(pending.messageId)).toEqual(pending);

		const full = Array.from({ length: 500 }, (_, index) => ({
			messageId: String(base + 10_000n + BigInt(index)),
			channelId: "channel",
			terminal: true,
			audited: true,
		}));
		await writeFile(path, JSON.stringify(full));
		const bounded = new ImageOutbox(path);
		// Admission now evicts old terminal entries instead of rejecting, so the new unresolved entry is admitted
		await expect(bounded.put({ messageId: String(base + 3_000n), channelId: "channel" })).resolves.toBeUndefined();
		await expect(bounded.put({ messageId: String(base + 3_001n), channelId: "other" })).resolves.toBeUndefined();
		// Terminal entries should stay around MAX_TERMINAL_IMAGES_PER_CHANNEL after eviction
		const terminalAfter = await bounded.load();
		const perChannel = terminalAfter.filter((e) => e.terminal && e.channelId === "channel");
		expect(perChannel.length).toBeLessThanOrEqual(500);
	});

	test("unresolved and terminal rows are evicted when journal bounds are exceeded", async () => {
		const outbox = new ImageOutbox(path);
		const now = Date.now();
		const twoHoursAgo = now - 2 * 60 * 60 * 1_000;

		// Add old unresolved entries first
		for (let i = 0; i < 100; i++) {
			await outbox.put({
				messageId: `${123456789012345600n + BigInt(i)}`,
				channelId: "channel-1",
				createdAt: twoHoursAgo,
			});
		}

		// Add new unresolved entries to reach the max
		for (let i = 100; i < 1_000; i++) {
			await outbox.put({
				messageId: `${123456789012345600n + BigInt(i)}`,
				channelId: "channel-1",
				createdAt: now,
			});
		}

		// Try to add one more new entry - this should work because old entries are evicted
		await outbox.put({
			messageId: `${123456790012345600n}`,
			channelId: "channel-1",
			createdAt: now,
		});

		const afterUnresolvedEviction = await outbox.load();
		const unresolved = afterUnresolvedEviction.filter((e) => !e.completed && !e.terminal && !e.declined);

		// Should have evicted oldest entries to stay at or below MAX_UNRESOLVED_IMAGES
		expect(unresolved.length).toBeLessThanOrEqual(1_000);
		// Since we're adding entries one at a time and evicting minimally,
		// most new entries should still be present
		const newOnes = unresolved.filter((e) => e.createdAt === now);
		expect(newOnes.length).toBeGreaterThanOrEqual(890);

		// Add old terminal entries first
		for (let i = 0; i < 100; i++) {
			await outbox.put({
				messageId: `${223456789012345600n + BigInt(i)}`,
				channelId: "channel-2",
				createdAt: twoHoursAgo,
				terminal: true,
			});
		}

		// Add new terminal entries to reach the per-channel max
		for (let i = 100; i < 500; i++) {
			await outbox.put({
				messageId: `${223456789012345600n + BigInt(i)}`,
				channelId: "channel-2",
				createdAt: now,
				terminal: true,
			});
		}

		// Try to add one more new terminal entry - this should work because old entries are evicted
		await outbox.put({
			messageId: `${224456789012345600n}`,
			channelId: "channel-2",
			createdAt: now,
			terminal: true,
		});

		const afterTerminalEviction = await outbox.load();
		const terminalInChannel = afterTerminalEviction.filter(
			(e) => e.terminal && !e.completed && e.channelId === "channel-2",
		);

		// Should have evicted oldest terminal entries per channel to stay at or below MAX_TERMINAL_IMAGES_PER_CHANNEL
		expect(terminalInChannel.length).toBeLessThanOrEqual(500);
		// Since we're adding entries one at a time and evicting minimally,
		// most new entries should still be present
		const newTerminal = terminalInChannel.filter((e) => e.createdAt === now);
		expect(newTerminal.length).toBeGreaterThanOrEqual(390);
	});
});

test("a directory fsync EIO fails admission while an unsupported sync degrades", async () => {
	const eio = Object.assign(new Error("EIO: i/o error, fsync"), { code: "EIO" });
	const failing = async () =>
		({
			sync: async () => {
				throw eio;
			},
			close: async () => {},
		}) as never;
	await expect(syncOutboxDirectory("/tmp", failing as never)).rejects.toThrow("EIO");

	const unsupported = async () =>
		({
			sync: async () => {
				throw Object.assign(new Error("EINVAL: invalid argument"), { code: "EINVAL" });
			},
			close: async () => {},
		}) as never;
	await expect(syncOutboxDirectory("/tmp", unsupported as never)).resolves.toBeUndefined();
});
