import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { mkdir, mkdtemp, readFile, rm, stat, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ImageOutbox, ImageOutboxCapacityError, type PendingImage, syncOutboxDirectory } from "../src/image-outbox";

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

	test("parked budget-deferred rows hold their own quota and never consume the work pool", async () => {
		const outbox = new ImageOutbox(path);
		const parked: string[] = [];
		let full = false;
		// Fill the parked pool until it refuses. The bounds are deliberately not
		// restated here so the assertion tracks the constants.
		for (let channel = 0; channel < 8 && !full; channel++) {
			for (let index = 0; index < 80 && !full; index++) {
				const messageId = `1234567890${String(channel).padStart(2, "0")}${String(index).padStart(6, "0")}`;
				try {
					await outbox.put({ messageId, channelId: `channel-${channel}`, deferred: true });
					parked.push(messageId);
				} catch (error) {
					if (!(error instanceof ImageOutboxCapacityError)) throw error;
					// A refusal on this channel's first row means the global parked bound is
					// reached; an earlier one only means this channel is full.
					if (index === 0) full = true;
					break;
				}
			}
		}
		expect(full).toBe(true);
		expect(parked.length).toBeLessThanOrEqual(250);
		// Authorized work is untouched by the parked backlog: its pool is separate.
		await outbox.put({ messageId: "123456789012999000", channelId: "channel-work", request: { image: "work" } });
		expect(await outbox.get("123456789012999000")).toMatchObject({ channelId: "channel-work" });
		// Converting an existing row into the parked pool respects the same bound.
		await expect(
			outbox.put({ messageId: "123456789012999000", channelId: "channel-work", deferred: true }),
		).rejects.toBeInstanceOf(ImageOutboxCapacityError);
		// Re-marking an already parked row is a retry, not a new occupant.
		await outbox.put({ messageId: parked[0] as string, channelId: "channel-0", deferred: true });
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
		await expect(outbox.put({ messageId: String(base + 2_000n), channelId: "channel" })).rejects.toThrow(
			"admission bound",
		);
		// An existing intent and a terminal row are still admitted.
		await outbox.put({ ...entries[0]!, failureAt: Date.now() });
		await outbox.put({ messageId: String(base + 2_001n), channelId: "channel", terminal: true, audited: true });
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
		await expect(bounded.put({ messageId: String(base + 3_000n), channelId: "channel" })).rejects.toThrow(
			"admission bound",
		);
		await expect(bounded.put({ messageId: String(base + 3_001n), channelId: "other" })).resolves.toBeUndefined();
		expect((await bounded.load()).filter((entry) => entry.terminal)).toHaveLength(500);
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
