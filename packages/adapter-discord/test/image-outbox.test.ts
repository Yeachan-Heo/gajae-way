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

	test("parked backpressure never consumes the unresolved pool authorized images compete for", async () => {
		const base = 1571558400000000000n;
		const work = Array.from({ length: 750 }, (_, index) => ({
			messageId: String(base + BigInt(index)),
			channelId: `work-${index % 3}`,
		}));
		const parked = Array.from({ length: 250 }, (_, index) => ({
			messageId: String(base + 10_000n + BigInt(index)),
			channelId: `parked-${index % 5}`,
			deferred: true,
		}));
		await writeFile(path, JSON.stringify([...work, ...parked]));
		const outbox = new ImageOutbox(path);
		const admitted: PendingImage = { messageId: String(base + 20_000n), channelId: "work-admitted" };
		// 750 deliverable rows plus 250 parked ones fill the journal only when parked
		// backpressure is charged to the pool it never ran in.
		await expect(outbox.put(admitted)).resolves.toBeUndefined();
		expect(await outbox.get(admitted.messageId)).toEqual(admitted);
		// The parked pool stays bounded on its own, independent of the work pool.
		await expect(
			outbox.put({ messageId: String(base + 20_001n), channelId: "parked-new", deferred: true }),
		).rejects.toBeInstanceOf(ImageOutboxCapacityError);
	});

	test("a full work pool still records parked backpressure on its own quota", async () => {
		const base = 1571558400000000000n;
		const work = Array.from({ length: 1_000 }, (_, index) => ({
			messageId: String(base + BigInt(index)),
			channelId: "channel",
		}));
		await writeFile(path, JSON.stringify(work));
		const outbox = new ImageOutbox(path);
		await expect(outbox.put({ messageId: String(base + 20_000n), channelId: "channel" })).rejects.toBeInstanceOf(
			ImageOutboxCapacityError,
		);
		// Parking is not admission: a full work pool must not turn every budget deferral
		// into a dead letter.
		await expect(
			outbox.put({ messageId: String(base + 20_001n), channelId: "channel", deferred: true }),
		).resolves.toBeUndefined();
	});

	test("terminal evidence at the cap never blocks an admitted intent from progressing", async () => {
		const base = 1571558400000000000n;
		const terminal = Array.from({ length: 500 }, (_, index) => ({
			messageId: String(base + BigInt(index)),
			channelId: "channel",
			terminal: true,
			audited: true,
		}));
		const pending: PendingImage = { messageId: String(base + 20_000n), channelId: "channel" };
		await writeFile(path, JSON.stringify([...terminal, pending]));
		const outbox = new ImageOutbox(path);
		await expect(outbox.put({ messageId: String(base + 30_000n), channelId: "channel" })).rejects.toBeInstanceOf(
			ImageOutboxCapacityError,
		);
		// The evidence cap binds admission only; an already admitted intent must still be
		// able to persist its failure clock and its prepared request.
		await expect(outbox.put({ ...pending, failureAt: Date.now() })).resolves.toBeUndefined();
		await expect(outbox.put({ ...pending, request: { text: "prepared" } })).resolves.toBeUndefined();
		expect(await outbox.get(pending.messageId)).toMatchObject({ request: { text: "prepared" } });
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
		const pending: PendingImage = { messageId: String(base + 20_000n), channelId: "channel" };
		await writeFile(
			path,
			JSON.stringify([
				...Array.from({ length: 10_000 }, (_, index) => ({
					messageId: String(base + BigInt(index)),
					channelId: "channel",
					completed: true,
					completedAt: Date.now(),
				})),
				pending,
			]),
		);
		const outbox = new ImageOutbox(path);
		await expect(outbox.put({ messageId: String(base + 30_000n), channelId: "channel" })).rejects.toThrow(
			"admission bound",
		);
		expect((await outbox.load()).filter((entry) => entry.completed)).toHaveLength(10_000);
		// The completion cap is an admission gate, never a reason to evict evidence or to
		// freeze an intent that is already past admission.
		await expect(outbox.put({ ...pending, failureAt: Date.now() })).resolves.toBeUndefined();
		await expect(outbox.put({ ...pending, request: { text: "prepared" } })).resolves.toBeUndefined();
		expect(await outbox.get(pending.messageId)).toMatchObject({ request: { text: "prepared" } });
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

	test("a restored overfull journal preserves every unresolved id on load and on update", async () => {
		const base = 1571558400000000000n;
		const ids = (count: number) => Array.from({ length: count }, (_, index) => String(base + BigInt(index)));
		const rows = (count: number) => ids(count).map((messageId) => ({ messageId, channelId: "channel" }));
		const persistedIds = async () =>
			(JSON.parse(await readFile(path, "utf8")) as Array<{ messageId: string }>).map((entry) => entry.messageId);

		for (const count of [1_000, 1_001]) {
			await writeFile(path, JSON.stringify(rows(count)));
			const outbox = new ImageOutbox(path);
			// Loading must not evict a row the admission bound protects: dropping the oldest
			// intent here would silently lose an image the gateway already accepted.
			expect((await outbox.load()).map((entry) => entry.messageId)).toEqual(ids(count));
			// An admitted intent keeps its exact journal slot while it records progress.
			await expect(
				outbox.put({ messageId: ids(count)[0] as string, channelId: "channel", failureAt: Date.now() }),
			).resolves.toBeUndefined();
			expect(await persistedIds()).toEqual(ids(count));
			// A refusal writes nothing: no eviction, no reordering, no dropped evidence.
			await expect(outbox.put({ messageId: String(base + 50_000n), channelId: "channel" })).rejects.toBeInstanceOf(
				ImageOutboxCapacityError,
			);
			expect(await persistedIds()).toEqual(ids(count));
		}
	});

	test("a restored journal preserves every terminal id at the per-channel and global evidence caps", async () => {
		const base = 1571558400000000000n;
		const terminal = (channelId: string, start: bigint, count: number) =>
			Array.from({ length: count }, (_, index) => ({
				messageId: String(start + BigInt(index)),
				channelId,
				terminal: true,
				audited: true,
			}));

		// Per channel: 500 rows is the exact cap and must survive load and later updates.
		const perChannel = terminal("channel-a", base, 500);
		const workRow: PendingImage = { messageId: String(base + 20_000n), channelId: "channel-a" };
		await writeFile(path, JSON.stringify([...perChannel, workRow]));
		const perChannelOutbox = new ImageOutbox(path);
		expect((await perChannelOutbox.load()).filter((entry) => entry.terminal)).toHaveLength(500);
		await expect(perChannelOutbox.put({ ...workRow, failureAt: Date.now() })).resolves.toBeUndefined();
		expect((await perChannelOutbox.load()).filter((entry) => entry.terminal)).toHaveLength(500);
		expect(await perChannelOutbox.get(perChannel[0]!.messageId)).toMatchObject({ terminal: true, audited: true });
		await expect(
			perChannelOutbox.put({ messageId: String(base + 30_000n), channelId: "channel-a" }),
		).rejects.toBeInstanceOf(ImageOutboxCapacityError);
		expect((await perChannelOutbox.load()).filter((entry) => entry.terminal)).toHaveLength(500);

		// Global: 10_000 terminal rows spread below the per-channel cap must survive too.
		const channels = Array.from({ length: 20 }, (_, index) => `global-${index}`);
		const globalTerminal = channels.flatMap((channelId, index) =>
			terminal(channelId, base + 100_000n + BigInt(index * 500), 500),
		);
		const globalWork: PendingImage = { messageId: String(base + 40_000n), channelId: "global-work" };
		await writeFile(path, JSON.stringify([...globalTerminal, globalWork]));
		const globalOutbox = new ImageOutbox(path);
		expect((await globalOutbox.load()).filter((entry) => entry.terminal)).toHaveLength(10_000);
		expect(await globalOutbox.get(globalTerminal[globalTerminal.length - 1]!.messageId)).toMatchObject({
			terminal: true,
		});
		await expect(globalOutbox.put({ ...globalWork, failureAt: Date.now() })).resolves.toBeUndefined();
		await expect(
			globalOutbox.put({ messageId: String(base + 50_000n), channelId: "global-work" }),
		).rejects.toBeInstanceOf(ImageOutboxCapacityError);
		expect((await globalOutbox.load()).filter((entry) => entry.terminal)).toHaveLength(10_000);
	});

	test("the last free work slot admits and the next intent is refused", async () => {
		const base = 1571558400000000000n;
		await writeFile(
			path,
			JSON.stringify(
				Array.from({ length: 999 }, (_, index) => ({
					messageId: String(base + BigInt(index)),
					channelId: "channel",
				})),
			),
		);
		const outbox = new ImageOutbox(path);
		const last: PendingImage = { messageId: String(base + 10_000n), channelId: "channel" };
		// The bound is a count of intents, so the 1000th still fits.
		await expect(outbox.put(last)).resolves.toBeUndefined();
		expect((await outbox.load()).filter((entry) => !entry.completed && !entry.terminal)).toHaveLength(1_000);
		await expect(outbox.put({ messageId: String(base + 10_001n), channelId: "channel" })).rejects.toBeInstanceOf(
			ImageOutboxCapacityError,
		);
		expect((await outbox.load()).filter((entry) => !entry.completed && !entry.terminal)).toHaveLength(1_000);
	});

	test("the last free parked slot admits and the next parked intent is refused", async () => {
		const base = 1571558400000000000n;
		const parked = (channelId: string, start: bigint, count: number): PendingImage[] =>
			Array.from({ length: count }, (_, index) => ({
				messageId: String(start + BigInt(index)),
				channelId,
				deferred: true,
			}));

		// Per channel: the 50th parked row is the last one a channel may hold.
		await writeFile(path, JSON.stringify(parked("parked-a", base, 49)));
		const perChannel = new ImageOutbox(path);
		await expect(
			perChannel.put({ messageId: String(base + 10_000n), channelId: "parked-a", deferred: true }),
		).resolves.toBeUndefined();
		await expect(
			perChannel.put({ messageId: String(base + 10_001n), channelId: "parked-a", deferred: true }),
		).rejects.toBeInstanceOf(ImageOutboxCapacityError);
		expect((await perChannel.load()).filter((entry) => entry.deferred === true)).toHaveLength(50);

		// Global: 250 parked rows are the whole parked pool, however they are spread.
		const spread = Array.from({ length: 5 }, (_, channel) =>
			parked(`parked-${channel}`, base + 100_000n + BigInt(channel * 50), 50),
		).flat();
		await writeFile(path, JSON.stringify(spread));
		const global = new ImageOutbox(path);
		await expect(
			global.put({ messageId: String(base + 20_000n), channelId: "parked-new", deferred: true }),
		).rejects.toBeInstanceOf(ImageOutboxCapacityError);
		// Re-marking an already parked row is a retry, not a new occupant.
		await expect(global.put({ ...(spread[0] as PendingImage) })).resolves.toBeUndefined();
		expect((await global.load()).filter((entry) => entry.deferred === true)).toHaveLength(250);
	});

	test("promoting a parked row is charged to the work pool without losing its durable intent", async () => {
		const base = 1571558400000000000n;
		const work = Array.from({ length: 999 }, (_, index) => ({
			messageId: String(base + BigInt(index)),
			channelId: "channel",
		}));
		const parked: PendingImage[] = ["parked-a", "parked-b"].map((channelId, index) => ({
			messageId: String(base + 10_000n + BigInt(index)),
			channelId,
			deferred: true,
		}));
		const evidence = Array.from({ length: 500 }, (_, index) => ({
			messageId: String(base + 20_000n + BigInt(index)),
			channelId: "channel",
			terminal: true,
			audited: true,
		}));
		await writeFile(path, JSON.stringify([...work, ...parked, ...evidence]));
		const outbox = new ImageOutbox(path);
		// Terminal evidence at its cap must not block a promotion: the parked row is already an
		// admitted intent, and the evidence it would wait for can never be evicted here.
		const promoted: PendingImage = { messageId: parked[0]!.messageId, channelId: parked[0]!.channelId };
		await expect(outbox.put(promoted)).resolves.toBeUndefined();
		expect(await outbox.get(promoted.messageId)).toEqual(promoted);
		// The work pool is exactly full now, so the next promotion keeps its parked row instead
		// of losing the intent or turning it terminal.
		await expect(
			outbox.put({ messageId: parked[1]!.messageId, channelId: parked[1]!.channelId }),
		).rejects.toBeInstanceOf(ImageOutboxCapacityError);
		expect(await outbox.get(parked[1]!.messageId)).toMatchObject({ deferred: true });
		// Making room lets the same promotion through with the same durable id.
		await outbox.remove(String(base));
		await expect(
			outbox.put({ messageId: parked[1]!.messageId, channelId: parked[1]!.channelId }),
		).resolves.toBeUndefined();
		expect(await outbox.get(parked[1]!.messageId)).toEqual({
			messageId: parked[1]!.messageId,
			channelId: parked[1]!.channelId,
		});
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
