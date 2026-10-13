import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ACK_TIMEOUT_MS, classifyDeliveryError, DeliveryService } from "../src/delivery/delivery";
import { Store } from "../src/store";

test("delivery ledger expires only after the fifth definitive failure", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-ledger-"));
	try {
		const database = await Store.open(join(directory, "gateway.db"));
		const ledger = new DeliveryService(database);
		ledger.createPending({ deliveryId: "one", turnId: "turn", originKey: "discord/channel/c", payloadJson: "{}" });
		expect(ledger.counts().pending).toBe(1);
		ledger.markInflight("one");
		expect(ledger.get("one")?.state).toBe("inflight");
		for (let attempt = 1; attempt < 5; attempt++) {
			ledger.fail("one");
			expect(ledger.get("one")?.state).toBe("pending");
			expect(ledger.get("one")?.attempts).toBe(attempt);
		}
		ledger.fail("one");
		expect(ledger.get("one")?.state).toBe("expired");
		ledger.createPending({ deliveryId: "two", turnId: "turn", originKey: "discord/channel/c", payloadJson: "{}" });
		ledger.confirm("two");
		expect(ledger.get("two")?.state).toBe("confirmed");
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a failed attempt records an allowlisted last_error and the scheduled next_retry_at", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-ledger-"));
	try {
		const database = await Store.open(join(directory, "gateway.db"));
		const ledger = new DeliveryService(database);
		ledger.createPending({ deliveryId: "retry", turnId: "turn", originKey: "discord/channel/c", payloadJson: "{}" });
		ledger.markInflight("retry");
		const inflight = ledger.get("retry")!;
		// An unsettled row that never failed is due on the next sweep.
		expect(inflight).toMatchObject({ lastError: null, nextRetryAt: inflight.updatedAt });
		ledger.fail("retry", true, "timeout after dispatch token=secret-value");
		const failed = ledger.get("retry")!;
		expect(failed).toMatchObject({ state: "failed_ambiguous", attempts: 1, lastError: "timeout" });
		expect(Date.parse(failed.nextRetryAt!)).toBe(Date.parse(failed.updatedAt) + 2_000);
		expect(JSON.stringify(ledger.counts())).not.toContain("secret-value");
		expect(ledger.counts().recentPending).toEqual([
			{
				deliveryId: "retry",
				originKey: "discord/channel/c",
				state: "failed_ambiguous",
				attempts: 1,
				lastError: "timeout",
				nextRetryAt: failed.nextRetryAt,
				createdAt: failed.createdAt,
			},
		]);
		for (let attempt = 2; attempt <= 5; attempt++) ledger.fail("retry", false, "Unknown Message");
		expect(ledger.get("retry")).toMatchObject({
			state: "expired",
			attempts: 5,
			lastError: "not_found",
			nextRetryAt: null,
		});
		expect(ledger.counts().recentExpired[0]).toMatchObject({ deliveryId: "retry", lastError: "not_found" });
		expect(ledger.requeueById("retry")).toEqual(["retry"]);
		const redriven = ledger.get("retry")!;
		expect(redriven).toMatchObject({ state: "pending", lastError: "not_found", nextRetryAt: redriven.updatedAt });
		ledger.confirm("retry");
		expect(ledger.get("retry")).toMatchObject({ state: "confirmed", nextRetryAt: null });
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("failure reasons map onto the allowlisted classification codes", () => {
	for (const [reason, code] of [
		["rate limited", "rate_limited"],
		["HTTP 429 Too Many Requests", "rate_limited"],
		["timeout after dispatch", "timeout"],
		["socket hang up", "network"],
		["Slack network lost", "network"],
		["Unknown Message", "not_found"],
		["Bad Request: REACTIONS_DISABLED", "forbidden"],
		["not_allowed", "forbidden"],
		["Invalid Form Body", "invalid_request"],
		["No persisted Telegram reply route for 22", "other"],
		[undefined, "other"],
	] as const)
		expect(classifyDeliveryError(reason)).toBe(code);
});

test("ambiguous failures never expire by count", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-ledger-"));
	try {
		const database = await Store.open(join(directory, "gateway.db"));
		const ledger = new DeliveryService(database);
		ledger.createPending({
			deliveryId: "ambiguous",
			turnId: "turn",
			originKey: "discord/channel/c",
			payloadJson: "{}",
		});
		for (let attempt = 0; attempt < 9; attempt++) ledger.fail("ambiguous", true);
		expect(ledger.get("ambiguous")).toMatchObject({ state: "failed_ambiguous", attempts: 9 });
		const updatedAt = Date.parse(ledger.get("ambiguous")!.updatedAt);
		expect(ledger.listUndelivered(24 * 60 * 60_000, updatedAt + 5 * 60_000 - 1)).toHaveLength(0);
		expect(ledger.listUndelivered(24 * 60 * 60_000, updatedAt + 5 * 60_000)).toHaveLength(1);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("retry backoff doubles from two seconds and caps at five minutes", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-ledger-"));
	try {
		const database = await Store.open(join(directory, "gateway.db"));
		const ledger = new DeliveryService(database);
		ledger.createPending({ deliveryId: "backoff", turnId: "turn", originKey: "discord/channel/c", payloadJson: "{}" });
		ledger.fail("backoff");
		let updatedAt = Date.parse(ledger.get("backoff")!.updatedAt);
		expect(ledger.listUndelivered(24 * 60 * 60_000, updatedAt + 1_999)).toHaveLength(0);
		expect(ledger.listUndelivered(24 * 60 * 60_000, updatedAt + 2_000)).toHaveLength(1);
		ledger.fail("backoff");
		updatedAt = Date.parse(ledger.get("backoff")!.updatedAt);
		expect(ledger.listUndelivered(24 * 60 * 60_000, updatedAt + 3_999)).toHaveLength(0);
		expect(ledger.listUndelivered(24 * 60 * 60_000, updatedAt + 4_000)).toHaveLength(1);
		// A newly negotiated adapter is a new transport: backoff does not gate its replay.
		expect(ledger.listUndelivered(24 * 60 * 60_000, updatedAt, true)).toHaveLength(1);
		for (let attempt = 0; attempt < 8; attempt++) ledger.fail("backoff", true);
		expect(ledger.get("backoff")?.attempts).toBe(10);
		updatedAt = Date.parse(ledger.get("backoff")!.updatedAt);
		expect(ledger.listUndelivered(24 * 60 * 60_000, updatedAt + 5 * 60_000 - 1)).toHaveLength(0);
		expect(ledger.listUndelivered(24 * 60 * 60_000, updatedAt + 5 * 60_000)).toHaveLength(1);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("an age sweep expires stale unsettled rows and returns expiry evidence", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-ledger-"));
	try {
		const database = await Store.open(join(directory, "gateway.db"));
		const ledger = new DeliveryService(database);
		ledger.createPending({ deliveryId: "stale", turnId: "turn", originKey: "discord/channel/c", payloadJson: "{}" });
		ledger.createPending({
			deliveryId: "confirmed",
			turnId: "turn",
			originKey: "discord/channel/c",
			payloadJson: "{}",
		});
		ledger.confirm("confirmed");
		const now = Date.now() + 24 * 60 * 60_000 + 1;
		const expired = ledger.expireStale(24 * 60 * 60_000, now);
		expect(expired).toEqual([
			{
				deliveryId: "stale",
				originKey: "discord/channel/c",
				attempts: 0,
				expiredAt: new Date(now).toISOString(),
				lastError: null,
			},
		]);
		expect(ledger.get("stale")?.state).toBe("expired");
		expect(ledger.get("confirmed")?.state).toBe("confirmed");
		expect(ledger.listUndelivered(24 * 60 * 60_000, now)).toHaveLength(0);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("counts include expired totals and the five newest metadata-only entries", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-ledger-"));
	try {
		const database = await Store.open(join(directory, "gateway.db"));
		const ledger = new DeliveryService(database);
		for (let index = 0; index < 7; index++) {
			const deliveryId = `expired-${index}`;
			ledger.createPending({
				deliveryId,
				turnId: "turn",
				originKey: `discord/channel/${index}`,
				payloadJson: JSON.stringify({ text: "private message body" }),
			});
			for (let attempt = 0; attempt < 5; attempt++) ledger.fail(deliveryId);
		}
		const counts = ledger.counts();
		expect(counts).toMatchObject({ pending: 0, oldestPendingAgeMs: null, expired: 7 });
		expect(counts.recentExpired).toHaveLength(5);
		expect(
			counts.recentExpired.every(
				(row) => Object.keys(row).sort().join(",") === "attempts,deliveryId,expiredAt,lastError,originKey",
			),
		).toBe(true);
		expect(counts.recentExpired.map((row) => row.attempts)).toEqual([5, 5, 5, 5, 5]);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("requeue-by-id and requeue-since reset eligible rows without touching confirmed rows", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-ledger-"));
	try {
		const database = await Store.open(join(directory, "gateway.db"));
		const ledger = new DeliveryService(database);
		for (const deliveryId of ["expired", "ambiguous", "pending", "confirmed"])
			ledger.createPending({ deliveryId, turnId: "turn", originKey: "discord/channel/c", payloadJson: "{}" });
		for (let attempt = 0; attempt < 5; attempt++) ledger.fail("expired");
		ledger.fail("ambiguous", true);
		ledger.fail("confirmed");
		ledger.confirm("confirmed");
		const priorUpdatedAt = ledger.get("expired")!.updatedAt;
		expect(ledger.requeueById("expired")).toEqual(["expired"]);
		expect(ledger.get("expired")).toMatchObject({ state: "pending", attempts: 0 });
		expect(Date.parse(ledger.get("expired")!.updatedAt)).toBeGreaterThanOrEqual(Date.parse(priorUpdatedAt));
		expect(ledger.requeueById("confirmed")).toEqual([]);
		expect(ledger.requeueById("unknown")).toEqual([]);
		const since = new Date(Date.now() - 10_000).toISOString();
		expect(new Set(ledger.requeueSince(since))).toEqual(new Set(["expired", "ambiguous", "pending"]));
		for (const deliveryId of ["expired", "ambiguous", "pending"])
			expect(ledger.get(deliveryId)).toMatchObject({ state: "pending", attempts: 0 });
		expect(ledger.get("confirmed")).toMatchObject({ state: "confirmed", attempts: 1 });
		expect(ledger.requeueSince(new Date(Date.now() + 60_000).toISOString())).toEqual([]);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a deterministic tail delivery id is admitted once across replay", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-ledger-"));
	try {
		const database = await Store.open(join(directory, "gateway.db"));
		const ledger = new DeliveryService(database);
		const row = { deliveryId: "gw-d-tail-event", turnId: "turn", originKey: "discord/channel/c", payloadJson: "{}" };
		expect(ledger.createPending(row)).toBe(true);
		expect(ledger.createPending(row)).toBe(false);
		expect(ledger.counts().pending).toBe(1);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("timed sweep does not re-broadcast fresh inflight rows until ack timeout expires", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-ledger-"));
	try {
		const database = await Store.open(join(directory, "gateway.db"));
		const ledger = new DeliveryService(database);
		ledger.createPending({ deliveryId: "inflight", turnId: "turn", originKey: "discord/channel/c", payloadJson: "{}" });
		ledger.markInflight("inflight");
		const createdAt = Date.parse(ledger.get("inflight")!.createdAt);
		const updatedAt = Date.parse(ledger.get("inflight")!.updatedAt);

		// Fresh inflight row should not be returned by the timed sweep (ignoreBackoff=false)
		expect(ledger.listUndelivered(24 * 60 * 60_000, updatedAt, false)).toHaveLength(0);

		// After ack timeout, it should be returned with duplicateWarning
		const afterTimeout = updatedAt + ACK_TIMEOUT_MS;
		const undelivered = ledger.listUndelivered(24 * 60 * 60_000, afterTimeout, false);
		expect(undelivered).toHaveLength(1);
		expect(undelivered[0]).toMatchObject({ deliveryId: "inflight", state: "inflight" });

		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("onConnect sweep replays inflight rows immediately without waiting for ack timeout", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-ledger-"));
	try {
		const database = await Store.open(join(directory, "gateway.db"));
		const ledger = new DeliveryService(database);
		ledger.createPending({ deliveryId: "inflight", turnId: "turn", originKey: "discord/channel/c", payloadJson: "{}" });
		ledger.markInflight("inflight");
		const updatedAt = Date.parse(ledger.get("inflight")!.updatedAt);

		// Immediately after marking inflight, with ignoreBackoff=true (onConnect case)
		const undelivered = ledger.listUndelivered(24 * 60 * 60_000, updatedAt, true);
		expect(undelivered).toHaveLength(1);
		expect(undelivered[0]).toMatchObject({ deliveryId: "inflight", state: "inflight" });

		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("timed sweep re-broadcasts inflight rows after ack timeout for crash/lost-ack recovery", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-ledger-"));
	try {
		const database = await Store.open(join(directory, "gateway.db"));
		const ledger = new DeliveryService(database);
		ledger.createPending({ deliveryId: "lost-ack", turnId: "turn", originKey: "discord/channel/c", payloadJson: "{}" });
		ledger.markInflight("lost-ack");
		const updatedAt = Date.parse(ledger.get("lost-ack")!.updatedAt);

		// Within ack timeout, not returned by timed sweep (ignoreBackoff=false)
		let undelivered = ledger.listUndelivered(24 * 60 * 60_000, updatedAt + ACK_TIMEOUT_MS - 1, false);
		expect(undelivered).toHaveLength(0);

		// After ack timeout, returned by timed sweep for recovery
		undelivered = ledger.listUndelivered(24 * 60 * 60_000, updatedAt + ACK_TIMEOUT_MS, false);
		expect(undelivered).toHaveLength(1);
		expect(undelivered[0]).toMatchObject({ deliveryId: "lost-ack", state: "inflight" });

		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
