import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../src/store";

let directory = "";
let database: Store | undefined;

afterEach(async () => {
	database?.close();
	database = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

test("REGRESSION: persisted authored output is immutable across stage regression from authored to failed and back to authoring", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-store-authored-immutable-"));
	const dbPath = join(directory, "gateway.db");
	database = await Store.open(dbPath);
	if (!database) throw new Error("Failed to open database");
	const db = database;

	const eventId = `event-${crypto.randomUUID()}`;
	const monitorId = `monitor-${crypto.randomUUID()}`;
	const eventType = "test.event";

	// Create an event in admitted stage
	db.transaction((tx) => {
		db.monitors.eventCreate(tx, {
			eventId,
			monitorId,
			eventType,
			payloadJson: "{}",
			firedAt: new Date().toISOString(),
		});
	});

	// First turn: author with a real, non-silent answer
	const firstAuthored = db.transaction((tx) => db.monitors.authorFenced(tx, eventId, "", "real answer", false));
	expect(firstAuthored).toBe(true);

	// Verify the stage is 'authored' and answer is stored
	let event = db.monitors.eventGet(eventId);
	expect(event?.stage).toBe("authored");
	let answer = db.monitors.authoredOutput(eventId);
	expect(answer).toBe("real answer");

	// Simulate a post-author failure: event transitions from 'authorized' to 'failed'
	// This can happen when the outbox fails to process the authored event.
	// First, move event to 'dispatched' and acquire a lease
	const leaseId = `lease-${crypto.randomUUID()}`;
	db.transaction((tx) => {
		db.monitors.eventUpdate(tx, eventId, "dispatched");
		db.monitors.eventAcquireLease(tx, eventId, "test-owner", leaseId, 60_000);
	});

	// Fail the event (which should work because it has a valid lease and is not in an excluded stage)
	const failedResult = db.transaction((tx) =>
		db.monitors.eventFencedFail(
			tx,
			eventId,
			leaseId,
			`batch-${crypto.randomUUID()}`,
			"post_auth_failure",
			"Output processing failed",
			Date.now(),
			false,
		),
	);
	expect(failedResult).toBe(true);

	// Verify event is now in 'failed' stage
	event = db.monitors.eventGet(eventId);
	expect(event?.stage).toBe("failed");

	// Verify stored output is still present
	answer = db.monitors.authoredOutput(eventId);
	expect(answer).toBe("real answer");

	// Interleaving scenario: another gateway generation tries to re-author the failed event
	// with a different answer (simulating a fresh dispatch after the failure)
	// Try to re-author with a new answer
	const reauthorResult = db.transaction((tx) => db.monitors.authorFenced(tx, eventId, "", "new answer", false));

	// The re-authoring should FAIL because there's already stored output
	expect(reauthorResult).toBe(false);

	// Verify that:
	// 1. The stage is still 'failed' (not moved back to 'authorized')
	event = db.monitors.eventGet(eventId);
	expect(event?.stage).toBe("failed");

	// 2. The stored output is still the original answer (not overwritten)
	answer = db.monitors.authoredOutput(eventId);
	expect(answer).toBe("real answer");
});
