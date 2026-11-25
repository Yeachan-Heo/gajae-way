import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { originKey } from "@gajae-gateway/protocol";
import { Store } from "../src/store";

let directory = "";
let database: Store | undefined;

afterEach(async () => {
	database?.close();
	database = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
});

async function open(): Promise<Store> {
	directory = await mkdtemp(join(tmpdir(), "thread-root-context-"));
	database = await Store.open(join(directory, "gateway.db"));
	return database;
}

test("hasThreadTurns returns false when no trigger/steer turns exist on origin", async () => {
	database = await open();
	const threadOriginKey = originKey({
		platform: "slack",
		kind: "thread",
		conversationId: "C1:1700000000.000100",
		parentId: "C1",
	});

	// No messages recorded yet
	const result = database!.inbox.hasThreadTurns(threadOriginKey);
	expect(result).toBe(false);
});

test("contextRowByMessage retrieves a specific message from conversation context", async () => {
	database = await open();
	const slackChannelOrigin = "slack/channel/C1";
	const messageId = "1700000000.000100";

	// Record a message
	database!.transaction((tx) => {
		database!.inbox.contextRecord(tx, {
			originKey: slackChannelOrigin,
			messageId,
			body: "This is a test message",
			authorName: "alice",
			authorId: "U-alice",
			receivedAt: new Date().toISOString(),
		});
	});

	// Retrieve it
	const row = database!.inbox.contextRowByMessage(slackChannelOrigin, messageId);
	expect(row).toBeDefined();
	expect(row?.body).toBe("This is a test message");
	expect(row?.author_name).toBe("alice");
	expect(row?.message_id).toBe(messageId);
});

test("contextRowByMessage returns undefined for non-existent message", async () => {
	database = await open();
	const slackChannelOrigin = "slack/channel/C1";

	const row = database!.inbox.contextRowByMessage(slackChannelOrigin, "non-existent");
	expect(row).toBeUndefined();
});

test("contextRowByMessage respects origin isolation", async () => {
	database = await open();
	const channel1Origin = "slack/channel/C1";
	const channel2Origin = "slack/channel/C2";
	const sharedTs = "1700000000.000100";

	// Record a message in C2
	database!.transaction((tx) => {
		database!.inbox.contextRecord(tx, {
			originKey: channel2Origin,
			messageId: sharedTs,
			body: "Message in different channel",
			authorName: "alice",
			authorId: "U-alice",
			receivedAt: new Date().toISOString(),
		});
	});

	// Try to fetch it from C1 - should not find it
	const wrongChannel = database!.inbox.contextRowByMessage(channel1Origin, sharedTs);
	expect(wrongChannel).toBeUndefined();

	// But it should exist in the correct origin
	const correct = database!.inbox.contextRowByMessage(channel2Origin, sharedTs);
	expect(correct?.body).toBe("Message in different channel");
});

test("thread root message can be retrieved from parent channel context", async () => {
	database = await open();
	const slackChannelOrigin = originKey({ platform: "slack", kind: "channel", conversationId: "C1" });
	const slackThreadRoot = "1700000000.000100";
	const slackThreadOrigin = originKey({
		platform: "slack",
		kind: "thread",
		conversationId: `C1:${slackThreadRoot}`,
		parentId: "C1",
	});

	// Record the thread root message in the parent channel context
	database!.transaction((tx) => {
		database!.inbox.contextRecord(tx, {
			originKey: slackChannelOrigin,
			messageId: slackThreadRoot,
			body: "This is the thread root message",
			authorName: "alice",
			authorId: "U-alice",
			receivedAt: new Date().toISOString(),
		});
	});

	// Verify we can fetch it from the parent channel
	const root = database!.inbox.contextRowByMessage(slackChannelOrigin, slackThreadRoot);
	expect(root).toBeDefined();
	expect(root?.body).toBe("This is the thread root message");

	// Verify hasThreadTurns returns false (no turns yet on thread origin)
	expect(database!.inbox.hasThreadTurns(slackThreadOrigin)).toBe(false);
});
