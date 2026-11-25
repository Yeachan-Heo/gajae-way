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

function recordMessage(
	db: Store,
	originKeyVal: string,
	messageId: string,
	body: string,
	authorName = "alice",
	authorId = "U-alice",
): void {
	db.transaction((tx) => {
		db.inbox.contextRecord(tx, {
			originKey: originKeyVal,
			messageId,
			body,
			authorName,
			authorId,
			receivedAt: new Date().toISOString(),
		});
	});
}

test("first thread turn includes the thread root from parent channel", async () => {
	database = await open();
	const parentChannelOrigin = originKey({ platform: "slack", kind: "channel", conversationId: "C1" });
	const threadOrigin = originKey({
		platform: "slack",
		kind: "thread",
		conversationId: "C1:1700000000.000100",
		parentId: "C1",
	});

	// Record the root message in the parent channel context
	recordMessage(
		database,
		parentChannelOrigin,
		"C1:1700000000.000100",
		"This is the thread root message",
		"bob",
		"U-bob",
	);

	// Record a message in the thread context
	recordMessage(database, threadOrigin, "C1:1700000000.000200", "First reply in thread", "alice", "U-alice");

	// Get context for the thread - the root should be fetchable
	const root = database.inbox.contextRowByMessage(parentChannelOrigin, "C1:1700000000.000100");
	expect(root).toBeDefined();
	expect(root?.body).toBe("This is the thread root message");
	expect(root?.author_name).toBe("bob");
});

test("a later turn does not need the root repeated", async () => {
	database = await open();
	const parentChannelOrigin = originKey({ platform: "slack", kind: "channel", conversationId: "C1" });
	const threadOrigin = originKey({
		platform: "slack",
		kind: "thread",
		conversationId: "C1:1700000000.000100",
		parentId: "C1",
	});

	// Record the root message in the parent channel context
	recordMessage(
		database,
		parentChannelOrigin,
		"C1:1700000000.000100",
		"This is the thread root message",
		"bob",
		"U-bob",
	);

	// Record multiple messages in the thread context
	recordMessage(database, threadOrigin, "C1:1700000000.000200", "First reply in thread", "alice", "U-alice");
	recordMessage(database, threadOrigin, "C1:1700000000.000300", "Second reply in thread", "bob", "U-bob");

	// The root should still be fetchable from the parent channel
	const root = database.inbox.contextRowByMessage(parentChannelOrigin, "C1:1700000000.000100");
	expect(root?.body).toBe("This is the thread root message");
});

test("contextRowByMessage returns undefined when message is not in that origin", async () => {
	database = await open();
	const parentChannelOrigin = originKey({ platform: "slack", kind: "channel", conversationId: "C1" });

	// Record a message in the parent channel
	recordMessage(
		database,
		parentChannelOrigin,
		"C1:1700000000.000100",
		"This is the thread root message",
		"bob",
		"U-bob",
	);

	// Verify the message exists in the correct origin
	const found = database.inbox.contextRowByMessage(parentChannelOrigin, "C1:1700000000.000100");
	expect(found?.body).toBe("This is the thread root message");

	// But it doesn't exist in a different origin (even with same message ID)
	const differentOrigin = originKey({ platform: "slack", kind: "channel", conversationId: "C2" });
	const notFound = database.inbox.contextRowByMessage(differentOrigin, "C1:1700000000.000100");
	expect(notFound).toBeUndefined();
});

test("a different channel's message with the same ts is never pulled", async () => {
	database = await open();
	const channel1Origin = originKey({ platform: "slack", kind: "channel", conversationId: "C1" });
	const channel2Origin = originKey({ platform: "slack", kind: "channel", conversationId: "C2" });

	// Record a message in C2 with the same timestamp as a C1 thread
	recordMessage(database, channel2Origin, "C2:1700000000.000100", "Message in different channel", "alice", "U-alice");

	// Try to fetch it as if it were the root of a C1 thread
	const wrongChannel = database.inbox.contextRowByMessage(channel1Origin, "C2:1700000000.000100");

	// It should not be found (origin isolation)
	expect(wrongChannel).toBeUndefined();

	// But it should exist in the correct origin
	const correct = database.inbox.contextRowByMessage(channel2Origin, "C2:1700000000.000100");
	expect(correct?.body).toBe("Message in different channel");
});
