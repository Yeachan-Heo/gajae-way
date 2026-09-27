import { expect, test } from "bun:test";
import { GatewayDatabase } from "../src/store/db";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

const DIRECT_ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "steer-context-dedup" } as const;
const DIRECT_ORIGIN_KEY = "loopback/loopback/steer-context-dedup";

test("PR #337: inboundSteerAccepted closes context message without steering", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-context-dedup-"));
	const path = join(home, "gateway.db");
	try {
		const database = await GatewayDatabase.open(path);

		const originKey = DIRECT_ORIGIN_KEY;
		const triggerMessageId = "trigger-msg-1";
		const contextMessageId = "context-msg-in-unread";
		const triggerOpRef = "gw-p-test-trigger-1";
		const epoch = 1;

		// Set up context window with a message that will be in unread
		database.contextRecord({
			messageId: contextMessageId,
			originKey,
			body: "This message is already in unread context",
		});

		// Enqueue the trigger message
		database.inboundEnqueue({
			messageId: triggerMessageId,
			originKey,
			originRefJson: JSON.stringify(DIRECT_ORIGIN),
			body: "Trigger message",
		});

		// Bind it as a turn (simulating dispatch)
		const boundTrigger = database.inboundBindTurn({
			messageId: triggerMessageId,
			originKey,
			epoch,
			opRef: triggerOpRef,
			sessionId: "session-1",
		});
		expect(boundTrigger.turn_op_ref).toBe(triggerOpRef);

		// Now enqueue the context message as a follow-up
		// (simulating recovery re-queue or out-of-order arrival)
		database.inboundEnqueue({
			messageId: contextMessageId,
			originKey,
			originRefJson: JSON.stringify(DIRECT_ORIGIN),
			body: "This message is already in unread context",
		});

		// Verify it's pending
		const pendingRow = database.inboundPendingOldest(originKey);
		expect(pendingRow?.message_id).toBe(contextMessageId);
		expect(pendingRow?.state).toBe("pending");

		// The fix: call inboundSteerAccepted which closes it WITHOUT steering
		const closed = database.inboundSteerAccepted({
			messageId: contextMessageId,
			epoch,
			opRef: triggerOpRef,
		});

		expect(closed).toBe(true);

		// Verify the row is closed as done steer of the turn
		const closedRow = database.inboundTurnRow(triggerOpRef);
		const allTurnRows = database.inboundTurnRows(triggerOpRef);
		const contextRow = allTurnRows.find((r) => r.message_id === contextMessageId);

		expect(contextRow).toBeDefined();
		expect(contextRow?.state).toBe("done");
		expect(contextRow?.turn_state).toBe("done");
		expect(contextRow?.turn_role).toBe("steer");

		// No more pending rows for this origin
		const nextPending = database.inboundPendingOldest(originKey);
		expect(nextPending).toBeUndefined();

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("PR #337: edit row is still steered when original is in context", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-edit-context-dedup-"));
	const path = join(home, "gateway.db");
	try {
		const database = await GatewayDatabase.open(path);

		const originKey = DIRECT_ORIGIN_KEY;
		const triggerMessageId = "trigger-edit-test";
		const originalMessageId = "orig-msg-for-edit";
		const editMessageId = "edit-msg-id";
		const triggerOpRef = "gw-p-test-trigger-edit";
		const epoch = 2;

		// Set up context with the original message
		database.contextRecord({
			messageId: originalMessageId,
			originKey,
			body: "Original message text",
		});

		// Enqueue trigger
		database.inboundEnqueue({
			messageId: triggerMessageId,
			originKey,
			originRefJson: JSON.stringify(DIRECT_ORIGIN),
			body: "Trigger message",
		});

		// Bind trigger as a turn
		database.inboundBindTurn({
			messageId: triggerMessageId,
			originKey,
			epoch,
			opRef: triggerOpRef,
			sessionId: "session-2",
		});

		// Enqueue the edit (new message id but for an original in context)
		database.inboundEnqueue({
			messageId: editMessageId,
			originKey,
			originRefJson: JSON.stringify(DIRECT_ORIGIN),
			body: "Edited message text - this is new content",
		});

		// Verify edit is pending
		const editPending = database.inboundPendingOldest(originKey);
		expect(editPending?.message_id).toBe(editMessageId);

		// The edit SHOULD be accepted (will be steered)
		// The key difference from the context dedup test is that edits have their own
		// message id, so they're not in the contextMessageIds set
		const editAccepted = database.inboundSteerAccepted({
			messageId: editMessageId,
			epoch,
			opRef: triggerOpRef,
			contextMessageId: originalMessageId, // The ORIGINAL message's context is consumed
		});

		expect(editAccepted).toBe(true);

		// Verify the edit row is done
		const editRow = database.inboundTurnRows(triggerOpRef).find((r) => r.message_id === editMessageId);
		expect(editRow?.state).toBe("done");
		expect(editRow?.turn_role).toBe("steer");

		// The original message's context should be consumed
		const contextDiag = database.contextDiagnostics(originKey);
		expect(contextDiag.unread).toBe(0);

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("PR #337: after turn terminal and restart, context-closed message never re-sends", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-restart-dedup-"));
	const path = join(home, "gateway.db");
	try {
		const database = await GatewayDatabase.open(path);

		const originKey = DIRECT_ORIGIN_KEY;
		const triggerMessageId = "trigger-terminal";
		const contextMessageId = "context-terminal";
		const triggerOpRef = "gw-p-test-trigger-term";
		const epoch = 3;

		// Set up context and trigger
		database.contextRecord({
			messageId: contextMessageId,
			originKey,
			body: "Context message terminal test",
		});

		database.inboundEnqueue({
			messageId: triggerMessageId,
			originKey,
			originRefJson: JSON.stringify(DIRECT_ORIGIN),
			body: "Trigger message terminal test",
		});

		// Bind and complete turn as terminal
		const boundTrigger = database.inboundBindTurn({
			messageId: triggerMessageId,
			originKey,
			epoch,
			opRef: triggerOpRef,
			sessionId: "session-3",
		});

		database.inboundTurnAccept(triggerOpRef);
		database.inboundTurnComplete(triggerOpRef, "no_delivery");

		const completedTrigger = database.inboundTurnRow(triggerOpRef);
		expect(completedTrigger?.turn_state).toBe("done");

		// Enqueue context message as follow-up
		database.inboundEnqueue({
			messageId: contextMessageId,
			originKey,
			originRefJson: JSON.stringify(DIRECT_ORIGIN),
			body: "Context message terminal test",
		});

		// Close it as in-context
		const closed = database.inboundSteerAccepted({
			messageId: contextMessageId,
			epoch,
			opRef: triggerOpRef,
		});
		expect(closed).toBe(true);

		// Simulate restart: close and re-open database
		database.close();

		const database2 = await GatewayDatabase.open(path);

		// After restart, verify:
		// 1. No nonterminal turns for this origin
		const nonterminal = database2.inboundNonterminalTurns(originKey);
		expect(nonterminal.length).toBe(0);

		// 2. No pending rows for this origin (both trigger and context are done)
		const pending = database2.inboundPendingOrigins();
		expect(pending).not.toContain(originKey);

		// 3. The context message is still done
		const allTurnRows = database2.inboundTurnRows(triggerOpRef);
		const contextRow = allTurnRows.find((r) => r.message_id === contextMessageId);
		expect(contextRow?.state).toBe("done");

		database2.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
