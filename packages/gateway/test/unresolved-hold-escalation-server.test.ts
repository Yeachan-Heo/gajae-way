import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { ScriptedSessionPort } from "./session-port.fake";

const MINUTE = 60_000;
let home = "";
let server: GatewayServer | undefined;

afterEach(async () => {
	await server?.stop("test cleanup");
	server = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
}, 30_000);

/** The held op's host is dead and its state unreadable; every other op behaves normally. */
class DeadHostPort extends ScriptedSessionPort {
	readonly unknownOps = new Set<string>();

	override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
		if (this.unknownOps.has(input.opRef))
			return { operationRef: input.opRef, status: { status: "unknown" as const }, summaryCompleted: false };
		return await super.status(input);
	}
}

test("a fresh epoch after escalation never re-renders the held request or its claimed context as unread (#686)", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-escalation-context-"));
	const workspace = join(home, "workspace");
	await mkdir(workspace, { recursive: true });
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		gjc: { agentDir: join(home, "agent") },
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
		sessions: { unresolvedHoldMs: 60_000 },
	};
	const db = await Store.open(config.dbPath);
	const origin = { platform: "discord", kind: "dm", conversationId: "escalation", peerId: "owner" } as const;
	const originKey = "discord/dm/escalation/peer=owner";
	const originRefJson = JSON.stringify(origin);
	const engagementJson = JSON.stringify({ authorId: "owner", authorName: "Alice", mentioned: true, group: false });
	const port = new DeadHostPort({
		onBind: (input) => `session-${input.epoch}`,
		onSend: (input, scripted) => scripted.complete(input.opRef, "fresh answer"),
	});
	const held = await port.bind({ originKey, epoch: 0, repo: workspace });
	expect(db.transaction((tx) => db.bindings.bindAtEpoch(tx, originKey, held.sessionId, 0))).toBe(true);

	const record = (messageId: string, body: string, receivedAt: string) =>
		db.transaction((tx) =>
			db.inbox.contextRecord(tx, { messageId, originKey, authorId: "owner", authorName: "Alice", body, receivedAt }),
		);
	// Context the held prompt rendered, then the held request itself: both may already be in the old session.
	const sentAt = new Date(Date.now() - 11 * MINUTE).toISOString();
	record("m-context", "CLAIMED-CONTEXT side remark", new Date(Date.now() - 12 * MINUTE).toISOString());
	record("m-held", "HELD-REQUEST deploy the release", sentAt);
	const heldOpRef = "gw-p-held-escalation";
	db.transaction((tx) => {
		db.inbox.enqueue(tx, {
			messageId: "m-held",
			originKey,
			originRefJson,
			body: "HELD-REQUEST deploy the release",
			engagementJson,
			receivedAt: sentAt,
		});
		db.inbox.bindTurn(tx, {
			messageId: "m-held",
			originKey,
			epoch: 0,
			opRef: heldOpRef,
			sessionId: held.sessionId,
			dispatchedAt: sentAt,
		});
		// The ACK was lost: the send intent and its rendered context are all that is durable.
		db.inbox.markSendAttempted(tx, heldOpRef, ["m-context", "m-held"], sentAt);
	});
	port.seedAcceptedSend({ sessionId: held.sessionId, repo: workspace, text: "HELD-REQUEST", opRef: heldOpRef });
	port.unknownOps.add(heldOpRef);
	port.setSessionState(held.sessionId, { live: false });

	// Arrived after the send: genuinely unread, and the next trigger.
	const laterAt = new Date(Date.now() - 2 * MINUTE).toISOString();
	record("m-later", "LATER-REMARK also check the logs", laterAt);
	const nextAt = new Date().toISOString();
	record("m-next", "NEXT-QUESTION how is it going", nextAt);
	db.transaction((tx) =>
		db.inbox.enqueue(tx, {
			messageId: "m-next",
			originKey,
			originRefJson,
			body: "NEXT-QUESTION how is it going",
			engagementJson,
			receivedAt: nextAt,
		}),
	);

	server = await startUnixServer({ config, database: db, sessionPort: port, onStop: () => db.close() });
	for (let i = 0; i < 400 && !port.sends.some((send) => send.sessionId === "session-1"); i++) await Bun.sleep(5);
	const fresh = port.sends.find((send) => send.sessionId === "session-1");
	expect(fresh).toBeDefined();
	expect(fresh!.text).toContain("NEXT-QUESTION");
	// Only what arrived after the held send is actionable; the held request and the
	// context its prompt carried may appear solely as answered session history.
	const unread = fresh!.text.slice(fresh!.text.indexOf("[Unread messages"));
	expect(unread).toContain("LATER-REMARK");
	expect(unread).not.toContain("HELD-REQUEST");
	expect(unread).not.toContain("CLAIMED-CONTEXT");
	// The only attempt under the held op-ref is the seeded original acceptance.
	expect(port.sendAttempts.filter((send) => send.opRef === heldOpRef)).toHaveLength(1);
	expect(db.inbox.turnRow(heldOpRef)?.turn_state).toBe("bound");
	// The escalation notice is in the delivery ledger for the origin.
	const notices = db.outbox.list({}).filter((row) => row.originKey === originKey && row.deliveryId.startsWith("gw-h-"));
	expect(notices).toHaveLength(1);
	expect(JSON.parse(notices[0]!.payloadJson).text).toContain("[turn held]");
}, 30_000);
