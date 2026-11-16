import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
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

interface Fixture {
	readonly config: GatewayConfig;
	readonly port: DeadHostPort;
	readonly originKey: string;
	readonly originRefJson: string;
	readonly engagementJson: string;
	readonly workspace: string;
}

async function fixture(name: string, unresolvedHoldMs = 60_000): Promise<Fixture> {
	home = await realpath(await mkdtemp(join(tmpdir(), `gajaeway-escalation-${name}-`)));
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
		sessions: { unresolvedHoldMs },
	};
	const origin = { platform: "discord", kind: "dm", conversationId: name, peerId: "owner" } as const;
	return {
		config,
		port: new DeadHostPort({
			onBind: (input) => `session-${input.epoch}`,
			onSend: (input, scripted) => scripted.complete(input.opRef, "fresh answer"),
		}),
		originKey: `discord/dm/${name}/peer=owner`,
		originRefJson: JSON.stringify(origin),
		engagementJson: JSON.stringify({ authorId: "owner", authorName: "Alice", mentioned: true, group: false }),
		workspace,
	};
}

function record(db: Store, originKey: string, messageId: string, body: string, receivedAt: string): void {
	db.transaction((tx) =>
		db.inbox.contextRecord(tx, { messageId, originKey, authorId: "owner", authorName: "Alice", body, receivedAt }),
	);
}

async function until(check: () => boolean): Promise<void> {
	for (let i = 0; i < 400 && !check(); i++) await Bun.sleep(5);
	expect(check()).toBe(true);
}

for (const state of ["bound", "accepted"] as const) {
	test(`a legacy ${state} hold upgraded to schema 39 never re-renders context its old prompt may have carried (#686)`, async () => {
		const f = await fixture(`legacy-${state}`);
		let db = await Store.open(f.config.dbPath);
		const held = await f.port.bind({ originKey: f.originKey, epoch: 0, repo: f.workspace });
		expect(db.transaction((tx) => db.bindings.bindAtEpoch(tx, f.originKey, held.sessionId, 0))).toBe(true);
		const dispatchedAt = new Date(Date.now() - 12 * MINUTE).toISOString();
		const heldOpRef = `gw-p-legacy-${state}`;
		// An edited request: its context row is keyed by the ORIGINAL message id.
		const triggerId = "edit:m-held:0123456789abcdef";
		record(db, f.originKey, "m-held", "HELD-REQUEST edited text", dispatchedAt);
		db.transaction((tx) => {
			db.inbox.enqueue(tx, {
				messageId: triggerId,
				originKey: f.originKey,
				originRefJson: f.originRefJson,
				body: "HELD-REQUEST edited text",
				engagementJson: f.engagementJson,
				receivedAt: dispatchedAt,
			});
			db.inbox.bindTurn(tx, {
				messageId: triggerId,
				originKey: f.originKey,
				epoch: 0,
				opRef: heldOpRef,
				sessionId: held.sessionId,
				dispatchedAt,
			});
			if (state === "accepted") db.inbox.turnAccept(tx, heldOpRef);
		});
		// Arrived after the dispatch stamp but before the upgrade: the old prompt may have rendered it.
		record(
			db,
			f.originKey,
			"m-race",
			"RACE-CONTEXT arrived while the old prompt rendered",
			new Date(Date.now() - 11 * MINUTE).toISOString(),
		);
		db.close();
		const raw = new Database(f.config.dbPath);
		raw.exec(
			"ALTER TABLE inbound_messages DROP COLUMN send_attempted_at; ALTER TABLE inbound_messages DROP COLUMN send_context_json",
		);
		raw.exec("DELETE FROM schema_migrations WHERE version >= 39");
		raw.close();

		db = await Store.open(f.config.dbPath);
		expect(db.inbox.sentContextIds(heldOpRef)).toEqual(expect.arrayContaining(["m-held", "m-race", triggerId]));
		f.port.seedAcceptedSend({ sessionId: held.sessionId, repo: f.workspace, text: "HELD-REQUEST", opRef: heldOpRef });
		f.port.unknownOps.add(heldOpRef);
		f.port.setSessionState(held.sessionId, { live: false });
		// After the upgrade: genuinely unread for the fresh session.
		const nextAt = new Date().toISOString();
		record(db, f.originKey, "m-after", "AFTER-UPGRADE remark", new Date(Date.now() - MINUTE).toISOString());
		record(db, f.originKey, "m-next", "NEXT-QUESTION", nextAt);
		db.transaction((tx) =>
			db.inbox.enqueue(tx, {
				messageId: "m-next",
				originKey: f.originKey,
				originRefJson: f.originRefJson,
				body: "NEXT-QUESTION",
				engagementJson: f.engagementJson,
				receivedAt: nextAt,
			}),
		);
		const opened = db;
		server = await startUnixServer({
			config: f.config,
			database: opened,
			sessionPort: f.port,
			workspace: f.workspace,
			onStop: () => opened.close(),
		});
		await until(() => f.port.sends.some((send) => send.sessionId === "session-1"));
		const fresh = f.port.sends.find((send) => send.sessionId === "session-1")!;
		const unread = fresh.text.slice(fresh.text.indexOf("[Unread messages"));
		expect(unread).toContain("AFTER-UPGRADE");
		expect(unread).not.toContain("HELD-REQUEST");
		expect(unread).not.toContain("RACE-CONTEXT");
		expect(f.port.sendAttempts.filter((send) => send.opRef === heldOpRef)).toHaveLength(1);
	}, 30_000);
}

test("a late answer settled after a restart commits only the context its prompt claimed (#686)", async () => {
	const f = await fixture("late-settlement");
	let db = await Store.open(f.config.dbPath);
	const held = await f.port.bind({ originKey: f.originKey, epoch: 0, repo: f.workspace });
	expect(db.transaction((tx) => db.bindings.bindAtEpoch(tx, f.originKey, held.sessionId, 0))).toBe(true);
	const sentAt = new Date(Date.now() - 11 * MINUTE).toISOString();
	const heldOpRef = "gw-p-late-settlement";
	record(db, f.originKey, "m-held", "HELD-REQUEST", sentAt);
	db.transaction((tx) => {
		db.inbox.enqueue(tx, {
			messageId: "m-held",
			originKey: f.originKey,
			originRefJson: f.originRefJson,
			body: "HELD-REQUEST",
			engagementJson: f.engagementJson,
			receivedAt: sentAt,
		});
		db.inbox.bindTurn(tx, {
			messageId: "m-held",
			originKey: f.originKey,
			epoch: 0,
			opRef: heldOpRef,
			sessionId: held.sessionId,
			dispatchedAt: sentAt,
		});
		db.inbox.markSendAttempted(tx, heldOpRef, ["m-held"], sentAt);
	});
	f.port.seedAcceptedSend({ sessionId: held.sessionId, repo: f.workspace, text: "HELD-REQUEST", opRef: heldOpRef });
	f.port.unknownOps.add(heldOpRef);
	f.port.setSessionState(held.sessionId, { live: false });
	// Ambient chatter after the send, with no newer addressed message: it stays unread.
	record(db, f.originKey, "m-ambient", "AMBIENT remark", new Date(Date.now() - MINUTE).toISOString());

	const first = db;
	server = await startUnixServer({
		config: f.config,
		database: first,
		sessionPort: f.port,
		workspace: f.workspace,
		onStop: () => first.close(),
	});
	await until(() => first.bindings.getSessionRecord(f.originKey)?.epoch === 1);
	await server.stop("restart");
	server = undefined;

	// The held op finishes while the gateway is down; the restarted gateway settles it.
	f.port.unknownOps.delete(heldOpRef);
	f.port.setSessionState(held.sessionId, { live: true });
	f.port.complete(heldOpRef, "LATE-ANSWER");
	db = await Store.open(f.config.dbPath);
	const second = db;
	server = await startUnixServer({
		config: f.config,
		database: second,
		sessionPort: f.port,
		workspace: f.workspace,
		onStop: () => second.close(),
	});
	await until(() => second.inbox.turnRow(heldOpRef)?.turn_state === "done");
	const answers = second.outbox
		.list({})
		.filter((row) => row.originKey === f.originKey && JSON.parse(row.payloadJson).text?.includes("LATE-ANSWER"));
	expect(answers).toHaveLength(1);
	const unread = second.inbox.contextUnread(f.originKey, 50).map((row) => row.message_id);
	expect(unread).toContain("m-ambient");
	expect(unread).not.toContain("m-held");
	expect(f.port.sendAttempts.filter((send) => send.opRef === heldOpRef)).toHaveLength(1);
}, 60_000);

test("a queued message the held prompt carried as context is closed as its input, never resent as a trigger (#686, #698 review)", async () => {
	const f = await fixture("claimed-queued");
	const db = await Store.open(f.config.dbPath);
	const held = await f.port.bind({ originKey: f.originKey, epoch: 0, repo: f.workspace });
	expect(db.transaction((tx) => db.bindings.bindAtEpoch(tx, f.originKey, held.sessionId, 0))).toBe(true);
	const sentAt = new Date(Date.now() - 11 * MINUTE).toISOString();
	const queuedAt = new Date(Date.now() - 11 * MINUTE + 1_000).toISOString();
	const heldOpRef = "gw-p-held-claimed-queued";
	const enqueue = (messageId: string, body: string, receivedAt: string) =>
		db.transaction((tx) =>
			db.inbox.enqueue(tx, {
				messageId,
				originKey: f.originKey,
				originRefJson: f.originRefJson,
				body,
				engagementJson: f.engagementJson,
				receivedAt,
			}),
		);
	record(db, f.originKey, "m-held", "HELD-REQUEST", sentAt);
	record(db, f.originKey, "m-queued", "QUEUED-REQUEST deploy staging too", queuedAt);
	enqueue("m-held", "HELD-REQUEST", sentAt);
	// Queued behind the held turn, and rendered into its prompt's unread window.
	enqueue("m-queued", "QUEUED-REQUEST deploy staging too", queuedAt);
	db.transaction((tx) => {
		db.inbox.bindTurn(tx, {
			messageId: "m-held",
			originKey: f.originKey,
			epoch: 0,
			opRef: heldOpRef,
			sessionId: held.sessionId,
			dispatchedAt: sentAt,
		});
		db.inbox.markSendAttempted(tx, heldOpRef, ["m-held", "m-queued"], sentAt);
	});
	f.port.seedAcceptedSend({ sessionId: held.sessionId, repo: f.workspace, text: "HELD-REQUEST", opRef: heldOpRef });
	f.port.unknownOps.add(heldOpRef);
	f.port.setSessionState(held.sessionId, { live: false });
	const nextAt = new Date().toISOString();
	record(db, f.originKey, "m-next", "NEXT-QUESTION", nextAt);
	enqueue("m-next", "NEXT-QUESTION", nextAt);

	server = await startUnixServer({
		config: f.config,
		database: db,
		sessionPort: f.port,
		workspace: f.workspace,
		onStop: () => db.close(),
	});
	await until(() => f.port.sends.some((send) => send.sessionId === "session-1"));
	const fresh = f.port.sends.filter((send) => send.sessionId === "session-1");
	expect(fresh).toHaveLength(1);
	expect(fresh[0]!.text).toContain("NEXT-QUESTION");
	expect(fresh[0]!.text).toContain("msg:m-next)]");
	// It may appear only as answered history, never as the trigger or unread input.
	const actionable = fresh[0]!.text.slice(fresh[0]!.text.indexOf("\n\n") + 2);
	expect(actionable).not.toContain("QUEUED-REQUEST");
	const queued = db.inbox.turnRows(heldOpRef).find((row) => row.message_id === "m-queued");
	expect(queued).toMatchObject({ state: "done", turn_role: "steer", turn_state: "done" });
	expect(f.port.sendAttempts.filter((send) => send.opRef === heldOpRef)).toHaveLength(1);
}, 30_000);
