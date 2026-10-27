import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { RuntimeCycleProjector } from "../src/ops/cycle";
import { findSilentOrigins, ORIGIN_SILENT_MS, OriginSilentAlerter } from "../src/ops/origin-silent";
import { Store } from "../src/store";
import { BROKER_PROMPT_BUSY_REFUSAL } from "../src/store/groups/bindings";
import { createOwnedSessionFixture, initializeTestBrokerAuthority } from "./session-port.fake";

let home = "";
let database: Store | undefined;

afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

const MINUTE = 60_000;
const NOW = Date.parse("2026-10-26T09:00:00.000Z");
const CHANNEL = "discord/channel/1508659103637966858";

async function open(): Promise<Store> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-origin-silent-"));
	database = await Store.open(join(home, "gateway.db"));
	return database;
}

function enqueue(
	db: Store,
	input: {
		messageId: string;
		originKey?: string;
		ageMs: number;
		authorIsBot?: boolean;
		replyTo?: string;
	},
): void {
	const originKey = input.originKey ?? CHANNEL;
	const [platform, kind, conversationId] = originKey.split("/");
	expect(
		db.transaction((tx) =>
			db.inbox.enqueue(tx, {
				messageId: input.messageId,
				originKey,
				originRefJson: JSON.stringify({ platform, kind, conversationId }),
				body: "hello?",
				engagementJson: JSON.stringify({
					mentioned: true,
					authorIsBot: input.authorIsBot ?? false,
					...(input.replyTo ? { replyTo: { messageId: input.replyTo, fromSelf: true } } : {}),
				}),
				receivedAt: new Date(NOW - input.ageMs).toISOString(),
			}),
		),
	).toBe(true);
}

function deliver(db: Store, deliveryId: string, ageMs: number, originKey = CHANNEL): void {
	db.transaction((tx) => {
		db.outbox.create(tx, { deliveryId, turnId: "t", originKey, payloadJson: "{}" }, NOW - ageMs);
		db.outbox.confirm(tx, deliveryId);
	});
}

/** Today's incident shape: one turn held as an unresolved send while user messages pile up behind it. */
function holdTurn(db: Store, messageId: string): string {
	const opRef = `gw-p-held-${messageId}`;
	db.transaction((tx) => {
		db.bindings.bindAtEpoch(tx, CHANNEL, "sdk-held", 0);
		db.inbox.bindTurn(tx, { messageId, originKey: CHANNEL, epoch: 0, opRef, sessionId: "sdk-held" });
		db.inbox.markSendAttempted(tx, opRef);
	});
	return opRef;
}

test("a held turn with unanswered user messages raises origin_silent with its blocking op-ref and hold reason (#688)", async () => {
	const db = await open();
	enqueue(db, { messageId: "m-1", ageMs: 40 * MINUTE });
	const opRef = holdTurn(db, "m-1");
	enqueue(db, { messageId: "m-2", ageMs: 30 * MINUTE });
	enqueue(db, { messageId: "m-3", ageMs: 1 * MINUTE });
	const cycle = new RuntimeCycleProjector(db, { queueDepth: 0 }).project(new Date(NOW));
	expect(cycle.gates).toContain("origin_silent");
	expect(cycle.phase).toBe("degraded");
	expect(cycle.silentOrigins).toEqual([
		{
			originKey: CHANNEL,
			oldestMessageId: "m-1",
			oldestPendingAgeMs: 40 * MINUTE,
			unanswered: 2,
			blockingOpRef: opRef,
			holdReason: "operation_state_unknown",
		},
	]);
});

test("hold reasons distinguish unsent, busy-refused, running and unbound origins (#688)", async () => {
	const db = await open();
	const authority = initializeTestBrokerAuthority(db, join(home, "agent"));
	enqueue(db, { messageId: "m-1", ageMs: 20 * MINUTE });
	const opRef = `gw-p-unsent`;
	await createOwnedSessionFixture(db, authority, {
		sessionId: "sdk-held",
		repo: join(home, "workspace"),
		originKey: CHANNEL,
		epoch: 0,
	});
	db.transaction((tx) =>
		db.inbox.bindTurn(tx, { messageId: "m-1", originKey: CHANNEL, epoch: 0, opRef, sessionId: "sdk-held" }),
	);
	expect(findSilentOrigins(db, NOW)[0]).toMatchObject({ blockingOpRef: opRef, holdReason: "send_not_attempted" });
	db.transaction((tx) => db.inbox.markSendAttempted(tx, opRef));
	expect(findSilentOrigins(db, NOW)[0]).toMatchObject({ blockingOpRef: opRef, holdReason: "operation_state_unknown" });
	// The host refused the prompt as busy: a durable refusal in the broker operation journal.
	db.transaction((tx) => {
		const scope = JSON.stringify(["sdk-held", opRef]);
		db.bindings.beginBrokerOperation(tx, authority, {
			operation: "turn.prompt",
			scope,
			operationRef: opRef,
			requestJson: "{}",
		});
		db.bindings.completeBrokerOperation(
			tx,
			authority,
			"turn.prompt",
			scope,
			opRef,
			JSON.stringify(BROKER_PROMPT_BUSY_REFUSAL),
		);
	});
	expect(findSilentOrigins(db, NOW)[0]).toMatchObject({ blockingOpRef: opRef, holdReason: "session_busy_wait" });
	db.transaction((tx) => db.inbox.turnAccept(tx, opRef));
	expect(findSilentOrigins(db, NOW)[0]).toMatchObject({ blockingOpRef: opRef, holdReason: "turn_running" });

	enqueue(db, { messageId: "q-1", originKey: "discord/channel/queued", ageMs: 20 * MINUTE });
	expect(findSilentOrigins(db, NOW).find((origin) => origin.originKey === "discord/channel/queued")).toMatchObject({
		blockingOpRef: null,
		holdReason: "dispatch_pending",
	});
});

test("the gate stays quiet under the threshold, for bot authors, muted threads, and answered origins (#688)", async () => {
	const db = await open();
	// Under the 15-minute threshold.
	enqueue(db, { messageId: "fresh", ageMs: ORIGIN_SILENT_MS - MINUTE });
	// A bot author never counts.
	enqueue(db, { messageId: "bot", originKey: "discord/channel/bots", ageMs: 60 * MINUTE, authorIsBot: true });
	// A muted ([SHUTUP]) thread never counts.
	const thread = "discord/thread/muted";
	enqueue(db, { messageId: "muted", originKey: thread, ageMs: 60 * MINUTE });
	db.transaction((tx) => db.inbox.muteThread(tx, thread));
	// A Slack reply under the persona's message stays on the channel session; its thread mute still applies.
	const slackChannel = "slack/channel/C1";
	enqueue(db, {
		messageId: "slack-reply",
		originKey: slackChannel,
		ageMs: 60 * MINUTE,
		replyTo: "C1:1700000000.000100",
	});
	db.transaction((tx) => db.inbox.muteThread(tx, "slack/thread/C1:1700000000.000100/parent=C1"));
	// An origin answered after the message arrived is not silent...
	enqueue(db, { messageId: "answered", originKey: "discord/channel/answered", ageMs: 60 * MINUTE });
	deliver(db, "gw-d-answer", 30 * MINUTE, "discord/channel/answered");
	// ...but a hold notice is not an answer.
	enqueue(db, { messageId: "noticed", originKey: "discord/channel/noticed", ageMs: 60 * MINUTE });
	deliver(db, "gw-h-notice", 30 * MINUTE, "discord/channel/noticed");
	expect(findSilentOrigins(db, NOW).map((origin) => origin.originKey)).toEqual(["discord/channel/noticed"]);
});

const OWNER = { platform: "discord", kind: "dm", conversationId: "owner-dm", peerId: "owner" } as const;

test("today's incident queues one owner DM per origin per incident, and one more when it clears (#688)", async () => {
	const db = await open();
	enqueue(db, { messageId: "m-1", ageMs: 16 * MINUTE });
	const opRef = holdTurn(db, "m-1");
	const alerter = new OriginSilentAlerter(db, () => OWNER, { log: () => {} });

	const first = alerter.sweep(NOW);
	expect(first).toHaveLength(1);
	expect(first[0]).toMatchObject({ kind: "silent", originKey: CHANNEL });
	expect(first[0]!.deliveryId).toMatch(/^gw-s-[0-9a-f]{32}$/);
	// The DM is in the delivery ledger, committed with the incident.
	const queued = db.outbox.get(first[0]!.deliveryId);
	expect(queued?.state).toBe("pending");
	expect(queued?.originKey).toBe("discord/dm/owner-dm/peer=owner");
	const text = first[0]!.payload!.text;
	expect(text).toContain(CHANNEL);
	expect(text).toContain("reason=operation_state_unknown");
	expect(text).toContain(`op=${opRef}`);
	// Later sweeps of the same incident, and a reopened store, stay quiet.
	enqueue(db, { messageId: "m-2", ageMs: 20 * MINUTE });
	expect(alerter.sweep(NOW + MINUTE)).toEqual([]);
	expect(new OriginSilentAlerter(db, () => OWNER).sweep(NOW + 2 * MINUTE)).toEqual([]);
	// The owner DM itself is not an answer on the silent origin.
	db.transaction((tx) => db.outbox.confirm(tx, first[0]!.deliveryId));
	expect(findSilentOrigins(db, NOW + 2 * MINUTE)).toHaveLength(1);

	// The hold clears and the origin answers.
	db.transaction((tx) => db.inbox.turnComplete(tx, opRef));
	deliver(db, "gw-d-reply", 0);
	const cleared = alerter.sweep(NOW + 3 * MINUTE);
	expect(cleared).toHaveLength(1);
	expect(cleared[0]).toMatchObject({ kind: "cleared", originKey: CHANNEL });
	expect(cleared[0]!.deliveryId).not.toBe(first[0]!.deliveryId);
	expect(db.outbox.get(cleared[0]!.deliveryId)?.state).toBe("pending");
	expect(alerter.sweep(NOW + 4 * MINUTE)).toEqual([]);
	expect(new RuntimeCycleProjector(db, { queueDepth: 0 }).project(new Date(NOW + 4 * MINUTE)).gates).not.toContain(
		"origin_silent",
	);
});

test("an incident transition never commits without its owner DM: a failed ledger write retries both (#688)", async () => {
	const db = await open();
	enqueue(db, { messageId: "m-1", ageMs: 16 * MINUTE });
	const alerter = new OriginSilentAlerter(db, () => OWNER, { log: () => {} });
	const create = db.outbox.create.bind(db.outbox);
	let fail = true;
	db.outbox.create = (...args) => {
		if (fail) throw new Error("disk full");
		return create(...args);
	};
	expect(() => alerter.sweep(NOW)).toThrow("disk full");
	fail = false;
	// Rolled back with the failed DM: the next sweep (or a restarted gateway) alerts again.
	const retried = new OriginSilentAlerter(db, () => OWNER, { log: () => {} }).sweep(NOW + MINUTE);
	expect(retried.map((transition) => transition.kind)).toEqual(["silent"]);
	expect(db.outbox.get(retried[0]!.deliveryId)?.state).toBe("pending");

	// Same for the clear: the incident stays until the cleared DM is queued.
	deliver(db, "gw-d-reply", 0);
	fail = true;
	expect(() => alerter.sweep(NOW + 2 * MINUTE)).toThrow("disk full");
	fail = false;
	expect(alerter.sweep(NOW + 3 * MINUTE).map((transition) => transition.kind)).toEqual(["cleared"]);
});

for (const raw of [
	"{not json",
	"null",
	"42",
	'"text"',
	"[]",
	'{"messageId":7,"alertedAt":"2026-10-26T00:00:00.000Z"}',
]) {
	test(`a corrupt incident row ${raw} is kept as evidence, not read as a valid incident, and the clear is still sent (#688)`, async () => {
		const db = await open();
		const logs: string[] = [];
		db.core.metaSet(`origin_silent:${CHANNEL}`, raw);
		const cleared = new OriginSilentAlerter(db, () => OWNER, { log: (line) => logs.push(line) }).sweep(NOW);
		expect(cleared.map((transition) => transition.kind)).toEqual(["cleared"]);
		expect(db.outbox.get(cleared[0]!.deliveryId)?.state).toBe("pending");
		expect(db.core.metaGet(`origin_silent:${CHANNEL}`)).toBeUndefined();
		expect(db.core.metaGet(`origin_silent_corrupt:${CHANNEL}`)).toBe(raw);
		expect(logs.some((line) => line.startsWith("origin_silent_incident_corrupt"))).toBe(true);
	});
}

test("without an owner target the incident is recorded for ops cycle and no DM is queued (#688)", async () => {
	const db = await open();
	enqueue(db, { messageId: "m-1", ageMs: 16 * MINUTE });
	const [transition] = new OriginSilentAlerter(db, () => undefined, { log: () => {} }).sweep(NOW);
	expect(transition).toMatchObject({ kind: "silent", payload: undefined });
	expect(db.outbox.get(transition!.deliveryId)).toBeUndefined();
});
