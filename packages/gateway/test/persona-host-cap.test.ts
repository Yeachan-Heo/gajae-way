import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type PersonaBindHoldInput, PersonaSessionManager } from "../src/orchestrator/persona-session";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

let home = "";
let database: Store | undefined;
let manager: PersonaSessionManager | undefined;

afterEach(async () => {
	await manager?.stop();
	database?.close();
	manager = undefined;
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

function origin(name: string) {
	return {
		key: `loopback/loopback/${name}`,
		ref: JSON.stringify({ platform: "loopback", kind: "loopback", conversationId: name }),
	};
}

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 400; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

async function harness(options: { maxLiveHosts: number; hostIdleRetireMs?: number; now?: () => number }) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-persona-host-cap-"));
	database = await Store.open(join(home, "gateway.db"));
	const port = attachTestBrokerOwnership(database, new ScriptedSessionPort(), join(home, "agent"));
	const logs: string[] = [];
	const holds: PersonaBindHoldInput[] = [];
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		submissionRetryDelayMs: 1,
		maxLiveHosts: options.maxLiveHosts,
		...(options.hostIdleRetireMs === undefined ? {} : { hostIdleRetireMs: options.hostIdleRetireMs }),
		...(options.now ? { now: options.now } : {}),
		log: (line) => {
			logs.push(line);
		},
		onBindHold: (input) => {
			holds.push(input);
		},
	});
	const active = manager;
	const db = database;
	/** Enqueues one message for an origin and waits until the actor either sent it or holds it. */
	const send = async (name: string, body: string) => {
		const { key, ref } = origin(name);
		expect(
			db.transaction((tx) =>
				db.inbox.enqueue(tx, {
					messageId: `${name}:${body}`,
					originKey: key,
					originRefJson: ref,
					body,
					source: "platform",
				}),
			),
		).toBe(true);
		await active.notifyInbound(key);
	};
	const sent = (body: string) => port.sends.find((input) => input.text === body);
	const sessionOf = (body: string): string => {
		const input = sent(body);
		if (!input) throw new Error(`turn ${body} was never sent`);
		return input.sessionId;
	};
	/** Completes a sent turn and waits for its origin to settle back to idle. */
	const complete = async (name: string, body: string) => {
		const { key } = origin(name);
		const input = sent(body);
		if (!input) throw new Error(`turn ${body} was never sent`);
		port.complete(input.opRef, `answer to ${body}`);
		await active.tick(key);
		await eventually(
			() => db.inbox.turnRow(input.opRef)?.turn_state === "done" && active.state(key) === "idle",
			`turn ${body} did not settle`,
		);
	};
	return { port, logs, holds, send, sent, sessionOf, complete, manager: active, database: db };
}

test("at the cap the least-recently-active idle host is ended before a new origin binds, and its session resumes later", async () => {
	const h = await harness({ maxLiveHosts: 2 });
	await h.send("a", "a1");
	await h.complete("a", "a1");
	await h.send("b", "b1");
	await h.complete("b", "b1");
	expect(h.manager.liveHostCount()).toBe(2);
	const sessionA = h.sessionOf("a1");
	const sessionB = h.sessionOf("b1");

	await h.send("c", "c1");
	await eventually(() => h.sent("c1") !== undefined, "c1 was not dispatched after an idle host was retired");
	expect(h.port.closes.map((close) => close.sessionId)).toEqual([sessionA]);
	expect(h.manager.liveHostCount()).toBe(2);
	expect(
		h.logs.some(
			(line) =>
				line.startsWith(`persona_host_retired origin=${origin("a").key}`) && line.includes("reason=host_capacity"),
		),
	).toBe(true);
	await h.complete("c", "c1");

	// Origin a returns: b is now the oldest idle host, and a's own session resumes with its history.
	await h.send("a", "a2");
	await eventually(() => h.sent("a2") !== undefined, "a2 was not dispatched");
	expect(h.port.closes.map((close) => close.sessionId)).toEqual([sessionA, sessionB]);
	expect(h.port.resumes.map((resume) => resume.sessionId)).toEqual([sessionA]);
	expect(h.sessionOf("a2")).toBe(sessionA);
	expect(h.holds).toHaveLength(0);
});

test("with every host busy a new origin is held, not spawned, and dispatches when a host frees", async () => {
	const h = await harness({ maxLiveHosts: 2 });
	await h.send("a", "a1");
	await h.send("b", "b1");
	await eventually(() => h.sent("a1") !== undefined && h.sent("b1") !== undefined, "a1/b1 were not sent");

	await h.send("c", "c1");
	await Bun.sleep(20);
	expect(h.sent("c1")).toBeUndefined();
	expect(h.port.binds.map((bind) => bind.originKey)).not.toContain(origin("c").key);
	expect(h.port.closes).toHaveLength(0);
	expect(h.manager.liveHostCount()).toBe(2);
	expect(h.manager.admissionHold(origin("c").key)).toBe("host_capacity");
	expect(h.holds).toHaveLength(1);
	expect(h.holds[0]).toMatchObject({ originKey: origin("c").key, reason: "host_capacity" });
	expect(h.holds[0]?.notice).toContain("2/2 busy");
	expect(
		h.logs.filter((line) => line.includes("reason=host_capacity") && line.startsWith("persona_bind_hold")),
	).toHaveLength(1);

	await h.complete("a", "a1");
	await eventually(() => h.sent("c1") !== undefined, "c1 was not dispatched once a host freed");
	// a's host was idle and least recently active, so it made room for c.
	expect(h.port.closes.map((close) => close.sessionId)).toEqual([h.sessionOf("a1")]);
	expect(h.manager.liveHostCount()).toBe(2);
	expect(h.manager.admissionHold(origin("c").key)).toBeUndefined();
});

test("idle hosts past the retire window are ended by the sweep and busy ones are left alone", async () => {
	let clock = 1_000_000;
	const h = await harness({ maxLiveHosts: 8, hostIdleRetireMs: 60_000, now: () => clock });
	await h.send("a", "a1");
	await h.complete("a", "a1");
	await h.send("b", "b1");
	await eventually(() => h.sent("b1") !== undefined, "b1 was not sent");
	expect(await h.manager.sweepIdleHosts()).toBe(0);
	clock += 60_000;
	expect(await h.manager.sweepIdleHosts()).toBe(1);
	expect(h.port.closes.map((close) => close.sessionId)).toEqual([h.sessionOf("a1")]);
	expect(h.manager.liveHostCount()).toBe(1);
	expect(h.logs.some((line) => line.includes("reason=host_idle"))).toBe(true);
	expect(await h.manager.sweepIdleHosts()).toBe(0);
});
