import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { SessionBusyStalledError, type SessionSendInput } from "../src/orchestrator/session-port";
import { Store } from "../src/store";
import { ScriptedSessionPort } from "./session-port.fake";

let home = "";
let database: Store | undefined;
const abandoned: PersonaSessionManager[] = [];

afterEach(async () => {
	abandoned.length = 0;
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

async function until(check: () => boolean): Promise<void> {
	for (let i = 0; i < 400 && !check(); i++) await new Promise((resolve) => setTimeout(resolve, 5));
	expect(check()).toBe(true);
}

const never = <T>() => new Promise<T>(() => {});

type Stage = "start_turn" | "attach" | "set_model" | "set_service_tier" | "send";

/** A port that can wedge any pre-send await forever: a process killed there never resumes it. */
class CrashablePort extends ScriptedSessionPort {
	hang: Stage | undefined;
	reached = new Set<Stage>();

	constructor() {
		super({ onSend: (input, port) => port.complete(input.opRef, `answer to ${input.opRef}`) });
	}

	#gate<T>(stage: Stage, work: () => Promise<T>): Promise<T> {
		this.reached.add(stage);
		return this.hang === stage ? never<T>() : work();
	}

	override attachTail(input: Parameters<ScriptedSessionPort["attachTail"]>[0]) {
		return this.#gate("attach", () => super.attachTail(input));
	}

	override setModel(input: Parameters<ScriptedSessionPort["setModel"]>[0]) {
		return this.#gate("set_model", () => super.setModel(input));
	}

	override setServiceTier(input: Parameters<ScriptedSessionPort["setServiceTier"]>[0]) {
		return this.#gate("set_service_tier", () => super.setServiceTier(input));
	}

	override send(input: SessionSendInput) {
		if (this.hang === "send") {
			this.reached.add("send");
			// Crash after the prompt left: the broker accepted it, the process never learned.
			this.seedAcceptedSend(input);
			return never<never>();
		}
		return super.send(input);
	}
}

async function fixture(name: string) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-send-intent-"));
	database = await Store.open(join(home, "gateway.db"));
	const db = database;
	const port = new CrashablePort();
	const originKey = `loopback/loopback/${name}`;
	const repo = join(home, "workspace");
	const binding = await port.bind({ originKey, epoch: 0, repo });
	expect(db.transaction((tx) => db.bindings.bindAtEpoch(tx, originKey, binding.sessionId, 0))).toBe(true);
	expect(
		db.transaction((tx) =>
			db.inbox.enqueue(tx, {
				messageId: "m-1",
				originKey,
				originRefJson: JSON.stringify({ platform: "loopback", kind: "loopback", conversationId: name }),
				body: "answer me exactly once",
				receivedAt: new Date().toISOString(),
			}),
		),
	).toBe(true);
	const logs: string[] = [];
	let startTurnHang = false;
	const startManager = () =>
		new PersonaSessionManager({
			database: db,
			port,
			instanceId: "send-intent",
			repo,
			log: (line: string) => logs.push(line),
			onTurnStart: async ({ trigger }) => {
				if (startTurnHang) {
					port.reached.add("start_turn");
					await never();
				}
				return { text: trigger.body, effectiveModel: { preset: "fixture" }, effectiveServiceTier: "priority" };
			},
		});
	return {
		db,
		port,
		originKey,
		binding,
		logs,
		startManager,
		hangStartTurn: (value: boolean) => {
			startTurnHang = value;
		},
	};
}

for (const stage of ["start_turn", "attach", "set_model", "set_service_tier"] as const) {
	test(`a crash at ${stage} between bindTurn and send is answered exactly once after restart, with no operator action (#685)`, async () => {
		const f = await fixture(`crash-${stage}`);
		if (stage === "start_turn") f.hangStartTurn(true);
		else f.port.hang = stage;
		const crashed = f.startManager();
		abandoned.push(crashed);
		void crashed.recover();
		await until(() => f.port.reached.has(stage));
		const bound = f.db.inbox.nonterminalTurns(f.originKey);
		expect(bound).toHaveLength(1);
		expect(bound[0]?.state).toBe("bound");
		expect(f.db.inbox.sendAttempted(bound[0]!.opRef)).toBe(false);
		expect(f.port.sendAttempts).toHaveLength(0);

		// The process died there. A fresh process recovers from the database alone.
		f.hangStartTurn(false);
		f.port.hang = undefined;
		const restarted = f.startManager();
		try {
			await restarted.recover();
			await until(() => f.port.sendAttempts.length === 1);
			const sent = f.port.sendAttempts[0]!;
			expect(sent.text).toContain("answer me exactly once");
			expect(sent.opRef).not.toBe(bound[0]!.opRef);
			await until(() => f.db.inbox.turnRow(sent.opRef)?.turn_state === "done");
			expect(
				f.logs.some(
					(line) =>
						line.startsWith("recovery_requeue_unsent") &&
						line.includes(`opRef=${bound[0]!.opRef}`) &&
						line.includes("reason=send_not_attempted"),
				),
			).toBe(true);
			await restarted.tick();
			expect(f.port.sendAttempts).toHaveLength(1);
		} finally {
			await restarted.stop();
		}
	});
}

test("a crash after the send intent is stamped goes through reconcile and is never blindly resent (#685)", async () => {
	const f = await fixture("crash-after-marker");
	f.port.hang = "send";
	const crashed = f.startManager();
	abandoned.push(crashed);
	void crashed.recover();
	await until(() => f.port.reached.has("send"));
	const [bound] = f.db.inbox.nonterminalTurns(f.originKey);
	expect(f.db.inbox.sendAttempted(bound!.opRef)).toBe(true);

	f.port.hang = undefined;
	const restarted = f.startManager();
	try {
		await restarted.recover();
		await restarted.tick();
		// The accepted op is reconciled under its original op-ref, never resubmitted.
		expect(f.port.sendAttempts.map((input) => input.opRef)).toEqual([bound!.opRef]);
		expect(f.logs.some((line) => line.startsWith("recovery_requeue_unsent"))).toBe(false);
		f.port.complete(bound!.opRef, "late answer");
		await restarted.reconcile(f.originKey);
		await until(() => f.db.inbox.turnRow(bound!.opRef)?.turn_state === "done");
		expect(f.port.sendAttempts).toHaveLength(1);
	} finally {
		await restarted.stop();
	}
});

test("a durable busy refusal on a stamped turn is positive non-admission evidence: recovery requeues it (#685)", async () => {
	const f = await fixture("busy-refused");
	const db = f.db;
	const [first] = (() => {
		const opRef = "gw-p-busy-refused";
		db.transaction((tx) => {
			db.inbox.bindTurn(tx, {
				messageId: "m-1",
				originKey: f.originKey,
				epoch: 0,
				opRef,
				sessionId: f.binding.sessionId,
			});
			db.inbox.markSendAttempted(tx, opRef);
		});
		return [opRef];
	})();
	const refused = new Set([first]);
	Object.assign(f.port, {
		promptRefusal: ({ opRef }: { opRef: string }) => (refused.has(opRef) ? "busy" : undefined),
	});
	const manager = f.startManager();
	try {
		await manager.recover();
		await until(() => f.port.sendAttempts.length === 1);
		expect(f.port.sendAttempts[0]?.opRef).not.toBe(first);
		expect(
			f.logs.some((line) => line.includes("recovery_requeue_unsent") && line.includes("reason=prompt_busy_refused")),
		).toBe(true);
	} finally {
		await manager.stop();
	}
});

test("a host that stays busy without progress during pre-send wait is ended and the turn answered on a resumed host (#687)", async () => {
	const f = await fixture("busy-stalled");
	let stalled = 0;
	const refused = new Set<string>();
	const port = f.port;
	const send = port.send.bind(port);
	Object.assign(port, {
		promptRefusal: ({ opRef }: { opRef: string }) => (refused.has(opRef) ? "busy" : undefined),
		send: async (input: SessionSendInput) => {
			if (stalled++ === 0) {
				port.sendAttempts.push(input);
				refused.add(input.opRef);
				throw new SessionBusyStalledError(input.sessionId, input.opRef, 300_000, "activity=active@1000");
			}
			return await send(input);
		},
	});
	const manager = f.startManager();
	try {
		await manager.recover();
		await until(() => port.sendAttempts.length === 2);
		const [wedged, retried] = port.sendAttempts;
		expect(retried!.opRef).not.toBe(wedged!.opRef);
		expect(retried!.text).toContain("answer me exactly once");
		// The wedged host was ended and the stored binding resumed, not replaced.
		expect(port.closes.map((close) => close.sessionId)).toEqual([f.binding.sessionId]);
		expect(port.resumes.map((resume) => resume.sessionId)).toEqual([f.binding.sessionId]);
		expect(f.logs.some((line) => line.startsWith("session_busy_stalled") && line.includes("silentMs=300000"))).toBe(
			true,
		);
		expect(
			f.logs.some((line) => line.startsWith("session_busy_host_terminated") && line.includes("outcome=terminated")),
		).toBe(true);
		await until(() => f.db.inbox.turnRow(retried!.opRef)?.turn_state === "done");
		expect(f.db.inbox.turnRow(wedged!.opRef)).toBeUndefined();
	} finally {
		await manager.stop();
	}
});

test("upgrading to schema 39 stamps legacy bound/accepted turns so recovery reconciles, never replays, them (#685)", async () => {
	const f = await fixture("legacy-upgrade");
	const db = f.db;
	const boundOpRef = "gw-p-legacy-bound";
	const acceptedOpRef = "gw-p-legacy-accepted";
	const legacyOrigin = `${f.originKey}-accepted`;
	expect(db.transaction((tx) => db.bindings.bindAtEpoch(tx, legacyOrigin, f.binding.sessionId, 0))).toBe(true);
	db.transaction((tx) => {
		db.inbox.enqueue(tx, {
			messageId: "m-accepted",
			originKey: legacyOrigin,
			originRefJson: JSON.stringify({ platform: "loopback", kind: "loopback", conversationId: "legacy-accepted" }),
			body: "accepted before the upgrade",
			receivedAt: "2026-10-26T06:00:00.000Z",
		});
		db.inbox.bindTurn(tx, {
			messageId: "m-1",
			originKey: f.originKey,
			epoch: 0,
			opRef: boundOpRef,
			sessionId: f.binding.sessionId,
		});
		db.inbox.bindTurn(tx, {
			messageId: "m-accepted",
			originKey: legacyOrigin,
			epoch: 0,
			opRef: acceptedOpRef,
			sessionId: f.binding.sessionId,
			dispatchedAt: "2026-10-26T06:00:05.000Z",
		});
		db.inbox.turnAccept(tx, acceptedOpRef);
	});
	const path = join(home, "gateway.db");
	db.close();
	database = undefined;
	// Restore the schema-38 shape: no send columns, and a bound row whose dispatch floor is unknown.
	const raw = new Database(path);
	raw.exec(`UPDATE inbound_messages SET dispatched_at = NULL WHERE turn_op_ref = '${boundOpRef}'`);
	raw.exec(
		"ALTER TABLE inbound_messages DROP COLUMN send_attempted_at; ALTER TABLE inbound_messages DROP COLUMN send_context_json",
	);
	raw.exec("DELETE FROM schema_migrations WHERE version >= 39");
	raw.close();

	database = await Store.open(path);
	const upgraded = database;
	expect(upgraded.schemaVersion).toBe(39);
	const received = upgraded.inbox.turnRow(boundOpRef)!.received_at;
	expect(upgraded.inbox.sendAttemptedAt(boundOpRef)).toBe(received);
	expect(upgraded.inbox.sendAttemptedAt(acceptedOpRef)).toBe("2026-10-26T06:00:05.000Z");
	// Idempotent: a rerun keeps the existing stamps.
	upgraded.close();
	const rerun = new Database(path);
	rerun.exec("DELETE FROM schema_migrations WHERE version >= 39");
	rerun.close();
	database = await Store.open(path);
	expect(database.inbox.sendAttemptedAt(boundOpRef)).toBe(received);

	// Unknown legacy history is held for reconcile, never requeued or resent.
	const logs: string[] = [];
	const manager = new PersonaSessionManager({
		database,
		port: f.port,
		instanceId: "send-intent",
		repo: join(home, "workspace"),
		log: (line: string) => logs.push(line),
		onTurnStart: ({ trigger }) => ({ text: trigger.body }),
	});
	try {
		await manager.recover();
		await manager.tick();
		expect(f.port.sendAttempts).toHaveLength(0);
		expect(database.inbox.turnRow(boundOpRef)?.turn_state).toBe("bound");
		expect(logs.some((line) => line.startsWith("recovery_requeue_unsent"))).toBe(false);
		expect(logs.some((line) => line.includes(`opRef=${boundOpRef}`) && line.includes("operation_state_unknown"))).toBe(
			true,
		);
	} finally {
		await manager.stop();
	}
});

for (const outcome of ["refused", "thrown", "terminating"] as const) {
	test(`a busy-stalled host whose termination is ${outcome} keeps the trigger pending for a later retry, never resending (#687)`, async () => {
		const f = await fixture(`busy-${outcome}`);
		const port = f.port;
		const refused = new Set<string>();
		Object.assign(port, {
			promptRefusal: ({ opRef }: { opRef: string }) => (refused.has(opRef) ? "busy" : undefined),
			send: async (input: SessionSendInput) => {
				port.sendAttempts.push(input);
				refused.add(input.opRef);
				throw new SessionBusyStalledError(input.sessionId, input.opRef, 300_000, "activity=active@1000");
			},
			terminateHost: async () => {
				if (outcome === "thrown") throw new Error("inspect timed out");
				return outcome === "refused"
					? { outcome: "refused", reason: "process_inspection_failed:eperm" }
					: { outcome: "terminating", pid: 4242 };
			},
		});
		const manager = f.startManager();
		try {
			await manager.recover();
			await until(() => f.logs.some((line) => line.startsWith("session_busy_stalled")));
			// The host was not proven gone: no immediate redispatch onto it.
			expect(port.sendAttempts).toHaveLength(1);
			expect(port.resumes).toHaveLength(0);
			const wedged = port.sendAttempts[0]!.opRef;
			expect(f.db.inbox.turnRow(wedged)).toBeUndefined();
			expect(f.db.inbox.pendingOldest(f.originKey)?.message_id).toBe("m-1");
			expect(
				f.logs.some((line) =>
					outcome === "thrown"
						? line.startsWith("session_busy_host_terminate_failed")
						: line.startsWith("session_busy_host_terminated") && line.includes(`outcome=${outcome}`),
				),
			).toBe(true);
		} finally {
			await manager.stop();
		}
	});
}

test("an unreadable busy-refusal record keeps a stamped turn held and is logged, not silently treated as no refusal (#685)", async () => {
	const f = await fixture("refusal-unreadable");
	const opRef = "gw-p-refusal-unreadable";
	f.db.transaction((tx) => {
		f.db.inbox.bindTurn(tx, {
			messageId: "m-1",
			originKey: f.originKey,
			epoch: 0,
			opRef,
			sessionId: f.binding.sessionId,
		});
		f.db.inbox.markSendAttempted(tx, opRef);
	});
	Object.assign(f.port, {
		promptRefusal: () => {
			throw new Error("journal read failed");
		},
	});
	const manager = f.startManager();
	try {
		await manager.recover();
		expect(f.port.sendAttempts).toHaveLength(0);
		expect(f.db.inbox.turnRow(opRef)?.turn_state).toBe("bound");
		expect(
			f.logs.some((line) => line.startsWith("send_unadmitted_probe_failed") && line.includes(`opRef=${opRef}`)),
		).toBe(true);
	} finally {
		await manager.stop();
	}
});
