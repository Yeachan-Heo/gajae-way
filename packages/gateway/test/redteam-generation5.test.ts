import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GjcCliError } from "@gajaeway/subsession";
import type { GatewayConfig } from "../src/config";
import { PersonaSessionManager, type PersonaSessionManagerOptions } from "../src/orchestrator/persona-session";
import type { SessionSendInput, SessionSteerInput } from "../src/orchestrator/session-port";
import { type GatewayServer, messageEditId, renderMessageEdit, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, ScriptedSessionPort, steerRefused } from "./session-port.fake";

const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "generation-5" } as const;
const ORIGIN_KEY = "loopback/loopback/generation-5";
const SERVER_ORIGIN = { platform: "discord", kind: "dm", conversationId: "generation-5", peerId: "owner" } as const;
const SERVER_ORIGIN_KEY = "discord/dm/generation-5/peer=owner";
const SERVER_ENGAGEMENT = { mentioned: false, group: false, authorId: "owner", authorName: "owner" };

class FixtureClock {
	#ms = Date.parse("2026-09-04T00:00:00.000Z");

	now = (): number => this.#ms;

	iso(): string {
		return new Date(this.#ms).toISOString();
	}

	advance(ms = 1): void {
		this.#ms += ms;
	}
}

type ScheduledTimer = { readonly work: () => void; readonly delayMs: number };

type DirectFixture = {
	readonly home: string;
	readonly clock: FixtureClock;
	readonly database: GatewayDatabase;
	readonly port: ScriptedSessionPort;
	readonly manager: PersonaSessionManager;
	readonly logs: string[];
	readonly discarded: string[];
	close(): Promise<void>;
};

type DirectFixtureOptions = {
	readonly port?: ScriptedSessionPort;
	readonly clock?: FixtureClock;
	readonly setTimeout?: (work: () => void, delayMs: number) => unknown;
	readonly clearTimeout?: (timer: unknown) => void;
	readonly onSteerAccepted?: PersonaSessionManagerOptions["onSteerAccepted"];
	readonly onHeldSteerAccepted?: PersonaSessionManagerOptions["onHeldSteerAccepted"];
};

async function eventually(predicate: () => boolean, message: string, attempts = 400): Promise<void> {
	for (let attempt = 0; attempt < attempts; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

function required<T>(value: T | undefined, message: string): T {
	if (value === undefined) throw new Error(message);
	return value;
}

function tornSteer(): GjcCliError {
	return new GjcCliError("gjc sdk turn.steer exited 1", 1, "socket reset");
}

function sessionUnavailable(): Error & { readonly code: "session_unavailable" } {
	return Object.assign(new Error("session_unavailable"), { code: "session_unavailable" as const });
}

function timerSeam(timers: ScheduledTimer[]) {
	return {
		setTimeout: (work: () => void, delayMs: number) => {
			const timer = { work, delayMs };
			timers.push(timer);
			return timer;
		},
		clearTimeout: () => {},
	};
}

async function directFixture(options: DirectFixtureOptions = {}): Promise<DirectFixture> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-generation5-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const clock = options.clock ?? new FixtureClock();
	const port =
		options.port ?? new ScriptedSessionPort({ onBind: (input) => `${input.originKey}-session-${input.epoch}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const discarded: string[] = [];
	const manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "generation5-redteam",
		repo: join(home, "workspace"),
		now: clock.now,
		...(options.setTimeout ? { setTimeout: options.setTimeout } : {}),
		...(options.clearTimeout ? { clearTimeout: options.clearTimeout } : {}),
		...(options.onSteerAccepted ? { onSteerAccepted: options.onSteerAccepted } : {}),
		...(options.onHeldSteerAccepted ? { onHeldSteerAccepted: options.onHeldSteerAccepted } : {}),
		onTurnStart: ({ trigger }) => ({ text: trigger.body }),
		onInboundDiscard: (messageIds) => {
			discarded.push(...messageIds);
		},
		log: (line) => logs.push(line),
	});
	return {
		home,
		clock,
		database,
		port,
		manager,
		logs,
		discarded,
		async close() {
			await manager.stop();
			database.close();
			await rm(home, { recursive: true, force: true });
		},
	};
}

function enqueue(fixture: DirectFixture, messageId: string, body: string): void {
	const receivedAt = fixture.clock.iso();
	fixture.clock.advance();
	expect(
		fixture.database.inboundEnqueue({
			messageId,
			originKey: ORIGIN_KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body,
			receivedAt,
		}),
	).toBe(true);
}

class TornReplayPort extends ScriptedSessionPort {
	torn = true;
	readonly steerAttempts: SessionSteerInput[] = [];

	async steer(input: SessionSteerInput): Promise<void> {
		this.steerAttempts.push(input);
		if (this.torn) throw tornSteer();
		await super.steer(input);
	}
}

class TornThenRefusedPort extends ScriptedSessionPort {
	mode: "torn" | "refused" | "accepted" = "torn";
	readonly steerAttempts: SessionSteerInput[] = [];

	async steer(input: SessionSteerInput): Promise<void> {
		this.steerAttempts.push(input);
		if (this.mode === "torn") throw tornSteer();
		if (this.mode === "refused") throw steerRefused("turn already ended");
		await super.steer(input);
	}
}

class AcceptedLivenessPort extends ScriptedSessionPort {
	staleOpRef = "";
	staleSessionId = "";
	live: boolean | undefined = undefined;

	constructor() {
		super({ onBind: (input) => `${input.originKey}-accepted-liveness-${input.epoch}` });
	}

	async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
		if (input.opRef === this.staleOpRef) throw sessionUnavailable();
		return await super.status(input);
	}

	async liveness(input: { sessionId: string; repo: string }) {
		if (input.sessionId === this.staleSessionId) return { live: this.live, disowned: false };
		return { live: true, disowned: false };
	}
}

class BoundLivenessPort extends ScriptedSessionPort {
	failedOpRef = "";
	failedSessionId = "";
	#failFirstSend = true;

	constructor() {
		super({ onBind: (input) => `${input.originKey}-bound-liveness-${input.epoch}` });
	}

	async send(input: SessionSendInput) {
		if (this.#failFirstSend) {
			this.#failFirstSend = false;
			this.failedOpRef = input.opRef;
			this.failedSessionId = input.sessionId;
			this.sendAttempts.push(input);
			throw new Error("torn send after inboundBindTurn");
		}
		return await super.send(input);
	}

	async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
		if (input.opRef === this.failedOpRef) throw sessionUnavailable();
		return await super.status(input);
	}

	async liveness(input: { sessionId: string; repo: string }) {
		if (input.sessionId === this.failedSessionId) return { live: undefined, disowned: false };
		return { live: true, disowned: false };
	}
}

class SendUnavailablePort extends ScriptedSessionPort {
	remainingFailures = 0;

	constructor() {
		super({ onBind: (input) => `${input.originKey}-send-unavailable-${input.epoch}` });
	}

	async send(input: SessionSendInput) {
		if (this.remainingFailures > 0) {
			this.remainingFailures--;
			this.sendAttempts.push(input);
			throw sessionUnavailable();
		}
		return await super.send(input);
	}
}

test("G1: a held steer blocks later pending steers until its clientRef resolves", async () => {
	const port = new TornReplayPort({ onBind: (input) => `${input.originKey}-session-${input.epoch}` });
	const fixture = await directFixture({ port });
	try {
		enqueue(fixture, "trigger", "first request");
		await fixture.manager.notifyInbound(ORIGIN_KEY);
		const send = required(port.sends[0], "initial turn was not sent");

		enqueue(fixture, "steer-a", "first follow-up");
		await fixture.manager.notifyInbound(ORIGIN_KEY);
		expect(port.steerAttempts).toHaveLength(3);
		const firstClientRef = required(port.steerAttempts[0], "first steer attempt missing").clientRef;
		expect(fixture.database.inboundSteersHeld(send.opRef).map((row) => row.message_id)).toEqual(["steer-a"]);

		enqueue(fixture, "steer-b", "second follow-up");
		await fixture.manager.notifyInbound(ORIGIN_KEY);
		expect(port.steerAttempts).toHaveLength(6);
		expect(port.steerAttempts.map((attempt) => attempt.clientRef)).toEqual(Array(6).fill(firstClientRef));
		expect(fixture.database.inboundSteersHeld(send.opRef).map((row) => row.message_id)).toEqual(["steer-a"]);
		expect(fixture.database.inboundPendingOldest(ORIGIN_KEY)).toMatchObject({
			message_id: "steer-b",
			body: "second follow-up",
			turn_state: null,
		});
		expect(port.sends).toHaveLength(1);

		port.torn = false;
		await fixture.manager.notifyInbound(ORIGIN_KEY);
		expect(port.steerAttempts.slice(-2).map((attempt) => attempt.clientRef)).toEqual([
			firstClientRef,
			required(port.steerAttempts.at(-1), "second steer acceptance missing").clientRef,
		]);
		expect(port.steers.map((steer) => steer.text.split("\n").at(-1))).toEqual(["first follow-up", "second follow-up"]);
		expect(fixture.database.inboundSteersHeld(send.opRef)).toEqual([]);
		expect(fixture.database.inboundTurnRows(send.opRef).map((row) => [row.message_id, row.turn_state])).toEqual([
			["trigger", "accepted"],
			["steer-a", "done"],
			["steer-b", "done"],
		]);
	} finally {
		await fixture.close();
	}
});

test("G2: /new retains a held steer on its retired turn instead of discarding or re-dispatching it", async () => {
	const port = new TornReplayPort({ onBind: (input) => `${input.originKey}-session-${input.epoch}` });
	const fixture = await directFixture({ port });
	try {
		enqueue(fixture, "trigger", "old request");
		await fixture.manager.notifyInbound(ORIGIN_KEY);
		const oldTurn = required(port.sends[0], "old turn was not sent");
		enqueue(fixture, "held", "maybe already received");
		await fixture.manager.notifyInbound(ORIGIN_KEY);
		expect(fixture.database.inboundSteersHeld(oldTurn.opRef).map((row) => row.message_id)).toEqual(["held"]);

		const resetFloor = fixture.clock.iso();
		fixture.clock.advance();
		await fixture.manager.reset(ORIGIN_KEY, JSON.stringify(ORIGIN), resetFloor);
		expect(fixture.discarded).not.toContain("held");
		expect(fixture.database.inboundSteersHeld(oldTurn.opRef).map((row) => row.message_id)).toEqual(["held"]);
		expect(port.sends).toHaveLength(1);

		port.complete(oldTurn.opRef, "retired answer");
		await fixture.manager.tick(ORIGIN_KEY);
		await fixture.manager.tick(ORIGIN_KEY);
		await eventually(
			() => fixture.database.inboundTurnRow(oldTurn.opRef)?.turn_state === "done",
			"retired turn did not close",
		);
		expect(fixture.database.inboundSteersHeld(oldTurn.opRef).map((row) => row.message_id)).toEqual(["held"]);
		expect(fixture.database.inboundPendingOldest(ORIGIN_KEY)).toBeUndefined();
		expect(port.sends).toHaveLength(1);

		port.torn = false;
		await fixture.manager.tick(ORIGIN_KEY);
		// RED-TEAM FINDING: once the retired turn is terminal, tick() never enters #resolveStaleHolds(), so a healthy replay is stranded until unrelated later dispatch work exists.
		expect(fixture.database.inboundSteersHeld(oldTurn.opRef)).toEqual([]);
		expect(fixture.database.inboundTurnRows(oldTurn.opRef).map((row) => [row.message_id, row.turn_state])).toEqual([
			["trigger", "done"],
			["held", "done"],
		]);
		expect(port.sends).toHaveLength(1);
	} finally {
		await fixture.close();
	}
});

test("G3: a terminal-time definitive steer refusal releases the original body for exactly one next turn", async () => {
	const port = new TornThenRefusedPort({ onBind: (input) => `${input.originKey}-session-${input.epoch}` });
	const fixture = await directFixture({ port });
	try {
		enqueue(fixture, "trigger", "old request");
		await fixture.manager.notifyInbound(ORIGIN_KEY);
		const oldTurn = required(port.sends[0], "old turn was not sent");
		enqueue(fixture, "held", "preserve this body");
		await fixture.manager.notifyInbound(ORIGIN_KEY);
		expect(fixture.database.inboundSteersHeld(oldTurn.opRef).map((row) => row.message_id)).toEqual(["held"]);

		port.mode = "refused";
		port.complete(oldTurn.opRef, "old answer");
		await eventually(() => port.sends.length === 2, "refused held steer was not sent as the next turn");
		const replacement = required(port.sends[1], "replacement turn missing");
		expect(replacement.text).toBe("preserve this body");
		expect(replacement.opRef).not.toBe(oldTurn.opRef);
		expect(port.sendAttempts).toHaveLength(2);
		expect(fixture.database.inboundSteersHeld(oldTurn.opRef)).toEqual([]);
		expect(
			fixture.database.inboundTurnRows(replacement.opRef).map((row) => [row.message_id, row.turn_role, row.body]),
		).toEqual([["held", "trigger", "preserve this body"]]);
		await fixture.manager.tick(ORIGIN_KEY);
		expect(port.sends).toHaveLength(2);
	} finally {
		await fixture.close();
	}
});

test("G4: finalizing a terminal-time held-steer acceptance is exactly once", async () => {
	const port = new TornReplayPort({ onBind: (input) => `${input.originKey}-session-${input.epoch}` });
	const observed: string[] = [];
	const fixture = await directFixture({
		port,
		onSteerAccepted: ({ messageId, opRef }) => {
			observed.push(`${messageId}:${opRef}`);
		},
	});
	try {
		enqueue(fixture, "trigger", "old request");
		await fixture.manager.notifyInbound(ORIGIN_KEY);
		const oldTurn = required(port.sends[0], "old turn was not sent");
		enqueue(fixture, "held", "accepted after replay");
		await fixture.manager.notifyInbound(ORIGIN_KEY);
		expect(fixture.database.inboundSteersHeld(oldTurn.opRef).map((row) => row.message_id)).toEqual(["held"]);

		port.torn = false;
		port.complete(oldTurn.opRef, "old answer");
		await eventually(
			() => fixture.database.inboundTurnRows(oldTurn.opRef).every((row) => row.turn_state === "done"),
			"terminal-time acceptance did not finalize",
		);
		await fixture.manager.tick(ORIGIN_KEY);
		await fixture.manager.tick(ORIGIN_KEY);
		await fixture.manager.tick(ORIGIN_KEY);

		expect(observed).toEqual([`held:${oldTurn.opRef}`]);
		expect(
			fixture.logs.filter((line) => line.startsWith("steer_delivered") && line.includes("messageId=held")),
		).toHaveLength(1);
		expect(port.steerAttempts).toHaveLength(4);
		expect(
			fixture.database.inboundTurnRows(oldTurn.opRef).map((row) => [row.message_id, row.state, row.turn_state]),
		).toEqual([
			["trigger", "done", "done"],
			["held", "done", "done"],
		]);
	} finally {
		await fixture.close();
	}
});

test("G5: accepted turns need positive death evidence, while bound turns release on an unknown liveness result", async () => {
	const acceptedPort = new AcceptedLivenessPort();
	const accepted = await directFixture({ port: acceptedPort });
	try {
		enqueue(accepted, "accepted", "accepted request");
		await accepted.manager.notifyInbound(ORIGIN_KEY);
		const first = required(acceptedPort.sends[0], "accepted turn was not sent");
		acceptedPort.staleOpRef = first.opRef;
		acceptedPort.staleSessionId = first.sessionId;

		await accepted.manager.tick(ORIGIN_KEY);
		expect(accepted.manager.state(ORIGIN_KEY)).toBe("turn-running");
		expect(accepted.database.inboundTurnRow(first.opRef)).toMatchObject({ turn_state: "accepted" });
		expect(acceptedPort.sends).toHaveLength(1);

		acceptedPort.live = false;
		await accepted.manager.tick(ORIGIN_KEY);
		await eventually(() => acceptedPort.sends.length === 2, "positive dead evidence did not release the accepted turn");
		const released = required(acceptedPort.sends[1], "released replacement missing");
		expect(released.opRef).not.toBe(first.opRef);
		await accepted.manager.tick(ORIGIN_KEY);
		expect(acceptedPort.sends).toHaveLength(2);
	} finally {
		await accepted.close();
	}

	const boundPort = new BoundLivenessPort();
	const bound = await directFixture({ port: boundPort });
	try {
		enqueue(bound, "bound", "bound request");
		await bound.manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => boundPort.sends.length === 1, "bound turn did not release after unknown liveness");
		const released = required(boundPort.sends[0], "bound replacement missing");
		expect(boundPort.failedOpRef).not.toBe("");
		expect(released.opRef).not.toBe(boundPort.failedOpRef);
		expect(boundPort.sendAttempts).toHaveLength(2);
		expect(bound.database.inboundTurnRow(boundPort.failedOpRef)).toBeUndefined();
		await bound.manager.tick(ORIGIN_KEY);
		expect(boundPort.sends).toHaveLength(1);
	} finally {
		await bound.close();
	}
});

test("G6: session-unavailable send failures are bounded per actor and reset after a successful send", async () => {
	const timers: ScheduledTimer[] = [];
	const port = new SendUnavailablePort();
	port.remainingFailures = 3;
	const fixture = await directFixture({ port, ...timerSeam(timers) });
	try {
		enqueue(fixture, "first", "first request");
		await fixture.manager.notifyInbound(ORIGIN_KEY);
		const goneAttempts = fixture.logs
			.filter((line) => line.startsWith("persona_send_session_gone"))
			.map((line) => /attempt=(\d+)/.exec(line)?.[1]);
		expect(goneAttempts).toEqual(["1", "2", "3"]);
		expect(
			fixture.logs.filter(
				(line) => line.startsWith("persona_send_unrecoverable") && line.includes("reason=session_unavailable"),
			),
		).toHaveLength(1);
		const retry = required(
			timers.find((timer) => timer.delayMs === 8_000),
			"8s send retry was not armed",
		);

		retry.work();
		await eventually(() => port.sends.length === 1, "armed retry did not send after recovery");
		const recovered = required(port.sends[0], "recovered send missing");
		port.complete(recovered.opRef, "first answer");
		await eventually(() => fixture.database.inboundPendingCount(ORIGIN_KEY) === 0, "recovered turn did not complete");

		port.remainingFailures = 1;
		enqueue(fixture, "second", "second request");
		await fixture.manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => port.sends.length === 2, "second request did not recover after one unavailable send");
		expect(
			fixture.logs
				.filter((line) => line.startsWith("persona_send_session_gone"))
				.map((line) => /attempt=(\d+)/.exec(line)?.[1]),
		).toEqual(["1", "2", "3", "1"]);
		expect(
			fixture.logs.filter(
				(line) => line.startsWith("persona_send_unrecoverable") && line.includes("reason=session_unavailable"),
			),
		).toHaveLength(1);
	} finally {
		await fixture.close();
	}
});

test("G7: migration 19 requeues a settled-bound trigger and ride-along member together in arrival order", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-generation5-migrate-"));
	const path = join(home, "gateway.db");
	let upgraded: GatewayDatabase | undefined;
	try {
		(await GatewayDatabase.open(path)).close();
		const raw = new Database(path);
		// Remove v22 completely before replaying historical DDL; missing objects are fixture errors.
		for (const table of ["inbound_messages", "lane_jobs", "work_attempt_runtime", "monitor_events", "authored_outputs"])
			for (const action of ["update", "delete"]) raw.exec(`DROP TRIGGER ${table}_quarantine_${action}`);
		for (const table of ["broker_owned_bindings", "broker_cutovers", "broker_quarantine", "broker_retired_sessions"])
			for (const action of ["update", "delete"]) raw.exec(`DROP TRIGGER ${table}_immutable_${action}`);
		for (const table of [
			"broker_authority",
			"broker_owned_bindings",
			"broker_tail_cursors",
			"broker_cutovers",
			"broker_quarantine",
			"broker_retired_sessions",
		])
			raw.exec(`DROP TABLE ${table}`);
		raw.exec(`
DROP TABLE work_attempt_runtime;
DROP TABLE inbound_messages;
CREATE TABLE inbound_messages (message_id TEXT PRIMARY KEY, origin_key TEXT NOT NULL, origin_ref_json TEXT NOT NULL, body TEXT NOT NULL, engagement_json TEXT, state TEXT NOT NULL CHECK(state IN ('pending','processing','done')), received_at TEXT NOT NULL);
CREATE INDEX inbound_messages_claim ON inbound_messages (origin_key, state, received_at);
ALTER TABLE inbound_messages ADD COLUMN batch_key TEXT;
ALTER TABLE inbound_messages ADD COLUMN batch_role TEXT CHECK(batch_role IS NULL OR batch_role IN ('trigger', 'member', 'steer'));
ALTER TABLE inbound_messages ADD COLUMN batch_epoch INTEGER;
ALTER TABLE inbound_messages ADD COLUMN batch_state TEXT CHECK(batch_state IS NULL OR batch_state IN ('settled', 'accepted', 'done'));
ALTER TABLE inbound_messages ADD COLUMN attributed_op_ref TEXT;
ALTER TABLE inbound_messages ADD COLUMN accepted_at TEXT;
ALTER TABLE inbound_messages ADD COLUMN bound_session_id TEXT;
ALTER TABLE inbound_messages ADD COLUMN dispatched_at TEXT;
ALTER TABLE inbound_messages ADD COLUMN terminal_delivery_id TEXT;
DELETE FROM schema_migrations WHERE version > 18;
INSERT INTO inbound_messages (message_id, origin_key, origin_ref_json, body, engagement_json, state, received_at, batch_key, batch_role, batch_epoch, batch_state, attributed_op_ref, accepted_at, bound_session_id, dispatched_at, terminal_delivery_id) VALUES
 ('ride-trigger', 'migration/ride', '{}', 'first', NULL, 'pending', '2026-09-04T00:00:00.000Z', 'batch', 'trigger', 7, 'settled', 'gw-p-ride', NULL, 'session-ride', '2026-09-04T00:00:00.001Z', NULL),
 ('ride-member', 'migration/ride', '{}', 'second', NULL, 'pending', '2026-09-04T00:00:00.002Z', 'batch', 'member', 7, 'settled', 'gw-p-ride', NULL, 'session-ride', '2026-09-04T00:00:00.001Z', NULL);
`);
		raw.close();

		upgraded = await GatewayDatabase.open(path);
		expect(upgraded.schemaVersion).toBe(22);
		expect(upgraded.inboundTurnRows("gw-p-ride").map((row) => [row.message_id, row.turn_role, row.turn_state])).toEqual(
			[
				["ride-trigger", "trigger", "bound"],
				["ride-member", "steer", "bound"],
			],
		);
		expect(upgraded.inboundTurnRequeue("gw-p-ride")).toBe(1);
		expect(upgraded.inboundNonterminalTurns("migration/ride")).toEqual([]);
		expect(upgraded.inboundPendingOldest("migration/ride")).toMatchObject({
			message_id: "ride-trigger",
			body: "first",
		});
		expect(upgraded.inboundPendingCount("migration/ride")).toBe(2);

		const verification = new Database(path, { readonly: true });
		const rows = verification
			.query<
				{
					message_id: string;
					state: string;
					turn_role: string | null;
					turn_epoch: number | null;
					turn_state: string | null;
					turn_op_ref: string | null;
					bound_session_id: string | null;
				},
				[]
			>(
				"SELECT message_id, state, turn_role, turn_epoch, turn_state, turn_op_ref, bound_session_id FROM inbound_messages WHERE origin_key = 'migration/ride' ORDER BY received_at, rowid",
			)
			.all();
		verification.close();
		expect(rows).toEqual([
			{
				message_id: "ride-trigger",
				state: "pending",
				turn_role: null,
				turn_epoch: null,
				turn_state: null,
				turn_op_ref: null,
				bound_session_id: null,
			},
			{
				message_id: "ride-member",
				state: "pending",
				turn_role: null,
				turn_epoch: null,
				turn_state: null,
				turn_op_ref: null,
				bound_session_id: null,
			},
		]);
	} finally {
		upgraded?.close();
		await rm(home, { recursive: true, force: true });
	}
});

type SocketFrame = {
	readonly type?: string;
	readonly id?: string;
	readonly result?: unknown;
	readonly error?: unknown;
};

type SocketClient = {
	readonly frames: SocketFrame[];
	send(value: unknown): void;
	close(): void;
	response(id: string): SocketFrame | undefined;
};

async function connect(socketPath: string): Promise<SocketClient> {
	const frames: SocketFrame[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line) as SocketFrame);
			},
		},
	});
	return {
		frames,
		send: (value) => socket.write(`${JSON.stringify(value)}\n`),
		close: () => socket.end(),
		response: (id) => frames.find((frame) => frame.type === "response" && frame.id === id),
	};
}

type ServerFixture = {
	readonly home: string;
	readonly database: GatewayDatabase;
	readonly port: TornReplayPort;
	readonly client: SocketClient;
	readonly server: GatewayServer;
	close(): Promise<void>;
};

async function serverFixture(port: TornReplayPort): Promise<ServerFixture> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-generation5-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const server = await startUnixServer({ config, database, sessionPort: port, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await eventually(() => client.frames.length > 0, "server negotiation did not complete");
	return {
		home,
		database,
		port,
		client,
		server,
		async close() {
			client.close();
			await server.stop();
			await rm(home, { recursive: true, force: true });
		},
	};
}

function chatSend(client: SocketClient, id: string, messageId: string, text: string): void {
	client.send({
		v: "0.1",
		type: "request",
		id,
		verb: "chat.send",
		params: { origin: SERVER_ORIGIN, messageId, text, engagement: SERVER_ENGAGEMENT },
	});
}

function chatEdit(client: SocketClient, id: string, messageId: string, text: string): void {
	client.send({
		v: "0.1",
		type: "request",
		id,
		verb: "chat.edit",
		params: { origin: SERVER_ORIGIN, messageId, text, engagement: SERVER_ENGAGEMENT },
	});
}

test("G8: a server-side edit of a held steer remains a pointer row and cannot duplicate the old turn", async () => {
	const port = new TornReplayPort({ onBind: (input) => `${input.originKey}-session-${input.epoch}` });
	const fixture = await serverFixture(port);
	try {
		chatSend(fixture.client, "trigger", "m-1", "original request");
		await eventually(() => port.sends.length === 1, "original request was not sent");
		const oldTurn = required(port.sends[0], "old turn missing");

		chatSend(fixture.client, "held", "m-2", "follow-up before edit");
		await eventually(() => port.steerAttempts.length === 3, "follow-up was not held after torn retries");
		expect(fixture.database.inboundSteersHeld(oldTurn.opRef).map((row) => row.message_id)).toEqual(["m-2"]);

		chatEdit(fixture.client, "edit", "m-2", "follow-up after edit");
		await eventually(() => fixture.client.response("edit") !== undefined, "edit did not receive a server response");
		await eventually(
			() => port.steerAttempts.length === 6,
			"edit admission did not replay the pre-existing hold first",
		);
		const editId = messageEditId("m-2", "follow-up after edit");
		expect(fixture.database.inboundSteersHeld(oldTurn.opRef).map((row) => row.message_id)).toEqual(["m-2"]);
		expect(fixture.database.inboundPendingOldest(SERVER_ORIGIN_KEY)).toMatchObject({
			message_id: editId,
			body: renderMessageEdit("m-2", "follow-up after edit"),
			turn_state: null,
		});
		expect(port.sends).toHaveLength(1);

		port.torn = false;
		port.complete(oldTurn.opRef, "old answer");
		await eventually(() => port.sends.length === 2, "pointer edit was not dispatched after the old hold resolved");
		const pointerTurn = required(port.sends[1], "pointer turn missing");
		expect(pointerTurn.opRef).not.toBe(oldTurn.opRef);
		expect(pointerTurn.text).toContain("[MESSAGE POINTER: m-2]");
		expect(pointerTurn.text).toContain("follow-up after edit");
		expect(
			fixture.database.inboundTurnRows(oldTurn.opRef).map((row) => [row.message_id, row.turn_role, row.turn_state]),
		).toEqual([
			["m-1", "trigger", "done"],
			["m-2", "steer", "done"],
		]);
		expect(
			fixture.database.inboundTurnRows(pointerTurn.opRef).map((row) => [row.message_id, row.turn_role, row.turn_state]),
		).toEqual([[editId, "trigger", "accepted"]]);
		expect(port.sendAttempts).toHaveLength(2);

		port.complete(pointerTurn.opRef, "pointer answer");
		await eventually(
			() => fixture.database.inboundPendingCount(SERVER_ORIGIN_KEY) === 0,
			"pointer turn did not complete",
		);
		expect(port.sends).toHaveLength(2);
	} finally {
		await fixture.close();
	}
});
