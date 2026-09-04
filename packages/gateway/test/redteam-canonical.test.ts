import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { StatusReport } from "@gajaeway/subsession";
import type { GatewayConfig } from "../src/config";
import {
	PersonaSessionManager,
	type PersonaTurnLifecycle,
	type PersonaTurnStartInput,
} from "../src/orchestrator/persona-session";
import type { SessionBindInput, SessionSteerInput } from "../src/orchestrator/session-port";
import type { TailAttachInput } from "../src/orchestrator/tail-runner";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { ScriptedSessionPort, steerRefused } from "./session-port.fake";

const DIRECT_ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "canonical-redteam" } as const;
const DIRECT_ORIGIN_KEY = "loopback/loopback/canonical-redteam";

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

type ScheduledTimer = { readonly work: () => void; readonly delayMs: number };

type LifecycleCapture = {
	readonly terminals: Array<{ readonly trigger: string; readonly text: string }>;
	readonly frames: Array<{ readonly trigger: string; readonly text: string }>;
};

type DirectFixtureOptions = {
	readonly port?: ScriptedSessionPort;
	readonly instanceId?: string;
	readonly now?: () => number;
	readonly setTimeout?: (work: () => void, delayMs: number) => unknown;
	readonly clearTimeout?: (timer: unknown) => void;
	readonly lifecycle?: (input: PersonaTurnStartInput, capture: LifecycleCapture) => PersonaTurnLifecycle;
};

type DirectFixture = LifecycleCapture & {
	readonly home: string;
	readonly database: GatewayDatabase;
	readonly port: ScriptedSessionPort;
	readonly manager: PersonaSessionManager;
	readonly logs: string[];
	readonly discarded: string[];
	close(): Promise<void>;
};

async function directFixture(options: DirectFixtureOptions = {}): Promise<DirectFixture> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-redteam-canonical-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = options.port ?? new ScriptedSessionPort();
	const terminals: Array<{ trigger: string; text: string }> = [];
	const frames: Array<{ trigger: string; text: string }> = [];
	const logs: string[] = [];
	const discarded: string[] = [];
	const capture = { terminals, frames };
	const manager = new PersonaSessionManager({
		database,
		port,
		instanceId: options.instanceId ?? "canonical-redteam",
		repo: join(home, "workspace"),
		...(options.now ? { now: options.now } : {}),
		...(options.setTimeout ? { setTimeout: options.setTimeout } : {}),
		...(options.clearTimeout ? { clearTimeout: options.clearTimeout } : {}),
		onTurnStart: (input) =>
			options.lifecycle?.(input, capture) ?? {
				text: input.trigger.body,
				onFrame: ({ frame }) => {
					if (frame.assistantText) frames.push({ trigger: input.turn.triggerMessageId, text: frame.assistantText });
				},
				onTerminal: ({ text }) => {
					terminals.push({ trigger: input.turn.triggerMessageId, text });
				},
			},
		onInboundDiscard: (messageIds) => {
			discarded.push(...messageIds);
		},
		log: (line) => {
			logs.push(line);
		},
	});
	return {
		home,
		database,
		port,
		manager,
		terminals,
		frames,
		logs,
		discarded,
		async close() {
			await manager.stop();
			database.close();
			await rm(home, { recursive: true, force: true });
		},
	};
}

function enqueue(
	fixture: Pick<DirectFixture, "database">,
	messageId: string,
	body: string,
	receivedAt = new Date().toISOString(),
): void {
	expect(
		fixture.database.inboundEnqueue({
			messageId,
			originKey: DIRECT_ORIGIN_KEY,
			originRefJson: JSON.stringify(DIRECT_ORIGIN),
			body,
			receivedAt,
		}),
	).toBe(true);
}

async function admit(fixture: DirectFixture, messageId: string, body: string, receivedAt?: string): Promise<void> {
	enqueue(fixture, messageId, body, receivedAt);
	await fixture.manager.notifyInbound(DIRECT_ORIGIN_KEY);
}

function steerBodies(port: ScriptedSessionPort): string[] {
	return port.steers.map((steer) => steer.text.split("\n").at(-1) ?? "");
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

function sessionUnavailable(): Error & { readonly code: "session_unavailable" } {
	return Object.assign(new Error("session_unavailable"), { code: "session_unavailable" as const });
}

class TrackingTailPort extends ScriptedSessionPort {
	readonly tailInputs: TailAttachInput[] = [];

	async attachTail(input: TailAttachInput) {
		this.tailInputs.push(input);
		return await super.attachTail(input);
	}
}

class FailSteerPort extends ScriptedSessionPort {
	readonly failedAttempts: ReadonlySet<number>;
	steerAttempts = 0;

	constructor(failedAttempts: ReadonlySet<number>) {
		super({ onBind: (input) => `session-${input.epoch}` });
		this.failedAttempts = failedAttempts;
	}

	async steer(input: SessionSteerInput): Promise<void> {
		this.steerAttempts++;
		if (this.failedAttempts.has(this.steerAttempts)) throw steerRefused(`scripted steer refusal ${this.steerAttempts}`);
		await super.steer(input);
	}
}

class FailFirstBindPort extends ScriptedSessionPort {
	bindAttempts = 0;

	async bind(input: SessionBindInput) {
		this.bindAttempts++;
		if (this.bindAttempts === 1) throw new Error("scripted initial bind failure");
		return await super.bind(input);
	}
}

class UnavailableReplacementBindPort extends ScriptedSessionPort {
	bindAttempts = 0;
	steerAttempts = 0;

	constructor() {
		super({ onBind: (input) => `session-${input.epoch}` });
	}

	async bind(input: SessionBindInput) {
		this.bindAttempts++;
		if (input.epoch > 0) throw sessionUnavailable();
		return await super.bind(input);
	}

	async steer(input: SessionSteerInput): Promise<void> {
		this.steerAttempts++;
		throw steerRefused("scripted steer refusal");
	}
}

/**
 * Models the runtime's own clock: `startedAt` and transcript row stamps come
 * from the SAME host, which may be offset from the gateway's clock. The rows
 * the port "has" are (hostStamp, text); fetchAssistantSince returns the newest
 * row stamped at/after the floor, with the same 2s slack as the real port.
 */
class HostClockPort extends ScriptedSessionPort {
	forceStartedAt: number | undefined;
	readonly rows: Array<{ at: number; text: string }> = [];
	readonly transcriptFloors: number[] = [];

	async status(input: { sessionId: string; repo: string; opRef: string }): Promise<StatusReport> {
		const report = await super.status(input);
		if (report.status.status !== "terminal_ok" || this.forceStartedAt === undefined) return report;
		return { ...report, status: { ...report.status, startedAt: this.forceStartedAt } };
	}

	async fetchAssistantSince(input: { sessionId: string; repo: string; notBeforeMs: number }) {
		this.transcriptFloors.push(input.notBeforeMs);
		const last = this.rows.at(-1);
		if (!last || last.at + 2_000 < input.notBeforeMs) return undefined;
		return { text: last.text, pages: 1, complete: true };
	}

	async fetchLastAssistant(): Promise<never> {
		throw new Error("host-clock fixture exercises turn-floored transcript recovery");
	}
}

class DisownedStatusPort extends ScriptedSessionPort {
	readonly disownedOps = new Set<string>();

	constructor() {
		super({ onBind: (input) => `session-${input.epoch}` });
	}

	async status(input: { sessionId: string; repo: string; opRef: string }): Promise<StatusReport> {
		if (this.disownedOps.has(input.opRef)) throw sessionUnavailable();
		return await super.status(input);
	}
}

test("C1a: an un-attributed replay 1,999ms before dispatch passes the documented 2s skew tolerance", async () => {
	const fixture = await directFixture();
	try {
		await admit(fixture, "skew-edge", "current prompt");
		const send = required(fixture.port.sends[0], "turn was not sent");
		const dispatchFloor = Date.parse(
			required(fixture.database.inboundTurnDispatchedAt(send.opRef), "dispatch floor missing"),
		);
		fixture.port.emitReplayedTranscriptRow(send.sessionId, "within skew replay", dispatchFloor - 1_999);
		await eventually(
			() => fixture.frames.some((frame) => frame.text === "within skew replay"),
			"inside-skew row was not admitted to the tail lifecycle",
		);
		expect(fixture.logs.some((line) => line.startsWith("tail_frame_pre_turn"))).toBe(false);
		fixture.port.completeWithoutAnswerFrame(send.opRef, "current durable answer");
		await eventually(() => fixture.terminals.length === 1, "terminal answer was not delivered");
		expect(fixture.terminals).toEqual([{ trigger: "skew-edge", text: "current durable answer" }]);
	} finally {
		await fixture.close();
	}
});

test("C1b: tail frames from a foreign session or an explicit foreign operation cannot cross into the current turn", async () => {
	const fixture = await directFixture();
	try {
		await admit(fixture, "session-op-fence", "current prompt");
		const send = required(fixture.port.sends[0], "turn was not sent");
		const dispatchFloor = Date.parse(
			required(fixture.database.inboundTurnDispatchedAt(send.opRef), "dispatch floor missing"),
		);
		fixture.port.emitReplayedTranscriptRow("foreign-session", "foreign-session output", dispatchFloor + 1, send.opRef);
		fixture.port.emitReplayedTranscriptRow(send.sessionId, "foreign-op output", dispatchFloor + 1, "gw-p-foreign-op");
		await Bun.sleep(25);
		expect(fixture.frames).toEqual([]);
		fixture.port.completeWithoutAnswerFrame(send.opRef, "current answer");
		await eventually(() => fixture.terminals.length === 1, "current turn did not complete");
		expect(fixture.terminals).toEqual([{ trigger: "session-op-fence", text: "current answer" }]);
	} finally {
		await fixture.close();
	}
});

test("C1c: three cursorless replays of prior answers cannot move a reply onto the next trigger", async () => {
	const fixture = await directFixture();
	try {
		const answers = ["answer one", "answer two", "answer three"];
		let previousAnswerAt: number | undefined;
		for (let index = 0; index < answers.length; index++) {
			if (previousAnswerAt !== undefined) await Bun.sleep(2_050);
			const messageId = `three-turn-${index + 1}`;
			await admit(fixture, messageId, `prompt ${index + 1}`);
			await eventually(() => fixture.port.sends.length === index + 1, `turn ${index + 1} was not sent`);
			const send = required(fixture.port.sends[index], `turn ${index + 1} send missing`);
			if (previousAnswerAt !== undefined)
				fixture.port.emitReplayedTranscriptRow(send.sessionId, answers[index - 1]!, previousAnswerAt);
			fixture.port.completeWithoutAnswerFrame(send.opRef, answers[index]!);
			await eventually(() => fixture.terminals.length === index + 1, `turn ${index + 1} did not deliver`);
			previousAnswerAt = Date.now();
		}
		expect(fixture.logs.filter((line) => line.startsWith("tail_frame_pre_turn")).length).toBeGreaterThanOrEqual(2);
		expect(fixture.terminals).toEqual([
			{ trigger: "three-turn-1", text: "answer one" },
			{ trigger: "three-turn-2", text: "answer two" },
			{ trigger: "three-turn-3", text: "answer three" },
		]);
	} finally {
		await fixture.close();
	}
}, 15_000);

test("C1d: startedAt is the floor on the host's own clock - a host clock BEHIND the gateway never reopens the previous turn's row, and a host clock AHEAD never hides the real answer", async () => {
	const port = new HostClockPort();
	const fixture = await directFixture({ port });
	try {
		for (const skewMs of [-3_000, +3_000]) {
			const tag = skewMs < 0 ? "behind" : "ahead";
			await admit(fixture, `prior-${tag}`, "prior prompt");
			const first = required(port.sends.at(-1), "prior turn was not sent");
			// The host stamps everything on ITS clock (gateway now + skew): the
			// op's startedAt and the answer row it writes 1ms later.
			const priorHostAt = Date.now() + skewMs;
			port.forceStartedAt = priorHostAt - 1;
			port.rows.push({ at: priorHostAt, text: `previous durable body ${tag}` });
			port.completeWithoutAnswerFrame(first.opRef, `previous durable body ${tag}`);
			await eventually(() => fixture.terminals.length === (skewMs < 0 ? 1 : 3), "prior turn did not complete");

			await admit(fixture, `current-${tag}`, "current prompt");
			const second = required(port.sends.at(-1), "current turn was not sent");
			// The runtime starts the op 5s (host clock) after the prior answer and
			// writes the current answer 1ms after starting.
			const startedAt = priorHostAt + 5_000;
			port.forceStartedAt = startedAt;
			port.rows.push({ at: startedAt + 1, text: `current durable body ${tag}` });
			port.completeWithoutAnswerFrame(second.opRef, `current durable body ${tag}`);
			await eventually(() => fixture.terminals.length === (skewMs < 0 ? 2 : 4), "current turn did not complete");
			expect(port.transcriptFloors.at(-1)).toBe(startedAt);
		}
		expect(fixture.terminals).toEqual([
			{ trigger: "prior-behind", text: "previous durable body behind" },
			{ trigger: "current-behind", text: "current durable body behind" },
			{ trigger: "prior-ahead", text: "previous durable body ahead" },
			{ trigger: "current-ahead", text: "current durable body ahead" },
		]);
		expect(fixture.logs.some((line) => line.includes("reason=no_assistant_text_for_terminal"))).toBe(false);
	} finally {
		await fixture.close();
	}
});

test("C2a: a 20-message burst has one send, nineteen ordered steers, and no unbound row", async () => {
	const fixture = await directFixture();
	try {
		const now = Date.now();
		for (let index = 0; index < 20; index++) {
			enqueue(fixture, `burst-${index}`, `message ${index}`, new Date(now + index).toISOString());
			await fixture.manager.notifyInbound(DIRECT_ORIGIN_KEY);
		}
		await eventually(
			() => fixture.port.sends.length === 1 && fixture.port.steers.length === 19,
			"burst did not drain to one send and nineteen steers",
		);
		expect(fixture.port.sends[0]?.text).toBe("message 0");
		expect(steerBodies(fixture.port)).toEqual(Array.from({ length: 19 }, (_, index) => `message ${index + 1}`));
		expect(fixture.database.inboundPendingOldest(DIRECT_ORIGIN_KEY)).toBeUndefined();
		fixture.port.complete(required(fixture.port.sends[0], "burst trigger missing").opRef, "burst answer");
		await eventually(
			() => fixture.database.inboundPendingCount(DIRECT_ORIGIN_KEY) === 0,
			"burst turn did not complete",
		);
		expect(fixture.terminals).toEqual([{ trigger: "burst-0", text: "burst answer" }]);
	} finally {
		await fixture.close();
	}
});

test("C2b: a message accepted after terminal observation but before dispatchNext is sent once, not lost or doubled", async () => {
	let markTerminalObserved!: () => void;
	const terminalObserved = new Promise<void>((resolve) => {
		markTerminalObserved = resolve;
	});
	let releaseTerminal!: () => void;
	const terminalGate = new Promise<void>((resolve) => {
		releaseTerminal = resolve;
	});
	const fixture = await directFixture({
		lifecycle: (input, capture) => ({
			text: input.trigger.body,
			onTerminal: async ({ text }) => {
				capture.terminals.push({ trigger: input.turn.triggerMessageId, text });
				markTerminalObserved();
				await terminalGate;
			},
		}),
	});
	try {
		await admit(fixture, "terminal-window-first", "first");
		const first = required(fixture.port.sends[0], "first turn was not sent");
		fixture.port.complete(first.opRef, "first answer");
		await terminalObserved;
		enqueue(fixture, "terminal-window-next", "next");
		const queuedNotice = fixture.manager.notifyInbound(DIRECT_ORIGIN_KEY);
		releaseTerminal();
		await queuedNotice;
		await eventually(() => fixture.port.sends.length === 2, "window message was not sent after the terminal");
		expect(fixture.port.sends.map((send) => send.text)).toEqual(["first", "next"]);
		expect(fixture.port.sendAttempts).toHaveLength(2);
		const second = required(fixture.port.sends[1], "next turn missing");
		fixture.port.complete(second.opRef, "next answer");
		await eventually(
			() => fixture.database.inboundPendingCount(DIRECT_ORIGIN_KEY) === 0,
			"window sequence did not drain",
		);
	} finally {
		await fixture.close();
	}
});

test("C2c: a bind failure preserves the row and dispatches it exactly once through the 2s retry seam", async () => {
	const timers: ScheduledTimer[] = [];
	const seam = timerSeam(timers);
	const port = new FailFirstBindPort();
	const fixture = await directFixture({ port, ...seam });
	try {
		await admit(fixture, "bind-retry", "retry me");
		// A failed bind waits for the retry seam; admissions never re-bind synchronously (this was the red-team C2c finding).
		expect(port.sends).toEqual([]);
		expect(fixture.database.inboundPendingOldest(DIRECT_ORIGIN_KEY)).toMatchObject({ message_id: "bind-retry" });
		const retry = required(
			timers.find((timer) => timer.delayMs === 2_000),
			"bind retry was not scheduled at 2s",
		);
		retry.work();
		await eventually(() => port.sends.length === 1, "retry did not dispatch the preserved row");
		expect(port.bindAttempts).toBe(2);
		expect(port.sends[0]?.text).toBe("retry me");
		fixture.port.complete(required(port.sends[0], "retried send missing").opRef, "retried answer");
		await eventually(
			() => fixture.database.inboundPendingCount(DIRECT_ORIGIN_KEY) === 0,
			"retried turn did not complete",
		);
	} finally {
		await fixture.close();
	}
});

test("C2d: /new discards only an undispatched row; the already-running turn still closes its trigger but its output is fenced as stale (the user asked for a fresh start)", async () => {
	const port = new TrackingTailPort();
	const fixture = await directFixture({ port });
	try {
		await admit(fixture, "reset-running", "running prompt");
		const running = required(port.sends[0], "running turn was not sent");
		enqueue(fixture, "reset-pending", "discard me");
		await fixture.manager.reset(
			DIRECT_ORIGIN_KEY,
			JSON.stringify(DIRECT_ORIGIN),
			new Date(Date.now() + 1_000).toISOString(),
		);
		expect(fixture.discarded).toEqual(["reset-pending"]);
		expect(fixture.database.inboundPendingOldest(DIRECT_ORIGIN_KEY)).toBeUndefined();
		await eventually(
			() => port.tailInputs.length >= 2,
			"retired running turn did not reattach for terminal observation",
		);
		port.complete(running.opRef, "running answer");
		await eventually(
			() => fixture.database.inboundTurnRow(running.opRef)?.turn_state === "done",
			"retired running turn did not reconcile terminal state",
		);
		expect(port.sends).toHaveLength(1);
		// Unlike a steer-failure rebind (answerWanted), /new is the user's explicit
		// discard: the retired turn's output is stale and is not delivered, while
		// its row is still closed so nothing is stranded.
		expect(fixture.terminals).toEqual([]);
		expect(fixture.database.inboundPendingCount(DIRECT_ORIGIN_KEY)).toBe(0);
	} finally {
		await fixture.close();
	}
});

test("C3a: a refused steer preserves the old trigger's answer and sends the refused message once on a new session", async () => {
	const port = new FailSteerPort(new Set([1]));
	const fixture = await directFixture({ port });
	try {
		await admit(fixture, "steer-old", "old prompt");
		const old = required(port.sends[0], "old turn was not sent");
		await admit(fixture, "steer-refused", "refused prompt");
		await eventually(() => port.sends.length === 2, "refused steer was not sent on the replacement session");
		const replacement = required(port.sends[1], "replacement turn missing");
		expect(replacement.sessionId).not.toBe(old.sessionId);
		expect(replacement.text).toBe("refused prompt");
		port.complete(old.opRef, "old answer");
		port.complete(replacement.opRef, "replacement answer");
		await eventually(() => fixture.terminals.length === 2, "both old and replacement answers were not delivered");
		expect(fixture.terminals).toEqual([
			{ trigger: "steer-old", text: "old answer" },
			{ trigger: "steer-refused", text: "replacement answer" },
		]);
		expect(port.sendAttempts.map((send) => send.opRef)).toHaveLength(2);
	} finally {
		await fixture.close();
	}
});

test("C3b: two consecutive steer refusals bump the epoch twice and each refused message is sent once", async () => {
	const port = new FailSteerPort(new Set([1, 2]));
	const fixture = await directFixture({ port });
	try {
		await admit(fixture, "twice-old", "old");
		await admit(fixture, "twice-first", "first refusal");
		await eventually(() => port.sends.length === 2, "first refused message was not sent");
		await admit(fixture, "twice-second", "second refusal");
		await eventually(() => port.sends.length === 3, "second refused message was not sent");
		expect(port.binds.map((bind) => bind.epoch)).toEqual([0, 1, 2]);
		expect(port.sends.map((send) => send.text)).toEqual(["old", "first refusal", "second refusal"]);
		expect(new Set(port.sends.map((send) => send.opRef)).size).toBe(3);
		for (const [index, send] of port.sends.entries()) port.complete(send.opRef, `answer ${index}`);
		await eventually(
			() => fixture.database.inboundPendingCount(DIRECT_ORIGIN_KEY) === 0,
			"consecutive-refusal turns did not drain",
		);
		expect(fixture.terminals).toEqual([
			{ trigger: "twice-old", text: "answer 0" },
			{ trigger: "twice-first", text: "answer 1" },
			{ trigger: "twice-second", text: "answer 2" },
		]);
	} finally {
		await fixture.close();
	}
});

test("C3c: repeated session_unavailable replacement binds remain pending but must report a bounded unrecoverable outcome", async () => {
	const timers: ScheduledTimer[] = [];
	const seam = timerSeam(timers);
	const port = new UnavailableReplacementBindPort();
	const fixture = await directFixture({ port, ...seam });
	try {
		await admit(fixture, "unavailable-old", "old");
		await admit(fixture, "unavailable-pending", "must remain pending");
		// Bind failures back off: 2s, 4s, 8s ... never abandoning the row.
		await eventually(
			() => timers.some((timer) => timer.delayMs === 2_000),
			"first replacement bind retry was not scheduled",
		);
		const fired = new Set<ScheduledTimer>();
		for (let failedAttempt = 1; failedAttempt < 3; failedAttempt++) {
			const retry = required(
				timers.find((timer) => !fired.has(timer) && timer.delayMs >= 2_000),
				`replacement bind retry ${failedAttempt} missing`,
			);
			fired.add(retry);
			retry.work();
			await eventually(
				() => port.bindAttempts >= failedAttempt + 2,
				`replacement bind attempt ${failedAttempt + 1} did not occur`,
			);
		}
		expect(timers.filter((timer) => timer.delayMs >= 2_000).map((timer) => timer.delayMs)).toEqual([
			2_000, 4_000, 8_000,
		]);
		expect(fixture.database.inboundPendingOldest(DIRECT_ORIGIN_KEY)).toMatchObject({
			message_id: "unavailable-pending",
		});
		expect(fixture.database.inboundTurnRow(required(port.sends[0], "old turn missing").opRef)?.turn_state).toBe(
			"accepted",
		);
		// The bound is an operator signal, not an abandonment: the row is still
		// pending and a retry is still armed (this was the red-team C3c finding).
		expect(
			fixture.logs.some((line) => line.startsWith("persona_send_unrecoverable") && line.includes("reason=bind_failed")),
		).toBe(true);
	} finally {
		await fixture.close();
	}
});

test("C4: a three-hour-old durable inbound row survives a manager restart and is dispatched without expiry", async () => {
	const fixture = await directFixture();
	let recovered: PersonaSessionManager | undefined;
	try {
		enqueue(fixture, "three-hours-old", "still deliver me", new Date(Date.now() - 3 * 60 * 60 * 1_000).toISOString());
		await fixture.manager.stop();
		recovered = new PersonaSessionManager({
			database: fixture.database,
			port: fixture.port,
			instanceId: "canonical-redteam-restarted",
			repo: join(fixture.home, "workspace"),
			onTurnStart: ({ trigger, turn }) => ({
				text: trigger.body,
				onTerminal: ({ text }) => {
					fixture.terminals.push({ trigger: turn.triggerMessageId, text });
				},
			}),
			log: (line) => fixture.logs.push(line),
		});
		await recovered.recover();
		await eventually(() => fixture.port.sends.length === 1, "three-hour-old row was not dispatched after restart");
		expect(fixture.port.sends[0]?.text).toBe("still deliver me");
		expect(fixture.logs.some((line) => line.includes("inbound_expired"))).toBe(false);
		fixture.port.complete(required(fixture.port.sends[0], "old row send missing").opRef, "old row answer");
		await eventually(
			() => fixture.database.inboundPendingCount(DIRECT_ORIGIN_KEY) === 0,
			"three-hour-old row did not complete",
		);
	} finally {
		await recovered?.stop();
		await fixture.close();
	}
});

type SocketClient = {
	readonly frames: any[];
	send(value: unknown): void;
	sendMany(values: readonly unknown[]): void;
	frame(id: string): any | undefined;
	response(id: string): any | undefined;
	close(): void;
};

async function connect(socketPath: string): Promise<SocketClient> {
	const frames: any[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line));
			},
		},
	});
	return {
		frames,
		send: (value) => socket.write(`${JSON.stringify(value)}\n`),
		sendMany: (values) => socket.write(values.map((value) => `${JSON.stringify(value)}\n`).join("")),
		frame: (id) => frames.find((frame) => frame.id === id),
		response: (id) => frames.find((frame) => frame.type === "response" && frame.id === id),
		close: () => socket.end(),
	};
}

type ServerFixture = {
	readonly home: string;
	readonly database: GatewayDatabase;
	readonly port: ScriptedSessionPort;
	readonly server: GatewayServer;
	readonly client: SocketClient;
	close(): Promise<void>;
};

async function serverFixture(
	options: {
		readonly port?: ScriptedSessionPort;
		readonly channels?: GatewayConfig["channels"];
		readonly dmPolicy?: GatewayConfig["dmPolicy"];
	} = {},
): Promise<ServerFixture> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-redteam-canonical-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		...(options.channels ? { channels: options.channels } : {}),
		...(options.dmPolicy ? { dmPolicy: options.dmPolicy } : {}),
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const port = options.port ?? new ScriptedSessionPort();
	const server = await startUnixServer({ config, database, sessionPort: port, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await eventually(
		() => client.frames.some((frame) => frame.type === "negotiated"),
		"gateway negotiation did not complete",
	);
	return {
		home,
		database,
		port,
		server,
		client,
		async close() {
			client.close();
			await server.stop();
			await rm(home, { recursive: true, force: true });
		},
	};
}

function request(id: string, verb: "chat.send" | "chat.edit", params: Record<string, unknown>) {
	return { v: "0.1", type: "request", id, verb, params };
}

const DM_ORIGIN = { platform: "discord", kind: "dm", conversationId: "canonical-dm", peerId: "owner" } as const;
const DM_ORIGIN_KEY = "discord/dm/canonical-dm/peer=owner";
const DM_ENGAGEMENT = { mentioned: false, group: false, authorId: "owner", authorName: "owner" };

test("C5a: an edit sent in the same socket packet as chat.send is admitted after the original and steered once", async () => {
	const fixture = await serverFixture({ dmPolicy: "open" });
	try {
		fixture.client.sendMany([
			request("race-send", "chat.send", {
				origin: DM_ORIGIN,
				messageId: "race-message",
				text: "original text",
				engagement: DM_ENGAGEMENT,
			}),
			request("race-edit", "chat.edit", {
				origin: DM_ORIGIN,
				messageId: "race-message",
				text: "edited before acknowledgement",
				engagement: DM_ENGAGEMENT,
			}),
		]);
		await eventually(
			() => fixture.client.response("race-send") !== undefined && fixture.client.response("race-edit") !== undefined,
			"send/edit race did not acknowledge both requests",
		);
		await eventually(
			() => fixture.port.sends.length === 1 && fixture.port.steers.length === 1,
			"race edit was not steered once",
		);
		expect(fixture.client.response("race-send")?.result).toMatchObject({ engaged: true });
		expect(fixture.client.response("race-edit")?.result).toMatchObject({ engaged: true });
		expect(fixture.port.sends[0]?.text).toContain("original text");
		expect(fixture.port.steers[0]?.text).toContain("[MESSAGE POINTER: race-message]");
		expect(fixture.port.steers[0]?.text).toContain("edited before acknowledgement");
		fixture.port.complete(required(fixture.port.sends[0], "race send missing").opRef, "done");
		await eventually(() => fixture.database.inboundPendingCount(DM_ORIGIN_KEY) === 0, "race turn did not complete");
	} finally {
		await fixture.close();
	}
});

test("C5b: two distinct edits during one running turn are steered in edit arrival order", async () => {
	const fixture = await serverFixture({ dmPolicy: "open" });
	try {
		fixture.client.send(
			request("edits-send", "chat.send", {
				origin: DM_ORIGIN,
				messageId: "edit-target",
				text: "version one",
				engagement: DM_ENGAGEMENT,
			}),
		);
		await eventually(() => fixture.port.sends.length === 1, "original edit target was not sent");
		fixture.client.sendMany([
			request("edit-one", "chat.edit", {
				origin: DM_ORIGIN,
				messageId: "edit-target",
				text: "version two",
				engagement: DM_ENGAGEMENT,
			}),
			request("edit-two", "chat.edit", {
				origin: DM_ORIGIN,
				messageId: "edit-target",
				text: "version three",
				engagement: DM_ENGAGEMENT,
			}),
		]);
		await eventually(() => fixture.port.steers.length === 2, "both edits were not steered");
		expect(steerBodies(fixture.port)).toEqual(["version two", "version three"]);
		expect(fixture.port.steers.every((steer) => steer.text.includes("[MESSAGE POINTER: edit-target]"))).toBe(true);
		fixture.port.complete(required(fixture.port.sends[0], "original send missing").opRef, "done");
		await eventually(() => fixture.database.inboundPendingCount(DM_ORIGIN_KEY) === 0, "edit turn did not complete");
	} finally {
		await fixture.close();
	}
});

test("C5c: an edit of context-only intake becomes a pointer turn once the open-channel edit is engaged", async () => {
	const origin = { platform: "discord", kind: "channel", conversationId: "context-channel" } as const;
	const originKey = "discord/channel/context-channel";
	const fixture = await serverFixture({ channels: { "context-channel": { engagement: "open" } } });
	try {
		fixture.client.send(
			request("context-send", "chat.send", {
				origin,
				messageId: "context-only",
				text: "context that did not start a turn",
				engagement: { mentioned: false, group: false, authorId: "human" },
			}),
		);
		await eventually(
			() => fixture.client.response("context-send") !== undefined,
			"context-only message was not acknowledged",
		);
		expect(fixture.client.response("context-send")?.result).toEqual({ turnId: null, engaged: false });
		expect(fixture.port.sends).toEqual([]);

		fixture.client.send(
			request("context-edit", "chat.edit", {
				origin,
				messageId: "context-only",
				text: "context now needs correction",
				engagement: { mentioned: false, group: true, authorId: "human" },
			}),
		);
		await eventually(() => fixture.port.sends.length === 1, "context-only edit was not sent as a pointer turn");
		expect(fixture.client.response("context-edit")?.result).toMatchObject({ engaged: true });
		expect(fixture.port.sends[0]?.text).toContain("[MESSAGE POINTER: context-only]");
		expect(fixture.port.sends[0]?.text).toContain("context now needs correction");
		fixture.port.complete(required(fixture.port.sends[0], "context pointer send missing").opRef, "done");
		await eventually(
			() => fixture.database.inboundPendingCount(originKey) === 0,
			"context pointer turn did not complete",
		);
	} finally {
		await fixture.close();
	}
});

test("C5d: malformed edits and loopback-origin edits must be rejected without an enqueue", async () => {
	const fixture = await serverFixture();
	const loopback = { platform: "loopback", kind: "loopback", conversationId: "edit-loopback" } as const;
	const loopbackKey = "loopback/loopback/edit-loopback";
	try {
		fixture.client.send(
			request("loopback-original", "chat.send", {
				origin: loopback,
				messageId: "loopback-message",
				text: "original loopback text",
			}),
		);
		await eventually(() => fixture.port.sends.length === 1, "loopback original was not sent");
		fixture.client.sendMany([
			request("invalid-empty", "chat.edit", {
				origin: DM_ORIGIN,
				messageId: "anything",
				text: "",
				engagement: DM_ENGAGEMENT,
			}),
			request("invalid-missing-id", "chat.edit", { origin: DM_ORIGIN, text: "missing id", engagement: DM_ENGAGEMENT }),
			request("invalid-loopback", "chat.edit", {
				origin: loopback,
				messageId: "loopback-message",
				text: "loopback edit",
			}),
		]);
		await eventually(
			() =>
				fixture.client.frame("invalid-empty") !== undefined &&
				fixture.client.frame("invalid-missing-id") !== undefined &&
				fixture.client.frame("invalid-loopback") !== undefined,
			"edit validation responses did not arrive",
		);
		expect(fixture.client.frame("invalid-empty")?.error?.code).toBe("invalid_params");
		expect(fixture.client.frame("invalid-missing-id")?.error?.code).toBe("invalid_params");
		// Loopback is a valid origin for chat.edit exactly as for chat.send: an
		// edit of an ingested loopback message is accepted and steered once.
		await eventually(() => fixture.port.steers.length === 1, "loopback edit was not steered");
		expect(fixture.client.frame("invalid-loopback")?.result).toMatchObject({ engaged: true });
		expect(fixture.port.steers).toHaveLength(1);
		fixture.port.complete(required(fixture.port.sends[0], "loopback original send missing").opRef, "done");
		await eventually(
			() => fixture.database.inboundPendingCount(loopbackKey) === 0,
			"loopback sequence did not complete",
		);
	} finally {
		await fixture.close();
	}
});

test("C6a: terminal recovery after a stop invokes the reconstructed lifecycle once without a second send", async () => {
	const timers: ScheduledTimer[] = [];
	const seam = timerSeam(timers);
	const fixture = await directFixture({ instanceId: "canonical-recover-terminal" });
	let recovered: PersonaSessionManager | undefined;
	try {
		await admit(fixture, "recover-terminal", "recover this answer");
		const send = required(fixture.port.sends[0], "initial recovery turn was not sent");
		await fixture.manager.stop();
		fixture.port.seedOperation(send.opRef, send.sessionId, "terminal_ok", "recovered answer");
		recovered = new PersonaSessionManager({
			database: fixture.database,
			port: fixture.port,
			instanceId: "canonical-recover-terminal",
			repo: join(fixture.home, "workspace"),
			...seam,
			onTurnStart: ({ trigger, turn }) => ({
				text: trigger.body,
				onTerminal: ({ text }) => {
					fixture.terminals.push({ trigger: turn.triggerMessageId, text });
				},
			}),
			log: (line) => fixture.logs.push(line),
		});
		await recovered.recover();
		const grace = required(
			timers.find((timer) => timer.delayMs === 250),
			"terminal recovery grace was not scheduled",
		);
		grace.work();
		await eventually(() => fixture.terminals.length === 1, "recovered terminal did not invoke onTerminal");
		expect(fixture.terminals).toEqual([{ trigger: "recover-terminal", text: "recovered answer" }]);
		expect(fixture.port.sends).toHaveLength(1);
		expect(fixture.port.sendAttempts).toHaveLength(1);
	} finally {
		await recovered?.stop();
		await fixture.close();
	}
});

test("C6b: an unknown operation on a broker-disowned session is re-sent once with a new opRef", async () => {
	const port = new DisownedStatusPort();
	const fixture = await directFixture({ port, instanceId: "canonical-recover-requeue" });
	let recovered: PersonaSessionManager | undefined;
	try {
		await admit(fixture, "recover-requeue", "retry after disown");
		const first = required(port.sends[0], "initial requeue turn was not sent");
		await fixture.manager.stop();
		port.setSessionState(first.sessionId, { live: false });
		port.disownedOps.add(first.opRef);
		recovered = new PersonaSessionManager({
			database: fixture.database,
			port,
			instanceId: "canonical-recover-requeue",
			repo: join(fixture.home, "workspace"),
			onTurnStart: ({ trigger, turn }) => ({
				text: trigger.body,
				onTerminal: ({ text }) => {
					fixture.terminals.push({ trigger: turn.triggerMessageId, text });
				},
			}),
			log: (line) => fixture.logs.push(line),
		});
		await recovered.recover();
		await eventually(() => port.sends.length === 2, "disowned operation was not sent on a new session");
		const replacement = required(port.sends[1], "replacement send missing");
		expect(replacement.opRef).not.toBe(first.opRef);
		expect(replacement.sessionId).not.toBe(first.sessionId);
		expect(port.sendAttempts).toHaveLength(2);
		port.complete(replacement.opRef, "replacement answer");
		await eventually(() => fixture.terminals.length === 1, "replacement turn did not complete");
		expect(fixture.terminals).toEqual([{ trigger: "recover-requeue", text: "replacement answer" }]);
		expect(port.sends).toHaveLength(2);
	} finally {
		await recovered?.stop();
		await fixture.close();
	}
});

// --- Generation-2 delta cases ------------------------------------------------

class BindFailsNTimesPort extends ScriptedSessionPort {
	bindAttempts = 0;
	failUntil = 0;

	async bind(input: SessionBindInput) {
		this.bindAttempts++;
		if (this.bindAttempts <= this.failUntil) throw new Error(`scripted bind failure ${this.bindAttempts}`);
		return await super.bind(input);
	}
}

test("D4: bind failures back off 2s,4s,8s,16s with one unrecoverable log at the bound, and a success resets the counter", async () => {
	const timers: ScheduledTimer[] = [];
	const port = new BindFailsNTimesPort();
	port.failUntil = 4;
	const fixture = await directFixture({ port, ...timerSeam(timers) });
	try {
		await admit(fixture, "backoff-1", "first");
		const fired = new Set<ScheduledTimer>();
		const fire = async () => {
			const timer = required(
				timers.find((candidate) => !fired.has(candidate) && candidate.delayMs >= 2_000),
				"bind retry not armed",
			);
			fired.add(timer);
			timer.work();
			await eventually(
				() =>
					fired.size === timers.filter((t) => t.delayMs >= 2_000).length - (port.sends.length ? 0 : 1) ||
					port.sends.length === 1,
				"retry did not run",
			);
		};
		await fire();
		await fire();
		await fire();
		await fire();
		await eventually(() => port.sends.length === 1, "fifth bind did not dispatch");
		console.log(
			"D4>>>",
			JSON.stringify(timers.map((t) => t.delayMs)),
			port.bindAttempts,
			JSON.stringify(fixture.logs.filter((l) => l.startsWith("persona_bind"))),
		);
		expect(timers.filter((t) => t.delayMs >= 2_000).map((t) => t.delayMs)).toEqual([2_000, 4_000, 8_000, 16_000]);
		expect(fixture.logs.filter((line) => line.startsWith("persona_send_unrecoverable"))).toHaveLength(1);
		port.complete(required(port.sends[0], "send missing").opRef, "ok");
		await eventually(() => fixture.database.inboundPendingCount(DIRECT_ORIGIN_KEY) === 0, "turn did not complete");
		// Counter reset: after /new (which forces a fresh bind) the next failure
		// starts the ladder again at 2s, not 32s.
		await fixture.manager.reset(DIRECT_ORIGIN_KEY, JSON.stringify(DIRECT_ORIGIN));
		port.failUntil = port.bindAttempts + 1;
		await admit(fixture, "backoff-2", "second");
		expect(timers.filter((t) => t.delayMs >= 2_000).map((t) => t.delayMs)).toEqual([
			2_000, 4_000, 8_000, 16_000, 2_000,
		]);
	} finally {
		await fixture.close();
	}
});

test("D5: a message admitted while a bind retry is armed waits for the retry; the retry dispatches the oldest and steers the newer", async () => {
	const timers: ScheduledTimer[] = [];
	const port = new BindFailsNTimesPort();
	port.failUntil = 1;
	const fixture = await directFixture({ port, ...timerSeam(timers) });
	try {
		await admit(fixture, "armed-1", "first");
		expect(port.bindAttempts).toBe(1);
		await admit(fixture, "armed-2", "second");
		expect(port.bindAttempts).toBe(1);
		expect(port.sends).toEqual([]);
		const retry = required(
			timers.find((t) => t.delayMs === 2_000),
			"retry not armed",
		);
		retry.work();
		await eventually(() => port.sends.length === 1 && port.steers.length === 1, "retry did not dispatch and steer");
		expect(port.sends[0]?.text).toBe("first");
		expect(steerBodies(port)).toEqual(["second"]);
		expect(port.bindAttempts).toBe(2);
	} finally {
		await fixture.close();
	}
});

test("D7: steers carry the same speaker/place/reply header as a trigger; a loopback steer without engagement is the raw body", async () => {
	// Sends are acknowledged and never completed: every turn stays running,
	// so the follow-ups are steers.
	const port = new ScriptedSessionPort();
	const fixture = await serverFixture({ port, dmPolicy: "open", channels: { "d7-chan": { engagement: "open" } } });
	try {
		const channel = { platform: "discord", kind: "channel", conversationId: "d7-chan" } as const;
		fixture.client.sendMany([
			request("dm-trigger", "chat.send", {
				origin: DM_ORIGIN,
				messageId: "dm-trigger",
				text: "dm start",
				engagement: DM_ENGAGEMENT,
			}),
			request("dm-steer", "chat.send", {
				origin: DM_ORIGIN,
				messageId: "dm-steer",
				text: "dm follow-up",
				engagement: { ...DM_ENGAGEMENT, authorName: "bellman" },
			}),
			request("ch-trigger", "chat.send", {
				origin: channel,
				messageId: "ch-trigger",
				text: "channel start",
				engagement: { mentioned: true, group: true, authorId: "u1", authorName: "alice" },
			}),
			request("ch-steer", "chat.send", {
				origin: channel,
				messageId: "ch-steer",
				text: "channel follow-up",
				engagement: {
					mentioned: true,
					group: true,
					authorId: "u2",
					authorName: "bob",
					replyTo: { messageId: "ch-trigger", authorName: "alice", fromSelf: false, excerpt: "channel start" },
				},
			}),
			request("lb-trigger", "chat.send", { origin: DIRECT_ORIGIN, text: "loopback start" }),
			request("lb-steer", "chat.send", { origin: DIRECT_ORIGIN, text: "loopback follow-up" }),
		]);
		await eventually(() => port.steers.length === 3, "not every follow-up was steered");
		const steerFor = (body: string) =>
			required(
				port.steers.find((s) => s.text.endsWith(body)),
				`${body} steer missing`,
			).text;
		expect(steerFor("dm follow-up")).toContain("bellman");
		expect(steerFor("dm follow-up")).toContain("msg:dm-steer");
		expect(steerFor("channel follow-up")).toContain("bob");
		expect(steerFor("channel follow-up")).toContain("msg:ch-steer");
		expect(steerFor("channel follow-up")).toMatch(/reply/i);
		expect(steerFor("loopback follow-up")).not.toContain("msg:");
		expect(steerFor("loopback follow-up").split("\n").at(-1)).toBe("loopback follow-up");
	} finally {
		for (const send of port.sends) port.complete(send.opRef, "done");
		await fixture.close();
	}
});

test("D8: migration 19 maps every v18 row exactly once even when a corrupt attributed_op_ref collides across batches", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-redteam-migrate-"));
	const path = join(home, "gateway.db");
	try {
		(await GatewayDatabase.open(path)).close();
		const raw = new (await import("bun:sqlite")).Database(path);
		raw.exec(`
DROP TABLE inbound_messages;
CREATE TABLE inbound_messages (message_id TEXT PRIMARY KEY, origin_key TEXT NOT NULL, origin_ref_json TEXT NOT NULL, body TEXT NOT NULL, engagement_json TEXT, state TEXT NOT NULL CHECK(state IN ('pending','processing','done')), received_at TEXT NOT NULL, batch_key TEXT, batch_role TEXT, batch_epoch INTEGER, batch_state TEXT, attributed_op_ref TEXT, accepted_at TEXT, bound_session_id TEXT, dispatched_at TEXT, terminal_delivery_id TEXT);
DELETE FROM schema_migrations WHERE version > 18;
INSERT INTO inbound_messages (message_id, origin_key, origin_ref_json, body, engagement_json, state, received_at, batch_key, batch_role, batch_epoch, batch_state, attributed_op_ref, accepted_at, bound_session_id, dispatched_at, terminal_delivery_id) VALUES
 ('live-trigger', 'o', '{}', 'a', NULL, 'pending', '2026-09-02T00:00:00.000Z', 'b1', 'trigger', 2, 'accepted', 'gw-p-live', NULL, 's', '2026-09-02T00:00:00.500Z', NULL),
 ('collision-member', 'o', '{}', 'b', NULL, 'pending', '2026-09-02T00:00:00.100Z', 'b1', 'member', 2, 'accepted', 'gw-p-old', NULL, 's', '2026-09-02T00:00:00.500Z', NULL),
 ('old-trigger', 'o', '{}', 'c', NULL, 'done', '2026-09-01T00:00:00.000Z', 'b0', 'trigger', 1, 'done', 'gw-p-old', NULL, 's', '2026-09-01T00:00:00.500Z', NULL),
 ('old-steer', 'o', '{}', 'd', NULL, 'done', '2026-09-01T00:00:01.000Z', 'b0', 'steer', 1, 'done', 'gw-p-old', NULL, NULL, NULL, NULL);
`);
		raw.close();
		const upgraded = await GatewayDatabase.open(path);
		const count = new (await import("bun:sqlite")).Database(path, { readonly: true })
			.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM inbound_messages")
			.get()?.n;
		expect(count).toBe(4);
		expect(upgraded.inboundNonterminalTurns("o").map((turn) => turn.opRef)).toEqual(["gw-p-live"]);
		expect(upgraded.inboundTurnRows("gw-p-old").map((row) => [row.message_id, row.turn_role, row.state])).toEqual([
			["old-trigger", "trigger", "done"],
			["old-steer", "steer", "done"],
			["collision-member", "steer", "done"],
		]);
		expect(upgraded.inboundPendingCount("o")).toBe(1);
		upgraded.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
