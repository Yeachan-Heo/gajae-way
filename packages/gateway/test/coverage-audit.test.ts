import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import type { InboundBatch } from "../src/store/db";
import { GatewayDatabase } from "../src/store/db";
import { ScriptedSessionPort } from "./session-port.fake";

const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "coverage" } as const;
const ORIGIN_KEY = "loopback/loopback/coverage";

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 250; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

type Fixture = {
	readonly database: GatewayDatabase;
	readonly manager: PersonaSessionManager;
	readonly port: ScriptedSessionPort;
	readonly batches: Map<string, InboundBatch>;
	close(): Promise<void>;
};

async function fixture(
	options: {
		readonly port?: ScriptedSessionPort;
		readonly settleWindowMs?: number;
		readonly now?: () => number;
		readonly setTimeout?: (work: () => void, delayMs: number) => unknown;
		readonly clearTimeout?: (timer: unknown) => void;
		readonly brokerGeneration?: () => number;
	} = {},
): Promise<Fixture> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-coverage-audit-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const batches = new Map<string, InboundBatch>();
	const manager = new PersonaSessionManager({
		database,
		port: options.port ?? new ScriptedSessionPort(),
		instanceId: "coverage-audit",
		repo: join(home, "workspace"),
		settleWindowMs: options.settleWindowMs ?? 0,
		...(options.now ? { now: options.now } : {}),
		...(options.setTimeout ? { setTimeout: options.setTimeout } : {}),
		...(options.clearTimeout ? { clearTimeout: options.clearTimeout } : {}),
		...(options.brokerGeneration ? { brokerGeneration: options.brokerGeneration } : {}),
		onTurnStart: ({ batch, rows }) => {
			batches.set(batch.batchKey, batch);
			return { text: rows.map((row) => row.body).join("\n") };
		},
	});
	return {
		database,
		manager,
		port: options.port ?? (manager.port as ScriptedSessionPort),
		batches,
		async close() {
			await manager.stop();
			database.close();
			await rm(home, { recursive: true, force: true });
		},
	};
}

function enqueue(
	database: GatewayDatabase,
	messageId: string,
	body: string,
	receivedAt = new Date().toISOString(),
): void {
	expect(
		database.inboundEnqueue({
			messageId,
			originKey: ORIGIN_KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body,
			receivedAt,
		}),
	).toBe(true);
}

function assertCoverage(fixture: Fixture, messageIds: readonly string[]): void {
	const rows = [...fixture.batches.values()].flatMap((batch) => fixture.database.inboundBatchRows(batch.batchKey));
	expect(rows.map((row) => row.message_id).sort()).toEqual([...messageIds].sort());
	for (const row of rows) {
		expect(row.batch_role === "trigger" || row.batch_role === "member" || row.batch_role === "steer").toBe(true);
		expect(row).toMatchObject({ state: "done", batch_state: "done", attributed_op_ref: expect.any(String) });
	}
	expect(fixture.database.inboundPendingCount(ORIGIN_KEY)).toBe(0);
}

test("coverage audit attributes a message arriving exactly at the settle fire boundary", async () => {
	const base = Date.parse("2026-09-02T00:00:00.000Z");
	let now = base;
	const fixtureState = await fixture({
		settleWindowMs: 100,
		now: () => now,
		setTimeout: () => 0,
		clearTimeout: () => {},
	});
	try {
		enqueue(fixtureState.database, "boundary-trigger", "first", new Date(base).toISOString());
		await fixtureState.manager.notifyInbound(ORIGIN_KEY);
		enqueue(fixtureState.database, "boundary-member", "at cutoff", new Date(base + 100).toISOString());
		await fixtureState.manager.notifyInbound(ORIGIN_KEY);
		now = base + 100;
		await fixtureState.manager.tick(ORIGIN_KEY);
		await eventually(() => fixtureState.port.sends.length === 1, "boundary batch did not dispatch");
		fixtureState.port.complete(fixtureState.port.sends[0]!.opRef, "done");
		await eventually(
			() => fixtureState.database.inboundPendingCount(ORIGIN_KEY) === 0,
			"boundary message remained pending past settle plus grace",
		);
		assertCoverage(fixtureState, ["boundary-trigger", "boundary-member"]);
	} finally {
		await fixtureState.close();
	}
});

class FailFirstSteerPort extends ScriptedSessionPort {
	steerAttempts = 0;
	async steer(input: Parameters<ScriptedSessionPort["steer"]>[0]): Promise<void> {
		this.steerAttempts++;
		if (this.steerAttempts === 1) throw new Error("scripted ambiguous steer transport failure");
		await super.steer(input);
	}
}

test("coverage audit leaves a failed steer pending only until the next durable batch", async () => {
	const port = new FailFirstSteerPort();
	const fixtureState = await fixture({ port });
	try {
		enqueue(fixtureState.database, "steer-trigger", "first");
		await fixtureState.manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => port.sends.length === 1, "trigger did not start");
		enqueue(fixtureState.database, "steer-failure", "must not disappear");
		await fixtureState.manager.notifyInbound(ORIGIN_KEY);
		expect(port.steerAttempts).toBe(1);
		// The accepted trigger still has legacy `pending` state; the failed steer
		// is the only unbatched row and must wait for the next settle boundary.
		expect(fixtureState.database.inboundPendingCount(ORIGIN_KEY)).toBe(2);
		expect(fixtureState.database.inboundNonterminalBatches(ORIGIN_KEY)).toHaveLength(1);

		port.complete(port.sends[0]!.opRef, "first done");
		await eventually(() => port.sends.length === 2, "failed steer was not admitted as the next batch");
		port.complete(port.sends[1]!.opRef, "second done");
		await eventually(
			() => fixtureState.database.inboundPendingCount(ORIGIN_KEY) === 0,
			"failed-steer message remained pending past settle plus grace",
		);
		assertCoverage(fixtureState, ["steer-trigger", "steer-failure"]);
	} finally {
		await fixtureState.close();
	}
});

test("coverage audit preserves attribution when a broker generation changes mid-turn", async () => {
	let generation = 1;
	const fixtureState = await fixture({ brokerGeneration: () => generation });
	try {
		enqueue(fixtureState.database, "restart-trigger", "in-flight before broker restart");
		await fixtureState.manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => fixtureState.port.sends.length === 1, "trigger did not start");
		const running = fixtureState.port.sends[0]!;

		generation = 2;
		await fixtureState.manager.onBrokerGeneration(generation);
		enqueue(fixtureState.database, "restart-steer", "arrived after broker replacement");
		await fixtureState.manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => fixtureState.port.steers.length === 1, "post-restart message was not steered");
		expect(fixtureState.port.sends).toHaveLength(1);
		expect(fixtureState.port.steers[0]).toMatchObject({
			sessionId: running.sessionId,
			text: expect.stringMatching(/\narrived after broker replacement$/),
		});

		fixtureState.port.complete(running.opRef, "restarted done");
		await eventually(
			() => fixtureState.database.inboundPendingCount(ORIGIN_KEY) === 0,
			"post-restart message remained pending past settle plus grace",
		);
		assertCoverage(fixtureState, ["restart-trigger", "restart-steer"]);
	} finally {
		await fixtureState.close();
	}
});
