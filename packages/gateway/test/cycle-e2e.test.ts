import { afterEach, expect, spyOn, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	encodeFrame,
	MAX_FRAME_BYTES,
	monitorSessionOrigin,
	OPS_CYCLE_DEFAULT_LIMIT,
	OPS_CYCLE_MAX_LIMIT,
	type OpsCycleResult,
	type OriginRef,
	originKey,
} from "@gajae-gateway/protocol";
import { type CliRunner, GjcCliError } from "@gajae-gateway/subsession";
import type { GatewayConfig } from "../src/config";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorRegistry } from "../src/monitors/registry";
import { RuntimeCycleProjector } from "../src/ops/cycle";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import {
	attachTestBrokerOwnership,
	createOwnedSessionFixture,
	initializeTestBrokerAuthority,
	noRelay,
	ScriptedSessionPort,
	sessionPortFromResponder,
} from "./session-port.fake";

let directory = "";
let server: GatewayServer | undefined;

afterEach(async () => {
	await server?.stop("test teardown");
	server = undefined;
});

const discordDm: OriginRef = { platform: "discord", kind: "dm", conversationId: "c1", peerId: "p1" };

function testConfig(dir: string): GatewayConfig {
	return {
		schemaVersion: 1,
		home: dir,
		gjc: { agentDir: join(dir, "agent") },
		configPath: join(dir, "config.json"),
		socketPath: join(dir, "gateway.sock"),
		dbPath: join(dir, "gateway.db"),
		logVerbosity: "info",
	};
}

test("projector reads durable rows through the database and stays fail-closed", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-cycle-db-"));
	const database = await Store.open(join(directory, "gateway.db"));
	try {
		const key = "discord/dm/c1/peer=p1";
		// Mid-rebind state: /new bumped the epoch and cleared the session binding.
		database.transaction((tx) => database.bindings.rotateEpoch(tx, key, JSON.stringify(discordDm)));
		const projector = new RuntimeCycleProjector(database, { queueDepth: 0 });
		const afterBump = projector.project();
		expect(afterBump.gates).toContain("stale_session_identity");
		expect(afterBump.phase).toBe("degraded");
		expect(afterBump.sessions[0]).toMatchObject({ originKey: key, epoch: 1, sessionId: "" });

		// A bound session clears the stale-identity gate.
		database.transaction((tx) => database.bindings.bind(tx, key, "sess-bound-000000000"));
		const bound = new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		expect(bound.gates).toEqual([]);
		expect(bound.phase).toBe("idle");
		expect(bound.instanceId).toBeString();
		expect(bound.agentDisk).toBeNull();
		// Broker-bound: the projector observes the agent directory's filesystem (issue #15).
		const observed = new RuntimeCycleProjector(database, { queueDepth: 0 }, { agentDir: directory }).project();
		expect(observed.agentDisk?.path).toBe(directory);
		expect(observed.agentDisk?.freeBytes).toBeGreaterThan(0);
		const missing = new RuntimeCycleProjector(
			database,
			{ queueDepth: 0 },
			{ agentDir: join(directory, "missing-agent") },
		).project();
		expect(missing.gates).toContain("agent_disk_headroom");

		// A durable pending inbound message projects dispatching and attaches to its origin.
		database.transaction((tx) =>
			database.inbox.enqueue(tx, {
				messageId: "m1",
				originKey: key,
				originRefJson: JSON.stringify(discordDm),
				body: "hello",
			}),
		);
		const dispatching = new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		expect(dispatching.phase).toBe("dispatching");
		expect(dispatching.sessions[0].pendingInbound).toBe(1);

		// Binding it as a turn keeps it dispatching; terminal completion returns to idle.
		database.transaction((tx) =>
			database.inbox.bindTurn(tx, { messageId: "m1", originKey: key, epoch: 0, opRef: "gw-p-m1", sessionId: "s1" }),
		);
		const boundCycle = new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		expect(boundCycle.phase).toBe("dispatching");
		expect(database.transaction((tx) => database.inbox.turnComplete(tx, "gw-p-m1"))).toBe(1);
		expect(new RuntimeCycleProjector(database, { queueDepth: 0 }).project().phase).toBe("idle");

		// A quarantined memory intent is a gate, not silence.
		database.transaction((tx) =>
			database.memory.createIntent(tx, { id: "mi1", kind: "daily_capture", payloadJson: "{}" }),
		);
		database.transaction((tx) => database.memory.quarantine(tx, "mi1", "Error: test quarantine"));
		const gated = new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		expect(gated.gates).toContain("memory_closure_blocked");
		expect(gated.phase).toBe("degraded");

		// Real-DB age regression (architect blocker): a seconds-vs-ms precedence bug in
		// the census SQL once returned ~-1.8e12 here. The age must be a plausible ms value.
		const ledger = new DeliveryService(database);
		const created = Date.now();
		ledger.createPending({
			deliveryId: "age-check",
			turnId: "t-age",
			originKey: key,
			payloadJson: JSON.stringify({ turnId: "t-age", origin: discordDm, role: "assistant", text: "x", final: true }),
		});
		await Bun.sleep(1100);
		const aged = new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		const age = aged.sessions[0].oldestUnsettledAgeMs;
		expect(age).toBeNumber();
		expect(age as number).toBeGreaterThanOrEqual(1000);
		expect(age as number).toBeLessThan(Date.now() - created + 5_000);
		expect(aged.deliveries.pending).toBe(1);
	} finally {
		database.close();
	}
});

test("origins unbound by a broker cutover stay ungated until a later rebind touches them", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-cycle-cutover-"));
	const database = await Store.open(join(directory, "gateway.db"));
	try {
		const key = "discord/dm/c1/peer=p1";
		database.transaction((tx) => {
			database.bindings.rotateEpoch(tx, key, JSON.stringify(discordDm));
			database.bindings.bind(tx, key, "sess-pre-cutover-0000");
		});
		expect(database.bindings.latestBrokerCutoverAt()).toBeNull();
		database.transaction((tx) =>
			database.bindings.cutoverBrokerAuthority(tx, {
				expectedAuthority: null,
				targetAuthority: { canonicalAgentDir: "/srv/gateway/agent", identity: "gateway-private" },
				evidence: "cycle projection test",
				disposition: "quarantine",
			}),
		);
		expect(database.bindings.latestBrokerCutoverAt()).toBeString();

		const afterCutover = new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		expect(afterCutover.sessions[0]).toMatchObject({ originKey: key, sessionId: "" });
		expect(afterCutover.gates).not.toContain("stale_session_identity");

		// The next rebind stamps activity past the cutover: mid-rebind is gated again.
		await Bun.sleep(5);
		database.transaction((tx) => database.bindings.rotateEpoch(tx, key, JSON.stringify(discordDm)));
		const rebinding = new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		expect(rebinding.gates).toContain("stale_session_identity");
	} finally {
		database.close();
	}
});

class FailingBindPort extends ScriptedSessionPort {
	bindAttempts = 0;
	async bind(_input: Parameters<ScriptedSessionPort["bind"]>[0]): ReturnType<ScriptedSessionPort["bind"]> {
		this.bindAttempts++;
		throw new Error("gjc sdk request failed: spawn_failed");
	}
}

test("a cutover origin whose post-cutover inbound turn fails its first bind is gated again", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-cycle-cutover-bind-"));
	const database = await Store.open(join(directory, "gateway.db"));
	const canonicalAgentDir = join(directory, "canonical-agent");
	const key = "discord/dm/c1/peer=p1";
	let manager: PersonaSessionManager | undefined;
	try {
		database.transaction((tx) => {
			database.bindings.rotateEpoch(tx, key, JSON.stringify(discordDm));
			database.bindings.bind(tx, key, "sess-pre-cutover-0000");
		});
		database.transaction((tx) =>
			database.bindings.cutoverBrokerAuthority(tx, {
				expectedAuthority: null,
				targetAuthority: { canonicalAgentDir, identity: `gjc:${canonicalAgentDir}` },
				evidence: "cycle projection test",
				disposition: "quarantine",
			}),
		);
		const cutoverActivity = database.bindings.sessionIdentityRows()[0]?.last_activity_at;
		expect(new RuntimeCycleProjector(database, { queueDepth: 0 }).project().gates).toEqual([]);

		const port = new FailingBindPort();
		attachTestBrokerOwnership(database, port, canonicalAgentDir);
		const logs: string[] = [];
		manager = new PersonaSessionManager({
			database,
			port,
			instanceId: "cycle-cutover-test",
			repo: join(directory, "workspace"),
			onTurnStart: ({ trigger }) => ({ text: trigger.body, onTerminal: () => undefined }),
			log: (line) => logs.push(line),
		});
		expect(
			database.transaction((tx) =>
				database.inbox.enqueue(tx, {
					messageId: "after-cutover",
					originKey: key,
					originRefJson: JSON.stringify(discordDm),
					body: "hello after cutover",
				}),
			),
		).toBe(true);
		await manager.notifyInbound(key);
		for (let attempt = 0; attempt < 200 && port.bindAttempts === 0; attempt++) await Bun.sleep(5);
		expect(logs.some((line) => line.startsWith(`persona_bind_failed origin=${key}`))).toBe(true);

		// The failed bind left the session row exactly as the cutover did...
		expect(database.bindings.sessionIdentityRows()[0]).toMatchObject({
			gjc_session_id: "",
			last_activity_at: cutoverActivity,
		});
		// ...so the accepted inbound row is what brings the gate back.
		const failed = new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		expect(failed.gates).toContain("stale_session_identity");
		expect(failed.phase).toBe("degraded");
	} finally {
		await manager?.stop();
		database.close();
	}
});

test("monitor events admitted after a cutover gate exactly the session origin dispatch would bind", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-cycle-cutover-monitor-"));
	const database = await Store.open(join(directory, "gateway.db"));
	try {
		const key = "monitor/eventtype/build.done/parent=m1";
		database.transaction((tx) => {
			database.bindings.rotateEpoch(
				tx,
				key,
				JSON.stringify({ platform: "monitor", kind: "eventtype", conversationId: "build.done", parentId: "m1" }),
			);
			database.bindings.bind(tx, key, "sess-build-done-pre-cutover");
			database.monitors.create(tx, {
				id: "m1",
				name: "builds",
				triggerJson: JSON.stringify({ kind: "webhook" }),
				eventTypesJson: JSON.stringify(["build.done"]),
				burstPolicy: "batch",
				channelTargetJson: null,
				enabled: true,
				instruction: null,
				modelJson: null,
				serviceTier: null,
			});
		});
		const event = (eventId: string, eventType: string) =>
			database.transaction((tx) =>
				database.monitors.eventCreate(tx, {
					eventId,
					monitorId: "m1",
					eventType,
					payloadJson: "{}",
					firedAt: new Date().toISOString(),
				}),
			);
		// A pre-cutover event is quarantined history and vouches for nothing.
		event("e-old", "build.done");
		database.transaction((tx) =>
			database.bindings.cutoverBrokerAuthority(tx, {
				expectedAuthority: null,
				targetAuthority: { canonicalAgentDir: "/srv/gateway/agent", identity: "gateway-private" },
				evidence: "cycle projection test",
				disposition: "quarantine",
			}),
		);
		const gates = () => new RuntimeCycleProjector(database, { queueDepth: 0 }).project().gates;
		expect(gates()).toEqual([]);

		// An undeclared type dispatches to the monitor's catch-all session, not this one.
		event("e-undeclared", "deploy.done");
		expect(gates()).toEqual([]);

		// A declared type dispatches to this origin: it was asked to rebind.
		event("e-declared", "build.done");
		expect(gates()).toContain("stale_session_identity");
	} finally {
		database.close();
	}
});

test("starvation is judged per origin from turn_state: an old accepted trigger is busy, an old unbound row alone is starved", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-cycle-starve-"));
	const database = await Store.open(join(directory, "gateway.db"));
	try {
		const key = "discord/dm/c1/peer=p1";
		database.transaction((tx) => database.bindings.bind(tx, key, "sess-bound-000000000"));
		const old = new Date(Date.now() - 20 * 60_000).toISOString();
		// An accepted trigger that has been running for 20 minutes is a long turn, not starvation.
		database.transaction((tx) =>
			database.inbox.enqueue(tx, {
				messageId: "long",
				originKey: key,
				originRefJson: JSON.stringify(discordDm),
				body: "x",
				receivedAt: old,
			}),
		);
		database.transaction((tx) =>
			database.inbox.bindTurn(tx, { messageId: "long", originKey: key, epoch: 0, opRef: "gw-p-long", sessionId: "s1" }),
		);
		database.transaction((tx) => database.inbox.turnAccept(tx, "gw-p-long"));
		// A message queued behind it for 20 minutes is waiting on that turn, not stuck.
		database.transaction((tx) =>
			database.inbox.enqueue(tx, {
				messageId: "queued",
				originKey: key,
				originRefJson: JSON.stringify(discordDm),
				body: "y",
				receivedAt: old,
			}),
		);
		// The long turn has been answering (an interim reply landed), so the origin is not silent (#688).
		database.transaction((tx) => {
			database.outbox.create(tx, { deliveryId: "gw-d-interim", turnId: "t", originKey: key, payloadJson: "{}" });
			database.outbox.confirm(tx, "gw-d-interim");
		});
		const busy = new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		expect(busy.gates).toEqual([]);
		expect(busy.phase).toBe("dispatching");
		expect(busy.inFlightInbound).toBe(1);
		// Another origin with an old unbound row and nothing in flight is stuck.
		const other = { ...discordDm, conversationId: "c2", peerId: "p2" };
		const otherKey = "discord/dm/c2/peer=p2";
		database.transaction((tx) => database.bindings.bind(tx, otherKey, "sess-other-000000000"));
		database.transaction((tx) =>
			database.inbox.enqueue(tx, {
				messageId: "stuck",
				originKey: otherKey,
				originRefJson: JSON.stringify(other),
				body: "z",
				receivedAt: old,
			}),
		);
		const starved = new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		// Its user has also gone unanswered for 20 minutes (#688).
		expect(starved.gates).toEqual(["inbound_starved", "origin_silent"]);
		expect(starved.silentOrigins.map((origin) => origin.originKey)).toEqual([otherKey]);
	} finally {
		database.close();
	}
});

test("monitor authoring loss gates ops.cycle while delivery stays healthy, and clears on recovery (#160)", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-cycle-monitor-loss-"));
	const database = await Store.open(join(directory, "gateway.db"));
	try {
		const at = (minutesAgo: number) => new Date(Date.now() - minutesAgo * 60_000).toISOString();
		const seed = (eventType: string, stage: string, minutesAgo: number, authored = false) => {
			const eventId = crypto.randomUUID();
			database.transaction((tx) => {
				database.monitors.eventCreate(tx, {
					eventId,
					monitorId: "m1",
					eventType,
					payloadJson: "{}",
					firedAt: at(minutesAgo),
				});
				database.monitors.eventUpdate(tx, eventId, stage as never, null);
				if (authored) database.monitors.authoredOutputCreate(tx, eventId, "note");
			});
			return eventId;
		};
		const project = () => new RuntimeCycleProjector(database, { queueDepth: 0 }).project();
		// One lost slot of one type after a delivered one is not yet an outage.
		seed("backlog.watch", "delivered", 120, true);
		seed("backlog.watch", "failed_no_retry", 60);
		expect(project().gates).toEqual([]);
		// Two monitor types whose latest slot was lost pre-author: a cross-type outage.
		seed("memory.canonicalize", "failed_no_retry", 50);
		const crossType = project();
		expect(crossType.gates).toEqual(["monitor_authoring_lost"]);
		expect(crossType.phase).toBe("degraded");
		expect(crossType.monitorAuthoringLost).toEqual([
			{ eventType: "backlog.watch", consecutive: 1, lastFiredAt: expect.any(String) },
			{ eventType: "memory.canonicalize", consecutive: 1, lastFiredAt: expect.any(String) },
		]);
		// The canonicalize type recovers; backlog.watch then loses a second consecutive slot.
		seed("memory.canonicalize", "delivered", 40, true);
		expect(project().gates).toEqual([]);
		seed("backlog.watch", "failed_no_retry", 30);
		const streak = project();
		expect(streak.gates).toEqual(["monitor_authoring_lost"]);
		expect(streak.monitorAuthoringLost).toMatchObject([{ eventType: "backlog.watch", consecutive: 2 }]);
		// A delivered slot of the lost type clears the gate again.
		seed("backlog.watch", "delivered", 10, true);
		expect(project().gates).toEqual([]);
		// Losses older than the observation window age out instead of gating forever.
		seed("retired.a", "failed_no_retry", 25 * 60);
		seed("retired.b", "failed_no_retry", 25 * 60);
		expect(project().gates).toEqual([]);
	} finally {
		database.close();
	}
});

test("issue #189: consecutive failed_no_retry monitor events degrade the projection; one delivery clears it", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-cycle-monitor-"));
	const database = await Store.open(join(directory, "gateway.db"));
	try {
		const fire = (id: string) =>
			database.transaction((tx) =>
				database.monitors.eventCreate(tx, {
					eventId: id,
					monitorId: "m1",
					eventType: "digest",
					payloadJson: "{}",
					firedAt: new Date().toISOString(),
				}),
			);
		fire("ok-0");
		database.transaction((tx) => database.monitors.eventUpdate(tx, "ok-0", "delivered"));
		for (const id of ["lost-1", "lost-2"]) {
			fire(id);
			expect(
				database.transaction((tx) =>
					database.monitors.eventTerminalFail(tx, id, "internal_error", "authoring turn failed"),
				),
			).toBe(true);
		}
		const projector = new RuntimeCycleProjector(database, { queueDepth: 0 });
		expect(database.monitors.consecutiveTerminalFailures()).toBe(2);
		// Two same-type pre-author losses already trip the #160 authoring gate;
		// the #189 dispatch streak stays below its threshold.
		expect(projector.project().gates).toEqual(["monitor_authoring_lost"]);
		fire("lost-3");
		database.transaction((tx) =>
			database.monitors.eventTerminalFail(tx, "lost-3", "internal_error", "authoring turn failed"),
		);
		const outage = projector.project();
		expect(outage.gates).toEqual(["monitor_authoring_lost", "monitor_dispatch_failing"]);
		expect(outage.phase).toBe("degraded");
		await Bun.sleep(2);
		fire("ok-4");
		database.transaction((tx) => database.monitors.eventUpdate(tx, "ok-4", "delivered"));
		expect(database.monitors.consecutiveTerminalFailures()).toBe(0);
		expect(projector.project().gates).toEqual([]);
		const churning = new RuntimeCycleProjector(database, { queueDepth: 0 }, { brokerRespawnChurn: () => true });
		expect(churning.project().gates).toEqual(["broker_respawn_churn"]);
	} finally {
		database.close();
	}
});

test("issue #369: contract-unmet events are terminal dispatch failures, not pre-author losses", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-cycle-contract-unmet-"));
	const database = await Store.open(join(directory, "gateway.db"));
	try {
		const fire = (id: string) =>
			database.transaction((tx) =>
				database.monitors.eventCreate(tx, {
					eventId: id,
					monitorId: "m1",
					eventType: "digest",
					payloadJson: "{}",
					firedAt: new Date().toISOString(),
				}),
			);
		fire("lost-before-authoring");
		database.transaction((tx) =>
			database.monitors.eventTerminalFail(tx, "lost-before-authoring", "internal_error", "authoring turn failed"),
		);
		fire("contract-unmet-1");
		database.transaction((tx) => database.monitors.eventUpdate(tx, "contract-unmet-1", "contract_unmet"));
		database.transaction((tx) => database.monitors.eventUpdate(tx, "contract-unmet-1", "delivered"));
		expect(database.monitors.eventGet("contract-unmet-1")?.stage).toBe("contract_unmet");
		expect(
			database.transaction((tx) =>
				database.monitors.eventTerminalFail(tx, "contract-unmet-1", "internal_error", "late failure"),
			),
		).toBe(false);
		expect(database.monitors.authoringLossStreaks("2026-01-01T00:00:00.000Z")).toEqual([]);

		const projector = new RuntimeCycleProjector(database, { queueDepth: 0 });
		for (const id of ["contract-unmet-2", "contract-unmet-3"]) {
			fire(id);
			database.transaction((tx) => database.monitors.eventUpdate(tx, id, "contract_unmet"));
		}
		expect(database.monitors.consecutiveTerminalFailures()).toBe(4);
		expect(projector.project().gates).toEqual(["monitor_dispatch_failing"]);
		await Bun.sleep(2);
		fire("recovered");
		database.transaction((tx) => database.monitors.eventUpdate(tx, "recovered", "delivered"));
		expect(database.monitors.consecutiveTerminalFailures()).toBe(0);
		expect(projector.project().gates).toEqual([]);
	} finally {
		database.close();
	}
});

test("ops.cycle serves a fresh fail-closed snapshot and broker provisioning status without a BrokerClient", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-cycle-e2e-"));
	const config = testConfig(directory);
	const database = await Store.open(config.dbPath);
	const key = "discord/dm/c1/peer=p1";
	database.transaction((tx) => database.bindings.rotateEpoch(tx, key, JSON.stringify(discordDm)));
	const ledger = new DeliveryService(database);
	ledger.createPending({
		deliveryId: "d1",
		turnId: "t1",
		originKey: key,
		payloadJson: JSON.stringify({ turnId: "t1", origin: discordDm, role: "assistant", text: "hi", final: true }),
	});
	const sessionPort = sessionPortFromResponder({ respond: async () => "mock reply" });
	const previousHome = process.env.HOME;
	const previousXdgDataHome = process.env.XDG_DATA_HOME;
	process.env.HOME = directory;
	process.env.XDG_DATA_HOME = join(directory, ".local", "share");
	try {
		server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
		const frames: unknown[] = [];
		const socket = Bun.connect({
			unix: config.socketPath,
			socket: {
				data(_socket, data) {
					for (const line of new TextDecoder().decode(data).split("\n")) if (line) frames.push(JSON.parse(line));
				},
			},
		});
		const conn = await socket;
		try {
			const send = (frame: unknown) => conn.write(`${JSON.stringify(frame)}\n`);
			send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
			send({ v: "0.1", type: "request", id: "cycle", verb: "ops.cycle" });
			send({ v: "0.1", type: "request", id: "slo", verb: "ops.slo", params: { days: 1 } });
			for (let i = 0; i < 100 && frames.length < 3; i++) await Bun.sleep(5);
		} finally {
			conn.end();
		}
		const responses = frames.filter((frame) => (frame as { type?: string }).type === "response") as Array<{
			id?: string;
			result?: {
				phase?: string;
				gates?: string[];
				sessions?: Array<Record<string, unknown>>;
				deliveries?: Record<string, number>;
				brokerAgent?: { status: string } | null;
				days?: Array<{ status: string }>;
				ok?: boolean;
				slo?: { days: Array<{ status: string }>; ok: boolean };
			};
		}>;
		const cycle = responses.find((frame) => frame.id === "cycle")?.result;
		const slo = responses.find((frame) => frame.id === "slo")?.result;
		expect(cycle?.phase).toBe("degraded");
		expect(cycle?.gates).toContain("stale_session_identity");
		expect(cycle?.gates).toContain("broker_agent_unprovisioned");
		expect(cycle?.deliveries).toMatchObject({ pending: 1 });
		expect(cycle?.sessions?.[0]).toMatchObject({ originKey: key, epoch: 1 });
		expect(cycle?.brokerAgent?.status).toBe("unprovisioned");
		expect(cycle?.slo?.days).toHaveLength(7);
		expect(cycle?.slo?.ok).toBe(false);
		expect(slo?.days).toHaveLength(1);
		expect(slo?.days?.[0]?.status).toBe("insufficient");
		expect(slo?.ok).toBe(false);
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
		if (previousXdgDataHome === undefined) delete process.env.XDG_DATA_HOME;
		else process.env.XDG_DATA_HOME = previousXdgDataHome;
	}
});

test("session_rebound dispatch failure cause reaches ops.cycle diagnostics (#600)", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-cycle-600-"));
	const config = testConfig(directory);
	const database = await Store.open(config.dbPath);
	let databaseClosed = false;
	const closeDatabase = () => {
		if (databaseClosed) return;
		database.close();
		databaseClosed = true;
	};
	const agentDir = join(directory, "agent");
	const repo = join(directory, "workspace");
	const authority = initializeTestBrokerAuthority(database, agentDir);
	const registry = new MonitorRegistry(database);
	const genericErrorMonitor = registry.add({
		name: "diagnostic-generic-error",
		trigger: { kind: "cron", schedule: "0 * * * *" },
		eventTypes: ["memory.audit"],
		burstPolicy: "dedupe",
		enabled: true,
	});
	const nestedTerminalMonitor = registry.add({
		name: "diagnostic-nested-terminal",
		trigger: { kind: "cron", schedule: "0 * * * *" },
		eventTypes: ["memory.audit"],
		burstPolicy: "dedupe",
		enabled: true,
	});
	const turnFailureMonitor = registry.add({
		name: "diagnostic-turn-failure",
		trigger: { kind: "cron", schedule: "0 * * * *" },
		eventTypes: ["memory.audit"],
		burstPolicy: "dedupe",
		enabled: true,
	});
	const genericErrorEventId = crypto.randomUUID();
	const nestedTerminalEventId = crypto.randomUUID();
	const turnFailureEventId = crypto.randomUUID();
	const eventIds = [genericErrorEventId, nestedTerminalEventId, turnFailureEventId];
	for (const [monitor, eventId] of [
		[genericErrorMonitor, genericErrorEventId],
		[nestedTerminalMonitor, nestedTerminalEventId],
		[turnFailureMonitor, turnFailureEventId],
	] as const)
		database.transaction((tx) =>
			database.monitors.eventCreate(tx, {
				eventId,
				monitorId: monitor.monitorId,
				eventType: "memory.audit",
				payloadJson: "{}",
				firedAt: new Date().toISOString(),
			}),
		);
	const reboundOriginKey = originKey(monitorSessionOrigin(nestedTerminalMonitor.monitorId, "memory.audit"));
	await createOwnedSessionFixture(database, authority, {
		sessionId: "saved-session",
		originKey: reboundOriginKey,
		epoch: 0,
		repo,
	});
	let createdSessions = 0;
	const cli: CliRunner = async (args) => {
		if (args.includes("inspect") && args.includes("saved-session"))
			return {
				exitCode: 1,
				stdout: JSON.stringify({
					ok: false,
					error: { code: "session_unavailable", message: "saved session unavailable" },
				}),
				stderr: "",
			};
		if (args.includes("session.create")) {
			createdSessions++;
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { sessionId: `fresh-session-${createdSessions}` } }),
				stderr: "",
			};
		}
		if (args.includes("inspect")) {
			const sessionId = args.at(-1);
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: { session: { sessionId, live: true, deleted: false, locator: { cwd: repo } } },
				}),
				stderr: "",
			};
		}
		throw new Error(`unexpected SDK call ${args.join(" ")}`);
	};
	const sessionPort = new BrokerSessionPort({
		database,
		authority,
		cli,
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	});
	sessionPort.request = async (input) => {
		if (input.originKey?.includes(genericErrorMonitor.monitorId)) throw new Error("generic request failure");
		const cause = Object.assign(new Error("sdk_prompt_terminal_failed"), {
			code: "sdk_prompt_terminal_failed",
			reason:
				"The model is temporarily unavailable; token=ghp_attacker-secret https://secret.example/path;/opt/app/credentials with spaces.json",
		});
		if (input.originKey?.includes(nestedTerminalMonitor.monitorId))
			throw new Error("monitor request failed", { cause });
		throw new GjcCliError("gjc sdk turn.prompt reported failure", 0, "", {
			code: "internal_error",
			cause: { code: cause.code, message: cause.code, reason: "model_overloaded" },
		});
	};
	const previousHome = process.env.HOME;
	const previousXdgDataHome = process.env.XDG_DATA_HOME;
	const reboundLogs: string[] = [];
	const info = spyOn(console, "info").mockImplementation((line) => reboundLogs.push(String(line)));
	process.env.HOME = directory;
	process.env.XDG_DATA_HOME = join(directory, ".local", "share");
	try {
		server = await startUnixServer({ config, database, sessionPort, onStop: closeDatabase });
		for (let i = 0; i < 200 && eventIds.some((id) => !database.monitors.failure(id)); i++) await Bun.sleep(5);
		const genericError = database.monitors.failure(genericErrorEventId);
		const nestedTerminal = database.monitors.failure(nestedTerminalEventId);
		const turnFailure = database.monitors.failure(turnFailureEventId);
		expect(genericError?.code).toBe("internal_error");
		expect(nestedTerminal?.code).toBe("authoring_turn_failed");
		expect(turnFailure?.code).toBe("authoring_turn_failed");
		expect(reboundLogs.some((line) => line.startsWith(`session_rebound origin=${reboundOriginKey}`))).toBe(true);
		expect(database.bindings.getSessionRecord(reboundOriginKey)).toMatchObject({
			epoch: 1,
			sessionId: expect.stringMatching(/^fresh-session-/),
		});
		expect(createdSessions).toBe(3);
		for (const detail of [genericError?.detail ?? "", nestedTerminal?.detail ?? "", turnFailure?.detail ?? ""]) {
			expect(detail).not.toContain("ghp_");
			expect(detail).not.toContain("secret.example");
			expect(detail).not.toContain("/opt/");
			expect(detail).not.toContain("C:\\service");
		}
		expect(nestedTerminal?.detail).toContain('"causeCode":"sdk_prompt_terminal_failed"');
		expect(nestedTerminal?.detail).toContain("The model is temporarily unavailable");
		expect(turnFailure?.detail).toContain('"causeCode":"sdk_prompt_terminal_failed"');
		expect(turnFailure?.detail).toContain('"message":"model_overloaded"');

		const frames: unknown[] = [];
		const conn = await Bun.connect({
			unix: config.socketPath,
			socket: {
				data(_socket, data) {
					for (const line of new TextDecoder().decode(data).split("\n")) if (line) frames.push(JSON.parse(line));
				},
			},
		});
		try {
			conn.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
			conn.write(`${JSON.stringify({ v: "0.1", type: "request", id: "cycle", verb: "ops.cycle" })}\n`);
			for (let i = 0; i < 100 && !frames.some((frame) => (frame as { id?: string }).id === "cycle"); i++)
				await Bun.sleep(5);
		} finally {
			conn.end();
		}
		const response = frames.find((frame) => (frame as { id?: string }).id === "cycle") as
			| { result?: OpsCycleResult }
			| undefined;
		expect(response?.result?.monitorFailures).toHaveLength(3);
		const failures = response?.result?.monitorFailures ?? [];
		expect(failures.find((failure) => failure.eventId === genericErrorEventId)).toMatchObject({
			monitorId: genericErrorMonitor.monitorId,
			eventType: "memory.audit",
			stage: "failed",
			code: "internal_error",
		});
		expect(failures.find((failure) => failure.eventId === nestedTerminalEventId)).toMatchObject({
			monitorId: nestedTerminalMonitor.monitorId,
			eventType: "memory.audit",
			stage: "failed",
			code: "authoring_turn_failed",
		});
		expect(failures.find((failure) => failure.eventId === turnFailureEventId)).toMatchObject({
			monitorId: turnFailureMonitor.monitorId,
			eventType: "memory.audit",
			stage: "failed",
			code: "authoring_turn_failed",
		});
		expect(failures.find((row) => row.eventId === nestedTerminalEventId)?.detail).toContain(
			'"causeCode":"sdk_prompt_terminal_failed"',
		);
		expect(failures.find((row) => row.eventId === nestedTerminalEventId)?.detail).toContain(
			"The model is temporarily unavailable",
		);
		expect(failures.find((row) => row.eventId === turnFailureEventId)?.detail).toContain(
			'"message":"model_overloaded"',
		);
	} finally {
		info.mockRestore();
		await server?.stop("issue #600 regression test");
		server = undefined;
		closeDatabase();
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
		if (previousXdgDataHome === undefined) delete process.env.XDG_DATA_HOME;
		else process.env.XDG_DATA_HOME = previousXdgDataHome;
		await rm(directory, { recursive: true, force: true });
	}
});

test("ops.cycle pages 1200 session rows under the frame cap without hiding whole-home gates (#574)", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-cycle-574-"));
	const config = testConfig(directory);
	const database = await Store.open(config.dbPath);
	let databaseClosed = false;
	const closeDatabase = () => {
		if (databaseClosed) return;
		database.close();
		databaseClosed = true;
	};
	try {
		const lastOrigin: OriginRef = {
			platform: "discord",
			kind: "thread",
			conversationId: `channel-${String(1_199).padStart(4, "0")}${"c".repeat(244)}`,
			parentId: `parent-${String(1_199).padStart(4, "0")}${"p".repeat(245)}`,
		};
		const lastOriginKey = originKey(lastOrigin);
		database.transaction((tx) => {
			for (let index = 0; index < 1_200; index++) {
				const suffix = String(index).padStart(4, "0");
				const origin: OriginRef = {
					platform: "discord",
					kind: "thread",
					conversationId: `channel-${suffix}${"c".repeat(244)}`,
					parentId: `parent-${suffix}${"p".repeat(245)}`,
				};
				const key = originKey(origin);
				const epoch = database.bindings.rotateEpoch(tx, key, JSON.stringify(origin));
				if (index < 1_199) database.bindings.bindAtEpoch(tx, key, `session-${suffix}`, epoch);
			}
		});

		server = await startUnixServer({
			config,
			database,
			sessionPort: sessionPortFromResponder({ respond: async () => "" }),
			onStop: closeDatabase,
		});
		const decoder = new TextDecoder();
		const frames: Array<{
			frame: {
				type?: string;
				id?: string;
				result?: OpsCycleResult;
				error?: { code?: string; message?: string };
			};
			bytes: number;
		}> = [];
		let buffer = "";
		const conn = await Bun.connect({
			unix: config.socketPath,
			socket: {
				data(_socket, data) {
					buffer += decoder.decode(data, { stream: true });
					for (;;) {
						const newline = buffer.indexOf("\n");
						if (newline < 0) break;
						const line = buffer.slice(0, newline);
						buffer = buffer.slice(newline + 1);
						if (line) frames.push({ frame: JSON.parse(line), bytes: Buffer.byteLength(line, "utf8") });
					}
				},
			},
		});
		try {
			const send = (frame: unknown) => conn.write(`${JSON.stringify(frame)}\n`);
			send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
			send({ v: "0.1", type: "request", id: "default", verb: "ops.cycle", params: {} });
			send({ v: "0.1", type: "request", id: "legacy", verb: "ops.cycle" });
			for (const [id, offset] of [
				["page-0", 0],
				["page-500", 500],
				["page-1000", 1_000],
			] as const)
				send({
					v: "0.1",
					type: "request",
					id,
					verb: "ops.cycle",
					params: { limit: OPS_CYCLE_MAX_LIMIT, offset },
				});
			send({
				v: "0.1",
				type: "request",
				id: "invalid-limit",
				verb: "ops.cycle",
				params: { limit: OPS_CYCLE_MAX_LIMIT + 1 },
			});
			const responseIds = ["default", "legacy", "page-0", "page-500", "page-1000", "invalid-limit"];
			for (
				let attempt = 0;
				attempt < 600 && responseIds.some((id) => !frames.some((entry) => entry.frame.id === id));
				attempt++
			)
				await Bun.sleep(5);
		} finally {
			conn.end();
		}

		const resultFor = (id: string): OpsCycleResult => {
			const frame = frames.find((entry) => entry.frame.id === id)?.frame;
			if (frame?.type !== "response" || !frame.result) throw new Error(`missing ops.cycle response ${id}`);
			return frame.result;
		};
		const defaultPage = resultFor("default");
		const legacy = frames.find((entry) => entry.frame.id === "legacy")?.frame;
		const pages = [resultFor("page-0"), resultFor("page-500"), resultFor("page-1000")];
		expect(defaultPage.sessions).toHaveLength(OPS_CYCLE_DEFAULT_LIMIT);
		expect(defaultPage.sessionsTotal).toBe(1_200);
		expect(defaultPage.sessionsOffset).toBe(0);
		expect(defaultPage.sessions.every((session) => session.sessionId !== "")).toBe(true);
		expect(defaultPage.gates).toContain("stale_session_identity");
		expect(legacy?.type).toBe("error");
		expect(legacy?.error?.code).toBe("invalid_params");
		expect(legacy?.error?.message).toContain("request limit/offset paging");
		expect(pages.map((page) => page.sessions.length)).toEqual([500, 500, 200]);
		expect(pages.map((page) => page.sessionsOffset)).toEqual([0, 500, 1_000]);
		expect(pages.every((page) => page.sessionsTotal === 1_200 && page.gates.includes("stale_session_identity"))).toBe(
			true,
		);
		const allSessions = pages.flatMap((page) => page.sessions);
		expect(new Set(allSessions.map((session) => session.originKey)).size).toBe(1_200);
		expect(allSessions.at(-1)).toMatchObject({ originKey: lastOriginKey, sessionId: "" });

		for (const id of ["default", "page-0", "page-500", "page-1000"]) {
			const frame = frames.find((entry) => entry.frame.id === id);
			expect(frame?.bytes).toBeLessThanOrEqual(MAX_FRAME_BYTES);
		}
		const firstPage = pages[0];
		if (!firstPage) throw new Error("first session page missing");
		expect(() =>
			encodeFrame({
				v: "0.1",
				type: "response",
				id: "legacy-unpaged",
				result: { ...firstPage, sessions: allSessions },
			}),
		).toThrow(`frame exceeds ${MAX_FRAME_BYTES} bytes`);
		const invalid = frames.find((entry) => entry.frame.id === "invalid-limit")?.frame;
		expect(invalid?.type).toBe("error");
		expect(invalid?.error?.code).toBe("invalid_params");
	} finally {
		await server?.stop("test teardown");
		server = undefined;
		closeDatabase();
		await rm(directory, { recursive: true, force: true });
		directory = "";
	}
});
