import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type OriginRef, originKey } from "@gajae-gateway/protocol";
import type { GatewayConfig } from "../src/config";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase, workAttemptDeliveryId } from "../src/store/db";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

const rootOrigin: OriginRef = { platform: "discord", kind: "channel", conversationId: "parent-channel" };
const rootKey = originKey(rootOrigin);
const fixtures: Array<{
	home: string;
	server: GatewayServer;
	database: GatewayDatabase;
	port: ScriptedSessionPort;
	client: Awaited<ReturnType<typeof connect>>;
}> = [];

afterEach(async () => {
	for (const fixture of fixtures.splice(0).reverse()) {
		fixture.client.close();
		await fixture.server.stop();
	}
});

async function eventually<T>(read: () => T | Promise<T>, accept: (value: T) => boolean, label: string): Promise<T> {
	for (let attempt = 0; attempt < 600; attempt++) {
		const value = await read();
		if (accept(value)) return value;
		await Bun.sleep(5);
	}
	throw new Error(`timed out waiting for ${label}`);
}

async function connect(socketPath: string) {
	const frames: any[] = [];
	let buffered = Buffer.alloc(0);
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered = Buffer.concat([buffered, Buffer.from(data)]);
				let newline = buffered.indexOf(10);
				while (newline >= 0) {
					const line = buffered.subarray(0, newline).toString("utf8");
					buffered = buffered.subarray(newline + 1);
					if (line) frames.push(JSON.parse(line));
					newline = buffered.indexOf(10);
				}
			},
		},
	});
	const client: {
		frames: any[];
		send(frame: unknown): void;
		close(): void;
		request(verb: string, params: unknown): Promise<any>;
	} = {
		frames,
		send: (frame: unknown) => socket.write(`${JSON.stringify(frame)}\n`),
		close: () => socket.end(),
		async request(verb: string, params: unknown): Promise<any> {
			const id = crypto.randomUUID();
			client.send({ v: "0.1", type: "request", id, verb, params });
			return await eventually<any>(() => frames.find((frame) => frame.id === id), Boolean, `${verb} response`);
		},
	};
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await eventually(
		() => frames,
		(value) => value.some((frame) => frame.type === "negotiated"),
		"socket negotiation",
	);
	return client;
}

async function bindPersonaSession(
	f: Awaited<ReturnType<typeof fixture>>,
	key: string,
	sessionId = crypto.randomUUID(),
): Promise<string> {
	const authority = f.database.inspectBrokerAuthority().authority;
	if (!authority) throw new Error("test broker authority missing");
	const recorded = f.database.recordOwnedBinding({
		authority,
		sessionId,
		originKey: key,
		epoch: 0,
		repo: join(f.home, "workspace"),
	});
	if (!recorded) throw new Error("persona fixture binding lost its epoch");
	return sessionId;
}
async function fixture(
	options: {
		readonly ownerTarget?: OriginRef | false;
		readonly allowNested?: boolean;
		readonly channels?: GatewayConfig["channels"];
	} = {},
) {
	const home = await mkdtemp(join(tmpdir(), "lane-parent-report-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
		...(options.ownerTarget === false ? {} : { ownerTarget: { origin: options.ownerTarget ?? rootOrigin } }),
		...(options.channels ? { channels: options.channels } : {}),
		...(options.allowNested === undefined ? {} : { work: { allowNested: options.allowNested } }),
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const sessions = new Map<string, string>();
	const port = new ScriptedSessionPort({
		onBind: (input) => {
			const bindingKey = `${input.originKey}#${input.epoch}`;
			const known = database.getSessionRecord(input.originKey);
			const knownSessionId = known?.epoch === input.epoch && known.sessionId ? known.sessionId : undefined;
			const sessionId = sessions.get(bindingKey) ?? knownSessionId ?? crypto.randomUUID();
			sessions.set(bindingKey, sessionId);
			return sessionId;
		},
	});
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	const server = await startUnixServer({ config, database, sessionPort: port, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	const current = { home, server, database, port, client };
	fixtures.push(current);
	return current;
}

async function startPersonaTurn(
	f: Awaited<ReturnType<typeof fixture>>,
	origin: OriginRef,
	messageId: string,
	text = "human request",
) {
	const before = f.port.sends.length;
	const response = await f.client.request("chat.send", {
		origin,
		messageId,
		text,
		engagement: { mentioned: true, group: true, authorId: "human-1", authorName: "Alice" },
	});
	expect(response.error).toBeUndefined();
	const sends = await eventually(
		() => f.port.sends,
		(items) => items.length > before,
		"persona send",
	);
	return sends.at(-1)!;
}

async function startLane(f: Awaited<ReturnType<typeof fixture>>, name: string, callerSessionId?: string) {
	const response = await f.client.request("work.start", {
		name,
		text: `work for ${name}`,
		cwd: f.home,
		...(callerSessionId ? { callerSessionId } : {}),
	});
	return response;
}

async function settleLane(f: Awaited<ReturnType<typeof fixture>>, opRef: string, answer = "lane answer") {
	f.port.complete(opRef, answer);
	return await eventually(
		() => f.database.workAttemptGet(opRef),
		(runtime) => runtime?.settledAt !== undefined && runtime.settledAt !== null,
		`lane ${opRef} settlement`,
	);
}

async function silencePersona(f: Awaited<ReturnType<typeof fixture>>, opRef: string): Promise<void> {
	f.port.complete(opRef, "[SILENT]");
	await eventually(
		() => f.database.inboundTurnRow(opRef),
		(row) => row?.turn_state === "done",
		`silent persona turn ${opRef}`,
	);
}

function chatMessages(f: Awaited<ReturnType<typeof fixture>>) {
	return f.client.frames.filter((frame) => frame.type === "event" && frame.event === "chat.message");
}

for (const [label, origin] of [
	["AC-A discord thread", { platform: "discord", kind: "thread", conversationId: "thread-1", parentId: "channel-1" }],
	["AC-B slack channel", { platform: "slack", kind: "channel", conversationId: "C1" }],
	["AC-B slack thread", { platform: "slack", kind: "thread", conversationId: "C1:1700000000.000001", parentId: "C1" }],
] as const) {
	test(`${label}: lane report becomes an internal turn; persona reply is the only post`, async () => {
		const channels: GatewayConfig["channels"] =
			origin.platform === "discord"
				? { "channel-1": { engagement: "open" as const, audience: "all" as const } }
				: {
						"slack:C1": { engagement: "open" as const, audience: "all" as const },
						"slack:C1:1700000000.000001": { engagement: "open" as const, audience: "all" as const },
					};
		const f = await fixture({ ownerTarget: origin, channels });
		const persona = await startPersonaTurn(f, origin, `${label}-trigger`);
		const lane = await startLane(f, "worker", persona.sessionId);
		expect(lane.error).toBeUndefined();
		const work = lane.result;
		await settleLane(f, work.opRef, "bounded result");
		const reportRow = await eventually(
			() => f.database.inboundTurnRows(persona.opRef),
			(rows) =>
				rows.some(
					(row) => row.source === "lane_report" && row.message_id === f.database.workAttemptGet(work.opRef)?.reportId,
				),
			"persona lane-report row",
		);
		const reportId = f.database.workAttemptGet(work.opRef)!.reportId;
		expect(reportRow.filter((row) => row.message_id === reportId)).toMatchObject([
			expect.objectContaining({
				source: "lane_report",
				origin_key: originKey(origin),
				turn_role: "steer",
				turn_state: "done",
			}),
		]);
		expect(chatMessages(f)).toHaveLength(0);
		expect(f.port.steers).toHaveLength(1);
		expect(f.port.steers[0]?.text).toContain("Internal lane report that arrived while you were working");
		expect(f.port.steers[0]?.text).not.toContain("Additional message from the user");
		expect(f.port.sendAttempts.some((send) => send.sessionId === work.sessionId)).toBe(true);
		f.port.complete(persona.opRef, "ack");
		await eventually(
			() => chatMessages(f),
			(messages) => messages.length === 1,
			"persona reply post",
		);
		expect(chatMessages(f).map((frame) => frame.payload.text)).toEqual(["ack"]);
		f.client.close();
	});
}

test("AC-A [SILENT] persona answer posts nothing", async () => {
	const origin: OriginRef = {
		platform: "discord",
		kind: "thread",
		conversationId: "thread-silent",
		parentId: "channel-silent",
	};
	const f = await fixture({
		ownerTarget: origin,
		channels: { "channel-silent": { engagement: "open", audience: "all" } },
	});
	const persona = await startPersonaTurn(f, origin, "silent-trigger");
	const lane = await startLane(f, "silent-worker", persona.sessionId);
	await settleLane(f, lane.result.opRef, "work done");
	await eventually(
		() => f.database.inboundTurnRows(persona.opRef),
		(rows) => rows.some((row) => row.source === "lane_report"),
		"internal lane report row",
	);
	f.port.complete(persona.opRef, "[SILENT]");
	await eventually(
		() => f.database.inboundTurnRow(persona.opRef),
		(row) => row?.turn_state === "done",
		"silent persona turn",
	);
	expect(chatMessages(f)).toHaveLength(0);
});

test("AC-C idle persona is woken with one send carrying the internal lane report", async () => {
	const f = await fixture();
	const personaSessionId = crypto.randomUUID();
	await bindPersonaSession(f, rootKey, personaSessionId);
	const lane = await startLane(f, "idle-worker", personaSessionId);
	await settleLane(f, lane.result.opRef, "idle report");
	const reportId = f.database.workAttemptGet(lane.result.opRef)!.reportId;
	const sends = await eventually(
		() => f.port.sends,
		(items) => items.some((send) => send.text.includes("[Internal lane report:")),
		"persona wake send",
	);
	const reportSend = sends.find((send) => send.text.includes("[Internal lane report:"))!;
	expect(reportSend.text).toContain("[lane idle-worker] completed: idle report");
	expect(reportSend.sessionId).toBe(personaSessionId);
	expect(f.database.inboundTurnRows(reportSend.opRef)).toMatchObject([
		expect.objectContaining({ message_id: reportId, source: "lane_report", origin_key: rootKey }),
	]);
	expect(chatMessages(f)).toHaveLength(0);
	f.port.complete(reportSend.opRef, "ack");
	await eventually(
		() => chatMessages(f),
		(messages) => messages.length === 1,
		"woken persona reply",
	);
	expect(chatMessages(f)[0]?.payload.text).toBe("ack");
});

for (const [label, callerSessionId] of [
	["no caller id", undefined],
	["unknown caller id", "unknown-session-id"],
] as const) {
	test(`AC-D ${label} routes to the ownerTarget persona`, async () => {
		const f = await fixture();
		const personaSessionId = crypto.randomUUID();
		await bindPersonaSession(f, rootKey, personaSessionId);
		const lane = await startLane(f, `owner-${label.replaceAll(" ", "-")}`, callerSessionId);
		await settleLane(f, lane.result.opRef, "owner report");
		const runtime = f.database.workAttemptGet(lane.result.opRef)!;
		expect(runtime.parent).toMatchObject({ kind: "persona", originKey: rootKey });
		const report = await eventually(
			() => f.port.sends,
			(sends) => Boolean(sends.find((send) => send.text.includes("[Internal lane report:"))),
			"owner-target report turn",
		);
		const reportSend = report.find((send) => send.text.includes("[Internal lane report:"))!;
		expect(reportSend.sessionId).toBe(personaSessionId);
		expect(chatMessages(f)).toHaveLength(0);
		await silencePersona(f, reportSend.opRef);
	});
}

test("AC-D no ownerTarget settles durably as no_target without inbound row or post", async () => {
	const f = await fixture({ ownerTarget: false });
	const lane = await startLane(f, "status-only");
	await settleLane(f, lane.result.opRef, "no destination");
	const runtime = f.database.workAttemptGet(lane.result.opRef)!;
	expect(runtime).toMatchObject({ parent: null, decision: "no_target" });
	expect(f.database.laneReportsByParent("status-only")).toEqual([]);
	expect(f.database.deliveryRows()).toEqual([]);
	expect(f.database.inboundTurnRows(lane.result.opRef)).toEqual([]);
	expect(chatMessages(f)).toHaveLength(0);
});

test("AC-E work.start from a lane is refused by default before bind, send, or job creation", async () => {
	const f = await fixture();
	const parent = await startLane(f, "parent-lane");
	const binds = f.port.binds.length;
	const sends = f.port.sendAttempts.length;
	const response = await startLane(f, "refused-child", parent.result.sessionId);
	expect(response.error).toMatchObject({
		code: "unauthorized",
		detail: { reasonCode: "nested_lane_forbidden", verb: "start" },
	});
	expect(f.port.binds).toHaveLength(binds);
	expect(f.port.sendAttempts).toHaveLength(sends);
	expect(f.database.laneJobJson("lanejob-" + Buffer.from("refused-child").toString("hex"))).toBeUndefined();
});

test("AC-E work.run from a lane is refused before bind or waiter creation", async () => {
	const f = await fixture();
	const parent = await startLane(f, "run-parent");
	const binds = f.port.binds.length;
	const sends = f.port.sendAttempts.length;
	const response = await f.client.request("work.run", {
		name: "run-child",
		text: "response only",
		cwd: f.home,
		callerSessionId: parent.result.sessionId,
	});
	expect(response.error).toMatchObject({
		code: "unauthorized",
		detail: { reasonCode: "nested_lane_forbidden", verb: "run" },
	});
	expect(f.port.binds).toHaveLength(binds);
	expect(f.port.sendAttempts).toHaveLength(sends);
	expect(f.database.laneJobJson(`lanejob-${Buffer.from("run-child").toString("hex")}`)).toBeUndefined();
});

test("AC-E allowed nested work.run remains response-only", async () => {
	const f = await fixture({ allowNested: true });
	const parent = await startLane(f, "run-parent");
	const responsePromise = f.client.request("work.run", {
		name: "run-child",
		text: "response only",
		cwd: f.home,
		callerSessionId: parent.result.sessionId,
	});
	await eventually(
		() => f.port.sends,
		(sends) => sends.length === 2,
		"nested run send",
	);
	const run = f.port.sends[1]!;
	f.port.complete(run.opRef, "response result");
	const response = await responsePromise;
	expect(response.error).toBeUndefined();
	expect(response.result.text).toBe("response result");
	expect(f.database.workAttemptGet(run.opRef)).toMatchObject({ mode: "run", parent: null, decision: "no_target" });
	expect(f.database.laneReportsByParent("run-parent")).toEqual([]);
	expect(f.database.deliveryRows()).toEqual([]);
	expect(chatMessages(f)).toHaveLength(0);
});

test("AC-F no sessions row falls back with one ledger row and one chat.message", async () => {
	const f = await fixture();
	const lane = await startLane(f, "fallback-worker");
	await settleLane(f, lane.result.opRef, "fallback body");
	const runtime = f.database.workAttemptGet(lane.result.opRef)!;
	expect(runtime.decision).toBe("fallback");
	expect(f.database.deliveryRows()).toHaveLength(1);
	expect(f.database.deliveryRows()[0]?.delivery_id).toBe(runtime.deliveryId);
	expect(f.database.deliveryRows()[0]?.origin_key).toBe(rootKey);
	await eventually(
		() => chatMessages(f),
		(messages) => messages.length === 1,
		"fallback post",
	);
	expect(chatMessages(f)[0]?.payload.text).toBe("[lane fallback-worker] completed: fallback body");
	await f.client.request("work.status", { name: "fallback-worker" });
	expect(f.database.deliveryRows()).toHaveLength(1);
	expect(chatMessages(f)).toHaveLength(1);
});

test("AC-F conflicting inbound id selects fallback atomically", async () => {
	const f = await fixture();
	await bindPersonaSession(f, rootKey);
	const lane = await startLane(f, "conflict-worker");
	const runtime = f.database.workAttemptGet(lane.result.opRef)!;
	expect(
		f.database.inboundEnqueue({
			messageId: runtime.reportId,
			originKey: rootKey,
			originRefJson: JSON.stringify(rootOrigin),
			body: "conflicting platform id",
			source: "platform",
		}),
	).toBe(true);
	await settleLane(f, lane.result.opRef, "conflict fallback");
	expect(f.database.workAttemptGet(lane.result.opRef)?.decision).toBe("fallback");
	expect(f.database.deliveryRows()).toHaveLength(1);
	expect(f.database.deliveryRows()[0]?.delivery_id).toBe(runtime.deliveryId);
	expect(f.database.inboundPendingOldest(rootKey)).toMatchObject({ message_id: runtime.reportId, source: "platform" });
});

test("AC-F a quarantined nonterminal persona turn rejects report admission", async () => {
	const origin: OriginRef = { platform: "discord", kind: "channel", conversationId: "quarantined-parent" };
	const key = originKey(origin);
	const f = await fixture({
		ownerTarget: origin,
		channels: { "quarantined-parent": { engagement: "open", audience: "all" } },
	});
	const personaSessionId = await bindPersonaSession(f, key);
	f.database.inboundEnqueue({
		messageId: "quarantine-human",
		originKey: key,
		originRefJson: JSON.stringify(origin),
		body: "nonterminal parent turn",
	});
	f.database.inboundBindTurn({
		messageId: "quarantine-human",
		originKey: key,
		epoch: 0,
		opRef: "gw-quarantined-persona-turn",
		sessionId: personaSessionId,
	});
	f.database.inboundTurnAccept("gw-quarantined-persona-turn");
	const raw = new Database(join(f.home, "gateway.db"));
	try {
		raw
			.query("INSERT INTO broker_quarantine(kind, subject_id, cutover_id) VALUES ('inbound', ?, 'test-cutover')")
			.run("quarantine-human");
	} finally {
		raw.close();
	}
	const lane = await startLane(f, "quarantine-worker", personaSessionId);
	await settleLane(f, lane.result.opRef, "held report");
	expect(f.database.workAttemptGet(lane.result.opRef)?.decision).toBe("fallback");
	expect(f.database.deliveryRows()).toHaveLength(1);
	expect(f.database.inboundTurnRow("gw-quarantined-persona-turn")).toMatchObject({
		message_id: "quarantine-human",
		turn_state: "accepted",
	});
});

for (const reset of ["/new", "rebindEpoch"] as const) {
	test(`AC-I lane report follows origin after ${reset}`, async () => {
		const f = await fixture({ channels: { "parent-channel": { engagement: "open", audience: "all" } } });
		const persona = await startPersonaTurn(f, rootOrigin, `epoch-human-${reset}`);
		f.port.complete(persona.opRef, "initial response");
		await eventually(
			() => f.database.inboundTurnRow(persona.opRef),
			(row) => row?.turn_state === "done",
			"initial persona turn did not settle",
		);
		const lane = await startLane(f, `epoch-worker-${reset === "/new" ? "new" : "rebind"}`, persona.sessionId);
		if (reset === "/new") {
			const command = await f.client.request("chat.send", {
				origin: rootOrigin,
				text: "/new",
				messageId: `reset-${reset}`,
				engagement: { mentioned: true, group: true, authorId: "human-1" },
			});
			expect(command.error).toBeUndefined();
		} else {
			f.database.rebindEpoch(rootKey);
		}
		const afterReset = f.database.getSessionRecord(rootKey)!;
		expect(afterReset.sessionId).toBe("");
		await settleLane(f, lane.result.opRef, "follows origin");
		const reportId = f.database.workAttemptGet(lane.result.opRef)!.reportId;
		const internalSend = await eventually(
			() => f.port.sends,
			(sends) => Boolean(sends.find((send) => send.text.includes("[Internal lane report:"))),
			"report was not sent to the rebound persona session",
		);
		const reportSend = internalSend.find((send) => send.text.includes("[Internal lane report:"))!;
		expect(reportSend.sessionId).not.toBe(persona.sessionId);
		expect(f.database.getSessionRecord(rootKey)?.epoch).toBe(afterReset.epoch);
		expect(f.database.inboundTurnRows(reportSend.opRef)).toMatchObject([
			expect.objectContaining({ message_id: reportId, source: "lane_report", origin_key: rootKey }),
		]);
		expect(f.database.workAttemptGet(lane.result.opRef)?.decision).toBe("reported");
		await silencePersona(f, reportSend.opRef);
	});
}

test("AC-J closed channel admits an internal report without engagement", async () => {
	const origin: OriginRef = { platform: "discord", kind: "channel", conversationId: "closed-channel" };
	const key = originKey(origin);
	const f = await fixture({
		ownerTarget: origin,
		channels: { "closed-channel": { engagement: "closed", audience: "all" } },
	});
	await bindPersonaSession(f, key);
	const lane = await startLane(f, "closed-worker");
	await settleLane(f, lane.result.opRef, "closed report");
	const reportId = f.database.workAttemptGet(lane.result.opRef)!.reportId;
	const sends = await eventually(
		() => f.port.sends,
		(items) => items.some((send) => send.text.includes("[Internal lane report:")),
		"closed channel did not admit internal report",
	);
	const reportSend = sends.find((send) => send.text.includes("[Internal lane report:"))!;
	expect(f.database.inboundTurnRows(reportSend.opRef)).toMatchObject([
		expect.objectContaining({ message_id: reportId, source: "lane_report", origin_key: key }),
	]);
	expect(f.database.workAttemptGet(lane.result.opRef)?.decision).toBe("reported");
	await silencePersona(f, reportSend.opRef);
});

test("AC-J mention-open internal report does not alter botAudience state", async () => {
	const origin: OriginRef = { platform: "discord", kind: "channel", conversationId: "mention-open-channel" };
	const key = originKey(origin);
	const f = await fixture({
		ownerTarget: origin,
		channels: { "mention-open-channel": { engagement: "mention-open", audience: "all" } },
	});
	await bindPersonaSession(f, key);
	const stateKey = `bot-audience-state:${key}`;
	f.database.metaSet(stateKey, JSON.stringify({ admissions: ["existing-human-admission"], count: 1 }));
	const before = f.database.metaGet(stateKey);
	const lane = await startLane(f, "mention-open-worker");
	await settleLane(f, lane.result.opRef, "mention-open report");
	const sends = await eventually(
		() => f.port.sends,
		(items) => items.some((send) => send.text.includes("[Internal lane report:")),
		"mention-open channel did not admit internal report",
	);
	expect(f.database.metaGet(stateKey)).toBe(before);
	const reportSend = sends.find((send) => send.text.includes("[Internal lane report:"))!;
	await silencePersona(f, reportSend.opRef);
});

test("AC-J chat.send and chat.edit reject internal lane-report ids", async () => {
	const f = await fixture();
	const send = await f.client.request("chat.send", {
		origin: rootOrigin,
		messageId: "lane-report-forbidden",
		text: "forged internal row",
		engagement: { mentioned: true, group: true, authorId: "human-1" },
	});
	expect(send.error).toMatchObject({ code: "invalid_params" });
	const edit = await f.client.request("chat.edit", {
		origin: rootOrigin,
		messageId: "lane-report-forbidden",
		text: "forged edit",
		engagement: { mentioned: true, group: true, authorId: "human-1" },
	});
	expect(edit.error).toMatchObject({ code: "invalid_params" });
	expect(f.database.inboundPendingCount(rootKey)).toBe(0);
	const humanCount = f.database.metaGet(`bot-audience-state:${rootKey}`);
	expect(humanCount).toBeUndefined();
});

test("A03 callerSessionId is an untrusted routing hint on the owner-trusted socket", async () => {
	const f = await fixture({ ownerTarget: false });
	const sessionId = crypto.randomUUID();
	const authority = f.database.inspectBrokerAuthority().authority;
	if (!authority) throw new Error("test broker authority missing");
	f.database.recordOwnedBinding({
		authority,
		sessionId,
		originKey: rootKey,
		epoch: 0,
		repo: join(f.home, "workspace"),
	});
	const lane = await startLane(f, "spoofed-route", sessionId);
	await settleLane(f, lane.result.opRef, "untrusted hint result");
	const runtime = f.database.workAttemptGet(lane.result.opRef)!;
	expect(runtime.parent).toMatchObject({ kind: "persona", originKey: rootKey });
	expect(runtime.decision).toBe("reported");
	const sends = await eventually(
		() => f.port.sends,
		(items) => items.some((send) => send.sessionId === sessionId && send.text.includes("[Internal lane report:")),
		"known presented caller id did not route to its persona origin",
	);
	const internalReport = sends.find(
		(send) => send.sessionId === sessionId && send.text.includes("[Internal lane report:"),
	)!;
	await silencePersona(f, internalReport.opRef);
});
