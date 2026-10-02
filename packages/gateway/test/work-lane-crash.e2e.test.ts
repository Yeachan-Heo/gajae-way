import { afterEach, expect, test } from "bun:test";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";
import { type Barrier, eventually, noticeOrigin, WorkFixture } from "./fixtures/work-lane-server";

const fixtures: WorkFixture[] = [];
afterEach(async () => {
	for (const fixture of fixtures.splice(0)) await fixture.cleanup();
});
async function fixture(barrier?: Barrier | { readonly allowNested?: boolean }) {
	const options = typeof barrier === "object" ? barrier : {};
	const barrierPoint = typeof barrier === "string" ? barrier : undefined;
	const fixture = await WorkFixture.create(options);
	fixtures.push(fixture);
	await fixture.start(barrierPoint);
	return fixture;
}
async function start(f: WorkFixture, name = "crash", callerSessionId?: string) {
	const client = await f.connect();
	const response = await client.request("work.start", {
		name,
		text: "fixture-owned work",
		cwd: f.home,
		...(callerSessionId ? { callerSessionId } : {}),
	});
	expect(response.error).toBeUndefined();
	expect(response.result).toMatchObject({ started: true, sessionKey: `work/task/${name}` });
	return { client, receipt: response.result };
}
async function settled(f: WorkFixture) {
	return eventually(
		() => f.snapshot(),
		(snapshot) => snapshot.runtimes.length === 1 && snapshot.runtimes[0].settledAt !== null,
		"durable settlement",
	);
}
function singleEffect(f: WorkFixture, opRef: string, sessionId: string) {
	expect(f.calls("send")).toHaveLength(1);
	expect(f.calls("send")[0]?.input).toMatchObject({ opRef, sessionId });
	expect(f.calls("bind")).toHaveLength(1);
	expect(f.calls("resume")).toHaveLength(0);
	for (const call of [...f.calls("status"), ...f.calls("output")])
		expect(call.input).toMatchObject({ opRef, sessionId });
	for (const call of f.calls("attach")) expect(call.input.sessionId).toBe(sessionId);
}
async function notice(client: Awaited<ReturnType<WorkFixture["connect"]>>) {
	return eventually(() => client.frames.find((frame) => frame.event === "chat.message"), Boolean, "start notification");
}

test("accepted start survives SIGKILL with same live unknown operation, then settles and notifies once logically", async () => {
	const f = await fixture();
	const { receipt } = await start(f);
	expect(f.snapshot().jobs[0].attempts[0].endedAt).toBeUndefined();
	expect(f.snapshot().deliveries).toHaveLength(0);
	await f.kill();
	await f.control(receipt.opRef, { unknown: true, live: "live" });
	await f.start();
	const client = await f.connect();
	expect((await client.request("work.status", { name: "crash" })).result).toMatchObject({
		sessionId: receipt.sessionId,
		attempt: { opRef: receipt.opRef },
		op: { status: "unknown" },
	});
	const overlap = await client.request("work.start", {
		name: "crash",
		text: "must not resend",
		resume: true,
		cwd: f.home,
	});
	expect(overlap.error.detail.reasonCode).toBe("attempt_open");
	expect(f.snapshot().jobs[0].attempts[0].endedAt).toBeUndefined();
	await f.control(receipt.opRef, { unknown: false, terminal: true, text: "same worker final answer" });
	const snapshot = await settled(f);
	expect(snapshot.jobs[0].attempts).toHaveLength(1);
	expect(snapshot.jobs[0].attempts[0].endState).toBe("completed");
	expect(snapshot.deliveries).toHaveLength(1);
	const event = await notice(client);
	expect(event.payload.text).toBe("[lane crash] completed: same worker final answer");
	expect(event.payload.deliveryId).toBe(snapshot.runtimes[0].deliveryId);
	singleEffect(f, receipt.opRef, receipt.sessionId);
}, 30_000);

for (const [live, reason] of [
	["dead", "session_dead"],
	["indeterminate", "recovery_indeterminate"],
	["disowned", "session_disowned"],
] as const) {
	test(`restart ${live} authority holds uncertainty without replacement`, async () => {
		const f = await fixture();
		const { receipt } = await start(f);
		await f.kill();
		await f.control(receipt.opRef, { live, unknown: true });
		await f.start();
		const client = await f.connect();
		const snapshot = await settled(f);
		expect(snapshot.jobs[0].state).toBe("awaiting_operator");
		expect(snapshot.jobs[0].attempts[0].endState).toBe("terminal_uncertain");
		expect(snapshot.runtimes[0].terminal.reasonCode).toBe(reason);
		expect((await notice(client)).payload.text).toContain(`[lane crash] attempt_ended: ${reason}`);
		const held = await client.request("work.start", { name: "crash", text: "not a retry", cwd: f.home });
		expect(held.result).toMatchObject({ started: false, held: true, state: "awaiting_operator" });
		const retired = await client.request("work.retire", { name: "crash" });
		expect(retired.result.retired).toBe(live !== "indeterminate");
		const status = await client.request("work.status", { name: "crash" });
		expect(status.result).toMatchObject({
			state: "awaiting_operator",
			sessionId: live === "indeterminate" ? receipt.sessionId : "",
			attempt: { endState: "terminal_uncertain" },
		});
		singleEffect(f, receipt.opRef, receipt.sessionId);
	}, 30_000);
}

for (const point of ["prepared", "accepted-before-save"] as const) {
	test(`SIGKILL at ${point} never blindly replays dispatch`, async () => {
		const f = await fixture(point);
		const client = await f.connect();
		client.send({
			v: "0.1",
			type: "request",
			id: "start",
			verb: "work.start",
			params: { name: "crash", text: "one dispatch", cwd: f.home },
		});
		await f.barrier(point);
		expect(client.frames.some((frame) => frame.id === "start" && frame.result?.started)).toBe(false);
		const pending = f.snapshot().runtimes[0];
		expect(pending.sendPhase).toBe("prepared");
		await f.kill();
		if (point === "prepared") await f.control(pending.sessionId, { live: "dead" });
		await f.start();
		const next = await f.connect();
		if (point === "accepted-before-save") {
			expect((await next.request("work.status", { name: "crash" })).result.attempt.endedAt).toBeUndefined();
			await f.control(pending.opRef, { terminal: true });
			expect((await settled(f)).jobs[0].attempts[0].endState).toBe("completed");
			singleEffect(f, pending.opRef, pending.sessionId);
		} else {
			expect((await settled(f)).jobs[0].attempts[0].endState).toBe("terminal_uncertain");
			expect(f.calls("send")).toHaveLength(0);
			expect(f.calls("bind")).toHaveLength(1);
			expect(f.calls("resume")).toHaveLength(0);
		}
	}, 30_000);
}

for (const point of [
	"terminal-before",
	"terminal-after",
	"output-claim",
	"settle-before",
	"settle-cas",
	"settle-history",
	"settle-activity",
	"settle-ledger",
	"settle-commit",
] as const) {
	test(`real process crash at ${point}: rollback or one committed obligation, then same-identity recovery`, async () => {
		const f = await fixture(point);
		const { client, receipt } = await start(f);
		await f.control(receipt.opRef, { terminal: true, text: `answer at ${point}` });
		await f.barrier(point);
		// Separate read-only SQLite connection sees only committed effects, even while
		// the child is synchronously blocked between individual settlement writes.
		const before = f.snapshot();
		expect(client.frames.filter((frame) => frame.event === "chat.message")).toHaveLength(0);
		if (point === "settle-commit") {
			expect(before.runtimes[0].decision).toBe("fallback");
			expect(before.sessions[0]?.last_activity_at).toBe(before.runtimes[0].settledAt);
			expect(before.jobs[0].attempts[0].endState).toBe("completed");
			expect(before.deliveries).toHaveLength(1);
		} else {
			expect(before.runtimes[0].settledAt).toBeNull();
			expect(before.runtimes[0].decision).toBe("undecided");
			expect(before.sessions[0]?.last_activity_at).toBe(before.runtimes[0].startedAt);
			expect(before.jobs[0].attempts[0].endedAt).toBeUndefined();
			expect(before.deliveries).toHaveLength(0);
		}
		if (point === "terminal-before") expect(before.runtimes[0].terminal).toBeNull();
		if (point === "terminal-after") expect(before.runtimes[0].terminal.reasonCode).toBe("end_turn");
		if (point === "output-claim") expect(before.runtimes[0].output.reads).toBe(1);
		await f.kill();
		await f.start();
		const replay = await f.connect();
		const after = await settled(f);
		const event = await notice(replay);
		expect(after.jobs[0].attempts).toHaveLength(1);
		expect(after.jobs[0].attempts[0].endState).toBe("completed");
		expect(after.runtimes[0].output.reads).toBeLessThanOrEqual(3);
		expect(after.deliveries).toHaveLength(1);
		expect(event.payload.deliveryId).toBe(before.runtimes[0].deliveryId);
		expect(event.payload.text).toBe(`[lane crash] completed: answer at ${point}`);
		singleEffect(f, receipt.opRef, receipt.sessionId);
	}, 30_000);
}

for (const point of ["settle-before", "settle-report", "settle-commit"] as const) {
	test(`persona parent crash at ${point}: one durable inbound report and one persona send`, async () => {
		const f = await fixture();
		const rootKey = "discord/channel/fixture";
		const rootSessionId = await f.seedPersonaSession();
		const priorPersonaSends = f.calls("send").filter((call) => call.input.sessionId === rootSessionId).length;
		const { client, receipt } = await start(f, "persona-child", rootSessionId);
		await f.armBarrier(point);
		await f.control(receipt.opRef, { terminal: true, text: `persona report at ${point}` });
		await f.barrier(point);
		const before = f.snapshot();
		if (point === "settle-commit") {
			expect(before.inbound.filter((row) => row.source === "lane_report")).toHaveLength(1);
		} else {
			expect(before.inbound.filter((row) => row.source === "lane_report")).toHaveLength(0);
		}
		expect(client.frames.filter((frame) => frame.event === "chat.message")).toHaveLength(0);
		await f.kill();
		await f.start();
		const replay = await f.connect();
		const after = await eventually(
			() => f.snapshot(),
			(snapshot) =>
				snapshot.inbound.some((row) => row.source === "lane_report" && row.message_id === before.runtimes[0].reportId),
			"persona lane report was not admitted after restart",
		);
		expect(
			after.inbound.filter((row) => row.source === "lane_report" && row.message_id === after.runtimes[0].reportId),
		).toHaveLength(1);
		expect(after.deliveries).toHaveLength(0);
		await eventually(
			() => f.calls("send").filter((call) => call.input.sessionId === rootSessionId).length,
			(count) => count === priorPersonaSends + 1,
			"persona actor did not dispatch exactly one internal turn",
		);
		expect(f.calls("send").filter((call) => call.input.sessionId === rootSessionId)).toHaveLength(
			priorPersonaSends + 1,
		);
		expect(
			replay.frames.filter((frame) => frame.event === "chat.message" && frame.payload?.text?.includes("[lane ")),
		).toHaveLength(0);
		client.close();
	}, 30_000);
}

test("lane parent crash after report claim replays the same deterministic wake once", async () => {
	const f = await fixture({ allowNested: true });
	const rootSessionId = await f.seedPersonaSession();
	const parent = await start(f, "parent", rootSessionId);
	await f.control(parent.receipt.opRef, { terminal: true, text: "parent initial result" });
	await eventually(
		() => f.snapshot(),
		(snapshot) =>
			snapshot.runtimes.some((runtime) => runtime.opRef === parent.receipt.opRef && runtime.settledAt !== null),
		"parent lane did not settle",
	);
	const child = await start(f, "child", parent.receipt.sessionId);
	await f.armBarrier("report-claim");
	await f.control(child.receipt.opRef, { terminal: true, text: "child report" });
	await f.barrier("report-claim");
	const claimed = f.snapshot().laneReports.find((row) => row.parent_name === "parent");
	if (!claimed?.claim_ref) throw new Error("parent report claim was not persisted");
	const wakeOpRef = claimed.claim_ref;
	expect(claimed.state).toBe("claimed");
	expect(f.calls("send").some((call) => call.input.opRef === wakeOpRef)).toBe(false);
	await f.kill();
	await f.start();
	const replay = await f.connect();
	const after = await eventually(
		() => f.snapshot(),
		(snapshot) => snapshot.laneReports.find((row) => row.report_id === claimed.report_id)?.state === "consumed",
		"claimed parent wake was not replayed",
	);
	expect(after.laneReports.find((row) => row.report_id === claimed.report_id)).toMatchObject({
		state: "consumed",
		claim_ref: wakeOpRef,
		consumed_op_ref: wakeOpRef,
	});
	expect(f.calls("send").filter((call) => call.input.opRef === wakeOpRef)).toHaveLength(1);
	expect(
		replay.frames.filter((frame) => frame.event === "chat.message" && frame.payload?.text?.startsWith("[lane ")),
	).toHaveLength(0);
}, 30_000);

for (const point of ["prepared", "accepted-before-save", "report-consume"] as const) {
	test(`lane parent crash at ${point} is replayed by proof with one stable wake identity`, async () => {
		const f = await fixture({ allowNested: true });
		const rootSessionId = await f.seedPersonaSession();
		const parent = await start(f, "parent", rootSessionId);
		await f.control(parent.receipt.opRef, { terminal: true, text: "parent initial result" });
		await eventually(
			() => f.snapshot(),
			(snapshot) =>
				snapshot.runtimes.some((runtime) => runtime.opRef === parent.receipt.opRef && runtime.settledAt !== null),
			"parent lane did not settle",
		);
		const child = await start(f, "child", parent.receipt.sessionId);
		await f.armBarrier(point);
		await f.control(child.receipt.opRef, { terminal: true, text: "child report" });
		await f.barrier(point);
		const before = f.snapshot();
		const childReport = before.laneReports.find((row) => row.parent_name === "parent");
		if (!childReport?.claim_ref) throw new Error("child lane report claim was not persisted");
		const wakeOpRef = childReport.claim_ref;
		expect(childReport.state).toBe("claimed");
		const beforeWakeCalls = f.calls("send").filter((call) => call.input.opRef === wakeOpRef).length;
		expect(beforeWakeCalls).toBe(point === "prepared" ? 0 : 1);
		await f.kill();
		if (point === "prepared") await f.control(parent.receipt.opRef, { live: "dead", unknown: true });
		if (point === "accepted-before-save") await f.persistAcceptedSend(wakeOpRef);
		if (point !== "prepared") await f.control(wakeOpRef, { terminal: true, text: "wake completed" });
		await f.start();
		const replay = await f.connect();
		const after = await eventually(
			() => f.snapshot(),
			(snapshot) => {
				const row = snapshot.laneReports.find((report) => report.report_id === childReport.report_id);
				return point === "prepared" ? row?.state === "held" : row?.state === "consumed";
			},
			"lane parent wake did not reach its proof-backed state",
		);
		const finalRow = after.laneReports.find((row) => row.report_id === childReport.report_id)!;
		if (point === "prepared") {
			expect(finalRow).toMatchObject({ state: "held", hold_reason: "wake_acceptance_uncertain" });
			expect(f.calls("send").filter((call) => call.input.opRef === wakeOpRef)).toHaveLength(0);
		} else {
			expect(finalRow).toMatchObject({ state: "consumed", claim_ref: wakeOpRef, consumed_op_ref: wakeOpRef });
			expect(f.calls("send").filter((call) => call.input.opRef === wakeOpRef)).toHaveLength(1);
		}
		expect(after.runtimes.filter((runtime) => runtime.opRef === wakeOpRef)).toHaveLength(1);
		expect(
			replay.frames.filter((frame) => frame.event === "chat.message" && frame.payload?.text?.startsWith("[lane ")),
		).toHaveLength(0);
	}, 30_000);
}

test("nested run refusal survives gateway restart without binding or sending", async () => {
	const f = await fixture();
	const parent = await start(f, "nested-parent");
	const sends = f.calls("send").length;
	const binds = f.calls("bind").length;
	const first = await parent.client.request("work.run", {
		name: "nested-child",
		text: "must not run",
		cwd: f.home,
		callerSessionId: parent.receipt.sessionId,
	});
	expect(first.error).toMatchObject({
		code: "unauthorized",
		detail: { reasonCode: "nested_lane_forbidden", verb: "run" },
	});
	expect(f.calls("send")).toHaveLength(sends);
	expect(f.calls("bind")).toHaveLength(binds);
	await f.kill();
	await f.start();
	const replay = await f.connect();
	const second = await replay.request("work.run", {
		name: "nested-child",
		text: "must not run after restart",
		cwd: f.home,
		callerSessionId: parent.receipt.sessionId,
	});
	expect(second.error).toMatchObject({
		code: "unauthorized",
		detail: { reasonCode: "nested_lane_forbidden", verb: "run" },
	});
	expect(f.calls("send")).toHaveLength(sends);
	expect(f.calls("bind")).toHaveLength(binds);
	expect(f.snapshot().jobs.some((job) => job.lane_key === "work-nested-child")).toBe(false);
});
test("three claimed post-terminal reads stay consumed across three process crashes", async () => {
	const f = await fixture("output-claim");
	const { receipt } = await start(f);
	await f.control(receipt.opRef, { terminal: true, unavailable: true });
	for (let reads = 1; reads <= 3; reads++) {
		await f.barrier("output-claim");
		expect(f.snapshot().runtimes[0].output.reads).toBe(reads);
		expect(f.snapshot().runtimes[0].settledAt).toBeNull();
		await f.kill();
		await f.start(reads < 3 ? "output-claim" : undefined);
	}
	const client = await f.connect();
	const snapshot = await settled(f);
	expect(snapshot.runtimes[0].output).toMatchObject({ reads: 3, disposition: "unavailable" });
	expect(snapshot.jobs[0].attempts[0].endState).toBe("completed");
	expect((await notice(client)).payload.text).toBe("[lane crash] completed: output_unavailable");
	// Each claim was killed before I/O, so recovery may not reset the counter
	// and sneak in a fourth read merely because no earlier read returned.
	expect(f.calls("output")).toHaveLength(0);
	singleEffect(f, receipt.opRef, receipt.sessionId);
}, 45_000);

for (const [scenario, text] of [
	["bare NO_REPLY", "NO_REPLY"],
	["preamble and SILENT marker", "Nothing to add.\n\n[SILENT]"],
	["SILENT marker beyond the persisted excerpt", "Nothing to add.\n\n" + "界".repeat(800) + "\n\n[SILENT]"],
] as const) {
	test(`original final silence (${scenario}) survives proof commit crash and unavailable full output on restart`, async () => {
		const f = await fixture("silence-after");
		const { client: original, receipt } = await start(f);
		await f.control(receipt.opRef, { terminal: true, text });
		await f.barrier("silence-after");
		const snapshot = f.snapshot();
		const before = snapshot.runtimes[0];
		expect(before.output.knownSilence).toMatchObject({
			opRef: receipt.opRef,
			sessionId: receipt.sessionId,
			source: "turn.result",
			fullness: "original",
			byteLength: Buffer.byteLength(text, "utf8"),
		});
		expect(before.output.proof).toEqual(before.output.knownSilence);
		expect(before.output.disposition).toBe("silent");
		expect(before.settledAt).toBeNull();
		expect(snapshot.deliveries).toHaveLength(0);
		expect(original.frames.filter((frame) => frame.event === "chat.message")).toHaveLength(0);
		if (Buffer.byteLength(text, "utf8") > 2048) {
			// The original body proves silence even though clipping removes the marker.
			expect(typeof before.output.excerpt).toBe("string");
			expect(Buffer.byteLength(before.output.excerpt, "utf8")).toBeLessThanOrEqual(2048);
			expect(before.output.excerpt).toStartWith("Nothing to add.");
			expect(before.output.excerpt).not.toContain("[SILENT]");
		}
		await f.kill();
		await f.control(receipt.opRef, { unavailable: true, live: "dead", unknown: true });
		await f.start();
		const client = await f.connect();
		const after = await settled(f);
		expect(after.runtimes[0].decision).toBe("suppressed");
		expect(after.runtimes[0].output.knownSilence).toEqual(before.output.knownSilence);
		expect(after.runtimes[0].output.excerpt).toBe(before.output.excerpt);
		expect(after.deliveries).toHaveLength(0);
		await client.request("work.status", { name: "crash" });
		expect(client.frames.filter((frame) => frame.event === "chat.message")).toHaveLength(0);
		singleEffect(f, receipt.opRef, receipt.sessionId);
	}, 30_000);
}

test("delivery replay keeps one logical identity; confirm and real ledger pruning never recreate notice", async () => {
	const f = await fixture();
	const { client, receipt } = await start(f);
	await f.control(receipt.opRef, { terminal: true });
	await settled(f);
	const original = await notice(client);
	await f.kill();
	await f.start();
	const replay = await f.connect();
	const duplicate = await notice(replay);
	expect(duplicate.payload).toMatchObject({
		deliveryId: original.payload.deliveryId,
		redelivered: true,
		duplicateWarning: true,
	});
	// The physical event is intentionally allowed twice, but only one logical ID exists.
	expect(new Set([original.payload.deliveryId, duplicate.payload.deliveryId]).size).toBe(1);
	expect((await replay.request("delivery.confirm", { deliveryId: original.payload.deliveryId })).error).toBeUndefined();
	await f.kill();
	const database = await GatewayDatabase.open(`${f.home}/gateway.db`);
	try {
		expect(new DeliveryLedger(database).prune(0, Date.now() + 1_000)).toBe(1);
	} finally {
		database.close();
	}
	await f.start();
	const final = await f.connect();
	await final.request("work.status", { name: "crash" });
	await final.request("gateway.status");
	expect(f.snapshot().runtimes[0].decision).toBe("fallback");
	expect(f.snapshot().deliveries).toHaveLength(0);
	expect(final.frames.filter((frame) => frame.event === "chat.message")).toHaveLength(0);
	singleEffect(f, receipt.opRef, receipt.sessionId);
}, 30_000);
