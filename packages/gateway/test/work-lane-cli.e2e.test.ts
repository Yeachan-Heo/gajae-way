import { afterEach, expect, test } from "bun:test";
import { join } from "node:path";
import { ProtocolError, type WorkStartResult, type WorkStatusResult } from "@gajae-gateway/protocol";
import { GajaewayClient } from "../../sdk/src/client";
import { eventually, repository, WorkFixture } from "./fixtures/work-lane-server";

const fixtures: WorkFixture[] = [];
const children = new Set<ReturnType<typeof Bun.spawn>>();
afterEach(async () => {
	for (const child of children) {
		child.kill("SIGKILL");
		await child.exited;
	}
	children.clear();
	for (const fixture of fixtures.splice(0)) await fixture.cleanup();
});
async function fixture() {
	const f = await WorkFixture.create();
	fixtures.push(f);
	await f.start();
	return f;
}

/** Executes the real CLI entrypoint. This same command may be run under a PTY. */
function launch(f: WorkFixture, args: string[]) {
	const child = Bun.spawn(
		[process.execPath, join(repository, "packages/cli/src/main.ts"), "--socket", f.socket, "work", ...args],
		{
			cwd: repository,
			env: { ...process.env, GAJAEWAY_HOME: f.home, GJCHOME: f.home },
			stdout: "pipe",
			stderr: "pipe",
		},
	);
	children.add(child);
	let done = false;
	const timeout = setTimeout(() => child.kill("SIGKILL"), 15_000);
	const result = Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited])
		.then(([stdout, stderr, code]) => ({ stdout, stderr, code }))
		.finally(() => {
			done = true;
			clearTimeout(timeout);
			children.delete(child);
		});
	return { child, result, done: () => done };
}
async function cli(f: WorkFixture, args: string[]) {
	return launch(f, args).result;
}
async function status(f: WorkFixture, name: string) {
	const output = await cli(f, ["status", name]);
	expect(output.code).toBe(0);
	expect(output.stderr).toBe("");
	return JSON.parse(output.stdout);
}

test("live CLI start/status/steer/jobs/retire: early receipt, open refusal, exact correlation and parent fallback", async () => {
	const f = await fixture();
	const observer = await f.connect();
	const started = await cli(f, ["start", "cli", "--cwd", f.home, "fixture work"]);
	expect(started.code).toBe(0);
	expect(started.stderr).toBe("");
	expect(started.stdout).toMatch(/^started: work\/task\/cli session=\S+ job=\S+ op=\S+\n$/);
	const pending = await status(f, "cli");
	expect(pending.attempt.endedAt).toBeUndefined();
	expect(pending.op.status).toBe("in_flight");
	expect(started.stdout).toContain(`session=${pending.sessionId} job=${pending.jobId} op=${pending.attempt.opRef}`);
	expect(f.snapshot().deliveries).toHaveLength(0);
	const beforeStatus = f.snapshot();
	const attachCount = f.calls("attach").length;
	await status(f, "cli");
	expect(f.snapshot()).toEqual(beforeStatus);
	expect(f.calls("attach")).toHaveLength(attachCount);
	const overlap = await cli(f, ["start", "cli", "--cwd", f.home, "--resume", "duplicate"]);
	expect(overlap.code).toBe(1);
	expect(overlap.stdout).toBe("");
	expect(overlap.stderr).toContain("attempt already open; use work.steer or wait");
	const retirement = await cli(f, ["retire", "cli"]);
	expect(retirement.stdout).toContain("not retired:");
	const steered = await cli(f, ["steer", "cli", "focus", "tests"]);
	expect(steered.code).toBe(0);
	expect(steered.stderr).toBe("");
	expect(steered.stdout).toBe(`steered: ${f.calls("steer")[0]?.input.clientRef}\n`);
	expect(f.calls("steer")[0]?.input).toMatchObject({ sessionId: pending.sessionId, text: "focus tests" });
	await f.control(pending.attempt.opRef, { refuseSteer: true });
	const refused = await cli(f, ["steer", "cli", "not accepted"]);
	expect(refused.code).toBe(1);
	expect(refused.stdout).toMatch(/^not steered: steer_refused:(busy|sdk_refused)\n$/);
	const jobs = await cli(f, ["jobs"]);
	expect(jobs.code).toBe(0);
	expect(jobs.stdout).toContain(`session=${pending.sessionId}`);
	expect(jobs.stdout).toContain(f.home);
	await f.control(pending.attempt.opRef, { terminal: true, text: "CLI final answer" });
	await eventually(
		() => f.snapshot(),
		(value) => value.runtimes[0]?.settledAt !== null,
		"CLI start settlement",
	);
	const final = await status(f, "cli");
	expect(final.attempt.endState).toBe("completed");
	const event = await eventually(
		() => observer.frames.find((frame) => frame.event === "chat.message"),
		Boolean,
		"CLI start event",
	);
	expect(event.payload).toMatchObject({
		text: "[lane cli] completed: CLI final answer",
		origin: { platform: "discord", kind: "channel", conversationId: "fixture" },
	});
	expect(f.calls("send")).toHaveLength(1);
	expect(f.calls("bind")).toHaveLength(1);
	expect(f.calls("resume")).toHaveLength(0);
}, 40_000);

test("run --notify is a usage error", async () => {
	const f = await fixture();
	const invalid = await cli(f, ["run", "invalid", "--notify", "discord/channel/fixture", "forbidden"]);
	expect(invalid.code).toBe(1);
	expect(invalid.stderr).toContain("usage: gajaeway work");
	expect(f.calls("send")).toHaveLength(0);
});

test("actual CLI run waits independently while status/steer work and returns original output", async () => {
	const f = await fixture();
	const observer = await f.connect();
	const run = launch(f, ["run", "waiting", "--cwd", f.home, "long fixture work"]);
	await eventually(
		() => f.calls("send"),
		(calls) => calls.length === 1,
		"CLI run acceptance",
	);
	const pending = await status(f, "waiting");
	expect(run.done()).toBe(false);
	expect(pending.attempt.endedAt).toBeUndefined();
	const steer = await cli(f, ["steer", "waiting", "continue"]);
	expect(steer.code).toBe(0);
	expect(steer.stdout).toContain("steered:");
	expect(run.done()).toBe(false);
	expect(f.calls("send")).toHaveLength(1);
	await f.control(pending.attempt.opRef, { terminal: true, text: "original CLI run answer" });
	const answer = await run.result;
	expect(answer).toEqual({ code: 0, stdout: "original CLI run answer\n", stderr: "" });
	expect((await status(f, "waiting")).attempt.endState).toBe("completed");
	expect(f.snapshot().runtimes[0]).toMatchObject({ mode: "run", parent: null, decision: "no_target" });
	expect(f.snapshot().deliveries).toHaveLength(0);
	expect(observer.frames.filter((frame) => frame.event === "chat.message")).toHaveLength(0);
}, 40_000);

test("killing only the CLI waiter leaves its run observable and notification-free across gateway crash", async () => {
	const f = await fixture();
	const run = launch(f, ["run", "detached", "--cwd", f.home, "caller-independent task"]);
	await eventually(
		() => f.calls("send"),
		(calls) => calls.length === 1,
		"detached run acceptance",
	);
	const pending = await status(f, "detached");
	run.child.kill("SIGKILL");
	await run.result;
	expect((await status(f, "detached")).attempt.endedAt).toBeUndefined();
	await f.kill();
	await f.start();
	const observer = await f.connect();
	expect((await status(f, "detached")).attempt).toMatchObject({ opRef: pending.attempt.opRef });
	await f.control(pending.attempt.opRef, { terminal: true, text: "answer after caller left" });
	await eventually(
		() => f.snapshot(),
		(snapshot) => snapshot.runtimes[0]?.settledAt !== null,
		"detached run completion",
	);
	expect((await status(f, "detached")).attempt.endState).toBe("completed");
	expect(f.snapshot().deliveries).toHaveLength(0);
	expect(f.snapshot().runtimes[0].decision).toBe("no_target");
	expect(observer.frames.filter((frame) => frame.event === "chat.message")).toHaveLength(0);
	expect(f.calls("send")).toHaveLength(1);
	expect(f.calls("bind")).toHaveLength(1);
	expect(f.calls("resume")).toHaveLength(0);
}, 40_000);

test("a caller timeout during bind can precede admission and a same-name start queues behind it", async () => {
	const f = await fixture();
	const gate = f.armBindGate();
	const first = launch(f, ["start", "delayed", "--cwd", f.home, "first private prompt"]);
	const arrival = await f.bindGateArrival(gate);
	expect(arrival.originKey).toBe("work/task/delayed");
	first.child.kill("SIGKILL");
	const timedOut = await first.result;
	expect(timedOut.stdout).toBe("");
	expect(timedOut.code).not.toBe(0);

	const unknown = await cli(f, ["status", "delayed"]);
	expect(unknown.code).toBe(1);
	expect(unknown.stdout).toBe("");
	expect(unknown.stderr).toContain("unknown work lane");
	expect(unknown.stderr).toContain("Do not blindly retry, even with the same name");
	expect(f.snapshot().jobs).toHaveLength(0);
	expect(f.snapshot().runtimes).toHaveLength(0);
	expect(f.calls("bind")).toHaveLength(0);
	expect(f.calls("send")).toHaveLength(0);

	const second = launch(f, ["start", "delayed", "--cwd", f.home, "second private prompt"]);
	expect((await f.bindGateContender(gate)).originKey).toBe(arrival.originKey);
	expect(second.done()).toBe(false);
	expect(f.calls("bind")).toHaveLength(0);
	expect(f.calls("send")).toHaveLength(0);
	f.releaseBindGate();
	const refused = await second.result;
	expect(refused.code).toBe(1);
	expect(refused.stdout).toBe("");
	expect(refused.stderr).toContain("attempt already open; use work.steer or wait");
	const pending = await status(f, "delayed");
	expect(pending.attempt.endedAt).toBeUndefined();
	expect(pending.op.status).toBe("in_flight");
	expect(f.calls("bind")).toHaveLength(1);
	expect(f.calls("send")).toHaveLength(1);
	expect(f.calls("send")[0]?.input).toMatchObject({ opRef: pending.attempt.opRef, text: "first private prompt" });

	await f.kill();
	await f.start();
	expect((await status(f, "delayed")).attempt.opRef).toBe(pending.attempt.opRef);
	await f.control(pending.attempt.opRef, { terminal: true, text: "late answer" });
	const settled = await eventually(
		() => f.snapshot(),
		(snapshot) => snapshot.runtimes[0]?.settledAt != null,
		"delayed start settlement after restart",
	);
	expect(settled.runtimes).toHaveLength(1);
	expect(settled.runtimes[0].output.excerpt).toBe("late answer");
	const final = await status(f, "delayed");
	expect(final.attempt).toMatchObject({ opRef: pending.attempt.opRef, endState: "completed" });
	expect(f.calls("bind")).toHaveLength(1);
	expect(f.calls("send")).toHaveLength(1);
	expect(f.calls("resume")).toHaveLength(0);
	expect(`${timedOut.stderr}${unknown.stderr}${refused.stderr}${JSON.stringify(final)}`).not.toContain(
		"private prompt",
	);
	expect(JSON.stringify(f.snapshot().deliveries)).not.toContain("private prompt");
	expect(JSON.stringify(f.calls("send"))).not.toContain("second private prompt");
}, 40_000);

test("SDK request timeout does not cancel a gated start over the real socket", async () => {
	const f = await fixture();
	const client = await GajaewayClient.connectSocket(f.socket, { requestTimeoutMs: 1_000 });
	try {
		const gate = f.armBindGate();
		const request = client
			.request<WorkStartResult>("work.start", {
				name: "sdk-timeout",
				cwd: f.home,
				text: "delayed SDK work",
			})
			.then(
				(result) => ({ result }),
				(error: unknown) => ({ error }),
			);
		expect((await f.bindGateArrival(gate)).originKey).toBe("work/task/sdk-timeout");
		const outcome = await request;
		expect(outcome).toHaveProperty("error");
		const error = "error" in outcome ? outcome.error : undefined;
		expect(error).toBeInstanceOf(ProtocolError);
		expect(error).toMatchObject({ code: "verb_failed", message: "request timed out after 1000ms" });

		const unknown = await client.request<WorkStatusResult>("work.status", { name: "sdk-timeout" }).then(
			(result) => ({ result }),
			(error: unknown) => ({ error }),
		);
		expect(unknown).toHaveProperty("error");
		const statusError = "error" in unknown ? unknown.error : undefined;
		expect(statusError).toBeInstanceOf(ProtocolError);
		expect(statusError).toMatchObject({
			code: "invalid_params",
			message: "unknown work lane",
			detail: { reasonCode: "unknown_work_lane", name: "sdk-timeout" },
		});
		expect(f.snapshot().jobs).toHaveLength(0);
		expect(f.calls("send")).toHaveLength(0);
		f.releaseBindGate();
		await eventually(
			() => f.calls("send"),
			(calls) => calls.length === 1,
			"timed-out SDK start acceptance",
		);
		const pending = await eventually(
			() => client.request<WorkStatusResult>("work.status", { name: "sdk-timeout" }),
			(value) => value.op?.status === "in_flight",
			"timed-out SDK start in flight",
		);
		expect(pending.attempt?.endedAt).toBeUndefined();
		expect(pending.op?.status).toBe("in_flight");
		expect(f.calls("bind")).toHaveLength(1);
		expect(f.calls("send")).toHaveLength(1);
		expect(f.calls("send")[0]?.input).toMatchObject({ opRef: pending.attempt?.opRef, text: "delayed SDK work" });
	} finally {
		f.releaseBindGate();
		await client.close();
	}
}, 40_000);

test("a same-name start after terminal settlement creates a distinct operation", async () => {
	const f = await fixture();
	const client = await GajaewayClient.connectSocket(f.socket, { requestTimeoutMs: 5_000 });
	try {
		const params = { name: "settled-retry", cwd: f.home, text: "fixture work" };
		const first = await client.request<WorkStartResult>("work.start", params);
		expect(first.started).toBe(true);
		if (!first.started) throw new Error("first fixture start was held");
		await f.control(first.opRef, { terminal: true, text: "first completed" });
		await eventually(
			() => f.snapshot(),
			(snapshot) => snapshot.runtimes[0]?.settledAt != null,
			"first same-name start settlement",
		);
		const settled = await client.request<WorkStatusResult>("work.status", { name: params.name });
		expect(settled.attempt).toMatchObject({ opRef: first.opRef, endState: "completed" });
		// This replay is deliberate test-only evidence: a name is not an idempotency key.
		const second = await client.request<WorkStartResult>("work.start", params);
		expect(second.started).toBe(true);
		if (!second.started) throw new Error(`second fixture start was held: ${second.state}`);
		expect(second.opRef).not.toBe(first.opRef);
		expect(second.jobId).toBe(first.jobId);
		expect(f.calls("send")).toHaveLength(2);
		expect(f.calls("send").map((call) => call.input.opRef)).toEqual([first.opRef, second.opRef]);
	} finally {
		await client.close();
	}
}, 40_000);
