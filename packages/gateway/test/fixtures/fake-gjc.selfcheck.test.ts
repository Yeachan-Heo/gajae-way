import { expect, test } from "bun:test";
import { createSocketDownFixture } from "../../../adapter-discord/test/fixtures/socket-down";
import { createFakeGjc, parseFakeModes, runFakeGjc, startCapacityExhaustedBroker } from "./fake-gjc.mjs";
import { fakePsTable } from "./fake-ps";

const script = new URL("./fake-gjc.mjs", import.meta.url).pathname;
async function subprocess(mode: string, args: string[]) {
	const child = Bun.spawn([process.execPath, script, ...args], {
		env: { ...process.env, GAJAEWAY_FAKE_GJC_MODES: mode },
		stdout: "pipe",
		stderr: "pipe",
	});
	try {
		const text = await new Response(child.stdout).text();
		expect(await child.exited).toBe(0);
		return JSON.parse(text);
	} finally {
		child.kill();
		await child.exited;
	}
}
const session = (command: string) => ["sdk", "session", command, "session-1", "op-1", "--repo", "/fixture"];

for (const code of ["invalid_input", "invalid_cursor", "cursor_expired", "snapshot_capacity_exceeded"]) {
	test(`fake cursor ${code}`, async () => {
		expect(await subprocess(`cursor:${code}`, [...session("tail"), "--cursor", "old"])).toEqual({
			ok: false,
			error: { code },
		});
	});
}
for (const mode of ["inspect:cwd-locator", "inspect:cwd-mismatch"]) {
	test(`fake ${mode}`, async () => {
		const result = await subprocess(mode, session("inspect"));
		expect(result.result.session).toMatchObject({ live: false, saved: true, deleted: false });
		expect(result.result.session.locator).toEqual({
			cwd: mode.endsWith("mismatch") ? "/fixture/fixture-other-repo" : "/fixture",
			worktreeRoot: mode.endsWith("mismatch") ? "/fixture/fixture-other-repo" : "/fixture",
			stateRoot: mode.endsWith("mismatch") ? "/fixture/fixture-other-repo/.gjc" : "/fixture/.gjc",
		});
		expect(result.result.session).not.toHaveProperty("repo");
		expect((await subprocess(mode, session("list"))).result.sessions).toHaveLength(1);
	});
}
test("fake unknown forever and failed turn", async () => {
	const command = createFakeGjc({ modes: "status:unknown-forever" });
	for (let i = 0; i < 3; i++)
		expect(JSON.parse((await command(session("status")))!.stdout).result.status.status).toBe("unknown");
	const failed = await subprocess("status:failed:provider_rejected", session("status"));
	expect(failed.result.turn.result.error).toEqual({ code: "provider_rejected" });
	expect(failed.result.status.status).toBe("failed");
});
for (const mode of ["status:content", "status:content:truncated"]) {
	test(`fake ${mode}`, async () => {
		expect((await subprocess(mode, session("status"))).result.turn.result.content).toEqual({
			text: "AUTHORITATIVE",
			truncated: mode.endsWith(":truncated"),
		});
	});
}
test("fake hang blocks status for thirty seconds", async () => {
	const start = performance.now();
	expect((await subprocess("status:hang-30s", session("status"))).result.status.status).toBe("unknown");
	expect(performance.now() - start).toBeGreaterThanOrEqual(30_000);
}, 35_000);

test("fake transcript preserves rows and binds continuation to connection", async () => {
	const rows = [
		{ id: "a", ts: "2026-09-05T00:00:00Z", revision: 2, body: "one,two" },
		{ id: "b", ts: "2026-09-05T00:00:01Z", revision: 3 },
	];
	const modes = `transcript:rows=${JSON.stringify(rows)},status:content`;
	expect(parseFakeModes(modes)).toHaveLength(2);
	const command = createFakeGjc({ modes });
	const args = ["sdk", "session", "raw", "query", "session-1", "--query", "transcript.list"];
	const first = JSON.parse((await command(args))!.stdout);
	expect(first.page.items).toEqual([rows[0]]);
	const continued = [...args, "--cursor", first.page.continuationCursor];
	expect(JSON.parse((await command(continued))!.stdout).page).toEqual({ items: [rows[1]], complete: true });
	expect(JSON.parse((await createFakeGjc({ modes })(continued))!.stdout).error.code).toBe("invalid_cursor");
});

test("fake capacity CLI and authenticated endpoint hello", async () => {
	expect((await subprocess("daemon:capacity-exhausted", session("inspect"))).error).toEqual({
		code: "invalid_input",
		message: "session.list cursor capacity is exhausted",
	});
	const broker = startCapacityExhaustedBroker();
	const socket = new WebSocket(`${broker.url}?token=${broker.token}`);
	try {
		await new Promise<void>((resolve, reject) => {
			socket.onerror = reject;
			socket.onmessage = (event) => {
				try {
					const frame = JSON.parse(String(event.data));
					if (frame.type === "broker_hello") {
						expect(frame.protocolVersion).toBe(3);
						socket.send(JSON.stringify({ id: "test", operation: "session.inspect" }));
					} else {
						expect(frame).toMatchObject({
							type: "broker_response",
							id: "test",
							ok: false,
							error: { code: "invalid_input" },
						});
						resolve();
					}
				} catch (error) {
					reject(error);
				}
			};
		});
	} finally {
		socket.close();
		broker.stop();
	}
});

test("fake serve answers query and control over same stdin", async () => {
	const child = Bun.spawn([process.execPath, script, "sdk", "serve", "--stdio"], {
		env: { ...process.env, GAJAEWAY_FAKE_GJC_MODES: "serve:bidirectional,status:content" },
		stdin: "pipe",
		stdout: "pipe",
		stderr: "pipe",
	});
	try {
		child.stdin.write(
			`${JSON.stringify({ type: "query_request", id: "q", query: "turn.result", input: {} })}\n${JSON.stringify({ type: "control_request", id: "c", op: "turn.prompt", input: { clientRef: "ref-1" } })}\n`,
		);
		child.stdin.end();
		const frames = (await new Response(child.stdout).text())
			.trim()
			.split("\n")
			.map((line) => JSON.parse(line));
		expect(await child.exited).toBe(0);
		expect(frames[0]).toMatchObject({
			type: "query_response",
			id: "q",
			ok: true,
			result: { content: { text: "AUTHORITATIVE" } },
		});
		expect(frames[1]).toMatchObject({ type: "control_response", id: "c", ok: true, result: { clientRef: "ref-1" } });
	} finally {
		child.kill();
		await child.exited;
	}
});

test("fake subprocess basic command surfaces", async () => {
	for (const command of ["list", "inspect", "status", "tail", "create", "resume"])
		expect((await subprocess("", session(command))).ok).toBe(true);
	expect(
		JSON.parse((await runFakeGjc(["sdk", "session", "raw", "control", "s", "--op", "model.profile.set"])).stdout).result
			.changed,
	).toBe(true);
});

test("fake ps tags ownership and socket-down uses exact boundary", async () => {
	expect(
		fakePsTable({
			GAJAEWAY_FAKE_PS_ROWS: '[{"pid":12,"ppid":1,"command":"gjc serve"}]',
			GAJAEWAY_FAKE_PS_TAG: "owned",
		}),
	).toContain("gjc serve GAJAEWAY_FAKE_PS_TAG=owned");
	let time = 0;
	const socket = createSocketDownFixture({ modes: "socket-down:45s", now: () => time, connect: () => "connected" });
	await expect(socket.connect()).rejects.toMatchObject({ code: "ECONNREFUSED" });
	time = 44_999;
	await expect(socket.connect()).rejects.toMatchObject({ code: "ECONNREFUSED" });
	time = 45_000;
	expect(await socket.connect()).toBe("connected");
	expect(socket.attemptsAt).toEqual([0, 44_999, 45_000]);
});
