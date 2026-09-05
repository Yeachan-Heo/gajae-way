/**
 * Plan I2/I3 acceptance (stage-05-final S6): the parts red 1 / red 5 do not cover.
 * - I2: 100 turns with an unusable tail cursor never rotate the epoch, never raise
 *   InboundTurnConflictError, never log `persona actor ... failed`.
 * - I3: locator normalization table (cwd / repo / neither / mismatch); restart drill
 *   with five bound origins under the Q4 default (live indexed hosts untouched, dead
 *   saved hosts resumed, zero rotations, identical session ids); a transient inspect
 *   failure keeps the binding; boot ordering pins 5a6ff9f (preflight is version-only,
 *   no agent-dir-bound session CLI before the supervisor's own reap/seed step).
 */
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { canonicalSessionPath, normalizeSession } from "@gajaeway/subsession";
import { MIN_GJC_VERSION } from "../src/orchestrator/broker";
import { bootGateway } from "../src/boot";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { GatewayDatabase, InboundTurnConflictError } from "../src/store/db";
import { createFakeGjc, runFakeGjc } from "./fixtures/fake-gjc.mjs";
import { eventually, harness, KEY } from "./red-first-harness";
import { ScriptedSessionPort } from "./session-port.fake";

test("I2: one hundred inbound rows over an unusable cursor produce zero rotations, zero conflicts, zero actor failures", async () => {
	const port = new ScriptedSessionPort();
	const h = await harness(port);
	const fake = createFakeGjc({ modes: "cursor:cursor_expired" });
	const tails = new TailRunner({ run: (args) => runFakeGjc(args, fake), repo: h.repo });
	port.attachTail = tails.attach.bind(tails);
	try {
		const binding = await port.bind({ originKey: KEY, epoch: 0, repo: h.repo });
		h.database.putSession(KEY, binding.sessionId);
		const conflicts: unknown[] = [];
		// The scripted port never terminates a turn, so row 0 is the prompt and rows
		// 1..99 are admitted as steers into it. Pre-I2 the stale cursor stranded the
		// bound trigger and every later row raised InboundTurnConflictError.
		for (let turn = 0; turn < 100; turn++) {
			h.database.tailCursorCommit(binding.sessionId, `stale-${turn}`);
			h.enqueue(`turn-${turn}`);
			try {
				await h.manager.notifyInbound(KEY);
			} catch (error) {
				if (error instanceof InboundTurnConflictError) conflicts.push(error);
				else throw error;
			}
		}
		await eventually(() => port.sends.length + port.steers.length >= 100, "rows were not admitted", 10_000);
		expect(conflicts).toEqual([]);
		expect(port.sends).toHaveLength(1);
		expect(port.steers).toHaveLength(99);
		expect(h.database.listEpochMutations({ sinceMs: 0 })).toEqual([]);
		expect(h.database.getSessionRecord(KEY)?.epoch).toBe(0);
		expect(h.logs.filter((line) => line.includes("persona actor") && line.includes("failed"))).toEqual([]);
		expect(
			h.logs.some((line) => line.startsWith("tail_cursor_discarded session=") && line.includes("code=cursor_expired")),
		).toBe(true);
	} finally {
		await h.close();
	}
});

test("I3: locator normalization is canonical, cwd-first, and fails closed without location", () => {
	const cwd = `${tmpdir()}/./ws/../ws`;
	const canonical = canonicalSessionPath(cwd);
	expect(
		normalizeSession({ sessionId: "s", locator: { cwd, worktreeRoot: cwd, stateRoot: `${cwd}/.gjc` } }),
	).toMatchObject({
		sessionId: "s",
		repo: canonical,
		worktreeRoot: canonical,
		stateRoot: canonicalSessionPath(`${cwd}/.gjc`),
		live: false,
		deleted: false,
	});
	// Legacy rows that only carry `repo` still resolve to the same canonical cwd.
	expect(normalizeSession({ sessionId: "s", locator: { repo: cwd } })?.repo).toBe(canonical);
	// cwd wins over repo when both exist (cwd is the supported DTO field).
	expect(normalizeSession({ sessionId: "s", locator: { cwd, repo: "/elsewhere" } })?.repo).toBe(canonical);
	// Neither -> undefined (fail closed: no authority, no resume, no rotation).
	expect(normalizeSession({ sessionId: "s", locator: {} })).toBeUndefined();
	expect(normalizeSession({ sessionId: "s" })).toBeUndefined();
	expect(normalizeSession({ locator: { cwd } })).toBeUndefined();
});

test("I3: restart drill with five bound origins keeps live hosts, resumes dead ones, rotates nothing", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-restart-drill-"));
	const repo = join(home, "workspace");
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const logs: string[] = [];
	const log = console.error;
	console.error = (line: unknown) => {
		logs.push(String(line));
	};
	try {
		const origins = ["a", "b", "c", "d", "e"].map((id) => `discord/channel/${id}`);
		// Two origins are backed by hosts the index still reports live; three are
		// saved-but-dead and must be resumed in place.
		const liveOrigins = new Set(origins.slice(0, 2));
		const resumes: string[] = [];
		// One fake per session so a resume flips liveness for that session only.
		const fakes = new Map<string, ReturnType<typeof createFakeGjc>>();
		const fakeFor = (sessionId: string) => {
			let fake = fakes.get(sessionId);
			if (!fake) fakes.set(sessionId, (fake = createFakeGjc({ modes: "inspect:cwd-locator" })));
			return fake;
		};
		const cli = async (args: readonly string[]) => {
			const control = args.indexOf("control");
			const sessionId = control >= 0 ? args[control + 1] : args[3];
			if (args[2] === "inspect" && [...liveOrigins].some((origin) => sessionId === `session-${origin}`)) {
				return {
					exitCode: 0,
					stdout: JSON.stringify({
						ok: true,
						result: {
							session: {
								sessionId,
								live: true,
								saved: true,
								deleted: false,
								locator: { cwd: repo, worktreeRoot: repo, stateRoot: `${repo}/.gjc` },
							},
						},
					}),
					stderr: "",
				};
			}
			if (args.includes("session.resume")) resumes.push(String(sessionId));
			return (await runFakeGjc([...args.slice(0, 4), "--repo", repo, ...args.slice(4)], fakeFor(String(sessionId))))!;
		};
		const port = new BrokerSessionPort({
			database,
			cli,
			instanceId: "drill",
			tailRunner: new TailRunner({ run: cli, repo }),
		});
		for (const origin of origins) database.putSession(origin, `session-${origin}`);
		const before = origins.map((origin) => database.getSessionRecord(origin)!);
		const bindings = [];
		for (const origin of origins) bindings.push(await port.bind({ originKey: origin, epoch: 0, repo }));
		const after = origins.map((origin) => database.getSessionRecord(origin)!);
		expect(bindings.map((binding) => binding.sessionId)).toEqual(before.map((row) => row.sessionId));
		expect(after.map((row) => row.sessionId)).toEqual(before.map((row) => row.sessionId));
		expect(after.map((row) => row.epoch)).toEqual([0, 0, 0, 0, 0]);
		expect(database.listEpochMutations({ sinceMs: 0 })).toEqual([]);
		expect(resumes.sort()).toEqual(
			origins
				.slice(2)
				.map((origin) => `session-${origin}`)
				.sort(),
		);
		expect(logs.filter((line) => line.includes("session_resumed reason=idle_dead_binding"))).toHaveLength(3);
		expect(logs.filter((line) => line.includes("session_rebound"))).toEqual([]);
	} finally {
		console.error = log;
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("I3: a transient inspect failure keeps the binding and is logged distinctly, never rotated", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-inspect-transient-"));
	const repo = join(home, "workspace");
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const logs: string[] = [];
	const log = console.error;
	console.error = (line: unknown) => {
		logs.push(String(line));
	};
	try {
		const cli = async () => ({ exitCode: 1, stdout: "", stderr: "gjc command timed out after 10000ms" });
		const port = new BrokerSessionPort({
			database,
			cli,
			instanceId: "transient",
			tailRunner: new TailRunner({ run: cli, repo }),
		});
		database.putSession(KEY, "saved-session");
		await expect(port.bind({ originKey: KEY, epoch: 0, repo })).rejects.toThrow();
		expect(database.getSessionRecord(KEY)).toMatchObject({ sessionId: "saved-session", epoch: 0 });
		expect(database.listEpochMutations({ sinceMs: 0 })).toEqual([]);
		expect(logs.some((line) => line.startsWith(`session_inspect_transient origin=${KEY}`))).toBe(true);
		expect(logs.filter((line) => line.includes("session_rebound"))).toEqual([]);
	} finally {
		console.error = log;
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("boot ordering (5a6ff9f): preflight is version-only; no agent-dir session CLI runs before the supervisor owns the dir", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-boot-order-"));
	const commands: string[][] = [];
	try {
		// A version below the floor aborts boot at preflight. Every CLI call observed
		// up to that abort is the complete pre-reap/pre-seed surface: it must be the
		// bare `--version` only, never an agent-dir-bound `sdk session ...` call.
		await expect(
			bootGateway({
				home,
				broker: {
					ssotAgentDir: null,
					command: async (args) => {
						commands.push([...args]);
						return { exitCode: 0, stdout: "gjc/0.15.5\n", stderr: "" };
					},
				},
			}),
		).rejects.toThrow(`requires gjc >= ${MIN_GJC_VERSION}; found 0.15.5`);
		expect(commands).toEqual([["--version"]]);
		expect(commands.some((args) => args.includes("session"))).toBe(false);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
