import { describe, expect, test } from "bun:test";
import {
	awaitReady,
	type BrokerSession,
	type CliResult,
	type ControllerOptions,
	GjcCliError,
	inspectSession,
	listSessions,
	parseEnvelope,
	selectSessionForWorktree,
	verifyReady,
} from "../src/cli";

const WORKTREE = "/wt/subsession-runtime";
const SESSION = "ad2f2494-2584-4d13-b7b6-c6ac24a1087f";

function rawSession(overrides: Record<string, unknown> = {}) {
	return {
		sessionId: SESSION,
		locator: { repo: WORKTREE, stateRoot: `${WORKTREE}/.gjc/state` },
		pid: 94406,
		live: true,
		deleted: false,
		activity: { state: "active" },
		lastHeartbeatAt: 1787730579915,
		...overrides,
	};
}

function controller(handler: (args: readonly string[]) => CliResult, calls: string[][] = []): ControllerOptions {
	return {
		repo: WORKTREE,
		run: async (args) => {
			calls.push([...args]);
			return handler(args);
		},
	};
}

const ok = (result: unknown): CliResult => ({
	exitCode: 0,
	stdout: JSON.stringify({ ok: true, result }),
	stderr: "",
});

describe("parseEnvelope", () => {
	test("unwraps a successful envelope", () => {
		expect(parseEnvelope<{ a: number }>(ok({ a: 1 }), "x")).toEqual({ a: 1 });
	});

	test("rejects a non-zero exit", () => {
		expect(() => parseEnvelope({ exitCode: 2, stdout: "", stderr: "boom" }, "x")).toThrow(GjcCliError);
	});

	test("rejects truncated stdout instead of guessing", () => {
		expect(() => parseEnvelope({ exitCode: 0, stdout: '{"ok":true,"resu', stderr: "" }, "x")).toThrow(
			/did not print a JSON envelope/,
		);
	});

	test("rejects ok:false", () => {
		expect(() =>
			parseEnvelope({ exitCode: 0, stdout: JSON.stringify({ ok: false, error: { code: "nope" } }), stderr: "" }, "x"),
		).toThrow(/reported failure/);
	});
});

describe("listSessions", () => {
	test("passes --repo and normalizes the broker rows", async () => {
		const calls: string[][] = [];
		const options = controller(() => ok({ sessions: [rawSession()] }), calls);
		const sessions = await listSessions(options);
		expect(sessions[0]).toMatchObject({ sessionId: SESSION, repo: WORKTREE, live: true });
		expect(calls[0]).toEqual(["sdk", "session", "list", "--repo", WORKTREE]);
	});

	test("drops rows without an identity or locator instead of failing the poll", async () => {
		const options = controller(() =>
			ok({ sessions: [rawSession(), { sessionId: "x" }, { locator: { repo: WORKTREE } }] }),
		);
		expect(await listSessions(options)).toHaveLength(1);
	});

	test("never reads an endpoint file or a token", async () => {
		const calls: string[][] = [];
		const options = controller(() => ok({ sessions: [] }), calls);
		await listSessions(options);
		const flat = calls.flat().join(" ");
		expect(flat).not.toContain("token");
		expect(flat).not.toContain(".gjc/state/sdk");
	});
});

describe("verifyReady", () => {
	test("accepts a live session whose cwd is the requested worktree", async () => {
		const options = controller(() => ok({ session: rawSession() }));
		const result = await verifyReady(options, { sessionId: SESSION, worktreePath: WORKTREE });
		expect(result.ready).toBe(true);
	});

	test.each([
		["deleted", { deleted: true }],
		["not-live", { live: false }],
	])("rejects a %s session", async (reason, overrides) => {
		const options = controller(() => ok({ session: rawSession(overrides) }));
		const result = await verifyReady(options, { sessionId: SESSION, worktreePath: WORKTREE });
		expect(result).toMatchObject({ ready: false, reason });
	});

	test("rejects a session running in a different worktree", async () => {
		const options = controller(() => ok({ session: rawSession({ locator: { repo: "/wt/other" } }) }));
		const result = await verifyReady(options, { sessionId: SESSION, worktreePath: WORKTREE });
		expect(result).toMatchObject({ ready: false, reason: "cwd-mismatch" });
	});

	test("reports not-found when the broker has no such session", async () => {
		const options = controller(() => ok({}));
		const result = await verifyReady(options, { sessionId: SESSION, worktreePath: WORKTREE });
		expect(result).toMatchObject({ ready: false, reason: "not-found" });
	});

	test("a CLI failure is not-found, not a thrown poll", async () => {
		const options = controller(() => ({ exitCode: 1, stdout: "", stderr: "broker down" }));
		const result = await verifyReady(options, { sessionId: SESSION, worktreePath: WORKTREE });
		expect(result).toMatchObject({ ready: false, reason: "not-found" });
	});
});

describe("awaitReady", () => {
	test("fails closed on timeout rather than reporting success", async () => {
		let clock = 0;
		const options = controller(() => ok({ session: rawSession({ live: false }) }));
		const result = await awaitReady(
			options,
			{ sessionId: SESSION, worktreePath: WORKTREE },
			{ timeoutMs: 10, pollMs: 1, now: () => (clock += 6), sleep: async () => {} },
		);
		expect(result).toMatchObject({ ready: false, reason: "not-live" });
	});

	test("returns as soon as the session becomes ready", async () => {
		let attempts = 0;
		const options = controller(() => {
			attempts += 1;
			return ok({ session: rawSession(attempts < 3 ? { live: false } : {}) });
		});
		const result = await awaitReady(
			options,
			{ sessionId: SESSION, worktreePath: WORKTREE },
			{ timeoutMs: 1_000, pollMs: 1, sleep: async () => {} },
		);
		expect(result.ready).toBe(true);
		expect(attempts).toBe(3);
	});
});

describe("selectSessionForWorktree", () => {
	const base: BrokerSession = {
		sessionId: "a",
		repo: WORKTREE,
		live: true,
		deleted: false,
		lastHeartbeatAt: 100,
	};

	test("prefers the most recently active live candidate", () => {
		const picked = selectSessionForWorktree([base, { ...base, sessionId: "b", lastHeartbeatAt: 500 }], WORKTREE);
		expect(picked?.sessionId).toBe("b");
	});

	test("ignores dead, deleted and foreign-worktree rows", () => {
		const picked = selectSessionForWorktree(
			[
				{ ...base, sessionId: "dead", live: false, lastHeartbeatAt: 900 },
				{ ...base, sessionId: "gone", deleted: true, lastHeartbeatAt: 900 },
				{ ...base, sessionId: "elsewhere", repo: "/wt/other", lastHeartbeatAt: 900 },
				base,
			],
			WORKTREE,
		);
		expect(picked?.sessionId).toBe("a");
	});

	test("returns nothing when no session owns the worktree", () => {
		expect(selectSessionForWorktree([], WORKTREE)).toBeUndefined();
	});
});

describe("inspectSession", () => {
	test("returns undefined for an empty payload", async () => {
		expect(
			await inspectSession(
				controller(() => ok({})),
				SESSION,
			),
		).toBeUndefined();
	});
});
