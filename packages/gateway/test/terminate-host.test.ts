import { expect, spyOn, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajae-gateway/subsession";
import { BrokerSessionPort, parseRunningJobs } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { Store } from "../src/store";
import { noRelay } from "./session-port.fake";

/**
 * The safety property of ending a retired session's host: the gateway may only
 * signal a pid that (a) the broker reports for THAT session and (b) `ps` shows
 * to be a `sdk session-host-internal`. A real child process stands in for the
 * host so the SIGTERM path is exercised for real, not mocked.
 */
async function harness(
	pidFor: (sessionId: string) => number | undefined,
	live = true,
	inspectErrorCode?: string,
	omitPid = false,
	sessionFields: Record<string, unknown> = {},
	inspectExitCode = 0,
) {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-terminate-"));
	const database = await Store.open(join(home, "gateway.db"));
	const authority = { canonicalAgentDir: home, identity: `gjc:${home}` };
	database.transaction((tx) =>
		database.bindings.initializeAuthority(tx, authority, database.inspectBrokerAuthority().populated),
	);
	const run: CliRunner = async (args) => {
		if (args.includes("inspect")) {
			const sessionId = args[args.indexOf("inspect") + 1]!;
			const pid = pidFor(sessionId);
			if (inspectErrorCode)
				return {
					exitCode: 0,
					stdout: JSON.stringify({ ok: false, error: { code: inspectErrorCode, message: "inspect unavailable" } }),
					stderr: "",
				};
			if (pid === undefined && !omitPid)
				return {
					exitCode: 1,
					stdout: JSON.stringify({ ok: false, error: { code: "session_unavailable", message: "gone" } }),
					stderr: "",
				};
			return {
				exitCode: inspectExitCode,
				stdout: JSON.stringify({ ok: true, result: { session: { sessionId, live, pid, ...sessionFields } } }),
				stderr: "",
			};
		}
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const repo = join(home, "workspace");
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		tailRunner: new TailRunner({ stream: noRelay, repo }),
		sleep: async () => {},
	});
	// The port refuses any session it does not own (#assertOwned): register the
	// ids these tests will name, exactly as bind() would have.
	for (const sessionId of ["s-1", "s-2", "s-3", "s-4", "s-5", "s-6", "s-7", "s-8"]) {
		const owned = database.transaction((tx) =>
			database.bindings.recordOwnedBinding(tx, {
				authority,
				sessionId,
				originKey: `loopback/loopback/${sessionId}`,
				epoch: 0,
				repo,
			}),
		);
		if (!owned) throw new Error(`could not register ownership of ${sessionId}`);
	}
	return {
		port,
		repo,
		async close() {
			database.close();
			await rm(home, { recursive: true, force: true });
		},
	};
}

/**
 * A long-lived child whose argv looks like a gjc session host; it exits on
 * SIGTERM. A shell no-op loop carries the tag in its own argv, so
 * `ps -o command=` shows it (`sleep` rejects extra arguments and exits at once).
 */
function fakeHost(argvTag: string, ignoreTerm = false, delayedTerm = false) {
	const loop = delayedTerm
		? "trap 'sleep 2; exit 0' TERM; while :; do sleep 1; done"
		: ignoreTerm
			? "trap '' TERM; while :; do sleep 1; done"
			: "while :; do sleep 1; done";
	return Bun.spawn(["sh", "-c", loop, argvTag], { stdout: "ignore", stderr: "ignore" });
}

test("a live session host reported by the broker is SIGTERMed and exits", async () => {
	const child = fakeHost("sdk session-host-internal");
	const h = await harness(() => child.pid);
	try {
		const outcome = await h.port.terminateHost({ sessionId: "s-1", repo: h.repo });
		expect(outcome).toEqual({ outcome: "terminated", pid: child.pid });
		expect(await child.exited).not.toBe(0); // signalled, not a clean exit
	} finally {
		child.kill();
		await h.close();
	}
});

test("a session host that ignores SIGTERM is escalated and confirmed exited", async () => {
	const child = fakeHost("sdk session-host-internal", true);
	const h = await harness(() => child.pid);
	try {
		expect(await h.port.terminateHost({ sessionId: "s-1", repo: h.repo })).toEqual({
			outcome: "terminated",
			pid: child.pid,
		});
		expect(await child.exited).not.toBe(0);
	} finally {
		child.kill();
		await h.close();
	}
});

test("a signaled host stays tracked when escalation is canceled until its process exits", async () => {
	const child = fakeHost("sdk session-host-internal", false, true);
	const h = await harness(() => child.pid);
	let guardCalls = 0;
	try {
		expect(
			await h.port.terminateHost({
				sessionId: "s-1",
				repo: h.repo,
				shouldTerminate: () => ++guardCalls === 1,
			}),
		).toEqual({ outcome: "terminating", pid: child.pid });
		expect(guardCalls).toBe(2);
		expect(await Promise.race([child.exited.then(() => "exited"), Bun.sleep(100).then(() => "alive")])).toBe("alive");
		expect(await h.port.terminateHost({ sessionId: "s-1", repo: h.repo, shouldTerminate: () => false })).toEqual({
			outcome: "terminating",
			pid: child.pid,
		});
		expect(guardCalls).toBe(2);
		expect(await child.exited).toBe(0);
		expect(await h.port.terminateHost({ sessionId: "s-1", repo: h.repo, shouldTerminate: () => false })).toEqual({
			outcome: "already_gone",
		});
	} finally {
		child.kill();
		await h.close();
	}
});

test("a signaled host is force-retired after a canceled escalation outlives its grace", async () => {
	const child = fakeHost("sdk session-host-internal", true);
	const h = await harness(() => child.pid);
	let guardCalls = 0;
	try {
		expect(
			await h.port.terminateHost({
				sessionId: "s-1",
				repo: h.repo,
				shouldTerminate: () => ++guardCalls === 1,
			}),
		).toEqual({ outcome: "terminating", pid: child.pid });
		expect(guardCalls).toBe(2);
		await Bun.sleep(1_100);
		expect(await h.port.terminateHost({ sessionId: "s-1", repo: h.repo, shouldTerminate: () => false })).toEqual({
			outcome: "terminated",
			pid: child.pid,
		});
		expect(guardCalls).toBe(2);
		expect(await child.exited).not.toBe(0);
	} finally {
		child.kill();
		await h.close();
	}
});

test("a pid that is not a session host is never signalled - broker, daemon, or anything else", async () => {
	for (const tag of ["sdk broker-internal", "sdk daemon-internal", "some-other-program"]) {
		const child = fakeHost(tag);
		const h = await harness(() => child.pid);
		try {
			const outcome = await h.port.terminateHost({ sessionId: "s-2", repo: h.repo });
			expect(outcome.outcome).toBe("not_a_host");
			// Still running: nothing was sent.
			expect(child.killed).toBe(false);
			expect(await Promise.race([child.exited, Bun.sleep(150).then(() => "alive")])).toBe("alive");
		} finally {
			child.kill();
			await h.close();
		}
	}
});

test("broker absence alone does not prove a session host process is gone", async () => {
	const gone = await harness(() => undefined);
	try {
		expect(await gone.port.terminateHost({ sessionId: "s-3", repo: gone.repo })).toEqual({
			outcome: "refused",
			reason: "inspect_failed:session_gone_without_process_identity",
		});
	} finally {
		await gone.close();
	}
	const child = fakeHost("sdk session-host-internal");
	const notLive = await harness(() => child.pid, false);
	try {
		expect(await notLive.port.terminateHost({ sessionId: "s-4", repo: notLive.repo })).toEqual({
			outcome: "refused",
			reason: "session_not_live_but_process_live",
		});
		expect(child.killed).toBe(false);
	} finally {
		child.kill();
		await notLive.close();
	}
});

test("a transient inspect envelope refuses termination without killing the live host", async () => {
	const child = fakeHost("sdk session-host-internal");
	const h = await harness(() => child.pid, true, "broker_unavailable");
	try {
		expect(await h.port.terminateHost({ sessionId: "s-6", repo: h.repo })).toEqual({
			outcome: "refused",
			reason: "inspect_failed:broker_unavailable",
		});
		expect(child.killed).toBe(false);
	} finally {
		child.kill();
		await h.close();
	}
});

test("hostExited rejects live:false for a live pid and never signals even a non-host process", async () => {
	for (const tag of ["sdk session-host-internal", "some-other-program"]) {
		const child = fakeHost(tag);
		const h = await harness(() => child.pid, false);
		try {
			expect(await h.port.hostExited({ sessionId: "s-1", repo: h.repo })).toBe(false);
			expect(child.killed).toBe(false);
			expect(await Promise.race([child.exited, Bun.sleep(50).then(() => "alive")])).toBe("alive");
		} finally {
			child.kill();
			await child.exited;
			await h.close();
		}
	}
});

test("hostExited proves an exited pid gone independently of broker liveness", async () => {
	const child = fakeHost("sdk session-host-internal");
	const pid = child.pid;
	child.kill();
	await child.exited;
	for (const live of [true, false]) {
		const h = await harness(() => pid, live);
		try {
			expect(await h.port.hostExited({ sessionId: "s-1", repo: h.repo })).toBe(true);
		} finally {
			await h.close();
		}
	}
});

/** A real zombie: a parent that never reaps its exited child. Resolves with the zombie's pid. */
async function zombieHost(): Promise<{ readonly pid: number; readonly parent: ReturnType<typeof Bun.spawn> }> {
	const parent = Bun.spawn(["perl", "-e", '$|=1; my $p = fork(); if ($p == 0) { exit 0 } print "$p\\n"; sleep 30'], {
		stdout: "pipe",
		stderr: "ignore",
	});
	const reader = (parent.stdout as ReadableStream<Uint8Array>).getReader();
	const { value } = await reader.read();
	reader.releaseLock();
	const pid = Number(new TextDecoder().decode(value).trim());
	// Wait until ps reports it as a zombie.
	for (let i = 0; i < 100; i++) {
		const ps = Bun.spawnSync(["ps", "-o", "stat=", "-p", String(pid)]);
		if (ps.stdout.toString().trim().startsWith("Z")) break;
		await Bun.sleep(10);
	}
	return { pid, parent };
}

test("a zombie host counts as exited: hostExited is true and terminateHost sends no signal", async () => {
	const zombie = await zombieHost();
	try {
		expect(
			Bun.spawnSync(["ps", "-o", "stat=", "-p", String(zombie.pid)])
				.stdout.toString()
				.trim(),
		).toStartWith("Z");
		for (const live of [true, false]) {
			const h = await harness(() => zombie.pid, live);
			try {
				expect(await h.port.hostExited({ sessionId: "s-1", repo: h.repo })).toBe(true);
				expect(await h.port.terminateHost({ sessionId: "s-1", repo: h.repo })).toEqual({ outcome: "already_gone" });
			} finally {
				await h.close();
			}
		}
		// Its unreaped parent is still running and was never touched.
		expect(await Promise.race([zombie.parent.exited, Bun.sleep(50).then(() => "alive")])).toBe("alive");
	} finally {
		zombie.parent.kill();
		await zombie.parent.exited;
	}
});

test("a zombie leader with live threads (Linux Zl) is not exit evidence; other zombie states are", async () => {
	const pid = 4242;
	const h = await harness(() => pid, false);
	let stat = "Zl";
	const spawn = spyOn(Bun, "spawn").mockImplementation((() => ({
		stdout: new Response(`${pid}     1  ${pid} ${stat}    Fri Nov 13 11:40:54 2026     [host] <defunct>\n`).body,
		stderr: new Response("").body,
		exited: Promise.resolve(0),
	})) as unknown as typeof Bun.spawn);
	try {
		for (const [state, exited] of [
			["Zl", false],
			["Zsl", false],
			["Zl+", false],
			["Z", true],
			["Z+", true],
			["Zs", true],
			["ZN", true],
		] as const) {
			stat = state;
			expect([state, await h.port.hostExited({ sessionId: "s-1", repo: h.repo })]).toEqual([state, exited]);
		}
		stat = "Zl";
		expect(await h.port.terminateHost({ sessionId: "s-1", repo: h.repo })).toEqual({
			outcome: "refused",
			reason: "process_inspection_failed:ps_zombie_threads",
		});
	} finally {
		spawn.mockRestore();
		await h.close();
	}
});

test("hostExited never treats an ambiguous or mismatched row's exited pid as the session's exit", async () => {
	// Several state roots can claim one session; the broker projects one representative row
	// (`ambiguous: true`). Its dead pid says nothing about the other, possibly live, host.
	const dead = fakeHost("sdk session-host-internal");
	const pid = dead.pid;
	dead.kill();
	await dead.exited;
	const other = fakeHost("sdk session-host-internal");
	try {
		for (const fields of [{ ambiguous: true }, { sessionId: "s-2" }, { sessionId: undefined }]) {
			const h = await harness(() => pid, false, undefined, false, fields);
			try {
				expect(await h.port.hostExited({ sessionId: "s-1", repo: h.repo })).toBe(false);
			} finally {
				await h.close();
			}
		}
		expect(other.killed).toBe(false);
		const exclusive = await harness(() => pid, false, undefined, false, { ambiguous: false });
		try {
			expect(await exclusive.port.hostExited({ sessionId: "s-1", repo: exclusive.repo })).toBe(true);
		} finally {
			await exclusive.close();
		}
	} finally {
		other.kill();
		await other.exited;
	}
});

test("hostExited needs a valid host pid in a successful inspect envelope", async () => {
	// A nonzero inspect exit is a failed invocation even when stdout carries an ok-shaped row.
	const dead = fakeHost("sdk session-host-internal");
	const deadPid = dead.pid;
	dead.kill();
	await dead.exited;
	const failed = await harness(() => deadPid, false, undefined, false, {}, 1);
	try {
		expect(await failed.port.hostExited({ sessionId: "s-1", repo: failed.repo })).toBe(false);
	} finally {
		await failed.close();
	}
	for (const pid of [undefined, 0, 1, -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
		const h = await harness(() => pid, false, undefined, true);
		try {
			expect(await h.port.hostExited({ sessionId: "s-1", repo: h.repo })).toBe(false);
		} finally {
			await h.close();
		}
	}
});

test("hostExited rejects session absence and transient inspect envelopes without signaling", async () => {
	const child = fakeHost("sdk session-host-internal");
	try {
		for (const code of ["session_unavailable", "broker_unavailable"]) {
			const h = await harness(() => child.pid, false, code);
			try {
				expect(await h.port.hostExited({ sessionId: "s-1", repo: h.repo })).toBe(false);
				expect(child.killed).toBe(false);
			} finally {
				await h.close();
			}
		}
		const gone = await harness(() => undefined);
		try {
			expect(await gone.port.hostExited({ sessionId: "s-1", repo: gone.repo })).toBe(false);
		} finally {
			await gone.close();
		}
	} finally {
		child.kill();
		await child.exited;
	}
});

test("hostExited returns false when the OS process probe is unreadable", async () => {
	const h = await harness(() => process.pid, false);
	const spawn = spyOn(Bun, "spawn").mockImplementation(() => {
		throw new Error("ps unavailable");
	});
	try {
		expect(await h.port.hostExited({ sessionId: "s-1", repo: h.repo })).toBe(false);
		expect(spawn).toHaveBeenCalledTimes(1);
		expect(spawn.mock.calls[0]?.[0]).toEqual([
			"ps",
			"-o",
			"pid=,ppid=,pgid=,stat=,lstart=,command=",
			"-p",
			String(process.pid),
		]);
	} finally {
		spawn.mockRestore();
		await h.close();
	}
});

test("a rejected final termination guard leaves the session host running", async () => {
	const child = fakeHost("sdk session-host-internal");
	const h = await harness(() => child.pid);
	try {
		expect(await h.port.terminateHost({ sessionId: "s-7", repo: h.repo, shouldTerminate: async () => false })).toEqual({
			outcome: "refused",
			reason: "termination_guard_rejected",
		});
		expect(child.killed).toBe(false);
	} finally {
		child.kill();
		await h.close();
	}
});

test("a pid that already exited is already gone, not an error", async () => {
	const child = fakeHost("sdk session-host-internal");
	const pid = child.pid;
	child.kill();
	await child.exited;
	const h = await harness(() => pid);
	try {
		expect(await h.port.terminateHost({ sessionId: "s-5", repo: h.repo })).toEqual({ outcome: "already_gone" });
	} finally {
		await h.close();
	}
});

test("an already-dead host is reported gone without invoking the signal guard", async () => {
	const child = fakeHost("sdk session-host-internal");
	const pid = child.pid;
	child.kill();
	await child.exited;
	const h = await harness(() => pid);
	try {
		let guardCalls = 0;
		expect(
			await h.port.terminateHost({
				sessionId: "s-8",
				repo: h.repo,
				shouldTerminate: () => {
					guardCalls++;
					return false;
				},
			}),
		).toEqual({ outcome: "already_gone" });
		expect(guardCalls).toBe(0);
	} finally {
		await h.close();
	}
});

// #41: the host's running background jobs are read before it is ended. Shape
// captured from a live gjc 0.17.2 `runtime.jobs.list` query response.
test("runtime.jobs.list: running jobs are parsed; an unreadable answer throws instead of reading as none", () => {
	const page = (running: unknown[]) =>
		JSON.stringify({
			type: "query_response",
			ok: true,
			page: {
				items: [{ running, recent: [], delivery: { queued: 0, delivering: false, pendingJobIds: [] } }],
				complete: true,
			},
		});
	expect(parseRunningJobs(page([]))).toEqual([]);
	expect(
		parseRunningJobs(page([{ id: "0-Review", type: "task", status: "running", label: "review", startTime: 1 }])),
	).toEqual([{ id: "0-Review", type: "task", label: "review" }]);
	expect(() => parseRunningJobs(JSON.stringify({ ok: false, error: { code: "resource_gone" } }))).toThrow();
	expect(() => parseRunningJobs(JSON.stringify({ ok: true, page: { items: [null], complete: true } }))).toThrow();
});
