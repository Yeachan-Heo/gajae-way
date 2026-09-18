import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajae-gateway/subsession";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";

/**
 * The safety property of ending a retired session's host: the gateway may only
 * signal a pid that (a) the broker reports for THAT session and (b) `ps` shows
 * to be a `sdk session-host-internal`. A real child process stands in for the
 * host so the SIGTERM path is exercised for real, not mocked.
 */
async function harness(pidFor: (sessionId: string) => number | undefined, live = true) {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-terminate-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = { canonicalAgentDir: home, identity: `gjc:${home}` };
	database.assertBrokerAuthority(authority, { initializeEmpty: true });
	const run: CliRunner = async (args) => {
		if (args.includes("inspect")) {
			const sessionId = args[args.indexOf("inspect") + 1]!;
			const pid = pidFor(sessionId);
			if (pid === undefined)
				return {
					exitCode: 1,
					stdout: JSON.stringify({ ok: false, error: { code: "session_unavailable", message: "gone" } }),
					stderr: "",
				};
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { session: { sessionId, live, pid } } }),
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
		instanceId: "terminate",
		tailRunner: new TailRunner({ run, repo }),
		sleep: async () => {},
	});
	// The port refuses any session it does not own (#assertOwned): register the
	// ids these tests will name, exactly as bind() would have.
	for (const sessionId of ["s-1", "s-2", "s-3", "s-4", "s-5"]) {
		const owned = database.recordOwnedBinding({
			authority,
			sessionId,
			originKey: `loopback/loopback/${sessionId}`,
			epoch: 0,
			repo,
		});
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
function fakeHost(argvTag: string) {
	return Bun.spawn(["sh", "-c", "while :; do sleep 1; done", argvTag], { stdout: "ignore", stderr: "ignore" });
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

test("a session the broker no longer knows, or reports not live, is already gone", async () => {
	const gone = await harness(() => undefined);
	try {
		expect(await gone.port.terminateHost({ sessionId: "s-3", repo: gone.repo })).toEqual({ outcome: "already_gone" });
	} finally {
		await gone.close();
	}
	const child = fakeHost("sdk session-host-internal");
	const notLive = await harness(() => child.pid, false);
	try {
		expect(await notLive.port.terminateHost({ sessionId: "s-4", repo: notLive.repo })).toEqual({
			outcome: "already_gone",
		});
		expect(child.killed).toBe(false);
	} finally {
		child.kill();
		await notLive.close();
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
