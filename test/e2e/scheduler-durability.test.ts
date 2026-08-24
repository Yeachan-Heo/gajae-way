import { expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { RpcClient } from "../../src/rpc-client";
import { FakeBrokerFixture } from "../helpers/main-session";

const repositoryRoot = path.resolve(import.meta.dir, "..", "..");

function compiledWay(): string {
	const executable = path.join(repositoryRoot, "dist", "gajaeway");
	if (!fs.existsSync(executable)) throw new Error("dist/gajaeway is missing; run bun scripts/compile.ts");
	return executable;
}

function profileToml(corpus: string, workspace: string, sessionId: string): string {
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = []

[main_session]
session_id = "${sessionId}"

[surfaces.owner]
id = "sched-owner"
platform = "test"
kind = "dm"
session_kind = "main"
`;
}

async function run(command: readonly string[], environment: NodeJS.ProcessEnv): Promise<void> {
	const proc = Bun.spawn({ cmd: [...command], cwd: repositoryRoot, env: environment, stdout: "pipe", stderr: "pipe" });
	if ((await proc.exited) !== 0) {
		throw new Error(`${command.join(" ")} failed: ${await new Response(proc.stderr).text()}`);
	}
}

async function connectHealthy(socketPath: string): Promise<RpcClient> {
	for (let attempt = 0; attempt < 200; attempt += 1) {
		try {
			const client = await RpcClient.connect(socketPath);
			const health = (await client.request("way.health", {})).result as { state?: string };
			if (health?.state === "running") return client;
			client.close();
		} catch {
			// Still starting.
		}
		await Bun.sleep(50);
	}
	throw new Error("compiled daemon never became healthy");
}

/**
 * Kill-9 durability drill against the COMPILED daemon.
 *
 * The unit tests prove the durable protocol by simulating a crash, but a
 * simulated crash cannot show that no in-memory scheduler state was
 * load-bearing: the tick loop keeps `deferredSince` in memory, and only a real
 * process death exercises that. Each window kills the daemon mid-protocol via a
 * `process.exit(137)` seam, restarts it, and asserts the durable outcome.
 */
async function drill(
	killEnvironmentKey: string,
	assertion: (client: RpcClient, jobId: string, fixture: FakeBrokerFixture, originalFireAtMs: number) => Promise<void>,
): Promise<void> {
	const executable = compiledWay();
	const fixture = new FakeBrokerFixture();
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajaeway-sched-kill-"));
	const corpus = path.join(root, "corpus");
	const state = path.join(root, "state");
	fs.mkdirSync(corpus, { recursive: true });
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(profilePath, profileToml(corpus, fixture.workspace, fixture.sessionId));
	const socketPath = path.join(state, "rpc.sock");
	const base = {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
	} as NodeJS.ProcessEnv;

	let daemon: { kill: () => void; exited: Promise<number> } | undefined;
	let client: RpcClient | undefined;
	try {
		await run([executable, "bootstrap", "--confirm", "--state-dir", state, "--profile", profilePath], base);

		// First boot: create a job that is already due, with the kill seam armed.
		const armed = { ...base, [killEnvironmentKey]: "1" } as NodeJS.ProcessEnv;
		daemon = Bun.spawn({
			cmd: [executable, "serve", "--state-dir", state, "--profile", profilePath],
			cwd: repositoryRoot,
			env: armed,
			stdout: "pipe",
			stderr: "pipe",
		});
		client = await connectHealthy(socketPath);
		const createResponse = await client.request("schedule.create", {
			idempotency_key: "kill-drill-1",
			name: "kill drill",
			kind: "every",
			spec: String(15_000),
			timezone: "UTC",
			payload_kind: "system_event",
			payload: { text: "kill drill work" },
		});
		if (createResponse.error) throw new Error(`schedule.create failed: ${JSON.stringify(createResponse.error)}`);
		const created = createResponse.result as { job_id: string; next_fire_at_ms: number };
		// The fire time BEFORE the crash, so recovery's advance can be measured.
		const originalFireAtMs = created.next_fire_at_ms;
		client.close();
		client = undefined;

		// The tick claims the due job and the seam kills the process mid-protocol.
		// The tick claims the due job and the seam kills the process.
		const exitCode = await daemon.exited;
		expect(exitCode).toBe(137);
		daemon = undefined;

		// Restart WITHOUT the seam and let reconciliation resolve the window.
		daemon = Bun.spawn({
			cmd: [executable, "serve", "--state-dir", state, "--profile", profilePath],
			cwd: repositoryRoot,
			env: base,
			stdout: "pipe",
			stderr: "pipe",
		});
		client = await connectHealthy(socketPath);
		await assertion(client, created.job_id, fixture, originalFireAtMs);
	} finally {
		client?.close();
		daemon?.kill();
		fs.rmSync(root, { recursive: true, force: true });
	}
}

test("window 1: a kill after the claim commits leaves exactly one run and never double-fires", async () => {
	await drill("GAJAEWAY_E2E_KILL_AFTER_SCHEDULE_RUN_CLAIMED", async (client, jobId, fixture, originalFireAtMs) => {
		// Recovery must resolve the orphaned claim rather than leaving it in flight.
		for (let attempt = 0; attempt < 100; attempt += 1) {
			const runs = (await client.request("schedule.runs", { job_id: jobId })).result as {
				runs: Array<{ outcome?: string; runId: string }>;
			};
			const settled = runs.runs.filter((entry) => entry.outcome !== undefined && entry.outcome !== null);
			if (settled.length > 0) {
				// EXACTLY one durable row for the claimed occurrence. Asserting only
				// that run ids are distinct would let a replay storm of fresh ids
				// pass, which is precisely the failure this window guards.
				expect(runs.runs).toHaveLength(1);
				expect(settled).toHaveLength(1);
				// Nothing was sent before the kill, so recovery must COMPLETE the
				// occurrence rather than record it as lost.
				expect(settled[0]?.outcome).toBe("ok");
				// EXACTLY one, not at-most-one: `<= 1` also passes on zero, which
				// would mask a LOST turn rather than a duplicated one. The kill
				// landed before any send, so recovery must have admitted it once.
				expect(fixture.admissionAttempts()).toHaveLength(1);
				// The claim NULLs next_fire_at_ms, so recovery must ADVANCE it exactly
				// once. Without this a recovery that settles the run but leaves the
				// field NULL leaves the job permanently stuck, and the drill would
				// still pass - which is precisely the in-memory-state failure G006
				// exists to catch.
				const job = (await client.request("schedule.get", { job_id: jobId })).result as {
					nextFireAtMs?: number | null;
				};
				// Not stuck: a NULL here means the job never fires again.
				expect(job.nextFireAtMs ?? null).not.toBeNull();
				const nextFire = job.nextFireAtMs as number;
				// Advanced past the occurrence that was interrupted.
				expect(nextFire).toBeGreaterThan(originalFireAtMs);
				// Advanced ONCE, not repeatedly: `advance` re-anchors to the
				// recovery instant rather than preserving the original epoch, so the
				// contract is a single forward step of at most one interval beyond
				// the moment recovery ran - not an interval-aligned multiple of the
				// original fire time, and never a backfilled burst.
				// On the job's OWN lattice: `every` anchors to the create-time
				// epoch, so every advance lands on createdAt + k*interval. That is
				// what makes "advanced exactly once" checkable instead of a
				// wall-clock guess, and it fails a drifting or re-anchored advance.
				const jobRow = (await client.request("schedule.get", { job_id: jobId })).result as {
					createdAtMs: number;
				};
				expect((nextFire - jobRow.createdAtMs) % 15_000).toBe(0);
				// The NEXT lattice point, not one further out. If recovery spans
				// several intervals the schedule correctly SKIPS rather than
				// backfilling, so the check is that no extra step was taken beyond
				// the first point past the recovery instant.
				expect(nextFire - 15_000).toBeLessThanOrEqual(Date.now());

				return;
			}
			await Bun.sleep(100);
		}
		throw new Error("the interrupted run was never resolved after restart");
	});
}, 120_000);

test("window 2: a kill after broker acceptance does not duplicate the admitted turn", async () => {
	await drill(
		"GAJAEWAY_E2E_KILL_AFTER_SCHEDULE_ADMISSION_BEFORE_RUN_FINALIZE",
		async (client, jobId, fixture, originalFireAtMs) => {
			for (let attempt = 0; attempt < 100; attempt += 1) {
				const runs = (await client.request("schedule.runs", { job_id: jobId })).result as {
					runs: Array<{ outcome?: string; runId: string }>;
				};
				const settled = runs.runs.filter((entry) => entry.outcome !== undefined && entry.outcome !== null);
				if (settled.length > 0) {
					// One durable row for the occurrence, not a replay storm.
					expect(runs.runs).toHaveLength(1);
					expect(settled).toHaveLength(1);
					// EXACTLY one. The kill landed AFTER the broker accepted, so the log
					// is guaranteed to hold that original turn; `<= 1` would vacuously
					// pass on zero and hide a lost turn. Two would mean the deterministic
					// key sched:<job>:<run> failed to replay and the transcript gained a
					// duplicated turn, which is the failure this window exists for.
					expect(fixture.admissionAttempts()).toHaveLength(1);
					// The claim NULLs next_fire_at_ms, so recovery must ADVANCE it exactly
					// once. Without this a recovery that settles the run but leaves the
					// field NULL leaves the job permanently stuck, and the drill would
					// still pass - which is precisely the in-memory-state failure G006
					// exists to catch.
					const job = (await client.request("schedule.get", { job_id: jobId })).result as {
						nextFireAtMs?: number | null;
					};
					// Not stuck: a NULL here means the job never fires again.
					expect(job.nextFireAtMs ?? null).not.toBeNull();
					const nextFire = job.nextFireAtMs as number;
					// Advanced past the occurrence that was interrupted.
					expect(nextFire).toBeGreaterThan(originalFireAtMs);
					// Advanced ONCE, not repeatedly: `advance` re-anchors to the
					// recovery instant rather than preserving the original epoch, so the
					// contract is a single forward step of at most one interval beyond
					// the moment recovery ran - not an interval-aligned multiple of the
					// original fire time, and never a backfilled burst.
					// On the job's OWN lattice: `every` anchors to the create-time
					// epoch, so every advance lands on createdAt + k*interval. That is
					// what makes "advanced exactly once" checkable instead of a
					// wall-clock guess, and it fails a drifting or re-anchored advance.
					const jobRow = (await client.request("schedule.get", { job_id: jobId })).result as {
						createdAtMs: number;
					};
					expect((nextFire - jobRow.createdAtMs) % 15_000).toBe(0);
					// The NEXT lattice point, not one further out. If recovery spans
					// several intervals the schedule correctly SKIPS rather than
					// backfilling, so the check is that no extra step was taken beyond
					// the first point past the recovery instant.
					expect(nextFire - 15_000).toBeLessThanOrEqual(Date.now());

					return;
				}
				await Bun.sleep(100);
			}
			throw new Error("the interrupted run was never resolved after restart");
		},
	);
}, 120_000);
