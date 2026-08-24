import { afterAll, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";

const temporaryDirectories: string[] = [];
const running: WayCoreHandle[] = [];

afterAll(() => {
	for (const core of running) {
		try {
			core.metricsHttpStop();
		} catch {
			// Already stopped.
		}
	}
	for (const directory of temporaryDirectories) fs.rmSync(directory, { recursive: true, force: true });
});

function core(): WayCoreHandle {
	const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-metrics-http-"));
	temporaryDirectories.push(stateDir);
	const handle = loadWayCore().WayCore.open(stateDir);
	running.push(handle);
	return handle;
}

test("the endpoint serves Prometheus text on GET /metrics only", async () => {
	const handle = core();
	const port = handle.metricsHttpStart(0);
	expect(port).toBeGreaterThan(0);

	const ok = await fetch(`http://127.0.0.1:${port}/metrics`);
	expect(ok.status).toBe(200);
	expect(ok.headers.get("content-type")).toContain("text/plain");
	const body = await ok.text();
	expect(body).toContain("gajaeway_journal_head_seq");
	expect(body).toContain("gajaeway_lock_quarantined");

	// Every other path and method is refused without echoing the request.
	for (const [method, target] of [
		["GET", "/healthz"],
		["POST", "/metrics"],
		["GET", "/metrics/../secret"],
	] as const) {
		const refused = await fetch(`http://127.0.0.1:${port}${target}`, { method });
		expect(refused.status).toBe(404);
		const refusedBody = await refused.text();
		expect(refusedBody).toBe("");
	}

	handle.metricsHttpStop();
});

/**
 * The exposition is unauthenticated, so it must never carry an identifier that
 * would let a local scraper learn who holds the corpus lock or which session is
 * adopted.
 */
test("the exposition contains no holder, session, lease, or reason identifiers", async () => {
	const handle = core();
	const port = handle.metricsHttpStart(0);

	const body = await (await fetch(`http://127.0.0.1:${port}/metrics`)).text();
	for (const forbidden of ["holder", "session_id", "lease_id", "reason"]) {
		expect(body).not.toContain(forbidden);
	}

	handle.metricsHttpStop();
});

test("starting twice is refused rather than silently rebinding", () => {
	const handle = core();
	handle.metricsHttpStart(0);
	expect(() => handle.metricsHttpStart(0)).toThrow();
	handle.metricsHttpStop();
});

test("stopping is idempotent and safe when never started", () => {
	const handle = core();
	expect(() => handle.metricsHttpStop()).not.toThrow();
	handle.metricsHttpStart(0);
	handle.metricsHttpStop();
	expect(() => handle.metricsHttpStop()).not.toThrow();
});
