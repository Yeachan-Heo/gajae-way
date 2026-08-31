import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";

/**
 * Integration coverage for issue #64: the unit tests pin the classifier, this
 * pins the *storage path*. A dispatch-time throw must land in
 * `monitor_failures.detail` as a specific code plus allowlisted frames only —
 * with no secret from the error message, no hostile class name, and no hostile
 * frame identifier surviving the trip through `#dispatchBatch`.
 */
async function dispatchAndCaptureFailure(error: Error): Promise<{ code: string; detail: string }> {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-failure-detail-"));
	try {
		const database = await GatewayDatabase.open(join(directory, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "failure-detail",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["changed"],
		});
		const gjc = {
			ensureSession: async () => ({ sessionId: "event-session" }),
			forgetRebinds: () => {},
			// Throws from inside the dispatch attempt, which is exactly where the
			// live `Database has closed` crash surfaced.
			sendTurn: async () => {
				throw error;
			},
		};
		const memory = { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} };
		const pipeline = new MonitorPropagator({
			database,
			registry,
			gjc,
			memory: memory as never,
			delivery: new DeliveryService(new DeliveryLedger(database)),
			emit: () => {},
		});
		pipeline.submit(monitor.monitorId, "changed", {});
		await Bun.sleep(350);
		const rows = database.monitorEventRows();
		expect(rows.length).toBe(1);
		const eventId = rows[0]?.event_id as string;
		const failure = database.monitorFailure(eventId);
		expect(failure).toBeDefined();
		const captured = { code: String(failure?.code), detail: String(failure?.detail) };
		database.close();
		return captured;
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
}

test("the live closed-handle crash persists as database_closed with allowlisted frames only", async () => {
	const error = new Error("Database has closed");
	error.stack = [
		"Error: Database has closed",
		"    at withTransaction (/$bunfs/root/gajaeway-gateway:1881:37)",
		"    at #dispatchBatch (/$bunfs/root/gajaeway-gateway:1902:11)",
	].join("\n");

	const { code, detail } = await dispatchAndCaptureFailure(error);

	expect(code).toBe("database_closed");
	expect(detail).toBe("dispatch phase failed (database_closed) [Error@withTransaction<-#dispatchBatch]");
	// The exact regression this issue is about: it must no longer be the fixed
	// opaque string that made 715+ rows indistinguishable.
	expect(detail).not.toBe("dispatch phase failed (internal_error)");
});

test("a secret in the error message never reaches the persisted detail", async () => {
	const error = new Error(
		"token=ghp_SECRETVALUE opening /Users/someone/private/db.sqlite via https://internal.example",
	);
	error.stack = [
		"Error: token=ghp_SECRETVALUE opening /Users/someone/private/db.sqlite",
		"    at withTransaction (/Users/someone/private/gateway.ts:1881:37)",
	].join("\n");

	const { detail } = await dispatchAndCaptureFailure(error);

	for (const secret of [
		"ghp_SECRETVALUE",
		"token=",
		"/Users/someone",
		"private",
		"db.sqlite",
		"https://",
		"internal.example",
		"1881",
	]) {
		expect(detail).not.toContain(secret);
	}
	expect(detail).toBe("dispatch phase failed (internal_error) [Error@withTransaction]");
});

test("a hostile class name and a hostile frame are both downgraded before storage", async () => {
	class ghp_SECRETVALUE extends Error {}
	const error = new ghp_SECRETVALUE("Database has closed");
	Object.defineProperty(error, "name", { value: "ghp_ALSO_SECRET" });
	error.stack = [
		"ghp_SECRETVALUE: Database has closed",
		"    at ghp_FRAME_SECRET (/p:1:1)",
		"    at #dispatchBatch (/p:2:2)",
	].join("\n");

	const { code, detail } = await dispatchAndCaptureFailure(error);

	expect(code).toBe("database_closed");
	for (const secret of ["ghp_SECRETVALUE", "ghp_ALSO_SECRET", "ghp_FRAME_SECRET"]) {
		expect(detail).not.toContain(secret);
	}
	expect(detail).toBe("dispatch phase failed (database_closed) [Error@unknown_frame<-#dispatchBatch]");
});

test("an unrecognized cause with an unrecognized stack stores no frame section at all", async () => {
	const error = new Error("nobody has classified this yet");
	error.stack = ["Error: x", "    at someVendorInternal (/node_modules/x/index.js:5:5)"].join("\n");

	const { code, detail } = await dispatchAndCaptureFailure(error);

	expect(code).toBe("internal_error");
	expect(detail).toBe("dispatch phase failed (internal_error) [Error]");
	expect(detail).not.toContain("someVendorInternal");
	expect(detail).not.toContain("node_modules");
});
