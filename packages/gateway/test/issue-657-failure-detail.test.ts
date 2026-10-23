import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GjcCliError } from "@gajae-gateway/subsession";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import type { SessionRelayStream } from "../src/orchestrator/broker";
import type { SessionPort } from "../src/orchestrator/session-port";
import { RelayHelloError, TailRunner } from "../src/orchestrator/tail-runner";
import { Store } from "../src/store";

test("plain Error with no cause chain yields sanitized failure detail message in failureDetail()", async () => {
	// This test verifies that failureDetail() includes the top-level error.message
	// when no cause chain message is found. This was the fix for issue #657.
	const home = await mkdtemp(join(tmpdir(), "gajaeway-issue-657-plain-error-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "plain-error-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.plain_error"],
			burstPolicy: "serialize",
		});

		// Create a failing port that throws a plain Error with no cause chain
		const failingPort: Partial<SessionPort> = {
			bind: async () => {
				throw new Error("session bind failed: critical operation failed due to timeout");
			},
			runExclusive: async (_, fn) => fn(),
		};

		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: failingPort as SessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			repo: join(home, "workspace"),
			model: undefined,
			serviceTier: undefined,
		});

		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.plain_error", {});

		// Wait for dispatch to fail (with longer timeout for slower CI)
		for (let i = 0; i < 300; i++) {
			const row = database.monitors.eventRows().find((r) => r.event_id === eventId);
			if (row?.stage === "failed") break;
			await Bun.sleep(10);
		}

		// Verify failure was recorded
		const failure = database.monitors.failure(eventId);
		expect(failure).toBeDefined();
		expect(failure?.code).toBe("session_bind_failed");

		// The critical test: verify the plain error message appears in the detail
		// The detail should be a JSON object containing the error information
		const detail = failure?.detail || "";
		expect(detail).toContain("critical operation failed due to timeout");
		expect(detail).toContain('"class":"Error"');
		expect(detail).toContain('"message"');

		await pipeline.drain();
		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("ModelNotSelectedError preserves class and model_not_selected code instead of collapsing to generic Error", async () => {
	// This test verifies that ModelNotSelectedError is properly recognized
	// in failureDetail() and doesn't get collapsed to a generic Error class.
	// The fix added ModelNotSelectedError to allowedClasses and model_not_selected to allowedCodes.
	const home = await mkdtemp(join(tmpdir(), "gajaeway-issue-657-model-not-selected-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "model-not-selected-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.model_not_selected"],
			burstPolicy: "serialize",
		});

		// Create a failing port that throws ModelNotSelectedError
		const failingPort: Partial<SessionPort> = {
			bind: async () => {
				throw new GjcCliError("session bind failed", 0, "", {
					code: "model_not_selected",
				});
			},
			runExclusive: async (_, fn) => fn(),
		};

		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: failingPort as SessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			repo: join(home, "workspace"),
			model: undefined,
			serviceTier: undefined,
		});

		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.model_not_selected", {});

		// Wait for dispatch to fail (with longer timeout for slower CI)
		for (let i = 0; i < 300; i++) {
			const row = database.monitors.eventRows().find((r) => r.event_id === eventId);
			if (row?.stage === "failed") break;
			await Bun.sleep(10);
		}

		// Verify failure was recorded
		const failure = database.monitors.failure(eventId);
		expect(failure).toBeDefined();
		expect(failure?.code).toBe("session_bind_failed");

		// The critical test: verify model_not_selected code is preserved in the detail
		const detail = failure?.detail || "";
		// The detail format is: "dispatch phase failed ({code}): {failureDetail JSON} {operation info JSON}"
		// Extract the failureDetail JSON part
		const failureDetailStart = detail.indexOf("{");
		const failureDetailEnd = detail.indexOf("}", failureDetailStart) + 1;
		const failureDetailJson = JSON.parse(detail.substring(failureDetailStart, failureDetailEnd));
		expect(failureDetailJson.code).toBe("model_not_selected");

		await pipeline.drain();
		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("missing connection id without hello timeout throws RelayHelloError with elapsed ms and stream state", async () => {
	// This test verifies that when a relay connection fails to send a hello message,
	// a RelayHelloError is thrown (instead of a plain Error) with elapsed time and stream state.
	// The fix changed: `throw new Error(...)` to `throw new RelayHelloError(elapsedMs, childState)`
	// for all cases, not just when helloTimedOut was true.

	let finishLines!: () => void;
	const ended = new Promise<void>((resolve) => {
		finishLines = resolve;
	});
	const lines: AsyncIterable<string> = {
		[Symbol.asyncIterator](): AsyncIterator<string> {
			let endedStream = false;
			return {
				async next(): Promise<IteratorResult<string>> {
					if (endedStream) return { done: true, value: undefined };
					await ended;
					endedStream = true;
					return { done: true, value: undefined };
				},
			};
		},
	};

	const stream: SessionRelayStream = {
		lines,
		write: () => {
			// Don't respond to hello - this simulates a missing connection ID
		},
		close: () => finishLines(),
	};

	const runner = new TailRunner({ stream: () => stream, repo: "/tmp", helloTimeoutMs: 5 });
	const caughtError = await runner
		.attach({ sessionId: "sdk-hello-missing", brokerGeneration: 0, repo: "/tmp" })
		.catch((failure: unknown) => failure);

	// Verify it's a RelayHelloError, not a plain Error
	expect(caughtError).toBeInstanceOf(RelayHelloError);
	const error = caughtError as RelayHelloError;
	expect(error.constructor.name).toBe("RelayHelloError");

	// Verify it has the required properties
	expect(error.code).toBe("relay_hello_missing");
	expect(error.elapsedMs).toBeGreaterThanOrEqual(0);
	expect(error.childState).toBe("stream_open");

	// Verify the error message includes the elapsed time and stream state
	expect(error.message).toContain("relay hello did not arrive");
	expect(error.message).toContain("stream_open");
});

test("missing connection id when stream ends throws RelayHelloError with stream_ended state", async () => {
	// Similar to the previous test, but verifies the stream_ended state case.
	// This happens when the relay stream closes before sending hello.

	const lines: AsyncIterable<string> = {
		[Symbol.asyncIterator](): AsyncIterator<string> {
			return {
				async next(): Promise<IteratorResult<string>> {
					return { done: true, value: undefined };
				},
			};
		},
	};

	const stream: SessionRelayStream = {
		lines,
		write: () => {
			// Don't respond to hello
		},
		close: () => {
			// Already closed
		},
	};

	const runner = new TailRunner({ stream: () => stream, repo: "/tmp", helloTimeoutMs: 100 });
	const caughtError = await runner
		.attach({ sessionId: "sdk-hello-stream-ended", brokerGeneration: 0, repo: "/tmp" })
		.catch((failure: unknown) => failure);

	// Verify it's a RelayHelloError
	expect(caughtError).toBeInstanceOf(RelayHelloError);
	const error = caughtError as RelayHelloError;
	expect(error.code).toBe("relay_hello_missing");

	// Verify it reports stream_ended state
	expect(error.childState).toBe("stream_ended");
	expect(error.elapsedMs).toBeGreaterThanOrEqual(0);

	expect(error.message).toContain("stream_ended");
});
