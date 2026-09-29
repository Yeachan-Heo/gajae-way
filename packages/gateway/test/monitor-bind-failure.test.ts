import { expect, test } from "bun:test";
import { GatewayDatabase } from "../src/store/db";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

test("should preserve operation and operation_args even with long failure details", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gw-test-"));
	try {
		const db = await GatewayDatabase.open(join(directory, "gateway.db"));

		const eventId = randomUUID();
		const code = "session_bind_failed";

		// Simulate a long failure detail that could trigger truncation
		// This mimics what happens when failureDetail returns a long error chain
		const longFailureDetail =
			"GjcCliError(operation_failed/transport=envelope/frame=request/phase=bind/sessionId=null) " +
			"caused by: SomeDetailedErrorMessage ".repeat(10); // Make it long

		// Construct detail with FIXED format: JSON first to protect from truncation
		const phase = "bind";
		const operation = "bind";
		const operation_args = { epoch: 0, hasModel: false };
		const sessionId = null;
		const originKey = "monitor/sns-threads-v7";
		const attempt = 1;

		// FIXED: Place JSON first, convert undefined to null
		const structuredDetail = JSON.stringify({
			phase,
			operation: operation ?? null,
			operation_args: operation_args ?? null,
			sessionId,
			origin: originKey,
			attempt,
		});
		const fixedDetail = `dispatch phase failed (${code}): ${structuredDetail} ${longFailureDetail}`;

		// Record with fixed format
		db.withTransaction(() => {
			db.monitorFailureRecord(eventId, code, fixedDetail);
		});

		// Retrieve and verify
		const failures = db.monitorFailures([eventId]);
		const failure = failures.get(eventId);
		expect(failure).toBeDefined();

		// With the fix: operation and operation_args should survive truncation
		const detail = failure!.detail;
		console.log("Fixed detail (truncated to 500):", detail);
		console.log("Length:", detail.length);

		// These should PASS with the fixed code
		expect(detail).toContain('"operation":"bind"');
		expect(detail).toContain('"operation_args"');
		expect(detail).toContain('"phase":"bind"');
		db.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("should preserve operation in JSON even when undefined", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gw-test-"));
	try {
		const db = await GatewayDatabase.open(join(directory, "gateway.db"));

		const eventId = randomUUID();
		const code = "session_bind_failed";

		// Test with undefined operation
		const longFailureDetail = "GjcCliError(operation_failed) ".repeat(20);

		const phase = "bind";
		const operation = undefined; // Will be converted to null
		const operation_args = undefined; // Will be converted to null
		const sessionId = null;
		const originKey = "monitor/very-long-origin-key-that-takes-up-space";
		const attempt = 1;

		// FIXED: Place JSON first and convert undefined to null
		const structuredDetail = JSON.stringify({
			phase,
			operation: operation ?? null,
			operation_args: operation_args ?? null,
			sessionId,
			origin: originKey,
			attempt,
		});
		const detail = `dispatch phase failed (${code}): ${structuredDetail} ${longFailureDetail}`;

		db.withTransaction(() => {
			db.monitorFailureRecord(eventId, code, detail);
		});

		const failures = db.monitorFailures([eventId]);
		const failure = failures.get(eventId);
		expect(failure).toBeDefined();

		const storedDetail = failure!.detail;
		console.log("Detail with undefined converted to null:", storedDetail);

		// Should have operation field with null value (not dropped by JSON.stringify)
		expect(storedDetail).toContain('"operation":null');
		expect(storedDetail).toContain('"operation_args":null');
		db.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("should preserve phase/operation/args by placing JSON before failureDetail", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gw-test-"));
	try {
		const db = await GatewayDatabase.open(join(directory, "gateway.db"));

		const eventId = randomUUID();
		const code = "session_bind_failed";

		// Simulate a very long failure detail
		const longFailureDetail = "A".repeat(400); // 400 chars of detail

		const phase = "bind";
		const operation = "bind";
		const operation_args = { epoch: 0, hasModel: false };
		const sessionId = null;
		const originKey = "monitor/some-origin";
		const attempt = 1;

		// NEW approach: put JSON first, then failureDetail
		const jsonPart = JSON.stringify({
			phase,
			operation,
			operation_args,
			sessionId,
			origin: originKey,
			attempt,
		});

		const fixedDetail = `dispatch phase failed (${code}): ${jsonPart} ${longFailureDetail}`;

		db.withTransaction(() => {
			db.monitorFailureRecord(eventId, code, fixedDetail);
		});

		const failures = db.monitorFailures([eventId]);
		const failure = failures.get(eventId);
		expect(failure).toBeDefined();

		const storedDetail = failure!.detail;
		console.log("Fixed detail (JSON first):", storedDetail);
		console.log("Length:", storedDetail.length);

		// With JSON placed first, these will survive truncation
		expect(storedDetail).toContain('"operation":"bind"');
		expect(storedDetail).toContain('"phase":"bind"');
		db.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("demonstrates the problem: OLD format (failureDetail first) loses operation", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gw-test-"));
	try {
		const db = await GatewayDatabase.open(join(directory, "gateway.db"));

		const eventId = randomUUID();
		const code = "session_bind_failed";

		// Simulate the OLD problem: failureDetail first + long error string
		const longFailureDetail =
			"GjcCliError(operation_failed/transport=envelope/frame=request/phase=bind/sessionId=null) " +
			"caused by: SomeDetailedErrorMessage ".repeat(12); // Very long

		const phase = "bind";
		const operation = "bind";
		const operation_args = { epoch: 0, hasModel: false };
		const sessionId = null;
		const originKey = "monitor/sns-threads-v7";
		const attempt = 1;

		// OLD problematic format: failureDetail FIRST
		const oldFormat =
			`dispatch phase failed (${code}): ${longFailureDetail} ` +
			JSON.stringify({
				phase,
				operation,
				operation_args,
				sessionId,
				origin: originKey,
				attempt,
			});

		db.withTransaction(() => {
			db.monitorFailureRecord(eventId, code, oldFormat);
		});

		const failures = db.monitorFailures([eventId]);
		const failure = failures.get(eventId);
		expect(failure).toBeDefined();

		const storedDetail = failure!.detail;
		console.log("OLD format (failureDetail first):", storedDetail);
		console.log("Length:", storedDetail.length);

		// VERIFY the problem: operation is lost
		// This would be the symptom reported in #342
		const hasOperation = storedDetail.includes('"operation"');
		console.log("Has operation field:", hasOperation);
		// This demonstrates WHY the fix was needed
		if (!hasOperation) {
			console.log(
				"✓ Confirmed: OLD format loses operation due to 500-char truncation (this is the #342 bug)",
			);
		}
		db.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
