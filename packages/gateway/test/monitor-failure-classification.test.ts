import { describe, expect, test } from "bun:test";
import { classifyFailure } from "../src/monitors/propagate";

/**
 * Issue #64: every dispatch failure used to collapse to the fixed string
 * `dispatch phase failed (internal_error)`. Two hosts produced 715+
 * byte-identical `monitor_failures` rows from demonstrably different root
 * causes, and on one of them the underlying error existed nowhere at all — the
 * daemon log only carried the wrapper line. These tests pin both halves of the
 * fix: causes are distinguishable, and distinguishing them leaks nothing.
 */
describe("dispatch failure classification", () => {
	test("maps the live closed-handle crash to its own code instead of internal_error", () => {
		const error = new Error("Database has closed");
		expect(classifyFailure(error).code).toBe("database_closed");
	});

	test("also matches the other observed closed-handle wording", () => {
		expect(classifyFailure(new RangeError("Cannot use a closed database")).code).toBe("database_closed");
	});

	test("keeps the pre-existing phase codes", () => {
		expect(classifyFailure(new Error("authoring response is not an array")).code).toBe("authoring_response_invalid");
		expect(classifyFailure(new Error("sendTurn stream produced no assistant text")).code).toBe("authoring_turn_failed");
		expect(classifyFailure(new Error("ensureSession refused the bind")).code).toBe("session_bind_failed");
	});

	test("still falls back to internal_error for genuinely unknown causes", () => {
		expect(classifyFailure(new Error("something nobody has classified yet")).code).toBe("internal_error");
	});

	test("shape carries the error class and stack function names", () => {
		const error = new Error("Database has closed");
		error.stack = [
			"Error: Database has closed",
			"    at withTransaction (/$bunfs/root/gajaeway-gateway:1881:37)",
			"    at #dispatchBatch (/$bunfs/root/gajaeway-gateway:1902:11)",
		].join("\n");
		const { shape } = classifyFailure(error);
		expect(shape).toBe("Error@withTransaction<-#dispatchBatch");
	});

	test("two different causes produce two different shapes", () => {
		const a = new Error("Database has closed");
		a.stack = "Error: x\n    at withTransaction (/p:1:1)";
		const b = new TypeError("nope");
		b.stack = "TypeError: nope\n    at deliverPayload (/p:2:2)";
		const first = classifyFailure(a);
		const second = classifyFailure(b);
		expect(first.shape).not.toBe(second.shape);
	});

	test("never leaks the message body, paths, URLs or line numbers", () => {
		const error = new Error(
			"token=ghp_SECRETVALUE opening /Users/someone/private/db.sqlite via https://internal.example",
		);
		error.stack = [
			"Error: token=ghp_SECRETVALUE opening /Users/someone/private/db.sqlite",
			"    at withTransaction (/Users/someone/private/gateway.ts:1881:37)",
		].join("\n");
		const { shape } = classifyFailure(error);
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
			expect(shape).not.toContain(secret);
		}
		expect(shape).toBe("Error@withTransaction");
	});

	test("caps the frame count so the detail column cannot grow unbounded", () => {
		const error = new Error("boom");
		error.stack = ["Error: boom", ...Array.from({ length: 40 }, (_, i) => `    at frame${i} (/p:${i}:1)`)].join("\n");
		const { shape } = classifyFailure(error);
		expect(shape).toBe("Error@frame0<-frame1<-frame2");
	});

	test("skips anonymous frames, which carry no diagnostic value once paths are stripped", () => {
		const error = new Error("boom");
		error.stack = ["Error: boom", "    at <anonymous> (/p:1:1)", "    at async realFrame (/p:2:2)"].join("\n");
		expect(classifyFailure(error).shape).toBe("Error@realFrame");
	});

	test("survives non-Error throws without inventing a stack", () => {
		expect(classifyFailure("just a string").shape).toBe("string:non-error");
		expect(classifyFailure(undefined).shape).toBe("undefined:non-error");
		expect(classifyFailure({ secret: "ghp_TOKEN" }).shape).toBe("object:non-error");
	});

	test("an Error with no stack degrades to the class name alone", () => {
		const error = new Error("boom");
		error.stack = undefined;
		expect(classifyFailure(error).shape).toBe("Error");
	});
});
