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

	test("shape carries the error class and known dispatch frames", () => {
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
		const known = ["withTransaction", "#dispatchBatch", "#author", "submit", "reconcile", "ensureSession"];
		const error = new Error("boom");
		error.stack = ["Error: boom", ...known.map((fn, i) => `    at ${fn} (/p:${i}:1)`)].join("\n");
		expect(classifyFailure(error).shape).toBe("Error@withTransaction<-#dispatchBatch<-#author");
	});

	test("an all-unknown stack degrades to the class name rather than a row of placeholders", () => {
		const error = new Error("boom");
		error.stack = ["Error: boom", ...Array.from({ length: 40 }, (_, i) => `    at frame${i} (/p:${i}:1)`)].join("\n");
		expect(classifyFailure(error).shape).toBe("Error");
	});

	test("skips anonymous frames and bare paths entirely", () => {
		const error = new Error("boom");
		error.stack = ["Error: boom", "    at <anonymous> (/p:1:1)", "    at async withTransaction (/p:2:2)"].join("\n");
		expect(classifyFailure(error).shape).toBe("Error@withTransaction");
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

	test("does not echo a hostile error.name (allowlist, not character filtering)", () => {
		const error = new Error("boom");
		Object.defineProperty(error, "name", { value: "ghp_SECRETVALUE" });
		const { shape } = classifyFailure(error);
		expect(shape).not.toContain("ghp_SECRETVALUE");
		expect(shape).toBe("Error");
	});

	test("does not echo a hostile constructor name", () => {
		class ghp_SECRETVALUE extends Error {}
		const error = new ghp_SECRETVALUE("boom");
		error.stack = "x\n    at withTransaction (/p:1:1)";
		const { shape } = classifyFailure(error);
		expect(shape).not.toContain("ghp_SECRETVALUE");
		expect(shape).toBe("Error@withTransaction");
	});

	test("does not echo a hostile stack frame identifier", () => {
		const error = new Error("boom");
		error.stack = ["Error: boom", "    at ghp_SECRETVALUE (/p:1:1)", "    at withTransaction (/p:2:2)"].join("\n");
		const { shape } = classifyFailure(error);
		expect(shape).not.toContain("ghp_SECRETVALUE");
		expect(shape).toBe("Error@unknown_frame<-withTransaction");
	});

	test("a credential-shaped frame is identifier-shaped, proving character filters are insufficient", () => {
		// Regression guard for the first version of this code, which kept any
		// [A-Za-z0-9_#$.] value and therefore leaked exactly this string.
		expect(/^[A-Za-z0-9_#$.]+$/.test("ghp_SECRETVALUE")).toBe(true);
		const error = new Error("boom");
		error.stack = "Error: boom\n    at ghp_SECRETVALUE (/p:1:1)";
		expect(classifyFailure(error).shape).toBe("Error");
	});

	test("keeps recognized non-generic error classes", () => {
		const error = new RangeError("Cannot use a closed database");
		error.stack = "RangeError: x\n    at withTransaction (/p:1:1)";
		expect(classifyFailure(error).shape).toBe("RangeError@withTransaction");
	});
});
