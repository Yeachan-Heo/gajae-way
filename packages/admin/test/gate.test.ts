import { describe, expect, test } from "bun:test";
import { type AuditEntry, DEFAULT_ALLOWLIST, type GateOptions, MutationGate } from "../src/gate";

function gate(overrides: GateOptions = {}) {
	const audit: AuditEntry[] = [];
	const instance = new MutationGate({
		audit: (entry) => {
			audit.push(entry);
		},
		now: () => new Date(0),
		...overrides,
	});
	return { instance, audit };
}

describe("DEFAULT_ALLOWLIST", () => {
	test("never contains an outbound-message operation", () => {
		expect(DEFAULT_ALLOWLIST.map((operation) => operation.method)).not.toContain("chat.send");
		expect(DEFAULT_ALLOWLIST.map((operation) => operation.id)).not.toContain("chat.send");
	});

	test("every entry maps to a concrete gateway method and declares its blast radius", () => {
		for (const operation of DEFAULT_ALLOWLIST) {
			expect(operation.method.length).toBeGreaterThan(0);
			expect(operation.summary.length).toBeGreaterThan(0);
			expect(["low", "medium", "high"]).toContain(operation.severity);
		}
	});

	test("only a destructive operation demands a typed target name", () => {
		for (const operation of DEFAULT_ALLOWLIST) {
			if (operation.confirmToken === undefined) continue;
			expect(operation.severity).toBe("high");
			// The token must be resolvable from a field, or the server cannot check it.
			expect(operation.fields.some((field) => field.kind === "monitor-ref")).toBe(true);
		}
	});

	test("a low-severity operation adds no ceremony that cannot fail", () => {
		for (const operation of DEFAULT_ALLOWLIST.filter((candidate) => candidate.severity === "low")) {
			expect(operation.confirmToken).toBeUndefined();
		}
	});

	test("every required field is typed, so no operator types json", () => {
		for (const operation of DEFAULT_ALLOWLIST) {
			for (const field of operation.fields) {
				expect(["text", "monitor-ref", "path", "json"]).toContain(field.kind);
				expect(field.label.length).toBeGreaterThan(0);
			}
		}
		expect(DEFAULT_ALLOWLIST.flatMap((operation) => operation.fields).some((field) => field.kind === "json")).toBe(
			false,
		);
	});
});

describe("MutationGate", () => {
	test("allows an allowlisted, confirmed, attributed mutation", async () => {
		const { instance, audit } = gate();
		const decision = await instance.evaluate({
			operationId: "ops.backup",
			actor: "형님",
			confirm: "ops.backup",
		});
		expect(decision.allowed).toBe(true);
		expect(audit).toEqual([
			{ at: new Date(0).toISOString(), operationId: "ops.backup", actor: "형님", decision: "allowed" },
		]);
	});

	test("rejects an operation outside the allowlist with 404", async () => {
		const { instance } = gate();
		const decision = await instance.evaluate({ operationId: "chat.send", actor: "a", confirm: "chat.send" });
		expect(decision).toMatchObject({ allowed: false, status: 404 });
	});

	test("requires an actor", async () => {
		const { instance } = gate();
		const decision = await instance.evaluate({ operationId: "ops.backup", confirm: "ops.backup" });
		expect(decision).toMatchObject({ allowed: false, status: 401 });
	});

	test("treats a whitespace actor as anonymous", async () => {
		const { instance } = gate();
		const decision = await instance.evaluate({
			operationId: "ops.backup",
			actor: "   ",
			confirm: "ops.backup",
		});
		expect(decision).toMatchObject({ allowed: false, status: 401 });
	});

	test("requires the confirmation to echo the operation id", async () => {
		const { instance } = gate();
		expect(await instance.evaluate({ operationId: "ops.backup", actor: "a" })).toMatchObject({
			status: 428,
		});
		expect(await instance.evaluate({ operationId: "ops.backup", actor: "a", confirm: "ops.integrity" })).toMatchObject({
			status: 428,
		});
	});

	test("audits rejections with the reason", async () => {
		const { instance, audit } = gate();
		await instance.evaluate({ operationId: "ops.backup", actor: "a" });
		const last = audit.at(-1);
		expect(last?.decision).toBe("rejected");
		expect(last?.reason).toMatch(/confirmation/);
	});

	test("a disabled deployment refuses before allowlist evaluation", async () => {
		const { instance, audit } = gate({ mutationsEnabled: false });
		const decision = await instance.evaluate({
			operationId: "ops.backup",
			actor: "형님",
			confirm: "ops.backup",
		});
		expect(decision).toMatchObject({ allowed: false, status: 403 });
		expect(audit.at(-1)).toMatchObject({ decision: "rejected" });
		expect(instance.mutationsEnabled).toBe(false);
	});

	test("a custom allowlist replaces the default entirely", async () => {
		const { instance } = gate({
			allowlist: [{ id: "only.this", method: "only.this", summary: "s", severity: "low", fields: [] }],
		});
		expect(instance.operations.map((operation) => operation.id)).toEqual(["only.this"]);
		expect(await instance.evaluate({ operationId: "ops.backup", actor: "a", confirm: "ops.backup" })).toMatchObject({
			status: 404,
		});
	});

	test("params are recorded in the audit trail", async () => {
		const { instance, audit } = gate();
		await instance.evaluate({
			operationId: "monitor.remove",
			actor: "형님",
			confirm: "monitor.remove",
			params: { monitorId: "m1" },
		});
		expect(audit.at(-1)).toMatchObject({ params: { monitorId: "m1" } });
	});

	test("record() lets a stricter surrounding check write its own rejection", async () => {
		const { instance, audit } = gate();
		await instance.record({
			operationId: "monitor.remove",
			actor: "형님",
			decision: "rejected",
			reason: "name mismatch",
		});
		expect(audit.at(-1)).toEqual({
			at: new Date(0).toISOString(),
			operationId: "monitor.remove",
			actor: "형님",
			decision: "rejected",
			reason: "name mismatch",
		});
	});
});
