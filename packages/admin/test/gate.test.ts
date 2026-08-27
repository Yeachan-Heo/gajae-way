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
	});

	test("every entry maps to a concrete gateway method", () => {
		for (const operation of DEFAULT_ALLOWLIST) {
			expect(operation.method.length).toBeGreaterThan(0);
			expect(operation.summary.length).toBeGreaterThan(0);
		}
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
	});

	test("a custom allowlist replaces the default entirely", async () => {
		const { instance } = gate({
			allowlist: [{ id: "only.this", method: "only.this", summary: "s" }],
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
});
