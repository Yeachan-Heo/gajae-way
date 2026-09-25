import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DeliveryService } from "../src/delivery/delivery";
import { MONITOR_PROCEDURE_MAX_BYTES } from "../src/monitors/procedure";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";
import { ScriptedSessionPort } from "./session-port.fake";

/** Real registry + propagator over one long-lived event-type session (no roll between firings). */
async function harness(home: string) {
	const workspace = join(home, "workspace");
	await mkdir(join(home, "memory", "ops"), { recursive: true });
	await mkdir(workspace, { recursive: true });
	await symlink("../memory", join(workspace, "memory"), "dir");
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const registry = new MonitorRegistry(database);
	const prompts: string[] = [];
	const sessions = new Set<string>();
	const sessionPort = new ScriptedSessionPort({
		onSend: (input, scripted) => {
			prompts.push(input.text);
			sessions.add(input.sessionId);
			scripted.complete(
				input.opRef,
				JSON.stringify(
					(JSON.parse(input.text.match(/\[[^[]*\]$/s)![0]) as Array<{ eventId: string }>).map(({ eventId }) => ({
						eventId,
						note: "recorded",
					})),
				),
			);
		},
	});
	const pipeline = new MonitorPropagator({
		database,
		registry,
		sessionPort,
		repo: workspace,
		memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
		delivery: new DeliveryService(new DeliveryLedger(database)),
		emit: () => {},
	});
	return { database, registry, pipeline, prompts, sessions, workspace };
}

const sha256 = (text: string) => new Bun.CryptoHasher("sha256").update(text).digest("hex");

test("a procedure edit reaches the very next firing of a long-lived event-type session (#82)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-procedure-refresh-"));
	try {
		const { database, registry, pipeline, prompts, sessions } = await harness(home);
		const procedurePath = join(home, "memory", "ops", "gajaeway-feedback.md");
		const before = "# Feedback procedure\nReport new friction items.";
		await writeFile(procedurePath, before);
		const monitor = registry.add({
			name: "feedback",
			trigger: { kind: "cron", schedule: "40 11 * * *" },
			eventTypes: ["gajaeway.feedback"],
			burstPolicy: "serialize",
			procedureFiles: ["memory/ops/gajaeway-feedback.md"],
		});
		expect(registry.get(monitor.monitorId)?.procedureFiles).toEqual(["memory/ops/gajaeway-feedback.md"]);

		await pipeline.submitAwaitable(monitor.monitorId, "gajaeway.feedback", {});
		expect(prompts).toHaveLength(1);
		expect(prompts[0]).toContain("Report new friction items.");

		// The operator edits the doctrine while the session keeps running.
		const rule = "A zero-count report MUST enumerate the query scope it was derived from.";
		const after = `${before}\n${rule}`;
		await writeFile(procedurePath, after);

		await pipeline.submitAwaitable(monitor.monitorId, "gajaeway.feedback", {});
		expect(prompts).toHaveLength(2);
		// Same session, no rotation: the rule still arrives on this firing.
		expect(sessions.size).toBe(1);
		expect(prompts[1]).toContain(rule);
		expect(prompts[1]).toContain(sha256(after).slice(0, 12));

		// Each authored event records the procedure version that produced it.
		const [second, first] = database.monitorEventRows(monitor.monitorId);
		expect(first?.stage).toBe("authored_no_delivery");
		expect(JSON.parse(first!.procedure_json!)).toEqual([
			expect.objectContaining({ path: "memory/ops/gajaeway-feedback.md", status: "ok", sha256: sha256(before) }),
		]);
		expect(JSON.parse(second!.procedure_json!)).toEqual([
			expect.objectContaining({ status: "ok", sha256: sha256(after) }),
		]);
		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("procedure files are confined to the workspace and report their state instead of failing the firing", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-procedure-confine-"));
	try {
		const { database, registry, pipeline, prompts, workspace } = await harness(home);
		await writeFile(join(home, "outside.md"), "secret doctrine");
		await symlink(join(home, "outside.md"), join(workspace, "escape.md"));
		await writeFile(join(workspace, "big.md"), "x".repeat(MONITOR_PROCEDURE_MAX_BYTES + 1));
		const monitor = registry.add({
			name: "confined",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["confined.check"],
			burstPolicy: "serialize",
			procedureFiles: ["escape.md", "absent.md", "big.md"],
		});
		await pipeline.submitAwaitable(monitor.monitorId, "confined.check", {});
		expect(prompts).toHaveLength(1);
		expect(prompts[0]).not.toContain("secret doctrine");
		const [row] = database.monitorEventRows(monitor.monitorId);
		expect(row?.stage).toBe("authored_no_delivery");
		expect(JSON.parse(row!.procedure_json!).map((entry: { status: string }) => entry.status)).toEqual([
			"outside_root",
			"missing",
			"too_large",
		]);

		const base = { name: "bad", trigger: { kind: "cron", schedule: "* * * * *" }, eventTypes: ["bad"] } as const;
		expect(() => registry.add({ ...base, procedureFiles: ["/etc/passwd"] })).toThrow("relative paths");
		expect(() => registry.add({ ...base, procedureFiles: ["../gateway.db"] })).toThrow("relative paths");
		expect(() => registry.add({ ...base, procedureFiles: "ops.md" as never })).toThrow("list of relative paths");

		expect(() => registry.update({ monitorId: monitor.monitorId, procedureFiles: ["../gateway.db"] })).toThrow(
			"relative paths",
		);
		registry.update({ monitorId: monitor.monitorId, procedureFiles: [" big.md "] });
		expect(registry.get(monitor.monitorId)?.procedureFiles).toEqual(["big.md"]);
		registry.update({ monitorId: monitor.monitorId, name: "renamed" });
		expect(registry.get(monitor.monitorId)?.procedureFiles).toEqual(["big.md"]);
		registry.update({ monitorId: monitor.monitorId, procedureFiles: [] });
		expect(registry.get(monitor.monitorId)?.procedureFiles).toBeUndefined();
		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("a monitor without procedure files keeps the pre-existing prompt and records no version", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-procedure-none-"));
	try {
		const { database, registry, pipeline, prompts } = await harness(home);
		const monitor = registry.add({
			name: "plain",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["p"],
			burstPolicy: "serialize",
		});
		await pipeline.submitAwaitable(monitor.monitorId, "p", {});
		expect(prompts[0]!.startsWith("Author monitor events. Respond ONLY with a JSON array")).toBe(true);
		expect(database.monitorEventRows(monitor.monitorId)[0]?.procedure_json).toBeNull();
		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
