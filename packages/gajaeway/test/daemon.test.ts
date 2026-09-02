import { afterEach, expect, test } from "bun:test";
import { Daemon } from "../src/daemon";
import { deferred, until } from "./helpers";

afterEach(() => {
	process.exitCode = 0;
});

function lock(calls: string[]) {
	return { release: async () => void calls.push("lock") };
}

test("shutdown is ordered supervisor, admin plus adapters in parallel, gateway, then lock", async () => {
	const calls: string[] = [];
	const adapterGate = deferred<void>();
	const adminGate = deferred<void>();
	const daemon = new Daemon({ lock: lock(calls), log: () => {} });
	daemon.setSupervisor({
		stop: async () => void calls.push("supervisor"),
		stopAdapters: async () => {
			calls.push("adapters");
			await adapterGate.promise;
		},
	});
	daemon.setAdmin(
		{
			stop: () => {
				calls.push("admin");
				adminGate.resolve();
			},
		},
		{ close: () => calls.push("admin-port") },
	);
	daemon.setGateway({
		stop: async () => {
			calls.push("gateway");
		},
	});
	const stopping = daemon.stop("test");
	await until(() => calls.includes("adapters") && calls.includes("admin"));
	expect(calls.indexOf("supervisor")).toBeLessThan(calls.indexOf("admin"));
	expect(calls.indexOf("supervisor")).toBeLessThan(calls.indexOf("adapters"));
	expect(calls).not.toContain("gateway");
	adapterGate.resolve();
	await stopping;
	expect(calls.indexOf("gateway")).toBeGreaterThan(calls.indexOf("admin-port"));
	expect(calls).toEqual(expect.arrayContaining(["supervisor", "admin", "admin-port", "adapters", "gateway", "lock"]));
});

test("the force timer exits with the monotonic requested status when gateway shutdown wedges", async () => {
	const calls: string[] = [];
	const exited: number[] = [];
	const daemon = new Daemon({
		lock: lock(calls),
		exit: (code) => exited.push(code),
		log: () => {},
		forceStopTimeoutMs: 10,
	});
	daemon.requestExit(1);
	daemon.setGateway({ stop: async () => await new Promise<void>(() => {}) });
	void daemon.stop("wedged gateway");
	await until(() => exited.length === 1);
	expect(exited).toEqual([1]);
});

for (const [name, order] of [
	["signal then escalation", ["stop", "escalate"]],
	["escalation then signal", ["escalate", "stop"]],
	["gateway verb then escalation", ["stop", "escalate"]],
] as const) {
	test(`${name} cannot downgrade exit status`, async () => {
		const calls: string[] = [];
		const gatewayGate = deferred<void>();
		const daemon = new Daemon({ lock: lock(calls), log: () => {} });
		daemon.setGateway({ stop: async () => await gatewayGate.promise });
		let stopping: Promise<void> | undefined;
		for (const step of order) {
			if (step === "stop") stopping = daemon.stop(name);
			else daemon.requestExit(1);
		}
		gatewayGate.resolve();
		await stopping;
		expect(daemon.requestedStatus).toBe(1);
		expect(process.exitCode).toBe(1);
	});
}

test("a SIGTERM during boot stops the partial owner, records failure status, and reports config abort", async () => {
	const calls: string[] = [];
	const logs: string[] = [];
	const daemon = new Daemon({ lock: lock(calls), log: (line) => logs.push(line) });
	daemon.installSignalHandlers();
	process.emit("SIGTERM");
	await daemon.stopped;
	expect(() => daemon.assertBoot("config")).toThrow("boot aborted during config");
	expect(calls).toEqual(["lock"]);
	expect(daemon.requestedStatus).toBe(1);
	expect(logs).toContain("boot_aborted phase=config");
});
