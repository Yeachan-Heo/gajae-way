import { expect, test } from "bun:test";
import type { GatewayServer, LocalGatewayPort } from "@gajaeway/gateway";
import { type AdapterHandle, AdapterSupervisor, type Generation, restartDecision } from "../src/supervisor";
import { deferred, until } from "./helpers";

class FakePort implements LocalGatewayPort {
	closed = 0;
	readonly handlers = new Map<string, Set<(payload: unknown) => void>>();

	constructor(readonly label: string) {}

	async request<T = unknown>(): Promise<T> {
		return {} as T;
	}

	on(event: string, handler: (payload: unknown) => void): () => void {
		const handlers = this.handlers.get(event) ?? new Set();
		handlers.add(handler);
		this.handlers.set(event, handlers);
		return () => handlers.delete(handler);
	}

	async open(): Promise<{ readonly replayed: number }> {
		return { replayed: 0 };
	}

	close(): void {
		this.closed++;
	}
}

class FakeServer implements GatewayServer {
	readonly ports: FakePort[] = [];

	async stop(): Promise<void> {}

	attach(label: string): LocalGatewayPort {
		const port = new FakePort(label);
		this.ports.push(port);
		return port;
	}
}

function handle(): {
	readonly handle: AdapterHandle;
	readonly settle: ReturnType<typeof deferred<void>>;
	readonly stops: () => number;
} {
	const settled = deferred<void>();
	let stopCalls = 0;
	return {
		handle: { settled: settled.promise, stop: async () => void stopCalls++ },
		settle: settled,
		stops: () => stopCalls,
	};
}

test("restartDecision follows the 1/2/4/8 second table, reset rule, and jitter bounds", () => {
	for (const [failures, base] of [
		[1, 1_000],
		[2, 2_000],
		[3, 4_000],
		[4, 8_000],
	] as const) {
		expect(restartDecision(failures, 0, () => 0)).toEqual({ action: "restart", delayMs: base * 0.75 });
		expect(restartDecision(failures, 0, () => 1)).toEqual({ action: "restart", delayMs: base * 1.25 });
	}
	expect(restartDecision(5, 0, () => 0.5)).toEqual({ action: "escalate" });
	expect(restartDecision(5, 60_000, () => 0.5)).toEqual({ action: "restart", delayMs: 1_000 });
});

test("a settled rejection creates a fresh monotonic generation and local port", async () => {
	const server = new FakeServer();
	const generations: Generation[] = [];
	const handles: ReturnType<typeof handle>[] = [];
	const delays: number[] = [];
	const supervisor = new AdapterSupervisor({
		server,
		escalate: () => {},
		random: () => 0.5,
		delay: async (ms) => void delays.push(ms),
	});
	await supervisor.start("discord", async (generation) => {
		generations.push(generation);
		const next = handle();
		handles.push(next);
		return next.handle;
	});
	await until(() => handles.length === 1);
	handles[0]?.settle.reject(new Error("temporary"));
	await until(() => handles.length === 2);
	expect(generations.map((generation) => generation.id)).toEqual([1, 2]);
	expect(server.ports.map((port) => port.label)).toEqual(["discord#1", "discord#2"]);
	expect(server.ports[0]?.closed).toBeGreaterThan(0);
	expect(delays).toEqual([1_000]);
	await supervisor.stopAdapters();
});

test("stop cancels a pending restart and failures observed afterward do not escalate", async () => {
	const server = new FakeServer();
	const current = handle();
	const escalations: string[] = [];
	let starts = 0;
	const supervisor = new AdapterSupervisor({
		server,
		escalate: (name) => {
			escalations.push(name);
		},
		delay: async () => await new Promise<void>(() => {}),
	});
	await supervisor.start("telegram", async () => {
		starts++;
		return current.handle;
	});
	await until(() => starts === 1);
	await until(() => server.ports[0]?.label === "telegram#1");
	await Bun.sleep(0);
	await supervisor.stop();
	current.settle.reject(new Error("stopping"));
	await Bun.sleep(10);
	expect(starts).toBe(1);
	expect(escalations).toEqual([]);
	await supervisor.stopAdapters();
});

test("an in-flight factory is aborted, and a late handle is disposed immediately", async () => {
	const server = new FakeServer();
	const late = deferred<AdapterHandle>();
	const created: Generation[] = [];
	const produced = handle();
	const logs: string[] = [];
	const supervisor = new AdapterSupervisor({ server, escalate: () => {}, log: (line) => logs.push(line) });
	await supervisor.start("discord", async (generation) => {
		created.push(generation);
		return await late.promise;
	});
	await until(() => created.length === 1);
	await supervisor.stop();
	expect(created[0]?.signal.aborted).toBe(true);
	late.resolve(produced.handle);
	await until(() => produced.stops() === 1);
	expect(logs).toContain("adapter_disposed_after_stop adapter=discord generation=1");
	expect(server.ports[0]?.closed).toBeGreaterThan(0);
});

test("five consecutive failures escalate exactly once after four restart delays", async () => {
	const server = new FakeServer();
	const handles: ReturnType<typeof handle>[] = [];
	const escalations: string[] = [];
	const supervisor = new AdapterSupervisor({
		server,
		escalate: (name) => {
			escalations.push(name);
		},
		random: () => 0.5,
		delay: async () => {},
	});
	await supervisor.start("telegram", async () => {
		const next = handle();
		handles.push(next);
		return next.handle;
	});
	for (let index = 0; index < 5; index++) {
		await until(() => handles.length === index + 1);
		handles[index]?.settle.reject(new Error(`failure ${index + 1}`));
	}
	await until(() => escalations.length === 1);
	expect(escalations).toEqual(["telegram"]);
	await supervisor.stopAdapters();
});

test("a stuck adapter stop times out after its bounded deadline and still closes the port", async () => {
	const server = new FakeServer();
	const logs: string[] = [];
	const supervisor = new AdapterSupervisor({
		server,
		escalate: () => {},
		log: (line) => logs.push(line),
		stopTimeoutMs: 10,
	});
	await supervisor.start("discord", async () => ({
		settled: new Promise<void>(() => {}),
		stop: async () => await new Promise<void>(() => {}),
	}));
	await until(() => logs.includes("adapter_started adapter=discord generation=1"));
	await supervisor.stopAdapters();
	expect(logs).toContain("adapter_stop_timeout adapter=discord");
	expect(server.ports[0]?.closed).toBeGreaterThan(0);
});
