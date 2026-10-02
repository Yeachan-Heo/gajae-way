import { expect, spyOn, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PROFILE_VERSION } from "@gajae-gateway/protocol";
import { AdminGateway, type AdminGatewayRetryOptions } from "../src/gateway";

type Peer = { end(): void; write(data: string): unknown };
type Frame = { type: string; id?: string; verb?: string };
type RetryTask = { callback: () => void; delayMs: number; cancelled: boolean; ran: boolean };

type FixtureOptions = {
	negotiate?: boolean;
	event?: { event: string; payload: unknown };
	onHello?: (socket: Peer) => void;
	onRequest?: (socket: Peer, frame: Frame) => void;
};

function listenGateway(path: string, options: FixtureOptions = {}) {
	let accepted = 0;
	let hellos = 0;
	let active = 0;
	let maxActive = 0;
	let stopped = false;
	const peers = new Set<Peer>();
	const requests: Frame[] = [];
	const listener = Bun.listen<{ buffer: string }>({
		unix: path,
		socket: {
			open(socket) {
				accepted++;
				active++;
				maxActive = Math.max(maxActive, active);
				peers.add(socket);
				socket.data = { buffer: "" };
			},
			data(socket, data) {
				socket.data.buffer += Buffer.from(data).toString("utf8");
				let newline = socket.data.buffer.indexOf("\n");
				while (newline >= 0) {
					const line = socket.data.buffer.slice(0, newline);
					socket.data.buffer = socket.data.buffer.slice(newline + 1);
					const frame = JSON.parse(line) as Frame;
					if (frame.type === "hello") {
						hellos++;
						options.onHello?.(socket);
						if (options.negotiate !== false) {
							socket.write(
								`${JSON.stringify({
									v: PROFILE_VERSION,
									type: "negotiated",
									payload: { profileVersion: PROFILE_VERSION, capabilities: [] },
								})}\n`,
							);
							if (options.event)
								socket.write(
									`${JSON.stringify({
										v: PROFILE_VERSION,
										type: "event",
										event: options.event.event,
										payload: options.event.payload,
									})}\n`,
								);
						}
					} else if (frame.type === "request") {
						requests.push(frame);
						if (options.onRequest) options.onRequest(socket, frame);
						else
							socket.write(
								`${JSON.stringify({ v: PROFILE_VERSION, type: "response", id: frame.id, result: { ok: true } })}\n`,
							);
					}
					newline = socket.data.buffer.indexOf("\n");
				}
			},
			close(socket) {
				if (peers.delete(socket)) active--;
			},
		},
	});
	return {
		requests,
		get accepted() {
			return accepted;
		},
		get helloCount() {
			return hellos;
		},
		get active() {
			return active;
		},
		get maxActive() {
			return maxActive;
		},
		stop() {
			if (stopped) return;
			stopped = true;
			listener.stop(true);
		},
	};
}

async function until(predicate: () => boolean): Promise<void> {
	const deadline = Date.now() + 2_000;
	while (!predicate()) {
		if (Date.now() >= deadline) throw new Error("timed out waiting for gateway fixture");
		await Bun.sleep(5);
	}
}

function retryScheduler() {
	const tasks: RetryTask[] = [];
	const options: AdminGatewayRetryOptions = {
		setTimeout: ((callback: () => void, delayMs: number) => {
			const task = { callback, delayMs, cancelled: false, ran: false };
			tasks.push(task);
			return task as unknown as ReturnType<typeof setTimeout>;
		}) as typeof setTimeout,
		clearTimeout(timer) {
			(timer as unknown as RetryTask).cancelled = true;
		},
	};
	return {
		options,
		pending: () => tasks.filter((task) => !task.cancelled && !task.ran),
		runNext: () => {
			const task = tasks.find((item) => !item.cancelled && !item.ran);
			if (!task) throw new Error("no scheduled retry");
			task.ran = true;
			task.callback();
			return task;
		},
	};
}

test("the admin gateway reconnects without replaying work and reattaches events once", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-admin-gateway-"));
	const path = join(directory, "gateway.sock");
	let first: ReturnType<typeof listenGateway> | undefined;
	let second: ReturnType<typeof listenGateway> | undefined;
	let third: ReturnType<typeof listenGateway> | undefined;
	let gateway: AdminGateway | undefined;
	const retries = retryScheduler();
	try {
		first = listenGateway(path, {
			event: { event: "monitor.event", payload: { source: "first" } },
			onRequest: () => {},
		});
		gateway = await AdminGateway.connect(path, retries.options);
		const connections: boolean[] = [];
		const offConnection = gateway.onConnectionChange((connected) => connections.push(connected));
		expect(connections).toEqual([true]);
		const delivered: string[] = [];
		gateway.on("monitor.event", (payload) => delivered.push((payload as { source: string }).source));
		await until(() => delivered.length === 1);
		const pending = gateway.request("ops.cycle");
		await until(() => first?.requests.length === 1);
		first.stop();
		await expect(pending).rejects.toThrow("gateway connection closed");
		await until(() => gateway?.connected === false);
		expect(gateway.connected).toBe(false);
		expect(connections).toEqual([true, false]);
		await expect(gateway.request("ops.cycle")).rejects.toThrow("gateway is not connected");
		expect(first.requests).toHaveLength(1);

		second = listenGateway(path, { event: { event: "monitor.event", payload: { source: "second" } } });
		await until(() => retries.pending().length === 1);
		retries.runNext();
		await until(() => gateway?.connected === true && delivered.length === 2);
		expect(delivered).toEqual(["first", "second"]);
		expect(connections).toEqual([true, false, true]);
		expect(first.active).toBe(0);
		expect(second.active).toBe(1);
		expect(second.maxActive).toBe(1);
		expect(second.requests).toHaveLength(0);
		await gateway.request("ops.cycle");
		expect(second.requests).toHaveLength(1);

		second.stop();
		await until(() => gateway?.connected === false);
		third = listenGateway(path, { event: { event: "monitor.event", payload: { source: "third" } } });
		await until(() => retries.pending().length === 1);
		retries.runNext();
		await until(() => gateway?.connected === true && delivered.length === 3);
		expect(delivered).toEqual(["first", "second", "third"]);
		expect(third.active).toBe(1);
		expect(third.maxActive).toBe(1);
		expect(connections).toEqual([true, false, true, false, true]);
		offConnection();
		const closeStates: boolean[] = [];
		gateway.onConnectionChange((connected) => closeStates.push(connected));
		await gateway.close();
		await gateway.close();
		expect(closeStates).toEqual([true, false]);
		expect(connections).toEqual([true, false, true, false, true]);
		const closedStates: boolean[] = [];
		gateway.onConnectionChange((connected) => closedStates.push(connected));
		expect(closedStates).toEqual([false]);
	} finally {
		await gateway?.close();
		first?.stop();
		second?.stop();
		third?.stop();
		await rm(directory, { recursive: true, force: true });
	}
});

test("reconnect backoff is jittered, capped, reset after recovery, and logged only on transitions", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-admin-retry-backoff-"));
	const path = join(directory, "gateway.sock");
	let initial: ReturnType<typeof listenGateway> | undefined;
	let replacement: ReturnType<typeof listenGateway> | undefined;
	let gateway: AdminGateway | undefined;
	const retries = retryScheduler();
	const logs: string[] = [];
	let random = 0.999;
	const errorSpy = spyOn(console, "error").mockImplementation((line) => logs.push(String(line)));
	try {
		initial = listenGateway(path);
		gateway = await AdminGateway.connect(path, { ...retries.options, random: () => random });
		initial.stop();
		await until(() => gateway?.connected === false);

		const baseDelays = [500, 1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000];
		for (const baseDelay of baseDelays) {
			await until(() => retries.pending().length === 1);
			const retry = retries.pending()[0]!;
			const jitter = Math.floor(random * Math.max(1, baseDelay / 4));
			expect(retry.delayMs).toBe(Math.min(30_000, baseDelay + jitter));
			expect(retry.delayMs).toBeLessThanOrEqual(30_000);
			retries.runNext();
			await until(() => retries.pending().length === 1);
		}
		expect(logs).toEqual(["Admin gateway disconnected; reconnecting."]);

		replacement = listenGateway(path);
		await until(() => retries.pending().length === 1);
		expect(retries.pending()[0]?.delayMs).toBe(30_000);
		retries.runNext();
		await until(() => gateway?.connected === true);
		expect(logs).toEqual(["Admin gateway disconnected; reconnecting.", "Admin gateway reconnected."]);

		random = 0;
		replacement.stop();
		await until(() => gateway?.connected === false);
		await until(() => retries.pending().length === 1);
		expect(retries.pending()[0]?.delayMs).toBe(500);
		expect(logs).toEqual([
			"Admin gateway disconnected; reconnecting.",
			"Admin gateway reconnected.",
			"Admin gateway disconnected; reconnecting.",
		]);
	} finally {
		errorSpy.mockRestore();
		await gateway?.close();
		initial?.stop();
		replacement?.stop();
		await rm(directory, { recursive: true, force: true });
	}
});

test("shutdown clears a scheduled retry", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-admin-retry-close-"));
	const path = join(directory, "gateway.sock");
	let initial: ReturnType<typeof listenGateway> | undefined;
	let replacement: ReturnType<typeof listenGateway> | undefined;
	let gateway: AdminGateway | undefined;
	const retries = retryScheduler();
	try {
		initial = listenGateway(path);
		gateway = await AdminGateway.connect(path, retries.options);
		initial.stop();
		await until(() => gateway?.connected === false);
		await until(() => retries.pending().length === 1);
		const retry = retries.pending()[0]!;
		await gateway.close();
		replacement = listenGateway(path);
		expect(retry.cancelled).toBe(true);
		expect(replacement.accepted).toBe(0);
		expect(replacement.active).toBe(0);
	} finally {
		await gateway?.close();
		initial?.stop();
		replacement?.stop();
		await rm(directory, { recursive: true, force: true });
	}
});

test("shutdown aborts an in-flight reconnect negotiation", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-admin-negotiation-close-"));
	const path = join(directory, "gateway.sock");
	let initial: ReturnType<typeof listenGateway> | undefined;
	let negotiating: ReturnType<typeof listenGateway> | undefined;
	let gateway: AdminGateway | undefined;
	const retries = retryScheduler();
	try {
		initial = listenGateway(path);
		gateway = await AdminGateway.connect(path, retries.options);
		initial.stop();
		await until(() => gateway?.connected === false);
		await until(() => retries.pending().length === 1);
		negotiating = listenGateway(path, {
			negotiate: false,
		});
		retries.runNext();
		await until(() => negotiating?.helloCount === 1);
		await gateway.close();
		await until(() => negotiating?.active === 0);
		expect(negotiating.accepted).toBe(1);
		expect(gateway.connected).toBe(false);
	} finally {
		await gateway?.close();
		initial?.stop();
		negotiating?.stop();
		await rm(directory, { recursive: true, force: true });
	}
});

test("an initial connection failure remains an immediate admin startup failure", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-admin-initial-failure-"));
	try {
		const path = join(directory, "missing.sock");
		const child = Bun.spawn([process.execPath, join(import.meta.dir, "../src/main.ts"), "serve"], {
			stdout: "pipe",
			stderr: "pipe",
			env: { ...process.env, GAJAEWAY_SOCKET: path, GAJAEWAY_HOME: directory },
		});
		const [stderr, code] = await Promise.all([new Response(child.stderr).text(), child.exited]);
		expect(code).toBe(1);
		expect(stderr).toContain(`Unable to connect to gateway socket ${path}`);
		expect(stderr).toContain("Start the daemon out-of-band first.");
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
