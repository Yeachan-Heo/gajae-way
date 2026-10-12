import { afterEach, expect, spyOn, test } from "bun:test";

import { createServer, type Server } from "node:net";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { encodeFrame, PROFILE_VERSION } from "@gajae-gateway/protocol";
import { GajaewayClient } from "../src/client";

const servers: Server[] = [];
const homes: string[] = [];

afterEach(async () => {
	for (const server of servers.splice(0)) await new Promise<void>((resolve) => server.close(() => resolve()));
	for (const home of homes.splice(0)) await rm(home, { recursive: true, force: true });
});

async function temporarySocket(): Promise<{ home: string; path: string }> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-sdk-backoff-"));
	homes.push(home);
	return { home, path: join(home, "gateway.sock") };
}

async function startGateway(path: string): Promise<Server> {
	const server = createServer((socket) => {
		let negotiated = false;
		socket.on("data", () => {
			if (negotiated) return;
			negotiated = true;
			socket.write(
				encodeFrame({
					v: PROFILE_VERSION,
					type: "negotiated",
					payload: { profileVersion: PROFILE_VERSION, capabilities: [] },
				}),
			);
		});
	});
	await new Promise<void>((resolve, reject) => {
		server.once("error", reject);
		server.listen(path, resolve);
	});
	servers.push(server);
	return server;
}

test("connectSocketWithBackoff retries until a late Unix socket appears", async () => {
	const { path } = await temporarySocket();
	const retries: Array<{ attempt: number; delayMs: number }> = [];
	let serverStarting = false;
	let server: Server | undefined;
	const clientPromise = GajaewayClient.connectSocketWithBackoff(path, {
		initialMs: 5,
		maxMs: 10,
		clientName: "backoff-test",
		onRetry: (attempt, delayMs) => {
			retries.push({ attempt, delayMs });
			if (!serverStarting) {
				serverStarting = true;
				void startGateway(path).then((started) => {
					server = started;
				});
			}
		},
	});
	const client = await clientPromise;
	try {
		expect(retries.length).toBeGreaterThan(0);
		expect(retries.map(({ attempt }) => attempt)).toEqual(retries.map((_, index) => index + 1));
	} finally {
		await client.close();
		if (server) await new Promise<void>((resolve) => server!.close(() => resolve()));
		const index = servers.indexOf(server!);
		if (index >= 0) servers.splice(index, 1);
	}
});

test("connectSocketWithBackoff doubles delays and respects the maximum cap", async () => {
	const { path } = await temporarySocket();
	const retries: number[] = [];
	await expect(
		GajaewayClient.connectSocketWithBackoff(path, {
			initialMs: 5,
			maxMs: 12,
			deadlineMs: 55,
			onRetry: (_attempt, delayMs) => retries.push(delayMs),
		}),
	).rejects.toBeInstanceOf(Error);
	expect(retries.length).toBeGreaterThan(2);
	expect(retries[0]).toBe(5);
	expect(retries[1]).toBe(10);
	expect(retries[2]).toBe(12);
	expect(retries.slice(2).every((delayMs) => delayMs <= 12)).toBe(true);
});

test("connectSocketWithBackoff rejects with the last error when its deadline expires", async () => {
	const failures = [new Error("first socket failure"), new Error("last socket failure")];
	let calls = 0;
	const connectSocket = spyOn(GajaewayClient, "connectSocket").mockImplementation(async () => {
		throw failures[Math.min(calls++, failures.length - 1)];
	});
	const retries: Array<{ attempt: number; delayMs: number; error: unknown }> = [];
	try {
		await expect(
			GajaewayClient.connectSocketWithBackoff("unused", {
				initialMs: 5,
				maxMs: 5,
				deadlineMs: 25,
				onRetry: (attempt, delayMs, error) => retries.push({ attempt, delayMs, error }),
			}),
		).rejects.toBe(failures.at(-1));
		expect(retries.length).toBeGreaterThan(0);
		expect(retries.at(-1)?.error).toBe(failures.at(-1));
		expect(retries.every(({ delayMs }) => delayMs <= 5)).toBe(true);
	} finally {
		connectSocket.mockRestore();
	}
});
