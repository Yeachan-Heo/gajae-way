import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	awaitReadyEndpoint,
	checkReady,
	controlUrl,
	discoverEndpoints,
	endpointDirectory,
	parseEndpoint,
	type SubsessionEndpoint,
} from "../src/endpoint";

let worktree: string;

beforeEach(async () => {
	worktree = await mkdtemp(join(tmpdir(), "gajaeway-subsession-"));
});

afterEach(async () => {
	await rm(worktree, { recursive: true, force: true });
});

async function publish(name: string, body: unknown): Promise<string> {
	const directory = endpointDirectory(worktree);
	await mkdir(directory, { recursive: true });
	const path = join(directory, name);
	await writeFile(path, typeof body === "string" ? body : JSON.stringify(body));
	return path;
}

const live = { isPidAlive: () => true, connect: async () => true };

function endpoint(overrides: Partial<SubsessionEndpoint> = {}): SubsessionEndpoint {
	return {
		version: 1,
		sessionId: "session-a",
		pid: 4242,
		url: "ws://127.0.0.1:8181",
		token: "tok en/+",
		sourcePath: "/tmp/session-a.json",
		...overrides,
	};
}

describe("parseEndpoint", () => {
	test("prefers the body sessionId over the filename", () => {
		const result = parseEndpoint(
			{ version: 1, sessionId: "from-body", pid: 10, url: "ws://x", token: "t" },
			"/tmp/from-filename.json",
		);
		expect(result.ok && result.endpoint.sessionId).toBe("from-body");
	});

	test("falls back to the filename stem when sessionId is absent", () => {
		const result = parseEndpoint({ version: 1, pid: 10, url: "ws://x", token: "t" }, "/tmp/abc.json");
		expect(result.ok && result.endpoint.sessionId).toBe("abc");
	});

	test.each([
		["missing url", { pid: 1, token: "t" }],
		["missing token", { pid: 1, url: "ws://x" }],
		["missing or invalid pid", { url: "ws://x", token: "t" }],
		["missing or invalid pid", { pid: 0, url: "ws://x", token: "t" }],
	])("rejects an endpoint with %s", (reason, body) => {
		const result = parseEndpoint(body, "/tmp/a.json");
		expect(result.ok).toBe(false);
		expect(!result.ok && result.reason).toBe(reason);
	});
});

describe("controlUrl", () => {
	test("url-encodes the token", () => {
		expect(controlUrl({ url: "ws://127.0.0.1:1", token: "a b/+" })).toBe("ws://127.0.0.1:1/?token=a%20b%2F%2B");
	});

	test("does not double a trailing slash", () => {
		expect(controlUrl({ url: "ws://h/", token: "t" })).toBe("ws://h/?token=t");
	});
});

describe("discoverEndpoints", () => {
	test("returns nothing for a worktree that never spawned a session", async () => {
		expect(await discoverEndpoints(worktree)).toEqual({ endpoints: [], failures: [] });
	});

	test("separates parseable endpoints from broken ones", async () => {
		await publish("good.json", { version: 1, sessionId: "good", pid: 7, url: "ws://x", token: "t" });
		await publish("broken.json", "{ not json");
		await publish("ignored.txt", "noise");
		const result = await discoverEndpoints(worktree);
		expect(result.endpoints.map((item) => item.sessionId)).toEqual(["good"]);
		expect(result.failures).toHaveLength(1);
	});
});

describe("checkReady", () => {
	test("a published file alone is not ready: stale wins", async () => {
		const result = await checkReady(endpoint({ stale: true }), live);
		expect(result).toMatchObject({ ready: false, reason: "marked-stale" });
	});

	test("a dead pid is not ready even when the file looks healthy", async () => {
		const result = await checkReady(endpoint(), { ...live, isPidAlive: () => false });
		expect(result).toMatchObject({ ready: false, reason: "pid-dead" });
	});

	test("a failed authenticated connect is not ready", async () => {
		const result = await checkReady(endpoint(), { ...live, connect: async () => false });
		expect(result).toMatchObject({ ready: false, reason: "connect-failed" });
	});

	test("all four conditions satisfied is ready, and the probe gets the tokenised url", async () => {
		const seen: string[] = [];
		const result = await checkReady(endpoint(), {
			isPidAlive: () => true,
			connect: async (url) => {
				seen.push(url);
				return true;
			},
		});
		expect(result.ready).toBe(true);
		expect(seen).toEqual(["ws://127.0.0.1:8181/?token=tok%20en%2F%2B"]);
	});

	test("does not probe the socket once the pid is known dead", async () => {
		let connects = 0;
		await checkReady(endpoint(), {
			isPidAlive: () => false,
			connect: async () => {
				connects += 1;
				return true;
			},
		});
		expect(connects).toBe(0);
	});
});

describe("awaitReadyEndpoint", () => {
	test("reports no-endpoint rather than hanging when nothing is ever published", async () => {
		let clock = 0;
		const result = await awaitReadyEndpoint(worktree, {
			timeoutMs: 10,
			pollMs: 5,
			probes: live,
			now: () => (clock += 6),
			sleep: async () => {},
		});
		expect(result).toMatchObject({ ready: false, reason: "no-endpoint" });
	});

	test("returns the endpoint once it becomes ready", async () => {
		await publish("s.json", { version: 1, sessionId: "s", pid: 9, url: "ws://x", token: "t" });
		let alive = false;
		const result = await awaitReadyEndpoint(worktree, {
			timeoutMs: 1_000,
			pollMs: 1,
			probes: {
				isPidAlive: () => {
					const previous = alive;
					alive = true;
					return previous;
				},
				connect: async () => true,
			},
			sleep: async () => {},
		});
		expect(result.ready).toBe(true);
	});

	test("surfaces the unhealthy reason instead of no-endpoint when a session exists", async () => {
		await publish("s.json", { version: 1, sessionId: "s", pid: 9, url: "ws://x", token: "t" });
		let clock = 0;
		const result = await awaitReadyEndpoint(worktree, {
			timeoutMs: 10,
			pollMs: 5,
			probes: { isPidAlive: () => false, connect: async () => true },
			now: () => (clock += 6),
			sleep: async () => {},
		});
		expect(result).toMatchObject({ ready: false, reason: "pid-dead" });
	});
});
