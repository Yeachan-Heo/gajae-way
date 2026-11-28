import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GajaewayClient } from "@gajae-gateway/sdk";
import * as updates from "../../cli/src/update";
import { runConsole } from "../src/console-dashboard";
import type { KeyPressEvent } from "../src/tui-core";

const timestamp = "2026-10-26T10:00:00.000Z";
function responses(): Record<string, unknown> {
	return {
		"gateway.status": {
			profileVersion: "0.1",
			capabilities: [],
			pid: 4242,
			startedAt: timestamp,
			schemaVersion: 7,
			sessions: { active: 1 },
		},
		"session.list": {
			sessions: [
				{
					origin: { platform: "discord", kind: "channel", conversationId: "dashboard-channel" },
					epoch: 3,
					createdAt: timestamp,
					lastActivityAt: timestamp,
					bootstrap: { epoch: 3, pending: false, byteCount: 0, truncated: false },
				},
			],
		},
		"work.jobs": {
			total: 1,
			offset: 0,
			jobs: [
				{
					job_id: "job-dashboard",
					lane_key: "dashboard-lane",
					state: "running",
					branch: "feature/dashboard",
					worktree_path: "/tmp/dashboard",
					session_id: "session-dashboard",
					last_activity_at: timestamp,
					updated_at: timestamp,
					last_commit: { sha: "abc1234", subject: "Add dashboard progress", committed_at: timestamp },
				},
			],
		},
	};
}

class MockClient {
	data = responses();
	requests: string[] = [];
	handlers = new Map<string, Set<(payload: unknown) => void>>();
	disconnectHandlers = new Set<(error: Error) => void>();
	closed = false;
	failure: Error | undefined;
	async request<T = unknown>(verb: string): Promise<T> {
		this.requests.push(verb);
		if (this.failure) throw this.failure;
		if (!(verb in this.data)) throw new Error(`Unexpected request: ${verb}`);
		return this.data[verb] as T;
	}
	on(event: string, handler: (payload: unknown) => void): () => void {
		const handlers = this.handlers.get(event) ?? new Set();
		handlers.add(handler);
		this.handlers.set(event, handlers);
		return () => {
			handlers.delete(handler);
		};
	}
	onDisconnect(handler: (error: Error) => void): () => void {
		this.disconnectHandlers.add(handler);
		return () => {
			this.disconnectHandlers.delete(handler);
		};
	}
	async close(): Promise<void> {
		this.closed = true;
	}
	disconnect(): void {
		for (const handler of this.disconnectHandlers) handler(new Error("Gateway disconnected"));
	}
}

class MockRenderer {
	frames: string[] = [];
	current = "";
	hidden = false;
	clear(): void {
		if (this.current) this.frames.push(this.current);
		this.current = "";
	}
	clearLine(): void {}
	moveCursor(): void {}
	write(text: string): void {
		this.current += text;
	}
	resetStyle(): void {}
	hideCursor(): void {
		this.hidden = true;
	}
	showCursor(): void {
		this.hidden = false;
	}
	get output(): string {
		return [...this.frames, this.current].join("\n");
	}
}

type Step = string | (() => void | Promise<void>);
class MockInput {
	closed = false;
	constructor(private readonly steps: Step[]) {}
	async readKey(): Promise<KeyPressEvent> {
		let step = this.steps.shift();
		while (typeof step === "function") {
			await step();
			step = this.steps.shift();
		}
		const key = step ?? "q";
		return { key, code: key, isShift: false, isCtrl: false, isAlt: false };
	}
	close(): void {
		this.closed = true;
	}
}

const homes: string[] = [];
afterEach(async () => {
	for (const home of homes.splice(0)) await rm(home, { recursive: true, force: true });
});
async function fixture() {
	const home = await mkdtemp(join(tmpdir(), "dashboard-test-"));
	homes.push(home);
	await writeFile(join(home, "gateway.log"), "gateway ready\n");
	const client = new MockClient();
	const renderer = new MockRenderer();
	return {
		home,
		client,
		renderer,
		async run(steps: Step[] = ["q"]) {
			const input = new MockInput(steps);
			await runConsole({ home, client, renderer, input });
			return input;
		},
	};
}

describe("console dashboard", () => {
	test("initializes over the requested gateway socket and closes the connection", async () => {
		const f = await fixture();
		const connect = spyOn(GajaewayClient, "connectSocket").mockResolvedValue(f.client as unknown as GajaewayClient);
		try {
			await runConsole({
				home: f.home,
				socket: "/tmp/dashboard-test.sock",
				renderer: f.renderer,
				input: new MockInput(["q"]),
			});
			expect(connect.mock.calls[0]?.[0]).toBe("/tmp/dashboard-test.sock");
			expect(f.client.requests).toContain("gateway.status");
			expect(f.client.closed).toBe(true);
		} finally {
			connect.mockRestore();
		}
	});

	test("status displays actual daemon PID and schema", async () => {
		const f = await fixture();
		await f.run();
		expect(f.renderer.output).toContain("4242");
		expect(f.renderer.output.toLowerCase()).toContain("schema");
	});

	test("initial socket failure rejects safely and restores terminal", async () => {
		const f = await fixture();
		const input = new MockInput(["q"]);
		const connect = spyOn(GajaewayClient, "connectSocket").mockRejectedValue(
			new Error("ECONNREFUSED token=private-socket-value"),
		);
		try {
			await expect(runConsole({ home: f.home, renderer: f.renderer, input })).rejects.toThrow(
				"Unable to connect to gateway",
			);
			expect(f.renderer.output).toContain("Unable to connect to gateway");
			expect(f.renderer.output).not.toContain("private-socket-value");
			expect(input.closed).toBe(true);
			expect(f.renderer.hidden).toBe(false);
		} finally {
			connect.mockRestore();
		}
	});

	test("gateway subscriptions refresh live data without a refresh key", async () => {
		const f = await fixture();
		await f.run([
			async () => {
				f.client.data["gateway.status"] = { ...(f.client.data["gateway.status"] as object), pid: 7654 };
				const handlers = f.client.handlers.get("chat.progress");
				expect(handlers?.size).toBeGreaterThan(0);
				for (const handler of handlers ?? []) handler({});
				for (let attempt = 0; attempt < 100 && !f.renderer.current.includes("7654"); attempt++)
					await new Promise((resolve) => setTimeout(resolve, 1));
				expect(f.renderer.current).toContain("7654");
			},
			"q",
		]);
	});

	test("sessions table renders gateway session metadata", async () => {
		const f = await fixture();
		await f.run(["2", "q"]);
		expect(f.renderer.current).toContain("dashboard-channel");
		expect(f.renderer.current).toContain("discord");
	});

	test("lanes table renders durable lane progress", async () => {
		const f = await fixture();
		await f.run(["3", "q"]);
		expect(f.renderer.current).toContain("dashboard-lane");
		expect(f.renderer.current).toContain("running");
		expect(f.renderer.current).toContain("Add dashboard progress");
	});

	test("number keys navigate all five panels", async () => {
		const f = await fixture();
		const check = spyOn(updates, "runUpdate").mockResolvedValue({
			current: "1.0.0",
			latest: "1.0.0",
			tag: "v1.0.0",
			repo: "test/repo",
			checkedOnly: true,
			upToDate: true,
			installed: [],
		});
		try {
			const panels: string[] = [];
			await f.run([
				"2",
				() => {
					panels.push(f.renderer.current);
				},
				"3",
				() => {
					panels.push(f.renderer.current);
				},
				"4",
				() => {
					panels.push(f.renderer.current);
				},
				"5",
				() => {
					panels.push(f.renderer.current);
				},
				"1",
				() => {
					panels.push(f.renderer.current);
				},
				"q",
			]);
			expect(panels[0]).toContain("dashboard-channel");
			expect(panels[1]).toContain("dashboard-lane");
			expect(panels[2]?.toLowerCase()).toContain("up to date");
			expect(panels[3]).toContain("gateway ready");
			expect(panels[4]).toContain("4242");
		} finally {
			check.mockRestore();
		}
	});

	test("refresh replaces displayed gateway data", async () => {
		const f = await fixture();
		await f.run([
			() => {
				f.client.data["gateway.status"] = { ...(f.client.data["gateway.status"] as object), pid: 9876 };
			},
			"r",
			"q",
		]);
		expect(f.renderer.current).toContain("9876");
		expect(f.client.requests.filter((verb) => verb === "gateway.status").length).toBeGreaterThan(1);
	});

	test("quit releases keyboard, cursor, subscriptions, and client", async () => {
		const f = await fixture();
		const input = await f.run();
		expect(input.closed).toBe(true);
		expect(f.renderer.hidden).toBe(false);
		expect(f.client.closed).toBe(true);
		expect(f.client.disconnectHandlers.size).toBe(0);
		for (const handlers of f.client.handlers.values()) expect(handlers.size).toBe(0);
	});

	test("gateway disconnection is shown without crashing", async () => {
		const f = await fixture();
		await f.run([
			() => {
				f.client.disconnect();
			},
			"q",
		]);
		expect(f.renderer.output.toLowerCase()).toContain("disconnected");
		expect(f.client.closed).toBe(true);
	});

	test("empty sessions have an explicit empty state", async () => {
		const f = await fixture();
		f.client.data["session.list"] = { sessions: [] };
		await f.run(["2", "q"]);
		expect(f.renderer.current.toLowerCase()).toMatch(/no (active )?sessions/);
	});

	test("no update available prevents update application", async () => {
		const f = await fixture();
		const check = spyOn(updates, "runUpdate").mockResolvedValue({
			current: "1.0.0",
			latest: "1.0.0",
			tag: "v1.0.0",
			repo: "test/repo",
			checkedOnly: true,
			upToDate: true,
			installed: [],
		});
		try {
			await f.run(["4", "u", "enter", "q"]);
			expect(f.renderer.output.toLowerCase()).toContain("up to date");
			expect(check).toHaveBeenCalled();
			for (const [options] of check.mock.calls) expect(options.args.check).toBe(true);
		} finally {
			check.mockRestore();
		}
	});

	test("failed daemon reads show an error and remain quittable", async () => {
		const f = await fixture();
		f.client.failure = new Error("gateway unavailable");
		await f.run();
		expect(f.renderer.output.toLowerCase()).toMatch(/unavailable|failed|error/);
		expect(f.client.closed).toBe(true);
	});

	test("large logs use a bounded window and allow backward scrolling", async () => {
		const f = await fixture();
		await writeFile(
			join(f.home, "gateway.log"),
			Array.from({ length: 150 }, (_, n) => `log-entry-${String(n).padStart(3, "0")}`).join("\n"),
		);
		let latest = "";
		await f.run([
			"5",
			() => {
				latest = f.renderer.current;
			},
			"up",
			"q",
		]);
		expect(latest).toContain("log-entry-149");
		expect(latest).not.toContain("log-entry-000");
		expect(latest.match(/log-entry-\d+/g)?.length).toBeLessThanOrEqual(20);
		expect(f.renderer.current).not.toBe(latest);
	});

	test("secrets in responses, errors, and logs are not displayed", async () => {
		const f = await fixture();
		f.client.data["gateway.status"] = { ...(f.client.data["gateway.status"] as object), token: "private-status-value" };
		await writeFile(
			join(f.home, "gateway.log"),
			"token=private-log-value\nAuthorization: Bearer private-bearer-value\n",
		);
		const check = spyOn(updates, "runUpdate").mockRejectedValue(new Error("token=private-update-value"));
		try {
			await f.run(["2", "3", "4", "5", "q"]);
			for (const secret of [
				"private-status-value",
				"private-log-value",
				"private-bearer-value",
				"private-update-value",
			])
				expect(f.renderer.output).not.toContain(secret);
		} finally {
			check.mockRestore();
		}
	});
});
