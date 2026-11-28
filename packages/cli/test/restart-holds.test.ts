import { describe, expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ProtocolError } from "@gajae-gateway/protocol";
import { GajaewayClient } from "@gajae-gateway/sdk";
import { main } from "../src/main";
import {
	effectiveRestartState,
	type RestartReceipt,
	renderRestartReceipt,
	runRestartStack,
} from "../src/restart-stack";

async function tempHome(): Promise<{ home: string; cleanup: () => Promise<void> }> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-restart-holds-"));
	const provisionDir = join(home, "broker", "provision");
	await mkdir(provisionDir, { recursive: true });
	await writeFile(join(provisionDir, "manifest.json"), "{}\n", "utf8");
	for (const adapter of ["discord", "slack", "telegram"])
		await writeFile(join(home, `adapter-${adapter}.json`), "{}\n", "utf8");
	return { home, cleanup: () => rm(home, { recursive: true, force: true }) };
}

async function fakeHost(options: { home: string; installed?: (label: string) => Promise<boolean> }) {
	let clock = Date.now();
	const ran: string[] = [];
	const pids = new Map<string, number>();
	const leases = new Map<string, never>();
	return {
		ran,
		close: async () => {},
		options: {
			home: options.home,
			id: "test-run",
			platform: "darwin" as const,
			uid: 501,
			env: { ...process.env, HOME: options.home },
			launchAgentsDir: join(options.home, "agents"),
			now: () => clock,
			sleep: async (ms: number) => {
				clock += ms;
			},
			verifyTimeoutMs: 5_000,
			pollMs: 1_000,
			runner: async (command: readonly string[]) => {
				ran.push(command.join(" "));
				return 0;
			},
			probe: async (label: string) => {
				const pid = pids.get(label);
				return pid === undefined ? undefined : { pid };
			},
			binaryModifiedAt: async () => Date.parse("2026-08-29T06:40:00.000Z"),
			isServiceInstalled: options.installed ?? (async () => false),
		},
	};
}

const BLOCKING_DETAIL =
	"persona turn delivery was not confirmed before the restart drain deadline; blocking: " +
	"origin=loopback/loopback/discord-thread-1541621305302974526 opRef=turn-1 session=session-1 reason=stale_escalated_retired_hold; " +
	"origin=loopback/loopback/telegram-dm opRef=turn-2 session=session-2 reason=stale_escalated_retired_hold";

describe("restart drain blocking-origin detail (#859)", () => {
	test("a refused drain's cancel status names the blocking origins and opRefs", async () => {
		const { home, cleanup } = await tempHome();
		const host = await fakeHost({ home, installed: async (label) => label === "dev.gajaeway.gateway" });
		try {
			const receipt = await runRestartStack({
				...host.options,
				id: "drain-origin-detail",
				prepareRestartDrain: async () => {
					throw new ProtocolError("verb_failed", BLOCKING_DETAIL);
				},
			});
			expect(receipt.state).toBe("failed");
			expect(receipt.detail).toContain("blocking:");
			expect(receipt.detail).toContain("opRef=turn-1");
			expect(receipt.detail).toContain("opRef=turn-2");
			expect(receipt.steps.every((step) => step.result === "skipped")).toBe(true);
			expect(host.ran).toEqual([]);
			const rendered = renderRestartReceipt(receipt);
			expect(rendered.some((line) => line.includes("opRef=turn-1") && line.includes("opRef=turn-2"))).toBe(true);
			// The durable receipt keeps the origins for `restart-stack --status` after the CLI exits.
			const persisted = JSON.parse(await Bun.file(join(home, "restart-stack.json")).text()) as RestartReceipt;
			expect(persisted.detail).toContain("opRef=turn-1");
			expect(await effectiveRestartState(persisted, home)).toBe("failed");
		} finally {
			await host.close();
			await cleanup();
		}
	});

	test("the CLI renders a persisted blocking detail as the --status output", async () => {
		const { home, cleanup } = await tempHome();
		try {
			const receipt = {
				id: "drain-detail-persist",
				trigger: "manual",
				state: "failed",
				platform: "linux",
				requestedAt: new Date().toISOString(),
				detail: BLOCKING_DETAIL,
				steps: [],
			} satisfies RestartReceipt;
			await writeFile(join(home, "restart-stack.json"), `${JSON.stringify(receipt, null, 2)}\n`, { mode: 0o600 });
			const lines = renderRestartReceipt(JSON.parse(await Bun.file(join(home, "restart-stack.json")).text()));
			const detail = lines.find((line) => line.startsWith("detail: "));
			expect(detail).toContain("origin=loopback/loopback/discord-thread-1541621305302974526 opRef=turn-1");
			expect(detail).toContain("reason=stale_escalated_retired_hold");
		} finally {
			await cleanup();
		}
	});

	test("a drain failure without named origins still renders its plain detail and fails closed", async () => {
		const { home, cleanup } = await tempHome();
		const host = await fakeHost({ home, installed: async (label) => label === "dev.gajaeway.gateway" });
		try {
			const receipt = await runRestartStack({
				...host.options,
				id: "drain-origin-plain",
				prepareRestartDrain: async () => {
					throw new ProtocolError(
						"verb_failed",
						"persona turn delivery was not confirmed before the restart drain deadline",
					);
				},
			});
			expect(receipt.state).toBe("failed");
			expect(receipt.detail).not.toContain("blocking:");
			expect(renderRestartReceipt(receipt)).toContain(
				"detail: persona turn drain failed: persona turn delivery was not confirmed before the restart drain deadline",
			);
			expect(host.ran).toEqual([]);
		} finally {
			await host.close();
			await cleanup();
		}
	});
});

describe("operator hold commands (#859)", () => {
	test("ops holds lists durable escalated holds naming origins and opRefs", async () => {
		const holds = [
			{
				originKey: "loopback/loopback/discord-thread",
				opRef: "turn-1",
				sessionId: "session-1",
				state: "bound",
				heldMs: 90 * 24 * 60 * 60_000,
				parked: false,
				answerWanted: true,
				wouldBlockDrain: true,
			},
			{
				originKey: "loopback/loopback/telegram-dm",
				opRef: "turn-2",
				sessionId: "session-2",
				state: "accepted",
				heldMs: 4 * 24 * 60 * 60_000,
				parked: true,
				answerWanted: true,
				wouldBlockDrain: false,
			},
		];
		const requests: Array<{ verb: string; params: unknown }> = [];
		const lines: string[] = [];
		const client: GajaewayClient = Object.create(GajaewayClient.prototype);
		client.request = async <T>(verb: string, params?: unknown): Promise<T> => {
			requests.push({ verb, params });
			return { holds } as T;
		};
		client.close = async () => {};
		const connect = spyOn(GajaewayClient, "connectSocketWithBackoff").mockResolvedValue(client);
		const log = spyOn(console, "log").mockImplementation((line) => lines.push(String(line)));
		try {
			await main(["--socket", "/test/holds.sock", "ops", "holds"]);
			expect(requests).toEqual([{ verb: "ops.holds", params: {} }]);
			const flat = lines.join("\n");
			expect(flat).toContain("turn-1");
			expect(flat).toContain("loopback/loopback/discord-thread");
			expect(flat).toContain("turn-2");
			// A drain-blocking hold is named as such, a parked one is not.
			expect(flat).toContain("blocks-drain");
		} finally {
			connect.mockRestore();
			log.mockRestore();
		}
	});

	test("ops holds --json prints the payload and leaves rendering to the operator", async () => {
		const holds = [
			{
				originKey: "loopback/loopback/discord-thread",
				opRef: "turn-1",
				sessionId: "session-1",
				state: "bound",
				heldMs: 1,
				parked: false,
				answerWanted: true,
				wouldBlockDrain: true,
			},
		];
		const client: GajaewayClient = Object.create(GajaewayClient.prototype);
		client.request = async <T>(): Promise<T> => ({ holds }) as T;
		client.close = async () => {};
		const connect = spyOn(GajaewayClient, "connectSocketWithBackoff").mockResolvedValue(client);
		const log = spyOn(console, "log").mockImplementation(() => {});
		try {
			await main(["--socket", "/test/holds.sock", "ops", "holds", "--json"]);
		} finally {
			connect.mockRestore();
			log.mockRestore();
		}
	});

	test("ops holds abandon clears the hold only with --confirm", async () => {
		const requests: Array<{ verb: string; params: unknown }> = [];
		const lines: string[] = [];
		const client: GajaewayClient = Object.create(GajaewayClient.prototype);
		client.request = async <T>(verb: string, params?: unknown): Promise<T> => {
			requests.push({ verb, params });
			return { cleared: true, originKey: "loopback/loopback/discord-thread" } as T;
		};
		client.close = async () => {};
		const connect = spyOn(GajaewayClient, "connectSocketWithBackoff").mockResolvedValue(client);
		const log = spyOn(console, "log").mockImplementation((line) => lines.push(String(line)));
		const previousExit = process.exitCode;
		try {
			await main(["--socket", "/test/holds.sock", "ops", "holds", "abandon", "turn-9", "--confirm"]);
			expect(requests).toEqual([{ verb: "ops.holds.abandon", params: { opRef: "turn-9", confirm: true } }]);
			expect(lines.join("\n")).toContain("turn-9");
		} finally {
			connect.mockRestore();
			log.mockRestore();
			process.exitCode = previousExit ?? 0;
		}
	});

	test("ops holds abandon without --confirm is refused before any request", async () => {
		const requests: Array<{ verb: string; params: unknown }> = [];
		const client: GajaewayClient = Object.create(GajaewayClient.prototype);
		client.request = async <T>(verb: string, params?: unknown): Promise<T> => {
			requests.push({ verb, params });
			return {} as T;
		};
		client.close = async () => {};
		const connect = spyOn(GajaewayClient, "connectSocketWithBackoff").mockResolvedValue(client);
		const previousExit = process.exitCode;
		try {
			await main(["--socket", "/test/holds.sock", "ops", "holds", "abandon", "turn-9"]);
			expect(process.exitCode).toBe(2);
			expect(requests).toEqual([]);
		} finally {
			connect.mockRestore();
			process.exitCode = previousExit ?? 0;
		}
	});
});
