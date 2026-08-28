import { describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { OpsCycleResult } from "@gajaeway/protocol";
import { cycleExitCode, parseArgs, renderCycle, restoreDatabase, socketPath } from "../src/main";

describe("cli arguments", () => {
	test("resolves home and socket override", () => {
		expect(socketPath("/tmp/gajae")).toBe("/tmp/gajae/gateway.sock");
		expect(parseArgs(["--socket", "/tmp/x", "status"])).toEqual({ command: "status", rest: [], socket: "/tmp/x" });
	});
});

test("offline restore preserves the current database then copies a header-validated backup", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-cli-restore-"));
	const previousHome = process.env.GAJAEWAY_HOME;
	process.env.GAJAEWAY_HOME = home;
	try {
		const databasePath = join(home, "gateway.db");
		const backupPath = join(home, "backup.db");
		await writeFile(databasePath, "current database");
		await writeFile(backupPath, Buffer.concat([Buffer.from("SQLite format 3\0"), Buffer.from(" backup")]));
		await restoreDatabase(join(home, "gateway.sock"), backupPath);
		expect(await Bun.file(databasePath).text()).toBe("SQLite format 3\0 backup");
		const preserved = (await Array.fromAsync(new Bun.Glob("gateway.db.pre-restore-*").scan({ cwd: home })))[0];
		expect(preserved).toBeString();
		expect(await Bun.file(join(home, preserved as string)).text()).toBe("current database");
	} finally {
		if (previousHome === undefined) delete process.env.GAJAEWAY_HOME;
		else process.env.GAJAEWAY_HOME = previousHome;
		await rm(home, { recursive: true, force: true });
	}
});
function cycleResult(overrides: Partial<OpsCycleResult> = {}): OpsCycleResult {
	return {
		phase: "idle",
		gates: [],
		generatedAt: "2026-08-26T00:00:00.000Z",
		instanceId: "inst-1",
		memoryClosing: false,
		sessions: [],
		memoryIntents: { queued: 0, written: 0, committed: 0, receipted: 0, quarantined: 0 },
		monitorEvents: [],
		deliveries: { pending: 0, inflight: 0, confirmed: 0, failedAmbiguous: 0, expired: 0 },
		inFlightInbound: 0,
		pendingInbound: 0,
		contextDiff: {
			unread: 0,
			expired: 0,
			truncated: 0,
			omittedOldestAt: null,
			omittedNewestAt: null,
			floorAt: null,
		},
		...overrides,
	};
}

describe("cycle rendering", () => {
	test("healthy cycle renders phase and explicit none-gate, no sessions block", () => {
		const lines = renderCycle(cycleResult());
		expect(lines[0]).toBe("phase: idle");
		expect(lines).toContain("gates: none");
		expect(lines.join("\n")).not.toContain("sessions:");
	});

	test("gates render visibly with every reason named", () => {
		const lines = renderCycle(
			cycleResult({ phase: "degraded", gates: ["stale_session_identity", "memory_closure_blocked"] }),
		).join("\n");
		expect(lines).toContain("gates: stale_session_identity, memory_closure_blocked");
	});

	test("mid-rebind session renders the rebinding marker instead of a session id", () => {
		const lines = renderCycle(
			cycleResult({
				gates: ["stale_session_identity"],
				sessions: [
					{
						originKey: "discord/dm/c1/peer=p1",
						origin: { platform: "discord", kind: "dm", conversationId: "c1", peerId: "p1" },
						epoch: 4,
						sessionId: "",
						createdAt: "2026-08-01T00:00:00.000Z",
						lastActivityAt: null,
						pendingInbound: 2,
						unsettledDeliveries: 1,
						oldestUnsettledAgeMs: 42_000,
						contextDiff: {
							unread: 0,
							expired: 0,
							truncated: 0,
							omittedOldestAt: null,
							omittedNewestAt: null,
							floorAt: "2026-08-01T00:00:00.000Z",
						},
					},
				],
			}),
		).join("\n");
		expect(lines).toContain("(rebinding)");
		expect(lines).toContain("discord/dm/c1/peer=p1");
		expect(lines).toContain("42s");
	});

	test("census lines always render so an empty subsystem is distinguishable from a missing one", () => {
		const lines = renderCycle(cycleResult()).join("\n");
		expect(lines).toContain("inbound: pending=0 inflight=0");
		expect(lines).toContain("context: unread=0 expired=0 truncated=0 omitted_oldest=- omitted_newest=-");
		expect(lines).toContain("deliveries: pending=0 inflight=0 confirmed=0 failed_ambiguous=0 expired=0");
		expect(lines).toContain("memory: queued=0 written=0 committed=0 receipted=0 quarantined=0");
		expect(lines).toContain("monitors: none");
	});

	test("exit-code contract: gates force exit 1, healthy is exit 0", () => {
		expect(cycleExitCode(cycleResult())).toBe(0);
		for (const gate of [
			"stale_session_identity",
			"delivery_settlement_unknown",
			"memory_closure_blocked",
			"monitor_settlement_failed",
		])
			expect(cycleExitCode(cycleResult({ gates: [gate as OpsCycleResult["gates"][number]] }))).toBe(1);
	});
});
