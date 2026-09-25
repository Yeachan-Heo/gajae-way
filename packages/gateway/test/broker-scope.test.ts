import { afterEach, expect, test } from "bun:test";
import { GlobalGjcClient } from "../src/orchestrator/broker";
import { createBrokerReleaser, type UnitScopePorts } from "../src/orchestrator/broker-scope";

const UNIT = "/user.slice/user-1000.slice/user@1000.service/app.slice/gajaeway-gateway.service";
const OTHER = "/user.slice/user-1000.slice/user@1000.service/app.slice/gjc-tmux-owner.service";

interface Proc {
	cgroup: string;
	argv: string[];
	children: number[];
}

function ports(
	table: Record<number, Proc>,
	self = UNIT,
): UnitScopePorts & { scopes: Array<{ name: string; pids: readonly number[]; user: boolean }> } {
	const scopes: Array<{ name: string; pids: readonly number[]; user: boolean }> = [];
	return {
		scopes,
		cgroup: (pid) => (pid === "self" ? self : table[pid]?.cgroup),
		argv: (pid) => table[pid]?.argv,
		children: (pid) => table[pid]?.children ?? [],
		async startScope(name, pids, user) {
			for (const pid of pids) if (!table[pid]) throw new Error(`Process with ID ${pid} does not exist.`);
			scopes.push({ name, pids, user });
			for (const pid of pids) table[pid]!.cgroup = `/user.slice/${name}`;
		},
	};
}

const broker = ["bun", "cli.ts", "sdk", "broker-internal", "--agent-dir", "/home/u/.gjc/agent"];
const host = ["bun", "cli.ts", "sdk", "session-host-internal"];

test("a GJC broker autostarted inside the gateway unit leaves it with its session hosts (#183)", async () => {
	const table: Record<number, Proc> = {
		3880: { cgroup: UNIT, argv: broker, children: [33758, 3131561, 777] },
		33758: { cgroup: UNIT, argv: host, children: [40000] },
		40000: { cgroup: UNIT, argv: ["bash"], children: [] },
		3131561: { cgroup: UNIT, argv: host, children: [] },
		// A descendant already living elsewhere is not the unit's to move.
		777: { cgroup: OTHER, argv: host, children: [] },
	};
	const p = ports(table);
	const result = await createBrokerReleaser(p)(3880);
	expect(result).toEqual({
		outcome: "released",
		scope: "gajaeway-gjc-broker-3880.scope",
		pids: [3880, 3131561, 33758, 40000],
	});
	expect(p.scopes).toEqual([
		{ name: "gajaeway-gjc-broker-3880.scope", pids: [3880, 3131561, 33758, 40000], user: true },
	]);
	// Nothing GJC owns is left for a control-group kill of the gateway unit.
	for (const pid of [3880, 33758, 40000, 3131561]) expect(table[pid]!.cgroup).not.toBe(UNIT);
	expect(table[777]!.cgroup).toBe(OTHER);
});

test("a broker started outside the gateway unit, or a pid that is not a broker, is never touched", async () => {
	const outside = ports({ 3880: { cgroup: OTHER, argv: broker, children: [] } });
	expect(await createBrokerReleaser(outside)(3880)).toEqual({ outcome: "skipped", reason: "outside_unit" });
	const impostor = ports({ 3880: { cgroup: UNIT, argv: ["gajaeway-gateway", "daemon"], children: [] } });
	expect(await createBrokerReleaser(impostor)(3880)).toEqual({ outcome: "skipped", reason: "not_a_gjc_broker" });
	const unmanaged = ports(
		{ 3880: { cgroup: "/user.slice/session-3.scope", argv: broker, children: [] } },
		"/user.slice/session-3.scope",
	);
	expect(await createBrokerReleaser(unmanaged)(3880)).toEqual({ outcome: "skipped", reason: "not_a_systemd_service" });
	for (const p of [outside, impostor, unmanaged]) expect(p.scopes).toEqual([]);
});

test("a session host exiting mid-release is re-enumerated rather than stranding the broker", async () => {
	const table: Record<number, Proc> = {
		3880: { cgroup: UNIT, argv: broker, children: [33758] },
		33758: { cgroup: UNIT, argv: host, children: [] },
	};
	const p = ports(table);
	const start = p.startScope;
	let calls = 0;
	p.startScope = async (name, pids, user) => {
		if (calls++ === 0) {
			delete table[33758];
			table[3880]!.children = [];
		}
		return await start(name, pids, user);
	};
	expect(await createBrokerReleaser(p)(3880)).toEqual({
		outcome: "released",
		scope: "gajaeway-gjc-broker-3880.scope",
		pids: [3880],
	});
	expect(calls).toBe(2);
});

const clients: GlobalGjcClient[] = [];
afterEach(async () => {
	await Promise.all(clients.splice(0).map((client) => client.stop()));
});

test("the gateway releases each newly observed broker generation exactly once", async () => {
	let pid = 3880;
	const released: number[] = [];
	const logs: string[] = [];
	const client = new GlobalGjcClient({
		executable: "/fake/gjc",
		agentDir: "/fake/global/agent",
		command: async () => ({ exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessions: [] } }), stderr: "" }),
		discovery: async () => ({ pid, url: `ws://127.0.0.1:${pid}`, token: "t", heartbeatAt: Date.now() }),
		healthProbe: async () => true,
		healthIntervalMs: 2,
		log: (line) => logs.push(line),
		releaseBrokerScope: async (target) => {
			released.push(target);
			return { outcome: "released", scope: `gajaeway-gjc-broker-${target}.scope`, pids: [target] };
		},
	});
	clients.push(client);
	await client.start();
	await Bun.sleep(20);
	expect(released).toEqual([3880]);
	pid = 4990;
	for (let i = 0; i < 100 && client.generation < 2; i++) await Bun.sleep(5);
	expect(client.generation).toBe(2);
	expect(released).toEqual([3880, 4990]);
	expect(logs).toContain("broker_scope_released pid=3880 scope=gajaeway-gjc-broker-3880.scope processes=1");
});
