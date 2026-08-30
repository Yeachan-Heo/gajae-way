import { describe, expect, test } from "bun:test";
import {
	ADMIN_HOSTNAME,
	auditToStdout,
	DEFAULT_ADMIN_PORT,
	DEFAULT_SOCKET_PATH,
	resolveAdminPort,
	resolveSocketPath,
} from "../src/main";
import { startAdminServer } from "../src/server";

describe("resolveSocketPath", () => {
	test("uses GAJAEWAY_SOCKET when set", () => {
		expect(resolveSocketPath({ GAJAEWAY_SOCKET: "/tmp/other.sock" })).toBe("/tmp/other.sock");
	});

	test("trims surrounding whitespace", () => {
		expect(resolveSocketPath({ GAJAEWAY_SOCKET: "  /tmp/spaced.sock \n" })).toBe("/tmp/spaced.sock");
	});

	test("falls back to the default when unset or blank", () => {
		expect(resolveSocketPath({})).toBe(DEFAULT_SOCKET_PATH);
		expect(resolveSocketPath({ GAJAEWAY_SOCKET: "   " })).toBe(DEFAULT_SOCKET_PATH);
	});
});

describe("resolveAdminPort", () => {
	test("uses GAJAEWAY_ADMIN_PORT when set", () => {
		expect(resolveAdminPort({ GAJAEWAY_ADMIN_PORT: "9001" })).toBe(9001);
		expect(resolveAdminPort({ GAJAEWAY_ADMIN_PORT: " 9001 " })).toBe(9001);
	});

	test("falls back to the default when unset or blank", () => {
		expect(resolveAdminPort({})).toBe(DEFAULT_ADMIN_PORT);
		expect(resolveAdminPort({ GAJAEWAY_ADMIN_PORT: "" })).toBe(DEFAULT_ADMIN_PORT);
	});

	test.each([
		"http",
		"80.5",
		"0",
		"65536",
		"-1",
		"0x2244",
		"1e3",
		"8788.",
		"+8788",
		"87 88",
	])("refuses %p instead of silently listening somewhere else", (raw) => {
		expect(() => resolveAdminPort({ GAJAEWAY_ADMIN_PORT: raw })).toThrow(/GAJAEWAY_ADMIN_PORT/);
	});

	test("accepts the boundary ports", () => {
		expect(resolveAdminPort({ GAJAEWAY_ADMIN_PORT: "1" })).toBe(1);
		expect(resolveAdminPort({ GAJAEWAY_ADMIN_PORT: "65535" })).toBe(65_535);
	});
});

describe("admin service entry", () => {
	test("ADMIN_HOSTNAME actually binds loopback only", async () => {
		const server = startAdminServer({ request: async (method) => ({ method }), hostname: ADMIN_HOSTNAME, port: 0 });
		try {
			expect(server.url).toStartWith("http://127.0.0.1:");
			const response = await fetch(`${server.url}/api/status`, { headers: { host: `127.0.0.1:${server.port}` } });
			expect(response.status).toBe(200);
		} finally {
			server.stop();
		}
	});

	test("writes every audit entry to stdout as one line", () => {
		const lines: string[] = [];
		auditToStdout(
			{ at: "2026-01-01T00:00:00.000Z", operationId: "ops.integrity", actor: "형님", decision: "allowed" },
			{ log: (line: string) => lines.push(line) },
		);
		expect(lines).toHaveLength(1);
		expect(lines[0]).toStartWith("admin.audit ");
		expect(JSON.parse(lines[0]?.slice("admin.audit ".length) ?? "")).toEqual({
			at: "2026-01-01T00:00:00.000Z",
			operationId: "ops.integrity",
			actor: "형님",
			decision: "allowed",
		});
	});
});
