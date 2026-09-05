import { describe, expect, spyOn, test } from "bun:test";
import type { GatewayStatusResult } from "@gajaeway/protocol";
import { GajaewayClient } from "@gajaeway/sdk";
import { HOLD_COLUMNS, parseListOptions, renderList } from "../src/list";
import { main } from "../src/main";

const hold: GatewayStatusResult["holds"][number] = {
	opRef: "op-held",
	originKey: "discord/channel/111",
	epoch: 3,
	state: "accepted",
	reason: "execution_uncertain",
	since: "2026-09-01T00:00:00.000Z",
	deadline: "2026-09-02T00:00:00.000Z",
	sweeps: 4,
	fence: { opRef: "op-held", since: "2026-09-01T00:00:00.000Z", deadline: null },
};

function render(args: string[], rows = [hold]): string[] {
	return renderList(HOLD_COLUMNS, rows, parseListOptions(args, HOLD_COLUMNS), {
		key: "holds",
		result: { holds: rows },
	});
}

describe("holds list rendering", () => {
	test("tables every hold field and a readable fence", () => {
		const lines = render([]);
		expect(lines[0]?.split(/\s+/)).toEqual([
			"OP_REF",
			"ORIGIN",
			"EPOCH",
			"STATE",
			"REASON",
			"SINCE",
			"DEADLINE",
			"SWEEPS",
			"FENCE",
		]);
		expect(lines[1]).toContain(hold.originKey);
		expect(lines[1]).toContain("execution_uncertain");
		expect(lines[1]).toContain('{"opRef":"op-held"');
		expect(lines[1]).not.toContain("[object Object]");
	});

	test("JSON round-trips full holds including nullable fields and nested fence", () => {
		const rows = [hold, { ...hold, opRef: "op-null", since: null, deadline: null, fence: null }];
		expect(JSON.parse(render(["--json"], rows)[0] as string)).toEqual({ holds: rows });
	});

	test("fields and paging preserve raw fence values", () => {
		expect(JSON.parse(render(["--json", "--fields", "origin,fence", "--limit", "1"])[0] as string)).toEqual({
			holds: [{ origin: hold.originKey, fence: hold.fence }],
		});
		expect(render(["--fields", "opRef", "--limit", "0"])).toEqual(["OP_REF", "(none)"]);
		expect(render(["--fields", "opRef", "--offset", "1"])).toEqual(["OP_REF", "(none)"]);
	});

	test("empty holds render explicitly", () => {
		expect(render([], []).at(-1)).toBe("(none)");
		expect(JSON.parse(render(["--json"], [])[0] as string)).toEqual({ holds: [] });
	});

	test("CLI uses status read-only and closes the client", async () => {
		let statusReads = 0;
		let closes = 0;
		const connect = spyOn(GajaewayClient, "connectSocket").mockResolvedValue({
			status: async () => {
				statusReads++;
				return { holds: [hold] };
			},
			close: async () => {
				closes++;
			},
		} as unknown as GajaewayClient);
		const lines: string[] = [];
		const log = spyOn(console, "log").mockImplementation((line) => {
			lines.push(String(line));
		});
		try {
			await main(["--socket", "/test/holds.sock", "holds", "list", "--json"]);
			expect(JSON.parse(lines[0] as string)).toEqual({ holds: [hold] });
			expect(connect).toHaveBeenCalledWith("/test/holds.sock");
			expect(statusReads).toBe(1);
			expect(closes).toBe(1);
		} finally {
			log.mockRestore();
			connect.mockRestore();
		}
	});

	for (const args of [["release"], ["list", "--fields", "bogus"], ["list", "--limit", "-1"]]) {
		test(`rejects ${args.join(" ")} before connecting`, async () => {
			const connect = spyOn(GajaewayClient, "connectSocket");
			const errors: string[] = [];
			const error = spyOn(console, "error").mockImplementation((line) => {
				errors.push(String(line));
			});
			const previousExit = process.exitCode;
			try {
				await main(["holds", ...args]);
				expect(connect).not.toHaveBeenCalled();
				expect(errors).toHaveLength(1);
				expect(errors[0]).toMatch(/^(usage: gajaeway holds list|unknown field|--limit expects)/);
			} finally {
				connect.mockRestore();
				error.mockRestore();
				process.exitCode = previousExit ?? 0;
			}
		});
	}
});
