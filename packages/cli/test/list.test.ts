import { describe, expect, test } from "bun:test";
import type { MonitorRecord } from "@gajae-gateway/protocol";
import {
	columnNames,
	MONITOR_COLUMNS,
	parseListOptions,
	renderList,
	SESSION_COLUMNS,
	type SessionListRow,
} from "../src/list";

function monitor(overrides: Partial<MonitorRecord> = {}): MonitorRecord {
	return {
		monitorId: "mon-1234-abcd",
		name: "nightly-audit",
		trigger: { kind: "cron", schedule: "0 3 * * *", timezone: "Asia/Seoul" },
		nextFireAt: {
			timezone: "Asia/Seoul",
			local: "2026-08-30 03:00:00",
			utc: "2026-08-29T18:00:00.000Z",
		},
		eventTypes: ["cron.tick"],
		burstPolicy: "coalesce",
		createdAt: "2026-08-29T00:00:00.000Z",
		enabled: true,
		channelTarget: { origin: { platform: "discord", kind: "channel", conversationId: "999" } },
		...overrides,
	} as MonitorRecord;
}

function session(overrides: Partial<SessionListRow> = {}): SessionListRow {
	return {
		origin: { platform: "discord", kind: "channel", conversationId: "111" },
		epoch: 2,
		createdAt: "2026-08-29T00:00:00.000Z",
		lastActivityAt: null,
		bootstrap: { pending: false, byteCount: 1024, truncated: false },
		...overrides,
	} as SessionListRow;
}

const monitorEnvelope = (monitors: MonitorRecord[]) => ({ key: "monitors", result: { monitors } });
const sessionEnvelope = (sessions: SessionListRow[]) => ({ key: "sessions", result: { sessions } });

describe("list option parsing", () => {
	test("defaults to table output with no paging", () => {
		expect(parseListOptions([], MONITOR_COLUMNS)).toEqual({ json: false, offset: 0 });
	});

	test("rejects an unknown field and lists every valid name", () => {
		expect(() => parseListOptions(["--fields", "id,nope"], MONITOR_COLUMNS)).toThrow(
			`unknown field(s): nope (valid: ${columnNames(MONITOR_COLUMNS).join(", ")})`,
		);
	});

	test("rejects an empty field list and non-integer paging", () => {
		expect(() => parseListOptions(["--fields"], MONITOR_COLUMNS)).toThrow("--fields expects");
		expect(() => parseListOptions(["--fields", " , "], MONITOR_COLUMNS)).toThrow("--fields expects");
		expect(() => parseListOptions(["--limit", "x"], MONITOR_COLUMNS)).toThrow(
			"--limit expects a non-negative integer, got: x",
		);
		expect(() => parseListOptions(["--offset", "-1"], MONITOR_COLUMNS)).toThrow(
			"--offset expects a non-negative integer, got: -1",
		);
		expect(() => parseListOptions(["--limit"], MONITOR_COLUMNS)).toThrow("(missing)");
		expect(() => parseListOptions(["--bogus"], MONITOR_COLUMNS)).toThrow("unknown option: --bogus");
	});

	test("parses fields, limit and offset together", () => {
		expect(parseListOptions(["--fields", "id, name", "--limit", "2", "--offset", "1"], MONITOR_COLUMNS)).toEqual({
			json: false,
			fields: ["id", "name"],
			limit: 2,
			offset: 1,
		});
	});
});

describe("monitors list rendering", () => {
	test("default output is an aligned one-row-per-monitor table", () => {
		const rows = [
			monitor(),
			monitor({
				monitorId: "mon-5678-efgh",
				name: "webhook-relay",
				trigger: { kind: "webhook", route: "r-1" },
				nextFireAt: null,
				eventTypes: ["push", "issue"],
				channelTarget: null,
				enabled: false,
			}),
		];
		const lines = renderList(MONITOR_COLUMNS, rows, parseListOptions([], MONITOR_COLUMNS), monitorEnvelope(rows));
		expect(lines).toHaveLength(3);
		expect(lines[0]).toBe(
			"ID        NAME           SCHEDULE     TIMEZONE    NEXT_FIRE                                EVENTS      TARGET               ENABLED",
		);
		expect(lines[1]).toContain("mon-1234");
		expect(lines[1]).toContain("0 3 * * *");
		expect(lines[1]).toContain("Asia/Seoul");
		expect(lines[1]).toContain("2026-08-30 03:00:00 / 2026-08-29T18:00Z");
		expect(lines[1]).toContain("cron.tick");
		expect(lines[1]).toEndWith("true");
		expect(lines[2]).toContain("webhook:r-1");
		expect(lines[2]).toContain("push,issue");
		// No channel target renders as "-", never as "null"/"undefined".
		expect(lines[2]).toContain("-");
		expect(lines[2]).toEndWith("false");
		// Every line stays a single line: agent shells can read the whole table.
		for (const line of lines) expect(line).not.toInclude("\n");
	});

	test("empty list renders a header plus an explicit (none) marker", () => {
		expect(renderList(MONITOR_COLUMNS, [], parseListOptions([], MONITOR_COLUMNS), monitorEnvelope([]))).toEqual([
			"ID  NAME  SCHEDULE  TIMEZONE  NEXT_FIRE  EVENTS  TARGET  ENABLED",
			"(none)",
		]);
	});

	test("--json without projection keeps the gateway shape verbatim", () => {
		const rows = [monitor()];
		expect(
			renderList(MONITOR_COLUMNS, rows, parseListOptions(["--json"], MONITOR_COLUMNS), monitorEnvelope(rows)),
		).toEqual([JSON.stringify({ monitors: rows })]);
	});

	test("--fields selects columns for the table and for --json", () => {
		const rows = [monitor()];
		expect(
			renderList(
				MONITOR_COLUMNS,
				rows,
				parseListOptions(["--fields", "name,enabled"], MONITOR_COLUMNS),
				monitorEnvelope(rows),
			),
		).toEqual(["NAME           ENABLED", "nightly-audit  true"]);
		expect(
			renderList(
				MONITOR_COLUMNS,
				rows,
				parseListOptions(["--json", "--fields", "name,enabled"], MONITOR_COLUMNS),
				monitorEnvelope(rows),
			),
		).toEqual([JSON.stringify({ monitors: [{ name: "nightly-audit", enabled: true }] })]);
	});

	test("--limit/--offset page the rows in both output modes", () => {
		const rows = [monitor({ name: "a" }), monitor({ name: "b" }), monitor({ name: "c" })];
		const table = renderList(
			MONITOR_COLUMNS,
			rows,
			parseListOptions(["--fields", "name", "--limit", "1", "--offset", "1"], MONITOR_COLUMNS),
			monitorEnvelope(rows),
		);
		expect(table).toEqual(["NAME", "b"]);
		expect(
			renderList(
				MONITOR_COLUMNS,
				rows,
				parseListOptions(["--json", "--limit", "2", "--offset", "1"], MONITOR_COLUMNS),
				monitorEnvelope(rows),
			),
		).toEqual([JSON.stringify({ monitors: [rows[1], rows[2]] })]);
		// Offset past the end pages to an empty result rather than throwing.
		expect(
			renderList(
				MONITOR_COLUMNS,
				rows,
				parseListOptions(["--offset", "9", "--fields", "name"], MONITOR_COLUMNS),
				monitorEnvelope(rows),
			),
		).toEqual(["NAME", "(none)"]);
	});
});

describe("sessions list rendering", () => {
	test("default output tables the existing identifying and state fields", () => {
		const rows = [session(), session({ epoch: 7, bootstrap: { pending: true, byteCount: 0, truncated: false } })];
		const lines = renderList(SESSION_COLUMNS, rows, parseListOptions([], SESSION_COLUMNS), sessionEnvelope(rows));
		expect(lines[0]).toBe("INDEX  ORIGIN               EPOCH  BOOTSTRAP  CREATED_AT                LAST_ACTIVITY_AT");
		expect(lines[1]).toStartWith("0 ");
		expect(lines[1]).toContain("discord/channel/111");
		expect(lines[1]).toContain("1024B");
		expect(lines[1]).toEndWith("-");
		expect(lines[2]).toContain("pending");
	});

	test("--json without projection keeps the gateway shape verbatim", () => {
		const rows = [session()];
		expect(
			renderList(SESSION_COLUMNS, rows, parseListOptions(["--json"], SESSION_COLUMNS), sessionEnvelope(rows)),
		).toEqual([JSON.stringify({ sessions: rows })]);
	});

	test("paging keeps the original index column stable", () => {
		const rows = [session(), session({ epoch: 3 }), session({ epoch: 5 })];
		expect(
			renderList(
				SESSION_COLUMNS,
				rows,
				parseListOptions(["--fields", "index,epoch", "--offset", "1"], SESSION_COLUMNS),
				sessionEnvelope(rows),
			),
		).toEqual(["INDEX  EPOCH", "1      3", "2      5"]);
	});
});
