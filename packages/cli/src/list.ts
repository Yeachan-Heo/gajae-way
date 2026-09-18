import type { MonitorRecord } from "@gajae-gateway/protocol";
import { originKey } from "@gajae-gateway/protocol";

/**
 * Presentation layer for `gajaeway monitors list` / `sessions list`.
 *
 * Rendering contract:
 * - Default output is a padded column table, one record per line, so agent
 *   shells with a ~1KB visible-output cap can read it without an artifact
 *   refetch or an external jq/python filter.
 * - `--json` keeps the machine shape: the gateway envelope verbatim when no
 *   projection flag is used, and the same envelope with a projected/paged
 *   array when `--fields`/`--limit`/`--offset` are present.
 * - `--fields` selects columns by name; an unknown name is a usage error that
 *   lists every valid name.
 */

/** Widest rendered cell before the value is elided; keeps rows terminal-sized. */
const MAX_CELL_WIDTH = 40;

export interface Column<T> {
	readonly name: string;
	/** Raw value used verbatim by `--json` projection. */
	readonly value: (row: T, index: number) => unknown;
}

export interface ListOptions {
	readonly json: boolean;
	readonly fields?: readonly string[];
	readonly limit?: number;
	readonly offset: number;
}

export function columnNames<T>(columns: readonly Column<T>[]): string[] {
	return columns.map((column) => column.name);
}

function positiveInteger(flag: string, raw: string | undefined): number {
	const value = Number(raw);
	if (raw === undefined || raw === "" || !Number.isInteger(value) || value < 0)
		throw new Error(`${flag} expects a non-negative integer, got: ${raw ?? "(missing)"}`);
	return value;
}

export function parseListOptions<T>(args: readonly string[], columns: readonly Column<T>[]): ListOptions {
	const valid = columnNames(columns);
	let json = false;
	let fields: string[] | undefined;
	let limit: number | undefined;
	let offset = 0;
	for (let i = 0; i < args.length; i++) {
		const arg = args[i] as string;
		if (arg === "--json") json = true;
		else if (arg === "--fields") {
			const raw = args[++i];
			if (!raw) throw new Error(`--fields expects a comma-separated column list (valid: ${valid.join(", ")})`);
			fields = raw
				.split(",")
				.map((field) => field.trim())
				.filter((field) => field.length > 0);
			if (fields.length === 0)
				throw new Error(`--fields expects a comma-separated column list (valid: ${valid.join(", ")})`);
			const unknown = fields.filter((field) => !valid.includes(field));
			if (unknown.length > 0) throw new Error(`unknown field(s): ${unknown.join(", ")} (valid: ${valid.join(", ")})`);
		} else if (arg === "--limit") limit = positiveInteger("--limit", args[++i]);
		else if (arg === "--offset") offset = positiveInteger("--offset", args[++i]);
		else throw new Error(`unknown option: ${arg}`);
	}
	return { json, ...(fields ? { fields } : {}), ...(limit === undefined ? {} : { limit }), offset };
}

export function selectColumns<T>(columns: readonly Column<T>[], fields?: readonly string[]): Column<T>[] {
	if (!fields) return [...columns];
	return fields.map((field) => columns.find((column) => column.name === field) as Column<T>);
}

/** Rows survive paging with their original index so `index` columns stay stable. */
export function pageRows<T>(rows: readonly T[], options: ListOptions): Array<{ row: T; index: number }> {
	const entries = rows.map((row, index) => ({ row, index }));
	const start = Math.min(options.offset, entries.length);
	return options.limit === undefined ? entries.slice(start) : entries.slice(start, start + options.limit);
}

/** `lastActivityAt` reads as `LAST_ACTIVITY_AT`, not as an unreadable letter run. */
export function headerLabel(name: string): string {
	return name.replace(/([a-z0-9])([A-Z])/g, "$1_$2").toUpperCase();
}

export function formatCell(value: unknown): string {
	if (value === null || value === undefined) return "-";
	const text = Array.isArray(value) ? (value.length === 0 ? "-" : value.join(",")) : String(value);
	if (text === "") return "-";
	return text.length > MAX_CELL_WIDTH ? `${text.slice(0, MAX_CELL_WIDTH - 1)}…` : text;
}

export function renderTable<T>(
	columns: readonly Column<T>[],
	entries: ReadonlyArray<{ row: T; index: number }>,
): string[] {
	if (columns.length === 0) return ["(no columns)"];
	const header = columns.map((column) => headerLabel(column.name));
	const body = entries.map((entry) => columns.map((column) => formatCell(column.value(entry.row, entry.index))));
	if (body.length === 0) return [header.join("  "), "(none)"];
	const widths = header.map((cell, index) =>
		Math.max(cell.length, ...body.map((row) => (row[index] as string).length)),
	);
	const line = (cells: string[]): string =>
		cells
			.map((cell, index) => (index === cells.length - 1 ? cell : cell.padEnd(widths[index] as number)))
			.join("  ")
			.trimEnd();
	return [line(header), ...body.map(line)];
}

export function projectRows<T>(
	columns: readonly Column<T>[],
	entries: ReadonlyArray<{ row: T; index: number }>,
): Array<Record<string, unknown>> {
	return entries.map((entry) =>
		Object.fromEntries(columns.map((column) => [column.name, column.value(entry.row, entry.index)])),
	);
}

function triggerSummary(monitor: MonitorRecord): string {
	const trigger = monitor.trigger;
	switch (trigger.kind) {
		case "cron":
			return trigger.schedule;
		case "webhook":
			return `webhook:${trigger.route}`;
		case "watcher":
			return `watch:${trigger.root}`;
		case "script":
			return `script:${trigger.intervalMs}ms`;
	}
}

export const MONITOR_COLUMNS: readonly Column<MonitorRecord>[] = [
	{ name: "id", value: (monitor) => monitor.monitorId.slice(0, 8) },
	{ name: "name", value: (monitor) => monitor.name },
	{ name: "schedule", value: triggerSummary },
	{ name: "events", value: (monitor) => [...monitor.eventTypes] },
	{ name: "target", value: (monitor) => (monitor.channelTarget ? originKey(monitor.channelTarget.origin) : null) },
	{ name: "enabled", value: (monitor) => monitor.enabled },
];

export interface SessionListRow {
	readonly origin: Parameters<typeof originKey>[0];
	readonly epoch: number;
	readonly createdAt: string;
	readonly lastActivityAt: string | null;
	readonly bootstrap: { readonly pending: boolean; readonly byteCount: number; readonly truncated: boolean };
}

export const SESSION_COLUMNS: readonly Column<SessionListRow>[] = [
	{ name: "index", value: (_session, index) => index },
	{ name: "origin", value: (session) => originKey(session.origin) },
	{ name: "epoch", value: (session) => session.epoch },
	{
		name: "bootstrap",
		value: (session) =>
			session.bootstrap.pending
				? "pending"
				: `${session.bootstrap.byteCount}B${session.bootstrap.truncated ? "/trunc" : ""}`,
	},
	{ name: "createdAt", value: (session) => session.createdAt },
	{ name: "lastActivityAt", value: (session) => session.lastActivityAt },
];

/**
 * Shared `list` output path: table by default, envelope-preserving JSON under
 * `--json`. `envelopeKey` is the array field of the gateway result so the
 * machine shape is identical to the pre-table output.
 */
export function renderList<T>(
	columns: readonly Column<T>[],
	rows: readonly T[],
	options: ListOptions,
	envelope: { readonly key: string; readonly result: unknown },
): string[] {
	const entries = pageRows(rows, options);
	const selected = selectColumns(columns, options.fields);
	if (!options.json) return renderTable(selected, entries);
	if (!options.fields && options.limit === undefined && options.offset === 0) return [JSON.stringify(envelope.result)];
	const projected = options.fields ? projectRows(selected, entries) : entries.map((entry) => entry.row);
	return [JSON.stringify({ ...(envelope.result as object), [envelope.key]: projected })];
}
