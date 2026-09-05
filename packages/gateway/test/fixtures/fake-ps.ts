export interface FakePsRow {
	readonly pid: number;
	readonly ppid: number;
	readonly command: string;
	readonly startedAt?: string;
}

/** ps -axo pid,ppid,lstart,command shaped output, with an ownership env tag. */
export function renderFakePsTable(rows: readonly FakePsRow[], tag: string): string {
	if (!/^[A-Za-z0-9_.-]+$/.test(tag)) throw new Error("fake ps tag must be a nonempty shell-safe token");
	return `${rows.map((row) => `${row.pid} ${row.ppid} ${row.startedAt ?? "Sun Sep  6 00:00:00 2026"} ${row.command} GAJAEWAY_FAKE_PS_TAG=${tag}`).join("\n")}\n`;
}

export function fakePsTable(env: Record<string, string | undefined> = process.env): string {
	const rows: unknown = JSON.parse(env.GAJAEWAY_FAKE_PS_ROWS ?? "[]");
	if (
		!Array.isArray(rows) ||
		rows.some((row) => !Number.isInteger(row?.pid) || !Number.isInteger(row?.ppid) || typeof row?.command !== "string")
	) {
		throw new Error("GAJAEWAY_FAKE_PS_ROWS must contain pid, ppid, command rows");
	}
	return renderFakePsTable(rows, env.GAJAEWAY_FAKE_PS_TAG ?? "fixture");
}

if (import.meta.main) process.stdout.write(fakePsTable());
