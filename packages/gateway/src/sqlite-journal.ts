export const SQLITE_JOURNAL_MODES = ["DELETE", "TRUNCATE", "PERSIST", "WAL"] as const;

export type SqliteJournalMode = (typeof SQLITE_JOURNAL_MODES)[number];

export function isSqliteJournalMode(value: unknown): value is SqliteJournalMode {
	return typeof value === "string" && SQLITE_JOURNAL_MODES.includes(value as SqliteJournalMode);
}
