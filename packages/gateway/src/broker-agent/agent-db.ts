import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";

/** Providers that use rotating OAuth refresh tokens and should not be copied to the broker agent. */
const ROTATING_OAUTH_PROVIDERS = new Set(["anthropic", "openai-codex"]);

export class AgentDatabaseError extends Error {
	readonly code = "agent_database_invalid";
	constructor(message: string) {
		super(message);
		this.name = "AgentDatabaseError";
	}
}

// SQLite's read-only WAL reader may rebuild or update the transient -shm index.
// It leaves agent.db and the WAL frames untouched; deleting -shm would invalidate concurrent readers.
export function snapshotAgentDb(source: string, destination: string): void {
	const sourceDb = new Database(source, { readonly: true });
	try {
		const quotedDestination = destination.replaceAll("'", "''");
		sourceDb.exec(`VACUUM INTO '${quotedDestination}'`);
	} finally {
		sourceDb.close();
	}
	assertAgentDb(destination);
}

/**
 * Copy the agent database to the broker agent directory, filtering out OAuth credentials
 * for providers with rotating refresh tokens to avoid sharing tokens between stores.
 * Returns the list of filtered provider names.
 */
export function snapshotAgentDbWithoutRotatingCredentials(source: string, destination: string): string[] {
	const sourceDb = new Database(source, { readonly: true });
	try {
		const quotedDestination = destination.replaceAll("'", "''");
		sourceDb.exec(`VACUUM INTO '${quotedDestination}'`);
	} finally {
		sourceDb.close();
	}

	// Filter out rotating OAuth credentials from the destination database
	const destDb = new Database(destination);
	const filtered: string[] = [];
	try {
		const columns = destDb.query("PRAGMA table_info(auth_credentials)").all() as Array<{ name: string }>;
		const hasCredentialType = columns.some((col) => col.name === "credential_type");

		const rows = destDb.query("SELECT provider FROM auth_credentials").all() as Array<{ provider: unknown }>;
		for (const row of rows) {
			const provider = typeof row.provider === "string" ? row.provider : "";
			if (ROTATING_OAUTH_PROVIDERS.has(provider)) {
				filtered.push(provider);
				if (hasCredentialType) {
					// Only delete OAuth type, keep API-key type
					destDb.query("DELETE FROM auth_credentials WHERE provider = ? AND credential_type = 'oauth'").run(provider);
				} else {
					// Fallback for schemas without credential_type column
					destDb.query("DELETE FROM auth_credentials WHERE provider = ?").run(provider);
				}
			}
		}
	} finally {
		destDb.close();
	}

	assertAgentDb(destination);
	return filtered;
}

/**
 * Merge rotating OAuth credentials from an existing broker agent.db into the staged copy.
 * This preserves the broker's independent login credentials when re-provisioning.
 */
export function mergeRotatingOAuthCredentials(brokerDbPath: string, stagedDbPath: string): void {
	const brokerDb = new Database(brokerDbPath, { readonly: true });
	try {
		const stagedDb = new Database(stagedDbPath);
		try {
			// Check if credential_type column exists
			const columns = brokerDb.query("PRAGMA table_info(auth_credentials)").all() as Array<{ name: string }>;
			const hasCredentialType = columns.some((col) => col.name === "credential_type");

			// Fetch rotating OAuth credentials from broker database
			let brokerRows: Array<Record<string, unknown>>;
			if (hasCredentialType) {
				brokerRows = brokerDb
					.query(
						`SELECT * FROM auth_credentials WHERE provider IN (${Array.from(ROTATING_OAUTH_PROVIDERS)
							.map((p) => `'${p.replaceAll("'", "''")}' `)
							.join(",")} ) AND credential_type = 'oauth'`,
					)
					.all() as Array<Record<string, unknown>>;
			} else {
				brokerRows = brokerDb
					.query(
						`SELECT * FROM auth_credentials WHERE provider IN (${Array.from(ROTATING_OAUTH_PROVIDERS)
							.map((p) => `'${p.replaceAll("'", "''")}' `)
							.join(",")} )`,
					)
					.all() as Array<Record<string, unknown>>;
			}

			// Insert broker credentials into staged database. The rowid comes from the broker store and can
			// collide with an unrelated operator row in the staged copy, so let SQLite assign a fresh one.
			for (const row of brokerRows) {
				const columnNames = Object.keys(row).filter((name) => name !== "id");
				const placeholders = columnNames.map(() => "?").join(", ");
				const values = columnNames.map((name) => row[name]);
				stagedDb
					.query(
						`INSERT INTO auth_credentials (${columnNames.map((n) => `"${n.replaceAll('"', '""')}"`).join(", ")}) VALUES (${placeholders})`,
					)
					.run(...(values as any[]));
			}
		} finally {
			stagedDb.close();
		}
	} finally {
		brokerDb.close();
	}
}

export function assertAgentDb(path: string): void {
	const database = new Database(path, { readonly: true });
	try {
		const integrity = database.query("PRAGMA integrity_check").get() as Record<string, unknown> | null;
		if (!integrity || Object.values(integrity)[0] !== "ok")
			throw new AgentDatabaseError("agent.db failed integrity_check");
		const credentials = database
			.query("SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = 'auth_credentials'")
			.get() as Record<string, unknown> | null;
		if (!credentials) throw new AgentDatabaseError("agent.db is missing auth_credentials");
	} catch (error) {
		if (error instanceof AgentDatabaseError) throw error;
		throw new AgentDatabaseError(
			`agent.db could not be validated: ${error instanceof Error ? error.message : String(error)}`,
		);
	} finally {
		database.close();
	}
}

export function readAuthCredentialData(path: string): string[] {
	const database = new Database(path, { readonly: true });
	try {
		const rows = database.query("SELECT data FROM auth_credentials").all() as Array<{ data: unknown }>;
		return rows.flatMap((row) => (typeof row.data === "string" ? [row.data] : []));
	} finally {
		database.close();
	}
}

export function credentialFingerprint(path: string): string {
	return credentialFingerprintFiltered(path, false);
}

/**
 * Fingerprint auth_credentials, optionally filtering out rotating OAuth rows.
 * When filtered=true, excludes rows for ROTATING_OAUTH_PROVIDERS with credential_type='oauth'.
 */
export function credentialFingerprintFiltered(path: string, filtered: boolean): string {
	const database = new Database(path, { readonly: true });
	try {
		const columns = database.query("PRAGMA table_info(auth_credentials)").all() as Array<{ name: string }>;
		if (columns.length === 0) throw new AgentDatabaseError("agent.db is missing auth_credentials");
		const columnNames = columns.map((column) => column.name);
		const quotedColumns = columnNames.map((name) => `"${name.replaceAll('"', '""')}"`);

		let query = `SELECT * FROM auth_credentials`;
		if (filtered) {
			const hasCredentialType = columns.some((col) => col.name === "credential_type");
			if (hasCredentialType) {
				// Exclude rotating OAuth rows by checking if NOT (provider in rotating set AND type='oauth')
				// Build dynamic WHERE clause for all rotating providers
				const conditions = Array.from(ROTATING_OAUTH_PROVIDERS)
					.map((provider) => `(provider = '${provider.replaceAll("'", "''")}' AND credential_type = 'oauth')`)
					.join(" OR ");
				query += ` WHERE NOT (${conditions})`;
			}
		}

		query += ` ORDER BY ${quotedColumns.join(", ")}`;
		const rows = database.query(query).all() as Array<Record<string, unknown>>;
		const canonical = JSON.stringify({
			columns: columnNames,
			rows: rows.map((row) => columnNames.map((name) => canonicalSqlValue(row[name]))),
		});
		return createHash("sha256").update(canonical).digest("hex");
	} catch (error) {
		if (error instanceof AgentDatabaseError) throw error;
		throw new AgentDatabaseError(
			`auth_credentials could not be fingerprinted: ${error instanceof Error ? error.message : String(error)}`,
		);
	} finally {
		database.close();
	}
}

function canonicalSqlValue(value: unknown): unknown {
	if (typeof value === "bigint") return { integer: value.toString() };
	if (value instanceof Uint8Array) return { blob: Buffer.from(value).toString("base64") };
	if (typeof value === "number" && !Number.isFinite(value)) return { number: String(value) };
	return value;
}
