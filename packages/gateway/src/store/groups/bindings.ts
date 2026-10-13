import { realpathSync } from "node:fs";
import { isAbsolute, normalize } from "node:path";
import { parseLaneJobRecord } from "@gajae-gateway/subsession";
import type { GroupDb, StoreCore } from "../core";
import { BrokerAuthorityError } from "../errors";
import type { WriteTx } from "../tx";

export interface BrokerAuthority {
	readonly canonicalAgentDir: string;
	readonly identity: string;
}

export interface OwnedBrokerBinding {
	readonly sessionId: string;
	readonly originKey: string;
	readonly epoch: number;
	readonly repo: string;
	readonly authority: BrokerAuthority;
}

export type BrokerQuarantineKind = "inbound" | "work" | "monitor";
export type GjcModelSelection = string | { readonly preset: string };

export interface ConversationModelRecord {
	readonly selection: GjcModelSelection;
	readonly setBy?: string;
	readonly updatedAt: string;
}

export interface SessionRecord {
	readonly sessionId: string;
	readonly epoch: number;
}

export interface SessionRow {
	readonly origin_key: string;
	readonly origin_ref_json: string | null;
	readonly gjc_session_id: string;
	readonly epoch: number;
	readonly turn_count: number;
	readonly created_at: string;
	readonly last_activity_at: string | null;
	readonly last_bootstrapped_epoch: number;
	readonly bootstrap_applied_at: string | null;
	readonly bootstrap_sections_json: string;
	readonly bootstrap_byte_count: number;
	readonly bootstrap_truncated: number;
	readonly bootstrap_diagnostics_json: string;
	readonly agents_md_epoch: number;
	readonly agents_md_digest: string | null;
}

export interface SessionBootstrap {
	readonly epoch: number;
	readonly lastBootstrappedEpoch: number;
	readonly agentsMdEpoch: number;
	readonly agentsMdDigest: string | null;
	readonly appliedAt: string | null;
	readonly includedSections: readonly string[];
	readonly byteCount: number;
	readonly truncated: boolean;
	readonly diagnostics: readonly string[];
}

function brokerAuthorityKey(authority: BrokerAuthority): string {
	if (
		!authority ||
		typeof authority.identity !== "string" ||
		!authority.identity.trim() ||
		authority.identity.includes("\0") ||
		typeof authority.canonicalAgentDir !== "string" ||
		!isAbsolute(authority.canonicalAgentDir) ||
		normalize(authority.canonicalAgentDir) !== authority.canonicalAgentDir ||
		authority.canonicalAgentDir.includes("\0")
	)
		throw new BrokerAuthorityError("invalid_authority");
	return JSON.stringify([authority.canonicalAgentDir, authority.identity]);
}

function sameRepo(recorded: string, requested: string): boolean {
	if (recorded === requested) return true;
	try {
		return realpathSync(recorded) === realpathSync(requested);
	} catch {
		return false;
	}
}

function parseStringList(value: string): readonly string[] {
	try {
		const parsed: unknown = JSON.parse(value);
		return Array.isArray(parsed) && parsed.every((item) => typeof item === "string") ? parsed : ["projection_corrupt"];
	} catch {
		return ["projection_corrupt"];
	}
}

export function parseModelSelection(value: unknown): GjcModelSelection | undefined {
	if (typeof value === "string") return value.trim() === "" ? undefined : value;
	if (typeof value !== "object" || value === null) return undefined;
	const keys = Object.keys(value);
	if (keys.length !== 1 || keys[0] !== "preset") return undefined;
	const preset = (value as { preset?: unknown }).preset;
	if (typeof preset !== "string" || preset.trim() === "") return undefined;
	return { preset };
}

/** Persistence for sessions, broker ownership, authority, cursors and model overrides. */
export class BindingsGroup {
	readonly #db: GroupDb;

	constructor(core: StoreCore) {
		this.#db = core.groupDb("bindings");
	}

	get activeSessionCount(): number {
		return (this.#db.prepare("SELECT COUNT(*) AS count FROM sessions").get() as { count: number } | null)?.count ?? 0;
	}

	getSessionRecord(originKey: string): SessionRecord | undefined {
		const row = this.#db.prepare("SELECT gjc_session_id, epoch FROM sessions WHERE origin_key = ?").get(originKey) as {
			gjc_session_id: string;
			epoch: number;
		} | null;
		return row ? { sessionId: row.gjc_session_id, epoch: row.epoch } : undefined;
	}

	getOriginByKey(originKey: string): Record<string, unknown> | undefined {
		const row = this.#db.prepare("SELECT origin_ref_json FROM sessions WHERE origin_key = ?").get(originKey) as {
			origin_ref_json: string | null;
		} | null;
		if (!row?.origin_ref_json) return undefined;
		try {
			return JSON.parse(row.origin_ref_json) as Record<string, unknown>;
		} catch {
			return undefined;
		}
	}

	getSessionBootstrap(originKey: string): SessionBootstrap | undefined {
		const row = this.#db
			.prepare(
				"SELECT epoch, last_bootstrapped_epoch, agents_md_epoch, agents_md_digest, bootstrap_applied_at, bootstrap_sections_json, bootstrap_byte_count, bootstrap_truncated, bootstrap_diagnostics_json FROM sessions WHERE origin_key = ?",
			)
			.get(originKey) as {
			epoch: number;
			last_bootstrapped_epoch: number;
			agents_md_epoch: number;
			agents_md_digest: string | null;
			bootstrap_applied_at: string | null;
			bootstrap_sections_json: string;
			bootstrap_byte_count: number;
			bootstrap_truncated: number;
			bootstrap_diagnostics_json: string;
		} | null;
		if (!row) return undefined;
		return {
			epoch: row.epoch,
			lastBootstrappedEpoch: row.last_bootstrapped_epoch,
			agentsMdEpoch: row.agents_md_epoch,
			agentsMdDigest: row.agents_md_digest,
			appliedAt: row.bootstrap_applied_at,
			includedSections: parseStringList(row.bootstrap_sections_json),
			byteCount: row.bootstrap_byte_count,
			truncated: row.bootstrap_truncated === 1,
			diagnostics: parseStringList(row.bootstrap_diagnostics_json),
		};
	}

	recordSessionAgentsBaseline(tx: WriteTx, originKey: string, epoch: number, digest: string): boolean {
		return (
			this.#db
				.prepare(
					"UPDATE sessions SET agents_md_epoch = ?, agents_md_digest = ? WHERE origin_key = ? AND epoch = ? AND (agents_md_epoch < ? OR (agents_md_epoch = ? AND agents_md_digest IS NOT ?))",
				)
				.run(tx, epoch, digest, originKey, epoch, epoch, epoch, digest).changes === 1
		);
	}

	markSessionBootstrapped(
		tx: WriteTx,
		originKey: string,
		epoch: number,
		projection: {
			readonly includedSections: readonly string[];
			readonly byteCount: number;
			readonly truncated: boolean;
			readonly diagnostics: readonly string[];
		},
	): boolean {
		return (
			this.#db
				.prepare(
					"UPDATE sessions SET last_bootstrapped_epoch = ?, bootstrap_applied_at = ?, bootstrap_sections_json = ?, bootstrap_byte_count = ?, bootstrap_truncated = ?, bootstrap_diagnostics_json = ? WHERE origin_key = ? AND epoch = ? AND last_bootstrapped_epoch < ?",
				)
				.run(
					tx,
					epoch,
					new Date().toISOString(),
					JSON.stringify(projection.includedSections),
					projection.byteCount,
					projection.truncated ? 1 : 0,
					JSON.stringify(projection.diagnostics),
					originKey,
					epoch,
					epoch,
				).changes === 1
		);
	}

	bind(tx: WriteTx, originKey: string, sessionId: string): void {
		if (this.brokerAuthority()) throw new BrokerAuthorityError("unowned_session");
		this.#db
			.prepare(
				"INSERT INTO sessions (origin_key, gjc_session_id, created_at) VALUES (?, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET gjc_session_id = excluded.gjc_session_id",
			)
			.run(tx, originKey, sessionId, new Date().toISOString());
	}

	bindAtEpoch(tx: WriteTx, originKey: string, sessionId: string, epoch: number): boolean {
		if (this.brokerAuthority()) throw new BrokerAuthorityError("unowned_session");
		if (!Number.isSafeInteger(epoch) || epoch < 0) throw new Error("session epoch must be a non-negative integer");
		return (
			this.#db
				.prepare(
					"INSERT INTO sessions (origin_key, gjc_session_id, epoch, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET gjc_session_id = excluded.gjc_session_id WHERE sessions.epoch = excluded.epoch",
				)
				.run(tx, originKey, sessionId, epoch, new Date().toISOString()).changes === 1
		);
	}

	rotateEpoch(tx: WriteTx, originKey: string, originRefJson?: string): number {
		const now = new Date().toISOString();
		if (originRefJson !== undefined) {
			this.#db
				.prepare(
					"INSERT INTO sessions (origin_key, origin_ref_json, gjc_session_id, epoch, created_at, last_activity_at) VALUES (?, ?, '', 1, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET epoch = epoch + 1, gjc_session_id = '', turn_count = 0, origin_ref_json = excluded.origin_ref_json, last_activity_at = excluded.last_activity_at",
				)
				.run(tx, originKey, originRefJson, now, now);
		} else {
			this.#db
				.prepare(
					"INSERT INTO sessions (origin_key, gjc_session_id, epoch, turn_count, created_at, last_activity_at) VALUES (?, '', 1, 0, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET epoch = epoch + 1, gjc_session_id = '', turn_count = 0, last_activity_at = excluded.last_activity_at",
				)
				.run(tx, originKey, now, now);
		}
		const epoch = (
			this.#db.prepare("SELECT epoch FROM sessions WHERE origin_key = ?").get(originKey) as { epoch: number } | null
		)?.epoch;
		if (epoch === undefined) throw new Error(`session row for ${originKey} disappeared during rotation`);
		return epoch;
	}

	retire(tx: WriteTx, sessionId: string, cutoverId: string): void {
		this.#db
			.prepare(
				"INSERT INTO broker_retired_sessions(session_id, cutover_id) VALUES (?, ?) ON CONFLICT(session_id) DO NOTHING",
			)
			.run(tx, sessionId, cutoverId);
	}

	touch(tx: WriteTx, originKey: string, originRefJson: string, at = new Date().toISOString()): void {
		this.#db
			.prepare("UPDATE sessions SET last_activity_at = ?, origin_ref_json = ? WHERE origin_key = ?")
			.run(tx, at, originRefJson, originKey);
	}

	touchBoundSession(tx: WriteTx, originKey: string, sessionId: string, epoch: number, at: string): boolean {
		return (
			this.#db
				.prepare(
					"UPDATE sessions SET last_activity_at = ? WHERE origin_key = ? AND gjc_session_id = ? AND epoch = ? AND (last_activity_at IS NULL OR last_activity_at < ?)",
				)
				.run(tx, at, originKey, sessionId, epoch, at).changes === 1
		);
	}

	touchWorkSession(
		tx: WriteTx,
		originKey: string,
		sessionId: string,
		epoch: number,
		at: string,
		originRefJson: string,
	): void {
		this.#db
			.prepare(
				"UPDATE sessions SET last_activity_at = ?, origin_ref_json = ? WHERE origin_key = ? AND gjc_session_id = ? AND epoch = ?",
			)
			.run(tx, at, originRefJson, originKey, sessionId, epoch);
	}

	incrementTurnCount(tx: WriteTx, originKey: string, originRefJson?: string): number {
		const now = new Date().toISOString();
		this.#db
			.prepare(
				"INSERT INTO sessions (origin_key, origin_ref_json, gjc_session_id, epoch, turn_count, created_at, last_activity_at) VALUES (?, ?, '', 0, 1, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET turn_count = turn_count + 1",
			)
			.run(tx, originKey, originRefJson ?? null, now, now);
		return (
			(
				this.#db.prepare("SELECT turn_count FROM sessions WHERE origin_key = ?").get(originKey) as {
					turn_count: number;
				} | null
			)?.turn_count ?? 0
		);
	}

	sessionRows(): Array<{
		origin_ref_json: string | null;
		created_at: string;
		last_activity_at: string | null;
		epoch: number;
		last_bootstrapped_epoch: number;
		bootstrap_applied_at: string | null;
		bootstrap_sections_json: string;
		bootstrap_byte_count: number;
		bootstrap_truncated: number;
		bootstrap_diagnostics_json: string;
	}> {
		return this.#db
			.prepare(
				"SELECT origin_ref_json, created_at, last_activity_at, epoch, last_bootstrapped_epoch, bootstrap_applied_at, bootstrap_sections_json, bootstrap_byte_count, bootstrap_truncated, bootstrap_diagnostics_json FROM sessions ORDER BY created_at",
			)
			.all() as Array<{
			origin_ref_json: string | null;
			created_at: string;
			last_activity_at: string | null;
			epoch: number;
			last_bootstrapped_epoch: number;
			bootstrap_applied_at: string | null;
			bootstrap_sections_json: string;
			bootstrap_byte_count: number;
			bootstrap_truncated: number;
			bootstrap_diagnostics_json: string;
		}>;
	}

	sessionIdentityRows(): Array<{
		origin_key: string;
		origin_ref_json: string | null;
		gjc_session_id: string;
		epoch: number;
		created_at: string;
		last_activity_at: string | null;
		last_bootstrapped_epoch: number;
		bootstrap_applied_at: string | null;
		bootstrap_sections_json: string;
		bootstrap_byte_count: number;
		bootstrap_truncated: number;
		bootstrap_diagnostics_json: string;
	}> {
		return this.#db
			.prepare(
				"SELECT origin_key, origin_ref_json, gjc_session_id, epoch, created_at, last_activity_at, last_bootstrapped_epoch, bootstrap_applied_at, bootstrap_sections_json, bootstrap_byte_count, bootstrap_truncated, bootstrap_diagnostics_json FROM sessions ORDER BY created_at",
			)
			.all() as Array<{
			origin_key: string;
			origin_ref_json: string | null;
			gjc_session_id: string;
			epoch: number;
			created_at: string;
			last_activity_at: string | null;
			last_bootstrapped_epoch: number;
			bootstrap_applied_at: string | null;
			bootstrap_sections_json: string;
			bootstrap_byte_count: number;
			bootstrap_truncated: number;
			bootstrap_diagnostics_json: string;
		}>;
	}

	contextSessionCreatedAt(originKey: string): string | undefined {
		return (
			this.#db.prepare("SELECT created_at FROM sessions WHERE origin_key = ?").get(originKey) as {
				created_at: string;
			} | null
		)?.created_at;
	}

	sessionTurnCount(originKey: string): number {
		return (
			(
				this.#db.prepare("SELECT turn_count FROM sessions WHERE origin_key = ?").get(originKey) as {
					turn_count: number;
				} | null
			)?.turn_count ?? 0
		);
	}

	getSession(originKey: string): string | undefined {
		return (
			this.#db.prepare("SELECT gjc_session_id FROM sessions WHERE origin_key = ?").get(originKey) as {
				gjc_session_id: string;
			} | null
		)?.gjc_session_id;
	}

	workLaneRows(): Array<{ origin_key: string; gjc_session_id: string; last_activity_at: string | null }> {
		return this.#db
			.prepare(
				"SELECT origin_key, gjc_session_id, last_activity_at FROM sessions WHERE origin_key LIKE 'work/task/%' AND gjc_session_id <> '' ORDER BY last_activity_at",
			)
			.all() as Array<{ origin_key: string; gjc_session_id: string; last_activity_at: string | null }>;
	}

	workLaneRepoBySessionId(sessionId: string): string | undefined {
		const authority = this.brokerAuthority();
		if (!authority) return undefined;
		return (
			this.#db
				.prepare("SELECT repo FROM broker_owned_bindings WHERE authority_key = ? AND session_id = ?")
				.get(brokerAuthorityKey(authority), sessionId) as { repo: string } | null
		)?.repo;
	}

	hasCutovers(): boolean {
		return this.#db.prepare("SELECT 1 FROM broker_cutovers LIMIT 1").get() !== null;
	}

	isBrokerMode(): boolean {
		return this.brokerAuthority() !== null;
	}

	sessionEpochs(): Array<{ epoch: number }> {
		return this.#db.prepare("SELECT epoch FROM sessions").all() as Array<{ epoch: number }>;
	}

	referencedSessionIds(): ReadonlySet<string> {
		const rows = this.#db
			.prepare(`SELECT b.session_id AS id FROM broker_owned_bindings b
				JOIN broker_authority a ON a.authority_key = b.authority_key
				WHERE a.singleton = 1 AND b.session_id IN (
					SELECT gjc_session_id FROM sessions WHERE gjc_session_id <> ''
					UNION SELECT bound_session_id FROM inbound_messages
					WHERE bound_session_id IS NOT NULL AND state = 'pending'
					AND message_id NOT IN (SELECT subject_id FROM broker_quarantine WHERE kind = 'inbound')
					UNION SELECT session_id FROM work_attempt_runtime WHERE settled_at IS NULL
					AND job_id NOT IN (SELECT subject_id FROM broker_quarantine WHERE kind = 'work')
				)`)
			.all() as Array<{ id: string }>;
		return new Set(rows.map((row) => row.id));
	}

	get authority(): BrokerAuthority | null {
		return this.brokerAuthority();
	}

	brokerAuthority(): BrokerAuthority | null {
		const row = this.#db.prepare("SELECT authority_key FROM broker_authority WHERE singleton = 1").get() as {
			authority_key: string;
		} | null;
		if (!row) return null;
		let value: unknown;
		try {
			value = JSON.parse(row.authority_key);
		} catch {
			throw new BrokerAuthorityError("invalid_authority");
		}
		if (!Array.isArray(value) || value.length !== 2 || typeof value[0] !== "string" || typeof value[1] !== "string")
			throw new BrokerAuthorityError("invalid_authority");
		const authority = { canonicalAgentDir: value[0], identity: value[1] };
		if (brokerAuthorityKey(authority) !== row.authority_key) throw new BrokerAuthorityError("invalid_authority");
		return authority;
	}

	assertAuthority(authority: BrokerAuthority): void {
		const current = this.brokerAuthority();
		if (current === null) throw new BrokerAuthorityError("cutover_required");
		if (brokerAuthorityKey(current) !== brokerAuthorityKey(authority))
			throw new BrokerAuthorityError("authority_mismatch");
	}

	setAuthority(tx: WriteTx, authority: BrokerAuthority): void {
		const key = brokerAuthorityKey(authority);
		this.#db
			.prepare(
				"INSERT INTO broker_authority(singleton, authority_key) VALUES (1, ?) ON CONFLICT(singleton) DO UPDATE SET authority_key = excluded.authority_key",
			)
			.run(tx, key);
	}

	initializeAuthority(tx: WriteTx, authority: BrokerAuthority, populated: boolean | (() => boolean)): void {
		const current = this.brokerAuthority();
		if (current !== null) {
			if (brokerAuthorityKey(current) !== brokerAuthorityKey(authority))
				throw new BrokerAuthorityError("authority_mismatch");
			return;
		}
		if (typeof populated === "function" ? populated() : populated) throw new BrokerAuthorityError("cutover_required");
		this.setAuthority(tx, authority);
	}

	assertInitialAuthority(tx: WriteTx, canonicalAgentDir: string, populated: () => boolean): void {
		if (
			!isAbsolute(canonicalAgentDir) ||
			normalize(canonicalAgentDir) !== canonicalAgentDir ||
			canonicalAgentDir.includes("\0")
		)
			throw new BrokerAuthorityError("invalid_authority");
		const expected: BrokerAuthority = { canonicalAgentDir, identity: `gjc:${canonicalAgentDir}` };
		const current = this.brokerAuthority();
		if (current !== null) {
			if (brokerAuthorityKey(current) !== brokerAuthorityKey(expected))
				throw new BrokerAuthorityError("authority_mismatch");
			return;
		}
		if (populated()) throw new BrokerAuthorityError("cutover_required");
		this.setAuthority(tx, expected);
	}

	originForSessionId(sessionId: string): string | undefined {
		if (!/^[A-Za-z0-9-]{1,128}$/.test(sessionId)) return undefined;
		const origins = new Set<string>();
		const session = this.#db.prepare("SELECT origin_key FROM sessions WHERE gjc_session_id = ?").get(sessionId) as {
			origin_key: string;
		} | null;
		if (session) origins.add(session.origin_key);
		const authority = this.brokerAuthority();
		if (authority) {
			for (const row of this.#db
				.prepare(
					"SELECT origin_key FROM broker_owned_bindings WHERE authority_key = ? AND session_id = ? AND NOT EXISTS (SELECT 1 FROM broker_retired_sessions r WHERE r.session_id = broker_owned_bindings.session_id)",
				)
				.all(brokerAuthorityKey(authority), sessionId) as Array<{ origin_key: string }>)
				origins.add(row.origin_key);
		}
		return origins.size === 1 ? origins.values().next().value : undefined;
	}

	recordOwnedBinding(tx: WriteTx, binding: OwnedBrokerBinding): boolean {
		this.assertAuthority(binding.authority);
		if (
			!binding.sessionId ||
			!binding.originKey ||
			!isAbsolute(binding.repo) ||
			normalize(binding.repo) !== binding.repo ||
			!Number.isSafeInteger(binding.epoch) ||
			binding.epoch < 0
		)
			throw new BrokerAuthorityError("unowned_session");
		if (this.#db.prepare("SELECT 1 FROM broker_retired_sessions WHERE session_id = ?").get(binding.sessionId))
			throw new BrokerAuthorityError("unowned_session");
		const current = this.getSessionRecord(binding.originKey);
		if (current && current.epoch !== binding.epoch) return false;
		const key = brokerAuthorityKey(binding.authority);
		const previous = this.#db
			.prepare("SELECT origin_key, epoch, repo FROM broker_owned_bindings WHERE authority_key = ? AND session_id = ?")
			.get(key, binding.sessionId) as { origin_key: string; epoch: number; repo: string } | null;
		if (
			previous &&
			(previous.origin_key !== binding.originKey ||
				previous.epoch !== binding.epoch ||
				!sameRepo(previous.repo, binding.repo))
		)
			throw new BrokerAuthorityError("unowned_session");
		this.#db
			.prepare(
				"INSERT INTO broker_owned_bindings(authority_key, session_id, origin_key, epoch, repo, created_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(authority_key, session_id) DO NOTHING",
			)
			.run(tx, key, binding.sessionId, binding.originKey, binding.epoch, binding.repo, new Date().toISOString());
		this.#db
			.prepare(
				"INSERT INTO sessions(origin_key, gjc_session_id, epoch, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET gjc_session_id = excluded.gjc_session_id",
			)
			.run(tx, binding.originKey, binding.sessionId, binding.epoch, new Date().toISOString());
		return true;
	}

	assertOwnedSession(sessionId: string, repo: string, authority: BrokerAuthority): OwnedBrokerBinding {
		this.assertAuthority(authority);
		const row = this.#db
			.prepare("SELECT origin_key, epoch, repo FROM broker_owned_bindings WHERE authority_key = ? AND session_id = ?")
			.get(brokerAuthorityKey(authority), sessionId) as { origin_key: string; epoch: number; repo: string } | null;
		if (!row || !sameRepo(row.repo, repo)) throw new BrokerAuthorityError("unowned_session");
		return { sessionId, originKey: row.origin_key, epoch: row.epoch, repo: row.repo, authority };
	}

	isQuarantined(kind: BrokerQuarantineKind, id: string): boolean {
		return this.#db.prepare("SELECT 1 FROM broker_quarantine WHERE kind = ? AND subject_id = ?").get(kind, id) !== null;
	}

	quarantineInvalidWork(tx: WriteTx, jobId: string): void {
		this.#db
			.prepare(
				"INSERT INTO broker_quarantine(kind, subject_id, cutover_id) VALUES (?, ?, ?) ON CONFLICT(kind, subject_id) DO NOTHING",
			)
			.run(tx, "work", jobId, "recovery-auto-quarantine");
	}

	hasCutoverTarget(authority: BrokerAuthority): boolean {
		return (
			this.#db
				.prepare("SELECT 1 FROM broker_cutovers WHERE old_authority = ? OR target_authority = ? LIMIT 1")
				.get(brokerAuthorityKey(authority), brokerAuthorityKey(authority)) !== null
		);
	}

	cutoverSnapshot(id: string): string | undefined {
		return (
			this.#db.prepare("SELECT snapshot_json FROM broker_cutovers WHERE id = ?").get(id) as {
				snapshot_json: string;
			} | null
		)?.snapshot_json;
	}

	cutoverBrokerAuthority(
		tx: WriteTx,
		input: {
			readonly expectedAuthority: BrokerAuthority | null;
			readonly targetAuthority: BrokerAuthority;
			readonly evidence: string;
			readonly disposition?: "quarantine";
		},
	): string {
		const target = brokerAuthorityKey(input.targetAuthority);
		const expected = input.expectedAuthority === null ? null : brokerAuthorityKey(input.expectedAuthority);
		const current = this.brokerAuthority();
		if ((current === null ? null : brokerAuthorityKey(current)) !== expected || expected === target)
			throw new BrokerAuthorityError("authority_mismatch");
		if (this.hasCutoverTarget(input.targetAuthority)) throw new BrokerAuthorityError("authority_mismatch");
		if (typeof input.evidence !== "string" || !input.evidence.trim()) throw new Error("cutover evidence is required");
		if (input.disposition !== undefined && input.disposition !== "quarantine")
			throw new Error("invalid cutover disposition");

		let openWork = 0;
		for (const row of this.#db.prepare("SELECT job_id, record_json FROM lane_jobs").all() as Array<{
			job_id: string;
			record_json: string;
		}>) {
			const job = parseLaneJobRecord(row.record_json);
			if (job.jobId !== row.job_id) throw new Error("invalid lane job identity");
			if (
				!this.isQuarantined("work", job.jobId) &&
				(job.attempts.some((attempt) => attempt.endedAt === undefined) || !["done", "aborted"].includes(job.state))
			)
				openWork++;
		}
		openWork +=
			(
				this.#db
					.prepare(
						"SELECT COUNT(*) AS n FROM work_attempt_runtime WHERE settled_at IS NULL AND NOT EXISTS (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'work' AND q.subject_id = work_attempt_runtime.job_id)",
					)
					.get() as { n: number } | null
			)?.n ?? 0;
		openWork +=
			(
				this.#db.prepare("SELECT COUNT(*) AS n FROM lane_reports WHERE state IN ('pending','claimed')").get() as {
					n: number;
				} | null
			)?.n ?? 0;
		const openInbound =
			(
				this.#db
					.prepare(
						"SELECT COUNT(*) AS n FROM inbound_messages WHERE (state <> 'done' OR turn_state IN ('bound','accepted')) AND NOT EXISTS (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'inbound' AND q.subject_id = inbound_messages.message_id)",
					)
					.get() as { n: number } | null
			)?.n ?? 0;
		const openMonitors =
			(
				this.#db
					.prepare(
						"SELECT COUNT(*) AS n FROM monitor_events WHERE stage NOT IN ('delivered','authored_no_delivery','failed_no_retry','skipped') AND NOT EXISTS (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'monitor' AND q.subject_id = monitor_events.event_id)",
					)
					.get() as { n: number } | null
			)?.n ?? 0;
		if ((openInbound || openWork || openMonitors) && input.disposition !== "quarantine")
			throw new BrokerAuthorityError("old_work_open");

		const snapshotTables = [
			"sessions",
			"deliveries",
			"recall_snippets",
			"meta",
			"memory_intents",
			"monitors",
			"monitor_events",
			"authored_outputs",
			"inbound_messages",
			"conversation_context",
			"lane_jobs",
			"monitor_failures",
			"monitor_slots",
			"dispatch_leases",
			"conversation_context_state",
			"conversation_model",
			"session_tail_cursors",
			"work_attempt_runtime",
			"lane_reports",
			"broker_owned_bindings",
			"broker_tail_cursors",
			"broker_retired_sessions",
			"broker_quarantine",
		] as const;
		const snapshot = Object.fromEntries(
			snapshotTables.map((table) => [table, this.#db.prepare(`SELECT * FROM ${table}`).all()]),
		);
		const id = crypto.randomUUID();
		this.#db
			.prepare(
				"INSERT INTO broker_cutovers(id, old_authority, target_authority, evidence, disposition, snapshot_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
			)
			.run(
				tx,
				id,
				expected,
				target,
				input.evidence,
				input.disposition ?? "quiescent",
				JSON.stringify(snapshot),
				new Date().toISOString(),
			);

		const quarantines: ReadonlyArray<readonly [BrokerQuarantineKind, string[]]> = [
			[
				"inbound",
				(this.#db.prepare("SELECT message_id AS id FROM inbound_messages").all() as Array<{ id: string }>).map(
					(row) => row.id,
				),
			],
			[
				"work",
				(this.#db.prepare("SELECT job_id AS id FROM lane_jobs").all() as Array<{ id: string }>).map((row) => row.id),
			],
			[
				"monitor",
				(this.#db.prepare("SELECT event_id AS id FROM monitor_events").all() as Array<{ id: string }>).map(
					(row) => row.id,
				),
			],
		];
		const quarantine = this.#db.prepare(
			"INSERT INTO broker_quarantine(kind, subject_id, cutover_id) VALUES (?, ?, ?) ON CONFLICT(kind, subject_id) DO NOTHING",
		);
		for (const [kind, ids] of quarantines) for (const subjectId of ids) quarantine.run(tx, kind, subjectId, id);

		const retired = new Set<string>();
		for (const row of this.#db
			.prepare(
				"SELECT gjc_session_id AS id FROM sessions WHERE gjc_session_id <> '' UNION SELECT bound_session_id FROM inbound_messages WHERE bound_session_id IS NOT NULL UNION SELECT session_id FROM work_attempt_runtime UNION SELECT session_id FROM session_tail_cursors UNION SELECT session_id FROM broker_owned_bindings",
			)
			.all() as Array<{ id: string }>)
			retired.add(row.id);
		for (const row of this.#db.prepare("SELECT record_json FROM lane_jobs").all() as Array<{ record_json: string }>)
			for (const sessionId of parseLaneJobRecord(row.record_json).sessions) retired.add(sessionId);
		const retire = this.#db.prepare(
			"INSERT INTO broker_retired_sessions(session_id, cutover_id) VALUES (?, ?) ON CONFLICT(session_id) DO NOTHING",
		);
		for (const sessionId of retired) retire.run(tx, sessionId, id);
		this.#db.prepare("UPDATE sessions SET epoch = epoch + 1, gjc_session_id = '', turn_count = 0").run(tx);
		this.setAuthority(tx, input.targetAuthority);
		return id;
	}

	modelGet(originKey: string): ConversationModelRecord | undefined {
		const row = this.#db
			.prepare("SELECT selection_json, set_by, updated_at FROM conversation_model WHERE origin_key = ?")
			.get(originKey) as { selection_json: string; set_by: string | null; updated_at: string } | null;
		if (!row) return undefined;
		let selection: unknown;
		try {
			selection = JSON.parse(row.selection_json);
		} catch {
			return undefined;
		}
		const parsed = parseModelSelection(selection);
		if (!parsed) return undefined;
		return { selection: parsed, ...(row.set_by === null ? {} : { setBy: row.set_by }), updatedAt: row.updated_at };
	}

	modelSet(tx: WriteTx, originKey: string, selection: GjcModelSelection, setBy?: string): void {
		this.#db
			.prepare(
				"INSERT OR REPLACE INTO conversation_model (origin_key, selection_json, set_by, updated_at) VALUES (?, ?, ?, ?)",
			)
			.run(tx, originKey, JSON.stringify(selection), setBy ?? null, new Date().toISOString());
	}

	modelClear(tx: WriteTx, originKey: string): boolean {
		return this.#db.prepare("DELETE FROM conversation_model WHERE origin_key = ?").run(tx, originKey).changes > 0;
	}

	sessionTailCursor(sessionId: string): string | undefined {
		return (
			this.#db.prepare("SELECT cursor FROM session_tail_cursors WHERE session_id = ?").get(sessionId) as {
				cursor: string;
			} | null
		)?.cursor;
	}

	setSessionTailCursor(tx: WriteTx, sessionId: string, cursor: string): void {
		this.#db
			.prepare(
				"INSERT INTO session_tail_cursors(session_id, cursor, updated_at) VALUES (?, ?, ?) ON CONFLICT(session_id) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at",
			)
			.run(tx, sessionId, cursor, new Date().toISOString());
	}

	brokerTailCursor(authority: BrokerAuthority, sessionId: string): string | undefined {
		const row = this.#db
			.prepare("SELECT cursor FROM broker_tail_cursors WHERE authority_key = ? AND session_id = ?")
			.get(brokerAuthorityKey(authority), sessionId) as { cursor: string } | null;
		return row?.cursor;
	}

	setBrokerTailCursor(tx: WriteTx, authority: BrokerAuthority, sessionId: string, cursor: string): void {
		this.assertAuthority(authority);
		this.#db
			.prepare(
				"INSERT INTO broker_tail_cursors(authority_key, session_id, cursor, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(authority_key, session_id) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at",
			)
			.run(tx, brokerAuthorityKey(authority), sessionId, cursor, new Date().toISOString());
	}
}
