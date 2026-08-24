import * as crypto from "node:crypto";
import type { CanonicalValue, ProfileIdentityProjection, WayProfile } from "../profile";
import { profileProjectionCanonical } from "../profile";

export type BootstrapState = "ABSENT" | "CREATING" | "CREATED" | "COMMITTED" | "FAILED_CLOSED";

export const GATEWAY_META_KEYS = [
	"bootstrap_state",
	"bootstrap_intent",
	"main_identity",
	"growth_intent",
	"profile_digest",
	"profile_digest_version",
	"profile_projection",
	"profile_tunables_revision",
	"profile_approved_at",
	"profile_approval_receipt",
	"failed_closed_reason",
	"failed_closed_since_ms",
	"alert_failed_closed",
	"alert_lock_quarantined",
	"tail_checkpoint",
	"tail_ring_rotation_count",
	"transcript_delivery_gap_count",
	"transcript_delivery_progress",
	"transcript_proof",
] as const;

/**
 * Alert payloads carry a condition and a BOUNDED reason token only. A free-text
 * reason is replaced rather than embedded, because these events render to chat
 * surfaces and must not become a vector for arbitrary text or leaked paths.
 */
function canonicalAlertPayload(condition: string, reason: string): string {
	const safe = /^[a-z][a-z0-9_]{0,63}$/.test(reason) ? reason : "unspecified";
	return stableMetadataJson({ condition, reason: safe });
}

export interface GatewayMetaEntry {
	readonly key: string;
	readonly value?: string;
}

export interface GatewayMetaReadOutput {
	readonly entries: readonly GatewayMetaEntry[];
}

export interface GatewayMetaExpectation {
	readonly key: string;
	readonly value: string;
}

export interface GatewayMetaPut {
	readonly key: string;
	readonly value: string;
}

export interface GatewayMetaTransactionInput {
	readonly expected: readonly GatewayMetaExpectation[];
	readonly puts: readonly GatewayMetaPut[];
	readonly deletes: readonly string[];
	readonly eventKind?: string;
	readonly eventPayloadJson?: string;
}

export interface GatewayMetaTransactionOutput {
	readonly applied: boolean;
	readonly cursor?: string;
}

/** Narrow durable-state surface shared by the real N-API core and unit fakes. */
export interface GatewayMetaBackend {
	gatewayMetaRead(keys: readonly string[]): GatewayMetaReadOutput;
	gatewayMetaTransaction(input: GatewayMetaTransactionInput): GatewayMetaTransactionOutput;
}

/** A broker-projected transcript snapshot, not a local file path or inode claim. */
export interface TranscriptFingerprint {
	readonly entryCount: number;
	readonly sha256: string;
}

/**
 * Durable identity of an operator-owned external GJC session. The locator and
 * transcript fingerprint bind adoption without granting gajaeway filesystem or
 * process ownership of the interactive session.
 */
export interface ExternalSessionIdentity {
	readonly version: 1;
	readonly sessionId: string;
	readonly locator: {
		readonly repo: string;
		readonly stateRoot: string;
	};
	readonly endpointGeneration: number;
	readonly hostIncarnation?: string;
	readonly transcript?: TranscriptFingerprint;
}

export interface BootstrapIntent {
	readonly nonce: string;
	readonly ts: number;
	/** Exact external session requested before any adoption state is committed. */
	readonly sessionId: string;
}

export interface GrowthIntent {
	readonly base: ExternalSessionIdentity;
	readonly startedAt: number;
}

/** Durable broker-tail envelope record; only its `(generation, seq)` pair is a watermark coordinate. */
export interface TailCheckpoint {
	readonly revision: number;
	readonly generation: number;
	readonly seq: number;
}

/** Durable replay point for finalized transcript entries with broker-stable ids. */
export interface TranscriptDeliveryProgress {
	readonly lastEntryId?: string;
	readonly fingerprint: TranscriptFingerprint;
}

/** Whether the durable adopted identity has a complete transcript fingerprint. */
export type TranscriptProof = "pending" | "proven";

/** Lexicographically compares the broker-tail `(generation, seq)` watermark pair. */
export function compareTailCheckpoints(left: TailCheckpoint, right: TailCheckpoint): number {
	if (left.generation !== right.generation) return left.generation < right.generation ? -1 : 1;
	if (left.seq !== right.seq) return left.seq < right.seq ? -1 : 1;
	return 0;
}

export interface DurableGatewayState {
	readonly bootstrapState: BootstrapState;
	readonly bootstrapIntent: BootstrapIntent | undefined;
	readonly mainIdentity: ExternalSessionIdentity | undefined;
	readonly growthIntent: GrowthIntent | undefined;
	readonly profileDigest: string | undefined;
	readonly profileDigestVersion: number;
	readonly profileProjection: CanonicalValue | undefined;
	readonly profileTunablesRevision: number;
	readonly profileApprovedAt: number | undefined;
	readonly profileApprovalReceipt: string | undefined;
	readonly failedClosedReason: string | undefined;
	readonly tailCheckpoint: TailCheckpoint | undefined;
	readonly tailRingRotationCount: number;
	readonly transcriptDeliveryGapCount: number;
	readonly transcriptDeliveryProgress: TranscriptDeliveryProgress | undefined;
	readonly transcriptProof: TranscriptProof;
}

export class GatewayStateError extends Error {
	readonly reason: string;

	constructor(reason: string, message = reason) {
		super(message);
		this.name = "GatewayStateError";
		this.reason = reason;
	}
}

export class GatewayStateConflictError extends GatewayStateError {
	constructor() {
		super("state_conflict", "The durable gateway state changed concurrently.");
		this.name = "GatewayStateConflictError";
	}
}

function parseJson(raw: string, key: string): unknown {
	try {
		return JSON.parse(raw) as unknown;
	} catch {
		throw new GatewayStateError("metadata_invalid", `${key} is not valid JSON.`);
	}
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requiredString(value: unknown, field: string): string {
	if (typeof value !== "string" || !value)
		throw new GatewayStateError("metadata_invalid", `${field} must be a non-empty string.`);
	return value;
}

function requiredNonNegativeInteger(value: unknown, field: string): number {
	if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
		throw new GatewayStateError("metadata_invalid", `${field} must be a non-negative safe integer.`);
	}
	return value;
}

function parseNullableJson(raw: string, key: string): unknown | undefined {
	const value = parseJson(raw, key);
	return value === null ? undefined : value;
}

function parseTranscriptFingerprint(value: unknown, key: string): TranscriptFingerprint {
	if (!isRecord(value)) throw new GatewayStateError("metadata_invalid", `${key} must be an object.`);
	const fingerprint = {
		entryCount: requiredNonNegativeInteger(value.entryCount, `${key}.entryCount`),
		sha256: requiredString(value.sha256, `${key}.sha256`),
	};
	if (!/^[a-f0-9]{64}$/i.test(fingerprint.sha256)) {
		throw new GatewayStateError("metadata_invalid", `${key}.sha256 must be a SHA-256 hex digest.`);
	}
	return fingerprint;
}

function parseTailCheckpoint(value: unknown, key: string): TailCheckpoint {
	if (!isRecord(value)) throw new GatewayStateError("metadata_invalid", `${key} must be an object.`);
	return {
		revision: requiredNonNegativeInteger(value.revision, `${key}.revision`),
		generation: requiredNonNegativeInteger(value.generation, `${key}.generation`),
		seq: requiredNonNegativeInteger(value.seq, `${key}.seq`),
	};
}

function parseExternalIdentity(value: unknown, key: string): ExternalSessionIdentity {
	if (!isRecord(value)) throw new GatewayStateError("metadata_invalid", `${key} must be an object.`);
	const locator = value.locator;
	if (!isRecord(locator)) throw new GatewayStateError("metadata_invalid", `${key}.locator must be an object.`);
	const hostIncarnation = value.hostIncarnation;
	if (hostIncarnation !== undefined && (typeof hostIncarnation !== "string" || !hostIncarnation)) {
		throw new GatewayStateError("metadata_invalid", `${key}.hostIncarnation must be a non-empty string.`);
	}
	if (value.version !== 1) throw new GatewayStateError("metadata_invalid", `${key}.version must be 1.`);
	return {
		version: 1,
		sessionId: requiredString(value.sessionId, `${key}.sessionId`),
		locator: {
			repo: requiredString(locator.repo, `${key}.locator.repo`),
			stateRoot: requiredString(locator.stateRoot, `${key}.locator.stateRoot`),
		},
		endpointGeneration: requiredNonNegativeInteger(value.endpointGeneration, `${key}.endpointGeneration`),
		...(hostIncarnation === undefined ? {} : { hostIncarnation }),
		...(value.transcript === undefined
			? {}
			: { transcript: parseTranscriptFingerprint(value.transcript, `${key}.transcript`) }),
	};
}

function parseBootstrapIntent(value: unknown): BootstrapIntent {
	if (!isRecord(value)) throw new GatewayStateError("metadata_invalid", "bootstrap_intent must be an object.");
	return {
		nonce: requiredString(value.nonce, "bootstrap_intent.nonce"),
		ts: requiredNonNegativeInteger(value.ts, "bootstrap_intent.ts"),
		sessionId: requiredString(value.sessionId, "bootstrap_intent.sessionId"),
	};
}

function parseGrowthIntent(value: unknown): GrowthIntent {
	if (!isRecord(value)) throw new GatewayStateError("metadata_invalid", "growth_intent must be an object.");
	return {
		base: parseExternalIdentity(value.base, "growth_intent.base"),
		startedAt: requiredNonNegativeInteger(value.startedAt, "growth_intent.startedAt"),
	};
}

function parseBootstrapState(raw: string): BootstrapState {
	if (raw === "ABSENT" || raw === "CREATING" || raw === "CREATED" || raw === "COMMITTED" || raw === "FAILED_CLOSED")
		return raw;
	throw new GatewayStateError("metadata_invalid", "bootstrap_state is invalid.");
}

function parseOptionalStringJson(raw: string, key: string): string | undefined {
	const parsed = parseNullableJson(raw, key);
	if (parsed === undefined) return undefined;
	return requiredString(parsed, key);
}

function parseOptionalProjection(raw: string): CanonicalValue | undefined {
	const parsed = parseNullableJson(raw, "profile_projection");
	return parsed as CanonicalValue | undefined;
}

function parseOptionalNonNegativeIntegerJson(raw: string, key: string): number | undefined {
	const parsed = parseNullableJson(raw, key);
	if (parsed === undefined) return undefined;
	return requiredNonNegativeInteger(parsed, key);
}

function parseOptionalTailCheckpoint(raw: string | undefined): TailCheckpoint | undefined {
	if (raw === undefined || raw === "null") return undefined;
	const parsed = parseNullableJson(raw, "tail_checkpoint");
	return parsed === undefined ? undefined : parseTailCheckpoint(parsed, "tail_checkpoint");
}

function parseTailRingRotationCount(raw: string): number {
	const count = Number(raw);
	if (!Number.isSafeInteger(count) || count < 0) {
		throw new GatewayStateError("metadata_invalid", "tail_ring_rotation_count is invalid.");
	}
	return count;
}

function parseTranscriptDeliveryGapCount(raw: string): number {
	const count = Number(raw);
	if (!Number.isSafeInteger(count) || count < 0) {
		throw new GatewayStateError("metadata_invalid", "transcript_delivery_gap_count is invalid.");
	}
	return count;
}

function parseOptionalTranscriptDeliveryProgress(raw: string | undefined): TranscriptDeliveryProgress | undefined {
	if (raw === undefined || raw === "null") return undefined;
	const value = parseNullableJson(raw, "transcript_delivery_progress");
	if (value === undefined) return undefined;
	if (!isRecord(value))
		throw new GatewayStateError("metadata_invalid", "transcript_delivery_progress must be an object.");
	const lastEntryId = value.lastEntryId;
	if (lastEntryId !== undefined && (typeof lastEntryId !== "string" || !lastEntryId)) {
		throw new GatewayStateError(
			"metadata_invalid",
			"transcript_delivery_progress.lastEntryId must be a non-empty string.",
		);
	}
	return {
		...(lastEntryId === undefined ? {} : { lastEntryId }),
		fingerprint: parseTranscriptFingerprint(value.fingerprint, "transcript_delivery_progress.fingerprint"),
	};
}

function parseTranscriptProof(raw: string): TranscriptProof {
	if (raw !== "pending" && raw !== "proven") {
		throw new GatewayStateError("metadata_invalid", "transcript_proof must be pending or proven.");
	}
	return raw;
}

function metadataMap(entries: readonly GatewayMetaEntry[]): Map<string, string> {
	const values = new Map<string, string>();
	for (const entry of entries) {
		if (entry.value !== undefined) values.set(entry.key, entry.value);
	}
	return values;
}

function requiredMeta(values: ReadonlyMap<string, string>, key: string): string {
	const value = values.get(key);
	if (value === undefined)
		throw new GatewayStateError("metadata_missing", `Required gateway metadata ${key} is missing.`);
	return value;
}

function parsedState(values: ReadonlyMap<string, string>): DurableGatewayState {
	const bootstrapState = parseBootstrapState(requiredMeta(values, "bootstrap_state"));
	const bootstrapIntentRaw = parseNullableJson(requiredMeta(values, "bootstrap_intent"), "bootstrap_intent");
	const mainIdentityRaw = parseNullableJson(requiredMeta(values, "main_identity"), "main_identity");
	const growthIntentRaw = parseNullableJson(requiredMeta(values, "growth_intent"), "growth_intent");
	const mainIdentity =
		mainIdentityRaw === undefined ? undefined : parseExternalIdentity(mainIdentityRaw, "main_identity");
	const growthIntent = growthIntentRaw === undefined ? undefined : parseGrowthIntent(growthIntentRaw);
	const transcriptProof = parseTranscriptProof(requiredMeta(values, "transcript_proof"));
	const transcriptDeliveryProgress = parseOptionalTranscriptDeliveryProgress(
		values.get("transcript_delivery_progress"),
	);
	if (mainIdentity?.transcript === undefined && transcriptProof === "proven") {
		throw new GatewayStateError(
			"metadata_invalid",
			"A proven transcript proof requires a fingerprinted main identity.",
		);
	}
	if (mainIdentity?.transcript !== undefined && transcriptProof === "pending") {
		throw new GatewayStateError(
			"metadata_invalid",
			"A pending transcript proof cannot carry a fingerprinted main identity.",
		);
	}
	if (
		transcriptProof === "pending" &&
		(growthIntent !== undefined || transcriptDeliveryProgress !== undefined || values.get("tail_checkpoint") !== "null")
	) {
		throw new GatewayStateError(
			"metadata_invalid",
			"A pending transcript proof cannot have transcript growth, delivery progress, or a ring watermark.",
		);
	}
	const profileDigestRaw = requiredMeta(values, "profile_digest");
	const profileDigest = profileDigestRaw === "null" ? undefined : requiredString(profileDigestRaw, "profile_digest");
	const profileDigestVersionRaw = requiredMeta(values, "profile_digest_version");
	const profileDigestVersion = Number(profileDigestVersionRaw);
	if (!Number.isSafeInteger(profileDigestVersion) || profileDigestVersion < 0) {
		throw new GatewayStateError("metadata_invalid", "profile_digest_version is invalid.");
	}
	const tunablesRevisionRaw = requiredMeta(values, "profile_tunables_revision");
	const profileTunablesRevision = Number(tunablesRevisionRaw);
	if (!Number.isSafeInteger(profileTunablesRevision) || profileTunablesRevision < 0) {
		throw new GatewayStateError("metadata_invalid", "profile_tunables_revision is invalid.");
	}
	return {
		bootstrapState,
		bootstrapIntent: bootstrapIntentRaw === undefined ? undefined : parseBootstrapIntent(bootstrapIntentRaw),
		mainIdentity,
		growthIntent,
		profileDigest,
		profileDigestVersion,
		profileProjection: parseOptionalProjection(requiredMeta(values, "profile_projection")),
		profileTunablesRevision,
		profileApprovedAt: parseOptionalNonNegativeIntegerJson(
			requiredMeta(values, "profile_approved_at"),
			"profile_approved_at",
		),
		profileApprovalReceipt: parseOptionalStringJson(
			requiredMeta(values, "profile_approval_receipt"),
			"profile_approval_receipt",
		),
		failedClosedReason: parseOptionalStringJson(requiredMeta(values, "failed_closed_reason"), "failed_closed_reason"),
		tailCheckpoint: parseOptionalTailCheckpoint(values.get("tail_checkpoint")),
		tailRingRotationCount: parseTailRingRotationCount(requiredMeta(values, "tail_ring_rotation_count")),
		transcriptDeliveryGapCount: parseTranscriptDeliveryGapCount(requiredMeta(values, "transcript_delivery_gap_count")),
		transcriptDeliveryProgress,
		transcriptProof,
	};
}

function stableMetadataJson(value: unknown): string {
	return JSON.stringify(value);
}

function identityJson(identity: ExternalSessionIdentity): string {
	return stableMetadataJson(identity);
}

function tailCheckpointJson(checkpoint: TailCheckpoint): string {
	return stableMetadataJson(checkpoint);
}

function transcriptDeliveryProgressJson(progress: TranscriptDeliveryProgress | undefined): string {
	return stableMetadataJson(progress ?? null);
}

function canonicalJson(value: unknown): string {
	if (value === null || typeof value === "boolean" || typeof value === "number" || typeof value === "string")
		return JSON.stringify(value);
	if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
	if (isRecord(value)) {
		return `{${Object.keys(value)
			.sort()
			.map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
			.join(",")}}`;
	}
	throw new GatewayStateError("transcript_fingerprint_invalid", "Transcript entries must be JSON values.");
}

/** Hashes a broker-projected transcript snapshot using deterministic object ordering. */
export function fingerprintTranscriptEntries(entries: readonly unknown[]): TranscriptFingerprint {
	const hash = crypto.createHash("sha256");
	for (const entry of entries) hash.update(canonicalJson(entry)).update("\n");
	return { entryCount: entries.length, sha256: hash.digest("hex") };
}

/** Compares the permanent adopted identity; broker process generations may legitimately advance. */
export function sameExternalSession(left: ExternalSessionIdentity, right: ExternalSessionIdentity): boolean {
	return (
		left.version === right.version &&
		left.sessionId === right.sessionId &&
		left.locator.repo === right.locator.repo &&
		left.locator.stateRoot === right.locator.stateRoot
	);
}

export function sameExternalFingerprint(left: ExternalSessionIdentity, right: ExternalSessionIdentity): boolean {
	if (!sameExternalSession(left, right)) return false;
	if (left.transcript === undefined || right.transcript === undefined) return left.transcript === right.transcript;
	return (
		left.transcript.entryCount === right.transcript.entryCount && left.transcript.sha256 === right.transcript.sha256
	);
}

/**
 * Recomputes the stored prefix from a fresh complete broker transcript snapshot.
 * This proves append-only growth without reading, resuming, or owning the GJC
 * transcript file directly.
 */
export function attestsExternalTranscriptGrowth(base: ExternalSessionIdentity, entries: readonly unknown[]): boolean {
	if (!base.transcript) return true;
	if (entries.length < base.transcript.entryCount) return false;
	const prefix = fingerprintTranscriptEntries(entries.slice(0, base.transcript.entryCount));
	return prefix.entryCount === base.transcript.entryCount && prefix.sha256 === base.transcript.sha256;
}

export class GatewayStateStore {
	readonly #backend: GatewayMetaBackend;

	constructor(backend: GatewayMetaBackend) {
		this.#backend = backend;
	}

	read(): DurableGatewayState {
		return parsedState(metadataMap(this.#backend.gatewayMetaRead(GATEWAY_META_KEYS).entries));
	}

	private transact(input: GatewayMetaTransactionInput): string | undefined {
		const result = this.#backend.gatewayMetaTransaction(input);
		if (!result.applied) throw new GatewayStateConflictError();
		return result.cursor;
	}

	markCreating(intent: BootstrapIntent): void {
		this.transact({
			expected: [{ key: "bootstrap_state", value: "ABSENT" }],
			puts: [
				{ key: "bootstrap_state", value: "CREATING" },
				{ key: "bootstrap_intent", value: stableMetadataJson(intent) },
			],
			deletes: [],
		});
	}

	markCreated(intent: BootstrapIntent): void {
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: "CREATING" },
				{ key: "bootstrap_intent", value: stableMetadataJson(intent) },
			],
			puts: [{ key: "bootstrap_state", value: "CREATED" }],
			deletes: [],
		});
	}

	commitBootstrap(
		expectedState: "CREATING" | "CREATED",
		intent: BootstrapIntent,
		identity: ExternalSessionIdentity,
		profile: WayProfile,
		ringCheckpoint: TailCheckpoint | undefined,
		transcriptProof: TranscriptProof,
		transcriptDeliveryProgress?: TranscriptDeliveryProgress,
	): void {
		if (transcriptProof === "proven") {
			if (
				!identity.transcript ||
				!ringCheckpoint ||
				!transcriptDeliveryProgress ||
				transcriptDeliveryProgress.fingerprint.entryCount !== identity.transcript.entryCount ||
				transcriptDeliveryProgress.fingerprint.sha256 !== identity.transcript.sha256
			) {
				throw new GatewayStateError(
					"transcript_proof_invalid",
					"A proven adoption requires a ring watermark, matching transcript fingerprint, and delivery baseline.",
				);
			}
		} else if (identity.transcript || ringCheckpoint || transcriptDeliveryProgress) {
			throw new GatewayStateError(
				"transcript_proof_invalid",
				"A pending adoption cannot carry a ring watermark, transcript fingerprint, or delivery baseline.",
			);
		}
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: expectedState },
				{ key: "bootstrap_intent", value: stableMetadataJson(intent) },
			],
			puts: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				{ key: "bootstrap_intent", value: "null" },
				{ key: "main_identity", value: identityJson(identity) },
				{ key: "growth_intent", value: "null" },
				{ key: "profile_digest", value: profile.digest.sha256 },
				{ key: "profile_digest_version", value: String(profile.digest.version) },
				{ key: "profile_projection", value: profileProjectionCanonical(profile.projection) },
				{ key: "profile_tunables_revision", value: String(profile.tunablesRevision) },
				{ key: "failed_closed_reason", value: "null" },
				{ key: "tail_checkpoint", value: ringCheckpoint === undefined ? "null" : tailCheckpointJson(ringCheckpoint) },
				{ key: "tail_ring_rotation_count", value: "0" },
				{ key: "transcript_delivery_gap_count", value: "0" },
				{ key: "transcript_delivery_progress", value: transcriptDeliveryProgressJson(transcriptDeliveryProgress) },
				{ key: "transcript_proof", value: transcriptProof },
			],
			deletes: [],
			...(ringCheckpoint === undefined
				? {}
				: {
						eventKind: "tail_adoption_start",
						eventPayloadJson: stableMetadataJson({ checkpoint: ringCheckpoint }),
					}),
		});
	}

	clearBootstrapIntent(expectedState: "CREATING" | "CREATED", intent: BootstrapIntent): void {
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: expectedState },
				{ key: "bootstrap_intent", value: stableMetadataJson(intent) },
			],
			puts: [
				{ key: "bootstrap_state", value: "ABSENT" },
				{ key: "bootstrap_intent", value: "null" },
			],
			deletes: [],
		});
	}

	writeGrowthIntent(identity: ExternalSessionIdentity, startedAt: number): void {
		if (!identity.transcript) {
			throw new GatewayStateError(
				"transcript_proof_pending",
				"Cannot open a growth window before the transcript proof is durable.",
			);
		}
		const intent: GrowthIntent = { base: identity, startedAt };
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				{ key: "main_identity", value: identityJson(identity) },
				{ key: "growth_intent", value: "null" },
				{ key: "transcript_proof", value: "proven" },
			],
			puts: [{ key: "growth_intent", value: stableMetadataJson(intent) }],
			deletes: [],
		});
	}

	refreshAfterGrowth(intent: GrowthIntent, identity: ExternalSessionIdentity): void {
		if (!identity.transcript) {
			throw new GatewayStateError(
				"transcript_proof_pending",
				"Cannot refresh transcript growth without a durable transcript proof.",
			);
		}
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				{ key: "growth_intent", value: stableMetadataJson(intent) },
				{ key: "transcript_proof", value: "proven" },
			],
			puts: [
				{ key: "main_identity", value: identityJson(identity) },
				{ key: "growth_intent", value: "null" },
			],
			deletes: [],
		});
	}

	/**
	 * Atomically binds a pending adoption to its first complete transcript and ring
	 * snapshot. A pending adoption has no durable delivery baseline, so consuming
	 * this first complete tail must leave an explicit loss record rather than
	 * silently treating its tip as delivered.
	 */
	persistTranscriptProof(
		durable: ExternalSessionIdentity,
		observed: ExternalSessionIdentity,
		transcriptDeliveryProgress: TranscriptDeliveryProgress,
		ringCheckpoint: TailCheckpoint,
	): void {
		if (
			durable.transcript ||
			!observed.transcript ||
			!sameExternalSession(durable, observed) ||
			transcriptDeliveryProgress.fingerprint.entryCount !== observed.transcript.entryCount ||
			transcriptDeliveryProgress.fingerprint.sha256 !== observed.transcript.sha256
		) {
			throw new GatewayStateError(
				"transcript_proof_invalid",
				"The observed transcript proof does not bind the committed external session.",
			);
		}
		const current = this.read();
		if (current.transcriptDeliveryGapCount >= Number.MAX_SAFE_INTEGER) {
			throw new GatewayStateError(
				"transcript_delivery_gap_overflow",
				"The durable transcript delivery-gap count overflowed.",
			);
		}
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				{ key: "main_identity", value: identityJson(durable) },
				{ key: "transcript_proof", value: "pending" },
				{ key: "tail_checkpoint", value: "null" },
				{ key: "transcript_delivery_progress", value: transcriptDeliveryProgressJson(undefined) },
				{ key: "transcript_delivery_gap_count", value: String(current.transcriptDeliveryGapCount) },
			],
			puts: [
				{ key: "main_identity", value: identityJson(observed) },
				{ key: "transcript_proof", value: "proven" },
				{ key: "tail_checkpoint", value: tailCheckpointJson(ringCheckpoint) },
				{ key: "transcript_delivery_progress", value: transcriptDeliveryProgressJson(transcriptDeliveryProgress) },
				{ key: "transcript_delivery_gap_count", value: String(current.transcriptDeliveryGapCount + 1) },
			],
			deletes: [],
			eventKind: "transcript_delivery_gap",
			eventPayloadJson: stableMetadataJson({
				reason: "transcript_delivery_progress_missing",
				adoption: "pending_proof_promotion",
				available_through_entry_id: transcriptDeliveryProgress.lastEntryId,
				checkpoint: ringCheckpoint,
			}),
		});
	}

	advanceTranscriptDeliveryProgress(
		expected: TranscriptDeliveryProgress | undefined,
		next: TranscriptDeliveryProgress,
	): void {
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				{ key: "transcript_proof", value: "proven" },
				{ key: "transcript_delivery_progress", value: transcriptDeliveryProgressJson(expected) },
			],
			puts: [{ key: "transcript_delivery_progress", value: transcriptDeliveryProgressJson(next) }],
			deletes: [],
		});
	}

	/**
	 * Re-binds the durable identity to an observed transcript that is an attested
	 * append-only extension of the persisted prefix, for autonomous persona growth
	 * that happened with no growth intent open (for example while the daemon was
	 * down). The prefix attestation is re-checked here so the durable write itself
	 * is guarded, never only the caller.
	 *
	 * `transcript_delivery_progress` is deliberately left untouched: the observed
	 * suffix stays undelivered, so the existing projection path must still either
	 * deliver it or journal an explicit `transcript_delivery_gap`. Absorbing growth
	 * must never imply the new entries were delivered.
	 */
	absorbAutonomousTranscriptGrowth(
		durable: ExternalSessionIdentity,
		observed: ExternalSessionIdentity,
		entries: readonly unknown[],
	): void {
		if (!durable.transcript || !observed.transcript) {
			throw new GatewayStateError(
				"transcript_proof_invalid",
				"Autonomous growth absorption requires a durable and observed transcript fingerprint.",
			);
		}
		if (!sameExternalSession(durable, observed)) {
			throw new GatewayStateError(
				"main_identity_mismatch",
				"Autonomous growth absorption cannot change the adopted external session.",
			);
		}
		if (!attestsExternalTranscriptGrowth(durable, entries)) {
			throw new GatewayStateError(
				"main_identity_mismatch",
				"The observed transcript is not an append-only extension of the persisted prefix.",
			);
		}
		const observedFingerprint = fingerprintTranscriptEntries(entries);
		if (
			observedFingerprint.entryCount !== observed.transcript.entryCount ||
			observedFingerprint.sha256 !== observed.transcript.sha256
		) {
			throw new GatewayStateError(
				"transcript_proof_mismatch",
				"The observed transcript entries do not match the observed fingerprint.",
			);
		}
		if (observed.transcript.entryCount === durable.transcript.entryCount) return;
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				{ key: "main_identity", value: identityJson(durable) },
				{ key: "growth_intent", value: "null" },
				{ key: "transcript_proof", value: "proven" },
			],
			puts: [{ key: "main_identity", value: identityJson(observed) }],
			deletes: [],
			eventKind: "main_identity_growth_absorbed",
			eventPayloadJson: stableMetadataJson({
				reason: "autonomous_append_only_growth",
				prior_entry_count: durable.transcript.entryCount,
				observed_entry_count: observed.transcript.entryCount,
				absorbed_entries: observed.transcript.entryCount - durable.transcript.entryCount,
			}),
		});
	}

	/** Atomically journals a transcript projection, its delivery replay point, and any detected delivery gap. */
	appendTranscriptProjection(
		expectedTail: TailCheckpoint | undefined,
		checkpoint: TailCheckpoint,
		expectedDelivery: TranscriptDeliveryProgress | undefined,
		nextDelivery: TranscriptDeliveryProgress,
		kind: string,
		payloadJson: string,
	): void {
		if (expectedTail && compareTailCheckpoints(checkpoint, expectedTail) < 0) {
			throw new GatewayStateError("tail_checkpoint_regression", "Broker-tail checkpoint regressed.");
		}
		const state = kind === "transcript_delivery_gap" ? this.read() : undefined;
		if (state && state.transcriptDeliveryGapCount >= Number.MAX_SAFE_INTEGER) {
			throw new GatewayStateError(
				"transcript_delivery_gap_overflow",
				"The durable transcript delivery-gap count overflowed.",
			);
		}
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				{ key: "transcript_proof", value: "proven" },
				...(expectedTail === undefined ? [] : [{ key: "tail_checkpoint", value: tailCheckpointJson(expectedTail) }]),
				{ key: "transcript_delivery_progress", value: transcriptDeliveryProgressJson(expectedDelivery) },
				...(state === undefined
					? []
					: [{ key: "transcript_delivery_gap_count", value: String(state.transcriptDeliveryGapCount) }]),
			],
			puts: [
				{ key: "tail_checkpoint", value: tailCheckpointJson(checkpoint) },
				{ key: "transcript_delivery_progress", value: transcriptDeliveryProgressJson(nextDelivery) },
				...(state === undefined
					? []
					: [{ key: "transcript_delivery_gap_count", value: String(state.transcriptDeliveryGapCount + 1) }]),
			],
			deletes: [],
			eventKind: kind,
			eventPayloadJson: payloadJson,
		});
	}

	setTunablesRevision(revision: number): void {
		if (!Number.isSafeInteger(revision) || revision < 0) {
			throw new GatewayStateError("revision_invalid", "Profile tunables revision is invalid.");
		}
		const state = this.read();
		if (state.profileTunablesRevision === revision) return;
		this.transact({
			expected: [{ key: "profile_tunables_revision", value: String(state.profileTunablesRevision) }],
			puts: [{ key: "profile_tunables_revision", value: String(revision) }],
			deletes: [],
		});
	}

	/** Records the first actual event-ring envelope as an adoption boundary without projecting it. */
	recordTailAdoptionStart(checkpoint: TailCheckpoint): void {
		const state = this.read();
		if (state.tailCheckpoint) {
			if (compareTailCheckpoints(state.tailCheckpoint, checkpoint) === 0) return;
			throw new GatewayStateError("tail_checkpoint_exists", "A broker-tail checkpoint is already durable.");
		}
		const raw = this.#backend.gatewayMetaRead(["tail_checkpoint"]).entries[0]?.value;
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				...(raw === undefined ? [] : [{ key: "tail_checkpoint", value: raw }]),
			],
			puts: [{ key: "tail_checkpoint", value: tailCheckpointJson(checkpoint) }],
			deletes: [],
			eventKind: "tail_adoption_start",
			eventPayloadJson: stableMetadataJson({ checkpoint }),
		});
	}

	/**
	 * Advances the durable broker-tail watermark only after the caller has
	 * committed every projection derived from that tail batch.
	 */
	advanceTailCheckpoint(expected: TailCheckpoint | undefined, checkpoint: TailCheckpoint): void {
		if (expected && compareTailCheckpoints(checkpoint, expected) < 0) {
			throw new GatewayStateError("tail_checkpoint_regression", "Broker-tail checkpoint regressed.");
		}
		if (expected && compareTailCheckpoints(checkpoint, expected) === 0) return;
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				...(expected === undefined ? [] : [{ key: "tail_checkpoint", value: tailCheckpointJson(expected) }]),
			],
			puts: [{ key: "tail_checkpoint", value: tailCheckpointJson(checkpoint) }],
			deletes: [],
		});
	}

	/** Atomically records a retention floor that advanced beyond the established tail coordinate. */
	recordTailRingRotation(previous: TailCheckpoint, resync: TailCheckpoint): void {
		if (compareTailCheckpoints(resync, previous) <= 0) {
			throw new GatewayStateError(
				"tail_ring_rotation_invalid",
				"A ring rotation resync must advance the established tail coordinate.",
			);
		}
		const state = this.read();
		if (state.tailRingRotationCount >= Number.MAX_SAFE_INTEGER) {
			throw new GatewayStateError("tail_ring_rotation_overflow", "The durable tail-ring rotation count overflowed.");
		}
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				{ key: "tail_checkpoint", value: tailCheckpointJson(previous) },
				{ key: "tail_ring_rotation_count", value: String(state.tailRingRotationCount) },
			],
			puts: [
				{ key: "tail_checkpoint", value: tailCheckpointJson(resync) },
				{ key: "tail_ring_rotation_count", value: String(state.tailRingRotationCount + 1) },
			],
			deletes: [],
			eventKind: "tail_ring_rotation",
			eventPayloadJson: stableMetadataJson({ prior_watermark: previous, resync_point: resync }),
		});
	}
	/** Atomically journals one projected tail event with its consumed watermark. */
	appendTailProjection(
		expected: TailCheckpoint | undefined,
		checkpoint: TailCheckpoint,
		kind: string,
		payloadJson: string,
	): void {
		if (expected && compareTailCheckpoints(checkpoint, expected) < 0) {
			throw new GatewayStateError("tail_checkpoint_regression", "Broker-tail checkpoint regressed.");
		}
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				...(expected === undefined ? [] : [{ key: "tail_checkpoint", value: tailCheckpointJson(expected) }]),
			],
			puts: [{ key: "tail_checkpoint", value: tailCheckpointJson(checkpoint) }],
			deletes: [],
			eventKind: kind,
			eventPayloadJson: payloadJson,
		});
	}

	/**
	 * Enters failed-closed and raises its alert in ONE commit. Two transactions
	 * would leave a window where the gateway is failed closed and silent, which
	 * is exactly the state an operator needs told. Writing only on a
	 * clear-to-raised transition keeps a restart from re-announcing.
	 */
	markFailedClosed(reason: string, now: number = Date.now()): void {
		if (!reason) throw new GatewayStateError("failure_reason_invalid", "A fail-closed reason is required.");
		const state = this.read();
		const alertRaised = this.rawMeta("alert_failed_closed") === "raised";
		this.transact({
			expected: [{ key: "bootstrap_state", value: state.bootstrapState }],
			puts: [
				{ key: "bootstrap_state", value: "FAILED_CLOSED" },
				{ key: "failed_closed_reason", value: stableMetadataJson(reason) },
				{ key: "failed_closed_since_ms", value: String(now) },
				{ key: "alert_failed_closed", value: "raised" },
			],
			deletes: [],
			...(alertRaised
				? {}
				: { eventKind: "alert_raised", eventPayloadJson: canonicalAlertPayload("failed_closed", reason) }),
		});
	}

	/**
	 * Startup catch-up for a durable failed-closed state carrying no raised
	 * alert: an upgraded database defaults the flag to `clear`, and a host-fatal
	 * path may mark state without folding an alert. Load-bearing rather than a
	 * belt, so it must run on EVERY startup that observes failed-closed.
	 */
	catchUpFailedClosedAlert(now: number = Date.now()): boolean {
		const state = this.read();
		if (state.bootstrapState !== "FAILED_CLOSED") return false;
		if (this.rawMeta("alert_failed_closed") === "raised") return false;
		this.transact({
			expected: [{ key: "alert_failed_closed", value: this.rawMeta("alert_failed_closed") ?? "clear" }],
			puts: [
				{ key: "alert_failed_closed", value: "raised" },
				...(this.rawMeta("failed_closed_since_ms") === "null"
					? [{ key: "failed_closed_since_ms", value: String(now) }]
					: []),
			],
			deletes: [],
			eventKind: "alert_raised",
			eventPayloadJson: canonicalAlertPayload("failed_closed", state.failedClosedReason ?? "unknown"),
		});
		return true;
	}

	/** Clears the fail-closed alert exactly once, when the condition ends. */
	clearFailedClosedAlert(): boolean {
		if (this.rawMeta("alert_failed_closed") !== "raised") return false;
		this.transact({
			expected: [{ key: "alert_failed_closed", value: "raised" }],
			puts: [
				{ key: "alert_failed_closed", value: "clear" },
				{ key: "failed_closed_since_ms", value: "null" },
			],
			deletes: [],
			eventKind: "alert_cleared",
			eventPayloadJson: canonicalAlertPayload("failed_closed", "resolved"),
		});
		return true;
	}

	private rawMeta(key: string): string | undefined {
		return this.#backend.gatewayMetaRead([key]).entries[0]?.value;
	}

	/**
	 * Clears exactly one recorded fail-closed reason after the caller has
	 * re-proven, from live evidence, that the condition no longer holds. The
	 * expected reason is part of the CAS, so a state that failed closed again for
	 * a different reason between re-verification and this write cannot be cleared.
	 *
	 * Journal rows, tail checkpoint, transcript delivery progress, consumer
	 * checkpoints, and admission records are deliberately untouched: recovery
	 * restores serviceability without discarding durable history.
	 */
	recoverFailedClosed(
		expectedReason: string,
		receiptId: string,
		recoveredAt: number,
		evidence: string,
		rebind?: { readonly observed: ExternalSessionIdentity; readonly entries: readonly unknown[] },
	): string | undefined {
		if (!expectedReason)
			throw new GatewayStateError("failure_reason_invalid", "A recovered fail-closed reason is required.");
		if (!receiptId) throw new GatewayStateError("recovery_receipt_invalid", "A recovery receipt id is required.");
		if (!evidence)
			throw new GatewayStateError("recovery_evidence_invalid", "Recovery requires recorded re-verification evidence.");
		const state = this.read();
		if (state.bootstrapState !== "FAILED_CLOSED") {
			throw new GatewayStateError("not_failed_closed", "Only a failed-closed gateway can be recovered.");
		}
		if (state.failedClosedReason !== expectedReason) {
			throw new GatewayStateError(
				"failed_closed_reason_changed",
				"The recorded fail-closed reason does not match the re-verified reason.",
			);
		}
		const durable = state.mainIdentity;
		if (!durable) {
			throw new GatewayStateError(
				"bootstrap_not_committed",
				"A gateway without a durable adopted identity cannot be recovered.",
			);
		}
		// A transcript re-bind must land in the SAME transaction that clears the
		// marker: clearing first would briefly publish a COMMITTED state whose
		// identity still disagreed with the broker, and re-binding first cannot
		// work at all because the identity write requires a COMMITTED state.
		let identityPut: { readonly key: string; readonly value: string } | undefined;
		if (rebind) {
			if (!durable.transcript || !rebind.observed.transcript) {
				throw new GatewayStateError(
					"transcript_proof_invalid",
					"A recovery re-bind requires a durable and observed transcript fingerprint.",
				);
			}
			if (!sameExternalSession(durable, rebind.observed)) {
				throw new GatewayStateError(
					"main_identity_mismatch",
					"A recovery re-bind cannot change the adopted external session.",
				);
			}
			if (!attestsExternalTranscriptGrowth(durable, rebind.entries)) {
				throw new GatewayStateError(
					"main_identity_mismatch",
					"The observed transcript is not an append-only extension of the persisted prefix.",
				);
			}
			const observedFingerprint = fingerprintTranscriptEntries(rebind.entries);
			if (
				observedFingerprint.entryCount !== rebind.observed.transcript.entryCount ||
				observedFingerprint.sha256 !== rebind.observed.transcript.sha256
			) {
				throw new GatewayStateError(
					"transcript_proof_mismatch",
					"The observed transcript entries do not match the observed fingerprint.",
				);
			}
			if (state.transcriptProof !== "proven") {
				throw new GatewayStateError(
					"transcript_proof_pending",
					"A recovery re-bind requires a proven durable transcript proof.",
				);
			}
			identityPut = { key: "main_identity", value: identityJson(rebind.observed) };
		}
		return this.transact({
			expected: [
				{ key: "bootstrap_state", value: "FAILED_CLOSED" },
				{ key: "failed_closed_reason", value: stableMetadataJson(expectedReason) },
				...(identityPut === undefined ? [] : [{ key: "main_identity", value: identityJson(durable) }]),
			],
			puts: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				{ key: "failed_closed_reason", value: "null" },
				...(identityPut === undefined ? [] : [identityPut]),
			],
			deletes: [],
			eventKind: "failed_closed_recovered",
			eventPayloadJson: stableMetadataJson({
				receipt_id: receiptId,
				recovered_at: recoveredAt,
				cleared_reason: expectedReason,
				verification_evidence: evidence,
				...(rebind === undefined
					? {}
					: {
							rebound_prior_entry_count: durable.transcript?.entryCount ?? 0,
							rebound_observed_entry_count: rebind.observed.transcript?.entryCount ?? 0,
						}),
			}),
		});
	}
	approveProfile(
		profile: WayProfile,
		receiptId: string,
		approvedAt: number,
	): { readonly cursor?: string; readonly previousProjection: CanonicalValue | undefined } {
		const state = this.read();
		if (
			state.bootstrapState === "ABSENT" ||
			state.bootstrapState === "CREATING" ||
			state.bootstrapState === "CREATED"
		) {
			throw new GatewayStateError(
				"bootstrap_not_committed",
				"A profile cannot be approved before bootstrap is committed.",
			);
		}
		if (state.bootstrapState === "FAILED_CLOSED" && state.failedClosedReason !== "profile_drift") {
			throw new GatewayStateError(
				"failed_closed_not_profile_drift",
				"Only profile drift can be cleared by profile approval.",
			);
		}
		const cursor = this.transact({
			expected: [{ key: "bootstrap_state", value: state.bootstrapState }],
			puts: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				{ key: "profile_digest", value: profile.digest.sha256 },
				{ key: "profile_digest_version", value: String(profile.digest.version) },
				{ key: "profile_projection", value: profileProjectionCanonical(profile.projection) },
				{ key: "profile_tunables_revision", value: String(profile.tunablesRevision) },
				{ key: "profile_approved_at", value: stableMetadataJson(approvedAt) },
				{ key: "profile_approval_receipt", value: stableMetadataJson(receiptId) },
				{ key: "failed_closed_reason", value: "null" },
				// Recovery clears the alert in the SAME commit that ends the
				// condition; otherwise way.status keeps reporting a resolved alert.
				{ key: "alert_failed_closed", value: "clear" },
				{ key: "failed_closed_since_ms", value: "null" },
			],
			deletes: [],
			eventKind: "profile_approved",
			eventPayloadJson: stableMetadataJson({
				receipt_id: receiptId,
				approved_at: approvedAt,
				profile_digest: profile.digest.sha256,
				profile_digest_version: profile.digest.version,
			}),
		});
		return { cursor, previousProjection: state.profileProjection };
	}
}
