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
] as const;

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
	if (typeof value !== "string" || !value) throw new GatewayStateError("metadata_invalid", `${field} must be a non-empty string.`);
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
		...(value.transcript === undefined ? {} : { transcript: parseTranscriptFingerprint(value.transcript, `${key}.transcript`) }),
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
	if (raw === "ABSENT" || raw === "CREATING" || raw === "CREATED" || raw === "COMMITTED" || raw === "FAILED_CLOSED") return raw;
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

function metadataMap(entries: readonly GatewayMetaEntry[]): Map<string, string> {
	const values = new Map<string, string>();
	for (const entry of entries) {
		if (entry.value !== undefined) values.set(entry.key, entry.value);
	}
	return values;
}

function requiredMeta(values: ReadonlyMap<string, string>, key: string): string {
	const value = values.get(key);
	if (value === undefined) throw new GatewayStateError("metadata_missing", `Required gateway metadata ${key} is missing.`);
	return value;
}

function parsedState(values: ReadonlyMap<string, string>): DurableGatewayState {
	const bootstrapState = parseBootstrapState(requiredMeta(values, "bootstrap_state"));
	const bootstrapIntentRaw = parseNullableJson(requiredMeta(values, "bootstrap_intent"), "bootstrap_intent");
	const mainIdentityRaw = parseNullableJson(requiredMeta(values, "main_identity"), "main_identity");
	const growthIntentRaw = parseNullableJson(requiredMeta(values, "growth_intent"), "growth_intent");
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
		mainIdentity: mainIdentityRaw === undefined ? undefined : parseExternalIdentity(mainIdentityRaw, "main_identity"),
		growthIntent: growthIntentRaw === undefined ? undefined : parseGrowthIntent(growthIntentRaw),
		profileDigest,
		profileDigestVersion,
		profileProjection: parseOptionalProjection(requiredMeta(values, "profile_projection")),
		profileTunablesRevision,
		profileApprovedAt: parseOptionalNonNegativeIntegerJson(requiredMeta(values, "profile_approved_at"), "profile_approved_at"),
		profileApprovalReceipt: parseOptionalStringJson(requiredMeta(values, "profile_approval_receipt"), "profile_approval_receipt"),
		failedClosedReason: parseOptionalStringJson(requiredMeta(values, "failed_closed_reason"), "failed_closed_reason"),
	};
}

function stableMetadataJson(value: unknown): string {
	return JSON.stringify(value);
}

function identityJson(identity: ExternalSessionIdentity): string {
	return stableMetadataJson(identity);
}

function canonicalJson(value: unknown): string {
	if (value === null || typeof value === "boolean" || typeof value === "number" || typeof value === "string") return JSON.stringify(value);
	if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
	if (isRecord(value)) {
		return `{${Object.keys(value)
			.sort()
			.map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
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
	return left.transcript.entryCount === right.transcript.entryCount && left.transcript.sha256 === right.transcript.sha256;
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

	commitBootstrap(expectedState: "CREATING" | "CREATED", intent: BootstrapIntent, identity: ExternalSessionIdentity, profile: WayProfile): void {
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
			],
			deletes: [],
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
		const intent: GrowthIntent = { base: identity, startedAt };
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				{ key: "main_identity", value: identityJson(identity) },
				{ key: "growth_intent", value: "null" },
			],
			puts: [{ key: "growth_intent", value: stableMetadataJson(intent) }],
			deletes: [],
		});
	}

	refreshAfterGrowth(intent: GrowthIntent, identity: ExternalSessionIdentity): void {
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				{ key: "growth_intent", value: stableMetadataJson(intent) },
			],
			puts: [
				{ key: "main_identity", value: identityJson(identity) },
				{ key: "growth_intent", value: "null" },
			],
			deletes: [],
		});
	}

	refreshRecoveredGrowth(intent: GrowthIntent, identity: ExternalSessionIdentity): void {
		this.refreshAfterGrowth(intent, identity);
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

	markFailedClosed(reason: string): void {
		if (!reason) throw new GatewayStateError("failure_reason_invalid", "A fail-closed reason is required.");
		const state = this.read();
		this.transact({
			expected: [{ key: "bootstrap_state", value: state.bootstrapState }],
			puts: [
				{ key: "bootstrap_state", value: "FAILED_CLOSED" },
				{ key: "failed_closed_reason", value: stableMetadataJson(reason) },
			],
			deletes: [],
		});
	}

	approveProfile(profile: WayProfile, receiptId: string, approvedAt: number): { readonly cursor?: string; readonly previousProjection: CanonicalValue | undefined } {
		const state = this.read();
		if (state.bootstrapState === "ABSENT" || state.bootstrapState === "CREATING" || state.bootstrapState === "CREATED") {
			throw new GatewayStateError("bootstrap_not_committed", "A profile cannot be approved before bootstrap is committed.");
		}
		if (state.bootstrapState === "FAILED_CLOSED" && state.failedClosedReason !== "profile_drift") {
			throw new GatewayStateError("failed_closed_not_profile_drift", "Only profile drift can be cleared by profile approval.");
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
