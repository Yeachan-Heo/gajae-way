import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
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

export interface SessionFingerprint {
	readonly canonicalPath: string;
	readonly sessionId: string;
	readonly device: string;
	readonly inode: string;
	readonly nlink: string;
	readonly size: number;
	/** Millisecond integer representation required by SessionManager.openExistingStrict. */
	readonly mtimeMs: number;
	readonly mtimeNs: string;
	readonly ctimeNs: string;
	readonly sha256: string;
}

export interface BootstrapIntent {
	readonly nonce: string;
	readonly ts: number;
}

export interface GrowthIntent {
	readonly base: SessionFingerprint;
	readonly startedAt: number;
}

export interface DurableGatewayState {
	readonly bootstrapState: BootstrapState;
	readonly bootstrapIntent: BootstrapIntent | undefined;
	readonly mainIdentity: SessionFingerprint | undefined;
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

export class SessionFingerprintError extends GatewayStateError {
	constructor(reason: string, message = reason) {
		super(reason, message);
		this.name = "SessionFingerprintError";
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

function parseFingerprint(value: unknown, key: string): SessionFingerprint {
	if (!isRecord(value)) throw new GatewayStateError("metadata_invalid", `${key} must be an object.`);
	const fingerprint: SessionFingerprint = {
		canonicalPath: requiredString(value.canonicalPath, `${key}.canonicalPath`),
		sessionId: requiredString(value.sessionId, `${key}.sessionId`),
		device: requiredString(value.device, `${key}.device`),
		inode: requiredString(value.inode, `${key}.inode`),
		nlink: requiredString(value.nlink, `${key}.nlink`),
		size: requiredNonNegativeInteger(value.size, `${key}.size`),
		mtimeMs: requiredNonNegativeInteger(value.mtimeMs, `${key}.mtimeMs`),


		mtimeNs: requiredString(value.mtimeNs, `${key}.mtimeNs`),
		ctimeNs: requiredString(value.ctimeNs, `${key}.ctimeNs`),
		sha256: requiredString(value.sha256, `${key}.sha256`),
	};
	if (!/^[a-f0-9]{64}$/i.test(fingerprint.sha256)) {
		throw new GatewayStateError("metadata_invalid", `${key}.sha256 must be a SHA-256 hex digest.`);
	}
	return fingerprint;
}

function parseBootstrapIntent(value: unknown): BootstrapIntent {
	if (!isRecord(value)) throw new GatewayStateError("metadata_invalid", "bootstrap_intent must be an object.");
	return { nonce: requiredString(value.nonce, "bootstrap_intent.nonce"), ts: requiredNonNegativeInteger(value.ts, "bootstrap_intent.ts") };
}

function parseGrowthIntent(value: unknown): GrowthIntent {
	if (!isRecord(value)) throw new GatewayStateError("metadata_invalid", "growth_intent must be an object.");
	return {
		base: parseFingerprint(value.base, "growth_intent.base"),
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
		mainIdentity: mainIdentityRaw === undefined ? undefined : parseFingerprint(mainIdentityRaw, "main_identity"),
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

function fingerprintJson(fingerprint: SessionFingerprint): string {
	return stableMetadataJson(fingerprint);
}

export function sameIdentityFields(left: SessionFingerprint, right: SessionFingerprint): boolean {
	return (
		left.canonicalPath === right.canonicalPath &&
		left.sessionId === right.sessionId &&
		left.device === right.device &&
		left.inode === right.inode &&
		left.nlink === right.nlink
	);
}

export function sameFingerprint(left: SessionFingerprint, right: SessionFingerprint): boolean {
	return (
		sameIdentityFields(left, right) &&
		left.size === right.size &&
		left.mtimeMs === right.mtimeMs &&
		left.mtimeNs === right.mtimeNs &&
		left.ctimeNs === right.ctimeNs &&
		left.sha256 === right.sha256
	);
}

function bigintStatValue(stat: fs.BigIntStats, field: keyof fs.BigIntStats): bigint {
	const value = stat[field];
	if (typeof value !== "bigint") throw new SessionFingerprintError("stat_invalid", `${String(field)} is not a bigint.`);
	return value;
}

function sessionIdFromTranscript(bytes: Buffer): string {
	const newline = bytes.indexOf(0x0a);
	const headerText = bytes.subarray(0, newline === -1 ? bytes.length : newline).toString("utf8");
	let header: unknown;
	try {
		header = JSON.parse(headerText) as unknown;
	} catch {
		throw new SessionFingerprintError("transcript_malformed", "Session transcript header is not JSON.");
	}
	if (!isRecord(header) || header.type !== "session") {
		throw new SessionFingerprintError("transcript_malformed", "Session transcript is missing its session header.");
	}
	return requiredString(header.id, "session.id");
}

function sameStat(before: fs.BigIntStats, after: fs.BigIntStats): boolean {
	return (
		bigintStatValue(before, "dev") === bigintStatValue(after, "dev") &&
		bigintStatValue(before, "ino") === bigintStatValue(after, "ino") &&
		bigintStatValue(before, "nlink") === bigintStatValue(after, "nlink") &&
		bigintStatValue(before, "size") === bigintStatValue(after, "size") &&
		bigintStatValue(before, "mtimeNs") === bigintStatValue(after, "mtimeNs") &&
		bigintStatValue(before, "ctimeNs") === bigintStatValue(after, "ctimeNs")
	);
}

/** Captures a stable, full transcript fingerprint without trusting a path alias. */
export function fingerprintSessionFile(sessionPath: string): SessionFingerprint {
	let canonicalPath: string;
	try {
		canonicalPath = fs.realpathSync.native(sessionPath);
	} catch (error) {
		throw new SessionFingerprintError("session_missing", error instanceof Error ? error.message : String(error));
	}
	const noFollow = fs.constants.O_NOFOLLOW ?? 0;
	let fileDescriptor: number;
	try {
		fileDescriptor = fs.openSync(canonicalPath, fs.constants.O_RDONLY | noFollow);
	} catch (error) {
		throw new SessionFingerprintError("session_open_failed", error instanceof Error ? error.message : String(error));
	}
	try {
		const before = fs.fstatSync(fileDescriptor, { bigint: true });
		if (!before.isFile()) throw new SessionFingerprintError("session_not_regular", "Session transcript is not a regular file.");
		const bytes = fs.readFileSync(fileDescriptor);
		const after = fs.fstatSync(fileDescriptor, { bigint: true });
		if (!sameStat(before, after)) throw new SessionFingerprintError("session_unstable", "Session transcript changed while it was fingerprinted.");
		const size = bigintStatValue(before, "size");
		if (size > BigInt(Number.MAX_SAFE_INTEGER)) throw new SessionFingerprintError("session_too_large", "Session transcript exceeds safe size bounds.");
		return {
			canonicalPath,
			sessionId: sessionIdFromTranscript(bytes),
			device: bigintStatValue(before, "dev").toString(),
			inode: bigintStatValue(before, "ino").toString(),
			nlink: bigintStatValue(before, "nlink").toString(),
			size: Number(size),
			mtimeMs: Number(bigintStatValue(before, "mtimeMs")),
			mtimeNs: bigintStatValue(before, "mtimeNs").toString(),
			ctimeNs: bigintStatValue(before, "ctimeNs").toString(),
			sha256: crypto.createHash("sha256").update(bytes).digest("hex"),
		};
	} finally {
		fs.closeSync(fileDescriptor);
	}
}

/** Proves that an unrefreshed transcript can only have grown by appending. */
export function attestAppendOnlyGrowth(base: SessionFingerprint, current: SessionFingerprint): boolean {
	if (!sameIdentityFields(base, current) || current.size < base.size) return false;
	const noFollow = fs.constants.O_NOFOLLOW ?? 0;
	let descriptor: number;
	try {
		descriptor = fs.openSync(current.canonicalPath, fs.constants.O_RDONLY | noFollow);
	} catch {
		return false;
	}
	try {
		const before = fs.fstatSync(descriptor, { bigint: true });
		if (
			bigintStatValue(before, "dev").toString() !== base.device ||
			bigintStatValue(before, "ino").toString() !== base.inode ||
			bigintStatValue(before, "nlink").toString() !== base.nlink ||
			bigintStatValue(before, "size") < BigInt(base.size)
		) {
			return false;
		}
		const hash = crypto.createHash("sha256");
		const buffer = Buffer.allocUnsafe(Math.min(64 * 1024, Math.max(base.size, 1)));
		let offset = 0;
		while (offset < base.size) {
			const wanted = Math.min(buffer.length, base.size - offset);
			const read = fs.readSync(descriptor, buffer, 0, wanted, offset);
			if (read <= 0) return false;
			hash.update(buffer.subarray(0, read));
			offset += read;
		}
		const after = fs.fstatSync(descriptor, { bigint: true });
		if (!sameStat(before, after)) return false;
		return hash.digest("hex") === base.sha256;
	} catch {
		return false;
	} finally {
		fs.closeSync(descriptor);
	}
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

	commitBootstrap(expectedState: "CREATING" | "CREATED", intent: BootstrapIntent, identity: SessionFingerprint, profile: WayProfile): void {
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: expectedState },
				{ key: "bootstrap_intent", value: stableMetadataJson(intent) },
			],
			puts: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				{ key: "bootstrap_intent", value: "null" },
				{ key: "main_identity", value: fingerprintJson(identity) },
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

	writeGrowthIntent(identity: SessionFingerprint, startedAt: number): void {
		const intent: GrowthIntent = { base: identity, startedAt };
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				{ key: "main_identity", value: fingerprintJson(identity) },
				{ key: "growth_intent", value: "null" },
			],
			puts: [{ key: "growth_intent", value: stableMetadataJson(intent) }],
			deletes: [],
		});
	}

	refreshAfterGrowth(intent: GrowthIntent, identity: SessionFingerprint): void {
		this.transact({
			expected: [
				{ key: "bootstrap_state", value: "COMMITTED" },
				{ key: "growth_intent", value: stableMetadataJson(intent) },
			],
			puts: [
				{ key: "main_identity", value: fingerprintJson(identity) },
				{ key: "growth_intent", value: "null" },
			],
			deletes: [],
		});
	}

	refreshRecoveredGrowth(intent: GrowthIntent, identity: SessionFingerprint): void {
		this.refreshAfterGrowth(intent, identity);
	}

	setTunablesRevision(revision: number): void {
		if (!Number.isSafeInteger(revision) || revision < 0) throw new GatewayStateError("revision_invalid", "Profile tunables revision is invalid.");
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
