import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";

/**
 * Profile schema and digest classification (v1):
 *
 * Digest-bound identity/security fields:
 * - `[corpus].path` and `[corpus].workspace` identify the corpus authority.
 * - `[injection].files` is ordered; changing order changes the resumed persona.
 * - `[restricted_files]` is a per-session-kind deny policy.
 * - `[surfaces.owner]`, `[owner_surface]`, or `[owner_surfaces]` identify owner authority.
 * - `[operator]` (or `[identity]`) contains operator identity fields.
 *
 * Outside the digest (hot-reloadable tunables): `[tunables]`, `[poll]`, `[ack]`,
 * `[adapter]`, `[adapters]`, `[policy]`, and the top-level scalar aliases
 * `poll_interval_ms`, `ack_budget`, and `adapter_credentials`. Credentials are
 * deliberately never copied into the identity projection or approval diff.
 */

export const PROFILE_DIGEST_VERSION = 1;
export const SESSION_KINDS = ["main", "conversation", "lane", "job", "unknown"] as const;

export type SessionKind = (typeof SESSION_KINDS)[number];
type TomlRecord = Record<string, unknown>;
export interface CanonicalArray extends ReadonlyArray<CanonicalValue> {}
export interface CanonicalObject {
	readonly [key: string]: CanonicalValue;
}
export type CanonicalValue = null | boolean | number | string | CanonicalArray | CanonicalObject;

export class ProfileValidationError extends Error {
	readonly code: "parse_error" | "invalid_type" | "missing_field" | "unknown_field" | "invalid_value";
	readonly field: string;

	constructor(code: ProfileValidationError["code"], field: string, message: string) {
		super(`Invalid profile ${field}: ${message}`);
		this.name = "ProfileValidationError";
		this.code = code;
		this.field = field;
	}
}

export interface ProfileInjection {
	readonly files: readonly string[];
}

export interface OwnerSurface {
	readonly id: string;
	readonly platform: string;
	readonly kind: string;
}

export interface ProfileDigest {
	readonly version: number;
	readonly sha256: string;
	readonly canonical: string;
}

export interface ProfileIdentityProjection {
	readonly corpus: {
		readonly path: string;
		readonly workspace: string;
	};
	readonly injectionFiles: readonly string[];
	readonly restrictedFilePolicy: Readonly<Record<SessionKind, readonly string[]>>;
	readonly ownerSurfaceMapping: readonly OwnerSurface[];
	readonly operatorIdentity: Readonly<Record<string, CanonicalValue>>;
}

export interface WayProfile {
	readonly sourcePath: string;
	readonly corpusPath: string;
	readonly workspace: string;
	readonly injection: ProfileInjection;
	readonly restrictedFiles: Readonly<Record<SessionKind, readonly string[]>>;
	readonly ownerSurfaces: readonly OwnerSurface[];
	/** Compatibility convenience for the first owner surface; ownerSurfaces is authoritative. */
	readonly ownerSurface: OwnerSurface;
	/** All profile-known admissible surfaces. Owner surfaces are included. */
	readonly knownSurfaces: readonly OwnerSurface[];
	readonly operator: Readonly<Record<string, CanonicalValue>>;
	readonly tunables: Readonly<Record<string, CanonicalValue>>;
	readonly tunablesCanonical: string;
	readonly tunablesRevision: number;
	readonly projection: ProfileIdentityProjection;
	readonly digest: ProfileDigest;
}

export interface LoadProfileOptions {
	readonly tunablesRevision?: number;
}

function isRecord(value: unknown): value is TomlRecord {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function fail(code: ProfileValidationError["code"], field: string, message: string): never {
	throw new ProfileValidationError(code, field, message);
}

function requiredRecord(value: unknown, field: string): TomlRecord {
	if (!isRecord(value)) fail(value === undefined ? "missing_field" : "invalid_type", field, "must be a table.");
	return value;
}

function optionalRecord(value: unknown, field: string): TomlRecord | undefined {
	if (value === undefined) return undefined;
	if (!isRecord(value)) fail("invalid_type", field, "must be a table.");
	return value;
}

function requiredString(value: unknown, field: string): string {
	if (typeof value !== "string") fail(value === undefined ? "missing_field" : "invalid_type", field, "must be a non-empty string.");
	const trimmed = value.trim();
	if (!trimmed) fail("invalid_value", field, "must be a non-empty string.");
	return trimmed;
}

function stringArray(value: unknown, field: string): string[] {
	if (!Array.isArray(value)) fail(value === undefined ? "missing_field" : "invalid_type", field, "must be an array of strings.");
	const values = value.map((entry, index) => requiredString(entry, `${field}[${index}]`));
	if (new Set(values).size !== values.length) fail("invalid_value", field, "must not contain duplicate entries.");
	return values;
}

function canonicalPath(rawPath: string, baseDirectory: string, field: string): string {
	const resolved = path.resolve(baseDirectory, rawPath);
	try {
		return fs.realpathSync.native(resolved);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return resolved;
		fail("invalid_value", field, `could not resolve path: ${error instanceof Error ? error.message : String(error)}`);
	}
}

function relativePolicyPath(value: string, field: string): string {
	if (path.isAbsolute(value)) fail("invalid_value", field, "must be relative to the corpus path.");
	const normalized = value.replaceAll("\\", "/");
	if (normalized.split("/").includes("..")) fail("invalid_value", field, "must not traverse above the corpus path.");
	return normalized;
}

function canonicalValue(value: unknown, field: string): CanonicalValue {
	if (value === null || typeof value === "boolean" || typeof value === "string") return value;
	if (typeof value === "number") {
		if (!Number.isFinite(value)) fail("invalid_value", field, "must be finite.");
		return value;
	}
	if (Array.isArray(value)) return value.map((entry, index) => canonicalValue(entry, `${field}[${index}]`));
	if (isRecord(value)) {
		const output: Record<string, CanonicalValue> = {};
		for (const key of Object.keys(value).sort()) output[key] = canonicalValue(value[key], `${field}.${key}`);
		return output;
	}
	fail("invalid_type", field, "must contain only TOML scalar, array, or table values.");
}

/** Stable JSON serialization: object keys sort lexically while array order remains semantic. */
export function canonicalSerialize(value: CanonicalValue): string {
	if (value === null) return "null";
	if (typeof value === "boolean") return value ? "true" : "false";
	if (typeof value === "number") return JSON.stringify(value);
	if (typeof value === "string") return JSON.stringify(value);
	if (Array.isArray(value)) return `[${value.map(canonicalSerialize).join(",")}]`;
	const record = value as CanonicalObject;
	return `{${Object.keys(record)
		.sort()
		.map(key => `${JSON.stringify(key)}:${canonicalSerialize(record[key] as CanonicalValue)}`)
		.join(",")}}`;
}

function noUnknownKeys(record: TomlRecord, allowed: readonly string[], field: string): void {
	for (const key of Object.keys(record)) {
		if (!allowed.includes(key)) fail("unknown_field", `${field}.${key}`, "is not a supported profile field.");
	}
}

function normalizeOwnerSurface(value: unknown, field: string): OwnerSurface {
	const record = requiredRecord(value, field);
	noUnknownKeys(record, ["id", "platform", "kind"], field);
	return {
		id: requiredString(record.id, `${field}.id`),
		platform: requiredString(record.platform, `${field}.platform`),
		kind: requiredString(record.kind, `${field}.kind`),
	};
}

function ownerSurfaces(document: TomlRecord): OwnerSurface[] {
	const sources: Array<{ value: unknown; field: string }> = [];
	if (document.owner_surface !== undefined) sources.push({ value: document.owner_surface, field: "owner_surface" });
	const surfaces = optionalRecord(document.surfaces, "surfaces");
	if (surfaces) noUnknownKeys(surfaces, ["owner", "known"], "surfaces");
	if (surfaces?.owner !== undefined) {
		sources.push({ value: surfaces.owner, field: "surfaces.owner" });
	}
	if (document.owner_surfaces !== undefined) {
		if (Array.isArray(document.owner_surfaces)) {
			document.owner_surfaces.forEach((value, index) => sources.push({ value, field: `owner_surfaces[${index}]` }));
		} else if (isRecord(document.owner_surfaces)) {
			for (const key of Object.keys(document.owner_surfaces).sort()) {
				const candidate = requiredRecord(document.owner_surfaces[key], `owner_surfaces.${key}`);
				sources.push({
					value: { id: candidate.id ?? key, platform: candidate.platform, kind: candidate.kind },
					field: `owner_surfaces.${key}`,
				});
			}
		} else {
			fail("invalid_type", "owner_surfaces", "must be an array or table of owner surfaces.");
		}
	}
	if (sources.length === 0) fail("missing_field", "surfaces.owner", "must define at least one owner surface.");
	const normalized = sources.map(source => normalizeOwnerSurface(source.value, source.field));
	const ids = new Set<string>();
	for (const surface of normalized) {
		if (ids.has(surface.id)) fail("invalid_value", "owner_surfaces", `contains duplicate owner surface id ${surface.id}.`);
		ids.add(surface.id);
	}
	return normalized.sort((left, right) => left.id.localeCompare(right.id));
}

/**
 * Non-owner routes are declared as `[[surfaces.known]]`; owner routes remain
 * authoritative from the digest-bound owner mapping above.
 */
function knownSurfaces(document: TomlRecord, owners: readonly OwnerSurface[]): OwnerSurface[] {
	const surfaces = optionalRecord(document.surfaces, "surfaces");
	const known = surfaces?.known;
	if (known === undefined) return [...owners];
	if (!Array.isArray(known)) fail("invalid_type", "surfaces.known", "must be an array of surface tables.");
	const result = [...owners];
	const byId = new Map(result.map(surface => [surface.id, surface]));
	for (const [index, value] of known.entries()) {
		const surface = normalizeOwnerSurface(value, `surfaces.known[${index}]`);
		const existing = byId.get(surface.id);
		if (existing) {
			if (existing.platform !== surface.platform || existing.kind !== surface.kind) {
				fail("invalid_value", `surfaces.known[${index}]`, `conflicts with configured owner surface ${surface.id}.`);
			}
			continue;
		}
		byId.set(surface.id, surface);
		result.push(surface);
	}
	return result.sort((left, right) => left.id.localeCompare(right.id));
}

function parseRestrictedFiles(document: TomlRecord): Readonly<Record<SessionKind, readonly string[]>> {
	const security = optionalRecord(document.security, "security");
	if (security) noUnknownKeys(security, ["restricted_files"], "security");
	if (document.restricted_files !== undefined && security?.restricted_files !== undefined) {
		fail("invalid_value", "restricted_files", "must not be defined both at the top level and under security.");
	}
	const rawPolicy = document.restricted_files ?? security?.restricted_files;
	const policy = rawPolicy === undefined ? {} : requiredRecord(rawPolicy, "restricted_files");
	noUnknownKeys(policy, SESSION_KINDS, "restricted_files");
	const normalized = {} as Record<SessionKind, readonly string[]>;
	for (const kind of SESSION_KINDS) {
		const values = policy[kind] === undefined ? [] : stringArray(policy[kind], `restricted_files.${kind}`);
		normalized[kind] = values.map((value, index) => relativePolicyPath(value, `restricted_files.${kind}[${index}]`));
	}
	return normalized;
}

function parseTunables(document: TomlRecord): Readonly<Record<string, CanonicalValue>> {
	const tunableKeys = ["tunables", "poll", "ack", "adapter", "adapters", "policy", "poll_interval_ms", "ack_budget", "adapter_credentials"] as const;
	const output: Record<string, CanonicalValue> = {};
	for (const key of tunableKeys) {
		if (document[key] !== undefined) output[key] = canonicalValue(document[key], key);
	}
	return output;
}

export function profileProjectionCanonical(projection: ProfileIdentityProjection): string {
	return canonicalSerialize({
		corpus: { path: projection.corpus.path, workspace: projection.corpus.workspace },
		injection_files: [...projection.injectionFiles],
		restricted_file_policy: Object.fromEntries(
			SESSION_KINDS.map(kind => [kind, [...projection.restrictedFilePolicy[kind]]]),
		) as Record<string, CanonicalValue>,
		owner_surface_mapping: projection.ownerSurfaceMapping.map(surface => ({
			id: surface.id,
			kind: surface.kind,
			platform: surface.platform,
		})),
		operator_identity: projection.operatorIdentity,
	} as CanonicalValue);
}

export function digestProfileProjection(projection: ProfileIdentityProjection): ProfileDigest {
	const canonical = canonicalSerialize({
		profile_digest_version: PROFILE_DIGEST_VERSION,
		projection: JSON.parse(profileProjectionCanonical(projection)) as CanonicalValue,
	});
	return {
		version: PROFILE_DIGEST_VERSION,
		canonical,
		sha256: crypto.createHash("sha256").update(canonical).digest("hex"),
	};
}

/** Parses, validates, normalizes, and digest-binds one TOML deployment profile. */
export function loadWayProfile(profilePath: string, options: LoadProfileOptions = {}): WayProfile {
	const sourcePath = path.resolve(profilePath);
	let document: TomlRecord;
	try {
		const parsed = (Bun.TOML as { parse(source: string): unknown }).parse(fs.readFileSync(sourcePath, "utf8"));
		document = requiredRecord(parsed, "root");
	} catch (error) {
		if (error instanceof ProfileValidationError) throw error;
		throw new ProfileValidationError("parse_error", sourcePath, error instanceof Error ? error.message : String(error));
	}

	noUnknownKeys(
		document,
		[
			"corpus",
			"injection",
			"restricted_files",
			"security",
			"surfaces",
			"owner_surface",
			"owner_surfaces",
			"operator",
			"identity",
			"tunables",
			"poll",
			"ack",
			"adapter",
			"adapters",
			"policy",
			"poll_interval_ms",
			"ack_budget",
			"adapter_credentials",
		],
		"root",
	);
	const corpus = requiredRecord(document.corpus, "corpus");
	noUnknownKeys(corpus, ["path", "workspace"], "corpus");
	const baseDirectory = path.dirname(sourcePath);
	const corpusPath = canonicalPath(requiredString(corpus.path, "corpus.path"), baseDirectory, "corpus.path");
	const workspace = canonicalPath(requiredString(corpus.workspace, "corpus.workspace"), baseDirectory, "corpus.workspace");
	const injection = requiredRecord(document.injection, "injection");
	noUnknownKeys(injection, ["files"], "injection");
	const files = stringArray(injection.files, "injection.files").map((file, index) => relativePolicyPath(file, `injection.files[${index}]`));
	const restrictedFiles = parseRestrictedFiles(document);
	if (document.operator !== undefined && document.identity !== undefined) {
		fail("invalid_value", "operator", "must not be defined together with identity.");
	}
	const rawOperator = document.operator ?? document.identity ?? {};
	const operator = canonicalValue(requiredRecord(rawOperator, "operator"), "operator") as Readonly<Record<string, CanonicalValue>>;
	const normalizedOwnerSurfaces = ownerSurfaces(document);
	const normalizedKnownSurfaces = knownSurfaces(document, normalizedOwnerSurfaces);
	const projection: ProfileIdentityProjection = {
		corpus: { path: corpusPath, workspace },
		injectionFiles: files,
		restrictedFilePolicy: restrictedFiles,
		ownerSurfaceMapping: normalizedOwnerSurfaces,
		operatorIdentity: operator,
	};
	const tunables = parseTunables(document);
	return {
		sourcePath,
		corpusPath,
		workspace,
		injection: { files },
		restrictedFiles,
		ownerSurfaces: normalizedOwnerSurfaces,
		ownerSurface: normalizedOwnerSurfaces[0] as OwnerSurface,
		knownSurfaces: normalizedKnownSurfaces,
		operator,
		tunables,
		tunablesCanonical: canonicalSerialize(tunables),
		tunablesRevision: options.tunablesRevision ?? 0,
		projection,
		digest: digestProfileProjection(projection),
	};
}

/** Keeps mutable tuning changes observable without re-binding the session identity digest. */
export class ProfileRevisionTracker {
	#tunablesCanonical: string | undefined;
	#revision = 0;

	load(profilePath: string): WayProfile {
		const loaded = loadWayProfile(profilePath);
		if (loaded.tunablesCanonical !== this.#tunablesCanonical) {
			this.#tunablesCanonical = loaded.tunablesCanonical;
			this.#revision += 1;
		}
		return { ...loaded, tunablesRevision: this.#revision };
	}

	get revision(): number {
		return this.#revision;
	}
}

export interface ProjectionDiff {
	readonly path: string;
	readonly before: CanonicalValue | undefined;
	readonly after: CanonicalValue | undefined;
}

/** Produces a deterministic, secret-free identity/security projection diff for approval receipts. */
export function diffProfileProjections(
	before: CanonicalValue | undefined,
	after: CanonicalValue | undefined,
	pathPrefix = "projection",
): ProjectionDiff[] {
	if (canonicalSerialize(before ?? null) === canonicalSerialize(after ?? null)) return [];
	if (isRecord(before) && isRecord(after)) {
		const changes: ProjectionDiff[] = [];
		for (const key of [...new Set([...Object.keys(before), ...Object.keys(after)])].sort()) {
			changes.push(...diffProfileProjections(before[key] as CanonicalValue | undefined, after[key] as CanonicalValue | undefined, `${pathPrefix}.${key}`));
		}
		return changes;
	}
	return [{ path: pathPrefix, before, after }];
}