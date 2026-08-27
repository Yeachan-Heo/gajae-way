/**
 * gajaeway-owned protocol profile versioning (plan §1 driver 1, ARCH-006).
 *
 * The wire `profileVersion` is distinct from any package semver. Pre-1.0 the
 * profile is explicitly unstable: breaking changes require a new profile
 * version and produce a typed incompatibility, never silent divergence.
 */

/** Current wire profile version served by this gateway build. */
export const PROFILE_VERSION = "1.0" as const;

/** Inclusive range of profile versions this build can serve. */
export const SUPPORTED_PROFILE_VERSIONS: readonly string[] = ["0.1", "1.0"];

/** Capability identifiers advertised at negotiation. Grows additively per phase. */
export const CAPABILITIES = [
	/** Core gateway lifecycle verbs: gateway.status, gateway.shutdown. */
	"gateway.core",
	/** Loopback chat verbs: chat.send + chat.* events (P0). */
	"chat.loopback",
	/** Emoji reactions in both directions: chat.react + engagement.reaction. */
	"chat.reactions",
] as const;

export type Capability = (typeof CAPABILITIES)[number];

export interface HelloPayload {
	/** Profile versions the client can speak, e.g. ["0.1"]. */
	readonly supportedVersions: readonly string[];
	/** Capabilities the client requires; missing ones cause typed rejection. */
	readonly requiredCapabilities?: readonly string[];
	/** Free-form client identity for diagnostics (never authority). */
	readonly clientInfo?: { readonly name?: string; readonly version?: string };
}

export interface NegotiatedPayload {
	readonly profileVersion: string;
	readonly capabilities: readonly string[];
}

export type NegotiationResult =
	| { readonly ok: true; readonly negotiated: NegotiatedPayload }
	| {
			readonly ok: false;
			readonly code: "incompatible_profile_version" | "missing_required_capability";
			readonly detail: string;
			readonly supportedVersions: readonly string[];
			readonly capabilities: readonly string[];
	  };

/**
 * Pure negotiation: pick the highest mutually supported version and verify
 * required capabilities. Unknown *optional* client fields are ignored by
 * contract; unknown *required* capabilities reject with a typed error.
 */
export function negotiate(
	hello: HelloPayload,
	serverVersions: readonly string[] = SUPPORTED_PROFILE_VERSIONS,
	serverCapabilities: readonly string[] = CAPABILITIES,
): NegotiationResult {
	const mutual = serverVersions.filter((v) => hello.supportedVersions.includes(v));
	if (mutual.length === 0) {
		return {
			ok: false,
			code: "incompatible_profile_version",
			detail: `client supports [${hello.supportedVersions.join(", ")}], server supports [${serverVersions.join(", ")}]`,
			supportedVersions: serverVersions,
			capabilities: serverCapabilities,
		};
	}
	const missing = (hello.requiredCapabilities ?? []).filter((c) => !serverCapabilities.includes(c));
	if (missing.length > 0) {
		return {
			ok: false,
			code: "missing_required_capability",
			detail: `server lacks required capabilities: ${missing.join(", ")}`,
			supportedVersions: serverVersions,
			capabilities: serverCapabilities,
		};
	}
	// Highest mutual version by numeric segment comparison.
	const best = [...mutual].sort(compareProfileVersions).at(-1) as string;
	return { ok: true, negotiated: { profileVersion: best, capabilities: serverCapabilities } };
}

export function compareProfileVersions(a: string, b: string): number {
	const pa = a.split(".").map(Number);
	const pb = b.split(".").map(Number);
	for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
		const d = (pa[i] ?? 0) - (pb[i] ?? 0);
		if (d !== 0) return d;
	}
	return 0;
}
