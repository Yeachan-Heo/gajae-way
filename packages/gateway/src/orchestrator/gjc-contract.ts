/**
 * The gjc error/version contract this gateway classifies against, in one place.
 *
 * Production uses a private exact-version runtime selected by the gateway
 * package pin; attach-only clients can observe a newer broker. Every
 * classification the gateway makes on a gjc envelope is only as good as the release it was
 * verified against: gjc 0.18 collapsed its internal error codes into a small
 * public set (`session_unavailable` became `endpoint_stale`,
 * `terminal_uncertain` became `operation_failed`), and code that still matched
 * the old names silently stopped recovering (gaebal-gajae, 2026-09-30).
 */

/**
 * Oldest gjc the gateway supports: 0.18.9 is the first release that honors the
 * attach-only contract (`GJC_SDK_BROKER_AUTOSTART=0`) and ships the public foreground
 * broker command the broker host supervises.
 */
export const MIN_GJC_VERSION = "0.18.9";

/**
 * Newest gjc minor whose envelopes this build's classification was verified
 * against (`packages/gateway/test/gjc-contract.e2e.test.ts`). A newer minor
 * still runs, but `ops cycle` gates on `gjc_unverified_version` until the
 * contract test passes on it and this constant is raised.
 */
export const VERIFIED_GJC_THROUGH = "0.19";

function versionParts(version: string): [number, number, number] | undefined {
	const match = /(\d+)\.(\d+)\.(\d+)/.exec(version);
	return match ? [Number(match[1]), Number(match[2]), Number(match[3])] : undefined;
}

/** True when `version` is at most the verified minor (any patch). Unparseable versions are unverified. */
export function isVerifiedGjcVersion(version: string, verifiedThrough = VERIFIED_GJC_THROUGH): boolean {
	const parts = versionParts(version);
	const ceiling = /^(\d+)\.(\d+)$/.exec(verifiedThrough);
	if (!parts || !ceiling) return false;
	const [major, minor] = parts;
	const [maxMajor, maxMinor] = [Number(ceiling[1]), Number(ceiling[2])];
	return major < maxMajor || (major === maxMajor && minor <= maxMinor);
}

/**
 * Codes meaning "the broker no longer serves this session id". gjc <= 0.17
 * reports `session_unavailable` on the CLI; the relay and gjc 0.18's public
 * contract report `endpoint_stale`; some verbs answer `not_found`.
 */
const SESSION_GONE_CODES: ReadonlySet<string> = new Set(["session_unavailable", "endpoint_stale", "not_found"]);

export function isSessionGoneCode(code: unknown): boolean {
	return typeof code === "string" && SESSION_GONE_CODES.has(code);
}
