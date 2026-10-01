/**
 * Grok CLI version resolution system.
 *
 * Resolves the latest Grok CLI version at runtime from official sources
 * (npm registry, GitHub releases), with in-memory and on-disk caching
 * including TTL. Falls back to a pinned recent version when offline.
 *
 * On HTTP 426 from xAI, parses the required minimum version from the error
 * and updates the cache accordingly.
 */

import * as fs from "fs";
import * as path from "path";
import * as os from "os";

/** Pinned fallback version when resolution fails offline. */
const PINNED_FALLBACK_VERSION = "1.0.13";

/** Cache TTL in milliseconds (24 hours). */
const CACHE_TTL_MS = 24 * 60 * 60 * 1000;

/** In-memory cache: { version, expiresAt } */
let memoryCache: { version: string; expiresAt: number } | null = null;

/**
 * Get the cache directory for storing the version file.
 * Uses .cache in user's home directory, or /tmp as fallback.
 */
function getCacheDir(): string {
	const home = os.homedir();
	const cacheDir = path.join(home, ".cache", "grok-cli");

	try {
		if (!fs.existsSync(cacheDir)) {
			fs.mkdirSync(cacheDir, { recursive: true, mode: 0o700 });
		}
		return cacheDir;
	} catch {
		// Fall back to /tmp if .cache is not writable
		return path.join(os.tmpdir(), "grok-cli-cache");
	}
}

/**
 * Get the path to the cached version file.
 */
function getCachePath(): string {
	return path.join(getCacheDir(), "version-cache.json");
}

/**
 * Read the cached version from disk if it exists and hasn't expired.
 */
function readCachedVersion(): string | null {
	try {
		const cachePath = getCachePath();
		if (!fs.existsSync(cachePath)) return null;

		const data = fs.readFileSync(cachePath, "utf8");
		const cached = JSON.parse(data) as { version: string; expiresAt: number };

		if (Date.now() < cached.expiresAt) {
			return cached.version;
		}
	} catch {
		// Ignore cache read errors
	}
	return null;
}

/**
 * Write the version to disk cache.
 */
function writeCachedVersion(version: string): void {
	try {
		const cachePath = getCachePath();
		const cacheDir = path.dirname(cachePath);

		if (!fs.existsSync(cacheDir)) {
			fs.mkdirSync(cacheDir, { recursive: true, mode: 0o700 });
		}

		const data = { version, expiresAt: Date.now() + CACHE_TTL_MS };
		fs.writeFileSync(cachePath, JSON.stringify(data), { mode: 0o600 });
	} catch {
		// Ignore cache write errors; this is best-effort
	}
}

/**
 * Fetch the latest version from npm registry.
 */
async function fetchFromNpm(packageName: string = "@xai-sdk/grok-cli"): Promise<string | null> {
	try {
		const response = await fetch(`https://registry.npmjs.org/${packageName}`);
		if (!response.ok) return null;

		const data = (await response.json()) as { "dist-tags"?: { latest?: string } };
		return data["dist-tags"]?.latest ?? null;
	} catch {
		return null;
	}
}

/**
 * Fetch the latest version from GitHub releases.
 */
async function fetchFromGithub(
	owner: string = "xai-org",
	repo: string = "grok-cli"
): Promise<string | null> {
	try {
		const response = await fetch(`https://api.github.com/repos/${owner}/${repo}/releases/latest`);
		if (!response.ok) return null;

		const data = (await response.json()) as { tag_name?: string };
		const tag = data.tag_name;
		if (!tag) return null;

		// Strip 'v' prefix if present
		return tag.startsWith("v") ? tag.slice(1) : tag;
	} catch {
		return null;
	}
}

/**
 * Parse the required version from an HTTP 426 error body.
 * Looks for patterns like "update to version X.Y.Z" or "version X.Y.Z is required".
 * Prefers explicit "update to version" or "required version" patterns over raw versions.
 */
export function parseVersionFrom426(errorBody: string): string | null {
	// Try pattern: "Please update to version X.Y.Z" (most explicit)
	const match1 = errorBody.match(/update to version\s+(\d+\.\d+\.\d+)/i);
	if (match1?.[1]) return match1[1];

	// Try pattern: "version X.Y.Z is required" or "requires version X.Y.Z"
	const match2 = errorBody.match(/(?:requires?|required) (?:version |v)?(\d+\.\d+\.\d+)/i);
	if (match2?.[1]) return match2[1];

	// Try pattern: "version X.Y.Z is"
	const match3 = errorBody.match(/version\s+(\d+\.\d+\.\d+)\s+is/i);
	if (match3?.[1]) return match3[1];

	// Last resort: find any semantic version
	const match4 = errorBody.match(/(\d+\.\d+\.\d+)/);
	if (match4?.[1]) return match4[1];

	return null;
}

/**
 * Get the latest Grok CLI version, with fallback logic.
 *
 * 1. Check memory cache (fast path)
 * 2. Check disk cache (if not expired)
 * 3. Attempt to fetch from npm registry
 * 4. Attempt to fetch from GitHub releases
 * 5. Fall back to pinned version
 */
export async function getLatestGrokCliVersion(timeoutMs: number = 5000): Promise<string> {
	// Check memory cache
	if (memoryCache && Date.now() < memoryCache.expiresAt) {
		return memoryCache.version;
	}

	// Check disk cache
	const cachedVersion = readCachedVersion();
	if (cachedVersion) {
		memoryCache = { version: cachedVersion, expiresAt: Date.now() + CACHE_TTL_MS };
		return cachedVersion;
	}

	// Try to fetch fresh version (with timeout)
	try {
		const timeoutPromise = new Promise<null>((resolve) => {
			setTimeout(() => resolve(null), timeoutMs);
		});

		// Try npm first (more reliable for CLI tools)
		const npmVersionPromise = fetchFromNpm().catch(() => null);
		const npmVersion = await Promise.race([npmVersionPromise, timeoutPromise]);
		if (npmVersion) {
			memoryCache = { version: npmVersion, expiresAt: Date.now() + CACHE_TTL_MS };
			writeCachedVersion(npmVersion);
			return npmVersion;
		}

		// Try GitHub as fallback
		const githubVersionPromise = fetchFromGithub().catch(() => null);
		const githubVersion = await Promise.race([githubVersionPromise, timeoutPromise]);
		if (githubVersion) {
			memoryCache = { version: githubVersion, expiresAt: Date.now() + CACHE_TTL_MS };
			writeCachedVersion(githubVersion);
			return githubVersion;
		}
	} catch {
		// Fetch failed
	}

	// Fall back to pinned version
	memoryCache = { version: PINNED_FALLBACK_VERSION, expiresAt: Date.now() + CACHE_TTL_MS };
	return PINNED_FALLBACK_VERSION;
}

/**
 * Update the cached version based on an HTTP 426 error response.
 * Parses the required version from the error and updates both caches.
 */
export async function updateVersionFrom426(errorBody: string): Promise<string | null> {
	const requiredVersion = parseVersionFrom426(errorBody);
	if (!requiredVersion) return null;

	// Update both caches
	memoryCache = { version: requiredVersion, expiresAt: Date.now() + CACHE_TTL_MS };
	writeCachedVersion(requiredVersion);

	return requiredVersion;
}

/**
 * Clear all caches (useful for testing).
 */
export function clearVersionCache(): void {
	memoryCache = null;
	try {
		const cachePath = getCachePath();
		if (fs.existsSync(cachePath)) {
			fs.unlinkSync(cachePath);
		}
	} catch {
		// Ignore errors
	}
}
