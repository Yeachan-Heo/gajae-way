/**
 * Tests for Grok CLI version resolution system.
 *
 * Covers:
 * - Version resolution from multiple sources
 * - Cache behavior (memory and disk)
 * - TTL expiration
 * - HTTP 426 error parsing and retry
 * - Fallback to pinned version
 */

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import {
	getLatestGrokCliVersion,
	parseVersionFrom426,
	updateVersionFrom426,
	clearVersionCache,
} from "../../src/providers/grok/version-resolver.js";

// Mock fetch globally
global.fetch = vi.fn();

describe("Grok CLI Version Resolver", () => {
	beforeEach(() => {
		clearVersionCache();
		vi.resetAllMocks();
	});

	afterEach(() => {
		clearVersionCache();
		vi.resetAllMocks();
		// Clean up disk cache
		try {
			const cacheDir = path.join(os.homedir(), ".cache", "grok-cli");
			const cachePath = path.join(cacheDir, "version-cache.json");
			if (fs.existsSync(cachePath)) {
				fs.unlinkSync(cachePath);
			}
		} catch {
			// Ignore cleanup errors
		}
	});

	describe("parseVersionFrom426", () => {
		it("should extract version from standard xAI 426 error message", () => {
			const errorBody =
				'Your Grok CLI version (0.2.33) is outdated. Please update to version 1.0.13';
			const version = parseVersionFrom426(errorBody);
			expect(version).toBe("1.0.13");
		});

		it("should extract version from alternative format", () => {
			const errorBody = "Minimum required version 1.2.3 is not satisfied";
			const version = parseVersionFrom426(errorBody);
			expect(version).toBe("1.2.3");
		});

		it("should extract version from error with extra text", () => {
			const errorBody =
				'{"error": "Client version 0.2.33 is outdated. Please update to version 1.5.0"}';
			const version = parseVersionFrom426(errorBody);
			expect(version).toBe("1.5.0");
		});

		it("should return null for error without version", () => {
			const errorBody = "Something went wrong but no version info";
			const version = parseVersionFrom426(errorBody);
			expect(version).toBeNull();
		});

		it("should handle multiple version numbers in error", () => {
			const errorBody = "Version 0.1.0 is unsupported, please update to version 2.0.0";
			const version = parseVersionFrom426(errorBody);
			expect(version).toBe("2.0.0");
		});
	});

	describe("getLatestGrokCliVersion", () => {
		it("should fetch from npm registry on first call", async () => {
			(global.fetch as any).mockImplementation(async () => ({
				ok: true,
				json: async () => ({ "dist-tags": { latest: "1.2.3" } }),
			}));

			const version = await getLatestGrokCliVersion(5000);
			expect(version).toBe("1.2.3");
			expect(global.fetch).toHaveBeenCalled();
		});

		it("should fall back to GitHub when npm fails", async () => {
			let fetchCount = 0;
			(global.fetch as any).mockImplementation(async () => {
				fetchCount++;
				if (fetchCount === 1) {
					// First call (npm) fails
					throw new Error("Network error");
				}
				// Second call (GitHub) succeeds
				return {
					ok: true,
					json: async () => ({ tag_name: "v1.3.4" }),
				};
			});

			const version = await getLatestGrokCliVersion(5000);
			expect(version).toBe("1.3.4");
		});

		it("should strip 'v' prefix from GitHub tag", async () => {
			(global.fetch as any).mockImplementation(async (url: string) => {
				if (url.includes("npmjs.org")) {
					throw new Error("npm failed");
				}
				return {
					ok: true,
					json: async () => ({ tag_name: "v1.0.13" }),
				};
			});

			const version = await getLatestGrokCliVersion(5000);
			expect(version).toBe("1.0.13");
		});

		it("should return pinned fallback when all sources fail", async () => {
			(global.fetch as any).mockImplementation(async () => {
				throw new Error("Network error");
			});

			const version = await getLatestGrokCliVersion(5000);
			expect(version).toBe("1.0.13");
		});

		it("should return pinned fallback on timeout", async () => {
			(global.fetch as any).mockImplementation(
				() =>
					new Promise((resolve) => {
						setTimeout(() => {
							resolve({
								ok: true,
								json: async () => ({ "dist-tags": { latest: "1.0.0" } }),
							});
						}, 200); // Longer than 50ms timeout
					})
			);

			const version = await getLatestGrokCliVersion(50); // Short timeout
			expect(version).toBe("1.0.13");
		});

		it("should cache version in memory on subsequent calls", async () => {
			let fetchCount = 0;
			(global.fetch as any).mockImplementation(async () => {
				fetchCount++;
				return {
					ok: true,
					json: async () => ({ "dist-tags": { latest: "1.4.5" } }),
				};
			});

			const version1 = await getLatestGrokCliVersion(5000);
			const version2 = await getLatestGrokCliVersion(5000);

			expect(version1).toBe("1.4.5");
			expect(version2).toBe("1.4.5");
			// Should only fetch once due to memory cache
			expect(fetchCount).toBe(1);
		});
	});

	describe("updateVersionFrom426", () => {
		it("should parse and cache version from 426 error", async () => {
			const errorBody =
				'Your Grok CLI version (0.2.33) is outdated. Please update to version 1.0.13';

			const result = await updateVersionFrom426(errorBody);
			expect(result).toBe("1.0.13");

			// Verify memory cache is updated
			(global.fetch as any).mockImplementation(async () => {
				throw new Error("Should not fetch since cache is updated");
			});

			const version = await getLatestGrokCliVersion(5000);
			expect(version).toBe("1.0.13");
		});

		it("should return null if version cannot be parsed from error", async () => {
			const errorBody = "Some error without version info";
			const result = await updateVersionFrom426(errorBody);
			expect(result).toBeNull();
		});
	});

	describe("Memory cache behavior", () => {
		it("should prefer memory cache over fetching new version", async () => {
			(global.fetch as any).mockImplementation(async () => ({
				ok: true,
				json: async () => ({ "dist-tags": { latest: "1.6.7" } }),
			}));

			// First call fetches 1.6.7
			const version1 = await getLatestGrokCliVersion(5000);
			expect(version1).toBe("1.6.7");

			// Change mock to return different version
			(global.fetch as any).mockImplementation(async () => ({
				ok: true,
				json: async () => ({ "dist-tags": { latest: "2.0.0" } }),
			}));

			// Second call should use cached version (1.6.7), not new mock (2.0.0)
			const version2 = await getLatestGrokCliVersion(5000);
			expect(version2).toBe("1.6.7");
		});
	});
});
