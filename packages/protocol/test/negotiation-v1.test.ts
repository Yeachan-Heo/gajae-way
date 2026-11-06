import { describe, expect, test } from "bun:test";
import { CAPABILITIES, decodeFrame, negotiate, PROFILE_VERSION, SUPPORTED_PROFILE_VERSIONS } from "../src/index";
import priorClientHello from "./fixtures/prior-client-hello-0.1.json";

/**
 * v1.1 freeze contract (P5, plan ARCH-006): clients must negotiate the correct
 * profile version, and breaking changes in newer versions are reflected in
 * incompatible_profile_version errors. A client from the future must be rejected
 * typed, never silently served.
 */
describe("profile v1.1 negotiation", () => {
	test("current profile is 1.1 and only 1.1 is served", () => {
		expect(PROFILE_VERSION).toBe("1.1");
		expect(SUPPORTED_PROFILE_VERSIONS).toEqual(["1.1"]);
	});

	test("old 0.1 client fixture gets incompatible_profile_version (breaking change: model authorization required)", () => {
		const frame = decodeFrame(JSON.stringify(priorClientHello));
		expect(frame.type).toBe("hello");
		const result = negotiate((frame as { payload: { supportedVersions: string[] } }).payload);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.code).toBe("incompatible_profile_version");
			expect(result.detail).toContain("0.1");
		}
	});

	test("client offering only 1.0 also gets incompatible_profile_version", () => {
		const result = negotiate({ supportedVersions: ["1.0"] });
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.code).toBe("incompatible_profile_version");
			expect(result.supportedVersions).toEqual(SUPPORTED_PROFILE_VERSIONS);
		}
	});

	test("future-only client rejects typed with the supported range", () => {
		const result = negotiate({ supportedVersions: ["2.0"] });
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.code).toBe("incompatible_profile_version");
			expect(result.supportedVersions).toEqual(SUPPORTED_PROFILE_VERSIONS);
		}
	});

	test("1.1 client negotiates 1.1", () => {
		const result = negotiate({ supportedVersions: ["1.1"] });
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.negotiated.profileVersion).toBe("1.1");
	});

	test("v1.1 capabilities no longer advertise instability", () => {
		expect(CAPABILITIES as readonly string[]).not.toContain("unstable");
	});
});
