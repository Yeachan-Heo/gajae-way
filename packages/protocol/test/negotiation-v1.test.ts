import { describe, expect, test } from "bun:test";
import { CAPABILITIES, decodeFrame, negotiate, PROFILE_VERSION, SUPPORTED_PROFILE_VERSIONS } from "../src/index";
import priorClientHello from "./fixtures/prior-client-hello-0.1.json";

/**
 * v1 freeze contract (P5, plan ARCH-006): frozen prior-release client
 * fixtures must keep negotiating against every later gateway, and a client
 * from the future must be rejected typed, never silently served.
 */
describe("profile v1 freeze", () => {
	test("current profile is 1.0 and 0.1 remains served", () => {
		expect(PROFILE_VERSION).toBe("1.0");
		expect(SUPPORTED_PROFILE_VERSIONS).toContain("0.1");
		expect(SUPPORTED_PROFILE_VERSIONS).toContain("1.0");
	});

	test("frozen 0.1 client fixture negotiates 0.1 (N vs N+1 gateway)", () => {
		const frame = decodeFrame(JSON.stringify(priorClientHello));
		expect(frame.type).toBe("hello");
		const result = negotiate((frame as { payload: { supportedVersions: string[] } }).payload);
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.negotiated.profileVersion).toBe("0.1");
	});

	test("future-only client rejects typed with the supported range", () => {
		const result = negotiate({ supportedVersions: ["2.0"] });
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.code).toBe("incompatible_profile_version");
			expect(result.supportedVersions).toEqual(SUPPORTED_PROFILE_VERSIONS);
		}
	});

	test("mixed-range client negotiates the highest mutual version", () => {
		const result = negotiate({ supportedVersions: ["0.1", "1.0"] });
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.negotiated.profileVersion).toBe("1.0");
	});

	test("v1 capabilities no longer advertise instability", () => {
		expect(CAPABILITIES as readonly string[]).not.toContain("unstable");
	});
});
