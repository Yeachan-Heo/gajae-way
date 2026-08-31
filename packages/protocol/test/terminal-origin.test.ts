import { describe, expect, test } from "bun:test";
import {
	ERROR_CODES,
	isErrorCode,
	LOOPBACK_ORIGIN,
	ORIGIN_KINDS,
	ORIGIN_PLATFORMS,
	originKey,
	PROFILE_VERSION,
	SUPPORTED_PROFILE_VERSIONS,
	TERMINAL_ORIGIN,
	VERBS_V01,
	validateOriginRef,
} from "../src/index";

/**
 * The terminal entrypath is deliberately ADDITIVE: a fixed conversationId on the
 * existing closed `loopback` platform, one new error code, one new verb — and no
 * change to the closed enums, the profile version, or the capability set.
 */

describe("TERMINAL_ORIGIN", () => {
	test("has the canonical opaque key", () => {
		expect(originKey(TERMINAL_ORIGIN)).toBe("loopback/loopback/terminal");
	});

	test("is structurally valid without widening the closed enums", () => {
		expect(() => validateOriginRef(TERMINAL_ORIGIN)).not.toThrow();
		// The closed sets must be untouched: adding a platform is a break for every
		// OriginRef consumer.
		expect(ORIGIN_PLATFORMS).toEqual(["loopback", "discord", "telegram", "monitor"]);
		expect(ORIGIN_KINDS).toEqual(["dm", "channel", "thread", "topic", "loopback", "eventtype"]);
	});

	test("is a different origin from the chat loopback origin", () => {
		expect(originKey(TERMINAL_ORIGIN)).not.toBe(originKey(LOOPBACK_ORIGIN));
		expect(LOOPBACK_ORIGIN.conversationId).toBe("loopback");
		expect(TERMINAL_ORIGIN.conversationId).toBe("terminal");
	});
});

describe("additive protocol surface", () => {
	test("session_lease_held is a real error code", () => {
		expect(ERROR_CODES).toContain("session_lease_held");
		expect(isErrorCode("session_lease_held")).toBe(true);
	});

	test("session.attach is catalogued", () => {
		expect(VERBS_V01).toContain("session.attach");
	});

	test("the profile version is unchanged and 0.1 stays supported", () => {
		expect(PROFILE_VERSION).toBe("1.0");
		expect(SUPPORTED_PROFILE_VERSIONS).toContain("0.1");
		expect(SUPPORTED_PROFILE_VERSIONS).toContain("1.0");
	});
});
