import { expect, test } from "bun:test";
import rootPackage from "../../../package.json";
import { VERSION } from "../src/version";

test("VERSION is bundled from the root package manifest", () => {
	expect(VERSION).toBe(rootPackage.version);
});
