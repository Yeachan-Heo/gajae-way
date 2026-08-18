import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { loadWayCore } from "../../src/native-loader";

const temporaryDirectories: string[] = [];

afterEach(() => {
	for (const directory of temporaryDirectories.splice(0)) {
		fs.rmSync(directory, { force: true, recursive: true });
	}
});

test("the native addon exposes a healthy P0 surface", () => {
	const native = loadWayCore();
	const health = native.healthInfo();
	const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-native-test-"));
	temporaryDirectories.push(stateDir);

	expect(health.version).toBe("0.1.0");
	expect(typeof health.bootEpoch).toBe("number");
	expect(health.bootEpoch).toBeGreaterThan(0);
	expect(native.WayCore.open(stateDir).stateDir).toBe(stateDir);
});
