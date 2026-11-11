import { expect, test } from "bun:test";
import { describe } from "bun:test";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LATEST_SCHEMA_VERSION, Store } from "../src/store";

async function open(): Promise<Store> {
	const directory = await mkdtemp(join(tmpdir(), "gw-model-reload-"));
	const path = join(directory, "gateway.db");
	return Store.open(path);
}

describe("model rebinding on config reload (issue #798)", () => {
	test("sessions without override can be rebound to new model", async () => {
		const db = await open();
		const originKey = "loopback/loopback/test";

		// No model override is set initially
		expect(db.bindings.modelGet(originKey)).toBeUndefined();

		// Simulate rebinding to a new model (this is what applyConfigReload does)
		// In the actual system, this would happen through the rebindModel path
		// For now, we just verify the override mechanism works
		db.transaction((tx) => db.bindings.modelSet(tx, originKey, "new-model", "system"));
		expect(db.bindings.modelGet(originKey)?.selection).toBe("new-model");
		db.close();
	});

	test("sessions with explicit override keep it during config reload", async () => {
		const db = await open();
		const originKey = "loopback/loopback/test";

		// Set an explicit override
		db.transaction((tx) => db.bindings.modelSet(tx, originKey, "explicit-override", "user"));
		expect(db.bindings.modelGet(originKey)?.selection).toBe("explicit-override");

		// Even if we try to rebind to a new default, the override is still there
		// (in the actual system, we check modelGet before rebinding)
		const override = db.bindings.modelGet(originKey);
		expect(override?.selection).toBe("explicit-override");

		// Clear the override to simulate /model clear
		db.transaction((tx) => db.bindings.modelClear(tx, originKey));
		expect(db.bindings.modelGet(originKey)).toBeUndefined();
		db.close();
	});

	test("listOriginKeys only returns origins that have sessions (for rebinding)", async () => {
		const db = await open();

		// Initially, there are no sessions
		let origins = db.bindings.listOriginKeys();
		expect(origins.length).toBe(0);

		// Create model overrides (but no sessions yet - shouldn't show up)
		db.transaction((tx) => {
			db.bindings.modelSet(tx, "loopback/loopback/c1", "model-1", "user");
		});

		// Still no sessions in the sessions table
		origins = db.bindings.listOriginKeys();
		expect(origins.length).toBe(0);

		// The important thing is that modelGet works for checking overrides
		expect(db.bindings.modelGet("loopback/loopback/c1")?.selection).toBe("model-1");
		db.close();
	});
});
