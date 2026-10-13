import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runGroupWrite, StoreCore } from "../src/store/core";
import { assertOpenWorkExported, mintOpenWorkExported, StoreTransactionError, type WriteTx } from "../src/store/tx";

async function withCore(operation: (core: StoreCore) => void | Promise<void>): Promise<void> {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-store-transaction-"));
	const core = await StoreCore.open(join(directory, "gateway.db"));
	try {
		await operation(core);
	} finally {
		core.close();
		await rm(directory, { recursive: true, force: true });
	}
}

function expectTransactionError(operation: () => unknown, reason: StoreTransactionError["reason"]): void {
	let error: unknown;
	try {
		operation();
	} catch (caught) {
		error = caught;
	}
	expect(error).toBeInstanceOf(StoreTransactionError);
	expect(error).toMatchObject({ reason });
}

test("rejects nested transactions and async callbacks before starting a transaction", async () => {
	await withCore((core) => {
		core.transaction(() => {
			expectTransactionError(() => core.transaction(() => undefined), "nested");
		});
		const asyncCallback = async (_tx: WriteTx) => undefined;
		expectTransactionError(() => core.transaction(asyncCallback), "async_callback");
		core.transaction((tx) => {
			runGroupWrite(
				core.groupDb("bindings"),
				tx,
				"INSERT INTO sessions (origin_key, gjc_session_id, created_at) VALUES ('async-check', 'session', 'now')",
			);
		});
		expect(
			(
				core.groupDb("bindings").query("SELECT origin_key FROM sessions WHERE origin_key = 'async-check'").get() as {
					origin_key: string;
				} | null
			)?.origin_key,
		).toBe("async-check");
	});
});

test("rolls back a sync callback that returns a thenable", async () => {
	await withCore((core) => {
		expectTransactionError(
			() =>
				core.transaction((tx) => {
					runGroupWrite(
						core.groupDb("bindings"),
						tx,
						"INSERT INTO sessions (origin_key, gjc_session_id, created_at) VALUES ('thenable', 'session', 'now')",
					);
					return Promise.resolve("not awaited");
				}),
			"async_callback",
		);
		expect(
			(core.groupDb("bindings").query("SELECT origin_key FROM sessions WHERE origin_key = 'thenable'").get() as {
				origin_key: string;
			} | null) ?? null,
		).toBeNull();
	});
});

test("rejects escaped and foreign transaction capabilities", async () => {
	await withCore((first) =>
		withCore((second) => {
			let escaped: WriteTx | undefined;
			first.transaction((tx) => {
				escaped = tx;
				first.assertActive(tx);
			});
			expectTransactionError(() => first.assertActive(escaped!), "inactive_tx");
			expectTransactionError(() => second.assertActive(escaped!), "foreign_store");
		}),
	);
});

test("OpenWorkExported is tied to the transaction that minted it", async () => {
	await withCore((core) => {
		let exported: ReturnType<typeof mintOpenWorkExported> | undefined;
		let mintedBy: WriteTx | undefined;
		core.transaction((tx) => {
			mintedBy = tx;
			exported = mintOpenWorkExported(tx);
			assertOpenWorkExported(tx, exported);
		});
		core.transaction((nextTx) => {
			expectTransactionError(() => assertOpenWorkExported(nextTx, exported!), "inactive_tx");
		});
		expect(mintedBy).toBeDefined();
	});
});
