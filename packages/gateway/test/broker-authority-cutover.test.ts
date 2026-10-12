import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, readdir, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { homeLayout } from "@gajae-gateway/home";
import { loadConfig } from "../src/config";
import { runCutover } from "../src/ops/cutover";
import { Store } from "../src/store";

const roots: string[] = [];
afterEach(async () => {
	await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

test("offline authority cutover preserves failed delivery evidence in its backup", async () => {
	const root = await mkdtemp(join(tmpdir(), "authority-cutover-command-"));
	roots.push(root);
	const home = join(root, "home");
	const oldAgent = join(root, "old-agent");
	const targetAgent = join(root, "target-agent");
	await Promise.all([mkdir(home), mkdir(oldAgent), mkdir(targetAgent)]);
	const canonicalOldAgent = await realpath(oldAgent);
	const canonicalTargetAgent = await realpath(targetAgent);
	await writeFile(
		join(home, "config.json"),
		JSON.stringify({ schemaVersion: 1, gjc: { agentDir: canonicalTargetAgent } }),
	);
	const config = await loadConfig({ home });
	const layout = homeLayout(home);
	const database = await Store.open(layout.db, { canonicalAgentDir: canonicalOldAgent });
	database.transaction((tx) =>
		database.inbox.enqueue(tx, {
			messageId: "old-authority-message",
			originKey: "discord:channel:authority-cutover",
			originRefJson: JSON.stringify({ platform: "discord", kind: "channel", conversationId: "authority-cutover" }),
			body: "accepted under the old authority",
		}),
	);
	database.transaction((tx) => {
		database.outbox.create(tx, {
			deliveryId: "failed-delivery",
			turnId: "old-turn",
			originKey: "discord:channel:authority-cutover",
			payloadJson: JSON.stringify({
				origin: { platform: "discord", kind: "channel", conversationId: "authority-cutover" },
				text: "held reply",
			}),
		});
		database.outbox.fail(tx, "failed-delivery", true);
	});
	database.close();

	const log = console.log;
	const error = console.error;
	console.log = () => {};
	console.error = () => {};
	try {
		expect(await runCutover({ config, agentDir: targetAgent })).toBe(0);
	} finally {
		console.log = log;
		console.error = error;
	}

	const backupName = (await readdir(layout.backupsDir)).find(
		(name) => name.startsWith("pre-cutover-") && name.endsWith(".db"),
	);
	expect(backupName).toBeDefined();
	const backup = new Database(join(layout.backupsDir, backupName as string), { readonly: true });
	try {
		expect(backup.query("SELECT delivery_id, state FROM deliveries").all()).toEqual([
			{ delivery_id: "failed-delivery", state: "failed_ambiguous" },
		]);
	} finally {
		backup.close();
	}

	const current = await Store.openReadonly(layout.db);
	try {
		expect(current.bindings.authority).toEqual({
			canonicalAgentDir: canonicalTargetAgent,
			identity: `gjc:${canonicalTargetAgent}`,
		});
		expect(current.bindings.isQuarantined("inbound", "old-authority-message")).toBe(true);
		expect(current.outbox.rows()).toMatchObject([{ delivery_id: "failed-delivery", state: "failed_ambiguous" }]);
	} finally {
		current.close();
	}
});
