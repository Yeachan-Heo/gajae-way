/**
 * I7c acceptance: the credential generation is a digest (never the key), a
 * changed generation on boot rotates only idle origins with reason
 * `credential_stale`, and a held/accepted origin is never rotated.
 */
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { computeCredentialGeneration } from "../src/boot";
import { GatewayDatabase } from "../src/store/db";

test("the generation is a stable sha256 over key, base url and the SSOT models.yml digest; the key is not recoverable", async () => {
	const dir = await mkdtemp(join(tmpdir(), "gajaeway-credgen-"));
	try {
		await Bun.write(join(dir, "models.yml"), "providers: {}\n");
		const a = await computeCredentialGeneration(
			{ OPENAI_API_KEY: "sk-live-1", OPENAI_BASE_URL: "https://api.test/v1" },
			dir,
		);
		const same = await computeCredentialGeneration(
			{ OPENAI_API_KEY: "sk-live-1", OPENAI_BASE_URL: "https://api.test/v1" },
			dir,
		);
		const keyChanged = await computeCredentialGeneration(
			{ OPENAI_API_KEY: "sk-live-2", OPENAI_BASE_URL: "https://api.test/v1" },
			dir,
		);
		const urlChanged = await computeCredentialGeneration(
			{ OPENAI_API_KEY: "sk-live-1", OPENAI_BASE_URL: "http://api.test/v1" },
			dir,
		);
		await Bun.write(join(dir, "models.yml"), "providers: { x: {} }\n");
		const modelsChanged = await computeCredentialGeneration(
			{ OPENAI_API_KEY: "sk-live-1", OPENAI_BASE_URL: "https://api.test/v1" },
			dir,
		);
		expect(a).toMatch(/^[0-9a-f]{64}$/);
		expect(same).toBe(a);
		expect(new Set([a, keyChanged, urlChanged, modelsChanged]).size).toBe(4);
		expect(a).not.toContain("sk-live");
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
});

test("idle origins are rotation candidates; held, fenced and open-lane origins are not", async () => {
	const dir = await mkdtemp(join(tmpdir(), "gajaeway-credgen-db-"));
	const database = await GatewayDatabase.open(join(dir, "gateway.db"));
	try {
		database.putSession("discord/channel/idle", "s-idle");
		database.putSession("discord/channel/held", "s-held");
		database.putSession("monitor/eventtype/x", "s-monitor");
		database.putSession("discord/channel/fenced", "s-fenced");
		database.inboundEnqueue({ messageId: "m", originKey: "discord/channel/held", originRefJson: "{}", body: "m" });
		database.inboundBindTurn({
			messageId: "m",
			originKey: "discord/channel/held",
			epoch: 0,
			opRef: "gw-held",
			sessionId: "s-held",
		});
		database.inboundTurnAccept("gw-held");
		database.inboundEnqueue({ messageId: "f", originKey: "discord/channel/fenced", originRefJson: "{}", body: "f" });
		database.inboundBindTurn({
			messageId: "f",
			originKey: "discord/channel/fenced",
			epoch: 0,
			opRef: "gw-fenced",
			sessionId: "s-fenced",
		});
		database.inboundTurnAccept("gw-fenced");
		database.inboundTurnLose({
			opRef: "gw-fenced",
			originKey: "discord/channel/fenced",
			epoch: 0,
			disposition: "operation_lost",
			failureCode: "operation_lost",
			nowMs: Date.now(),
			fence: true,
			fenceTtlMs: 60_000,
		});
		const candidates = database
			.idleOriginsForCredentialRotation()
			.map((row) => row.origin_key)
			.sort();
		expect(candidates).toEqual(["discord/channel/idle", "monitor/eventtype/x"]);
		for (const row of database.idleOriginsForCredentialRotation())
			database.mutateEpoch(row.origin_key, {
				scope: row.origin_key.startsWith("monitor/") ? "monitor" : "persona",
				reason: "credential_stale",
				cause: { kind: "policy" },
			});
		expect(
			database
				.listEpochMutations({ sinceMs: 0 })
				.map((m) => [m.originKey, m.reason])
				.sort(),
		).toEqual([
			["discord/channel/idle", "credential_stale"],
			["monitor/eventtype/x", "credential_stale"],
		]);
		expect(database.getSessionRecord("discord/channel/held")?.epoch).toBe(0);
		expect(database.inboundTurnRow("gw-held")?.turn_state).toBe("accepted");
	} finally {
		database.close();
		await rm(dir, { recursive: true, force: true });
	}
});
