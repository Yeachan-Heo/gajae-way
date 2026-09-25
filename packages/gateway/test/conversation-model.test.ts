import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GatewayDatabase, parseModelSelection } from "../src/store/db";

async function openAt(): Promise<{ db: GatewayDatabase; path: string }> {
	const directory = await mkdtemp(join(tmpdir(), "gw-model-"));
	const path = join(directory, "gateway.db");
	return { db: await GatewayDatabase.open(path), path };
}

async function open(): Promise<GatewayDatabase> {
	return (await openAt()).db;
}

describe("parseModelSelection", () => {
	test("accepts a selector string and a preset object", () => {
		expect(parseModelSelection("anthropic/claude-opus-5")).toBe("anthropic/claude-opus-5");
		expect(parseModelSelection({ preset: "gpt-heavy" })).toEqual({ preset: "gpt-heavy" });
	});

	test("fails closed on anything else", () => {
		// A malformed override must degrade to the configured default rather than
		// becoming argv for a gjc spawn.
		for (const bad of [
			"",
			"   ",
			null,
			undefined,
			42,
			[],
			{},
			{ preset: "" },
			{ preset: 7 },
			{ preset: "gpt-heavy", model: "sneaky" },
			{ Preset: "gpt-heavy" },
		]) {
			expect(parseModelSelection(bad)).toBeUndefined();
		}
	});
});

describe("conversation model override", () => {
	test("round-trips a preset and a selector, keyed per conversation", async () => {
		const db = await open();
		expect(db.conversationModelGet("discord:c1")).toBeUndefined();

		db.conversationModelSet("discord:c1", { preset: "gpt-heavy" }, "660473980301344768");
		const stored = db.conversationModelGet("discord:c1");
		expect(stored?.selection).toEqual({ preset: "gpt-heavy" });
		expect(stored?.setBy).toBe("660473980301344768");
		expect(typeof stored?.updatedAt).toBe("string");

		// A second conversation is unaffected.
		expect(db.conversationModelGet("discord:c2")).toBeUndefined();
		db.conversationModelSet("discord:c2", "z-ai/glm-5.3");
		expect(db.conversationModelGet("discord:c2")?.selection).toBe("z-ai/glm-5.3");
		expect(db.conversationModelGet("discord:c1")?.selection).toEqual({ preset: "gpt-heavy" });
		db.close();
	});

	test("setting twice replaces rather than accumulating", async () => {
		const db = await open();
		db.conversationModelSet("discord:c1", { preset: "glm-gpt" });
		db.conversationModelSet("discord:c1", { preset: "frontier-heavy" });
		expect(db.conversationModelGet("discord:c1")?.selection).toEqual({ preset: "frontier-heavy" });
		db.close();
	});

	test("clear reports whether it actually removed an override", async () => {
		const db = await open();
		expect(db.conversationModelClear("discord:c1")).toBe(false);
		db.conversationModelSet("discord:c1", { preset: "gpt-heavy" });
		expect(db.conversationModelClear("discord:c1")).toBe(true);
		expect(db.conversationModelGet("discord:c1")).toBeUndefined();
		db.close();
	});

	test("a corrupt stored row reads as no override instead of throwing", async () => {
		const { db, path } = await openAt();
		db.conversationModelSet("discord:c1", { preset: "gpt-heavy" });
		db.close();
		// Simulate a hand-edited or half-written row: unparseable JSON, and JSON
		// that parses but is not a valid selection.
		for (const payload of ["{not json", '{"preset":""}', '{"model":"sneaky"}', "[]"]) {
			const raw = new Database(path);
			raw.query("UPDATE conversation_model SET selection_json = ? WHERE origin_key = ?").run(payload, "discord:c1");
			raw.close();
			const reopened = await GatewayDatabase.open(path);
			expect(reopened.conversationModelGet("discord:c1")).toBeUndefined();
			reopened.close();
		}
	});

	test("the override survives a reopen and the schema reports 23", async () => {
		const { db, path } = await openAt();
		db.conversationModelSet("discord:c1", { preset: "lunamaxxing-local" });
		db.close();
		const reopened = await GatewayDatabase.open(path);
		expect(reopened.conversationModelGet("discord:c1")?.selection).toEqual({ preset: "lunamaxxing-local" });
		const raw = new Database(path);
		const row = raw.query<{ v: number }, []>("SELECT MAX(version) AS v FROM schema_migrations").get();
		expect(row?.v).toBe(23);
		raw.close();
		reopened.close();
	});
});
