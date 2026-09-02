import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadConfig } from "../src/config";
import { type GatewayServer, type LocalGatewayPort, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { sessionPortFromResponder } from "./session-port.fake";

/**
 * Engagement gating has ONE owner: the gateway, from live config. Adapters
 * report raw facts (`mentioned`, `group`, `authorId`); they never promote an
 * open room to "mentioned". So an open→closed reload declines the next
 * unmentioned message and flips the "addressed" notice without any adapter restart.
 */

let directory = "";
let server: GatewayServer | undefined;
afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

async function writeConfig(home: string, value: unknown): Promise<void> {
	await Bun.write(join(home, "config.json"), JSON.stringify(value));
}

async function daemon(initial: Record<string, unknown>) {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-engagement-reload-"));
	await writeConfig(directory, { schemaVersion: 1, ...initial });
	const config = await loadConfig({ home: directory });
	const database = await GatewayDatabase.open(config.dbPath);
	const preambles: string[] = [];
	const sessionPort = sessionPortFromResponder({
		respond: async (_sessionId, _text, systemPreamble) => {
			preambles.push(systemPreamble ?? "");
			return "mock reply";
		},
	});
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const port = server.attach("test");
	await port.open();
	return { port, home: directory, preambles };
}

function unmentioned(port: LocalGatewayPort, platform: "discord" | "telegram", id: string) {
	return port.request<{ engaged: boolean; turnId: string | null }>("chat.send", {
		origin: { platform, kind: "channel", conversationId: "room" },
		text: `hello ${id}`,
		messageId: `${platform}-${id}`,
		engagement: { mentioned: false, group: true, authorId: "someone" },
	});
}

async function reload(port: LocalGatewayPort) {
	return port.request<{ ok: boolean; changed: string[] }>("gateway.reloadConfig");
}

async function untilPreambles(preambles: string[], count: number): Promise<void> {
	for (let i = 0; i < 600 && preambles.length < count; i++) await Bun.sleep(5);
	expect(preambles.length).toBeGreaterThanOrEqual(count);
}

for (const platform of ["discord", "telegram"] as const) {
	test(`${platform}: open→closed reload declines the next unmentioned message; closed→open engages it`, async () => {
		const { port, home, preambles } = await daemon({
			settleWindowMs: 0,
			channels: { [`${platform}:room`]: { engagement: "open" } },
		});
		const open = await unmentioned(port, platform, "1");
		expect(open.engaged).toBe(true);
		await untilPreambles(preambles, 1);
		expect(preambles[0]).toContain("You were explicitly addressed here");

		await writeConfig(home, { schemaVersion: 1, settleWindowMs: 0, channels: { [`${platform}:room`]: {} } });
		expect((await reload(port)).changed).toContain("channels");
		const closed = await unmentioned(port, platform, "2");
		expect(closed.engaged).toBe(false);

		await writeConfig(home, {
			schemaVersion: 1,
			settleWindowMs: 0,
			channels: { [`${platform}:room`]: { engagement: "open" } },
		});
		expect((await reload(port)).changed).toContain("channels");
		const reopened = await unmentioned(port, platform, "3");
		expect(reopened.engaged).toBe(true);
		await untilPreambles(preambles, 2);
		expect(preambles[1]).toContain("You were explicitly addressed here");
	});
}

test("a mentioned message in a closed room from the owner is addressed; an unmentioned open-room bot is not", async () => {
	const { port, preambles } = await daemon({
		settleWindowMs: 0,
		mentionAllowlist: ["owner"],
		channels: { "discord:room": {} },
	});
	const sent = await port.request<{ engaged: boolean }>("chat.send", {
		origin: { platform: "discord", kind: "channel", conversationId: "room" },
		text: "hi",
		messageId: "m-owner",
		engagement: { mentioned: true, group: true, authorId: "owner" },
	});
	expect(sent.engaged).toBe(true);
	await untilPreambles(preambles, 1);
	expect(preambles[0]).toContain("You were explicitly addressed here");
});
