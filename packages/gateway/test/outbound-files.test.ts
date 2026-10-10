import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { checkOutboundFile, deterministicFileDeliveryId } from "../src/server/outbound-files";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, sessionPortFromResponder } from "./session-port.fake";

const SLACK_ORIGIN = { platform: "slack", kind: "channel", conversationId: "C1" } as const;
const DISCORD_ORIGIN = { platform: "discord", kind: "channel", conversationId: "chan-1" } as const;
const SLACK_TRIGGER = "C1:1726543210.000100";

let directory = "";
let server: GatewayServer | undefined;
afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

interface Client {
	send(value: unknown): void;
	frames: any[];
}

async function connect(socketPath: string): Promise<Client> {
	const frames: any[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line));
			},
		},
	});
	return { send: (value) => socket.write(`${JSON.stringify(value)}\n`), frames };
}

async function settle(): Promise<void> {
	for (let attempt = 0; attempt < 60; attempt++) await Bun.sleep(5);
}

/** Home with a workspace file and a secret; `reply` gets the home so it can name real paths. */
async function gateway(reply: (home: string) => string): Promise<{ client: Client; database: GatewayDatabase }> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-files-"));
	await mkdir(join(directory, "workspace"), { recursive: true });
	await mkdir(join(directory, "secrets"), { recursive: true });
	await writeFile(join(directory, "workspace", "report.csv"), "a,b\n1,2\n");
	await writeFile(join(directory, "secrets", "slack-bot-token"), "xoxb-not-a-real-token");
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "slack:C1": { engagement: "open" }, "chan-1": { engagement: "open" } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const answer = reply(directory);
	const sessionPort = sessionPortFromResponder({
		bind: async (originKey, epoch) => `session-${originKey}-${epoch}`,
		respond: async () => answer,
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	for (let attempt = 0; attempt < 60 && client.frames.length < 1; attempt++) await Bun.sleep(5);
	return { client, database };
}

function sendMessage(client: Client, origin: unknown, messageId: string): void {
	client.send({
		v: "0.1",
		type: "request",
		id: "c1",
		verb: "chat.send",
		params: {
			origin,
			text: "형님 보고서 파일로 주세요",
			messageId,
			engagement: { mentioned: true, group: true, authorId: "human-1", authorName: "형님" },
		},
	});
}

function messages(frames: any[]): any[] {
	return frames.filter((frame) => frame.type === "event" && frame.event === "chat.message");
}

test("a MEDIA: line becomes a file delivery in the reply's thread and leaves the text clean", async () => {
	const { client, database } = await gateway(
		(home) => `보고서 올립니다.\nMEDIA:${join(home, "workspace", "report.csv")}`,
	);
	sendMessage(client, SLACK_ORIGIN, SLACK_TRIGGER);
	await settle();
	const delivered = messages(client.frames);
	const text = delivered.filter((frame) => !frame.payload.file);
	const files = delivered.filter((frame) => frame.payload.file);
	expect(text.map((frame) => frame.payload.text)).toEqual(["보고서 올립니다."]);
	expect(files).toHaveLength(1);
	const [file] = files;
	expect(file.payload.file.name).toBe("report.csv");
	expect(file.payload.file.size).toBe(8);
	expect(file.payload.text).toBe("📎 report.csv");
	// Same thread as the text: a channel mention is answered under the trigger.
	expect(file.payload.replyToMessageId).toBe(SLACK_TRIGGER);
	expect(text[0].payload.replyToMessageId).toBe(SLACK_TRIGGER);
	// A settleable ledger row under the deterministic per-file id.
	expect(file.payload.deliveryId).toBe(
		deterministicFileDeliveryId("slack/channel/C1", SLACK_TRIGGER, join(directory, "workspace", "report.csv")),
	);
	expect(database.deliveryRows().some((row) => row.delivery_id === file.payload.deliveryId)).toBe(true);
});

test("a file-only reply uploads the file and speaks no empty message", async () => {
	const { client } = await gateway((home) => `MEDIA:${join(home, "workspace", "report.csv")}`);
	sendMessage(client, SLACK_ORIGIN, SLACK_TRIGGER);
	await settle();
	const delivered = messages(client.frames);
	expect(delivered).toHaveLength(1);
	expect(delivered[0].payload.file.name).toBe("report.csv");
});

test("a file under the gateway home outside the workspace is refused with a visible note, never uploaded", async () => {
	const { client } = await gateway((home) => `토큰입니다\nMEDIA:${join(home, "secrets", "slack-bot-token")}`);
	sendMessage(client, SLACK_ORIGIN, SLACK_TRIGGER);
	await settle();
	const delivered = messages(client.frames);
	expect(delivered.some((frame) => frame.payload.file)).toBe(false);
	expect(delivered.map((frame) => frame.payload.text)).toEqual([
		"토큰입니다",
		"(file not sent: slack-bot-token - files under the gateway home (outside the workspace) are not sent)",
	]);
});

test("on a platform without ledger uploads the MEDIA: line is passed through for the adapter", async () => {
	const { client } = await gateway((home) => `여기요\nMEDIA:${join(home, "workspace", "report.csv")}`);
	sendMessage(client, DISCORD_ORIGIN, "m1");
	await settle();
	const delivered = messages(client.frames);
	expect(delivered.map((frame) => frame.payload.text)).toEqual([
		`여기요\nMEDIA:${join(directory, "workspace", "report.csv")}`,
	]);
	expect(delivered.some((frame) => frame.payload.file)).toBe(false);
});

test("checkOutboundFile accepts workspace and outside files and refuses the rest", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-files-check-"));
	const home = join(directory, "home");
	const workspace = join(home, "workspace");
	const outside = join(directory, "outside");
	await mkdir(workspace, { recursive: true });
	await mkdir(join(home, "secrets"), { recursive: true });
	await mkdir(outside, { recursive: true });
	await writeFile(join(workspace, "a.txt"), "hello");
	await writeFile(join(outside, "b.png"), "png-bytes");
	await writeFile(join(outside, "empty.txt"), "");
	await writeFile(join(home, "secrets", "token"), "secret");
	await writeFile(join(home, "gateway.db"), "db");
	// A workspace symlink cannot launder a secret past the check.
	await symlink(join(home, "secrets", "token"), join(workspace, "innocent.txt"));
	const scope = { home, workspace, maxBytes: 6 };
	const ok = await checkOutboundFile(join(workspace, "a.txt"), scope);
	expect(ok.ok && ok.file.name === "a.txt" && ok.file.size === 5).toBe(true);
	const reasons = await Promise.all(
		[
			"relative/a.txt",
			join(workspace, "missing.txt"),
			workspace,
			join(outside, "empty.txt"),
			join(outside, "b.png"),
			join(home, "secrets", "token"),
			join(home, "gateway.db"),
			join(workspace, "innocent.txt"),
		].map(async (requested) => {
			const result = await checkOutboundFile(requested, scope);
			return result.ok ? "ok" : result.reason;
		}),
	);
	expect(reasons).toEqual([
		"path must be absolute",
		"file not found",
		"not a regular file",
		"file is empty",
		"file is 9 bytes, over the 6-byte limit",
		"files under the gateway home (outside the workspace) are not sent",
		"files under the gateway home (outside the workspace) are not sent",
		"files under the gateway home (outside the workspace) are not sent",
	]);
});
