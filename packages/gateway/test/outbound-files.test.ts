import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { checkOutboundFile, deterministicFileDeliveryId } from "../src/server/outbound-files";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, sessionPortFromResponder } from "./session-port.fake";

const SLACK_ORIGIN = { platform: "slack", kind: "channel", conversationId: "C1" } as const;
const DISCORD_ORIGIN = { platform: "discord", kind: "channel", conversationId: "chan-1" } as const;
const TELEGRAM_TOPIC_ORIGIN = {
	platform: "telegram",
	kind: "topic",
	conversationId: "-100123.99",
	parentId: "-100123",
} as const;
const SLACK_TRIGGER = "C1:1726543210.000100";

let directory = "";
let externalWorkspace = "";
let server: GatewayServer | undefined;
afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	if (externalWorkspace) await rm(externalWorkspace, { recursive: true, force: true });
	directory = "";
	externalWorkspace = "";
});

interface Client {
	send(value: unknown): void;
	frames: TestFrame[];
}

interface TestFrame {
	readonly type: string;
	readonly event?: string;
	readonly payload?: unknown;
}

interface ChatMessageFrame {
	readonly type: "event";
	readonly event: "chat.message";
	readonly payload: {
		readonly text: string;
		readonly file?: { readonly name: string; readonly size: number };
		readonly replyToMessageId?: string;
		readonly deliveryId?: string;
	};
}

type FileMessageFrame = Omit<ChatMessageFrame, "payload"> & {
	readonly payload: ChatMessageFrame["payload"] & { readonly file: NonNullable<ChatMessageFrame["payload"]["file"]> };
};

async function connect(socketPath: string): Promise<Client> {
	const frames: TestFrame[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line) as TestFrame);
			},
		},
	});
	return { send: (value) => socket.write(`${JSON.stringify(value)}\n`), frames };
}

async function settle(): Promise<void> {
	for (let attempt = 0; attempt < 60; attempt++) await Bun.sleep(5);
}

/** Home with a workspace file and a secret; `reply` gets the home so it can name real paths. */
async function gateway(
	reply: (home: string) => string,
	setupWorkspace?: (home: string) => Promise<void>,
): Promise<{ client: Client; database: Store }> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-files-"));
	if (setupWorkspace) {
		await setupWorkspace(directory);
	} else {
		await mkdir(join(directory, "workspace"), { recursive: true });
		await writeFile(join(directory, "workspace", "report.csv"), "a,b\n1,2\n");
	}
	await mkdir(join(directory, "secrets"), { recursive: true });
	await writeFile(join(directory, "secrets", "slack-bot-token"), "xoxb-not-a-real-token");
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: {
			"slack:C1": { engagement: "open" },
			"chan-1": { engagement: "open" },
			"telegram:-100123": { engagement: "open" },
		},
	};
	const database = await Store.open(config.dbPath);
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

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function messages(frames: readonly TestFrame[]): ChatMessageFrame[] {
	return frames.filter((frame): frame is ChatMessageFrame => {
		if (frame.type !== "event" || frame.event !== "chat.message" || !isRecord(frame.payload)) return false;
		const { file, replyToMessageId, deliveryId, text } = frame.payload;
		return (
			typeof text === "string" &&
			(file === undefined || (isRecord(file) && typeof file.name === "string" && typeof file.size === "number")) &&
			(replyToMessageId === undefined || typeof replyToMessageId === "string") &&
			(deliveryId === undefined || typeof deliveryId === "string")
		);
	});
}

function fileMessages(frames: readonly ChatMessageFrame[]): FileMessageFrame[] {
	return frames.filter((frame): frame is FileMessageFrame => frame.payload.file !== undefined);
}

test("a MEDIA: line becomes a file delivery in the reply's thread and leaves the text clean", async () => {
	const { client, database } = await gateway(
		(home) => `보고서 올립니다.\nMEDIA:${join(home, "workspace", "report.csv")}`,
	);
	sendMessage(client, SLACK_ORIGIN, SLACK_TRIGGER);
	await settle();
	const delivered = messages(client.frames);
	const text = delivered.filter((frame) => !frame.payload.file);
	const files = fileMessages(delivered);
	expect(text.map((frame) => frame.payload.text)).toEqual(["보고서 올립니다."]);
	expect(files).toHaveLength(1);
	const [file] = files;
	if (!file) throw new Error("expected a file delivery");
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
	expect(database.outbox.rows().some((row) => row.delivery_id === file.payload.deliveryId)).toBe(true);
});

test("a file-only reply uploads the file and speaks no empty message", async () => {
	const { client } = await gateway((home) => `MEDIA:${join(home, "workspace", "report.csv")}`);
	sendMessage(client, SLACK_ORIGIN, SLACK_TRIGGER);
	await settle();
	const delivered = messages(client.frames);
	expect(delivered).toHaveLength(1);
	expect(fileMessages(delivered)[0]?.payload.file.name).toBe("report.csv");
});

test("a MEDIA file in a symlinked persona workspace uploads from the canonical workspace", async () => {
	const { client } = await gateway(
		(home) => `보고서입니다\nMEDIA:${join(home, "workspace", "report.csv")}`,
		async (home) => {
			externalWorkspace = await mkdtemp(join(tmpdir(), "gajaeway-linked-workspace-"));
			await writeFile(join(externalWorkspace, "report.csv"), "a,b\n1,2\n");
			await symlink(externalWorkspace, join(home, "workspace"));
		},
	);
	sendMessage(client, SLACK_ORIGIN, SLACK_TRIGGER);
	await settle();
	const delivered = messages(client.frames);
	expect(delivered.filter((frame) => !frame.payload.file).map((frame) => frame.payload.text)).toEqual(["보고서입니다"]);
	expect(fileMessages(delivered).map((frame) => frame.payload.file.name)).toEqual(["report.csv"]);
	expect(delivered.some((frame) => frame.payload.text.includes("outside the workspace"))).toBe(false);
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

test("a readable file outside the workspace is refused with a visible note, never uploaded", async () => {
	const { client } = await gateway(() => "호스트 파일입니다\nMEDIA:/etc/hosts");
	sendMessage(client, SLACK_ORIGIN, SLACK_TRIGGER);
	await settle();
	const delivered = messages(client.frames);
	expect(delivered.some((frame) => frame.payload.file)).toBe(false);
	expect(delivered.map((frame) => frame.payload.text)).toEqual([
		"호스트 파일입니다",
		"(file not sent: hosts - only files inside the workspace or a configured outbound root are sent)",
	]);
});

test("a Telegram topic reply turns a MEDIA: line into its own file delivery in the same topic", async () => {
	const { client } = await gateway((home) => `보고서입니다\nMEDIA:${join(home, "workspace", "report.csv")}`);
	sendMessage(client, TELEGRAM_TOPIC_ORIGIN, "tg-1");
	await settle();
	const delivered = messages(client.frames);
	expect(delivered.map((frame) => frame.payload.text)).toEqual(["보고서입니다", "📎 report.csv"]);
	const [file] = fileMessages(delivered);
	if (!file) throw new Error("no file delivery");
	expect(file.payload.file).toMatchObject({ name: "report.csv", size: 8 });
	// The adapter routes by origin (chat + topic), exactly like the text part.
	expect(delivered.map((frame) => (frame.payload as { origin?: unknown }).origin)).toEqual([
		TELEGRAM_TOPIC_ORIGIN,
		TELEGRAM_TOPIC_ORIGIN,
	]);
	expect(file.payload.deliveryId).toBe(
		deterministicFileDeliveryId(
			"telegram/topic/-100123.99/parent=-100123",
			"tg-1",
			join(directory, "workspace", "report.csv"),
		),
	);
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

test("checkOutboundFile sends only workspace and configured-root files and refuses the rest", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-files-check-"));
	const home = join(directory, "home");
	const workspace = join(home, "workspace");
	const outbox = join(directory, "outbox");
	const userHome = join(directory, "user");
	await mkdir(workspace, { recursive: true });
	await mkdir(join(home, "secrets"), { recursive: true });
	await mkdir(outbox, { recursive: true });
	await mkdir(join(userHome, ".ssh"), { recursive: true });
	await mkdir(join(userHome, ".aws"), { recursive: true });
	await writeFile(join(workspace, "a.txt"), "hello");
	await writeFile(join(workspace, "big.png"), "png-bytes");
	await writeFile(join(workspace, "empty.txt"), "");
	await writeFile(join(outbox, "c.csv"), "a,b");
	await writeFile(join(userHome, ".ssh", "id_ed25519"), "key");
	await writeFile(join(userHome, ".aws", "credentials"), "aws");
	await writeFile(join(directory, "loose.txt"), "loose");
	await writeFile(join(home, "secrets", "token"), "secret");
	await writeFile(join(home, "gateway.db"), "db");
	// A workspace symlink cannot launder a file past the allowlist.
	await symlink(join(home, "secrets", "token"), join(workspace, "innocent.txt"));
	await symlink(join(userHome, ".ssh", "id_ed25519"), join(workspace, "key.txt"));
	// A configured root that contains the gateway home does not open its secrets.
	const scope = { home, workspace, roots: [outbox, directory, "relative/root"], maxBytes: 6 };
	const narrow = { home, workspace, maxBytes: 6 };
	const ok = await checkOutboundFile(join(workspace, "a.txt"), narrow);
	expect(ok.ok && ok.file.name === "a.txt" && ok.file.size === 5).toBe(true);
	const fromRoot = await checkOutboundFile(join(outbox, "c.csv"), { ...narrow, roots: [outbox] });
	expect(fromRoot.ok && fromRoot.file.name === "c.csv").toBe(true);
	const reasons = async (requests: readonly string[], within: typeof narrow) =>
		Promise.all(
			requests.map(async (requested) => {
				const result = await checkOutboundFile(requested, within);
				return result.ok ? "ok" : result.reason;
			}),
		);
	const notAllowed = "only files inside the workspace or a configured outbound root are sent";
	const homeRefusal = "files under the gateway home (outside the workspace) are not sent";
	expect(
		await reasons(
			[
				"relative/a.txt",
				join(workspace, "missing.txt"),
				workspace,
				join(workspace, "empty.txt"),
				join(workspace, "big.png"),
				join(userHome, ".ssh", "id_ed25519"),
				join(userHome, ".aws", "credentials"),
				join(directory, "loose.txt"),
				join(outbox, "c.csv"),
				join(workspace, "key.txt"),
				join(home, "secrets", "token"),
				join(home, "gateway.db"),
				join(workspace, "innocent.txt"),
			],
			narrow,
		),
	).toEqual([
		"path must be absolute",
		"file not found",
		"not a regular file",
		"file is empty",
		"file is 9 bytes, over the 6-byte limit",
		notAllowed,
		notAllowed,
		notAllowed,
		notAllowed,
		notAllowed,
		homeRefusal,
		homeRefusal,
		homeRefusal,
	]);
	expect(
		await reasons([join(directory, "loose.txt"), join(home, "secrets", "token"), join(home, "gateway.db")], scope),
	).toEqual(["ok", homeRefusal, homeRefusal]);
});

test("a workspace root symlink cannot expose gateway-home secrets", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-files-workspace-link-"));
	const home = join(directory, "home");
	const workspace = join(home, "workspace");
	const secrets = join(home, "secrets");
	await mkdir(secrets, { recursive: true });
	await writeFile(join(secrets, "token"), "secret");
	await symlink(secrets, workspace);

	const result = await checkOutboundFile(join(workspace, "token"), { home, workspace });

	expect(result).toEqual({
		ok: false,
		name: "token",
		reason: "files under the gateway home (outside the workspace) are not sent",
	});
});
