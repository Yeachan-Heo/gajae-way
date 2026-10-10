import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload } from "@gajae-gateway/protocol";
import { SlackApiError, SlackWebApi } from "../src/api";
import { type GatewayClientLike, settleSlackDelivery, settleSlackFile } from "../src/main";

let directory = "";
afterEach(async () => {
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

interface Sent {
	readonly url: string;
	readonly headers: Headers;
	readonly body: unknown;
}

/** Answers the two Web API steps and the upload URL; records exactly what went over the wire. */
function uploadFixture(options: { uploadStatus?: number; complete?: () => Response } = {}) {
	const sent: Sent[] = [];
	const api = new SlackWebApi("xoxb-secret", async (input, init) => {
		const url = String(input);
		sent.push({ url, headers: new Headers(init?.headers), body: init?.body });
		if (url.endsWith("/files.getUploadURLExternal"))
			return Response.json({ ok: true, upload_url: "https://files.slack.com/upload/v1/abc", file_id: "F123" });
		if (url === "https://files.slack.com/upload/v1/abc")
			return new Response("OK", { status: options.uploadStatus ?? 200 });
		if (url.endsWith("/files.completeUploadExternal"))
			return options.complete?.() ?? Response.json({ ok: true, files: [] });
		throw new Error(`unexpected ${url}`);
	});
	return { api, sent };
}

class Gateway implements Pick<GatewayClientLike, "request"> {
	readonly requests: { verb: string; params: unknown }[] = [];
	async request<T = unknown>(verb: string, params?: unknown): Promise<T> {
		this.requests.push({ verb, params });
		return {} as T;
	}
}

async function fileDelivery(extra: Partial<ChatMessagePayload> = {}): Promise<ChatMessagePayload> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-slack-files-"));
	const path = join(directory, "report.csv");
	await writeFile(path, "a,b\n1,2\n");
	return {
		turnId: "turn",
		origin: { platform: "slack", kind: "channel", conversationId: "C1" },
		role: "assistant",
		text: "📎 report.csv",
		final: false,
		deliveryId: "file-delivery",
		file: { path, name: "report.csv", size: 8 },
		...extra,
	};
}

test("uploadFile reserves a URL, sends the bytes there without the bot token, and completes into the thread", async () => {
	const { api, sent } = uploadFixture();
	await api.uploadFile("C1", { name: "report.csv", bytes: new TextEncoder().encode("a,b\n") }, "1726543210.000100");
	expect(sent.map((request) => request.url)).toEqual([
		"https://slack.com/api/files.getUploadURLExternal",
		"https://files.slack.com/upload/v1/abc",
		"https://slack.com/api/files.completeUploadExternal",
	]);
	const [reserve, upload, complete] = sent as [Sent, Sent, Sent];
	expect(reserve.headers.get("content-type")).toBe("application/x-www-form-urlencoded");
	expect(Object.fromEntries(new URLSearchParams(String(reserve.body)))).toEqual({
		filename: "report.csv",
		length: "4",
	});
	expect(upload.headers.get("authorization")).toBeNull();
	expect(new TextDecoder().decode(upload.body as Uint8Array)).toBe("a,b\n");
	expect(Object.fromEntries(new URLSearchParams(String(complete.body)))).toEqual({
		files: JSON.stringify([{ id: "F123", title: "report.csv" }]),
		channel_id: "C1",
		thread_ts: "1726543210.000100",
	});
	expect(complete.headers.get("authorization")).toBe("Bearer xoxb-secret");
});

test("a failed byte transfer is a definitive failure and never completes the upload", async () => {
	const { api, sent } = uploadFixture({ uploadStatus: 500 });
	const error = await api.uploadFile("C1", { name: "a.txt", bytes: new Uint8Array([1]) }).catch((caught) => caught);
	expect(error).toBeInstanceOf(SlackApiError);
	expect((error as SlackApiError).code).toBe("upload_http_500");
	expect(sent.some((request) => request.url.endsWith("/files.completeUploadExternal"))).toBe(false);
});

test("a file delivery uploads into the reply thread and confirms; it never posts a text message", async () => {
	const { api, sent } = uploadFixture();
	const gateway = new Gateway();
	await settleSlackDelivery(gateway, api, await fileDelivery({ replyToMessageId: "C1:1726543210.000100" }));
	expect(sent.map((request) => request.url.split("/").at(-1))).toEqual([
		"files.getUploadURLExternal",
		"abc",
		"files.completeUploadExternal",
	]);
	expect(new URLSearchParams(String(sent[2]?.body)).get("thread_ts")).toBe("1726543210.000100");
	expect(gateway.requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "file-delivery" } }]);
});

test("a file that vanished before delivery fails definitively instead of posting anything", async () => {
	const { api, sent } = uploadFixture();
	const gateway = new Gateway();
	const message = await fileDelivery();
	await rm(message.file!.path);
	await settleSlackFile(gateway, api, message);
	expect(sent).toHaveLength(0);
	expect(gateway.requests).toHaveLength(1);
	expect(gateway.requests[0]?.verb).toBe("delivery.fail");
	expect(gateway.requests[0]?.params).toMatchObject({ deliveryId: "file-delivery", ambiguous: false });
});

test("a Slack rejection at completion (missing files:write) is reported, not swallowed", async () => {
	const { api } = uploadFixture({ complete: () => Response.json({ ok: false, error: "missing_scope" }) });
	const gateway = new Gateway();
	await settleSlackFile(gateway, api, await fileDelivery());
	expect(gateway.requests).toEqual([
		{
			verb: "delivery.fail",
			params: { deliveryId: "file-delivery", reason: expect.stringContaining("missing_scope"), ambiguous: false },
		},
	]);
});
