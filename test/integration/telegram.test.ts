import { expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { TelegramPlatform } from "../../src/adapter/telegram/platform";

test("Telegram offset is persisted only after handler completion and resumes without replay", async () => {
	const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "gajaeway-telegram-offset-"));
	let polls = 0;
	const updates = [{ update_id: 10, message: { message_id: 1, chat: { id: -100 }, from: { id: 5, is_bot: false }, text: "hello" } }];
	const platform = new TelegramPlatform({
		token: "token",
		stateDir,
		fetch: async (input, init) => {
			const method = String(input).split("/").at(-1) ?? "";
			if (method === "getMe") return new Response(JSON.stringify({ ok: true, result: { id: 9 } }));
			if (method === "getUpdates") {
				polls += 1;
				return new Response(JSON.stringify({ ok: true, result: polls === 1 ? updates : [] }));
			}
			return new Response(JSON.stringify({ ok: true, result: true }));
		},
	});
	const seen: string[] = [];
	platform.onMessage(message => { seen.push(message.text); });
	await platform.connect();
	await Bun.sleep(25);
	await platform.disconnect();
	expect(seen).toEqual(["hello"]);
	const persisted = JSON.parse(fs.readFileSync(path.join(stateDir, "telegram-offset.json"), "utf8"));
	expect(persisted.offset).toBe(11);
});
