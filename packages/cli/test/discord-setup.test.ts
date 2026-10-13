import { describe, expect, test } from "bun:test";
import {
	DISCORD_BOT_PERMISSIONS,
	DISCORD_BOT_SCOPES,
	generateDiscordInviteUrl,
	printDiscordBotReady,
	printDiscordSetupGuide,
} from "../src/discord-setup";

describe("Discord setup utilities", () => {
	test("generateDiscordInviteUrl creates valid invite URL", () => {
		const appId = "1234567890";
		const url = generateDiscordInviteUrl(appId);

		// URL structure: https://discord.com/oauth2/authorize?client_id=<appId>&scope=<scopes>&permissions=<perms>
		expect(url).toContain("https://discord.com/oauth2/authorize");
		expect(url).toContain(`client_id=${appId}`);
		expect(url).toContain("scope=");
		expect(url).toContain("permissions=");
	});

	test("invite URL includes all required scopes", () => {
		const appId = "test123";
		const url = generateDiscordInviteUrl(appId);

		// Should include 'bot' and 'applications.commands' scopes
		expect(url).toContain("bot");
		expect(url).toContain("applications.commands");
	});

	test("invite URL includes all required permissions", () => {
		const appId = "test123";
		const url = generateDiscordInviteUrl(appId);

		// Permissions should be URL-encoded as a number
		expect(url).toContain(`permissions=${DISCORD_BOT_PERMISSIONS}`);
	});

	test("printDiscordSetupGuide outputs guidance text", () => {
		const lines: string[] = [];
		const log = (line: string) => lines.push(line);

		printDiscordSetupGuide(log);

		const output = lines.join("\n");

		// Should include key steps
		expect(output).toContain("Discord Developer Portal");
		expect(output).toContain("New Application");
		expect(output).toContain("Bot");
		expect(output).toContain("Reset Token");
		expect(output).toContain("MESSAGE CONTENT INTENT");
		expect(output).toContain("https://discord.com/developers/applications");
	});

	test("printDiscordBotReady outputs invite URL and instructions", () => {
		const lines: string[] = [];
		const log = (line: string) => lines.push(line);
		const appId = "9876543210";

		printDiscordBotReady(log, appId, "MyAwesomeBot");

		const output = lines.join("\n");

		// Should include the invite URL
		const inviteUrl = generateDiscordInviteUrl(appId);
		expect(output).toContain(inviteUrl);

		// Should include authorization steps
		expect(output).toContain("Authorize");
		expect(output).toContain("CAPTCHA");
		expect(output).toContain("DM");

		// Should include bot name if provided
		expect(output).toContain("MyAwesomeBot");
	});

	test("permission bits are correctly calculated", () => {
		// Permission values from Discord API:
		// SEND_MESSAGES: 2048
		// MANAGE_MESSAGES: 8192
		// ADD_REACTIONS: 64
		// EMBED_LINKS: 16384
		// ATTACH_FILES: 33554432
		// READ_MESSAGE_HISTORY: 65536
		// MENTION_EVERYONE: 131072

		const expectedPermissions = 2048 | 8192 | 64 | 16384 | 33554432 | 65536 | 131072;
		expect(DISCORD_BOT_PERMISSIONS).toBe(expectedPermissions);
	});

	test("scopes are in correct format for OAuth2", () => {
		expect(DISCORD_BOT_SCOPES).toContain("bot");
		expect(DISCORD_BOT_SCOPES).toContain("applications.commands");
		expect(DISCORD_BOT_SCOPES.length).toBe(2);
	});

	test("multiple calls to printDiscordBotReady with different IDs generate different URLs", () => {
		const appId1 = "111111";
		const appId2 = "222222";

		const url1 = generateDiscordInviteUrl(appId1);
		const url2 = generateDiscordInviteUrl(appId2);

		expect(url1).not.toEqual(url2);
		expect(url1).toContain(appId1);
		expect(url2).toContain(appId2);
	});
});
