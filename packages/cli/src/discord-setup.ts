/**
 * Discord setup helpers: guided bot token acquisition and invite URL generation.
 */

const DEVELOPER_PORTAL_URL = "https://discord.com/developers/applications";

/**
 * Discord OAuth2 scopes needed for the adapter.
 * `bot` allows the bot to join servers.
 * `applications.commands` allows slash commands (future feature).
 */
export const DISCORD_BOT_SCOPES = ["bot", "applications.commands"];

/**
 * Discord permissions needed for the bot to function.
 * Calculated as the bitwise OR of individual permission bits.
 *
 * - SEND_MESSAGES (2048): Send messages in channels
 * - EMBED_LINKS (16384): Embed links in messages
 * - ATTACH_FILES (33554432): Attach files to messages
 * - READ_MESSAGE_HISTORY (65536): Read past messages
 * - MENTION_EVERYONE (131072): Mention @everyone, @here
 * - MANAGE_MESSAGES (8192): Manage messages (delete, etc.)
 * - ADD_REACTIONS (64): React to messages
 */
export const DISCORD_BOT_PERMISSIONS = 2048 | 16384 | 33554432 | 65536 | 131072 | 8192 | 64;

/**
 * Prints step-by-step guidance for getting a Discord bot token.
 * Called before asking the user to paste the token.
 */
export function printDiscordSetupGuide(log: (line: string) => void): void {
	log("");
	log("═══════════════════════════════════════════════════════════════════════════════════");
	log("  Getting your Discord bot token");
	log("═══════════════════════════════════════════════════════════════════════════════════");
	log("");
	log("1. Open the Discord Developer Portal:");
	log(`   ${DEVELOPER_PORTAL_URL}`);
	log("");
	log("2. Click 'New Application' and choose a name (e.g., 'My gajae-way agent')");
	log("");
	log("3. In the left menu, click 'Bot' → 'Add Bot'");
	log("");
	log("4. Under 'TOKEN', click 'Reset Token' → 'Yes, do it!'");
	log("");
	log("5. Click 'Copy' to copy your bot token (it starts with 'MTI...')");
	log("");
	log("6. (Important) Scroll down to 'GATEWAY INTENTS' and enable 'MESSAGE CONTENT INTENT'");
	log("   This allows the bot to read message text, not just usernames.");
	log("");
	log("7. Click 'Save Changes'");
	log("");
	log("8. Come back here and paste your token below");
	log("");
	log("═══════════════════════════════════════════════════════════════════════════════════");
	log("");
}

/**
 * Generates an OAuth2 invite URL for the Discord bot.
 * The URL allows server owners to authorize the bot with the required permissions.
 *
 * @param appId - The Discord application ID (obtained from the API)
 * @returns A clickable invite URL
 */
export function generateDiscordInviteUrl(appId: string): string {
	const scopeParam = DISCORD_BOT_SCOPES.join("+");
	const permissionsParam = String(DISCORD_BOT_PERMISSIONS);
	return `https://discord.com/oauth2/authorize?client_id=${appId}&scope=${scopeParam}&permissions=${permissionsParam}`;
}

/**
 * Formats the Discord bot setup success message with the invite URL.
 * Includes clear instructions for the user to authorize the bot in their server.
 */
export function printDiscordBotReady(log: (line: string) => void, appId: string, botName?: string): void {
	const inviteUrl = generateDiscordInviteUrl(appId);

	log("");
	log("═══════════════════════════════════════════════════════════════════════════════════");
	log("  ✓ Discord bot token validated");
	log("═══════════════════════════════════════════════════════════════════════════════════");
	log("");
	log(`Application ID: ${appId}`);
	if (botName) {
		log(`Bot name: ${botName}`);
	}
	log("");
	log("To invite your bot to your Discord server, click this link:");
	log("");
	log(`  ${inviteUrl}`);
	log("");
	log("Then:");
	log("  1. Choose the server where you want to add the bot");
	log("  2. Click 'Authorize'");
	log("  3. Complete the CAPTCHA");
	log("");
	log("Once authorized, send the bot a DM and start chatting!");
	log("");
	log("═══════════════════════════════════════════════════════════════════════════════════");
	log("");
}
