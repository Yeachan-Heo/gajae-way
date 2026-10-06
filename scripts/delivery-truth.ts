#!/usr/bin/env bun
/**
 * Delivery truth spot-check: every recent `deliveries.confirmed` row must be
 * visible as a bot message in the target Discord channel.
 *
 *   bun scripts/delivery-truth.ts <gateway.db> <discord-token-file> [minutes=30]
 *
 * Exits 1 when a confirmed delivery has no matching Discord message.
 */
import { Database } from "bun:sqlite";

const [dbPath, tokenFile, minutesArg] = process.argv.slice(2);
if (!dbPath || !tokenFile) {
	console.error("usage: bun scripts/delivery-truth.ts <gateway.db> <discord-token-file> [minutes]");
	process.exit(2);
}
const minutes = Number(minutesArg ?? 30);
const token = (await Bun.file(tokenFile).text()).trim();
const db = new Database(dbPath, { readonly: true });
const since = new Date(Date.now() - minutes * 60_000).toISOString();
const rows = db
	.query<{ turn_id: string; created_at: string; payload_json: string }, [string]>(
		"SELECT turn_id, created_at, payload_json FROM deliveries WHERE state = 'confirmed' AND created_at >= ? ORDER BY created_at DESC",
	)
	.all(since);

const byChannel = new Map<string, Array<{ text: string; at: string; turnId: string }>>();
for (const row of rows) {
	const payload = JSON.parse(row.payload_json) as {
		origin?: { conversationId?: string };
		text?: string;
		reaction?: unknown;
	};
	const conversationId = payload.origin?.conversationId;
	// Reactions are emoji on an existing message, not a message; skip them.
	if (!conversationId || typeof payload.text !== "string" || payload.reaction !== undefined) continue;
	const list = byChannel.get(conversationId) ?? [];
	list.push({ text: payload.text, at: row.created_at, turnId: row.turn_id });
	byChannel.set(conversationId, list);
}

let missing = 0;
let checked = 0;
for (const [channelId, deliveries] of byChannel) {
	const response = await fetch(`https://discord.com/api/v10/channels/${channelId}/messages?limit=100`, {
		headers: { Authorization: `Bot ${token}` },
	});
	if (!response.ok) {
		console.log(`channel ${channelId}: HTTP ${response.status} (skipped ${deliveries.length} deliveries)`);
		continue;
	}
	const messages = (await response.json()) as Array<{ content: string; timestamp: string; author: { bot?: boolean } }>;
	const botContents = messages.filter((message) => message.author.bot).map((message) => message.content);
	for (const delivery of deliveries) {
		checked++;
		// Discord chunks long deliveries; match on the first 60 chars of the text.
		const needle = delivery.text.slice(0, 60);
		const found = botContents.some((content) => content.includes(needle));
		if (!found) {
			missing++;
			console.log(
				`MISSING channel=${channelId} turn=${delivery.turnId} at=${delivery.at} text=${JSON.stringify(needle)}`,
			);
		}
	}
}
console.log(`delivery-truth: checked=${checked} missing=${missing} window=${minutes}m`);
process.exit(missing === 0 ? 0 : 1);
