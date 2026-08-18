/** Durable Discord outbox boundary reserved for P7. */
export interface DiscordOutboxItem {
	readonly cursor: string;
	readonly dedupeKey: string;
}
