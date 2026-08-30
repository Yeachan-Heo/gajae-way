/**
 * Bounded insertion-ordered id memory.
 *
 * Used for inbound message ids and settled delivery ids: both must dedupe across a
 * long-lived process without growing without bound.
 */
export class LruSet {
	readonly #values = new Map<string, undefined>();
	constructor(readonly limit = 10_000) {
		if (!Number.isSafeInteger(limit) || limit < 1) throw new Error("LRU limit must be a positive integer");
	}

	addIfAbsent(value: string): boolean {
		if (this.#values.has(value)) {
			this.#values.delete(value);
			this.#values.set(value, undefined);
			return false;
		}
		this.#values.set(value, undefined);
		if (this.#values.size > this.limit) this.#values.delete(this.#values.keys().next().value as string);
		return true;
	}
}
