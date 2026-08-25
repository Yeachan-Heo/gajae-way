/**
 * Serializes async work per key: tasks with the same key run strictly FIFO,
 * tasks with different keys run concurrently.
 *
 * The gateway uses one key per origin so concurrent inbound turns on one
 * conversation can never race two `gjc --resume` processes on the same
 * session (live P1 drill finding: the second resume aborts the first turn
 * and both replies are lost).
 */
export class KeyedQueue {
	readonly #tails = new Map<string, Promise<void>>();

	run<T>(key: string, task: () => Promise<T>): Promise<T> {
		const tail = this.#tails.get(key) ?? Promise.resolve();
		const result = tail.then(task);
		const next = result.then(
			() => undefined,
			() => undefined,
		);
		this.#tails.set(key, next);
		void next.then(() => {
			if (this.#tails.get(key) === next) this.#tails.delete(key);
		});
		return result;
	}
}
