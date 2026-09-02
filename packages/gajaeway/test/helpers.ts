export function deferred<T>(): {
	readonly promise: Promise<T>;
	resolve(value: T | PromiseLike<T>): void;
	reject(reason?: unknown): void;
} {
	let resolve!: (value: T | PromiseLike<T>) => void;
	let reject!: (reason?: unknown) => void;
	const promise = new Promise<T>((resolvePromise, rejectPromise) => {
		resolve = resolvePromise;
		reject = rejectPromise;
	});
	return { promise, resolve, reject };
}

export async function until(condition: () => boolean, timeoutMs = 1_000): Promise<void> {
	const started = Date.now();
	while (!condition()) {
		if (Date.now() - started > timeoutMs) throw new Error("condition was not met before timeout");
		await Bun.sleep(1);
	}
}
