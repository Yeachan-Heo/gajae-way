/** `socket-down:<s>` is the only mode this adapter fixture reads; the gateway fixture's mode grammar is not imported to keep the package boundary intact. */
const SOCKET_DOWN = /(?:^|,)\s*socket-down:([^,]*)/;

/** Zero-duration Unix-connect failure seam; the injected clock owns elapsed time. */
export function createSocketDownFixture<T = void>(
	options: { modes?: string; now?: () => number; connect?: () => T | Promise<T> } = {},
) {
	const match = SOCKET_DOWN.exec(options.modes ?? process.env.GAJAEWAY_FAKE_GJC_MODES ?? "");
	const mode = match ? `socket-down:${match[1].trim()}` : undefined;
	const seconds = mode ? Number(mode.slice("socket-down:".length).replace(/s$/, "")) : 0;
	if (!Number.isFinite(seconds) || seconds < 0) throw new Error("socket-down duration must be nonnegative seconds");
	const now = options.now ?? Date.now;
	const startedAt = now();
	const availableAtMs = seconds * 1_000;
	const attemptsAt: number[] = [];
	return {
		availableAtMs,
		attemptsAt,
		async connect(): Promise<T | undefined> {
			const elapsed = now() - startedAt;
			attemptsAt.push(elapsed);
			if (elapsed < availableAtMs)
				throw Object.assign(new Error("connect ECONNREFUSED fixture.sock"), {
					code: "ECONNREFUSED",
					syscall: "connect",
					address: "fixture.sock",
				});
			return await options.connect?.();
		},
	};
}
