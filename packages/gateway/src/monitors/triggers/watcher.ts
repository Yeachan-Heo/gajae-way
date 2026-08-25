import { lstat, realpath } from "node:fs/promises";
import { resolve, sep } from "node:path";

export async function startWatcher(
	root: string,
	allowedRoots: readonly string[],
	fire: (path: string) => void,
	debounceMs = 100,
): Promise<() => void> {
	const actual = await realpath(root);
	const allowed = await Promise.all(allowedRoots.map((r) => realpath(r)));
	if (!allowed.some((base) => actual === base || actual.startsWith(`${base}${sep}`)))
		throw new Error("watcher root is not allowlisted");
	let timer: ReturnType<typeof setTimeout> | undefined;
	const watcher = (await import("node:fs")).watch(actual, async (_event, filename) => {
		if (!filename) return;
		const path = resolve(actual, filename.toString());
		try {
			if ((await lstat(path)).isSymbolicLink()) return;
		} catch {
			return;
		}
		if (timer) clearTimeout(timer);
		timer = setTimeout(() => fire(path), debounceMs);
	});
	return () => {
		if (timer) clearTimeout(timer);
		watcher.close();
	};
}
