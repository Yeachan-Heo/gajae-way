/**
 * True for the virtual directory a `bun build --compile` binary embeds its modules under.
 *
 * A compiled module reports this virtual root from `import.meta.dir`/`import.meta.path` while
 * `__filename` keeps the build-time source path, so a decision about how the running program
 * was delivered must use `import.meta`, never `__filename`.
 */
export function isEmbeddedModuleDir(dir: string): boolean {
	return dir === "/$bunfs/root" || dir.startsWith("/$bunfs/") || /^[A-Z]:[\\/]~BUN[\\/]/i.test(dir);
}
