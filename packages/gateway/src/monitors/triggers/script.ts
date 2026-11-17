import { realpath } from "node:fs/promises";
import { sep } from "node:path";
import { ActionGuard } from "../../guard/action-guard";

export async function startScript(
	command: readonly string[],
	intervalMs: number,
	scriptRoot: string,
	fire: (output: string) => void,
	overlap: "skip" | "queue" = "skip",
): Promise<() => void> {
	if (!command.length) throw new Error("script command is required");
	const root = await realpath(scriptRoot);
	const executable = await realpath(command[0]!);
	if (!(executable === root || executable.startsWith(`${root}${sep}`)))
		throw new Error("script executable is outside allowlist");
	const guard = new ActionGuard();
	const verdict = guard.checkCommand(command.join(" "));
	if ("refused" in verdict) throw new Error(verdict.reason);
	let running = false;
	let queued = false;
	const run = async () => {
		if (running) {
			if (overlap === "queue") queued = true;
			return;
		}
		running = true;
		try {
			const child = Bun.spawn({
				cmd: [executable, ...command.slice(1)],
				stdout: "pipe",
				stderr: "ignore",
				env: { PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "" },
			});
			const timer = setTimeout(() => child.kill(), 30_000);
			const output = await new Response(child.stdout).text();
			clearTimeout(timer);
			fire(output.slice(0, 64 * 1024));
		} finally {
			running = false;
			if (queued) {
				queued = false;
				void run();
			}
		}
	};
	void run();
	const timer = setInterval(() => void run(), intervalMs);
	return () => clearInterval(timer);
}
