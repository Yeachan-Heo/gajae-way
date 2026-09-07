import { homedir } from "node:os";
import { resolve, sep } from "node:path";

export type ActionGuardMode = "permissive" | "restricted";
export type ActionGuardResult =
	| { readonly allowed: true }
	| { readonly refused: true; readonly floor: "unrecoverable" | "path-scope"; readonly reason: string };

/**
 * Gateway-side command floors. P1 does not execute commands in the gateway;
 * gjc receives the same notice as a system clause. These predicates are kept
 * here for the P4 execution hook, where they remain unoverridable regardless
 * of configured guard mode.
 */
export class ActionGuard {
	readonly #home: string;
	readonly #gajaewayHome: string;
	readonly mode: ActionGuardMode;

	constructor(
		options: { readonly mode?: ActionGuardMode; readonly home?: string; readonly gajaewayHome?: string } = {},
	) {
		this.mode = options.mode ?? "permissive";
		this.#home = resolve(options.home ?? homedir());
		this.#gajaewayHome = resolve(options.gajaewayHome ?? process.env.GAJAEWAY_HOME ?? `${this.#home}/.gajaeway`);
	}

	checkCommand(command: string): ActionGuardResult {
		if (isUnrecoverable(command))
			return { refused: true, floor: "unrecoverable", reason: "unrecoverable command floor" };
		if (violatesPathScope(command, this.#home, this.#gajaewayHome))
			return { refused: true, floor: "path-scope", reason: "recursive deletion path is outside permitted scope" };
		return { allowed: true };
	}
}

export const ACTION_GUARD_SYSTEM_NOTICE =
	"Never execute unrecoverable commands or recursively delete $HOME itself or absolute paths outside $HOME and $GAJAEWAY_HOME. These safety floors are unoverridable. Never launch gjc sessions directly from a turn (tmux/nohup/setsid gjc, gjc -p, gjc sdk session create): delegated coding work runs through the gateway work.run verb (optionally with a model preset) and is retired with work.retire, because only gateway-owned lanes are counted against the lane cap, indexed, and retired.";

function isUnrecoverable(command: string): boolean {
	return (
		/(?:^|[;&|]\s*|\s)rm\s+(?:-[A-Za-z]*[rRfF][A-Za-z]*\s+|--recursive\s+)(?:--no-preserve-root\s+)?\/(?:\s|$)/.test(
			command,
		) ||
		/\brm\s+--no-preserve-root\b/.test(command) ||
		/\bmkfs(?:\.[\w-]+)?\s+(?:\S+\s+)*\/dev\/(?:sd|vd|xvd|nvme|disk)\S*/.test(command) ||
		/\bdd\b[^\n]*\bof=\/dev\//.test(command) ||
		/:\(\)\s*\{\s*:\|:\s*&\s*}\s*;\s*:/s.test(command)
	);
}

function violatesPathScope(command: string, home: string, gajaewayHome: string): boolean {
	const match =
		/(?:^|[;&|]\s*)rm\s+(?:-[A-Za-z]*[rR][A-Za-z]*\s+|--recursive\s+)(?:--[^\s]+\s+)*(~\/[^\s;|&]*|~|\/[^\s;|&]*)/g;
	for (const found of command.matchAll(match)) {
		const raw = found[1];
		if (!raw) continue;
		if (!raw.startsWith("/") && raw !== "~" && !raw.startsWith("~/")) continue;
		const target = raw === "~" ? home : raw.startsWith("~/") ? resolve(home, raw.slice(2)) : resolve(raw);
		if (target === home || (!within(target, home) && !within(target, gajaewayHome))) return true;
	}
	return false;
}

function within(path: string, parent: string): boolean {
	return path === parent || path.startsWith(`${parent}${sep}`);
}
