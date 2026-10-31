import { homedir } from "node:os";
import { resolve, sep } from "node:path";

export type ActionGuardMode = "permissive" | "restricted";
export type ActionGuardResult =
	| { readonly allowed: true }
	| {
			readonly refused: true;
			readonly floor: "unrecoverable" | "path-scope" | "gjc-direct-spawn";
			readonly reason: string;
	  };

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
		if (spawnsGjcDirectly(command))
			return {
				refused: true,
				floor: "gjc-direct-spawn",
				reason: "gjc sessions must be launched through gajaeway work.start",
			};
		return { allowed: true };
	}
}

export const ACTION_GUARD_SYSTEM_NOTICE =
	"Never execute unrecoverable commands or recursively delete $HOME itself or absolute paths outside $HOME and $GAJAEWAY_HOME. These safety floors are unoverridable. Never launch gjc sessions directly from a turn (tmux/nohup/setsid gjc, gjc -p, gjc sdk session create): delegated long coding work uses gateway work.start (optionally with a model preset), work.status for read-only observation, and work.steer for an open attempt; an accepted start receipt is not completion. Use work.retire only after the attempt settles and ownership is proven. When the owner adds a standing prohibition or constraint mid-conversation, register it with `gajaeway work directive add <text>`: it is steered into this conversation's running lanes and prepended to every later lane start. Synchronous work.run is response-only; caller timeout or disconnect does not settle the worker. Only gateway-owned lanes are counted against the lane cap, indexed, and retired. Delegated lanes report to you, not to chat: a work.start lane is bound to this conversation, and its settled result arrives here as an internal lane report turn (not from a human). Lane -> you -> human is the formal path; relay what the conversation needs in your own words, or answer [SILENT].";

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

/**
 * Detects patterns that attempt to spawn gjc sessions directly from a turn.
 * Blocked patterns: bare gjc, gjc -p/--print, gjc <prompt>, gjc sdk session create, tmux/nohup/setsid/screen gjc.
 * Does NOT block: gjc state, gjc accounts, gjc --version, gjc help, or other non-spawn subcommands.
 * Does NOT block: gajaeway work.start with gjc commands, legitimate gjc wrapper invocations.
 * Known bypass: complex shell syntax like printf/echo/eval, $(subshell gjc),
 * or deliberate obfuscation that doesn't literally contain 'gjc' boundaries.
 */
function spawnsGjcDirectly(command: string): boolean {
	// Strategy: Only block gjc invocations that spawn new sessions.
	// Subcommands like 'state', 'accounts', 'help', etc. do not spawn sessions and are allowed.

	const removeLegitimateArgs = (cmd: string): string => {
		// Remove quoted arguments to gajaeway/work.* commands to avoid false positives
		// e.g., 'gajaeway work.start "task" "gjc -p"' should not be blocked
		return cmd.replace(/(?:gajaeway|work\.\w+)\s+[^;&|]*['"]([^'"]*)['"][^;&|]*/g, (match, _inside) => {
			return match.replace(/['"][^'"]*['"]/g, "'_'");
		});
	};

	const cleaned = removeLegitimateArgs(command);

	// Block execution wrappers: tmux, nohup, setsid, screen
	if (/\btmux\s+(?:new-session|new-window|send-keys)[\s\S]*?\bgjc\b/.test(command)) return true;
	if (/\b(?:nohup|setsid|screen)\b[\s\S]*?\bgjc\b/.test(command)) return true;

	// Block explicit spawn subcommand
	if (/\bgjc\s+sdk\s+session\s+create\b/.test(cleaned)) return true;

	// Check for bare gjc (no arguments) or gjc with only separators/redirects after it
	if (/(?:^|[;&|]\s*|\bexec\s+)\bgjc(?:\s*(?:[;&|]|$))/.test(cleaned)) return true;

	// Check for gjc with arguments
	const gjcWithArgsMatch = /(?:^|[;&|]\s*|\bexec\s+)\bgjc\s+([^\s;&|]+)/.exec(cleaned);
	if (gjcWithArgsMatch) {
		const firstArg = gjcWithArgsMatch[1];

		// Block print flags (-p, --print)
		if (firstArg === "-p" || firstArg === "--print") return true;

		// Known non-spawning subcommands and flags
		// These are gjc commands that don't launch sessions, so they should be allowed
		const nonSpawningPatterns = new Set([
			// Informational subcommands
			"state",
			"--version",
			"-v",
			"version",
			"help",
			"-h",
			"--help",
			// Configuration/account subcommands
			"accounts",
			"setup",
			"skill",
			"move",
			// SDK subcommands (sdk itself doesn't spawn; specific ones like 'sdk session create' are blocked separately)
			"sdk",
		]);

		// If first arg is a flag starting with -, allow it (these are options to gjc itself, not prompts)
		if (firstArg.startsWith("-") && !nonSpawningPatterns.has(firstArg)) {
			// Unknown flag - but don't block flags, as they might be valid gjc options
			return false;
		}

		// If it's a known non-spawning subcommand, it's safe
		if (nonSpawningPatterns.has(firstArg)) return false;

		// Otherwise, treat as a positional prompt argument and block it
		// (e.g., 'gjc "write a function"')
		return true;
	}

	return false;
}
