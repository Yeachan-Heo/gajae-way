import { expect, test } from "bun:test";
import { ACTION_GUARD_SYSTEM_NOTICE, ActionGuard } from "../src/guard/action-guard";

for (const mode of ["permissive", "restricted"] as const) {
	test(`unrecoverable floors hold in ${mode} mode`, () => {
		const guard = new ActionGuard({ mode, home: "/home/owner", gajaewayHome: "/home/owner/.gajaeway" });
		for (const command of [
			"rm -rf /",
			"rm -rf --no-preserve-root /",
			"mkfs.ext4 /dev/sda",
			"dd if=x of=/dev/disk1",
			":(){ :|:& };:",
		])
			expect(guard.checkCommand(command)).toMatchObject({ refused: true, floor: "unrecoverable" });
	});
	test(`recursive deletion scope floor holds in ${mode} mode`, () => {
		const guard = new ActionGuard({ mode, home: "/home/owner", gajaewayHome: "/home/owner/.gajaeway" });
		expect(guard.checkCommand("rm -rf ~")).toMatchObject({ refused: true, floor: "path-scope" });
		expect(guard.checkCommand("rm -rf /tmp/other")).toMatchObject({ refused: true, floor: "path-scope" });
		expect(guard.checkCommand("rm -rf /home/owner/work")).toEqual({ allowed: true });
	});
}

test("gjc direct spawn floor is detected in both modes", () => {
	const guard = new ActionGuard({ mode: "permissive", home: "/home/owner", gajaewayHome: "/home/owner/.gajaeway" });

	// Blocked patterns - only patterns that actually spawn sessions
	const blockedPatterns = [
		"gjc", // bare gjc with no args
		"gjc -p", // print flag
		"gjc --print", // print flag long form
		"gjc sdk session create", // explicit spawn subcommand
		"nohup gjc -p", // wrapped in nohup
		"setsid gjc sdk session create", // wrapped in setsid
		'tmux new-session -d -c /tmp "gjc -p"', // wrapped in tmux new-session
		"tmux send-keys -t session 'gjc'", // wrapped in tmux send-keys
		"exec gjc -p", // exec gjc -p
		"screen -S mysession gjc -p", // wrapped in screen
		"setsid gjc 2>&1", // bare gjc wrapped in setsid
		'gjc "write a function"', // gjc with prompt argument
	];

	for (const pattern of blockedPatterns) {
		const result = guard.checkCommand(pattern);
		expect(result, pattern).toMatchObject({ refused: true, floor: "gjc-direct-spawn" });
	}
});

test("gjc direct spawn floor allows legitimate commands", () => {
	const guard = new ActionGuard({ mode: "permissive", home: "/home/owner", gajaewayHome: "/home/owner/.gajaeway" });

	// Allowed patterns - gjc subcommands that do not spawn sessions
	const allowedPatterns = [
		// gajaeway work.start with gjc commands (wrapped in work.start)
		"gajaeway work.start mywork 'gjc -p'",
		"gajaeway work.start 'exec-task' --cwd /tmp 'gjc sdk session create'",

		// Non-spawn gjc subcommands
		"gjc state", // state subcommand
		"gjc accounts", // accounts subcommand
		"gjc --version", // version flag
		"gjc -v", // version short flag
		"gjc version", // version subcommand
		"gjc help", // help subcommand
		"gjc -h", // help short flag
		"gjc --help", // help long flag
		"gjc skill", // skill subcommand
		"gjc setup", // setup subcommand
		"gjc move", // move subcommand
		"gjc sdk", // sdk subcommand (spawning requires 'gjc sdk session create')

		// Non-gjc commands
		"git config core.hooksPath",
		"echo 'gjc' | cat",
		"ls -la /usr/bin/gjc",
		"# gjc is great",
		"GJCFOO=bar npm test",
		"my-gjc-wrapper -p",
	];

	for (const pattern of allowedPatterns) {
		const result = guard.checkCommand(pattern);
		expect(result, pattern).toMatchObject({ allowed: true });
	}
});

test("system notice preserves safety floors and routes delegated work through owned lanes", () => {
	expect(ACTION_GUARD_SYSTEM_NOTICE).toStartWith(
		"Never execute unrecoverable commands or recursively delete $HOME itself or absolute paths outside $HOME and $GAJAEWAY_HOME. These safety floors are unoverridable.",
	);
	expect(ACTION_GUARD_SYSTEM_NOTICE).toBe(
		"Never execute unrecoverable commands or recursively delete $HOME itself or absolute paths outside $HOME and $GAJAEWAY_HOME. These safety floors are unoverridable. Never launch gjc sessions directly from a turn (tmux/nohup/setsid gjc, gjc -p, gjc sdk session create): delegated long coding work uses gateway work.start (optionally with a model preset), work.status for read-only observation, and work.steer for an open attempt; an accepted start receipt is not completion. Use work.retire only after the attempt settles and ownership is proven. When the owner adds a standing prohibition or constraint mid-conversation, register it with `gajaeway work directive add <text>`: it is steered into this conversation's running lanes and prepended to every later lane start. Synchronous work.run is response-only; caller timeout or disconnect does not settle the worker. Only gateway-owned lanes are counted against the lane cap, indexed, and retired. Delegated lanes report to you, not to chat: a work.start lane is bound to this conversation, and its settled result arrives here as an internal lane report turn (not from a human). Lane -> you -> human is the formal path; relay what the conversation needs in your own words, or answer [SILENT].",
	);
});
