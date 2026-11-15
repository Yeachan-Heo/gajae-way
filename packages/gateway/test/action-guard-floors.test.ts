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

test("system notice preserves safety floors and routes delegated work through owned lanes", () => {
	expect(ACTION_GUARD_SYSTEM_NOTICE).toStartWith(
		"Never execute unrecoverable commands or recursively delete $HOME itself or absolute paths outside $HOME and $GAJAEWAY_HOME. These safety floors are unoverridable.",
	);
	expect(ACTION_GUARD_SYSTEM_NOTICE).toBe(
		'Never execute unrecoverable commands or recursively delete $HOME itself or absolute paths outside $HOME and $GAJAEWAY_HOME. These safety floors are unoverridable. Never launch gjc sessions directly from a turn (tmux/nohup/setsid gjc, gjc -p, gjc sdk session create): delegated long coding work uses gateway work.start (optionally with a model preset); an accepted start receipt is not completion. Asking about a lane and redirecting it are two separate operations, never mixed. A question about its state ("how is it going?", "status?", "what is it doing?", "is it stuck?") is answered with `gajaeway work status <name> --progress`: it reads the tail of the lane\'s session transcript (recent assistant text, recent tool calls with their intent and result, the tool running now, when the turn started) in about a second, never reaches the lane, never interrupts its turn and adds nothing to its transcript; plain `work status <name>` adds the durable job and live operation state. `gajaeway work steer <name> <text>` is only for instructions that change what the open attempt should do (a new constraint, a different approach, stop or redirect, e.g. "stop and use the v2 API" or "fix the tests before the docs"); it interrupts the lane\'s current turn. Never steer a lane to ask for a progress report. Use work.retire only after the attempt settles and ownership is proven. When the owner adds a standing prohibition or constraint mid-conversation, register it with `gajaeway work directive add <text>`: it is steered into this conversation\'s running lanes and prepended to every later lane start. Synchronous work.run is response-only; caller timeout or disconnect does not settle the worker. Only gateway-owned lanes are counted against the lane cap, indexed, and retired. Delegated lanes report to you, not to chat: a work.start lane is bound to this conversation, and its settled result arrives here as an internal lane report turn (not from a human). Lane -> you -> human is the formal path; relay what the conversation needs in your own words, or answer [SILENT].',
	);
});
