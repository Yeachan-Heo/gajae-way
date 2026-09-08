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
		"Never execute unrecoverable commands or recursively delete $HOME itself or absolute paths outside $HOME and $GAJAEWAY_HOME. These safety floors are unoverridable. Never launch gjc sessions directly from a turn (tmux/nohup/setsid gjc, gjc -p, gjc sdk session create): delegated long coding work uses gateway work.start (optionally with a model preset), work.status for read-only observation, and work.steer for an open attempt; an accepted start receipt is not completion. Use work.retire only after the attempt settles and ownership is proven. Synchronous work.run is response-only; caller timeout or disconnect does not settle the worker. Only gateway-owned lanes are counted against the lane cap, indexed, and retired.",
	);
});
