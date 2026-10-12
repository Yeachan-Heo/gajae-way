import { acquireLease, LeaseHeldError } from "../../src/index";

// argv: <home> <name> [--child] [--release]
const [home, name, ...flags] = process.argv.slice(2);
try {
	const lease = await acquireLease(home as string, name as "gateway");
	if (flags.includes("--child")) {
		const child = Bun.spawn(["sleep", "60"], { stdio: ["ignore", "ignore", "ignore"] });
		console.log(`child ${child.pid}`);
	}
	console.log("held");
	if (flags.includes("--release")) {
		await lease.release();
		process.exit(0);
	}
	setInterval(() => {}, 1 << 30);
} catch (error) {
	console.log(error instanceof LeaseHeldError ? "lease_held" : `error ${String(error)}`);
	process.exit(1);
}
