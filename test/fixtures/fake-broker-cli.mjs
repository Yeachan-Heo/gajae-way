#!/usr/bin/env node
import fs from "node:fs";

const statePath = process.env.GAJAEWAY_BROKER_FIXTURE_STATE;
if (!statePath) {
	console.error("GAJAEWAY_BROKER_FIXTURE_STATE is required");
	process.exitCode = 2;
} else {
	const state = JSON.parse(fs.readFileSync(statePath, "utf8"));
	const args = process.argv.slice(2);
	if (args.length === 3 && args[0] === "sdk" && args[1] === "session" && args[2] === "list") {
		process.stdout.write(`${JSON.stringify(state.list)}\n`);
	} else if (
		args.length === 7 &&
		args[0] === "sdk" &&
		args[1] === "session" &&
		args[2] === "raw" &&
		args[3] === "query" &&
		args[5] === "--query" &&
		args[6] === "session.metadata"
	) {
		const sessionId = args[4];
		fs.appendFileSync(`${statePath}.queries`, `${sessionId}\n`);
		const metadata = state.metadata?.[sessionId];
		if (!metadata || metadata.unavailable === true) {
			process.stdout.write(`${JSON.stringify({ ok: false, error: { code: "session_unavailable", message: "fixture unavailable" } })}\n`);
			process.exitCode = 1;
		} else {
			process.stdout.write(`${JSON.stringify({ ok: true, result: metadata })}\n`);
		}
	} else {
		console.error(`unexpected fake broker argv: ${JSON.stringify(args)}`);
		process.exitCode = 2;
	}
}
