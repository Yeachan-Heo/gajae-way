#!/usr/bin/env bun
import { appendFile } from "node:fs/promises";
import { createFakeGjc, runFakeGjc } from "./fake-gjc.mjs";
const args = process.argv.slice(2).filter((_, i, raw) => raw[i] !== "--agent-dir" && raw[i - 1] !== "--agent-dir");
await appendFile(process.env.RED_FIRST_CHILD_PIDS, `${process.pid}\n`);
if (args[2] === "status") await appendFile(process.env.RED_FIRST_STATUS_MARKER, "status entered\n");
if (args.includes("serve") && args.includes("--stdio")) {
	for await (const _chunk of process.stdin) {
		/* silent, attached tail */
	}
} else {
	const result = await runFakeGjc(args, createFakeGjc());
	process.stdout.write(`${result.stdout}\n`);
	process.exitCode = result.exitCode;
}
