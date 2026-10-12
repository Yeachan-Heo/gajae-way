import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajae-gateway/subsession";
import { isSessionGoneCode, isVerifiedGjcVersion } from "../src/orchestrator/gjc-contract";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { Store } from "../src/store";
import { noRelay } from "./session-port.fake";

const directories: string[] = [];
afterEach(async () => {
	for (const directory of directories.splice(0)) await rm(directory, { recursive: true, force: true });
});
async function tempDir(prefix: string): Promise<string> {
	const directory = await mkdtemp(join(tmpdir(), prefix));
	directories.push(directory);
	return directory;
}

test("the verified gjc range is inclusive of every patch of the ceiling minor and nothing newer", () => {
	expect(isVerifiedGjcVersion("0.16.0")).toBe(true);
	expect(isVerifiedGjcVersion("gjc/0.18.1")).toBe(true);
	expect(isVerifiedGjcVersion("0.18.99")).toBe(true);
	expect(isVerifiedGjcVersion("0.19.0")).toBe(false);
	expect(isVerifiedGjcVersion("1.0.0")).toBe(false);
	expect(isVerifiedGjcVersion("not a version")).toBe(false);
});

test("every gjc spelling of a dropped session is one session-gone code", () => {
	for (const code of ["session_unavailable", "endpoint_stale", "not_found"]) expect(isSessionGoneCode(code)).toBe(true);
	for (const code of ["broker_unavailable", "operation_failed", undefined, 3])
		expect(isSessionGoneCode(code)).toBe(false);
});


/** The envelope gjc 0.18.1 prints for `sdk session inspect` of an id the broker no longer serves (gaebal-gajae, 2026-09-30). */
const INSPECT_ENDPOINT_STALE = JSON.stringify({
	schema: "gjc.command-error",
	version: 1,
	ok: false,
	command: ["sdk", "session", "inspect"],
	error: {
		code: "endpoint_stale",
		category: "unavailable",
		message: "The SDK endpoint is stale or unavailable.",
		retryability: "unknown",
		outcomeCertainty: "unknown",
		references: [{ kind: "sessionId", value: "old-session" }],
		nextSteps: [],
	},
});

test("gjc 0.18 endpoint_stale on inspect retires the saved session instead of reusing it", async () => {
	const home = await tempDir("gajaeway-stale-inspect-");
	const database = await Store.open(join(home, "gateway.db"));
	const authority = { canonicalAgentDir: home, identity: `gjc:${home}` };
	database.transaction((tx) => database.bindings.initializeAuthority(tx, authority, database.inspectBrokerAuthority().populated));
	const repo = join(home, "workspace");
	let dropped = false;
	let creates = 0;
	const run: CliRunner = async (args) => {
		if (args.includes("session.create")) {
			creates++;
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { sessionId: creates === 1 ? "old-session" : "new-session" } }),
				stderr: "",
			};
		}
		if (args.includes("inspect")) {
			const sessionId = args[args.indexOf("inspect") + 1];
			if (dropped && sessionId === "old-session") return { exitCode: 1, stdout: INSPECT_ENDPOINT_STALE, stderr: "" };
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { session: { sessionId, live: true } } }),
				stderr: "",
			};
		}
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "stale-inspect",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
		sleep: async () => {},
	});
	try {
		expect(await port.bind({ originKey: "discord/channel/c", epoch: 0, repo })).toMatchObject({
			sessionId: "old-session",
			epoch: 0,
		});
		dropped = true;
		expect(await port.liveness({ sessionId: "old-session", repo })).toEqual({ live: undefined, disowned: true });
		expect(await port.bind({ originKey: "discord/channel/c", epoch: 0, repo })).toMatchObject({
			sessionId: "new-session",
			epoch: 1,
		});
	} finally {
		database.close();
	}
});
