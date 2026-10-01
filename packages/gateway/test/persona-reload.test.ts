import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { lstat, mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { originKey } from "@gajae-gateway/protocol";
import type { GatewayConfig } from "../src/config";
import { PersonaLoader, SELF_OPS_PREAMBLE_POINTER } from "../src/persona/persona";
import { startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, sessionPortFromResponder } from "./session-port.fake";

const LOOPBACK_ORIGIN = { platform: "loopback" as const, kind: "loopback" as const, conversationId: "loopback" };
const UPDATED_AGENTS_MD_HEADING =
	"## AGENTS.md (updated since this session started; supersedes the project-context copy loaded at session start)";

type TestFrame = {
	type?: string;
	id?: string;
	event?: string;
	payload?: { final?: boolean; role?: string; text?: string };
};

type TestSessionPort = ReturnType<typeof sessionPortFromResponder>;

async function startAgentsGateway(
	home: string,
	seen: string[],
	options: { readonly database?: GatewayDatabase; readonly sessionPort?: TestSessionPort } = {},
) {
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
	};
	const database = options.database ?? (await GatewayDatabase.open(config.dbPath));
	const sessionPort =
		options.sessionPort ??
		sessionPortFromResponder({
			bind: (key, epoch) => `${key}#${epoch}`,
			respond: async (_id, _text, preamble) => {
				seen.push(preamble ?? "");
				return "reply";
			},
		});
	if (!options.sessionPort) attachTestBrokerOwnership(database, sessionPort, join(home, "agent"));
	const server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	return { config, database, sessionPort, server };
}

async function connectLoopbackClient(config: GatewayConfig) {
	const frames: TestFrame[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: config.socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line) as TestFrame);
			},
		},
	});
	const send = (value: unknown) => socket.write(`${JSON.stringify(value)}\n`);
	const waitFor = async (predicate: (frame: TestFrame) => boolean, message: string) => {
		for (let i = 0; i < 1_000; i++) {
			if (frames.some(predicate)) return;
			await Bun.sleep(5);
		}
		throw new Error(message);
	};
	send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor((frame) => frame.type === "negotiated", "gateway did not negotiate the loopback test client");
	return {
		sendTurn: async (id: string, text: string) => {
			send({
				v: "0.1",
				type: "request",
				id,
				verb: "chat.send",
				params: { origin: LOOPBACK_ORIGIN, text },
			});
			await waitFor(
				(frame) =>
					frame.type === "event" && frame.event === "chat.message" && frame.id === id && frame.payload?.final === true,
				`loopback turn ${id} did not finish`,
			);
		},
		close: () => socket.end(),
	};
}

function agentsSectionCount(preamble: string): number {
	return (
		preamble.match(
			/^## AGENTS\.md(?: \(updated since this session started; supersedes the project-context copy loaded at session start\))?$/gm,
		)?.length ?? 0
	);
}

function preambleAt(seen: readonly string[], index: number): string {
	const preamble = seen[index];
	if (preamble === undefined) throw new Error(`turn ${index} did not record a system preamble`);
	return preamble;
}

test("persona USER.md edits are included on the next turn", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-persona-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
	};
	const seen: string[] = [];
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => `${key}#${epoch}`,
		respond: async (_id, _text, preamble) => {
			seen.push(preamble ?? "");
			return "reply";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(home, "agent"));
	const server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	try {
		await mkdir(join(home, "workspace"), { recursive: true });
		await Bun.write(join(home, "workspace/USER.md"), "first");
		const frames: unknown[] = [];
		let buffered = "";
		const socket = await Bun.connect({
			unix: config.socketPath,
			socket: {
				data(_socket, data) {
					buffered += Buffer.from(data).toString();
					const lines = buffered.split("\n");
					buffered = lines.pop() ?? "";
					for (const line of lines) if (line) frames.push(JSON.parse(line));
				},
			},
		});
		const send = (value: unknown) => socket.write(`${JSON.stringify(value)}\n`);
		const wait = async (count: number) => {
			for (let i = 0; i < 100 && frames.length < count; i++) await Bun.sleep(5);
		};
		send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await wait(1);
		send({
			v: "0.1",
			type: "request",
			id: "one",
			verb: "chat.send",
			params: { origin: { platform: "loopback", kind: "loopback", conversationId: "loopback" }, text: "one" },
		});
		await wait(3);
		await Bun.write(join(home, "workspace/USER.md"), "second");
		send({
			v: "0.1",
			type: "request",
			id: "two",
			verb: "chat.send",
			params: { origin: { platform: "loopback", kind: "loopback", conversationId: "loopback" }, text: "two" },
		});
		for (let i = 0; i < 1_000 && seen.length < 2; i++) await Bun.sleep(5);
		expect(seen).toHaveLength(2);
		expect(seen[0]).toContain("first");
		expect(seen[1]).toContain("second");
		// Session-context grounding: every preamble names the bound conversation.
		expect(seen[0]).toContain("## Current conversation");
		expect(seen[0]).toContain("loopback");
		socket.end();
	} finally {
		await server.stop();
		await rm(home, { recursive: true, force: true });
	}
});

test("fresh persona workspace discovers the bundled self-ops skill without preamble bulk", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-self-ops-"));
	try {
		const persona = new PersonaLoader(home);
		await Promise.all([persona.ensureWorkspace(), persona.ensureWorkspace()]);
		const skillRoot = join(home, "workspace", ".gjc", "skills", "self-ops");
		const skill = await readFile(join(skillRoot, "SKILL.md"), "utf8");
		expect(skill).toContain("name: self-ops");
		expect(skill).toContain("service-control.md");
		expect(await readFile(join(skillRoot, "service-control.md"), "utf8")).toContain("launchctl kickstart -k");

		const preamble = await persona.systemPreamble();
		expect(Buffer.byteLength(SELF_OPS_PREAMBLE_POINTER, "utf8")).toBeLessThan(2 * 1024);
		expect(preamble).toContain("/skill:self-ops");
		expect(preamble).not.toContain("launchctl kickstart -k");
		expect(preamble).not.toContain("gateway.db");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("persona workspace maps relative memory writes to the canonical corpus", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-persona-memory-"));
	try {
		const persona = new PersonaLoader(home);
		await Promise.all([persona.ensureWorkspace(), persona.ensureWorkspace()]);
		const workspaceMemory = join(home, "workspace", "memory");
		expect((await lstat(workspaceMemory)).isSymbolicLink()).toBe(true);
		expect(await realpath(workspaceMemory)).toBe(await realpath(join(home, "memory")));
		await writeFile(join(workspaceMemory, "relative-write.md"), "canonical");
		expect(await readFile(join(home, "memory", "relative-write.md"), "utf8")).toBe("canonical");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("self-ops workspace amendments survive later seeding", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-self-ops-amendment-"));
	try {
		const persona = new PersonaLoader(home);
		await persona.ensureWorkspace();
		const skill = join(home, "workspace", ".gjc", "skills", "self-ops", "SKILL.md");
		await writeFile(skill, "host-local self-ops amendment\n");
		await persona.ensureWorkspace();
		expect(await readFile(skill, "utf8")).toBe("host-local self-ops amendment\n");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("persona workspace refuses a pre-existing memory directory without modifying it", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-persona-memory-conflict-"));
	try {
		await mkdir(join(home, "workspace", "memory"), { recursive: true });
		await writeFile(join(home, "workspace", "memory", "stray.md"), "preserve me");
		await expect(new PersonaLoader(home).ensureWorkspace()).rejects.toThrow("workspace_memory_path_conflict");
		expect(await readFile(join(home, "workspace", "memory", "stray.md"), "utf8")).toBe("preserve me");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("persona workspace refuses a memory symlink that escapes the canonical corpus", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-persona-memory-escape-"));
	try {
		await mkdir(join(home, "workspace"), { recursive: true });
		await mkdir(join(home, "outside"), { recursive: true });
		await symlink(join(home, "outside"), join(home, "workspace", "memory"), "dir");
		await expect(new PersonaLoader(home).ensureWorkspace()).rejects.toThrow("workspace_memory_path_conflict");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("persona workspace accepts a workspace that itself links to the corpus's parent", async () => {
	// Live shape on jip: workspace -> ~/clawd and memory -> ~/clawd/memory, so
	// workspace/memory is a real directory that IS the canonical corpus.
	const home = await mkdtemp(join(tmpdir(), "gajaeway-persona-memory-parent-link-"));
	try {
		await mkdir(join(home, "clawd", "memory"), { recursive: true });
		await symlink(join(home, "clawd"), join(home, "workspace"), "dir");
		await symlink(join(home, "clawd", "memory"), join(home, "memory"), "dir");
		await new PersonaLoader(home).ensureWorkspace();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("#377 AGENTS.md is removed from persona preamble so gjc discovery owns it uniquely", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-agents-once-"));
	try {
		const persona = new PersonaLoader(home);
		await persona.ensureWorkspace();
		// Create SOUL.md, AGENTS.md, and USER.md in the workspace.
		const workspace = join(home, "workspace");
		await writeFile(join(workspace, "SOUL.md"), "soul content");
		await writeFile(join(workspace, "AGENTS.md"), "agents content");
		await writeFile(join(workspace, "USER.md"), "user content");
		// The persona preamble should NOT include AGENTS.md.
		const preamble = await persona.systemPreamble();
		expect(preamble).toContain("## SOUL.md");
		expect(preamble).toContain("soul content");
		expect(preamble).toContain("## USER.md");
		expect(preamble).toContain("user content");
		// AGENTS.md should NOT appear as a section header in the preamble.
		expect(preamble).not.toContain("## AGENTS.md");
		// But AGENTS.md content might exist if it was accidentally included,
		// so let's verify the section is really absent.
		const agentsSectionPattern = /## AGENTS\.md\s*\n/;
		expect(agentsSectionPattern.test(preamble)).toBe(false);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("fresh session leaves AGENTS.md to gjc project-context discovery", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-agents-fresh-"));
	const seen: string[] = [];
	let gateway: Awaited<ReturnType<typeof startAgentsGateway>> | undefined;
	let client: Awaited<ReturnType<typeof connectLoopbackClient>> | undefined;
	try {
		await mkdir(join(home, "workspace"), { recursive: true });
		await writeFile(join(home, "workspace", "AGENTS.md"), "session-start instructions");
		gateway = await startAgentsGateway(home, seen);
		client = await connectLoopbackClient(gateway.config);
		await client.sendTurn("fresh", "first turn");
		await client.sendTurn("still-current", "second turn with unchanged AGENTS.md");

		expect(seen).toHaveLength(2);
		expect(agentsSectionCount(preambleAt(seen, 0))).toBe(0);
		expect(agentsSectionCount(preambleAt(seen, 1))).toBe(0);
		expect(gateway.database.getSessionBootstrap(originKey(LOOPBACK_ORIGIN))).toMatchObject({
			epoch: 0,
			lastBootstrappedEpoch: 0,
			agentsMdEpoch: 0,
			agentsMdDigest: createHash("sha256").update("session-start instructions").digest("hex"),
		});
	} finally {
		client?.close();
		await gateway?.server.stop();
		await rm(home, { recursive: true, force: true });
	}
});

test("AGENTS.md edits and deletion supersede the session-start copy once per turn", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-agents-updated-"));
	const agentsPath = join(home, "workspace", "AGENTS.md");
	const seen: string[] = [];
	let gateway: Awaited<ReturnType<typeof startAgentsGateway>> | undefined;
	let client: Awaited<ReturnType<typeof connectLoopbackClient>> | undefined;
	try {
		await mkdir(join(home, "workspace"), { recursive: true });
		await writeFile(agentsPath, "session-start instructions");
		gateway = await startAgentsGateway(home, seen);
		client = await connectLoopbackClient(gateway.config);
		await client.sendTurn("before-edit", "first turn");
		expect(agentsSectionCount(preambleAt(seen, 0))).toBe(0);

		await writeFile(agentsPath, "updated instructions");
		await client.sendTurn("after-edit", "second turn");
		expect(agentsSectionCount(preambleAt(seen, 1))).toBe(1);
		expect(seen[1]).toContain(`${UPDATED_AGENTS_MD_HEADING}\nupdated instructions`);
		expect(seen[1]).not.toContain("session-start instructions");
		expect(gateway.database.getSessionBootstrap(originKey(LOOPBACK_ORIGIN))).toMatchObject({
			agentsMdEpoch: 0,
			agentsMdDigest: createHash("sha256").update("updated instructions").digest("hex"),
		});

		await client.sendTurn("edit-still-current", "third turn with unchanged AGENTS.md");
		expect(agentsSectionCount(preambleAt(seen, 2))).toBe(0);

		await rm(agentsPath);
		await client.sendTurn("after-delete", "fourth turn");
		expect(agentsSectionCount(preambleAt(seen, 3))).toBe(1);
		expect(seen[3]).toContain(
			`${UPDATED_AGENTS_MD_HEADING}\n[AGENTS.md was deleted from the workspace after this session started.]`,
		);
		await client.sendTurn("deletion-still-current", "fifth turn with unchanged AGENTS.md");
		expect(agentsSectionCount(preambleAt(seen, 4))).toBe(0);
	} finally {
		client?.close();
		await gateway?.server.stop();
		await rm(home, { recursive: true, force: true });
	}
});

test("a new session epoch establishes the current AGENTS.md as its baseline", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-agents-new-epoch-"));
	const agentsPath = join(home, "workspace", "AGENTS.md");
	const seen: string[] = [];
	let gateway: Awaited<ReturnType<typeof startAgentsGateway>> | undefined;
	let client: Awaited<ReturnType<typeof connectLoopbackClient>> | undefined;
	try {
		await mkdir(join(home, "workspace"), { recursive: true });
		await writeFile(agentsPath, "old epoch instructions");
		gateway = await startAgentsGateway(home, seen);
		client = await connectLoopbackClient(gateway.config);
		await client.sendTurn("old-epoch", "first turn");

		await writeFile(agentsPath, "new epoch instructions");
		await client.sendTurn("reset", "/new");
		await client.sendTurn("new-epoch", "first turn in the new session");

		expect(agentsSectionCount(preambleAt(seen, 0))).toBe(0);
		expect(agentsSectionCount(preambleAt(seen, 1))).toBe(0);
		expect(gateway.database.getSessionBootstrap(originKey(LOOPBACK_ORIGIN))).toMatchObject({
			epoch: 1,
			lastBootstrappedEpoch: 1,
			agentsMdEpoch: 1,
			agentsMdDigest: createHash("sha256").update("new epoch instructions").digest("hex"),
		});
	} finally {
		client?.close();
		await gateway?.server.stop();
		await rm(home, { recursive: true, force: true });
	}
});

test("legacy bootstrapped sessions receive an AGENTS.md deletion notice once", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-agents-legacy-"));
	const agentsPath = join(home, "workspace", "AGENTS.md");
	const seen: string[] = [];
	let gateway: Awaited<ReturnType<typeof startAgentsGateway>> | undefined;
	let client: Awaited<ReturnType<typeof connectLoopbackClient>> | undefined;
	try {
		await mkdir(join(home, "workspace"), { recursive: true });
		await writeFile(agentsPath, "instructions from the pre-migration session");
		gateway = await startAgentsGateway(home, seen);
		client = await connectLoopbackClient(gateway.config);
		await client.sendTurn("legacy-start", "first turn");
		expect(agentsSectionCount(preambleAt(seen, 0))).toBe(0);

		// Version 25 adds these columns as -1/NULL for sessions already bootstrapped.
		const raw = new Database(gateway.config.dbPath);
		try {
			raw
				.query("UPDATE sessions SET agents_md_epoch = -1, agents_md_digest = NULL WHERE origin_key = ?")
				.run(originKey(LOOPBACK_ORIGIN));
		} finally {
			raw.close();
		}
		await rm(agentsPath);
		await client.sendTurn("legacy-delete", "second turn after deletion");
		expect(agentsSectionCount(preambleAt(seen, 1))).toBe(1);
		expect(seen[1]).toContain(
			`${UPDATED_AGENTS_MD_HEADING}\n[AGENTS.md was deleted from the workspace after this session started.]`,
		);

		await client.sendTurn("legacy-delete-once", "third turn after deletion");
		expect(agentsSectionCount(preambleAt(seen, 2))).toBe(0);
	} finally {
		client?.close();
		await gateway?.server.stop();
		await rm(home, { recursive: true, force: true });
	}
});

test("AGENTS.md baseline survives a gateway and database reload", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-agents-restart-"));
	const agentsPath = join(home, "workspace", "AGENTS.md");
	const seen: string[] = [];
	let gateway: Awaited<ReturnType<typeof startAgentsGateway>> | undefined;
	let client: Awaited<ReturnType<typeof connectLoopbackClient>> | undefined;
	let reopenedDatabase: GatewayDatabase | undefined;
	try {
		await mkdir(join(home, "workspace"), { recursive: true });
		await writeFile(agentsPath, "persisted baseline");
		gateway = await startAgentsGateway(home, seen);
		client = await connectLoopbackClient(gateway.config);
		await client.sendTurn("before-restart", "first turn");
		expect(agentsSectionCount(preambleAt(seen, 0))).toBe(0);
		const expectedDigest = createHash("sha256").update("persisted baseline").digest("hex");
		const sessionPort = gateway.sessionPort;
		const dbPath = gateway.config.dbPath;

		client.close();
		client = undefined;
		await gateway.server.stop();
		gateway = undefined;
		reopenedDatabase = await GatewayDatabase.open(dbPath);
		expect(reopenedDatabase.getSessionBootstrap(originKey(LOOPBACK_ORIGIN))).toMatchObject({
			agentsMdEpoch: 0,
			agentsMdDigest: expectedDigest,
		});

		await writeFile(agentsPath, "changed after restart");
		gateway = await startAgentsGateway(home, seen, { database: reopenedDatabase, sessionPort });
		reopenedDatabase = undefined;
		client = await connectLoopbackClient(gateway.config);
		await client.sendTurn("after-restart", "second turn");
		expect(agentsSectionCount(preambleAt(seen, 1))).toBe(1);
		expect(seen[1]).toContain(`${UPDATED_AGENTS_MD_HEADING}\nchanged after restart`);
		expect(seen[1]).not.toContain("persisted baseline");
	} finally {
		client?.close();
		await gateway?.server.stop();
		reopenedDatabase?.close();
		await rm(home, { recursive: true, force: true });
	}
});
