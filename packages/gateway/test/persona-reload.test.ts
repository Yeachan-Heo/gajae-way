import { expect, test } from "bun:test";
import { lstat, mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { PersonaLoader, SELF_OPS_PREAMBLE_POINTER } from "../src/persona/persona";
import { startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { sessionPortFromResponder } from "./session-port.fake";

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
		respond: async (_id, _text, preamble) => {
			seen.push(preamble ?? "");
			return "reply";
		},
	});
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
		await expect(new PersonaLoader(home).ensureWorkspace()).rejects.toThrow("workspace_memory_path_escape");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
