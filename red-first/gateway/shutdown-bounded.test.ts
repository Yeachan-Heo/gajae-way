/** a81bb27; fixture fake-gjc version 1: status:hang-30s, silent attached tail.
 * Failing assertion: expect(exitAtMs - t0).toBeLessThanOrEqual(15_000).
 * HEAD: >=30000ms (status CLI remains in flight); post-fix: <=15000ms.
 * Matched log: shutdown_hold origin=...; process remains alive. Real main.ts daemon subprocess, child.exited oracle; outer SIGKILL at 60s.
 */
import { expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { GatewayDatabase } from "../../packages/gateway/src/store/db";
import { eventually, KEY, ORIGIN } from "./harness";

test("red 2: SIGTERM exits an accepted-turn daemon within 15 seconds", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-red-shutdown-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const agentDir = join(home, "broker", database.instanceId, "agent");
	await mkdir(join(agentDir, "sdk"), { recursive: true });
	await mkdir(join(home, "bin"));
	await mkdir(join(home, "workspace"));
	await mkdir(join(home, ".gjc", "agent"), { recursive: true });
	await writeFile(join(home, ".gjc", "agent", "models.yml"), "providers: {}\n");
	await symlink(resolve("red-first/gateway/subprocess-gjc.mjs"), join(home, "bin", "gjc"));
	await chmod(resolve("red-first/gateway/subprocess-gjc.mjs"), 0o755);
	await writeFile(join(home, "config.json"), JSON.stringify({ schemaVersion: 1 }));
	const transport = Bun.serve({ hostname: "127.0.0.1", port: 0,
		fetch(request, server) { return server.upgrade(request) ? undefined : new Response("upgrade", { status: 400 }); },
		websocket: {
			open(socket) { socket.send(JSON.stringify({ type: "broker_hello", protocolVersion: 3 })); },
			message(socket, data) { const frame = JSON.parse(String(data)); socket.send(JSON.stringify({ type: "broker_response", id: frame.id, ok: true, result: { sessions: [] } })); },
		},
	});
	const discovery = () => writeFile(join(agentDir, "sdk", "broker.json"), JSON.stringify({ protocolVersion: 3, host: "127.0.0.1", url: `ws://127.0.0.1:${transport.port}`, token: "red-first", pid: process.pid, heartbeatAt: Date.now() }));
	await discovery();
	const heartbeat = setInterval(() => { void discovery(); }, 1000);
	const child = Bun.spawn({ cmd: [process.execPath, "packages/gateway/src/main.ts", "daemon"], cwd: resolve("."),
		env: { ...process.env, HOME: home, GAJAEWAY_HOME: home, PATH: `${home}/bin:${process.env.PATH}`, GJC_CODING_AGENT_DIR: agentDir,
			GAJAEWAY_FAKE_GJC_MODES: "status:hang-30s", RED_FIRST_CHILD_PIDS: join(home, "children"), RED_FIRST_STATUS_MARKER: join(home, "status-entered") },
		stdin: "ignore", stdout: "pipe", stderr: "pipe" });
	let output = "";
	const consume = async (stream: ReadableStream<Uint8Array>) => { for await (const chunk of stream) output += new TextDecoder().decode(chunk); };
	const readers = Promise.all([consume(child.stdout), consume(child.stderr)]);
	let socket: Awaited<ReturnType<typeof Bun.connect>> | undefined;
	const failSafe = setTimeout(() => child.kill("SIGKILL"), 60_000);
	try {
		for (let i = 0; i < 600; i++) {
			try { socket = await Bun.connect({ unix: join(home, "gateway.sock"), socket: { data() {} } }); break; }
			catch { if (child.exitCode !== null) throw new Error(`daemon exited before socket: ${output}`); await Bun.sleep(25); }
		}
		if (!socket) throw new Error(`gateway socket unavailable: ${output}`);
		socket.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
		socket.write(`${JSON.stringify({ v: "0.1", type: "request", id: "accepted", verb: "chat.send", params: { origin: ORIGIN, text: "remain accepted" } })}\n`);
		await eventually(() => database.inboundNonterminalTurns(KEY).some(row => row.state === "accepted"), `accepted turn not seeded: ${output}`, 15_000);
		const t0 = performance.now();
		child.kill("SIGTERM");
		await child.exited;
		const exitAtMs = performance.now();
		await readers;
		console.info("red2", { elapsedMs: exitAtMs - t0, exitCode: child.exitCode, output });
		expect(exitAtMs - t0).toBeLessThanOrEqual(15_000);
	} finally {
		clearTimeout(failSafe); clearInterval(heartbeat); socket?.end();
		if (child.exitCode === null) child.kill("SIGKILL");
		await child.exited;
		const pids = await readFile(join(home, "children"), "utf8").catch(() => "");
		for (const value of pids.trim().split("\n")) { if (!value) continue; try { process.kill(Number(value), "SIGKILL"); } catch {} }
		transport.stop(true); database.close(); await rm(home, { recursive: true, force: true });
	}
}, 70_000);
