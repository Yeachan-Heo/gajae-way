import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import type { GjcPort } from "../src/orchestrator/gjc-client";
import { startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";

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
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "session" }),
		forgetRebinds: () => {},
		sendTurn: async (_id, _text, preamble) => {
			seen.push(preamble ?? "");
			return "reply";
		},
	};
	const server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	try {
		await mkdir(join(home, "workspace"), { recursive: true });
		await Bun.write(join(home, "workspace/USER.md"), "first");
		const frames: any[] = [];
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
		await wait(5);
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
