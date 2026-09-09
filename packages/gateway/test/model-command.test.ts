import { describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { applyModelCommand, parseModelArgument } from "../src/server/model-command";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import type { GjcModelSelection } from "../src/store/db";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

function store(initial?: GjcModelSelection) {
	let current = initial;
	const writes: GjcModelSelection[] = [];
	return {
		conversationModelGet: () => (current === undefined ? undefined : { selection: current }),
		conversationModelSet: (_key: string, selection: GjcModelSelection) => {
			current = selection;
			writes.push(selection);
		},
		conversationModelClear: () => {
			const had = current !== undefined;
			current = undefined;
			return had;
		},
		writes,
		get current() {
			return current;
		},
	};
}

const CHANNEL = { platform: "discord" };

describe("parseModelArgument", () => {
	test("a bare name is a preset, a slashed name is an explicit selector", () => {
		expect(parseModelArgument("gpt-heavy")).toEqual({ preset: "gpt-heavy" });
		expect(parseModelArgument("z-ai/glm-5.3")).toBe("z-ai/glm-5.3");
	});

	test("prefixes force the interpretation", () => {
		expect(parseModelArgument("preset:frontier-heavy")).toEqual({ preset: "frontier-heavy" });
		// A provider selector with no slash would otherwise be read as a preset.
		expect(parseModelArgument("model:some-local-model")).toBe("some-local-model");
	});

	test("rejects empty and multi-token input", () => {
		expect(parseModelArgument("")).toEqual({ error: "empty selection" });
		expect(parseModelArgument("   ")).toEqual({ error: "empty selection" });
		expect(parseModelArgument("gpt heavy")).toEqual({ error: "a selection cannot contain spaces" });
	});
});

describe("/model show", () => {
	test("reports the conversation override and names it as such", () => {
		const s = store({ preset: "gpt-heavy" });
		const out = applyModelCommand("/model", "k", CHANNEL, s, "config-default");
		expect(out.text).toContain("preset gpt-heavy");
		expect(out.text).toContain("this conversation");
		expect(out.rebind).toBeUndefined();
	});

	test("falls back to the gateway default and says where it came from", () => {
		const out = applyModelCommand("/model show", "k", CHANNEL, store(), { preset: "frontier-default" });
		expect(out.text).toContain("preset frontier-default");
		expect(out.text).toContain("gateway default");
	});

	test("reports gjc's own default when nothing is configured", () => {
		const out = applyModelCommand("/model", "k", CHANNEL, store(), undefined);
		expect(out.text).toContain("gjc default");
	});
});

describe("/model set", () => {
	test("stores the selection and returns a same-session rebind intent", () => {
		const s = store();
		const out = applyModelCommand("/model set gpt-heavy", "k", CHANNEL, s, undefined);
		expect(s.current).toEqual({ preset: "gpt-heavy" });
		expect(out.rebind).toEqual({ kind: "set", selection: { preset: "gpt-heavy" } });
		expect(out.text).toContain("same session");
	});

	test("accepts the bare form without the set keyword", () => {
		const s = store();
		const out = applyModelCommand("/model glm-gpt", "k", CHANNEL, s, undefined);
		expect(s.current).toEqual({ preset: "glm-gpt" });
		expect(out.rebind).toEqual({ kind: "set", selection: { preset: "glm-gpt" } });
	});

	test("a bad selection changes nothing and explains the usage", () => {
		const s = store({ preset: "gpt-heavy" });
		const out = applyModelCommand("/model set two words", "k", CHANNEL, s, undefined);
		expect(s.writes).toHaveLength(0);
		expect(s.current).toEqual({ preset: "gpt-heavy" });
		expect(out.rebind).toBeUndefined();
		expect(out.text).toContain("/model set");
	});

	test("replaces an existing override rather than stacking", () => {
		const s = store({ preset: "glm-gpt" });
		applyModelCommand("/model set frontier-heavy", "k", CHANNEL, s, undefined);
		expect(s.current).toEqual({ preset: "frontier-heavy" });
	});
});

describe("/model clear", () => {
	test("removes an override and returns a clear rebind intent", () => {
		const s = store({ preset: "gpt-heavy" });
		const out = applyModelCommand("/model clear", "k", CHANNEL, s, "config-default");
		expect(s.current).toBeUndefined();
		expect(out.rebind).toEqual({ kind: "clear" });
		expect(out.text).toContain("config-default");
		expect(out.text).toContain("same session");
	});

	test("does not erase a live override when no concrete default can be applied", () => {
		const s = store({ preset: "gpt-heavy" });
		const out = applyModelCommand("/model clear", "k", CHANNEL, s, undefined);
		expect(s.current).toEqual({ preset: "gpt-heavy" });
		expect(out.rebind).toBeUndefined();
		expect(out.text).toContain("cannot live-clear");
	});

	test("clearing nothing does not claim a reset that did not happen", () => {
		const out = applyModelCommand("/model clear", "k", CHANNEL, store(), undefined);
		expect(out.rebind).toBeUndefined();
		expect(out.text).toContain("no conversation override");
	});
});

async function waitFor(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 300; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

async function connect(socketPath: string): Promise<{ send(value: unknown): void; close(): void }> {
	let socket!: ReturnType<typeof Bun.connect> extends Promise<infer T> ? T : never;
	socket = await Bun.connect({ unix: socketPath, socket: { data() {} } });
	return { send: (value) => socket.write(`${JSON.stringify(value)}\n`), close: () => socket.end() };
}

test("/model live-rebind keeps the session transcript and applies the new model to the next persistent turn", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-model-rebind-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		model: { preset: "base" },
		serviceTier: "priority",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const port = new ScriptedSessionPort();
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	let server: GatewayServer | undefined;
	let client: { send(value: unknown): void; close(): void } | undefined;
	const logs: string[] = [];
	const previousError = console.error;
	console.error = (...values: unknown[]) => logs.push(values.map((value) => String(value)).join(" "));
	try {
		server = await startUnixServer({ config, database, sessionPort: port, onStop: () => database.close() });
		client = await connect(config.socketPath);
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await Bun.sleep(5);
		const origin = { platform: "loopback", kind: "loopback", conversationId: "model" };
		client.send({
			v: "0.1",
			type: "request",
			id: "first",
			verb: "chat.send",
			params: { origin, messageId: "m-1", text: "first" },
		});
		await waitFor(() => port.sends.length === 1, "first persistent turn did not start");
		const first = port.sends[0]!;
		expect(port.serviceTiers).toEqual([
			{ sessionId: first.sessionId, repo: join(home, "workspace"), tier: "priority" },
		]);
		port.complete(first.opRef, "first transcript");
		await waitFor(
			() => port.transcript(first.sessionId).includes("first transcript"),
			"first transcript was not retained",
		);

		client.send({
			v: "0.1",
			type: "request",
			id: "model",
			verb: "chat.send",
			params: { origin, text: "/model set next" },
		});
		await waitFor(
			() =>
				port.models.some(
					(entry) => entry.selection && typeof entry.selection !== "string" && entry.selection.preset === "next",
				),
			"live model.set was not issued",
		);
		expect(database.getSessionRecord("loopback/loopback/model")).toMatchObject({
			epoch: 0,
			sessionId: first.sessionId,
		});
		expect(port.transcript(first.sessionId)).toContain("first transcript");
		expect(
			logs.some((line) =>
				line.includes(
					`persona_model origin=loopback/loopback/model epoch=0 session=${first.sessionId} effective=preset:next changed=true source=/model`,
				),
			),
		).toBe(true);

		client.send({
			v: "0.1",
			type: "request",
			id: "second",
			verb: "chat.send",
			params: { origin, messageId: "m-2", text: "second" },
		});
		await waitFor(() => port.sends.length === 2, "next persistent turn did not start");
		const second = port.sends[1]!;
		expect(second.sessionId).toBe(first.sessionId);
		expect(second.model).toBeUndefined();
		expect(port.models.at(-1)).toMatchObject({ sessionId: first.sessionId, selection: { preset: "next" } });
		expect(port.serviceTiers).toHaveLength(1);
		port.complete(second.opRef, "second transcript");
		client.send({
			v: "0.1",
			type: "request",
			id: "clear",
			verb: "chat.send",
			params: { origin, text: "/model clear" },
		});
		await waitFor(() => {
			const selection = port.models.at(-1)?.selection;
			return typeof selection !== "string" && selection?.preset === "base";
		}, "clearing the override did not restore the configured model on the same session");
		client.send({
			v: "0.1",
			type: "request",
			id: "third",
			verb: "chat.send",
			params: { origin, messageId: "m-3", text: "third" },
		});
		await waitFor(() => port.sends.length === 3, "cleared-model persistent turn did not start");
		expect(port.sends[2]?.sessionId).toBe(first.sessionId);
		port.complete(port.sends[2]!.opRef, "third transcript");
	} finally {
		client?.close();
		try {
			await server?.stop();
		} finally {
			console.error = previousError;
			if (!server) database.close();
			await rm(home, { recursive: true, force: true });
		}
	}
});
