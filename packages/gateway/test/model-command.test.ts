import { describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { GlobalGjcClient, parseListModelsOutput } from "../src/orchestrator/broker";
import {
	applyModelCommand,
	listModelChoices,
	ProviderModelCatalog,
	parseModelArgument,
} from "../src/server/model-command";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import type { GjcModelSelection } from "../src/store/groups/bindings";
import type { WriteTx } from "../src/store/tx";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

function store(initial?: GjcModelSelection) {
	let current = initial;
	const writes: GjcModelSelection[] = [];
	return {
		transaction: <T>(operation: (tx: WriteTx) => T) => operation(undefined as unknown as WriteTx),
		bindings: {
			modelGet: () => (current === undefined ? undefined : { selection: current }),
			modelSet: (_tx: WriteTx, _key: string, selection: GjcModelSelection) => {
				current = selection;
				writes.push(selection);
			},
			modelClear: (_tx: WriteTx) => {
				const had = current !== undefined;
				current = undefined;
				return had;
			},
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

describe("listModelChoices", () => {
	test("lists models.yml profile names plus the configured selector, sorted and deduped", async () => {
		const agentDir = await mkdtemp(join(tmpdir(), "gajaeway-model-choices-"));
		try {
			await Bun.write(
				join(agentDir, "models.yml"),
				[
					"providers:",
					"  local:",
					"    baseUrl: http://127.0.0.1",
					"profiles:",
					"  gpt-heavy:",
					"    model_mapping: { default: a/b }",
					"  frontier-default:",
					"    model_mapping: { default: c/d }",
					'  "has space":',
					"    model_mapping: { default: e/f }",
				].join("\n"),
			);
			expect(await listModelChoices(agentDir, { preset: "gpt-heavy" })).toEqual(["frontier-default", "gpt-heavy"]);
			expect(await listModelChoices(agentDir, "z-ai/glm-5.3")).toEqual([
				"frontier-default",
				"gpt-heavy",
				"z-ai/glm-5.3",
			]);
		} finally {
			await rm(agentDir, { recursive: true, force: true });
		}
	});

	test("fails soft on a missing, unparseable or profile-less models file", async () => {
		const agentDir = await mkdtemp(join(tmpdir(), "gajaeway-model-choices-"));
		try {
			expect(await listModelChoices(agentDir, undefined)).toEqual([]);
			expect(await listModelChoices(undefined, { preset: "base" })).toEqual(["base"]);
			await Bun.write(join(agentDir, "models.yml"), "profiles: [unterminated");
			expect(await listModelChoices(agentDir, { preset: "base" })).toEqual(["base"]);
			await Bun.write(join(agentDir, "models.yml"), "profiles:\n  - not-a-map\n");
			expect(await listModelChoices(agentDir, undefined)).toEqual([]);
		} finally {
			await rm(agentDir, { recursive: true, force: true });
		}
	});

	test("appends gjc-listed provider models after the named choices, deduped and sorted", async () => {
		const agentDir = await mkdtemp(join(tmpdir(), "gajaeway-model-choices-"));
		try {
			await Bun.write(
				join(agentDir, "models.yml"),
				["profiles:", "  zeta-preset:", "    model_mapping: { default: a/b }"].join("\n"),
			);
			const providerModels = [
				"xai/grok-4.7",
				"openai-codex/gpt-6-luna",
				"xai/grok-4.7",
				"z-ai/glm-5.3",
				"",
				"bad provider/model",
				"google-antigravity/gemini-4-pro",
			];
			// Named choices lead even when a provider id sorts before them, so an
			// empty query keeps presets inside Discord's 25-choice window.
			expect(await listModelChoices(agentDir, "z-ai/glm-5.3", providerModels)).toEqual([
				"z-ai/glm-5.3",
				"zeta-preset",
				"google-antigravity/gemini-4-pro",
				"openai-codex/gpt-6-luna",
				"xai/grok-4.7",
			]);
			// No provider listing keeps the pre-existing result exactly.
			expect(await listModelChoices(agentDir, "z-ai/glm-5.3")).toEqual(["z-ai/glm-5.3", "zeta-preset"]);
			expect(await listModelChoices(agentDir, "z-ai/glm-5.3", [])).toEqual(["z-ai/glm-5.3", "zeta-preset"]);
		} finally {
			await rm(agentDir, { recursive: true, force: true });
		}
	});

	test("still offers provider models when models.yml is missing or malformed", async () => {
		const agentDir = await mkdtemp(join(tmpdir(), "gajaeway-model-choices-"));
		try {
			expect(await listModelChoices(agentDir, undefined, ["xai/grok-4.7"])).toEqual(["xai/grok-4.7"]);
			await Bun.write(join(agentDir, "models.yml"), "profiles: [unterminated");
			expect(await listModelChoices(agentDir, { preset: "base" }, ["xai/grok-4.7"])).toEqual(["base", "xai/grok-4.7"]);
			expect(await listModelChoices(undefined, undefined, ["xai/grok-4.7"])).toEqual(["xai/grok-4.7"]);
		} finally {
			await rm(agentDir, { recursive: true, force: true });
		}
	});
});

// Shape of `gjc --list-models` (0.18.12). Every provider row gjc prints is
// taken as-is, with no gateway-side provider filtering; the canonical table is
// a second view of the same models and is ignored.
const LIST_MODELS_STDOUT = [
	"Canonical models",
	"canonical    selected                 variants  context  max-out",
	"grok-4.7     xai/grok-4.7             2         2M       64K    ",
	"",
	"Provider models",
	"provider            model                 context  max-out  thinking          images",
	"openai-codex        gpt-6-luna            400K     128K     low,medium,high   yes   ",
	"xai                 grok-4.7              2M       64K      low,medium,high   yes   ",
	"amazon-bedrock      anthropic.claude-3-5-haiku-20241022-v1:0  200K  8.2K  -  yes",
	"",
].join("\n");

describe("parseListModelsOutput", () => {
	test("reads provider/model ids from the provider table only", () => {
		expect(parseListModelsOutput(LIST_MODELS_STDOUT)).toEqual([
			"openai-codex/gpt-6-luna",
			"xai/grok-4.7",
			"amazon-bedrock/anthropic.claude-3-5-haiku-20241022-v1:0",
		]);
	});

	test("yields nothing when gjc has no available models or the table layout changed", () => {
		expect(parseListModelsOutput("No models available. Set API keys in environment variables.\n")).toEqual([]);
		expect(parseListModelsOutput('No models matching "zzz"\n')).toEqual([]);
		expect(parseListModelsOutput("Provider models\nname  id\nxai  grok-4.7\n")).toEqual([]);
		expect(parseListModelsOutput("")).toEqual([]);
	});
});

describe("GlobalGjcClient.listAvailableModels", () => {
	test("runs gjc --list-models as background work and parses the provider table", async () => {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-list-models-"));
		const calls: Array<{ args: readonly string[]; priority?: string }> = [];
		let result = { exitCode: 0, stdout: LIST_MODELS_STDOUT, stderr: "" };
		try {
			const broker = new GlobalGjcClient({
				executable: "/fake/gjc",
				agentDir: join(home, "agent"),
				cwd: home,
				command: async (args, options) => {
					calls.push({ args, priority: options?.priority });
					return result;
				},
			});
			expect(await broker.listAvailableModels()).toEqual([
				"openai-codex/gpt-6-luna",
				"xai/grok-4.7",
				"amazon-bedrock/anthropic.claude-3-5-haiku-20241022-v1:0",
			]);
			expect(calls).toEqual([{ args: ["--list-models"], priority: "background" }]);
			result = { exitCode: 1, stdout: "", stderr: "boom" };
			await expect(broker.listAvailableModels()).rejects.toThrow("gjc --list-models exited 1");
		} finally {
			await rm(home, { recursive: true, force: true });
		}
	});
});

describe("ProviderModelCatalog", () => {
	function clock(start = 1_000) {
		let at = start;
		return { now: () => at, advance: (ms: number) => (at += ms) };
	}

	test("serves a fresh listing from memory and shares one in-flight load", async () => {
		const time = clock();
		let loads = 0;
		const catalog = new ProviderModelCatalog(
			async () => {
				loads++;
				return ["xai/grok-4.7"];
			},
			{ ttlMs: 1_000, now: time.now },
		);
		expect(await Promise.all([catalog.list(), catalog.list(), catalog.list()])).toEqual([
			["xai/grok-4.7"],
			["xai/grok-4.7"],
			["xai/grok-4.7"],
		]);
		time.advance(999);
		expect(await catalog.list()).toEqual(["xai/grok-4.7"]);
		expect(loads).toBe(1);
	});

	test("answers stale while it refreshes after the TTL", async () => {
		const time = clock();
		let next = ["xai/grok-4.7"];
		let loads = 0;
		const catalog = new ProviderModelCatalog(
			async () => {
				loads++;
				return next;
			},
			{ ttlMs: 1_000, now: time.now },
		);
		expect(await catalog.list()).toEqual(["xai/grok-4.7"]);
		next = ["xai/grok-4.7", "xai/grok-5"];
		time.advance(1_000);
		expect(await catalog.list()).toEqual(["xai/grok-4.7"]);
		await Bun.sleep(1);
		expect(await catalog.list()).toEqual(["xai/grok-4.7", "xai/grok-5"]);
		expect(loads).toBe(2);
	});

	test("a cold listing slower than the wait answers empty, then lands for the next call", async () => {
		let release: ((models: readonly string[]) => void) | undefined;
		const catalog = new ProviderModelCatalog(
			() =>
				new Promise<readonly string[]>((resolve) => {
					release = resolve;
				}),
			{ waitMs: 5 },
		);
		expect(await catalog.list()).toEqual([]);
		release?.(["xai/grok-4.7"]);
		await Bun.sleep(1);
		expect(await catalog.list()).toEqual(["xai/grok-4.7"]);
	});

	test("fails soft: errors log, back off, and keep the last good listing", async () => {
		const time = clock();
		const logs: string[] = [];
		let loads = 0;
		let fail = true;
		const catalog = new ProviderModelCatalog(
			async () => {
				loads++;
				if (fail) throw new Error("gjc --list-models exited 1");
				return ["xai/grok-4.7"];
			},
			{ ttlMs: 1_000, retryMs: 100, now: time.now, log: (line) => logs.push(line) },
		);
		expect(await catalog.list()).toEqual([]);
		expect(logs).toEqual(["model_choices_provider_list_failed reason=gjc --list-models exited 1"]);
		// Inside the retry window a broken gjc is not respawned per keystroke.
		time.advance(99);
		expect(await catalog.list()).toEqual([]);
		expect(loads).toBe(1);
		fail = false;
		time.advance(1);
		expect(await catalog.list()).toEqual(["xai/grok-4.7"]);
		fail = true;
		time.advance(1_000);
		expect(await catalog.list()).toEqual(["xai/grok-4.7"]);
		await Bun.sleep(1);
		expect(await catalog.list()).toEqual(["xai/grok-4.7"]);
		expect(loads).toBe(3);
	});

	test("a loader that throws synchronously is contained", async () => {
		const logs: string[] = [];
		const catalog = new ProviderModelCatalog(
			() => {
				throw new Error("listAvailableModels is not a function");
			},
			{ log: (line) => logs.push(line) },
		);
		expect(await catalog.list()).toEqual([]);
		expect(logs).toHaveLength(1);
	});
});

async function waitFor(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 300; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

async function connect(
	socketPath: string,
): Promise<{ send(value: unknown): void; close(): void; frames: Array<Record<string, unknown>> }> {
	const frames: Array<Record<string, unknown>> = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, chunk) {
				buffered += chunk.toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line.trim()) frames.push(JSON.parse(line));
			},
		},
	});
	return { send: (value) => socket.write(`${JSON.stringify(value)}\n`), close: () => socket.end(), frames };
}

test("/model live-rebind keeps the session transcript and applies the new model to the next persistent turn", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-model-rebind-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		gjc: { agentDir: join(home, "agent") },
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		model: { preset: "base" },
		serviceTier: "priority",
	};
	const database = await Store.open(config.dbPath);
	const port = new ScriptedSessionPort();
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	let server: GatewayServer | undefined;
	let client: Awaited<ReturnType<typeof connect>> | undefined;
	const logs: string[] = [];
	const previousInfo = console.info;
	console.info = (...values: unknown[]) => logs.push(values.map((value) => String(value)).join(" "));
	try {
		server = await startUnixServer({ config, database, sessionPort: port, onStop: () => database.close() });
		client = await connect(config.socketPath);
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await Bun.sleep(5);
		// Autocomplete source over the socket: no broker agent dir here, so only the
		// configured selector is offered.
		client.send({ v: "0.1", type: "request", id: "choices", verb: "session.modelChoices" });
		const choicesClient = client;
		await waitFor(
			() => choicesClient.frames.some((frame) => frame.id === "choices"),
			"session.modelChoices did not answer",
		);
		expect(client.frames.find((frame) => frame.id === "choices")).toMatchObject({
			type: "response",
			result: { choices: ["base"] },
		});
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
		expect(database.bindings.getSessionRecord("loopback/loopback/model")).toMatchObject({
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
			console.info = previousInfo;
			if (!server) database.close();
			await rm(home, { recursive: true, force: true });
		}
	}
});

/** Starts a gateway with the given provider listing and reads `session.modelChoices` once. */
async function modelChoicesOverSocket(listProviderModels: () => Promise<readonly string[]>): Promise<unknown> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-model-choices-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		gjc: { agentDir: join(home, "agent") },
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		model: { preset: "base" },
	};
	const database = await Store.open(config.dbPath);
	const port = new ScriptedSessionPort();
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	let server: GatewayServer | undefined;
	let client: Awaited<ReturnType<typeof connect>> | undefined;
	try {
		server = await startUnixServer({
			config,
			database,
			sessionPort: port,
			onStop: () => database.close(),
			listProviderModels,
		});
		client = await connect(config.socketPath);
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await Bun.sleep(5);
		client.send({ v: "0.1", type: "request", id: "choices", verb: "session.modelChoices" });
		const connected = client;
		await waitFor(
			() => connected.frames.some((frame) => frame.id === "choices"),
			"session.modelChoices did not answer",
		);
		return connected.frames.find((frame) => frame.id === "choices");
	} finally {
		client?.close();
		try {
			await server?.stop();
		} finally {
			if (!server) database.close();
			await rm(home, { recursive: true, force: true });
		}
	}
}

test("session.modelChoices appends gjc-listed provider models to the named choices", async () => {
	expect(await modelChoicesOverSocket(async () => ["xai/grok-4.7", "openai-codex/gpt-6-luna"])).toMatchObject({
		type: "response",
		result: { choices: ["base", "openai-codex/gpt-6-luna", "xai/grok-4.7"] },
	});
});

test("session.modelChoices keeps the named choices when the provider listing fails", async () => {
	const previousError = console.error;
	const errors: string[] = [];
	console.error = (...values: unknown[]) => errors.push(values.map((value) => String(value)).join(" "));
	try {
		expect(
			await modelChoicesOverSocket(async () => {
				throw new Error("gjc --list-models exited 1");
			}),
		).toMatchObject({ type: "response", result: { choices: ["base"] } });
	} finally {
		console.error = previousError;
	}
	expect(errors).toContain("model_choices_provider_list_failed reason=gjc --list-models exited 1");
});
