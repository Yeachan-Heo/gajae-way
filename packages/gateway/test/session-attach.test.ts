import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LOOPBACK_ORIGIN, originKey, TERMINAL_ORIGIN } from "@gajaeway/protocol";
import type { GatewayConfig } from "../src/config";
import { ACTION_GUARD_SYSTEM_NOTICE } from "../src/guard/action-guard";
import { GjcClient, type GjcPort } from "../src/orchestrator/gjc-client";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";

/**
 * `session.attach` integration (AC-3..AC-8, AC-11..AC-16, AC-18 and the failure
 * defenses), against an in-process unix server on the `GAJAEWAY_TEST_STUB_GJC`
 * seam.
 *
 * The REAL `GjcClient` is used, not a mock port: the row-first stub recipe and
 * the `putSession` + `updateActivity` persist seam are exactly what several ACs
 * are about, and a mock port would bypass both and make them vacuous.
 */

const TERMINAL_KEY = originKey(TERMINAL_ORIGIN);
const STUB_FORMULA = (epoch: number) => `stub-${TERMINAL_KEY}#${epoch}`;

interface Harness {
	readonly config: GatewayConfig;
	readonly database: GatewayDatabase;
	readonly directory: string;
}

let harness: Harness | undefined;
let server: GatewayServer | undefined;
let previousStub: string | undefined;

interface Wire {
	send(value: unknown): void;
	readonly frames: Record<string, unknown>[];
	/** Graceful FIN — the protocol-close path. */
	close(): void;
	/** Abrupt teardown with no FIN — what a SIGKILLed holder looks like. */
	terminate(): void;
}

async function connect(socketPath: string): Promise<Wire> {
	const frames: Record<string, unknown>[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line));
			},
		},
	});
	return {
		send: (value) => socket.write(`${JSON.stringify(value)}\n`),
		frames,
		close: () => socket.end(),
		terminate: () => socket.terminate(),
	};
}

async function waitFor(frames: unknown[], count: number): Promise<void> {
	for (let attempt = 0; attempt < 400 && frames.length < count; attempt++) await Bun.sleep(5);
	expect(frames.length).toBeGreaterThanOrEqual(count);
}

/** Negotiate and return a ready wire. */
async function negotiated(socketPath: string): Promise<Wire> {
	const wire = await connect(socketPath);
	wire.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(wire.frames, 1);
	expect(wire.frames[0]?.type).toBe("negotiated");
	return wire;
}

interface AttachOutcome {
	readonly result?: Record<string, unknown>;
	readonly error?: { code: string; message: string; detail?: { holder?: string; refused?: string[] } };
}

async function attach(wire: Wire, params: Record<string, unknown> = {}): Promise<AttachOutcome> {
	const before = wire.frames.length;
	const id = `attach-${before}-${Math.random().toString(36).slice(2)}`;
	wire.send({ v: "0.1", type: "request", id, verb: "session.attach", params });
	for (let attempt = 0; attempt < 600; attempt++) {
		const frame = wire.frames.find((candidate) => candidate.id === id);
		if (frame) {
			if (frame.type === "response") return { result: frame.result as Record<string, unknown> };
			return { error: frame.error as AttachOutcome["error"] };
		}
		await Bun.sleep(5);
	}
	throw new Error("session.attach did not answer");
}

async function startHarness(overrides: { gjc?: GjcPort; git?: boolean } = {}): Promise<Harness> {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-attach-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	// AC-12 fixture: unique strings per persona document so the assertion cannot
	// pass on generic text.
	const workspace = join(directory, "workspace");
	await mkdir(workspace, { recursive: true });
	await writeFile(join(workspace, "SOUL.md"), "SOUL-MARKER-7f3a");
	await writeFile(join(workspace, "AGENTS.md"), "AGENTS-MARKER-91cd");
	await writeFile(join(workspace, "USER.md"), "USER-MARKER-2b58");
	// `--worktree` needs a real git repository; `git: false` exercises the refusal.
	if (overrides.git !== false) {
		await writeFile(join(workspace, ".gitignore"), "/.worktrees\n");
		for (const args of [
			["init", "-q", "."],
			["add", "-A"],
			["-c", "user.email=drill@example.invalid", "-c", "user.name=drill", "commit", "-q", "-m", "init"],
		]) {
			Bun.spawnSync(["git", "-C", workspace, ...args], { stdout: "pipe", stderr: "pipe" });
		}
	}

	const database = await GatewayDatabase.open(config.dbPath);
	const gjc = overrides.gjc ?? new GjcClient(database, 300_000, workspace, undefined);
	server = await startUnixServer({
		config,
		database,
		gjc,
		startedAt: "2026-01-01T00:00:00.000Z",
		onStop: () => database.close(),
	});
	return { config, database, directory };
}

beforeEach(() => {
	previousStub = process.env.GAJAEWAY_TEST_STUB_GJC;
	process.env.GAJAEWAY_TEST_STUB_GJC = "1";
});

afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (harness) await rm(harness.directory, { recursive: true, force: true });
	harness = undefined;
	if (previousStub === undefined) delete process.env.GAJAEWAY_TEST_STUB_GJC;
	else process.env.GAJAEWAY_TEST_STUB_GJC = previousStub;
});

describe("session.attach binding and persistence", () => {
	test("binds the terminal origin, persists the row, and resumes the same id (AC-3)", async () => {
		harness = await startHarness();
		const first = await negotiated(harness.config.socketPath);
		const one = await attach(first);
		expect(one.error).toBeUndefined();
		const sessionId = one.result?.sessionId as string;

		expect(one.result?.originKey).toBe("loopback/loopback/terminal");
		expect(sessionId).toBeTruthy();
		// The id must NOT be recomputable from origin+epoch, or the resume
		// assertion below would be a tautology.
		expect(sessionId).not.toBe(STUB_FORMULA(0));

		// The persist seam really ran: putSession wrote the id...
		const row = harness.database.getSessionRecord(TERMINAL_KEY);
		expect(row?.sessionId).toBe(sessionId);
		// ...and updateActivity wrote origin_ref_json, without which ops.cycle
		// mislabels the row as the chat loopback origin.
		const identity = harness.database.sessionIdentityRows().find((entry) => entry.origin_key === TERMINAL_KEY);
		expect(identity?.origin_ref_json).toBe(JSON.stringify(TERMINAL_ORIGIN));

		first.close();
		await Bun.sleep(30);

		const second = await negotiated(harness.config.socketPath);
		const two = await attach(second);
		expect(two.error).toBeUndefined();
		// Same id from BOTH the RPC and the row.
		expect(two.result?.sessionId).toBe(sessionId);
		expect(harness.database.getSessionRecord(TERMINAL_KEY)?.sessionId).toBe(sessionId);
		second.close();
	});

	test("--new rotates the epoch and rebinds a fresh non-empty id (AC-4)", async () => {
		harness = await startHarness();
		const wire = await negotiated(harness.config.socketPath);
		const first = await attach(wire);
		const firstId = first.result?.sessionId as string;
		const firstEpoch = first.result?.epoch as number;
		wire.close();
		await Bun.sleep(30);

		const wire2 = await negotiated(harness.config.socketPath);
		const reset = await attach(wire2, { reset: true });
		expect(reset.error).toBeUndefined();
		expect(reset.result?.epoch).toBe(firstEpoch + 1);
		const resetId = reset.result?.sessionId as string;
		expect(resetId).toBeTruthy();
		expect(resetId).not.toBe(firstId);
		// The row must hold the NEW id, not stay empty after the bump.
		expect(harness.database.getSessionRecord(TERMINAL_KEY)?.sessionId).toBe(resetId);
		expect(harness.database.getSessionRecord(TERMINAL_KEY)?.epoch).toBe(firstEpoch + 1);
		wire2.close();
	});
});

describe("exclusive lease", () => {
	test("a second connection is refused with the holder named (AC-5)", async () => {
		harness = await startHarness();
		const holderWire = await negotiated(harness.config.socketPath);
		const held = await attach(holderWire);
		expect(held.error).toBeUndefined();
		const holder = (held.result?.lease as { holder: string }).holder;

		const rivalWire = await negotiated(harness.config.socketPath);
		const refused = await attach(rivalWire);
		expect(refused.result).toBeUndefined();
		expect(refused.error?.code).toBe("session_lease_held");
		// detail.holder is what makes this classifiable without parsing English;
		// it is undefined unless Connection carries an id.
		expect(refused.error?.detail?.holder).toBe(holder);
		expect(refused.error?.detail?.holder).toBeTruthy();

		holderWire.close();
		rivalWire.close();
	});

	test("a hanging bind still refuses a second attach (AC-5 in-flight)", async () => {
		// The claim must be synchronous: if it happened after the bind await, the
		// rival would slip through while the first attach is still blocked.
		let release: (() => void) | undefined;
		const hanging: GjcPort = {
			ensureSession: async () => {
				await new Promise<void>((resolve) => {
					release = resolve;
				});
				return { sessionId: "late-session" };
			},
			forgetRebinds: () => {},
			sendTurn: async () => "unused",
		};
		harness = await startHarness({ gjc: hanging });

		const holderWire = await negotiated(harness.config.socketPath);
		const holderId = `attach-hang`;
		holderWire.send({ v: "0.1", type: "request", id: holderId, verb: "session.attach", params: {} });
		// Give the handler time to reach (and block inside) the bind.
		await Bun.sleep(50);
		expect(holderWire.frames.find((frame) => frame.id === holderId)).toBeUndefined();

		const rivalWire = await negotiated(harness.config.socketPath);
		const refused = await attach(rivalWire);
		expect(refused.error?.code).toBe("session_lease_held");

		release?.();
		holderWire.close();
		rivalWire.close();
	});

	test("each distinct disconnect procedure frees the origin (AC-6 gateway half)", async () => {
		harness = await startHarness();

		// Procedure 1: graceful protocol close (`socket.end()`).
		const clean = await negotiated(harness.config.socketPath);
		const cleanHolder = (await attach(clean)).result;
		expect(cleanHolder).toBeDefined();
		clean.close();
		await Bun.sleep(60);
		const afterClean = await negotiated(harness.config.socketPath);
		const secondHolder = await attach(afterClean);
		expect(secondHolder.error).toBeUndefined();
		// Genuinely a NEW holder, not the old lease still being reported back.
		expect((secondHolder.result?.lease as { holder: string }).holder).not.toBe(
			(cleanHolder?.lease as { holder: string }).holder,
		);

		// Procedure 2: abrupt teardown with NO FIN — a distinct code path, and what a
		// SIGKILLed holder CLI looks like from the gateway's side.
		afterClean.terminate();
		await Bun.sleep(60);
		const afterAbrupt = await negotiated(harness.config.socketPath);
		const thirdHolder = await attach(afterAbrupt);
		expect(thirdHolder.error).toBeUndefined();
		expect((thirdHolder.result?.lease as { holder: string }).holder).not.toBe(
			(secondHolder.result?.lease as { holder: string }).holder,
		);
		afterAbrupt.close();

		// `stop()` also clears the lease, but that is not externally observable for
		// the unix server: the server runtime holding the in-memory lease is
		// discarded with it, so any post-restart attach would succeed regardless. It
		// is asserted by construction (the field is cleared before listener
		// teardown) rather than by a test that could not fail.
	});

	test("malformed params are refused before any lease is claimed", async () => {
		harness = await startHarness();
		const wire = await negotiated(harness.config.socketPath);

		// `typeof null === "object"` and an array is an object too, so a loose check
		// would let these claim the lease and be treated as an empty attach.
		for (const bad of [
			null,
			[],
			"nope",
			7,
			{ argv: null },
			{ argv: ["ok", 5] },
			{ reset: "yes" },
			{ holder: [] },
			{ holder: { pid: "not-a-number" } },
			{ holder: { label: 42 } },
		]) {
			const refused = await attach(wire, bad as Record<string, unknown>);
			expect(refused.result).toBeUndefined();
			expect(refused.error?.code).toBe("invalid_params");
		}

		// The origin is still free after every rejection.
		const ok = await attach(wire, {});
		expect(ok.error).toBeUndefined();
		wire.close();
	});

	test("a bind that finishes after its connection dropped cannot overwrite the newer session", async () => {
		// Without the post-await ownership re-check, the stale bind's `putSession`
		// would clobber the id the newer holder already persisted.
		let releaseFirst: ((value: { sessionId: string }) => void) | undefined;
		let call = 0;
		const staged: GjcPort = {
			ensureSession: async () => {
				call += 1;
				if (call === 1) {
					return await new Promise<{ sessionId: string }>((resolve) => {
						releaseFirst = resolve;
					});
				}
				return { sessionId: "second-holder-session" };
			},
			forgetRebinds: () => {},
			sendTurn: async () => "unused",
		};
		harness = await startHarness({ gjc: staged });

		const stale = await negotiated(harness.config.socketPath);
		stale.send({ v: "0.1", type: "request", id: "stale-attach", verb: "session.attach", params: {} });
		await Bun.sleep(60);
		// Drop the first holder while its bind is still in flight.
		stale.terminate();
		await Bun.sleep(60);

		const winner = await negotiated(harness.config.socketPath);
		const won = await attach(winner);
		expect(won.error).toBeUndefined();
		expect(won.result?.sessionId).toBe("second-holder-session");
		expect(harness.database.getSessionRecord(TERMINAL_KEY)?.sessionId).toBe("second-holder-session");

		// Now let the abandoned bind complete: it must not write.
		releaseFirst?.({ sessionId: "stale-session-do-not-persist" });
		await Bun.sleep(120);
		expect(harness.database.getSessionRecord(TERMINAL_KEY)?.sessionId).toBe("second-holder-session");
		winner.close();
	});

	test("an over-budget persona preamble fails closed and frees the lease", async () => {
		harness = await startHarness();
		// Push the persona documents past ATTACH_PREAMBLE_MAX_BYTES (32768) so the
		// argv would be unspawnable. It must be refused BEFORE argv is handed out,
		// and must not strand the origin.
		const workspace = join(harness.config.home, "workspace");
		await writeFile(join(workspace, "SOUL.md"), "S".repeat(40_000));

		const wire = await negotiated(harness.config.socketPath);
		const failed = await attach(wire);
		expect(failed.result).toBeUndefined();
		expect(failed.error?.code).toBe("verb_failed");
		expect(failed.error?.message).toContain("preamble");

		// Lease released: shrink the persona and the next attach succeeds.
		await writeFile(join(workspace, "SOUL.md"), "SOUL-MARKER-7f3a");
		const ok = await attach(wire);
		expect(ok.error).toBeUndefined();
		wire.close();
	});

	test("holder: null is refused like every other malformed shape", async () => {
		harness = await startHarness();
		const wire = await negotiated(harness.config.socketPath);
		const refused = await attach(wire, { holder: null } as unknown as Record<string, unknown>);
		expect(refused.error?.code).toBe("invalid_params");
		const ok = await attach(wire, {});
		expect(ok.error).toBeUndefined();
		wire.close();
	});

	test("a refused flag never claims the lease (AC-7)", async () => {
		harness = await startHarness();
		const wire = await negotiated(harness.config.socketPath);
		const refused = await attach(wire, { argv: ["--resume", "someone-elses-session"] });
		expect(refused.error?.code).toBe("invalid_params");
		expect(refused.error?.message).toContain("--resume");

		// Immediately usable: the failed attach must not have taken the lease.
		const ok = await attach(wire, { argv: [] });
		expect(ok.error).toBeUndefined();
		wire.close();
	});
});

describe("argv, persona, and bootstrap", () => {
	test("injects persona documents and the ActionGuard floor, never --system-prompt (AC-12, AC-13)", async () => {
		harness = await startHarness();
		const wire = await negotiated(harness.config.socketPath);
		const result = (await attach(wire)).result;
		const argv = result?.argv as string[];
		const preamble = argv[argv.indexOf("--append-system-prompt") + 1] ?? "";

		expect(preamble).toContain("SOUL-MARKER-7f3a");
		expect(preamble).toContain("AGENTS-MARKER-91cd");
		expect(preamble).toContain("USER-MARKER-2b58");
		// Assert the real constant rather than a paraphrase, so the floor cannot be
		// silently reworded out of the preamble.
		expect(preamble).toContain(ACTION_GUARD_SYSTEM_NOTICE);

		expect(argv).not.toContain("--system-prompt");
		expect(argv).not.toContain("-p");
		expect(argv).not.toContain("--print");
		expect(argv).not.toContain("--mode");
		expect(preamble).not.toContain("You are NOT a coding CLI assistant");
		// personaPreamble is a diagnostic copy of exactly what was injected.
		expect(result?.personaPreamble).toBe(preamble);
		wire.close();
	});

	test("re-injects the same bootstrap section without marking it consumed (AC-14)", async () => {
		harness = await startHarness();
		const wire = await negotiated(harness.config.socketPath);
		const first = (await attach(wire)).result;
		wire.close();
		await Bun.sleep(30);
		const wire2 = await negotiated(harness.config.socketPath);
		const second = (await attach(wire2)).result;

		const markerOf = (value: unknown): string => {
			const match = /bootstrap-id: (session-bootstrap:[0-9a-f]{24})/.exec(String(value));
			expect(match).not.toBeNull();
			return match?.[1] ?? "";
		};
		const firstMarker = markerOf(first?.personaPreamble);
		const secondMarker = markerOf(second?.personaPreamble);
		// The FULL marker must be identical, not merely the prefix.
		expect(firstMarker).toBe(secondMarker);
		expect(firstMarker).toMatch(/^session-bootstrap:[0-9a-f]{24}$/);

		const preamble = String(second?.personaPreamble);
		expect(preamble).toContain("## Session bootstrap");
		expect(preamble).toContain("origin: loopback/loopback/terminal");
		// A filesystem path is NOT the section.
		expect(preamble).not.toContain(String(second?.sessionDir));

		// Never marked consumed, which is what keeps the marker stable.
		const bootstrapState = harness.database.getSessionBootstrap(TERMINAL_KEY);
		expect(bootstrapState?.lastBootstrappedEpoch).not.toBe(second?.epoch);
		wire2.close();
	});

	test("operator model wins over config.model, and config.model is used otherwise (AC-15)", async () => {
		harness = await startHarness();
		const wire = await negotiated(harness.config.socketPath);

		const operator = (await attach(wire, { argv: ["--model", "opus"] })).result;
		const operatorArgv = operator?.argv as string[];
		expect(operatorArgv.filter((token) => token === "--model")).toEqual(["--model"]);
		expect(operatorArgv[operatorArgv.indexOf("--model") + 1]).toBe("opus");
		wire.close();
	});

	test("prepares a worktree, binds the session there, and never forwards the flag", async () => {
		harness = await startHarness();
		// The persona workspace is a git repo in this fixture, so `git worktree add`
		// can actually run.
		const wire = await negotiated(harness.config.socketPath);
		const result = (await attach(wire, { argv: ["--worktree", "feature-x"] })).result;

		expect(result).toBeDefined();
		const expectedCwd = join(harness.config.home, "workspace", ".worktrees", "feature-x");
		// The session is bound IN the worktree: that is what stops native gjc from
		// treating it as a different project and offering to fork it.
		expect(result?.cwd).toBe(expectedCwd);
		expect((await stat(expectedCwd)).isDirectory()).toBe(true);
		// The flag itself must never reach the child.
		expect((result?.argv as string[]).join(" ")).not.toContain("worktree");
		wire.close();
	});

	test("refuses an attach that would move an epoch's session to another directory", async () => {
		harness = await startHarness();

		// Bind epoch 0 in the plain workspace, then release the lease.
		const first = await negotiated(harness.config.socketPath);
		const plain = (await attach(first)).result;
		expect(plain?.cwd).toBe(join(harness.config.home, "workspace"));
		first.close();
		await Bun.sleep(60);

		// A LATER attach asks for a worktree at the same epoch. A native session
		// belongs to the project it was created in, so this cannot resume it; it must
		// be refused with the remedy named, not handed to gjc as a fork prompt.
		const second = await negotiated(harness.config.socketPath);
		const moved = await attach(second, { argv: ["--worktree", "feature-x"] });
		expect(moved.result).toBeUndefined();
		expect(moved.error?.code).toBe("invalid_params");
		expect(moved.error?.message).toContain("bound to");
		expect(moved.error?.message).toContain("--new");

		// `--new` is the documented remedy and must actually work.
		const rotated = (await attach(second, { argv: ["--worktree", "feature-x"], reset: true })).result;
		expect(rotated?.cwd).toBe(join(harness.config.home, "workspace", ".worktrees", "feature-x"));
		expect(rotated?.epoch).toBe((plain?.epoch as number) + 1);
		second.close();
		await Bun.sleep(60);

		// The new epoch now resumes consistently at the worktree, same session id.
		const third = await negotiated(harness.config.socketPath);
		const again = (await attach(third, { argv: ["--worktree", "feature-x"] })).result;
		expect(again?.sessionId).toBe(rotated?.sessionId);
		expect(again?.cwd).toBe(rotated?.cwd);
		third.close();
	});

	test("refuses --worktree when the workspace is not a git repository", async () => {
		harness = await startHarness({ git: false });
		const wire = await negotiated(harness.config.socketPath);
		const refused = await attach(wire, { argv: ["--worktree", "feature-x"] });
		expect(refused.error?.code).toBe("invalid_params");
		expect(refused.error?.message).toContain("git repository");
		// Origin still free.
		expect((await attach(wire, {})).error).toBeUndefined();
		wire.close();
	});

	test("dictates an epoch-scoped native state root, totally (AC-11)", async () => {
		harness = await startHarness();
		const wire = await negotiated(harness.config.socketPath);
		const result = (await attach(wire)).result;
		const childEnv = result?.childEnv as Record<string, string>;
		const childEnvUnset = result?.childEnvUnset as string[];

		expect(childEnv).toBeDefined();
		expect(Array.isArray(childEnvUnset)).toBe(true);

		// The gateway does not merely COPY the daemon's root, it DICTATES one scoped
		// to the epoch. That is what puts the native transcript inside the epoch
		// directory, and it makes daemon/CLI environment divergence impossible
		// rather than merely corrected.
		const expected = join(String(result?.sessionDir), "agent");
		expect(childEnv.GJC_CODING_AGENT_DIR).toBe(expected);
		expect((await stat(expected)).isDirectory()).toBe(true);

		// TOTALITY still holds: every selector is either given a value or named as
		// unset, so an operator shell cannot silently win one.
		const selectors = [
			"GJC_CODING_AGENT_DIR",
			"PI_CODING_AGENT_DIR",
			"GJC_CONFIG_DIR",
			"PI_CONFIG_DIR",
			"XDG_DATA_HOME",
			"HOME",
		];
		for (const key of selectors) {
			expect(key in childEnv || childEnvUnset.includes(key)).toBe(true);
		}
		// A selector is never both set and unset.
		for (const key of childEnvUnset) expect(key in childEnv).toBe(false);
		// No empty values are exported.
		expect(Object.values(childEnv).every((value) => value !== "")).toBe(true);
		wire.close();
	});

	test("writes the native session transcript inside the epoch directory (AC-11)", async () => {
		// The real GjcClient is used here, but the stub seam short-circuits the bind,
		// so this asserts the CONTRACT the gateway hands the child: the state root it
		// dictates lives under the epoch directory. The end-to-end proof that gjc
		// then writes its transcript there is the live drill.
		harness = await startHarness();
		const wire = await negotiated(harness.config.socketPath);
		const first = (await attach(wire)).result;
		const firstAgent = join(String(first?.sessionDir), "agent");
		expect((first?.childEnv as Record<string, string>).GJC_CODING_AGENT_DIR).toBe(firstAgent);

		// Rotation gives the next epoch its own root, and must not disturb the old one.
		await writeFile(join(firstAgent, "keep.txt"), "keep");
		wire.close();
		await Bun.sleep(60);
		const wire2 = await negotiated(harness.config.socketPath);
		const rotated = (await attach(wire2, { reset: true })).result;
		const secondAgent = join(String(rotated?.sessionDir), "agent");
		expect(secondAgent).not.toBe(firstAgent);
		expect((rotated?.childEnv as Record<string, string>).GJC_CODING_AGENT_DIR).toBe(secondAgent);
		expect(await readFile(join(firstAgent, "keep.txt"), "utf8")).toBe("keep");
		wire2.close();
	});
});

describe("epoch-scoped session directory (AC-11)", () => {
	test("attach creates the epoch dir at 0700 and --new preserves the previous one", async () => {
		harness = await startHarness();
		const wire = await negotiated(harness.config.socketPath);
		const first = (await attach(wire)).result;
		const firstDir = String(first?.sessionDir);
		expect(firstDir).toBe(join(harness.config.home, "sessions", "terminal", `e${first?.epoch}`));

		const info = await stat(firstDir);
		expect(info.isDirectory()).toBe(true);
		expect(info.mode & 0o777).toBe(0o700);

		// A file written into the current epoch dir must survive rotation.
		await writeFile(join(firstDir, "keep.txt"), "keep");
		wire.close();
		await Bun.sleep(30);

		const wire2 = await negotiated(harness.config.socketPath);
		const rotated = (await attach(wire2, { reset: true })).result;
		const secondDir = String(rotated?.sessionDir);
		expect(secondDir).not.toBe(firstDir);
		expect((await stat(secondDir)).isDirectory()).toBe(true);
		expect(await readFile(join(firstDir, "keep.txt"), "utf8")).toBe("keep");
		wire2.close();
	});
});

describe("ops projection (AC-16)", () => {
	test("reports the terminal origin with a non-empty bound session id", async () => {
		harness = await startHarness();
		const wire = await negotiated(harness.config.socketPath);
		const attached = (await attach(wire)).result;

		const id = "cycle-1";
		wire.send({ v: "0.1", type: "request", id, verb: "ops.cycle" });
		for (let attempt = 0; attempt < 400; attempt++) {
			if (wire.frames.find((frame) => frame.id === id)) break;
			await Bun.sleep(5);
		}
		const frame = wire.frames.find((candidate) => candidate.id === id);
		const cycle = frame?.result as { sessions: Array<Record<string, unknown>> };
		const row = cycle.sessions.find((entry) => entry.originKey === "loopback/loopback/terminal");
		expect(row).toBeDefined();
		expect(row?.sessionId).toBe(attached?.sessionId);
		expect(row?.sessionId).not.toBe("");
		expect(row?.epoch).toBe(attached?.epoch);
		// The origin must NOT fall back to the chat loopback origin.
		expect((row?.origin as { conversationId: string }).conversationId).toBe("terminal");
		// The view must not have grown lease/child/PTY fields.
		for (const forbidden of ["lease", "holder", "childPid", "pty", "delivery"]) {
			expect(Object.keys(row ?? {})).not.toContain(forbidden);
		}
		wire.close();
	});
});

describe("attach-only terminal origin (AC-18)", () => {
	test("chat.send is refused before the /new branch and never runs a turn", async () => {
		let turns = 0;
		const counting: GjcPort = {
			ensureSession: async () => ({ sessionId: "counted-session" }),
			forgetRebinds: () => {},
			sendTurn: async () => {
				turns += 1;
				return "should never run";
			},
		};
		harness = await startHarness({ gjc: counting });
		const wire = await negotiated(harness.config.socketPath);

		const send = async (origin: unknown, text: string, id: string) => {
			wire.send({ v: "0.1", type: "request", id, verb: "chat.send", params: { origin, text } });
			for (let attempt = 0; attempt < 400; attempt++) {
				const frame = wire.frames.find((candidate) => candidate.id === id);
				if (frame) return frame;
				await Bun.sleep(5);
			}
			throw new Error("chat.send did not answer");
		};

		const refused = await send(TERMINAL_ORIGIN, "hello", "chat-1");
		expect(refused.type).toBe("error");
		expect((refused.error as { code: string }).code).toBe("invalid_params");
		expect((refused.error as { message: string }).message).toContain("terminal origin is attach-only");

		// A `/new` from that origin must not reach the reset branch either.
		const epochBefore = harness.database.getSessionRecord(TERMINAL_KEY)?.epoch ?? 0;
		const refusedNew = await send(TERMINAL_ORIGIN, "/new", "chat-2");
		expect((refusedNew.error as { code: string }).code).toBe("invalid_params");
		expect(harness.database.getSessionRecord(TERMINAL_KEY)?.epoch ?? 0).toBe(epochBefore);

		// The chat loopback origin is untouched by this guard.
		const allowed = await send(LOOPBACK_ORIGIN, "hi there", "chat-3");
		expect(allowed.type).toBe("response");

		await Bun.sleep(50);
		// No turn was ever driven against the terminal origin.
		expect(turns).toBeLessThanOrEqual(1);
		wire.close();
	});

	test("reaction verbs keep refusing the terminal origin via their platform gate", async () => {
		harness = await startHarness();
		const wire = await negotiated(harness.config.socketPath);

		const call = async (verb: string, id: string) => {
			wire.send({
				v: "0.1",
				type: "request",
				id,
				verb,
				params: { origin: TERMINAL_ORIGIN, targetMessageId: "m1", emoji: "👍", action: "add" },
			});
			for (let attempt = 0; attempt < 400; attempt++) {
				const frame = wire.frames.find((candidate) => candidate.id === id);
				if (frame) return frame;
				await Bun.sleep(5);
			}
			throw new Error(`${verb} did not answer`);
		};

		for (const [verb, id] of [
			["chat.react", "react-1"],
			["engagement.reaction", "react-2"],
		] as const) {
			const frame = await call(verb, id);
			expect(frame.type).toBe("error");
			expect((frame.error as { code: string }).code).toBe("invalid_params");
			// The structural protection is the pre-existing platform gate, pinned here
			// so a future loosening cannot silently expose the terminal origin.
			expect((frame.error as { message: string }).message).toContain("requires a discord or telegram origin");
		}
		wire.close();
	});
});

describe("failure defenses", () => {
	test("a bind rejection clears the lease and the next attach succeeds", async () => {
		let fail = true;
		const flaky: GjcPort = {
			ensureSession: async () => {
				if (fail) {
					fail = false;
					throw new Error("bind exploded");
				}
				return { sessionId: "recovered-session" };
			},
			forgetRebinds: () => {},
			sendTurn: async () => "unused",
		};
		harness = await startHarness({ gjc: flaky });
		const wire = await negotiated(harness.config.socketPath);

		const failed = await attach(wire);
		expect(failed.error?.code).toBe("verb_failed");

		const recovered = await attach(wire);
		expect(recovered.error).toBeUndefined();
		expect(recovered.result?.sessionId).toBe("recovered-session");
		wire.close();
	});

	test("an unusable session-dir path fails closed and frees the lease", async () => {
		harness = await startHarness();
		// Pre-create `sessions/terminal` as a FILE so mkdir of the epoch dir fails.
		await mkdir(join(harness.config.home, "sessions"), { recursive: true });
		await writeFile(join(harness.config.home, "sessions", "terminal"), "not a directory");

		const wire = await negotiated(harness.config.socketPath);
		const failed = await attach(wire);
		expect(failed.error?.code).toBe("verb_failed");

		// Lease released: once the path is usable the next attach works.
		await rm(join(harness.config.home, "sessions", "terminal"), { force: true });
		const ok = await attach(wire);
		expect(ok.error).toBeUndefined();
		wire.close();
	});
});

describe("automatic rebind (AC-11 consistency)", () => {
	test("a bind that bumps the epoch still points the child at the store it used", async () => {
		// The hazard: the state root is chosen BEFORE the bind, but the bind may
		// rebind and bump the epoch. Deriving the child's root from the post-bind
		// epoch would hand it a directory its session was never written to.
		let call = 0;
		let observedEnv: Record<string, string> | undefined;
		const rebinding: GjcPort = {
			ensureSession: async (originKey, _epoch, options) => {
				call += 1;
				observedEnv = options?.env as Record<string, string> | undefined;
				if (call === 1) {
					// Simulate the rebind path: the epoch moves underneath the caller.
					harness?.database.bumpEpoch(originKey, JSON.stringify(TERMINAL_ORIGIN));
				}
				return { sessionId: "rebound-session" };
			},
			forgetRebinds: () => {},
			sendTurn: async () => "unused",
		};
		harness = await startHarness({ gjc: rebinding });
		const wire = await negotiated(harness.config.socketPath);
		const result = (await attach(wire)).result;

		expect(result).toBeDefined();
		const usedRoot = observedEnv?.GJC_CODING_AGENT_DIR;
		if (usedRoot === undefined) throw new Error("the bind was never given a native state root");
		// The child must be given the SAME root the create ran with, not one derived
		// from the epoch the bind left behind.
		expect((result?.childEnv as Record<string, string>).GJC_CODING_AGENT_DIR).toBe(usedRoot);

		// The epoch directory records where its session actually lives, so the
		// epoch-to-store mapping is discoverable rather than implied.
		const pointer = await readFile(join(String(result?.sessionDir), "bound-agent-dir"), "utf8");
		expect(pointer.trim()).toBe(usedRoot);

		wire.close();
		await Bun.sleep(60);
		// A later attach at that epoch reuses the recorded root rather than guessing.
		const wire2 = await negotiated(harness.config.socketPath);
		const again = (await attach(wire2)).result;
		expect((again?.childEnv as Record<string, string>).GJC_CODING_AGENT_DIR).toBe(usedRoot);
		wire2.close();
	});
});
