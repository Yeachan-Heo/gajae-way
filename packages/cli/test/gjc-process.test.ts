import { describe, expect, test } from "bun:test";
import { assertInteractiveStdin, exitCodeForChild, runGjc, type SpawnedChild } from "../src/gjc";

/**
 * Process contract (AC-1, AC-9, AC-10, AC-19) plus the transport-terminal child
 * guard (AC-6 child-safety half), driven with injected fakes so no daemon, no
 * model, and no real TTY are needed.
 */

interface FakeChild extends SpawnedChild {
	readonly signals: string[];
	settle(result: { exitCode?: number | null; signalCode?: string | null }): void;
}

function makeChild(): FakeChild {
	const signals: string[] = [];
	let resolveExit: ((code: number) => void) | undefined;
	const exited = new Promise<number>((resolve) => {
		resolveExit = resolve;
	});
	const child = {
		pid: 4242,
		exited,
		exitCode: null as number | null,
		signalCode: null as string | null,
		signals,
		kill(signal?: number | NodeJS.Signals) {
			signals.push(String(signal ?? "SIGTERM"));
		},
		settle(result: { exitCode?: number | null; signalCode?: string | null }) {
			child.exitCode = result.exitCode ?? null;
			child.signalCode = result.signalCode ?? null;
			resolveExit?.(0);
		},
	};
	return child as unknown as FakeChild;
}

interface FakeClientHooks {
	/** Fire the terminal notification the moment it is registered. */
	readonly fireOnRegister?: boolean;
}

function makeClient(attachResult: unknown, hooks: FakeClientHooks = {}) {
	let terminalHandler: ((error: Error) => void) | undefined;
	let stoppingHandler: (() => void) | undefined;
	const events: string[] = [];
	const client = {
		request: async () => {
			events.push("attach");
			if (attachResult instanceof Error) throw attachResult;
			return attachResult;
		},
		onTransportTerminal(handler: (error: Error) => void) {
			events.push("register-terminal");
			terminalHandler = handler;
			// Simulates the transport dying in the window between attach returning and
			// the child being spawned: the notification arrives at registration time.
			if (hooks.fireOnRegister) handler(new Error("transport died during setup"));
			return () => {
				events.push("unregister-terminal");
				terminalHandler = undefined;
			};
		},
		on(event: string, handler: () => void) {
			if (event === "gateway.stopping") stoppingHandler = handler;
			return () => {
				stoppingHandler = undefined;
			};
		},
		close: async () => {
			events.push("close");
		},
		events,
		fire: () => terminalHandler?.(new Error("transport died")),
		fireStopping: () => stoppingHandler?.(),
	};
	return client;
}

const attachResult = {
	sessionId: "bound-1",
	cwd: "/tmp/workspace",
	epoch: 0,
	originKey: "loopback/loopback/terminal",
	sessionDir: "/tmp/home/sessions/terminal/e0",
	personaPreamble: "PERSONA",
	argv: ["gjc", "--resume", "bound-1"],
	lease: { holder: "pid=1 connection=abc" },
};

describe("assertInteractiveStdin (AC-19)", () => {
	test("throws when stdin is not a TTY and the stub env is unset", () => {
		expect(() => assertInteractiveStdin({ isTTY: false }, {})).toThrow(/TTY/);
		expect(() => assertInteractiveStdin({}, {})).toThrow(/TTY/);
	});

	test("does not throw for a real TTY", () => {
		expect(() => assertInteractiveStdin({ isTTY: true }, {})).not.toThrow();
	});

	test("the stub env is the only exemption, even with a non-TTY stdin", () => {
		expect(() => assertInteractiveStdin({ isTTY: false }, { GAJAEWAY_TEST_STUB_GJC: "1" })).not.toThrow();
		// A different value is NOT an exemption.
		expect(() => assertInteractiveStdin({ isTTY: false }, { GAJAEWAY_TEST_STUB_GJC: "0" })).toThrow(/TTY/);
	});
});

describe("exitCodeForChild (AC-10)", () => {
	test("passes a normal exit code through", () => {
		expect(exitCodeForChild({ exitCode: 7, signalCode: null })).toBe(7);
		expect(exitCodeForChild({ exitCode: 0, signalCode: null })).toBe(0);
	});

	test("translates a signal death to 128+N", () => {
		expect(exitCodeForChild({ exitCode: null, signalCode: "SIGHUP" })).toBe(129);
		expect(exitCodeForChild({ exitCode: null, signalCode: "SIGINT" })).toBe(130);
		expect(exitCodeForChild({ exitCode: null, signalCode: "SIGTERM" })).toBe(143);
	});

	test("a missing exit code degrades to 1 rather than a false success", () => {
		expect(exitCodeForChild({ exitCode: null, signalCode: null })).toBe(1);
	});
});

describe("runGjc", () => {
	test("refuses a non-TTY invocation before connecting (AC-19)", async () => {
		let connected = false;
		const code = await runGjc("/tmp/x.sock", [], {
			env: {},
			stdin: { isTTY: false },
			connect: async () => {
				connected = true;
				throw new Error("must not connect");
			},
			errorLog: () => {},
		});
		expect(code).toBe(1);
		// The gate must run BEFORE connectSocket, so no attach and no lease.
		expect(connected).toBe(false);
	});

	test("reports the resolved socket path when the daemon is down (AC-9)", async () => {
		const lines: string[] = [];
		const code = await runGjc("/tmp/does-not-exist.sock", [], {
			env: {},
			stdin: { isTTY: true },
			connect: async () => {
				throw new Error("ECONNREFUSED");
			},
			errorLog: (line) => lines.push(line),
		});
		expect(code).toBe(1);
		expect(lines.join("\n")).toContain("/tmp/does-not-exist.sock");
	});

	test("spawns the attach argv verbatim and waits for the child (AC-1)", async () => {
		const child = makeChild();
		let spawnedCmd: string[] | undefined;
		let spawnedCwd: string | undefined;
		const client = makeClient(attachResult);

		const run = runGjc("/tmp/x.sock", [], {
			env: {},
			stdin: { isTTY: true },
			connect: async () => client as never,
			spawn: (cmd, cwd) => {
				spawnedCmd = cmd;
				spawnedCwd = cwd;
				return child;
			},
			errorLog: () => {},
		});

		await Bun.sleep(5);
		// Still waiting: the wrapper must not exit before the child.
		expect(spawnedCmd).toEqual(["gjc", "--resume", "bound-1"]);
		expect(spawnedCwd).toBe("/tmp/workspace");

		child.settle({ exitCode: 7 });
		expect(await run).toBe(7);
	});

	test("registers the transport guard before spawn and unregisters before close", async () => {
		const child = makeChild();
		const client = makeClient(attachResult);
		const run = runGjc("/tmp/x.sock", [], {
			env: {},
			stdin: { isTTY: true },
			connect: async () => client as never,
			spawn: () => child,
			errorLog: () => {},
		});
		await Bun.sleep(5);
		child.settle({ exitCode: 0 });
		await run;

		const order = client.events;
		expect(order.indexOf("register-terminal")).toBeGreaterThan(order.indexOf("attach"));
		// Ordinary teardown must not be able to self-trigger the kill path.
		expect(order.indexOf("unregister-terminal")).toBeLessThan(order.indexOf("close"));
	});

	test("kills the child when the transport dies, then exits nonzero (AC-6)", async () => {
		const child = makeChild();
		const client = makeClient(attachResult);
		const run = runGjc("/tmp/x.sock", [], {
			env: {},
			stdin: { isTTY: true },
			connect: async () => client as never,
			spawn: () => child,
			errorLog: () => {},
			killGraceMs: 5,
		});
		await Bun.sleep(5);

		client.fire();
		await Bun.sleep(20);
		expect(child.signals).toContain("SIGTERM");
		expect(child.signals).toContain("SIGKILL");

		// Even if the child reports a clean code after being killed, the wrapper
		// must not claim success.
		child.settle({ exitCode: 0 });
		expect(await run).toBe(1);
	});

	test("kills the child when the gateway announces shutdown", async () => {
		const child = makeChild();
		const client = makeClient(attachResult);
		const run = runGjc("/tmp/x.sock", [], {
			env: {},
			stdin: { isTTY: true },
			connect: async () => client as never,
			spawn: () => child,
			errorLog: () => {},
			killGraceMs: 5,
		});
		await Bun.sleep(5);

		client.fireStopping();
		await Bun.sleep(20);
		expect(child.signals).toContain("SIGTERM");

		child.settle({ signalCode: "SIGTERM" });
		expect(await run).toBe(143);
	});

	test("never spawns when the transport dies between attach and spawn", async () => {
		// The dangerous window: the gateway has already released the lease, so
		// spawning here would leave an orphan on a session another terminal can
		// immediately claim.
		const client = makeClient(attachResult, { fireOnRegister: true });
		let spawned = false;
		const code = await runGjc("/tmp/x.sock", [], {
			env: {},
			stdin: { isTTY: true },
			connect: async () => client as never,
			spawn: () => {
				spawned = true;
				return makeChild();
			},
			errorLog: () => {},
			killGraceMs: 5,
		});

		expect(spawned).toBe(false);
		expect(code).toBe(1);
	});

	test("applies the gateway's native-state decision as a TOTAL override", async () => {
		// The dangerous split: the daemon does NOT have a selector set, but the
		// operator's shell does. A value-only merge would leave the operator's value
		// in place and the child would resolve a store where the bound session does
		// not exist.
		const child = makeChild();
		const client = makeClient({
			...attachResult,
			childEnv: { GJC_CODING_AGENT_DIR: "/daemon/agent" },
			childEnvUnset: ["XDG_DATA_HOME", "PI_CODING_AGENT_DIR"],
		});
		let spawnedEnv: Record<string, string | undefined> | undefined;

		const run = runGjc("/tmp/x.sock", [], {
			env: {
				PATH: "/usr/bin",
				GJC_CODING_AGENT_DIR: "/operator/agent",
				XDG_DATA_HOME: "/operator/xdg",
				PI_CODING_AGENT_DIR: "/operator/legacy",
			},
			stdin: { isTTY: true },
			connect: async () => client as never,
			spawn: (_cmd, _cwd, spawnEnv) => {
				spawnedEnv = spawnEnv;
				return child;
			},
			errorLog: () => {},
		});
		await Bun.sleep(5);
		child.settle({ exitCode: 0 });
		await run;

		// The daemon's value wins where it has one...
		expect(spawnedEnv?.GJC_CODING_AGENT_DIR).toBe("/daemon/agent");
		// ...and where it has none, the operator's value must be GONE, not kept.
		expect(spawnedEnv?.XDG_DATA_HOME).toBeUndefined();
		expect(spawnedEnv?.PI_CODING_AGENT_DIR).toBeUndefined();
		// Unrelated variables are untouched.
		expect(spawnedEnv?.PATH).toBe("/usr/bin");
	});

	test("maps unknown_verb to an upgrade hint and never spawns", async () => {
		const { ProtocolError } = await import("@gajaeway/protocol");
		const client = makeClient(new ProtocolError("unknown_verb", "unknown verb: session.attach"));
		let spawned = false;
		const lines: string[] = [];
		const code = await runGjc("/tmp/x.sock", [], {
			env: {},
			stdin: { isTTY: true },
			connect: async () => client as never,
			spawn: () => {
				spawned = true;
				return makeChild();
			},
			errorLog: (line) => lines.push(line),
		});
		expect(code).toBe(1);
		expect(spawned).toBe(false);
		expect(lines.join("\n")).toContain("too old");
	});

	test("surfaces a lease-held refusal message and never spawns (AC-5)", async () => {
		const { ProtocolError } = await import("@gajaeway/protocol");
		const client = makeClient(
			new ProtocolError("session_lease_held", "pid=99 connection=xyz holds the terminal gjc lease", {
				holder: "pid=99 connection=xyz",
			}),
		);
		let spawned = false;
		const lines: string[] = [];
		const code = await runGjc("/tmp/x.sock", [], {
			env: {},
			stdin: { isTTY: true },
			connect: async () => client as never,
			spawn: () => {
				spawned = true;
				return makeChild();
			},
			errorLog: (line) => lines.push(line),
		});
		expect(code).toBe(1);
		expect(spawned).toBe(false);
		expect(lines.join("\n")).toContain("holds the terminal gjc lease");
	});

	test("surfaces a refused-flag message and never spawns (AC-7)", async () => {
		const { ProtocolError } = await import("@gajaeway/protocol");
		const client = makeClient(new ProtocolError("invalid_params", "session.attach refuses --resume"));
		let spawned = false;
		const lines: string[] = [];
		const code = await runGjc("/tmp/x.sock", ["--resume", "abc"], {
			env: {},
			stdin: { isTTY: true },
			connect: async () => client as never,
			spawn: () => {
				spawned = true;
				return makeChild();
			},
			errorLog: (line) => lines.push(line),
		});
		expect(code).toBe(1);
		expect(spawned).toBe(false);
		expect(lines.join("\n")).toContain("--resume");
	});
});
