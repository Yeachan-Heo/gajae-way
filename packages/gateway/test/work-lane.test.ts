import { afterEach, expect, spyOn, test } from "bun:test";
import { chmod, lstat, mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { pathToFileURL } from "node:url";
import { type ChatMessagePayload, type OriginRef, originKey, ProtocolError } from "@gajae-gateway/protocol";
import {
	appendAttempt,
	type CliRunner,
	closeAttempt,
	createLaneJobRecord,
	GjcCliError,
	parseLaneJobRecord,
} from "@gajae-gateway/subsession";
import { GjcCliUnavailableError } from "../src/orchestrator/broker";
import {
	classifyCheckRollup,
	failedCheckLogTargets,
	ghCheckStatusClient,
	sanitizedFailureLogStreamTail,
} from "../src/orchestrator/gh-checks";
import {
	LaneGovernor,
	laneJobIdentity,
	managedLaneBranchName,
	managedLaneRepositoryPath,
	managedLaneWorktreePath,
	workSessionKey,
} from "../src/orchestrator/lane-governor";
import {
	BrokerSessionPort,
	parseWorkerOutputResponse,
	type SessionBindInput,
	type WorkerOutputResult,
} from "../src/orchestrator/session-port";
import { RelayClosedError, TailRunner } from "../src/orchestrator/tail-runner";
import {
	assertOutsideServiceDirectory,
	type CheckStatusClient,
	type CheckStatusSnapshot,
	getServiceDirectory,
	laneLastCommits,
	laneSystemNotice,
	reportText,
	utf8Prefix,
	WorkLaneManager,
	type WorkLaneManagerOptions,
} from "../src/orchestrator/work-lane";
import { BrokerAuthorityError, Store } from "../src/store";
import { WorkAttemptStateError } from "../src/store/groups/work";
import {
	createOwnedSessionFixture,
	initializeTestBrokerAuthority,
	noRelay,
	ScriptedSessionPort,
} from "./session-port.fake";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});
const origin: OriginRef = { platform: "discord", kind: "channel", conversationId: "work-results" };

test("managed repository and worktree paths preserve exact lane-name identity", () => {
	const root = "/tmp/gajaeway";
	expect(managedLaneRepositoryPath(root, "Foo")).not.toBe(managedLaneRepositoryPath(root, "foo"));
	expect(managedLaneRepositoryPath(root, "Foo")).toBe(join(root, "repositories", Buffer.from("Foo").toString("hex")));
	expect(managedLaneWorktreePath(root, "Foo")).not.toBe(managedLaneWorktreePath(root, "foo"));
	expect(managedLaneWorktreePath(root, "Foo")).toBe(join(root, "lanes", Buffer.from("Foo").toString("hex")));
});

async function until(predicate: () => boolean): Promise<void> {
	for (let i = 0; i < 600 && !predicate(); i++) await Bun.sleep(5);
	expect(predicate()).toBe(true);
}
function deferred<T>() {
	let resolve!: (value: T) => void;
	const promise = new Promise<T>((done) => {
		resolve = done;
	});
	return { promise, resolve };
}
async function fixture(
	options: Partial<WorkLaneManagerOptions> = {},
	portOptions: ConstructorParameters<typeof ScriptedSessionPort>[0] = {},
) {
	let directory = await mkdtemp(join(tmpdir(), "gajaeway-work-lane-"));
	// Normalize path to resolve macOS /var -> /private/var symlink
	directory = await realpath(directory);
	const gajaewayHome = join(directory, "gajaeway");
	// Create gajaewayHome directory for service directory validation tests
	await mkdir(gajaewayHome, { recursive: true });
	// The service checkout is a sibling of the lane root: lanes and their worktrees live outside it.
	const serviceDirectory = join(directory, "service");
	await mkdir(serviceDirectory, { recursive: true });
	const db = await Store.open(join(directory, "gateway.db"));
	const port = new ScriptedSessionPort({
		...portOptions,
		onBind: (input) => {
			const sessionId = db.bindings.getSessionRecord(input.originKey)?.sessionId || crypto.randomUUID();
			db.transaction((tx) => db.bindings.bind(tx, input.originKey, sessionId));
			return sessionId;
		},
	});
	const notices: ChatMessagePayload[] = [];
	const lanes = new LaneGovernor({ database: db, sessionPort: port, laneRoot: gajaewayHome, maxLanes: 2 });
	let implicitOwner: OriginRef | undefined;
	const ownerTarget = options.ownerTarget ?? (() => implicitOwner);
	let manager = new WorkLaneManager({
		database: db,
		port,
		lanes,
		gajaewayHome,
		ownerTarget,
		pollMs: 5,
		waitTimeoutMs: 2000,
		deliverFallback: (payload) => notices.push(payload),
		serviceDirectory,
		// Bind failures are final unless a test opts into the broker readiness wait.
		brokerWait: { deadlineMs: 0 },
		...options,
	});
	cleanups.push(async () => {
		await manager.stop();
		db.close();
		await rm(directory, { recursive: true, force: true });
	});
	const job = (name = "a") => parseLaneJobRecord(db.work.laneJobJson(laneJobIdentity(name).jobId)!);
	return {
		db,
		port,
		lanes,
		notices,
		directory,
		gajaewayHome,
		serviceDirectory,
		job,
		get manager() {
			return manager;
		},
		restart: async (extra: Partial<WorkLaneManagerOptions> = {}) => {
			await manager.stop();
			manager = new WorkLaneManager({
				database: db,
				port,
				lanes,
				gajaewayHome,
				serviceDirectory,
				ownerTarget,
				pollMs: 5,
				deliverFallback: (payload) => notices.push(payload),
				...options,
				...extra,
			});
			await manager.recover();
			return manager;
		},
		setOwnerTarget: (target: OriginRef | undefined) => {
			implicitOwner = target;
		},
	};
}
async function started(f: Awaited<ReturnType<typeof fixture>>, name = "a", parentOrigin?: OriginRef) {
	if (parentOrigin) f.setOwnerTarget(parentOrigin);
	const result = await f.manager.start({ name, text: "work", cwd: f.directory });
	if (!result.started) throw new Error("unexpected hold");
	return result;
}

test("work.start during a broker restart waits for broker readiness and starts the lane", async () => {
	const logs: string[] = [];
	const sleeps: number[] = [];
	let brokerUp = false;
	const f = await fixture({
		brokerWait: {
			initialMs: 100,
			maxMs: 400,
			deadlineMs: 10_000,
			log: (line) => logs.push(line),
			sleep: async (ms) => {
				sleeps.push(ms);
				// The supervisor brings the broker back during the third wait.
				if (sleeps.length === 3) brokerUp = true;
			},
		},
	});
	const bind = f.port.bind.bind(f.port);
	let attempts = 0;
	f.port.bind = async (input) => {
		attempts++;
		if (!brokerUp) throw new GjcCliUnavailableError("broker restarting");
		return await bind(input);
	};
	const result = await f.manager.start({ name: "a", text: "work", cwd: f.directory });
	expect(result).toMatchObject({ started: true, sessionKey: workSessionKey("a") });
	expect(attempts).toBe(4);
	expect(sleeps).toEqual([100, 200, 400]);
	expect(logs).toHaveLength(3);
	expect(logs[0]).toStartWith("gateway_waiting_for_broker step=work.bind:a attempt=1 retry_in_ms=100 ");
	expect(f.port.sends).toHaveLength(1);
});

test("work.start reusing a persisted binding during a broker restart waits in the real BrokerSessionPort bind", async () => {
	const logs: string[] = [];
	const sleeps: number[] = [];
	const f = await fixture({
		brokerWait: {
			initialMs: 100,
			maxMs: 400,
			deadlineMs: 10_000,
			log: (line) => logs.push(line),
			sleep: async (ms) => {
				sleeps.push(ms);
			},
		},
	});
	const authority = initializeTestBrokerAuthority(f.db, join(f.directory, "agent"));
	const sessionKey = workSessionKey("a");
	await createOwnedSessionFixture(f.db, authority, {
		sessionId: "7d1f2c3a-0b4e-4c5d-8e6f-9a0b1c2d3e4f",
		originKey: sessionKey,
		epoch: 0,
		repo: f.directory,
	});
	// The broker is restarting for the first two inspects: the client first
	// raises its own unavailable error, then the broker answers an
	// `ok:false broker_unavailable` envelope; the third inspect sees it healthy.
	const commands: string[][] = [];
	const cli: CliRunner = async (args) => {
		commands.push([...args]);
		if (!args.includes("inspect")) throw new Error(`unexpected command ${args.join(" ")}`);
		const inspects = commands.filter((command) => command.includes("inspect")).length;
		if (inspects === 1) throw new GjcCliUnavailableError("broker restarting");
		if (inspects === 2)
			return {
				exitCode: 1,
				stdout: JSON.stringify({ ok: false, error: { code: "broker_unavailable", message: "supervisor restart" } }),
				stderr: "",
			};
		return {
			exitCode: 0,
			stdout: JSON.stringify({
				ok: true,
				result: {
					session: {
						sessionId: "7d1f2c3a-0b4e-4c5d-8e6f-9a0b1c2d3e4f",
						live: true,
						deleted: false,
						locator: { repo: f.directory },
					},
				},
			}),
			stderr: "",
		};
	};
	const brokerPort = new BrokerSessionPort({
		database: f.db,
		authority,
		cli,
		tailRunner: new TailRunner({ stream: noRelay, repo: f.directory }),
		sleep: async () => {},
	});
	// Binding is the production BrokerSessionPort; only the relay-side turn
	// machinery stays scripted.
	f.port.bind = (input) => brokerPort.bind(input);

	const result = await f.manager.start({ name: "a", text: "work", cwd: f.directory });

	expect(result).toMatchObject({ started: true, sessionKey });
	expect(commands.map((command) => command.slice(0, 4))).toEqual([
		["sdk", "session", "inspect", "7d1f2c3a-0b4e-4c5d-8e6f-9a0b1c2d3e4f"],
		["sdk", "session", "inspect", "7d1f2c3a-0b4e-4c5d-8e6f-9a0b1c2d3e4f"],
		["sdk", "session", "inspect", "7d1f2c3a-0b4e-4c5d-8e6f-9a0b1c2d3e4f"],
	]);
	expect(sleeps).toEqual([100, 200]);
	expect(logs).toHaveLength(2);
	expect(logs[0]).toStartWith("gateway_waiting_for_broker step=work.bind:a attempt=1 retry_in_ms=100 ");
	expect(f.db.bindings.getSessionRecord(sessionKey)).toMatchObject({
		sessionId: "7d1f2c3a-0b4e-4c5d-8e6f-9a0b1c2d3e4f",
		epoch: 0,
	});
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.sends[0]?.sessionId).toBe("7d1f2c3a-0b4e-4c5d-8e6f-9a0b1c2d3e4f");
});

test("work.start retries when the broker restarts before a cold-created session is indexed", async () => {
	const logs: string[] = [];
	const sleeps: number[] = [];
	const commands: string[][] = [];
	const f = await fixture({
		brokerWait: {
			initialMs: 100,
			maxMs: 400,
			deadlineMs: 10_000,
			log: (line) => logs.push(line),
			sleep: async (ms) => {
				sleeps.push(ms);
			},
		},
	});
	const authority = initializeTestBrokerAuthority(f.db, join(f.directory, "agent"));
	const createKeys: string[] = [];
	const sessionId = "7d1f2c3a-0b4e-4c5d-8e6f-9a0b1c2d3e4f";
	let inspects = 0;
	const cli: CliRunner = async (args) => {
		commands.push([...args]);
		const operationIndex = args.indexOf("--op");
		if (operationIndex >= 0 && args[operationIndex + 1] === "session.create") {
			createKeys.push(args[args.indexOf("--idempotency-key") + 1] ?? "");
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessionId } }), stderr: "" };
		}
		if (args.includes("inspect")) {
			inspects++;
			if (inspects === 1) throw new GjcCliUnavailableError("broker restarted after create");
			if (inspects === 2)
				return {
					exitCode: 1,
					stdout: JSON.stringify({ ok: false, error: { code: "broker_unavailable", message: "supervisor restart" } }),
					stderr: "",
				};
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { session: { live: true, deleted: false } } }),
				stderr: "",
			};
		}
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const brokerPort = new BrokerSessionPort({
		database: f.db,
		authority,
		cli,
		tailRunner: new TailRunner({ stream: noRelay, repo: f.directory }),
		sleep: async () => {},
	});
	f.port.bind = (input) => brokerPort.bind(input);

	const result = await f.manager.start({ name: "a", text: "work", cwd: f.directory });

	expect(result).toMatchObject({ started: true, sessionKey: workSessionKey("a") });
	expect(createKeys).toHaveLength(1);
	expect(inspects).toBe(3);
	expect(sleeps).toEqual([100, 200]);
	expect(logs).toHaveLength(2);
	expect(logs[0]).toStartWith("gateway_waiting_for_broker step=work.bind:a attempt=1 retry_in_ms=100 ");
	expect(commands.filter((command) => command.includes("session.create"))).toHaveLength(1);
	expect(f.db.bindings.getSessionRecord(workSessionKey("a"))).toMatchObject({
		sessionId,
		epoch: 0,
	});
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.sends[0]?.sessionId).toBe(sessionId);
});

test("managed repo cold binding retains its worktree across a broker readiness timeout", async () => {
	const sourceRepo = await mkdtemp(join(tmpdir(), "test-repo-cold-bind-"));
	const bareRepo = await mkdtemp(join(tmpdir(), "bare-repo-cold-bind-"));
	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(async () => errors.mockRestore());
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", sourceRepo, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "test",
					GIT_AUTHOR_EMAIL: "test@test",
					GIT_COMMITTER_NAME: "test",
					GIT_COMMITTER_EMAIL: "test@test",
				},
			});
		git("init", "-q");
		git("config", "user.name", "test");
		git("config", "user.email", "test@test");
		git("commit", "-q", "--allow-empty", "-m", "initial commit");
		Bun.spawnSync(["git", "clone", "--bare", sourceRepo, bareRepo], { stdout: "ignore", stderr: "ignore" });

		let clock = 0;
		let brokerAvailable = false;
		let inspectCount = 0;
		const createKeys: string[] = [];
		const createInputs: string[] = [];
		const sessionId = "7d1f2c3a-0b4e-4c5d-8e6f-9a0b1c2d3e4f";
		const f = await fixture({
			brokerWait: {
				initialMs: 100,
				maxMs: 100,
				deadlineMs: 350,
				now: () => clock,
				log: () => {},
				sleep: async (ms) => {
					clock += ms;
				},
			},
		});
		const authority = initializeTestBrokerAuthority(f.db, join(f.directory, "agent"));
		const cli: CliRunner = async (args) => {
			const operationIndex = args.indexOf("--op");
			if (operationIndex >= 0 && args[operationIndex + 1] === "session.create") {
				createKeys.push(args[args.indexOf("--idempotency-key") + 1] ?? "");
				createInputs.push(args[args.indexOf("--json-input") + 1] ?? "{}");
				return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessionId } }), stderr: "" };
			}
			if (args.includes("inspect")) {
				inspectCount++;
				if (!brokerAvailable) throw new GjcCliUnavailableError("broker unavailable while indexing");
				return {
					exitCode: 0,
					stdout: JSON.stringify({
						ok: true,
						result: { session: { sessionId, live: true, deleted: false, locator: { repo: worktreePath } } },
					}),
					stderr: "",
				};
			}
			throw new Error(`unexpected command ${args.join(" ")}`);
		};
		const brokerPort = new BrokerSessionPort({
			database: f.db,
			authority,
			cli,
			tailRunner: new TailRunner({ stream: noRelay, repo: f.directory }),
			sleep: async () => {},
		});
		f.port.bind = (input) => brokerPort.bind(input);
		const name = "cold-repo";
		const worktreePath = managedLaneWorktreePath(f.gajaewayHome, name);
		const input = { name, text: "work", repo: bareRepo };

		await expect(f.manager.start(input)).rejects.toMatchObject({
			code: "verb_failed",
			detail: { reasonCode: "bind_failed", cause: "broker_unavailable" },
		});
		expect(createKeys).toHaveLength(1);
		expect(JSON.parse(createInputs[0] ?? "{}")).toMatchObject({ cwd: worktreePath });
		expect(f.db.bindings.getSessionRecord(workSessionKey(name))).toMatchObject({ sessionId, epoch: 0 });
		expect(
			f.db.bindings.brokerOperation(authority, "session.create", JSON.stringify([workSessionKey(name), 0])),
		).toMatchObject({
			state: "pending",
		});
		expect(f.job(name)).toMatchObject({
			lane: { repositoryPath: managedLaneRepositoryPath(f.gajaewayHome, name), worktreePath },
			attempts: [],
		});
		const retainedWorktree = Bun.spawnSync(["git", "-C", worktreePath, "rev-parse", "--show-toplevel"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(retainedWorktree.exitCode).toBe(0);
		expect(retainedWorktree.stdout?.toString().trim()).toBe(worktreePath);

		brokerAvailable = true;
		const retried = await f.manager.start(input);
		expect(retried.started).toBe(true);
		expect(createKeys).toHaveLength(1);
		expect(createInputs).toHaveLength(1);
		expect(f.job(name)).toMatchObject({ lane: { worktreePath }, attempts: [{ sessionId }] });
		expect(f.port.sends).toHaveLength(1);
		expect(f.port.sends[0]?.sessionId).toBe(sessionId);
		expect(inspectCount).toBe(5);
	} finally {
		await rm(sourceRepo, { recursive: true, force: true });
		await rm(bareRepo, { recursive: true, force: true });
	}
});

test("work.start fails bind_failed once the broker readiness wait is exhausted", async () => {
	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(async () => errors.mockRestore());
	let clock = 0;
	const f = await fixture({
		brokerWait: {
			initialMs: 100,
			maxMs: 100,
			deadlineMs: 350,
			now: () => clock,
			log: () => {},
			sleep: async (ms) => {
				clock += ms;
			},
		},
	});
	let attempts = 0;
	f.port.bind = async () => {
		attempts++;
		throw new Error("gjc sdk request failed: broker_unavailable (broker unavailable)");
	};
	await expect(f.manager.start({ name: "a", text: "work", cwd: f.directory })).rejects.toMatchObject({
		code: "verb_failed",
		detail: { reasonCode: "bind_failed", cause: "broker_unavailable" },
	});
	expect(attempts).toBe(4);
});

test("a non-broker bind failure is not waited on", async () => {
	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(async () => errors.mockRestore());
	const f = await fixture({ brokerWait: { initialMs: 1, deadlineMs: 10_000, log: () => {}, sleep: async () => {} } });
	let attempts = 0;
	f.port.bind = async () => {
		attempts++;
		throw new BrokerAuthorityError("unowned_session");
	};
	await expect(f.manager.start({ name: "a", text: "work", cwd: f.directory })).rejects.toMatchObject({
		detail: { reasonCode: "bind_failed", cause: "cwd_rejected" },
	});
	expect(attempts).toBe(1);
});

test("bind failures map causes and emit one redacted diagnostic each", async () => {
	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(async () => errors.mockRestore());
	const failures = [
		["broker unavailable", "broker_unavailable", () => new GjcCliUnavailableError("broker unavailable")],
		[
			"sanitized broker unavailable",
			"broker_unavailable",
			() => new Error("gjc sdk request failed: broker_unavailable (broker unavailable)"),
		],
		["timeout", "timeout", () => new GjcCliUnavailableError("request timed out after 1000ms")],
		["session-create timeout", "timeout", () => new Error("gjc session.create did not complete within 60000ms")],
		["cwd rejected", "cwd_rejected", () => new BrokerAuthorityError("unowned_session")],
		["epoch conflict", "epoch_conflict", () => new BrokerAuthorityError("authority_mismatch")],
		[
			"ProtocolError reasonCode",
			"broker_unavailable",
			() => new ProtocolError("verb_failed", "bind rejected", { reasonCode: "session_unavailable" }),
		],
		[
			"unknown fallback",
			"unknown",
			() => new Error(`authentication failed token=sk_live_123456789abcdef\n${"x".repeat(240)}`),
		],
	] as const;
	for (const [label, cause, makeError] of failures) {
		const f = await fixture();
		f.port.bind = async () => {
			throw makeError();
		};

		errors.mockClear();
		let failure: unknown;
		try {
			await f.manager.start({ name: "a", text: "work", cwd: f.directory });
		} catch (error) {
			failure = error;
		}
		expect(failure).toBeInstanceOf(ProtocolError);
		expect(failure).toMatchObject({
			code: "verb_failed",
			message: `work lane bind failed (cause: ${cause})`,
			detail: { reasonCode: "bind_failed", cause, name: "a" },
		});

		const lines = errors.mock.calls.map(([line]) => String(line)).filter((line) => line.startsWith("work_bind_failed"));
		expect(lines).toHaveLength(1);
		const line = lines[0];
		expect(line).toBeDefined();
		if (line === undefined) throw new Error("bind failure diagnostic was not captured");
		expect(line).toStartWith(
			`work_bind_failed name=a origin=${workSessionKey("a")} cwd=${f.directory} cause=${cause} error=`,
		);
		expect(line).not.toMatch(/[\r\n]/);
		const errorMessage = line.slice(line.indexOf(" error=") + " error=".length);
		expect(errorMessage.length).toBeLessThanOrEqual(200);
		if (label === "unknown fallback") expect(line).not.toContain("sk_live_123456789abcdef");
	}
});
for (const model of ["startup-model", { preset: "startup-preset" }]) {
	test(`proven startup model skips only the newly bound session's duplicate send model: ${JSON.stringify(model)}`, async () => {
		const f = await fixture();
		const first = await f.manager.start({ name: "a", text: "work", cwd: f.directory, model });
		if (!first.started) throw new Error("unexpected hold");
		expect(f.port.binds[0]?.model).toEqual(model);
		expect(f.port.sends).toHaveLength(1);
		expect(Object.hasOwn(f.port.sends[0]!, "model")).toBe(false);
		f.port.complete(first.opRef, "done");
		await until(() => f.db.work.workAttemptGet(first.opRef)?.settledAt !== null);

		const bind = f.port.bind.bind(f.port);
		f.port.bind = async (input) => ({ ...(await bind(input)), startupModelApplied: false });
		const nextModel = "later-turn-model";
		const second = await f.manager.start({ name: "a", text: "again", cwd: f.directory, model: nextModel });
		if (!second.started) throw new Error("unexpected hold");
		expect(second.sessionId).toBe(first.sessionId);
		expect(f.port.binds[1]?.model).toBe(nextModel);
		expect(f.port.sends).toHaveLength(2);
		expect(f.port.sends[1]?.model).toBe(nextModel);
	});
}

for (const startupModelApplied of [false, undefined]) {
	test(`binding without startup proof forwards the requested model: ${startupModelApplied}`, async () => {
		const f = await fixture();
		const bind = f.port.bind.bind(f.port);
		f.port.bind = async (input) => {
			const { startupModelApplied: _, ...binding } = await bind(input);
			return startupModelApplied === undefined ? binding : { ...binding, startupModelApplied };
		};
		const model = { preset: "requested-preset" };
		await f.manager.start({ name: "a", text: "work", cwd: f.directory, model });
		expect(f.port.binds[0]?.model).toEqual(model);
		expect(f.port.sends).toHaveLength(1);
		expect(f.port.sends[0]?.model).toEqual(model);
	});
}

test("failed send after proven startup model remains uncertain without retry or resend", async () => {
	const f = await fixture();
	let attempts = 0;
	f.port.send = async (input) => {
		attempts++;
		f.port.sendAttempts.push(input);
		throw new Error("session_unavailable");
	};
	await expect(
		f.manager.start({ name: "a", text: "work", cwd: f.directory, model: "startup-model" }),
	).rejects.toMatchObject({ detail: { reasonCode: "send_acceptance_uncertain" } });
	const opRef = f.job().attempts[0]!.opRef;
	expect(f.db.work.workAttemptGet(opRef)?.sendPhase).toBe("uncertain");
	await f.manager.recover();
	await f.restart();
	expect(attempts).toBe(1);
	expect(Object.hasOwn(f.port.sendAttempts[0]!, "model")).toBe(false);
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.resumes).toHaveLength(0);
	expect(f.db.work.workAttemptGet(opRef)?.sendEvidence).toBeNull();
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
});

test("start acknowledges before terminal, one observer survives caller-free completion and refreshes activity", async () => {
	const f = await fixture();
	let attaches = 0;
	const attach = f.port.attachTail.bind(f.port);
	f.port.attachTail = async (input) => {
		attaches++;
		return attach(input);
	};
	const result = await started(f, "a", origin);
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
	expect(f.port.sends).toHaveLength(1);
	await until(() => attaches === 1);
	await Promise.all([f.manager.recover(), f.manager.recover()]);
	expect(attaches).toBe(1);
	const activity = f.db.bindings.workLaneRows()[0]!.last_activity_at;
	await Bun.sleep(10);
	f.port.complete(result.opRef, "done");
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt !== null);
	expect(f.job().attempts[0]?.endState).toBe("completed");
	expect(f.db.bindings.workLaneRows()[0]!.last_activity_at! > activity!).toBe(true);
	expect(f.notices).toHaveLength(1);
	expect(f.notices[0]).toMatchObject({ turnId: result.opRef, origin, text: "[lane a] completed: done" });
	expect(f.db.outbox.rows()).toHaveLength(1);
	await f.manager.recover();
	expect(f.notices).toHaveLength(1);
});

test("simultaneous cap admission and same-name resume refuse without overlapping sends", async () => {
	const f = await fixture();
	const results = await Promise.allSettled([started(f, "a"), started(f, "b"), started(f, "c")]);
	expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(2);
	const rejected = results.find((r) => r.status === "rejected") as PromiseRejectedResult;
	expect(rejected.reason).toMatchObject({ code: "lane_capacity", detail: { active: 2, maxLanes: 2 } });
	for (const resume of [false, true])
		await expect(f.manager.start({ name: "a", text: "again", cwd: f.directory, resume })).rejects.toMatchObject({
			code: "invalid_params",
			detail: { reasonCode: "attempt_open" },
		});
	expect(f.port.binds).toHaveLength(2);
	expect(f.port.sends).toHaveLength(2);
	expect(await f.lanes.retire("a", "operator")).toMatchObject({ retired: false });
	expect(await f.lanes.sweep(Date.now() + 1e9)).toBe(0);
});

test("status is pure during a run wait and steer returns the exact supplied clientRef", async () => {
	const f = await fixture();
	const owner = {};
	const run = f.manager.run({ name: "a", text: "work", cwd: f.directory }, owner);
	await until(() => f.port.sends.length === 1);
	const op = f.port.sends[0]!.opRef;
	const before = f.db.work.laneJobJson(laneJobIdentity("a").jobId);
	const activity = f.db.bindings.workLaneRows()[0]!.last_activity_at;
	const snapshot = await f.manager.status({ name: "a" });
	expect(snapshot).toMatchObject({ attempt: { opRef: op }, op: { status: "in_flight" } });
	expect(f.db.work.laneJobJson(snapshot.jobId)).toBe(before);
	expect(f.db.bindings.workLaneRows()[0]!.last_activity_at).toBe(activity);
	const steer = await f.manager.steer({ name: "a", text: "correction" });
	expect(steer.steered).toBe(true);
	if (steer.steered) expect(f.port.steers[0]?.clientRef).toBe(steer.clientRef);
	f.port.complete(op, "full answer");
	expect(await run).toMatchObject({ held: false, text: "full answer" });
	expect(f.notices).toHaveLength(0);
	expect(f.port.binds).toHaveLength(1);
	await expect(f.manager.steer({ name: "a", text: "late" })).rejects.toMatchObject({
		detail: { reasonCode: "no_open_attempt" },
	});
});

test("status unknown, malformed and changed-binding edges never rewrite durable evidence", async () => {
	const f = await fixture();
	const result = await started(f);
	const before = f.db.work.laneJobJson(result.jobId);
	f.port.status = async (input) => ({
		operationRef: input.opRef,
		status: { status: "unknown" },
		summaryCompleted: false,
	});
	expect((await f.manager.status({ name: "a" })).op?.status).toBe("unknown");
	f.port.status = async () => ({ operationRef: "wrong", status: { status: "terminal_ok" }, summaryCompleted: true });
	await expect(f.manager.status({ name: "a" })).rejects.toMatchObject({ detail: { reasonCode: "status_unavailable" } });
	expect(f.db.work.laneJobJson(result.jobId)).toBe(before);
	f.db.transaction((tx) => f.db.bindings.rotateEpoch(tx, workSessionKey("a")));
	expect(await f.manager.status({ name: "a" })).toMatchObject({ sessionId: "", op: null });
	await expect(f.manager.status({ name: "missing" })).rejects.toMatchObject({
		detail: { reasonCode: "unknown_work_lane" },
	});
});

test("steer distinguishes structured refusal from transport uncertainty without leaking text", async () => {
	const f = await fixture();
	await started(f);
	f.port.steer = async () => {
		throw new GjcCliError("secret token", 0, "", { code: "busy" });
	};
	expect(await f.manager.steer({ name: "a", text: "change" })).toEqual({
		steered: false,
		reason: "steer_refused:busy",
	});
	f.port.steer = async () => {
		throw new GjcCliError("secret token", 0, "", { code: "private_policy_code", refused: true });
	};
	expect(await f.manager.steer({ name: "a", text: "change" })).toEqual({
		steered: false,
		reason: "steer_refused:sdk_refused",
	});
	f.port.steer = async () => {
		throw new GjcCliError("secret token", 0, "", { code: "receipt_identity_mismatch" });
	};
	await expect(f.manager.steer({ name: "a", text: "change" })).rejects.toMatchObject({
		detail: { reasonCode: "steer_acceptance_uncertain" },
	});
	f.port.steer = async () => {
		throw new Error("secret token");
	};
	await expect(f.manager.steer({ name: "a", text: "change" })).rejects.toMatchObject({
		message: "work steer acceptance uncertain",
		detail: { reasonCode: "steer_acceptance_uncertain" },
	});
	expect(f.port.sends).toHaveLength(1);
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
});

test("caller timeout only detaches and a later completion settles the same operation response-only", async () => {
	const f = await fixture({ waitTimeoutMs: 25, ownerTarget: () => origin });
	const run = f.manager.run({ name: "a", text: "work", cwd: f.directory }, {});
	await expect(run).rejects.toMatchObject({ detail: { reasonCode: "work_wait_timeout" } });
	const op = f.port.sends[0]!.opRef;
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
	f.port.complete(op, "late answer");
	await until(() => f.db.work.workAttemptGet(op)?.settledAt !== null);
	expect(f.job().attempts[0]?.endState).toBe("completed");
	expect(f.notices).toHaveLength(0);
});

test("disconnect and AbortSignal release only their own waits, not worker observation", async () => {
	const f = await fixture();
	const ownerA = {};
	const ownerB = {};
	const abort = new AbortController();
	const a = f.manager.run({ name: "a", text: "work", cwd: f.directory }, ownerA).catch((e: unknown) => e);
	const b = f.manager.run({ name: "b", text: "work", cwd: f.directory }, ownerB, abort.signal).catch((e: unknown) => e);
	await until(() => f.port.sends.length === 2);
	f.manager.detachWaiters(ownerA);
	expect(await a).toBeInstanceOf(ProtocolError);
	expect(f.job("b").attempts[0]?.endedAt).toBeUndefined();
	abort.abort();
	expect(await b).toBeInstanceOf(ProtocolError);
	for (const send of f.port.sends) f.port.complete(send.opRef, "finished after caller left");
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.port.sends).toHaveLength(2);
});

test("a disconnect before send receipt cannot leave a newly registered waiter behind", async () => {
	const f = await fixture();
	const receipt = deferred<void>();
	const send = f.port.send.bind(f.port);
	f.port.send = async (input) => {
		const result = await send(input);
		await receipt.promise;
		return result;
	};
	const owner = {};
	const run = f.manager.run({ name: "a", text: "work", cwd: f.directory }, owner).catch((e: unknown) => e);
	await until(() => f.port.sends.length === 1);
	f.manager.detachWaiters(owner);
	receipt.resolve();
	expect(await run).toBeInstanceOf(ProtocolError);
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
});

test("notification target is immutable across owner reload; no-target remains no-target", async () => {
	let owner: OriginRef | undefined = origin;
	const f = await fixture({ ownerTarget: () => owner });
	const first = await started(f);
	owner = { ...origin, conversationId: "new-target" };
	f.port.complete(first.opRef, "first");
	await until(() => f.notices.length === 1);
	expect(f.notices[0]?.origin).toEqual(origin);
	owner = undefined;
	const second = await started(f, "b");
	owner = origin;
	f.port.complete(second.opRef, "second");
	await until(() => f.db.work.workAttemptGet(second.opRef)?.settledAt !== null);
	expect(f.notices).toHaveLength(1);
	expect(f.db.work.workAttemptGet(second.opRef)?.decision).toBe("no_target");
});

test("proven original silence suppresses before prefix and UTF8 clipping, and survives restart", async () => {
	const f = await fixture();
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "[SILENT]");
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt !== null);
	expect(f.db.work.workAttemptGet(result.opRef)?.output.knownSilence).toMatchObject({
		opRef: result.opRef,
		source: "turn.result",
		fullness: "original",
	});
	expect(f.db.work.workAttemptGet(result.opRef)?.decision).toBe("suppressed");
	await f.restart();
	expect(f.notices).toHaveLength(0);
	expect(f.db.outbox.rows()).toHaveLength(0);
	expect(utf8Prefix("HEAD" + "가".repeat(1000) + "TAIL")).toBe("HEAD" + "가".repeat(681));
	expect(utf8Prefix("😀가tail", 6)).toBe("😀");
	expect(utf8Prefix("😀가tail", 7)).toBe("😀가");
});

for (const [label, text] of [
	["preamble", "Nothing to report. [SILENT]"],
	["lowercase", "Nothing to report. [silent]"],
	["beyond excerpt", "HEAD:" + "가".repeat(1000) + "[SILENT]"],
] as const) {
	test(`proven original ${label} silence persists before clipping and suppresses start delivery`, async () => {
		const f = await fixture();
		const result = await started(f, "a", origin);
		f.port.complete(result.opRef, text);
		await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt != null);
		const runtime = f.db.work.workAttemptGet(result.opRef)!;
		expect(runtime.output.disposition).toBe("silent");
		expect(runtime.output.knownSilence).toMatchObject({
			opRef: result.opRef,
			sessionId: result.sessionId,
			epoch: runtime.epoch,
			source: "turn.result",
			fullness: "original",
			attribution: "operation_ref",
			clientRef: result.opRef,
			byteLength: Buffer.byteLength(text),
		});
		expect(runtime.output.knownSilence).toEqual(runtime.output.proof);
		expect(runtime.output.excerpt).toBe(utf8Prefix(text));
		if (label === "beyond excerpt") expect(runtime.output.excerpt).not.toContain("[SILENT]");
		expect(runtime.decision).toBe("suppressed");
		expect(f.notices).toHaveLength(0);
		expect(f.db.outbox.rows()).toHaveLength(0);
		await f.restart();
		expect(f.db.work.workAttemptGet(result.opRef)?.output.knownSilence).toEqual(runtime.output.knownSilence);
		expect(f.notices).toHaveLength(0);
		expect(f.db.outbox.rows()).toHaveLength(0);
	});

	test(`run returns full original ${label} silence body without a completion notification`, async () => {
		const f = await fixture({ ownerTarget: () => origin });
		const run = f.manager.run({ name: "a", text: "work", cwd: f.directory }, {});
		await until(() => f.port.sends.length === 1);
		const opRef = f.port.sends[0]!.opRef;
		f.port.complete(opRef, text);
		expect(await run).toMatchObject({ held: false, text, opRef });
		const runtime = f.db.work.workAttemptGet(opRef)!;
		expect(runtime.mode).toBe("run");
		expect(runtime.parent).toBeNull();
		expect(runtime.output.knownSilence).toMatchObject({
			opRef,
			byteLength: Buffer.byteLength(text),
			fullness: "original",
		});
		expect(runtime.output.disposition).toBe("silent");
		expect(runtime.decision).toBe("suppressed");
		expect(f.notices).toHaveLength(0);
		expect(f.db.outbox.rows()).toHaveLength(0);
	});
}

test("unavailable output preserves completed outcome without prior assistant fallback", async () => {
	const f = await fixture();
	f.port.fetchWorkerOutput = async () => ({ status: "unavailable", code: "identity_mismatch" });
	f.port.fetchLastAssistant = async () => {
		throw new Error("must not read unscoped output");
	};
	const run = f.manager.run({ name: "a", text: "work", cwd: f.directory }, {});
	await until(() => f.port.sends.length === 1);
	f.port.complete(f.port.sends[0]!.opRef, "untrusted");
	await expect(run).rejects.toMatchObject({ detail: { reasonCode: "output_unavailable" } });
	expect(f.job().attempts[0]?.endState).toBe("completed");
	expect(f.notices).toHaveLength(0);
});

test("three durable output read claims retain 1s/5s eligibility across restart", async () => {
	let now = Date.now();
	const f = await fixture({ now: () => now });
	let reads = 0;
	f.port.fetchWorkerOutput = async () => {
		reads++;
		return { status: "absent", code: "output_pending" };
	};
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "not published");
	await until(() => reads === 1);
	expect(f.db.work.workAttemptGet(result.opRef)?.output.reads).toBe(1);
	await f.restart();
	await Bun.sleep(20);
	expect(reads).toBe(1);
	now += 1000;
	await until(() => reads === 2);
	expect(f.db.work.workAttemptGet(result.opRef)?.output.reads).toBe(2);
	now += 4999;
	await Bun.sleep(20);
	expect(reads).toBe(2);
	now += 1;
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt !== null);
	expect(reads).toBe(3);
	expect(f.notices[0]?.text).toBe("[lane a] completed: output_unavailable");
});

for (const [reason, state] of [
	["cancelled", "attempt_ended"],
	["max_tokens", "attempt_ended"],
	["max_turn_requests", "attempt_ended"],
	["refusal", "attempt_ended"],
	["unknown_reason", "attempt_ended"],
] as const) {
	test(`terminal ${reason} is not run success and notifications preserve safe reason`, async () => {
		const f = await fixture();
		const result = await started(f, "a", origin);
		f.port.complete(result.opRef, "가".repeat(1000));
		f.port.status = async (input) => ({
			operationRef: input.opRef,
			status: { status: "terminal_ok", receiptState: "present", outcome: { reason } },
			summaryCompleted: true,
		});
		await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt !== null);
		expect(f.job().attempts[0]?.endState).toBe(state);
		const suffix = f.notices[0]!.text.slice("[lane a] attempt_ended: ".length);
		expect(suffix.startsWith(`${reason === "unknown_reason" ? "stopped_incomplete" : reason}: `)).toBe(true);
		expect(Buffer.byteLength(suffix)).toBeLessThanOrEqual(2048);
	});
}

test("failing status reads back off and log one diagnostic per distinct reason (#262)", async () => {
	const f = await fixture();
	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(async () => errors.mockRestore());
	const result = await started(f);
	let queries = 0;
	let message = "reconciliation unavailable token=sk-secret-value";
	f.port.status = async () => {
		queries++;
		throw new Error(message);
	};
	await Bun.sleep(300);
	// Fixed 5ms polling would have issued ~60 reads; backoff bounds it.
	expect(queries).toBeGreaterThan(0);
	expect(queries).toBeLessThan(12);
	const lines = () =>
		errors.mock.calls
			.map((call) => String(call[0]))
			.filter((line) => line.startsWith("work_reconciliation_unavailable"));
	expect(lines()).toHaveLength(1);
	expect(lines()[0]).toContain(`opRef=${result.opRef}`);
	expect(lines()[0]).toContain("reason=Error: reconciliation unavailable");
	expect(lines()[0]).not.toContain("sk-secret-value");
	message = "offline";
	await until(() => lines().length === 2);
	expect(lines()[1]).toContain("reason=Error: offline");
	expect(f.db.work.workAttemptGet(result.opRef)?.settledAt).toBeNull();
});

test("a rebound lane stops the observer and settles the attempt as session_disowned (#262)", async () => {
	const f = await fixture({ pollMs: 1 });
	const result = await started(f, "a", origin);
	let queries = 0;
	f.port.status = async () => {
		queries++;
		throw new Error("invalid status identity");
	};
	await until(() => queries > 0);
	f.db.transaction((tx) => f.db.bindings.rotateEpoch(tx, result.sessionKey));
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt !== null);
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("session_disowned");
	expect(f.job().attempts[0]?.endState).toBe("terminal_uncertain");
	expect(f.job().state).toBe("awaiting_operator");
	expect(f.notices[0]?.text).toBe("[lane a] attempt_ended: session_disowned: output_unavailable");
	const settledQueries = queries;
	await Bun.sleep(50);
	expect(queries).toBe(settledQueries);
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.resumes).toHaveLength(0);
});

test("persistently failing status without liveness proof settles instead of polling forever (#262)", async () => {
	const f = await fixture({ pollMs: 1 });
	const result = await started(f, "a", origin);
	let queries = 0;
	f.port.status = async () => {
		queries++;
		throw new Error("offline");
	};
	f.port.liveness = async () => {
		throw new Error("offline");
	};
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt !== null);
	expect(queries).toBeLessThanOrEqual(10);
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("recovery_indeterminate");
	expect(f.job().attempts[0]?.endState).toBe("terminal_uncertain");
	const settledQueries = queries;
	await Bun.sleep(50);
	expect(queries).toBe(settledQueries);
	expect(f.port.sends).toHaveLength(1);
});

test("live restart reattaches exact session/op without bind, resume or replay", async () => {
	const f = await fixture();
	const result = await started(f);
	await f.restart();
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.resumes).toHaveLength(0);
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
	await expect(f.manager.start({ name: "a", text: "again", cwd: f.directory, resume: true })).rejects.toMatchObject({
		detail: { reasonCode: "attempt_open" },
	});
	f.port.complete(result.opRef, "recovered");
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.job().attempts[0]?.endState).toBe("completed");
});

test("live unknown restart observes without replay or false acceptance", async () => {
	const f = await fixture();
	const result = await started(f);
	f.port.status = async (input) => ({
		operationRef: input.opRef,
		status: { status: "unknown" },
		summaryCompleted: false,
	});
	await f.restart();
	await Bun.sleep(20);
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal).toBeNull();
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.resumes).toHaveLength(0);
});

test("dead restart settles uncertainty into a sticky hold, never replacement bind", async () => {
	const f = await fixture();
	const result = await started(f, "a", origin);
	f.port.setSessionState(result.sessionId, { live: false });
	await f.restart();
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.job().state).toBe("awaiting_operator");
	expect(f.job().attempts[0]?.endState).toBe("terminal_uncertain");
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("session_dead");
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.resumes).toHaveLength(0);
	expect(f.port.sends).toHaveLength(1);
	expect(await f.manager.start({ name: "a", text: "again", cwd: f.directory })).toMatchObject({
		started: false,
		held: true,
	});
	expect(f.notices[0]?.text).toBe("[lane a] attempt_ended: session_dead: output_unavailable");
});

test("force-retiring an open dead lane settles its runtime and permits later work", async () => {
	const f = await fixture({ ownerTarget: () => origin });
	const result = await started(f, "a", origin);
	f.port.setSessionState(result.sessionId, { live: false });

	expect(await f.lanes.retireAllDead()).toEqual({ count: 1, names: ["a"] });
	const runtime = f.db.work.workAttemptGet(result.opRef);
	const attempt = f.job().attempts[0];
	expect(runtime).toBeDefined();
	expect(attempt).toBeDefined();
	expect(runtime?.settledAt).toBe(attempt?.endedAt);
	expect(runtime?.terminal).toMatchObject({ kind: "local", reasonCode: "session_dead" });
	expect(attempt).toMatchObject({ endState: "attempt_ended", errorCode: "host_lost" });
	expect(f.db.work.workAttemptOpen()).toHaveLength(0);

	const next = await f.manager.start({ name: "a", text: "again", cwd: f.directory, resume: true });
	expect(next.started).toBe(true);
	await expect(f.manager.recover()).resolves.toBeUndefined();
});

test("force retirement releases a pending work.run waiter promptly", async () => {
	const f = await fixture({ waitTimeoutMs: 60_000 });
	const run = f.manager.run({ name: "a", text: "work", cwd: f.directory }, {}).then(
		(value) => ({ value }),
		(error: unknown) => ({ error }),
	);
	await until(() => f.port.sends.length === 1);
	await Bun.sleep(10);
	const { sessionId } = f.port.sends[0]!;
	f.port.setSessionState(sessionId, { live: false });

	expect(await f.lanes.forceRetire("a")).toMatchObject({ retired: true });
	const outcome = await Promise.race([run, Bun.sleep(250).then(() => null)]);
	expect(outcome).not.toBeNull();
	expect(outcome).toMatchObject({ error: { detail: { reasonCode: "session_dead" } } });
});

test("force retirement preserves a recorded terminal outcome in lane history", async () => {
	const f = await fixture({ ownerTarget: () => origin });
	f.port.fetchWorkerOutput = async () => ({ status: "absent", code: "output_pending" });
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "durable result");
	await until(() => {
		const runtime = f.db.work.workAttemptGet(result.opRef);
		return runtime?.terminal?.reasonCode === "end_turn" && runtime.output.reads > 0;
	});
	f.port.setSessionState(result.sessionId, { live: false });

	expect(await f.lanes.forceRetire("a")).toMatchObject({ retired: true });
	const runtime = f.db.work.workAttemptGet(result.opRef);
	const attempt = f.job().attempts[0];
	expect(runtime?.terminal).toMatchObject({ kind: "broker", reasonCode: "end_turn" });
	expect(attempt).toMatchObject({ endState: "completed" });
	expect(attempt?.errorCode).toBeUndefined();
});

test("force-retiring a disowned lane records a session_disowned terminal", async () => {
	const f = await fixture();
	const result = await started(f);
	f.port.liveness = async () => ({ live: undefined, disowned: true });

	expect(await f.lanes.retireAllDead()).toEqual({ count: 1, names: ["a"] });
	const runtime = f.db.work.workAttemptGet(result.opRef);
	const attempt = f.job().attempts[0];
	expect(runtime?.settledAt).toBe(attempt?.endedAt);
	expect(runtime?.terminal).toMatchObject({ kind: "local", reasonCode: "session_disowned" });
});

test("one inconsistent runtime is logged with its opRef and does not block other lane recovery", async () => {
	const f = await fixture();
	const bad = await started(f, "a");
	const good = await started(f, "b");
	await f.manager.stop();
	const closed = closeAttempt({
		record: f.job("a"),
		opRef: bad.opRef,
		endState: "attempt_ended",
		errorCode: "host_lost",
		endedAt: new Date(Date.now() + 1_000).toISOString(),
	});
	const identity = laneJobIdentity("a");
	f.db.transaction((tx) =>
		f.db.work.putLaneJob(tx, { ...closed, laneKey: identity.laneKey, json: JSON.stringify(closed) }),
	);
	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(async () => errors.mockRestore());
	let recoveredOther = false;
	const status = f.port.status.bind(f.port);
	f.port.status = async (input) => {
		if (input.opRef === good.opRef) recoveredOther = true;
		return status(input);
	};

	await expect(f.restart()).resolves.toBeDefined();
	await until(() => recoveredOther);

	// Verify that the invalid attempt was logged and then quarantined
	const errorLogged = errors.mock.calls.some((call) => {
		const line = String(call[0]);
		return (
			line.includes("work_recovery_invalid_attempt") &&
			line.includes(bad.opRef) &&
			line.includes("attempt.endedAt matches runtime.settledAt")
		);
	});
	expect(errorLogged).toBe(true);

	// Verify that the attempt is quarantined: it should not be queried in recovery
	const isQuarantined = f.db.bindings.isQuarantined("work", f.db.work.workAttemptJobId(bad.opRef)!);
	expect(isQuarantined).toBe(true);
	f.port.complete(good.opRef, "recovered other lane");
	await until(() => f.db.work.workAttemptGet(good.opRef)?.settledAt !== null);
});

test("an inconsistent runtime is quarantined and not re-logged on subsequent recovery calls (issue #407)", async () => {
	const f = await fixture();
	const bad = await started(f, "a");
	const good = await started(f, "b");
	await f.manager.stop();
	const endedAt = new Date(Date.now() + 1_000).toISOString();
	const closed = closeAttempt({
		record: f.job("a"),
		opRef: bad.opRef,
		endState: "attempt_ended",
		errorCode: "host_lost",
		endedAt,
	});
	const identity = laneJobIdentity("a");
	f.db.transaction((tx) =>
		f.db.work.putLaneJob(tx, { ...closed, laneKey: identity.laneKey, json: JSON.stringify(closed) }),
	);
	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(async () => errors.mockRestore());
	let recoveredOther = false;
	const status = f.port.status.bind(f.port);
	f.port.status = async (input) => {
		if (input.opRef === good.opRef) recoveredOther = true;
		return status(input);
	};

	await expect(f.restart()).resolves.toBeDefined();
	await until(() => recoveredOther);

	// Verify that the invalid attempt was logged
	const errorLogged = errors.mock.calls.some((call) => {
		const line = String(call[0]);
		return (
			line.includes("work_recovery_invalid_attempt") &&
			line.includes(bad.opRef) &&
			line.includes("attempt.endedAt matches runtime.settledAt")
		);
	});
	expect(errorLogged).toBe(true);

	const badJobId = f.db.work.workAttemptJobId(bad.opRef)!;
	// Verify that the attempt's job was quarantined
	expect(f.db.bindings.isQuarantined("work", badJobId)).toBe(true);

	// Verify that recovery doesn't re-log the same error on subsequent calls
	const errorCountBefore = errors.mock.calls.length;
	await f.manager.recover();
	const errorCountAfter = errors.mock.calls.length;
	// Should not have new work_recovery_invalid_attempt logs
	const newInvalidLogs = [...errors.mock.calls.slice(errorCountBefore)].filter((call) => {
		const line = String(call[0]);
		return line.includes("work_recovery_invalid_attempt") && line.includes(bad.opRef);
	});
	expect(newInvalidLogs.length).toBe(0);

	f.port.complete(good.opRef, "recovered other lane");
	await until(() => f.db.work.workAttemptGet(good.opRef)?.settledAt !== null);
});

test("a lane row torn under a live observer is logged and dropped, not an unhandled rejection (#401)", async () => {
	const rejections: unknown[] = [];
	const onRejection = (reason: unknown) => rejections.push(reason);
	process.on("unhandledRejection", onRejection);
	cleanups.push(async () => {
		process.off("unhandledRejection", onRejection);
	});
	const errors = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(async () => errors.mockRestore());
	const f = await fixture();
	const torn = await started(f, "a");
	const healthy = await started(f, "b");
	// The #401 shape: the lane row says the attempt ended host_lost while its
	// runtime row is still open, so every runtime read for it throws.
	const closed = closeAttempt({
		record: f.job("a"),
		opRef: torn.opRef,
		endState: "attempt_ended",
		errorCode: "host_lost",
		endedAt: new Date(Date.now() + 1_000).toISOString(),
	});
	f.db.transaction((tx) =>
		f.db.work.putLaneJob(tx, { ...closed, laneKey: laneJobIdentity("a").laneKey, json: JSON.stringify(closed) }),
	);
	expect(() => f.db.work.workAttemptGet(torn.opRef)).toThrow(WorkAttemptStateError);

	await until(() =>
		errors.mock.calls.some((call) => {
			const line = String(call[0]);
			return line.includes("work_observer_failed") && line.includes(torn.opRef);
		}),
	);
	// Several more poll periods: a dropped observer must not keep throwing.
	await Bun.sleep(50);
	expect(rejections).toEqual([]);
	f.port.complete(healthy.opRef, "other lane still served");
	await until(() => f.db.work.workAttemptGet(healthy.opRef)?.settledAt !== null);
	expect(rejections).toEqual([]);
});

test("saved terminal proof wins over dead liveness and output recovery consumes saved budget", async () => {
	let now = Date.now();
	const f = await fixture({ now: () => now });
	const original = f.port.fetchWorkerOutput.bind(f.port);
	f.port.fetchWorkerOutput = async () => ({ status: "absent", code: "output_pending" });
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "durable result");
	await until(() => f.db.work.workAttemptGet(result.opRef)?.output.reads === 1);
	f.port.setSessionState(result.sessionId, { live: false });
	f.port.status = async () => {
		throw new Error("offline");
	};
	f.port.fetchWorkerOutput = original;
	now += 1000;
	await f.restart();
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.job().attempts[0]?.endState).toBe("completed");
	expect(f.notices[0]?.text).toBe("[lane a] completed: durable result");
});

test("historical open attempt recovery uses no current notification target and never replays", async () => {
	const f = await fixture({ ownerTarget: () => origin });
	const { jobId, laneKey } = laneJobIdentity("old");
	const sessionId = crypto.randomUUID();
	f.db.transaction((tx) => f.db.bindings.bind(tx, workSessionKey("old"), sessionId));
	f.port.setSessionState(sessionId, { repo: f.directory, live: false });
	const job = appendAttempt(createLaneJobRecord({ jobId, branch: "work/old", worktreePath: f.directory }), {
		opRef: "old-open-attempt",
		sessionId,
		startedAt: new Date().toISOString(),
	});
	f.db.transaction((tx) => f.db.work.putLaneJob(tx, { ...job, laneKey, json: JSON.stringify(job) }));
	await f.manager.recover();
	await until(() => f.db.work.workAttemptGet("old-open-attempt")?.settledAt != null);
	expect(f.db.work.workAttemptGet("old-open-attempt")).toMatchObject({
		mode: "historical",
		parent: null,
		decision: "no_target",
	});
	expect(f.notices).toHaveLength(0);
	expect(f.port.binds).toHaveLength(0);
	expect(f.port.sends).toHaveLength(0);
});

test("late output from an obsolete generation cannot settle or fan out", async () => {
	let generation = 1;
	const f = await fixture({ brokerGeneration: () => generation });
	const gate = deferred<WorkerOutputResult>();
	let reading = false;
	const original = f.port.fetchWorkerOutput.bind(f.port);
	f.port.fetchWorkerOutput = async () => {
		reading = true;
		return gate.promise;
	};
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "answer");
	await until(() => reading);
	const before = f.db.work.workAttemptGet(result.opRef)!;
	generation++;
	const recovery = f.manager.onBrokerGeneration();
	gate.resolve({ status: "unavailable", code: "output_unavailable" });
	await recovery;
	expect(f.db.work.workAttemptGet(result.opRef)?.output.disposition).toBe("pending");
	expect(f.notices).toHaveLength(0);
	expect(f.db.work.workAttemptGet(result.opRef)?.version).toBe(before.version);
	f.port.fetchWorkerOutput = original;
});

test("binding epoch fences an already-running output read", async () => {
	const f = await fixture();
	const gate = deferred<WorkerOutputResult>();
	let reading = false;
	f.port.fetchWorkerOutput = async () => {
		reading = true;
		return gate.promise;
	};
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "answer");
	await until(() => reading);
	const before = f.db.work.workAttemptGet(result.opRef)!;
	f.db.transaction((tx) => f.db.bindings.rotateEpoch(tx, result.sessionKey));
	gate.resolve({ status: "unavailable", code: "output_unavailable" });
	await Bun.sleep(20);
	expect(f.db.work.workAttemptGet(result.opRef)?.version).toBe(before.version);
	expect(f.notices).toHaveLength(0);
});

test("stop releases run before draining a blocked finite output read and preserves open durable state", async () => {
	const f = await fixture();
	const gate = deferred<WorkerOutputResult>();
	let reading = false;
	f.port.fetchWorkerOutput = async () => {
		reading = true;
		return gate.promise;
	};
	const run = f.manager.run({ name: "a", text: "work", cwd: f.directory }, {}).catch((e: unknown) => e);
	await until(() => f.port.sends.length === 1);
	const op = f.port.sends[0]!.opRef;
	f.port.complete(op, "answer");
	await until(() => reading);
	const stop = f.manager.stop();
	expect(await run).toBeInstanceOf(ProtocolError);
	gate.resolve({ status: "unavailable", code: "cancelled" });
	await stop;
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
	expect(f.db.work.workAttemptGet(op)?.output.reads).toBe(1);
	expect(f.db.work.workAttemptGet(op)?.output.disposition).toBe("pending");
	expect(f.notices).toHaveLength(0);
});

test("invalid shared parameters have no bind/send effects and manager registration is unique", async () => {
	const f = await fixture();
	for (const params of [
		{ name: "../bad", text: "x" },
		{ name: "a", text: "" },
		{ name: "a", text: "x", cwd: "relative" },
		{ name: "a", text: "x", resume: "yes" },
		{ name: "a", text: "x", model: { preset: "x", extra: true } },
		{ name: "a", text: "x", extra: true },
	])
		await expect(f.manager.start(params)).rejects.toMatchObject({
			code: "invalid_params",
			message: "invalid work parameters",
		});
	await expect(f.manager.run({ name: "a", text: "x", extra: true }, {})).rejects.toMatchObject({
		detail: { field: "extra" },
	});
	expect(f.port.binds).toHaveLength(0);
	expect(f.port.sends).toHaveLength(0);
	expect(
		() =>
			new WorkLaneManager({
				database: f.db,
				port: f.port,
				lanes: f.lanes,
				gajaewayHome: f.gajaewayHome,
				serviceDirectory: f.serviceDirectory,
			}),
	).toThrow("already registered");
});

test("start and run nested refusal happens before recover and bind", async () => {
	const f = await fixture();
	const parent = await f.manager.start({ name: "parent", text: "parent task", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	let recoverCalls = 0;
	f.manager.recover = async () => {
		recoverCalls++;
	};
	const binds = f.port.binds.length;
	const sends = f.port.sendAttempts.length;
	await expect(
		f.manager.run({ name: "child", text: "nested run", cwd: f.directory, callerSessionId: parent.sessionId }, {}),
	).rejects.toMatchObject({
		code: "unauthorized",
		detail: { reasonCode: "nested_lane_forbidden", verb: "run", parent: "parent" },
	});
	await expect(
		f.manager.start({ name: "child", text: "nested start", cwd: f.directory, callerSessionId: parent.sessionId }),
	).rejects.toMatchObject({
		code: "unauthorized",
		detail: { reasonCode: "nested_lane_forbidden", verb: "start", parent: "parent" },
	});
	expect(f.db.work.laneJobJson(laneJobIdentity("child").jobId)).toBeUndefined();
	expect(recoverCalls).toBe(0);
	expect(f.port.binds).toHaveLength(binds);
	expect(f.port.sendAttempts).toHaveLength(sends);
});
test("nested starts refuse self and ancestor cycles before binding", async () => {
	const f = await fixture({ allowNested: () => true });
	const root = await f.manager.start({ name: "root", text: "root task", cwd: f.directory });
	if (!root.started) throw new Error("root did not start");
	const child = await f.manager.start({
		name: "child",
		text: "child task",
		cwd: f.directory,
		callerSessionId: root.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	const binds = f.port.binds.length;
	const sends = f.port.sendAttempts.length;
	for (const name of ["child", "root"]) {
		await expect(
			f.manager.start({ name, text: "cycle", cwd: f.directory, callerSessionId: child.sessionId }),
		).rejects.toMatchObject({ code: "invalid_params", detail: { reasonCode: "nested_lane_cycle" } });
	}
	expect(f.port.binds).toHaveLength(binds);
	expect(f.port.sendAttempts).toHaveLength(sends);
});

test("issue #480: a new lane requires either cwd or repo", async () => {
	const f = await fixture();
	await expect(f.manager.start({ name: "missing-location", text: "work" })).rejects.toMatchObject({
		code: "invalid_params",
		detail: { reasonCode: "repo_or_cwd_required" },
	});
	expect(f.port.binds).toHaveLength(0);
	expect(f.db.work.laneJobJson(laneJobIdentity("missing-location").jobId)).toBeUndefined();
});

test("issue #480: base cannot be selected without a repository binding", async () => {
	const f = await fixture();
	await expect(
		f.manager.start({ name: "base-without-repo", text: "work", cwd: f.directory, base: "dev" }),
	).rejects.toMatchObject({
		code: "invalid_params",
		detail: { reasonCode: "base_requires_repo" },
	});
	expect(f.port.binds).toHaveLength(0);
});

test("issue #480: repository URLs with embedded credentials are rejected", async () => {
	const f = await fixture();
	for (const repo of ["https://user:password@example.test/project.git", "https://user@example.test/project.git"]) {
		await expect(f.manager.start({ name: "credentials", text: "work", repo })).rejects.toMatchObject({
			code: "invalid_params",
			detail: { reasonCode: "invalid_work_params", field: "repo" },
		});
	}
	expect(f.port.binds).toHaveLength(0);
});

test("issue #480: an existing managed repository path is not removed on collision", async () => {
	const { bareRepo, cleanup } = await bareRepoFixture("repository-path-collision");
	try {
		const f = await fixture();
		const repositoryPath = managedLaneRepositoryPath(f.gajaewayHome, "occupied");
		await mkdir(repositoryPath, { recursive: true });
		const sentinelPath = join(repositoryPath, "keep.txt");
		await Bun.write(sentinelPath, "preserve");

		await expect(f.manager.start({ name: "occupied", text: "work", repo: bareRepo })).rejects.toMatchObject({
			code: "invalid_params",
			detail: { reasonCode: "lane_repository_path_exists" },
		});
		expect(await Bun.file(sentinelPath).text()).toBe("preserve");
		expect(f.port.binds).toHaveLength(0);
	} finally {
		await cleanup();
	}
});

test("issue #480: repo URL creates a cwd-independent worktree with the bound push remote", async () => {
	let repoDir = await mkdtemp(join(tmpdir(), "test-repo-binding-"));
	let bareRepo = await mkdtemp(join(tmpdir(), "bare-repo-binding-"));
	// Normalize paths to resolve macOS /var -> /private/var symlink
	repoDir = await realpath(repoDir);
	bareRepo = await realpath(bareRepo);
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", repoDir, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "test",
					GIT_AUTHOR_EMAIL: "test@test",
					GIT_COMMITTER_NAME: "test",
					GIT_COMMITTER_EMAIL: "test@test",
				},
			});
		git("init", "-q");
		git("config", "user.name", "test");
		git("config", "user.email", "test@test");
		git("commit", "-q", "--allow-empty", "-m", "initial");
		Bun.spawnSync(["git", "clone", "--bare", repoDir, bareRepo], {
			stdout: "ignore",
			stderr: "ignore",
		});

		const repositoryUrl = pathToFileURL(bareRepo).href;
		const f = await fixture({ serviceDirectory: process.cwd() });
		const result = await f.manager.start({
			name: "remote-binding",
			text: "work",
			repo: repositoryUrl,
		});
		if (!result.started) throw new Error("work did not start");
		const job = f.job("remote-binding");
		expect(job.lane.worktreePath).toBe(managedLaneWorktreePath(f.gajaewayHome, "remote-binding"));
		expect(job.lane.repository).toBe(repositoryUrl);
		expect(job.lane.repositoryPath).toBe(managedLaneRepositoryPath(f.gajaewayHome, "remote-binding"));
		expect(job.baselineSha).toMatch(/^[0-9a-f]{40}$/);
		const originUrl = Bun.spawnSync(["git", "-C", job.lane.worktreePath, "remote", "get-url", "origin"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(originUrl.exitCode).toBe(0);
		expect(originUrl.stdout?.toString().trim()).toBe(repositoryUrl);
		expect(f.port.binds.at(-1)?.repo).toBe(job.lane.worktreePath);
		expect(await f.manager.status({ name: "remote-binding" })).toMatchObject({
			repository: { url: repositoryUrl, branch: managedLaneBranchName("remote-binding"), base: job.baselineSha },
		});
	} finally {
		await rm(repoDir, { recursive: true, force: true });
		await rm(bareRepo, { recursive: true, force: true });
	}
});

test("remote clones are asynchronous, preserve repo_clone_failed, and clean up managed paths", async () => {
	const requestReceived = deferred<void>();
	const server = createServer((socket) => {
		socket.once("data", () => {
			requestReceived.resolve(undefined);
			setTimeout(() => socket.end("0014ERR unavailable\n"), 50);
		});
	});
	await new Promise<void>((resolve, reject) => {
		server.once("error", reject);
		server.listen(0, "127.0.0.1", resolve);
	});
	try {
		const address = server.address();
		if (!address || typeof address === "string") throw new Error("local git server did not bind a TCP port");
		const f = await fixture();
		const name = "missing-remote-clone";
		const remoteUrl = `git://127.0.0.1:${address.port}/missing.git`;
		const clone = f.manager.start({ name, text: "work", repo: remoteUrl });
		await requestReceived.promise;
		await Bun.sleep(10);
		await expect(clone).rejects.toMatchObject({
			code: "invalid_params",
			detail: { reasonCode: "repo_clone_failed", repo: remoteUrl },
		});
		await expect(lstat(managedLaneWorktreePath(f.gajaewayHome, name))).rejects.toMatchObject({ code: "ENOENT" });
		await expect(lstat(managedLaneRepositoryPath(f.gajaewayHome, name))).rejects.toMatchObject({ code: "ENOENT" });
	} finally {
		await new Promise<void>((resolve) => server.close(() => resolve()));
	}
});
test("run of the caller's own open lane is attempt_open, not a cycle", async () => {
	const f = await fixture({ allowNested: () => true });
	const parent = await f.manager.start({ name: "same", text: "parent task", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	await expect(
		f.manager.run({ name: "same", text: "nested run", cwd: f.directory, callerSessionId: parent.sessionId }, {}),
	).rejects.toMatchObject({ code: "invalid_params", detail: { reasonCode: "attempt_open" } });
});

test("run from a lane with nesting allowed remains response-only", async () => {
	const f = await fixture({ allowNested: () => true });
	const parent = await f.manager.start({ name: "parent", text: "parent task", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	const run = f.manager.run(
		{ name: "run-child", text: "response-only task", cwd: f.directory, callerSessionId: parent.sessionId },
		{},
	);
	await until(() => f.port.sends.length === 2);
	const opRef = f.port.sends[1]!.opRef;
	f.port.complete(opRef, "response only");
	expect(await run).toMatchObject({ held: false, text: "response only", opRef });
	const runtime = f.db.work.workAttemptGet(opRef)!;
	expect(runtime).toMatchObject({ mode: "run", parent: null, decision: "no_target" });
	expect(f.db.work.laneReportsByParent("parent")).toEqual([]);
	expect(f.db.outbox.rows()).toHaveLength(0);
	expect(f.port.steers).toHaveLength(0);
});
// receiptState=missing is covered by the #248 late-receipt/held tests below:
// a proven same-op final body reconciles it instead of holding.
for (const [receiptState, reasonCode, endState] of [["unknown", "terminal_uncertain", "terminal_uncertain"]] as const) {
	test(`terminal receipt ${receiptState} is a held non-success with a safe notice`, async () => {
		const f = await fixture();
		const result = await started(f, "a", origin);
		f.port.complete(result.opRef, "partial answer");
		f.port.status = async (input) => ({
			operationRef: input.opRef,
			status: { status: "terminal_ok", receiptState, outcome: { reason: "end_turn" } },
			summaryCompleted: true,
		});
		await until(() => f.db.work.workAttemptOpen().length === 0);
		expect(f.job().attempts[0]?.endState).toBe(endState);
		expect(f.job().state).toBe("awaiting_operator");
		expect(f.notices[0]?.text).toBe(`[lane a] attempt_ended: ${reasonCode}: partial answer`);
	});
}

test("late receipt after a missing-receipt terminal is reconciled once, without resend (#248)", async () => {
	let now = Date.now();
	const f = await fixture({ now: () => now });
	const original = f.port.fetchWorkerOutput.bind(f.port);
	let reads = 0;
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "PR opened");
	const status = f.port.status.bind(f.port);
	// The terminal is observed before the final body lands: the SDK reports
	// receiptState=missing, then enriches the same op to present.
	f.port.status = async (input) => {
		const report = await status(input);
		return { ...report, status: { ...report.status, receiptState: "missing" } };
	};
	f.port.fetchWorkerOutput = async (input) => {
		reads++;
		if (reads === 1) {
			const early = await original(input);
			expect(early.status).toBe("proven");
			return parseWorkerOutputResponse(
				input,
				{
					exitCode: 0,
					stdout: JSON.stringify({
						ok: true,
						result: {
							kind: "prompt",
							clientRef: input.opRef,
							...input.terminalIdentity,
							status: "terminal_ok",
							terminalAt: input.notBeforeMs + 1,
							receiptState: "missing",
						},
					}),
					stderr: "",
				},
				Date.now(),
			);
		}
		return original(input);
	};
	await until(() => reads === 1);
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("terminal_missing_receipt");
	expect(f.db.work.workAttemptGet(result.opRef)?.settledAt).toBeNull();
	now += 1000;
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt != null);
	expect(reads).toBe(2);
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("end_turn");
	expect(f.job().attempts[0]?.endState).toBe("completed");
	expect(f.job().state).not.toBe("awaiting_operator");
	expect(f.notices.map((notice) => notice.text)).toEqual(["[lane a] completed: PR opened"]);
	// Reprocessing the same terminal neither re-notifies nor re-runs the work.
	await f.restart();
	await Bun.sleep(20);
	expect(f.notices).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.resumes).toHaveLength(0);
});

test("#562 a missing receipt stays open during turn activity and recovery until lifecycle end", async () => {
	let now = Date.now();
	const f = await fixture({ now: () => now });
	let statusReads = 0;
	let outputReads = 0;
	const result = await started(f, "a", origin);
	f.port.status = async (input) => {
		statusReads++;
		return {
			operationRef: input.opRef,
			status: { status: "terminal_ok", receiptState: "missing", outcome: { reason: "end_turn" } },
			summaryCompleted: true,
		};
	};
	f.port.fetchWorkerOutput = async () => {
		outputReads++;
		return { status: "absent", code: "output_pending" };
	};
	await until(() => statusReads > 0);
	f.port.emitTool(result.sessionId, { toolName: "bash" });
	await until(() => statusReads > 1);
	await Bun.sleep(30);
	expect(f.db.work.workAttemptGet(result.opRef)).toMatchObject({
		settledAt: null,
		terminal: null,
		output: { reads: 0, disposition: "pending" },
	});
	expect(outputReads).toBe(0);
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
	expect(f.notices).toEqual([]);

	await f.restart();
	await until(() => f.port.tailsOf(result.sessionId).some((tail) => tail.opRef === result.opRef && tail.running));
	await Bun.sleep(30);
	expect(f.db.work.workAttemptGet(result.opRef)).toMatchObject({ settledAt: null, terminal: null });
	expect(outputReads).toBe(0);
	expect(f.notices).toEqual([]);

	// Only the turn lifecycle end begins the bounded missing-receipt retrieval budget.
	f.port.emitTurnEnd(result.sessionId, result.opRef, "agent_end");
	await until(() => outputReads === 1);
	expect(f.db.work.workAttemptGet(result.opRef)).toMatchObject({
		turnEndObservedAt: expect.any(String),
		terminal: { reasonCode: "terminal_missing_receipt" },
		settledAt: null,
	});
	now += 1000;
	await until(() => outputReads === 2);
	now += 5000;
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt !== null);
	expect(outputReads).toBe(3);
	expect(f.job().attempts[0]?.endState).toBe("terminal_missing_receipt");
});

test("a receipt still missing after the output budget stays held with a non-loss diagnostic (#248)", async () => {
	let now = Date.now();
	const f = await fixture({ now: () => now });
	let reads = 0;
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "unused");
	f.port.status = async (input) => ({
		operationRef: input.opRef,
		status: { status: "terminal_ok", receiptState: "missing", outcome: { reason: "end_turn", kind: "stopped" } },
		summaryCompleted: true,
	});
	f.port.fetchWorkerOutput = async (input) => {
		reads++;
		return parseWorkerOutputResponse(
			input,
			{
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: {
						kind: "prompt",
						clientRef: input.opRef,
						status: "terminal_ok",
						terminalAt: input.notBeforeMs + 1,
						receiptState: "missing",
					},
				}),
				stderr: "",
			},
			Date.now(),
		);
	};
	await until(() => reads === 1);
	now += 1000;
	await until(() => reads === 2);
	now += 5000;
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(reads).toBe(3);
	expect(f.job().attempts[0]?.endState).toBe("terminal_missing_receipt");
	expect(f.job().state).toBe("awaiting_operator");
	expect(f.notices.map((notice) => notice.text)).toEqual([
		`[lane a] attempt_ended: terminal_missing_receipt: final_response_missing opRef=${result.opRef}`,
	]);
	expect(f.port.sends).toHaveLength(1);
});

test("broker deadline ends the attempt but caller timeout never does", async () => {
	const f = await fixture();
	const result = await started(f, "a", origin);
	f.port.status = async (input) => ({
		operationRef: input.opRef,
		status: { status: "failed", error: { code: "prompt_deadline_exceeded", message: "secret" } },
		summaryCompleted: true,
	});
	f.port.fetchWorkerOutput = async () => ({ status: "unavailable", code: "output_unavailable" });
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.job().attempts[0]?.endState).toBe("attempt_ended");
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.status?.error).toEqual({ code: "prompt_deadline_exceeded" });
	expect(f.notices[0]?.text).toBe("[lane a] attempt_ended: prompt_deadline_exceeded: output_unavailable");
});

test("torn send accepts only same-op status evidence; unknown remains open with no resend", async () => {
	const f = await fixture();
	const send = f.port.send.bind(f.port);
	f.port.send = async (input) => {
		await send(input);
		throw new Error("lost receipt secret");
	};
	const result = await started(f);
	expect(f.db.work.workAttemptGet(result.opRef)?.sendEvidence?.source).toBe("status");
	f.port.send = async () => {
		throw new Error("lost receipt secret");
	};
	await expect(f.manager.start({ name: "b", text: "work", cwd: f.directory })).rejects.toMatchObject({
		message: "work send acceptance uncertain",
		detail: { reasonCode: "send_acceptance_uncertain" },
	});
	const pending = f.job("b").attempts[0]!;
	expect(f.db.work.workAttemptGet(pending.opRef)?.sendPhase).toBe("uncertain");
	await f.manager.recover();
	expect(f.job("b").attempts[0]?.endedAt).toBeUndefined();
	expect(f.port.sends).toHaveLength(1);
});

test("definitive send rejection atomically holds and enqueues one safe start-only notice", async () => {
	const f = await fixture();
	f.port.send = async () => {
		throw new GjcCliError("secret", 0, "", { code: "busy" });
	};
	f.setOwnerTarget(origin);
	await expect(f.manager.start({ name: "a", text: "work", cwd: f.directory })).rejects.toMatchObject({
		detail: { reasonCode: "send_rejected" },
	});
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.job().attempts[0]?.endState).toBe("failed");
	expect(f.job().state).toBe("awaiting_operator");
	expect(f.notices[0]?.text).toBe("[lane a] failed: send_rejected: output_unavailable");
	expect(f.db.outbox.rows()).toHaveLength(1);
});

test("CAS loss does not fan out or perform a separate history/activity update", async () => {
	const f = await fixture();
	const settle = f.db.work.workAttemptSettle.bind(f.db.work);
	let calls = 0;
	f.db.work.workAttemptSettle = () => {
		calls++;
		return undefined;
	};
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "answer");
	const history = f.db.work.laneJobJson(result.jobId);
	await until(() => calls > 0);
	// Streamed frames may refresh activity before settlement; a lost settlement
	// CAS must not write it (or history) again on any later retry.
	const activity = f.db.bindings.workLaneRows()[0]!.last_activity_at;
	const seen = calls;
	await until(() => calls > seen);
	expect(f.db.work.laneJobJson(result.jobId)).toBe(history);
	expect(f.db.bindings.workLaneRows()[0]!.last_activity_at).toBe(activity);
	expect(f.notices).toHaveLength(0);
	expect(f.db.outbox.rows()).toHaveLength(0);
	f.db.work.workAttemptSettle = settle;
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.notices).toHaveLength(1);
});

test("persisted original silence survives a crash between output staging and settlement", async () => {
	const f = await fixture();
	const settle = f.db.work.workAttemptSettle.bind(f.db.work);
	f.db.work.workAttemptSettle = () => {
		throw new Error("injected pre-transaction crash");
	};
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "[SILENT]");
	await until(() => f.db.work.workAttemptGet(result.opRef)?.output.knownSilence != null);
	await f.manager.stop();
	f.db.work.workAttemptSettle = settle;
	f.port.fetchWorkerOutput = async () => {
		throw new Error("original output no longer accessible");
	};
	await f.restart();
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.db.work.workAttemptGet(result.opRef)?.decision).toBe("suppressed");
	expect(f.db.work.workAttemptGet(result.opRef)?.output.reads).toBe(1);
	expect(f.notices).toHaveLength(0);
	expect(f.db.outbox.rows()).toHaveLength(0);
});

test("reopening SQLite recovers the same live operation and immutable notification intent", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-work-reopen-"));
	const path = join(directory, "gateway.db");
	let db = await Store.open(path);
	const gajaewayHome = join(directory, "gajaeway");
	// Create gajaewayHome directory for service directory validation
	await mkdir(gajaewayHome, { recursive: true });
	const port = new ScriptedSessionPort({
		onBind: (input) => {
			const id = crypto.randomUUID();
			db.transaction((tx) => db.bindings.bind(tx, input.originKey, id));
			return id;
		},
	});
	const notices: ChatMessagePayload[] = [];
	let manager = new WorkLaneManager({
		database: db,
		port,
		lanes: new LaneGovernor({ database: db, sessionPort: port, laneRoot: gajaewayHome }),
		gajaewayHome,
		serviceDirectory: gajaewayHome,
		ownerTarget: () => origin,
		pollMs: 5,
		deliverFallback: (payload) => notices.push(payload),
	});
	cleanups.push(async () => {
		await manager.stop();
		db.close();
		await rm(directory, { recursive: true, force: true });
	});
	const result = await manager.start({ name: "a", text: "work", cwd: directory });
	if (!result.started) throw new Error("unexpected hold");
	await manager.stop();
	db.close();
	db = await Store.open(path);
	manager = new WorkLaneManager({
		database: db,
		port,
		lanes: new LaneGovernor({ database: db, sessionPort: port, laneRoot: gajaewayHome }),
		gajaewayHome,
		serviceDirectory: gajaewayHome,
		pollMs: 5,
		ownerTarget: () => ({ ...origin, conversationId: "changed" }),
		deliverFallback: (payload) => notices.push(payload),
	});
	await manager.recover();
	expect(db.work.workAttemptOpen()).toHaveLength(1);
	expect(port.sends).toHaveLength(1);
	expect(port.resumes).toHaveLength(0);
	port.complete(result.opRef, "after database reopen");
	await until(() => db.work.workAttemptOpen().length === 0);
	expect(notices).toHaveLength(1);
	expect(notices[0]?.origin).toEqual(origin);
	expect(notices[0]?.turnId).toBe(result.opRef);
});
for (const status of ["terminal_ok", "failed"] as const) {
	for (const receiptState of ["missing", "unknown", "absent", undefined, "present"] as const) {
		for (const observedAtSend of [true, false]) {
			test(`torn send ${status}/${receiptState} accounting at ${observedAtSend ? "send" : "poll"} never invents acceptance`, async () => {
				const f = await fixture();
				let queries = 0;
				let sends = 0;
				f.port.send = async () => {
					sends++;
					throw new Error("lost send receipt");
				};
				f.port.status = async (input) => {
					queries++;
					return {
						operationRef: input.opRef,
						summaryCompleted: true,
						status:
							!observedAtSend && queries === 1
								? { status: "unknown" }
								: {
										status,
										receiptState,
										outcome: { reason: "end_turn" },
										error: status === "failed" ? { code: "sdk_failed" } : undefined,
									},
					};
				};
				f.port.fetchWorkerOutput = async () => ({ status: "unavailable", code: "output_unavailable" });
				const response = await f.manager
					.start({ name: "a", text: "work", cwd: f.directory })
					.catch((error: unknown) => error);
				if (observedAtSend && receiptState === "present") expect(response).toMatchObject({ started: true });
				else
					expect(response).toMatchObject({ code: "verb_failed", detail: { reasonCode: "send_acceptance_uncertain" } });
				const opRef = f.job().attempts[0]!.opRef;
				if (receiptState === "missing")
					f.port.emitTurnEnd(f.job().attempts[0]!.sessionId, opRef, status === "failed" ? "agent_failed" : "agent_end");
				await until(() => f.db.work.workAttemptGet(opRef)?.settledAt != null);
				const runtime = f.db.work.workAttemptGet(opRef)!;
				expect(runtime.sendPhase).toBe(receiptState === "present" ? "accepted" : "uncertain");
				if (receiptState === "present") expect(runtime.sendEvidence?.source).toBe("status");
				else expect(runtime.sendEvidence).toBeNull();
				expect(runtime.terminal?.status).toMatchObject({ status });
				expect(f.job().attempts).toHaveLength(1);
				await f.manager.recover();
				expect(sends).toBe(1);
				expect(f.port.sends).toHaveLength(0);
				expect(f.port.binds).toHaveLength(1);
				expect(f.port.resumes).toHaveLength(0);
			});
		}
	}
}

test("completion excerpt retains the leading marker and scalar-safe prefix, not the tail", async () => {
	const f = await fixture();
	const result = await started(f, "a", origin);
	const source = "HEAD:" + "😀".repeat(1000) + ":TAIL";
	f.port.complete(result.opRef, source);
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt != null);
	const head = "[lane a] completed: ";
	const excerpt = utf8Prefix(source);
	expect(f.db.work.workAttemptGet(result.opRef)?.output.excerpt).toBe(excerpt);
	expect(f.notices[0]?.text).toBe(head + utf8Prefix(source, 2048 - Buffer.byteLength(head, "utf8")));
	expect(f.notices[0]?.text.includes(":TAIL")).toBe(false);
});

test("report and fallback text stay within 2048 UTF-8 bytes including the lane head", async () => {
	const name = "n".repeat(64);
	for (const scalar of ["界", "😀"]) {
		const output = {
			disposition: "available" as const,
			reads: 0,
			nextReadAt: null,
			excerpt: scalar.repeat(1000),
			proof: null,
			knownSilence: null,
		};
		const head = `[lane ${name}] attempt_ended: recovery_indeterminate: `;
		const text = reportText(name, "attempt_ended", "recovery_indeterminate", "op-ref", output);
		expect(text).toBe(head + utf8Prefix(output.excerpt, 2048 - Buffer.byteLength(head, "utf8")));
		expect(Buffer.byteLength(text, "utf8")).toBeLessThanOrEqual(2048);
	}
	const f = await fixture();
	const result = await started(f, name, origin);
	f.port.complete(result.opRef, "界".repeat(1000));
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt != null);
	const runtime = f.db.work.workAttemptGet(result.opRef)!;
	expect(f.notices[0]?.text).toBe(reportText(name, "completed", "end_turn", result.opRef, runtime.output));
	expect(Buffer.byteLength(f.notices[0]!.text, "utf8")).toBeLessThanOrEqual(2048);
});

test("repository and PR head facts survive a large completion excerpt", () => {
	const headSha = "a".repeat(40);
	const prHeadSha = "b".repeat(40);
	const output = {
		disposition: "available" as const,
		reads: 0,
		nextReadAt: null,
		excerpt: "x".repeat(8_000),
		proof: null,
		knownSilence: null,
	};
	const facts = {
		headSha,
		dirtyFiles: 0,
		pushed: true,
		remoteHeadSha: headSha,
		checkStatus: { prNumber: 545, headSha: prHeadSha, status: "green" as const, failingChecks: [] },
	};
	const text = reportText("a", "completed", "end_turn", "op-ref", output, facts);
	expect(text.startsWith("[lane a] attempt_ended:")).toBe(true);
	expect(text).toContain("head=aaaaaaaaaaaa");
	expect(text).toContain("pushed=true");
	expect(text).toContain("remoteHead=aaaaaaaaaaaa");
	expect(text).toContain("pr=545 checks=green prHead=bbbbbbbbbbbb");
	expect(Buffer.byteLength(text, "utf8")).toBeLessThanOrEqual(2048);
});

test("failed CI logs are redacted incrementally before retaining a bounded tail", async () => {
	const secret = "z".repeat(4_000);
	const arraySecret = "array-value-that-must-not-escape";
	const splitArraySecret = "control-split-array-value";
	const log = `${"x".repeat(4_000)} secret=${secret}\n{"secrets": [\n  "${arraySecret}",\n  "another-value"\n]}\n{"secr\0ets": [\n  "${splitArraySecret}"\n]}\n`;
	const bytes = Buffer.from(log, "utf8");
	const stream = new ReadableStream<Uint8Array>({
		start(controller) {
			for (let offset = 0; offset < bytes.length; offset += 113)
				controller.enqueue(bytes.subarray(offset, Math.min(offset + 113, bytes.length)));
			controller.close();
		},
	});
	const result = await sanitizedFailureLogStreamTail(stream);
	expect(result.overflowed).toBe(false);
	expect(result.tail).toContain("secret=[redacted]");
	expect(result.tail).toContain('"secrets": [redacted]');
	expect(result.tail).not.toContain(secret);
	expect(result.tail).not.toContain(arraySecret);
	expect(result.tail).not.toContain(splitArraySecret);
	expect(Buffer.byteLength(result.tail ?? "", "utf8")).toBeLessThanOrEqual(3_200);
	const oversized = new ReadableStream<Uint8Array>({
		start(controller) {
			controller.enqueue(Buffer.from(`${"x".repeat(70 * 1024)} secret=${secret}`));
			controller.close();
		},
	});
	const bounded = await sanitizedFailureLogStreamTail(oversized);
	expect(bounded.overflowed).toBe(true);
	expect(bounded.tail).toBeUndefined();
});

test("AC-K notice names persona parent", () => {
	expect(
		laneSystemNotice({
			name: "worker",
			parent: { kind: "persona", originKey: originKey(origin), origin },
			allowNested: false,
		}),
	).toContain(`Parent: persona conversation ${originKey(origin)}.`);
});

test("AC-K notice names lane parent and nested allowed", () => {
	const notice = laneSystemNotice({
		name: "child",
		parent: { kind: "lane", name: "parent", root: null },
		allowNested: true,
	});
	expect(notice).toContain("Parent: work lane parent.");
	expect(notice).toContain("Nested work.start and work.run are allowed");
});

test("AC-K notice says none for no parent and names both refused verbs", () => {
	const notice = laneSystemNotice({ name: "worker", parent: null, allowNested: false });
	expect(notice).toContain("Parent: none (status-only).");
	expect(notice).toContain("Nested work.start and work.run are refused");
});

test("AC-K first accepted send per epoch carries notice; same-epoch same-parent attempt does not", async () => {
	const f = await fixture();
	f.setOwnerTarget(origin);
	const first = await started(f);
	const firstNotice = f.port.sends[0]?.systemPreamble;
	expect(firstNotice).toContain(`persona conversation ${originKey(origin)}`);
	expect(JSON.parse(f.db.core.metaGet(`lane-notice:${workSessionKey("a")}`)!).hash).toBe(
		f.db.work.workAttemptGet(first.opRef)?.noticeHash,
	);
	f.port.complete(first.opRef, "first done");
	await until(() => f.db.work.workAttemptGet(first.opRef)?.settledAt !== null);
	const second = await started(f);
	expect(f.port.sends[1]?.systemPreamble).toBeUndefined();
	f.port.complete(second.opRef, "second done");
	await until(() => f.db.work.workAttemptGet(second.opRef)?.settledAt !== null);
});

test("AC-K parent change in the same epoch re-prepends the notice", async () => {
	const f = await fixture();
	f.setOwnerTarget(origin);
	const first = await started(f);
	f.port.complete(first.opRef, "first done");
	await until(() => f.db.work.workAttemptGet(first.opRef)?.settledAt !== null);
	const changedOrigin = { ...origin, conversationId: "different-parent" };
	f.setOwnerTarget(changedOrigin);
	const second = await started(f);
	expect(f.port.sends[1]?.systemPreamble).toContain(`persona conversation ${originKey(changedOrigin)}`);
});

test("AC-K nested-refused notice is prepended to start sends and run sends carry no notice", async () => {
	const f = await fixture();
	const start = await started(f);
	expect(f.port.sends[0]?.systemPreamble).toContain("Nested work.start and work.run are refused");
	expect(f.port.sends[0]?.systemPreamble).toContain("Parent: none (status-only).");
	f.port.complete(start.opRef, "finished");
	await until(() => f.db.work.workAttemptGet(start.opRef)?.settledAt !== null);
	const run = f.manager.run({ name: "a", text: "run text", cwd: f.directory }, {});
	await until(() => f.port.sends.length === 2);
	expect(f.port.sends[1]?.systemPreamble).toBeUndefined();
	f.port.complete(f.port.sends[1]!.opRef, "run result");
	expect(await run).toMatchObject({ held: false, text: "run result" });
});

test("AC-K uncertain send acceptance re-prepends on the next send", async () => {
	const f = await fixture();
	f.setOwnerTarget(origin);
	const send = f.port.send.bind(f.port);
	f.port.send = async () => {
		throw new Error("lost send receipt");
	};
	await expect(f.manager.start({ name: "a", text: "first", cwd: f.directory })).rejects.toMatchObject({
		detail: { reasonCode: "send_acceptance_uncertain" },
	});
	const first = f.job().attempts[0]!;
	f.port.setSessionState(first.sessionId, { live: false });
	await f.restart();
	await until(() => f.db.work.workAttemptGet(first.opRef)?.settledAt !== null);
	f.port.send = send;
	const retry = await f.manager.start({ name: "a", text: "retry", cwd: f.directory, resume: true });
	if (!retry.started) throw new Error("retry was held");
	expect(f.port.sendAttempts.at(-1)?.systemPreamble).toContain(`persona conversation ${originKey(origin)}`);
});

test("personaHold broker_wedged selects fallback inside the settlement transaction", async () => {
	const f = await fixture({ ownerTarget: () => origin, personaHold: () => "broker_wedged" });
	const result = await started(f);
	f.port.complete(result.opRef, "held persona result");
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt !== null);
	const runtime = f.db.work.workAttemptGet(result.opRef)!;
	expect(runtime.decision).toBe("fallback");
	expect(f.db.outbox.rows()).toHaveLength(1);
	expect(f.db.outbox.rows()[0]?.delivery_id).toBe(runtime.deliveryId);
	expect(f.db.inbox.pendingOldest(originKey(origin))).toBeUndefined();
	expect(f.notices).toHaveLength(1);
});

test("personaHold emitted hold reason selects fallback", async () => {
	const f = await fixture({ ownerTarget: () => origin, personaHold: () => "sdk_unavailable" });
	const result = await started(f);
	f.port.complete(result.opRef, "held persona result");
	await until(() => f.db.work.workAttemptGet(result.opRef)?.settledAt !== null);
	expect(f.db.work.workAttemptGet(result.opRef)?.decision).toBe("fallback");
	expect(f.db.outbox.rows()).toHaveLength(1);
	expect(f.notices[0]?.text).toBe("[lane a] completed: held persona result");
});
test("lane inbox steers an open nested parent and consumes only after acceptance", async () => {
	const f = await fixture({ allowNested: () => true });
	f.setOwnerTarget(origin);
	const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	const child = await f.manager.start({
		name: "child",
		text: "child work",
		cwd: f.directory,
		callerSessionId: parent.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	expect(f.port.sends[1]?.systemPreamble).toContain("Parent: work lane parent.");
	expect(f.port.sends[1]?.systemPreamble).toContain("Nested work.start and work.run are allowed");
	f.port.complete(child.opRef, "child result");
	await until(() => f.db.work.laneReportsByParent("parent")[0]?.state === "consumed");
	const report = f.db.work.laneReportsByParent("parent")[0]!;
	expect(report).toMatchObject({ state: "consumed", claim_kind: "steer", consumed_op_ref: report.claim_ref });
	expect(report.body).toBe("[lane child] completed: child result");
	expect(f.port.steers).toHaveLength(1);
	expect(f.port.steers[0]?.clientRef).toBe(report.claim_ref!);
	expect(f.port.steers[0]?.text).toContain("Report from child work lane child; not from a human.");
	expect(f.port.sendAttempts).toHaveLength(2);
	expect(f.notices).toHaveLength(0);
});

test("child reports fall back when a nested parent is parked or done", async () => {
	for (const parentState of ["awaiting_checks", "done"] as const) {
		const checks = scriptedChecks({ prNumber: 510, status: "pending", failingChecks: [] });
		const f = await fixture({
			allowNested: () => true,
			checkStatusClient: checks.client,
			ownerTarget: () => origin,
		});
		f.setOwnerTarget(origin);
		await laneRepo(f.directory, `fix/parent-${parentState}`, true);
		const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
		if (!parent.started) throw new Error("parent did not start");
		const child = await f.manager.start({
			name: "child",
			text: "child work",
			cwd: f.directory,
			callerSessionId: parent.sessionId,
		});
		if (!child.started) throw new Error("child did not start");
		f.port.complete(parent.opRef, "parent result");
		await until(() => f.job("parent").state === "awaiting_checks");
		if (parentState === "done") {
			checks.set({ prNumber: 510, status: "green", failingChecks: [] });
			await pollUntil(f, () => f.job("parent").state === "done");
		}
		f.port.complete(child.opRef, "child result after parent parked");
		await until(() => f.notices.some((notice) => notice.text.includes("child result after parent parked")));
		expect(f.db.work.laneReportsByParent("parent")).toEqual([]);
		expect(f.notices.at(-1)?.text).toContain(
			parentState === "done" ? "[lane child] completed" : "[lane child] attempt_ended",
		);
	}
});

test("#481 check-settlement reports fall back when a nested parent was aborted", async () => {
	const checks = scriptedChecks({ prNumber: 510, status: "pending", failingChecks: [] });
	const f = await fixture({
		allowNested: () => true,
		checkStatusClient: checks.client,
		ownerTarget: () => origin,
	});
	f.setOwnerTarget(origin);
	await laneRepo(f.directory, "fix/parent-aborted", true);
	const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	const child = await f.manager.start({
		name: "child",
		text: "child work",
		cwd: f.directory,
		callerSessionId: parent.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	f.port.complete(parent.opRef, "parent result");
	await until(() => f.job("parent").state === "awaiting_checks");
	f.port.complete(child.opRef, "child result");
	await until(() => f.job("child").state === "awaiting_checks");
	const { awaitingChecks: _, ...parentWithoutChecks } = f.job("parent");
	const abortedParent = {
		...parentWithoutChecks,
		state: "aborted" as const,
		updatedAt: new Date().toISOString(),
	};
	f.db.transaction((tx) =>
		f.db.work.putLaneJob(tx, {
			...abortedParent,
			laneKey: laneJobIdentity("parent").laneKey,
			json: JSON.stringify(abortedParent),
		}),
	);
	checks.set({ prNumber: 510, status: "green", failingChecks: [] });
	await pollUntil(f, () => f.job("child").state === "done");
	expect(f.job("parent").state).toBe("aborted");
	expect(f.db.work.laneReportsByParent("parent")).toEqual([]);
	expect(f.notices.some((notice) => notice.text.startsWith("[lane child] checks passed:"))).toBe(true);
});

test("torn nested steer replays with the same clientRef after restart and consumes once", async () => {
	const f = await fixture({ allowNested: () => true });
	f.setOwnerTarget(origin);
	const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	const steer = f.port.steer.bind(f.port);
	let torn = true;
	const refs: string[] = [];
	f.port.steer = async (input) => {
		refs.push(input.clientRef);
		if (torn) throw new Error("steer transport torn");
		return steer(input);
	};
	const child = await f.manager.start({
		name: "child",
		text: "child work",
		cwd: f.directory,
		callerSessionId: parent.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	f.port.complete(child.opRef, "child result");
	await until(() => refs.length === 1);
	const claimed = f.db.work.laneReportsByParent("parent")[0]!;
	expect(claimed).toMatchObject({ state: "claimed", claim_kind: "steer" });
	torn = false;
	await f.restart();
	await until(() => f.db.work.laneReportGet(claimed.report_id)?.state === "consumed");
	const claimRef = claimed.claim_ref;
	if (!claimRef) throw new Error("claimed lane report has no claim_ref");
	expect(new Set(refs)).toEqual(new Set([claimRef]));
	expect(f.db.work.laneReportGet(claimed.report_id)?.consumed_op_ref).toBe(claimRef);
	expect(f.port.sendAttempts.filter((attempt) => attempt.opRef.includes("-lr-"))).toHaveLength(0);
});

test("AC-K wake send in a new epoch carries its lane notice", async () => {
	const f = await fixture({ allowNested: () => true });
	f.setOwnerTarget(origin);
	const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	f.port.complete(parent.opRef, "parent first result");
	await until(() => f.db.work.workAttemptGet(parent.opRef)?.settledAt !== null);
	let rebound = false;
	const claim = f.db.work.laneReportClaim.bind(f.db.work);
	f.db.work.laneReportClaim = (tx, reportId, kind, ref, targetOpRef) => {
		const row = claim(tx, reportId, kind, ref, targetOpRef);
		if (row?.claim_kind === "wake" && !rebound) {
			rebound = true;
			f.db.bindings.rotateEpoch(tx, workSessionKey("parent"));
		}
		return row;
	};
	const child = await f.manager.start({
		name: "child",
		text: "child work",
		cwd: f.directory,
		callerSessionId: parent.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	f.port.complete(child.opRef, "child report");
	await until(() => f.db.work.laneReportsByParent("parent")[0]?.state === "consumed");
	expect(rebound).toBe(true);
	expect(f.port.sendAttempts[2]?.systemPreamble).toContain("You are work lane parent.");
	expect(f.port.sendAttempts[2]?.systemPreamble).toContain(
		"Parent: persona conversation discord/channel/work-results.",
	);
});

test("idle lane parent wake completes without lock re-entry", async () => {
	const f = await fixture({ allowNested: () => true });
	f.setOwnerTarget(origin);
	f.db.transaction((tx) => f.db.bindings.bind(tx, originKey(origin), crypto.randomUUID()));
	const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	f.port.complete(parent.opRef, "parent first result");
	await until(() => f.db.work.workAttemptGet(parent.opRef)?.settledAt !== null);
	const child = await f.manager.start({
		name: "child",
		text: "child work",
		cwd: f.directory,
		callerSessionId: parent.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	f.port.complete(child.opRef, "child result");
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		await Promise.race([
			until(() => f.db.work.laneReportsByParent("parent")[0]?.state === "consumed"),
			new Promise<never>((_, reject) => {
				timer = setTimeout(() => reject(new Error("lane parent wake timed out")), 2000);
			}),
		]);
	} finally {
		if (timer) clearTimeout(timer);
	}
	const report = f.db.work.laneReportsByParent("parent")[0]!;
	const wake = f.db.work.workAttemptGet(report.consumed_op_ref!)!;
	expect(wake).toMatchObject({ wakeReportId: report.report_id, parent: { kind: "persona" } });
	expect(report).toMatchObject({ state: "consumed", claim_kind: "wake", consumed_op_ref: report.claim_ref });
	expect(f.port.sendAttempts).toHaveLength(3);
	expect(f.port.sendAttempts[2]?.opRef).toBe(report.claim_ref!);
	expect(f.port.sendAttempts[2]?.text).toContain("Report from child work lane child; not from a human.");
});

test("rejected nested wake settles wake_unaccepted and falls back the child exactly once", async () => {
	const f = await fixture({ allowNested: () => true });
	f.setOwnerTarget(origin);
	f.db.transaction((tx) => f.db.bindings.bind(tx, originKey(origin), crypto.randomUUID()));
	const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	f.port.complete(parent.opRef, "parent first result");
	await until(() => f.db.work.workAttemptGet(parent.opRef)?.settledAt !== null);
	const rootInboundBefore = f.db.inbox.pendingCount(originKey(origin));
	let wakeSendAttempts = 0;
	f.notices.length = 0;
	const send = f.port.send.bind(f.port);
	f.port.send = async (input) => {
		if (input.opRef.includes("-lr-")) {
			wakeSendAttempts++;
			throw new GjcCliError("refused", 0, "", { code: "busy" });
		}
		return send(input);
	};
	const child = await f.manager.start({
		name: "child",
		text: "child work",
		cwd: f.directory,
		callerSessionId: parent.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	f.port.complete(child.opRef, "child result");
	await until(() => f.db.work.laneReportsByParent("parent")[0]?.state === "fallback");
	const report = f.db.work.laneReportsByParent("parent")[0]!;
	const wake = f.db.work.workAttemptGet(report.claim_ref!)!;
	expect(wake.decision).toBe("wake_unaccepted");
	expect(report.state).toBe("fallback");
	expect(f.db.work.workAttemptGet(wake.opRef)?.reportId).toBe(wake.reportId);
	expect(f.db.outbox.rows()).toHaveLength(1);
	expect(f.db.outbox.rows()[0]?.turn_id).toBe(child.opRef);
	expect(f.db.outbox.rows()[0]?.delivery_id).not.toBe(wake.deliveryId);
	expect(f.notices).toHaveLength(1);
	expect(f.notices[0]?.deliveryId).toBe(f.db.outbox.rows()[0]?.delivery_id);
	expect(f.db.inbox.pendingCount(originKey(origin))).toBe(rootInboundBefore);
	await f.manager.recover();
	expect(f.db.work.laneReportsByParent("parent")[0]?.state).toBe("fallback");
	expect(f.db.outbox.rows()).toHaveLength(1);
	expect(wakeSendAttempts).toBe(1);
});

test("uncertain nested wake keeps its claim and consumes after status acceptance without resend", async () => {
	const f = await fixture({ allowNested: () => true });
	f.setOwnerTarget(origin);
	f.db.transaction((tx) => f.db.bindings.bind(tx, originKey(origin), crypto.randomUUID()));
	const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	f.port.complete(parent.opRef, "parent first result");
	await until(() => f.db.work.workAttemptGet(parent.opRef)?.settledAt !== null);
	let wakeSendAttempts = 0;
	const send = f.port.send.bind(f.port);
	f.port.send = async (input) => {
		if (input.opRef.includes("-lr-")) {
			wakeSendAttempts++;
			throw new Error("wake send receipt lost");
		}
		return send(input);
	};
	const child = await f.manager.start({
		name: "child",
		text: "child work",
		cwd: f.directory,
		callerSessionId: parent.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	f.port.complete(child.opRef, "child result");
	await until(() => f.db.work.laneReportsByParent("parent")[0]?.state === "claimed");
	const claimed = f.db.work.laneReportsByParent("parent")[0]!;
	const wakeOpRef = claimed.claim_ref!;
	f.port.status = async (input) => ({
		operationRef: input.opRef,
		status: { status: "accepted", clientRef: input.opRef },
		summaryCompleted: false,
	});
	await f.restart();
	await until(() => f.db.work.laneReportGet(claimed.report_id)?.state === "consumed");
	expect(f.db.work.workAttemptGet(wakeOpRef)?.sendPhase).toBe("accepted");
	expect(f.port.sendAttempts.filter((attempt) => attempt.opRef.includes("-lr-")).length).toBe(0);
	expect(wakeSendAttempts).toBe(1);
});
test("recovered terminal status with receipt present proves wake acceptance without output proof", async () => {
	const f = await fixture({ allowNested: () => true });
	f.setOwnerTarget(origin);
	f.db.transaction((tx) => f.db.bindings.bind(tx, originKey(origin), crypto.randomUUID()));
	const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	f.port.complete(parent.opRef, "parent first result");
	await until(() => f.db.work.workAttemptGet(parent.opRef)?.settledAt !== null);
	const send = f.port.send.bind(f.port);
	f.port.send = async (input) => {
		if (input.opRef.includes("-lr-")) throw new Error("wake send receipt lost");
		return send(input);
	};
	const child = await f.manager.start({
		name: "child",
		text: "child work",
		cwd: f.directory,
		callerSessionId: parent.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	f.port.complete(child.opRef, "child result");
	await until(() => f.db.work.laneReportsByParent("parent")[0]?.state === "claimed");
	const claimed = f.db.work.laneReportsByParent("parent")[0]!;
	const wakeOpRef = claimed.claim_ref!;
	f.port.fetchWorkerOutput = async () => ({ status: "unavailable", code: "identity_mismatch" });
	f.port.status = async (input) => ({
		operationRef: input.opRef,
		status: {
			status: "terminal_ok",
			clientRef: input.opRef,
			receiptState: "present",
			outcome: { reason: "end_turn" },
		},
		summaryCompleted: true,
	});
	await f.restart();
	await until(() => f.db.work.workAttemptGet(wakeOpRef)?.settledAt != null);
	const wake = f.db.work.workAttemptGet(wakeOpRef)!;
	expect(wake.sendPhase).toBe("accepted");
	expect(wake.decision).not.toBe("wake_unaccepted");
	expect(f.db.work.laneReportGet(claimed.report_id)?.state).toBe("consumed");
});
test("open-parent steer refusal requeues and the idle parent receives one wake", async () => {
	const f = await fixture({ allowNested: () => true });
	f.setOwnerTarget(origin);
	f.db.transaction((tx) => f.db.bindings.bind(tx, originKey(origin), crypto.randomUUID()));
	const parent = await f.manager.start({ name: "parent", text: "parent work", cwd: f.directory });
	if (!parent.started) throw new Error("parent did not start");
	let steerAttempts = 0;
	const steer = f.port.steer.bind(f.port);
	f.port.steer = async (input) => {
		steerAttempts++;
		if (steerAttempts === 1) throw new GjcCliError("busy", 0, "", { code: "busy", refused: true });
		return steer(input);
	};
	const child = await f.manager.start({
		name: "child",
		text: "child work",
		cwd: f.directory,
		callerSessionId: parent.sessionId,
	});
	if (!child.started) throw new Error("child did not start");
	f.port.complete(child.opRef, "child result");
	await until(() => f.db.work.laneReportsByParent("parent")[0]?.state === "pending");
	expect(steerAttempts).toBe(1);
	f.port.complete(parent.opRef, "parent completed");
	await until(() => f.db.work.laneReportsByParent("parent")[0]?.state === "consumed");
	expect(f.db.work.laneReportsByParent("parent")[0]).toMatchObject({ claim_kind: "wake" });
	expect(steerAttempts).toBe(1);
	expect(f.port.sendAttempts).toHaveLength(3);
});
test("a queued steer captured for A refuses after A settles and B starts", async () => {
	const f = await fixture();
	const a = await started(f);
	const entered = deferred<void>();
	const release = deferred<void>();
	const exclusive = f.port.runExclusive.bind(f.port);
	const recover = f.manager.recover.bind(f.manager);
	// Recovery is already registered; isolate the steer mutation lock boundary.
	f.manager.recover = async () => {};
	let intercept = true;
	f.port.runExclusive = async <T>(key: string, work: () => Promise<T>): Promise<T> => {
		if (intercept && key === workSessionKey("a")) {
			intercept = false;
			entered.resolve();
			await release.promise;
		}
		return exclusive(key, work);
	};
	const steer = f.manager.steer({ name: "a", text: "for A only" }).catch((error: unknown) => error);
	try {
		await entered.promise;
		f.port.complete(a.opRef, "A complete");
		await until(() => f.db.work.workAttemptGet(a.opRef)?.settledAt != null);
		const b = await started(f);
		expect(b.opRef).not.toBe(a.opRef);
		const binds = f.port.binds.length;
		const sends = f.port.sends.length;
		release.resolve();
		expect(await steer).toMatchObject({ code: "invalid_params", detail: { reasonCode: "no_open_attempt" } });
		expect(f.port.steers).toHaveLength(0);
		expect(f.port.binds).toHaveLength(binds);
		expect(f.port.sends).toHaveLength(sends);
		expect(sends).toBe(2);
		expect(f.db.work.workAttemptGet(b.opRef)?.settledAt).toBeNull();
	} finally {
		release.resolve();
		await steer;
		f.port.runExclusive = exclusive;
		f.manager.recover = recover;
	}
});

test("quarantined accepted work reserves its name without querying the shared broker", async () => {
	const f = await fixture();
	const old = await started(f, "old", origin);
	await f.manager.stop();
	const history = f.db.work.laneJobJson(old.jobId);
	const runtime = f.db.work.workAttemptGet(old.opRef);
	const authority = { canonicalAgentDir: "/tmp/global-agent", identity: "shared-broker" };
	f.db.transaction((tx) =>
		f.db.bindings.cutoverBrokerAuthority(tx, {
			expectedAuthority: null,
			targetAuthority: authority,
			evidence: "test operator quarantined private broker work",
			disposition: "quarantine",
		}),
	);
	// The shared broker may answer the same old ID; it must never be asked.
	f.port.setSessionState(old.sessionId, { repo: f.directory, live: true });
	const calls: string[] = [];
	for (const method of [
		"status",
		"liveness",
		"inspect",
		"resume",
		"steer",
		"close",
		"attachTail",
		"fetchWorkerOutput",
	] as const) {
		const original = f.port[method];
		Object.assign(f.port, {
			[method]: (...args: unknown[]) => {
				calls.push(method);
				return Reflect.apply(original, f.port, args);
			},
		});
	}
	const binds = f.port.binds.length;
	const sends = f.port.sends.length;
	await f.restart();
	const failure = {
		code: "verb_failed",
		message: "work lane belongs to a quarantined broker authority",
		detail: { reasonCode: "broker_authority_quarantined", jobId: old.jobId, name: "old" },
	};
	await expect(f.manager.status({ name: "old" })).rejects.toMatchObject(failure);
	for (const resume of [false, true]) {
		await expect(f.manager.start({ name: "old", text: "again", cwd: f.directory, resume })).rejects.toMatchObject(
			failure,
		);
		await expect(f.manager.run({ name: "old", text: "again", cwd: f.directory, resume }, {})).rejects.toMatchObject(
			failure,
		);
	}
	await expect(f.manager.steer({ name: "old", text: "change" })).rejects.toMatchObject(failure);
	expect(await f.lanes.retire("old", "operator")).toEqual({
		retired: false,
		sessionKey: workSessionKey("old"),
		reason: "broker_authority_quarantined",
	});
	expect(calls).toEqual([]);
	expect(f.port.binds).toHaveLength(binds);
	expect(f.port.sends).toHaveLength(sends);
	expect(f.db.work.laneJobJson(old.jobId)).toBe(history);
	expect(f.db.work.workAttemptGet(old.opRef)).toEqual(runtime);
	expect(f.db.work.laneJobRows()).toEqual([]);
	expect(f.db.work.laneJobRows(true).map((row) => row.job_id)).toEqual([old.jobId]);
	// Explicit test-only provenance models a successful shared SDK bind.
	f.port.bind = async (input) => {
		const binding = { ...input, sessionId: crypto.randomUUID() };
		f.db.transaction((tx) => f.db.bindings.recordOwnedBinding(tx, { ...binding, authority }));
		return binding;
	};
	const fresh = await started(f, "fresh");
	expect(fresh.jobId).not.toBe(old.jobId);
	expect(f.port.sends).toHaveLength(sends + 1);
	expect(f.notices).toHaveLength(0);
});

function sessionUnavailable(): GjcCliError {
	return new GjcCliError("gjc sdk request failed: session_unavailable", 1, "", { code: "session_unavailable" });
}

test("#308 host loss settles the open attempt as host_lost; resume and retire then work", async () => {
	const f = await fixture({ hostLostGraceMs: 20 });
	const result = await started(f, "a", origin);
	const status = f.port.status.bind(f.port);
	// The host process died: the router disowns the session for every read.
	f.port.status = async () => {
		throw sessionUnavailable();
	};
	f.port.setSessionState(result.sessionId, { live: false });
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("host_lost");
	expect(f.job().attempts[0]?.endState).toBe("attempt_ended");
	expect(f.job().attempts[0]?.errorCode).toBe("host_lost");
	expect(f.job().state).toBe("attempt_ended");
	expect(f.notices[0]?.text).toBe("[lane a] attempt_ended: host_lost: output_unavailable");
	expect(await f.manager.status({ name: "a" })).toMatchObject({
		attempt: { opRef: result.opRef, endState: "attempt_ended" },
		op: null,
	});
	expect(f.port.sends).toHaveLength(1);
	// Retire proves the host gone from the same router verdict.
	expect(await f.lanes.retire("a", "operator")).toMatchObject({ retired: true, sessionId: result.sessionId });
	f.port.status = status;
	const resumed = await f.manager.start({ name: "a", text: "continue", cwd: f.directory, resume: true });
	expect(resumed).toMatchObject({ started: true });
	expect(f.port.sends).toHaveLength(2);
	expect(f.job().attempts).toHaveLength(2);
});

test("#308 open attempt resumes after host loss without retiring first", async () => {
	const f = await fixture({ hostLostGraceMs: 0 });
	const result = await started(f);
	f.port.status = async () => {
		throw sessionUnavailable();
	};
	f.port.setSessionState(result.sessionId, { live: false });
	await until(() => f.db.work.workAttemptOpen().length === 0);
	const resumed = await f.manager.start({ name: "a", text: "continue", cwd: f.directory, resume: true });
	expect(resumed).toMatchObject({ started: true });
});

for (const [label, failure, live] of [
	["broker_unavailable", () => new GjcCliUnavailableError("connect ECONNREFUSED"), false],
	["an unstructured transport failure", () => new Error("gjc sdk request failed: timeout"), false],
	["session_unavailable while inspect still reports the host live", sessionUnavailable, true],
] as const) {
	test(`#308 ${label} never settles an open attempt`, async () => {
		const f = await fixture({ hostLostGraceMs: 0 });
		const result = await started(f);
		let reads = 0;
		f.port.status = async () => {
			reads++;
			throw failure();
		};
		f.port.setSessionState(result.sessionId, { live });
		await until(() => reads >= 5);
		expect(f.db.work.workAttemptGet(result.opRef)?.terminal).toBeNull();
		expect(f.job().attempts[0]?.endedAt).toBeUndefined();
		expect(await f.lanes.retire("a", "operator")).toMatchObject({ retired: false });
	});
}

test("#308 host loss must persist across the grace window before settling", async () => {
	let now = Date.now();
	const f = await fixture({ now: () => now, hostLostGraceMs: 60_000 });
	const result = await started(f);
	let reads = 0;
	f.port.status = async () => {
		reads++;
		throw sessionUnavailable();
	};
	f.port.setSessionState(result.sessionId, { live: false });
	await until(() => reads >= 3);
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal).toBeNull();
	now += 60_000;
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("host_lost");
});

test("#308 restart recovery classifies a router-disowned open attempt as host_lost", async () => {
	const f = await fixture();
	const result = await started(f);
	await f.manager.stop();
	f.port.status = async () => {
		throw sessionUnavailable();
	};
	f.port.setSessionState(result.sessionId, { live: false });
	await f.restart();
	await until(() => f.db.work.workAttemptOpen().length === 0);
	expect(f.db.work.workAttemptGet(result.opRef)?.terminal?.reasonCode).toBe("host_lost");
	expect(f.job().state).toBe("attempt_ended");
	expect(await f.lanes.retire("a", "operator")).toMatchObject({ retired: true });
});

test("#308 streamed turn frames advance lane activity past the attempt start", async () => {
	let now = Date.now();
	const f = await fixture({ now: () => now });
	const result = await started(f);
	const startedAt = f.db.bindings.workLaneRows()[0]!.last_activity_at!;
	expect(startedAt).toBe(f.db.work.workAttemptGet(result.opRef)!.startedAt);
	now += 5_000;
	f.port.emitTool(result.sessionId, { toolName: "bash" });
	await until(() => f.db.bindings.workLaneRows()[0]!.last_activity_at !== startedAt);
	expect(f.db.bindings.workLaneRows()[0]!.last_activity_at).toBe(new Date(now).toISOString());
	expect(f.job().attempts[0]?.endedAt).toBeUndefined();
});

test("transport failure ECONNRESET without errorStatus yields transport cause in lane notice", async () => {
	const f = await fixture();
	const result = await started(f, "a", origin);
	f.port.status = async (input) => ({
		operationRef: input.opRef,
		status: { status: "failed", receiptState: "missing" },
		summaryCompleted: true,
	});
	f.port.fetchWorkerOutput = async () => ({ status: "unavailable", code: "output_unavailable" });
	f.port.fail(result.opRef, "private failure", { code: "sdk_failed" });
	// Mock the transport failure
	f.port.setFailedTransportCause(result.sessionId, {
		kind: "transport",
		nativeErrorCode: "ECONNRESET",
		requestBytes: 1238792,
		endpointClass: "custom",
		retryMaxAttempts: 1,
	});
	await until(() => f.db.work.workAttemptOpen().length === 0);
	const attempt = f.db.work.workAttemptGet(result.opRef);
	expect(attempt?.terminal?.reasonCode).toBe("terminal_missing_receipt");
	expect(attempt?.output?.transportCause).toEqual({
		kind: "transport",
		nativeErrorCode: "ECONNRESET",
		requestBytes: 1238792,
		endpointClass: "custom",
		retryMaxAttempts: 1,
	});
	const notice = f.notices[0]?.text ?? "";
	expect(notice).toContain("final_response_missing");
	expect(notice).toContain("cause=transport");
	expect(notice).toContain("ECONNRESET");
	expect(notice).toContain("requestBytes=1238792");
	expect(notice).toContain("retryMaxAttempts=1");
	expect(notice).toContain("endpointClass=custom");
});

test("transport failure never leaks errorMessage secrets in lane notice", async () => {
	const f = await fixture();
	const result = await started(f, "a", origin);
	f.port.status = async (input) => ({
		operationRef: input.opRef,
		status: { status: "failed", receiptState: "missing" },
		summaryCompleted: true,
	});
	f.port.fetchWorkerOutput = async () => ({ status: "unavailable", code: "output_unavailable" });
	f.port.fail(result.opRef, "private failure", { code: "sdk_failed" });
	await until(() => f.db.work.workAttemptOpen().length === 0);
	const attempt = f.db.work.workAttemptGet(result.opRef);
	const notice = f.notices[0]?.text ?? "";
	// Private error messages must never appear in lane notice
	expect(notice).not.toContain("secret");
	expect(notice).not.toContain("private");
	expect(notice).not.toContain("authentication");
});

async function laneRepo(directory: string, branch: string, push: boolean) {
	const env = {
		...process.env,
		GIT_AUTHOR_NAME: "t",
		GIT_AUTHOR_EMAIL: "t@t",
		GIT_COMMITTER_NAME: "t",
		GIT_COMMITTER_EMAIL: "t@t",
	};
	const git = (cwd: string, ...args: string[]) => {
		const run = Bun.spawnSync(["git", "-C", cwd, ...args], { env, stdout: "pipe", stderr: "pipe" });
		if (run.exitCode !== 0) throw new Error(`git ${args.join(" ")}: ${run.stderr.toString()}`);
		return run.stdout.toString().trim();
	};
	const remote = `${directory}-origin.git`;
	cleanups.push(() => rm(remote, { recursive: true, force: true }));
	git(directory, "init", "-q", "--bare", remote);
	git(directory, "init", "-q", "-b", branch);
	git(directory, "remote", "add", "origin", remote);
	git(directory, "commit", "-q", "--allow-empty", "-m", "lane head");
	if (push) git(directory, "push", "-q", "origin", branch);
	return git(directory, "rev-parse", "HEAD");
}

type CheckStatusAnswer = Omit<CheckStatusSnapshot, "headSha"> & { readonly headSha?: string };

function withHeadSha(
	answer: CheckStatusAnswer | null | undefined,
	cwd: string,
): CheckStatusSnapshot | null | undefined {
	if (answer === null || answer === undefined) return answer;
	const headSha = answer.headSha ?? Bun.spawnSync(["git", "-C", cwd, "rev-parse", "HEAD"]).stdout.toString().trim();
	return { ...answer, headSha };
}

function recordingCheckClient(answer: CheckStatusAnswer | null) {
	const calls: Array<{ headSha: string; cwd: string }> = [];
	const client: CheckStatusClient = {
		async getCheckStatus(headSha, cwd) {
			calls.push({ headSha, cwd });
			return withHeadSha(answer, cwd);
		},
	};
	return { calls, client };
}

async function settledReport(f: Awaited<ReturnType<typeof fixture>>, label = "completed") {
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "done");
	await until(() => f.notices.some((notice) => notice.text.includes(`[lane a] ${label}`)));
	return f.notices.find((notice) => notice.text.includes(`[lane a] ${label}`))?.text ?? "";
}

test("#481 settled report of an unpushed lane carries its head and pushed=false when no PR is found", async () => {
	const { calls, client } = recordingCheckClient(null);
	const f = await fixture({ checkStatusClient: client, ownerTarget: () => origin });
	const head = await laneRepo(f.directory, "fix/unpushed", false);
	const text = await settledReport(f);
	expect(text).toContain(`head=${head.slice(0, 12)}`);
	expect(text).toContain("pushed=false");
	expect(text).not.toContain("pr=");
	expect(calls).toEqual([{ headSha: head, cwd: f.directory }]);
});

test("#481 settled report of a pushed lane without an open PR says pushed=true and no checks", async () => {
	const { calls, client } = recordingCheckClient(null);
	const f = await fixture({ checkStatusClient: client, ownerTarget: () => origin });
	const head = await laneRepo(f.directory, "fix/no-pr", true);
	const text = await settledReport(f);
	expect(text).toContain(`head=${head.slice(0, 12)}`);
	expect(text).toContain("pushed=true");
	expect(text).not.toContain("checks=");
	expect(calls).toEqual([{ headSha: head, cwd: f.directory }]);
});

test("#481 repo-backed bare clones detect pushed branches without remote-tracking refs", async () => {
	const { bareRepo, cleanup } = await bareRepoFixture("bare-clone-push-check");
	const { calls, client } = recordingCheckClient({ prNumber: 484, status: "green", failingChecks: [] });
	try {
		const f = await fixture({ checkStatusClient: client, ownerTarget: () => origin });
		const name = "bare-push";
		const result = await f.manager.start({ name, text: "work", repo: bareRepo });
		if (!result.started) throw new Error("work did not start");
		const job = f.job(name);
		const branch = managedLaneBranchName(name);
		const trackingRef = Bun.spawnSync(
			["git", "-C", job.lane.repositoryPath!, "show-ref", "--verify", `refs/remotes/origin/${branch}`],
			{ stdout: "pipe", stderr: "pipe" },
		);
		expect(trackingRef.exitCode).not.toBe(0);

		const push = Bun.spawnSync(["git", "-C", job.lane.worktreePath, "push", "-u", "origin", branch], {
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(push.exitCode).toBe(0);
		const headSha = Bun.spawnSync(["git", "-C", job.lane.worktreePath, "rev-parse", "HEAD"]).stdout.toString().trim();

		f.port.complete(result.opRef, "pushed");
		await until(() => f.notices.some((notice) => notice.text.includes("[lane bare-push] completed")));
		const report = f.notices.find((notice) => notice.text.includes("[lane bare-push] completed"))?.text ?? "";
		expect(report).toContain("pushed=true");
		expect(report).toContain("pr=484");
		expect(report).toContain("checks=green");
		expect(calls).toEqual([{ headSha, cwd: job.lane.worktreePath }]);
	} finally {
		await cleanup();
	}
});

test("#481 a local checkout lane pushes to its configured origin push URL", async () => {
	const { repoDir, bareRepo, cleanup } = await bareRepoFixture("checkout-push-target");
	const { calls, client } = recordingCheckClient({ prNumber: 485, status: "green", failingChecks: [] });
	const env = {
		...process.env,
		GIT_AUTHOR_NAME: "test",
		GIT_AUTHOR_EMAIL: "test@test",
		GIT_COMMITTER_NAME: "test",
		GIT_COMMITTER_EMAIL: "test@test",
	};
	const git = (cwd: string, ...args: string[]) => {
		const result = Bun.spawnSync(["git", "-C", cwd, ...args], { env, stdout: "pipe", stderr: "pipe" });
		if (result.exitCode !== 0) throw new Error(`git ${args.join(" ")}: ${result.stderr.toString()}`);
		return result.stdout.toString().trim();
	};
	try {
		const fetchUrl = "https://github.com/example/project.git";
		git(repoDir, "remote", "add", "origin", fetchUrl);
		git(repoDir, "remote", "set-url", "--push", "origin", bareRepo);

		const f = await fixture({ checkStatusClient: client, ownerTarget: () => origin });
		const name = "checkout-push";
		const result = await f.manager.start({ name, text: "work", repo: repoDir });
		if (!result.started) throw new Error("work did not start");
		const job = f.job(name);
		const branch = managedLaneBranchName(name);
		expect(git(job.lane.worktreePath, "remote", "get-url", "origin")).toBe(fetchUrl);
		expect(git(job.lane.worktreePath, "remote", "get-url", "--push", "origin")).toBe(bareRepo);
		git(job.lane.worktreePath, "push", "-u", "origin", branch);
		git(bareRepo, "show-ref", "--verify", `refs/heads/${branch}`);
		const accidentalLocalPush = Bun.spawnSync(["git", "-C", repoDir, "show-ref", "--verify", `refs/heads/${branch}`], {
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(accidentalLocalPush.exitCode).not.toBe(0);
		const headSha = git(job.lane.worktreePath, "rev-parse", "HEAD");

		f.port.complete(result.opRef, "pushed upstream");
		await until(() => f.notices.some((notice) => notice.text.includes("[lane checkout-push] completed")));
		const report = f.notices.find((notice) => notice.text.includes("[lane checkout-push] completed"))?.text ?? "";
		expect(report).toContain("pushed=true");
		expect(report).toContain("pr=485");
		expect(report).toContain("checks=green");
		expect(calls).toEqual([{ headSha, cwd: job.lane.worktreePath }]);
	} finally {
		await cleanup();
	}
});

test("#550 local checkout lanes resolve relative origin fetch and push URLs against the source repository", async () => {
	const { repoDir, bareRepo, cleanup } = await bareRepoFixture("relative-origin-url");
	let pushRepo = await mkdtemp(join(tmpdir(), "relative-origin-push-"));
	// Normalize path to resolve macOS /var -> /private/var symlink
	pushRepo = await realpath(pushRepo);
	const env = {
		...process.env,
		GIT_AUTHOR_NAME: "test",
		GIT_AUTHOR_EMAIL: "test@test",
		GIT_COMMITTER_NAME: "test",
		GIT_COMMITTER_EMAIL: "test@test",
	};
	const git = (cwd: string, ...args: string[]) => {
		const result = Bun.spawnSync(["git", "-C", cwd, ...args], { env, stdout: "pipe", stderr: "pipe" });
		if (result.exitCode !== 0) throw new Error(`git ${args.join(" ")}: ${result.stderr.toString()}`);
		return result.stdout.toString().trim();
	};
	try {
		Bun.spawnSync(["git", "init", "--bare", "-q", pushRepo], { env });
		git(repoDir, "remote", "add", "origin", relative(repoDir, bareRepo));
		git(repoDir, "remote", "set-url", "--push", "origin", relative(repoDir, pushRepo));
		git(repoDir, "fetch", "origin");
		const defaultBranch = git(repoDir, "branch", "--show-current");
		const initialRemoteSha = git(repoDir, "rev-parse", `refs/remotes/origin/${defaultBranch}`);

		const f = await fixture();
		const name = "relative-remote";
		const result = await f.manager.start({ name, text: "work", repo: repoDir });
		if (!result.started) throw new Error("work did not start");
		const worktreePath = f.job(name).lane.worktreePath;
		const branch = managedLaneBranchName(name);
		expect(git(worktreePath, "remote", "get-url", "origin")).toBe(bareRepo);
		expect(git(worktreePath, "remote", "get-url", "--push", "origin")).toBe(pushRepo);
		expect(git(worktreePath, "config", "--get", "remote.origin.fetch")).toBe("+refs/heads/*:refs/remotes/origin/*");
		expect(git(worktreePath, "rev-parse", `refs/remotes/origin/${defaultBranch}`)).toBe(initialRemoteSha);
		git(repoDir, "commit", "--allow-empty", "-m", "advance upstream");
		const advancedSha = git(repoDir, "rev-parse", "HEAD");
		git(repoDir, "push", bareRepo, `HEAD:refs/heads/${defaultBranch}`);
		git(worktreePath, "fetch", "origin");
		expect(git(worktreePath, "rev-parse", `refs/remotes/origin/${defaultBranch}`)).toBe(advancedSha);
		git(worktreePath, "push", "-u", "origin", branch);
		git(pushRepo, "show-ref", "--verify", `refs/heads/${branch}`);
	} finally {
		await cleanup();
		await rm(pushRepo, { recursive: true, force: true });
	}
});

test("#550 local checkout lanes preserve scp-like origin URLs", async () => {
	const { repoDir, cleanup } = await bareRepoFixture("scp-origin-url");
	const env = { ...process.env, GIT_AUTHOR_NAME: "test", GIT_AUTHOR_EMAIL: "test@test" };
	const git = (cwd: string, ...args: string[]) => {
		const result = Bun.spawnSync(["git", "-C", cwd, ...args], { env, stdout: "pipe", stderr: "pipe" });
		if (result.exitCode !== 0) throw new Error(`git ${args.join(" ")}: ${result.stderr.toString()}`);
		return result.stdout.toString().trim();
	};
	try {
		const fetchUrl = "git@github.com:example/project.git";
		const pushUrl = "git@example.net:mirror/project.git";
		git(repoDir, "remote", "add", "origin", fetchUrl);
		git(repoDir, "remote", "set-url", "--push", "origin", pushUrl);

		const f = await fixture();
		const result = await f.manager.start({ name: "scp-origin", text: "work", repo: repoDir });
		if (!result.started) throw new Error("work did not start");
		const worktreePath = f.job("scp-origin").lane.worktreePath;
		expect(git(worktreePath, "remote", "get-url", "origin")).toBe(fetchUrl);
		expect(git(worktreePath, "remote", "get-url", "--push", "origin")).toBe(pushUrl);
	} finally {
		await cleanup();
	}
});

test("#481 remote push probes are asynchronous and bounded", async () => {
	const { bareRepo, cleanup } = await bareRepoFixture("bounded-remote-probe");
	const { calls, client } = recordingCheckClient({ prNumber: 485, status: "red", failingChecks: ["lint"] });
	const f = await fixture({ checkStatusClient: client, ownerTarget: () => origin });
	let remoteRequests = 0;
	const slowRemote = Bun.serve({
		hostname: "127.0.0.1",
		port: 0,
		async fetch() {
			remoteRequests++;
			await Bun.sleep(250);
			return new Response("remote unavailable", { status: 503 });
		},
	});
	try {
		const name = "slow-remote";
		const result = await f.manager.start({ name, text: "work", repo: bareRepo });
		if (!result.started) throw new Error("work did not start");
		const setPushUrl = Bun.spawnSync(
			[
				"git",
				"-C",
				f.job(name).lane.worktreePath,
				"remote",
				"set-url",
				"--push",
				"origin",
				`http://127.0.0.1:${slowRemote.port}/repo.git`,
			],
			{ stdout: "pipe", stderr: "pipe" },
		);
		expect(setPushUrl.exitCode).toBe(0);
		let timerFired = false;
		const timer = setTimeout(() => {
			timerFired = true;
		}, 50);
		const start = Date.now();
		f.port.complete(result.opRef, "remote probe");
		const completionCallMs = Date.now() - start;
		await Bun.sleep(100);
		clearTimeout(timer);
		expect(completionCallMs).toBeLessThan(100);
		expect(timerFired).toBe(true);
		await until(() => remoteRequests > 0);

		await until(() => f.job(name).state === "awaiting_checks");
		const report = f.notices.find((notice) => notice.text.includes("[lane slow-remote] attempt_ended"))?.text ?? "";
		const headSha = Bun.spawnSync(["git", "-C", f.job(name).lane.worktreePath, "rev-parse", "HEAD"])
			.stdout.toString()
			.trim();
		expect(report).toContain("pushed=unknown");
		expect(report).toContain("pr=485");
		expect(f.job(name).awaitingChecks?.headSha).toBe(headSha);
		expect(calls.length).toBeGreaterThan(0);
		expect(f.port.sends).toHaveLength(1);
	} finally {
		await slowRemote.stop(true);
		await cleanup();
	}
});

test("#481 settled report of a pushed lane with a red PR names the PR and failing checks", async () => {
	const { client } = recordingCheckClient({ prNumber: 484, status: "red", failingChecks: ["lint", "test"] });
	const f = await fixture({ checkStatusClient: client, ownerTarget: () => origin });
	await laneRepo(f.directory, "fix/red", true);
	const text = await settledReport(f, "attempt_ended");
	expect(text).toContain("pushed=true");
	expect(text).toContain("pr=484");
	expect(text).toContain("checks=red(lint,test)");
});

test("#481 check rollup: latest named checks must all be terminal before settling", () => {
	const run = (name: string, status: string, conclusion = "") => ({ __typename: "CheckRun", name, status, conclusion });
	const headSha = "a".repeat(40);
	expect(
		classifyCheckRollup(1, headSha, [
			run("lint", "COMPLETED", "FAILURE"),
			run("lint", "COMPLETED", "FAILURE"),
			run("test", "IN_PROGRESS"),
		]),
	).toEqual({ prNumber: 1, headSha, status: "pending", failingChecks: [] });
	expect(
		classifyCheckRollup(10, headSha, [
			{ ...run("lint", "COMPLETED", "FAILURE"), startedAt: "2026-10-18T12:00:00Z" },
			{ ...run("lint", "COMPLETED", "SUCCESS"), startedAt: "2026-10-18T12:01:00Z" },
			run("test", "COMPLETED", "SUCCESS"),
		]),
	).toEqual({ prNumber: 10, headSha, status: "green", failingChecks: [] });
	expect(
		classifyCheckRollup(11, headSha, [
			{ ...run("lint", "COMPLETED", "SUCCESS"), startedAt: "2026-10-18T12:00:00Z" },
			{ ...run("lint", "COMPLETED", "FAILURE"), startedAt: "2026-10-18T12:01:00Z" },
		]),
	).toEqual({ prNumber: 11, headSha, status: "red", failingChecks: ["lint"] });
	const sameNamedCheckRunAndStatus = [
		{
			...run("ci", "COMPLETED", "FAILURE"),
			startedAt: "2026-10-18T12:00:00Z",
			detailsUrl: "https://github.com/example/repo/actions/runs/104",
		},
		{
			__typename: "StatusContext",
			context: "ci",
			state: "SUCCESS",
			createdAt: "2026-10-18T12:01:00Z",
		},
	];
	expect(classifyCheckRollup(12, headSha, sameNamedCheckRunAndStatus)).toEqual({
		prNumber: 12,
		headSha,
		status: "red",
		failingChecks: ["ci"],
	});
	expect(failedCheckLogTargets(sameNamedCheckRunAndStatus)).toEqual([{ runId: "104" }]);
	expect(
		failedCheckLogTargets([
			{
				...run("lint", "COMPLETED", "FAILURE"),
				startedAt: "2026-10-18T12:00:00Z",
				detailsUrl: "https://github.com/example/repo/actions/runs/100",
			},
			{
				...run("lint", "COMPLETED", "FAILURE"),
				startedAt: "2026-10-18T12:01:00Z",
				detailsUrl: "https://github.com/example/repo/actions/runs/101",
			},
			{
				...run("lint", "COMPLETED", "FAILURE"),
				startedAt: "2026-10-18T12:02:00Z",
				detailsUrl: "https://github.com/example/repo/actions/runs/102",
			},
			{
				...run("lint", "COMPLETED", "FAILURE"),
				startedAt: "2026-10-18T12:03:00Z",
				detailsUrl: "https://github.com/example/repo/actions/runs/103",
			},
		]),
	).toEqual([{ runId: "103" }]);
	expect(classifyCheckRollup(2, headSha, [run("lint", "COMPLETED", "SUCCESS"), run("test", "QUEUED")]).status).toBe(
		"pending",
	);
	expect(classifyCheckRollup(3, headSha, []).status).toBe("pending");
	expect(
		classifyCheckRollup(4, headSha, [
			run("lint", "COMPLETED", "SUCCESS"),
			{ __typename: "StatusContext", context: "ci/x", state: "SUCCESS" },
		]).status,
	).toBe("green");
	expect(
		classifyCheckRollup(5, headSha, [{ __typename: "StatusContext", context: "ci/x", state: "ERROR" }]).failingChecks,
	).toEqual(["ci/x"]);
	expect(classifyCheckRollup(6, headSha, [run("lint", "COMPLETED", "STALE")]).status).toBe("pending");
	expect(classifyCheckRollup(7, headSha, [run("lint", "COMPLETED", "UNKNOWN")]).status).toBe("pending");
	for (const conclusion of ["NEUTRAL", "SKIPPED"])
		expect(classifyCheckRollup(9, headSha, [run("optional", "COMPLETED", conclusion)]).status).toBe("green");
	expect(
		failedCheckLogTargets([
			{
				status: "COMPLETED",
				conclusion: "FAILURE",
				detailsUrl: "https://github.com/example/repo/actions/runs/123/job/456",
			},
			{
				status: "COMPLETED",
				conclusion: "FAILURE",
				detailsUrl: "https://github.com/example/repo/actions/runs/123junk",
			},
		]),
	).toEqual([{ runId: "123", jobId: "456" }]);
	expect(() => classifyCheckRollup(8, "invalid", [])).toThrow("PR head SHA is invalid");
});

async function writeGhCheckStub(
	ghPath: string,
	associated: readonly object[],
	details: object,
	argsPath = `${ghPath}.args`,
	branchCandidates: readonly object[] = [],
): Promise<void> {
	const script = [
		"#!/bin/sh",
		`printf '%s\\n' "$*" >> ${JSON.stringify(argsPath)}`,
		`if [ "$1" = api ] && [ "$2" != "repos/{owner}/{repo}/pulls" ]; then printf '%s' ${JSON.stringify(JSON.stringify(associated))}; elif [ "$1" = api ]; then printf '%s' ${JSON.stringify(JSON.stringify(branchCandidates))}; else printf '%s' ${JSON.stringify(JSON.stringify(details))}; fi`,
		"",
	].join("\n");
	await writeFile(ghPath, script);
	await chmod(ghPath, 0o755);
}

test("#628 resolves an exact pushed head even when its PR uses a different branch", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-gh-checks-"));
	cleanups.push(() => rm(directory, { recursive: true, force: true }));
	const argsPath = join(directory, "args.txt");
	const headSha = "a".repeat(40);
	const ghPath = join(directory, "gh");
	await writeGhCheckStub(
		ghPath,
		[
			{
				number: 626,
				state: "open",
				merged_at: null,
				updated_at: "2026-10-19T04:00:00Z",
				head: { ref: "feature/thread-shutup", sha: headSha },
			},
		],
		{
			number: 626,
			headRefOid: headSha,
			state: "OPEN",
			mergedAt: null,
			statusCheckRollup: [{ name: "lint", status: "COMPLETED", conclusion: "FAILURE" }],
		},
		argsPath,
	);

	const snapshot = await ghCheckStatusClient(1_000, ghPath).getCheckStatus(
		headSha,
		directory,
		undefined,
		"work/67772d69737375652d363235",
	);
	expect(snapshot).toMatchObject({
		prNumber: 626,
		headSha,
		status: "red",
		failingChecks: ["lint"],
		prState: "open",
		merged: false,
	});
	expect((await readFile(argsPath, "utf8")).trim().split("\n").sort()).toEqual(
		[
			`api repos/{owner}/{repo}/commits/${headSha}/pulls -X GET -F per_page=100`,
			"api repos/{owner}/{repo}/pulls -X GET -F head={owner}:work/67772d69737375652d363235 -F state=all -F per_page=100",
			"pr view 626 --json number,headRefOid,state,mergedAt,statusCheckRollup",
		].sort(),
	);
});

test("#595 GH check lookup keeps the lane branch as a SHA-checked PR candidate", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-gh-checks-"));
	cleanups.push(() => rm(directory, { recursive: true, force: true }));
	const headSha = "b".repeat(40);
	const ghPath = join(directory, "gh");
	await writeGhCheckStub(
		ghPath,
		[],
		{
			number: 630,
			headRefOid: headSha,
			state: "OPEN",
			mergedAt: null,
			statusCheckRollup: [{ name: "ci", status: "IN_PROGRESS" }],
		},
		undefined,
		[
			{
				number: 630,
				head: { ref: "work/lane", sha: headSha },
				state: "open",
				merged_at: null,
				updated_at: "2026-10-19T04:00:00Z",
			},
		],
	);

	const snapshot = await ghCheckStatusClient(1_000, ghPath).getCheckStatus(headSha, directory, undefined, "work/lane");
	expect(snapshot).toMatchObject({ prNumber: 630, headSha, status: "pending", prState: "open", merged: false });
});

test("#595 GH check lookup chooses the lane-branch candidate deterministically for a shared head SHA", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-gh-checks-"));
	cleanups.push(() => rm(directory, { recursive: true, force: true }));
	const headSha = "c".repeat(40);
	const ghPath = join(directory, "gh");
	await writeGhCheckStub(
		ghPath,
		[
			{
				number: 571,
				state: "open",
				updated_at: "2026-10-19T04:02:00Z",
				head: { ref: "feature/other", sha: headSha },
			},
			{
				number: 572,
				state: "open",
				updated_at: "2026-10-19T04:01:00Z",
				head: { ref: "work/lane", sha: headSha },
			},
			{
				number: 569,
				state: "closed",
				merged_at: null,
				head: { ref: "work/lane", sha: headSha },
			},
		],
		{
			number: 572,
			headRefOid: headSha,
			state: "OPEN",
			mergedAt: null,
			statusCheckRollup: [{ name: "ci", status: "IN_PROGRESS" }],
		},
	);

	const snapshot = await ghCheckStatusClient(1_000, ghPath).getCheckStatus(headSha, directory, undefined, "work/lane");
	expect(snapshot).toMatchObject({ prNumber: 572, headSha, status: "pending", prState: "open", merged: false });
});

test("#595 GH check lookup breaks an exact-head PR tie by newest update then number", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-gh-checks-"));
	cleanups.push(() => rm(directory, { recursive: true, force: true }));
	const headSha = "d".repeat(40);
	const ghPath = join(directory, "gh");
	await writeGhCheckStub(
		ghPath,
		[
			{ number: 580, state: "open", updated_at: "2026-10-19T04:00:00Z", head: { ref: "feature/older", sha: headSha } },
			{ number: 581, state: "open", updated_at: "2026-10-19T04:02:00Z", head: { ref: "feature/newer", sha: headSha } },
			{ number: 579, state: "open", updated_at: "2026-10-19T04:02:00Z", head: { ref: "feature/tied", sha: headSha } },
		],
		{
			number: 579,
			headRefOid: headSha,
			state: "OPEN",
			mergedAt: null,
			statusCheckRollup: [{ name: "ci", status: "IN_PROGRESS" }],
		},
	);

	const snapshot = await ghCheckStatusClient(1_000, ghPath).getCheckStatus(headSha, directory, undefined, "work/lane");
	expect(snapshot).toMatchObject({ prNumber: 579, headSha, status: "pending", prState: "open", merged: false });
});

test("#595 GH check lookup preserves the persisted PR over another open duplicate", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-gh-checks-"));
	cleanups.push(() => rm(directory, { recursive: true, force: true }));
	const headSha = "e".repeat(40);
	const ghPath = join(directory, "gh");
	await writeGhCheckStub(
		ghPath,
		[
			{ number: 574, state: "open", head: { ref: "work/lane", sha: headSha } },
			{
				number: 575,
				state: "closed",
				merged_at: "2026-10-14T12:00:00Z",
				head: { ref: "feature/closed", sha: headSha },
			},
		],
		{
			number: 575,
			headRefOid: headSha,
			state: "CLOSED",
			mergedAt: "2026-10-14T12:00:00Z",
			statusCheckRollup: [{ name: "ci", status: "COMPLETED", conclusion: "SUCCESS" }],
		},
	);

	const snapshot = await ghCheckStatusClient(1_000, ghPath).getCheckStatus(headSha, directory, 575);
	expect(snapshot).toMatchObject({
		prNumber: 575,
		headSha,
		status: "green",
		prState: "closed",
		merged: true,
	});
});

test("#595 GH check lookup treats a full result page as ambiguous, with or without a match", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-gh-checks-"));
	cleanups.push(() => rm(directory, { recursive: true, force: true }));
	const headSha = "f".repeat(40);
	const ghPath = join(directory, "gh");
	const client = ghCheckStatusClient(1_000, ghPath);
	const prResults = (count: number, includeExactHead = false) =>
		Array.from({ length: count }, (_, index) => ({
			number: index + 1,
			state: "closed",
			head: {
				ref: `old/${index + 1}`,
				sha: includeExactHead && index === 0 ? headSha : (index + 1).toString(16).padStart(40, "0"),
			},
		}));
	const lookup = async (count: number, includeExactHead = false) => {
		await writeGhCheckStub(ghPath, prResults(count, includeExactHead), { number: 1, headRefOid: headSha });
		return client.getCheckStatus(headSha, directory);
	};

	expect(await lookup(99)).toBeNull();
	expect(await lookup(100)).toBeUndefined();
	expect(await lookup(100, true)).toBeUndefined();
});

/** A check client whose answer the test changes between polls. */
function scriptedChecks(initial: CheckStatusAnswer | null | undefined) {
	let answer = initial;
	const calls: Array<{ headSha: string; cwd: string }> = [];
	const requestedPrNumbers: Array<number | undefined> = [];
	const client: CheckStatusClient = {
		async getCheckStatus(headSha, cwd, prNumber) {
			calls.push({ headSha, cwd });
			requestedPrNumbers.push(prNumber);
			return withHeadSha(answer, cwd);
		},
	};
	return {
		client,
		calls,
		requestedPrNumbers,
		set(next: CheckStatusAnswer | null | undefined) {
			answer = next;
		},
	};
}

/** Starts lane `a` on a pushed branch and completes its attempt into awaiting_checks. */
async function awaitingLane(f: Awaited<ReturnType<typeof fixture>>, branch: string, name = "a", cwd = f.directory) {
	const head = await laneRepo(cwd, branch, true);
	const result = await started(f, name, origin);
	f.port.complete(result.opRef, "done");
	await until(() => f.job(name).state === "awaiting_checks");
	return { head, sessionId: result.sessionId };
}

/** Runs recovery passes (each ends with one awaiting_checks poll) until `predicate` holds. */
async function pollUntil(f: Awaited<ReturnType<typeof fixture>>, predicate: () => boolean): Promise<void> {
	for (let i = 0; i < 200 && !predicate(); i++) {
		await f.manager.recover();
		await Bun.sleep(5);
	}
	expect(predicate()).toBe(true);
}

/** Runs `passes` complete recovery passes. */
async function pollTimes(f: Awaited<ReturnType<typeof fixture>>, passes: number): Promise<void> {
	for (let i = 0; i < passes; i++) {
		await f.manager.recover();
		await f.manager.recover();
	}
}

function checksNotice(f: Awaited<ReturnType<typeof fixture>>, lead: string): string | undefined {
	return f.notices.find((notice) => notice.text.startsWith(lead))?.text;
}

test("#481 a lane with pending PR checks reports an unfinished attempt and waits with its PR and head recorded", async () => {
	const checks = scriptedChecks({ prNumber: 485, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const { head } = await awaitingLane(f, "fix/pending");
	const job = f.job();
	expect(job.awaitingChecks).toMatchObject({ prNumber: 485, headSha: head, retries: 0 });
	const notice = checksNotice(f, "[lane a] attempt_ended");
	expect(notice).toContain(`head=${head.slice(0, 12)}`);
	expect(notice).toContain("pr=485");
	expect(notice).toContain("checks=pending");
});

test("#481 multiple awaiting lanes poll GitHub concurrently", async () => {
	let measure = false;
	let active = 0;
	let maximumActive = 0;
	const client: CheckStatusClient = {
		async getCheckStatus(headSha, cwd) {
			if (measure) {
				active++;
				maximumActive = Math.max(maximumActive, active);
				await Bun.sleep(50);
				active--;
			}
			return {
				prNumber: cwd.endsWith("second-repo") ? 502 : 501,
				headSha,
				status: "pending",
				failingChecks: [],
			};
		},
	};
	const f = await fixture({ checkStatusClient: client, ownerTarget: () => origin });
	const secondRepo = join(f.directory, "second-repo");
	await mkdir(secondRepo);
	await awaitingLane(f, "fix/concurrent-first", "a");
	await awaitingLane(f, "fix/concurrent-second", "b", secondRepo);
	measure = true;
	await f.manager.recover();
	expect(maximumActive).toBe(2);
});

test("#481 a temporary initial PR lookup failure stays durable until the PR is discovered", async () => {
	const checks = scriptedChecks(undefined);
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const head = await laneRepo(f.directory, "fix/pr-appears-later", true);
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "done");
	await until(() => f.job().state === "awaiting_checks");
	const since = f.job().awaitingChecks?.since;
	expect(f.job().awaitingChecks).toMatchObject({ prNumber: null, headSha: head, retries: 0 });
	checks.set({ prNumber: 497, status: "pending", failingChecks: [] });
	await pollUntil(f, () => f.job().awaitingChecks?.prNumber === 497);
	expect(f.job().state).toBe("awaiting_checks");
	expect(f.job().awaitingChecks?.since).toBe(since);
	expect(checks.calls.some((call) => call.headSha === head)).toBe(true);
});

test("#595 an initially unknown PR is found from the lane head and terminal-green checks settle on the first poll", async () => {
	const checks = scriptedChecks(undefined);
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const { head } = await awaitingLane(f, "fix/review-only-head-lookup");
	expect(f.job().awaitingChecks).toMatchObject({ prNumber: null, headSha: head });
	checks.set({ prNumber: 595, headSha: head, status: "green", failingChecks: [] });
	await pollUntil(f, () => f.job().state !== "awaiting_checks");
	expect(f.job().state).toBe("done");
	expect(f.job().awaitingChecks).toBeUndefined();
	expect(checks.calls.at(-1)).toEqual({ headSha: head, cwd: f.job().lane.worktreePath });
	expect(checksNotice(f, "[lane a] checks passed")).toContain(`head=${head.slice(0, 12)} pr=595 checks=green`);
});

test("#595 an exact PR head settles checks when its ref is not the worktree's local branch", async () => {
	const checks = scriptedChecks({ prNumber: 599, status: "pending", failingChecks: [], prState: "open" });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const head = await laneRepo(f.directory, "review/fork-head", false);
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "review-only branch");
	await until(() => f.job().state === "awaiting_checks");
	expect(f.job().awaitingChecks).toMatchObject({ prNumber: 599, headSha: head });

	checks.set({ prNumber: 599, headSha: head, status: "red", failingChecks: ["lint"], prState: "open" });
	await pollUntil(f, () => f.port.sends.length === 2);
	expect(f.job().awaitingChecks).toMatchObject({ prNumber: 599, retries: 1 });
	checks.set({ prNumber: 599, headSha: head, status: "green", failingChecks: [], prState: "open" });
	const repair = f.port.sends[1];
	if (!repair) throw new Error("red checks did not start a repair attempt");
	f.port.complete(repair.opRef, "checks fixed");
	await until(() => f.job().state === "done");
	expect(f.job().state).toBe("done");
	expect(f.job().awaitingChecks).toBeUndefined();
});

test("#628 a successful no-PR lookup waits for the existing timeout, then permits resume", async () => {
	let now = Date.parse("2026-10-19T00:00:00.000Z");
	const checks = scriptedChecks(undefined);
	const f = await fixture({ now: () => now, checkStatusClient: checks.client, ownerTarget: () => origin });
	const { head } = await awaitingLane(f, "fix/review-without-pr");
	expect(f.job().awaitingChecks).toMatchObject({ prNumber: null, headSha: head });
	checks.set(null);
	await pollTimes(f, 2);
	expect(f.job().state).toBe("awaiting_checks");
	now += 3_600_000 - 1;
	await pollTimes(f, 2);
	expect(f.job().state).toBe("awaiting_checks");
	now++;
	await pollUntil(f, () => f.job().state !== "awaiting_checks");
	expect(f.job().state).toBe("attempt_ended");
	expect(f.job().awaitingChecks).toBeUndefined();
	expect(f.job().escalations.at(-1)).toContain("awaiting_checks ended: timeout after 60m");
	expect(checksNotice(f, "[lane a] attempt_ended: awaiting_checks timeout")).toContain("pr=unknown");
	const resumed = await f.manager.start({ name: "a", text: "continue after no-PR timeout", cwd: f.directory });
	expect(resumed).toMatchObject({ started: true });
});

test("#481 awaiting_checks rejects a new external attempt while checks are pending", async () => {
	const checks = scriptedChecks({ prNumber: 488, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	await awaitingLane(f, "fix/reject-start-during-checks");
	const held = await f.manager.start({ name: "a", text: "skip the check wait", cwd: f.directory });
	expect(held).toMatchObject({ started: false, held: true, state: "awaiting_checks" });
	expect(f.port.sends).toHaveLength(1);
	checks.set({ prNumber: 488, status: "green", failingChecks: [] });
	await pollUntil(f, () => f.job().state === "done");
	const afterDone = await f.manager.start({ name: "a", text: "reopen completed work", cwd: f.directory });
	expect(afterDone).toMatchObject({ started: false, held: true, state: "done" });
	expect(f.port.sends).toHaveLength(1);
});

test("#481 a lane whose PR is already red at settle starts the bounded repair cycle", async () => {
	const checks = scriptedChecks({ prNumber: 486, status: "red", failingChecks: ["test"] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	await laneRepo(f.directory, "fix/red-pr", true);
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "done");
	await until(() => f.job().state === "awaiting_checks");
	await pollUntil(f, () => f.port.sends.length === 2);
	expect(f.job().state).toBe("running");
	expect(f.job().awaitingChecks?.retries).toBe(1);
	expect(f.port.sends[1]?.text).toContain("test");
	expect(checksNotice(f, "[lane a] completed")).toBeUndefined();
	expect(checksNotice(f, "[lane a] attempt_ended")).toContain("checks=red(test)");
});

test("#481 a lane whose HEAD is unpushed never waits for checks", async () => {
	const checks = scriptedChecks(null);
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const head = await laneRepo(f.directory, "fix/unpushed-head", false);
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "done");
	await until(() => checksNotice(f, "[lane a] completed") !== undefined);
	expect(f.job().state).toBe("attempt_ended");
	expect(checksNotice(f, "[lane a] completed")).toContain("pushed=false");
	expect(checks.calls).toEqual([{ headSha: head, cwd: f.directory }]);
});

test("#481 a failed PR lookup does not park a commit known to be unpushed", async () => {
	const checks = scriptedChecks(undefined);
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const head = await laneRepo(f.directory, "fix/unpushed-lookup-failed", false);
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "done");
	await until(() => checksNotice(f, "[lane a] attempt_ended") !== undefined);
	expect(f.job().state).toBe("attempt_ended");
	expect(f.job().awaitingChecks).toBeUndefined();
	expect(checksNotice(f, "[lane a] attempt_ended")).toContain("pushed=false");
	expect(checks.calls).toEqual([{ headSha: head, cwd: f.directory }]);
});

test("#481 a detached checkout is not parked when no PR lookup can be attempted", async () => {
	const checks = scriptedChecks(undefined);
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	await laneRepo(f.directory, "fix/detached-head", true);
	const result = await started(f, "a", origin);
	const worktreePath = f.job().lane.worktreePath;
	const checkout = Bun.spawnSync(["git", "-C", worktreePath, "checkout", "--detach", "HEAD"], {
		stdout: "pipe",
		stderr: "pipe",
	});
	expect(checkout.exitCode).toBe(0);
	f.port.complete(result.opRef, "done");
	await until(() => checksNotice(f, "[lane a] attempt_ended") !== undefined);
	expect(f.job().state).toBe("attempt_ended");
	expect(f.job().awaitingChecks).toBeUndefined();
	expect(checks.calls).toEqual([]);
});

test("#595 completed lane with already-green remote-head checks settles without parking", async () => {
	const checks = scriptedChecks({ prNumber: 496, status: "green", failingChecks: [], prState: "open" });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const head = await laneRepo(f.directory, "fix/already-green", true);
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "reviewed PR");
	await until(() => f.job().state === "done");
	expect(f.job().state).toBe("done");
	expect(f.job().awaitingChecks).toBeUndefined();
	expect(checksNotice(f, "[lane a] completed")).toContain(
		`head=${head.slice(0, 12)} pushed=true remoteHead=${head.slice(0, 12)} pr=496 checks=green`,
	);
	expect(checks.calls).toEqual([{ headSha: head, cwd: f.directory }]);
});

test("#595 a green fast path cannot settle a different PR than the persisted checks cycle", async () => {
	const checks = scriptedChecks({ prNumber: 596, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	await awaitingLane(f, "fix/fast-path-pr-mismatch");
	checks.set({ prNumber: 596, status: "red", failingChecks: ["lint"], prState: "open" });
	await pollUntil(f, () => f.port.sends.length === 2);
	expect(f.job().awaitingChecks?.prNumber).toBe(596);
	checks.set({ prNumber: 597, status: "green", failingChecks: [], prState: "open" });
	const repair = f.port.sends[1];
	if (!repair) throw new Error("red checks did not start a repair attempt");
	const noticesBefore = f.notices.length;
	f.port.complete(repair.opRef, "finished repair");
	await until(() => f.notices.length > noticesBefore);
	expect(f.job().state).toBe("awaiting_checks");

	await pollUntil(f, () => f.job().state !== "awaiting_checks");
	expect(f.job().state).toBe("attempt_ended");
	expect(checks.requestedPrNumbers.at(-1)).toBe(596);
	expect(f.job().escalations.at(-1)).toContain("awaiting_checks ended: pr_closed pr=596");
	const resumed = await f.manager.start({ name: "a", text: "resume after PR changed", cwd: f.directory });
	expect(resumed).toMatchObject({ started: true });
});

test("#595 green checks on a closed, unmerged PR leave the lane resumable", async () => {
	const checks = scriptedChecks({
		prNumber: 598,
		status: "green",
		failingChecks: [],
		prState: "closed",
		merged: false,
	});
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	await laneRepo(f.directory, "fix/closed-unmerged-green", true);
	const result = await started(f, "a", origin);
	f.port.complete(result.opRef, "closed PR must not finalize lane");
	await until(() => f.notices.some((notice) => notice.text.includes("[lane a] completed")));
	expect(f.job().state).toBe("awaiting_checks");

	await pollUntil(f, () => f.job().state !== "awaiting_checks");
	expect(f.job().state).toBe("attempt_ended");
	expect(f.job().escalations.at(-1)).toContain("awaiting_checks ended: pr_closed pr=598");
	const resumed = await f.manager.start({ name: "a", text: "resume closed-PR work", cwd: f.directory });
	expect(resumed).toMatchObject({ started: true });
});

test("#481 awaiting_checks: green checks settle the lane done and report the pass", async () => {
	const checks = scriptedChecks({ prNumber: 487, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	await awaitingLane(f, "fix/pending-to-green");
	checks.set({ prNumber: 487, status: "green", failingChecks: [] });
	await pollUntil(f, () => f.job().state !== "awaiting_checks");
	expect(f.job().state).toBe("done");
	expect(f.job().awaitingChecks).toBeUndefined();
	expect(checksNotice(f, "[lane a] checks passed")).toContain("pr=487 checks=green");
});

test("#481 awaiting_checks: red checks re-steer the lane session with the failing check names and persist retry 1", async () => {
	const checks = scriptedChecks({ prNumber: 490, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const { sessionId } = await awaitingLane(f, "fix/red-resteer");
	const since = f.job().awaitingChecks?.since;
	expect(f.port.sends).toHaveLength(1);
	checks.set({
		prNumber: 490,
		status: "red",
		failingChecks: ["unit-test", "lint-check"],
		failureLogTail: "FAILED unit-test: expected 2, received 1",
	});
	await pollUntil(f, () => f.port.sends.length === 2);
	const steer = f.port.sends[1];
	expect(steer.sessionId).toBe(sessionId);
	expect(steer.text).toContain("PR #490 checks failed");
	expect(steer.text).toContain("unit-test, lint-check");
	expect(steer.text).toContain("Failed log tail:\nFAILED unit-test: expected 2, received 1");
	const job = parseLaneJobRecord(f.db.work.laneJobJson(laneJobIdentity("a").jobId)!);
	expect(job.awaitingChecks?.retries).toBe(1);
	expect(job.state).toBe("running");
	expect(job.attempts.at(-1)?.opRef).toBe(steer.opRef);
	checks.set({ prNumber: 490, status: "pending", failingChecks: [] });
	f.port.complete(steer.opRef, "tried a fix");
	await until(() => f.job().state === "awaiting_checks");
	expect(f.job().awaitingChecks?.since).toBe(since);
	expect(f.job().awaitingChecks?.retries).toBe(1);
});

test("#481 force-retiring a dead red-check retry clears its old checks cycle", async () => {
	const checks = scriptedChecks({ prNumber: 498, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const { sessionId } = await awaitingLane(f, "fix/retire-red-retry");
	checks.set({ prNumber: 498, status: "red", failingChecks: ["unit-test"] });
	await pollUntil(f, () => f.port.sends.length === 2);
	expect(f.job().awaitingChecks?.retries).toBe(1);
	f.port.setSessionState(sessionId, { live: false });
	expect(await f.lanes.retireAllDead()).toEqual({ count: 1, names: ["a"] });
	expect(f.job().state).toBe("attempt_ended");
	expect(f.job().awaitingChecks).toBeUndefined();
	expect(checksNotice(f, "[lane a] completed")).toBeUndefined();
	expect(checksNotice(f, "[lane a] attempt_ended")).toContain("pr=498 checks=pending");
});

test("#595 force-retiring a lane awaiting checks clears the hold and allows resume", async () => {
	const checks = scriptedChecks({ prNumber: 598, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const { sessionId } = await awaitingLane(f, "fix/retire-awaiting-checks");
	expect(f.job().state).toBe("awaiting_checks");
	f.port.setSessionState(sessionId, { live: false });
	expect(await f.lanes.forceRetire("a")).toMatchObject({ retired: true, forced: true });
	expect(f.job().state).toBe("attempt_ended");
	expect(f.job().awaitingChecks).toBeUndefined();
	expect(f.job().escalations.at(-1)).toContain("awaiting_checks ended: lane force-retired before checks settled");
	const resumed = await f.manager.start({
		name: "a",
		text: "continue after retirement",
		cwd: f.directory,
		resume: true,
	});
	expect(resumed).toMatchObject({ started: true });
});

test("#481 awaiting_checks: the third red result settles the lane failed with the red checks in its report", async () => {
	const red: CheckStatusAnswer = { prNumber: 491, status: "red", failingChecks: ["unit-test", "lint-check"] };
	const checks = scriptedChecks({ prNumber: 491, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	await awaitingLane(f, "fix/red-exhausted");
	checks.set(red);
	for (const retry of [1, 2]) {
		await pollUntil(f, () => f.port.sends.length === retry + 1);
		expect(f.job().awaitingChecks?.retries).toBe(retry);
		// The re-steered attempt ends while its PR is still red: the lane parks again.
		f.port.complete(f.port.sends[retry].opRef, "tried a fix");
		await until(() => f.job().state === "awaiting_checks");
	}
	await pollUntil(f, () => f.job().state !== "awaiting_checks");
	const job = f.job();
	expect(job.state).toBe("awaiting_operator");
	expect(job.awaitingChecks).toBeUndefined();
	expect(job.escalations.at(-1)).toContain(
		"awaiting_checks failed: checks=red(unit-test,lint-check) after 2 re-steers",
	);
	expect(f.port.sends).toHaveLength(3);
	const report = checksNotice(f, "[lane a] failed");
	expect(report).toContain("checks=red(unit-test,lint-check)");
	expect(report).toContain("pr=491");
	const resumed = await f.manager.start({
		name: "a",
		text: "resume after exhausted red checks",
		cwd: f.job().lane.worktreePath,
		resume: true,
	});
	expect(resumed).toMatchObject({ started: true });
	expect(f.job().state).toBe("running");
});

test("#481 awaiting_checks: past one hour on the injected clock the lane settles with a timeout", async () => {
	let now = Date.parse("2026-10-13T00:00:00.000Z");
	const checks = scriptedChecks({ prNumber: 492, status: "pending", failingChecks: [] });
	const f = await fixture({ now: () => now, checkStatusClient: checks.client, ownerTarget: () => origin });
	await awaitingLane(f, "fix/timeout");
	now += 3_600_000 - 1;
	await pollTimes(f, 2);
	expect(f.job().state).toBe("awaiting_checks");
	now += 1;
	await pollUntil(f, () => f.job().state !== "awaiting_checks");
	const job = f.job();
	expect(job.state).toBe("attempt_ended");
	expect(job.escalations.at(-1)).toContain("awaiting_checks ended: timeout after 60m");
	expect(checksNotice(f, "[lane a] attempt_ended: awaiting_checks timeout")).toContain("pr=492");
	expect(f.port.sends).toHaveLength(1);
});

test("#481 awaiting_checks: a closed PR settles the lane with pr_closed", async () => {
	const checks = scriptedChecks({ prNumber: 493, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	await awaitingLane(f, "fix/pr-closed");
	checks.set({ prNumber: 493, status: "pending", failingChecks: [], prState: "closed", merged: false });
	await pollUntil(f, () => f.job().state !== "awaiting_checks");
	const job = f.job();
	expect(job.state).toBe("attempt_ended");
	expect(job.escalations.at(-1)).toContain("awaiting_checks ended: pr_closed pr=493");
	expect(checksNotice(f, "[lane a] attempt_ended: awaiting_checks pr_closed")).toContain("pr=493");
	expect(f.port.sends).toHaveLength(1);
});

test("#481 awaiting_checks: a HEAD other than the recorded one settles the lane with head_moved", async () => {
	const checks = scriptedChecks({ prNumber: 494, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const { head } = await awaitingLane(f, "fix/head-moved");
	const env = {
		...process.env,
		GIT_COMMITTER_NAME: "t",
		GIT_COMMITTER_EMAIL: "t@t",
		GIT_AUTHOR_NAME: "t",
		GIT_AUTHOR_EMAIL: "t@t",
	};
	for (const args of [
		["commit", "-q", "--allow-empty", "-m", "outside push"],
		["push", "-q", "origin", "fix/head-moved"],
	])
		expect(Bun.spawnSync(["git", "-C", f.directory, ...args], { env }).exitCode).toBe(0);
	const moved = Bun.spawnSync(["git", "-C", f.directory, "rev-parse", "HEAD"]).stdout.toString().trim();
	expect(moved).not.toBe(head);
	await pollUntil(f, () => f.job().state !== "awaiting_checks");
	const job = f.job();
	expect(job.state).toBe("attempt_ended");
	expect(job.escalations.at(-1)).toContain(
		`awaiting_checks ended: head_moved from=${head.slice(0, 12)} to=${moved.slice(0, 12)}`,
	);
	expect(checksNotice(f, "[lane a] attempt_ended: awaiting_checks head_moved")).toContain("pr=494");
	expect(f.port.sends).toHaveLength(1);
});

test("#481 awaiting_checks binds a green rollup to the remote PR head, not the stale local checkout", async () => {
	const checks = scriptedChecks({ prNumber: 496, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const { head } = await awaitingLane(f, "fix/remote-head-moved");
	const branch = Bun.spawnSync(["git", "-C", f.job().lane.worktreePath, "branch", "--show-current"])
		.stdout.toString()
		.trim();
	const env = {
		...process.env,
		GIT_AUTHOR_NAME: "test",
		GIT_AUTHOR_EMAIL: "test@test",
		GIT_COMMITTER_NAME: "test",
		GIT_COMMITTER_EMAIL: "test@test",
	};
	const worktree = f.job().lane.worktreePath;
	expect(
		Bun.spawnSync(["git", "-C", worktree, "commit", "-q", "--allow-empty", "-m", "move PR head"], { env }).exitCode,
	).toBe(0);
	const remoteHead = Bun.spawnSync(["git", "-C", worktree, "rev-parse", "HEAD"]).stdout.toString().trim();
	expect(Bun.spawnSync(["git", "-C", worktree, "push", "-q", "origin", branch]).exitCode).toBe(0);
	expect(Bun.spawnSync(["git", "-C", worktree, "reset", "-q", "--hard", head]).exitCode).toBe(0);
	checks.set({ prNumber: 496, headSha: remoteHead, status: "green", failingChecks: [] });
	await pollUntil(f, () => f.job().state !== "awaiting_checks");
	expect(f.job().state).toBe("attempt_ended");
	expect(f.job().awaitingChecks).toBeUndefined();
	expect(f.job().escalations.at(-1)).toContain(
		`awaiting_checks ended: head_moved from=${head.slice(0, 12)} to=${remoteHead.slice(0, 12)}`,
	);
	expect(checksNotice(f, "[lane a] attempt_ended: awaiting_checks head_moved")).toContain("pr=496");
	expect(f.port.sends).toHaveLength(1);
});

test("#481 waits when GitHub check metadata still reports the prior head", async () => {
	const checks = scriptedChecks({ prNumber: 501, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const { head } = await awaitingLane(f, "fix/gh-head-lag");
	checks.set({ prNumber: 501, headSha: "0".repeat(40), status: "green", failingChecks: [] });
	await pollTimes(f, 3);
	expect(f.job().state).toBe("awaiting_checks");
	expect(f.port.sends).toHaveLength(1);
	expect(checksNotice(f, "[lane a] checks passed")).toBeUndefined();
	checks.set({ prNumber: 501, headSha: head, status: "green", failingChecks: [] });
	await pollUntil(f, () => f.job().state === "done");
});

test("#481 a merged PR settles successfully even when its source branch was deleted", async () => {
	const checks = scriptedChecks({ prNumber: 502, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const branch = "fix/merged-source-deleted";
	const { head } = await awaitingLane(f, branch);
	expect(Bun.spawnSync(["git", "-C", f.directory, "push", "-q", "origin", "--delete", branch]).exitCode).toBe(0);
	checks.set({ prNumber: 502, headSha: head, status: "pending", failingChecks: [], prState: "closed", merged: true });
	await pollUntil(f, () => f.job().state === "done");
	expect(checksNotice(f, "[lane a] checks passed")).toContain("pr=502");
});

test("#481 initial settlement discovers a merged PR whose source branch was already deleted", async () => {
	const checks = scriptedChecks({
		prNumber: 504,
		status: "pending",
		failingChecks: [],
		prState: "closed",
		merged: true,
	});
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const branch = "fix/deleted-before-settle";
	const head = await laneRepo(f.directory, branch, true);
	const result = await started(f, "a", origin);
	expect(Bun.spawnSync(["git", "-C", f.directory, "push", "-q", "origin", "--delete", branch]).exitCode).toBe(0);
	f.port.complete(result.opRef, "merged deliverable");
	await pollUntil(f, () => f.job().state === "done");
	expect(f.job().attempts[0]?.opRef).toBe(result.opRef);
	expect(checksNotice(f, "[lane a] checks passed")).toContain(`head=${head.slice(0, 12)} pr=504`);
});

test("#481 an exact merged PR snapshot settles before later source-branch movement", async () => {
	const checks = scriptedChecks({ prNumber: 503, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const branch = "fix/merged-source-advanced";
	const { head } = await awaitingLane(f, branch);
	const env = {
		...process.env,
		GIT_AUTHOR_NAME: "test",
		GIT_AUTHOR_EMAIL: "test@test",
		GIT_COMMITTER_NAME: "test",
		GIT_COMMITTER_EMAIL: "test@test",
	};
	const advanced = Bun.spawnSync(
		["git", "-C", f.directory, "commit", "-q", "--allow-empty", "-m", "reuse merged branch"],
		{ env, stdout: "pipe", stderr: "pipe" },
	);
	expect(advanced.exitCode).toBe(0);
	const advancedHead = Bun.spawnSync(["git", "-C", f.directory, "rev-parse", "HEAD"]).stdout.toString().trim();
	expect(Bun.spawnSync(["git", "-C", f.directory, "push", "-q", "origin", branch]).exitCode).toBe(0);
	expect(Bun.spawnSync(["git", "-C", f.directory, "reset", "-q", "--hard", head]).exitCode).toBe(0);
	checks.set({ prNumber: 503, headSha: head, status: "pending", failingChecks: [], prState: "closed", merged: true });
	await pollUntil(f, () => f.job().state === "done");
	expect(checks.calls.at(-1)?.headSha).toBe(head);
	expect(checks.requestedPrNumbers.at(-1)).toBe(503);
	expect(checksNotice(f, "[lane a] checks passed")).toContain(
		`pr=503 checks=green prHead=${head.slice(0, 12)} remoteHead=${advancedHead.slice(0, 12)}`,
	);
});

test("#481 an exact merged PR snapshot wins over later local HEAD movement", async () => {
	const checks = scriptedChecks({ prNumber: 505, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const branch = "fix/merged-local-advanced";
	const { head } = await awaitingLane(f, branch);
	const env = {
		...process.env,
		GIT_AUTHOR_NAME: "test",
		GIT_AUTHOR_EMAIL: "test@test",
		GIT_COMMITTER_NAME: "test",
		GIT_COMMITTER_EMAIL: "test@test",
	};
	const advanced = Bun.spawnSync(
		["git", "-C", f.directory, "commit", "-q", "--allow-empty", "-m", "unrelated local commit"],
		{ env, stdout: "pipe", stderr: "pipe" },
	);
	expect(advanced.exitCode).toBe(0);
	const advancedHead = Bun.spawnSync(["git", "-C", f.directory, "rev-parse", "HEAD"]).stdout.toString().trim();
	expect(advancedHead).not.toBe(head);
	checks.set({ prNumber: 505, headSha: head, status: "pending", failingChecks: [], prState: "closed", merged: true });
	await pollUntil(f, () => f.job().state === "done");
	expect(checksNotice(f, "[lane a] checks passed")).toContain(`head=${head.slice(0, 12)} pr=505`);
});

test("#481 green checks cannot settle after the remote branch advances beyond the local head", async () => {
	const checks = scriptedChecks({ prNumber: 499, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const { head } = await awaitingLane(f, "fix/remote-ref-moved");
	const worktree = f.job().lane.worktreePath;
	const branch = Bun.spawnSync(["git", "-C", worktree, "branch", "--show-current"]).stdout.toString().trim();
	const env = {
		...process.env,
		GIT_AUTHOR_NAME: "test",
		GIT_AUTHOR_EMAIL: "test@test",
		GIT_COMMITTER_NAME: "test",
		GIT_COMMITTER_EMAIL: "test@test",
	};
	const commit = Bun.spawnSync(["git", "-C", worktree, "commit", "-q", "--allow-empty", "-m", "advance remote"], {
		env,
		stdout: "pipe",
		stderr: "pipe",
	});
	expect(commit.exitCode).toBe(0);
	const remoteHead = Bun.spawnSync(["git", "-C", worktree, "rev-parse", "HEAD"]).stdout.toString().trim();
	expect(Bun.spawnSync(["git", "-C", worktree, "push", "-q", "origin", branch]).exitCode).toBe(0);
	expect(Bun.spawnSync(["git", "-C", worktree, "reset", "-q", "--hard", head]).exitCode).toBe(0);
	checks.set({ prNumber: 499, headSha: head, status: "green", failingChecks: [] });
	await pollUntil(f, () => f.job().state !== "awaiting_checks");
	expect(f.job().state).toBe("attempt_ended");
	expect(f.job().escalations.at(-1)).toContain(
		`awaiting_checks ended: head_moved from=${head.slice(0, 12)} to=${remoteHead.slice(0, 12)}`,
	);
	expect(checksNotice(f, "[lane a] attempt_ended: awaiting_checks head_moved")).toContain(
		`prHead=${head.slice(0, 12)} remoteHead=${remoteHead.slice(0, 12)}`,
	);
});

test("#481 checks on the old PR head cannot settle a new local commit that was not pushed", async () => {
	const checks = scriptedChecks({ prNumber: 497, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	const { head } = await awaitingLane(f, "fix/unpushed-new-head");
	const env = {
		...process.env,
		GIT_COMMITTER_NAME: "t",
		GIT_COMMITTER_EMAIL: "t@t",
		GIT_AUTHOR_NAME: "t",
		GIT_AUTHOR_EMAIL: "t@t",
	};
	expect(
		Bun.spawnSync(["git", "-C", f.directory, "commit", "-q", "--allow-empty", "-m", "local only"], { env }).exitCode,
	).toBe(0);
	const localHead = Bun.spawnSync(["git", "-C", f.directory, "rev-parse", "HEAD"]).stdout.toString().trim();
	checks.set({ prNumber: 497, headSha: head, status: "green", failingChecks: [] });
	await pollUntil(f, () => f.job().state !== "awaiting_checks");
	expect(f.job().state).toBe("attempt_ended");
	expect(f.job().escalations.at(-1)).toContain(
		`awaiting_checks ended: head_moved from=${head.slice(0, 12)} to=${localHead.slice(0, 12)}`,
	);
	expect(f.port.sends).toHaveLength(1);
});

test("#481 awaiting_checks: checks temporarily unavailable keep the lane waiting", async () => {
	const checks = scriptedChecks({ prNumber: 495, status: "pending", failingChecks: [] });
	const f = await fixture({ checkStatusClient: checks.client, ownerTarget: () => origin });
	await awaitingLane(f, "fix/checks-unavailable");
	const before = f.job();
	const lookups = checks.calls.length;
	const notices = f.notices.length;
	checks.set(undefined);
	await pollTimes(f, 3);
	expect(checks.calls.length).toBeGreaterThan(lookups);
	const job = f.job();
	expect(job.state).toBe("awaiting_checks");
	expect(job.awaitingChecks).toEqual(before.awaitingChecks);
	expect(job.escalations).toEqual(before.escalations);
	expect(f.port.sends).toHaveLength(1);
	expect(f.notices).toHaveLength(notices);
});

test("#407: laneLastCommits reads each distinct worktree once, concurrently, null for non-repos", async () => {
	const repo = await mkdtemp(join(tmpdir(), "lane-commit-"));
	const missing = join(repo, "does-not-exist");
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", repo, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "t",
					GIT_AUTHOR_EMAIL: "t@t",
					GIT_COMMITTER_NAME: "t",
					GIT_COMMITTER_EMAIL: "t@t",
				},
			});
		git("init", "-q");
		git("commit", "-q", "--allow-empty", "-m", "lane head");
		const spawn = spyOn(Bun, "spawn");
		const commits = await laneLastCommits([repo, missing, repo, repo], 2);
		expect(spawn).toHaveBeenCalledTimes(2);
		spawn.mockRestore();
		expect(commits.get(repo)?.subject).toBe("lane head");
		expect(commits.get(repo)?.sha).toMatch(/^[0-9a-f]{40}$/);
		expect(commits.get(missing)).toBeNull();
	} finally {
		await rm(repo, { recursive: true, force: true });
	}
});

test("work.start with repo creates an encoded worktree path from a bare git repo", async () => {
	// Create a bare git repo to test against
	const repoDir = await mkdtemp(join(tmpdir(), "test-repo-"));
	const bareRepo = await mkdtemp(join(tmpdir(), "bare-repo-"));
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", repoDir, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "test",
					GIT_AUTHOR_EMAIL: "test@test",
					GIT_COMMITTER_NAME: "test",
					GIT_COMMITTER_EMAIL: "test@test",
				},
			});

		// Initialize a regular repo
		git("init", "-q");
		git("config", "user.name", "test");
		git("config", "user.email", "test@test");
		git("commit", "-q", "--allow-empty", "-m", "initial commit");

		// Create a bare clone
		Bun.spawnSync(["git", "clone", "--bare", repoDir, bareRepo], {
			stdout: "ignore",
			stderr: "ignore",
		});

		const f = await fixture();
		const laneRoot = f.gajaewayHome;
		const result = await f.manager.start({
			name: "worktree-test",
			text: "work",
			cwd: f.directory,
			repo: bareRepo,
		});

		expect(result.started).toBe(true);
		const job = f.job("worktree-test");
		expect(job.lane.worktreePath).toBe(managedLaneWorktreePath(laneRoot, "worktree-test"));
		expect(job.lane.worktreePath.startsWith(join(laneRoot, "lanes"))).toBe(true);
		expect(job.lane.repository).toBe(bareRepo);
		expect(job.lane.repositoryPath).toBe(managedLaneRepositoryPath(laneRoot, "worktree-test"));

		// Verify worktree was created with proper branch
		const branchResult = Bun.spawnSync(["git", "-C", job.lane.worktreePath, "rev-parse", "--abbrev-ref", "HEAD"], {
			stdout: "pipe",
		});
		expect(branchResult.exitCode).toBe(0);
		const branch = branchResult.stdout?.toString().trim();
		expect(branch).toBe(managedLaneBranchName("worktree-test"));
		const originUrl = Bun.spawnSync(["git", "-C", job.lane.worktreePath, "remote", "get-url", "origin"], {
			stdout: "pipe",
		});
		expect(originUrl.stdout?.toString().trim()).toBe(repoDir);
	} finally {
		await rm(repoDir, { recursive: true, force: true });
		await rm(bareRepo, { recursive: true, force: true });
	}
});

test("case-distinct lane names create distinct encoded worktree directories", async () => {
	const { bareRepo, cleanup } = await bareRepoFixture("case-distinct-worktrees");
	try {
		const f = await fixture();
		for (const name of ["Foo", "foo"]) {
			const result = await f.manager.start({ name, text: "work", repo: bareRepo });
			if (!result.started) throw new Error(`lane ${name} did not start`);
			expect(f.job(name).lane.worktreePath).toBe(managedLaneWorktreePath(f.gajaewayHome, name));
		}
		expect(f.job("Foo").lane.worktreePath).not.toBe(f.job("foo").lane.worktreePath);
	} finally {
		await cleanup();
	}
});

test("work.start normalizes a nested local repository path to its checkout root", async () => {
	const { repoDir, cleanup } = await bareRepoFixture("nested-repo-path");
	try {
		const nestedPath = join(repoDir, "src");
		await mkdir(nestedPath);
		const f = await fixture();
		const name = "nested-repo-path";
		const result = await f.manager.start({ name, text: "work", repo: nestedPath });
		if (!result.started) throw new Error("work did not start");
		const job = f.job(name);
		const sourceHead = Bun.spawnSync(["git", "-C", repoDir, "rev-parse", "HEAD"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(sourceHead.exitCode).toBe(0);
		expect(job.baselineSha).toBe(sourceHead.stdout?.toString().trim());
		expect(job.lane.repository).toBe(nestedPath);
		const originUrl = Bun.spawnSync(["git", "-C", job.lane.worktreePath, "remote", "get-url", "origin"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(originUrl.stdout?.toString().trim()).toBe(repoDir);
	} finally {
		await cleanup();
	}
});

test("work.start with invalid repo throws invalid_params with git stderr", async () => {
	const f = await fixture();
	const invalidRepo = "/nonexistent/repo";

	await expect(
		f.manager.start({
			name: "bad-repo",
			text: "work",
			cwd: f.directory,
			repo: invalidRepo,
		}),
	).rejects.toMatchObject({
		code: "invalid_params",
		detail: {
			reasonCode: "repo_not_git",
		},
	});

	// Verify no worktree was created
	const job = f.db.work.laneJobJson(laneJobIdentity("bad-repo").jobId);
	expect(job).toBeUndefined();
});

test("work.start with special characters in lane name uses hex-encoded branch name", async () => {
	const { bareRepo, cleanup } = await bareRepoFixture("special-char-lane-name");
	try {
		const f = await fixture();
		const name = "bad..name";
		const result = await f.manager.start({ name, text: "work", repo: bareRepo });
		if (!result.started) throw new Error("work did not start");
		const job = f.job(name);
		const branchResult = Bun.spawnSync(
			["git", "-C", job.lane.repositoryPath!, "show-ref", "--verify", `refs/heads/${managedLaneBranchName(name)}`],
			{ stdout: "pipe", stderr: "pipe" },
		);
		expect(branchResult.exitCode).toBe(0);
	} finally {
		await cleanup();
	}
});

test("repo-bound work.start retains its created worktree after bind failure so retry reuses it", async () => {
	const repoDir = await mkdtemp(join(tmpdir(), "test-repo-bind-retry-"));
	const bareRepo = await mkdtemp(join(tmpdir(), "bare-repo-bind-retry-"));
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", repoDir, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "test",
					GIT_AUTHOR_EMAIL: "test@test",
					GIT_COMMITTER_NAME: "test",
					GIT_COMMITTER_EMAIL: "test@test",
				},
			});
		git("init", "-q");
		git("config", "user.name", "test");
		git("config", "user.email", "test@test");
		git("commit", "-q", "--allow-empty", "-m", "initial commit");
		Bun.spawnSync(["git", "clone", "--bare", repoDir, bareRepo], { stdout: "ignore", stderr: "ignore" });

		const f = await fixture();
		const bind = f.port.bind.bind(f.port);
		let failFirstBind = true;
		f.port.bind = async (input) => {
			if (failFirstBind) {
				failFirstBind = false;
				throw new GjcCliUnavailableError("broker unavailable");
			}
			return await bind(input);
		};
		const input = { name: "bind-retry", text: "work", cwd: f.directory, repo: bareRepo };

		await expect(f.manager.start(input)).rejects.toMatchObject({
			code: "verb_failed",
			detail: { reasonCode: "bind_failed", cause: "broker_unavailable" },
		});
		const worktreePath = join(f.gajaewayHome, "lanes", Buffer.from("bind-retry", "utf8").toString("hex"));
		const retainedWorktree = Bun.spawnSync(["git", "-C", worktreePath, "rev-parse", "--show-toplevel"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(retainedWorktree.exitCode).toBe(0);
		expect(retainedWorktree.stdout?.toString().trim()).toBe(worktreePath);
		expect(f.job("bind-retry")).toMatchObject({ lane: { worktreePath }, attempts: [] });

		const retried = await f.manager.start(input);
		expect(retried.started).toBe(true);
		expect(f.job("bind-retry").lane.worktreePath).toBe(worktreePath);
	} finally {
		await rm(repoDir, { recursive: true, force: true });
		await rm(bareRepo, { recursive: true, force: true });
	}
});

test("work.start with repo and base parameter creates worktree from specified base", async () => {
	const repoDir = await mkdtemp(join(tmpdir(), "test-repo-base-"));
	const bareRepo = await mkdtemp(join(tmpdir(), "bare-repo-base-"));
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", repoDir, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "test",
					GIT_AUTHOR_EMAIL: "test@test",
					GIT_COMMITTER_NAME: "test",
					GIT_COMMITTER_EMAIL: "test@test",
				},
			});

		// Initialize a regular repo with branches
		git("init", "-q");
		git("config", "user.name", "test");
		git("config", "user.email", "test@test");
		git("commit", "-q", "--allow-empty", "-m", "initial commit");
		git("checkout", "-q", "-b", "dev");
		git("commit", "-q", "--allow-empty", "-m", "dev commit");

		// Create a bare clone
		Bun.spawnSync(["git", "clone", "--bare", repoDir, bareRepo], {
			stdio: ["ignore", "ignore", "ignore"],
		});

		const f = await fixture();
		const result = await f.manager.start({
			name: "base-test",
			text: "work",
			cwd: f.directory,
			repo: bareRepo,
			base: "dev",
		});

		expect(result.started).toBe(true);
		const job = f.job("base-test");
		expect(job.lane.repository).toBe(bareRepo);

		// Verify worktree was created from dev branch
		const logResult = Bun.spawnSync(["git", "-C", job.lane.worktreePath, "log", "-1", "--format=%s"], {
			stdout: "pipe",
		});
		expect(logResult.stdout?.toString().trim()).toBe("dev commit");
	} finally {
		await rm(repoDir, { recursive: true, force: true });
		await rm(bareRepo, { recursive: true, force: true });
	}
});

test("issue #480: operator retire cleans up worktree and branch", async () => {
	// Create a bare git repo to test against
	const repoDir = await mkdtemp(join(tmpdir(), "test-repo-retire-"));
	const bareRepo = await mkdtemp(join(tmpdir(), "bare-repo-retire-"));
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", repoDir, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "test",
					GIT_AUTHOR_EMAIL: "test@test",
					GIT_COMMITTER_NAME: "test",
					GIT_COMMITTER_EMAIL: "test@test",
				},
			});

		git("init", "-q");
		git("config", "user.name", "test");
		git("config", "user.email", "test@test");
		git("commit", "-q", "--allow-empty", "-m", "initial");

		Bun.spawnSync(["git", "clone", "--bare", repoDir, bareRepo], {
			stdout: "ignore",
			stderr: "ignore",
		});

		const f = await fixture();
		const result = await f.manager.start({
			name: "retire-test",
			text: "work",
			cwd: f.directory,
			repo: bareRepo,
		});
		if (!result.started) throw new Error("work did not start");

		const job = f.job("retire-test");
		const worktreePath = job.lane.worktreePath;
		expect(worktreePath).toBeDefined();

		// Verify worktree exists
		const existsBefore = Bun.spawnSync(["git", "-C", worktreePath, "rev-parse", "--git-dir"], {
			stdout: "pipe",
		});
		expect(existsBefore.exitCode).toBe(0);

		// Complete the work and retire the lane
		f.port.complete(result.opRef, "test complete");
		await until(() => f.db.work.workAttemptOpen().length === 0);

		const retireOutcome = await f.lanes.retire("retire-test", "operator");
		expect(retireOutcome.retired).toBe(true);

		// Verify worktree was deleted
		const exitsAfter = Bun.spawnSync(["git", "-C", worktreePath, "rev-parse", "--git-dir"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(exitsAfter.exitCode).not.toBe(0);

		// Verify branch was deleted
		const branchCheck = Bun.spawnSync(["git", "-C", bareRepo, "show-ref", "work/retire-test"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(branchCheck.exitCode).not.toBe(0);
	} finally {
		await rm(repoDir, { recursive: true, force: true });
		await rm(bareRepo, { recursive: true, force: true });
	}
});

test("issue #480: cleanup failure on operator retire fails the retirement", async () => {
	const repoDir = await mkdtemp(join(tmpdir(), "test-repo-cleanup-fail-"));
	const bareRepo = await mkdtemp(join(tmpdir(), "bare-repo-cleanup-fail-"));
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", repoDir, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "test",
					GIT_AUTHOR_EMAIL: "test@test",
					GIT_COMMITTER_NAME: "test",
					GIT_COMMITTER_EMAIL: "test@test",
				},
			});

		git("init", "-q");
		git("config", "user.name", "test");
		git("config", "user.email", "test@test");
		git("commit", "-q", "--allow-empty", "-m", "initial");

		Bun.spawnSync(["git", "clone", "--bare", repoDir, bareRepo], {
			stdout: "ignore",
			stderr: "ignore",
		});

		const f = await fixture();
		const result = await f.manager.start({
			name: "cleanup-fail",
			text: "work",
			cwd: f.directory,
			repo: bareRepo,
		});
		expect(result.started).toBe(true);

		if (!result.started) throw new Error("work did not start");
		const job = f.job("cleanup-fail");
		const repository = job.lane.repositoryPath;

		// Complete the work
		f.port.complete(result.opRef, "test complete");
		await until(() => f.db.work.workAttemptOpen().length === 0);

		// Remove the managed bare repository so git cannot unlink its registered worktree.
		if (repository) await rm(repository, { recursive: true, force: true });

		// Attempt to retire should fail due to cleanup failure
		const retireOutcome = await f.lanes.retire("cleanup-fail", "operator");
		expect(retireOutcome.retired).toBe(false);
		if (!retireOutcome.retired) {
			expect(retireOutcome.reason).toContain("worktree cleanup failed");
		}
	} finally {
		await rm(repoDir, { recursive: true, force: true });
		await rm(bareRepo, { recursive: true, force: true });
	}
});

test("issue #480: session binding uses worktree path, not input cwd", async () => {
	const repoDir = await mkdtemp(join(tmpdir(), "test-repo-session-bind-"));
	const bareRepo = await mkdtemp(join(tmpdir(), "bare-repo-session-bind-"));
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", repoDir, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "test",
					GIT_AUTHOR_EMAIL: "test@test",
					GIT_COMMITTER_NAME: "test",
					GIT_COMMITTER_EMAIL: "test@test",
				},
			});

		git("init", "-q");
		git("config", "user.name", "test");
		git("config", "user.email", "test@test");
		git("commit", "-q", "--allow-empty", "-m", "initial");

		Bun.spawnSync(["git", "clone", "--bare", repoDir, bareRepo], {
			stdout: "ignore",
			stderr: "ignore",
		});

		// Create fixture first, then set up bind tracking
		const f = await fixture();
		const bindCalls: SessionBindInput[] = [];
		// Spy on port.bind to capture the input
		const originalBind = f.port.bind.bind(f.port);
		f.port.bind = async (input: SessionBindInput) => {
			bindCalls.push(input);
			return originalBind(input);
		};

		const result = await f.manager.start({
			name: "session-bind-test",
			text: "work",
			cwd: f.directory, // This is the input cwd
			repo: bareRepo,
		});

		expect(result.started).toBe(true);
		const job = f.job("session-bind-test");
		const worktreePath = job.lane.worktreePath;

		// Verify that the bind call received the worktree path, not the input cwd
		const bindCall = bindCalls[bindCalls.length - 1]; // Last bind call should be for our lane
		expect(bindCall?.repo).toBe(worktreePath);
		expect(bindCall?.repo).not.toBe(f.directory);
	} finally {
		await rm(repoDir, { recursive: true, force: true });
		await rm(bareRepo, { recursive: true, force: true });
	}
});

test("issue #480: a lane cwd inside the service checkout is refused", async () => {
	const f = await fixture();
	const inside = join(f.serviceDirectory, "packages", "gateway");
	await mkdir(inside, { recursive: true });
	for (const cwd of [f.serviceDirectory, inside, join(f.serviceDirectory, "not-yet-created")]) {
		await expect(f.manager.start({ name: "a", text: "work", cwd })).rejects.toMatchObject({
			code: "invalid_params",
			detail: { reasonCode: "cwd_inside_service_dir", name: "a" },
		});
	}
	expect(f.port.binds).toHaveLength(0);
	expect(f.port.sends).toHaveLength(0);
});

test("issue #480: a generated worktree inside the service checkout is refused", async () => {
	const { bareRepo, cleanup } = await bareRepoFixture("svc-worktree");
	try {
		const f = await fixture();
		const manager = await f.restart({ laneRoot: join(f.serviceDirectory, "lanes-root") });
		await expect(manager.start({ name: "a", text: "work", cwd: f.directory, repo: bareRepo })).rejects.toMatchObject({
			code: "invalid_params",
			detail: { reasonCode: "cwd_inside_service_dir" },
		});
	} finally {
		await cleanup();
	}
});

test("issue #480: re-starting a repo-bound lane is matched by repository, not by the caller cwd", async () => {
	const { bareRepo, cleanup } = await bareRepoFixture("rebind");
	try {
		const f = await fixture();
		const first = await f.manager.start({ name: "a", text: "work", cwd: f.directory, repo: bareRepo });
		expect(first.started).toBe(true);
		// Same repository from the same caller cwd: the lane is recognized (its attempt is still
		// open), not rejected as a cwd mismatch against the generated worktree path.
		await expect(f.manager.start({ name: "a", text: "again", cwd: f.directory, repo: bareRepo })).rejects.toMatchObject(
			{ detail: { reasonCode: "attempt_open" } },
		);
		// A different repository for the same lane name is a mismatch.
		await expect(
			f.manager.start({ name: "a", text: "again", cwd: f.directory, repo: `${bareRepo}-other` }),
		).rejects.toMatchObject({ detail: { reasonCode: "lane_repository_mismatch" } });
	} finally {
		await cleanup();
	}
});

test("issue #480: the production service directory is this source checkout, not the bun runtime", () => {
	const root = Bun.spawnSync(["git", "-C", import.meta.dir, "rev-parse", "--show-toplevel"])
		.stdout.toString()
		.trim();
	expect(getServiceDirectory()).toBe(root);
	expect(getServiceDirectory()).not.toBe(dirname(process.execPath));
});

test("#439 work-lane status reads use relay when tail is attached to avoid CLI spawns", async () => {
	const f = await fixture();
	const result = await started(f);
	for (let i = 0; i < 5; i++) {
		const readsBefore = f.port.statusReads.filter((read) => read.opRef === result.opRef).length;
		f.port.emitTool(result.sessionId, { toolName: "bash" });
		await until(() => f.port.statusReads.filter((read) => read.opRef === result.opRef).length > readsBefore);
	}
	const statusReads = f.port.statusReads.filter((r) => r.opRef === result.opRef);
	expect(statusReads.length).toBeGreaterThanOrEqual(5);
	expect(statusReads.every((read) => read.relayUsed)).toBe(true);
});

test("#439 explicit work.status reads use the active observer relay", async () => {
	const f = await fixture({ pollMs: 60_000 });
	const result = await started(f);
	await until(() => f.port.statusReads.some((read) => read.opRef === result.opRef && read.relayUsed));
	const readsBefore = f.port.statusReads.length;
	const status = await f.manager.status({ name: "a" });
	expect(status.attempt?.opRef).toBe(result.opRef);
	const statusReads = f.port.statusReads.slice(readsBefore).filter((read) => read.opRef === result.opRef);
	expect(statusReads).toHaveLength(1);
	expect(statusReads[0]?.relayUsed).toBe(true);
});

test("#439 relay transport failure triggers exactly one CLI fallback per status call", async () => {
	const f = await fixture();
	const result = await started(f);
	const originalStatus = f.port.status.bind(f.port);
	// Track call sequence for specific opRef
	let relayCalls = 0;
	let fallbackCalls = 0;
	// Inject relay transport failure on first relay attempt
	f.port.status = async (input) => {
		const isRelay = input.relay !== undefined;
		if (input.opRef === result.opRef) {
			if (isRelay && relayCalls++ === 0) {
				throw new RelayClosedError(input.sessionId, "test relay failure");
			}
			if (!isRelay) {
				fallbackCalls++;
			}
		}
		return originalStatus(input);
	};
	// Trigger one status poll cycle
	f.port.emitTool(result.sessionId, { toolName: "bash" });
	await until(() => fallbackCalls > 0);
	// With relay failure, should have triggered both relay attempt and CLI fallback
	expect(relayCalls).toBeGreaterThanOrEqual(1);
	expect(fallbackCalls).toBe(1);
});

test("#439 relay non-transport failure is not caught as transport failure", async () => {
	const f = await fixture();
	const result = await started(f);
	const originalStatus = f.port.status.bind(f.port);
	// Track whether relay was called and if fallback was attempted
	let relayErrorThrown = false;
	let fallbackCalled = false;
	// Inject a non-transport error (regular Error, not RelayClosedError)
	f.port.status = async (input) => {
		const isRelay = input.relay !== undefined;
		if (input.opRef === result.opRef) {
			if (isRelay && !relayErrorThrown) {
				relayErrorThrown = true;
				// This error is NOT a transport failure (not RelayClosedError)
				throw new Error("non-transport relay error");
			}
			if (!isRelay) {
				fallbackCalled = true;
			}
		}
		return originalStatus(input);
	};
	// Trigger one status poll cycle
	f.port.emitTool(result.sessionId, { toolName: "bash" });
	await until(() => f.port.statusReads.some((read) => read.opRef === result.opRef));
	// Non-transport error should not trigger fallback
	expect(relayErrorThrown).toBe(true);
	expect(fallbackCalled).toBe(false);
	expect(f.port.statusReads.filter((read) => read.opRef === result.opRef).every((read) => read.relayUsed)).toBe(true);
});

test("work.start resolves origin bases from local remote-tracking refs", async () => {
	const { repoDir, bareRepo, cleanup } = await bareRepoFixture("remote-tracking-base");
	const env = {
		...process.env,
		GIT_AUTHOR_NAME: "test",
		GIT_AUTHOR_EMAIL: "test@test",
		GIT_COMMITTER_NAME: "test",
		GIT_COMMITTER_EMAIL: "test@test",
	};
	const git = (...args: string[]) => {
		const result = Bun.spawnSync(["git", "-C", repoDir, ...args], { env, stdout: "pipe", stderr: "pipe" });
		if (result.exitCode !== 0) throw new Error(`git ${args.join(" ")}: ${result.stderr.toString()}`);
		return result.stdout.toString().trim();
	};
	try {
		const defaultBranch = git("branch", "--show-current");
		git("remote", "add", "origin", bareRepo);
		git("push", "-q", "-u", "origin", defaultBranch);
		git("checkout", "-q", "-b", "dev");
		git("commit", "-q", "--allow-empty", "-m", "remote-only dev commit");
		git("push", "-q", "-u", "origin", "dev");
		const remoteDevSha = git("rev-parse", "refs/remotes/origin/dev");
		git("checkout", "-q", defaultBranch);
		git("branch", "-D", "dev");
		const localDevBranch = Bun.spawnSync(["git", "-C", repoDir, "show-ref", "--verify", "refs/heads/dev"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(localDevBranch.exitCode).not.toBe(0);

		const f = await fixture();
		const result = await f.manager.start({
			name: "remote-tracking-base",
			text: "work",
			repo: repoDir,
			base: "origin/dev",
		});
		if (!result.started) throw new Error("work did not start");
		const job = f.job("remote-tracking-base");
		expect(job.baselineSha).toBe(remoteDevSha);
		expect(git("rev-parse", "refs/remotes/origin/dev")).toBe(remoteDevSha);
		const worktreeHead = Bun.spawnSync(["git", "-C", job.lane.worktreePath, "rev-parse", "HEAD"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(worktreeHead.stdout?.toString().trim()).toBe(remoteDevSha);
	} finally {
		await cleanup();
	}
});

test("work.start defaults to origin/HEAD when its branch exists only as a remote-tracking ref", async () => {
	const { repoDir, bareRepo, cleanup } = await bareRepoFixture("remote-default-base");
	const env = {
		...process.env,
		GIT_AUTHOR_NAME: "test",
		GIT_AUTHOR_EMAIL: "test@test",
		GIT_COMMITTER_NAME: "test",
		GIT_COMMITTER_EMAIL: "test@test",
	};
	const git = (...args: string[]) => {
		const result = Bun.spawnSync(["git", "-C", repoDir, ...args], { env, stdout: "pipe", stderr: "pipe" });
		if (result.exitCode !== 0) throw new Error(`git ${args.join(" ")}: ${result.stderr.toString()}`);
		return result.stdout.toString().trim();
	};
	try {
		const defaultBranch = git("branch", "--show-current");
		git("remote", "add", "origin", bareRepo);
		git("push", "-q", "-u", "origin", defaultBranch);
		git("remote", "set-head", "origin", defaultBranch);
		expect(git("symbolic-ref", "refs/remotes/origin/HEAD")).toBe(`refs/remotes/origin/${defaultBranch}`);
		const remoteDefaultSha = git("rev-parse", `refs/remotes/origin/${defaultBranch}`);
		const currentBranch = `${defaultBranch}-current`;
		git("checkout", "-q", "-b", currentBranch);
		git("commit", "-q", "--allow-empty", "-m", "current local branch");
		git("branch", "-D", defaultBranch);
		const deletedLocalDefault = Bun.spawnSync(
			["git", "-C", repoDir, "show-ref", "--verify", `refs/heads/${defaultBranch}`],
			{ env, stdout: "pipe", stderr: "pipe" },
		);
		expect(deletedLocalDefault.exitCode).not.toBe(0);

		const f = await fixture();
		const result = await f.manager.start({ name: "remote-default-base", text: "work", repo: repoDir });
		if (!result.started) throw new Error("work did not start");
		const job = f.job("remote-default-base");
		expect(job.baselineSha).toBe(remoteDefaultSha);
		expect(git("rev-parse", `refs/remotes/origin/${defaultBranch}`)).toBe(remoteDefaultSha);
		const managedDefaultSha = Bun.spawnSync(["git", "rev-parse", "--verify", `refs/heads/${defaultBranch}^{commit}`], {
			cwd: job.lane.repositoryPath,
			env,
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(managedDefaultSha.exitCode).toBe(0);
		expect(managedDefaultSha.stdout?.toString().trim()).toBe(remoteDefaultSha);
		const worktreeHead = Bun.spawnSync(["git", "-C", job.lane.worktreePath, "rev-parse", "HEAD"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(worktreeHead.stdout?.toString().trim()).toBe(remoteDefaultSha);
	} finally {
		await cleanup();
	}
});

test("issue #480: operator retire cleans up worktree and branch", async () => {
	// Create a bare git repo to test against
	const repoDir = await mkdtemp(join(tmpdir(), "test-repo-retire-"));
	const bareRepo = await mkdtemp(join(tmpdir(), "bare-repo-retire-"));
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", repoDir, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "test",
					GIT_AUTHOR_EMAIL: "test@test",
					GIT_COMMITTER_NAME: "test",
					GIT_COMMITTER_EMAIL: "test@test",
				},
			});

		git("init", "-q");
		git("config", "user.name", "test");
		git("config", "user.email", "test@test");
		git("commit", "-q", "--allow-empty", "-m", "initial");

		Bun.spawnSync(["git", "clone", "--bare", repoDir, bareRepo], {
			stdout: "ignore",
			stderr: "ignore",
		});

		const f = await fixture();
		const result = await f.manager.start({
			name: "retire-test",
			text: "work",
			cwd: f.directory,
			repo: bareRepo,
		});
		if (!result.started) throw new Error("work did not start");

		const job = f.job("retire-test");
		const worktreePath = job.lane.worktreePath;
		const repositoryPath = job.lane.repositoryPath!;
		expect(worktreePath).toBeDefined();
		const branchBefore = Bun.spawnSync(
			["git", "-C", repositoryPath, "show-ref", "--verify", `refs/heads/${managedLaneBranchName("retire-test")}`],
			{ stdout: "pipe", stderr: "pipe" },
		);
		expect(branchBefore.exitCode).toBe(0);

		// Verify worktree exists
		const existsBefore = Bun.spawnSync(["git", "-C", worktreePath, "rev-parse", "--git-dir"], {
			stdout: "pipe",
		});
		expect(existsBefore.exitCode).toBe(0);

		// Complete the work and retire the lane
		f.port.complete(result.opRef, "test complete");
		await until(() => f.db.work.workAttemptOpen().length === 0);

		const retireOutcome = await f.lanes.retire("retire-test", "operator");
		expect(retireOutcome.retired).toBe(true);

		// Verify worktree was deleted
		const exitsAfter = Bun.spawnSync(["git", "-C", worktreePath, "rev-parse", "--git-dir"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(exitsAfter.exitCode).not.toBe(0);

		// The per-lane bare repository is removed with the worktree and branch.
		expect(await Bun.file(join(repositoryPath, "HEAD")).exists()).toBe(false);
	} finally {
		await rm(repoDir, { recursive: true, force: true });
		await rm(bareRepo, { recursive: true, force: true });
	}
});

test("issue #480: cleanup failure on operator retire fails the retirement", async () => {
	const repoDir = await mkdtemp(join(tmpdir(), "test-repo-cleanup-fail-"));
	const bareRepo = await mkdtemp(join(tmpdir(), "bare-repo-cleanup-fail-"));
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", repoDir, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "test",
					GIT_AUTHOR_EMAIL: "test@test",
					GIT_COMMITTER_NAME: "test",
					GIT_COMMITTER_EMAIL: "test@test",
				},
			});

		git("init", "-q");
		git("config", "user.name", "test");
		git("config", "user.email", "test@test");
		git("commit", "-q", "--allow-empty", "-m", "initial");

		Bun.spawnSync(["git", "clone", "--bare", repoDir, bareRepo], {
			stdout: "ignore",
			stderr: "ignore",
		});

		const f = await fixture();
		const result = await f.manager.start({
			name: "cleanup-fail",
			text: "work",
			cwd: f.directory,
			repo: bareRepo,
		});
		expect(result.started).toBe(true);

		if (!result.started) throw new Error("work did not start");
		const job = f.job("cleanup-fail");
		const repository = job.lane.repositoryPath;

		// Complete the work
		f.port.complete(result.opRef, "test complete");
		await until(() => f.db.work.workAttemptOpen().length === 0);

		// Remove the managed bare repository so git cannot unlink its registered worktree.
		if (repository) await rm(repository, { recursive: true, force: true });

		// Attempt to retire should fail due to cleanup failure
		const retireOutcome = await f.lanes.retire("cleanup-fail", "operator");
		expect(retireOutcome.retired).toBe(false);
		if (!retireOutcome.retired) {
			expect(retireOutcome.reason).toContain("worktree cleanup failed");
		}
	} finally {
		await rm(repoDir, { recursive: true, force: true });
		await rm(bareRepo, { recursive: true, force: true });
	}
});

test("issue #480: operator retire refuses a persisted repository path outside its managed lane directory", async () => {
	const { bareRepo, cleanup } = await bareRepoFixture("repository-path-corruption");
	try {
		const f = await fixture();
		const name = "corrupt-repository-path";
		const result = await f.manager.start({ name, text: "work", repo: bareRepo });
		if (!result.started) throw new Error("work did not start");
		const managedRepositoryPath = f.job(name).lane.repositoryPath!;
		const unrelatedDirectory = join(f.directory, "unrelated-repository");
		await mkdir(unrelatedDirectory);
		const sentinelPath = join(unrelatedDirectory, "keep.txt");
		await Bun.write(sentinelPath, "preserve");

		f.port.complete(result.opRef, "test complete");
		await until(() => f.db.work.workAttemptOpen().length === 0);

		const job = f.job(name);
		const corrupted = { ...job, lane: { ...job.lane, repositoryPath: unrelatedDirectory } };
		f.db.transaction((tx) =>
			f.db.work.putLaneJob(tx, {
				jobId: corrupted.jobId,
				laneKey: `work-${name}`,
				state: corrupted.state,
				createdAt: corrupted.createdAt,
				updatedAt: corrupted.updatedAt,
				lane: { branch: corrupted.lane.branch, worktreePath: corrupted.lane.worktreePath },
				json: JSON.stringify(corrupted),
			}),
		);

		const retired = await f.lanes.retire(name, "operator");
		expect(retired.retired).toBe(false);
		if (!retired.retired) expect(retired.reason).toContain("worktree cleanup failed");
		expect(await Bun.file(sentinelPath).text()).toBe("preserve");
		expect(await Bun.file(join(managedRepositoryPath, "HEAD")).exists()).toBe(true);
	} finally {
		await cleanup();
	}
});

test("issue #480: operator retire tolerates an externally removed worktree", async () => {
	const { bareRepo, cleanup } = await bareRepoFixture("external-worktree-removal");
	try {
		const f = await fixture();
		const name = "external-removal";
		const result = await f.manager.start({ name, text: "work", cwd: f.directory, repo: bareRepo });
		if (!result.started) throw new Error("work did not start");
		const job = f.job(name);
		const repositoryPath = job.lane.repositoryPath!;

		f.port.complete(result.opRef, "test complete");
		await until(() => f.db.work.workAttemptOpen().length === 0);

		// Simulate an external rm -rf: the worktree directory disappears but Git
		// still has its administrative entry until `worktree prune` runs.
		await rm(job.lane.worktreePath, { recursive: true, force: true });
		const staleRegistration = Bun.spawnSync(["git", "worktree", "list", "--porcelain"], {
			cwd: repositoryPath,
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(staleRegistration.exitCode).toBe(0);
		const registeredPaths = staleRegistration.stdout
			?.toString()
			.split(/\r?\n/)
			.filter((line) => line.startsWith("worktree "))
			.map((line) => line.slice("worktree ".length).replace(/\\/g, "/"));
		expect(registeredPaths?.includes(managedLaneWorktreePath(f.gajaewayHome, name).replace(/\\/g, "/"))).toBe(true);

		const retired = await f.lanes.retire(name, "operator");
		expect(retired.retired).toBe(true);
		expect(await Bun.file(join(repositoryPath, "HEAD")).exists()).toBe(false);
	} finally {
		await cleanup();
	}
});

test("issue #480: force retire preserves worktree and branch for resume", async () => {
	const repoDir = await mkdtemp(join(tmpdir(), "test-repo-force-retire-"));
	const bareRepo = await mkdtemp(join(tmpdir(), "bare-repo-force-retire-"));
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", repoDir, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "test",
					GIT_AUTHOR_EMAIL: "test@test",
					GIT_COMMITTER_NAME: "test",
					GIT_COMMITTER_EMAIL: "test@test",
				},
			});

		git("init", "-q");
		git("config", "user.name", "test");
		git("config", "user.email", "test@test");
		git("commit", "-q", "--allow-empty", "-m", "initial");

		Bun.spawnSync(["git", "clone", "--bare", repoDir, bareRepo], {
			stdout: "ignore",
			stderr: "ignore",
		});

		const f = await fixture({ hostLostGraceMs: 20 });
		const result = await f.manager.start({
			name: "force-retire-test",
			text: "work",
			cwd: f.directory,
			repo: bareRepo,
		});
		expect(result.started).toBe(true);

		if (!result.started) throw new Error("work did not start");
		const job = f.job("force-retire-test");
		const worktreePath = job.lane.worktreePath;
		const repositoryPath = job.lane.repositoryPath!;
		const unpushedCommit = Bun.spawnSync(
			[
				"git",
				"-C",
				worktreePath,
				"-c",
				"user.name=test",
				"-c",
				"user.email=test@test",
				"commit",
				"--allow-empty",
				"-m",
				"unpushed work",
			],
			{ stdout: "pipe", stderr: "pipe" },
		);
		expect(unpushedCommit.exitCode).toBe(0);

		// Simulate host loss by making session unavailable and marking as dead
		const originalStatus = f.port.status;
		f.port.status = async () => {
			throw new Error("Session unavailable");
		};
		f.port.setSessionState(result.sessionId, { live: false });

		// Force retire must preserve the managed worktree and branch.
		const retireOutcome = await f.lanes.forceRetire("force-retire-test");
		expect(retireOutcome.retired).toBe(true);
		f.port.status = originalStatus;

		// The durable job record still points to this worktree; forced retirement
		// must not discard its unpushed branch or make the lane unusable.
		const existsAfter = Bun.spawnSync(["git", "-C", worktreePath, "rev-parse", "--git-dir"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(existsAfter.exitCode).toBe(0);
		const headSubject = Bun.spawnSync(["git", "-C", worktreePath, "log", "-1", "--format=%s"], { stdout: "pipe" });
		expect(headSubject.stdout?.toString().trim()).toBe("unpushed work");

		const branchCheck = Bun.spawnSync(
			["git", "-C", repositoryPath, "show-ref", "--verify", `refs/heads/${managedLaneBranchName("force-retire-test")}`],
			{
				stdout: "pipe",
				stderr: "pipe",
			},
		);
		expect(branchCheck.exitCode).toBe(0);

		const resumed = await f.manager.start({ name: "force-retire-test", text: "continue", resume: true });
		expect(resumed).toMatchObject({ started: true });
	} finally {
		await rm(repoDir, { recursive: true, force: true });
		await rm(bareRepo, { recursive: true, force: true });
	}
});

test("issue #480: session binding uses worktree path, not input cwd", async () => {
	const repoDir = await mkdtemp(join(tmpdir(), "test-repo-session-bind-"));
	const bareRepo = await mkdtemp(join(tmpdir(), "bare-repo-session-bind-"));
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", repoDir, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "test",
					GIT_AUTHOR_EMAIL: "test@test",
					GIT_COMMITTER_NAME: "test",
					GIT_COMMITTER_EMAIL: "test@test",
				},
			});

		git("init", "-q");
		git("config", "user.name", "test");
		git("config", "user.email", "test@test");
		git("commit", "-q", "--allow-empty", "-m", "initial");

		Bun.spawnSync(["git", "clone", "--bare", repoDir, bareRepo], {
			stdout: "ignore",
			stderr: "ignore",
		});

		// Create fixture first, then set up bind tracking
		const f = await fixture();
		const bindCalls: SessionBindInput[] = [];
		// Spy on port.bind to capture the input
		const originalBind = f.port.bind.bind(f.port);
		f.port.bind = async (input: SessionBindInput) => {
			bindCalls.push(input);
			return originalBind(input);
		};

		const result = await f.manager.start({
			name: "session-bind-test",
			text: "work",
			cwd: f.directory, // This is the input cwd
			repo: bareRepo,
		});

		expect(result.started).toBe(true);
		const job = f.job("session-bind-test");
		const worktreePath = job.lane.worktreePath;

		// Verify that the bind call received the worktree path, not the input cwd
		const bindCall = bindCalls[bindCalls.length - 1]; // Last bind call should be for our lane
		expect(bindCall?.repo).toBe(worktreePath);
		expect(bindCall?.repo).not.toBe(f.directory);
	} finally {
		await rm(repoDir, { recursive: true, force: true });
		await rm(bareRepo, { recursive: true, force: true });
	}
});

test("issue #480: all-dead retirement preserves the worktree", async () => {
	const repoDir = await mkdtemp(join(tmpdir(), "test-repo-all-dead-"));
	const bareRepo = await mkdtemp(join(tmpdir(), "bare-repo-all-dead-"));
	try {
		const git = (...args: string[]) =>
			Bun.spawnSync(["git", "-C", repoDir, ...args], {
				env: {
					...process.env,
					GIT_AUTHOR_NAME: "test",
					GIT_AUTHOR_EMAIL: "test@test",
					GIT_COMMITTER_NAME: "test",
					GIT_COMMITTER_EMAIL: "test@test",
				},
			});

		git("init", "-q");
		git("config", "user.name", "test");
		git("config", "user.email", "test@test");
		git("commit", "-q", "--allow-empty", "-m", "initial");

		Bun.spawnSync(["git", "clone", "--bare", repoDir, bareRepo], {
			stdout: "ignore",
			stderr: "ignore",
		});

		const f = await fixture();
		const result = await f.manager.start({
			name: "all-dead-test",
			text: "work",
			cwd: f.directory,
			repo: bareRepo,
		});

		if (!result.started) throw new Error("work did not start");
		const job = f.job("all-dead-test");
		const worktreePath = job.lane.worktreePath;
		const repositoryPath = job.lane.repositoryPath!;

		// Verify worktree exists before force retire
		const existsBefore = Bun.spawnSync(["git", "-C", worktreePath, "rev-parse", "--git-dir"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(existsBefore.exitCode).toBe(0);

		// Make the session dead so it qualifies for all-dead retirement
		f.port.setSessionState(result.sessionId, { live: false });

		// Unattended all-dead retirement preserves the durable worktree.
		const retireOutcome = await f.lanes.forceRetire("all-dead-test");
		expect(retireOutcome.retired).toBe(true);

		const existsAfter = Bun.spawnSync(["git", "-C", worktreePath, "rev-parse", "--git-dir"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(existsAfter.exitCode).toBe(0);
		expect(await Bun.file(join(repositoryPath, "HEAD")).exists()).toBe(true);
	} finally {
		await rm(repoDir, { recursive: true, force: true });
		await rm(bareRepo, { recursive: true, force: true });
	}
});

async function bareRepoFixture(
	prefix: string,
): Promise<{ repoDir: string; bareRepo: string; cleanup: () => Promise<void> }> {
	let repoDir = await mkdtemp(join(tmpdir(), `${prefix}-src-`));
	let bareRepo = await mkdtemp(join(tmpdir(), `${prefix}-bare-`));
	// Normalize paths to resolve macOS /var -> /private/var symlink
	repoDir = await realpath(repoDir);
	bareRepo = await realpath(bareRepo);
	const env = {
		...process.env,
		GIT_AUTHOR_NAME: "test",
		GIT_AUTHOR_EMAIL: "test@test",
		GIT_COMMITTER_NAME: "test",
		GIT_COMMITTER_EMAIL: "test@test",
	};
	Bun.spawnSync(["git", "-C", repoDir, "init", "-q"], { env });
	Bun.spawnSync(["git", "-C", repoDir, "commit", "-q", "--allow-empty", "-m", "initial"], { env });
	Bun.spawnSync(["git", "clone", "-q", "--bare", repoDir, bareRepo], { env });
	return {
		repoDir,
		bareRepo,
		cleanup: async () => {
			await rm(repoDir, { recursive: true, force: true });
			await rm(bareRepo, { recursive: true, force: true });
		},
	};
}

test("issue #480: a lane cwd inside the service checkout is refused", async () => {
	const f = await fixture();
	const inside = join(f.serviceDirectory, "packages", "gateway");
	await mkdir(inside, { recursive: true });
	for (const cwd of [f.serviceDirectory, inside, join(f.serviceDirectory, "not-yet-created")]) {
		await expect(f.manager.start({ name: "a", text: "work", cwd })).rejects.toMatchObject({
			code: "invalid_params",
			detail: { reasonCode: "cwd_inside_service_dir", name: "a" },
		});
	}
	expect(f.port.binds).toHaveLength(0);
	expect(f.port.sends).toHaveLength(0);
});

test("issue #480: a generated worktree inside the service checkout is refused", async () => {
	const { bareRepo, cleanup } = await bareRepoFixture("svc-worktree");
	try {
		const f = await fixture();
		const manager = await f.restart({ laneRoot: join(f.serviceDirectory, "lanes-root") });
		await expect(manager.start({ name: "a", text: "work", cwd: f.directory, repo: bareRepo })).rejects.toMatchObject({
			code: "invalid_params",
			detail: { reasonCode: "cwd_inside_service_dir" },
		});
		expect(await Bun.file(managedLaneRepositoryPath(join(f.serviceDirectory, "lanes-root"), "a")).exists()).toBe(false);
	} finally {
		await cleanup();
	}
});

test("issue #480: a managed repository symlink into the service checkout is refused", async () => {
	const { bareRepo, cleanup } = await bareRepoFixture("svc-repository");
	try {
		const f = await fixture();
		const laneRoot = join(f.gajaewayHome, "lanes-root");
		await mkdir(laneRoot, { recursive: true });
		await symlink(f.serviceDirectory, join(laneRoot, "repositories"));
		const manager = await f.restart({ laneRoot });
		await expect(manager.start({ name: "a", text: "work", repo: bareRepo })).rejects.toMatchObject({
			code: "invalid_params",
			detail: { reasonCode: "cwd_inside_service_dir" },
		});
		expect(f.port.binds).toHaveLength(0);
		expect(await Bun.file(join(f.serviceDirectory, "a")).exists()).toBe(false);
	} finally {
		await cleanup();
	}
});

test("issue #480: re-starting a repo-bound lane is matched by repository, not by the caller cwd", async () => {
	const { bareRepo, cleanup } = await bareRepoFixture("rebind");
	try {
		const f = await fixture();
		const first = await f.manager.start({ name: "a", text: "work", cwd: f.directory, repo: bareRepo });
		expect(first.started).toBe(true);
		// Same repository from the same caller cwd: the lane is recognized (its attempt is still
		// open), not rejected as a cwd mismatch against the generated worktree path.
		await expect(f.manager.start({ name: "a", text: "again", cwd: f.directory, repo: bareRepo })).rejects.toMatchObject(
			{ detail: { reasonCode: "attempt_open" } },
		);
		// A different repository for the same lane name is a mismatch.
		await expect(
			f.manager.start({ name: "a", text: "again", cwd: f.directory, repo: `${bareRepo}-other` }),
		).rejects.toMatchObject({ detail: { reasonCode: "lane_repository_mismatch" } });
	} finally {
		await cleanup();
	}
});

test("issue #480: the production service directory is this source checkout, not the bun runtime", () => {
	const root = Bun.spawnSync(["git", "-C", import.meta.dir, "rev-parse", "--show-toplevel"])
		.stdout.toString()
		.trim();
	expect(getServiceDirectory()).toBe(root);
	expect(getServiceDirectory()).not.toBe(dirname(process.execPath));
});

test("issue #579: a compiled binary resolves the service directory from its own path, not the virtual module root", async () => {
	const dir = await mkdtemp(join(tmpdir(), "gw-579-"));
	try {
		const entry = join(dir, "entry.ts");
		await writeFile(
			entry,
			`import { getServiceDirectory } from ${JSON.stringify(join(import.meta.dir, "../src/orchestrator/work-lane"))};\nconsole.log(getServiceDirectory());\n`,
		);
		const outfile = join(dir, "probe");
		const build = Bun.spawnSync([process.execPath, "build", "--compile", entry, "--outfile", outfile]);
		expect(build.exitCode).toBe(0);
		const run = Bun.spawnSync([outfile], { cwd: dir });
		expect(run.exitCode).toBe(0);
		const resolved = run.stdout.toString().trim();
		expect(resolved).toBe(await realpath(dir));
		expect(() => assertOutsideServiceDirectory(resolved, join(tmpdir(), "elsewhere"), "probe")).not.toThrow();
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
}, 60_000);
