import {
	type BrokerSession,
	type CliResult,
	GjcCliError,
	OpRefRejectedError,
	type SendReceipt,
	type StatusReport,
} from "@gajae-gateway/subsession";
import type { GjcModelSelection, GjcServiceTier } from "../src/config";
import type { SessionRelayStream } from "../src/orchestrator/broker";
import type {
	RunningHostJob,
	SessionBindInput,
	SessionBinding,
	SessionPort,
	SessionRequestInput,
	SessionRequestResult,
	SessionSendInput,
	SessionSteerInput,
	TerminateHostOutcome,
	WorkerOutputInput,
	WorkerOutputResult,
} from "../src/orchestrator/session-port";
import { BrokerSessionPort, parseWorkerOutputResponse } from "../src/orchestrator/session-port";
import {
	RelayClosedError,
	type RelayRequestOptions,
	type RelayResponse,
	type TailAttachInput,
	type TailFrame,
	type TailHandle,
	TailRunner,
	type TailStreamSpawner,
	type TurnCorrelation,
} from "../src/orchestrator/tail-runner";
import type { BrokerAuthority, GatewayDatabase } from "../src/store/db";

/** Initializes only a fresh test DB; never adopts legacy bindings or writes a GJC profile. */
export function initializeTestBrokerAuthority(database: GatewayDatabase, canonicalAgentDir: string): BrokerAuthority {
	const authority = { canonicalAgentDir, identity: `gjc:${canonicalAgentDir}` };
	database.assertBrokerAuthority(authority, { initializeEmpty: true });
	return authority;
}

/** Opt-in durable ownership for fake-backed harnesses; call before inserting app data. */
export function attachTestBrokerOwnership<T extends SessionPort>(
	database: GatewayDatabase,
	port: T,
	canonicalAgentDir: string,
): T {
	const authority = initializeTestBrokerAuthority(database, canonicalAgentDir);
	const bind = port.bind.bind(port);
	const resume = port.resume.bind(port);
	port.bind = async (input) => {
		database.assertBrokerAuthority(authority);
		const binding = await bind(input);
		if (binding.originKey !== input.originKey || binding.repo !== input.repo || binding.epoch !== input.epoch)
			throw new Error("test session binding does not match its creation request");
		if (!database.recordOwnedBinding({ ...binding, authority }))
			throw new Error("test session binding lost to a durable epoch change");
		return binding;
	};
	port.resume = async (input) => {
		// Resume is not creation: seeded unknown IDs must never acquire provenance.
		database.assertOwnedSession(input.sessionId, input.repo, authority);
		const binding = await resume(input);
		if (
			binding.sessionId !== input.sessionId ||
			binding.repo !== input.repo ||
			binding.originKey !== input.originKey ||
			binding.epoch !== input.epoch
		)
			throw new Error("test session resume changed its binding identity");
		database.assertOwnedSession(binding.sessionId, binding.repo, authority);
		return binding;
	};
	return port;
}

/** Explicit successful gateway creation fixture, separate from the command spy under test. */
export async function createOwnedSessionFixture(
	database: GatewayDatabase,
	authority: BrokerAuthority,
	binding: { sessionId: string; originKey: string; epoch: number; repo: string },
): Promise<SessionBinding> {
	const run = async (args: readonly string[]): Promise<CliResult> => {
		if (args.includes("session.create"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { sessionId: binding.sessionId } }),
				stderr: "",
			};
		if (args.includes("inspect"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: {
						session: { sessionId: binding.sessionId, live: true, deleted: false, locator: { repo: binding.repo } },
					},
				}),
				stderr: "",
			};
		throw new Error(`unexpected creation fixture command ${args.join(" ")}`);
	};
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "creation-fixture",
		tailRunner: new TailRunner({ stream: noRelay, repo: binding.repo }),
	});
	return await port.bind(binding);
}

/** Fixture ports never open a relay; the command spy under test is the CLI runner. */
export function noRelay(sessionId: string): never {
	throw new Error(`test fixture opened a relay for ${sessionId}`);
}

/** What gjc answers when the session itself refuses a steer (`ok:false` envelope): a decision, not a transport failure. */
export function steerRefused(message = "no running turn"): GjcCliError {
	return new GjcCliError(`gjc sdk turn.steer reported failure: ${JSON.stringify({ code: "busy", message })}`, 0, "", {
		code: "busy",
		refused: true,
		message,
	});
}

export class ScriptedSessionPort implements SessionPort {
	readonly sends: SessionSendInput[] = [];
	readonly sendAttempts: SessionSendInput[] = [];
	readonly steers: SessionSteerInput[] = [];
	readonly workerOutputReads: WorkerOutputInput[] = [];
	readonly #workerOutputFixtures = new Map<string, CliResult>();
	readonly binds: SessionBindInput[] = [];
	readonly resumes: Array<{ sessionId: string; repo: string; originKey: string; epoch: number }> = [];
	readonly inspections: Array<{ sessionId: string; repo: string }> = [];
	readonly models: Array<{ sessionId: string; repo: string; selection: GjcModelSelection }> = [];
	readonly serviceTiers: Array<{ sessionId: string; repo: string; tier: GjcServiceTier }> = [];
	readonly closes: Array<{ sessionId: string; repo: string }> = [];
	readonly #sessions = new Map<string, string>();
	readonly #sessionStates = new Map<string, BrokerSession>();
	readonly #resumeFailures = new Map<string, Error>();
	readonly #operations = new Map<string, Operation>();
	readonly #transcripts = new Map<string, string[]>();
	readonly #tails = new Map<string, Set<ScriptedTailHandle>>();
	readonly #tailFrames = new Map<string, TailFrame[]>();
	/** Relay that submitted each op (undefined: a throwaway send relay, so nobody observes the content). */
	readonly #owner = new Map<string, ScriptedTailHandle | undefined>();
	readonly #chains = new Map<string, Promise<void>>();
	readonly onSend?: (input: SessionSendInput, port: ScriptedSessionPort) => void | Promise<void>;
	readonly onSteer?: (input: SessionSteerInput, port: ScriptedSessionPort) => void | Promise<void>;
	readonly #onBind:
		| ((
				input: SessionBindInput,
		  ) => string | { readonly sessionId: string } | Promise<string | { readonly sessionId: string }>)
		| undefined;
	readonly #sessionIdForBind: ((input: SessionBindInput) => string) | undefined;

	constructor(
		options: {
			onSend?: (input: SessionSendInput, port: ScriptedSessionPort) => void | Promise<void>;
			onSteer?: (input: SessionSteerInput, port: ScriptedSessionPort) => void | Promise<void>;
			onBind?: (
				input: SessionBindInput,
			) => string | { readonly sessionId: string } | Promise<string | { readonly sessionId: string }>;
			sessionIdForBind?: (input: SessionBindInput) => string;
		} = {},
	) {
		this.onSend = options.onSend;
		this.onSteer = options.onSteer;
		this.#onBind = options.onBind;
		this.#sessionIdForBind = options.sessionIdForBind;
	}

	async bind(input: SessionBindInput): Promise<SessionBinding> {
		this.binds.push(input);
		const cacheKey = this.#onBind ? `${input.originKey}#${input.epoch}` : input.originKey;
		const supplied = await this.#onBind?.(input);
		const sessionId =
			this.#sessions.get(cacheKey) ??
			(typeof supplied === "string" ? supplied : supplied?.sessionId) ??
			this.#sessionIdForBind?.(input) ??
			`session-${this.#sessions.size + 1}`;
		this.#sessions.set(cacheKey, sessionId);
		this.#sessionStates.set(sessionId, {
			sessionId,
			repo: input.repo,
			live: true,
			deleted: false,
		});
		if (input.model) this.models.push({ sessionId, repo: input.repo, selection: input.model });
		return {
			sessionId,
			originKey: input.originKey,
			epoch: input.epoch,
			repo: input.repo,
			...(input.model ? { startupModelApplied: true } : {}),
		};
	}

	async inspect(input: { sessionId: string; repo: string }): Promise<BrokerSession | undefined> {
		this.inspections.push(input);
		return this.#sessionStates.get(input.sessionId);
	}

	async close(input: { sessionId: string; repo: string }): Promise<void> {
		this.closes.push(input);
		const state = this.#sessionStates.get(input.sessionId);
		if (state) this.#sessionStates.set(input.sessionId, { ...state, live: false });
	}

	/** Background jobs each scripted host reports as running; an Error makes the read fail. */
	readonly hostJobs = new Map<string, readonly RunningHostJob[] | Error>();

	async runningJobs(input: { sessionId: string; repo: string }): Promise<readonly RunningHostJob[]> {
		const jobs = this.hostJobs.get(input.sessionId) ?? [];
		if (jobs instanceof Error) throw jobs;
		return jobs;
	}

	/** Ends the host of a retired session; a live seeded session is "terminated", anything else "already_gone". */
	async terminateHost(input: { sessionId: string; repo: string }): Promise<TerminateHostOutcome> {
		const state = this.#sessionStates.get(input.sessionId);
		if (!state || state.live === false) return { outcome: "already_gone" };
		await this.close(input);
		return { outcome: "terminated", pid: 40_000 + this.closes.length };
	}

	/** Broker liveness from the scripted state table; an unseeded id is disowned, like an id the broker never indexed. */
	async liveness(input: {
		sessionId: string;
		repo: string;
	}): Promise<{ live: boolean | undefined; disowned: boolean }> {
		const state = this.#sessionStates.get(input.sessionId);
		if (!state) return { live: undefined, disowned: true };
		return { live: state.live, disowned: false };
	}

	async resume(input: { sessionId: string; repo: string; originKey: string; epoch: number }): Promise<SessionBinding> {
		this.resumes.push(input);
		const failure = this.#resumeFailures.get(input.sessionId);
		if (failure) throw failure;
		const state = this.#sessionStates.get(input.sessionId);
		if (!state || state.deleted || state.repo !== input.repo)
			throw new Error(`scripted saved authority unavailable for ${input.sessionId}`);
		this.#sessionStates.set(input.sessionId, { ...state, live: true });
		return { sessionId: input.sessionId, originKey: input.originKey, epoch: input.epoch, repo: input.repo };
	}

	async send(input: SessionSendInput): Promise<SendReceipt> {
		this.sendAttempts.push(input);
		if (this.#operations.has(input.opRef))
			throw new OpRefRejectedError(input.opRef, "client_ref_conflict", { code: "client_ref_conflict" });
		this.sends.push(input);
		const correlation = { commandId: `command-${input.opRef}`, turnId: `turn-${input.opRef}` };
		this.#operations.set(input.opRef, {
			sessionId: input.sessionId,
			state: "in_flight",
			text: "",
			startedAt: Date.now(),
			...correlation,
		});
		// The relay that submitted the prompt owns the turn: it is the only
		// handle that receives this operation's content frames.
		this.#owner.set(input.opRef, input.relay instanceof ScriptedTailHandle ? input.relay : undefined);
		input.relay?.correlate(input.opRef, correlation);
		// Acceptance is independent of terminal completion, just like SDK send.
		// A response-script failure after acceptance is a failed operation, not a
		// fabricated send refusal that invites a duplicate prompt.
		void Promise.resolve()
			.then(() => this.onSend?.(input, this))
			.catch((error: unknown) => {
				this.fail(input.opRef, error instanceof Error ? error.message : String(error));
			});
		return { sessionId: input.sessionId, operationRef: input.opRef, ...correlation } as SendReceipt;
	}

	async steer(input: SessionSteerInput): Promise<void> {
		this.steers.push(input);
		await this.onSteer?.(input, this);
	}

	async setModel(input: {
		sessionId: string;
		repo: string;
		selection: GjcModelSelection;
	}): Promise<{ readonly changed: boolean }> {
		this.models.push(input);
		return { changed: true };
	}

	async setServiceTier(input: {
		sessionId: string;
		repo: string;
		tier: GjcServiceTier;
	}): Promise<{ readonly changed: boolean }> {
		this.serviceTiers.push(input);
		return { changed: true };
	}

	/** When set, status omits startedAt (older gjc reports), exercising the batch acceptedAt floor. */
	omitStartedAt = false;

	readonly failureEvidence = new Map<string, { reason: "unsupported_input_status" | "context_exhausted" }>();
	readonly failureEvidenceProbes: Array<{
		sessionId: string;
		repo: string;
		startedAtMs: number;
		terminalAtMs: number;
	}> = [];

	setFailedTurnEvidence(sessionId: string, reason: "unsupported_input_status" | "context_exhausted"): void {
		this.failureEvidence.set(sessionId, { reason });
	}

	async failedTurnEvidence(input: { sessionId: string; repo: string; startedAtMs: number; terminalAtMs: number }) {
		this.failureEvidenceProbes.push(input);
		return this.failureEvidence.get(input.sessionId);
	}

	async status(input: { sessionId: string; repo: string; opRef: string }): Promise<StatusReport> {
		const operation = this.#operations.get(input.opRef);
		if (!operation || operation.sessionId !== input.sessionId)
			return { operationRef: input.opRef, status: { status: "unknown" }, summaryCompleted: false };
		const startedAt = this.omitStartedAt ? {} : { startedAt: operation.startedAt };
		return {
			operationRef: input.opRef,
			status:
				operation.state === "terminal_ok"
					? {
							status: "terminal_ok",
							...startedAt,
							commandId: operation.commandId,
							turnId: operation.turnId,
							clientRef: input.opRef,
							terminalAt: operation.terminalAt,
							receiptState: "present",
							outcome: { reason: "end_turn" },
						}
					: operation.state === "failed"
						? {
								status: "failed",
								...startedAt,
								terminalAt: operation.terminalAt,
								error: {
									...(operation.errorCode ? { code: operation.errorCode } : {}),
									...(operation.error === undefined ? {} : { message: operation.error }),
								},
								...(operation.failureOutcome ? { outcome: operation.failureOutcome } : {}),
							}
						: { status: "in_flight", ...startedAt, commandId: operation.commandId, turnId: operation.turnId },
			summaryCompleted: operation.state !== "in_flight",
		};
	}

	/** Raw query envelopes run through the same evidence parser as production. */
	setWorkerOutputFixture(opRef: string, response: CliResult): void {
		this.#workerOutputFixtures.set(opRef, response);
	}

	async fetchWorkerOutput(input: WorkerOutputInput): Promise<WorkerOutputResult> {
		this.workerOutputReads.push(input);
		const fixture = this.#workerOutputFixtures.get(input.opRef);
		if (fixture) return parseWorkerOutputResponse(input, fixture, Date.now());
		const operation = this.#operations.get(input.opRef);
		const result =
			!operation || operation.sessionId !== input.sessionId
				? { status: "unknown" }
				: {
						kind: "prompt",
						clientRef: input.opRef,
						...(operation.commandId ? { commandId: operation.commandId } : {}),
						...(operation.turnId ? { turnId: operation.turnId } : {}),
						status: operation.state,
						...(this.omitStartedAt ? {} : { startedAt: operation.startedAt }),
						...(operation.terminalAt === undefined ? {} : { terminalAt: operation.terminalAt }),
						...(operation.state === "terminal_ok"
							? {
									receiptState: "present",
									content: {
										version: 1,
										type: "text",
										text: operation.text,
										byteLength: new TextEncoder().encode(operation.text).length,
										truncated: false,
									},
								}
							: {}),
					};
		return parseWorkerOutputResponse(
			input,
			{
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result }),
				stderr: "",
			},
			Date.now(),
		);
	}

	/** Same turn-floor rule as the broker port: only an assistant row produced at/after `notBeforeMs` counts. */
	async fetchAssistantSince(input: { sessionId: string; repo: string; notBeforeMs: number }) {
		const operation = [...this.#operations.values()]
			.reverse()
			.find((entry) => entry.sessionId === input.sessionId && entry.state === "terminal_ok");
		if (!operation || operation.terminalAt === undefined || operation.terminalAt + 2_000 < input.notBeforeMs)
			return undefined;
		return { text: operation.text, pages: 1, complete: true };
	}

	async fetchLastAssistant(input: { sessionId: string; repo: string }) {
		const operation = [...this.#operations.values()].reverse().find((entry) => entry.sessionId === input.sessionId);
		if (!operation || operation.state !== "terminal_ok")
			throw new Error("scripted session has no terminal assistant output");
		return { text: operation.text, pages: 1, complete: true };
	}

	async attachTail(input: TailAttachInput): Promise<TailHandle> {
		const handles = this.#tails.get(input.sessionId) ?? new Set<ScriptedTailHandle>();
		let handle!: ScriptedTailHandle;
		handle = new ScriptedTailHandle(input, () => handles.delete(handle));
		handles.add(handle);
		this.#tails.set(input.sessionId, handles);
		return handle;
	}

	async runCompaction(_input: {
		sessionId: string;
		repo: string;
		originKey: string;
	}): Promise<{ readonly status: "unavailable" }> {
		return { status: "unavailable" };
	}

	/** Counts running-server stall heartbeats so a started server can be asserted on. */
	stallChecks = 0;

	checkStalls(_now?: number): void {
		this.stallChecks++;
	}

	setStallTimeoutMs(_timeoutMs: number): void {}

	async runExclusive<T>(key: string, work: () => Promise<T>): Promise<T> {
		const previous = this.#chains.get(key);
		if (!previous) {
			const task = work();
			const settled = task.then(
				() => undefined,
				() => undefined,
			);
			this.#chains.set(key, settled);
			try {
				return await task;
			} finally {
				if (this.#chains.get(key) === settled) this.#chains.delete(key);
			}
		}
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		const chain = previous.catch(() => undefined).then(() => gate);
		this.#chains.set(key, chain);
		await previous.catch(() => undefined);
		try {
			return await work();
		} finally {
			release();
			if (this.#chains.get(key) === chain) this.#chains.delete(key);
		}
	}

	async request(input: SessionRequestInput): Promise<SessionRequestResult> {
		const relay = await this.attachTail({ sessionId: input.sessionId, brokerGeneration: 0, repo: input.repo });
		relay.beginTurn(input.opRef);
		const receipt = await this.send({ ...input, relay });
		for (let attempts = 0; attempts < 10_000; attempts++) {
			const status = await this.status({ sessionId: input.sessionId, repo: input.repo, opRef: input.opRef });
			if (status.status.status === "terminal_ok") {
				await relay.close();
				return {
					receipt,
					status,
					assistant: await this.fetchLastAssistant({ sessionId: input.sessionId, repo: input.repo }),
				};
			}
			if (status.status.status === "failed") {
				await relay.close();
				throw new Error(status.status.error?.message ?? "scripted failure");
			}
			await Bun.sleep(1);
		}
		await relay.close();
		throw new Error("scripted request did not settle");
	}

	/**
	 * An assistant message the host streams to the turn's owner relay. Without
	 * an explicit opRef the session's in-flight operation is the turn.
	 * `eventId: null` emits a frame with NO stable message id.
	 */
	emitAssistant(
		sessionId: string,
		text: string,
		eventId: string | null = `event-${crypto.randomUUID()}`,
		opRef?: string,
	): void {
		const transcript = this.#transcripts.get(sessionId) ?? [];
		transcript.push(text);
		this.#transcripts.set(sessionId, transcript);
		const ref = opRef ?? this.#inFlightOpRef(sessionId);
		this.#emit(sessionId, ref, {
			kind: "message_end",
			rawKind: "message_end",
			...(eventId === null ? {} : { eventId }),
			payload: { role: "assistant", content: [{ text }] },
			assistantText: text,
			steerEcho: false,
			idle: false,
		});
	}

	/** Marks the operation terminal_ok with `text` as its durable answer, then emits only the lifecycle end frame. */
	completeWithoutAnswerFrame(opRef: string, text: string): void {
		const operation = this.#operations.get(opRef);
		if (!operation) throw new Error(`unknown scripted operation ${opRef}`);
		operation.state = "terminal_ok";
		operation.text = text;
		operation.terminalAt = Date.now();
		this.#emit(operation.sessionId, opRef, {
			kind: "agent_end",
			rawKind: "agent_end",
			payload: { outcome: { reason: "end_turn" } },
			steerEcho: false,
			idle: true,
		});
	}

	/** The user/steer row the host echoes on message_end: attribution evidence, never chat output. */
	emitSteerEcho(sessionId: string, text: string, eventId = `steer-${crypto.randomUUID()}`): void {
		this.#emit(sessionId, this.#inFlightOpRef(sessionId), {
			kind: "message_end",
			rawKind: "message_end",
			eventId,
			payload: { role: "user", content: [{ text }] },
			steerEcho: true,
			idle: false,
		});
	}

	emitTool(
		sessionId: string,
		tool?: { readonly toolName: string; readonly intent?: string; readonly args?: unknown },
	): void {
		this.#emit(sessionId, this.#inFlightOpRef(sessionId), {
			kind: "tool_execution_start",
			rawKind: "tool_execution_start",
			payload: tool ? { ...tool, toolCallStarted: true } : { toolCallStarted: true },
			steerEcho: false,
			idle: false,
		});
	}

	emitToolEnd(sessionId: string, toolName: string): void {
		this.#emit(sessionId, this.#inFlightOpRef(sessionId), {
			kind: "tool_execution_end",
			rawKind: "tool_execution_end",
			payload: { toolName },
			steerEcho: false,
			idle: false,
		});
	}

	emitActivity(sessionId: string, progress: { readonly toolCalls: number; readonly outputTokens: number }): void {
		// Correlated to the in-flight turn: progress counters attributed to a turn
		// only reach the relay that owns it, like every other turn frame.
		this.#emit(sessionId, this.#inFlightOpRef(sessionId), {
			kind: "activity",
			rawKind: "activity",
			payload: { toolCalls: progress.toolCalls, outputTokens: progress.outputTokens },
			steerEcho: false,
			idle: false,
		});
	}

	complete(opRef: string, text: string): void {
		const operation = this.#operations.get(opRef);
		if (!operation) throw new Error(`unknown scripted operation ${opRef}`);
		operation.state = "terminal_ok";
		operation.text = text;
		operation.terminalAt = Date.now();
		this.emitAssistant(operation.sessionId, text, `final-${opRef}`, opRef);
		this.#emit(operation.sessionId, opRef, {
			kind: "agent_end",
			rawKind: "agent_end",
			payload: { outcome: { reason: "end_turn" } },
			steerEcho: false,
			idle: true,
		});
	}

	/**
	 * A scripted terminal failure. `detail` carries what the runtime reports next
	 * to the (possibly redacted) message: its bounded code and terminal outcome
	 * classifiers, which are the whole diagnosis for a post-start failure (#244).
	 */
	fail(opRef: string, error: string | undefined = "scripted failure", detail: ScriptedFailureDetail = {}): void {
		const operation = this.#operations.get(opRef);
		if (!operation) throw new Error(`unknown scripted operation ${opRef}`);
		operation.state = "failed";
		operation.error = error;
		if (detail.code !== undefined) operation.errorCode = detail.code;
		if (detail.outcome !== undefined) operation.failureOutcome = detail.outcome;
		operation.terminalAt = Date.now();
		this.#emit(operation.sessionId, opRef, {
			kind: "agent_failed",
			rawKind: "agent_failed",
			payload: {
				error: { ...(detail.code ? { code: detail.code } : {}), ...(error === undefined ? {} : { message: error }) },
			},
			steerEcho: false,
			idle: true,
		});
	}

	emitStall(sessionId: string, elapsedMs = 120_000): void {
		for (const tail of this.#tails.get(sessionId) ?? []) tail.stall(elapsedMs);
	}

	/** The relay that owns `opRef` dies mid-turn: its content is lost, the turn settles by status. */
	loseRelay(opRef: string): void {
		this.#owner.get(opRef)?.lose();
	}

	/** The relay that owns `opRef` is declared dead: closed, never reopened; the turn settles by CLI status. */
	killRelay(opRef: string): void {
		this.#owner.get(opRef)?.die();
	}

	/** Handles currently attached to a session. */
	tailsOf(sessionId: string): readonly ScriptedTailHandle[] {
		return [...(this.#tails.get(sessionId) ?? [])];
	}

	/** Patches a bound session's state, or seeds one the port never bound (a session left on disk by an earlier runtime). */
	setSessionState(sessionId: string, patch: Partial<Pick<BrokerSession, "repo" | "live" | "deleted">>): void {
		const current = this.#sessionStates.get(sessionId) ?? { sessionId, repo: "", live: false, deleted: false };
		this.#sessionStates.set(sessionId, { ...current, ...patch });
	}

	failResume(sessionId: string, message = "scripted resume impossible"): void {
		this.#resumeFailures.set(sessionId, new Error(message));
	}

	seedOperation(opRef: string, sessionId: string, state: Operation["state"] = "in_flight", text = ""): void {
		const existing = this.#operations.get(opRef);
		const now = Date.now();
		this.#operations.set(opRef, {
			sessionId,
			state,
			text,
			startedAt: existing?.startedAt ?? now,
			...(state !== "in_flight" ? { terminalAt: now } : {}),
		});
	}

	seedAcceptedSend(input: SessionSendInput, state: Operation["state"] = "in_flight", text = ""): void {
		this.sendAttempts.push(input);
		this.sends.push(input);
		const now = Date.now();
		this.#operations.set(input.opRef, {
			sessionId: input.sessionId,
			state,
			text,
			startedAt: now,
			...(state !== "in_flight" ? { terminalAt: now } : {}),
		});
	}

	transcript(sessionId: string): readonly string[] {
		return this.#transcripts.get(sessionId) ?? [];
	}

	tailFrames(sessionId: string): readonly TailFrame[] {
		return this.#tailFrames.get(sessionId) ?? [];
	}

	/** The session's most recent in-flight operation, the turn an un-attributed emit belongs to. */
	#inFlightOpRef(sessionId: string): string | undefined {
		return [...this.#operations.entries()]
			.reverse()
			.find(([, entry]) => entry.sessionId === sessionId && entry.state === "in_flight")?.[0];
	}

	/**
	 * Host delivery contract: a frame correlated to an operation reaches ONLY
	 * the relay that submitted it (stamped with that op's commandId/turnId);
	 * an uncorrelated frame (activity) reaches every relay on the session.
	 */
	#emit(sessionId: string, opRef: string | undefined, frame: TailFrame): void {
		const operation = opRef ? this.#operations.get(opRef) : undefined;
		const stamped: TailFrame = operation
			? {
					...frame,
					...(operation.commandId ? { commandId: operation.commandId } : {}),
					...(operation.turnId ? { turnId: operation.turnId } : {}),
				}
			: frame;
		const frames = this.#tailFrames.get(sessionId) ?? [];
		frames.push(stamped);
		this.#tailFrames.set(sessionId, frames);
		if (!opRef) {
			for (const tail of this.#tails.get(sessionId) ?? []) tail.emit(stamped);
			return;
		}
		if (!this.#owner.has(opRef)) {
			// Seeded/recovered operation: nobody submitted it on a relay this
			// process holds, so no handle receives its content (as in production).
			return;
		}
		this.#owner.get(opRef)?.emit(stamped);
	}
}

/**
 * Test-only SDK port that completes each accepted operation from a response
 * function. It keeps all turn behavior behind SessionPort rather than reviving
 * a deleted spawn-per-turn transport seam.
 */
export function respondingSessionPort(
	respond: (input: SessionSendInput, port: ScriptedSessionPort) => string | Promise<string>,
	options: { readonly sessionIdForBind?: (input: SessionBindInput) => string } = {},
): ScriptedSessionPort {
	return new ScriptedSessionPort({
		...options,
		onSend: async (input, port) => port.complete(input.opRef, await respond(input, port)),
	});
}

export type SessionPortResponder = (
	sessionId: string,
	text: string,
	systemPreamble?: string,
	onProgress?: (progress: { readonly toolCalls: number; readonly outputTokens: number }) => void,
	options?: { readonly onAssistantText?: (text: string) => void },
) => string | Promise<string>;

/**
 * Adapts concise response scripts onto a real ScriptedSessionPort. The callback
 * shape preserves test readability while the exercised transport is the SDK
 * SessionPort contract, including tails, status, and terminal bodies.
 */
export function sessionPortFromResponder(options: {
	readonly respond: SessionPortResponder;
	readonly bind?: (
		originKey: string,
		epoch: number,
		input: SessionBindInput,
	) => string | { readonly sessionId: string } | Promise<string | { readonly sessionId: string }>;
}): ScriptedSessionPort {
	return new ScriptedSessionPort({
		onBind: (input) => options.bind?.(input.originKey, input.epoch, input) ?? `session-${input.epoch}`,
		onSend: async (input, port) => {
			try {
				const text = await options.respond(
					input.sessionId,
					input.text,
					input.systemPreamble,
					(progress) => port.emitActivity(input.sessionId, progress),
					{ onAssistantText: (text) => port.emitAssistant(input.sessionId, text) },
				);
				port.complete(input.opRef, text);
			} catch (error) {
				port.fail(input.opRef, error instanceof Error ? error.message : String(error));
			}
		},
	});
}

/**
 * A response script completes an accepted SDK operation through the same
 * ScriptedSessionPort status and tail surface used by recovery tests.
 */
export function sessionPortFromScript(script: {
	readonly bind?: (
		originKey: string,
		epoch: number,
		input: SessionBindInput,
	) => string | { readonly sessionId: string } | Promise<string | { readonly sessionId: string }>;
	readonly respond: SessionPortResponder;
}): ScriptedSessionPort {
	return new ScriptedSessionPort({
		onBind: (input) => script.bind?.(input.originKey, input.epoch, input) ?? `session-${input.epoch}`,
		onSend: (input, port) => {
			void (async () => {
				try {
					const text = await script.respond(
						input.sessionId,
						input.text,
						input.systemPreamble,
						(progress) => port.emitActivity(input.sessionId, progress),
						{ onAssistantText: (text) => port.emitAssistant(input.sessionId, text) },
					);
					port.complete(input.opRef, text);
				} catch (error) {
					port.fail(input.opRef, error instanceof Error ? error.message : String(error));
				}
			})();
		},
	});
}

/** What the runtime reports alongside a failed turn's message. */
export type ScriptedFailureDetail = {
	readonly code?: string;
	readonly outcome?: Record<string, string>;
};

type Operation = {
	readonly sessionId: string;
	state: "in_flight" | "terminal_ok" | "failed";
	text: string;
	error?: string;
	errorCode?: string;
	failureOutcome?: Record<string, string>;
	readonly startedAt: number;
	terminalAt?: number;
	readonly commandId?: string;
	readonly turnId?: string;
};

/** Same fencing as ManagedTailHandle: only frames correlated to the turn it was told about are delivered. */
export class ScriptedTailHandle implements TailHandle {
	readonly sessionId: string;
	readonly brokerGeneration: number;
	readonly ready = Promise.resolve();
	readonly #input: TailAttachInput;
	readonly #remove: () => void;
	/** Same contract as ManagedTailHandle: frames deliver in order, outside the caller's turn. */
	#flush: Promise<void> = Promise.resolve();
	#opRef: string | undefined;
	#correlation: TurnCorrelation = {};
	#running = false;
	#closed = false;

	constructor(input: TailAttachInput, remove: () => void) {
		this.#input = input;
		this.#remove = remove;
		this.sessionId = input.sessionId;
		this.brokerGeneration = input.brokerGeneration;
	}

	get opRef(): string | undefined {
		return this.#opRef;
	}

	get running(): boolean {
		return this.#running;
	}

	async control(
		_operation: string,
		_input: Record<string, unknown>,
		_options?: RelayRequestOptions,
	): Promise<RelayResponse> {
		throw new RelayClosedError(this.sessionId, "scripted relays carry no raw controls; use the port methods");
	}

	async query(_name: string, _input: Record<string, unknown>, _options?: RelayRequestOptions): Promise<RelayResponse> {
		throw new RelayClosedError(this.sessionId, "scripted relays carry no raw queries; use the port methods");
	}

	beginTurn(opRef: string, correlation: TurnCorrelation = {}): void {
		if (this.#closed) return;
		this.#opRef = opRef;
		this.#correlation = { ...correlation };
	}

	correlate(opRef: string, correlation: TurnCorrelation): void {
		if (this.#opRef !== opRef) return;
		this.#correlation = { ...this.#correlation, ...correlation };
	}

	setTurnRunning(running: boolean): void {
		this.#running = running;
	}

	emit(frame: TailFrame): void {
		if (this.#closed) return;
		if (frame.commandId !== undefined || frame.turnId !== undefined) {
			if (this.#opRef === undefined) return;
			const known = this.#correlation;
			if (known.commandId === undefined && known.turnId === undefined) {
				this.#correlation = {
					...(frame.commandId ? { commandId: frame.commandId } : {}),
					...(frame.turnId ? { turnId: frame.turnId } : {}),
				};
			} else if (
				(known.commandId !== undefined && frame.commandId !== undefined && known.commandId !== frame.commandId) ||
				(known.turnId !== undefined && frame.turnId !== undefined && known.turnId !== frame.turnId) ||
				!(
					(known.commandId !== undefined && frame.commandId !== undefined) ||
					(known.turnId !== undefined && frame.turnId !== undefined)
				)
			)
				return;
		} else if (!frame.idle) return;
		this.#flush = this.#flush.then(async () => {
			if (this.#closed) return;
			await this.#input.onFrame?.(frame);
		});
		this.#flush.catch(() => {});
	}

	stall(elapsedMs: number): void {
		if (!this.#closed)
			void this.#input.onStall?.({ sessionId: this.sessionId, brokerGeneration: this.brokerGeneration, elapsedMs });
	}

	/** Simulates the relay process dying under a running turn. */
	lose(): void {
		if (this.#closed) return;
		void this.#input.onRelayLost?.({ sessionId: this.sessionId, brokerGeneration: this.brokerGeneration });
	}

	/**
	 * Simulates the relay giving up (six immediate deaths): the handle closes and
	 * will not reopen. Fires even on an already-closed handle: production's
	 * give-up runs on the runner's own loop and can land after the actor let
	 * the handle go, which is exactly the late notice the actor must fence.
	 */
	die(): void {
		void this.close();
		void this.#input.onRelayDead?.({ sessionId: this.sessionId, brokerGeneration: this.brokerGeneration });
	}

	async close(): Promise<void> {
		if (this.#closed) return;
		this.#closed = true;
		this.#remove();
	}
}

/**
 * A scripted `gjc sdk serve --stdio` host for BrokerSessionPort tests: answers
 * hello, routes each control/query request to `respond`, and records what the
 * gateway wrote. Push host-initiated frames with `host()`.
 */
export type ScriptedRelayRequest = {
	readonly type: "control_request" | "query_request";
	readonly operation: string;
	readonly input: Record<string, unknown>;
};
export type ScriptedRelayReply =
	| { readonly ok: true; readonly result?: Record<string, unknown> }
	| { readonly ok: false; readonly error: { readonly code: string; readonly message?: string } };

export function scriptedRelay(
	respond: (request: ScriptedRelayRequest) => ScriptedRelayReply | Promise<ScriptedRelayReply>,
) {
	const requests: ScriptedRelayRequest[] = [];
	const streams: ScriptedRelayStream[] = [];
	const spawn: TailStreamSpawner = (sessionId) => {
		const stream = new ScriptedRelayStream(sessionId, `connection:${streams.length + 1}`, async (frame) => {
			const request: ScriptedRelayRequest = {
				type: frame.type as ScriptedRelayRequest["type"],
				operation: String(frame.type === "control_request" ? frame.operation : frame.query),
				input: (frame.input as Record<string, unknown>) ?? {},
			};
			requests.push(request);
			const reply = await respond(request);
			const responseType = frame.type === "control_request" ? "control_response" : "query_response";
			return { type: responseType, id: frame.id, ...reply };
		});
		streams.push(stream);
		return stream;
	};
	return { spawn, requests, streams };
}

export class ScriptedRelayStream implements SessionRelayStream {
	readonly lines: AsyncIterable<string>;
	readonly written: Record<string, unknown>[] = [];
	closed = false;
	#queue: Array<string | null> = [];
	#waiters: Array<(line: string | null) => void> = [];

	constructor(
		readonly sessionId: string,
		readonly connectionId: string,
		private readonly respond: (frame: Record<string, unknown>) => Promise<Record<string, unknown>>,
	) {
		const next = () =>
			new Promise<string | null>((resolve) => {
				const queued = this.#queue.shift();
				if (queued !== undefined) resolve(queued);
				else this.#waiters.push(resolve);
			});
		this.lines = {
			[Symbol.asyncIterator]: () => ({
				next: async () => {
					const line = await next();
					return line === null ? { done: true, value: undefined } : { done: false, value: line };
				},
			}),
		};
	}

	host(frame: Record<string, unknown>): void {
		const line = JSON.stringify(frame);
		const waiter = this.#waiters.shift();
		if (waiter) waiter(line);
		else this.#queue.push(line);
	}

	write(line: string): void {
		if (this.closed) throw new Error("relay closed");
		const frame = JSON.parse(line) as Record<string, unknown>;
		this.written.push(frame);
		if (frame.type === "hello") {
			this.host({ type: "hello", protocolVersion: 3, connectionId: this.connectionId });
			return;
		}
		if (frame.type === "control_request" || frame.type === "query_request")
			void this.respond(frame).then((response) => this.host(response));
	}

	close(): void {
		if (this.closed) return;
		this.closed = true;
		const waiter = this.#waiters.shift();
		if (waiter) waiter(null);
		else this.#queue.push(null);
	}
}
