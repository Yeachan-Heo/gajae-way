import {
	type BrokerSession,
	GjcCliError,
	OpRefRejectedError,
	type SendReceipt,
	type StatusReport,
} from "@gajaeway/subsession";
import type { GjcModelSelection, GjcServiceTier } from "../src/config";
import type {
	SessionBindInput,
	SessionBinding,
	SessionPort,
	SessionRequestInput,
	SessionRequestResult,
	SessionSendInput,
	SessionSteerInput,
} from "../src/orchestrator/session-port";
import type { TailAttachInput, TailFrame, TailHandle } from "../src/orchestrator/tail-runner";

/** What gjc answers when the session itself refuses a steer (`ok:false` envelope): a decision, not a transport failure. */
export function steerRefused(message = "no running turn"): GjcCliError {
	return new GjcCliError(`gjc sdk turn.steer reported failure: ${JSON.stringify({ code: "busy", message })}`, 0, "", {
		code: "busy",
		message,
	});
}

export class ScriptedSessionPort implements SessionPort {
	readonly sends: SessionSendInput[] = [];
	readonly sendAttempts: SessionSendInput[] = [];
	readonly steers: SessionSteerInput[] = [];
	readonly binds: SessionBindInput[] = [];
	readonly resumes: Array<{ sessionId: string; repo: string; originKey: string; epoch: number }> = [];
	readonly inspections: Array<{ sessionId: string; repo: string }> = [];
	readonly models: Array<{ sessionId: string; repo: string; selection: GjcModelSelection }> = [];
	readonly serviceTiers: Array<{ sessionId: string; repo: string; tier: GjcServiceTier }> = [];
	readonly #sessions = new Map<string, string>();
	readonly #sessionStates = new Map<string, BrokerSession>();
	readonly #resumeFailures = new Map<string, Error>();
	readonly #operations = new Map<string, Operation>();
	readonly #transcripts = new Map<string, string[]>();
	readonly #tails = new Map<string, Set<ScriptedTailHandle>>();
	readonly #tailFrames = new Map<string, TailFrame[]>();
	readonly #chains = new Map<string, Promise<void>>();
	readonly onSend?: (input: SessionSendInput, port: ScriptedSessionPort) => void | Promise<void>;
	readonly #onBind:
		| ((
				input: SessionBindInput,
		  ) => string | { readonly sessionId: string } | Promise<string | { readonly sessionId: string }>)
		| undefined;
	readonly #sessionIdForBind: ((input: SessionBindInput) => string) | undefined;

	constructor(
		options: {
			onSend?: (input: SessionSendInput, port: ScriptedSessionPort) => void | Promise<void>;
			onBind?: (
				input: SessionBindInput,
			) => string | { readonly sessionId: string } | Promise<string | { readonly sessionId: string }>;
			sessionIdForBind?: (input: SessionBindInput) => string;
		} = {},
	) {
		this.onSend = options.onSend;
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
		this.#operations.set(input.opRef, {
			sessionId: input.sessionId,
			state: "in_flight",
			text: "",
			startedAt: Date.now(),
		});
		await this.onSend?.(input, this);
		return { sessionId: input.sessionId, operationRef: input.opRef } as SendReceipt;
	}

	async steer(input: SessionSteerInput): Promise<void> {
		this.steers.push(input);
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

	async status(input: { sessionId: string; repo: string; opRef: string }): Promise<StatusReport> {
		const operation = this.#operations.get(input.opRef);
		if (!operation || operation.sessionId !== input.sessionId)
			return { operationRef: input.opRef, status: { status: "unknown" }, summaryCompleted: false };
		const startedAt = this.omitStartedAt ? {} : { startedAt: operation.startedAt };
		return {
			operationRef: input.opRef,
			status:
				operation.state === "terminal_ok"
					? { status: "terminal_ok", ...startedAt }
					: operation.state === "failed"
						? { status: "failed", ...startedAt, error: { message: operation.error ?? "scripted failure" } }
						: { status: "in_flight", ...startedAt },
			summaryCompleted: operation.state !== "in_flight",
		};
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
		handle = new ScriptedTailHandle(input, () => handles.delete(handle), this.#tailFrames.get(input.sessionId) ?? []);
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
		const receipt = await this.send(input);
		for (let attempts = 0; attempts < 10_000; attempts++) {
			const status = await this.status({ sessionId: input.sessionId, repo: input.repo, opRef: input.opRef });
			if (status.status.status === "terminal_ok") {
				return {
					receipt,
					status,
					assistant: await this.fetchLastAssistant({ sessionId: input.sessionId, repo: input.repo }),
				};
			}
			if (status.status.status === "failed") throw new Error(status.status.error?.message ?? "scripted failure");
			await Bun.sleep(1);
		}
		throw new Error("scripted request did not settle");
	}

	/** `eventId: null` emits a frame with NO id, as gjc 0.16 does for synthesized/idless rows. */
	emitAssistant(
		sessionId: string,
		text: string,
		eventId: string | null = `event-${crypto.randomUUID()}`,
		opRef?: string,
	): void {
		const transcript = this.#transcripts.get(sessionId) ?? [];
		transcript.push(text);
		this.#transcripts.set(sessionId, transcript);
		this.#emit(sessionId, {
			kind: "transcript",
			rawKind: "transcript",
			...(eventId === null ? {} : { eventId }),
			payload: { role: "assistant", content: [{ text }], ...(opRef ? { opRef } : {}) },
			assistantText: text,
			steerEcho: false,
			idle: false,
		});
	}

	/**
	 * A durable transcript row as replayed by a cursorless tail re-attach:
	 * host `ts` stamp, and no opRef unless the test attributes it explicitly.
	 */
	emitReplayedTranscriptRow(sessionId: string, text: string, tsMs: number, opRef?: string): void {
		this.#emit(sessionId, {
			kind: "transcript",
			rawKind: "transcript",
			payload: {
				role: "assistant",
				content: [{ text }],
				ts: new Date(tsMs).toISOString(),
				...(opRef ? { opRef } : {}),
			},
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
		this.#emit(operation.sessionId, {
			kind: "agent_end",
			rawKind: "agent_end",
			payload: { opRef },
			steerEcho: false,
			idle: true,
		});
	}

	emitSteerEcho(sessionId: string, text: string, eventId = `steer-${crypto.randomUUID()}`): void {
		this.#emit(sessionId, {
			kind: "transcript",
			rawKind: "transcript",
			eventId,
			payload: { role: "user", content: [{ text }] },
			steerEcho: true,
			idle: false,
		});
	}

	emitTool(sessionId: string): void {
		this.#emit(sessionId, {
			kind: "event",
			rawKind: "tool_execution_start",
			payload: {},
			steerEcho: false,
			idle: false,
		});
	}

	emitActivity(sessionId: string, progress: { readonly toolCalls: number; readonly outputTokens: number }): void {
		this.#emit(sessionId, {
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
		this.#emit(operation.sessionId, {
			kind: "agent_end",
			rawKind: "agent_end",
			payload: { opRef },
			steerEcho: false,
			idle: true,
		});
	}

	fail(opRef: string, error = "scripted failure"): void {
		const operation = this.#operations.get(opRef);
		if (!operation) throw new Error(`unknown scripted operation ${opRef}`);
		operation.state = "failed";
		operation.error = error;
		this.#emit(operation.sessionId, {
			kind: "agent_failed",
			rawKind: "agent_failed",
			payload: {},
			steerEcho: false,
			idle: true,
		});
	}

	emitStall(sessionId: string, elapsedMs = 120_000): void {
		for (const tail of this.#tails.get(sessionId) ?? []) tail.stall(elapsedMs);
	}

	setSessionState(sessionId: string, patch: Partial<Pick<BrokerSession, "repo" | "live" | "deleted">>): void {
		const current = this.#sessionStates.get(sessionId);
		if (!current) throw new Error(`unknown scripted session ${sessionId}`);
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
			...(state === "terminal_ok" ? { terminalAt: now } : {}),
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
			...(state === "terminal_ok" ? { terminalAt: now } : {}),
		});
	}

	transcript(sessionId: string): readonly string[] {
		return this.#transcripts.get(sessionId) ?? [];
	}

	tailFrames(sessionId: string): readonly TailFrame[] {
		return this.#tailFrames.get(sessionId) ?? [];
	}

	#emit(sessionId: string, frame: TailFrame): void {
		const frames = this.#tailFrames.get(sessionId) ?? [];
		frames.push(frame);
		this.#tailFrames.set(sessionId, frames);
		for (const tail of this.#tails.get(sessionId) ?? []) tail.emit(frame);
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

type Operation = {
	readonly sessionId: string;
	state: "in_flight" | "terminal_ok" | "failed";
	text: string;
	error?: string;
	readonly startedAt: number;
	terminalAt?: number;
};

class ScriptedTailHandle implements TailHandle {
	readonly sessionId: string;
	readonly brokerGeneration: number;
	readonly ready = Promise.resolve();
	readonly cursor = undefined;
	readonly #input: TailAttachInput;
	readonly #remove: () => void;
	readonly #buffer: TailFrame[] = [];
	/** Same contract as ManagedTailHandle: frames flush in order, outside the accepting caller's turn. */
	#flush: Promise<void> = Promise.resolve();
	#accepted = false;
	#acceptedOpRef: string | undefined;
	#closed = false;

	constructor(input: TailAttachInput, remove: () => void, historical: readonly TailFrame[]) {
		this.#input = input;
		this.#remove = remove;
		this.sessionId = input.sessionId;
		this.brokerGeneration = input.brokerGeneration;
		this.#buffer.push(...historical);
	}

	async markAccepted(opRef: string): Promise<void> {
		if (this.#closed) return;
		this.#accepted = true;
		this.#acceptedOpRef = opRef;
		const buffered = this.#buffer
			.splice(0)
			.filter((frame) => frameOperationRef(frame) === undefined || frameOperationRef(frame) === opRef);
		// Not awaited: markAccepted runs inside the origin mailbox and onFrame
		// re-enters it (production ManagedTailHandle has the identical shape).
		this.#flush = this.#flush.then(async () => {
			for (const frame of buffered) await this.#input.onFrame?.(frame);
		});
		this.#flush.catch(() => {});
	}

	setTurnRunning(_running: boolean): void {}

	emit(frame: TailFrame): void {
		if (this.#closed) return;
		if (!this.#accepted) {
			this.#buffer.push(frame);
			return;
		}
		if (frameOperationRef(frame) !== undefined && frameOperationRef(frame) !== this.#acceptedOpRef) return;
		this.#flush = this.#flush.then(async () => {
			await this.#input.onFrame?.(frame);
		});
		this.#flush.catch(() => {});
	}

	stall(elapsedMs: number): void {
		if (!this.#closed)
			void this.#input.onStall?.({ sessionId: this.sessionId, brokerGeneration: this.brokerGeneration, elapsedMs });
	}

	async close(): Promise<void> {
		if (this.#closed) return;
		this.#closed = true;
		this.#remove();
	}
}

function frameOperationRef(frame: TailFrame): string | undefined {
	for (const value of [frame.payload.opRef, frame.payload.operationRef])
		if (typeof value === "string" && value.length > 0) return value;
	return undefined;
}
