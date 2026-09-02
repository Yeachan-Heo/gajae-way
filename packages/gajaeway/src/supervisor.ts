import type { GatewayServer, LocalGatewayPort } from "@gajaeway/gateway";
import { sanitizeDiagnostic } from "@gajaeway/gateway";
import type { ChatMessagePayload, ChatProgressPayload } from "@gajaeway/protocol";

const HEALTHY_UPTIME_MS = 60_000;
const ADAPTER_STOP_TIMEOUT_MS = 10_000;

export interface AdapterHandle {
	readonly stop: () => Promise<void>;
	readonly settled: Promise<void>;
}

export interface AdapterGatewayClient extends LocalGatewayPort {
	onChatMessage(handler: (message: ChatMessagePayload) => void): () => void;
	onChatProgress(handler: (progress: ChatProgressPayload) => void): () => void;
}

export interface Generation {
	readonly id: number;
	readonly signal: AbortSignal;
	readonly port: AdapterGatewayClient;
	/** Registers a runtime task; generation teardown waits for registered work. */
	track<T>(task: Promise<T>): Promise<T>;
	/** Rejects with AbortError if this generation is stopped. */
	sleep(ms: number): Promise<void>;
}

export type AdapterFactory = (gen: Generation) => Promise<AdapterHandle>;

export type RestartDecision =
	| { readonly action: "restart"; readonly delayMs: number }
	| { readonly action: "escalate" };

/** Pure restart policy: four bounded retries, then process-level escalation. */
export function restartDecision(
	consecutiveFailures: number,
	lastUptimeMs: number,
	random: () => number = Math.random,
): RestartDecision {
	const failures = lastUptimeMs >= HEALTHY_UPTIME_MS ? 1 : Math.max(1, Math.trunc(consecutiveFailures));
	if (failures >= 5) return { action: "escalate" };
	const base = 1_000 * 2 ** (failures - 1);
	const sample = Math.min(1, Math.max(0, random()));
	return { action: "restart", delayMs: Math.round(base * (0.75 + sample * 0.5)) };
}

export interface AdapterSupervisorOptions {
	readonly server: GatewayServer;
	readonly escalate: (name: string) => void | Promise<void>;
	readonly log?: (line: string) => void;
	readonly random?: () => number;
	readonly now?: () => number;
	/** Internal test seam; production uses an abortable timer. */
	readonly delay?: (ms: number, signal: AbortSignal) => Promise<void>;
	/** Internal test seam; production always gives an adapter ten seconds. */
	readonly stopTimeoutMs?: number;
}

interface ManagedGeneration {
	readonly id: number;
	readonly controller: AbortController;
	readonly port: AdapterGatewayClient;
	readonly generation: Generation;
	readonly tasks: Set<Promise<unknown>>;
	start?: Promise<AdapterHandle>;
	handle?: AdapterHandle;
	startedAt?: number;
	disposed?: Promise<void>;
	failureHandled: boolean;
}

interface AdapterEntry {
	readonly name: string;
	readonly factory: AdapterFactory;
	nextId: number;
	failures: number;
	current?: ManagedGeneration;
	restarting?: Promise<void>;
	escalated: boolean;
}

/**
 * Owns one generation at a time for each adapter. It never reuses a local port:
 * subscriptions are installed before every fresh port opens, which preserves replay order.
 */
export class AdapterSupervisor {
	readonly #server: GatewayServer;
	readonly #escalate: (name: string) => void | Promise<void>;
	readonly #log: (line: string) => void;
	readonly #random: () => number;
	readonly #now: () => number;
	readonly #delay: (ms: number, signal: AbortSignal) => Promise<void>;
	readonly #stopTimeoutMs: number;
	readonly #entries = new Map<string, AdapterEntry>();
	readonly #stopController = new AbortController();
	#stopped = false;
	#stopPromise: Promise<void> | undefined;

	constructor(options: AdapterSupervisorOptions) {
		this.#server = options.server;
		this.#escalate = options.escalate;
		this.#log = options.log ?? ((line) => console.error(line));
		this.#random = options.random ?? Math.random;
		this.#now = options.now ?? Date.now;
		this.#delay = options.delay ?? abortableSleep;
		this.#stopTimeoutMs = options.stopTimeoutMs ?? ADAPTER_STOP_TIMEOUT_MS;
	}

	/** Starts supervision without blocking daemon readiness on a platform login. */
	async start(name: string, factory: AdapterFactory): Promise<void> {
		if (!name) throw new Error("adapter name must not be empty");
		if (this.#entries.has(name)) throw new Error(`adapter ${name} is already supervised`);
		const entry: AdapterEntry = { name, factory, nextId: 0, failures: 0, escalated: false };
		this.#entries.set(name, entry);
		if (!this.#stopped) this.#launch(entry);
	}

	/** Cancels restarts and immediately aborts every generation, including starts in flight. */
	stop(): Promise<void> {
		if (this.#stopPromise) return this.#stopPromise;
		this.#stopped = true;
		this.#stopController.abort();
		for (const entry of this.#entries.values()) {
			entry.current?.controller.abort();
			// A factory that has not returned cannot perform its own teardown yet. Its
			// port must reject late work now; the returned handle is stopped on arrival.
			if (entry.current && !entry.current.handle) entry.current.port.close();
		}
		this.#stopPromise = Promise.resolve();
		return this.#stopPromise;
	}

	/**
	 * Stops currently live handles and JOINS in-flight starts: a factory that
	 * resolves after stop() still produces a handle that must be torn down before
	 * the daemon releases its lock, so the wait is bounded by the adapter deadline
	 * and the late handle's disposal is awaited rather than fire-and-forget.
	 */
	async stopAdapters(): Promise<void> {
		await this.stop();
		await Promise.all(
			[...this.#entries.values()].map(async (entry) => {
				const generation = entry.current;
				if (!generation) return;
				// ONE absolute deadline per adapter covers both the pending factory and
				// the handle disposal, so a late start cannot double the stop budget.
				const deadline = this.#now() + this.#stopTimeoutMs;
				const remaining = () => Math.max(0, deadline - this.#now());
				if (!generation.handle && generation.start) {
					// #started/#failed observe the outcome and dispose; awaiting
					// `disposed` afterwards joins that work.
					await completesBefore(
						generation.start.then(
							() => undefined,
							() => undefined,
						),
						remaining(),
					);
					// Give the start observers a tick to attach `disposed`.
					await Promise.resolve();
				}
				const disposal =
					generation.disposed ?? (generation.handle ? this.#dispose(entry, generation, remaining()) : undefined);
				const done = disposal ? await completesBefore(disposal, remaining()) : false;
				if (!done) {
					// The budget is spent: whatever the late teardown is still doing, the
					// port is closed NOW so the daemon can release its lock without a
					// straggler reaching the gateway afterwards.
					this.#log(`adapter_stop_timeout adapter=${entry.name}`);
					generation.controller.abort();
					generation.port.close();
				}
			}),
		);
	}

	#launch(entry: AdapterEntry): void {
		if (this.#stopped || entry.escalated || entry.current) return;
		const managed = this.#createGeneration(entry, ++entry.nextId);
		entry.current = managed;
		const start = Promise.resolve().then(async () => await entry.factory(managed.generation));
		managed.start = start;
		void start.then(
			(handle) => {
				void this.#started(entry, managed, handle).catch((error: unknown) =>
					this.#log(`adapter_start_observer_error adapter=${entry.name} error=${describe(error)}`),
				);
			},
			(error: unknown) => {
				void this.#failed(entry, managed, error).catch((failure: unknown) =>
					this.#log(`adapter_failure_observer_error adapter=${entry.name} error=${describe(failure)}`),
				);
			},
		);
	}

	#createGeneration(entry: AdapterEntry, id: number): ManagedGeneration {
		const controller = new AbortController();
		const raw = this.#server.attach(`${entry.name}#${id}`);
		const tasks = new Set<Promise<unknown>>();
		const port: AdapterGatewayClient = {
			label: raw.label,
			request: async <T = unknown>(verb: string, params?: unknown): Promise<T> => await raw.request<T>(verb, params),
			on: (event, handler) => raw.on(event, handler),
			onChatMessage: (handler) => raw.on("chat.message", (payload) => handler(payload as ChatMessagePayload)),
			onChatProgress: (handler) => raw.on("chat.progress", (payload) => handler(payload as ChatProgressPayload)),
			open: async () => await raw.open(),
			close: () => raw.close(),
		};
		const track = <T>(task: Promise<T>): Promise<T> => {
			tasks.add(task);
			void task.then(
				() => tasks.delete(task),
				() => tasks.delete(task),
			);
			return task;
		};
		const generation: Generation = {
			id,
			signal: controller.signal,
			port,
			track,
			sleep: async (ms) => await abortableSleep(ms, controller.signal),
		};
		return { id, controller, port, generation, tasks, failureHandled: false };
	}

	async #started(entry: AdapterEntry, managed: ManagedGeneration, handle: AdapterHandle): Promise<void> {
		managed.handle = handle;
		if (this.#stopped || managed.controller.signal.aborted || entry.current !== managed) {
			// A factory may produce a handle after stop() raced its resolution. Observe
			// any later fault so teardown cannot surface an unhandled rejection.
			void handle.settled.catch(() => {});
			this.#log(`adapter_disposed_after_stop adapter=${entry.name} generation=${managed.id}`);
			await this.#dispose(entry, managed);
			return;
		}
		managed.startedAt = this.#now();
		this.#log(`adapter_started adapter=${entry.name} generation=${managed.id}`);
		void handle.settled.then(
			() => {
				void this.#settled(entry, managed).catch((error: unknown) =>
					this.#log(`adapter_settled_observer_error adapter=${entry.name} error=${describe(error)}`),
				);
			},
			(error: unknown) => {
				void this.#failed(entry, managed, error).catch((failure: unknown) =>
					this.#log(`adapter_failure_observer_error adapter=${entry.name} error=${describe(failure)}`),
				);
			},
		);
	}

	async #settled(entry: AdapterEntry, managed: ManagedGeneration): Promise<void> {
		if (entry.current !== managed) return;
		if (this.#stopped || managed.controller.signal.aborted) return;
		entry.current = undefined;
		await this.#dispose(entry, managed);
	}

	async #failed(entry: AdapterEntry, managed: ManagedGeneration, error: unknown): Promise<void> {
		if (managed.failureHandled) return;
		managed.failureHandled = true;
		if (this.#stopped || managed.controller.signal.aborted || entry.current !== managed) {
			// Teardown-originated faults are operational noise, never a reason to upgrade exit status.
			if (!managed.handle) {
				this.#log(
					`adapter_factory_failed_after_stop adapter=${entry.name} generation=${managed.id} error=${describe(error)}`,
				);
				await this.#dispose(entry, managed);
			}
			return;
		}

		const uptime = managed.startedAt === undefined ? 0 : Math.max(0, this.#now() - managed.startedAt);
		entry.failures = uptime >= HEALTHY_UPTIME_MS ? 1 : entry.failures + 1;
		const decision = restartDecision(entry.failures, uptime, this.#random);
		if (decision.action === "escalate") {
			entry.escalated = true;
			this.#log(`adapter_escalated adapter=${entry.name} failures=5`);
			try {
				// The daemon upgrades its monotonic status before it owns the memoized shutdown.
				await this.#escalate(entry.name);
			} catch (escalationError) {
				this.#log(`adapter_escalation_error adapter=${entry.name} error=${describe(escalationError)}`);
			}
			await this.#dispose(entry, managed);
			return;
		}

		this.#log(
			`adapter_failed adapter=${entry.name} generation=${managed.id} reason=${describe(error)} restartInMs=${decision.delayMs}`,
		);
		await this.#dispose(entry, managed);
		if (entry.current === managed) entry.current = undefined;
		if (!this.#stopped) this.#scheduleRestart(entry, decision.delayMs);
	}

	#scheduleRestart(entry: AdapterEntry, delayMs: number): void {
		let restart!: Promise<void>;
		restart = (async () => {
			try {
				await this.#delay(delayMs, this.#stopController.signal);
				if (!this.#stopped && !entry.escalated && !entry.current) this.#launch(entry);
			} catch (error) {
				if (!isAbortError(error))
					this.#log(`adapter_restart_wait_failed adapter=${entry.name} error=${describe(error)}`);
			} finally {
				if (entry.restarting === restart) entry.restarting = undefined;
			}
		})();
		entry.restarting = restart;
		void restart.catch(() => {});
	}

	async #dispose(entry: AdapterEntry, managed: ManagedGeneration, budgetMs = this.#stopTimeoutMs): Promise<void> {
		managed.disposed ??= (async () => {
			managed.controller.abort();
			const work = Promise.allSettled([
				...(managed.handle ? [Promise.resolve().then(async () => await managed.handle?.stop())] : []),
				this.#drainTracked(managed),
			]).then(() => undefined);
			const completed = await completesBefore(work, budgetMs);
			if (!completed) this.#log(`adapter_stop_timeout adapter=${entry.name}`);
			managed.port.close();
			this.#log(`adapter_stopped adapter=${entry.name} generation=${managed.id}`);
		})();
		await managed.disposed;
	}

	async #drainTracked(managed: ManagedGeneration): Promise<void> {
		while (managed.tasks.size > 0) await Promise.allSettled([...managed.tasks]);
	}
}

function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
	return new Promise((resolve, reject) => {
		if (signal.aborted) {
			reject(abortError());
			return;
		}
		const timer = setTimeout(() => {
			signal.removeEventListener("abort", onAbort);
			resolve();
		}, ms);
		const onAbort = () => {
			clearTimeout(timer);
			signal.removeEventListener("abort", onAbort);
			reject(abortError());
		};
		signal.addEventListener("abort", onAbort, { once: true });
	});
}

function completesBefore(task: Promise<void>, timeoutMs: number): Promise<boolean> {
	return new Promise((resolve) => {
		let settled = false;
		const timer = setTimeout(() => {
			if (!settled) resolve(false);
		}, timeoutMs);
		void task.then(() => {
			settled = true;
			clearTimeout(timer);
			resolve(true);
		});
	});
}

function abortError(): Error {
	return Object.assign(new Error("The operation was aborted"), { name: "AbortError" });
}

function isAbortError(error: unknown): boolean {
	return error instanceof Error && error.name === "AbortError";
}

function describe(error: unknown): string {
	return sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "unknown_error";
}
