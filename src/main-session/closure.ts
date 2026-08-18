import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as path from "node:path";
import { randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import * as readline from "node:readline";
import { fileURLToPath } from "node:url";
import type { WayCoreHandle } from "../native-loader";

export type ClosureClass = "interactive" | "batch";

/** One in-daemon corpus closure. Paths are literal, corpus-relative paths. */
export interface ClosureRequest {
	readonly sessionId: string;
	readonly corpusPath: string;
	readonly label: string;
	readonly class?: ClosureClass;
	readonly paths: readonly string[];
	readonly commitMessage: string;
	readonly remote?: string;
}

export interface ClosureResult {
	readonly leaseId: string;
	readonly fencingToken: string;
	readonly committed: boolean;
}

export type ClosureStep = "after_acquire" | "after_pull" | "after_stage" | "after_commit" | "after_push";

export interface ClosureHookContext {
	readonly step: ClosureStep;
	readonly request: ClosureRequest;
	readonly leaseId: string;
	readonly fencingToken: string;
	readonly childPid: number;
	readonly childPgid: number;
	killChild(): Promise<void>;
}

/** Test-only hooks model crash boundaries without exposing a user-facing CLI. */
export interface ClosureExecutorOptions {
	readonly core: WayCoreHandle;
	readonly ttlMs?: number;
	readonly acquireWaitMs?: number;
	readonly heartbeatMs?: number;
	readonly hooks?: Partial<Record<ClosureStep, (context: ClosureHookContext) => void | Promise<void>>>;
}

export interface ClosureExecutor {
	execute(request: ClosureRequest): Promise<ClosureResult>;
	shutdown(): Promise<void>;
}

export class ClosureError extends Error {
	readonly code?: number;

	constructor(message: string, code?: number) {
		super(message);
		this.name = "ClosureError";
		this.code = code;
	}
}

interface WorkerResult {
	readonly exitCode: number | null;
	readonly signal: string | null;
	readonly stdout: string;
	readonly stderr: string;
}

interface PendingWorkerCommand {
	readonly id: number;
	readonly resolve: (result: WorkerResult) => void;
	readonly reject: (error: Error) => void;
}

interface ActiveClosure {
	readonly leaseId: string;
	readonly fencingToken: string;
	readonly worker: ClosureWorker;
	readonly request: ClosureRequest;
	readonly childPgid: number;
	cancelled?: ClosureError;
	heartbeat?: ReturnType<typeof setInterval>;
	temporaryIndex?: string;
}

interface ClosureRecoveryMarker {
	readonly sessionId: string;
	readonly corpusPath: string;
	readonly baseHead: string;
	readonly baseTree: string;
	readonly stagedTree: string;
}

const CLOSURE_RECOVERY_KEY = "gitlock_closure_recovery";

interface WorkerRunInstruction {
	readonly type: "run";
	readonly id: number;
	readonly args: string[];
	readonly cwd: string;
	readonly env?: Record<string, string>;
}

interface WorkerShutdownInstruction {
	readonly type: "shutdown";
}

const executorsByCore = new WeakMap<object, Set<ClosureExecutorImpl>>();

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorCode(error: unknown): number | undefined {
	const message = error instanceof Error ? error.message : String(error);
	const match = /(?:^|\s)(120[0-8])\b/.exec(message);
	return match ? Number(match[1]) : undefined;
}

function nativeClosureError(error: unknown): ClosureError {
	if (error instanceof ClosureError) return error;
	const message = error instanceof Error ? error.message : String(error);
	return new ClosureError(message, errorCode(error));
}

function workerEntrypoint(): string[] {
	const sourceEntrypoint = fileURLToPath(new URL("../main.ts", import.meta.url));
	// Tests and source-mode development run Bun itself. A compiled `way` binary
	// re-execs itself, where the environment flag selects the hidden worker path.
	if (path.basename(process.execPath).startsWith("bun") && fs.existsSync(sourceEntrypoint)) {
		return [process.execPath, sourceEntrypoint];
	}
	return [process.execPath];
}

class ClosureWorker {
	readonly #child: ChildProcess;
	readonly #ready: Promise<void>;
	readonly #closed: Promise<void>;
	#resolveReady!: () => void;
	#rejectReady!: (error: Error) => void;
	#resolveClosed!: () => void;
	#buffer = "";
	#pending: PendingWorkerCommand | undefined;
	#nextId = 1;
	#closedState = false;
	#pgid: number | undefined;

	private constructor(child: ChildProcess) {
		this.#child = child;
		this.#ready = new Promise<void>((resolve, reject) => {
			this.#resolveReady = resolve;
			this.#rejectReady = reject;
		});
		this.#closed = new Promise<void>(resolve => {
			this.#resolveClosed = resolve;
		});
		child.stdout?.setEncoding("utf8");
		child.stdout?.on("data", chunk => this.receive(String(chunk)));
		child.stderr?.resume();
		child.once("error", error => this.close(new ClosureError(`closure worker failed: ${error.message}`)));
		child.once("close", () => this.close(new ClosureError("closure worker exited before completing the Git command")));
	}

	static async start(): Promise<ClosureWorker> {
		const command = workerEntrypoint();
		const child = spawn(command[0] as string, command.slice(1), {
			detached: true,
			env: { ...process.env, WAY_INTERNAL_CLOSURE_WORKER: "1" },
			stdio: ["pipe", "pipe", "pipe"],
		});
		const worker = new ClosureWorker(child);
		await worker.waitReady();
		return worker;
	}

	get pid(): number {
		if (!this.#child.pid) throw new ClosureError("closure worker did not expose a PID");
		return this.#child.pid;
	}

	setProcessGroup(pgid: number): void {
		this.#pgid = pgid;
	}

	async run(args: readonly string[], cwd: string, env?: Record<string, string>): Promise<WorkerResult> {
		if (this.#closedState) throw new ClosureError("closure worker is already closed");
		if (this.#pending) throw new ClosureError("closure worker received concurrent Git commands");
		const id = this.#nextId++;
		return await new Promise<WorkerResult>((resolve, reject) => {
			this.#pending = { id, resolve, reject };
			try {
				this.send({ type: "run", id, args: [...args], cwd, ...(env ? { env } : {}) });
			} catch (error) {
				this.#pending = undefined;
				reject(nativeClosureError(error));
			}
		});
	}

	async shutdown(): Promise<void> {
		if (this.#closedState) return;
		try {
			this.send({ type: "shutdown" });
			this.#child.stdin?.end();
			await this.waitForClose(2_000);
		} catch {
			await this.terminate();
		}
	}

	async terminate(): Promise<void> {
		if (this.#closedState) return;
		const pgid = this.#pgid ?? this.pid;
		try {
			process.kill(-pgid, "SIGKILL");
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
		}
		await this.waitForClose(5_000);
	}

	private async waitReady(): Promise<void> {
		await this.withTimeout(this.#ready, 5_000, "closure worker did not become ready");
	}

	private async waitForClose(timeoutMs: number): Promise<void> {
		await this.withTimeout(this.#closed, timeoutMs, "closure worker process group did not reap");
	}

	private async withTimeout(promise: Promise<void>, timeoutMs: number, message: string): Promise<void> {
		let timeout: ReturnType<typeof setTimeout> | undefined;
		try {
			await Promise.race([
				promise,
				new Promise<void>((_, reject) => {
					timeout = setTimeout(() => reject(new ClosureError(message)), timeoutMs);
				}),
			]);
		} finally {
			if (timeout) clearTimeout(timeout);
		}
	}

	private send(instruction: WorkerRunInstruction | WorkerShutdownInstruction): void {
		if (this.#closedState || !this.#child.stdin) throw new ClosureError("closure worker stdin is unavailable");
		this.#child.stdin.write(`${JSON.stringify(instruction)}\n`);
	}

	private receive(chunk: string): void {
		this.#buffer += chunk;
		for (;;) {
			const newline = this.#buffer.indexOf("\n");
			if (newline < 0) return;
			const line = this.#buffer.slice(0, newline);
			this.#buffer = this.#buffer.slice(newline + 1);
			let message: unknown;
			try {
				message = JSON.parse(line);
			} catch {
				continue;
			}
			if (!isRecord(message)) continue;
			if (message.type === "ready") {
				this.#resolveReady();
				continue;
			}
			if (message.type !== "result" || typeof message.id !== "number" || !this.#pending || message.id !== this.#pending.id) {
				continue;
			}
			const pending = this.#pending;
			this.#pending = undefined;
			pending.resolve({
				exitCode: typeof message.exitCode === "number" ? message.exitCode : null,
				signal: typeof message.signal === "string" ? message.signal : null,
				stdout: typeof message.stdout === "string" ? message.stdout : "",
				stderr: typeof message.stderr === "string" ? message.stderr : "",
			});
		}
	}

	private close(error: Error): void {
		if (this.#closedState) return;
		this.#closedState = true;
		this.#rejectReady(error);
		if (this.#pending) {
			this.#pending.reject(error);
			this.#pending = undefined;
		}
		this.#resolveClosed();
	}
}

function workerMessage(value: Record<string, unknown>): void {
	process.stdout.write(`${JSON.stringify(value)}\n`);
}

function boundedAppend(current: string, chunk: string): string {
	const next = `${current}${chunk}`;
	return next.length <= 64 * 1024 ? next : next.slice(next.length - 64 * 1024);
}

async function runWorkerGit(instruction: WorkerRunInstruction): Promise<WorkerResult> {
	return await new Promise<WorkerResult>(resolve => {
		let stdout = "";
		let stderr = "";
		let settled = false;
		const finish = (result: WorkerResult) => {
			if (settled) return;
			settled = true;
			resolve(result);
		};
		const child = spawn("git", instruction.args, {
			cwd: instruction.cwd,
			env: { ...process.env, ...instruction.env },
			stdio: ["ignore", "pipe", "pipe"],
		});
		child.stdout?.setEncoding("utf8");
		child.stderr?.setEncoding("utf8");
		child.stdout?.on("data", chunk => {
			stdout = boundedAppend(stdout, String(chunk));
		});
		child.stderr?.on("data", chunk => {
			stderr = boundedAppend(stderr, String(chunk));
		});
		child.once("error", error => finish({ exitCode: null, signal: null, stdout, stderr: boundedAppend(stderr, error.message) }));
		child.once("close", (exitCode, signal) => finish({ exitCode, signal, stdout, stderr }));
	});
}

/** Hidden worker entrypoint used only by the in-daemon executor. */
export async function runClosureWorker(): Promise<void> {
	workerMessage({ type: "ready" });
	const input = readline.createInterface({ input: process.stdin, crlfDelay: Number.POSITIVE_INFINITY });
	for await (const line of input) {
		let instruction: unknown;
		try {
			instruction = JSON.parse(line);
		} catch {
			continue;
		}
		if (!isRecord(instruction)) continue;
		if (instruction.type === "shutdown") return;
		if (
			instruction.type !== "run" ||
			typeof instruction.id !== "number" ||
			typeof instruction.cwd !== "string" ||
			!Array.isArray(instruction.args) ||
			instruction.args.some(argument => typeof argument !== "string")
		) {
			continue;
		}
		const env = isRecord(instruction.env)
			? Object.fromEntries(Object.entries(instruction.env).filter((entry): entry is [string, string] => typeof entry[1] === "string"))
			: undefined;
		const result = await runWorkerGit({
			type: "run",
			id: instruction.id,
			cwd: instruction.cwd,
			args: instruction.args,
			...(env ? { env } : {}),
		});
		workerMessage({ type: "result", id: instruction.id, ...result });
	}
}

class ClosureExecutorImpl implements ClosureExecutor {
	readonly #core: WayCoreHandle;
	readonly #ttlMs: number;
	readonly #acquireWaitMs: number;
	readonly #heartbeatMs: number;
	readonly #hooks: ClosureExecutorOptions["hooks"];
	readonly #active = new Map<string, ActiveClosure>();
	readonly #running = new Set<Promise<ClosureResult>>();
	#disposed = false;

	constructor(options: ClosureExecutorOptions) {
		this.#core = options.core;
		this.#ttlMs = options.ttlMs ?? 120_000;
		this.#acquireWaitMs = options.acquireWaitMs ?? 30_000;
		this.#heartbeatMs = options.heartbeatMs ?? Math.min(1_000, Math.max(100, Math.floor(this.#ttlMs / 3)));
		this.#hooks = options.hooks;
		if (!Number.isSafeInteger(this.#ttlMs) || this.#ttlMs < 5_000 || this.#ttlMs > 600_000) {
			throw new ClosureError("ttlMs must be in 5000..=600000", 1202);
		}
		if (!Number.isSafeInteger(this.#acquireWaitMs) || this.#acquireWaitMs < 0 || this.#acquireWaitMs > 300_000) {
			throw new ClosureError("acquireWaitMs must be in 0..=300000");
		}
		if (!Number.isSafeInteger(this.#heartbeatMs) || this.#heartbeatMs < 25) {
			throw new ClosureError("heartbeatMs must be an integer of at least 25");
		}
		const key = this.#core as object;
		const executors = executorsByCore.get(key) ?? new Set<ClosureExecutorImpl>();
		executors.add(this);
		executorsByCore.set(key, executors);
	}

	async execute(request: ClosureRequest): Promise<ClosureResult> {
		if (this.#disposed) throw new ClosureError("closure executor is shut down");
		const task = this.executeInner(request);
		this.#running.add(task);
		try {
			return await task;
		} finally {
			this.#running.delete(task);
		}
	}

	async shutdown(): Promise<void> {
		if (this.#disposed) return;
		this.#disposed = true;
		const active = [...this.#active.values()];
		for (const closure of active) this.cancel(closure, new ClosureError("daemon shutdown cancelled closure"));
		await Promise.all(active.map(async closure => await closure.worker.terminate()));
		await Promise.allSettled([...this.#running]);
		const key = this.#core as object;
		const executors = executorsByCore.get(key);
		executors?.delete(this);
		if (executors?.size === 0) executorsByCore.delete(key);
	}

	receiveRevocation(leaseId: string): void {
		const active = this.#active.get(leaseId);
		if (active) this.cancel(active, new ClosureError("closure lease was revoked", 1202));
	}

	private async executeInner(request: ClosureRequest): Promise<ClosureResult> {
		const normalized = validateRequest(request);
		const worker = await ClosureWorker.start();
		let active: ActiveClosure | undefined;
		let result: ClosureResult | undefined;
		let failure: Error | undefined;
		try {
			const identity = this.#core.processIdentity(worker.pid);
			worker.setProcessGroup(identity.pgid);
			const lease = await this.acquire(normalized, identity);
			active = {
				leaseId: lease.leaseId,
				fencingToken: lease.fencingToken,
				worker,
				request: normalized,
				childPgid: identity.pgid,
			};
			this.#active.set(active.leaseId, active);
			this.startHeartbeat(active);
			await this.assertFence(active);
			await this.hook("after_acquire", active);

			await this.recoverCommittedIndex(active);
			const indexState = await this.git(active, ["diff", "--cached", "--quiet"]);
			if (indexState.exitCode === 1) throw new ClosureError("corpus git index is dirty", 1206);
			this.assertGit(indexState, ["diff", "--cached", "--quiet"]);

			this.assertGit(await this.git(active, ["pull", "--ff-only"]), ["pull", "--ff-only"]);
			await this.hook("after_pull", active);

			active.temporaryIndex = await this.prepareTemporaryIndex(active);
			const indexEnvironment = { GIT_INDEX_FILE: active.temporaryIndex };
			this.assertGit(await this.git(active, ["add", "--", ...normalized.paths], indexEnvironment), ["add", "--", ...normalized.paths]);
			await this.writeRecoveryMarker(active, indexEnvironment);
			await this.hook("after_stage", active);

			const staged = await this.git(active, ["diff", "--cached", "--quiet"], indexEnvironment);
			let committed = false;
			if (staged.exitCode === 1) {
				this.assertGit(await this.git(active, ["commit", "-m", normalized.commitMessage], indexEnvironment), ["commit"]);
				// The temporary index keeps a pre-commit kill from dirtying the
				// real index. Once the commit is durable, synchronize the real index
				// before exposing the post-commit crash boundary.
				this.assertGit(await this.git(active, ["read-tree", "HEAD"]), ["read-tree", "HEAD"]);
				committed = true;
			} else {
				this.assertGit(staged, ["diff", "--cached", "--quiet"]);
			}
			await this.hook("after_commit", active);

			const push = normalized.remote ? ["push", normalized.remote] : ["push"];
			this.assertGit(await this.git(active, push), push);
			await this.hook("after_push", active);
			await this.assertFence(active);
			this.clearRecoveryMarker(active);
			if (active.cancelled) throw active.cancelled;
			result = { leaseId: active.leaseId, fencingToken: active.fencingToken, committed };
		} catch (error) {
			failure = active?.cancelled ?? nativeClosureError(error);
		} finally {
			const cleanupFailure = await this.cleanup(worker, active);
			if (!failure && cleanupFailure) failure = cleanupFailure;
		}
		if (failure) throw failure;
		if (!result) throw new ClosureError("closure completed without a result");
		return result;
	}

	private async acquire(
		request: ClosureRequest,
		identity: { pid: number; pidStartTime: string; pgid: number; pgidStartTime?: string },
	): Promise<{ leaseId: string; fencingToken: string }> {
		const deadline = Date.now() + this.#acquireWaitMs;
		for (;;) {
			if (this.#disposed) throw new ClosureError("daemon shutdown cancelled closure");
			try {
				const lease = this.#core.lockAcquire({
					label: request.label,
					class: request.class,
					waitMs: 0,
					ttlMs: this.#ttlMs,
					holder: {
						holderKind: "in_daemon",
						sessionId: request.sessionId,
						pid: identity.pid,
						pidStartTime: identity.pidStartTime,
						pgid: identity.pgid,
						...(identity.pgidStartTime ? { pgidStartTime: identity.pgidStartTime } : {}),
						connId: "way.in_daemon_executor.v1",
					},
				});
				return { leaseId: lease.leaseId, fencingToken: lease.fencingToken };
			} catch (error) {
				if (errorCode(error) !== 1200 || Date.now() >= deadline) throw nativeClosureError(error);
				await Bun.sleep(Math.min(50, Math.max(1, deadline - Date.now())));
			}
		}
	}

	private startHeartbeat(active: ActiveClosure): void {
		active.heartbeat = setInterval(() => this.heartbeat(active), this.#heartbeatMs);
	}

	private heartbeat(active: ActiveClosure): void {
		if (active.cancelled || !this.#active.has(active.leaseId)) return;
		try {
			dispatchCoreRevocations(this.#core);
			if (active.cancelled) return;
			this.#core.lockRenew(active.leaseId);
			dispatchCoreRevocations(this.#core);
			if (active.cancelled || !this.#core.lockFencingValid(active.leaseId, active.fencingToken)) {
				this.cancel(active, new ClosureError("closure fencing token is no longer valid", 1202));
			}
		} catch (error) {
			this.cancel(active, nativeClosureError(error));
		}
	}

	private async assertFence(active: ActiveClosure): Promise<void> {
		dispatchCoreRevocations(this.#core);
		if (active.cancelled || !this.#core.lockFencingValid(active.leaseId, active.fencingToken)) {
			this.cancel(active, new ClosureError("closure fencing token is no longer valid", 1202));
			throw active.cancelled ?? new ClosureError("closure fencing token is no longer valid", 1202);
		}
	}

	private async git(active: ActiveClosure, args: readonly string[], env?: Record<string, string>): Promise<WorkerResult> {
		await this.assertFence(active);
		const result = await active.worker.run(args, active.request.corpusPath, env);
		if (active.cancelled) throw active.cancelled;
		await this.assertFence(active);
		return result;
	}

	private assertGit(result: WorkerResult, args: readonly string[]): void {
		if (result.exitCode === 0) return;
		const detail = result.stderr.trim() || result.stdout.trim() || result.signal || "unknown Git failure";
		throw new ClosureError(`git ${args[0] ?? "command"} failed: ${detail}`);
	}

	private async prepareTemporaryIndex(active: ActiveClosure): Promise<string> {
		const gitDirectory = await this.git(active, ["rev-parse", "--git-dir"]);
		this.assertGit(gitDirectory, ["rev-parse", "--git-dir"]);
		const directory = gitDirectory.stdout.trim();
		if (!directory) throw new ClosureError("git did not report its index directory");
		const temporaryIndex = path.resolve(active.request.corpusPath, directory, `way-closure-index-${process.pid}-${randomUUID()}`);
		await fsp.rm(temporaryIndex, { force: true });
		this.assertGit(await this.git(active, ["read-tree", "HEAD"], { GIT_INDEX_FILE: temporaryIndex }), ["read-tree", "HEAD"]);
		return temporaryIndex;
	}

	private recoveryMarker(): { raw: string; marker: ClosureRecoveryMarker } | undefined {
		const entry = this.#core.gatewayMetaRead([CLOSURE_RECOVERY_KEY]).entries[0];
		if (!entry?.value) return undefined;
		try {
			const parsed = JSON.parse(entry.value) as unknown;
			if (
				!isRecord(parsed) ||
				typeof parsed.sessionId !== "string" ||
				typeof parsed.corpusPath !== "string" ||
				typeof parsed.baseHead !== "string" ||
				typeof parsed.baseTree !== "string" ||
				typeof parsed.stagedTree !== "string"
			) {
				return undefined;
			}
			return {
				raw: entry.value,
				marker: {
					sessionId: parsed.sessionId,
					corpusPath: parsed.corpusPath,
					baseHead: parsed.baseHead,
					baseTree: parsed.baseTree,
					stagedTree: parsed.stagedTree,
				},
			};
		} catch {
			return undefined;
		}
	}

	private async recoverCommittedIndex(active: ActiveClosure): Promise<void> {
		const indexState = await this.git(active, ["diff", "--cached", "--quiet"]);
		if (indexState.exitCode === 0) return;
		if (indexState.exitCode !== 1) {
			this.assertGit(indexState, ["diff", "--cached", "--quiet"]);
			return;
		}
		const persisted = this.recoveryMarker();
		if (
			!persisted ||
			persisted.marker.sessionId !== active.request.sessionId ||
			persisted.marker.corpusPath !== active.request.corpusPath
		) {
			return;
		}
		const indexTree = await this.git(active, ["write-tree"]);
		const baseTree = await this.git(active, ["rev-parse", `${persisted.marker.baseHead}^{tree}`]);
		const head = await this.git(active, ["rev-parse", "HEAD"]);
		const headTree = await this.git(active, ["rev-parse", "HEAD^{tree}"]);
		const parent = await this.git(active, ["rev-parse", "HEAD^"]);
		for (const result of [indexTree, baseTree, head, headTree, parent]) this.assertGit(result, ["rev-parse"]);
		if (
			indexTree.stdout.trim() !== persisted.marker.baseTree ||
			baseTree.stdout.trim() !== persisted.marker.baseTree ||
			head.stdout.trim() === persisted.marker.baseHead ||
			headTree.stdout.trim() !== persisted.marker.stagedTree ||
			parent.stdout.trim() !== persisted.marker.baseHead
		) {
			return;
		}
		this.assertGit(await this.git(active, ["read-tree", "HEAD"]), ["read-tree", "HEAD"]);
	}

	private async writeRecoveryMarker(active: ActiveClosure, indexEnvironment: Record<string, string>): Promise<void> {
		const baseHead = await this.git(active, ["rev-parse", "HEAD"]);
		const baseTree = await this.git(active, ["rev-parse", "HEAD^{tree}"]);
		const stagedTree = await this.git(active, ["write-tree"], indexEnvironment);
		for (const result of [baseHead, baseTree, stagedTree]) this.assertGit(result, ["rev-parse"]);
		const marker: ClosureRecoveryMarker = {
			sessionId: active.request.sessionId,
			corpusPath: active.request.corpusPath,
			baseHead: baseHead.stdout.trim(),
			baseTree: baseTree.stdout.trim(),
			stagedTree: stagedTree.stdout.trim(),
		};
		const raw = JSON.stringify(marker);
		this.#core.gatewayMetaTransaction({
			expected: [],
			puts: [{ key: CLOSURE_RECOVERY_KEY, value: raw }],
			deletes: [],
		});
	}

	private clearRecoveryMarker(active: ActiveClosure): void {
		const persisted = this.recoveryMarker();
		if (
			!persisted ||
			persisted.marker.sessionId !== active.request.sessionId ||
			persisted.marker.corpusPath !== active.request.corpusPath
		) {
			return;
		}
		const cleared = this.#core.gatewayMetaTransaction({
			expected: [{ key: CLOSURE_RECOVERY_KEY, value: persisted.raw }],
			puts: [],
			deletes: [CLOSURE_RECOVERY_KEY],
		});
		if (!cleared.applied) throw new ClosureError("closure recovery marker changed concurrently");
	}

	private async hook(step: ClosureStep, active: ActiveClosure): Promise<void> {
		const hook = this.#hooks?.[step];
		if (!hook) return;
		await hook({
			step,
			request: active.request,
			leaseId: active.leaseId,
			fencingToken: active.fencingToken,
			childPid: active.worker.pid,
			childPgid: active.childPgid,
			killChild: async () => {
				this.cancel(active, new ClosureError(`closure child killed at ${step}`));
				await active.worker.terminate();
			},
		});
		if (active.cancelled) throw active.cancelled;
		await this.assertFence(active);
	}

	private cancel(active: ActiveClosure, error: ClosureError): void {
		if (active.cancelled) return;
		active.cancelled = error;
		void active.worker.terminate().catch(() => {
			// The close handler/reap timeout is observed by the owning closure path.
		});
	}

	private async cleanup(worker: ClosureWorker, active: ActiveClosure | undefined): Promise<Error | undefined> {
		let failure: Error | undefined;
		if (active?.heartbeat) clearInterval(active.heartbeat);
		if (active) this.#active.delete(active.leaseId);
		try {
			if (active?.cancelled) await worker.terminate();
			else await worker.shutdown();
		} catch (error) {
			failure = nativeClosureError(error);
		}
		if (active) {
			try {
				this.#core.lockRelease(active.leaseId);
			} catch (error) {
				const code = errorCode(error);
				if (code !== 1202 && code !== 1203 && !failure) failure = nativeClosureError(error);
			}
			if (active.temporaryIndex) {
				await Promise.all([
					fsp.rm(active.temporaryIndex, { force: true }),
					fsp.rm(`${active.temporaryIndex}.lock`, { force: true }),
				]);
			}
		} else {
			try {
				await worker.terminate();
			} catch (error) {
				failure ??= nativeClosureError(error);
			}
		}
		return failure;
	}
}

function dispatchCoreRevocations(core: WayCoreHandle): void {
	const revoked = core.lockDrainRevocations();
	if (revoked.length === 0) return;
	const executors = executorsByCore.get(core as object);
	for (const leaseId of revoked) {
		for (const executor of executors ?? []) executor.receiveRevocation(leaseId);
	}
}

function validateRequest(request: ClosureRequest): ClosureRequest {
	if (!request.sessionId.trim()) throw new ClosureError("sessionId must not be empty");
	if (!request.label.trim()) throw new ClosureError("label must not be empty");
	if (request.class !== undefined && request.class !== "interactive" && request.class !== "batch") {
		throw new ClosureError("class must be interactive or batch");
	}
	if (!request.commitMessage.trim()) throw new ClosureError("commitMessage must not be empty");
	if (request.remote !== undefined && !request.remote.trim()) throw new ClosureError("remote must not be empty when provided");
	if (!request.corpusPath.trim()) throw new ClosureError("corpusPath must not be empty");
	if (request.paths.length === 0) throw new ClosureError("paths must list at least one explicit path");
	const corpusPath = path.resolve(request.corpusPath);
	const paths = request.paths.map(candidate => {
		if (!candidate || path.isAbsolute(candidate) || candidate.startsWith(":")) {
			throw new ClosureError("paths must be literal corpus-relative paths");
		}
		const absolute = path.resolve(corpusPath, candidate);
		const relative = path.relative(corpusPath, absolute);
		if (!relative || relative === ".." || relative.startsWith(`..${path.sep}`)) {
			throw new ClosureError("paths must remain inside the corpus and cannot name the corpus root");
		}
		return relative.split(path.sep).join("/");
	});
	if (new Set(paths).size !== paths.length) throw new ClosureError("paths must not contain duplicates");
	return { ...request, corpusPath, paths };
}

/** Creates the internal-only executor used by the daemon and P5 drills. */
export function createClosureExecutor(options: ClosureExecutorOptions): ClosureExecutor {
	return new ClosureExecutorImpl(options);
}
