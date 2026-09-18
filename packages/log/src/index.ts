import { appendFileSync, mkdirSync, renameSync, statSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { format } from "node:util";

export const DEFAULT_LOG_ROTATION_BYTES = 10 * 1024 * 1024;
export const DEFAULT_LOG_RETAINED_FILES = 5;
export const DEFAULT_LOG_HEARTBEAT_INTERVAL_MS = 60 * 60 * 1000;

type LogLevel = "error" | "log";

/** Synchronous filesystem seam; tests substitute an in-memory one. */
export interface StructuredLogFileSystem {
	readonly mkdir: (path: string) => void;
	/** Size of the live sink in bytes; throws when it does not exist yet. */
	readonly size: (path: string) => number;
	readonly append: (path: string, data: string) => void;
	readonly rename: (from: string, to: string) => void;
	readonly unlink: (path: string) => void;
}

export interface StructuredLoggingOptions {
	/** Sink this process owns. It must not also be a service-manager stdout redirect target. */
	readonly path: string;
	/** Rotate before a line would push the live file past this size. */
	readonly rotationBytes?: number;
	/** Rotated suffixes kept (`.1` through `.N`); zero truncates instead. */
	readonly retainedFiles?: number;
	/** Zero disables the heartbeat. */
	readonly heartbeatIntervalMs?: number;
	/** Appended to the heartbeat line after `uptime=`. */
	readonly heartbeatDetails?: () => string;
	readonly now?: () => number;
	readonly filesystem?: StructuredLogFileSystem;
	/** Console this wraps; production uses the global one. */
	readonly console?: Pick<Console, "error" | "log">;
	readonly setInterval?: (handler: () => void, delayMs: number) => unknown;
	readonly clearInterval?: (timer: unknown) => void;
}

interface PendingRun {
	readonly level: LogLevel;
	readonly message: string;
	count: number;
	readonly firstAt: string;
	lastAt: string;
}

const DEFAULT_FILE_SYSTEM: StructuredLogFileSystem = {
	mkdir: (path) => mkdirSync(path, { recursive: true, mode: 0o700 }),
	size: (path) => statSync(path).size,
	append: (path, data) => appendFileSync(path, data, { encoding: "utf8", mode: 0o600 }),
	rename: renameSync,
	unlink: unlinkSync,
};

function renderedMessage(args: readonly unknown[]): string {
	if (args.length === 0) return "";
	return format(...(args as [string, ...unknown[]]));
}

function messageLines(message: string): string[] {
	const lines = message.replaceAll("\r\n", "\n").replaceAll("\r", "\n").split("\n");
	// `console.error`/`console.log` append their own final newline. Avoid
	// manufacturing a second blank record when the caller's string ends in one.
	if (lines.length > 1 && lines.at(-1) === "") lines.pop();
	return lines.length > 0 ? lines : [""];
}

function positiveInteger(value: number | undefined, fallback: number, minimum: number): number {
	if (value === undefined || !Number.isFinite(value)) return fallback;
	return Math.max(minimum, Math.floor(value));
}

function optionalInterval(value: number | undefined): number {
	if (value === undefined || !Number.isFinite(value)) return DEFAULT_LOG_HEARTBEAT_INTERVAL_MS;
	return Math.max(0, value);
}

/**
 * Install a timestamping, rotating, heartbeat-producing sink around the two
 * console methods used by the gateway and adapters. The returned disposer is
 * idempotent and restores the exact functions that were present at install.
 */
export function installStructuredLogging(options: StructuredLoggingOptions): () => void {
	const path = options.path;
	const rotationBytes = positiveInteger(options.rotationBytes, DEFAULT_LOG_ROTATION_BYTES, 1);
	const retainedFiles = positiveInteger(options.retainedFiles, DEFAULT_LOG_RETAINED_FILES, 0);
	const heartbeatIntervalMs = optionalInterval(options.heartbeatIntervalMs);
	const now = options.now ?? Date.now;
	const details = options.heartbeatDetails;
	const target = options.console ?? console;
	const fs = options.filesystem ?? DEFAULT_FILE_SYSTEM;
	let liveBytes = 0;
	const startedAtMs = now();
	let disposed = false;
	let pending: PendingRun | undefined;

	try {
		fs.mkdir(dirname(path));
	} catch {
		// The first append below remains best-effort; the original console stream
		// must continue to receive diagnostics even if the owned sink is unavailable.
	}
	try {
		liveBytes = Math.max(0, fs.size(path));
	} catch {
		liveBytes = 0;
	}

	const originalError = target.error;
	const originalLog = target.log;

	const append = (data: string): void => {
		try {
			fs.append(path, data);
			liveBytes += Buffer.byteLength(data, "utf8");
		} catch {
			// Logging must never take the service down. The journal echo below is
			// still attempted even when the owned sink is unwritable.
		}
	};

	const rotate = (): void => {
		try {
			if (retainedFiles === 0) {
				try {
					fs.unlink(path);
				} catch {
					// Absent live files are already rotated state.
				}
				liveBytes = 0;
				return;
			}
			for (let suffix = retainedFiles; suffix >= 1; suffix--) {
				const destination = `${path}.${suffix}`;
				if (suffix === retainedFiles) {
					try {
						fs.unlink(destination);
					} catch {
						// No file at the retention boundary is normal.
					}
				}
				if (suffix > 1) {
					try {
						fs.rename(`${path}.${suffix - 1}`, destination);
					} catch {
						// Missing lower suffixes are normal on early rotations.
					}
				}
			}
			try {
				fs.rename(path, `${path}.1`);
			} catch {
				// A missing live file needs no rename; the next append recreates it.
			}
		} finally {
			liveBytes = 0;
		}
	};

	const writeLine = (level: LogLevel, message: string, timestamp: string): void => {
		for (const line of messageLines(message)) {
			const data = `${timestamp} ${level} ${line}\n`;
			const bytes = Buffer.byteLength(data, "utf8");
			if (liveBytes > 0 && liveBytes + bytes > rotationBytes) rotate();
			append(data);
		}
	};

	const echo = (level: LogLevel, message: string, timestamp: string): void => {
		try {
			const prefixed = messageLines(message)
				.map((line) => `${timestamp} ${level} ${line}`)
				.join("\n");
			(level === "error" ? originalError : originalLog).call(target, prefixed);
		} catch {
			// A hostile test double must not disable the owned sink.
		}
	};

	const emit = (level: LogLevel, message: string, timestamp: string, echoOriginal = true): void => {
		writeLine(level, message, timestamp);
		if (echoOriginal) echo(level, message, timestamp);
	};

	const flushPending = (timestamp: string): void => {
		const run = pending;
		pending = undefined;
		if (!run || run.count <= 1) return;
		emit(run.level, `${run.message} x${run.count - 1} (identical, first=${run.firstAt} last=${run.lastAt})`, timestamp);
	};

	const receive = (level: LogLevel, args: readonly unknown[]): void => {
		if (disposed) {
			(level === "error" ? originalError : originalLog).apply(target, args as never);
			return;
		}
		const message = renderedMessage(args);
		const atMs = now();
		const timestamp = new Date(atMs).toISOString();
		if (pending && pending.level === level && pending.message === message) {
			pending.count += 1;
			pending.lastAt = timestamp;
			return;
		}
		flushPending(timestamp);
		emit(level, message, timestamp);
		pending = { level, message, count: 1, firstAt: timestamp, lastAt: timestamp };
	};

	const wrappedError = (...args: unknown[]): void => receive("error", args);
	const wrappedLog = (...args: unknown[]): void => receive("log", args);
	target.error = wrappedError as Console["error"];
	target.log = wrappedLog as Console["log"];

	let heartbeatTimer: unknown;
	if (heartbeatIntervalMs > 0) {
		const setTimer = options.setInterval ?? ((handler: () => void, delay: number) => setInterval(handler, delay));
		heartbeatTimer = setTimer(() => {
			if (disposed) return;
			const atMs = now();
			const timestamp = new Date(atMs).toISOString();
			flushPending(timestamp);
			let detail = "";
			try {
				detail = details?.() ?? "";
			} catch {
				detail = "";
			}
			const uptime = Math.max(0, Math.floor((atMs - startedAtMs) / 1000));
			emit("log", `service_alive uptime=${uptime}${detail ? ` ${detail}` : ""}`, timestamp);
		}, heartbeatIntervalMs);
		if (typeof heartbeatTimer === "object" && heartbeatTimer !== null && "unref" in heartbeatTimer) {
			(heartbeatTimer as { unref?: () => void }).unref?.();
		}
	}

	return (): void => {
		if (disposed) return;
		disposed = true;
		if (heartbeatTimer !== undefined) {
			const clearTimer = options.clearInterval ?? ((timer: unknown) => clearInterval(timer as never));
			try {
				clearTimer(heartbeatTimer);
			} catch {
				// A test timer seam may already have disposed itself.
			}
		}
		flushPending(new Date(now()).toISOString());
		if (target.error === wrappedError) target.error = originalError;
		if (target.log === wrappedLog) target.log = originalLog;
	};
}
