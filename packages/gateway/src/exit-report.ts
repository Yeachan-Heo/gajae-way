import { writeSync } from "node:fs";
import { sanitizeDiagnostic } from "./orchestrator/rebind";

/**
 * Why this process is going away. Every cause is a class, never free text: the
 * line has to be greppable from a journal that is read after the fact, and the
 * operator has to be able to count restarts per class (#182).
 */
export type ExitCause =
	| "boot_failure"
	| "broker_unreachable"
	| "uncaught_exception"
	| "unhandled_rejection"
	| "signal"
	| "unexpected_exit";

export type ExitWriter = (line: string) => void;

export interface ExitReporterTarget {
	on(event: string, listener: (...args: unknown[]) => void): unknown;
	exitCode?: number | string | null;
}

export interface ExitReporterOptions {
	readonly writer?: ExitWriter;
	readonly target?: ExitReporterTarget;
	readonly exit?: (code: number) => void;
}

export interface ExitReporter {
	/** Emit the single structured cause line, at most once per process. */
	report(cause: ExitCause, detail: unknown, code?: number): void;
}

/** The detail field is bounded so a multi-megabyte throwable cannot flood the journal. */
export const EXIT_DETAIL_MAX = 300;

/**
 * One structured line before the process goes away, written with a synchronous
 * syscall.
 *
 * `process.stderr.write` is asynchronous whenever stderr is a pipe — the
 * systemd journal, a supervisor's capture pipe, a log collector — so a line
 * queued there is dropped when the process exits immediately afterwards. That
 * is exactly the shape #182 reports: 131 restarts, `status=1` every time, and
 * not one line from the service itself in 2559 journal lines. `writeSync`
 * returns only after the kernel holds the bytes, so the cause survives the
 * crash it describes.
 */
export const DEFAULT_EXIT_WRITER: ExitWriter = (line) => {
	try {
		writeSync(2, line);
	} catch {
		// A reporter that throws would replace the real cause with its own, and
		// there is nothing left to write to anyway.
	}
};

function describeDetail(detail: unknown): string {
	if (detail instanceof Error) return detail.message;
	if (typeof detail === "string") return detail;
	if (detail === undefined || detail === null) return "";
	try {
		return JSON.stringify(detail) ?? String(detail);
	} catch {
		// Circular throwables: the class name still beats an empty field.
		return Object.prototype.toString.call(detail);
	}
}

export function renderExitReport(cause: ExitCause, detail: unknown, code?: number): string {
	// `sanitizeDiagnostic` strips control characters but keeps newlines, because
	// message bodies are allowed to be multi-line. This field is not: the whole
	// point is that one death is exactly one greppable line, so breaks fold into
	// spaces here rather than one level down.
	const cleaned = (sanitizeDiagnostic(describeDetail(detail)) || "no_detail").replace(/[\r\n]+/g, " ").trim();
	const managed = typeof code === "number" && Number.isInteger(code) && code >= 0 && code <= 255 ? ` code=${code}` : "";
	return `gateway_exit cause=${cause}${managed} detail=${(cleaned || "no_detail").slice(0, EXIT_DETAIL_MAX)}\n`;
}

/**
 * Installs the crash-exit reporter: uncaught exceptions, unhandled rejections,
 * and the exit hook. Exactly one line is emitted per process — the first cause
 * wins — so a throw that unwinds into the exit hook does not print twice and a
 * reader can count one line per death.
 *
 * A clean exit (status 0, nothing reported before it) stays silent on purpose:
 * a restart loop has to stay countable, and a line for every ordinary stop
 * would bury it.
 */
export function installExitReporter(options: ExitReporterOptions = {}): ExitReporter {
	const writer = options.writer ?? DEFAULT_EXIT_WRITER;
	const target = options.target ?? (process as ExitReporterTarget);
	const exit = options.exit ?? ((code: number) => process.exit(code));
	let reported = false;

	const report: ExitReporter["report"] = (cause, detail, code) => {
		if (reported) return;
		reported = true;
		writer(renderExitReport(cause, detail, code));
	};

	target.on("uncaughtException", (...args: unknown[]) => {
		report("uncaught_exception", args[0], 1);
		exit(1);
	});
	target.on("unhandledRejection", (...args: unknown[]) => {
		report("unhandled_rejection", args[0], 1);
		exit(1);
	});
	// Last resort: a process that exits with none of the handlers above having
	// fired still records *that* it died and with which status. This is the line
	// the #182 journal was missing entirely.
	target.on("exit", (...args: unknown[]) => {
		const code = typeof args[0] === "number" ? args[0] : 0;
		if (code !== 0) report("unexpected_exit", `exit status ${code}`, code);
	});

	return { report };
}
