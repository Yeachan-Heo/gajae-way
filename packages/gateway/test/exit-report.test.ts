import { expect, test } from "bun:test";
import { EXIT_DETAIL_MAX, installExitReporter, renderExitReport } from "../src/exit-report";

class FakeTarget {
	readonly listeners = new Map<string, Array<(...args: unknown[]) => void>>();
	exitCode: number | null = null;

	on(event: string, listener: (...args: unknown[]) => void): this {
		const existing = this.listeners.get(event) ?? [];
		existing.push(listener);
		this.listeners.set(event, existing);
		return this;
	}

	emit(event: string, ...args: unknown[]): void {
		for (const listener of this.listeners.get(event) ?? []) listener(...args);
	}
}

function harness() {
	const target = new FakeTarget();
	const lines: string[] = [];
	const exits: number[] = [];
	const reporter = installExitReporter({
		target,
		writer: (line) => lines.push(line),
		exit: (code) => exits.push(code),
	});
	return { target, lines, exits, reporter };
}

test("an uncaught exception emits exactly one structured line before the process dies", () => {
	const { target, lines, exits } = harness();
	target.emit("uncaughtException", new Error("broker handshake failed"));
	expect(lines).toEqual(["gateway_exit cause=uncaught_exception code=1 detail=broker handshake failed\n"]);
	expect(exits).toEqual([1]);
});

test("the exit hook does not double-report a cause that was already written", () => {
	const { target, lines } = harness();
	target.emit("uncaughtException", new Error("boom"));
	target.emit("exit", 1);
	expect(lines).toHaveLength(1);
});

test("a rejected promise is reported as its own class, not as an unexpected exit", () => {
	const { target, lines, exits } = harness();
	target.emit("unhandledRejection", new Error("socket closed"));
	expect(lines).toEqual(["gateway_exit cause=unhandled_rejection code=1 detail=socket closed\n"]);
	expect(exits).toEqual([1]);
});

test("an exit with no cause handler records that it died and with which status", () => {
	const { target, lines } = harness();
	target.emit("exit", 1);
	expect(lines).toEqual(["gateway_exit cause=unexpected_exit code=1 detail=exit status 1\n"]);
});

test("a clean exit is silent so a restart loop stays countable", () => {
	const { target, lines } = harness();
	target.emit("exit", 0);
	expect(lines).toEqual([]);
});

test("a signalled stop reports the signal as a clean cause", () => {
	const { reporter, lines } = harness();
	reporter.report("signal", "SIGTERM", 0);
	expect(lines).toEqual(["gateway_exit cause=signal code=0 detail=SIGTERM\n"]);
});

test("a boot failure reports through the same single line", () => {
	const { reporter, lines } = harness();
	reporter.report("boot_failure", "config unreadable", 1);
	expect(lines).toEqual(["gateway_exit cause=boot_failure code=1 detail=config unreadable\n"]);
});

test("an empty detail is named instead of leaving the field blank", () => {
	expect(renderExitReport("unexpected_exit", "", 1)).toBe(
		"gateway_exit cause=unexpected_exit code=1 detail=no_detail\n",
	);
});

test("control characters cannot smuggle a second line or a NUL into the journal", () => {
	const line = renderExitReport("uncaught_exception", "boom\u0000\nsecond line", 1);
	expect(line).toBe("gateway_exit cause=uncaught_exception code=1 detail=boom second line\n");
	expect(line.endsWith("\n")).toBe(true);
	expect(line.slice(0, -1)).not.toContain("\n");
	expect(line).not.toContain("\u0000");
});

test("a pathological detail is bounded", () => {
	const line = renderExitReport("uncaught_exception", "x".repeat(5000), 1);
	const prefix = "gateway_exit cause=uncaught_exception code=1 detail=";
	expect(line).toBe(`${prefix}${"x".repeat(EXIT_DETAIL_MAX)}\n`);
});

test("a cause outside 0-255 is left off rather than printed as a status", () => {
	expect(renderExitReport("signal", "SIGTERM")).toBe("gateway_exit cause=signal detail=SIGTERM\n");
	expect(renderExitReport("signal", "SIGTERM", 4242)).toBe("gateway_exit cause=signal detail=SIGTERM\n");
});
