import { expect, spyOn, test } from "bun:test";
import { mkdtemp, readdir, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { installStructuredLogging } from "../src/index";

const ISO_LINE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z (?:error|warn|info) /;

function target() {
	const echoed: Array<{ level: "error" | "warn" | "info"; args: unknown[] }> = [];
	return {
		echoed,
		console: {
			error: (...args: unknown[]) => echoed.push({ level: "error", args }),
			warn: (...args: unknown[]) => echoed.push({ level: "warn", args }),
			info: (...args: unknown[]) => echoed.push({ level: "info", args }),
			log: (...args: unknown[]) => echoed.push({ level: "info", args }),
		} as Pick<Console, "error" | "warn" | "info" | "log">,
	};
}

async function temporarySink(): Promise<{ directory: string; path: string }> {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-log-sink-"));
	return { directory, path: join(directory, "gateway.log") };
}

test("prefixes every line, including multiline messages, with an ISO-8601 UTC timestamp", async () => {
	const sink = await temporarySink();
	try {
		const { console: output } = target();
		const dispose = installStructuredLogging({
			path: sink.path,
			console: output,
			heartbeatIntervalMs: 0,
			now: () => Date.parse("2026-09-18T00:00:00.000Z"),
		});
		output.error("first line\nsecond line");
		output.log("third line");
		dispose();

		const lines = (await readFile(sink.path, "utf8")).trimEnd().split("\n");
		expect(lines).toHaveLength(3);
		expect(lines.every((line) => ISO_LINE.test(line))).toBe(true);
		expect(lines.map((line) => line.replace(ISO_LINE, ""))).toEqual(["first line", "second line", "third line"]);
		expect(lines.filter((line) => ISO_LINE.test(line)).length / lines.length).toBe(1);
	} finally {
		await rm(sink.directory, { recursive: true, force: true });
	}
});

test("records informational, warning, and error diagnostics at their emitted severity", async () => {
	const sink = await temporarySink();
	try {
		const { console: output, echoed } = target();
		const dispose = installStructuredLogging({
			path: sink.path,
			console: output,
			heartbeatIntervalMs: 0,
			now: () => Date.parse("2026-09-18T00:00:00.000Z"),
		});
		output.info("steer_delivered opRef=op-1");
		output.warn("recovery_hold opRef=op-2");
		output.error("transport failure opRef=op-3");
		dispose();

		const lines = (await readFile(sink.path, "utf8")).trimEnd().split("\n");
		expect(lines.map((line) => line.replace(/^\S+ /, ""))).toEqual([
			"info steer_delivered opRef=op-1",
			"warn recovery_hold opRef=op-2",
			"error transport failure opRef=op-3",
		]);
		expect(lines.every((line) => ISO_LINE.test(line))).toBe(true);
		expect(echoed.map(({ level }) => level)).toEqual(["info", "warn", "error"]);
		expect(echoed.map(({ args }) => args[0])).toEqual([
			"2026-09-18T00:00:00.000Z info steer_delivered opRef=op-1",
			"2026-09-18T00:00:00.000Z warn recovery_hold opRef=op-2",
			"2026-09-18T00:00:00.000Z error transport failure opRef=op-3",
		]);
	} finally {
		await rm(sink.directory, { recursive: true, force: true });
	}
});

test("rotates the live sink before a write crosses the size threshold and bounds retained suffixes", async () => {
	const sink = await temporarySink();
	try {
		const { console: output } = target();
		const dispose = installStructuredLogging({
			path: sink.path,
			console: output,
			rotationBytes: 110,
			retainedFiles: 2,
			heartbeatIntervalMs: 0,
			now: () => Date.parse("2026-09-18T00:00:00.000Z"),
		});
		output.error("rotation-one");
		output.error("rotation-two");
		output.error("rotation-three");
		output.error("rotation-four");
		dispose();

		const entries = (await readdir(sink.directory)).filter((entry) => entry.startsWith(basename(sink.path)));
		const rotated = entries.filter((entry) => /^gateway\.log\.\d+$/.test(entry));
		expect(rotated.length).toBeLessThanOrEqual(2);
		expect(rotated).toContain("gateway.log.1");
		expect(await stat(sink.path)).toMatchObject({ size: expect.any(Number) });
		expect((await stat(sink.path)).size).toBeLessThan(110);
	} finally {
		await rm(sink.directory, { recursive: true, force: true });
	}
});

test("collapses repeated events into one x4 summary while ending on an interleaved event", async () => {
	const sink = await temporarySink();
	try {
		const { console: output } = target();
		let current = Date.parse("2026-09-18T00:00:00.000Z");
		const dispose = installStructuredLogging({
			path: sink.path,
			console: output,
			heartbeatIntervalMs: 0,
			now: () => current,
		});
		for (let index = 0; index < 5; index++) {
			output.error("repeated event");
			current += 1_000;
		}
		output.error("different event");
		dispose();

		const lines = (await readFile(sink.path, "utf8")).trimEnd().split("\n");
		expect(lines).toHaveLength(3);
		expect(lines[0]).toContain("error repeated event");
		expect(lines[1]).toContain(
			"error repeated event x4 (identical, first=2026-09-18T00:00:00.000Z last=2026-09-18T00:00:04.000Z)",
		);
		expect(lines[2]).toContain("error different event");
	} finally {
		await rm(sink.directory, { recursive: true, force: true });
	}
});

test("emits exactly one heartbeat line during an otherwise idle interval", async () => {
	const sink = await temporarySink();
	try {
		const { console: output } = target();
		let current = 0;
		let tick: (() => void) | undefined;
		const dispose = installStructuredLogging({
			path: sink.path,
			console: output,
			heartbeatIntervalMs: 60_000,
			now: () => current,
			setInterval: (handler) => {
				tick = handler;
				return { unref() {} };
			},
		});
		current = 60_000;
		tick?.();
		dispose();

		const lines = (await readFile(sink.path, "utf8")).trimEnd().split("\n");
		expect(lines).toHaveLength(1);
		expect(lines[0]).toContain("info service_alive uptime=60");
	} finally {
		await rm(sink.directory, { recursive: true, force: true });
	}
});

test("disposer restores every console method and an uninstalled console spy sees raw text", async () => {
	const sink = await temporarySink();
	const original = { error: console.error, warn: console.warn, info: console.info, log: console.log };
	try {
		const dispose = installStructuredLogging({ path: sink.path, heartbeatIntervalMs: 0 });
		expect(console.error).not.toBe(original.error);
		expect(console.warn).not.toBe(original.warn);
		expect(console.info).not.toBe(original.info);
		expect(console.log).not.toBe(original.log);
		dispose();
		expect(console.error).toBe(original.error);
		expect(console.warn).toBe(original.warn);
		expect(console.info).toBe(original.info);
		expect(console.log).toBe(original.log);

		const spy = spyOn(console, "error").mockImplementation(() => {});
		try {
			console.error("raw message");
			expect(spy).toHaveBeenCalledWith("raw message");
		} finally {
			spy.mockRestore();
		}
	} finally {
		if (console.error !== original.error) console.error = original.error;
		if (console.warn !== original.warn) console.warn = original.warn;
		if (console.info !== original.info) console.info = original.info;
		if (console.log !== original.log) console.log = original.log;
		await rm(sink.directory, { recursive: true, force: true });
	}
});
