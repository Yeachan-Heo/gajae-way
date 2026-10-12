import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { acquireLease, leaseHeld } from "../src/index";

const FIXTURE = join(import.meta.dir, "fixtures", "hold-lease.ts");
let home: string;
const spawned: number[] = [];

beforeEach(async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-lease-proc-"));
});
afterEach(async () => {
	for (const pid of spawned.splice(0)) {
		try {
			process.kill(pid, "SIGKILL");
		} catch {}
	}
	await rm(home, { recursive: true, force: true });
});

function holder(...flags: string[]) {
	const child = Bun.spawn([process.execPath, FIXTURE, home, "gateway", ...flags], { stdout: "pipe", stderr: "inherit" });
	spawned.push(child.pid);
	return child;
}

async function readLines(stream: ReadableStream<Uint8Array>, count: number): Promise<string[]> {
	const lines: string[] = [];
	const decoder = new TextDecoder();
	let buffer = "";
	for await (const chunk of stream) {
		buffer += decoder.decode(chunk);
		const parts = buffer.split("\n");
		buffer = parts.pop() ?? "";
		lines.push(...parts);
		if (lines.length >= count) break;
	}
	return lines;
}

test("20 concurrent contenders produce exactly one holder", async () => {
	const contenders = Array.from({ length: 20 }, () => holder());
	const firstLines = await Promise.all(contenders.map(async (c) => (await readLines(c.stdout, 1))[0]));
	expect(firstLines.filter((line) => line === "held")).toHaveLength(1);
	expect(firstLines.filter((line) => line === "lease_held")).toHaveLength(19);
}, 30_000);

test("SIGKILL of the holder releases the lease", async () => {
	const child = holder();
	expect(await readLines(child.stdout, 1)).toEqual(["held"]);
	expect(await leaseHeld(home, "gateway")).toBe(true);
	child.kill("SIGKILL");
	await child.exited;
	const lease = await acquireLease(home, "gateway");
	await lease.release();
});

// Outcome check. Bun's spawn also closes non-standard descriptors in children,
// so FD_CLOEXEC itself is enforced by acquireLease's fcntl(F_GETFD) check,
// which refuses any lease descriptor that lacks it.
test("a surviving child does not keep the lease after the holder dies (FD_CLOEXEC)", async () => {
	const child = holder("--child");
	const lines = await readLines(child.stdout, 2);
	const grandchild = Number(lines.find((line) => line.startsWith("child "))?.slice("child ".length));
	expect(Number.isSafeInteger(grandchild)).toBe(true);
	spawned.push(grandchild);
	child.kill("SIGKILL");
	await child.exited;
	expect(() => process.kill(grandchild, 0)).not.toThrow();
	const lease = await acquireLease(home, "gateway");
	await lease.release();
});
