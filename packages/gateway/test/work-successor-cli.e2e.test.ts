import { afterEach, expect, test } from "bun:test";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { GajaewayClient } from "../../sdk/src/index";
import { eventually, repository, WorkFixture } from "./fixtures/work-lane-server";

const fixtures: WorkFixture[] = [];
const clients: GajaewayClient[] = [];
const children = new Set<ReturnType<typeof Bun.spawn>>();
afterEach(async () => {
	for (const client of clients.splice(0)) await client.close();
	for (const child of children) {
		child.kill("SIGKILL");
		await child.exited;
	}
	children.clear();
	for (const f of fixtures.splice(0)) await f.cleanup();
});
async function fixture(options: { allowNested?: boolean; rootless?: boolean } = {}) {
	const f = await WorkFixture.create(options);
	fixtures.push(f);
	await f.start();
	return f;
}
async function sdk(f: WorkFixture) {
	// Unix socket ownership and fixture broker identity are the real local trust boundary.
	const client = await GajaewayClient.connectSocket(f.socket, { requestTimeoutMs: 12_000 });
	clients.push(client);
	return client;
}
function cli(f: WorkFixture, args: string[]) {
	const child = Bun.spawn(
		[process.execPath, join(repository, "packages/cli/src/main.ts"), "--socket", f.socket, "work", ...args],
		{
			cwd: repository,
			env: { ...process.env, GAJAEWAY_HOME: f.home, GJCHOME: f.home },
			stdout: "pipe",
			stderr: "pipe",
		},
	);
	children.add(child);
	const timeout = setTimeout(() => child.kill("SIGKILL"), 20_000);
	const result = Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited])
		.then(([stdout, stderr, code]) => ({ stdout, stderr, code }))
		.finally(() => {
			clearTimeout(timeout);
			children.delete(child);
		});
	return { child, result };
}
function git(cwd: string, ...args: string[]) {
	const result = Bun.spawnSync(["git", ...args], { cwd });
	if (result.exitCode !== 0) throw new Error(result.stderr.toString());
	return result.stdout.toString().trim();
}

test("PR701 actual CLI response-only successor preserves null route and immutable linkage across SIGKILL and SDK query failure", async () => {
	const f = await fixture();
	const repo = join(f.home, "source");
	await mkdir(repo);
	git(repo, "init", "-b", "main");
	git(repo, "config", "user.email", "fixture@example.invalid");
	git(repo, "config", "user.name", "Isolated fixture");
	await writeFile(join(repo, "input.txt"), "reviewed input\n");
	git(repo, "add", "input.txt");
	git(repo, "commit", "-m", "reviewed input");
	const base = git(repo, "rev-parse", "HEAD");
	const running = cli(f, ["run", "response-source", "--repo", repo, "--base", base, "accepted original work"]);
	const accepted = await eventually(
		() => f.snapshot(),
		(s) => s.runtimes.some((r) => r.mode === "run" && r.sendPhase === "accepted"),
		"CLI run accepted",
	);
	const source = accepted.runtimes.find((r) => r.mode === "run")!;
	expect(source.parent).toBeNull();
	running.child.kill("SIGKILL");
	await running.result;
	await f.control(source.opRef, { missingReceipt: true, live: "live" });
	await eventually(
		() => f.snapshot(),
		(s) => s.jobs.some((j) => j.jobId === source.jobId && j.state === "awaiting_operator"),
		"original held without settlement",
	);
	const args = [
		"start",
		"response-successor",
		"--repo",
		repo,
		"--base",
		base,
		"--successor-of",
		"response-source",
		"--source-op",
		source.opRef,
		"--source-session",
		source.sessionId,
		"--acknowledge-unresolved",
		"separately authorized task",
	];
	const started = await cli(f, args).result;
	expect(started.code).toBe(0);
	expect(started.stderr).toBe("");
	const linked = f.snapshot();
	const successor = linked.runtimes.find((r) => r.successorOf?.opRef === source.opRef)!;
	expect(successor).toBeDefined();
	expect(successor.parent).toBeNull();
	expect(successor.cwd).not.toBe(source.cwd);
	expect(git(successor.cwd, "rev-parse", "HEAD")).toBe(base);
	expect(linked.runtimes.find((r) => r.opRef === source.opRef)).toMatchObject({
		successor: { opRef: successor.opRef },
		settledAt: null,
		terminal: null,
	});
	await f.kill();
	await f.control(source.opRef, { statusUnavailable: true, live: "indeterminate" });
	await f.control(successor.opRef, { statusUnavailable: true, live: "indeterminate" });
	await f.start();
	const client = await sdk(f);
	for (const [name, link] of [
		["response-source", "successor"],
		["response-successor", "successorOf"],
	] as const) {
		const status = await client.request<any>("work.status", { name });
		expect(status.opUnavailable).toBe(true);
		expect(status[link].opRef).toBe(link === "successor" ? successor.opRef : source.opRef);
	}
	const retry = await cli(f, args).result;
	expect(retry.code).toBe(0);
	expect(retry.stdout).toContain(successor.opRef);
	const duplicate = await cli(
		f,
		args.map((a) => (a === "response-successor" ? "another-successor" : a)),
	).result;
	expect(duplicate.code).toBe(1);
	expect(duplicate.stderr).toContain("successor_already_authorized");
	expect(f.calls("send").filter((c) => c.input.codingRegister === true)).toHaveLength(2);
	expect(f.calls("resume")).toHaveLength(0);
	for (const runtime of f.snapshot().runtimes) expect(runtime.settledAt).toBeNull();
	expect(f.snapshot().deliveries).toHaveLength(0);
}, 60_000);

test("PR701 SDK rootless child report remains inspectable for accepted open held parent across SIGKILL", async () => {
	const f = await fixture({ allowNested: true, rootless: true });
	const client = await sdk(f);
	const parent = await client.request<any>("work.start", {
		name: "rootless-parent",
		cwd: f.home,
		text: "original parent",
	});
	const child = await client.request<any>("work.start", {
		name: "rootless-child",
		cwd: f.home,
		callerSessionId: parent.sessionId,
		text: "child output",
	});
	expect(f.snapshot().runtimes.find((r) => r.opRef === child.opRef)?.parent).toEqual({
		kind: "lane",
		name: "rootless-parent",
		root: null,
	});
	await f.control(parent.opRef, { missingReceipt: true, live: "live" });
	await eventually(
		() => f.snapshot(),
		(s) => s.jobs.some((j) => j.jobId === parent.jobId && j.state === "awaiting_operator"),
		"rootless parent held",
	);
	await f.control(child.opRef, { terminal: true, text: "durable private child result" });
	const settled = await eventually(
		() => f.snapshot(),
		(s) => s.runtimes.find((r) => r.opRef === child.opRef)?.settledAt !== null,
		"child actually settled",
	);
	expect(settled.runtimes.find((r) => r.opRef === child.opRef)?.decision).toBe("reported");
	expect(settled.laneReports).toHaveLength(1);
	expect(settled.laneReports[0]).toMatchObject({ parent_name: "rootless-parent", child_op_ref: child.opRef });
	expect(settled.deliveries).toHaveLength(0);
	await client.close();
	await f.kill();
	await f.control(parent.opRef, { statusUnavailable: true, live: "indeterminate" });
	await f.start();
	const recovered = await sdk(f);
	const status = await recovered.request<any>("work.status", { name: "rootless-parent" });
	expect(status.attempt.opRef).toBe(parent.opRef);
	expect(status.attempt.endedAt).toBeUndefined();
	expect(f.snapshot().laneReports).toHaveLength(1);
	expect(f.snapshot().runtimes.find((r) => r.opRef === parent.opRef)).toMatchObject({
		terminal: null,
		settledAt: null,
	});
	expect(f.calls("send").filter((c) => c.input.codingRegister === true)).toHaveLength(2);
	expect(f.snapshot().deliveries).toHaveLength(0);
}, 60_000);
