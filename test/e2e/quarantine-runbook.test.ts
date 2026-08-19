import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { ChildProcess } from "node:child_process";
import { afterEach, expect, test } from "bun:test";
import { createClosureExecutor } from "../../src/main-session/closure";
import { startWayServer } from "../../src/main";
import type { WayCoreHandle } from "../../src/native-loader";
import { RpcClient, type JsonRpcResponse } from "../../src/rpc-client";
import { ManagedProcessRegistry } from "../helpers/managed-process";

const managedProcesses = new ManagedProcessRegistry();

afterEach(async () => {
	await managedProcesses.reapAll();
});


const repositoryRoot = path.resolve(import.meta.dir, "..", "..");

interface Holder {
	readonly child: ChildProcess;
	readonly identity: { readonly pid: number; readonly pidStartTime: string; readonly pgid: number; readonly pgidStartTime?: string };
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

async function run(command: readonly string[], cwd = repositoryRoot): Promise<string> {
	const child = Bun.spawn({ cmd: [...command], cwd, stdout: "pipe", stderr: "pipe" });
	const [exitCode, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
	if (exitCode !== 0) throw new Error(`${command.join(" ")} failed (${exitCode}): ${stderr || stdout}`);
	return stdout;
}

async function connectEventually(socketPath: string): Promise<RpcClient> {
	let lastError: unknown;
	for (let attempt = 0; attempt < 100; attempt += 1) {
		try {
			return await RpcClient.connect(socketPath);
		} catch (error) {
			lastError = error;
			await Bun.sleep(10);
		}
	}
	throw new Error(`RPC socket did not become available: ${lastError instanceof Error ? lastError.message : "unknown error"}`);
}

/** Mirrors the operations runbook's NDJSON `way_rpc METHOD JSON` helper. */
async function wayRpc(client: RpcClient, method: string, paramsJson: string): Promise<JsonRpcResponse> {
	return await client.request(method, JSON.parse(paramsJson) as unknown);
}

function responseError(response: JsonRpcResponse): { readonly code: number; readonly message: string } {
	if (!response.error) throw new Error(`Expected RPC error, got ${JSON.stringify(response)}`);
	return response.error;
}

function responseResult(response: JsonRpcResponse): Record<string, unknown> {
	if (response.error) throw new Error(`Unexpected RPC error: ${response.error.code} ${response.error.message}`);
	if (!isRecord(response.result)) throw new Error(`Expected object RPC result, got ${JSON.stringify(response.result)}`);
	return response.result;
}

async function spawnHolder(core: WayCoreHandle): Promise<Holder> {
	const child = managedProcesses.spawnNodeGroup("/bin/sh", ["-c", "exec sleep 60"], { stdio: "ignore" });
	if (!child.pid) throw new Error("holder process did not expose a pid");
	let lastError: unknown;
	for (let attempt = 0; attempt < 100; attempt += 1) {
		try {
			return { child, identity: core.processIdentity(child.pid) };
		} catch (error) {
			lastError = error;
			await Bun.sleep(10);
		}
	}
	await managedProcesses.crashNodeGroup(child);
	throw new Error(`holder process did not publish an incarnation: ${lastError instanceof Error ? lastError.message : "unknown error"}`);
}

async function killHolder(holder: Holder): Promise<void> {
	await managedProcesses.crashNodeGroup(holder.child);
}

function acquire(core: WayCoreHandle, holder: Holder) {
	return core.lockAcquire({
		label: "quarantine-runbook-live-holder",
		waitMs: 0,
		ttlMs: 5_000,
		holder: {
			holderKind: "in_daemon",
			sessionId: "quarantine-runbook-holder",
			pid: holder.identity.pid,
			pidStartTime: holder.identity.pidStartTime,
			pgid: holder.identity.pgid,
			...(holder.identity.pgidStartTime ? { pgidStartTime: holder.identity.pgidStartTime } : {}),
			connId: "way.in_daemon_executor.v1",
		},
	});
}

test("quarantine runbook fences a live holder, records only a complete bound receipt, and reopens a local bare remote", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "way-quarantine-runbook-"));
	const corpus = path.join(root, "corpus");
	const remote = path.join(root, "remote.git");
	const stateDirectory = path.join(root, "state");
	let core: WayCoreHandle | undefined;
	let client: RpcClient | undefined;
	let holder: Holder | undefined;
	let executor: ReturnType<typeof createClosureExecutor> | undefined;
	try {
		await run(["git", "init", "--bare", remote]);
		await run(["git", "init", corpus]);
		await run(["git", "-C", corpus, "config", "user.name", "Quarantine Runbook Drill"]);
		await run(["git", "-C", corpus, "config", "user.email", "quarantine-runbook@example.test"]);
		fs.writeFileSync(path.join(corpus, "base.txt"), "base\n");
		await run(["git", "-C", corpus, "add", "--", "base.txt"]);
		await run(["git", "-C", corpus, "commit", "-m", "base"]);
		await run(["git", "-C", corpus, "branch", "-M", "main"]);
		await run(["git", "-C", corpus, "remote", "add", "origin", remote]);
		await run(["git", "-C", corpus, "push", "-u", "origin", "main"]);
		await run(["git", `--git-dir=${remote}`, "symbolic-ref", "HEAD", "refs/heads/main"]);

		core = startWayServer(stateDirectory);
		core.setRpcHealth("running");
		client = await connectEventually(path.join(stateDirectory, "rpc.sock"));
		holder = await spawnHolder(core);
		const lease = acquire(core, holder);

		// The documented detection and fencing requests use the real public RPC
		// names and exact snake_case fields from quarantine.md.
		const initialStatus = responseResult(await wayRpc(client, "way.status", "{}"));
		expect(initialStatus).toMatchObject({ lock: { holder: { lease_id: lease.leaseId }, quarantined: false }, write_mode: true });

		const forced = await wayRpc(
			client,
			"gitlock.force_release",
			JSON.stringify({ lease_id: lease.leaseId, confirm: true, idempotency_key: "force-quarantine-runbook" }),
		);
		expect(responseError(forced)).toMatchObject({ code: 1207, message: "lock_holder_unverified" });

		const quarantined = responseResult(
			await wayRpc(
				client,
				"gitlock.quarantine_override",
				JSON.stringify({
					lease_id: lease.leaseId,
					confirm: true,
					acknowledge_unverified: true,
					idempotency_key: "quarantine-quarantine-runbook",
				}),
			),
		);
		expect(quarantined).toMatchObject({ quarantined: true });
		expect(responseResult(await wayRpc(client, "way.status", "{}"))).toMatchObject({ lock: { quarantined: true }, write_mode: false });

		// The runbook's process inspection precedes its Git status/fsck/fetch/log
		// checks. A killed holder gives record_quarantine_receipt a fresh death proof.
		await killHolder(holder);
		await run(["git", "-C", corpus, "status", "--porcelain=v1"]);
		await run(["git", "-C", corpus, "fsck", "--full"]);
		await run(["git", "-C", corpus, "fetch", "--prune", "origin"]);
		await run(["git", "-C", corpus, "log", "--left-right", "--graph", "--cherry-pick", "origin/main...HEAD"]);
		await run(["git", `--git-dir=${remote}`, "fsck", "--full"]);

		const incomplete = await wayRpc(
			client,
			"gitlock.record_quarantine_receipt",
			JSON.stringify({
				lease_id: lease.leaseId,
				corpus: "corpus",
				checks: {
					process_inspected: true,
					git_status_checked: true,
					git_log_checked: true,
					git_fsck_checked: false,
					remote_verified: true,
				},
				idempotency_key: "incomplete-quarantine-receipt",
			}),
		);
		expect(responseError(incomplete)).toMatchObject({ code: -32602, message: expect.stringContaining("must include") });

		const wrongCorpus = await wayRpc(
			client,
			"gitlock.record_quarantine_receipt",
			JSON.stringify({
				lease_id: lease.leaseId,
				corpus: "other-corpus",
				checks: {
					process_inspected: true,
					git_status_checked: true,
					git_log_checked: true,
					git_fsck_checked: true,
					remote_verified: true,
				},
				idempotency_key: "wrong-corpus-quarantine-receipt",
			}),
		);
		expect(responseError(wrongCorpus)).toMatchObject({ code: -32602, message: expect.stringContaining("corpus") });

		const unbound = await wayRpc(
			client,
			"gitlock.clear_quarantine",
			'{"verification_receipt_id":"git-verify-00000000000000000000000000000000","confirm":true,"idempotency_key":"unbound-quarantine-receipt"}',
		);
		expect(responseError(unbound)).toMatchObject({ code: -32602, message: expect.stringContaining("does not exist") });

		const receipt = responseResult(
			await wayRpc(
				client,
				"gitlock.record_quarantine_receipt",
				JSON.stringify({
					lease_id: lease.leaseId,
					corpus: "corpus",
					checks: {
						process_inspected: true,
						git_status_checked: true,
						git_log_checked: true,
						git_fsck_checked: true,
						remote_verified: true,
					},
					idempotency_key: "bound-quarantine-receipt",
				}),
			),
		);
		expect(receipt).toMatchObject({ lease_id: lease.leaseId, corpus: "corpus" });
		const receiptId = receipt.receipt_id;
		if (typeof receiptId !== "string") throw new Error("record_quarantine_receipt did not return receipt_id");
		expect(receiptId).toMatch(/^git-verify-[a-f0-9]{32}$/);

		const cleared = responseResult(
			await wayRpc(
				client,
				"gitlock.clear_quarantine",
				JSON.stringify({ verification_receipt_id: receiptId, confirm: true, idempotency_key: "clear-quarantine-runbook" }),
			),
		);
		expect(cleared).toMatchObject({ quarantined: false });
		expect(responseResult(await wayRpc(client, "way.status", "{}"))).toMatchObject({ lock: { quarantined: false }, write_mode: true });

		// Verify that the documented status transition reopens actual supervised
		// writes, not merely an in-memory flag, against the authoritative bare remote.
		fs.writeFileSync(path.join(corpus, "reopened.txt"), "writes resumed\n");
		executor = createClosureExecutor({ core, heartbeatMs: 50 });
		const closure = await executor.execute({
			sessionId: "quarantine-runbook-reopened",
			corpusPath: corpus,
			label: "quarantine-runbook-reopened",
			paths: ["reopened.txt"],
			commitMessage: "quarantine runbook writes resumed",
		});
		expect(closure.committed).toBe(true);
		expect(await run(["git", `--git-dir=${remote}`, "show", "main:reopened.txt"])).toBe("writes resumed\n");

		const runbook = fs.readFileSync(path.join(repositoryRoot, "ops/runbooks/quarantine.md"), "utf8");
		const adapterUnit = fs.readFileSync(path.join(repositoryRoot, "ops/systemd/gajaeway-discord.service"), "utf8");
		expect(adapterUnit).toMatch(/^ConditionPathExists=!\/etc\/gajaeway\/recovery\/no-discord-ingress$/m);
		expect(runbook).toContain("sudo install -o root -g root -m 0600 /dev/null /etc/gajaeway/recovery/no-discord-ingress");
		expect(runbook).toContain("ConditionResult gajaeway-discord.service");
		expect(runbook).toContain("gitlock.record_quarantine_receipt");
		expect(runbook).toContain('"lease_id":"LEASE_ID","corpus":"corpus","checks"');
		expect(runbook).toContain('"process_inspected":true');
		expect(runbook).toContain('"git_status_checked":true');
		expect(runbook).toContain('"git_log_checked":true');
		expect(runbook).toContain('"git_fsck_checked":true');
		expect(runbook).toContain('"remote_verified":true');
		expect(runbook).toContain('"verification_receipt_id":"RECEIPT_ID"');
		const forceRelease = runbook.indexOf("gitlock.force_release");
		const override = runbook.indexOf("gitlock.quarantine_override");
		const manualGit = runbook.indexOf("git -C /srv/gajaeway/corpus status --porcelain=v1");
		const recordReceipt = runbook.indexOf("gitlock.record_quarantine_receipt");
		const clearQuarantine = runbook.indexOf("gitlock.clear_quarantine");
		expect(forceRelease).toBeGreaterThanOrEqual(0);
		expect(forceRelease).toBeLessThan(override);
		expect(override).toBeLessThan(manualGit);
		expect(manualGit).toBeLessThan(recordReceipt);
		expect(recordReceipt).toBeLessThan(clearQuarantine);
	} finally {
		await executor?.shutdown();
		if (holder) await killHolder(holder);
		client?.close();
		core?.shutdownRpcServer();
		await Bun.sleep(40);
		fs.rmSync(root, { force: true, recursive: true });
	}
}, 30_000);
