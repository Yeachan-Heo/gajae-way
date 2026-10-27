import { Database } from "bun:sqlite";
import { appendFileSync, existsSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { mkdtemp, rename, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { StatusReport } from "@gajae-gateway/subsession";
import type {
	SessionBindInput,
	SessionSendInput,
	SessionSteerInput,
	WorkerOutputInput,
} from "../../src/orchestrator/session-port";
import type { TailAttachInput } from "../../src/orchestrator/tail-runner";
import { startUnixServer } from "../../src/server/server";
import { Store } from "../../src/store";
import type { BrokerAuthority } from "../../src/store/groups/bindings";
import type { WriteTx } from "../../src/store/tx";
import { ScriptedSessionPort, steerRefused } from "../session-port.fake";

export const repository = resolve(import.meta.dir, "../../../..");
export const noticeOrigin = { platform: "discord", kind: "channel", conversationId: "fixture" } as const;
export type Control = {
	terminal?: boolean;
	terminalAt?: number;
	text?: string;
	live?: "live" | "dead" | "indeterminate" | "disowned";
	unknown?: boolean;
	refuseSteer?: boolean;
	unavailable?: boolean;
	missingReceipt?: boolean;
	statusUnavailable?: boolean;
};
export type Call = { method: string; input: Record<string, unknown> };
export type Barrier =
	| "journal-replayed"
	| "prepared"
	| "accepted-before-save"
	| "terminal-before"
	| "terminal-after"
	| "output-claim"
	| "silence-after"
	| "settle-before"
	| "settle-cas"
	| "settle-history"
	| "settle-activity"
	| "settle-ledger"
	| "settle-report"
	| "settle-commit"
	| "report-claim"
	| "report-consume";

function json<T>(path: string, fallback: T): T {
	return existsSync(path) ? (JSON.parse(readFileSync(path, "utf8")) as T) : fallback;
}

// Synchronous publication keeps barriers inside their original transaction and SIGKILL window.
// The writer parameter lets regressions interleave readers with an incomplete temporary file.
export function writeFixtureJson(
	path: string,
	value: unknown,
	write: (path: string, data: string) => void = writeFileSync,
): void {
	const temporary = `${path}.${crypto.randomUUID()}.tmp`;
	try {
		write(temporary, JSON.stringify(value));
		renameSync(temporary, path);
	} finally {
		rmSync(temporary, { force: true });
	}
}

/** Only fixture files supply broker authority; rehydration never invokes send or bind. */
class PersistentPort extends ScriptedSessionPort {
	constructor(
		readonly home: string,
		readonly hit: (point: Barrier, opRef?: string) => void,
		readonly database: Store,
		readonly authority: BrokerAuthority,
	) {
		super({
			sessionIdForBind: (input) =>
				database.bindings.getSessionRecord(input.originKey)?.sessionId || crypto.randomUUID(),
		});
	}
	private record(method: string, input: object) {
		appendFileSync(join(this.home, "calls.jsonl"), `${JSON.stringify({ method, input })}\n`);
	}
	private controls() {
		return json<Record<string, Control>>(join(this.home, "control.json"), {});
	}
	private sync(opRef: string) {
		const saved = json<SessionSendInput | null>(join(this.home, `${opRef}.send.json`), null);
		const control = this.controls()[opRef] ?? {};
		if (saved) {
			this.setSessionState(saved.sessionId, { repo: saved.repo, live: control.live !== "dead" });
			this.seedOperation(
				opRef,
				saved.sessionId,
				control.terminal ? "terminal_ok" : "in_flight",
				control.text ?? "fixture completed",
			);
		}
		return control;
	}
	override async bind(input: SessionBindInput) {
		this.record("bind", input);
		const binding = await super.bind(input);
		if (
			!this.database.transaction((tx) =>
				this.database.bindings.recordOwnedBinding(tx, { ...binding, authority: this.authority }),
			)
		)
			throw new Error("fixture session bind lost its epoch");
		writeFixtureJson(join(this.home, `${binding.sessionId}.session.json`), binding);
		return binding;
	}
	override async resume(input: Parameters<ScriptedSessionPort["resume"]>[0]) {
		this.record("resume", input);
		return super.resume(input);
	}
	override async send(input: SessionSendInput) {
		this.record("send", input);
		if (existsSync(join(this.home, `${input.opRef}.send.json`))) throw new Error("fixture detected duplicate send");
		const receipt = await super.send(input);
		writeFixtureJson(join(this.home, `${input.opRef}.send.json`), { ...input, startedAt: Date.now() });
		// Persona report delivery can send concurrently with a worker wake. A
		// worker crash barrier must never stop that unrelated persona prompt.
		if (input.codingRegister === true) this.hit("accepted-before-save", input.opRef);
		return receipt;
	}
	override async status(input: Parameters<ScriptedSessionPort["status"]>[0]): Promise<StatusReport> {
		this.record("status", input);
		const control = this.sync(input.opRef);
		if (control.statusUnavailable) throw new Error("fixture status transport unavailable");
		if (control.unknown) return { operationRef: input.opRef, status: { status: "unknown" }, summaryCompleted: false };
		const report = await super.status(input);
		const saved = json<{ startedAt: number } | null>(join(this.home, `${input.opRef}.send.json`), null);
		if (!saved) return report;
		const status = { ...report.status, startedAt: saved.startedAt };
		if (control.missingReceipt)
			return {
				operationRef: input.opRef,
				status: {
					status: "failed",
					clientRef: input.opRef,
					commandId: `fixture-command-${input.opRef}`,
					turnId: `fixture-turn-${input.opRef}`,
					startedAt: saved.startedAt,
					terminalAt: saved.startedAt + 1,
					receiptState: "missing",
					error: { code: "prompt_deadline_exceeded" },
				},
				summaryCompleted: true,
			};
		return control.terminal
			? {
					...report,
					status: { ...status, status: "terminal_ok", receiptState: "present", outcome: { reason: "end_turn" } },
				}
			: { ...report, status };
	}
	async turnRunning() {
		return true;
	}
	override async liveness(input: Parameters<ScriptedSessionPort["liveness"]>[0]) {
		this.record("liveness", input);
		const sends = readCalls(this.home).filter(
			(call) => call.method === "send" && call.input.sessionId === input.sessionId,
		);
		const opRef = sends.at(-1)?.input.opRef as string | undefined;
		if (!existsSync(join(this.home, `${input.sessionId}.session.json`))) return { live: undefined, disowned: true };
		const control = opRef ? this.sync(opRef) : (this.controls()[input.sessionId] ?? {});
		return {
			live: control.live === "indeterminate" || control.live === "disowned" ? undefined : control.live !== "dead",
			disowned: control.live === "disowned",
		};
	}
	override async steer(input: SessionSteerInput) {
		this.record("steer", input);
		const refused = Object.values(this.controls()).some((control) => control.refuseSteer);
		if (refused) throw steerRefused();
		return super.steer(input);
	}
	override async attachTail(input: TailAttachInput) {
		this.record("attach", { sessionId: input.sessionId, brokerGeneration: input.brokerGeneration });
		return super.attachTail(input);
	}
	override async fetchWorkerOutput(input: WorkerOutputInput) {
		this.record("output", input);
		const control = this.sync(input.opRef);
		const saved = json<(SessionSendInput & { startedAt: number }) | null>(
			join(this.home, `${input.opRef}.send.json`),
			null,
		);
		const text = control.text ?? "fixture completed";
		const result =
			!saved || control.unavailable
				? { status: "unknown" }
				: {
						kind: "prompt",
						clientRef: input.opRef,
						status: control.terminal ? "terminal_ok" : "in_flight",
						startedAt: saved.startedAt,
						terminalAt: control.terminalAt,
						receiptState: "present",
						content: { version: 1, type: "text", text, byteLength: Buffer.byteLength(text), truncated: false },
					};
		this.setWorkerOutputFixture(input.opRef, { exitCode: 0, stdout: JSON.stringify({ ok: true, result }), stderr: "" });
		return super.fetchWorkerOutput(input);
	}
}

export function readCalls(home: string): Call[] {
	const path = join(home, "calls.jsonl");
	return existsSync(path)
		? readFileSync(path, "utf8")
				.trim()
				.split("\n")
				.filter(Boolean)
				.map((line) => JSON.parse(line))
		: [];
}

async function serve(home: string, barrier?: Barrier) {
	const hit = (point: Barrier, opRef?: string) => {
		const armed = json<{ readonly point?: Barrier | null }>(join(home, "barrier-arm.json"), {}).point;
		if (barrier !== point && armed !== point) return;
		writeFixtureJson(join(home, "barrier-hit.json"), { point, pid: process.pid, opRef });
		if (armed === point) writeFixtureJson(join(home, "barrier-arm.json"), { point: null });
		// A synchronous fixture-only barrier can stop inside a real SQLite transaction.
		// The parent SIGKILLs precisely this child; no production fault flag exists.
		Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 30_000);
		throw new Error(`fixture barrier ${point} was not killed within 30 seconds`);
	};
	const database = await Store.open(join(home, "gateway.db"));
	database.replayJournal();
	hit("journal-replayed");
	const agentDir = resolve(join(home, "agent"));
	const authority = { canonicalAgentDir: agentDir, identity: `gjc:${agentDir}` };
	database.transaction((tx) =>
		database.bindings.initializeAuthority(tx, authority, database.inspectBrokerAuthority().populated),
	);
	let acceptanceBarrierPending = false;
	let settlementCommitPending = false;
	let settling = false;
	const committedBarriers: Barrier[] = [];
	const withTransaction = database.transaction.bind(database);
	database.transaction = <T>(operation: (tx: WriteTx) => T): T => {
		try {
			const result = withTransaction((tx) => {
				const committed = operation(tx);
				if (acceptanceBarrierPending) {
					acceptanceBarrierPending = false;
					hit("report-consume");
				}
				return committed;
			});
			if (settlementCommitPending) {
				settlementCommitPending = false;
				hit("settle-commit");
			}
			for (const point of committedBarriers.splice(0)) hit(point);
			return result;
		} finally {
			settling = false;
			settlementCommitPending = false;
			acceptanceBarrierPending = false;
			committedBarriers.length = 0;
		}
	};
	const prepare = database.work.workAttemptPrepare.bind(database.work);
	database.work.workAttemptPrepare = (...args) => {
		prepare(...args);
		committedBarriers.push("prepared");
	};
	const update = database.work.workAttemptUpdate.bind(database.work);
	database.work.workAttemptUpdate = (...args) => {
		if (args[3].terminal) hit("terminal-before");
		const prior = database.work.workAttemptGet(args[1]);
		if (
			prior?.wakeReportId &&
			(args[3].sendPhase === "accepted" ||
				args[3].output?.proof !== undefined ||
				args[3].output?.knownSilence !== undefined)
		)
			acceptanceBarrierPending = true;
		const previousReads = prior?.output.reads ?? 0;
		const result = update(...args);
		if (result && args[3].terminal) committedBarriers.push("terminal-after");
		if (result && result.output.reads > previousReads) committedBarriers.push("output-claim");
		if (result?.output.knownSilence) committedBarriers.push("silence-after");
		return result;
	};
	const settle = database.work.workAttemptSettle.bind(database.work);
	database.work.workAttemptSettle = (...args) => {
		hit("settle-before");
		settling = true;
		const result = settle(...args);
		if (result) settlementCommitPending = true;
		return result;
	};
	const enqueue = database.inbox.enqueue.bind(database.inbox);
	database.inbox.enqueue = (tx, row) => {
		const result = enqueue(tx, row);
		if (settling && row.source === "lane_report") hit("settle-report");
		return result;
	};
	const claimReport = database.work.laneReportClaim.bind(database.work);
	database.work.laneReportClaim = (...args) => {
		const result = claimReport(...args);
		if (result) committedBarriers.push("report-claim");
		return result;
	};
	const history = database.work.putLaneJob.bind(database.work);
	database.work.putLaneJob = (...args) => {
		if (settling) hit("settle-cas");
		history(...args);
		if (settling) hit("settle-history");
	};
	const touch = database.bindings.touchWorkSession.bind(database.bindings);
	database.bindings.touchWorkSession = (...args) => {
		if (settling) hit("settle-activity");
		return touch(...args);
	};
	const outboxEnqueue = database.outbox.enqueue.bind(database.outbox);
	database.outbox.enqueue = (...args) => {
		const result = outboxEnqueue(...args);
		if (settling) hit("settle-ledger");
		return result;
	};
	const storedConfig = json<{
		readonly work?: { readonly allowNested?: boolean };
		readonly rootless?: boolean;
	}>(join(home, "config.json"), {});
	const config = {
		schemaVersion: 1 as const,
		home,
		gjc: { agentDir: join(home, "agent") },
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		...(storedConfig.rootless ? {} : { ownerTarget: { origin: noticeOrigin } }),
		channels: { fixture: { engagement: "open" as const, audience: "all" as const } },
		work: { maxLanes: 2, ...storedConfig.work },
	};
	const server = await startUnixServer({
		config,
		database,
		sessionPort: new PersistentPort(home, hit, database, authority),
		onStop: () => database.close(),
	});
	process.once("SIGTERM", () => void server.stop());
}

// Infer the observed value from read(), not from Boolean's unknown parameter.
// Boolean is only an acceptance predicate; it supplies no evidence about T.
export async function eventually<T>(
	read: () => T | Promise<T>,
	accept: (value: NoInfer<T>) => boolean,
	label: string,
	timeout = 12_000,
): Promise<T> {
	const deadline = Date.now() + timeout;
	while (Date.now() < deadline) {
		const value = await read();
		if (accept(value)) return value;
		await Bun.sleep(20);
	}
	throw new Error(`timed out: ${label}`);
}

export async function wire(socketPath: string) {
	const frames: any[] = [];
	let buffered = Buffer.alloc(0);
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered = Buffer.concat([buffered, Buffer.from(data)]);
				let end = buffered.indexOf(10);
				while (end >= 0) {
					const line = buffered.subarray(0, end).toString("utf8");
					buffered = buffered.subarray(end + 1);
					if (line) frames.push(JSON.parse(line));
					end = buffered.indexOf(10);
				}
			},
		},
	});
	const send = (value: unknown) => socket.write(`${JSON.stringify(value)}\n`);
	send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await eventually(
		() => frames,
		(items) => items.some((frame) => frame.type === "negotiated"),
		"negotiation",
	);
	return {
		frames,
		send,
		close: () => socket.end(),
		async request(verb: string, params?: unknown) {
			const id = crypto.randomUUID();
			send({ v: "0.1", type: "request", id, verb, params });
			return eventually(() => frames.find((frame) => frame.id === id), Boolean, verb);
		},
	};
}

export class WorkFixture {
	child?: ReturnType<typeof Bun.spawn>;
	readonly clients: Awaited<ReturnType<typeof wire>>[] = [];
	private controls: Record<string, Control> = {};
	constructor(readonly home: string) {}
	get socket() {
		return join(this.home, "gateway.sock");
	}
	static async create(options: { readonly allowNested?: boolean; readonly rootless?: boolean } = {}) {
		const fixture = new WorkFixture(await mkdtemp(join(tmpdir(), "work-crash-")));
		await Bun.write(
			join(fixture.home, "config.json"),
			JSON.stringify({
				schemaVersion: 1,
				ownerTarget: { origin: noticeOrigin },
				...(options.rootless ? { rootless: true } : {}),
				...(options.allowNested === undefined ? {} : { work: { allowNested: options.allowNested } }),
			}),
		);
		return fixture;
	}
	async start(barrier?: Barrier) {
		await rm(join(this.home, "barrier-hit.json"), { force: true });
		this.child = Bun.spawn(
			[
				process.execPath,
				join(import.meta.dir, "work-lane-server.ts"),
				"serve",
				this.home,
				...(barrier ? [barrier] : []),
			],
			{
				cwd: repository,
				env: { ...process.env, GAJAEWAY_HOME: this.home, GJCHOME: this.home },
				stdout: "ignore",
				stderr: "inherit",
			},
		);
		await eventually(
			async () => {
				if (this.child?.exitCode !== null) throw new Error(`fixture exited ${this.child?.exitCode}`);
				if (barrier && json<{ pid?: number }>(join(this.home, "barrier-hit.json"), {}).pid === this.child?.pid)
					return true;
				return Bun.connect({ unix: this.socket, socket: { data() {} } }).then(
					(socket) => {
						socket.end();
						return true;
					},
					() => false,
				);
			},
			Boolean,
			"fixture socket readiness",
		);
	}
	async seedPersonaSession(): Promise<string> {
		const sessionId = crypto.randomUUID();
		const database = await Store.open(join(this.home, "gateway.db"));
		try {
			const authority = database.inspectBrokerAuthority().authority;
			if (!authority) throw new Error("fixture broker authority missing");
			const binding = {
				sessionId,
				originKey: `${noticeOrigin.platform}/${noticeOrigin.kind}/${noticeOrigin.conversationId}`,
				epoch: 0,
				repo: join(this.home, "workspace"),
				authority,
			};
			if (!database.transaction((tx) => database.bindings.recordOwnedBinding(tx, binding)))
				throw new Error("persona fixture binding lost its epoch");
			writeFixtureJson(join(this.home, `${sessionId}.session.json`), binding);
		} finally {
			database.close();
		}
		return sessionId;
	}
	async connect() {
		const client = await wire(this.socket);
		this.clients.push(client);
		return client;
	}
	async kill() {
		for (const client of this.clients.splice(0)) client.close();
		const child = this.child;
		this.child = undefined;
		if (child) {
			child.kill("SIGKILL");
			await child.exited;
		}
	}
	async cleanup() {
		await this.kill();
		await rm(this.home, { recursive: true, force: true });
	}
	async armBarrier(point: Barrier): Promise<void> {
		writeFixtureJson(join(this.home, "barrier-arm.json"), { point });
		await rm(join(this.home, "barrier-hit.json"), { force: true });
	}
	async persistAcceptedSend(opRef: string): Promise<void> {
		const call = this.calls("send").find((entry) => entry.input.opRef === opRef);
		if (!call) throw new Error(`missing send call for ${opRef}`);
		writeFixtureJson(join(this.home, `${opRef}.send.json`), { ...call.input, startedAt: Date.now() });
	}
	async control(opRef: string, control: Control) {
		this.controls[opRef] = {
			...this.controls[opRef],
			...control,
			...(control.terminal && !this.controls[opRef]?.terminalAt ? { terminalAt: Date.now() } : {}),
		};
		const path = join(this.home, "control.json");
		await Bun.write(`${path}.tmp`, JSON.stringify(this.controls));
		await rename(`${path}.tmp`, path);
	}
	async barrier(point: Barrier) {
		return eventually(
			() => json<{ point?: string; pid?: number; opRef?: string }>(join(this.home, "barrier-hit.json"), {}),
			(value) => value.point === point && value.pid === this.child?.pid,
			point,
		);
	}
	calls(method: string) {
		return readCalls(this.home).filter((call) => call.method === method);
	}
	snapshot() {
		const database = new Database(join(this.home, "gateway.db"), { readonly: true });
		try {
			return {
				runtimes: database
					.query<{ record_json: string }, []>("SELECT record_json FROM work_attempt_runtime")
					.all()
					.map((row) => JSON.parse(row.record_json)),
				jobs: database
					.query<{ record_json: string }, []>("SELECT record_json FROM lane_jobs")
					.all()
					.map((row) => JSON.parse(row.record_json)),
				deliveries: database
					.query<{ delivery_id: string; payload_json: string; state: string }, []>(
						"SELECT delivery_id, payload_json, state FROM deliveries",
					)
					.all(),
				sessions: database
					.query<{ origin_key: string; last_activity_at: string | null }, []>(
						"SELECT origin_key, last_activity_at FROM sessions WHERE origin_key LIKE 'work/task/%'",
					)
					.all(),
				inbound: database
					.query<
						{
							message_id: string;
							origin_key: string;
							source: string;
							state: string;
							turn_role: string | null;
							turn_state: string | null;
							turn_op_ref: string | null;
						},
						[]
					>(
						"SELECT message_id, origin_key, source, state, turn_role, turn_state, turn_op_ref FROM inbound_messages ORDER BY rowid",
					)
					.all(),
				laneReports: database
					.query<
						{
							report_id: string;
							parent_name: string;
							child_op_ref: string;
							state: string;
							claim_ref: string | null;
							consumed_op_ref: string | null;
							hold_reason: string | null;
						},
						[]
					>(
						"SELECT report_id, parent_name, child_op_ref, state, claim_ref, consumed_op_ref, hold_reason FROM lane_reports ORDER BY created_at",
					)
					.all(),
			};
		} finally {
			database.close();
		}
	}
}

// Reusable real fixture command for PTY capture: bun .../work-lane-server.ts serve <fixture-home>.
// Complete through <home>/control.json keyed by opRef from actual work start/status output.
if (import.meta.main) {
	if (process.argv[2] !== "serve" || !process.argv[3])
		throw new Error("usage: work-lane-server.ts serve <existing-fixture-home> [barrier]");
	await serve(resolve(process.argv[3]), process.argv[4] as Barrier | undefined);
}
