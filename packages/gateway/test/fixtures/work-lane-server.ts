import { Database } from "bun:sqlite";
import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, rename, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { StatusReport } from "@gajaeway/subsession";
import type {
	SessionBindInput,
	SessionSendInput,
	SessionSteerInput,
	WorkerOutputInput,
} from "../../src/orchestrator/session-port";
import type { TailAttachInput } from "../../src/orchestrator/tail-runner";
import { startUnixServer } from "../../src/server/server";
import { GatewayDatabase } from "../../src/store/db";
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
};
export type Call = { method: string; input: Record<string, unknown> };
export type Barrier =
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
	| "settle-commit";

function json<T>(path: string, fallback: T): T {
	return existsSync(path) ? (JSON.parse(readFileSync(path, "utf8")) as T) : fallback;
}

/** Only fixture files supply broker authority; rehydration never invokes send or bind. */
class PersistentPort extends ScriptedSessionPort {
	constructor(
		readonly home: string,
		readonly hit: (point: Barrier) => void,
		readonly database: GatewayDatabase,
	) {
		super({
			sessionIdForBind: (input) => database.getSessionRecord(input.originKey)?.sessionId || crypto.randomUUID(),
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
		this.database.putSession(input.originKey, binding.sessionId);
		writeFileSync(join(this.home, `${binding.sessionId}.session.json`), JSON.stringify(binding));
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
		writeFileSync(join(this.home, `${input.opRef}.send.json`), JSON.stringify({ ...input, startedAt: Date.now() }));
		this.hit("accepted-before-save");
		return receipt;
	}
	override async status(input: Parameters<ScriptedSessionPort["status"]>[0]): Promise<StatusReport> {
		this.record("status", input);
		const control = this.sync(input.opRef);
		if (control.unknown) return { operationRef: input.opRef, status: { status: "unknown" }, summaryCompleted: false };
		const report = await super.status(input);
		const saved = json<{ startedAt: number } | null>(join(this.home, `${input.opRef}.send.json`), null);
		if (!saved) return report;
		const status = { ...report.status, startedAt: saved.startedAt };
		return control.terminal
			? {
					...report,
					status: { ...status, status: "terminal_ok", receiptState: "present", outcome: { reason: "end_turn" } },
				}
			: { ...report, status };
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
	const hit = (point: Barrier) => {
		if (barrier !== point) return;
		writeFileSync(join(home, "barrier-hit.json"), JSON.stringify({ point, pid: process.pid }));
		// A synchronous fixture-only barrier can stop inside a real SQLite transaction.
		// The parent SIGKILLs precisely this child; no production fault flag exists.
		Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 30_000);
		throw new Error(`fixture barrier ${point} was not killed within 30 seconds`);
	};
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const prepare = database.workAttemptPrepare.bind(database);
	database.workAttemptPrepare = (...args) => {
		prepare(...args);
		hit("prepared");
	};
	const update = database.workAttemptUpdate.bind(database);
	database.workAttemptUpdate = (...args) => {
		if (args[2].terminal) hit("terminal-before");
		const previousReads = database.workAttemptGet(args[0])?.output.reads ?? 0;
		const result = update(...args);
		if (result && args[2].terminal) hit("terminal-after");
		if (result && result.output.reads > previousReads) hit("output-claim");
		if (result?.output.knownSilence) hit("silence-after");
		return result;
	};
	let settling = false;
	const settle = database.workAttemptSettle.bind(database);
	database.workAttemptSettle = (...args) => {
		hit("settle-before");
		settling = true;
		try {
			const result = settle(...args);
			if (result) hit("settle-commit");
			return result;
		} finally {
			settling = false;
		}
	};
	const history = database.putLaneJob.bind(database);
	database.putLaneJob = (...args) => {
		if (settling) hit("settle-cas");
		history(...args);
		if (settling) hit("settle-history");
	};
	const delivery = database.deliveryCreateInTransaction.bind(database);
	database.deliveryCreateInTransaction = (...args) => {
		if (settling) hit("settle-activity");
		const result = delivery(...args);
		if (settling) hit("settle-ledger");
		return result;
	};
	const config = {
		schemaVersion: 1 as const,
		home,
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		ownerTarget: { origin: noticeOrigin },
	};
	const server = await startUnixServer({
		config,
		database,
		sessionPort: new PersistentPort(home, hit, database),
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
	static async create() {
		const fixture = new WorkFixture(await mkdtemp(join(tmpdir(), "work-crash-")));
		await Bun.write(
			join(fixture.home, "config.json"),
			JSON.stringify({ schemaVersion: 1, ownerTarget: { origin: noticeOrigin } }),
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
			() => json<{ point?: string; pid?: number }>(join(this.home, "barrier-hit.json"), {}),
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
