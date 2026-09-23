import { Database } from "bun:sqlite";
import { describe, expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, readdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { OpsCycleResult } from "@gajae-gateway/protocol";
import { originKey, parseOriginKey } from "@gajae-gateway/protocol";
import { GajaewayClient } from "@gajae-gateway/sdk";
import {
	CLI_USAGE,
	COMMANDS,
	cycleExitCode,
	main,
	parseArgs,
	parseServicesArgs,
	renderCycle,
	restoreDatabase,
	restoreTargetPath,
	socketPath,
	USAGE_EXIT_CODE,
	usageFor,
	verifyBackupIntegrity,
} from "../src/main";
import { installServices, resolvePath } from "../src/services";

describe("cli arguments", () => {
	test("resolves home and socket override", () => {
		expect(socketPath("/tmp/gajae")).toBe("/tmp/gajae/gateway.sock");
		expect(parseArgs(["--socket", "/tmp/x", "status"])).toEqual({ command: "status", rest: [], socket: "/tmp/x" });
	});

	test("an unknown memory-audit argument is refused instead of degrading to a plain audit", async () => {
		// `memory audit --fix` used to connect, run the read-only audit and exit 1,
		// which reads as a repair attempt that reproduced the failure.
		const errors: string[] = [];
		const console_error = console.error;
		console.error = (message: unknown) => errors.push(String(message));
		const previousExit = process.exitCode;
		try {
			await main(["--socket", join(tmpdir(), "gajaeway-absent.sock"), "memory", "audit", "--fix"]);
			await main(["--socket", join(tmpdir(), "gajaeway-absent.sock"), "memory", "autolink", "--dry-run"]);
			expect(errors.join("\n")).toContain("unknown argument: --fix");
			expect(errors.join("\n")).toContain("unknown argument: --dry-run");
			expect(process.exitCode).toBe(1);
		} finally {
			console.error = console_error;
			process.exitCode = previousExit ?? 0;
		}
	});

	test("ops redeliver sends either the delivery id or an ISO since value", async () => {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-cli-redeliver-"));
		const path = join(home, "fake.sock");
		const received: Array<{ verb: string; params: unknown }> = [];
		const output: string[] = [];
		const originalLog = console.log;
		console.log = (line: unknown) => output.push(String(line));
		const listener = Bun.listen<{ buffer: string }>({
			unix: path,
			socket: {
				open(socket) {
					socket.data = { buffer: "" };
				},
				data(socket, data) {
					socket.data.buffer += Buffer.from(data).toString("utf8");
					let newline = socket.data.buffer.indexOf("\n");
					while (newline >= 0) {
						const line = socket.data.buffer.slice(0, newline);
						socket.data.buffer = socket.data.buffer.slice(newline + 1);
						const frame = JSON.parse(line) as { type: string; id?: string; verb?: string; params?: unknown };
						if (frame.type === "hello")
							socket.write(
								`${JSON.stringify({ v: "0.1", type: "negotiated", payload: { profileVersion: "v0.1" } })}\n`,
							);
						else if (frame.type === "request") {
							received.push({ verb: frame.verb as string, params: frame.params });
							socket.write(
								`${JSON.stringify({ v: "0.1", type: "response", id: frame.id, result: { requeued: [] } })}\n`,
							);
						}
						newline = socket.data.buffer.indexOf("\n");
					}
				},
			},
		});
		try {
			await main(["--socket", path, "ops", "redeliver", "delivery-42"]);
			await main(["--socket", path, "ops", "redeliver", "--since", "2026-09-23T10:00:00.000Z"]);
			expect(received).toEqual([
				{ verb: "ops.redeliver", params: { deliveryId: "delivery-42" } },
				{ verb: "ops.redeliver", params: { since: "2026-09-23T10:00:00.000Z" } },
			]);
			expect(output).toEqual(['{"requeued":[]}', '{"requeued":[]}']);
			expect(CLI_USAGE).toContain("ops redeliver <deliveryId>");
			expect(CLI_USAGE).toContain("ops redeliver --since <iso>");
		} finally {
			console.log = originalLog;
			listener.stop(true);
			await rm(home, { recursive: true, force: true });
		}
	});
});

describe("service installation", () => {
	test("install and repair select the same idempotent writer contract", () => {
		expect(parseServicesArgs(["install", "--bin-dir", "/opt/gajaeway/bin"]).action).toBe("install");
		expect(parseServicesArgs(["repair", "--bin-dir", "/opt/gajaeway/bin"]).action).toBe("repair");
	});
	test("managed, login, and fallback PATH entries are ordered and deduplicated", async () => {
		const commands: string[][] = [];
		const path = await resolvePath({
			binDir: "/opt/gajaeway/bin",
			home: "/Users/operator",
			env: { SHELL: "/bin/fish" },
			runtime: {},
			loginPathRunner: async (command) => {
				commands.push([...command]);
				return "~/bin:/opt/gajaeway/bin::/usr/bin:/usr/local/bin";
			},
		});
		expect(commands).toEqual([["/bin/fish", "-lc", 'printf %s "$PATH"']]);
		expect(path.split(":")).toEqual([
			"/opt/gajaeway/bin",
			"/Users/operator/bin",
			"/Users/operator/.local/bin",
			"/Users/operator/.bun/bin",
			"/usr/bin",
			"/usr/local/bin",
			"/bin",
			"/usr/sbin",
			"/sbin",
		]);
	});

	test("explicit PATH and disabled inheritance never invoke login discovery", async () => {
		let calls = 0;
		const runner = async (): Promise<string> => {
			calls++;
			throw new Error("login discovery must not run");
		};
		const explicit = await resolvePath({
			binDir: "/opt/bin",
			home: "/Users/operator",
			runtime: { path: ["~/custom", "/usr/bin"] },
			loginPathRunner: runner,
		});
		const disabled = await resolvePath({
			binDir: "/opt/bin",
			home: "/Users/operator",
			runtime: { inheritLoginPath: false },
			loginPathRunner: runner,
		});
		expect(calls).toBe(0);
		expect(explicit.split(":")).toEqual([
			"/opt/bin",
			"/Users/operator/bin",
			"/Users/operator/.local/bin",
			"/Users/operator/.bun/bin",
			"/Users/operator/custom",
			"/usr/bin",
			"/usr/local/bin",
			"/bin",
			"/usr/sbin",
			"/sbin",
		]);
		expect(disabled.split(":")).toEqual([
			"/opt/bin",
			"/Users/operator/bin",
			"/Users/operator/.local/bin",
			"/Users/operator/.bun/bin",
			"/usr/local/bin",
			"/usr/bin",
			"/bin",
			"/usr/sbin",
			"/sbin",
		]);
	});

	test("install and repair each rewrite exactly four safe LaunchAgent plists", async () => {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-services-"));
		try {
			await writeFile(
				join(home, "config.json"),
				JSON.stringify({
					schemaVersion: 1,
					credentials: { provider: { credentialFile: "TOP_SECRET" } },
					runtime: { path: ["~/custom", "/usr/bin"] },
				}),
			);
			const env = { HOME: "/Users/operator", GAJAEWAY_HOME: home };
			const writes: Array<{ path: string; contents: string }> = [];
			const writer = async (path: string, contents: string): Promise<void> => {
				writes.push({ path, contents });
			};
			const options = {
				binDir: "~/gaja&<bin>",
				// This case is about the launchd definitions, so it pins the platform
				// rather than depending on the host the suite happens to run on.
				platform: "darwin" as const,
				launchAgentsDir: join(home, "LaunchAgents"),
				env,
				loginPathRunner: async (): Promise<string> => "/should-not-be-read",
				writeFile: writer,
			};
			const installed = await installServices(options);
			expect(installed).toHaveLength(4);
			expect(writes).toHaveLength(4);
			expect(writes.map(({ path }) => path)).toEqual([...installed]);
			for (const { contents } of writes) {
				expect(contents).not.toContain("TOP_SECRET");
				expect(contents).toContain("&amp;");
				expect(contents).toContain("&lt;");
				expect(contents).toContain("&gt;");
				expect(contents).toContain("<key>GAJAEWAY_HOME</key>");
				expect(contents).toContain("<key>PATH</key>");
				expect(contents).toContain("StandardOutPath");
				expect(contents).toContain("StandardErrorPath");
			}
			const gateway = writes.find(({ path }) => path.endsWith("dev.gajaeway.gateway.plist"))?.contents ?? "";
			expect(gateway).toContain("<string>daemon</string>");
			expect(gateway).toContain("gateway.stdout.log");
			expect(gateway).toContain("gateway.stderr.log");
			const slack = writes.find(({ path }) => path.endsWith("dev.gajaeway.adapter-slack.plist"))?.contents ?? "";
			expect(slack).toContain("gajaeway-slack</string>");
			expect(slack).toContain("adapter-slack.stdout.log");
			writes.length = 0;
			await installServices(options);
			expect(writes).toHaveLength(4);
		} finally {
			await rm(home, { recursive: true, force: true });
		}
	});

	test("invalid service selection fails before any plist write", async () => {
		const writes: string[] = [];
		const errors: string[] = [];
		const originalError = console.error;
		const previousExitCode = process.exitCode;
		console.error = (line: unknown) => errors.push(String(line));
		try {
			await main(["services", "unknown", "--bin-dir", "/tmp/bin"], {
				services: {
					writeFile: async (path) => {
						writes.push(path);
					},
				},
			});
		} finally {
			console.error = originalError;
			process.exitCode = previousExitCode;
		}
		expect(writes).toHaveLength(0);
		expect(errors[0]).toContain("usage: gajaeway services install|repair");
	});
});

describe("list flag validation", () => {
	// A bad column name must not require a reachable gateway to be reported.
	const cases = [
		["monitors", "list", "--fields", "bogus"],
		["sessions", "list", "--fields", "bogus"],
		["monitors", "list", "--limit", "nope"],
	];
	for (const args of cases)
		test(`${args.join(" ")} fails before connecting`, async () => {
			const errors: string[] = [];
			const original = console.error;
			console.error = (line: string) => errors.push(line);
			const previousExit = process.exitCode ?? 0;
			try {
				await main(["--socket", "/nonexistent/gajaeway-list-test.sock", ...args]);
			} finally {
				console.error = original;
				process.exitCode = previousExit;
			}
			expect(errors).toHaveLength(1);
			expect(errors[0]).toMatch(/^(unknown field\(s\): bogus \(valid: |--limit expects a non-negative integer)/);
		});
});

/** A real, well-formed SQLite file with one marker row. */
function writeSqlite(path: string, marker: string): void {
	const database = new Database(path);
	try {
		database.exec("CREATE TABLE marker(value TEXT NOT NULL)");
		database.query("INSERT INTO marker(value) VALUES (?)").run(marker);
	} finally {
		database.close();
	}
}

test("offline restore preserves the current database then copies an integrity-checked backup", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-cli-restore-"));
	const previousHome = process.env.GAJAEWAY_HOME;
	process.env.GAJAEWAY_HOME = home;
	try {
		const databasePath = join(home, "gateway.db");
		const backupPath = join(home, "backup.db");
		await writeFile(databasePath, "current database");
		writeSqlite(backupPath, "backup");
		await restoreDatabase(join(home, "gateway.sock"), backupPath);
		expect(await readFile(databasePath)).toEqual(await readFile(backupPath));
		const preserved = (await Array.fromAsync(new Bun.Glob("gateway.db.pre-restore-*").scan({ cwd: home })))[0];
		expect(preserved).toBeString();
		expect(await Bun.file(join(home, preserved as string)).text()).toBe("current database");
	} finally {
		if (previousHome === undefined) delete process.env.GAJAEWAY_HOME;
		else process.env.GAJAEWAY_HOME = previousHome;
		await rm(home, { recursive: true, force: true });
	}
});

describe("verifyBackupIntegrity", () => {
	async function withHome(run: (home: string) => Promise<void>): Promise<void> {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-cli-restore-verify-"));
		try {
			await run(home);
		} finally {
			await rm(home, { recursive: true, force: true });
		}
	}

	test("a well-formed database passes and is left closed", async () => {
		await withHome(async (home) => {
			const path = join(home, "good.db");
			writeSqlite(path, "ok");
			expect(() => verifyBackupIntegrity(path)).not.toThrow();
			// Still openable afterwards: the verifier held no lock and left no journal.
			const database = new Database(path, { readonly: true });
			try {
				expect(database.query<{ value: string }, []>("SELECT value FROM marker").get()?.value).toBe("ok");
			} finally {
				database.close();
			}
		});
	});

	test("a file that is only the 16-byte SQLite header is refused", async () => {
		await withHome(async (home) => {
			const path = join(home, "header-only.db");
			await writeFile(path, Buffer.concat([Buffer.from("SQLite format 3\0"), Buffer.alloc(64, 0xff)]));
			expect(() => verifyBackupIntegrity(path)).toThrow(/Backup is not a readable SQLite database: .*header-only\.db/);
		});
	});

	test("a truncated copy of a real database is refused", async () => {
		await withHome(async (home) => {
			const good = join(home, "good.db");
			writeSqlite(good, "x".repeat(4096));
			const bytes = await readFile(good);
			const truncated = join(home, "truncated.db");
			await writeFile(truncated, bytes.subarray(0, Math.floor(bytes.length / 2)));
			expect(() => verifyBackupIntegrity(truncated)).toThrow(
				/Backup is not a readable SQLite database: .*truncated\.db/,
			);
		});
	});

	test("a zero-length file is refused even though SQLite calls it a valid empty database", async () => {
		await withHome(async (home) => {
			const path = join(home, "empty.db");
			await writeFile(path, "");
			expect(() => verifyBackupIntegrity(path)).toThrow(/Backup is an empty SQLite database: .*empty\.db/);
		});
	});

	test("a file of zero bytes with a non-zero length is refused", async () => {
		await withHome(async (home) => {
			const path = join(home, "zeros.db");
			await writeFile(path, Buffer.alloc(200));
			expect(() => verifyBackupIntegrity(path)).toThrow(/Backup is not a readable SQLite database: .*zeros\.db/);
		});
	});

	test("a WAL-mode backup passes and any sidecars land beside the backup, not elsewhere", async () => {
		await withHome(async (home) => {
			const path = join(home, "wal.db");
			const database = new Database(path);
			try {
				database.exec("PRAGMA journal_mode = WAL; CREATE TABLE marker(value TEXT NOT NULL)");
				database.query("INSERT INTO marker(value) VALUES (?)").run("wal");
			} finally {
				database.close();
			}
			const before = await readFile(path);
			expect(() => verifyBackupIntegrity(path)).not.toThrow();
			expect(await readFile(path)).toEqual(before);
			const entries = await readdir(home);
			for (const entry of entries) expect(entry.startsWith("wal.db")).toBe(true);
		});
	});

	test("a database whose integrity_check reports problems is refused with the report", async () => {
		await withHome(async (home) => {
			const good = join(home, "good.db");
			const source = new Database(good);
			try {
				source.exec("PRAGMA page_size = 4096; CREATE TABLE m(v TEXT); CREATE INDEX i ON m(v)");
				for (let index = 0; index < 40; index++)
					source.query("INSERT INTO m(v) VALUES (?)").run(`row-${index}-${"x".repeat(200)}`);
			} finally {
				source.close();
			}
			// Zero the body of the index root page but keep the file header and page
			// count intact. The page number comes from the schema rather than being
			// assumed. Which refusal fires is a SQLite build detail: some builds let
			// integrity_check report the missing index entries, others raise
			// SQLITE_CORRUPT while the pragma runs. Both must refuse the backup and
			// name the file, and neither may be silently restored.
			const inspect = new Database(good, { readonly: true });
			let rootPage: number;
			try {
				rootPage =
					inspect.query<{ rootpage: number }, []>("SELECT rootpage FROM sqlite_schema WHERE name = 'i'").get()
						?.rootpage ?? 0;
			} finally {
				inspect.close();
			}
			expect(rootPage).toBeGreaterThan(1);
			const bytes = Buffer.from(await readFile(good));
			bytes.fill(0, 4096 * (rootPage - 1) + 100, 4096 * rootPage);
			const damaged = join(home, "damaged.db");
			await writeFile(damaged, bytes);
			expect(() => verifyBackupIntegrity(damaged)).toThrow(
				/Backup (?:failed SQLite integrity_check|is not a readable SQLite database): .*damaged\.db \(/,
			);
		});
	});

	test("a non-database file is refused", async () => {
		await withHome(async (home) => {
			const path = join(home, "text.db");
			await writeFile(path, "not a database at all");
			expect(() => verifyBackupIntegrity(path)).toThrow(/Backup is not a readable SQLite database: .*text\.db/);
		});
	});

	test("a missing file is refused as unreadable", async () => {
		await withHome(async (home) => {
			expect(() => verifyBackupIntegrity(join(home, "absent.db"))).toThrow(/Backup is not readable/);
		});
	});
});

test("offline restore targets the configured dbPath, not the default gateway.db", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-cli-restore-dbpath-"));
	const previousHome = process.env.GAJAEWAY_HOME;
	process.env.GAJAEWAY_HOME = home;
	try {
		const databasePath = join(home, "custom", "live.db");
		await mkdir(join(home, "custom"));
		await writeFile(join(home, "config.json"), JSON.stringify({ schemaVersion: 1, dbPath: databasePath }));
		await writeFile(databasePath, "configured database");
		const backupPath = join(home, "backup.db");
		writeSqlite(backupPath, "backup");
		await restoreDatabase(join(home, "gateway.sock"), backupPath);
		expect(await readFile(databasePath)).toEqual(await readFile(backupPath));
		expect(await Bun.file(join(home, "gateway.db")).exists()).toBe(false);
		const preserved = (
			await Array.fromAsync(new Bun.Glob("live.db.pre-restore-*").scan({ cwd: join(home, "custom") }))
		)[0];
		expect(preserved).toBeString();
		expect(await Bun.file(join(home, "custom", preserved as string)).text()).toBe("configured database");
	} finally {
		if (previousHome === undefined) delete process.env.GAJAEWAY_HOME;
		else process.env.GAJAEWAY_HOME = previousHome;
		await rm(home, { recursive: true, force: true });
	}
});

describe("restoreTargetPath", () => {
	async function withHome(run: (home: string) => Promise<void>): Promise<void> {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-cli-restore-target-"));
		try {
			await run(home);
		} finally {
			await rm(home, { recursive: true, force: true });
		}
	}

	test("missing config.json falls back to <home>/gateway.db like gateway boot", async () => {
		await withHome(async (home) => {
			expect(await restoreTargetPath(home)).toBe(join(home, "gateway.db"));
		});
	});

	test("config.json without dbPath falls back to <home>/gateway.db", async () => {
		await withHome(async (home) => {
			await writeFile(join(home, "config.json"), JSON.stringify({ schemaVersion: 1 }));
			expect(await restoreTargetPath(home)).toBe(join(home, "gateway.db"));
		});
	});

	test("malformed config.json refuses instead of guessing the default", async () => {
		await withHome(async (home) => {
			await writeFile(join(home, "config.json"), "{ not json");
			await expect(restoreTargetPath(home)).rejects.toThrow(/Cannot parse .*config\.json; refusing restore/);
		});
	});

	test("empty dbPath refuses with the gateway's non-empty-string rule", async () => {
		await withHome(async (home) => {
			await writeFile(join(home, "config.json"), JSON.stringify({ schemaVersion: 1, dbPath: "" }));
			await expect(restoreTargetPath(home)).rejects.toThrow(/dbPath must be a non-empty string; refusing restore/);
		});
	});

	test("config.json whose root is not an object refuses", async () => {
		await withHome(async (home) => {
			await writeFile(join(home, "config.json"), "[]");
			await expect(restoreTargetPath(home)).rejects.toThrow(/config must be an object; refusing restore/);
		});
	});

	test("a config.json that exists but cannot be read refuses instead of defaulting", async () => {
		await withHome(async (home) => {
			// A directory in place of the file: readFile fails with EISDIR, not ENOENT.
			await mkdir(join(home, "config.json"));
			await expect(restoreTargetPath(home)).rejects.toThrow(/Cannot read .*config\.json; refusing restore/);
		});
	});

	test("a dangling config.json symlink is unreadable, not absent", async () => {
		await withHome(async (home) => {
			// readFile reports ENOENT here, exactly like a missing file; only lstat
			// tells the two apart. The gateway refuses to boot on this layout.
			await symlink(join(home, "moved-away.json"), join(home, "config.json"));
			await expect(restoreTargetPath(home)).rejects.toThrow(/Cannot read .*config\.json; refusing restore/);
		});
	});
});
function cycleResult(overrides: Partial<OpsCycleResult> = {}): OpsCycleResult {
	return {
		phase: "idle",
		gates: [],
		generatedAt: "2026-08-26T00:00:00.000Z",
		instanceId: "inst-1",
		memoryClosing: false,
		sessions: [],
		memoryIntents: { queued: 0, written: 0, committed: 0, receipted: 0, quarantined: 0 },
		monitorEvents: [],
		deliveries: { pending: 0, inflight: 0, confirmed: 0, failedAmbiguous: 0, expired: 0 },
		inFlightInbound: 0,
		pendingInbound: 0,
		contextDiff: {
			unread: 0,
			expired: 0,
			truncated: 0,
			omittedOldestAt: null,
			omittedNewestAt: null,
			floorAt: null,
		},
		lanes: { active: 0, max: 8 },
		...overrides,
	};
}

describe("cycle rendering", () => {
	test("healthy cycle renders phase and explicit none-gate, no sessions block", () => {
		const lines = renderCycle(cycleResult());
		expect(lines[0]).toBe("phase: idle");
		expect(lines).toContain("gates: none");
		expect(lines.join("\n")).not.toContain("sessions:");
	});

	test("gates render visibly with every reason named", () => {
		const lines = renderCycle(
			cycleResult({ phase: "degraded", gates: ["stale_session_identity", "memory_closure_blocked"] }),
		).join("\n");
		expect(lines).toContain("gates: stale_session_identity, memory_closure_blocked");
	});

	test("mid-rebind session renders the rebinding marker instead of a session id", () => {
		const lines = renderCycle(
			cycleResult({
				gates: ["stale_session_identity"],
				sessions: [
					{
						originKey: "discord/dm/c1/peer=p1",
						origin: { platform: "discord", kind: "dm", conversationId: "c1", peerId: "p1" },
						epoch: 4,
						sessionId: "",
						createdAt: "2026-08-01T00:00:00.000Z",
						lastActivityAt: null,
						pendingInbound: 2,
						unsettledDeliveries: 1,
						oldestUnsettledAgeMs: 42_000,
						contextDiff: {
							unread: 0,
							expired: 0,
							truncated: 0,
							omittedOldestAt: null,
							omittedNewestAt: null,
							floorAt: "2026-08-01T00:00:00.000Z",
						},
						bootstrap: {
							epoch: 4,
							pending: true,
							appliedAt: null,
							includedSections: [],
							byteCount: 0,
							truncated: false,
							diagnostics: [],
						},
					},
				],
			}),
		).join("\n");
		expect(lines).toContain("(rebinding)");
		expect(lines).toContain("discord/dm/c1/peer=p1");
		expect(lines).toContain("42s");
	});

	test("census lines always render so an empty subsystem is distinguishable from a missing one", () => {
		const lines = renderCycle(cycleResult()).join("\n");
		expect(lines).toContain("inbound: pending=0 inflight=0");
		expect(lines).toContain("context: unread=0 expired=0 truncated=0 omitted_oldest=- omitted_newest=-");
		expect(lines).toContain("deliveries: pending=0 inflight=0 confirmed=0 failed_ambiguous=0 expired=0");
		expect(lines).toContain("memory: queued=0 written=0 committed=0 receipted=0 quarantined=0");
		expect(lines).toContain("monitors: none");
	});

	test("exit-code contract: gates force exit 1, healthy is exit 0", () => {
		expect(cycleExitCode(cycleResult())).toBe(0);
		for (const gate of [
			"stale_session_identity",
			"delivery_settlement_unknown",
			"memory_closure_blocked",
			"monitor_settlement_failed",
		])
			expect(cycleExitCode(cycleResult({ gates: [gate as OpsCycleResult["gates"][number]] }))).toBe(1);
	});
});
describe("usage guard", () => {
	test("an empty argv is a usage error, not a dispatchable command", () => {
		expect(usageFor(undefined)).toBe(CLI_USAGE);
		expect(parseArgs([]).command).toBeUndefined();
		expect(usageFor(parseArgs([]).command)).toBe(CLI_USAGE);
	});

	test("an unknown subcommand is a usage error", () => {
		for (const command of ["bogus", "--help", "-h", "status-ish", ""]) expect(usageFor(command)).toBe(CLI_USAGE);
	});

	test("every dispatchable command passes the guard", () => {
		for (const command of COMMANDS) expect(usageFor(command)).toBeUndefined();
	});

	test("a socket override alone still resolves to no command", () => {
		expect(usageFor(parseArgs(["--socket", "/tmp/x"]).command)).toBe(CLI_USAGE);
	});
});

describe("usage exits the process instead of blocking", () => {
	async function run(args: string[]): Promise<{ code: number; stderr: string; stdout: string }> {
		const child = Bun.spawn(["bun", join(import.meta.dir, "../src/main.ts"), ...args], {
			stdout: "pipe",
			stderr: "pipe",
			env: { ...process.env, GAJAEWAY_HOME: join(tmpdir(), "gajaeway-cli-usage-nonexistent") },
		});
		const [stdout, stderr, code] = await Promise.all([
			new Response(child.stdout).text(),
			new Response(child.stderr).text(),
			child.exited,
		]);
		return { code, stderr, stdout };
	}

	test("no arguments prints usage on stderr and exits non-zero", async () => {
		const result = await run([]);
		expect(result.code).toBe(USAGE_EXIT_CODE);
		expect(result.stderr).toContain(CLI_USAGE);
		expect(result.stdout).toBe("");
	}, 30_000);

	test("an unknown subcommand prints usage on stderr and exits non-zero", async () => {
		const result = await run(["bogus"]);
		expect(result.code).toBe(USAGE_EXIT_CODE);
		expect(result.stderr).toContain(CLI_USAGE);
	}, 30_000);
});

describe("work operator commands", () => {
	async function run(args: string[], result: unknown) {
		const requests: Array<{ verb: string; params: unknown }> = [];
		const lines: string[] = [];
		const errors: string[] = [];
		let closed = false;
		const client: GajaewayClient = Object.create(GajaewayClient.prototype);
		client.request = async <T>(verb: string, params?: unknown): Promise<T> => {
			requests.push({ verb, params });
			if (result instanceof Error) throw result;
			return result as T;
		};
		client.close = async () => {
			closed = true;
		};
		const connect = spyOn(GajaewayClient, "connectSocket").mockResolvedValue(client);
		const log = spyOn(console, "log").mockImplementation((line) => {
			lines.push(String(line));
		});
		const error = spyOn(console, "error").mockImplementation((line) => {
			errors.push(String(line));
		});
		const previousExit = process.exitCode;
		try {
			await main(["--socket", "/test/work.sock", "work", ...args]);
			return {
				requests,
				lines,
				errors,
				closed,
				connections: connect.mock.calls.length,
				exitCode: process.exitCode,
				connectOptions: connect.mock.calls[0]?.[1],
			};
		} finally {
			connect.mockRestore();
			log.mockRestore();
			error.mockRestore();
			process.exitCode = previousExit;
		}
	}

	for (const [flag, value, model] of [
		["--preset", "reliable", { preset: "reliable" }],
		["--model", "openai/gpt-5.2", "openai/gpt-5.2"],
	] as const) {
		test(`work run forwards ${flag} with cwd and resume`, async () => {
			const output = await run(["run", "fix", "--cwd", "/repo", "--resume", flag, value, "fix", "tests"], {
				held: false,
				text: "done",
			});
			expect(output.requests).toEqual([
				{
					verb: "work.run",
					params: { name: "fix", cwd: "/repo", resume: true, model, text: "fix tests" },
				},
			]);
			expect(output.lines).toEqual(["done"]);
			expect(output.errors).toEqual([]);
			expect(output.closed).toBe(true);
		});
	}

	for (const [flag, value, model] of [
		["--preset", "reliable", { preset: "reliable" }],
		["--model", "openai/gpt-5.2", "openai/gpt-5.2"],
	] as const) {
		test(`work start forwards ${flag} and canonical notification target`, async () => {
			const output = await run(
				[
					"start",
					"fix",
					"--cwd",
					"/repo",
					"--resume",
					flag,
					value,
					"--notify",
					"telegram/topic/-100/parent=-200",
					"fix",
					"tests",
				],
				{
					started: true,
					jobId: "job-1",
					opRef: "op-1",
					sessionKey: "work/task/fix",
					sessionId: "session-1",
				},
			);
			expect(output.requests).toEqual([
				{
					verb: "work.start",
					params: {
						name: "fix",
						cwd: "/repo",
						resume: true,
						model,
						text: "fix tests",
						notify: { platform: "telegram", kind: "topic", conversationId: "-100", parentId: "-200" },
					},
				},
			]);
			expect(output.lines).toEqual(["started: work/task/fix session=session-1 job=job-1 op=op-1"]);
			expect(output.connectOptions).toEqual({ requestTimeoutMs: 120_000 });
			expect(output.closed).toBe(true);
		});
	}

	for (const key of [
		"loopback/loopback/loopback",
		"discord/channel/123",
		"discord/dm/123/peer=456",
		"discord/thread/123/parent=456",
		"monitor/eventtype/build.done",
	]) {
		test(`work start accepts canonical origin ${key}`, async () => {
			expect(originKey(parseOriginKey(key))).toBe(key);
			const output = await run(["start", "fix", "--notify", key, "task"], {
				started: true,
				jobId: "job-1",
				opRef: "op-1",
				sessionKey: "work/task/fix",
				sessionId: "session-1",
			});
			expect(output.errors).toEqual([]);
			expect(output.requests).toHaveLength(1);
		});
	}

	test("work start without notify leaves target selection to the gateway", async () => {
		const output = await run(["start", "fix", "task"], {
			started: true,
			jobId: "j",
			opRef: "o",
			sessionKey: "k",
			sessionId: "s",
		});
		expect(output.requests).toEqual([{ verb: "work.start", params: { name: "fix", text: "task" } }]);
	});

	for (const command of ["run", "start"]) {
		test(`work ${command} prints held without claiming acceptance`, async () => {
			const output = await run([command, "fix", "task"], {
				...(command === "start" ? { started: false } : {}),
				held: true,
				jobId: "job-1",
				state: "awaiting_operator",
				reason: "terminal_uncertain",
			});
			expect(output.lines).toEqual(["HELD: terminal_uncertain\njob: job-1 state: awaiting_operator"]);
			expect(output.exitCode).toBe(1);
			expect(output.closed).toBe(true);
		});
	}

	test("work run remains response-only with a caller wait budget", async () => {
		const output = await run(["run", "fix", "task"], { held: false, text: "answer" });
		expect(output.requests).toEqual([{ verb: "work.run", params: { name: "fix", text: "task" } }]);
		expect(output.connectOptions).toEqual({ requestTimeoutMs: 3_600_000 });
		expect(output.lines).toEqual(["answer"]);
	});

	for (const op of [null, { status: "unknown" }, { status: "terminal_ok", outcome: { reason: "cancelled" } }]) {
		test(`work status prints exact snapshot for ${JSON.stringify(op)}`, async () => {
			const result = {
				jobId: "job-1",
				state: "awaiting_operator",
				sessionId: "",
				lastActivityAt: null,
				attempt: null,
				op,
			};
			const output = await run(["status", "fix"], result);
			expect(output.requests).toEqual([{ verb: "work.status", params: { name: "fix" } }]);
			expect(output.lines).toEqual([JSON.stringify(result)]);
			expect(output.closed).toBe(true);
		});
	}

	test("work steer returns the accepted correlation ref", async () => {
		const output = await run(["steer", "fix", "focus", "tests"], { steered: true, clientRef: "client-exact" });
		expect(output.requests).toEqual([{ verb: "work.steer", params: { name: "fix", text: "focus tests" } }]);
		expect(output.lines).toEqual(["steered: client-exact"]);
		expect(output.closed).toBe(true);
	});

	test("work steer refusal is not acceptance", async () => {
		const output = await run(["steer", "fix", "focus"], { steered: false, reason: "steer_refused:sdk_refused" });
		expect(output.lines).toEqual(["not steered: steer_refused:sdk_refused"]);
		expect(output.exitCode).toBe(1);
	});

	for (const command of ["run", "start", "status", "steer"]) {
		test(`work ${command} reports gateway errors and closes without retry`, async () => {
			const output = await run(
				[command, "fix", ...(command === "status" ? [] : ["task"])],
				new Error("work status unavailable"),
			);
			expect(output.lines).toEqual([]);
			expect(output.errors).toEqual(["work status unavailable"]);
			expect(output.requests).toHaveLength(1);
			expect(output.exitCode).toBe(1);
			expect(output.closed).toBe(true);
		});
	}

	for (const closed of [true, false]) {
		test(`work retire prints a successful retirement with closed=${closed}`, async () => {
			const output = await run(["retire", "fix"], {
				retired: true,
				sessionKey: "work/task/fix",
				sessionId: "session-1",
				closed,
			});
			expect(output.requests).toEqual([{ verb: "work.retire", params: { name: "fix" } }]);
			expect(output.lines).toEqual([`retired: work/task/fix session=session-1 closed=${closed}`]);
			expect(output.closed).toBe(true);
		});
	}

	test("work retire prints refusal without inventing a session", async () => {
		const output = await run(["retire", "fix"], {
			retired: false,
			sessionKey: "work/task/fix",
			reason: "active attempt",
		});
		expect(output.lines).toEqual(["not retired: active attempt"]);
		expect(output.closed).toBe(true);
	});

	test("work jobs renders bound and unbound lanes", async () => {
		const output = await run(["jobs"], {
			jobs: [
				{
					lane_key: "fix",
					state: "running",
					session_id: "session-1",
					last_activity_at: "2026-09-07T00:00:00Z",
					worktree_path: "/repo/fix",
				},
				{ lane_key: "done", state: "done", session_id: "", last_activity_at: null, worktree_path: "/repo/done" },
			],
		});
		expect(output.requests).toEqual([{ verb: "work.jobs", params: undefined }]);
		expect(output.lines).toEqual([
			"fix running session=session-1 last=2026-09-07T00:00:00Z /repo/fix",
			"done done session=- last=- /repo/done",
		]);
		expect(output.closed).toBe(true);
	});

	test("work jobs distinguishes quarantined history from current running lanes", async () => {
		const output = await run(["jobs"], {
			jobs: [
				{
					lane_key: "work-current",
					state: "running",
					session_id: "global-session",
					last_activity_at: "2026-09-09T00:00:00Z",
					worktree_path: "/repo/current",
				},
				{
					lane_key: "work-legacy",
					state: "running",
					session_id: "private-session",
					last_activity_at: "2026-09-07T00:00:00Z",
					worktree_path: "/repo/legacy",
					quarantined: true,
					reason: "broker_authority_quarantined",
				},
			],
		});
		expect(output.requests).toEqual([{ verb: "work.jobs", params: undefined }]);
		expect(output.lines).toEqual([
			"current running session=global-session last=2026-09-09T00:00:00Z /repo/current",
			"legacy HELD: quarantined reason=broker_authority_quarantined historical_state=running session=private-session last=2026-09-07T00:00:00Z /repo/legacy",
		]);
		expect(output.errors).toEqual([]);
		expect(output.closed).toBe(true);
	});

	for (const args of [
		...[
			"discord/dm/c",
			"discord/channel/c/peer=p",
			"discord/thread/t",
			"discord/thread/t/peer=p",
			"discord/dm/c/peer=p/peer=q",
			"discord/dm/c/peer=",
			"discord/channel/c/",
			"discord/channel/c/extra",
			"discord/channel/a%2Fb",
			" discord/channel/c",
			"discord/channel/c ",
			"unknown/channel/c",
			"discord/channel/c//",
			"loopback/channel/c",
			"monitor/channel/c",
			"discord/dm/c/peer=p=extra",
		].map((key) => ["start", "fix", "--notify", key, "task"]),
		["run", "fix", "--notify", "discord/channel/c", "task"],
		["start", "fix", "--notify"],
		["start", "fix", "--notify", "discord/channel/c", "--notify", "discord/channel/d", "task"],
		["start", "fix", "--model", "model", "--preset", "preset", "task"],
		["start", "fix", "--preset", "preset", "--model", "model", "task"],
		["start", "fix", "--model"],
		["start", "fix", "--preset", "--resume", "task"],
		["start", "fix", "--cwd", "relative", "task"],
		["run", "fix", "--cwd", "relative", "task"],
		["start", "bad/name", "task"],
		["start", "fix", "   "],
		["status"],
		["status", "fix", "extra"],
		["steer", "fix"],
		["steer", "fix", "--resume", "task"],
		["run", "fix", "--model", "model", "--preset", "preset", "task"],
		["run", "fix", "--preset", "preset", "--model", "model", "task"],
		["run", "fix", "task", "--model"],
		["run", "fix", "task", "--preset"],
		["run", "fix", "task", "--preset", "--resume"],
		["retire"],
		["retire", "fix", "extra"],
		["jobs", "extra"],
	]) {
		test(`invalid work arguments fail before connecting: ${args.join(" ")}`, async () => {
			const output = await run(args, {});
			expect(output.connections).toBe(0);
			expect(output.requests).toEqual([]);
			expect(output.errors[0]).toContain("usage: gajaeway work");
		});
	}
});
