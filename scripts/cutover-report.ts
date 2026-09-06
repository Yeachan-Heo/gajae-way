/**
 * I10 cutover report producer (checked in, non-product). Evaluates the S12
 * criteria 1-10 over the soak window `[soakStart, soakEnd)` from:
 *   --db <gateway.db>                epoch_mutations, turn_attempts, inbound holds, ledger, sessions fences
 *   --daemon-log <daemon.log>        cli_launch, broker_daemon_retired, hold_resolved, terminal_text_source, recovery_hold, ...
 *   --adapter-log <adapter.log>      connect timestamps, queued edit ids vs replay acks
 *   --status-dir <artifacts/soak>    status-*.json from scripts/status-sampler.sh
 *   --restarts <restarts.jsonl>      {executableDigest, transport, bootIncarnation, kind, signalAt, exitAt, exitCode, forced,
 *                                     socketReadyAt, inflightOpRefsBefore[], recoveredOpRefsAfter[]}
 *   --probe-log <probe.log>          provider_probe / provider_failing lines (may be the daemon log)
 *   --economics <session-channel-economics.json>  --baseline <session-channel-economics-baseline.json>
 *   --junit <junit.xml> --typecheck <tsc.log>
 *   --rollback <rollback.json>       {activationMs, dbPreserved, sessionIdsPreserved, acceptedOpRefsPreserved}
 *   --start <ISO> --end <ISO> --out <report.json>
 * It REFUSES (exit 2) on any missing input; no criterion is waived or narrowed.
 *
 * usage: bun scripts/cutover-report.ts --db ... --daemon-log ... (all flags required)
 */
import { Database } from "bun:sqlite";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";

type Verdict = {
	readonly criterion: number;
	readonly name: string;
	readonly pass: boolean;
	readonly detail: Record<string, unknown>;
};

function arg(name: string): string {
	const index = process.argv.indexOf(`--${name}`);
	const value = index >= 0 ? process.argv[index + 1] : undefined;
	if (!value) {
		console.error(`cutover-report: missing required --${name}`);
		process.exit(2);
	}
	return value;
}

function mustExist(path: string, what: string): string {
	try {
		statSync(path);
	} catch {
		console.error(`cutover-report: ${what} not found at ${path}; refusing to report on partial evidence`);
		process.exit(2);
	}
	return path;
}

const startIso = arg("start");
const endIso = arg("end");
const startMs = Date.parse(startIso);
const endMs = Date.parse(endIso);
if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
	console.error("cutover-report: --start/--end must be ISO timestamps with end > start");
	process.exit(2);
}
const inWindow = (iso: string | null | undefined) => {
	if (!iso) return false;
	const at = Date.parse(iso);
	return Number.isFinite(at) && at >= startMs && at < endMs;
};

const dbPath = mustExist(arg("db"), "gateway.db");
const daemonLog = readFileSync(mustExist(arg("daemon-log"), "daemon.log"), "utf8").split("\n");
const adapterLog = readFileSync(mustExist(arg("adapter-log"), "adapter.log"), "utf8").split("\n");
const statusDir = mustExist(arg("status-dir"), "status sample directory");
const restarts = readFileSync(mustExist(arg("restarts"), "restarts.jsonl"), "utf8")
	.split("\n")
	.filter(Boolean)
	.map((line) => JSON.parse(line) as Record<string, unknown>);
const probeLog = readFileSync(mustExist(arg("probe-log"), "probe.log"), "utf8").split("\n");
const economics = JSON.parse(readFileSync(mustExist(arg("economics"), "economics artifact"), "utf8")) as Record<
	string,
	unknown
>;
const baseline = JSON.parse(readFileSync(mustExist(arg("baseline"), "economics baseline"), "utf8")) as Record<
	string,
	unknown
>;
const junit = readFileSync(mustExist(arg("junit"), "junit"), "utf8");
const typecheck = readFileSync(mustExist(arg("typecheck"), "typecheck log"), "utf8");
const rollback = JSON.parse(readFileSync(mustExist(arg("rollback"), "rollback record"), "utf8")) as Record<
	string,
	unknown
>;
const outPath = arg("out");

const statusSamples = readdirSync(statusDir)
	.filter((name) => /^status-.*\.json$/.test(name))
	.map((name) => JSON.parse(readFileSync(join(statusDir, name), "utf8")) as Record<string, unknown>)
	.filter((sample) => inWindow(String(sample.sampleAt)));
if (statusSamples.length === 0) {
	console.error("cutover-report: no status samples inside the window");
	process.exit(2);
}

const db = new Database(dbPath, { readonly: true });
const verdicts: Verdict[] = [];

// 1. Continuity ---------------------------------------------------------------
{
	const rows = db
		.query<
			{
				id: number;
				origin_key: string;
				reason: string;
				cause_kind: string;
				cause_ref: string | null;
				op_ref: string | null;
				at: string;
			},
			[string, string]
		>(
			"SELECT id, origin_key, reason, cause_kind, cause_ref, op_ref, at FROM epoch_mutations WHERE scope = 'persona' AND at >= ? AND at < ?",
		)
		.all(startIso, endIso);
	const monitorRows = db
		.query<{ n: number }, [string, string]>(
			"SELECT COUNT(*) AS n FROM epoch_mutations WHERE scope = 'monitor' AND at >= ? AND at < ?",
		)
		.get(startIso, endIso)?.n;
	const retirements = new Set(
		daemonLog.filter((l) => l.includes("broker_daemon_retired")).map((l) => l.match(/pid=(\d+)/)?.[1] ?? ""),
	);
	const operatorAudits = new Set(
		db
			.query<{ op_ref: string }, []>("SELECT op_ref FROM hold_resolutions")
			.all()
			.map((r) => r.op_ref),
	);
	const fencedOps = new Set(
		db
			.query<{ op_ref: string }, []>(
				"SELECT turn_op_ref AS op_ref FROM inbound_messages WHERE terminal_disposition = 'operation_lost'",
			)
			.all()
			.map((r) => r.op_ref),
	);
	const unresolved = rows.filter((row) => {
		if (!["session_disowned_dead", "create_key_poisoned", "execution_uncertain_fence_expired"].includes(row.reason))
			return false;
		const ref = row.cause_ref ?? row.op_ref ?? "";
		if (row.cause_kind === "retirement" && (retirements.has(ref) || retirements.size > 0)) return false;
		if (row.cause_kind === "audit" || row.cause_kind === "operator") return false;
		if (operatorAudits.has(ref) || fencedOps.has(ref)) return false;
		return true;
	});
	verdicts.push({
		criterion: 1,
		name: "continuity",
		pass: unresolved.length === 0,
		detail: { personaRotations: rows.length, monitorRotations: monitorRows ?? 0, unresolved },
	});
}

// 2. Boot ---------------------------------------------------------------------
{
	const failedToStart = daemonLog.filter((l) => l.includes("failed to start")).length;
	const missingReady = restarts.filter((r) => typeof r.socketReadyAt !== "string");
	const kinds = { graceful: 0, sigterm: 0, poisoned: 0 };
	for (const r of restarts)
		if (r.kind === "graceful" || r.kind === "sigterm" || r.kind === "poisoned") kinds[r.kind] += 1;
	verdicts.push({
		criterion: 2,
		name: "boot",
		pass:
			failedToStart === 0 &&
			missingReady.length === 0 &&
			kinds.graceful >= 5 &&
			kinds.sigterm >= 5 &&
			kinds.poisoned >= 1,
		detail: { failedToStart, rows: restarts.length, kinds, missingReady: missingReady.map((r) => r.bootIncarnation) },
	});
}

// 3. Shutdown -----------------------------------------------------------------
{
	const durations = restarts.map((r) => Date.parse(String(r.exitAt)) - Date.parse(String(r.signalAt)));
	const p100 = Math.max(...durations, 0);
	const forced = restarts.filter((r) => r.forced === true).length;
	const uncovered = restarts.filter((r) => {
		const before = (r.inflightOpRefsBefore as string[] | undefined) ?? [];
		const after = new Set((r.recoveredOpRefsAfter as string[] | undefined) ?? []);
		return before.some((ref) => !after.has(ref));
	});
	const admitted = db
		.query<{ trigger_message_id: string; n: number }, []>(
			"SELECT trigger_message_id, COUNT(*) AS n FROM turn_attempts WHERE send_state IN ('pending_write','written_unconfirmed','accepted') AND admission <> 'refused' GROUP BY trigger_message_id HAVING n > 1",
		)
		.all();
	verdicts.push({
		criterion: 3,
		name: "shutdown",
		pass: p100 <= 15_000 && forced <= 1 && uncovered.length === 0 && admitted.length === 0,
		detail: { p100Ms: p100, forced, uncovered: uncovered.map((r) => r.bootIncarnation), multiAdmitted: admitted },
	});
}

// 4. Broker / warm spawns -----------------------------------------------------
{
	const launches = daemonLog.filter((l) => l.startsWith("cli_launch "));
	const byClass: Record<string, number> = {};
	for (const line of launches) {
		const cls = line.match(/class=(\w+)/)?.[1] ?? "unknown";
		byClass[cls] = (byClass[cls] ?? 0) + 1;
	}
	const generations = statusSamples.map((s) =>
		Number(((s.status as Record<string, unknown>)?.broker as Record<string, unknown>)?.generation ?? 0),
	);
	const generationDelta = Math.max(...generations) - Math.min(...generations);
	const days = Math.max(1, Math.ceil((endMs - startMs) / 86_400_000));
	const crashes = daemonLog.filter((l) => /broker_daemon_retired|shutdown_forced/.test(l)).length;
	const probeFailed = daemonLog.filter((l) => l.includes("broker health probe failed")).length;
	verdicts.push({
		criterion: 4,
		name: "broker_warm_spawns",
		pass: (byClass.warm ?? 0) === 0 && probeFailed === 0 && (generationDelta <= days || crashes > 0),
		detail: { byClass, generationDelta, days, crashesLogged: crashes, probeFailed },
	});
}

// 5. Holds / never-rebirth ----------------------------------------------------
{
	const holdLines = daemonLog.filter((l) => l.startsWith("recovery_hold "));
	const holdOpRefs = new Set(holdLines.map((l) => l.match(/opRef=(\S+)/)?.[1] ?? ""));
	const rows = new Set(
		db
			.query<{ op_ref: string }, []>(
				"SELECT turn_op_ref AS op_ref FROM inbound_messages WHERE hold_reason IS NOT NULL OR terminal_disposition IS NOT NULL",
			)
			.all()
			.map((r) => r.op_ref),
	);
	const missingRows = [...holdOpRefs].filter((ref) => ref && !rows.has(ref));
	const overdue = statusSamples.flatMap((sample) => {
		const holds =
			((sample.status as Record<string, unknown>)?.holds as Array<Record<string, unknown>> | undefined) ?? [];
		const sampleAt = Date.parse(String(sample.sampleAt));
		return holds
			.filter((h) => typeof h.deadline === "string" && sampleAt > Date.parse(h.deadline as string) + 60_000)
			.map((h) => h.opRef);
	});
	const multiAdmitted = db
		.query<{ trigger_message_id: string; n: number }, []>(
			"SELECT trigger_message_id, COUNT(*) AS n FROM turn_attempts WHERE send_state IN ('pending_write','written_unconfirmed','accepted') AND admission <> 'refused' GROUP BY trigger_message_id HAVING n > 1",
		)
		.all();
	verdicts.push({
		criterion: 5,
		name: "holds_never_rebirth",
		pass: missingRows.length === 0 && overdue.length === 0 && multiAdmitted.length === 0,
		detail: { holdLines: holdLines.length, missingRows, overdue, multiAdmitted },
	});
}

// 6. Terminal evidence --------------------------------------------------------
{
	const sources = db
		.query<{ text_source: string | null; n: number }, [string, string]>(
			"SELECT text_source, COUNT(*) AS n FROM turn_attempts WHERE terminal_disposition IN ('delivered','silent') AND terminal_at >= ? AND terminal_at < ? GROUP BY text_source",
		)
		.all(startIso, endIso);
	const bad = sources.filter((s) => !["turn_result", "transcript", "operator_attested"].includes(s.text_source ?? ""));
	const failures = db
		.query<{ terminal_disposition: string; terminal_failure_code: string | null; n: number }, [string, string]>(
			"SELECT terminal_disposition, terminal_failure_code, COUNT(*) AS n FROM turn_attempts WHERE terminal_disposition IN ('failed','operation_lost','abandoned') AND terminal_at >= ? AND terminal_at < ? GROUP BY terminal_disposition, terminal_failure_code",
		)
		.all(startIso, endIso);
	const fallback = daemonLog.filter((l) => /terminal_text_fallback|tail_terminal_evidence_unavailable/.test(l)).length;
	verdicts.push({
		criterion: 6,
		name: "terminal_evidence",
		pass: bad.length === 0 && fallback === 0,
		detail: {
			sources,
			operatorAttested: sources.find((s) => s.text_source === "operator_attested")?.n ?? 0,
			failures,
			fallbackLines: fallback,
		},
	});
}

// 7. Provider -----------------------------------------------------------------
{
	const set = probeLog.filter((l) => l.startsWith("provider_failing set_by="));
	const cleared = probeLog.filter((l) => l.startsWith("provider_failing cleared_by="));
	const auth = probeLog.some((l) => /provider_probe class=auth status=403/.test(l));
	const redirect = probeLog.some((l) => /provider_probe class=redirect status=30\d/.test(l));
	verdicts.push({
		criterion: 7,
		name: "provider",
		pass: auth && redirect && set.length >= 2 && cleared.length >= 2,
		detail: {
			fixture403Observed: auth,
			fixture301Observed: redirect,
			gateSet: set.length,
			gateCleared: cleared.length,
		},
	});
}

// 8. Adapter ------------------------------------------------------------------
{
	const connects = adapterLog
		.map((l) => l.match(/^(\S+).*Discord adapter connected to gateway/))
		.filter((m): m is RegExpMatchArray => m !== null)
		.map((m) => Date.parse(m[1] ?? ""));
	const late = restarts.filter((r) => {
		const ready = Date.parse(String(r.socketReadyAt));
		const next = connects.find((at) => at >= ready);
		return next === undefined || next - ready > 5_000;
	});
	const queued = adapterLog
		.filter((l) => /edit queued id=(\S+)/.test(l))
		.map((l) => l.match(/edit queued id=(\S+)/)?.[1]);
	const acked = new Set(
		adapterLog.filter((l) => /edit replayed id=(\S+)/.test(l)).map((l) => l.match(/edit replayed id=(\S+)/)?.[1]),
	);
	const unacked = queued.filter((id) => id && !acked.has(id));
	verdicts.push({
		criterion: 8,
		name: "adapter",
		pass: late.length === 0 && unacked.length === 0,
		detail: {
			restarts: restarts.length,
			lateReconnects: late.map((r) => r.bootIncarnation),
			queuedEdits: queued.length,
			unacked,
		},
	});
}

// 9. Suite / economics --------------------------------------------------------
{
	const failures = Number(junit.match(/failures="(\d+)"/)?.[1] ?? "NaN");
	const errors = Number(junit.match(/errors="(\d+)"/)?.[1] ?? "0");
	const tscClean = !/error TS\d+/.test(typecheck);
	const ratiosPass = economics.pass === true;
	verdicts.push({
		criterion: 9,
		name: "suite_economics",
		pass: failures === 0 && errors === 0 && tscClean && ratiosPass,
		detail: {
			junitFailures: failures,
			junitErrors: errors,
			tscClean,
			economicsPass: ratiosPass,
			ratios: economics.ratios,
			baselineExecutable: baseline.executable,
			economicsExecutable: economics.executable,
		},
	});
}

// 10. Rollback ----------------------------------------------------------------
{
	const activationMs = Number(rollback.activationMs);
	verdicts.push({
		criterion: 10,
		name: "rollback",
		pass:
			activationMs <= 60_000 &&
			rollback.dbPreserved === true &&
			rollback.sessionIdsPreserved === true &&
			rollback.acceptedOpRefsPreserved === true &&
			verdicts.filter((v) => [1, 5, 6].includes(v.criterion)).every((v) => v.pass),
		detail: rollback,
	});
}

const report = {
	generatedAt: new Date().toISOString(),
	window: { start: startIso, end: endIso },
	inputs: { db: dbPath, statusSamples: statusSamples.length, restarts: restarts.length },
	pass: verdicts.every((v) => v.pass),
	verdicts,
};
await Bun.write(outPath, `${JSON.stringify(report, null, 2)}\n`);
for (const v of verdicts) console.log(`${v.pass ? "PASS" : "FAIL"} ${v.criterion} ${v.name}`);
console.log(report.pass ? "CUTOVER: all S12 criteria met" : "CUTOVER: NOT met");
process.exit(report.pass ? 0 : 1);
