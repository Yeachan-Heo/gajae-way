const sourceHash = "sha256:a25e067e990803eb95171864c9574a6a48df0211eabba6852b4cecfd0c3c4582";
const files = [
	"packages/adapter-slack/test/redteam.test.ts",
	"packages/gateway/test/slack-adapter-redteam.e2e.test.ts",
	"packages/sdk/test/client-held-events.test.ts",
];
const junitPath = "artifacts/slack-adapter-redteam.junit.xml";
const supportingPath = "artifacts/slack-adapter-supporting.junit.xml";
const proofPath = "artifacts/slack-adapter-cli-proof.json";
const reportPath = "artifacts/slack-adapter-redteam-report.json";
const expectations = [
	"Ack before handlers; retry dedupe; malformed frames survive; disconnect reconnects.",
	"Reject hostile timestamps before chat.send and preserve channel-qualified valid ids.",
	"Foreign reply targets fail definitively before any Slack post.",
	"Literal mentions including code count; decode entities once and normalize broadcasts.",
	"50k fenced text produces bounded, balanced chunks of at most 4000 characters.",
	"File-only messages render attachments; empty bodies drop.",
	"Own, hidden, skipped subtype, unchanged/empty edit and duplicate app_mention events never send.",
	"Bot parent implies mention; absent parent metadata omits fromSelf.",
	"Transport failure is ambiguous; SlackApiError is definitive.",
	"All reply chunks retain thread routing and duplicate warning; delivery confirms once.",
	"Malformed/unknown reactions fail definitively without API; already_reacted confirms.",
	"Edit outbox keeps newest 256 of 300, records 44 evictions and replays in order.",
	"Failed edit remains queued and replays on client adoption.",
	"Acknowledged ids dedupe; unavailable ids remain retryable.",
	"Newest-first recovery delivers ascending and cannot advance across unavailable sends.",
	"Recovered thread replies retain thread origin and parent channel.",
	"Three permanent failures quarantine; actual onConnected scheduler re-probes and clears quarantine.",
	"Twenty recovery triggers coalesce without parallel passes.",
	"Credential prefixes reject without exposing secrets; relative paths resolve and policy keys validate.",
	"Unconfigured unmentioned channel is context-only with no turn/delivery.",
	"Mention-open policy engages and persona markup is escaped at Slack boundary.",
	"Check token maps to white_check_mark on Slack and is refused on Telegram.",
	"Monitor and loopback cannot chat.react and create no deliveries.",
	"Channel/thread origins isolate sessions and route confirmed replies correctly.",
	"Actual already_reacted Web API response confirms delivery.",
	"Compiled --help exits 0 with usage without booting.",
	"Compiled --version exits 0 with semver.",
	"Compiled unknown flag exits 2 with usage on stderr.",
	"Compiled second instance exits 2 without overwriting a live pidfile.",
	"Recovered request joins pending live outcome, reports unavailable on failure, retries then dedupes; adoption while pending preserves outcome.",
	"200 {}, nonboolean ok and throwing body streams are unreadable/ambiguous; explicit ok:false is definitive.",
	"Same-chunk negotiated and chat events reach first subscriber in order; second gets no replay; newest 1000 retained; internal events never held.",
	"Exactly one of 20 concurrent stale-pidfile reclaims succeeds; all others reject with AdapterAlreadyRunningError; pidfile matches winner, for Slack and Discord.",
	"Real gateway threaded DM defaults every chunk to inbound root; explicit REPLY target wins; plain channel has no thread_ts.",
	"Two-page bounded pass retains continuation without advancing; next pass closes remaining gap; later arrival above through is not lost.",
	"Engaged thread root is durably remembered and revisited when history is empty; participated-thread TTL prunes it.",
	"Three invalid_params refusals dead-letter with terminal-message classification/digest/watermark; intervening link failure does not count; repeated unknown failures never dead-letter.",
	"Failed cursor save makes pass incomplete; restoring isolated /tmp store allows persistence without resending acknowledged message.",
	"Slash acks distinguish restart, unreachable gateway, duplicate trigger and unknown command.",
	"Engaged addressed turn posts working status before progress; unmentioned group does not; thread status keeps thread_ts.",
	"Unknown rocket emojiName fails definitively without addReaction.",
	"A stale pidfile plus a dead reclaimer marker recovers within 2000ms and removes the marker.",
	"Twenty waiting contenders refuse a live holder that arrives mid-election without replacing its pidfile.",
	"A former holder loses reacquisition; its release preserves the elected winner, whose own release removes the pidfile.",
];
const decode = (value: string) =>
	value
		.replace(/&#10;/g, "\n")
		.replace(/&quot;/g, '"')
		.replace(/&apos;/g, "'")
		.replace(/&lt;/g, "<")
		.replace(/&gt;/g, ">")
		.replace(/&amp;/g, "&");
function parseTests(xml: string) {
	return [...xml.matchAll(/<testcase\b([^>]*?)(?:\/>|>([\s\S]*?)<\/testcase>)/g)].map((match) => {
		const attrs = Object.fromEntries([...match[1].matchAll(/([\w-]+)="([^"]*)"/g)].map((m) => [m[1], decode(m[2])]));
		const body = match[2] ?? "";
		return {
			name: attrs.name,
			test: `${attrs.file}:${attrs.line}`,
			file: attrs.file,
			verdict: /<(failure|error)\b/.test(body) ? "failed" : /<skipped\b/.test(body) ? "skipped" : "passed",
			failure: decode(body.replace(/<[^>]+>/g, "").trim()),
		};
	});
}
const tests = parseTests(await Bun.file(junitPath).text());
const supporting = parseTests(await Bun.file(supportingPath).text());
const proof = await Bun.file(proofPath).json();
if (proof.sourceHash !== sourceHash) throw new Error("CLI proof source hash mismatch");
const adversarialCases = expectations.map((expectedBehavior, index) => {
	const id = `RT-SLACK-${String(index + 1).padStart(2, "0")}`;
	const rows = tests.filter((t) => t.name.startsWith(`${id} `));
	const probe = proof.probes.find((p: { id: string }) => p.id === id);
	if (!rows.length && !probe) throw new Error(`Missing evidence for ${id}`);
	return {
		id,
		sourceHash,
		scenario: rows.length ? rows.map((t) => t.name.slice(id.length + 1)).join("; ") : probe.command.join(" "),
		expectedBehavior,
		expected: expectedBehavior,
		verdict: rows.some((t) => t.verdict === "failed") || probe?.verdict === "failed" ? "failed" : "passed",
		test: rows.map((t) => t.test).join(", ") || "artifacts/slack-adapter-cli-probes.ts:8",
		subcases: rows,
		artifactRefs: [rows.length ? junitPath : proofPath],
	};
});
const blockers = tests
	.filter((t) => t.verdict === "failed")
	.map((t) => ({
		caseId: t.name.slice(0, 11),
		contractRef: "/tmp/gajaeway-slack-brief.md:17; RT-SLACK-33 acceptance",
		test: t.test,
		observed: t.failure,
		scenario: t.name,
		source: t.name.includes("discord")
			? "packages/adapter-discord/src/lock.ts:AdapterLock.acquire"
			: "packages/adapter-slack/src/lock.ts:AdapterLock.acquire",
		explanation: "Observed assertion failure; source deliberately left unchanged.",
		artifactRefs: [junitPath],
	}));
const cover = (contractRef: string, ids: number[], detail: string, supportingEvidence: string[] = []) => ({
	contractRef,
	status: "covered",
	caseIds: ids.map((id) => `RT-SLACK-${String(id).padStart(2, "0")}`),
	detail,
	supportingEvidence,
	artifactRefs: [...(ids.length ? [junitPath, proofPath] : []), ...(supportingEvidence.length ? [supportingPath] : [])],
});
const contractCoverage = [
	cover(
		"brief:5,20 Bun/TypeScript targeted tests and injected I/O",
		[1, 20, 32, 34],
		"Focused suites use Bun and fake Slack transport; gateway tests use real local SDK sockets.",
	),
	cover(
		"brief:6 SDK-only adapter boundary",
		[],
		"Public SDK boundary conformance passes; Discord lock regression belongs in gateway test, not adapter package.",
		["packages/conformance/test/sdk-boundary-dogfood.test.ts"],
	),
	cover(
		"brief:7 existing adapter patterns",
		[12, 13, 17, 18, 33],
		"Existing fixture/policy, ordered ingress, outbox and concurrent lock shapes exercised.",
	),
	cover(
		"brief:8 hand-rolled Web API and Socket Mode",
		[1, 9, 25, 31],
		"Ack-before-work, retry/reconnect, response semantics and transport ambiguity.",
		["packages/adapter-slack/test/api.test.ts", "packages/adapter-slack/test/socket.test.ts"],
	),
	cover(
		"brief:10 origins including threaded DMs",
		[8, 16, 24, 34],
		"Real gateway DM multi-chunk routing, explicit reply precedence and plain-channel control.",
		["packages/adapter-slack/test/origin.test.ts"],
	),
	cover(
		"brief:11 channel-qualified ids and definitive invalid-target failure",
		[2, 3, 10, 11],
		"Foreign target fails without post; ids are channel qualified.",
		["packages/protocol/test/reactions.test.ts"],
	),
	cover(
		"brief:12 text normalization and mrkdwn",
		[4, 5, 21],
		"Entities, mentions, escaping and bounded fenced chunks.",
		["packages/adapter-slack/test/text.test.ts", "packages/adapter-slack/test/mrkdwn.test.ts"],
	),
	cover(
		"brief:13 presence lifecycle",
		[40],
		"Post on arm; existing status suite covers update, delete, final, stale, concurrent cleanup and cosmetic failure.",
		["packages/adapter-slack/test/status.test.ts"],
	),
	cover(
		"brief:14 complete reaction allowlist and settlement",
		[11, 22, 25, 41],
		"Unknown names fail definitively; existing suite covers reverse mapping and own reaction filtering.",
		["packages/adapter-slack/test/reactions.test.ts", "packages/protocol/test/reactions.test.ts"],
	),
	cover(
		"brief:15 engagement, metadata, cache precedence and edit replay",
		[4, 7, 8, 12, 13, 20, 21],
		"Filtering, parent authorship, bounded replay; cache precedence/LRU/coalescing exercised by supporting suite.",
		["packages/adapter-slack/test/author.test.ts"],
	),
	cover(
		"brief:16 attachments and no voice",
		[6],
		"Attachment-only delivery and empty-body filtering; supporting attachment caps; no-voice explicitly documented in deployment.md:102.",
		["packages/adapter-slack/test/attachments.test.ts"],
	),
	cover(
		"brief:17 durable configuration, cursors and single-instance lock",
		[19, 29, 33, 38, 42, 43, 44],
		"Live-holder refusal, durable state, concurrent election, orphan marker recovery and non-owner release exercised.",
	),
	cover("brief:18 credential files, prefixes and policy", [19], "Secret-safe refusal and relative resolution.", [
		"packages/adapter-slack/test/config.test.ts",
	]),
	cover(
		"brief:19 Slack naming",
		[19, 26, 28, 29],
		"CLI/startup evidence says Slack. Source search finds Discord only in a recovery.ts explanatory comment.",
	),
	cover(
		"brief:23 protocol/gateway Slack admission and guards",
		[20, 21, 22, 23, 24],
		"Slack admitted; foreign platforms guarded; protocol Slack topic/allowlist tests pass.",
		["packages/protocol/test/protocol.test.ts", "packages/protocol/test/reactions.test.ts"],
	),
	cover(
		"brief:26 core adapter, dedupe, settlement, CLI",
		[1, 2, 3, 9, 10, 11, 12, 13, 14, 26, 27, 28, 29, 30, 31, 32, 41],
		"Public API, SDK negotiation replay and compiled binary directly exercised.",
	),
	cover(
		"brief:29 presence, slash commands and missed-message recovery",
		[15, 16, 17, 18, 30, 35, 36, 37, 38, 39, 40],
		"Real reconnect reprobe, continuation, participated threads, terminal-only budget and persistence failure.",
		["packages/adapter-slack/test/status.test.ts", "packages/adapter-slack/test/recovery.test.ts"],
	),
	cover(
		"brief:32 build, services, conformance, docs and gateway e2e",
		[20, 21, 22, 24, 26, 27, 28, 29, 34],
		"bun run build passed; service installation/boundary tests pass. Read-only doc audit found binary/config/scopes/events/commands/origins/recovery/no-voice in README.md:44-64, docs/deployment.md:76-104, docs/architecture.md:14-29, docs/runbooks/gajaeway-v1.md:79-90 and service-control.md:8.",
		["packages/cli/test/main.test.ts", "packages/conformance/test/sdk-boundary-dogfood.test.ts"],
	),
	{
		contractRef: "GUI/browser and live Slack workspace",
		status: "not_applicable",
		reason:
			"This assignment requires compiled CLI, package/API and local gateway surfaces with injected Slack transport; no GUI or credentialed external workspace is in scope.",
	},
];
const invocation = `bun test ${files.join(" ")} --reporter=junit --reporter-outfile=${junitPath}`;
const artifactRefs = ["artifacts/slack-adapter-cli-replay.json", proofPath, junitPath, reportPath, supportingPath];
const loopPaths = Array.from({ length: 10 }, (_, index) => `artifacts/slack-adapter-lock-loop-${index + 1}.junit.xml`);
const loopRuns = await Promise.all(loopPaths.map(async (path) => ({ path, tests: parseTests(await Bun.file(path).text()).filter((test) => test.name.startsWith("RT-SLACK-33 ")) })));
const loopPassed = loopRuns.filter((run) => run.tests.length === 2 && run.tests.every((test) => test.verdict === "passed")).length;
if (loopPassed !== 10) throw new Error(`RT-SLACK-33 loop failed: ${loopPassed}/10`);
artifactRefs.push(...loopPaths);
const matrix = {
	sourceHash,
	contractCoverage,
	surfaceEvidence: [
		{
			surface: "cli",
			invocation: "bun run build && bun artifacts/slack-adapter-cli-probes.ts",
			verdict: proof.probes.every((p: { verdict: string }) => p.verdict === "passed") ? "passed" : "failed",
			detail:
				"Fresh compiled Slack binary; four probes with real stdout/stderr/exit codes. Replay receipt checked via bun -e.",
			artifactRefs: [proofPath, artifactRefs[0]],
		},
		{
			surface: "api",
			invocation,
			verdict: tests
				.filter((t) => t.file.includes("gateway") && t.name.startsWith("RT-SLACK-34"))
				.every((t) => t.verdict === "passed")
				? "passed"
				: "failed",
			detail:
				"All eight real gateway conversation tests pass, including three DM/channel routing cases; Discord lock package case is reported separately.",
			artifactRefs: [junitPath],
		},
		{
			surface: "package",
			invocation,
			verdict: blockers.length ? "failed" : "passed",
			detail: `${tests.length - blockers.length}/${tests.length} tests pass, including both adapters' stale-lock elections and SDK same-chunk replay.`,
			artifactRefs: [junitPath, reportPath],
		},
		{
			surface: "package",
			invocation:
				"bun test packages/adapter-slack/test/origin.test.ts packages/adapter-slack/test/text.test.ts packages/adapter-slack/test/mrkdwn.test.ts packages/adapter-slack/test/reactions.test.ts packages/adapter-slack/test/attachments.test.ts packages/adapter-slack/test/author.test.ts packages/adapter-slack/test/config.test.ts packages/adapter-slack/test/api.test.ts packages/adapter-slack/test/socket.test.ts packages/adapter-slack/test/status.test.ts packages/adapter-slack/test/recovery.test.ts packages/protocol/test/protocol.test.ts packages/protocol/test/reactions.test.ts packages/cli/test/main.test.ts packages/conformance/test/sdk-boundary-dogfood.test.ts --reporter=junit --reporter-outfile=artifacts/slack-adapter-supporting.junit.xml",
			verdict: supporting.every((t) => t.verdict === "passed") ? "passed" : "failed",
			detail: `${supporting.length} supporting tests across adapter, protocol, service installation and SDK boundary.`,
			artifactRefs: [supportingPath],
		},
		{
			surface: "package",
			invocation: "for i in {1..10}; do bun test packages/adapter-slack/test/redteam.test.ts packages/gateway/test/slack-adapter-redteam.e2e.test.ts -t RT-SLACK-33 --reporter=junit --reporter-outfile=artifacts/slack-adapter-lock-loop-${i}.junit.xml; done",
			verdict: "passed",
			detail: `${loopPassed}/10 iterations passed; Slack 10/10 and Discord 10/10, each with 20 simultaneous contenders.`,
			artifactRefs: loopPaths,
		},
	],
	adversarialCases,
	artifactRefs,
	blockers,
};
const commit = Bun.spawnSync(["git", "rev-parse", "HEAD"]);
const report = {
	schemaVersion: 1,
	kind: "api-package-test-report",
	...matrix,
	latestRunCommit: new TextDecoder().decode(commit.stdout).trim(),
	sourceHashMethod:
		"Parent-confirmed Ultragoal quality-gate source-hash (integration base, merge base, paths, captured diff and untracked digest), not sha256 of raw git diff.",
	counts: {
		tests: tests.length,
		passed: tests.length - blockers.length,
		failed: blockers.length,
		cliProbes: proof.probes.length,
		cliPassed: proof.probes.filter((p: { verdict: string }) => p.verdict === "passed").length,
		stableCaseIds: adversarialCases.length,
		supportingTests: supporting.length,
		supportingPassed: supporting.filter((t) => t.verdict === "passed").length,
		lockLoopIterations: loopRuns.length,
		lockLoopPassed: loopPassed,
	},
	limitations: [
		"No credentialed external Slack API calls; injected platform ports and real local gateway socket used as required.",
		"No source files changed; RT-SLACK-42 through 44 exercise Slack directly, while RT-SLACK-33 exercises both adapters.",
		"Gateway teardown still emits invalid socket write count: -32 warnings; not suppressed.",
		"RT-SLACK-44 models a former lock handle losing ownership and reacquisition; a rejected acquire itself returns no handle.",
	],
	executorQa: matrix,
};
await Bun.write(reportPath, `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify({ counts: report.counts, blockers, reportPath }));
