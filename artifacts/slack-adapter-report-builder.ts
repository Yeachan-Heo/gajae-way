const sourceHash = "sha256:bc28aafd226fc34e5fd65823450f6e58f40d78ffcd5831723193eb525a25b7ed";
const files = [
	"packages/adapter-slack/test/redteam.test.ts",
	"packages/gateway/test/slack-adapter-redteam.e2e.test.ts",
];
const junit = await Bun.file("artifacts/slack-adapter-redteam.junit.xml").text();
const expectations: Record<string, string> = {
	"01": "Ack before handler; handler failure does not poison later dispatch; retry dedupes; invalid/missing-id frames do not dispatch; disconnect creates second socket.",
	"02": "All six hostile timestamps drop before chat.send; subsequent valid message has a protocol-safe id.",
	"03": "Foreign-channel reply target must fail definitively before postMessage, per brief line 11; it must never become thread_ts.",
	"04": "Brief literal text-contains rule includes code-span mentions; normalize broadcasts and subteam labels; decode entities exactly once.",
	"05": "50k fenced message produces pieces <=4000 characters with balanced fences.",
	"06": "File-only message renders attachment body; message without files or text drops.",
	"07": "Own user, own bot_id, hidden, all SKIPPED_SUBTYPES, unchanged/empty edits and app_mention cause no gateway requests.",
	"08": "Bot-authored parent sets mentioned; missing parent_user_id leaves fromSelf absent.",
	"09": "TypeError produces ambiguous:true, SlackApiError ambiguous:false.",
	"10": "Three-chunk thread reply keeps thread_ts on every chunk, prefixes duplicate warning, confirms once.",
	"11": "Malformed target and unknown emojiName fail definitively without reaction API calls. Already-reacted coverage independently verified by RT-SLACK-25.",
	"12": "300 disconnected edits retain newest 256, log 44 evictions, and replay in order on adoptClient.",
	"13": "Failed chat.edit remains pending and schedules reconnect; adopted healthy client replays it.",
	"14": "Successful message id is not resent; unavailable message is forgotten and retried successfully.",
	"15": "Newest-first paginated history delivers ascending; watermark stops before unavailable message.",
	"16": "Recovered thread reply has thread origin and correct parent/channel id.",
	"17": "Third permanent failure quarantines; later gateway reconnection/pass must re-probe restored access and clear quarantine.",
	"18": "Twenty concurrent triggers run one pass at a time and coalesce to one follow-up.",
	"19": "Both incorrect token prefixes rejected without secret echo; relative token paths resolve from config; unknown channel key rejected.",
	"20": "Unconfigured unmentioned channel records no persona turn or delivery.",
	"21": "Namespaced mention-open policy engages and literal persona <script>& is escaped at Slack postMessage.",
	"22": "Persona check token maps to white_check_mark and confirmed Slack delivery; Telegram produces no reaction and explicit chat.react refuses it.",
	"23": "Monitor and loopback chat.react requests fail and leave no delivery rows.",
	"24": "Thread and channel have distinct session origin records and correctly routed confirmed replies.",
	"25": "Actual SlackWebApi already_reacted response is treated as success and delivery.confirm is emitted.",
	"26": "Real binary --help exits 0 and prints usage without booting.",
	"27": "Real binary --version exits 0 and prints semver.",
	"28": "Real binary unknown argument exits 2 and prints usage to stderr.",
	"29": "Real binary refuses second instance with live pidfile, exits 2, does not overwrite pidfile.",
};
const cases: Array<Record<string, unknown>> = [];
for (const file of files) {
	const lines = (await Bun.file(file).text()).split("\n");
	for (const [line, text] of lines.entries()) {
		const match = /^test\("(RT-SLACK-(\d+)) ([^"]+)"/.exec(text);
		if (!match) continue;
		const testcase = [...junit.matchAll(/<testcase\b[^>]*[\s\S]*?<\/testcase>|<testcase\b[^>]*\/>/g)].find((m) =>
			m[0].includes(match[1]!),
		)?.[0];
		if (!testcase) throw new Error(`Missing JUnit evidence ${match[1]}`);
		cases.push({
			id: match[1],
			scenario: match[3],
			expectedBehavior: expectations[match[2]!],
			verdict: testcase.includes("<failure") || testcase.includes("<error") ? "failed" : "passed",
			test: `${file}:${line + 1}`,
		});
	}
}
const cli = await Bun.file("artifacts/slack-adapter-cli-proof.json").json();
for (const probe of cli.probes)
	cases.push({
		id: probe.id,
		scenario: probe.command.join(" ") || "pidfile refusal",
		expectedBehavior: expectations[probe.id.slice(-2)],
		verdict: probe.verdict,
		test: "artifacts/slack-adapter-cli-probes.ts:8",
		artifact: "artifacts/slack-adapter-cli-proof.json",
	});
cases.sort((a, b) => String(a.id).localeCompare(String(b.id)));
const blockers = [
	{
		caseId: "RT-SLACK-03",
		contractRef: "/tmp/gajaeway-slack-brief.md:11",
		observed: "Foreign C2:1.0 reply target posts a top-level C1 reply and confirms instead of definitive failure.",
		source: "packages/adapter-slack/src/main.ts:replyThreadTs/settleSlackDelivery",
	},
	{
		caseId: "RT-SLACK-11",
		contractRef: "User minimum adversarial case 5; definitive invalid reaction settlement",
		observed:
			"Unknown emojiName throws generic Error, classified ambiguous:true; no API was called, so delivery failure should be definitive.",
		source:
			"packages/adapter-slack/src/reactions.ts:slackReactionFor; packages/adapter-slack/src/main.ts:settleSlackReaction",
	},
	{
		caseId: "RT-SLACK-17",
		contractRef: "User minimum adversarial case 7; recoverMissedMessages reconnect re-probe promise",
		observed:
			"After three missing_scope failures, subsequent pass and gateway reconnect never call history again (3 calls, expected 4). failures>=3 skip occurs before clean-pass clearing; no callback resets counters. Quarantine persists indefinitely, including across restarts via cursor file.",
		source: "packages/adapter-slack/src/main.ts:recoverMissedMessages/gateway.onConnected",
	},
];
const coverage = [
	["brief:8 Socket Mode immediate ack/retry/reconnect", ["01"]],
	["brief:10 Slack origins, channel/thread isolation", ["08", "16", "24"]],
	["brief:11 channel-qualified safe ids and definitive foreign-id refusal", ["02", "03"]],
	["brief:12 inbound entities/tokens and outbound escaping/chunking", ["04", "05", "21"]],
	["brief:14 reaction mapping and settlement", ["11", "22", "25"]],
	["brief:15 engagement, filtering, edits and missing parent metadata", ["04", "07", "08", "12", "13", "20", "21"]],
	["brief:16 attachment-only and empty bodies", ["06"]],
	["brief:17 pidfile single-instance state", ["29"]],
	["brief:18 credential loading and policy validation", ["19"]],
	["brief:23 gateway Slack admission and platform reaction guards", ["20", "21", "22", "23", "24"]],
	[
		"brief:26 delivery ambiguity, bounded edit replay, inbound dedupe, CLI",
		["09", "10", "11", "12", "13", "14", "26", "27", "28", "29"],
	],
	["brief:29 recovery order/watermark/thread/quarantine/single-flight", ["15", "16", "17", "18"]],
	["brief:32 compiled binary and real gateway e2e integration", ["20", "21", "22", "24", "26", "27", "28", "29"]],
].map(([contractRef, ids]) => ({
	contractRef,
	status: "covered",
	caseIds: (ids as string[]).map((id) => `RT-SLACK-${id}`),
}));
const invocation = `bun test ${files.join(" ")} --reporter=junit --reporter-outfile=artifacts/slack-adapter-redteam.junit.xml`;
const matrix = {
	sourceHash,
	contractCoverage: [
		...coverage,
		{
			contractRef: "Computer-use / GUI / web",
			status: "not_applicable",
			reason: "No such product surface belongs to this Slack adapter change.",
		},
	],
	surfaceEvidence: [
		{
			surface: "cli",
			invocation: "bun artifacts/slack-adapter-cli-probes.ts",
			verdict: "passed",
			artifactIds: ["artifacts/slack-adapter-cli-proof.json", "artifacts/slack-adapter-cli-replay.json"],
		},
		{
			surface: "api",
			invocation,
			verdict: "passed",
			detail: "All five real gateway socket cases passed.",
			artifactIds: ["artifacts/slack-adapter-redteam.junit.xml"],
		},
		{
			surface: "package",
			invocation,
			verdict: "failed",
			detail: "17 of 20 adapter tests passed; three demonstrated blockers.",
			artifactIds: ["artifacts/slack-adapter-redteam.junit.xml", "artifacts/slack-adapter-redteam-report.json"],
		},
	],
	adversarialCases: cases,
	blockers,
	artifactRefs: [
		"artifacts/slack-adapter-cli-replay.json",
		"artifacts/slack-adapter-cli-proof.json",
		"artifacts/slack-adapter-redteam.junit.xml",
		"artifacts/slack-adapter-redteam-report.json",
	],
	limitations: [
		"Parent rebased the branch mid-QA. Original frozen hash run: 24 cases, 21 passed, same three defects. Latest JUnit: 25 cases, 22 passed, same three defects after adapting only new gateway fixture to upstream attachTestBrokerOwnership. Intermediate fixture-drift failures are not attributed to the frozen hash.",
		"Coverage rows enumerate obligations exercised, not all brief obligations: working-status, slash-command authorization, cache eviction and service/doc integration were outside this new minimum red-team suite.",
		"RT-SLACK-17 disables automatic recovery scheduling for deterministic manual passes; source inspection establishes onConnected only triggers scheduler and never resets quarantine.",
		"Gateway teardown emits invalid socket write count: -32 warnings; these are retained, not suppressed.",
	],
};
const commit = Bun.spawnSync(["git", "rev-parse", "HEAD"]);
await Bun.write(
	"artifacts/slack-adapter-redteam-report.json",
	JSON.stringify(
		{
			schemaVersion: 1,
			kind: "api-package-test-report",
			...matrix,
			latestRunCommit: new TextDecoder().decode(commit.stdout).trim(),
			counts: { tests: 25, passed: 22, failed: 3, cliProbes: 4, cliPassed: 4 },
			executorQa: matrix,
		},
		null,
		2,
	) + "\n",
);
console.log(
	JSON.stringify({
		cases: cases.length,
		blockers: blockers.map((b) => b.caseId),
		report: "artifacts/slack-adapter-redteam-report.json",
	}),
);
