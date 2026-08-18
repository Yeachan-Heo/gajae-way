import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { BootstrapError, bootstrapMainSession, recoverBootstrap } from "../../src/main-session/bootstrap";

import { createMainSessionHost } from "../../src/main-session/host";
import { approveProfile, previewProfileApproval } from "../../src/main-session/profile-approval";
import { ResumeError, strictResumeMainSession } from "../../src/main-session/resume";
import { GatewayStateStore } from "../../src/main-session/state";
import { loadWayProfile } from "../../src/profile";
import { FileSdkDouble, MemoryGatewayMeta } from "../helpers/main-session";

const temporaryDirectories: string[] = [];

afterEach(() => {
	for (const directory of temporaryDirectories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

function fixture(profileTail = ""): { profilePath: string; corpus: string; workspace: string } {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-main-session-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = ["SOUL.md", "USER.md", "daily/{date}.md", "MEMORY.md"]

[restricted_files]
conversation = ["MEMORY.md"]

[surfaces.owner]
id = "owner"
platform = "test"
kind = "dm"

[operator]
id = "operator-1"
${profileTail}`,
	);
	return { profilePath, corpus, workspace };
}

async function committedFixture(): Promise<{
	profilePath: string;
	state: GatewayStateStore;
	backend: MemoryGatewayMeta;
	sdk: FileSdkDouble;
}> {
	const setup = fixture();
	const profile = loadWayProfile(setup.profilePath);
	const backend = new MemoryGatewayMeta();
	const state = new GatewayStateStore(backend);
	const sdk = new FileSdkDouble();
	await bootstrapMainSession({ confirm: true, profile, state, sdk });
	return { profilePath: setup.profilePath, state, backend, sdk };
}

class KillBoundary extends Error {}

test("bootstrap recovers every durable FSM boundary without guessing", async () => {
	const boundaries = [
		"afterCreatingIntent",
		"afterSdkCreate",
		"afterFirstMessage",
		"afterCreated",
		"beforeCommit",
		"afterCommit",
	] as const;
	for (const boundary of boundaries) {
		const setup = fixture();
		const profile = loadWayProfile(setup.profilePath);
		const state = new GatewayStateStore(new MemoryGatewayMeta());
		const sdk = new FileSdkDouble();
		await expect(
			bootstrapMainSession({
				confirm: true,
				profile,
				state,
				sdk,
				hooks: {
					[boundary]: () => {
						if (boundary === "afterSdkCreate") {
							expect(sdk.createdSessionFiles).toHaveLength(1);
							expect(fs.existsSync(sdk.createdSessionFiles[0] as string)).toBe(false);
						}
						throw new KillBoundary(boundary);
					},
				},
			}),
		).rejects.toBeInstanceOf(KillBoundary);
		const recovered = await recoverBootstrap({ profile, state, sdk });
		if (boundary === "afterCreatingIntent" || boundary === "afterSdkCreate") {
			expect(recovered).toEqual({ kind: "bootstrap_required" });
			expect(state.read().bootstrapState).toBe("ABSENT");
		} else {
			expect(recovered.kind).toBe("committed");
			expect(state.read().bootstrapState).toBe("COMMITTED");
		}
	}
});

test("bootstrap times out cleanly when the deferred transcript never appears", async () => {
	const setup = fixture();
	const state = new GatewayStateStore(new MemoryGatewayMeta());
	const sdk = new FileSdkDouble({ persistBootstrapTranscript: false });
	await expect(
		bootstrapMainSession({
			confirm: true,
			profile: loadWayProfile(setup.profilePath),
			state,
			sdk,
			transcriptTimeoutMs: 25,
			transcriptPollMs: 1,
		}),
	).rejects.toMatchObject({ reason: "bootstrap_transcript_timeout" } satisfies Partial<BootstrapError>);
	expect(state.read().bootstrapState).toBe("ABSENT");
	expect(fs.existsSync(sdk.createdSessionFiles[0] as string)).toBe(false);
	const retry = await bootstrapMainSession({
		confirm: true,
		profile: loadWayProfile(setup.profilePath),
		state,
		sdk: new FileSdkDouble(),
	});
	expect(retry.kind).toBe("committed");
});

test("bootstrap orphan scan fails closed when a nonce maps to multiple transcripts", async () => {
	const setup = fixture();
	const profile = loadWayProfile(setup.profilePath);
	const state = new GatewayStateStore(new MemoryGatewayMeta());
	const sdk = new FileSdkDouble();
	const intent = { nonce: "orphan-nonce", ts: 100 };
	state.markCreating(intent);
	await sdk.createOrphan(profile.workspace, intent.nonce);
	await sdk.createOrphan(profile.workspace, intent.nonce);
	const result = await recoverBootstrap({ profile, state, sdk });
	expect(result).toEqual({ kind: "failed_closed", reason: "bootstrap_orphan_ambiguous" });
	expect(state.read()).toMatchObject({ bootstrapState: "FAILED_CLOSED", failedClosedReason: "bootstrap_orphan_ambiguous" });
});

test("open growth intent recovers append-only transcript growth by prefix attestation", async () => {
	const { profilePath, state, sdk } = await committedFixture();
	const profile = loadWayProfile(profilePath);
	const initial = state.read().mainIdentity;
	if (!initial) throw new Error("missing initial identity");
	state.writeGrowthIntent(initial, 777);
	sdk.appendRaw(initial.canonicalPath, { type: "message", role: "user", content: "mid-turn append" });
	const resumed = await strictResumeMainSession({ profile, state, sdk });
	expect(resumed.recoveredGrowthIntent).toBe(true);
	expect(state.read().growthIntent).toBeUndefined();
	expect(state.read().mainIdentity?.size).toBeGreaterThan(initial.size);
});

test("tampering a growth prefix fails closed instead of accepting a transcript rewrite", async () => {
	const { profilePath, state, sdk } = await committedFixture();
	const profile = loadWayProfile(profilePath);
	const initial = state.read().mainIdentity;
	if (!initial) throw new Error("missing initial identity");
	state.writeGrowthIntent(initial, 777);
	const transcript = fs.readFileSync(initial.canonicalPath, "utf8");
	fs.writeFileSync(initial.canonicalPath, transcript.replace("way-bootstrap", "wayXbootstrap"));
	await expect(strictResumeMainSession({ profile, state, sdk })).rejects.toMatchObject({ reason: "growth_intent_mismatch" } satisfies Partial<ResumeError>);
	expect(state.read()).toMatchObject({ bootstrapState: "FAILED_CLOSED", failedClosedReason: "growth_intent_mismatch" });
});

test("open growth intent with no append clears idempotently", async () => {
	const { profilePath, state, sdk } = await committedFixture();
	const profile = loadWayProfile(profilePath);
	const initial = state.read().mainIdentity;
	if (!initial) throw new Error("missing initial identity");
	state.writeGrowthIntent(initial, 778);
	const resumed = await strictResumeMainSession({ profile, state, sdk });
	expect(resumed.recoveredGrowthIntent).toBe(true);
	expect(state.read().mainIdentity).toEqual(initial);
	expect(state.read().growthIntent).toBeUndefined();
	await resumed.session.dispose();
});

test("same-session restart opens the persisted session identity every time", async () => {
	const { profilePath, state, sdk } = await committedFixture();
	const profile = loadWayProfile(profilePath);
	const first = await strictResumeMainSession({ profile, state, sdk });
	await first.session.dispose();
	const second = await strictResumeMainSession({ profile, state, sdk });
	await second.session.dispose();
	expect(first.identity.sessionId).toBe(second.identity.sessionId);
	expect(sdk.openedIdentities).toHaveLength(2);
	expect(sdk.openedIdentities[0]?.canonicalPath).toBe(sdk.openedIdentities[1]?.canonicalPath);
});

test("profile drift blocks resume until explicit approval, while tunables remain resumable", async () => {
	const { profilePath, state, backend, sdk } = await committedFixture();
	const original = fs.readFileSync(profilePath, "utf8");
	fs.writeFileSync(profilePath, original.replace('"SOUL.md", "USER.md"', '"USER.md", "SOUL.md"'));
	const changedProfile = loadWayProfile(profilePath);
	await expect(strictResumeMainSession({ profile: changedProfile, state, sdk })).rejects.toMatchObject({ reason: "profile_drift" } satisfies Partial<ResumeError>);
	expect(state.read().failedClosedReason).toBe("profile_drift");
	const preview = previewProfileApproval(state, changedProfile);
	expect(preview.changes.length).toBeGreaterThan(0);
	const approval = approveProfile(state, changedProfile, true, { now: () => 999, receiptId: () => "receipt-1" });
	expect(approval.receiptId).toBe("receipt-1");
	expect(backend.events).toEqual([expect.objectContaining({ kind: "profile_approved" })]);
	const afterApproval = await strictResumeMainSession({ profile: changedProfile, state, sdk });
	await afterApproval.session.dispose();
	fs.writeFileSync(profilePath, `${fs.readFileSync(profilePath, "utf8")}\n[poll]\ninterval_ms = 20000\n`);
	const tuningOnly = loadWayProfile(profilePath);
	const afterTuning = await strictResumeMainSession({ profile: tuningOnly, state, sdk });
	await afterTuning.session.dispose();
});

test("host writes growth intent before prompt, refreshes it after append, and journals SDK events synchronously", async () => {
	const { profilePath, state, backend, sdk } = await committedFixture();
	const profile = loadWayProfile(profilePath);
	const resumed = await strictResumeMainSession({ profile, state, sdk });
	const journal: Array<{ kind: string; payload: string }> = [];
	const host = createMainSessionHost({
		session: resumed.session,
		identity: resumed.identity,
		state,
		journal: { journalAppend: (kind, payload) => journal.push({ kind, payload }) },
		now: () => 1234,
	});
	await host.prompt("hello");
	expect(state.read().growthIntent).toBeUndefined();
	expect(state.read().mainIdentity?.size).toBeGreaterThan(resumed.identity.size);
	expect(journal).toHaveLength(1);
	expect(journal[0]?.kind).toBe("main_session_event");
	expect(backend.events).toHaveLength(0);
	await host.dispose();
});

test("journal append failure degrades the host without losing its growth refresh", async () => {
	const { profilePath, state, sdk } = await committedFixture();
	const resumed = await strictResumeMainSession({ profile: loadWayProfile(profilePath), state, sdk });
	let reportedReason: string | undefined;
	const host = createMainSessionHost({
		session: resumed.session,
		identity: resumed.identity,
		state,
		journal: {
			journalAppend: () => {
				throw new Error("journal unavailable");
			},
			setRpcHealth: (_state, reason) => {
				reportedReason = reason;
			},
		},
	});
	await host.prompt("event triggers durable journal failure");
	expect(host.degraded).toBe(true);
	expect(reportedReason).toBe("journal_append_failed");
	expect(state.read().growthIntent).toBeUndefined();
	await host.dispose();
});
