import { afterAll, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { GatewayStateStore } from "../../src/main-session/state";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { loadWayProfile } from "../../src/profile";

const temporaryDirectories: string[] = [];

afterAll(() => {
	for (const directory of temporaryDirectories) fs.rmSync(directory, { recursive: true, force: true });
});

function fixture(): { core: WayCoreHandle; state: GatewayStateStore } {
	const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-alerts-"));
	temporaryDirectories.push(stateDir);
	const core = loadWayCore().WayCore.open(stateDir);
	core.gatewayMetaTransaction({
		expected: [],
		puts: [{ key: "bootstrap_state", value: "COMMITTED" }],
		deletes: [],
	});
	return { core, state: new GatewayStateStore(core) };
}

function writeProfile(): string {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-alerts-profile-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = []\n\n[surfaces.owner]\nid = "owner-dm"\nplatform = "test"\nkind = "dm"\nsession_kind = "main"\n`,
	);
	return profilePath;
}

function alertEvents(core: WayCoreHandle): Array<{ kind: string; payload: unknown }> {
	const read = core.journalRead("1:0", 100);
	return read.events
		.filter((event) => event.kind === "alert_raised" || event.kind === "alert_cleared")
		.map((event) => ({ kind: event.kind, payload: JSON.parse(event.payloadJson) as unknown }));
}

/**
 * The alert must share a commit with the state it reports. Two transactions
 * would leave a window in which the gateway is failed closed and silent, which
 * is exactly the condition an operator needs told.
 */
test("entering failed-closed raises its alert in the same transaction", () => {
	const { core, state } = fixture();

	state.markFailedClosed("profile_drift", 1_000);

	expect(state.read().bootstrapState).toBe("FAILED_CLOSED");
	const events = alertEvents(core);
	expect(events).toHaveLength(1);
	expect(events[0]?.kind).toBe("alert_raised");
	expect(events[0]?.payload).toEqual({ condition: "failed_closed", reason: "profile_drift" });
});

test("a restart while still failed closed does not re-announce the condition", () => {
	const { core, state } = fixture();

	state.markFailedClosed("profile_drift", 1_000);
	// A second mark (or a restart catch-up) must not emit a duplicate.
	state.markFailedClosed("profile_drift", 2_000);
	expect(state.catchUpFailedClosedAlert(3_000)).toBe(false);

	expect(alertEvents(core)).toHaveLength(1);
});

/**
 * The seven host-fatal `markFailedClosed` call sites are deliberately not
 * rewritten, so the catch-up is load-bearing: without it a crash on one of those
 * paths leaves a failed-closed gateway that never announced itself.
 */
test("a failed-closed state recorded without an alert is caught up exactly once", () => {
	const { core, state } = fixture();

	// Simulate a host-fatal path that marked state without folding an alert.
	core.gatewayMetaTransaction({
		expected: [],
		puts: [
			{ key: "bootstrap_state", value: "FAILED_CLOSED" },
			{ key: "failed_closed_reason", value: JSON.stringify("growth_protocol_invalid") },
		],
		deletes: [],
	});
	expect(alertEvents(core)).toEqual([]);

	expect(state.catchUpFailedClosedAlert(5_000)).toBe(true);
	const events = alertEvents(core);
	expect(events).toHaveLength(1);
	expect(events[0]?.payload).toEqual({ condition: "failed_closed", reason: "growth_protocol_invalid" });

	// Idempotent: a second catch-up adds nothing.
	expect(state.catchUpFailedClosedAlert(6_000)).toBe(false);
	expect(alertEvents(core)).toHaveLength(1);
});

test("clearing emits exactly one alert_cleared and only when raised", () => {
	const { core, state } = fixture();

	expect(state.clearFailedClosedAlert()).toBe(false);
	state.markFailedClosed("profile_drift", 1_000);
	expect(state.clearFailedClosedAlert()).toBe(true);
	expect(state.clearFailedClosedAlert()).toBe(false);

	const events = alertEvents(core);
	expect(events.map((event) => event.kind)).toEqual(["alert_raised", "alert_cleared"]);
});

/**
 * These events reach chat surfaces, so a free-text reason must never be
 * embedded verbatim.
 */
test("an unsafe reason is replaced rather than embedded in the alert payload", () => {
	const { core, state } = fixture();

	state.markFailedClosed("Something went wrong: /srv/secret/path", 1_000);

	const events = alertEvents(core);
	expect(events[0]?.payload).toEqual({ condition: "failed_closed", reason: "unspecified" });
	expect(JSON.stringify(events[0]?.payload)).not.toContain("/srv/secret");
});

/**
 * Recovery must clear the alert in the same commit that ends the condition.
 *
 * Profile approval is the only production path that resolves a `profile_drift`
 * fail-closed. Without the clear, `way.status.alerts` and the metrics exposition
 * keep reporting a condition that no longer exists - a stale alert trains an
 * operator to ignore alerts.
 */
test("profile approval clears the fail-closed alert it resolves", () => {
	const { core, state } = fixture();

	state.markFailedClosed("profile_drift", 1_000);
	expect(alertEvents(core).map((event) => event.kind)).toEqual(["alert_raised"]);

	const profile = loadWayProfile(writeProfile());
	state.approveProfile(profile, "receipt-1", 2_000);

	// The condition ended, so the alert must no longer be raised.
	const read = core.gatewayMetaRead(["alert_failed_closed", "failed_closed_since_ms"]).entries;
	expect(read.find((entry) => entry.key === "alert_failed_closed")?.value).toBe("clear");
	expect(read.find((entry) => entry.key === "failed_closed_since_ms")?.value).toBe("null");
});
