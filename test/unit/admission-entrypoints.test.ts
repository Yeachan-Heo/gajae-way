import { afterAll, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { createMainAdmissionHandler } from "../../src/main-session/admission";
import { loadWayCore } from "../../src/native-loader";
import { loadWayProfile } from "../../src/profile";

const temporaryDirectories: string[] = [];

afterAll(() => {
	for (const directory of temporaryDirectories) fs.rmSync(directory, { recursive: true, force: true });
});

interface RecordedAdmission {
	readonly deliveredAs: string;
	readonly text: string;
	readonly surfaceId: string | undefined;
}

function fixture(turnState: "idle" | "busy") {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-admission-entrypoints-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	const stateDir = path.join(root, "state");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	fs.mkdirSync(stateDir);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = []\n\n[surfaces.owner]\nid = "owner-dm"\nplatform = "test"\nkind = "dm"\nsession_kind = "main"\n`,
	);
	const profile = loadWayProfile(profilePath);
	const core = loadWayCore().WayCore.open(stateDir);
	const admissions: RecordedAdmission[] = [];
	const target = {
		turnState,
		async admit(
			deliveredAs: string,
			text: string,
			opRef: string,
			finalizePendingClaim?: () => void,
			recordAttemptIds?: (attemptIds: readonly string[]) => void,
			surfaceId?: string,
		): Promise<void> {
			admissions.push({ deliveredAs, text, surfaceId });
			// The real host records broker attempt ids before finalizing; the
			// durable attribution row is written by that call, not by finalize.
			recordAttemptIds?.([`attempt-${opRef}`]);
			finalizePendingClaim?.();
		},
	};
	// biome-ignore lint/suspicious/noExplicitAny: the stub intentionally implements only the members admission uses.
	const handler = createMainAdmissionHandler(target as any, profile, core);
	return { handler, admissions, core, profile };
}

/**
 * D1: the scheduler entry point must not be reachable from `main.submit`.
 *
 * The load-bearing property is structural, not a rejected parameter: the
 * function bound to the RPC method is a different function from the one the
 * scheduler holds, so no request shape can cross over.
 */
test("the RPC-bound entry point is a different function from the scheduler entry point", () => {
	const { handler } = fixture("idle");

	expect(typeof handler.submitFromRpc).toBe("function");
	expect(typeof handler.admitSystemEvent).toBe("function");
	expect(handler.submitFromRpc).not.toBe(handler.admitSystemEvent);
	// The scheduler entry point takes typed arguments, not `unknown` RPC params.
	expect(handler.admitSystemEvent.length).toBe(1);
});

/**
 * Two distinct functions only matter if the RPC bridge binds the right one, so
 * assert the actual wiring rather than merely the shape. Previously this file
 * asserted the shape and called it isolation, which is weaker than its name.
 */
test("main.ts binds only submitFromRpc to main.submit and hands the scheduler admitSystemEvent", () => {
	const source = fs.readFileSync(path.join(import.meta.dir, "..", "..", "src", "main.ts"), "utf8");

	// The bridge forwards main.submit to the RPC entry point only.
	expect(source).toMatch(/if \(method === "main\.submit"\) return await admissionHandler\(params\);/u);
	expect(source).toContain("const { submitFromRpc: admissionHandler, admitSystemEvent }");
	// The scheduler receives the in-process entry point.
	expect(source).toMatch(/createScheduler\(\{[\s\S]*?admitSystemEvent,/u);
	// No RPC method may be bound to the scheduler entry point.
	expect(source).not.toMatch(/method === "[^"]+"\) return await admitSystemEvent/u);
});

test("submitFromRpc rejects an unknown parameter rather than honouring a delivery request", async () => {
	const { handler, admissions } = fixture("idle");

	await expect(
		handler.submitFromRpc({
			text: "attempt to request a delivery mode over RPC",
			surface_id: "owner-dm",
			idempotency_key: "rpc-delivery",
			delivery: "system_event",
		}),
	).rejects.toMatchObject({ code: -32602 });
	expect(admissions).toEqual([]);
});

test("submitFromRpc cannot enqueue a follow_up without a known surface", async () => {
	const { handler, admissions } = fixture("idle");

	await expect(
		handler.submitFromRpc({ text: "unknown surface", surface_id: "not-declared", idempotency_key: "rpc-unknown" }),
	).rejects.toMatchObject({ code: 1300 });
	expect(admissions).toEqual([]);
});

/**
 * The scheduler path derives `follow_up` without consulting `turnState` and
 * without any surface entry, so a scheduled system event can never steer a live
 * operator turn and never borrows admission authority from configured routing.
 */
test("admitSystemEvent enqueues follow_up with no surface entry, even while the operator turn is busy", async () => {
	const { handler, admissions, profile } = fixture("busy");

	// An owner-surface submit during a busy turn steers; the scheduler must not.
	expect(profile.ownerSurfaces.map((surface) => surface.id)).toEqual(["owner-dm"]);

	const response = await handler.admitSystemEvent({
		text: "scheduled system event",
		idempotencyKey: "sched:job-1:run-1",
	});

	expect(response.accepted).toBe(true);
	expect(response.delivered_as).toBe("follow_up");
	expect(admissions).toEqual([{ deliveredAs: "follow_up", text: "scheduled system event", surfaceId: undefined }]);
});

test("a scheduler admission is durably attributed by origin rather than a surface", async () => {
	const { handler, core } = fixture("idle");

	await handler.admitSystemEvent({ text: "attributed by origin", idempotencyKey: "sched:job-2:run-1" });

	const attributions = core.mainAdmissionAttributions();
	expect(attributions).toHaveLength(1);
	expect(attributions[0]?.origin).toBe("scheduler");
	expect(attributions[0]?.surfaceId ?? null).toBeNull();
});

test("an owner-surface submit is still attributed by surface", async () => {
	const { handler, core } = fixture("idle");

	await handler.submitFromRpc({ text: "operator prompt", surface_id: "owner-dm", idempotency_key: "rpc-owner" });

	const attributions = core.mainAdmissionAttributions();
	expect(attributions).toHaveLength(1);
	expect(attributions[0]?.surfaceId).toBe("owner-dm");
	expect(attributions[0]?.origin ?? null).toBeNull();
});
