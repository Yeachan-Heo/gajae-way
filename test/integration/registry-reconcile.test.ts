import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { BrokerCli, BrokerDtoParseError, type SdkSessionRowV1 } from "../../src/broker/cli";
import { defaultConfig } from "../../src/config";
import { BrokerReconciler, type RegistryRow } from "../../src/broker/reconcile";
import { createMainAdmissionHandler } from "../../src/main-session/admission";
import { loadWayProfile } from "../../src/profile";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { createRpcBridge, RpcBridgeException } from "../../src/rpc-bridge";
import { RpcClient } from "../helpers/rpc-client";

const temporaryDirectories: string[] = [];
const fixtureScript = path.join(import.meta.dir, "..", "fixtures", "fake-broker-cli.mjs");

afterEach(() => {
	for (const directory of temporaryDirectories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

interface FixtureState {
	list: unknown;
	metadata: Record<string, unknown>;
}

function temporaryDirectory(name: string): string {
	const directory = fs.mkdtempSync(path.join(os.tmpdir(), `gajae-way-p6-${name}-`));
	temporaryDirectories.push(directory);
	return directory;
}

function shellQuote(value: string): string {
	return `'${value.replaceAll("'", "'\\''")}'`;
}

function fixtureBroker(root: string, statePath: string): string {
	const wrapper = path.join(root, "fixture-gjc");
	fs.writeFileSync(
		wrapper,
		`#!/bin/sh\nexport WAY_BROKER_FIXTURE_STATE=${shellQuote(statePath)}\nexec ${shellQuote(process.execPath)} ${shellQuote(fixtureScript)} "$@"\n`,
	);
	fs.chmodSync(wrapper, 0o755);
	return wrapper;
}

function writeState(statePath: string, state: FixtureState): void {
	fs.writeFileSync(statePath, `${JSON.stringify(state)}\n`);
}

function readState(statePath: string): FixtureState {
	return JSON.parse(fs.readFileSync(statePath, "utf8")) as FixtureState;
}

function queryIds(statePath: string): string[] {
	try {
		return fs
			.readFileSync(`${statePath}.queries`, "utf8")
			.split("\n")
			.filter(Boolean);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
		throw error;
	}
}

function clearQueryLog(statePath: string): void {
	fs.rmSync(`${statePath}.queries`, { force: true });
}

function row(sessionId: string, overrides: Partial<SdkSessionRowV1> = {}): SdkSessionRowV1 {
	return {
		sessionId,
		locator: { repo: "/fixture/repo", stateRoot: "/fixture/repo/.gjc/state" },
		endpointGeneration: 1,
		pid: 4242,
		live: true,
		deleted: false,
		indexSeq: 1,
		identityProvenance: "composite",
		activity: { state: "active", at: 100 },
		lastHeartbeatAt: 100,
		...overrides,
	};
}

function list(sessions: readonly SdkSessionRowV1[], indexSeq = 1): unknown {
	return {
		ok: true,
		result: {
			version: 1,
			source: "broker",
			indexSeq,
			sessions,
			warnings: [],
		},
	};
}

function metadata(sessionId: string, name = sessionId): object {
	return { sessionId, name, cwd: "/fixture/repo", kind: "main" };
}

function createFixture(name: string, state: FixtureState): { root: string; statePath: string; core: WayCoreHandle; broker: BrokerCli } {
	const root = temporaryDirectory(name);
	const statePath = path.join(root, "broker-state.json");
	writeState(statePath, state);
	const executable = fixtureBroker(root, statePath);
	return {
		root,
		statePath,
		core: loadWayCore().WayCore.open(path.join(root, "state")),
		broker: new BrokerCli({ executable }),
	};
}

async function eventually<T>(read: () => T | undefined | Promise<T | undefined>, description: string, timeoutMs = 2_000): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		const value = await read();
		if (value !== undefined) return value;
		await Bun.sleep(10);
	}
	throw new Error(`Timed out waiting for ${description}.`);
}

async function connectEventually(socketPath: string): Promise<RpcClient> {
	return await eventually(async () => {
		if (!fs.existsSync(socketPath)) return undefined;
		try {
			return await RpcClient.connect(socketPath);
		} catch {
			return undefined;
		}
	}, `RPC listener ${socketPath}`);
}

function rows(core: WayCoreHandle): RegistryRow[] {
	return core.registryList({ limit: 500 }).rows;
}

function metadataName(core: WayCoreHandle, sessionId: string): string | undefined {
	try {
		return core.registryGet(sessionId).metaName;
	} catch {
		return undefined;
	}
}

test("reconciliation reflects rename within the shortened poll SLA and preserves list-only authority changes", async () => {
	const fixture = createFixture("rename-authority", {
		list: list([row("rename")]),
		metadata: { rename: metadata("rename", "before rename") },
	});
	const configured = defaultConfig({ WAY_BROKER_CLI: fixture.broker.executable, WAY_RECONCILE_POLL_MS: "25" });
	expect(configured).toMatchObject({ brokerCliPath: fixture.broker.executable, reconcilePollMs: 25 });
	const reconciler = new BrokerReconciler({ core: fixture.core, broker: fixture.broker, pollMs: 25, cycleSlaMs: 2_000 });
	try {
		reconciler.start();
		await eventually(() => (metadataName(fixture.core, "rename") === "before rename" ? true : undefined), "initial metadata");
		const state = readState(fixture.statePath);
		state.metadata.rename = metadata("rename", "after rename");
		writeState(fixture.statePath, state);
		const changedAt = Date.now();
		await eventually(() => (metadataName(fixture.core, "rename") === "after rename" ? true : undefined), "metadata rename");
		expect(Date.now() - changedAt).toBeLessThan(2_000);
		const discovered = fixture.core.registryGet("rename");
		expect(discovered).toMatchObject({ kind: "unknown", source: "reconciler" });
	} finally {
		reconciler.stop();
	}

	const authority = createFixture("authority-only", {
		list: list([row("authority")]),
		metadata: { authority: metadata("authority") },
	});
	const authorityReconciler = new BrokerReconciler({ core: authority.core, broker: authority.broker, cycleSlaMs: 2_000 });
	await authorityReconciler.trigger();
	expect(authority.core.registryGet("authority")).toMatchObject({ kind: "unknown", status: "discovered", source: "reconciler" });
	const changed = readState(authority.statePath);
	clearQueryLog(authority.statePath);
	changed.list = list([
		row("authority", {
			live: false,
			deleted: true,
			indexSeq: 2,
			activity: { state: "idle", at: 200 },
			lastHeartbeatAt: 200,
		}),
	]);
	writeState(authority.statePath, changed);
	await authorityReconciler.trigger();
	const authorityRow = authority.core.registryGet("authority");
	expect(authorityRow).toMatchObject({ live: false, deleted: true, indexSeq: 2, activityState: "idle", activityAt: 200 });
	expect(queryIds(authority.statePath)).toHaveLength(0);
	const changes = authority.core
		.journalRead("1:0", 100)
		.events.filter(event => event.kind === "registry_change")
		.map(event => JSON.parse(event.payloadJson) as { session_id: string });
	expect(changes).toContainEqual(expect.objectContaining({ session_id: "authority" }));
});

test("gateway authority survives reconciliation and all routing quarantine predicates fail closed", async () => {
	const fixture = createFixture("gateway-quarantine", {
		list: list([row("gateway"), row("ambiguous"), row("uncertain"), row("deleted")]),
		metadata: {
			gateway: metadata("gateway"),
			ambiguous: metadata("ambiguous"),
			uncertain: metadata("uncertain"),
			deleted: metadata("deleted"),
		},
	});
	fixture.core.registryConfigureSurfaces([
		{ surfaceId: "surface-a", platform: "test", kind: "dm", isOwnerSurface: true },
		{ surfaceId: "surface-b", platform: "test", kind: "channel", isOwnerSurface: false },
		{ surfaceId: "surface-c", platform: "test", kind: "channel", isOwnerSurface: false },
	]);
	fixture.core.registryRegisterGatewaySession({
		sessionId: "gateway",
		kind: "conversation",
		purpose: "operator-owned purpose",
		status: "starting",
	});
	const reconciler = new BrokerReconciler({ core: fixture.core, broker: fixture.broker, cycleSlaMs: 2_000 });
	await reconciler.trigger();
	expect(fixture.core.registryGet("gateway")).toMatchObject({
		source: "gateway",
		kind: "conversation",
		purpose: "operator-owned purpose",
	});
	fixture.core.registryBindSurface("surface-a", "ambiguous");
	fixture.core.registryBindSurface("surface-b", "uncertain");
	fixture.core.registryBindSurface("surface-c", "deleted");
	const state = readState(fixture.statePath);
	state.list = list([
		row("gateway"),
		row("ambiguous", { ambiguous: true, indexSeq: 2 }),
		row("uncertain", { terminalUncertain: true, indexSeq: 2 }),
		row("deleted", { deleted: true, indexSeq: 2 }),
	]);
	writeState(fixture.statePath, state);
	await reconciler.trigger();
	for (const [sessionId, surfaceId] of [
		["ambiguous", "surface-a"],
		["uncertain", "surface-b"],
		["deleted", "surface-c"],
	] as const) {
		expect(fixture.core.registryGet(sessionId).quarantined).toBe(true);
		expect(fixture.core.surfaceResolve(surfaceId)).toMatchObject({ sessionId, quarantined: true });
	}

	const corpus = path.join(fixture.root, "corpus");
	const workspace = path.join(fixture.root, "workspace");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const profilePath = path.join(fixture.root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = []\n\n[surfaces.owner]\nid = "surface-a"\nplatform = "test"\nkind = "dm"\n`,
	);
	const profile = loadWayProfile(profilePath);
	const submit = createMainAdmissionHandler(
		{
			turnState: "idle",
			async prompt() {},
			async steer() {},
			async followUp() {},
		},
		profile,
		fixture.core,
		{ isSurfaceQuarantined: surface => fixture.core.surfaceResolve(surface.id).quarantined },
	);
	await expect(submit({ text: "must not route", surface_id: "surface-a", idempotency_key: "quarantine-submit" })).rejects.toMatchObject({
		code: 1302,
		message: "session_quarantined",
	});
});

test("DTO drift leaves the registry unchanged and reconcile status stale", async () => {
	const fixture = createFixture("drift", {
		list: list([row("stable")]),
		metadata: { stable: metadata("stable") },
	});
	const socketPath = path.join(fixture.root, "state", "rpc.sock");
	fixture.core.startRpcServer(
		socketPath,
		createRpcBridge(fixture.core, () => {
			throw new RpcBridgeException(-32601, "method not found");
		}),
	);
	const client = await connectEventually(socketPath);
	const reconciler = new BrokerReconciler({ core: fixture.core, broker: fixture.broker, cycleSlaMs: 2_000 });
	try {
		await reconciler.trigger();
		const statusBefore = await client.request("way.status");
		const lastOkAt = (statusBefore.result as { reconcile: { last_ok_at: number } }).reconcile.last_ok_at;
		const before = rows(fixture.core);
		writeState(fixture.statePath, {
			list: { ok: true, result: { version: 1, source: "broker", sessions: [], warnings: [], unexpected: true } },
			metadata: {},
		});
		await expect(reconciler.trigger()).rejects.toBeInstanceOf(BrokerDtoParseError);
		expect(rows(fixture.core)).toEqual(before);
		const statusAfter = await client.request("way.status");
		expect((statusAfter.result as { reconcile: { last_ok_at: number } }).reconcile.last_ok_at).toBe(lastOkAt);
	} finally {
		client.close();
		fixture.core.shutdownRpcServer();
	}
});


test("registry RPC methods expose durable rows, annotations, and surface resolution", async () => {
	const fixture = createFixture("registry-rpc", { list: list([]), metadata: {} });
	fixture.core.registryConfigureSurfaces([{ surfaceId: "rpc-surface", platform: "test", kind: "dm", isOwnerSurface: true }]);
	fixture.core.registryApplyBrokerSnapshot({
		observedAt: 1,
		rows: [
			{
				sessionId: "rpc-session",
				locator: JSON.stringify({ repo: "/fixture/repo", stateRoot: "/fixture/repo/.gjc/state" }),
				endpointGeneration: 1,
				identityProvenance: "composite",
				indexSeq: 1,
				live: true,
				deleted: false,
				terminalUncertain: false,
				ambiguous: false,
				activityState: "active",
				activityAt: 1,
				lastHeartbeatAt: 1,
			},
		],
	});
	fixture.core.registryBindSurface("rpc-surface", "rpc-session", 2);
	const socketPath = path.join(fixture.root, "state", "rpc.sock");
	fixture.core.startRpcServer(
		socketPath,
		createRpcBridge(fixture.core, () => {
			throw new RpcBridgeException(-32601, "method not found");
		}),
	);
	const client = await connectEventually(socketPath);
	try {
		const listed = await client.request("registry.list", {});
		expect(listed.result).toMatchObject({ total: 1, rows: [expect.objectContaining({ session_id: "rpc-session", quarantined: false })] });
		const missing = await client.request("registry.get", { session_id: "missing" });
		expect(missing.error).toMatchObject({ code: 1301, message: "unknown_session" });
		const annotation = await client.request("registry.annotate", {
			session_id: "rpc-session",
			purpose: "operator annotation",
			brief: "brief",
			idempotency_key: "registry-annotation-1",
		});
		expect(annotation.result).toMatchObject({ row: { purpose: "operator annotation", brief: "brief" } });
		const replay = await client.request("registry.annotate", {
			session_id: "rpc-session",
			purpose: "operator annotation",
			brief: "brief",
			idempotency_key: "registry-annotation-1",
		});
		expect(replay.result).toEqual(annotation.result);
		const resolved = await client.request("surface.resolve", { surface_id: "rpc-surface" });
		expect(resolved.result).toMatchObject({ session_id: "rpc-session", quarantined: false, policy: { owner: true } });
	} finally {
		client.close();
		fixture.core.shutdownRpcServer();
	}
});
test("metadata enrichment is bounded to ten commands and unavailable rows honor exponential backoff", async () => {
	let now = 0;
	const sessions = Array.from({ length: 12 }, (_, index) => row(`bulk-${String(index).padStart(2, "0")}`));
	const fixture = createFixture("bounds", {
		list: list(sessions),
		metadata: Object.fromEntries(
			sessions.map(session => [session.sessionId, session.sessionId === "bulk-00" ? { unavailable: true } : metadata(session.sessionId)]),
		),
	});
	const reconciler = new BrokerReconciler({ core: fixture.core, broker: fixture.broker, cycleSlaMs: 2_000, now: () => now });
	await reconciler.trigger();
	expect(queryIds(fixture.statePath)).toHaveLength(10);
	expect(rows(fixture.core).filter(row => row.metadataState === "enriched")).toHaveLength(9);
	expect(fixture.core.registryGet("bulk-00").metadataState).toBe("unavailable");
	expect(rows(fixture.core).filter(row => row.metadataState === "pending")).toHaveLength(2);
	expect(queryIds(fixture.statePath).filter(id => id === "bulk-00")).toHaveLength(1);

	now = 29_999;
	await reconciler.trigger();
	expect(queryIds(fixture.statePath).filter(id => id === "bulk-00")).toHaveLength(1);

	now = 30_000;
	await reconciler.trigger();
	expect(queryIds(fixture.statePath).filter(id => id === "bulk-00")).toHaveLength(2);
});
