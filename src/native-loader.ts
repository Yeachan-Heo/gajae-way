import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { createRequire } from "node:module";
import type {
	GatewayMetaReadOutput,
	GatewayMetaTransactionInput,
	GatewayMetaTransactionOutput,
} from "./main-session/state";
import { embeddedAddon } from "../native/embedded-addon";

export interface HealthInfo {
	version: string;
	bootEpoch: number;
}

export interface RpcBridgeRequest {
	reqId: number;
	method: string;
	paramsJson: string;
}

export interface RpcBridgeStats {
	inFlight: number;
	overloads: number;
	timeouts: number;
	duplicateCompletions: number;
	lateCompletions: number;
	queueClosed: number;
}

export type RpcBridgeCallback = (error: Error | null, request: RpcBridgeRequest) => void;

export interface WayCoreHandle {
	readonly stateDir: string;
	startRpcServer(socketPath: string, bridgeCallback: RpcBridgeCallback): void;
	bridgeComplete(reqId: number, resultJson: string): boolean;
	shutdownRpcServer(): void;
	rpcBridgeStats(): RpcBridgeStats;
	rpcDroppedNotificationCount(): number;
	setRpcHealth(state: "booting" | "verifying" | "running" | "failed_closed" | "degraded", reason?: string): void;
	setMainSessionStatus(turnState: "idle" | "busy", followUpQueueDepth: number): void;
	resetMainSessionStatus(): void;
	setJournalDegraded(degraded: boolean): void;
	sdNotifyStatus(status: string): void;
	sdNotifyReady(status: string): void;
	lockAcquire(input: {
		label: string;
		class?: "interactive" | "batch";
		waitMs?: number;
		ttlMs?: number;
		holder: {
			holderKind: "in_daemon";
			sessionId: string;
			pid: number;
			pidStartTime: string;
			pgid: number;
			pgidStartTime?: string;
			connId?: "way.in_daemon_executor.v1";
		};
	}): { leaseId: string; fencingToken: string; expiresAt: number; queueWaitedMs: number };
	lockRenew(leaseId: string): { expiresAt: number };
	lockRelease(leaseId: string): { released: boolean; heldMs: number };
	lockFencingValid(leaseId: string, fencingToken: string): boolean;
	lockDrainRevocations(): string[];
	processIdentity(pid: number): { pid: number; pidStartTime: string; pgid: number; pgidStartTime?: string };
	lockStatus(): {
		held: boolean;
		holder?: { leaseId: string; sessionId: string; pid: number; pgid: number; fencingToken: string; state: string };
		expiresAt?: number;
		fencingToken?: string;
		queue: Array<{ class: string; label: string; waitedMs: number }>;
		stuck: boolean;
		quarantined: boolean;
	};
	lockForceRelease(leaseId: string, confirm: boolean): { released: boolean; heldMs: number };
	lockQuarantineOverride(
		leaseId: string,
		confirm: boolean,
		acknowledgeUnverified: boolean,
	): {
		held: boolean;
		quarantined: boolean;
	};
	lockRecordQuarantineReceipt(input: {
		leaseId: string;
		corpus: string;
		processInspected: boolean;
		gitStatusChecked: boolean;
		gitLogChecked: boolean;
		gitFsckChecked: boolean;
		remoteVerified: boolean;
	}): { receiptId: string; leaseId: string; corpus: string };
	lockClearQuarantine(verificationReceiptId: string, confirm: boolean): { held: boolean; quarantined: boolean };
	gatewayMetaRead(keys: readonly string[]): GatewayMetaReadOutput;
	gatewayMetaTransaction(input: GatewayMetaTransactionInput): GatewayMetaTransactionOutput;
	registryApplyBrokerSnapshot(input: {
		observedAt: number;
		rows: Array<{
			sessionId: string;
			locator: string;
			endpointGeneration: number;
			hostIncarnation?: string;
			identityProvenance?: "composite" | "legacy";
			indexSeq: number;
			live: boolean;
			deleted: boolean;
			terminalUncertain: boolean;
			ambiguous: boolean;
			activityState?: "active" | "idle";
			activityAt?: number;
			lastHeartbeatAt?: number;
		}>;
	}): { newSessionIds: string[]; changedSessionIds: string[]; changedIndexSeqSessionIds: string[]; driftCount: number };
	registryList(input?: { kind?: string; status?: string; surfaceId?: string; limit?: number; offset?: number }): {
		rows: Array<{
			sessionId: string;
			kind: string;
			purpose?: string;
			brief?: string;
			status: string;
			surfaceId?: string;
			locator?: string;
			endpointGeneration?: number;
			hostIncarnation?: string;
			identityProvenance?: string;
			indexSeq?: number;
			live: boolean;
			deleted: boolean;
			terminalUncertain: boolean;
			ambiguous: boolean;
			activityState?: "active" | "idle";
			activityAt?: number;
			lastHeartbeatAt?: number;
			metaName?: string;
			metaCwd?: string;
			metaKind?: string;
			metadataState: "pending" | "enriched" | "unavailable";
			metadataAt?: number;
			source: "gateway" | "reconciler";
			createdAt: number;
			lastSeenAt?: number;
			closedAt?: number;
			registryRev: number;
			quarantined: boolean;
		}>;
		total: number;
	};
	registryGet(sessionId: string): ReturnType<WayCoreHandle["registryList"]>["rows"][number];
	registryAnnotate(input: {
		sessionId: string;
		purpose?: string;
		brief?: string;
		observedAt?: number;
	}): ReturnType<WayCoreHandle["registryList"]>["rows"][number];
	registryApplyMetadata(input: {
		sessionId: string;
		name: string;
		cwd: string;
		kind: string;
		observedAt: number;
	}): ReturnType<WayCoreHandle["registryList"]>["rows"][number];
	registryMarkMetadataUnavailable(input: {
		sessionId: string;
		observedAt: number;
	}): ReturnType<WayCoreHandle["registryList"]>["rows"][number];
	registryConfigureSurfaces(
		surfaces: Array<{ surfaceId: string; platform: string; kind: string; isOwnerSurface: boolean }>,
		observedAt?: number,
	): void;
	registryBindSurface(surfaceId: string, sessionId: string, observedAt?: number): void;
	registryRegisterGatewaySession(input: {
		sessionId: string;
		kind: "main" | "conversation" | "lane" | "job";
		purpose?: string;
		brief?: string;
		status: "discovered" | "starting" | "active" | "idle" | "closing" | "closed" | "lost";
		surfaceId?: string;
		observedAt?: number;
	}): ReturnType<WayCoreHandle["registryList"]>["rows"][number];
	surfaceResolve(surfaceId: string): {
		surface: { surfaceId: string; platform: string; kind: string; isOwnerSurface: boolean };
		sessionId?: string;
		quarantined: boolean;
	};
	setReconcileStatus(input: { lastOkAt: number; cycleMs: number; driftCount: number }): void;
	journalAppend(kind: string, payloadJson: string): { cursor: string; seq: string };
	journalRead(
		cursor?: string,
		limit?: number,
	): {
		events: Array<{ seq: string; ts: number; kind: string; payloadJson: string }>;
		nextCursor: string;
		gap?: { missingFrom: string; missingTo: string; resyncCursor: string };
	};
	consumerClaim(consumerId: string, claimTtlMs?: number): { claimId: string; cursor: string; expiresAt: number };
	consumerCommit(input: {
		consumerId: string;
		claimId: string;
		cursor: string;
		proofs: Array<{ seq: string; platformMsgId?: string; dedupeKey?: string }>;
	}): { committedCursor: string };
	consumerCursor(consumerId: string): string | undefined;
	consumerOutbox(consumerId: string): Array<{
		consumerId: string;
		seq: string;
		state: string;
		platformMsgId?: string;
		dedupeKey?: string;
	}>;
	idempotencyReplay(input: { scope: string; key: string; requestJson: string }): {
		replayed: boolean;
		responseJson?: string;
	};
	idempotencyStore(input: { scope: string; key: string; requestJson: string; responseJson: string }): void;
	closureOperationClaim(input: {
		scope: string;
		key: string;
		requestJson: string;
		intentJson: string;
		operationJson: string;
	}): { claimed: boolean; responseJson?: string };
	closureOperationFinalize(input: {
		scope: string;
		key: string;
		requestJson: string;
		intentJson: string;
		operationJson: string;
		responseJson: string;
	}): { responseJson: string };
}

export interface WayCoreConstructor {
	open(stateDir: string): WayCoreHandle;
	openWithTestHardCap(stateDir: string, hardHoldCapMs: number): WayCoreHandle;
}

export interface WayCoreBindings {
	healthInfo(): HealthInfo;
	WayCore: WayCoreConstructor;
}

const requiredExports = ["healthInfo", "WayCore"] as const;

function platformTag(): string {
	return `${process.platform}-${process.arch}`;
}

function localAddonPath(): string {
	return path.join(import.meta.dir, "..", "native", `way_core.${platformTag()}.node`);
}

function materializeEmbeddedAddon(): string {
	if (!embeddedAddon) return localAddonPath();
	if (embeddedAddon.platformTag !== platformTag()) {
		throw new Error(`Embedded ${embeddedAddon.filename} cannot run on ${platformTag()}.`);
	}

	// The cache key MUST be the addon's content hash. Version+platform+size is
	// not enough: rebuilds of the same crate version routinely produce same-size
	// addons, and a stale extraction then silently serves OLD native code to
	// every new binary (observed in production: a compiled daemon reporting
	// status healthy for a degraded gateway that source had already fixed).
	const cacheDir = path.join(os.tmpdir(), "gajaeway", "native", embeddedAddon.contentHash, embeddedAddon.platformTag);
	const targetPath = path.join(cacheDir, embeddedAddon.filename);
	const sourceBytes = fs.readFileSync(embeddedAddon.filePath);
	const hash = (bytes: Uint8Array): string => {
		const hasher = new Bun.CryptoHasher("sha256");
		hasher.update(bytes);
		return hasher.digest("hex");
	};
	if (hash(sourceBytes) !== embeddedAddon.contentHash) {
		throw new Error(`Embedded addon ${embeddedAddon.filename} does not match its recorded content hash.`);
	}
	try {
		if (hash(fs.readFileSync(targetPath)) === embeddedAddon.contentHash) return targetPath;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}

	fs.mkdirSync(cacheDir, { recursive: true });
	const temporaryPath = `${targetPath}.tmp.${process.pid}`;
	fs.writeFileSync(temporaryPath, sourceBytes);
	try {
		fs.renameSync(temporaryPath, targetPath);
	} catch (renameError) {
		try {
			fs.rmSync(targetPath, { force: true });
			fs.renameSync(temporaryPath, targetPath);
		} catch (fallbackError) {
			fs.rmSync(temporaryPath, { force: true });
			throw new Error(
				`Could not materialize embedded addon ${embeddedAddon.filename}: ${
					fallbackError instanceof Error ? fallbackError.message : String(renameError)
				}`,
			);
		}
	}
	return targetPath;
}

function assertWayCoreBindings(
	bindings: Record<string, unknown>,
	addonPath: string,
): asserts bindings is WayCoreBindings & Record<string, unknown> {
	const missing = requiredExports.filter((symbol) => typeof bindings[symbol] !== "function");
	if (missing.length > 0) {
		throw new Error(`Native addon ${addonPath} is missing required exports: ${missing.join(", ")}.`);
	}
}

/** Loads either the development addon or the addon embedded into a Bun standalone executable. */
export function loadWayCore(): WayCoreBindings {
	const addonPath = materializeEmbeddedAddon();
	const bindings = createRequire(import.meta.url)(addonPath) as Record<string, unknown>;
	assertWayCoreBindings(bindings, addonPath);
	return bindings;
}
