//! N-API surface for the gajae-way durable runtime.
//!
//! Rust owns SQLite state, fenced lock ownership, journal settlement,
//! idempotency, and the authenticated P2 UDS RPC boundary.

use std::{
	collections::HashSet,
	path::PathBuf,
	str::FromStr,
	sync::{Mutex, OnceLock},
	time::SystemTime,
};

use napi::threadsafe_function::ThreadsafeFunction;
use napi_derive::napi;

use crate::rpc::{
	RpcServerHandle,
	dispatch::{BridgeRequest, GatewayState, RpcBridgeStats, RpcDispatcher, TsfnBridge},
};
use crate::{
	events::{ConsumerClaim, Cursor, DeliveryProof, EventJournal, JournalError, JournalGap, JournalRead, OutboxRow, append_in_transaction},
	lock::{
		AcquireRequest, AcquireResult, HARD_HOLD_CAP_MS, HolderKind, IN_DAEMON_EXECUTOR_CONN_ID, Lease, LeaseHolder, LockClass, LockError, LockManager,
		LockStatus, ProcessObservation, ProcessProbe, QuarantineReceiptEvidence, QueueEntry, ReleaseResult, SystemProcessProbe,
	},
	registry::{BrokerSessionRow, BrokerSnapshot, GatewaySession, MetadataEnrichment, RegistryAnnotation, RegistryListFilter, SurfaceRecord},
	store::{ClosureOperationClaim, MainAdmissionOperationClaim, PendingMainAdmissionOperation, Store, StoreError, meta_get_tx, meta_set_tx, unix_epoch_ms},
};

pub mod events;
pub mod lock;
pub mod registry;
pub mod rpc;
pub mod store;
pub mod systemd;

static BOOT_EPOCH: OnceLock<u32> = OnceLock::new();

#[napi(object)]
pub struct HealthInfo {
	pub version: String,
	#[napi(js_name = "bootEpoch")]
	pub boot_epoch: u32,
}

/// Returns the process-local boot identity used by health probes.
#[napi(js_name = "healthInfo")]
pub fn health_info() -> HealthInfo {
	let boot_epoch = *BOOT_EPOCH.get_or_init(|| {
		SystemTime::now()
			.duration_since(SystemTime::UNIX_EPOCH)
			.expect("system time must not precede the Unix epoch")
			.as_secs()
			.try_into()
			.expect("boot epoch fits in u32")
	});

	HealthInfo { version: env!("CARGO_PKG_VERSION").to_string(), boot_epoch }
}

#[napi(object)]
pub struct LockHolderInput {
	#[napi(js_name = "holderKind")]
	pub holder_kind: String,
	#[napi(js_name = "sessionId")]
	pub session_id: String,
	pub pid: i32,
	#[napi(js_name = "pidStartTime")]
	pub pid_start_time: String,
	pub pgid: i32,
	#[napi(js_name = "pgidStartTime")]
	pub pgid_start_time: Option<String>,
	#[napi(js_name = "connId")]
	pub conn_id: Option<String>,
}

#[napi(object)]
pub struct LockAcquireInput {
	pub label: String,
	#[napi(js_name = "class")]
	pub class: Option<String>,
	#[napi(js_name = "waitMs")]
	pub wait_ms: Option<u32>,
	#[napi(js_name = "ttlMs")]
	pub ttl_ms: Option<u32>,
	pub holder: LockHolderInput,
}

#[napi(object)]
pub struct LockAcquireOutput {
	#[napi(js_name = "leaseId")]
	pub lease_id: String,
	#[napi(js_name = "fencingToken")]
	pub fencing_token: String,
	#[napi(js_name = "expiresAt")]
	pub expires_at: f64,
	#[napi(js_name = "queueWaitedMs")]
	pub queue_waited_ms: f64,
}

#[napi(object)]
pub struct LockRenewOutput {
	#[napi(js_name = "expiresAt")]
	pub expires_at: f64,
}

#[napi(object)]
pub struct LockReleaseOutput {
	pub released: bool,
	#[napi(js_name = "heldMs")]
	pub held_ms: f64,
}

#[napi(object)]
pub struct LockQuarantineReceiptInput {
	#[napi(js_name = "leaseId")]
	pub lease_id: String,
	pub corpus: String,
	#[napi(js_name = "processInspected")]
	pub process_inspected: bool,
	#[napi(js_name = "gitStatusChecked")]
	pub git_status_checked: bool,
	#[napi(js_name = "gitLogChecked")]
	pub git_log_checked: bool,
	#[napi(js_name = "gitFsckChecked")]
	pub git_fsck_checked: bool,
	#[napi(js_name = "remoteVerified")]
	pub remote_verified: bool,
}

#[napi(object)]
pub struct LockQuarantineReceiptOutput {
	#[napi(js_name = "receiptId")]
	pub receipt_id: String,
	#[napi(js_name = "leaseId")]
	pub lease_id: String,
	pub corpus: String,
}

#[napi(object)]
pub struct LeaseOutput {
	#[napi(js_name = "leaseId")]
	pub lease_id: String,
	#[napi(js_name = "holderKind")]
	pub holder_kind: String,
	#[napi(js_name = "sessionId")]
	pub session_id: String,
	pub label: String,
	pub pid: i32,
	#[napi(js_name = "pidStartTime")]
	pub pid_start_time: String,
	pub pgid: i32,
	#[napi(js_name = "pgidStartTime")]
	pub pgid_start_time: Option<String>,
	#[napi(js_name = "connId")]
	pub conn_id: Option<String>,
	#[napi(js_name = "class")]
	pub class: String,
	pub state: String,
	#[napi(js_name = "fencingToken")]
	pub fencing_token: String,
	#[napi(js_name = "expiresAt")]
	pub expires_at: f64,
}

#[napi(object)]
pub struct QueueEntryOutput {
	#[napi(js_name = "class")]
	pub class: String,
	pub label: String,
	#[napi(js_name = "waitedMs")]
	pub waited_ms: f64,
}

#[napi(object)]
pub struct LockStatusOutput {
	pub held: bool,
	pub holder: Option<LeaseOutput>,
	#[napi(js_name = "expiresAt")]
	pub expires_at: Option<f64>,
	#[napi(js_name = "fencingToken")]
	pub fencing_token: Option<String>,
	pub queue: Vec<QueueEntryOutput>,
	pub stuck: bool,
	pub quarantined: bool,
}

#[napi(object)]
pub struct ProcessIdentityOutput {
	pub pid: i32,
	#[napi(js_name = "pidStartTime")]
	pub pid_start_time: String,
	pub pgid: i32,
	#[napi(js_name = "pgidStartTime")]
	pub pgid_start_time: Option<String>,
}

#[napi(object)]
pub struct JournalAppendOutput {
	pub cursor: String,
	pub seq: String,
}

#[napi(object)]
pub struct JournalEventOutput {
	pub seq: String,
	pub ts: f64,
	pub kind: String,
	#[napi(js_name = "payloadJson")]
	pub payload_json: String,
}

#[napi(object)]
pub struct JournalGapOutput {
	#[napi(js_name = "missingFrom")]
	pub missing_from: String,
	#[napi(js_name = "missingTo")]
	pub missing_to: String,
	#[napi(js_name = "resyncCursor")]
	pub resync_cursor: String,
}

#[napi(object)]
pub struct JournalReadOutput {
	pub events: Vec<JournalEventOutput>,
	#[napi(js_name = "nextCursor")]
	pub next_cursor: String,
	pub gap: Option<JournalGapOutput>,
}

#[napi(object)]
pub struct ConsumerClaimOutput {
	#[napi(js_name = "claimId")]
	pub claim_id: String,
	pub cursor: String,
	#[napi(js_name = "expiresAt")]
	pub expires_at: f64,
}

#[napi(object)]
pub struct DeliveryProofInput {
	pub seq: String,
	#[napi(js_name = "platformMsgId")]
	pub platform_msg_id: Option<String>,
	#[napi(js_name = "dedupeKey")]
	pub dedupe_key: Option<String>,
}

#[napi(object)]
pub struct ConsumerCommitInput {
	#[napi(js_name = "consumerId")]
	pub consumer_id: String,
	#[napi(js_name = "claimId")]
	pub claim_id: String,
	pub cursor: String,
	pub proofs: Vec<DeliveryProofInput>,
}

#[napi(object)]
pub struct ConsumerCommitOutput {
	#[napi(js_name = "committedCursor")]
	pub committed_cursor: String,
}

#[napi(object)]
pub struct OutboxRowOutput {
	#[napi(js_name = "consumerId")]
	pub consumer_id: String,
	pub seq: String,
	pub state: String,
	#[napi(js_name = "platformMsgId")]
	pub platform_msg_id: Option<String>,
	#[napi(js_name = "dedupeKey")]
	pub dedupe_key: Option<String>,
}

#[napi(object)]
pub struct IdempotencyReplayInput {
	pub scope: String,
	pub key: String,
	#[napi(js_name = "requestJson")]
	pub request_json: String,
}

#[napi(object)]
pub struct IdempotencyReplayOutput {
	pub replayed: bool,
	#[napi(js_name = "responseJson")]
	pub response_json: Option<String>,
}

#[napi(object)]
pub struct IdempotencyStoreInput {
	pub scope: String,
	pub key: String,
	#[napi(js_name = "requestJson")]
	pub request_json: String,
	#[napi(js_name = "responseJson")]
	pub response_json: String,
}

#[napi(object)]
pub struct ClosureOperationClaimInput {
	pub scope: String,
	pub key: String,
	#[napi(js_name = "requestJson")]
	pub request_json: String,
	#[napi(js_name = "intentJson")]
	pub intent_json: String,
	#[napi(js_name = "operationJson")]
	pub operation_json: String,
}

#[napi(object)]
pub struct ClosureOperationClaimOutput {
	pub claimed: bool,
	#[napi(js_name = "responseJson")]
	pub response_json: Option<String>,
}

#[napi(object)]
pub struct ClosureOperationFinalizeInput {
	pub scope: String,
	pub key: String,
	#[napi(js_name = "requestJson")]
	pub request_json: String,
	#[napi(js_name = "intentJson")]
	pub intent_json: String,
	#[napi(js_name = "responseJson")]
	pub response_json: String,
	#[napi(js_name = "operationJson")]
	pub operation_json: String,
}

#[napi(object)]
pub struct ClosureOperationFinalizeOutput {
	#[napi(js_name = "responseJson")]
	pub response_json: String,
}

#[napi(object)]
pub struct MainAdmissionOperationClaimInput {
	pub scope: String,
	pub key: String,
	#[napi(js_name = "requestJson")]
	pub request_json: String,
	#[napi(js_name = "intentJson")]
	pub intent_json: String,
}

#[napi(object)]
pub struct MainAdmissionOperationClaimOutput {
	pub claimed: bool,
	#[napi(js_name = "responseJson")]
	pub response_json: Option<String>,
}

#[napi(object)]
pub struct MainAdmissionOperationFinalizeInput {
	pub scope: String,
	pub key: String,
	#[napi(js_name = "requestJson")]
	pub request_json: String,
	#[napi(js_name = "intentJson")]
	pub intent_json: String,
	#[napi(js_name = "responseJson")]
	pub response_json: String,
}

#[napi(object)]
pub struct MainAdmissionOperationFinalizeOutput {
	#[napi(js_name = "responseJson")]
	pub response_json: String,
}

#[napi(object)]
pub struct PendingMainAdmissionOperationOutput {
	pub scope: String,
	pub key: String,
	#[napi(js_name = "requestJson")]
	pub request_json: String,
	#[napi(js_name = "intentJson")]
	pub intent_json: String,
}

/// One metadata value returned from the durable gateway state store. A missing
/// key is represented by an absent value rather than a synthetic default.
#[napi(object)]
pub struct GatewayMetaEntry {
	pub key: String,
	pub value: Option<String>,
}

/// A compare-and-set condition evaluated inside a single SQLite transaction.
#[napi(object)]
pub struct GatewayMetaExpectation {
	pub key: String,
	pub value: String,
}

/// A durable metadata write performed by `gatewayMetaTransaction`.
#[napi(object)]
pub struct GatewayMetaPut {
	pub key: String,
	pub value: String,
}

/// Atomic metadata mutation used by the bootstrap, growth, and profile-approval
/// protocols. `eventKind` and `eventPayloadJson` are committed in the same WAL
/// transaction when present.
#[napi(object)]
pub struct GatewayMetaTransactionInput {
	pub expected: Vec<GatewayMetaExpectation>,
	pub puts: Vec<GatewayMetaPut>,
	pub deletes: Vec<String>,
	#[napi(js_name = "eventKind")]
	pub event_kind: Option<String>,
	#[napi(js_name = "eventPayloadJson")]
	pub event_payload_json: Option<String>,
}

#[napi(object)]
pub struct GatewayMetaTransactionOutput {
	pub applied: bool,
	pub cursor: Option<String>,
}

#[napi(object)]
pub struct RegistryBrokerRowInput {
	#[napi(js_name = "sessionId")]
	pub session_id: String,
	/// Canonical JSON for the SDK's credential-free locator object.
	pub locator: String,
	#[napi(js_name = "endpointGeneration")]
	pub endpoint_generation: f64,
	#[napi(js_name = "hostIncarnation")]
	pub host_incarnation: Option<String>,
	#[napi(js_name = "identityProvenance")]
	pub identity_provenance: Option<String>,
	#[napi(js_name = "indexSeq")]
	pub index_seq: f64,
	pub live: bool,
	pub deleted: bool,
	#[napi(js_name = "terminalUncertain")]
	pub terminal_uncertain: bool,
	pub ambiguous: bool,
	#[napi(js_name = "activityState")]
	pub activity_state: Option<String>,
	#[napi(js_name = "activityAt")]
	pub activity_at: Option<f64>,
	#[napi(js_name = "lastHeartbeatAt")]
	pub last_heartbeat_at: Option<f64>,
}

#[napi(object)]
pub struct RegistryApplyBrokerSnapshotInput {
	#[napi(js_name = "observedAt")]
	pub observed_at: f64,
	pub rows: Vec<RegistryBrokerRowInput>,
}

#[napi(object)]
pub struct RegistrySnapshotApplyOutput {
	#[napi(js_name = "newSessionIds")]
	pub new_session_ids: Vec<String>,
	#[napi(js_name = "changedSessionIds")]
	pub changed_session_ids: Vec<String>,
	#[napi(js_name = "changedIndexSeqSessionIds")]
	pub changed_index_seq_session_ids: Vec<String>,
	#[napi(js_name = "driftCount")]
	pub drift_count: f64,
}

#[napi(object)]
pub struct RegistryListInput {
	pub kind: Option<String>,
	pub status: Option<String>,
	#[napi(js_name = "surfaceId")]
	pub surface_id: Option<String>,
	pub limit: Option<u32>,
	pub offset: Option<f64>,
}

#[napi(object)]
pub struct RegistryRowOutput {
	#[napi(js_name = "sessionId")]
	pub session_id: String,
	pub kind: String,
	pub purpose: Option<String>,
	pub brief: Option<String>,
	pub status: String,
	#[napi(js_name = "surfaceId")]
	pub surface_id: Option<String>,
	pub locator: Option<String>,
	#[napi(js_name = "endpointGeneration")]
	pub endpoint_generation: Option<f64>,
	#[napi(js_name = "hostIncarnation")]
	pub host_incarnation: Option<String>,
	#[napi(js_name = "identityProvenance")]
	pub identity_provenance: Option<String>,
	#[napi(js_name = "indexSeq")]
	pub index_seq: Option<f64>,
	pub live: bool,
	pub deleted: bool,
	#[napi(js_name = "terminalUncertain")]
	pub terminal_uncertain: bool,
	pub ambiguous: bool,
	#[napi(js_name = "activityState")]
	pub activity_state: Option<String>,
	#[napi(js_name = "activityAt")]
	pub activity_at: Option<f64>,
	#[napi(js_name = "lastHeartbeatAt")]
	pub last_heartbeat_at: Option<f64>,
	#[napi(js_name = "metaName")]
	pub meta_name: Option<String>,
	#[napi(js_name = "metaCwd")]
	pub meta_cwd: Option<String>,
	#[napi(js_name = "metaKind")]
	pub meta_kind: Option<String>,
	#[napi(js_name = "metadataState")]
	pub metadata_state: String,
	#[napi(js_name = "metadataAt")]
	pub metadata_at: Option<f64>,
	pub source: String,
	#[napi(js_name = "createdAt")]
	pub created_at: f64,
	#[napi(js_name = "lastSeenAt")]
	pub last_seen_at: Option<f64>,
	#[napi(js_name = "closedAt")]
	pub closed_at: Option<f64>,
	#[napi(js_name = "registryRev")]
	pub registry_rev: f64,
	pub quarantined: bool,
}

#[napi(object)]
pub struct RegistryListOutput {
	pub rows: Vec<RegistryRowOutput>,
	pub total: f64,
}

#[napi(object)]
pub struct RegistryAnnotationInput {
	#[napi(js_name = "sessionId")]
	pub session_id: String,
	pub purpose: Option<String>,
	pub brief: Option<String>,
	#[napi(js_name = "observedAt")]
	pub observed_at: Option<f64>,
}

#[napi(object)]
pub struct RegistryMetadataInput {
	#[napi(js_name = "sessionId")]
	pub session_id: String,
	pub name: String,
	pub cwd: String,
	pub kind: String,
	#[napi(js_name = "observedAt")]
	pub observed_at: f64,
}

#[napi(object)]
pub struct RegistryMetadataUnavailableInput {
	#[napi(js_name = "sessionId")]
	pub session_id: String,
	#[napi(js_name = "observedAt")]
	pub observed_at: f64,
}

#[napi(object)]
pub struct RegistrySurfaceInput {
	#[napi(js_name = "surfaceId")]
	pub surface_id: String,
	pub platform: String,
	pub kind: String,
	#[napi(js_name = "isOwnerSurface")]
	pub is_owner_surface: bool,
}

#[napi(object)]
pub struct RegistryGatewaySessionInput {
	#[napi(js_name = "sessionId")]
	pub session_id: String,
	pub kind: String,
	pub purpose: Option<String>,
	pub brief: Option<String>,
	pub status: String,
	#[napi(js_name = "surfaceId")]
	pub surface_id: Option<String>,
	#[napi(js_name = "observedAt")]
	pub observed_at: Option<f64>,
}

#[napi(object)]
pub struct SurfaceResolutionOutput {
	pub surface: RegistrySurfaceInput,
	#[napi(js_name = "sessionId")]
	pub session_id: Option<String>,
	pub quarantined: bool,
}

#[napi(object)]
pub struct ReconcileStatusInput {
	#[napi(js_name = "lastOkAt")]
	pub last_ok_at: f64,
	#[napi(js_name = "cycleMs")]
	pub cycle_ms: f64,
	#[napi(js_name = "driftCount")]
	pub drift_count: f64,
}
#[napi(object)]
pub struct GatewayMetaReadOutput {
	pub entries: Vec<GatewayMetaEntry>,
}

/// N-API handle over a real, migrated SQLite state directory.
#[napi]
pub struct WayCore {
	state_dir: String,
	store: Store,
	locks: LockManager,
	journal: EventJournal,
	rpc_server: Mutex<Option<RpcServerHandle>>,
}

#[napi]
impl WayCore {
	/// Opens the state directory, migrates it under SQLite WAL, and runs
	/// `integrity_check` before exposing any mutation APIs. Startup also performs
	/// the v1 in-daemon lease death-check before the daemon accepts work.
	#[napi(factory)]
	pub fn open(state_dir: String) -> napi::Result<Self> {
		open_way_core(state_dir, None)
	}

	/// Internal deterministic test seam for the ten-minute hard-cap protocol.
	/// The shipped daemon always uses the fixed production cap.
	#[napi(factory, js_name = "openWithTestHardCap")]
	pub fn open_with_test_hard_cap(state_dir: String, hard_hold_cap_ms: u32) -> napi::Result<Self> {
		if hard_hold_cap_ms == 0 || u64::from(hard_hold_cap_ms) > HARD_HOLD_CAP_MS {
			return Err(napi::Error::from_reason("test hard hold cap must be in 1..=600000 ms"));
		}
		open_way_core(state_dir, Some(u64::from(hard_hold_cap_ms)))
	}

	#[napi(getter, js_name = "stateDir")]
	pub fn state_dir(&self) -> String {
		self.state_dir.clone()
	}

	/// Starts the hardened state-directory UDS server and registers the TSFN
	/// bridge callback before accepting any client request.
	#[napi(
		js_name = "startRpcServer",
		ts_args_type = "socketPath: string, bridgeCallback: (err: null | Error, request: BridgeRequest) => void"
	)]
	pub fn start_rpc_server(&self, socket_path: String, bridge_callback: ThreadsafeFunction<BridgeRequest>) -> napi::Result<()> {
		if socket_path.trim().is_empty() {
			return Err(napi::Error::from_reason("socketPath must not be empty"));
		}
		let mut server = self.rpc_server.lock().map_err(|_| napi::Error::from_reason("RPC server lock was poisoned"))?;
		if server.is_some() {
			return Err(napi::Error::from_reason("RPC server is already running"));
		}
		let bridge = TsfnBridge::new(bridge_callback);
		let dispatcher = RpcDispatcher::new(self.store.clone(), self.locks.clone(), self.journal.clone(), bridge);
		let handle = RpcServerHandle::start(PathBuf::from(socket_path), PathBuf::from(&self.state_dir), dispatcher)
			.map_err(|error| napi::Error::from_reason(error.to_string()))?;
		*server = Some(handle);
		Ok(())
	}

	/// Resolves one TypeScript bridge request. The completion payload is a JSON
	/// result, or `{\"error\": ...}` for a typed bridge-side failure.
	#[napi(js_name = "bridgeComplete")]
	pub fn bridge_complete(&self, request_id: i64, result_json: String) -> napi::Result<bool> {
		let request_id = u64::try_from(request_id).map_err(|_| napi::Error::from_reason("reqId must be a positive integer"))?;
		let server = self.rpc_server.lock().map_err(|_| napi::Error::from_reason("RPC server lock was poisoned"))?;
		Ok(server.as_ref().is_some_and(|server| server.bridge_complete(request_id, &result_json)))
	}

	#[napi(js_name = "shutdownRpcServer")]
	pub fn shutdown_rpc_server(&self) -> napi::Result<()> {
		let server = self.rpc_server.lock().map_err(|_| napi::Error::from_reason("RPC server lock was poisoned"))?.take();
		if let Some(server) = server {
			server.shutdown();
		}
		Ok(())
	}

	#[napi(js_name = "rpcBridgeStats")]
	pub fn rpc_bridge_stats(&self) -> napi::Result<RpcBridgeStats> {
		let server = self.rpc_server.lock().map_err(|_| napi::Error::from_reason("RPC server lock was poisoned"))?;
		server
			.as_ref()
			.map(RpcServerHandle::bridge_stats)
			.ok_or_else(|| napi::Error::from_reason("RPC server is not running"))
	}

	#[napi(js_name = "rpcDroppedNotificationCount")]
	pub fn rpc_dropped_notification_count(&self) -> napi::Result<i64> {
		let server = self.rpc_server.lock().map_err(|_| napi::Error::from_reason("RPC server lock was poisoned"))?;
		let count = server
			.as_ref()
			.map(RpcServerHandle::dropped_notification_count)
			.ok_or_else(|| napi::Error::from_reason("RPC server is not running"))?;
		Ok(i64::try_from(count).unwrap_or(i64::MAX))
	}

	/// Updates the in-memory health state exposed by the already-running RPC
	/// listener. Durable failure reasons are stored separately in gateway_meta.
	#[napi(js_name = "setRpcHealth")]
	pub fn set_rpc_health(&self, state: String, reason: Option<String>) -> napi::Result<()> {
		let state = match state.as_str() {
			"booting" => GatewayState::Booting,
			"verifying" => GatewayState::Verifying,
			"running" => GatewayState::Running,
			"failed_closed" => GatewayState::FailedClosed,
			"degraded" => GatewayState::Degraded,
			_ => {
				return Err(napi::Error::from_reason("state must be booting, verifying, running, failed_closed, or degraded"));
			}
		};
		let server = self.rpc_server.lock().map_err(|_| napi::Error::from_reason("RPC server lock was poisoned"))?;
		let server = server.as_ref().ok_or_else(|| napi::Error::from_reason("RPC server is not running"))?;
		server.set_gateway_state(state, reason);
		Ok(())
	}

	/// Publishes live main-session state after strict resume for `way.status`.
	#[napi(js_name = "setMainSessionStatus")]
	pub fn set_main_session_status(&self, turn_state: String, follow_up_queue_depth: u32) -> napi::Result<()> {
		if !matches!(turn_state.as_str(), "idle" | "busy") {
			return Err(napi::Error::from_reason("turnState must be idle or busy"));
		}
		let server = self.rpc_server.lock().map_err(|_| napi::Error::from_reason("RPC server lock was poisoned"))?;
		let server = server.as_ref().ok_or_else(|| napi::Error::from_reason("RPC server is not running"))?;
		server.set_main_session_status(turn_state, u64::from(follow_up_queue_depth));
		Ok(())
	}

	/// Clears process-local resume facts once the hosted main session is gone.
	#[napi(js_name = "resetMainSessionStatus")]
	pub fn reset_main_session_status(&self) -> napi::Result<()> {
		let server = self.rpc_server.lock().map_err(|_| napi::Error::from_reason("RPC server lock was poisoned"))?;
		let server = server.as_ref().ok_or_else(|| napi::Error::from_reason("RPC server is not running"))?;
		server.reset_main_session_status();
		Ok(())
	}

	/// Marks journal-derived delivery as halted after a synchronous append failure.
	#[napi(js_name = "setJournalDegraded")]
	pub fn set_journal_degraded(&self, degraded: bool) -> napi::Result<()> {
		let server = self.rpc_server.lock().map_err(|_| napi::Error::from_reason("RPC server lock was poisoned"))?;
		let server = server.as_ref().ok_or_else(|| napi::Error::from_reason("RPC server is not running"))?;
		server.set_journal_degraded(degraded);
		Ok(())
	}

	/// Sends a best-effort systemd STATUS notification. It is intentionally a
	/// no-op when this process was not started with NOTIFY_SOCKET.
	#[napi(js_name = "sdNotifyStatus")]
	pub fn sd_notify_status(&self, status: String) -> napi::Result<()> {
		crate::systemd::notify_status(&status).map_err(|error| napi::Error::from_reason(error.to_string()))
	}

	/// Sends systemd `READY=1` and STATUS after the daemon is fully usable.
	/// It is a no-op when the process was not started with NOTIFY_SOCKET.
	#[napi(js_name = "sdNotifyReady")]
	pub fn sd_notify_ready(&self, status: String) -> napi::Result<()> {
		crate::systemd::notify_ready(&status).map_err(|error| napi::Error::from_reason(error.to_string()))
	}

	#[napi(js_name = "gatewayMetaRead")]
	pub fn gateway_meta_read(&self, keys: Vec<String>) -> napi::Result<GatewayMetaReadOutput> {
		validate_meta_keys(&keys)?;
		let mut connection = self.store.connection().map_err(store_napi_error)?;
		let transaction = connection.transaction().map_err(|error| store_napi_error(error.into()))?;
		let entries = keys
			.into_iter()
			.map(|key| {
				let value = meta_get_tx(&transaction, &key).map_err(store_napi_error)?;
				Ok(GatewayMetaEntry { key, value })
			})
			.collect::<napi::Result<Vec<_>>>()?;
		transaction.commit().map_err(|error| store_napi_error(error.into()))?;
		Ok(GatewayMetaReadOutput { entries })
	}

	/// Applies a compare-and-set metadata update and optional journal event in
	/// one SQLite WAL transaction. A false `applied` result means no write or
	/// event was committed because an expected value changed.
	#[napi(js_name = "gatewayMetaTransaction")]
	pub fn gateway_meta_transaction(&self, input: GatewayMetaTransactionInput) -> napi::Result<GatewayMetaTransactionOutput> {
		validate_gateway_meta_transaction(&input)?;
		let mut connection = self.store.connection().map_err(store_napi_error)?;
		let transaction = connection.transaction().map_err(|error| store_napi_error(error.into()))?;

		for expected in &input.expected {
			if meta_get_tx(&transaction, &expected.key).map_err(store_napi_error)? != Some(expected.value.clone()) {
				return Ok(GatewayMetaTransactionOutput { applied: false, cursor: None });
			}
		}
		for put in &input.puts {
			meta_set_tx(&transaction, &put.key, &put.value).map_err(store_napi_error)?;
		}
		for key in &input.deletes {
			transaction
				.execute("DELETE FROM gateway_meta WHERE k = ?1", [key])
				.map_err(|error| store_napi_error(error.into()))?;
		}
		let cursor = match (&input.event_kind, &input.event_payload_json) {
			(Some(kind), Some(payload_json)) => Some(
				append_in_transaction(&transaction, kind, payload_json, unix_epoch_ms())
					.map_err(journal_napi_error)?
					.to_string(),
			),
			(None, None) => None,
			_ => unreachable!("validated event fields"),
		};
		transaction.commit().map_err(|error| store_napi_error(error.into()))?;
		Ok(GatewayMetaTransactionOutput { applied: true, cursor })
	}

	#[napi(js_name = "lockAcquire")]
	pub fn lock_acquire(&self, input: LockAcquireInput) -> napi::Result<LockAcquireOutput> {
		let request = acquire_request_from_napi(input)?;
		self.locks.acquire(request).map(lock_acquire_output).map_err(lock_napi_error)
	}

	#[napi(js_name = "lockRenew")]
	pub fn lock_renew(&self, lease_id: String) -> napi::Result<LockRenewOutput> {
		self.locks
			.renew(&lease_id)
			.map(|expires_at| LockRenewOutput { expires_at: expires_at as f64 })
			.map_err(lock_napi_error)
	}

	/// Returns whether this exact lease/token still owns write authority at a
	/// Git step boundary. A false result is a fail-stop instruction.
	#[napi(js_name = "lockFencingValid")]
	pub fn lock_fencing_valid(&self, lease_id: String, fencing_token: String) -> napi::Result<bool> {
		let fencing_token = fencing_token
			.parse::<u64>()
			.map_err(|_| napi::Error::from_reason("fencingToken must be a u64 string"))?;
		self.locks.fencing_valid(&lease_id, fencing_token).map_err(lock_napi_error)
	}

	/// Returns durable FSM revocation notices once. The in-daemon executor owns
	/// the actual child handle and confirms process-group reaping.
	#[napi(js_name = "lockDrainRevocations")]
	pub fn lock_drain_revocations(&self) -> Vec<String> {
		self.locks.drain_revocations()
	}

	/// Captures a process and process-group incarnation for an in-daemon child
	/// before its lease row is created.
	#[napi(js_name = "processIdentity")]
	pub fn process_identity(&self, pid: i32) -> napi::Result<ProcessIdentityOutput> {
		process_identity_output(pid)
	}

	#[napi(js_name = "lockRelease")]
	pub fn lock_release(&self, lease_id: String) -> napi::Result<LockReleaseOutput> {
		self.locks.release(&lease_id).map(lock_release_output).map_err(lock_napi_error)
	}

	#[napi(js_name = "lockStatus")]
	pub fn lock_status(&self) -> napi::Result<LockStatusOutput> {
		self.locks.status().map(lock_status_output).map_err(lock_napi_error)
	}

	#[napi(js_name = "lockForceRelease")]
	pub fn lock_force_release(&self, lease_id: String, confirm: bool) -> napi::Result<LockReleaseOutput> {
		self.locks.force_release(&lease_id, confirm).map(lock_release_output).map_err(lock_napi_error)
	}

	#[napi(js_name = "lockQuarantineOverride")]
	pub fn lock_quarantine_override(&self, lease_id: String, confirm: bool, acknowledge_unverified: bool) -> napi::Result<LockStatusOutput> {
		self.locks
			.quarantine_override(&lease_id, confirm, acknowledge_unverified)
			.map(lock_status_output)
			.map_err(lock_napi_error)
	}

	/// Persists a structurally complete operator verification receipt for the
	/// exact quarantined corpus lease after runtime death proof.
	#[napi(js_name = "lockRecordQuarantineReceipt")]
	pub fn lock_record_quarantine_receipt(&self, input: LockQuarantineReceiptInput) -> napi::Result<LockQuarantineReceiptOutput> {
		let evidence = QuarantineReceiptEvidence {
			process_inspected: input.process_inspected,
			git_status_checked: input.git_status_checked,
			git_log_checked: input.git_log_checked,
			git_fsck_checked: input.git_fsck_checked,
			remote_verified: input.remote_verified,
		};
		let receipt_id = self
			.locks
			.record_quarantine_receipt(&input.lease_id, &input.corpus, evidence)
			.map_err(lock_napi_error)?;
		Ok(LockQuarantineReceiptOutput { receipt_id, lease_id: input.lease_id, corpus: input.corpus })
	}

	#[napi(js_name = "lockClearQuarantine")]
	pub fn lock_clear_quarantine(&self, verification_receipt_id: String, confirm: bool) -> napi::Result<LockStatusOutput> {
		self.locks
			.clear_quarantine(&verification_receipt_id, confirm)
			.map(lock_status_output)
			.map_err(lock_napi_error)
	}

	#[napi(js_name = "journalAppend")]
	pub fn journal_append(&self, kind: String, payload_json: String) -> napi::Result<JournalAppendOutput> {
		self.journal
			.append(&kind, &payload_json)
			.map(|cursor| JournalAppendOutput { cursor: cursor.to_string(), seq: cursor.seq.to_string() })
			.map_err(journal_napi_error)
	}

	#[napi(js_name = "journalRead")]
	pub fn journal_read(&self, cursor: Option<String>, limit: Option<u32>) -> napi::Result<JournalReadOutput> {
		let cursor = cursor
			.map(|value| Cursor::from_str(&value).map_err(|_| napi::Error::from_reason("invalid journal cursor")))
			.transpose()?;
		self.journal.read(cursor, limit.unwrap_or(100)).map(journal_read_output).map_err(journal_napi_error)
	}

	#[napi(js_name = "consumerClaim")]
	pub fn consumer_claim(&self, consumer_id: String, claim_ttl_ms: Option<u32>) -> napi::Result<ConsumerClaimOutput> {
		self.journal
			.claim_consumer(&consumer_id, claim_ttl_ms.map(u64::from))
			.map(consumer_claim_output)
			.map_err(journal_napi_error)
	}

	#[napi(js_name = "consumerCommit")]
	pub fn consumer_commit(&self, input: ConsumerCommitInput) -> napi::Result<ConsumerCommitOutput> {
		let cursor = Cursor::from_str(&input.cursor).map_err(|_| napi::Error::from_reason("invalid journal cursor"))?;
		let proofs = input
			.proofs
			.into_iter()
			.map(|proof| {
				proof
					.seq
					.parse::<u64>()
					.map(|seq| DeliveryProof { seq, platform_msg_id: proof.platform_msg_id, dedupe_key: proof.dedupe_key })
					.map_err(|_| napi::Error::from_reason("delivery proof seq must be a u64 string"))
			})
			.collect::<napi::Result<Vec<_>>>()?;
		self.journal
			.commit_consumer(&input.consumer_id, &input.claim_id, cursor, &proofs)
			.map(|cursor| ConsumerCommitOutput { committed_cursor: cursor.to_string() })
			.map_err(journal_napi_error)
	}

	#[napi(js_name = "consumerCursor")]
	pub fn consumer_cursor(&self, consumer_id: String) -> napi::Result<Option<String>> {
		self.journal
			.consumer_cursor(&consumer_id)
			.map(|cursor| cursor.map(|value| value.to_string()))
			.map_err(journal_napi_error)
	}

	#[napi(js_name = "consumerOutbox")]
	pub fn consumer_outbox(&self, consumer_id: String) -> napi::Result<Vec<OutboxRowOutput>> {
		self.journal
			.outbox_rows(&consumer_id)
			.map(|rows| rows.into_iter().map(outbox_row_output).collect())
			.map_err(journal_napi_error)
	}

	#[napi(js_name = "registryApplyBrokerSnapshot")]
	pub fn registry_apply_broker_snapshot(&self, input: RegistryApplyBrokerSnapshotInput) -> napi::Result<RegistrySnapshotApplyOutput> {
		let snapshot = broker_snapshot_from_napi(input)?;
		registry::apply_broker_snapshot(&self.store, snapshot)
			.map(snapshot_apply_output)
			.map_err(registry_napi_error)
	}

	#[napi(js_name = "registryList")]
	pub fn registry_list(&self, input: Option<RegistryListInput>) -> napi::Result<RegistryListOutput> {
		let filter = registry_list_filter_from_napi(input)?;
		registry::list(&self.store, filter)
			.map(|listed| RegistryListOutput { rows: listed.rows.into_iter().map(registry_row_output).collect(), total: listed.total as f64 })
			.map_err(registry_napi_error)
	}

	#[napi(js_name = "registryGet")]
	pub fn registry_get(&self, session_id: String) -> napi::Result<RegistryRowOutput> {
		registry::get(&self.store, &session_id).map(registry_row_output).map_err(registry_napi_error)
	}

	#[napi(js_name = "registryAnnotate")]
	pub fn registry_annotate(&self, input: RegistryAnnotationInput) -> napi::Result<RegistryRowOutput> {
		let observed_at = input.observed_at.map(napi_i64).transpose()?.unwrap_or_else(unix_epoch_ms);
		registry::annotate(
			&self.store,
			RegistryAnnotation { session_id: input.session_id, purpose: input.purpose, brief: input.brief, observed_at },
		)
		.map(registry_row_output)
		.map_err(registry_napi_error)
	}

	#[napi(js_name = "registryApplyMetadata")]
	pub fn registry_apply_metadata(&self, input: RegistryMetadataInput) -> napi::Result<RegistryRowOutput> {
		let observed_at = napi_i64(input.observed_at)?;
		registry::apply_metadata(
			&self.store,
			MetadataEnrichment { session_id: input.session_id, name: input.name, cwd: input.cwd, kind: input.kind, observed_at },
		)
		.map(registry_row_output)
		.map_err(registry_napi_error)
	}

	#[napi(js_name = "registryMarkMetadataUnavailable")]
	pub fn registry_mark_metadata_unavailable(&self, input: RegistryMetadataUnavailableInput) -> napi::Result<RegistryRowOutput> {
		registry::mark_metadata_unavailable(&self.store, &input.session_id, napi_i64(input.observed_at)?)
			.map(registry_row_output)
			.map_err(registry_napi_error)
	}

	#[napi(js_name = "registryConfigureSurfaces")]
	pub fn registry_configure_surfaces(&self, surfaces: Vec<RegistrySurfaceInput>, observed_at: Option<f64>) -> napi::Result<()> {
		let observed_at = observed_at.map(napi_i64).transpose()?.unwrap_or_else(unix_epoch_ms);
		let surfaces = surfaces
			.into_iter()
			.map(|surface| SurfaceRecord {
				surface_id: surface.surface_id,
				platform: surface.platform,
				kind: surface.kind,
				is_owner_surface: surface.is_owner_surface,
			})
			.collect::<Vec<_>>();
		registry::configure_surfaces(&self.store, &surfaces, observed_at).map_err(registry_napi_error)
	}

	#[napi(js_name = "registryBindSurface")]
	pub fn registry_bind_surface(&self, surface_id: String, session_id: String, observed_at: Option<f64>) -> napi::Result<()> {
		let observed_at = observed_at.map(napi_i64).transpose()?.unwrap_or_else(unix_epoch_ms);
		registry::bind_surface(&self.store, &surface_id, &session_id, observed_at).map_err(registry_napi_error)
	}

	#[napi(js_name = "registryRegisterGatewaySession")]
	pub fn registry_register_gateway_session(&self, input: RegistryGatewaySessionInput) -> napi::Result<RegistryRowOutput> {
		let observed_at = input.observed_at.map(napi_i64).transpose()?.unwrap_or_else(unix_epoch_ms);
		registry::register_gateway_session(
			&self.store,
			GatewaySession {
				session_id: input.session_id,
				kind: input.kind,
				purpose: input.purpose,
				brief: input.brief,
				status: input.status,
				surface_id: input.surface_id,
				observed_at,
			},
		)
		.map(registry_row_output)
		.map_err(registry_napi_error)
	}

	#[napi(js_name = "surfaceResolve")]
	pub fn surface_resolve(&self, surface_id: String) -> napi::Result<SurfaceResolutionOutput> {
		registry::resolve_surface(&self.store, &surface_id)
			.map(|resolved| SurfaceResolutionOutput {
				surface: RegistrySurfaceInput {
					surface_id: resolved.surface.surface_id,
					platform: resolved.surface.platform,
					kind: resolved.surface.kind,
					is_owner_surface: resolved.surface.is_owner_surface,
				},
				session_id: resolved.session_id,
				quarantined: resolved.quarantined,
			})
			.map_err(registry_napi_error)
	}

	#[napi(js_name = "setReconcileStatus")]
	pub fn set_reconcile_status(&self, input: ReconcileStatusInput) -> napi::Result<()> {
		let last_ok_at = napi_i64(input.last_ok_at)?;
		let cycle_ms = napi_i64(input.cycle_ms)?;
		let drift_count = napi_u64(input.drift_count)?;
		self.store.set_meta("reconcile_last_ok_at", &last_ok_at.to_string()).map_err(store_napi_error)?;
		self.store.set_meta("reconcile_cycle_ms", &cycle_ms.to_string()).map_err(store_napi_error)?;
		self.store.set_meta("reconcile_drift_count", &drift_count.to_string()).map_err(store_napi_error)
	}

	#[napi(js_name = "idempotencyReplay")]
	pub fn idempotency_replay(&self, input: IdempotencyReplayInput) -> napi::Result<IdempotencyReplayOutput> {
		self.store
			.replay_idempotency(&input.scope, &input.key, &input.request_json, unix_epoch_ms())
			.map(|response_json| IdempotencyReplayOutput { replayed: response_json.is_some(), response_json })
			.map_err(store_napi_error)
	}

	#[napi(js_name = "idempotencyStore")]
	pub fn idempotency_store(&self, input: IdempotencyStoreInput) -> napi::Result<()> {
		self.store
			.store_idempotency_response(&input.scope, &input.key, &input.request_json, &input.response_json, unix_epoch_ms())
			.map_err(store_napi_error)
	}

	/// Claims a corpus closure's durable intent before its Git effect begins.
	#[napi(js_name = "closureOperationClaim")]
	pub fn closure_operation_claim(&self, input: ClosureOperationClaimInput) -> napi::Result<ClosureOperationClaimOutput> {
		match self
			.store
			.claim_closure_operation(
				&input.scope,
				&input.key,
				&input.request_json,
				&input.intent_json,
				&input.operation_json,
				unix_epoch_ms(),
			)
			.map_err(store_napi_error)?
		{
			ClosureOperationClaim::Claimed => Ok(ClosureOperationClaimOutput { claimed: true, response_json: None }),
			ClosureOperationClaim::Existing { response_json } => {
				Ok(ClosureOperationClaimOutput { claimed: false, response_json: Some(response_json) })
			}
		}
	}

	/// Atomically publishes a completed corpus closure response and removes its
	/// active intent only when the exact original intent still owns the key.
	#[napi(js_name = "closureOperationFinalize")]
	pub fn closure_operation_finalize(&self, input: ClosureOperationFinalizeInput) -> napi::Result<ClosureOperationFinalizeOutput> {
		let response_json = self
			.store
			.finalize_closure_operation(
				&input.scope,
				&input.key,
				&input.request_json,
				&input.intent_json,
				&input.operation_json,
				&input.response_json,
				unix_epoch_ms(),
			)
			.map_err(store_napi_error)?;
		Ok(ClosureOperationFinalizeOutput { response_json })
	}

	/// Claims a main admission's durable broker operation intent before the broker send.
	#[napi(js_name = "mainAdmissionOperationClaim")]
	pub fn main_admission_operation_claim(
		&self,
		input: MainAdmissionOperationClaimInput,
	) -> napi::Result<MainAdmissionOperationClaimOutput> {
		match self
			.store
			.claim_main_admission_operation(
				&input.scope,
				&input.key,
				&input.request_json,
				&input.intent_json,
				unix_epoch_ms(),
			)
			.map_err(store_napi_error)?
		{
			MainAdmissionOperationClaim::Claimed => Ok(MainAdmissionOperationClaimOutput { claimed: true, response_json: None }),
			MainAdmissionOperationClaim::Existing { response_json } => {
				Ok(MainAdmissionOperationClaimOutput { claimed: false, response_json: Some(response_json) })
			}
		}
	}

	/// Finalizes a broker-accepted main admission without changing its operation reference.
	#[napi(js_name = "mainAdmissionOperationFinalize")]
	pub fn main_admission_operation_finalize(
		&self,
		input: MainAdmissionOperationFinalizeInput,
	) -> napi::Result<MainAdmissionOperationFinalizeOutput> {
		let response_json = self
			.store
			.finalize_main_admission_operation(
				&input.scope,
				&input.key,
				&input.request_json,
				&input.intent_json,
				&input.response_json,
				unix_epoch_ms(),
			)
			.map_err(store_napi_error)?;
		Ok(MainAdmissionOperationFinalizeOutput { response_json })
	}

	/// Returns unresolved pre-effect admissions for startup recovery only.
	#[napi(js_name = "mainAdmissionOperationsPending")]
	pub fn main_admission_operations_pending(&self) -> napi::Result<Vec<PendingMainAdmissionOperationOutput>> {
		self.store
			.pending_main_admission_operations()
			.map(|operations| operations.into_iter().map(pending_main_admission_operation_output).collect())
			.map_err(store_napi_error)
	}
}

fn open_way_core(state_dir: String, hard_hold_cap_ms: Option<u64>) -> napi::Result<WayCore> {
	if state_dir.trim().is_empty() {
		return Err(napi::Error::from_reason("stateDir must not be empty"));
	}
	let store = Store::open(&state_dir).map_err(store_napi_error)?;
	let locks = match hard_hold_cap_ms {
		Some(hard_hold_cap_ms) => LockManager::with_hard_hold_cap(store.clone(), hard_hold_cap_ms),
		None => LockManager::new(store.clone()),
	};
	locks.reconcile().map_err(lock_napi_error)?;
	let journal = EventJournal::new(store.clone());
	Ok(WayCore { state_dir, store, locks, journal, rpc_server: Mutex::new(None) })
}

fn process_identity_output(pid: i32) -> napi::Result<ProcessIdentityOutput> {
	if pid <= 0 {
		return Err(napi::Error::from_reason("pid must be positive"));
	}
	let probe = SystemProcessProbe;
	let pid_start_time = match probe.process(pid) {
		ProcessObservation::Present { start_time: Some(start_time) } => start_time,
		ProcessObservation::Absent => return Err(napi::Error::from_reason("process is absent")),
		ProcessObservation::Present { start_time: None } | ProcessObservation::Unprovable => {
			return Err(napi::Error::from_reason("process incarnation is unprovable"));
		}
	};
	let pgid = unsafe { libc::getpgid(pid) };
	if pgid <= 0 {
		return Err(napi::Error::from_reason("process group is unavailable"));
	}
	let pgid_start_time = match probe.process_group(pgid) {
		ProcessObservation::Present { start_time } => start_time.map(|value| value.to_string()),
		ProcessObservation::Absent => {
			return Err(napi::Error::from_reason("process group is absent"));
		}
		ProcessObservation::Unprovable => {
			return Err(napi::Error::from_reason("process group incarnation is unprovable"));
		}
	};
	Ok(ProcessIdentityOutput { pid, pid_start_time: pid_start_time.to_string(), pgid, pgid_start_time })
}

fn acquire_request_from_napi(input: LockAcquireInput) -> napi::Result<AcquireRequest> {
	let holder_kind = HolderKind::from_str(&input.holder.holder_kind).map_err(|_| napi::Error::from_reason("holder.holderKind must be in_daemon"))?;
	if holder_kind != HolderKind::InDaemon {
		return Err(napi::Error::from_reason("v1 lockAcquire only accepts the in-daemon closure executor"));
	}
	if input.holder.conn_id.as_deref() != Some(IN_DAEMON_EXECUTOR_CONN_ID) {
		return Err(napi::Error::from_reason("v1 lockAcquire requires the supervised in-daemon executor marker"));
	}
	let class = input
		.class
		.as_deref()
		.unwrap_or("interactive")
		.parse::<LockClass>()
		.map_err(|_| napi::Error::from_reason("class must be interactive or batch"))?;
	let pid_start_time = input
		.holder
		.pid_start_time
		.parse()
		.map_err(|_| napi::Error::from_reason("holder.pidStartTime must be a u64 string"))?;
	let pgid_start_time = input
		.holder
		.pgid_start_time
		.map(|value| value.parse().map_err(|_| napi::Error::from_reason("holder.pgidStartTime must be a u64 string")))
		.transpose()?;
	Ok(AcquireRequest {
		holder: LeaseHolder {
			holder_kind,
			session_id: input.holder.session_id,
			label: input.label,
			pid: input.holder.pid,
			pid_start_time,
			pgid: input.holder.pgid,
			pgid_start_time,
			conn_id: input.holder.conn_id,
		},
		class,
		wait_ms: input.wait_ms.map(u64::from).unwrap_or(lock::DEFAULT_WAIT_MS),
		ttl_ms: input.ttl_ms.map(u64::from).unwrap_or(lock::DEFAULT_TTL_MS),
	})
}

fn lock_acquire_output(result: AcquireResult) -> LockAcquireOutput {
	LockAcquireOutput {
		lease_id: result.lease_id,
		fencing_token: result.fencing_token.to_string(),
		expires_at: result.expires_at as f64,
		queue_waited_ms: result.queue_waited_ms as f64,
	}
}

fn lock_release_output(result: ReleaseResult) -> LockReleaseOutput {
	LockReleaseOutput { released: result.released, held_ms: result.held_ms as f64 }
}

fn lease_output(lease: Lease) -> LeaseOutput {
	LeaseOutput {
		lease_id: lease.lease_id,
		holder_kind: lease.holder.holder_kind.as_sql().to_owned(),
		session_id: lease.holder.session_id,
		label: lease.holder.label,
		pid: lease.holder.pid,
		pid_start_time: lease.holder.pid_start_time.to_string(),
		pgid: lease.holder.pgid,
		pgid_start_time: lease.holder.pgid_start_time.map(|value| value.to_string()),
		conn_id: lease.holder.conn_id,
		class: lease.class.as_sql().to_owned(),
		state: lease.state.as_sql().to_owned(),
		fencing_token: lease.fencing_token.to_string(),
		expires_at: lease.expires_at as f64,
	}
}

fn queue_entry_output(entry: QueueEntry) -> QueueEntryOutput {
	QueueEntryOutput { class: entry.class.as_sql().to_owned(), label: entry.label, waited_ms: entry.waited_ms as f64 }
}

fn lock_status_output(status: LockStatus) -> LockStatusOutput {
	LockStatusOutput {
		held: status.held,
		holder: status.holder.map(lease_output),
		expires_at: status.expires_at.map(|value| value as f64),
		fencing_token: status.fencing_token.map(|value| value.to_string()),
		queue: status.queue.into_iter().map(queue_entry_output).collect(),
		stuck: status.stuck,
		quarantined: status.quarantined,
	}
}

fn pending_main_admission_operation_output(operation: PendingMainAdmissionOperation) -> PendingMainAdmissionOperationOutput {
	PendingMainAdmissionOperationOutput {
		scope: operation.scope,
		key: operation.key,
		request_json: operation.request_json,
		intent_json: operation.intent_json,
	}
}

fn journal_read_output(read: JournalRead) -> JournalReadOutput {
	JournalReadOutput {
		events: read
			.events
			.into_iter()
			.map(|event| JournalEventOutput { seq: event.seq.to_string(), ts: event.ts as f64, kind: event.kind, payload_json: event.payload_json })
			.collect(),
		next_cursor: read.next_cursor.to_string(),
		gap: read.gap.map(journal_gap_output),
	}
}

fn journal_gap_output(gap: JournalGap) -> JournalGapOutput {
	JournalGapOutput {
		missing_from: gap.missing_from.to_string(),
		missing_to: gap.missing_to.to_string(),
		resync_cursor: gap.resync_cursor.to_string(),
	}
}

fn consumer_claim_output(claim: ConsumerClaim) -> ConsumerClaimOutput {
	ConsumerClaimOutput { claim_id: claim.claim_id, cursor: claim.cursor.to_string(), expires_at: claim.expires_at as f64 }
}

fn outbox_row_output(row: OutboxRow) -> OutboxRowOutput {
	OutboxRowOutput {
		consumer_id: row.consumer_id,
		seq: row.seq.to_string(),
		state: row.state,
		platform_msg_id: row.platform_msg_id,
		dedupe_key: row.dedupe_key,
	}
}

fn napi_u64(value: f64) -> napi::Result<u64> {
	if !value.is_finite() || value < 0.0 || value.fract() != 0.0 || value > 9_007_199_254_740_991.0 {
		return Err(napi::Error::from_reason("numeric value must be a non-negative safe integer"));
	}
	Ok(value as u64)
}

fn napi_i64(value: f64) -> napi::Result<i64> {
	let value = napi_u64(value)?;
	i64::try_from(value).map_err(|_| napi::Error::from_reason("numeric value exceeds i64"))
}

fn broker_snapshot_from_napi(input: RegistryApplyBrokerSnapshotInput) -> napi::Result<BrokerSnapshot> {
	let observed_at = napi_i64(input.observed_at)?;
	let rows = input
		.rows
		.into_iter()
		.map(|row| {
			Ok(BrokerSessionRow {
				session_id: row.session_id,
				locator: row.locator,
				endpoint_generation: napi_u64(row.endpoint_generation)?,
				host_incarnation: row.host_incarnation,
				identity_provenance: row.identity_provenance,
				index_seq: napi_u64(row.index_seq)?,
				live: row.live,
				deleted: row.deleted,
				terminal_uncertain: row.terminal_uncertain,
				ambiguous: row.ambiguous,
				activity_state: row.activity_state,
				activity_at: row.activity_at.map(napi_i64).transpose()?,
				last_heartbeat_at: row.last_heartbeat_at.map(napi_i64).transpose()?,
			})
		})
		.collect::<napi::Result<Vec<_>>>()?;
	Ok(BrokerSnapshot { observed_at, rows })
}

fn registry_list_filter_from_napi(input: Option<RegistryListInput>) -> napi::Result<RegistryListFilter> {
	let Some(input) = input else {
		return Ok(RegistryListFilter::default());
	};
	let offset = input.offset.map(napi_u64).transpose()?.unwrap_or(0);
	Ok(RegistryListFilter {
		kind: input.kind,
		status: input.status,
		surface_id: input.surface_id,
		limit: input.limit.unwrap_or(registry::DEFAULT_LIST_LIMIT),
		offset,
	})
}

fn snapshot_apply_output(result: registry::SnapshotApplyResult) -> RegistrySnapshotApplyOutput {
	RegistrySnapshotApplyOutput {
		new_session_ids: result.new_session_ids,
		changed_session_ids: result.changed_session_ids,
		changed_index_seq_session_ids: result.changed_index_seq_session_ids,
		drift_count: result.drift_count as f64,
	}
}

fn registry_row_output(row: registry::RegistryRow) -> RegistryRowOutput {
	RegistryRowOutput {
		session_id: row.session_id,
		kind: row.kind,
		purpose: row.purpose,
		brief: row.brief,
		status: row.status,
		surface_id: row.surface_id,
		locator: row.locator,
		endpoint_generation: row.endpoint_generation.map(|value| value as f64),
		host_incarnation: row.host_incarnation,
		identity_provenance: row.identity_provenance,
		index_seq: row.index_seq.map(|value| value as f64),
		live: row.live,
		deleted: row.deleted,
		terminal_uncertain: row.terminal_uncertain,
		ambiguous: row.ambiguous,
		activity_state: row.activity_state,
		activity_at: row.activity_at.map(|value| value as f64),
		last_heartbeat_at: row.last_heartbeat_at.map(|value| value as f64),
		meta_name: row.meta_name,
		meta_cwd: row.meta_cwd,
		meta_kind: row.meta_kind,
		metadata_state: row.metadata_state,
		metadata_at: row.metadata_at.map(|value| value as f64),
		source: row.source,
		created_at: row.created_at as f64,
		last_seen_at: row.last_seen_at.map(|value| value as f64),
		closed_at: row.closed_at.map(|value| value as f64),
		registry_rev: row.registry_rev as f64,
		quarantined: row.quarantined,
	}
}

fn registry_napi_error(error: registry::RegistryError) -> napi::Error {
	let prefix = error.code().map(|code| format!("{code} ")).unwrap_or_default();
	napi::Error::from_reason(format!("{prefix}{error}"))
}

fn lock_napi_error(error: LockError) -> napi::Error {
	let prefix = error.code().map(|code| format!("{code} ")).unwrap_or_default();
	napi::Error::from_reason(format!("{prefix}{error}"))
}

fn journal_napi_error(error: JournalError) -> napi::Error {
	let prefix = error.code().map(|code| format!("{code} ")).unwrap_or_default();
	napi::Error::from_reason(format!("{prefix}{error}"))
}

fn store_napi_error(error: StoreError) -> napi::Error {
	let prefix = if matches!(error, StoreError::IdempotencyConflict) { "1500 " } else { "" };
	napi::Error::from_reason(format!("{prefix}{error}"))
}

fn validate_meta_key(key: &str) -> napi::Result<()> {
	if key.is_empty() || key.len() > 128 || !key.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')) {
		return Err(napi::Error::from_reason("gateway metadata keys must be 1..=128 ASCII [A-Za-z0-9_.-] characters"));
	}
	Ok(())
}

fn validate_meta_keys(keys: &[String]) -> napi::Result<()> {
	let mut unique = HashSet::new();
	for key in keys {
		validate_meta_key(key)?;
		if !unique.insert(key) {
			return Err(napi::Error::from_reason("gateway metadata keys must be unique"));
		}
	}
	Ok(())
}

fn validate_gateway_meta_transaction(input: &GatewayMetaTransactionInput) -> napi::Result<()> {
	let expected_keys = input.expected.iter().map(|entry| entry.key.clone()).collect::<Vec<_>>();
	validate_meta_keys(&expected_keys)?;
	let put_keys = input.puts.iter().map(|entry| entry.key.clone()).collect::<Vec<_>>();
	validate_meta_keys(&put_keys)?;
	validate_meta_keys(&input.deletes)?;
	let put_key_set = put_keys.into_iter().collect::<HashSet<_>>();
	if input.deletes.iter().any(|key| put_key_set.contains(key)) {
		return Err(napi::Error::from_reason("a gateway metadata key cannot be both written and deleted"));
	}
	for entry in &input.puts {
		if entry.value.len() > 4 * 1024 * 1024 {
			return Err(napi::Error::from_reason("gateway metadata values must not exceed 4 MiB"));
		}
	}
	match (&input.event_kind, &input.event_payload_json) {
		(Some(_), Some(payload)) if payload.len() <= 4 * 1024 * 1024 => Ok(()),
		(Some(_), Some(_)) => Err(napi::Error::from_reason("journal event payload must not exceed 4 MiB")),
		(None, None) => Ok(()),
		_ => Err(napi::Error::from_reason("eventKind and eventPayloadJson must be provided together")),
	}
}
