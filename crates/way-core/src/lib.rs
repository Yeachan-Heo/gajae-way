//! N-API surface for the gajae-way durable runtime.
//!
//! Rust owns SQLite state, fenced lock ownership, journal settlement,
//! idempotency, and the authenticated P2 UDS RPC boundary.

use std::{collections::HashSet, path::PathBuf, str::FromStr, sync::{Mutex, OnceLock}, time::SystemTime};


use napi::threadsafe_function::ThreadsafeFunction;
use napi_derive::napi;

use crate::{
	events::{
		append_in_transaction, ConsumerClaim, Cursor, DeliveryProof, EventJournal, JournalError, JournalGap,
		JournalRead, OutboxRow,
	},
	lock::{
		AcquireRequest, AcquireResult, HolderKind, Lease, LeaseHolder, LockClass, LockError, LockManager, LockStatus,
		QueueEntry, ReleaseResult,
	},
	store::{meta_get_tx, meta_set_tx, unix_epoch_ms, Store, StoreError},
};
use crate::rpc::{
	dispatch::{BridgeRequest, GatewayState, RpcBridgeStats, RpcDispatcher, TsfnBridge},
	RpcServerHandle,
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

/// Returns the process-local boot identity used by the P0 health probe.
#[napi(js_name = "healthInfo")]
pub fn health_info() -> HealthInfo {
	let boot_epoch = *BOOT_EPOCH.get_or_init(|| {
		SystemTime::now()
			.duration_since(SystemTime::UNIX_EPOCH)
			.expect("system time must not precede the Unix epoch")
			.as_secs()
			.try_into()
			.expect("P0 boot epoch fits in u32")
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
	/// `integrity_check` before exposing any mutation APIs.
	#[napi(factory)]
	pub fn open(state_dir: String) -> napi::Result<Self> {
		if state_dir.trim().is_empty() {
			return Err(napi::Error::from_reason("stateDir must not be empty"));
		}
		let store = Store::open(&state_dir).map_err(store_napi_error)?;
		let locks = LockManager::new(store.clone());
		let journal = EventJournal::new(store.clone());
		Ok(Self { state_dir, store, locks, journal, rpc_server: Mutex::new(None) })
	}

	#[napi(getter, js_name = "stateDir")]
	pub fn state_dir(&self) -> String {
		self.state_dir.clone()
	}

	/// Starts the hardened state-directory UDS server and registers the TSFN
	/// bridge callback before accepting any client request.
	#[napi(js_name = "startRpcServer", ts_args_type = "socketPath: string, bridgeCallback: (err: null | Error, request: BridgeRequest) => void")]
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
			_ => return Err(napi::Error::from_reason("state must be booting, verifying, running, failed_closed, or degraded")),
		};
		let server = self.rpc_server.lock().map_err(|_| napi::Error::from_reason("RPC server lock was poisoned"))?;
		let server = server.as_ref().ok_or_else(|| napi::Error::from_reason("RPC server is not running"))?;
		server.set_gateway_state(state, reason);
		Ok(())
	}

	/// Publishes the live main-session admission state used by `way.status`.
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
			transaction.execute("DELETE FROM gateway_meta WHERE k = ?1", [key]).map_err(|error| store_napi_error(error.into()))?;

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
		self.locks
			.force_release(&lease_id, confirm)
			.map(lock_release_output)
			.map_err(lock_napi_error)
	}

	#[napi(js_name = "lockQuarantineOverride")]
	pub fn lock_quarantine_override(
		&self,
		lease_id: String,
		confirm: bool,
		acknowledge_unverified: bool,
	) -> napi::Result<LockStatusOutput> {
		self.locks
			.quarantine_override(&lease_id, confirm, acknowledge_unverified)
			.map(lock_status_output)
			.map_err(lock_napi_error)
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
		self.journal
			.read(cursor, limit.unwrap_or(100))
			.map(journal_read_output)
			.map_err(journal_napi_error)
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
			.store_idempotency_response(
				&input.scope,
				&input.key,
				&input.request_json,
				&input.response_json,
				unix_epoch_ms(),
			)
			.map_err(store_napi_error)
	}
}

fn acquire_request_from_napi(input: LockAcquireInput) -> napi::Result<AcquireRequest> {
	let holder_kind = HolderKind::from_str(&input.holder.holder_kind)
		.map_err(|_| napi::Error::from_reason("holder.holderKind must be in_daemon or external"))?;
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

fn journal_read_output(read: JournalRead) -> JournalReadOutput {
	JournalReadOutput {
		events: read
			.events
			.into_iter()
			.map(|event| JournalEventOutput {
				seq: event.seq.to_string(),
				ts: event.ts as f64,
				kind: event.kind,
				payload_json: event.payload_json,
			})
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
	if key.is_empty()
		|| key.len() > 128
		|| !key.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
	{
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
