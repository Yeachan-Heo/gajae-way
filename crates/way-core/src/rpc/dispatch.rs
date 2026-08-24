//! JSON-RPC method dispatch, application-error mapping, and the fallible TSFN bridge.

use std::{
	collections::{HashMap, VecDeque},
	fmt,
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
	time::{Duration, Instant},
};

use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use serde_json::{Map, Value, json};
use tokio::{sync::oneshot, time::Instant as TokioInstant};

use crate::{
	events::{ConsumerClaim, Cursor, DeliveryProof, EventJournal, JournalError, JournalGap, JournalRead},
	lock::{Lease, LockError, LockManager, LockStatus, QuarantineReceiptEvidence, ReleaseResult},
	registry::{self, RegistryAnnotation, RegistryError, RegistryListFilter},
	store::{Store, StoreError, unix_epoch_ms},
};

use super::CancellationToken;

pub const BRIDGE_TIMEOUT_MS: u64 = 30_000;
pub const MAX_BRIDGE_IN_FLIGHT: usize = 64;

pub const MAIN_EVENT_KINDS: &[&str] = &[
	"assistant_message",
	"turn_start",
	"turn_end",
	"tail_ring_rotation",
	"transcript_delivery_gap",
	"gate_open",
	"gate_resolved",
	"health_change",
	"registry_change",
	"lock_event",
	"follow_up_attempted",
	"follow_up_confirmed",
	"profile_approved",
	"main_identity_growth_absorbed",
	"failed_closed_recovered",
	"schedule_run",
	"alert_raised",
	"alert_cleared",
	"memory_index_rebuilt",
];

/// Aggregate scheduler counters for `way.status`.
///
/// Deliberately counters and states only: no payload text, no surface ids, and
/// no operator-authored content, so this projection stays safe to expose beside
/// the rest of the status document.
fn schedule_status_json(store: &crate::store::Store) -> Result<serde_json::Value, RpcError> {
	let connection = store.connection().map_err(store_error)?;
	let counts = |state: &str| -> Result<i64, RpcError> {
		connection
			.query_row(
				"SELECT COUNT(*) FROM schedule_jobs WHERE state = ?1",
				rusqlite::params![state],
				|row| row.get::<_, i64>(0),
			)
			.map_err(|error| store_error(crate::store::StoreError::Sql(error)))
	};
	let active = counts("active")?;
	let backoff = counts("backoff")?;
	let suspended = counts("suspended")?;
	let next_fire_at_ms = connection
		.query_row(
			"SELECT MIN(next_fire_at_ms) FROM schedule_jobs WHERE state IN ('active','backoff') AND next_fire_at_ms IS NOT NULL",
			[],
			|row| row.get::<_, Option<i64>>(0),
		)
		.map_err(|error| store_error(crate::store::StoreError::Sql(error)))?;
	let in_flight = connection
		.query_row("SELECT COUNT(*) FROM schedule_runs WHERE outcome IS NULL", [], |row| row.get::<_, i64>(0))
		.map_err(|error| store_error(crate::store::StoreError::Sql(error)))?;
	Ok(json!({
		"active_jobs": active,
		"backoff_jobs": backoff,
		"suspended_jobs": suspended,
		"in_flight_runs": in_flight,
		"next_fire_at_ms": next_fire_at_ms,
	}))
}

static NEXT_CORRELATION_ID: AtomicU64 = AtomicU64::new(1);

/// The complete application error space reserved by the wire contract.
pub const APP_ERROR_CODES: &[(i64, &str)] = &[
	(1000, "unhealthy_failed_closed"),
	(1003, "transcript_proof_pending"),
	(1100, "gate_not_found"),
	(1101, "gate_expired"),
	(1102, "gate_session_mismatch"),
	(1200, "lock_held"),
	(1201, "lock_wait_timeout"),
	(1202, "lease_expired"),
	(1203, "not_lock_holder"),
	(1204, "lock_reentrant_denied"),
	(1205, "lock_stuck"),
	(1206, "lock_dirty_index"),
	(1207, "lock_holder_unverified"),
	(1208, "corpus_quarantined"),
	(1300, "unknown_surface"),
	(1301, "unknown_session"),
	(1302, "session_quarantined"),
	(1500, "idempotency_conflict"),
	(1502, "payload_too_large"),
	(1600, "cursor_before_retention"),
	(1601, "consumer_claim_held"),
	(1700, "schedule_not_found"),
	(1701, "schedule_invalid_spec"),
	(1702, "schedule_refused"),
	(1801, "memory_index_stale"),
	(1900, "unauthorized"),
];

pub fn app_error_name(code: i64) -> Option<&'static str> {
	APP_ERROR_CODES.iter().find_map(|(candidate, name)| (*candidate == code).then_some(*name))
}

/// A wire-safe JSON-RPC error. Every internal error constructor emits a logged
/// correlation identifier.

#[derive(Debug, Clone, PartialEq)]
pub struct RpcError {
	pub code: i64,
	pub message: String,
	pub data: Option<Value>,
}

#[derive(Debug)]
struct BridgeDiagnostic {
	error_type: String,
	message: String,
	stack: Option<String>,
}

impl RpcError {
	pub fn new(code: i64, message: impl Into<String>) -> Self {
		Self { code, message: message.into(), data: None }
	}

	pub fn with_data(code: i64, message: impl Into<String>, data: Value) -> Self {
		Self { code, message: message.into(), data: Some(data) }
	}

	pub fn invalid_request(message: impl Into<String>) -> Self {
		Self::new(-32600, message)
	}

	pub fn method_not_found(method: &str) -> Self {
		Self::new(-32601, format!("method not found: {method}"))
	}

	pub fn invalid_params(message: impl Into<String>) -> Self {
		Self::new(-32602, message)
	}

	pub fn internal(message: impl Into<String>) -> Self {
		let message = message.into();
		let correlation_id = format!("rpc-{}", NEXT_CORRELATION_ID.fetch_add(1, Ordering::Relaxed));
		eprintln!("way-core rpc internal_error correlation_id={correlation_id} message={message}");
		Self::with_data(-32603, message, json!({ "correlation_id": correlation_id }))
	}

	fn bridge_exception(reason: String, diagnostic: Option<BridgeDiagnostic>) -> Self {
		let correlation_id = format!("rpc-{}", NEXT_CORRELATION_ID.fetch_add(1, Ordering::Relaxed));
		match diagnostic {
			Some(diagnostic) => {
				let error_type = bounded_bridge_log_value(&diagnostic.error_type);
				let message = bounded_bridge_log_value(&diagnostic.message);
				if let Some(stack) = diagnostic.stack {
					let stack = bounded_bridge_log_value(&stack);
					eprintln!(
						"way-core rpc bridge_exception correlation_id={correlation_id} reason={reason} error_type={error_type:?} message={message:?} stack={stack:?}"
					);
				} else {
					eprintln!(
						"way-core rpc bridge_exception correlation_id={correlation_id} reason={reason} error_type={error_type:?} message={message:?}"
					);
				}
			}
			None => eprintln!(
				"way-core rpc bridge_exception correlation_id={correlation_id} reason={reason} error_type=\"unknown\" message=\"bridge exception\""
			),
		}
		Self::with_data(-32603, "bridge_exception", json!({ "correlation_id": correlation_id, "reason": reason }))
	}

	pub fn app(code: i64, data: Option<Value>) -> Self {
		let message = app_error_name(code).unwrap_or("application_error").to_owned();
		Self { code, message, data }
	}

	pub fn as_json(&self) -> Value {
		let mut error = Map::new();
		error.insert("code".to_owned(), Value::from(self.code));
		error.insert("message".to_owned(), Value::from(self.message.clone()));
		if let Some(data) = &self.data {
			error.insert("data".to_owned(), data.clone());
		}
		Value::Object(error)
	}
}

/// Request data delivered from Rust's TSFN into the thin TypeScript shim.
#[napi(object)]
pub struct BridgeRequest {
	#[napi(js_name = "reqId")]
	pub req_id: i64,
	pub method: String,
	#[napi(js_name = "paramsJson")]
	pub params_json: String,
}

#[napi(object)]
pub struct RpcBridgeStats {
	#[napi(js_name = "inFlight")]
	pub in_flight: u32,
	pub overloads: i64,
	pub timeouts: i64,
	#[napi(js_name = "duplicateCompletions")]
	pub duplicate_completions: i64,
	#[napi(js_name = "lateCompletions")]
	pub late_completions: i64,
	#[napi(js_name = "queueClosed")]
	pub queue_closed: i64,
}

#[derive(Debug)]
enum BridgeCompletion {
	Success(Value),
	RemoteError(Value),
	ShuttingDown,
}

#[derive(Debug)]
enum BridgeFailure {
	Overloaded,
	ShuttingDown,
	Timeout,
	Cancelled,
	Remote(Value),
	CallFailed(napi::Status),
}

struct PendingBridgeCall {
	deadline: TokioInstant,
	responder: oneshot::Sender<BridgeCompletion>,
}

struct BridgeState {
	next_request_id: AtomicU64,
	closed: AtomicBool,
	pending: Mutex<HashMap<u64, PendingBridgeCall>>,
	late_request_ids: Mutex<VecDeque<u64>>,
	overloads: AtomicU64,
	timeouts: AtomicU64,
	duplicate_completions: AtomicU64,
	late_completions: AtomicU64,
	queue_closed: AtomicU64,
}

impl Default for BridgeState {
	fn default() -> Self {
		Self {
			next_request_id: AtomicU64::new(1),
			closed: AtomicBool::new(false),
			pending: Mutex::new(HashMap::new()),
			late_request_ids: Mutex::new(VecDeque::new()),
			overloads: AtomicU64::new(0),
			timeouts: AtomicU64::new(0),
			duplicate_completions: AtomicU64::new(0),
			late_completions: AtomicU64::new(0),
			queue_closed: AtomicU64::new(0),
		}
	}
}

/// Correlated, bounded bridge into TypeScript.
#[derive(Clone)]
pub struct TsfnBridge {
	callback: Arc<Mutex<Option<ThreadsafeFunction<BridgeRequest>>>>,
	state: Arc<BridgeState>,
}

impl fmt::Debug for TsfnBridge {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.debug_struct("TsfnBridge").field("stats", &self.stats().in_flight).finish_non_exhaustive()
	}
}

impl TsfnBridge {
	pub fn new(callback: ThreadsafeFunction<BridgeRequest>) -> Self {
		Self { callback: Arc::new(Mutex::new(Some(callback))), state: Arc::new(BridgeState::default()) }
	}

	/// Builds a callback-free bridge for pure Rust dispatch tests.
	#[cfg(test)]
	pub fn for_tests() -> Self {
		Self { callback: Arc::new(Mutex::new(None)), state: Arc::new(BridgeState::default()) }
	}

	pub fn stats(&self) -> RpcBridgeStats {
		let in_flight = self.state.pending.lock().map(|pending| pending.len() as u32).unwrap_or(u32::MAX);
		RpcBridgeStats {
			in_flight,
			overloads: saturating_i64(self.state.overloads.load(Ordering::Relaxed)),
			timeouts: saturating_i64(self.state.timeouts.load(Ordering::Relaxed)),
			duplicate_completions: saturating_i64(self.state.duplicate_completions.load(Ordering::Relaxed)),
			late_completions: saturating_i64(self.state.late_completions.load(Ordering::Relaxed)),
			queue_closed: saturating_i64(self.state.queue_closed.load(Ordering::Relaxed)),
		}
	}

	fn reserve(&self) -> Result<(u64, oneshot::Receiver<BridgeCompletion>, TokioInstant), BridgeFailure> {
		if self.state.closed.load(Ordering::Acquire) {
			return Err(BridgeFailure::ShuttingDown);
		}
		let mut pending = self.state.pending.lock().map_err(|_| BridgeFailure::ShuttingDown)?;
		if self.state.closed.load(Ordering::Acquire) {
			return Err(BridgeFailure::ShuttingDown);
		}
		if pending.len() >= MAX_BRIDGE_IN_FLIGHT {
			self.state.overloads.fetch_add(1, Ordering::Relaxed);
			return Err(BridgeFailure::Overloaded);
		}
		let request_id = self.state.next_request_id.fetch_add(1, Ordering::Relaxed);
		let (responder, receiver) = oneshot::channel();
		let pending_call = PendingBridgeCall { deadline: TokioInstant::now() + std::time::Duration::from_millis(BRIDGE_TIMEOUT_MS), responder };
		let deadline = pending_call.deadline;
		pending.insert(request_id, pending_call);
		Ok((request_id, receiver, deadline))
	}

	fn remove_pending(&self, request_id: u64) -> Option<PendingBridgeCall> {
		self.state.pending.lock().ok()?.remove(&request_id)
	}

	fn mark_late(&self, request_id: u64) {
		let Ok(mut requests) = self.state.late_request_ids.lock() else {
			return;
		};
		if requests.contains(&request_id) {
			return;
		}
		requests.push_back(request_id);
		if requests.len() > 256 {
			requests.pop_front();
		}
	}

	fn is_late(&self, request_id: u64) -> bool {
		self.state.late_request_ids.lock().is_ok_and(|requests| requests.contains(&request_id))
	}

	async fn call(&self, method: String, params: Value, cancellation: CancellationToken) -> Result<Value, BridgeFailure> {
		let (request_id, receiver, deadline) = self.reserve()?;
		let params_json = serde_json::to_string(&params).map_err(|_| BridgeFailure::Remote(json!({ "message": "could not serialize bridge parameters" })))?;
		let request = BridgeRequest { req_id: request_id.try_into().expect("bridge request ids fit i64"), method, params_json };
		let status = match self.callback.lock() {
			Ok(callback) => callback
				.as_ref()
				.map(|callback| callback.call(Ok(request), ThreadsafeFunctionCallMode::NonBlocking)),
			Err(_) => None,
		};
		match status {
			Some(napi::Status::Ok) => self.await_completion(request_id, deadline, receiver, cancellation).await,
			Some(napi::Status::QueueFull) => {
				if self.remove_pending(request_id).is_some() {
					self.mark_late(request_id);
				}
				self.state.overloads.fetch_add(1, Ordering::Relaxed);
				Err(BridgeFailure::Overloaded)
			}
			Some(napi::Status::Closing) | None => {
				if self.remove_pending(request_id).is_some() {
					self.mark_late(request_id);
				}
				self.state.queue_closed.fetch_add(1, Ordering::Relaxed);
				Err(BridgeFailure::ShuttingDown)
			}
			Some(status) => {
				if self.remove_pending(request_id).is_some() {
					self.mark_late(request_id);
				}
				Err(BridgeFailure::CallFailed(status))
			}
		}
	}

	async fn await_completion(
		&self,
		request_id: u64,
		deadline: TokioInstant,
		receiver: oneshot::Receiver<BridgeCompletion>,
		cancellation: CancellationToken,
	) -> Result<Value, BridgeFailure> {
		tokio::select! {
			completion = receiver => match completion {
				Ok(BridgeCompletion::Success(result)) => Ok(result),
				Ok(BridgeCompletion::RemoteError(error)) => Err(BridgeFailure::Remote(error)),
				Ok(BridgeCompletion::ShuttingDown) | Err(_) => Err(BridgeFailure::ShuttingDown),
			},
			_ = tokio::time::sleep_until(deadline) => {
				if self.remove_pending(request_id).is_some() {
					self.mark_late(request_id);
					self.state.timeouts.fetch_add(1, Ordering::Relaxed);
				}
				Err(BridgeFailure::Timeout)
			},
			_ = cancellation.cancelled() => {
				if self.remove_pending(request_id).is_some() {
					self.mark_late(request_id);
				}
				Err(BridgeFailure::Cancelled)
			},
		}
	}

	/// Completes a request at most once. Late and duplicate completions are
	/// deliberately observable but never overwrite the first result.
	pub fn complete_json(&self, request_id: u64, result_json: &str) -> bool {
		let completion = match serde_json::from_str::<Value>(result_json) {
			Ok(Value::Object(mut object)) if object.contains_key("error") => BridgeCompletion::RemoteError(object.remove("error").unwrap_or(Value::Null)),
			Ok(result) => BridgeCompletion::Success(result),
			Err(_) => BridgeCompletion::RemoteError(json!({
				"code": -32603,
				"message": "bridge returned invalid JSON",
			})),
		};
		let Some(pending) = self.remove_pending(request_id) else {
			if self.state.closed.load(Ordering::Acquire) || self.is_late(request_id) {
				self.state.late_completions.fetch_add(1, Ordering::Relaxed);
			} else {
				self.state.duplicate_completions.fetch_add(1, Ordering::Relaxed);
			}
			return false;
		};
		if pending.responder.send(completion).is_err() {
			self.mark_late(request_id);
			self.state.late_completions.fetch_add(1, Ordering::Relaxed);
			return false;
		}
		true
	}

	pub fn shutdown(&self) {
		if self.state.closed.swap(true, Ordering::AcqRel) {
			return;
		}
		if let Ok(mut callback) = self.callback.lock() {
			callback.take();
		}
		if let Ok(mut pending) = self.state.pending.lock() {
			for (request_id, call) in pending.drain() {
				self.mark_late(request_id);
				let _ = call.responder.send(BridgeCompletion::ShuttingDown);
			}
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayState {
	Booting,
	Verifying,
	Running,
	FailedClosed,
	Degraded,
}

impl GatewayState {
	fn as_str(self) -> &'static str {
		match self {
			Self::Booting => "booting",
			Self::Verifying => "verifying",
			Self::Running => "running",
			Self::FailedClosed => "failed_closed",
			Self::Degraded => "degraded",
		}
	}

	fn health_status(self) -> &'static str {
		match self {
			Self::Booting | Self::Verifying => "booting",
			Self::Running => "healthy",
			Self::FailedClosed | Self::Degraded => "unhealthy",
		}
	}
}

fn is_machine_reason(value: &str) -> bool {
	!value.is_empty()
		&& value.len() <= 64
		&& value.bytes().enumerate().all(|(index, byte)| {
			if index == 0 {
				byte.is_ascii_lowercase()
			} else {
				byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'
			}
		})
}

fn durable_failed_closed_reason(store: &Store) -> Result<Option<String>, RpcError> {
	let bootstrap_state = store.get_meta("bootstrap_state").map_err(store_error)?;
	let raw_reason = store.get_meta("failed_closed_reason").map_err(store_error)?;
	let recorded_failure = bootstrap_state.as_deref() == Some("FAILED_CLOSED")
		|| raw_reason.as_deref().is_some_and(|value| value != "null");
	if !recorded_failure {
		return Ok(None);
	}
	let reason = raw_reason
		.as_deref()
		.and_then(|value| serde_json::from_str::<Value>(value).ok())
		.and_then(|value| value.as_str().map(str::to_owned))
		.filter(|value| is_machine_reason(value))
		.unwrap_or_else(|| "failed_closed".to_owned());
	Ok(Some(reason))
}

fn durable_transcript_proof(store: &Store) -> Result<Option<String>, RpcError> {
	let bootstrap_state = store.get_meta("bootstrap_state").map_err(store_error)?;
	if !matches!(bootstrap_state.as_deref(), Some("COMMITTED") | Some("FAILED_CLOSED")) {
		return Ok(None);
	}
	match store.get_meta("transcript_proof").map_err(store_error)?.as_deref() {
		Some("pending") => Ok(Some("pending".to_owned())),
		Some("proven") => Ok(Some("proven".to_owned())),
		_ => Ok(None),
	}
}

fn durable_tail_ring_rotation_count(store: &Store) -> Result<u64, RpcError> {
	match store.get_meta("tail_ring_rotation_count").map_err(store_error)? {
		Some(raw) => raw
			.parse::<u64>()
			.map_err(|_| RpcError::internal("tail_ring_rotation_count metadata is invalid")),
		None => Ok(0),
	}
}

fn durable_transcript_delivery_gap_count(store: &Store) -> Result<u64, RpcError> {
	match store.get_meta("transcript_delivery_gap_count").map_err(store_error)? {
		Some(raw) => raw
			.parse::<u64>()
			.map_err(|_| RpcError::internal("transcript_delivery_gap_count metadata is invalid")),
		None => Ok(0),
	}
}

fn main_admission_transcript_proof_pending(store: &Store) -> Result<bool, RpcError> {
	if store.get_meta("bootstrap_state").map_err(store_error)?.as_deref() != Some("COMMITTED") {
		return Ok(false);
	}
	Ok(store.get_meta("transcript_proof").map_err(store_error)?.as_deref() != Some("proven"))
}

fn main_session_mutation_requires_transcript_proof(method: &str) -> bool {
	matches!(method, "main.submit" | "main.corpus.close")
}

#[derive(Debug, Clone)]
struct GatewayHealth {
	state: GatewayState,
	reason: Option<String>,
}

#[derive(Debug, Clone)]
struct MainSessionStatus {
	resumed: bool,
	turn_state: String,
	follow_up_queue_depth: u64,
	transcript_verification: String,
	journal_degraded: bool,
}

impl Default for MainSessionStatus {
	fn default() -> Self {
		Self {
			resumed: false,
			turn_state: "idle".to_owned(),
			follow_up_queue_depth: 0,
			transcript_verification: "unavailable".to_owned(),
			journal_degraded: false,
		}
	}
}

/// State and method table used by each UDS connection.
#[derive(Clone)]
pub struct RpcDispatcher {
	store: Store,
	locks: LockManager,
	journal: EventJournal,
	bridge: TsfnBridge,
	health: Arc<Mutex<GatewayHealth>>,
	main_session: Arc<Mutex<MainSessionStatus>>,
	started_at: Arc<Instant>,
}

impl fmt::Debug for RpcDispatcher {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.debug_struct("RpcDispatcher").finish_non_exhaustive()
	}
}

impl RpcDispatcher {
	pub fn new(store: Store, locks: LockManager, journal: EventJournal, bridge: TsfnBridge) -> Self {
		Self {
			store,
			locks,
			journal,
			bridge,
			health: Arc::new(Mutex::new(GatewayHealth { state: GatewayState::Running, reason: None })),
			main_session: Arc::new(Mutex::new(MainSessionStatus::default())),
			started_at: Arc::new(Instant::now()),
		}
	}

	pub fn bridge(&self) -> TsfnBridge {
		self.bridge.clone()
	}

	pub fn set_gateway_state(&self, state: GatewayState, reason: Option<String>) {
		if let Ok(mut health) = self.health.lock() {
			// A failed tail can race daemon startup. A later startup publication must
			// never turn an already-degraded serving process healthy again.
			if health.state == GatewayState::Degraded && state == GatewayState::Running {
				return;
			}
			health.state = state;
			health.reason = reason;
		}
	}

	fn reconciled_health(&self) -> Result<GatewayHealth, RpcError> {
		let failed_closed_reason = durable_failed_closed_reason(&self.store)?;
		let mut health = self.health.lock().map_err(|_| RpcError::internal("gateway health lock poisoned"))?;
		if let Some(reason) = failed_closed_reason {
			health.state = GatewayState::FailedClosed;
			health.reason = Some(reason);
		}
		Ok(health.clone())
	}

	/// Publishes runtime main-session state only after strict resume has opened
	/// the durable session for this daemon process.
	pub fn set_main_session_status(&self, turn_state: String, follow_up_queue_depth: u64, transcript_verification: String) {
		if let Ok(mut status) = self.main_session.lock() {
			status.resumed = true;
			status.turn_state = turn_state;
			status.follow_up_queue_depth = follow_up_queue_depth;
			status.transcript_verification = transcript_verification;
		}
	}

	/// Clears live main-session facts when the host is disposed or the daemon
	/// enters failed-closed mode. Durable bootstrap state is intentionally kept.
	pub fn reset_main_session_status(&self) {
		if let Ok(mut status) = self.main_session.lock() {
			status.resumed = false;
			status.turn_state = "idle".to_owned();
			status.follow_up_queue_depth = 0;
			status.transcript_verification = "unavailable".to_owned();
		}
	}

	pub fn set_journal_degraded(&self, degraded: bool) {
		if let Ok(mut status) = self.main_session.lock() {
			status.journal_degraded = degraded;
		}
	}

	pub async fn dispatch(&self, method: String, params: Value, cancellation: CancellationToken) -> Result<Value, RpcError> {
		if cancellation.is_cancelled() {
			return Err(RpcError::internal("request cancelled"));
		}
		let health = self.reconciled_health()?;
		if health.state == GatewayState::FailedClosed && !matches!(method.as_str(), "way.health" | "way.status" | "way.metrics" | "profile.approve" | "main.events.read" | "main.gate.answer") {
			return Err(RpcError::app(1000, health.reason.map(|reason| json!({ "reason": reason }))));
		}
		if main_session_mutation_requires_transcript_proof(&method) && main_admission_transcript_proof_pending(&self.store)? {
			return Err(RpcError::app(1003, None));
		}

		match method.as_str() {
			"way.health" => self.health_response(params),
			"way.status" => self.status_response(params).await,
			"way.metrics" => self.metrics_response(params).await,
			"memory.search" => self.memory_search_response(params).await,
			"gitlock.acquire" => Err(RpcError::invalid_params(
				"gitlock.acquire is reserved for the daemon's supervised in-daemon closure executor in v1",
			)),
			"gitlock.renew" => self.idempotent(&method, &params, || self.lock_renew(params.clone())).await,
			"gitlock.release" => self.idempotent(&method, &params, || self.lock_release(params.clone())).await,
			"gitlock.status" => self.lock_status(params).await,
			"gitlock.force_release" => self.idempotent(&method, &params, || self.lock_force_release(params.clone())).await,
			"gitlock.quarantine_override" => self.idempotent(&method, &params, || self.lock_quarantine_override(params.clone())).await,
			"gitlock.clear_quarantine" => self.idempotent(&method, &params, || self.lock_clear_quarantine(params.clone())).await,
			"gitlock.record_quarantine_receipt" => self.idempotent(&method, &params, || self.lock_record_quarantine_receipt(params.clone())).await,
			"registry.list" => self.registry_list(params).await,
			"registry.get" => self.registry_get(params).await,
			"registry.annotate" => self.idempotent(&method, &params, || self.registry_annotate(params.clone())).await,
			"surface.resolve" => self.surface_resolve(params).await,
			"main.events.read" => self.main_events_read(params, cancellation).await,
			"consumer.claim" => self.consumer_claim(params).await,
			"consumer.commit" => self.consumer_commit(params).await,
			method if method.starts_with("main.") || method.starts_with("schedule.") || method == "profile.approve" => self.bridge_method(method, params, cancellation).await,

			_ => Err(RpcError::method_not_found(&method)),
		}
	}

	fn health_response(&self, params: Value) -> Result<Value, RpcError> {
		ensure_empty_params(&params)?;
		let health = self.reconciled_health()?;
		let main_session = self
			.main_session
			.lock()
			.map_err(|_| RpcError::internal("main session status lock poisoned"))?
			.clone();
		let session_id = if main_session.resumed { Some(durable_main_session_id(&self.store)?) } else { None };
		let generation = self.store.journal_generation().map_err(store_error)?;
		let mut response = json!({
			"status": health.state.health_status(),
			"state": health.state.as_str(),
			"main": { "resumed": main_session.resumed, "session_id": session_id },
			"boot_epoch": crate::health_info().boot_epoch,
			"journal_generation": generation,
			"version": env!("CARGO_PKG_VERSION"),
			"uptime_ms": self.started_at.elapsed().as_millis() as u64,
		});
		if let Some(reason) = health.reason {
			response
				.as_object_mut()
				.expect("health response is an object")
				.insert("reason".to_owned(), Value::String(reason));
		}
		Ok(response)
	}

	/// Scrapeable counters projected from state the daemon already tracks.
	///
	/// Deliberately counters, ages, and booleans only. Holder identity, session
	/// ids, lease ids, and free-text reasons are excluded so this projection is
	/// safe to expose over the unauthenticated loopback HTTP endpoint; the
	/// authenticated UDS path may still read the richer `way.status`.
	/// Corpus recall, deliberately NOT in the fail-closed allowlist.
	///
	/// `profile_drift` is precisely the state in which the digest-bound policy
	/// authorizing this read is unverified, so answering would apply an
	/// unverified redaction mask.
	async fn memory_search_response(&self, params: Value) -> Result<Value, RpcError> {
		let object = params.as_object().ok_or_else(|| RpcError::invalid_params("memory.search params must be an object"))?;
		for key in object.keys() {
			if !matches!(key.as_str(), "query" | "session_kind" | "limit") {
				return Err(RpcError::invalid_params(&format!("unsupported memory.search field: {key}")));
			}
		}
		let query = object
			.get("query")
			.and_then(Value::as_str)
			.filter(|value| !value.trim().is_empty())
			.ok_or_else(|| RpcError::invalid_params("memory.search requires a non-empty query"))?;
		let session_kind = object.get("session_kind").and_then(Value::as_str).unwrap_or("unknown");
		let limit = object.get("limit").and_then(Value::as_i64).unwrap_or(10).clamp(1, 50);

		let digest = self.store.get_meta("profile_digest").map_err(store_error)?.unwrap_or_default();
		// The mask is derived from the digest-bound projection by the SAME
		// predicate the injector uses, stored per session kind at index build.
		let mask_key = format!("memory_mask_{session_kind}");
		let mask = match self.store.get_meta(&mask_key).map_err(store_error)? {
			Some(raw) => raw.trim().parse::<u64>().unwrap_or(u64::MAX),
			// An unbound, unresolvable, or absent class falls back to the most
			// restrictive posture rather than the most permissive.
			None => self
				.store
				.get_meta("memory_mask_unknown")
				.map_err(store_error)?
				.and_then(|raw| raw.trim().parse::<u64>().ok())
				.unwrap_or(u64::MAX),
		};

		match crate::memory::search(&self.store, &digest, query, mask, limit) {
			Ok(hits) => Ok(json!({
				// `hits.length` is the ONLY observable cardinality: no total,
				// matched, or score_sum, because any of those would disclose the
				// existence of redacted documents.
				"hits": hits
					.into_iter()
					.map(|hit| json!({ "path": hit.path, "snippet": hit.snippet, "rank": hit.rank }))
					.collect::<Vec<_>>(),
			})),
			Err(crate::memory::MemoryError::IndexStale { .. }) => Err(RpcError::app(1801, None)),
			Err(error) => Err(RpcError::internal(&error.to_string())),
		}
	}

	async fn metrics_response(&self, _params: Value) -> Result<Value, RpcError> {
		let health = self.reconciled_health()?;
		let main_session = self
			.main_session
			.lock()
			.map_err(|_| RpcError::internal("main session status lock poisoned"))?
			.clone();
		// Reuses the same durable projection the HTTP endpoint serves rather than
		// duplicating its queries, then adds the in-memory session fields that
		// only this authenticated path exposes.
		let mut projection = crate::metrics::durable_projection(&self.store, &self.locks, crate::store::unix_epoch_ms())
			.map_err(store_error)?;
		let object = projection
			.as_object_mut()
			.ok_or_else(|| RpcError::internal("durable metrics projection is not an object"))?;
		object.insert("gajaeway_state".to_owned(), json!(health.state.as_str()));
		object.insert("gajaeway_journal_degraded".to_owned(), json!(main_session.journal_degraded));
		object.insert("gajaeway_turn_busy".to_owned(), json!(main_session.turn_state == "busy"));
		object.insert(
			"gajaeway_follow_up_queue_depth".to_owned(),
			json!(main_session.follow_up_queue_depth),
		);
		object.insert(
			"gajaeway_transcript_verified".to_owned(),
			json!(main_session.transcript_verification == "verified"),
		);
		Ok(projection)
	}

	async fn status_response(&self, params: Value) -> Result<Value, RpcError> {
		ensure_empty_params(&params)?;
		let health = self.health_response(Value::Object(Map::new()))?;
		let transcript_proof = durable_transcript_proof(&self.store)?;
		let tail_ring_rotation_count = durable_tail_ring_rotation_count(&self.store)?;
		let transcript_delivery_gap_count = durable_transcript_delivery_gap_count(&self.store)?;
		let main_session = self
			.main_session
			.lock()
			.map_err(|_| RpcError::internal("main session status lock poisoned"))?
			.clone();
		let locks = self.locks.clone();
		let journal = self.journal.clone();
		let store = self.store.clone();
		let (
			lock_status,
			head_cursor,
			consumer_checkpoints,
			write_mode,
			profile_digest,
			profile_version,
			profile_approved_at,
			reconcile_last_ok_at,
			reconcile_cycle_ms,
			reconcile_drift_count,
		) = tokio::task::spawn_blocking(move || {
			let lock_status = locks.status();
			let head_cursor = journal.head_cursor();
			let consumer_checkpoints = journal.consumer_checkpoints();
			let write_mode = store.get_meta("write_mode");
			let profile_digest = store.get_meta("profile_digest");
			let profile_version = store.get_meta("profile_digest_version");
			let profile_approved_at = store.get_meta("profile_approved_at");
			let reconcile_last_ok_at = store.get_meta("reconcile_last_ok_at");
			let reconcile_cycle_ms = store.get_meta("reconcile_cycle_ms");
			let reconcile_drift_count = store.get_meta("reconcile_drift_count");
			(
				lock_status,
				head_cursor,
				consumer_checkpoints,
				write_mode,
				profile_digest,
				profile_version,
				profile_approved_at,
				reconcile_last_ok_at,
				reconcile_cycle_ms,
				reconcile_drift_count,
			)
		})
		.await
		.map_err(|error| RpcError::internal(format!("status worker failed: {error}")))?;
		let lock_status = lock_status.map_err(lock_error)?;
		let head_cursor = head_cursor.map_err(|error| RpcError::internal(format!("journal status failed: {error}")))?;
		let consumer_checkpoints = consumer_checkpoints.map_err(journal_error)?;
		let write_mode = write_mode.map_err(store_error)?;
		let profile_digest = profile_digest.map_err(store_error)?;
		let profile_version = profile_version.map_err(store_error)?;
		let profile_approved_at = profile_approved_at.map_err(store_error)?;
		let reconcile_last_ok_at = reconcile_last_ok_at.map_err(store_error)?;
		let reconcile_cycle_ms = reconcile_cycle_ms.map_err(store_error)?;
		let reconcile_drift_count = reconcile_drift_count.map_err(store_error)?;
		let queue_len = lock_status.queue.len();
		let mut response = health.as_object().cloned().expect("health response is an object");
		response.insert("turn_state".to_owned(), json!(main_session.turn_state));
		response.insert("follow_up_queue_depth".to_owned(), json!(main_session.follow_up_queue_depth));
		response.insert("transcript_proof".to_owned(), json!(transcript_proof));
		response.insert("transcript_verification".to_owned(), json!(main_session.transcript_verification));
		response.insert("tail_ring_rotation_count".to_owned(), json!(tail_ring_rotation_count));
		response.insert("transcript_delivery_gap_count".to_owned(), json!(transcript_delivery_gap_count));
		response.insert("transcript_delivery_gap_detected".to_owned(), json!(transcript_delivery_gap_count > 0));
		// Scheduler state is surfaced here so the owner console and way.metrics
		// can see the subsystem; without it a new durable writer would be
		// invisible to the operator surfaces this project treats as the
		// observability contract.
		response.insert("schedule".to_owned(), schedule_status_json(&self.store)?);
		// Raised alert conditions, so an operator surface can see an active
		// condition without tailing the journal.
		response.insert(
			"alerts".to_owned(),
			json!(crate::alerts::raised_conditions(&self.store).map_err(store_error)?),
		);
		let mut lock = lock_status_json(lock_status);
		lock.as_object_mut()
			.expect("lock status is an object")
			.insert("queue_len".to_owned(), json!(queue_len));
		response.insert("lock".to_owned(), lock);
		response.insert(
			"journal".to_owned(),
			json!({ "head_cursor": head_cursor.to_string(), "degraded": main_session.journal_degraded }),
		);
		response.insert(
			"consumers".to_owned(),
			json!(
				consumer_checkpoints
					.into_iter()
					.map(|checkpoint| {
						json!({
							"consumer_id": checkpoint.consumer_id,
							"cursor": checkpoint.cursor.to_string(),
							"claim_id": checkpoint.claim_id,
							"claim_expires_at": checkpoint.claim_expires_at,
							"updated_at": checkpoint.updated_at,
						})
					})
					.collect::<Vec<_>>()
			),
		);
		response.insert(
			"reconcile".to_owned(),
			json!({
				"last_ok_at": reconcile_last_ok_at.and_then(|value| value.parse::<i64>().ok()),
				"cycle_ms": reconcile_cycle_ms.and_then(|value| value.parse::<i64>().ok()),
				"drift_count": reconcile_drift_count.and_then(|value| value.parse::<u64>().ok()).unwrap_or(0),
			}),
		);
		response.insert("write_mode".to_owned(), json!(write_mode.as_deref() == Some("on")));
		response.insert(
			"profile".to_owned(),
			json!({
				"digest": profile_digest.filter(|digest| digest != "null"),
				"digest_version": profile_version.and_then(|value| value.parse::<u64>().ok()).unwrap_or(0),
				"approved_at": metadata_json_optional_i64(profile_approved_at.as_deref(), "profile_approved_at")?,
			}),
		);
		Ok(Value::Object(response))
	}

	async fn bridge_method(&self, method: &str, params: Value, cancellation: CancellationToken) -> Result<Value, RpcError> {
		match self.bridge.call(method.to_owned(), params, cancellation).await {
			Ok(result) => Ok(result),
			Err(BridgeFailure::Overloaded) => Err(RpcError::internal("bridge_overloaded")),
			Err(BridgeFailure::ShuttingDown) => Err(RpcError::internal("shutting_down")),
			Err(BridgeFailure::Timeout) => Err(RpcError::internal("bridge_timeout")),
			Err(BridgeFailure::Cancelled) => Err(RpcError::internal("request cancelled")),
			Err(BridgeFailure::CallFailed(status)) => Err(RpcError::internal(format!("bridge TSFN call failed: {status}"))),
			Err(BridgeFailure::Remote(error)) => Err(remote_bridge_error(error)),
		}
	}

	async fn main_events_read(&self, params: Value, cancellation: CancellationToken) -> Result<Value, RpcError> {
		let request = parse_main_events_read_request(&params)?;
		let journal = self.journal.clone();
		let mut cursor = match request.consumer_id {
			Some(consumer_id) => tokio::task::spawn_blocking(move || journal.consumer_cursor(&consumer_id))
				.await
				.map_err(|error| RpcError::internal(format!("consumer cursor worker failed: {error}")))?
				.map_err(journal_error)?,
			None => request.cursor,
		};
		let deadline = TokioInstant::now() + Duration::from_millis(request.wait_ms);
		loop {
			let journal = self.journal.clone();
			let kinds = request.kinds.clone();
			let read = tokio::task::spawn_blocking(move || journal.read_filtered(cursor, request.limit, &kinds))
				.await
				.map_err(|error| RpcError::internal(format!("event read worker failed: {error}")))?
				.map_err(journal_error)?;
			if read.gap.is_some() || !read.events.is_empty() || request.wait_ms == 0 || TokioInstant::now() >= deadline {
				return Ok(journal_read_json(read));
			}
			cursor = Some(read.next_cursor);
			let pause = Duration::from_millis(25).min(deadline.saturating_duration_since(TokioInstant::now()));
			tokio::select! {
				_ = cancellation.cancelled() => return Err(RpcError::internal("request cancelled")),
				_ = tokio::time::sleep(pause) => {}
			}
		}
	}

	async fn consumer_claim(&self, params: Value) -> Result<Value, RpcError> {
		let request = parse_consumer_claim_request(&params)?;
		let journal = self.journal.clone();
		let claim = tokio::task::spawn_blocking(move || journal.claim_consumer(&request.consumer_id, request.claim_ttl_ms))
			.await
			.map_err(|error| RpcError::internal(format!("consumer claim worker failed: {error}")))?
			.map_err(journal_error)?;
		Ok(consumer_claim_json(claim))
	}

	async fn consumer_commit(&self, params: Value) -> Result<Value, RpcError> {
		let request = parse_consumer_commit_request(&params)?;
		let journal = self.journal.clone();
		let cursor = tokio::task::spawn_blocking(move || journal.commit_consumer(&request.consumer_id, &request.claim_id, request.cursor, &request.proofs))
			.await
			.map_err(|error| RpcError::internal(format!("consumer commit worker failed: {error}")))?
			.map_err(journal_error)?;
		Ok(json!({ "committed_cursor": cursor.to_string() }))
	}

	async fn idempotent<F, Fut>(&self, method: &str, params: &Value, operation: F) -> Result<Value, RpcError>
	where
		F: FnOnce() -> Fut,
		Fut: std::future::Future<Output = Result<Value, RpcError>>,
	{
		let object = params_object(params)?;
		let key = required_string(object, "idempotency_key")?;
		let request_json = serde_json::to_string(params).map_err(|_| RpcError::internal("could not encode idempotency request"))?;
		match self.store.replay_idempotency(method, key, &request_json, unix_epoch_ms()) {
			Ok(Some(response_json)) => {
				return serde_json::from_str(&response_json).map_err(|_| RpcError::internal("stored idempotency response is invalid JSON"));
			}
			Ok(None) => {}
			Err(StoreError::IdempotencyConflict) => return Err(RpcError::app(1500, None)),
			Err(error) => return Err(store_error(error)),
		}
		let response = operation().await?;
		let response_json = serde_json::to_string(&response).map_err(|_| RpcError::internal("could not encode idempotency response"))?;
		match self.store.store_idempotency_response(method, key, &request_json, &response_json, unix_epoch_ms()) {
			Ok(()) => Ok(response),
			Err(StoreError::IdempotencyConflict) => Err(RpcError::app(1500, None)),
			Err(error) => Err(store_error(error)),
		}
	}

	async fn lock_renew(&self, params: Value) -> Result<Value, RpcError> {
		let object = params_object(&params)?;
		ensure_allowed_keys(object, &["lease_id", "idempotency_key"])?;
		let lease_id = required_string(object, "lease_id")?.to_owned();
		let locks = self.locks.clone();
		let expires_at = tokio::task::spawn_blocking(move || locks.renew(&lease_id))
			.await
			.map_err(|error| RpcError::internal(format!("lock renew worker failed: {error}")))?
			.map_err(lock_error)?;
		Ok(json!({ "expires_at": expires_at }))
	}

	async fn lock_release(&self, params: Value) -> Result<Value, RpcError> {
		let object = params_object(&params)?;
		ensure_allowed_keys(object, &["lease_id", "idempotency_key"])?;
		let lease_id = required_string(object, "lease_id")?.to_owned();
		let locks = self.locks.clone();
		let result = tokio::task::spawn_blocking(move || locks.release(&lease_id))
			.await
			.map_err(|error| RpcError::internal(format!("lock release worker failed: {error}")))?
			.map_err(lock_error)?;
		Ok(lock_release_json(result))
	}

	async fn lock_status(&self, params: Value) -> Result<Value, RpcError> {
		ensure_empty_params(&params)?;
		let locks = self.locks.clone();
		let status = tokio::task::spawn_blocking(move || locks.status())
			.await
			.map_err(|error| RpcError::internal(format!("lock status worker failed: {error}")))?
			.map_err(lock_error)?;
		Ok(lock_status_json(status))
	}

	async fn lock_force_release(&self, params: Value) -> Result<Value, RpcError> {
		let object = params_object(&params)?;
		ensure_allowed_keys(object, &["lease_id", "confirm", "idempotency_key"])?;
		let lease_id = required_string(object, "lease_id")?.to_owned();
		let confirm = required_bool(object, "confirm")?;
		let locks = self.locks.clone();
		let result = tokio::task::spawn_blocking(move || locks.force_release(&lease_id, confirm))
			.await
			.map_err(|error| RpcError::internal(format!("force release worker failed: {error}")))?
			.map_err(lock_error)?;
		Ok(lock_release_json(result))
	}

	async fn lock_quarantine_override(&self, params: Value) -> Result<Value, RpcError> {
		let object = params_object(&params)?;
		ensure_allowed_keys(object, &["lease_id", "confirm", "acknowledge_unverified", "idempotency_key"])?;
		let lease_id = required_string(object, "lease_id")?.to_owned();
		let confirm = required_bool(object, "confirm")?;
		let acknowledge_unverified = required_bool(object, "acknowledge_unverified")?;
		let locks = self.locks.clone();
		let status = tokio::task::spawn_blocking(move || locks.quarantine_override(&lease_id, confirm, acknowledge_unverified))
			.await
			.map_err(|error| RpcError::internal(format!("quarantine worker failed: {error}")))?
			.map_err(lock_error)?;
		Ok(lock_status_json(status))
	}

	async fn lock_clear_quarantine(&self, params: Value) -> Result<Value, RpcError> {
		let object = params_object(&params)?;
		ensure_allowed_keys(object, &["verification_receipt_id", "confirm", "idempotency_key"])?;
		let verification_receipt_id = required_string(object, "verification_receipt_id")?.to_owned();
		let confirm = required_bool(object, "confirm")?;
		let locks = self.locks.clone();
		let status = tokio::task::spawn_blocking(move || locks.clear_quarantine(&verification_receipt_id, confirm))
			.await
			.map_err(|error| RpcError::internal(format!("clear quarantine worker failed: {error}")))?
			.map_err(lock_error)?;
		Ok(lock_status_json(status))
	}

	async fn lock_record_quarantine_receipt(&self, params: Value) -> Result<Value, RpcError> {
		let request = parse_quarantine_receipt_request(&params)?;
		let lease_id = request.lease_id;
		let corpus = request.corpus;
		let evidence = request.evidence;
		let locks = self.locks.clone();
		let receipt_lease_id = lease_id.clone();
		let receipt_corpus = corpus.clone();
		let receipt_id = tokio::task::spawn_blocking(move || locks.record_quarantine_receipt(&receipt_lease_id, &receipt_corpus, evidence))
			.await
			.map_err(|error| RpcError::internal(format!("record verification receipt worker failed: {error}")))?
			.map_err(lock_error)?;
		Ok(json!({ "receipt_id": receipt_id, "lease_id": lease_id, "corpus": corpus }))
	}

	async fn registry_list(&self, params: Value) -> Result<Value, RpcError> {
		let request = parse_registry_list_request(&params)?;
		let store = self.store.clone();
		let listed = tokio::task::spawn_blocking(move || registry::list(&store, request))
			.await
			.map_err(|error| RpcError::internal(format!("registry list worker failed: {error}")))?
			.map_err(registry_error)?;
		Ok(json!({
			"rows": listed.rows.into_iter().map(registry_row_json).collect::<Vec<_>>(),
			"total": listed.total,
		}))
	}

	async fn registry_get(&self, params: Value) -> Result<Value, RpcError> {
		let object = params_object(&params)?;
		ensure_allowed_keys(object, &["session_id"])?;
		let session_id = required_string(object, "session_id")?.to_owned();
		let store = self.store.clone();
		let row = tokio::task::spawn_blocking(move || registry::get(&store, &session_id))
			.await
			.map_err(|error| RpcError::internal(format!("registry get worker failed: {error}")))?
			.map_err(registry_error)?;
		Ok(json!({ "row": registry_row_json(row) }))
	}

	async fn registry_annotate(&self, params: Value) -> Result<Value, RpcError> {
		let object = params_object(&params)?;
		ensure_allowed_keys(object, &["session_id", "purpose", "brief", "idempotency_key"])?;
		let session_id = required_string(object, "session_id")?.to_owned();
		let purpose = optional_string(object, "purpose")?;
		let brief = optional_string(object, "brief")?;
		if purpose.is_none() && brief.is_none() {
			return Err(RpcError::invalid_params("registry.annotate requires purpose or brief"));
		}
		let store = self.store.clone();
		let row =
			tokio::task::spawn_blocking(move || registry::annotate(&store, RegistryAnnotation { session_id, purpose, brief, observed_at: unix_epoch_ms() }))
				.await
				.map_err(|error| RpcError::internal(format!("registry annotate worker failed: {error}")))?
				.map_err(registry_error)?;
		Ok(json!({ "row": registry_row_json(row) }))
	}

	async fn surface_resolve(&self, params: Value) -> Result<Value, RpcError> {
		let object = params_object(&params)?;
		ensure_allowed_keys(object, &["surface_id"])?;
		let surface_id = required_string(object, "surface_id")?.to_owned();
		let store = self.store.clone();
		let resolved = tokio::task::spawn_blocking(move || registry::resolve_surface(&store, &surface_id))
			.await
			.map_err(|error| RpcError::internal(format!("surface resolve worker failed: {error}")))?
			.map_err(registry_error)?;
		Ok(json!({
			"surface": {
				"surface_id": resolved.surface.surface_id,
				"platform": resolved.surface.platform,
				"kind": resolved.surface.kind,
				"is_owner_surface": resolved.surface.is_owner_surface,
			},
			"session_id": resolved.session_id,
			"policy": { "owner": resolved.surface.is_owner_surface },
			"quarantined": resolved.quarantined,
		}))
	}
}

struct MainEventsReadRequest {
	consumer_id: Option<String>,
	cursor: Option<Cursor>,
	limit: u32,
	wait_ms: u64,
	kinds: Vec<String>,
}

struct ConsumerClaimRequest {
	consumer_id: String,
	claim_ttl_ms: Option<u64>,
}

struct ConsumerCommitRequest {
	consumer_id: String,
	claim_id: String,
	cursor: Cursor,
	proofs: Vec<DeliveryProof>,
}

struct QuarantineReceiptRequest {
	lease_id: String,
	corpus: String,
	evidence: QuarantineReceiptEvidence,
}

fn parse_main_events_read_request(params: &Value) -> Result<MainEventsReadRequest, RpcError> {
	let object = params_object(params)?;
	ensure_allowed_keys(object, &["consumer_id", "cursor", "limit", "wait_ms", "kinds"])?;
	let consumer_id = optional_string(object, "consumer_id")?;
	let cursor = optional_string(object, "cursor")?
		.map(|value| {
			value
				.parse::<Cursor>()
				.map_err(|_| RpcError::invalid_params("cursor must be journal_generation:seq"))
		})
		.transpose()?;
	if consumer_id.is_some() && cursor.is_some() {
		return Err(RpcError::invalid_params("consumer_id and cursor are mutually exclusive"));
	}
	let limit = optional_u64(object, "limit")?.unwrap_or(100);
	let limit = u32::try_from(limit).map_err(|_| RpcError::invalid_params("limit must be in 1..=500"))?;
	if !(1..=500).contains(&limit) {
		return Err(RpcError::invalid_params("limit must be in 1..=500"));
	}
	let wait_ms = optional_u64(object, "wait_ms")?.unwrap_or(0);
	if wait_ms > 60_000 {
		return Err(RpcError::invalid_params("wait_ms must be in 0..=60000"));
	}
	let kinds = match object.get("kinds") {
		None => Vec::new(),
		Some(Value::Array(values)) if !values.is_empty() => {
			let mut kinds = Vec::with_capacity(values.len());
			for value in values {
				let kind = value
					.as_str()
					.filter(|value| !value.is_empty())
					.ok_or_else(|| RpcError::invalid_params("kinds must contain non-empty strings"))?;
				if !MAIN_EVENT_KINDS.contains(&kind) {
					return Err(RpcError::invalid_params(format!("unsupported event kind: {kind}")));
				}
				if kinds.iter().any(|existing| existing == kind) {
					return Err(RpcError::invalid_params("kinds must not contain duplicates"));
				}
				kinds.push(kind.to_owned());
			}
			kinds
		}
		Some(Value::Array(_)) => return Err(RpcError::invalid_params("kinds must not be empty")),
		Some(_) => {
			return Err(RpcError::invalid_params("kinds must be an array of event kinds"));
		}
	};
	Ok(MainEventsReadRequest { consumer_id, cursor, limit, wait_ms, kinds })
}

fn parse_consumer_claim_request(params: &Value) -> Result<ConsumerClaimRequest, RpcError> {
	let object = params_object(params)?;
	ensure_allowed_keys(object, &["consumer_id", "claim_ttl_ms"])?;
	Ok(ConsumerClaimRequest {
		consumer_id: required_string(object, "consumer_id")?.to_owned(),
		claim_ttl_ms: optional_u64(object, "claim_ttl_ms")?,
	})
}

fn parse_consumer_commit_request(params: &Value) -> Result<ConsumerCommitRequest, RpcError> {
	let object = params_object(params)?;
	ensure_allowed_keys(object, &["consumer_id", "claim_id", "cursor", "proofs"])?;
	let cursor = required_string(object, "cursor")?
		.parse::<Cursor>()
		.map_err(|_| RpcError::invalid_params("cursor must be journal_generation:seq"))?;
	let proofs_value = object.get("proofs").ok_or_else(|| RpcError::invalid_params("proofs is required"))?;
	let proofs_array = proofs_value.as_array().ok_or_else(|| RpcError::invalid_params("proofs must be an array"))?;
	let mut proofs = Vec::with_capacity(proofs_array.len());
	for proof in proofs_array {
		let proof = proof.as_object().ok_or_else(|| RpcError::invalid_params("proofs entries must be objects"))?;
		ensure_allowed_keys(proof, &["seq", "platform_msg_id", "dedupe_key"])?;
		let sequence = proof.get("seq").ok_or_else(|| RpcError::invalid_params("proofs[].seq is required"))?;
		let seq = parse_u64(sequence, "proofs[].seq")?;
		proofs.push(DeliveryProof {
			seq,
			platform_msg_id: optional_string(proof, "platform_msg_id")?,
			dedupe_key: Some(required_string(proof, "dedupe_key")?.to_owned()),
		});
	}
	Ok(ConsumerCommitRequest {
		consumer_id: required_string(object, "consumer_id")?.to_owned(),
		claim_id: required_string(object, "claim_id")?.to_owned(),
		cursor,
		proofs,
	})
}

fn parse_quarantine_receipt_request(params: &Value) -> Result<QuarantineReceiptRequest, RpcError> {
	let object = params_object(params)?;
	ensure_allowed_keys(object, &["lease_id", "corpus", "checks", "idempotency_key"])?;
	let checks = object
		.get("checks")
		.and_then(Value::as_object)
		.ok_or_else(|| RpcError::invalid_params("checks must be an object"))?;
	ensure_allowed_keys(
		checks,
		&["process_inspected", "git_status_checked", "git_log_checked", "git_fsck_checked", "remote_verified"],
	)?;
	Ok(QuarantineReceiptRequest {
		lease_id: required_string(object, "lease_id")?.to_owned(),
		corpus: required_string(object, "corpus")?.to_owned(),
		evidence: QuarantineReceiptEvidence {
			process_inspected: required_bool(checks, "process_inspected")?,
			git_status_checked: required_bool(checks, "git_status_checked")?,
			git_log_checked: required_bool(checks, "git_log_checked")?,
			git_fsck_checked: required_bool(checks, "git_fsck_checked")?,
			remote_verified: required_bool(checks, "remote_verified")?,
		},
	})
}

fn parse_registry_list_request(params: &Value) -> Result<RegistryListFilter, RpcError> {
	let object = params_object(params)?;
	ensure_allowed_keys(object, &["kind", "status", "surface_id", "limit", "offset"])?;
	let limit = optional_u64(object, "limit")?.unwrap_or(u64::from(registry::DEFAULT_LIST_LIMIT));
	let limit = u32::try_from(limit).map_err(|_| RpcError::invalid_params("limit must be in 1..=500"))?;
	if !(1..=registry::MAX_LIST_LIMIT).contains(&limit) {
		return Err(RpcError::invalid_params("limit must be in 1..=500"));
	}
	Ok(RegistryListFilter {
		kind: optional_string(object, "kind")?,
		status: optional_string(object, "status")?,
		surface_id: optional_string(object, "surface_id")?,
		limit,
		offset: optional_u64(object, "offset")?.unwrap_or(0),
	})
}

fn journal_read_json(read: JournalRead) -> Value {
	let events = read
		.events
		.into_iter()
		.map(|event| {
			json!({
				"seq": event.seq,
				"ts": event.ts,
				"kind": event.kind,
				"payload": serde_json::from_str::<Value>(&event.payload_json).expect("stored journal payload is valid JSON"),
			})
		})
		.collect::<Vec<_>>();
	let mut response = Map::new();
	response.insert("events".to_owned(), Value::Array(events));
	response.insert("next_cursor".to_owned(), Value::String(read.next_cursor.to_string()));
	if let Some(gap) = read.gap {
		response.insert("gap".to_owned(), journal_gap_json(gap));
	}
	Value::Object(response)
}

fn journal_gap_json(gap: JournalGap) -> Value {
	json!({
		"missing_from": gap.missing_from.to_string(),
		"missing_to": gap.missing_to.to_string(),
		"resync_cursor": gap.resync_cursor.to_string(),
	})
}

fn consumer_claim_json(claim: ConsumerClaim) -> Value {
	json!({
		"claim_id": claim.claim_id,
		"cursor": claim.cursor.to_string(),
		"expires_at": claim.expires_at,
	})
}

fn registry_row_json(row: registry::RegistryRow) -> Value {
	let locator = row
		.locator
		.as_deref()
		.and_then(|value| serde_json::from_str::<Value>(value).ok())
		.unwrap_or(Value::Null);
	json!({
		"session_id": row.session_id,
		"kind": row.kind,
		"purpose": row.purpose,
		"brief": row.brief,
		"status": row.status,
		"surface_id": row.surface_id,
		"locator": locator,
		"endpoint_generation": row.endpoint_generation,
		"host_incarnation": row.host_incarnation,
		"identity_provenance": row.identity_provenance,
		"index_seq": row.index_seq,
		"live": row.live,
		"deleted": row.deleted,
		"terminal_uncertain": row.terminal_uncertain,
		"ambiguous": row.ambiguous,
		"activity_state": row.activity_state,
		"activity_at": row.activity_at,
		"last_heartbeat_at": row.last_heartbeat_at,
		"meta_name": row.meta_name,
		"meta_cwd": row.meta_cwd,
		"meta_kind": row.meta_kind,
		"metadata_state": row.metadata_state,
		"metadata_at": row.metadata_at,
		"source": row.source,
		"created_at": row.created_at,
		"last_seen_at": row.last_seen_at,
		"closed_at": row.closed_at,
		"registry_rev": row.registry_rev,
		"quarantined": row.quarantined,
	})
}
fn durable_main_session_id(store: &Store) -> Result<String, RpcError> {
	let raw_identity = store
		.get_meta("main_identity")
		.map_err(store_error)?
		.ok_or_else(|| RpcError::internal("runtime main session is missing main_identity"))?;
	let identity: Value = serde_json::from_str(&raw_identity).map_err(|_| RpcError::internal("main_identity gateway metadata is invalid"))?;
	identity
		.as_object()
		.and_then(|object| object.get("sessionId"))
		.and_then(Value::as_str)
		.filter(|value| !value.is_empty())
		.map(str::to_owned)
		.ok_or_else(|| RpcError::internal("runtime main session identity has no sessionId"))
}

fn metadata_json_optional_i64(raw: Option<&str>, key: &str) -> Result<Option<i64>, RpcError> {
	let Some(raw) = raw else {
		return Ok(None);
	};
	let value: Value = serde_json::from_str(raw).map_err(|_| RpcError::internal(format!("{key} gateway metadata is invalid")))?;
	match value {
		Value::Null => Ok(None),
		Value::Number(number) if number.as_i64().is_some_and(|value| value >= 0) => Ok(number.as_i64()),
		_ => Err(RpcError::internal(format!("{key} gateway metadata must be a non-negative integer or null"))),
	}
}

fn journal_error(error: JournalError) -> RpcError {
	match error {
		JournalError::CursorBeforeRetention(gap) => RpcError::with_data(1600, "cursor_before_retention", journal_gap_json(gap)),
		JournalError::ConsumerClaimHeld => RpcError::app(1601, None),
		JournalError::InvalidPayload
		| JournalError::InvalidKind
		| JournalError::InvalidLimit
		| JournalError::InvalidClaimTtl
		| JournalError::InvalidCursor
		| JournalError::ClaimNotHeld
		| JournalError::ClaimExpired
		| JournalError::CursorRegression
		| JournalError::CursorAheadOfHead
		| JournalError::InvalidProof(_) => RpcError::invalid_params(error.to_string()),
		JournalError::Store(error) => store_error(error),
		JournalError::Overflow => RpcError::internal("journal overflow"),
	}
}

fn bounded_bridge_log_value(value: &str) -> String {
	value.chars().take(16_384).collect()
}

fn bridge_failure_reason(data: Option<&Value>) -> String {
	data
		.and_then(Value::as_object)
		.and_then(|object| object.get("reason"))
		.and_then(Value::as_str)
		.filter(|reason| is_machine_reason(reason))
		.map(str::to_owned)
		.unwrap_or_else(|| "bridge_exception".to_owned())
}

fn bridge_diagnostic(data: Option<&Value>) -> Option<BridgeDiagnostic> {
	let diagnostic = data?.as_object()?.get("diagnostic")?.as_object()?;
	let error_type = diagnostic.get("error_type")?.as_str()?.to_owned();
	let message = diagnostic.get("message")?.as_str()?.to_owned();
	let stack = diagnostic.get("stack").and_then(Value::as_str).map(str::to_owned);
	Some(BridgeDiagnostic { error_type, message, stack })
}

fn remote_bridge_error(error: Value) -> RpcError {
	let Some(object) = error.as_object() else {
		return RpcError::bridge_exception("bridge_protocol_invalid".to_owned(), None);
	};
	let Some(code) = object.get("code").and_then(Value::as_i64) else {
		return RpcError::bridge_exception("bridge_protocol_invalid".to_owned(), None);
	};
	if code == -32603 {
		let data = object.get("data");
		return RpcError::bridge_exception(bridge_failure_reason(data), bridge_diagnostic(data));
	}
	let message = object.get("message").and_then(Value::as_str).unwrap_or("bridge_exception");
	let data = object.get("data").cloned();
	RpcError { code, message: message.to_owned(), data }
}

fn store_error(error: StoreError) -> RpcError {
	match error {
		StoreError::IdempotencyConflict => RpcError::app(1500, None),
		error => RpcError::internal(format!("store failure: {error}")),
	}
}

fn registry_error(error: RegistryError) -> RpcError {
	if let Some(code) = error.code() {
		return RpcError::app(i64::from(code), None);
	}
	match error {
		RegistryError::InvalidInput(message) => RpcError::invalid_params(message),
		RegistryError::Store(error) => store_error(error),
		RegistryError::Overflow => RpcError::internal("registry numeric overflow"),
		RegistryError::UnknownSession | RegistryError::UnknownSurface | RegistryError::SessionQuarantined => {
			unreachable!("registry application errors have codes")
		}
	}
}

fn lock_error(error: LockError) -> RpcError {
	if let Some(code) = error.code() {
		return RpcError::app(i64::from(code), None);
	}
	match error {
		LockError::InvalidTtl | LockError::InvalidWait | LockError::InvalidHolder(_) | LockError::ConfirmationRequired => {
			RpcError::invalid_params(error.to_string())
		}
		LockError::Store(StoreError::IdempotencyConflict) => RpcError::app(1500, None),
		error => RpcError::internal(format!("lock failure: {error}")),
	}
}

fn saturating_i64(value: u64) -> i64 {
	i64::try_from(value).unwrap_or(i64::MAX)
}

fn ensure_empty_params(params: &Value) -> Result<(), RpcError> {
	match params {
		Value::Null => Ok(()),
		Value::Object(object) if object.is_empty() => Ok(()),
		_ => Err(RpcError::invalid_params("method does not accept parameters")),
	}
}

fn params_object(params: &Value) -> Result<&Map<String, Value>, RpcError> {
	params.as_object().ok_or_else(|| RpcError::invalid_params("params must be an object"))
}

fn ensure_allowed_keys(object: &Map<String, Value>, allowed: &[&str]) -> Result<(), RpcError> {
	if let Some(unexpected) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
		return Err(RpcError::invalid_params(format!("unknown parameter: {unexpected}")));
	}
	Ok(())
}

fn required_string<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a str, RpcError> {
	object
		.get(key)
		.and_then(Value::as_str)
		.filter(|value| !value.is_empty())
		.ok_or_else(|| RpcError::invalid_params(format!("{key} must be a non-empty string")))
}

fn optional_string(object: &Map<String, Value>, key: &str) -> Result<Option<String>, RpcError> {
	let Some(value) = object.get(key) else {
		return Ok(None);
	};
	let value = value
		.as_str()
		.filter(|value| !value.is_empty())
		.ok_or_else(|| RpcError::invalid_params(format!("{key} must be a non-empty string")))?;
	Ok(Some(value.to_owned()))
}

fn parse_u64(value: &Value, key: &str) -> Result<u64, RpcError> {
	match value {
		Value::Number(number) => number
			.as_u64()
			.ok_or_else(|| RpcError::invalid_params(format!("{key} must be an unsigned integer"))),
		Value::String(value) => value
			.parse::<u64>()
			.map_err(|_| RpcError::invalid_params(format!("{key} must be an unsigned integer"))),
		_ => Err(RpcError::invalid_params(format!("{key} must be an unsigned integer"))),
	}
}

fn required_bool(object: &Map<String, Value>, key: &str) -> Result<bool, RpcError> {
	object
		.get(key)
		.and_then(Value::as_bool)
		.ok_or_else(|| RpcError::invalid_params(format!("{key} must be a boolean")))
}

fn optional_u64(object: &Map<String, Value>, key: &str) -> Result<Option<u64>, RpcError> {
	let Some(value) = object.get(key) else {
		return Ok(None);
	};
	match value {
		Value::Number(number) => number
			.as_u64()
			.map(Some)
			.ok_or_else(|| RpcError::invalid_params(format!("{key} must be an unsigned integer"))),
		Value::String(value) => value
			.parse::<u64>()
			.map(Some)
			.map_err(|_| RpcError::invalid_params(format!("{key} must be an unsigned integer"))),
		_ => Err(RpcError::invalid_params(format!("{key} must be an unsigned integer"))),
	}
}

fn lock_release_json(result: ReleaseResult) -> Value {
	json!({ "released": result.released, "held_ms": result.held_ms })
}

fn lease_json(lease: Lease) -> Value {
	json!({
		"lease_id": lease.lease_id,
		"holder_kind": lease.holder.holder_kind.as_sql(),
		"session_id": lease.holder.session_id,
		"label": lease.holder.label,
		"pid": lease.holder.pid,
		"pid_start_time": lease.holder.pid_start_time.to_string(),
		"pgid": lease.holder.pgid,
		"pgid_start_time": lease.holder.pgid_start_time.map(|value| value.to_string()),
		"conn_id": lease.holder.conn_id,
		"class": lease.class.as_sql(),
		"state": lease.state.as_sql(),
		"fencing_token": lease.fencing_token.to_string(),
		"expires_at": lease.expires_at,
	})
}

fn lock_status_json(status: LockStatus) -> Value {
	json!({
		"held": status.held,
		"holder": status.holder.map(lease_json),
		"expires_at": status.expires_at,
		"fencing_token": status.fencing_token.map(|value| value.to_string()),
		"queue": status.queue.into_iter().map(|entry| json!({
			"class": entry.class.as_sql(),
			"label": entry.label,
			"waited_ms": entry.waited_ms,
		})).collect::<Vec<_>>(),
		"stuck": status.stuck,
		"quarantined": status.quarantined,
	})
}

#[cfg(test)]
mod tests {

	use serde_json::json;

	use super::{APP_ERROR_CODES, GatewayState, MAX_BRIDGE_IN_FLIGHT, RpcDispatcher, TsfnBridge, app_error_name};
	use crate::{events::EventJournal, lock::LockManager, store::Store};

	fn dispatcher() -> RpcDispatcher {
		let store = Store::default();
		let locks = LockManager::new(store.clone());
		let journal = EventJournal::new(store.clone());
		RpcDispatcher::new(store, locks, journal, TsfnBridge::for_tests())
	}

	#[test]
	fn app_error_space_has_unique_names_and_codes() {
		for (code, name) in APP_ERROR_CODES {
			assert_eq!(app_error_name(*code), Some(*name));
		}
		assert_eq!(APP_ERROR_CODES.len(), 26);
	}

	#[tokio::test]
	async fn pending_transcript_proof_fences_main_mutations_but_keeps_observation_available() {
		let store = Store::default();
		store.set_meta("bootstrap_state", "COMMITTED").unwrap();
		store.set_meta("transcript_proof", "pending").unwrap();
		let locks = LockManager::new(store.clone());
		let journal = EventJournal::new(store.clone());
		let dispatcher = RpcDispatcher::new(store, locks, journal, TsfnBridge::for_tests());

		let status = dispatcher
			.dispatch("way.status".to_owned(), json!({}), super::super::CancellationToken::new())
			.await
			.unwrap();
		assert_eq!(status["transcript_proof"], "pending");
		assert_eq!(status["tail_ring_rotation_count"], 0);
		assert_eq!(status["transcript_delivery_gap_count"], 0);
		assert_eq!(status["transcript_delivery_gap_detected"], false);

		let blocked = dispatcher
			.dispatch("main.submit".to_owned(), json!({ "text": "must not send" }), super::super::CancellationToken::new())
			.await
			.unwrap_err();
		assert_eq!(blocked.code, 1003);
		assert_eq!(blocked.message, "transcript_proof_pending");
		let blocked_closure = dispatcher
			.dispatch("main.corpus.close".to_owned(), json!({}), super::super::CancellationToken::new())
			.await
			.unwrap_err();
		assert_eq!(blocked_closure.code, 1003);
		assert_eq!(blocked_closure.message, "transcript_proof_pending");

		let observed = dispatcher
			.dispatch("main.events.read".to_owned(), json!({}), super::super::CancellationToken::new())
			.await
			.unwrap();
		assert!(observed["events"].as_array().unwrap().is_empty());
	}

	#[tokio::test]
	async fn failed_closed_fences_mutations_but_keeps_observation_and_gate_answers_available() {
		let dispatcher = dispatcher();
		dispatcher.set_gateway_state(GatewayState::FailedClosed, Some("profile_drift".to_owned()));
		let cancellation = super::super::CancellationToken::new();
		let health = dispatcher.dispatch("way.health".to_owned(), json!({}), cancellation.clone()).await.unwrap();
		assert_eq!(health["status"], "unhealthy");
		assert_eq!(health["reason"], "profile_drift");
		let error = dispatcher.dispatch("gitlock.status".to_owned(), json!({}), cancellation.clone()).await.unwrap_err();
		assert_eq!(error.code, 1000);
		let events = dispatcher
			.dispatch("main.events.read".to_owned(), json!({}), cancellation.clone())
			.await
			.unwrap();
		assert!(events["events"].as_array().unwrap().is_empty());
		let gate = dispatcher
			.dispatch("main.gate.answer".to_owned(), json!({}), cancellation.clone())
			.await
			.unwrap_err();
		assert_ne!(gate.code, 1000);
		let approval = dispatcher
			.dispatch("profile.approve".to_owned(), json!({}), cancellation)
			.await
			.unwrap_err();
		assert_ne!(approval.code, 1000);
	}

	/// Every `schedule.*` method must be refused while failed closed.
	///
	/// This holds because the family routes through the bridge arm and is absent
	/// from the fail-closed allowlist, so it needs no second list to maintain.
	/// The test pins that consequence rather than the mechanism, so moving the
	/// routing without re-checking the gate fails here.
	#[tokio::test]
	async fn failed_closed_blocks_every_schedule_method() {
		let dispatcher = dispatcher();
		dispatcher.set_gateway_state(GatewayState::FailedClosed, Some("profile_drift".to_owned()));
		let cancellation = super::super::CancellationToken::new();
		for method in [
			"schedule.create",
			"schedule.update",
			"schedule.delete",
			"schedule.run_now",
			"schedule.get",
			"schedule.list",
			"schedule.runs",
		] {
			let error = dispatcher
				.dispatch(method.to_owned(), json!({}), cancellation.clone())
				.await
				.unwrap_err();
			assert_eq!(error.code, 1000, "{method} must be fenced while failed closed");
		}
	}

	/// `memory.search` must be REFUSED while failed closed. Unlike way.metrics,
	/// this read is authorized by the digest-bound redaction policy, and
	/// `profile_drift` is exactly the state in which that policy is unverified.
	#[tokio::test]
	async fn failed_closed_blocks_memory_search() {
		let dispatcher = dispatcher();
		dispatcher.set_gateway_state(GatewayState::FailedClosed, Some("profile_drift".to_owned()));
		let error = dispatcher
			.dispatch("memory.search".to_owned(), json!({ "query": "anything" }), super::super::CancellationToken::new())
			.await
			.unwrap_err();
		assert_eq!(error.code, 1000);
	}

	#[tokio::test]
	async fn memory_search_rejects_unknown_fields_and_an_empty_query() {
		let dispatcher = dispatcher();
		let cancellation = super::super::CancellationToken::new();
		for params in [json!({ "query": "x", "mask": 0 }), json!({ "query": "   " }), json!({})] {
			let error = dispatcher
				.dispatch("memory.search".to_owned(), params, cancellation.clone())
				.await
				.unwrap_err();
			assert_eq!(error.code, -32602);
		}
	}

	/// The response must expose no cardinality beyond `hits.length`; a total or
	/// match count would disclose the existence of redacted documents.
	#[tokio::test]
	async fn memory_search_exposes_no_cardinality_beyond_hits() {
		let dispatcher = dispatcher();
		let digest = dispatcher.store.get_meta("profile_digest").unwrap().unwrap_or_default();
		crate::memory::rebuild(&dispatcher.store, &digest, &[(0, "SOUL.md".to_owned(), "quokka".to_owned())]).unwrap();

		let result = dispatcher
			.dispatch("memory.search".to_owned(), json!({ "query": "quokka" }), super::super::CancellationToken::new())
			.await
			.unwrap();

		let object = result.as_object().unwrap();
		assert_eq!(object.len(), 1, "hits is the only field");
		assert!(object.contains_key("hits"));
		for forbidden in ["total", "matched", "score_sum", "count"] {
			assert!(!object.contains_key(forbidden), "must not expose {forbidden}");
		}
	}

	/// The spec maps an unbound, unresolvable, or absent class to `unknown`.
	/// This asserts that mapping explicitly rather than relying on an empty
	/// store, which is what the previous version of this test actually proved.
	#[tokio::test]
	async fn an_unresolvable_session_kind_uses_the_unknown_mask() {
		let dispatcher = dispatcher();
		let digest = dispatcher.store.get_meta("profile_digest").unwrap().unwrap_or_default();
		crate::memory::rebuild(
			&dispatcher.store,
			&digest,
			&[(0, "SOUL.md".to_owned(), "quokka".to_owned()), (1, "MEMORY.md".to_owned(), "quokka".to_owned())],
		)
		.unwrap();
		// `unknown` denies bit 1 only.
		dispatcher.store.set_meta("memory_mask_unknown", "2").unwrap();

		let result = dispatcher
			.dispatch(
				"memory.search".to_owned(),
				json!({ "query": "quokka", "session_kind": "nonexistent" }),
				super::super::CancellationToken::new(),
			)
			.await
			.unwrap();
		let hits = result["hits"].as_array().unwrap();
		assert_eq!(hits.len(), 1, "an unresolvable class inherits unknown's deny list");
		assert_eq!(hits[0]["path"], "SOUL.md");
	}

	/// With no mask recorded at all, the fallback must be deny-all rather than
	/// permit-all: an unconfigured class is where guessing permissively is worst.
	#[tokio::test]
	async fn an_absent_mask_denies_everything() {
		let dispatcher = dispatcher();
		let digest = dispatcher.store.get_meta("profile_digest").unwrap().unwrap_or_default();
		crate::memory::rebuild(&dispatcher.store, &digest, &[(0, "SOUL.md".to_owned(), "quokka".to_owned())]).unwrap();

		let result = dispatcher
			.dispatch(
				"memory.search".to_owned(),
				json!({ "query": "quokka", "session_kind": "nonexistent" }),
				super::super::CancellationToken::new(),
			)
			.await
			.unwrap();
		assert!(result["hits"].as_array().unwrap().is_empty(), "no mask means deny all");
	}

	/// `way.metrics` must answer while failed closed: that is the one state an
	/// operator most needs telemetry, so refusing it would be a self-inflicted
	/// blind spot.
	#[tokio::test]
	async fn metrics_answers_while_failed_closed_and_excludes_identifiers() {
		let dispatcher = dispatcher();
		dispatcher.set_gateway_state(GatewayState::FailedClosed, Some("profile_drift".to_owned()));
		let metrics = dispatcher
			.dispatch("way.metrics".to_owned(), json!({}), super::super::CancellationToken::new())
			.await
			.unwrap();

		assert_eq!(metrics["gajaeway_state"], "failed_closed");
		assert!(metrics["gajaeway_journal_head_seq"].is_number());
		assert!(metrics["gajaeway_lock_quarantined"].is_boolean());
		assert!(metrics["gajaeway_schedule"]["active_jobs"].is_number());

		// No holder identity, session id, lease id, or free-text reason may
		// appear: this projection is exposed over unauthenticated loopback HTTP.
		let rendered = serde_json::to_string(&metrics).unwrap();
		for forbidden in ["holder", "session_id", "lease_id", "reason"] {
			assert!(!rendered.contains(forbidden), "metrics must not expose {forbidden}");
		}
	}

	/// `way.status` must expose the scheduler subsystem, or a new durable writer
	/// is invisible to the operator surfaces this project treats as the
	/// observability contract.
	#[tokio::test]
	async fn status_exposes_scheduler_counters() {
		let dispatcher = dispatcher();
		let status = dispatcher
			.dispatch("way.status".to_owned(), json!({}), super::super::CancellationToken::new())
			.await
			.unwrap();
		let schedule = &status["schedule"];
		assert_eq!(schedule["active_jobs"], 0);
		assert_eq!(schedule["backoff_jobs"], 0);
		assert_eq!(schedule["suspended_jobs"], 0);
		assert_eq!(schedule["in_flight_runs"], 0);
		assert!(schedule["next_fire_at_ms"].is_null());
	}

	#[tokio::test]
	async fn degraded_health_remains_unhealthy_when_startup_attempts_to_publish_running() {
		let dispatcher = dispatcher();
		dispatcher.set_gateway_state(GatewayState::Degraded, Some("tail_unavailable".to_owned()));
		dispatcher.set_gateway_state(GatewayState::Running, None);

		let health = dispatcher
			.dispatch("way.health".to_owned(), json!({}), super::super::CancellationToken::new())
			.await
			.unwrap();
		assert_eq!(health["status"], "unhealthy");
		assert_eq!(health["state"], "degraded");
		assert_eq!(health["reason"], "tail_unavailable");

		let status = dispatcher
			.dispatch("way.status".to_owned(), json!({}), super::super::CancellationToken::new())
			.await
			.unwrap();
		assert_eq!(status["status"], "unhealthy");
		assert_eq!(status["state"], "degraded");
		assert_eq!(status["reason"], "tail_unavailable");
	}

	#[tokio::test]
	async fn durable_failed_closed_marker_overrides_stale_running_health() {
		let store = Store::default();
		store.set_meta("bootstrap_state", "FAILED_CLOSED").unwrap();
		store.set_meta("failed_closed_reason", r#""growth_protocol_invalid""#).unwrap();
		let locks = LockManager::new(store.clone());
		let journal = EventJournal::new(store.clone());
		let dispatcher = RpcDispatcher::new(store, locks, journal, TsfnBridge::for_tests());
		dispatcher.set_gateway_state(GatewayState::Running, None);

		let health = dispatcher
			.dispatch("way.health".to_owned(), json!({}), super::super::CancellationToken::new())
			.await
			.unwrap();
		assert_eq!(health["status"], "unhealthy");
		assert_eq!(health["state"], "failed_closed");
		assert_eq!(health["reason"], "growth_protocol_invalid");

		let status = dispatcher
			.dispatch("way.status".to_owned(), json!({}), super::super::CancellationToken::new())
			.await
			.unwrap();
		assert_eq!(status["status"], "unhealthy");
		assert_eq!(status["state"], "failed_closed");
		assert_eq!(status["reason"], "growth_protocol_invalid");

		let blocked = dispatcher
			.dispatch("main.submit".to_owned(), json!({}), super::super::CancellationToken::new())
			.await
			.unwrap_err();
		assert_eq!(blocked.code, 1000);
		assert_eq!(blocked.data, Some(json!({ "reason": "growth_protocol_invalid" })));
	}

	#[tokio::test]
	async fn health_and_status_publish_main_resume_only_after_runtime_confirmation() {
		let store = Store::default();
		store.set_meta("bootstrap_state", "COMMITTED").unwrap();
		store
			.set_meta(
				"main_identity",
				r#"{"canonicalPath":"/tmp/main.jsonl","sessionId":"durable-main","device":"1","inode":"2","nlink":"1","size":"1","mtimeMs":"1","prefixSha256":"x"}"#,
			)
			.unwrap();
		store.set_meta("profile_digest", "durable-profile-digest").unwrap();
		store.set_meta("profile_digest_version", "1").unwrap();
		store.set_meta("profile_approved_at", "1710000000000").unwrap();
		let locks = LockManager::new(store.clone());
		let journal = EventJournal::new(store.clone());
		let claim = journal.claim_consumer("discord", Some(5_000)).unwrap();
		let dispatcher = RpcDispatcher::new(store, locks, journal, TsfnBridge::for_tests());

		// Verification starts from a false runtime publication even if a prior
		// process committed the bootstrap record.
		dispatcher.set_gateway_state(GatewayState::Verifying, None);
		let verifying_health = dispatcher
			.dispatch("way.health".to_owned(), json!({}), super::super::CancellationToken::new())
			.await
			.unwrap();
		assert_eq!(verifying_health["main"]["resumed"], false);

		// A committed prior bootstrap alone is never evidence that this daemon has
		// opened the session. Failed-closed linger must not advertise it as resumed.
		dispatcher.set_gateway_state(GatewayState::FailedClosed, Some("strict_resume_failed".to_owned()));
		let lingering_health = dispatcher
			.dispatch("way.health".to_owned(), json!({}), super::super::CancellationToken::new())
			.await
			.unwrap();
		assert_eq!(lingering_health["main"]["resumed"], false);
		assert!(lingering_health["main"]["session_id"].is_null());
		let lingering_status = dispatcher
			.dispatch("way.status".to_owned(), json!({}), super::super::CancellationToken::new())
			.await
			.unwrap();
		assert_eq!(lingering_status["main"]["resumed"], false);

		// This live publication occurs only after main.ts completes
		// strictResumeMainSession and constructs its host.
		dispatcher.set_gateway_state(GatewayState::Running, None);
		dispatcher.set_main_session_status("idle".to_owned(), 0, "verified".to_owned());
		let health = dispatcher
			.dispatch("way.health".to_owned(), json!({}), super::super::CancellationToken::new())
			.await
			.unwrap();
		assert_eq!(health["main"]["resumed"], true);
		assert_eq!(health["main"]["session_id"], "durable-main");
		let status = dispatcher
			.dispatch("way.status".to_owned(), json!({}), super::super::CancellationToken::new())
			.await
			.unwrap();
		assert_eq!(status["profile"]["approved_at"], 1_710_000_000_000_i64);
		assert_eq!(status["profile"]["digest"], "durable-profile-digest");
		assert_eq!(status["consumers"][0]["consumer_id"], "discord");
		assert_eq!(status["consumers"][0]["claim_id"], claim.claim_id);
		assert_eq!(status["consumers"][0]["cursor"], claim.cursor.to_string());

		// Disposing the host or entering failed-closed clears only live runtime
		// publication; durable identity remains available for a later strict resume.
		dispatcher.reset_main_session_status();
		dispatcher.set_gateway_state(GatewayState::FailedClosed, Some("host_disposed".to_owned()));
		let disposed_health = dispatcher
			.dispatch("way.health".to_owned(), json!({}), super::super::CancellationToken::new())
			.await
			.unwrap();
		assert_eq!(disposed_health["main"]["resumed"], false);
		assert!(disposed_health["main"]["session_id"].is_null());
	}

	#[test]
	fn bridge_capacity_is_bounded_before_the_sixty_fifth_request() {
		let bridge = TsfnBridge::for_tests();
		let mut reservations = Vec::new();
		for _ in 0..MAX_BRIDGE_IN_FLIGHT {
			reservations.push(bridge.reserve().unwrap());
		}
		assert!(matches!(bridge.reserve(), Err(super::BridgeFailure::Overloaded)));
		assert_eq!(bridge.stats().overloads, 1);
		drop(reservations);
	}

	#[tokio::test]
	async fn rpc_callers_cannot_mint_v1_in_daemon_or_external_lock_holders() {
		let dispatcher = dispatcher();
		for holder_kind in ["in_daemon", "external"] {
			let error = dispatcher
				.dispatch(
					"gitlock.acquire".to_owned(),
					json!({
						"label": "forbidden",
						"holder": {
							"holder_kind": holder_kind,
							"session_id": "caller-controlled",
							"pid": std::process::id(),
							"pid_start_time": "0",
							"pgid": std::process::id(),
							"conn_id": "way.in_daemon_executor.v1",
						},
						"idempotency_key": format!("forbidden-{holder_kind}"),
					}),
					super::super::CancellationToken::new(),
				)
				.await
				.unwrap_err();
			assert_eq!(error.code, -32602);
			assert!(error.message.contains("supervised"));
		}
	}

	#[tokio::test]
	async fn main_event_reads_filter_known_kinds_and_return_generation_gaps() {
		let store = Store::default();
		let locks = LockManager::new(store.clone());
		let journal = EventJournal::new(store.clone());
		let dispatcher = RpcDispatcher::new(store, locks, journal.clone(), TsfnBridge::for_tests());
		journal.append("assistant_message", r#"{"text":"skip"}"#).unwrap();
		journal.append("turn_start", r#"{"turn":"one"}"#).unwrap();

		let filtered = dispatcher
			.dispatch(
				"main.events.read".to_owned(),
				json!({ "cursor": "1:0", "kinds": ["turn_start"] }),
				super::super::CancellationToken::new(),
			)
			.await
			.unwrap();
		assert_eq!(filtered["events"].as_array().unwrap().len(), 1);
		assert_eq!(filtered["events"][0]["kind"], "turn_start");
		assert_eq!(filtered["next_cursor"], "1:2");

		let gap = dispatcher
			.dispatch("main.events.read".to_owned(), json!({ "cursor": "0:0" }), super::super::CancellationToken::new())
			.await
			.unwrap();
		assert_eq!(gap["gap"]["resync_cursor"], "1:0");
	}

	#[tokio::test]
	async fn consumer_claim_and_commit_dispatch_the_durable_settlement_protocol() {
		let store = Store::default();
		let locks = LockManager::new(store.clone());
		let journal = EventJournal::new(store.clone());
		let dispatcher = RpcDispatcher::new(store, locks, journal.clone(), TsfnBridge::for_tests());
		let event = journal.append("assistant_message", r#"{"text":"deliver"}"#).unwrap();
		let claim = dispatcher
			.dispatch(
				"consumer.claim".to_owned(),
				json!({ "consumer_id": "discord" }),
				super::super::CancellationToken::new(),
			)
			.await
			.unwrap();
		let held = dispatcher
			.dispatch(
				"consumer.claim".to_owned(),
				json!({ "consumer_id": "discord" }),
				super::super::CancellationToken::new(),
			)
			.await
			.unwrap_err();
		assert_eq!(held.code, 1601);
		let committed = dispatcher
			.dispatch(
				"consumer.commit".to_owned(),
				json!({
					"consumer_id": "discord",
					"claim_id": claim["claim_id"],
					"cursor": event.to_string(),
					"proofs": [{ "seq": event.seq, "dedupe_key": "discord:1" }],
				}),
				super::super::CancellationToken::new(),
			)
			.await
			.unwrap();
		assert_eq!(committed["committed_cursor"], event.to_string());
		assert_eq!(journal.consumer_cursor("discord").unwrap(), Some(event));
		assert_eq!(journal.outbox_rows("discord").unwrap().len(), 1);
	}

	#[tokio::test(start_paused = true)]
	async fn bridge_deadline_times_out_and_ignores_a_late_completion() {
		let bridge = TsfnBridge::for_tests();
		let (request_id, receiver, deadline) = bridge.reserve().unwrap();
		let waiting_bridge = bridge.clone();
		let waiting = tokio::spawn(async move {
			waiting_bridge
				.await_completion(request_id, deadline, receiver, super::super::CancellationToken::new())
				.await
		});
		tokio::task::yield_now().await;
		tokio::time::advance(std::time::Duration::from_millis(super::BRIDGE_TIMEOUT_MS + 1)).await;
		assert!(matches!(waiting.await.unwrap(), Err(super::BridgeFailure::Timeout)));
		assert!(!bridge.complete_json(request_id, "{\"late\":true}"));
		assert_eq!(bridge.stats().timeouts, 1);
		assert_eq!(bridge.stats().late_completions, 1);
	}

	#[test]
	fn late_or_second_bridge_completion_is_a_counted_noop() {
		let bridge = TsfnBridge::for_tests();
		let (request_id, receiver, _) = bridge.reserve().unwrap();
		assert!(bridge.complete_json(request_id, "{\"ok\":true}"));
		assert!(!bridge.complete_json(request_id, "{\"ok\":false}"));
		assert_eq!(bridge.stats().duplicate_completions, 1);
		drop(receiver);
	}

	#[test]
	fn bridge_shutdown_marks_unresolved_calls_closed() {
		let bridge = TsfnBridge::for_tests();
		let (_, mut receiver, _) = bridge.reserve().unwrap();
		bridge.shutdown();
		assert!(receiver.try_recv().is_ok());
		assert_eq!(bridge.stats().in_flight, 0);
	}
}
