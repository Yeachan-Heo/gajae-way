//! JSON-RPC method dispatch, application-error mapping, and the fallible TSFN bridge.

use std::{
	collections::{HashMap, VecDeque},
	fmt,
	sync::{
		atomic::{AtomicBool, AtomicU64, Ordering},
		Arc, Mutex,
	},
	time::{Duration, Instant},
};

use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use serde_json::{json, Map, Value};
use tokio::{sync::oneshot, time::Instant as TokioInstant};

use crate::{
	events::{ConsumerClaim, Cursor, DeliveryProof, EventJournal, JournalError, JournalGap, JournalRead},
	lock::{
		AcquireRequest, AcquireResult, HolderKind, Lease, LeaseHolder, LockClass, LockError, LockManager, LockStatus,
		ReleaseResult, IN_DAEMON_EXECUTOR_CONN_ID,
	},
	store::{unix_epoch_ms, Store, StoreError},
};

use super::CancellationToken;

pub const BRIDGE_TIMEOUT_MS: u64 = 30_000;
pub const MAX_BRIDGE_IN_FLIGHT: usize = 64;

pub const MAIN_EVENT_KINDS: &[&str] = &[
	"assistant_message",
	"turn_start",
	"turn_end",
	"gate_open",
	"gate_resolved",
	"health_change",
	"registry_change",
	"lock_event",
	"follow_up_attempted",
	"follow_up_confirmed",
	"profile_approved",
];

static NEXT_CORRELATION_ID: AtomicU64 = AtomicU64::new(1);

/// The complete application error space reserved by the wire contract.
pub const APP_ERROR_CODES: &[(i64, &str)] = &[
	(1000, "unhealthy_failed_closed"),
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
	(1900, "unauthorized"),
];

pub fn app_error_name(code: i64) -> Option<&'static str> {
	APP_ERROR_CODES.iter().find_map(|(candidate, name)| (*candidate == code).then_some(*name))
}

/// A wire-safe JSON-RPC error. `internal` is the only constructor for -32603,
/// ensuring every internal error has a logged correlation identifier.
#[derive(Debug, Clone, PartialEq)]
pub struct RpcError {
	pub code: i64,
	pub message: String,
	pub data: Option<Value>,
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

	/// A deliberately unavailable bridge used by pure Rust dispatch tests.
	pub fn unavailable() -> Self {
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
		let pending_call = PendingBridgeCall {
			deadline: TokioInstant::now() + std::time::Duration::from_millis(BRIDGE_TIMEOUT_MS),
			responder,
		};
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
			Ok(Value::Object(mut object)) if object.contains_key("error") => {
				BridgeCompletion::RemoteError(object.remove("error").unwrap_or(Value::Null))
			}
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
			Self::FailedClosed => "unhealthy",
			Self::Running | Self::Degraded => "healthy",
		}
	}
}

#[derive(Debug, Clone)]
struct GatewayHealth {
	state: GatewayState,
	reason: Option<String>,
}

#[derive(Debug, Clone)]
struct MainSessionStatus {
	turn_state: String,
	follow_up_queue_depth: u64,
	journal_degraded: bool,
}

impl Default for MainSessionStatus {
	fn default() -> Self {
		Self { turn_state: "idle".to_owned(), follow_up_queue_depth: 0, journal_degraded: false }
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
			health.state = state;
			health.reason = reason;
		}
	}

	pub fn set_main_session_status(&self, turn_state: String, follow_up_queue_depth: u64) {
		if let Ok(mut status) = self.main_session.lock() {
			status.turn_state = turn_state;
			status.follow_up_queue_depth = follow_up_queue_depth;
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
		let health = self.health.lock().map_err(|_| RpcError::internal("gateway health lock poisoned"))?.clone();
		if health.state == GatewayState::FailedClosed
			&& method != "way.health"
			&& method != "way.status"
			&& method != "profile.approve" {
			return Err(RpcError::app(1000, health.reason.map(|reason| json!({ "reason": reason }))));
		}

		match method.as_str() {
			"way.health" => self.health_response(params),
			"way.status" => self.status_response(params).await,
			"gitlock.acquire" => {
				self.idempotent(&method, &params, || self.lock_acquire(params.clone(), cancellation.clone())).await
			}
			"gitlock.renew" => self.idempotent(&method, &params, || self.lock_renew(params.clone())).await,
			"gitlock.release" => self.idempotent(&method, &params, || self.lock_release(params.clone())).await,
			"gitlock.status" => self.lock_status(params).await,
			"gitlock.force_release" => {
				self.idempotent(&method, &params, || self.lock_force_release(params.clone())).await
			}
			"gitlock.quarantine_override" => {
				self.idempotent(&method, &params, || self.lock_quarantine_override(params.clone())).await
			}
			"gitlock.clear_quarantine" => {
				self.idempotent(&method, &params, || self.lock_clear_quarantine(params.clone())).await
			}
			"main.events.read" => self.main_events_read(params, cancellation).await,
			"consumer.claim" => self.consumer_claim(params).await,
			"consumer.commit" => self.consumer_commit(params).await,
			method if method.starts_with("main.") || method == "profile.approve" => {
				self.bridge_method(method, params, cancellation).await
			}

			_ => Err(RpcError::method_not_found(&method)),
		}
	}

	fn health_response(&self, params: Value) -> Result<Value, RpcError> {
		ensure_empty_params(&params)?;
		let health = self.health.lock().map_err(|_| RpcError::internal("gateway health lock poisoned"))?.clone();
		let generation = self.store.journal_generation().map_err(store_error)?;
		let mut response = json!({
			"status": health.state.health_status(),
			"state": health.state.as_str(),
			"main": { "resumed": false },
			"boot_epoch": crate::health_info().boot_epoch,
			"journal_generation": generation,
			"version": env!("CARGO_PKG_VERSION"),
			"uptime_ms": self.started_at.elapsed().as_millis() as u64,
		});
		if let Some(reason) = health.reason {
			response.as_object_mut().expect("health response is an object").insert("reason".to_owned(), Value::String(reason));
		}
		Ok(response)
	}

	async fn status_response(&self, params: Value) -> Result<Value, RpcError> {
		ensure_empty_params(&params)?;
		let health = self.health_response(Value::Object(Map::new()))?;
		let main_session = self
			.main_session
			.lock()
			.map_err(|_| RpcError::internal("main session status lock poisoned"))?
			.clone();
		let locks = self.locks.clone();
		let journal = self.journal.clone();
		let store = self.store.clone();
		let (lock_status, head_cursor, write_mode, profile_digest, profile_version) = tokio::task::spawn_blocking(move || {
			let lock_status = locks.status();
			let head_cursor = journal.head_cursor();
			let write_mode = store.get_meta("write_mode");
			let profile_digest = store.get_meta("profile_digest");
			let profile_version = store.get_meta("profile_digest_version");
			(lock_status, head_cursor, write_mode, profile_digest, profile_version)
		})
		.await
		.map_err(|error| RpcError::internal(format!("status worker failed: {error}")))?;
		let lock_status = lock_status.map_err(lock_error)?;
		let head_cursor = head_cursor.map_err(|error| RpcError::internal(format!("journal status failed: {error}")))?;
		let write_mode = write_mode.map_err(store_error)?;
		let profile_digest = profile_digest.map_err(store_error)?;
		let profile_version = profile_version.map_err(store_error)?;
		let queue_len = lock_status.queue.len();
		let mut response = health.as_object().cloned().expect("health response is an object");
		response.insert("turn_state".to_owned(), json!(main_session.turn_state));
		response.insert("follow_up_queue_depth".to_owned(), json!(main_session.follow_up_queue_depth));
		let mut lock = lock_status_json(lock_status);
		lock.as_object_mut().expect("lock status is an object").insert("queue_len".to_owned(), json!(queue_len));
		response.insert("lock".to_owned(), lock);
		response.insert(
			"journal".to_owned(),
			json!({ "head_cursor": head_cursor.to_string(), "degraded": main_session.journal_degraded }),
		);
		response.insert("consumers".to_owned(), json!([]));
		response.insert("reconcile".to_owned(), json!({ "last_ok_at": Value::Null, "cycle_ms": Value::Null, "drift_count": 0 }));
		response.insert("write_mode".to_owned(), json!(write_mode.as_deref() == Some("on")));
		response.insert(
			"profile".to_owned(),
			json!({
				"digest": profile_digest.filter(|digest| digest != "null"),
				"digest_version": profile_version.and_then(|value| value.parse::<u64>().ok()).unwrap_or(0),
				"approved_at": Value::Null,
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
		let cursor = tokio::task::spawn_blocking(move || {
			journal.commit_consumer(&request.consumer_id, &request.claim_id, request.cursor, &request.proofs)
		})
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
				return serde_json::from_str(&response_json)
					.map_err(|_| RpcError::internal("stored idempotency response is invalid JSON"));
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

	async fn lock_acquire(&self, params: Value, cancellation: CancellationToken) -> Result<Value, RpcError> {
		let request = parse_acquire_request(&params)?;
		let locks = self.locks.clone();
		let cancelled = cancellation.atomic_flag();
		let result = tokio::task::spawn_blocking(move || locks.acquire_cancellable(request, &cancelled))
			.await
			.map_err(|error| RpcError::internal(format!("lock acquire worker failed: {error}")))?
			.map_err(lock_error)?;
		Ok(lock_acquire_json(result))
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

fn parse_main_events_read_request(params: &Value) -> Result<MainEventsReadRequest, RpcError> {
	let object = params_object(params)?;
	ensure_allowed_keys(object, &["consumer_id", "cursor", "limit", "wait_ms", "kinds"])?;
	let consumer_id = optional_string(object, "consumer_id")?;
	let cursor = optional_string(object, "cursor")?
		.map(|value| value.parse::<Cursor>().map_err(|_| RpcError::invalid_params("cursor must be journal_generation:seq")))
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
		Some(_) => return Err(RpcError::invalid_params("kinds must be an array of event kinds")),
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

fn remote_bridge_error(error: Value) -> RpcError {
	let Some(object) = error.as_object() else {
		return RpcError::internal("bridge_exception");
	};
	let Some(code) = object.get("code").and_then(Value::as_i64) else {
		return RpcError::internal("bridge_exception");
	};
	let message = object.get("message").and_then(Value::as_str).unwrap_or("bridge_exception");
	if code == -32603 {
		return RpcError::internal(message);
	}
	let data = object.get("data").cloned();
	RpcError { code, message: message.to_owned(), data }
}

fn store_error(error: StoreError) -> RpcError {
	match error {
		StoreError::IdempotencyConflict => RpcError::app(1500, None),
		error => RpcError::internal(format!("store failure: {error}")),
	}
}

fn lock_error(error: LockError) -> RpcError {
	if let Some(code) = error.code() {
		return RpcError::app(i64::from(code), None);
	}
	match error {
		LockError::InvalidTtl
		| LockError::InvalidWait
		| LockError::InvalidHolder(_)
		| LockError::ConfirmationRequired => RpcError::invalid_params(error.to_string()),
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
		Value::Number(number) => number.as_u64().ok_or_else(|| RpcError::invalid_params(format!("{key} must be an unsigned integer"))),
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

fn required_i32(object: &Map<String, Value>, key: &str) -> Result<i32, RpcError> {
	object
		.get(key)
		.and_then(Value::as_i64)
		.and_then(|value| i32::try_from(value).ok())
		.ok_or_else(|| RpcError::invalid_params(format!("{key} must be an i32")))
}

fn parse_acquire_request(params: &Value) -> Result<AcquireRequest, RpcError> {
	let object = params_object(params)?;
	ensure_allowed_keys(object, &["label", "class", "wait_ms", "ttl_ms", "holder", "idempotency_key"])?;
	let label = required_string(object, "label")?.to_owned();
	let class = match object.get("class") {
		None => LockClass::Interactive,
		Some(Value::String(value)) => value.parse().map_err(|_| RpcError::invalid_params("class must be interactive or batch"))?,
		Some(_) => return Err(RpcError::invalid_params("class must be interactive or batch")),
	};
	let wait_ms = optional_u64(object, "wait_ms")?.unwrap_or(crate::lock::DEFAULT_WAIT_MS);
	let ttl_ms = optional_u64(object, "ttl_ms")?.unwrap_or(crate::lock::DEFAULT_TTL_MS);
	let holder_value = object.get("holder").ok_or_else(|| RpcError::invalid_params("holder is required"))?;
	let holder_object = holder_value.as_object().ok_or_else(|| RpcError::invalid_params("holder must be an object"))?;
	ensure_allowed_keys(
		holder_object,
		&["holder_kind", "session_id", "pid", "pid_start_time", "pgid", "pgid_start_time", "conn_id"],
	)?;
	let holder_kind = required_string(holder_object, "holder_kind")?
		.parse::<HolderKind>()
		.map_err(|_| RpcError::invalid_params("holder.holder_kind must be in_daemon"))?;
	let pid_start_time = optional_u64(holder_object, "pid_start_time")?
		.ok_or_else(|| RpcError::invalid_params("holder.pid_start_time is required"))?;
	let holder = LeaseHolder {
		holder_kind,
		session_id: required_string(holder_object, "session_id")?.to_owned(),
		label,
		pid: required_i32(holder_object, "pid")?,
		pid_start_time,
		pgid: required_i32(holder_object, "pgid")?,
		pgid_start_time: optional_u64(holder_object, "pgid_start_time")?,
		conn_id: match holder_object.get("conn_id") {
			None | Some(Value::Null) => None,
			Some(Value::String(value)) if value == IN_DAEMON_EXECUTOR_CONN_ID => {
				return Err(RpcError::invalid_params("holder.conn_id is reserved for the in-daemon executor"));
			}
			Some(Value::String(value)) if !value.is_empty() => Some(value.clone()),
			Some(_) => return Err(RpcError::invalid_params("holder.conn_id must be a non-empty string when present")),
		},
	};
	Ok(AcquireRequest { holder, class, wait_ms, ttl_ms })
}

fn lock_acquire_json(result: AcquireResult) -> Value {
	json!({
		"lease_id": result.lease_id,
		"fencing_token": result.fencing_token.to_string(),
		"expires_at": result.expires_at,
		"queue_waited_ms": result.queue_waited_ms,
	})
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

/// Retained for callers that previously asserted the skeleton boundary.
pub fn unavailable(method: &str) -> String {
	format!("RPC method {method:?} is unavailable")
}

#[cfg(test)]
mod tests {

	use serde_json::json;

	use super::{app_error_name, GatewayState, RpcDispatcher, TsfnBridge, APP_ERROR_CODES, MAX_BRIDGE_IN_FLIGHT};
	use crate::{events::EventJournal, lock::LockManager, store::Store};

	fn dispatcher() -> RpcDispatcher {
		let store = Store::default();
		let locks = LockManager::new(store.clone());
		let journal = EventJournal::new(store.clone());
		RpcDispatcher::new(store, locks, journal, TsfnBridge::unavailable())
	}

	#[test]
	fn app_error_space_has_unique_names_and_codes() {
		for (code, name) in APP_ERROR_CODES {
			assert_eq!(app_error_name(*code), Some(*name));
		}
		assert_eq!(APP_ERROR_CODES.len(), 21);
	}

	#[tokio::test]
	async fn failed_closed_serves_health_status_and_owner_profile_approval_only() {

		let dispatcher = dispatcher();
		dispatcher.set_gateway_state(GatewayState::FailedClosed, Some("profile_drift".to_owned()));
		let cancellation = super::super::CancellationToken::new();
		let health = dispatcher.dispatch("way.health".to_owned(), json!({}), cancellation.clone()).await.unwrap();
		assert_eq!(health["status"], "unhealthy");
		assert_eq!(health["reason"], "profile_drift");
		let error = dispatcher
			.dispatch("gitlock.status".to_owned(), json!({}), cancellation)
			.await
			.unwrap_err();
		assert_eq!(error.code, 1000);
		let approval = dispatcher
			.dispatch("profile.approve".to_owned(), json!({}), super::super::CancellationToken::new())
			.await
			.unwrap_err();
		assert_ne!(approval.code, 1000);
	}

	#[test]
	fn bridge_capacity_is_bounded_before_the_sixty_fifth_request() {
		let bridge = TsfnBridge::unavailable();
		let mut reservations = Vec::new();
		for _ in 0..MAX_BRIDGE_IN_FLIGHT {
			reservations.push(bridge.reserve().unwrap());
		}
		assert!(matches!(bridge.reserve(), Err(super::BridgeFailure::Overloaded)));
		assert_eq!(bridge.stats().overloads, 1);
		drop(reservations);
	}

	#[tokio::test]
	async fn mutating_rpc_replays_idempotency_and_rejects_conflicting_reuse() {
		let dispatcher = dispatcher();
		let params = json!({
			"label": "idempotent",
			"holder": {
				"holder_kind": "in_daemon",
				"session_id": "idempotent-session",
				"pid": std::process::id(),
				"pid_start_time": "0",
				"pgid": std::process::id(),
			},
			"idempotency_key": "same-key",
		});
		let first = dispatcher
			.dispatch("gitlock.acquire".to_owned(), params.clone(), super::super::CancellationToken::new())
			.await
			.unwrap();
		let replay = dispatcher
			.dispatch("gitlock.acquire".to_owned(), params.clone(), super::super::CancellationToken::new())
			.await
			.unwrap();
		assert_eq!(replay, first);
		let mut conflicting = params;
		conflicting.as_object_mut().unwrap().insert("label".to_owned(), json!("different"));
		let error = dispatcher
			.dispatch("gitlock.acquire".to_owned(), conflicting, super::super::CancellationToken::new())
			.await
			.unwrap_err();
		assert_eq!(error.code, 1500);
	}

	#[tokio::test]
	async fn main_event_reads_filter_known_kinds_and_return_generation_gaps() {
		let store = Store::default();
		let locks = LockManager::new(store.clone());
		let journal = EventJournal::new(store.clone());
		let dispatcher = RpcDispatcher::new(store, locks, journal.clone(), TsfnBridge::unavailable());
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
			.dispatch(
				"main.events.read".to_owned(),
				json!({ "cursor": "0:0" }),
				super::super::CancellationToken::new(),
			)
			.await
			.unwrap();
		assert_eq!(gap["gap"]["resync_cursor"], "1:0");
	}

	#[tokio::test]
	async fn consumer_claim_and_commit_dispatch_the_durable_settlement_protocol() {
		let store = Store::default();
		let locks = LockManager::new(store.clone());
		let journal = EventJournal::new(store.clone());
		let dispatcher = RpcDispatcher::new(store, locks, journal.clone(), TsfnBridge::unavailable());
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
		let bridge = TsfnBridge::unavailable();
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
		let bridge = TsfnBridge::unavailable();
		let (request_id, receiver, _) = bridge.reserve().unwrap();
		assert!(bridge.complete_json(request_id, "{\"ok\":true}"));
		assert!(!bridge.complete_json(request_id, "{\"ok\":false}"));
		assert_eq!(bridge.stats().duplicate_completions, 1);
		drop(receiver);
	}

	#[test]
	fn bridge_shutdown_marks_unresolved_calls_closed() {
		let bridge = TsfnBridge::unavailable();
		let (_, mut receiver, _) = bridge.reserve().unwrap();
		bridge.shutdown();
		assert!(receiver.try_recv().is_ok());
		assert_eq!(bridge.stats().in_flight, 0);
	}
}
