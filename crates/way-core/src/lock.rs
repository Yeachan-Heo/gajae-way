//! Durable, fenced corpus lease state machine.
//!
//! A lease timeout is only a failure detector. It never transfers write
//! authority by itself: a successor is admitted only after an explicit release,
//! a proof that both the process and process group are gone or reincarnated, a
//! completed revocation, or a verified quarantine clear.

use std::{
	collections::VecDeque,
	fmt,
	io,
	str::FromStr,
	sync::{
		atomic::{AtomicBool, Ordering},
		Arc, Condvar, Mutex,
	},
	time::{Duration, Instant},
};

#[cfg(target_os = "linux")]
use std::fs;


use rusqlite::{params, OptionalExtension, Row};

use crate::{
	events::append_in_transaction,
	store::{meta_get_tx, meta_set_tx, Clock, Store, StoreError, SystemClock},
};

pub const DEFAULT_TTL_MS: u64 = 120_000;
pub const MIN_TTL_MS: u64 = 5_000;
pub const MAX_TTL_MS: u64 = 600_000;
pub const HARD_HOLD_CAP_MS: u64 = 600_000;
pub const DEFAULT_WAIT_MS: u64 = 30_000;
pub const MAX_WAIT_MS: u64 = 300_000;
pub const BATCH_STARVATION_MS: i64 = 60_000;
pub const MAX_CONSECUTIVE_INTERACTIVE_GRANTS: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HolderKind {
	InDaemon,
	External,
}

impl HolderKind {
	pub(crate) fn as_sql(self) -> &'static str {
		match self {
			Self::InDaemon => "in_daemon",
			Self::External => "external",
		}
	}
}

impl FromStr for HolderKind {
	type Err = ();

	fn from_str(value: &str) -> Result<Self, Self::Err> {
		match value {
			"in_daemon" => Ok(Self::InDaemon),
			"external" => Ok(Self::External),
			_ => Err(()),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockClass {
	Interactive,
	Batch,
}

impl LockClass {
	pub(crate) fn as_sql(self) -> &'static str {
		match self {
			Self::Interactive => "interactive",
			Self::Batch => "batch",
		}
	}
}

impl FromStr for LockClass {
	type Err = ();

	fn from_str(value: &str) -> Result<Self, Self::Err> {
		match value {
			"interactive" => Ok(Self::Interactive),
			"batch" => Ok(Self::Batch),
			_ => Err(()),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseState {
	Active,
	Expiring,
	Stuck,
	Quarantined,
	Released,
	Revoked,
}

impl LeaseState {
	pub(crate) fn as_sql(self) -> &'static str {
		match self {
			Self::Active => "active",
			Self::Expiring => "expiring",
			Self::Stuck => "stuck",
			Self::Quarantined => "quarantined",
			Self::Released => "released",
			Self::Revoked => "revoked",
		}
	}

	fn unresolved(self) -> bool {
		matches!(self, Self::Active | Self::Expiring | Self::Stuck | Self::Quarantined)
	}
}

impl FromStr for LeaseState {
	type Err = ();

	fn from_str(value: &str) -> Result<Self, Self::Err> {
		match value {
			"active" => Ok(Self::Active),
			"expiring" => Ok(Self::Expiring),
			"stuck" => Ok(Self::Stuck),
			"quarantined" => Ok(Self::Quarantined),
			"released" => Ok(Self::Released),
			"revoked" => Ok(Self::Revoked),
			_ => Err(()),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseHolder {
	pub holder_kind: HolderKind,
	pub session_id: String,
	pub label: String,
	pub pid: i32,
	/// OS-specific process incarnation value. Linux uses `/proc/<pid>/stat`
	/// starttime; macOS uses libproc's start timeval expressed in microseconds.
	pub pid_start_time: u64,
	pub pgid: i32,
	/// The group-leader incarnation where known. Daemon-owned closure children
	/// use a fresh group and therefore normally set this to `pid_start_time`.
	pub pgid_start_time: Option<u64>,
	pub conn_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquireRequest {
	pub holder: LeaseHolder,
	pub class: LockClass,
	pub wait_ms: u64,
	pub ttl_ms: u64,
}

impl AcquireRequest {
	pub fn new(holder: LeaseHolder) -> Self {
		Self { holder, class: LockClass::Interactive, wait_ms: DEFAULT_WAIT_MS, ttl_ms: DEFAULT_TTL_MS }
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
	pub lease_id: String,
	pub lock_name: String,
	pub holder: LeaseHolder,
	pub class: LockClass,
	pub state: LeaseState,
	pub fencing_token: u64,
	pub ttl_ms: u64,
	pub acquired_at: i64,
	pub expires_at: i64,
	pub hard_expires_at: i64,
	pub released_at: Option<i64>,
	pub release_reason: Option<String>,
	pub quarantine_receipt_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquireResult {
	pub lease_id: String,
	pub fencing_token: u64,
	pub expires_at: i64,
	pub queue_waited_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseResult {
	pub released: bool,
	pub held_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueEntry {
	pub class: LockClass,
	pub label: String,
	pub waited_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockStatus {
	pub held: bool,
	pub holder: Option<Lease>,
	pub expires_at: Option<i64>,
	pub fencing_token: Option<u64>,
	pub queue: Vec<QueueEntry>,
	pub stuck: bool,
	pub quarantined: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessObservation {
	Absent,
	/// The process/group is present. A group has a start time only when its
	/// leader can be inspected; a non-leader group is deliberately not proof.
	Present { start_time: Option<u64> },
	Unprovable,
}

/// Injectable OS observation boundary. The lock manager never assumes that a
/// PID alone identifies an authority holder, which makes PID reuse testable.
pub trait ProcessProbe: Send + Sync {
	fn process(&self, pid: i32) -> ProcessObservation;
	fn process_group(&self, pgid: i32) -> ProcessObservation;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeathCheck {
	ProvenDead,
	Alive,
	Unprovable,
}

/// Injectable in-daemon revocation boundary. The production implementation
/// SIGKILLs the supervised process group; tests make its completion explicit.
pub trait Revoker: Send + Sync {
	fn revoke(&self, lease: &Lease) -> bool;
}

#[derive(Debug, Default)]
pub struct SystemRevoker;

impl Revoker for SystemRevoker {
	fn revoke(&self, lease: &Lease) -> bool {
		#[cfg(unix)]
		{
			// A negative pid targets only the supervised process group. A missing
			// group is a completed revocation, not an error.
			let result = unsafe { libc::kill(-lease.holder.pgid, libc::SIGKILL) };
			if result == 0 {
				return true;
			}
			return io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
		}
		#[cfg(not(unix))]
		{
			let _ = lease;
			false
		}
	}
}

#[derive(Debug, Default)]
pub struct SystemProcessProbe;

impl ProcessProbe for SystemProcessProbe {
	fn process(&self, pid: i32) -> ProcessObservation {
		#[cfg(target_os = "linux")]
		{
			return linux_process(pid);
		}
		#[cfg(target_os = "macos")]
		{
			return macos_process(pid);
		}
		#[cfg(not(any(target_os = "linux", target_os = "macos")))]
		{
			return fallback_process(pid);
		}
	}

	fn process_group(&self, pgid: i32) -> ProcessObservation {
		#[cfg(target_os = "linux")]
		{
			return linux_process_group(pgid);
		}
		#[cfg(target_os = "macos")]
		{
			return macos_process_group(pgid);
		}
		#[cfg(not(any(target_os = "linux", target_os = "macos")))]
		{
			return fallback_process_group(pgid);
		}
	}
}

#[cfg(target_os = "linux")]
fn linux_process(pid: i32) -> ProcessObservation {
	match linux_proc_stat(pid) {
		Ok(Some((_, start_time))) => ProcessObservation::Present { start_time: Some(start_time) },
		Ok(None) => ProcessObservation::Absent,
		Err(()) => ProcessObservation::Unprovable,
	}
}

#[cfg(target_os = "linux")]
fn linux_process_group(pgid: i32) -> ProcessObservation {
	let entries = match fs::read_dir("/proc") {
		Ok(entries) => entries,
		Err(_) => return ProcessObservation::Unprovable,
	};
	let mut unreadable = false;
	let mut leader_start_time = None;
	let mut found_member = false;
	for entry in entries {
		let Ok(entry) = entry else {
			unreadable = true;
			continue;
		};
		let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
			continue;
		};
		let Ok(pid) = name.parse::<i32>() else {
			continue;
		};
		match linux_proc_stat(pid) {
			Ok(Some((member_group, start_time))) if member_group == pgid => {
				found_member = true;
				if pid == pgid {
					leader_start_time = Some(start_time);
				}
			}
			Ok(_) => {}
			Err(()) => unreadable = true,
		}
	}
	if found_member {
		ProcessObservation::Present { start_time: leader_start_time }
	} else if unreadable {
		ProcessObservation::Unprovable
	} else {
		ProcessObservation::Absent
	}
}

#[cfg(target_os = "linux")]
fn linux_proc_stat(pid: i32) -> Result<Option<(i32, u64)>, ()> {
	let path = format!("/proc/{pid}/stat");
	let value = match fs::read_to_string(path) {
		Ok(value) => value,
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
		Err(_) => return Err(()),
	};
	// `comm` can contain spaces and parentheses; the final ')' is the only
	// reliable boundary before field 3.
	let Some(end_of_comm) = value.rfind(')') else {
		return Err(());
	};
	let fields = value[end_of_comm + 1..].split_whitespace().collect::<Vec<_>>();
	let process_group = fields.get(2).ok_or(())?.parse().map_err(|_| ())?;
	let start_time = fields.get(19).ok_or(())?.parse().map_err(|_| ())?;
	Ok(Some((process_group, start_time)))
}

#[cfg(target_os = "macos")]
fn macos_process(pid: i32) -> ProcessObservation {
	let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
	let result = unsafe {
		libc::proc_pidinfo(
			pid,
			libc::PROC_PIDTBSDINFO,
			0,
			info.as_mut_ptr().cast(),
			std::mem::size_of::<libc::proc_bsdinfo>() as i32,
		)
	};
	if result == std::mem::size_of::<libc::proc_bsdinfo>() as i32 {
		let info = unsafe { info.assume_init() };
		let start_time = info
			.pbi_start_tvsec
			.checked_mul(1_000_000)
			.and_then(|seconds| seconds.checked_add(info.pbi_start_tvusec));
		return start_time
			.map(|start_time| ProcessObservation::Present { start_time: Some(start_time) })
			.unwrap_or(ProcessObservation::Unprovable);
	}
	if io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
		ProcessObservation::Absent
	} else {
		ProcessObservation::Unprovable
	}
}

#[cfg(target_os = "macos")]
fn macos_process_group(pgid: i32) -> ProcessObservation {
	let byte_count = unsafe { libc::proc_listpgrppids(pgid, std::ptr::null_mut(), 0) };
	if byte_count < 0 {
		return ProcessObservation::Unprovable;
	}
	if byte_count == 0 {
		return ProcessObservation::Absent;
	}
	let mut pids = vec![0_i32; byte_count as usize / std::mem::size_of::<i32>() + 1];
	let result = unsafe {
		libc::proc_listpgrppids(
			pgid,
			pids.as_mut_ptr().cast(),
			(pids.len() * std::mem::size_of::<i32>()) as i32,
		)
	};
	if result < 0 {
		return ProcessObservation::Unprovable;
	}
	if result == 0 {
		return ProcessObservation::Absent;
	}
	let member_count = result as usize / std::mem::size_of::<i32>();
	let leader = pids.into_iter().take(member_count).find(|pid| *pid == pgid);
	match leader {
		Some(pid) => match macos_process(pid) {
			ProcessObservation::Present { start_time } => ProcessObservation::Present { start_time },
			ProcessObservation::Absent => ProcessObservation::Present { start_time: None },
			ProcessObservation::Unprovable => ProcessObservation::Unprovable,
		},
		None => ProcessObservation::Present { start_time: None },
	}
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn fallback_process(pid: i32) -> ProcessObservation {
	let result = unsafe { libc::kill(pid, 0) };
	if result == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM) {
		ProcessObservation::Present { start_time: None }
	} else if io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
		ProcessObservation::Absent
	} else {
		ProcessObservation::Unprovable
	}
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn fallback_process_group(pgid: i32) -> ProcessObservation {
	let result = unsafe { libc::kill(-pgid, 0) };
	if result == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM) {
		ProcessObservation::Present { start_time: None }
	} else if io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
		ProcessObservation::Absent
	} else {
		ProcessObservation::Unprovable
	}
}

#[derive(Debug)]
pub enum LockError {
	Store(StoreError),
	Journal(String),
	Held,
	WaitTimeout,
	LeaseExpired,
	NotHolder,
	Reentrant { existing_lease_id: String },
	Stuck,
	DirtyIndex,
	HolderUnverified,
	Quarantined,
	InvalidTtl,
	InvalidWait,
	InvalidHolder(String),
	FencingOverflow,
	ConfirmationRequired,
	Cancelled,
}

impl LockError {
	pub const fn code(&self) -> Option<u16> {
		match self {
			Self::Held => Some(1200),
			Self::WaitTimeout => Some(1201),
			Self::LeaseExpired => Some(1202),
			Self::NotHolder => Some(1203),
			Self::Reentrant { .. } => Some(1204),
			Self::Stuck => Some(1205),
			Self::DirtyIndex => Some(1206),
			Self::HolderUnverified => Some(1207),
			Self::Quarantined => Some(1208),
			_ => None,
		}
	}
}

impl fmt::Display for LockError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Store(error) => write!(formatter, "lock store error: {error}"),
			Self::Journal(error) => write!(formatter, "lock journal error: {error}"),
			Self::Held => formatter.write_str("corpus lock is held"),
			Self::WaitTimeout => formatter.write_str("corpus lock wait timed out"),
			Self::LeaseExpired => formatter.write_str("lease has expired or was lost"),
			Self::NotHolder => formatter.write_str("lease does not identify the current holder"),
			Self::Reentrant { existing_lease_id } => write!(formatter, "lock is non-reentrant; existing lease is {existing_lease_id}"),
			Self::Stuck => formatter.write_str("lock liveness is unprovable; write mode is fenced"),
			Self::DirtyIndex => formatter.write_str("corpus git index is dirty"),
			Self::HolderUnverified => formatter.write_str("lock holder termination is not proven"),
			Self::Quarantined => formatter.write_str("corpus is quarantined and write mode is off"),
			Self::InvalidTtl => formatter.write_str("lock TTL must be in 5000..=600000 ms"),
			Self::InvalidWait => formatter.write_str("lock wait_ms must be in 0..=300000 ms"),
			Self::InvalidHolder(message) => write!(formatter, "invalid lock holder: {message}"),
			Self::FencingOverflow => formatter.write_str("fencing token exhausted u64"),
			Self::Cancelled => formatter.write_str("lock wait was cancelled"),
			Self::ConfirmationRequired => formatter.write_str("operator confirmation is required"),
		}
	}
}

impl std::error::Error for LockError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Store(error) => Some(error),
			_ => None,
		}
	}
}

impl From<StoreError> for LockError {
	fn from(error: StoreError) -> Self {
		Self::Store(error)
	}
}

impl From<rusqlite::Error> for LockError {
	fn from(error: rusqlite::Error) -> Self {
		Self::Store(StoreError::from(error))
	}
}

pub type LockResult<T> = Result<T, LockError>;

#[derive(Debug, Clone)]
struct Waiter {
	ticket: u64,
	class: LockClass,
	label: String,
	enqueued_at: i64,
}

#[derive(Debug, Default)]
struct QueueState {
	waiters: VecDeque<Waiter>,
	next_ticket: u64,
	consecutive_interactive_grants: u8,
	consecutive_batch_grants: u8,
}

impl QueueState {
	fn selected_ticket(&self, now: i64) -> Option<u64> {
		let batch_head = self.waiters.iter().find(|waiter| waiter.class == LockClass::Batch);
		let interactive_head = self.waiters.iter().find(|waiter| waiter.class == LockClass::Interactive);
		match (interactive_head, batch_head) {
			(None, None) => None,
			(None, Some(batch)) => Some(batch.ticket),
			(Some(interactive), None) => Some(interactive.ticket),
			(Some(interactive), Some(batch)) => {
				let batch_waited = now.saturating_sub(batch.enqueued_at);
				// A symmetric burst cap is necessary for the stated bounded-service
				// property. The following branch preserves Detail 1's required batch
				// preference after interactive grants or more than 60 seconds of batch waiting.
				if self.consecutive_batch_grants >= MAX_CONSECUTIVE_INTERACTIVE_GRANTS {
					Some(interactive.ticket)
				} else if self.consecutive_interactive_grants >= MAX_CONSECUTIVE_INTERACTIVE_GRANTS
					|| batch_waited > BATCH_STARVATION_MS
				{
					Some(batch.ticket)
				} else {
					Some(interactive.ticket)
				}
			}
		}
	}

	fn record_grant(&mut self, class: LockClass) {
		match class {
			LockClass::Interactive => {
				self.consecutive_interactive_grants = self.consecutive_interactive_grants.saturating_add(1);
				self.consecutive_batch_grants = 0;
			}
			LockClass::Batch => {
				self.consecutive_batch_grants = self.consecutive_batch_grants.saturating_add(1);
				self.consecutive_interactive_grants = 0;
			}
		}
	}
}

#[derive(Debug, Default)]
struct QueueControl {
	state: Mutex<QueueState>,
	changed: Condvar,
}

/// Durable lock authority. Queue waiters intentionally live only in memory;
/// restarting a daemon clears requests but preserves the unresolved lease row.
#[derive(Clone)]
pub struct LockManager {
	store: Store,
	clock: Arc<dyn Clock>,
	process_probe: Arc<dyn ProcessProbe>,
	revoker: Arc<dyn Revoker>,
	queue: Arc<QueueControl>,
	startup_recovery_complete: Arc<AtomicBool>,
}

impl fmt::Debug for LockManager {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.debug_struct("LockManager").field("store", &self.store).finish_non_exhaustive()
	}
}

impl LockManager {
	pub fn new(store: Store) -> Self {
		Self::with_components(
			store,
			Arc::new(SystemClock),
			Arc::new(SystemProcessProbe),
			Arc::new(SystemRevoker),
		)
	}

	pub fn with_components(
		store: Store,
		clock: Arc<dyn Clock>,
		process_probe: Arc<dyn ProcessProbe>,
		revoker: Arc<dyn Revoker>,
	) -> Self {
		Self {
			store,
			clock,
			process_probe,
			revoker,
			queue: Arc::new(QueueControl::default()),
			startup_recovery_complete: Arc::new(AtomicBool::new(false)),
		}
	}

	pub fn acquire(&self, request: AcquireRequest) -> LockResult<AcquireResult> {
		self.acquire_with_cancel(request, None)
	}

	/// Acquires a lock while allowing a connection-owned long-poll to remove its
	/// in-memory waiter as soon as that connection goes away.
	pub fn acquire_cancellable(&self, request: AcquireRequest, cancelled: &AtomicBool) -> LockResult<AcquireResult> {
		self.acquire_with_cancel(request, Some(cancelled))
	}

	fn acquire_with_cancel(&self, request: AcquireRequest, cancelled: Option<&AtomicBool>) -> LockResult<AcquireResult> {
		validate_request(&request)?;
		self.reconcile()?;
		if let Some(lease) = self.current_lease()? {
			if lease.holder.session_id == request.holder.session_id {
				return Err(LockError::Reentrant { existing_lease_id: lease.lease_id });
			}
			match lease.state {
				LeaseState::Quarantined => return Err(LockError::Quarantined),
				LeaseState::Stuck => return Err(LockError::Stuck),
				_ => {}
			}
		}
		if self.write_mode_off()? {
			return Err(if self.current_lease()?.is_some_and(|lease| lease.state == LeaseState::Quarantined) {
				LockError::Quarantined
			} else {
				LockError::Stuck
			});
		}

		if self.queue_is_empty()? && self.current_lease()?.is_none() {
			let result = self.grant(&request, 0)?;
			self.record_grant(request.class)?;
			return Ok(result);
		}
		if request.wait_ms == 0 {
			return Err(LockError::Held);
		}

		let enqueued_at = self.clock.now_ms();
		let ticket = self.enqueue(request.class, request.holder.label.clone(), enqueued_at)?;
		let deadline = Instant::now() + Duration::from_millis(request.wait_ms);
		loop {
			if cancelled.is_some_and(|flag| flag.load(Ordering::Acquire)) {
				self.remove_waiter(ticket)?;
				return Err(LockError::Cancelled);
			}
			self.reconcile()?;
			if let Some(lease) = self.current_lease()? {
				if lease.holder.session_id == request.holder.session_id {
					self.remove_waiter(ticket)?;
					return Err(LockError::Reentrant { existing_lease_id: lease.lease_id });
				}
				match lease.state {
					LeaseState::Quarantined => {
						self.remove_waiter(ticket)?;
						return Err(LockError::Quarantined);
					}
					LeaseState::Stuck => {
						self.remove_waiter(ticket)?;
						return Err(LockError::Stuck);
					}
					_ => {}
				}
			}
			if self.write_mode_off()? {
				self.remove_waiter(ticket)?;
				return Err(LockError::Stuck);
			}
			if self.current_lease()?.is_none() && self.selected_ticket()? == Some(ticket) {
				match self.grant(&request, self.clock.now_ms().saturating_sub(enqueued_at)) {
					Ok(result) => {
						self.finish_grant(ticket, request.class)?;
						return Ok(result);
					}
					Err(LockError::Held) => {}
					Err(error) => {
						self.remove_waiter(ticket)?;
						return Err(error);
					}
				}
			}
			let now = Instant::now();
			if now >= deadline {
				self.remove_waiter(ticket)?;
				return Err(LockError::WaitTimeout);
			}
			let wait_for = deadline.saturating_duration_since(now).min(Duration::from_millis(100));
			let guard = self.queue.state.lock().map_err(|_| LockError::Store(StoreError::Poisoned))?;
			let _ = self.queue.changed.wait_timeout(guard, wait_for).map_err(|_| LockError::Store(StoreError::Poisoned))?;
		}
	}

	pub fn renew(&self, lease_id: &str) -> LockResult<i64> {
		let now = self.clock.now_ms();
		self.reconcile()?;
		let lease = self.lease_by_id(lease_id)?.ok_or(LockError::NotHolder)?;
		if lease.state != LeaseState::Active || now >= lease.expires_at || now >= lease.hard_expires_at {
			return Err(LockError::LeaseExpired);
		}
		let ttl: i64 = lease.ttl_ms.try_into().map_err(|_| LockError::LeaseExpired)?;
		let expires_at = now.checked_add(ttl).ok_or(LockError::LeaseExpired)?.min(lease.hard_expires_at);
		let connection = self.store.connection()?;
		let updated = connection.execute(
			"UPDATE leases SET expires_at = ?2 WHERE lease_id = ?1 AND state = 'active'",
			params![lease_id, expires_at],
		)?;
		if updated != 1 {
			return Err(LockError::LeaseExpired);
		}
		Ok(expires_at)
	}

	pub fn release(&self, lease_id: &str) -> LockResult<ReleaseResult> {
		let now = self.clock.now_ms();
		self.reconcile()?;
		let lease = self.lease_by_id(lease_id)?.ok_or(LockError::NotHolder)?;
		let held_ms = now.saturating_sub(lease.acquired_at);
		if lease.state == LeaseState::Released {
			return if lease.release_reason.as_deref() == Some("explicit") {
				Ok(ReleaseResult { released: false, held_ms })
			} else {
				Err(LockError::LeaseExpired)
			};
		}
		if !lease.state.unresolved() || lease.state != LeaseState::Active || now >= lease.expires_at {
			return Err(LockError::LeaseExpired);
		}
		self.release_lease(lease_id, "explicit", now)?;
		self.notify_waiters();
		Ok(ReleaseResult { released: true, held_ms })
	}

	pub fn force_release(&self, lease_id: &str, confirm: bool) -> LockResult<ReleaseResult> {
		if !confirm {
			return Err(LockError::ConfirmationRequired);
		}
		let lease = self.lease_by_id(lease_id)?.ok_or(LockError::NotHolder)?;
		if !lease.state.unresolved() {
			return Err(LockError::LeaseExpired);
		}
		if lease.state == LeaseState::Quarantined {
			return Err(LockError::Quarantined);
		}
		match self.prove_dead(&lease) {
			DeathCheck::ProvenDead => {}
			DeathCheck::Alive => return Err(LockError::HolderUnverified),
			DeathCheck::Unprovable => {
				self.set_state(&lease.lease_id, LeaseState::Stuck)?;
				return Err(LockError::HolderUnverified);
			}
		}
		let now = self.clock.now_ms();
		self.release_lease(lease_id, "force_proven_death", now)?;
		self.notify_waiters();
		Ok(ReleaseResult { released: true, held_ms: now.saturating_sub(lease.acquired_at) })
	}

	pub fn quarantine_override(&self, lease_id: &str, confirm: bool, acknowledge_unverified: bool) -> LockResult<LockStatus> {
		if !confirm || !acknowledge_unverified {
			return Err(LockError::ConfirmationRequired);
		}
		let lease = self.lease_by_id(lease_id)?.ok_or(LockError::NotHolder)?;
		if !lease.state.unresolved() {
			return Err(LockError::LeaseExpired);
		}
		let now = self.clock.now_ms();
		let mut connection = self.store.connection()?;
		let transaction = connection.transaction()?;
		transaction.execute(
			"UPDATE leases SET state = 'quarantined' WHERE lease_id = ?1 AND state IN ('active', 'expiring', 'stuck')",
			[lease_id],
		)?;
		meta_set_tx(&transaction, "write_mode", "off")?;
		let payload = format!("{{\"action\":\"quarantine_override\",\"lease_id\":{}}}", json_string(lease_id));
		append_in_transaction(&transaction, "lock_event", &payload, now).map_err(|error| LockError::Journal(error.to_string()))?;
		transaction.commit()?;
		drop(connection);
		self.notify_waiters();
		self.status()
	}

	pub fn clear_quarantine(&self, verification_receipt_id: &str, confirm: bool) -> LockResult<LockStatus> {
		if !confirm {
			return Err(LockError::ConfirmationRequired);
		}
		if verification_receipt_id.trim().is_empty() {
			return Err(LockError::InvalidHolder("verification_receipt_id must not be empty".to_owned()));
		}
		let lease = self.current_lease()?.ok_or(LockError::NotHolder)?;
		if lease.state != LeaseState::Quarantined {
			return Err(LockError::NotHolder);
		}
		let now = self.clock.now_ms();
		let mut connection = self.store.connection()?;
		let transaction = connection.transaction()?;
		transaction.execute(
			"UPDATE leases SET state = 'released', released_at = ?2, release_reason = 'quarantine_cleared',
			 quarantine_receipt_id = ?3 WHERE lease_id = ?1 AND state = 'quarantined'",
			params![lease.lease_id, now, verification_receipt_id],
		)?;
		meta_set_tx(&transaction, "write_mode", "on")?;
		let payload = format!(
			"{{\"action\":\"quarantine_cleared\",\"lease_id\":{},\"verification_receipt_id\":{}}}",
			json_string(&lease.lease_id),
			json_string(verification_receipt_id)
		);
		append_in_transaction(&transaction, "lock_event", &payload, now).map_err(|error| LockError::Journal(error.to_string()))?;
		transaction.commit()?;
		drop(connection);
		self.notify_waiters();
		self.status()
	}

	pub fn status(&self) -> LockResult<LockStatus> {
		self.reconcile()?;
		let holder = self.current_lease()?;
		let now = self.clock.now_ms();
		let queue = {
			let queue = self.queue.state.lock().map_err(|_| LockError::Store(StoreError::Poisoned))?;
			queue
				.waiters
				.iter()
				.map(|waiter| QueueEntry {
					class: waiter.class,
					label: waiter.label.clone(),
					waited_ms: now.saturating_sub(waiter.enqueued_at),
				})
				.collect()
		};
		let quarantined = holder.as_ref().is_some_and(|lease| lease.state == LeaseState::Quarantined) || self.write_mode_off()?;
		let stuck = holder.as_ref().is_some_and(|lease| lease.state == LeaseState::Stuck);
		Ok(LockStatus {
			held: holder.is_some(),
			expires_at: holder.as_ref().map(|lease| lease.expires_at),
			fencing_token: holder.as_ref().map(|lease| lease.fencing_token),
			holder,
			queue,
			stuck,
			quarantined,
		})
	}

	/// Advances expiry/revocation state. This is called before every lock API and
	/// can also be driven by the daemon health tick.
	pub fn reconcile(&self) -> LockResult<()> {
		let startup_recovery = !self.startup_recovery_complete.swap(true, Ordering::AcqRel);
		let Some(lease) = self.current_lease()? else {
			return Ok(());
		};
		if lease.state == LeaseState::Quarantined {
			return Ok(());
		}
		let now = self.clock.now_ms();
		// A daemon restart deliberately rechecks a resurrected lease even while
		// its TTL remains in the future. It is still never transferred without
		// proof; a live holder remains fenced by the row.
		if startup_recovery {
			match self.prove_dead(&lease) {
				DeathCheck::ProvenDead => {
					self.release_lease(&lease.lease_id, "startup_proven_death", now)?;
					self.notify_waiters();
					return Ok(());
				}
				DeathCheck::Alive => {}
				DeathCheck::Unprovable => self.set_state(&lease.lease_id, LeaseState::Stuck)?,
			}
		}
		if now > lease.hard_expires_at {
			let revoked = self.revoker.revoke(&lease);
			if revoked && self.prove_dead(&lease) == DeathCheck::ProvenDead {
				self.release_lease(&lease.lease_id, "hard_cap_revoked", now)?;
				self.notify_waiters();
			} else {
				self.set_state(&lease.lease_id, LeaseState::Stuck)?;
			}
			return Ok(());
		}
		if now >= lease.expires_at || matches!(lease.state, LeaseState::Expiring | LeaseState::Stuck) {
			if lease.state == LeaseState::Active {
				self.set_state(&lease.lease_id, LeaseState::Expiring)?;
			}
			match self.prove_dead(&lease) {
				DeathCheck::ProvenDead => {
					self.release_lease(&lease.lease_id, "proven_death", now)?;
					self.notify_waiters();
				}
				DeathCheck::Alive => {}
				DeathCheck::Unprovable => self.set_state(&lease.lease_id, LeaseState::Stuck)?,
			}
		}
		Ok(())
	}

	pub fn prove_dead(&self, lease: &Lease) -> DeathCheck {
		let pid = component_death(self.process_probe.process(lease.holder.pid), Some(lease.holder.pid_start_time));
		let expected_group_start = lease
			.holder
			.pgid_start_time
			.or_else(|| (lease.holder.pid == lease.holder.pgid).then_some(lease.holder.pid_start_time));
		let group = component_death(self.process_probe.process_group(lease.holder.pgid), expected_group_start);
		match (pid, group) {
			(Some(true), Some(true)) => DeathCheck::ProvenDead,
			(Some(false), _) | (_, Some(false)) => DeathCheck::Alive,
			_ => DeathCheck::Unprovable,
		}
	}

	fn grant(&self, request: &AcquireRequest, queue_waited_ms: i64) -> LockResult<AcquireResult> {
		let now = self.clock.now_ms();
		let ttl: i64 = request.ttl_ms.try_into().map_err(|_| LockError::InvalidTtl)?;
		let hard_cap: i64 = HARD_HOLD_CAP_MS.try_into().map_err(|_| LockError::InvalidTtl)?;
		let hard_expires_at = now.checked_add(hard_cap).ok_or(LockError::InvalidTtl)?;
		let expires_at = now.checked_add(ttl).ok_or(LockError::InvalidTtl)?.min(hard_expires_at);
		let mut connection = self.store.connection()?;
		let transaction = connection.transaction()?;
		if meta_get_tx(&transaction, "write_mode")?.as_deref() != Some("on") {
			return Err(LockError::Stuck);
		}
		if let Some(existing) = query_current_lease(&transaction)? {
			if existing.holder.session_id == request.holder.session_id {
				return Err(LockError::Reentrant { existing_lease_id: existing.lease_id });
			}
			return match existing.state {
				LeaseState::Quarantined => Err(LockError::Quarantined),
				LeaseState::Stuck => Err(LockError::Stuck),
				_ => Err(LockError::Held),
			};
		}
		let previous_token = meta_get_tx(&transaction, "lock_fencing_token")?
			.ok_or_else(|| LockError::Store(StoreError::InvalidMetadata("lock_fencing_token is missing".to_owned())))?
			.parse::<u64>()
			.map_err(|_| LockError::Store(StoreError::InvalidMetadata("lock_fencing_token is not a u64".to_owned())))?;
		let fencing_token = previous_token.checked_add(1).ok_or(LockError::FencingOverflow)?;
		let lease_id: String = transaction.query_row("SELECT lower(hex(randomblob(16)))", [], |row| row.get(0))?;
		let group_start = request
			.holder
			.pgid_start_time
			.or_else(|| (request.holder.pid == request.holder.pgid).then_some(request.holder.pid_start_time));
		transaction.execute(
			"INSERT INTO leases(
				lease_id, lock_name, holder_kind, session_id, label, pid, pid_start_time, pgid, pgid_start_time,
				conn_id, class, state, fencing_token, ttl_ms, acquired_at, expires_at, hard_expires_at
			) VALUES (?1, 'corpus', ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'active', ?11, ?12, ?13, ?14, ?15)",
			params![
				lease_id,
				request.holder.holder_kind.as_sql(),
				request.holder.session_id,
				request.holder.label,
				request.holder.pid,
				request.holder.pid_start_time.to_string(),
				request.holder.pgid,
				group_start.map(|value| value.to_string()),
				request.holder.conn_id,
				request.class.as_sql(),
				fencing_token.to_string(),
				request.ttl_ms as i64,
				now,
				expires_at,
				hard_expires_at,
			],
		)?;
		meta_set_tx(&transaction, "lock_fencing_token", &fencing_token.to_string())?;
		transaction.commit()?;
		Ok(AcquireResult { lease_id, fencing_token, expires_at, queue_waited_ms })
	}

	fn current_lease(&self) -> LockResult<Option<Lease>> {
		let connection = self.store.connection()?;
		Ok(query_current_lease(&connection)?)
	}

	fn lease_by_id(&self, lease_id: &str) -> LockResult<Option<Lease>> {
		let connection = self.store.connection()?;
		Ok(query_lease_by_id(&connection, lease_id)?)
	}

	fn release_lease(&self, lease_id: &str, reason: &str, now: i64) -> LockResult<()> {
		let connection = self.store.connection()?;
		connection.execute(
			"UPDATE leases SET state = 'released', released_at = ?2, release_reason = ?3
			 WHERE lease_id = ?1 AND state IN ('active', 'expiring', 'stuck', 'quarantined')",
			params![lease_id, now, reason],
		)?;
		Ok(())
	}

	fn set_state(&self, lease_id: &str, state: LeaseState) -> LockResult<()> {
		let connection = self.store.connection()?;
		connection.execute(
			"UPDATE leases SET state = ?2 WHERE lease_id = ?1 AND state IN ('active', 'expiring', 'stuck')",
			params![lease_id, state.as_sql()],
		)?;
		Ok(())
	}

	fn write_mode_off(&self) -> LockResult<bool> {
		Ok(self.store.get_meta("write_mode")?.as_deref() != Some("on"))
	}

	fn queue_is_empty(&self) -> LockResult<bool> {
		Ok(self.queue.state.lock().map_err(|_| LockError::Store(StoreError::Poisoned))?.waiters.is_empty())
	}

	fn enqueue(&self, class: LockClass, label: String, enqueued_at: i64) -> LockResult<u64> {
		let mut queue = self.queue.state.lock().map_err(|_| LockError::Store(StoreError::Poisoned))?;
		let ticket = queue.next_ticket;
		queue.next_ticket = queue.next_ticket.checked_add(1).ok_or(LockError::FencingOverflow)?;
		queue.waiters.push_back(Waiter { ticket, class, label, enqueued_at });
		Ok(ticket)
	}

	fn selected_ticket(&self) -> LockResult<Option<u64>> {
		let queue = self.queue.state.lock().map_err(|_| LockError::Store(StoreError::Poisoned))?;
		Ok(queue.selected_ticket(self.clock.now_ms()))
	}

	fn remove_waiter(&self, ticket: u64) -> LockResult<()> {
		let mut queue = self.queue.state.lock().map_err(|_| LockError::Store(StoreError::Poisoned))?;
		if let Some(index) = queue.waiters.iter().position(|waiter| waiter.ticket == ticket) {
			queue.waiters.remove(index);
		}
		drop(queue);
		self.notify_waiters();
		Ok(())
	}

	fn finish_grant(&self, ticket: u64, class: LockClass) -> LockResult<()> {
		let mut queue = self.queue.state.lock().map_err(|_| LockError::Store(StoreError::Poisoned))?;
		if let Some(index) = queue.waiters.iter().position(|waiter| waiter.ticket == ticket) {
			queue.waiters.remove(index);
		}
		queue.record_grant(class);
		drop(queue);
		self.notify_waiters();
		Ok(())
	}

	fn record_grant(&self, class: LockClass) -> LockResult<()> {
		let mut queue = self.queue.state.lock().map_err(|_| LockError::Store(StoreError::Poisoned))?;
		queue.record_grant(class);
		Ok(())
	}

	fn notify_waiters(&self) {
		self.queue.changed.notify_all();
	}
}

fn validate_request(request: &AcquireRequest) -> LockResult<()> {
	if !(MIN_TTL_MS..=MAX_TTL_MS).contains(&request.ttl_ms) {
		return Err(LockError::InvalidTtl);
	}
	if request.wait_ms > MAX_WAIT_MS {
		return Err(LockError::InvalidWait);
	}
	if request.holder.holder_kind != HolderKind::InDaemon {
		return Err(LockError::InvalidHolder("external holders ship with the P9 supervisor".to_owned()));
	}
	if request.holder.session_id.trim().is_empty() {
		return Err(LockError::InvalidHolder("session_id must not be empty".to_owned()));
	}
	if request.holder.label.trim().is_empty() {
		return Err(LockError::InvalidHolder("label must not be empty".to_owned()));
	}
	if request.holder.pid <= 0 || request.holder.pgid <= 0 {
		return Err(LockError::InvalidHolder("pid and pgid must be positive".to_owned()));
	}
	Ok(())
}

fn component_death(observation: ProcessObservation, expected_start_time: Option<u64>) -> Option<bool> {
	match observation {
		ProcessObservation::Absent => Some(true),
		ProcessObservation::Present { start_time: Some(actual) } if expected_start_time.is_some_and(|expected| actual != expected) => {
			Some(true)
		}
		ProcessObservation::Present { .. } => Some(false),
		ProcessObservation::Unprovable => None,
	}
}

fn json_string(value: &str) -> String {
	// serde_json string serialization cannot fail; using it keeps lease ids and
	// receipt ids from ever breaking the durable journal payload.
	serde_json::to_string(value).expect("serializing a string cannot fail")
}

fn query_current_lease(connection: &rusqlite::Connection) -> rusqlite::Result<Option<Lease>> {
	connection
		.query_row(
			"SELECT lease_id, lock_name, holder_kind, session_id, label, pid, pid_start_time, pgid, pgid_start_time,
			 conn_id, class, state, fencing_token, ttl_ms, acquired_at, expires_at, hard_expires_at, released_at,
			 release_reason, quarantine_receipt_id
			 FROM leases WHERE lock_name = 'corpus' AND state IN ('active', 'expiring', 'stuck', 'quarantined')
			 ORDER BY acquired_at DESC LIMIT 1",
			[],
			lease_from_row,
		)
		.optional()
}

fn query_lease_by_id(connection: &rusqlite::Connection, lease_id: &str) -> rusqlite::Result<Option<Lease>> {
	connection
		.query_row(
			"SELECT lease_id, lock_name, holder_kind, session_id, label, pid, pid_start_time, pgid, pgid_start_time,
			 conn_id, class, state, fencing_token, ttl_ms, acquired_at, expires_at, hard_expires_at, released_at,
			 release_reason, quarantine_receipt_id FROM leases WHERE lease_id = ?1",
			[lease_id],
			lease_from_row,
		)
		.optional()
}

fn lease_from_row(row: &Row<'_>) -> rusqlite::Result<Lease> {
	let holder_kind = parse_sql_enum::<HolderKind>(row.get::<_, String>(2)?, 2)?;
	let class = parse_sql_enum::<LockClass>(row.get::<_, String>(10)?, 10)?;
	let state = parse_sql_enum::<LeaseState>(row.get::<_, String>(11)?, 11)?;
	Ok(Lease {
		lease_id: row.get(0)?,
		lock_name: row.get(1)?,
		holder: LeaseHolder {
			holder_kind,
			session_id: row.get(3)?,
			label: row.get(4)?,
			pid: row.get(5)?,
			pid_start_time: parse_sql_u64(row.get::<_, String>(6)?, 6)?,
			pgid: row.get(7)?,
			pgid_start_time: row
				.get::<_, Option<String>>(8)?
				.map(|value| parse_sql_u64(value, 8))
				.transpose()?,
			conn_id: row.get(9)?,
		},
		class,
		state,
		fencing_token: parse_sql_u64(row.get::<_, String>(12)?, 12)?,
		ttl_ms: row.get::<_, i64>(13)? as u64,
		acquired_at: row.get(14)?,
		expires_at: row.get(15)?,
		hard_expires_at: row.get(16)?,
		released_at: row.get(17)?,
		release_reason: row.get(18)?,
		quarantine_receipt_id: row.get(19)?,
	})
}

fn parse_sql_enum<T: FromStr<Err = ()>>(value: String, index: usize) -> rusqlite::Result<T> {
	value.parse().map_err(|_| sql_conversion_error(index, format!("unknown enum value {value}")))
}

fn parse_sql_u64(value: String, index: usize) -> rusqlite::Result<u64> {
	value
		.parse()
		.map_err(|_| sql_conversion_error(index, format!("expected u64, found {value}")))
}

fn sql_conversion_error(index: usize, message: String) -> rusqlite::Error {
	rusqlite::Error::FromSqlConversionFailure(
		index,
		rusqlite::types::Type::Text,
		Box::new(io::Error::new(io::ErrorKind::InvalidData, message)),
	)
}

#[cfg(test)]
mod tests {
	use std::{
		collections::HashMap,
		fs,
		path::PathBuf,
		sync::{
			atomic::{AtomicI64, AtomicUsize, Ordering},
			Arc, Mutex,
		},
	};

	use proptest::prelude::*;

	use super::{
		AcquireRequest, Clock, DeathCheck, HolderKind, LeaseHolder, LockClass, LockError, LockManager, ProcessObservation,
		ProcessProbe, QueueState, Revoker, BATCH_STARVATION_MS, HARD_HOLD_CAP_MS,
	};
	use crate::store::Store;

	#[derive(Default)]
	struct FakeClock(AtomicI64);

	impl FakeClock {
		fn advance(&self, ms: i64) {
			self.0.fetch_add(ms, Ordering::Relaxed);
		}
	}

	impl Clock for FakeClock {
		fn now_ms(&self) -> i64 {
			self.0.load(Ordering::Relaxed)
		}
	}

	#[derive(Default)]
	struct FakeProbe {
		processes: Mutex<HashMap<i32, ProcessObservation>>,
		groups: Mutex<HashMap<i32, ProcessObservation>>,
	}

	impl FakeProbe {
		fn set_alive(&self, pid: i32, pgid: i32, start: u64) {
			self.processes.lock().unwrap().insert(pid, ProcessObservation::Present { start_time: Some(start) });
			self.groups.lock().unwrap().insert(pgid, ProcessObservation::Present { start_time: Some(start) });
		}

		fn set_dead(&self, pid: i32, pgid: i32) {
			self.processes.lock().unwrap().insert(pid, ProcessObservation::Absent);
			self.groups.lock().unwrap().insert(pgid, ProcessObservation::Absent);
		}
	}

	impl ProcessProbe for FakeProbe {
		fn process(&self, pid: i32) -> ProcessObservation {
			self.processes.lock().unwrap().get(&pid).cloned().unwrap_or(ProcessObservation::Unprovable)
		}

		fn process_group(&self, pgid: i32) -> ProcessObservation {
			self.groups.lock().unwrap().get(&pgid).cloned().unwrap_or(ProcessObservation::Unprovable)
		}
	}

	struct FakeRevoker {
		probe: Arc<FakeProbe>,
		calls: AtomicUsize,
	}

	impl FakeRevoker {
		fn new(probe: Arc<FakeProbe>) -> Self {
			Self { probe, calls: AtomicUsize::new(0) }
		}
	}

	impl Revoker for FakeRevoker {
		fn revoke(&self, lease: &super::Lease) -> bool {
			self.calls.fetch_add(1, Ordering::Relaxed);
			self.probe.set_dead(lease.holder.pid, lease.holder.pgid);
			true
		}
	}

	fn holder(session_id: &str, pid: i32) -> LeaseHolder {
		LeaseHolder {
			holder_kind: HolderKind::InDaemon,
			session_id: session_id.to_owned(),
			label: format!("closure-{session_id}"),
			pid,
			pid_start_time: 100,
			pgid: pid,
			pgid_start_time: Some(100),
			conn_id: None,
		}
	}

	fn manager(store: Store) -> (LockManager, Arc<FakeClock>, Arc<FakeProbe>, Arc<FakeRevoker>) {
		let clock = Arc::new(FakeClock::default());
		let probe = Arc::new(FakeProbe::default());
		let revoker = Arc::new(FakeRevoker::new(probe.clone()));
		let manager = LockManager::with_components(store, clock.clone(), probe.clone(), revoker.clone());
		(manager, clock, probe, revoker)
	}

	#[cfg(target_os = "macos")]
	#[test]
	fn system_probe_uses_libproc_for_current_process_and_group() {
		let probe = super::SystemProcessProbe;
		assert!(matches!(
			probe.process(std::process::id() as i32),
			ProcessObservation::Present { start_time: Some(_) }
		));
		let pgid = unsafe { libc::getpgrp() };
		assert!(matches!(probe.process_group(pgid), ProcessObservation::Present { .. }));
	}

	fn request(session_id: &str, pid: i32) -> AcquireRequest {
		let mut request = AcquireRequest::new(holder(session_id, pid));
		request.wait_ms = 0;
		request
	}

	#[test]
	fn reentrancy_is_denied_with_1204_and_existing_lease_id() {
		let (manager, _, probe, _) = manager(Store::default());
		probe.set_alive(11, 11, 100);
		let lease = manager.acquire(request("same", 11)).unwrap();
		let error = manager.acquire(request("same", 11)).unwrap_err();
		assert_eq!(error.code(), Some(1204));
		assert!(matches!(error, LockError::Reentrant { existing_lease_id } if existing_lease_id == lease.lease_id));
	}

	#[test]
	fn renew_extends_an_unexpired_lease() {
		let (manager, clock, probe, _) = manager(Store::default());
		probe.set_alive(12, 12, 100);
		let lease = manager.acquire(request("renew", 12)).unwrap();
		clock.advance(1_000);
		let renewed = manager.renew(&lease.lease_id).unwrap();
		assert!(renewed > lease.expires_at);
	}

	#[test]
	fn release_is_idempotent() {
		let (manager, _, probe, _) = manager(Store::default());
		probe.set_alive(13, 13, 100);
		let lease = manager.acquire(request("release", 13)).unwrap();
		assert!(manager.release(&lease.lease_id).unwrap().released);
		assert!(!manager.release(&lease.lease_id).unwrap().released);
	}

	#[test]
	fn fencing_tokens_are_monotonic_across_leases() {
		let (manager, _, probe, _) = manager(Store::default());
		probe.set_alive(14, 14, 100);
		let first = manager.acquire(request("one", 14)).unwrap();
		manager.release(&first.lease_id).unwrap();
		probe.set_alive(15, 15, 100);
		let second = manager.acquire(request("two", 15)).unwrap();
		assert_eq!(second.fencing_token, first.fencing_token + 1);
	}

	#[test]
	fn pid_reuse_start_time_mismatch_is_proven_death_for_pid_and_group() {
		let (manager, clock, probe, _) = manager(Store::default());
		probe.set_alive(16, 16, 100);
		let first = manager.acquire(request("old", 16)).unwrap();
		clock.advance(120_001);
		probe.set_alive(16, 16, 999);
		assert_eq!(manager.prove_dead(&manager.lease_by_id(&first.lease_id).unwrap().unwrap()), DeathCheck::ProvenDead);
		probe.set_alive(17, 17, 100);
		let second = manager.acquire(request("new", 17)).unwrap();
		assert!(second.fencing_token > first.fencing_token);
	}

	#[test]
	fn force_release_requires_death_proof_and_marks_unprovable_liveness_stuck() {
		let (manager, _, probe, _) = manager(Store::default());
		probe.set_alive(26, 26, 100);
		let lease = manager.acquire(request("force", 26)).unwrap();
		let error = manager.force_release(&lease.lease_id, true).unwrap_err();
		assert_eq!(error.code(), Some(1207));
		probe.set_dead(26, 26);
		assert!(manager.force_release(&lease.lease_id, true).unwrap().released);

		let uncertain = manager.acquire(request("uncertain-force", 27)).unwrap();
		let error = manager.force_release(&uncertain.lease_id, true).unwrap_err();
		assert_eq!(error.code(), Some(1207));
		assert!(manager.status().unwrap().stuck);
	}

	#[test]
	fn ttl_expiry_alone_never_admits_a_live_successor() {
		let (manager, clock, probe, _) = manager(Store::default());
		probe.set_alive(18, 18, 100);
		let first = manager.acquire(request("live", 18)).unwrap();
		clock.advance(120_001);
		probe.set_alive(19, 19, 100);
		assert!(matches!(manager.acquire(request("successor", 19)), Err(LockError::Held)));
		let status = manager.status().unwrap();
		assert!(status.held);
		assert_eq!(status.holder.unwrap().lease_id, first.lease_id);
	}

	#[test]
	fn hard_cap_runs_supervised_revocation_before_successor() {
		let (manager, clock, probe, revoker) = manager(Store::default());
		probe.set_alive(20, 20, 100);
		let first = manager.acquire(request("cap", 20)).unwrap();
		clock.advance(HARD_HOLD_CAP_MS as i64 + 1);
		probe.set_alive(21, 21, 100);
		let second = manager.acquire(request("after-cap", 21)).unwrap();
		assert!(second.fencing_token > first.fencing_token);
		assert_eq!(revoker.calls.load(Ordering::Relaxed), 1);
	}

	#[test]
	fn quarantine_fences_acquires_until_a_receipt_clears_it() {
		let (manager, _, probe, _) = manager(Store::default());
		probe.set_alive(22, 22, 100);
		let first = manager.acquire(request("uncertain", 22)).unwrap();
		manager.quarantine_override(&first.lease_id, true, true).unwrap();
		probe.set_alive(23, 23, 100);
		let error = manager.acquire(request("fenced", 23)).unwrap_err();
		assert_eq!(error.code(), Some(1208));
		assert!(manager.status().unwrap().quarantined);
		manager.clear_quarantine("receipt-verified", true).unwrap();
		let second = manager.acquire(request("reopened", 23)).unwrap();
		assert!(second.fencing_token > first.fencing_token);
	}

	#[test]
	fn resurrected_lease_row_is_reaped_only_after_death_proof() {
		let state_dir = temporary_state_dir("resurrection");
		let store = Store::open(&state_dir).unwrap();
		let (first_manager, _, probe, _) = manager(store.clone());
		probe.set_alive(24, 24, 100);
		let first = first_manager.acquire(request("killed-daemon", 24)).unwrap();
		drop(first_manager);
		drop(store);

		let reopened = Store::open(&state_dir).unwrap();
		let (second_manager, _, second_probe, _) = manager(reopened);
		second_probe.set_dead(24, 24);
		second_probe.set_alive(25, 25, 100);
		let second = second_manager.acquire(request("after-kill-9", 25)).unwrap();
		assert!(second.fencing_token > first.fencing_token);
		fs::remove_dir_all(state_dir).unwrap();
	}

	#[test]
	fn batch_head_wins_after_three_interactive_grants_or_sixty_seconds() {
		let mut queue = QueueState::default();
		queue.waiters.push_back(super::Waiter { ticket: 0, class: LockClass::Batch, label: "batch".to_owned(), enqueued_at: 0 });
		for ticket in 1..=3 {
			queue.waiters.push_back(super::Waiter {
				ticket,
				class: LockClass::Interactive,
				label: format!("interactive-{ticket}"),
				enqueued_at: 0,
			});
		}
		for _ in 0..3 {
			let ticket = queue.selected_ticket(0).unwrap();
			assert_ne!(ticket, 0);
			let index = queue.waiters.iter().position(|waiter| waiter.ticket == ticket).unwrap();
			let waiter = queue.waiters.remove(index).unwrap();
			queue.record_grant(waiter.class);
		}
		assert_eq!(queue.selected_ticket(0), Some(0));

		let mut aged = QueueState::default();
		aged.waiters.push_back(super::Waiter { ticket: 4, class: LockClass::Batch, label: "aged".to_owned(), enqueued_at: 0 });
		aged.waiters.push_back(super::Waiter {
			ticket: 5,
			class: LockClass::Interactive,
			label: "interactive".to_owned(),
			enqueued_at: 0,
		});
		assert_eq!(aged.selected_ticket(BATCH_STARVATION_MS + 1), Some(4));
	}

	proptest! {
		#[test]
		fn every_class_head_is_granted_within_three_hold_caps_plus_starvation_window(
			waiters in proptest::collection::vec((any::<bool>(), 0_i64..=HARD_HOLD_CAP_MS as i64), 1..40)
		) {
			let mut queue = QueueState::default();
			for (ticket, (is_batch, _)) in waiters.iter().enumerate() {
				queue.waiters.push_back(super::Waiter {
					ticket: ticket as u64,
					class: if *is_batch { LockClass::Batch } else { LockClass::Interactive },
					label: format!("waiter-{ticket}"),
					enqueued_at: 0,
				});
			}
			let mut head_since = HashMap::new();
			let mut now = 0_i64;
			while !queue.waiters.is_empty() {
				for class in [LockClass::Interactive, LockClass::Batch] {
					if let Some(head) = queue.waiters.iter().find(|waiter| waiter.class == class) {
						head_since.entry(head.ticket).or_insert(now);
					}
				}
				let ticket = queue.selected_ticket(now).unwrap();
				let waited = now.saturating_sub(head_since.remove(&ticket).unwrap());
				prop_assert!(waited <= 3 * HARD_HOLD_CAP_MS as i64 + BATCH_STARVATION_MS);
				let index = queue.waiters.iter().position(|waiter| waiter.ticket == ticket).unwrap();
				let waiter = queue.waiters.remove(index).unwrap();
				queue.record_grant(waiter.class);
				now = now.saturating_add(waiters[ticket as usize].1);
			}
		}
	}

	fn temporary_state_dir(name: &str) -> PathBuf {
		let path = std::env::temp_dir().join(format!("gajae-way-lock-{name}-{}", std::process::id()));
		let _ = fs::remove_dir_all(&path);
		fs::create_dir_all(&path).unwrap();
		path
	}
}
