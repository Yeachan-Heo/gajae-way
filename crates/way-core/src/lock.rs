//! Durable, fenced corpus lease state machine.
//!
//! A lease timeout is only a failure detector. It never transfers write
//! authority by itself: a successor is admitted only after an explicit release,
//! a proof that both the process and process group are gone or reincarnated, a
//! completed revocation, or a verified quarantine clear.

use std::{
    collections::VecDeque,
    fmt, io,
    str::FromStr,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[cfg(target_os = "linux")]
use std::fs;

use rusqlite::{OptionalExtension, Row, params};

use crate::{
    events::append_in_transaction,
    store::{Clock, Store, StoreError, SystemClock, meta_get_tx, meta_set_tx},
};

pub const DEFAULT_TTL_MS: u64 = 120_000;
pub const MIN_TTL_MS: u64 = 5_000;
pub const MAX_TTL_MS: u64 = 600_000;
pub const HARD_HOLD_CAP_MS: u64 = 600_000;
pub const DEFAULT_WAIT_MS: u64 = 30_000;
pub const MAX_WAIT_MS: u64 = 300_000;
pub const BATCH_STARVATION_MS: i64 = 60_000;
pub const MAX_CONSECUTIVE_INTERACTIVE_GRANTS: u8 = 3;
/// Durable marker written only by the daemon's direct closure executor. RPC
/// lock callers cannot register a process group for fail-stop signalling.
pub const IN_DAEMON_EXECUTOR_CONN_ID: &str = "way.in_daemon_executor.v1";

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
        matches!(
            self,
            Self::Active | Self::Expiring | Self::Stuck | Self::Quarantined
        )
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
        Self {
            holder,
            class: LockClass::Interactive,
            wait_ms: DEFAULT_WAIT_MS,
            ttl_ms: DEFAULT_TTL_MS,
        }
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

/// Operator-attested manual checks recorded before a quarantined corpus can
/// reopen. Runtime liveness is independently re-proven at record and clear.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuarantineReceiptEvidence {
    pub process_inspected: bool,
    pub git_status_checked: bool,
    pub git_log_checked: bool,
    pub git_fsck_checked: bool,
    pub remote_verified: bool,
}

impl QuarantineReceiptEvidence {
    fn complete(self) -> bool {
        self.process_inspected
            && self.git_status_checked
            && self.git_log_checked
            && self.git_fsck_checked
            && self.remote_verified
    }
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
    Present {
        start_time: Option<u64>,
    },
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
        Ok(Some((_, start_time))) => ProcessObservation::Present {
            start_time: Some(start_time),
        },
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
        ProcessObservation::Present {
            start_time: leader_start_time,
        }
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
    let fields = value[end_of_comm + 1..]
        .split_whitespace()
        .collect::<Vec<_>>();
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
            .map(|start_time| ProcessObservation::Present {
                start_time: Some(start_time),
            })
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
    // `proc_listpgrppids` is not reliable for a just-created detached session on
    // every supported macOS release. POSIX group signalling is the authority we
    // actually use for revocation: it proves whether any member remains. The
    // leader PID supplies the incarnation whenever it still exists.
    match macos_process(pgid) {
        ProcessObservation::Present { start_time } => ProcessObservation::Present { start_time },
        ProcessObservation::Absent | ProcessObservation::Unprovable => macos_group_signal(pgid),
    }
}

#[cfg(target_os = "macos")]
fn macos_group_signal(pgid: i32) -> ProcessObservation {
    let result = unsafe { libc::kill(-pgid, 0) };
    if result == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM) {
        ProcessObservation::Present { start_time: None }
    } else if io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
        ProcessObservation::Absent
    } else {
        ProcessObservation::Unprovable
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
            Self::Reentrant { existing_lease_id } => write!(
                formatter,
                "lock is non-reentrant; existing lease is {existing_lease_id}"
            ),
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
        let batch_head = self
            .waiters
            .iter()
            .find(|waiter| waiter.class == LockClass::Batch);
        let interactive_head = self
            .waiters
            .iter()
            .find(|waiter| waiter.class == LockClass::Interactive);
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
                self.consecutive_interactive_grants =
                    self.consecutive_interactive_grants.saturating_add(1);
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
    hard_hold_cap_ms: u64,
    queue: Arc<QueueControl>,
    revocations: Arc<Mutex<Vec<String>>>,
    startup_recovery_complete: Arc<AtomicBool>,
}

impl fmt::Debug for LockManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LockManager")
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

impl LockManager {
    pub fn new(store: Store) -> Self {
        Self::with_components_and_hard_cap(
            store,
            Arc::new(SystemClock),
            Arc::new(SystemProcessProbe),
            Arc::new(SystemRevoker),
            HARD_HOLD_CAP_MS,
        )
    }

    /// The shortened cap is intentionally an in-process/native test seam. The
    /// daemon never supplies it in normal operation.
    pub fn with_hard_hold_cap(store: Store, hard_hold_cap_ms: u64) -> Self {
        Self::with_components_and_hard_cap(
            store,
            Arc::new(SystemClock),
            Arc::new(SystemProcessProbe),
            Arc::new(SystemRevoker),
            hard_hold_cap_ms,
        )
    }

    pub fn with_components(
        store: Store,
        clock: Arc<dyn Clock>,
        process_probe: Arc<dyn ProcessProbe>,
        revoker: Arc<dyn Revoker>,
    ) -> Self {
        Self::with_components_and_hard_cap(store, clock, process_probe, revoker, HARD_HOLD_CAP_MS)
    }

    fn with_components_and_hard_cap(
        store: Store,
        clock: Arc<dyn Clock>,
        process_probe: Arc<dyn ProcessProbe>,
        revoker: Arc<dyn Revoker>,
        hard_hold_cap_ms: u64,
    ) -> Self {
        Self {
            store,
            clock,
            process_probe,
            revoker,
            hard_hold_cap_ms,
            queue: Arc::new(QueueControl::default()),
            revocations: Arc::new(Mutex::new(Vec::new())),
            startup_recovery_complete: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn acquire(&self, request: AcquireRequest) -> LockResult<AcquireResult> {
        self.acquire_with_cancel(request, None)
    }

    /// Acquires a lock while allowing a connection-owned long-poll to remove its
    /// in-memory waiter as soon as that connection goes away.
    pub fn acquire_cancellable(
        &self,
        request: AcquireRequest,
        cancelled: &AtomicBool,
    ) -> LockResult<AcquireResult> {
        self.acquire_with_cancel(request, Some(cancelled))
    }

    fn acquire_with_cancel(
        &self,
        request: AcquireRequest,
        cancelled: Option<&AtomicBool>,
    ) -> LockResult<AcquireResult> {
        validate_request(&request)?;
        self.reconcile()?;
        if let Some(lease) = self.current_lease()? {
            if lease.holder.session_id == request.holder.session_id {
                return Err(LockError::Reentrant {
                    existing_lease_id: lease.lease_id,
                });
            }
            match lease.state {
                LeaseState::Quarantined => return Err(LockError::Quarantined),
                LeaseState::Stuck => return Err(LockError::Stuck),
                _ => {}
            }
        }
        if self.write_mode_off()? {
            return Err(
                if self
                    .current_lease()?
                    .is_some_and(|lease| lease.state == LeaseState::Quarantined)
                {
                    LockError::Quarantined
                } else {
                    LockError::Stuck
                },
            );
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
                    return Err(LockError::Reentrant {
                        existing_lease_id: lease.lease_id,
                    });
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
            let wait_for = deadline
                .saturating_duration_since(now)
                .min(Duration::from_millis(100));
            let guard = self
                .queue
                .state
                .lock()
                .map_err(|_| LockError::Store(StoreError::Poisoned))?;
            let _ = self
                .queue
                .changed
                .wait_timeout(guard, wait_for)
                .map_err(|_| LockError::Store(StoreError::Poisoned))?;
        }
    }

    pub fn renew(&self, lease_id: &str) -> LockResult<i64> {
        let now = self.clock.now_ms();
        self.reconcile()?;
        let lease = self.lease_by_id(lease_id)?.ok_or(LockError::NotHolder)?;
        if lease.state != LeaseState::Active
            || now >= lease.expires_at
            || now >= lease.hard_expires_at
        {
            return Err(LockError::LeaseExpired);
        }
        let ttl: i64 = lease
            .ttl_ms
            .try_into()
            .map_err(|_| LockError::LeaseExpired)?;
        let expires_at = now
            .checked_add(ttl)
            .ok_or(LockError::LeaseExpired)?
            .min(lease.hard_expires_at);
        let mut connection = self.store.connection()?;
        let transaction = connection.transaction()?;
        let updated = transaction.execute(
            "UPDATE leases SET expires_at = ?2 WHERE lease_id = ?1 AND state = 'active'",
            params![lease_id, expires_at],
        )?;
        if updated != 1 {
            return Err(LockError::LeaseExpired);
        }
        let payload = format!(
            "{{\"action\":\"renewed\",\"lease_id\":{},\"expires_at\":{}}}",
            json_string(lease_id),
            expires_at
        );
        append_in_transaction(&transaction, "lock_event", &payload, now)
            .map_err(|error| LockError::Journal(error.to_string()))?;
        transaction.commit()?;
        Ok(expires_at)
    }

    /// Checks the durable lease identity at a closure step boundary. A matching
    /// token alone is insufficient: only the current, active, unexpired lease
    /// authorizes another Git operation.
    pub fn fencing_valid(&self, lease_id: &str, fencing_token: u64) -> LockResult<bool> {
        self.reconcile()?;
        let now = self.clock.now_ms();
        let Some(lease) = self.current_lease()? else {
            return Ok(false);
        };
        Ok(lease.lease_id == lease_id
            && lease.fencing_token == fencing_token
            && lease.state == LeaseState::Active
            && now < lease.expires_at
            && now < lease.hard_expires_at)
    }

    /// Revocations are generated by the durable FSM and consumed by the
    /// in-daemon closure executor, which owns the child handle required to reap
    /// the process group. Draining is idempotent.
    pub fn drain_revocations(&self) -> Vec<String> {
        match self.revocations.lock() {
            Ok(mut revocations) => std::mem::take(&mut *revocations),
            Err(_) => Vec::new(),
        }
    }

    pub fn release(&self, lease_id: &str) -> LockResult<ReleaseResult> {
        let now = self.clock.now_ms();
        self.reconcile()?;
        let lease = self.lease_by_id(lease_id)?.ok_or(LockError::NotHolder)?;
        let held_ms = now.saturating_sub(lease.acquired_at);
        if lease.state == LeaseState::Released {
            return if lease.release_reason.as_deref() == Some("explicit") {
                Ok(ReleaseResult {
                    released: false,
                    held_ms,
                })
            } else {
                Err(LockError::LeaseExpired)
            };
        }
        if !lease.state.unresolved() || lease.state != LeaseState::Active || now >= lease.expires_at
        {
            return Err(LockError::LeaseExpired);
        }
        self.release_lease(lease_id, "explicit", now)?;
        self.notify_waiters();
        Ok(ReleaseResult {
            released: true,
            held_ms,
        })
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
        Ok(ReleaseResult {
            released: true,
            held_ms: now.saturating_sub(lease.acquired_at),
        })
    }

    pub fn quarantine_override(
        &self,
        lease_id: &str,
        confirm: bool,
        acknowledge_unverified: bool,
    ) -> LockResult<LockStatus> {
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
        let payload = format!(
            "{{\"action\":\"quarantine_override\",\"lease_id\":{}}}",
            json_string(lease_id)
        );
        append_in_transaction(&transaction, "lock_event", &payload, now)
            .map_err(|error| LockError::Journal(error.to_string()))?;
        // The quarantine alert shares this commit with the lease row itself, so
        // there is no window in which the corpus is quarantined and silent. The
        // lease state and its announcement are one durable fact, not two.
        if meta_get_tx(&transaction, "alert_lock_quarantined")?.as_deref() != Some("raised") {
            meta_set_tx(&transaction, "alert_lock_quarantined", "raised")?;
            append_in_transaction(
                &transaction,
                "alert_raised",
                "{\"condition\":\"lock_quarantined\",\"reason\":\"quarantine_override\"}",
                now,
            )
            .map_err(|error| LockError::Journal(error.to_string()))?;
        }
        transaction.commit()?;
        drop(connection);
        self.notify_revocation(&lease.lease_id);
        self.notify_waiters();
        self.status()
    }

    /// Records the completed manual Git checks against the exact quarantined
    /// lease. The receipt cannot be forged by choosing an opaque identifier:
    /// this process generates it only after runtime death proof and stores the
    /// recorded holder incarnation with it.
    pub fn record_quarantine_receipt(
        &self,
        lease_id: &str,
        corpus: &str,
        evidence: QuarantineReceiptEvidence,
    ) -> LockResult<String> {
        if corpus != "corpus" {
            return Err(LockError::InvalidHolder(
                "verification receipt corpus must be corpus".to_owned(),
            ));
        }
        if !evidence.complete() {
            return Err(LockError::InvalidHolder(
				"verification receipt must include process inspection and status/log/fsck/remote checks".to_owned(),
			));
        }
        let lease = self.current_lease()?.ok_or(LockError::NotHolder)?;
        if lease.lease_id != lease_id
            || lease.state != LeaseState::Quarantined
            || lease.lock_name != corpus
        {
            return Err(LockError::NotHolder);
        }
        if self.prove_dead(&lease) != DeathCheck::ProvenDead {
            return Err(LockError::HolderUnverified);
        }

        let now = self.clock.now_ms();
        let mut connection = self.store.connection()?;
        let transaction = connection.transaction()?;
        let lock_name = transaction
            .query_row(
                "SELECT lock_name FROM leases WHERE lease_id = ?1 AND state = 'quarantined'",
                [lease_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or(LockError::NotHolder)?;
        if lock_name != corpus {
            return Err(LockError::InvalidHolder(
                "verification receipt corpus does not bind the lease".to_owned(),
            ));
        }
        let token: String =
            transaction.query_row("SELECT lower(hex(randomblob(16)))", [], |row| row.get(0))?;
        let receipt_id = format!("git-verify-{token}");
        transaction.execute(
            "INSERT INTO verification_receipts(
				receipt_id, lease_id, lock_name, pid, pid_start_time, pgid, pgid_start_time,
				process_inspected, git_status_checked, git_log_checked, git_fsck_checked, remote_verified,
				verified_at, consumed_at
			) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, 1, 1, 1, 1, ?8, NULL)",
            params![
                &receipt_id,
                lease_id,
                corpus,
                lease.holder.pid,
                lease.holder.pid_start_time.to_string(),
                lease.holder.pgid,
                lease.holder.pgid_start_time.map(|value| value.to_string()),
                now,
            ],
        )?;
        let payload = format!(
            "{{\"action\":\"verification_receipt_recorded\",\"lease_id\":{},\"corpus\":{},\"verification_receipt_id\":{}}}",
            json_string(lease_id),
            json_string(corpus),
            json_string(&receipt_id)
        );
        append_in_transaction(&transaction, "lock_event", &payload, now)
            .map_err(|error| LockError::Journal(error.to_string()))?;
        transaction.commit()?;
        Ok(receipt_id)
    }

    pub fn clear_quarantine(
        &self,
        verification_receipt_id: &str,
        confirm: bool,
    ) -> LockResult<LockStatus> {
        if !confirm {
            return Err(LockError::ConfirmationRequired);
        }
        if !valid_verification_receipt_id(verification_receipt_id) {
            return Err(LockError::InvalidHolder(
                "verification_receipt_id has an invalid format".to_owned(),
            ));
        }
        let lease = self.current_lease()?.ok_or(LockError::NotHolder)?;
        if lease.state != LeaseState::Quarantined {
            return Err(LockError::NotHolder);
        }
        // Receipt evidence supplements, never substitutes for, a fresh kernel
        // liveness proof. A still-live or unprovable holder keeps the fence on.
        if self.prove_dead(&lease) != DeathCheck::ProvenDead {
            return Err(LockError::HolderUnverified);
        }

        let now = self.clock.now_ms();
        let mut connection = self.store.connection()?;
        let transaction = connection.transaction()?;
        let receipt = transaction
			.query_row(
				"SELECT lease_id, lock_name, pid, pid_start_time, pgid, pgid_start_time,
					process_inspected, git_status_checked, git_log_checked, git_fsck_checked, remote_verified, consumed_at
				 FROM verification_receipts WHERE receipt_id = ?1",
				[verification_receipt_id],
				|row| {
					Ok((
						row.get::<_, String>(0)?,
						row.get::<_, String>(1)?,
						row.get::<_, i32>(2)?,
						row.get::<_, String>(3)?,
						row.get::<_, i32>(4)?,
						row.get::<_, Option<String>>(5)?,
						row.get::<_, i64>(6)?,
						row.get::<_, i64>(7)?,
						row.get::<_, i64>(8)?,
						row.get::<_, i64>(9)?,
						row.get::<_, i64>(10)?,
						row.get::<_, Option<i64>>(11)?,
					))
				},
			)
			.optional()?
			.ok_or_else(|| LockError::InvalidHolder("verification receipt does not exist".to_owned()))?;
        let (
            receipt_lease_id,
            receipt_lock_name,
            receipt_pid,
            receipt_pid_start_time,
            receipt_pgid,
            receipt_pgid_start_time,
            process_inspected,
            git_status_checked,
            git_log_checked,
            git_fsck_checked,
            remote_verified,
            consumed_at,
        ) = receipt;
        if receipt_lease_id != lease.lease_id
            || receipt_lock_name != lease.lock_name
            || receipt_pid != lease.holder.pid
            || receipt_pid_start_time != lease.holder.pid_start_time.to_string()
            || receipt_pgid != lease.holder.pgid
            || receipt_pgid_start_time
                != lease.holder.pgid_start_time.map(|value| value.to_string())
            || process_inspected != 1
            || git_status_checked != 1
            || git_log_checked != 1
            || git_fsck_checked != 1
            || remote_verified != 1
            || consumed_at.is_some()
        {
            return Err(LockError::InvalidHolder(
                "verification receipt does not bind the quarantined corpus lease".to_owned(),
            ));
        }
        let released = transaction.execute(
			"UPDATE leases SET state = 'released', released_at = ?2, release_reason = 'quarantine_cleared',
			 quarantine_receipt_id = ?3 WHERE lease_id = ?1 AND state = 'quarantined'",
			params![lease.lease_id, now, verification_receipt_id],
		)?;
        if released != 1 {
            return Err(LockError::NotHolder);
        }
        let consumed = transaction.execute(
			"UPDATE verification_receipts SET consumed_at = ?2 WHERE receipt_id = ?1 AND consumed_at IS NULL",
			params![verification_receipt_id, now],
		)?;
        if consumed != 1 {
            return Err(LockError::InvalidHolder(
                "verification receipt was already consumed".to_owned(),
            ));
        }
        meta_set_tx(&transaction, "write_mode", "on")?;
        let payload = format!(
            "{{\"action\":\"quarantine_cleared\",\"lease_id\":{},\"verification_receipt_id\":{}}}",
            json_string(&lease.lease_id),
            json_string(verification_receipt_id)
        );
        append_in_transaction(&transaction, "lock_event", &payload, now)
            .map_err(|error| LockError::Journal(error.to_string()))?;
        // Clearing shares this commit with the lease release, exactly as raising
        // shares its commit with the quarantine. Without this the alert stays
        // raised after a documented recovery and `way.status.alerts` reports a
        // condition that no longer exists.
        if meta_get_tx(&transaction, "alert_lock_quarantined")?.as_deref() == Some("raised") {
            meta_set_tx(&transaction, "alert_lock_quarantined", "clear")?;
            append_in_transaction(
                &transaction,
                "alert_cleared",
                "{\"condition\":\"lock_quarantined\",\"reason\":\"quarantine_cleared\"}",
                now,
            )
            .map_err(|error| LockError::Journal(error.to_string()))?;
        }
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
            let queue = self
                .queue
                .state
                .lock()
                .map_err(|_| LockError::Store(StoreError::Poisoned))?;
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
        let quarantined = holder
            .as_ref()
            .is_some_and(|lease| lease.state == LeaseState::Quarantined)
            || self.write_mode_off()?;
        let stuck = holder
            .as_ref()
            .is_some_and(|lease| lease.state == LeaseState::Stuck);
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
        // A v1 holder is daemon-owned. A restarted daemon must fail-stop any
        // residual matching group before it can consider the durable lease dead.
        // A recycled PID/PGID is never signalled: an incarnation mismatch is
        // already proof that the recorded holder is gone.
        if startup_recovery {
            match self.prove_dead(&lease) {
                DeathCheck::ProvenDead => {
                    self.release_lease(&lease.lease_id, "startup_proven_death", now)?;
                    self.notify_waiters();
                    return Ok(());
                }
                DeathCheck::Alive
                    if lease.holder.holder_kind == HolderKind::InDaemon
                        && self.can_revoke_group(&lease) =>
                {
                    self.notify_revocation(&lease.lease_id);
                    if self.revoke_and_prove_dead(&lease) {
                        self.release_lease(&lease.lease_id, "startup_in_daemon_reaped", now)?;
                        self.notify_waiters();
                    } else {
                        self.set_state(&lease.lease_id, LeaseState::Stuck)?;
                    }
                    return Ok(());
                }
                DeathCheck::Alive => {}
                DeathCheck::Unprovable => {
                    self.set_state(&lease.lease_id, LeaseState::Stuck)?;
                    return Ok(());
                }
            }
        }
        if now > lease.hard_expires_at {
            match self.prove_dead(&lease) {
                DeathCheck::ProvenDead => {
                    self.release_lease(&lease.lease_id, "hard_cap_proven_death", now)?;
                    self.notify_waiters();
                }
                _ if self.can_revoke_group(&lease) => {
                    self.notify_revocation(&lease.lease_id);
                    if self.revoke_and_prove_dead(&lease) {
                        self.release_lease(&lease.lease_id, "hard_cap_revoked", now)?;
                        self.notify_waiters();
                    } else {
                        self.set_state(&lease.lease_id, LeaseState::Stuck)?;
                    }
                }
                _ => self.set_state(&lease.lease_id, LeaseState::Stuck)?,
            }
            return Ok(());
        }
        if now >= lease.expires_at
            || matches!(lease.state, LeaseState::Expiring | LeaseState::Stuck)
        {
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
        let pid = component_death(
            self.process_probe.process(lease.holder.pid),
            Some(lease.holder.pid_start_time),
        );
        let expected_group_start = lease.holder.pgid_start_time.or_else(|| {
            (lease.holder.pid == lease.holder.pgid).then_some(lease.holder.pid_start_time)
        });
        let group = component_death(
            self.process_probe.process_group(lease.holder.pgid),
            expected_group_start,
        );
        match (pid, group) {
            (Some(true), Some(true)) => DeathCheck::ProvenDead,
            (Some(false), _) | (_, Some(false)) => DeathCheck::Alive,
            _ => DeathCheck::Unprovable,
        }
    }

    fn can_revoke_group(&self, lease: &Lease) -> bool {
        if lease.holder.holder_kind != HolderKind::InDaemon
            || lease.holder.conn_id.as_deref() != Some(IN_DAEMON_EXECUTOR_CONN_ID)
        {
            return false;
        }
        let expected = lease.holder.pgid_start_time.or_else(|| {
            (lease.holder.pid == lease.holder.pgid).then_some(lease.holder.pid_start_time)
        });
        match self.process_probe.process_group(lease.holder.pgid) {
            ProcessObservation::Present {
                start_time: Some(actual),
            } => expected == Some(actual),
            // A process group can outlive its leader while it reaps descendant Git
            // processes. Once the recorded leader PID is absent, that group ID is
            // still reserved by those descendants and is safe to kill as residual
            // daemon-owned work. An unprovable leader remains fail-closed.
            ProcessObservation::Present { start_time: None }
                if lease.holder.pid == lease.holder.pgid =>
            {
                match self.process_probe.process(lease.holder.pid) {
                    ProcessObservation::Absent => true,
                    ProcessObservation::Present {
                        start_time: Some(actual),
                    } => expected == Some(actual),
                    ProcessObservation::Present { start_time: None }
                    | ProcessObservation::Unprovable => false,
                }
            }
            _ => false,
        }
    }

    fn revoke_and_prove_dead(&self, lease: &Lease) -> bool {
        if !self.revoker.revoke(lease) {
            return false;
        }
        for attempt in 0..=25 {
            if self.prove_dead(lease) == DeathCheck::ProvenDead {
                return true;
            }
            if attempt < 25 {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        false
    }

    fn grant(&self, request: &AcquireRequest, queue_waited_ms: i64) -> LockResult<AcquireResult> {
        let now = self.clock.now_ms();
        let ttl: i64 = request
            .ttl_ms
            .try_into()
            .map_err(|_| LockError::InvalidTtl)?;
        let hard_cap: i64 = self
            .hard_hold_cap_ms
            .try_into()
            .map_err(|_| LockError::InvalidTtl)?;
        let hard_expires_at = now.checked_add(hard_cap).ok_or(LockError::InvalidTtl)?;
        let expires_at = now
            .checked_add(ttl)
            .ok_or(LockError::InvalidTtl)?
            .min(hard_expires_at);
        let mut connection = self.store.connection()?;
        let transaction = connection.transaction()?;
        if meta_get_tx(&transaction, "write_mode")?.as_deref() != Some("on") {
            return Err(LockError::Stuck);
        }
        if let Some(existing) = query_current_lease(&transaction)? {
            if existing.holder.session_id == request.holder.session_id {
                return Err(LockError::Reentrant {
                    existing_lease_id: existing.lease_id,
                });
            }
            return match existing.state {
                LeaseState::Quarantined => Err(LockError::Quarantined),
                LeaseState::Stuck => Err(LockError::Stuck),
                _ => Err(LockError::Held),
            };
        }
        let previous_token = meta_get_tx(&transaction, "lock_fencing_token")?
            .ok_or_else(|| {
                LockError::Store(StoreError::InvalidMetadata(
                    "lock_fencing_token is missing".to_owned(),
                ))
            })?
            .parse::<u64>()
            .map_err(|_| {
                LockError::Store(StoreError::InvalidMetadata(
                    "lock_fencing_token is not a u64".to_owned(),
                ))
            })?;
        let fencing_token = previous_token
            .checked_add(1)
            .ok_or(LockError::FencingOverflow)?;
        let lease_id: String =
            transaction.query_row("SELECT lower(hex(randomblob(16)))", [], |row| row.get(0))?;
        let group_start = request.holder.pgid_start_time.or_else(|| {
            (request.holder.pid == request.holder.pgid).then_some(request.holder.pid_start_time)
        });
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
        meta_set_tx(
            &transaction,
            "lock_fencing_token",
            &fencing_token.to_string(),
        )?;
        let payload = format!(
            "{{\"action\":\"acquired\",\"lease_id\":{},\"fencing_token\":{}}}",
            json_string(&lease_id),
            json_string(&fencing_token.to_string())
        );
        append_in_transaction(&transaction, "lock_event", &payload, now)
            .map_err(|error| LockError::Journal(error.to_string()))?;
        transaction.commit()?;
        Ok(AcquireResult {
            lease_id,
            fencing_token,
            expires_at,
            queue_waited_ms,
        })
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
        let mut connection = self.store.connection()?;
        let transaction = connection.transaction()?;
        let updated = transaction.execute(
            "UPDATE leases SET state = 'released', released_at = ?2, release_reason = ?3
			 WHERE lease_id = ?1 AND state IN ('active', 'expiring', 'stuck', 'quarantined')",
            params![lease_id, now, reason],
        )?;
        if updated != 1 {
            return Err(LockError::LeaseExpired);
        }
        let payload = format!(
            "{{\"action\":\"released\",\"lease_id\":{},\"reason\":{}}}",
            json_string(lease_id),
            json_string(reason)
        );
        append_in_transaction(&transaction, "lock_event", &payload, now)
            .map_err(|error| LockError::Journal(error.to_string()))?;
        transaction.commit()?;
        Ok(())
    }

    fn set_state(&self, lease_id: &str, state: LeaseState) -> LockResult<()> {
        let now = self.clock.now_ms();
        let mut connection = self.store.connection()?;
        let transaction = connection.transaction()?;
        let updated = transaction.execute(
			"UPDATE leases SET state = ?2 WHERE lease_id = ?1 AND state IN ('active', 'expiring', 'stuck') AND state <> ?2",
			params![lease_id, state.as_sql()],
		)?;
        if updated == 1 {
            let payload = format!(
                "{{\"action\":\"state_changed\",\"lease_id\":{},\"state\":{}}}",
                json_string(lease_id),
                json_string(state.as_sql())
            );
            append_in_transaction(&transaction, "lock_event", &payload, now)
                .map_err(|error| LockError::Journal(error.to_string()))?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn write_mode_off(&self) -> LockResult<bool> {
        Ok(self.store.get_meta("write_mode")?.as_deref() != Some("on"))
    }

    fn queue_is_empty(&self) -> LockResult<bool> {
        Ok(self
            .queue
            .state
            .lock()
            .map_err(|_| LockError::Store(StoreError::Poisoned))?
            .waiters
            .is_empty())
    }

    fn enqueue(&self, class: LockClass, label: String, enqueued_at: i64) -> LockResult<u64> {
        let mut queue = self
            .queue
            .state
            .lock()
            .map_err(|_| LockError::Store(StoreError::Poisoned))?;
        let ticket = queue.next_ticket;
        queue.next_ticket = queue
            .next_ticket
            .checked_add(1)
            .ok_or(LockError::FencingOverflow)?;
        queue.waiters.push_back(Waiter {
            ticket,
            class,
            label,
            enqueued_at,
        });
        Ok(ticket)
    }

    fn selected_ticket(&self) -> LockResult<Option<u64>> {
        let queue = self
            .queue
            .state
            .lock()
            .map_err(|_| LockError::Store(StoreError::Poisoned))?;
        Ok(queue.selected_ticket(self.clock.now_ms()))
    }

    fn remove_waiter(&self, ticket: u64) -> LockResult<()> {
        let mut queue = self
            .queue
            .state
            .lock()
            .map_err(|_| LockError::Store(StoreError::Poisoned))?;
        if let Some(index) = queue
            .waiters
            .iter()
            .position(|waiter| waiter.ticket == ticket)
        {
            queue.waiters.remove(index);
        }
        drop(queue);
        self.notify_waiters();
        Ok(())
    }

    fn finish_grant(&self, ticket: u64, class: LockClass) -> LockResult<()> {
        let mut queue = self
            .queue
            .state
            .lock()
            .map_err(|_| LockError::Store(StoreError::Poisoned))?;
        if let Some(index) = queue
            .waiters
            .iter()
            .position(|waiter| waiter.ticket == ticket)
        {
            queue.waiters.remove(index);
        }
        queue.record_grant(class);
        drop(queue);
        self.notify_waiters();
        Ok(())
    }

    fn record_grant(&self, class: LockClass) -> LockResult<()> {
        let mut queue = self
            .queue
            .state
            .lock()
            .map_err(|_| LockError::Store(StoreError::Poisoned))?;
        queue.record_grant(class);
        Ok(())
    }

    fn notify_revocation(&self, lease_id: &str) {
        if let Ok(mut revocations) = self.revocations.lock() {
            if !revocations.iter().any(|candidate| candidate == lease_id) {
                revocations.push(lease_id.to_owned());
            }
        }
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
        return Err(LockError::InvalidHolder(
            "external holders ship with the P9 supervisor".to_owned(),
        ));
    }
    if request.holder.conn_id.as_deref() != Some(IN_DAEMON_EXECUTOR_CONN_ID) {
        return Err(LockError::InvalidHolder(
            "in-daemon leases must be owned by the supervised closure executor".to_owned(),
        ));
    }
    if request.holder.pgid_start_time.is_none() {
        return Err(LockError::InvalidHolder(
            "in-daemon leases require a process-group start-time incarnation".to_owned(),
        ));
    }
    if request.holder.session_id.trim().is_empty() {
        return Err(LockError::InvalidHolder(
            "session_id must not be empty".to_owned(),
        ));
    }
    if request.holder.label.trim().is_empty() {
        return Err(LockError::InvalidHolder(
            "label must not be empty".to_owned(),
        ));
    }
    if request.holder.pid <= 0 || request.holder.pgid <= 0 {
        return Err(LockError::InvalidHolder(
            "pid and pgid must be positive".to_owned(),
        ));
    }
    Ok(())
}

fn component_death(
    observation: ProcessObservation,
    expected_start_time: Option<u64>,
) -> Option<bool> {
    match observation {
        ProcessObservation::Absent => Some(true),
        ProcessObservation::Present {
            start_time: Some(actual),
        } if expected_start_time.is_some_and(|expected| actual != expected) => Some(true),
        ProcessObservation::Present { .. } => Some(false),
        ProcessObservation::Unprovable => None,
    }
}

fn json_string(value: &str) -> String {
    // serde_json string serialization cannot fail; using it keeps lease ids and
    // receipt ids from ever breaking the durable journal payload.
    serde_json::to_string(value).expect("serializing a string cannot fail")
}

fn valid_verification_receipt_id(value: &str) -> bool {
    value.strip_prefix("git-verify-").is_some_and(|token| {
        token.len() == 32
            && token
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    })
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

fn query_lease_by_id(
    connection: &rusqlite::Connection,
    lease_id: &str,
) -> rusqlite::Result<Option<Lease>> {
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
    value
        .parse()
        .map_err(|_| sql_conversion_error(index, format!("unknown enum value {value}")))
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
            Arc, Mutex,
            atomic::{AtomicI64, AtomicUsize, Ordering},
        },
    };

    use proptest::prelude::*;

    use super::{
        AcquireRequest, BATCH_STARVATION_MS, Clock, DeathCheck, HARD_HOLD_CAP_MS, HolderKind,
        IN_DAEMON_EXECUTOR_CONN_ID, LeaseHolder, LockClass, LockError, LockManager,
        ProcessObservation, ProcessProbe, QuarantineReceiptEvidence, QueueState, Revoker,
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
            self.processes.lock().unwrap().insert(
                pid,
                ProcessObservation::Present {
                    start_time: Some(start),
                },
            );
            self.groups.lock().unwrap().insert(
                pgid,
                ProcessObservation::Present {
                    start_time: Some(start),
                },
            );
        }

        fn set_dead(&self, pid: i32, pgid: i32) {
            self.processes
                .lock()
                .unwrap()
                .insert(pid, ProcessObservation::Absent);
            self.groups
                .lock()
                .unwrap()
                .insert(pgid, ProcessObservation::Absent);
        }
    }

    impl ProcessProbe for FakeProbe {
        fn process(&self, pid: i32) -> ProcessObservation {
            self.processes
                .lock()
                .unwrap()
                .get(&pid)
                .cloned()
                .unwrap_or(ProcessObservation::Unprovable)
        }

        fn process_group(&self, pgid: i32) -> ProcessObservation {
            self.groups
                .lock()
                .unwrap()
                .get(&pgid)
                .cloned()
                .unwrap_or(ProcessObservation::Unprovable)
        }
    }

    struct FakeRevoker {
        probe: Arc<FakeProbe>,
        calls: AtomicUsize,
    }

    impl FakeRevoker {
        fn new(probe: Arc<FakeProbe>) -> Self {
            Self {
                probe,
                calls: AtomicUsize::new(0),
            }
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
            conn_id: Some(IN_DAEMON_EXECUTOR_CONN_ID.to_owned()),
        }
    }

    fn manager(
        store: Store,
    ) -> (
        LockManager,
        Arc<FakeClock>,
        Arc<FakeProbe>,
        Arc<FakeRevoker>,
    ) {
        let clock = Arc::new(FakeClock::default());
        let probe = Arc::new(FakeProbe::default());
        let revoker = Arc::new(FakeRevoker::new(probe.clone()));
        let manager =
            LockManager::with_components(store, clock.clone(), probe.clone(), revoker.clone());
        (manager, clock, probe, revoker)
    }

    /// Clearing must share a commit with the release, exactly as raising shares
    /// one with the quarantine. A sticky alert after a documented recovery
    /// trains an operator to ignore alerts.
    ///
    /// Deliberately not platform-gated: it uses FakeProbe and an in-memory
    /// Store, and gating it would mean the Linux CI majority never executes the
    /// contract it protects.
    #[test]
    fn clearing_quarantine_also_clears_its_alert_in_the_same_commit() {
        let store = Store::default();
        let (manager, _, probe, _) = manager(store.clone());
        probe.set_alive(43, 43, 100);
        let acquired = manager.acquire(request("quarantine-clear", 43)).unwrap();
        let lease_id = acquired.lease_id;
        manager.quarantine_override(&lease_id, true, true).unwrap();
        assert!(crate::alerts::is_raised(&store, "lock_quarantined").unwrap());

        // A receipt is only accepted once the holder is provably gone.
        probe.set_dead(43, 43);
        let receipt_id = manager
            .record_quarantine_receipt(
                &lease_id,
                "corpus",
                QuarantineReceiptEvidence {
                    process_inspected: true,
                    git_status_checked: true,
                    git_log_checked: true,
                    git_fsck_checked: true,
                    remote_verified: true,
                },
            )
            .expect("receipt recorded");
        manager.clear_quarantine(&receipt_id, true).unwrap();

        assert!(!crate::alerts::is_raised(&store, "lock_quarantined").unwrap());
        let connection = store.connection().unwrap();
        let cleared: i64 = connection
            .query_row("SELECT COUNT(*) FROM events WHERE kind = 'alert_cleared'", [], |row| row.get(0))
            .unwrap();
        assert_eq!(cleared, 1, "recovery must announce exactly once");
    }

    /// The quarantine alert must be one durable fact with the lease row.
    ///
    /// If they were separate commits, a crash between them would leave the
    /// corpus quarantined and silent - the exact condition an operator must be
    /// told about.
    #[test]
    fn quarantine_override_raises_its_alert_in_the_same_commit() {
        let store = Store::default();
        let (manager, _, probe, _) = manager(store.clone());
        probe.set_alive(41, 41, 100);
        let acquired = manager.acquire(request("quarantine-alert", 41)).unwrap();
        let lease_id = acquired.lease_id;

        manager.quarantine_override(&lease_id, true, true).unwrap();

        let connection = store.connection().unwrap();
        let alert: String = connection
            .query_row("SELECT v FROM gateway_meta WHERE k = 'alert_lock_quarantined'", [], |row| row.get(0))
            .unwrap();
        assert_eq!(alert, "raised");
        let raised: i64 = connection
            .query_row("SELECT COUNT(*) FROM events WHERE kind = 'alert_raised'", [], |row| row.get(0))
            .unwrap();
        assert_eq!(raised, 1, "quarantine must announce itself exactly once");
        let quarantined: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM leases WHERE lease_id = ?1 AND state = 'quarantined'",
                [&lease_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(quarantined, 1, "the lease row and its alert are one durable fact");
    }

    #[test]
    fn system_probe_uses_libproc_for_current_process_and_group() {
        let probe = super::SystemProcessProbe;
        assert!(matches!(
            probe.process(std::process::id() as i32),
            ProcessObservation::Present {
                start_time: Some(_)
            }
        ));
        let pgid = unsafe { libc::getpgrp() };
        assert!(matches!(
            probe.process_group(pgid),
            ProcessObservation::Present { .. }
        ));
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
        assert!(
            matches!(error, LockError::Reentrant { existing_lease_id } if existing_lease_id == lease.lease_id)
        );
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
    fn fencing_validity_requires_the_current_active_lease_and_exact_token() {
        let (manager, _, probe, _) = manager(Store::default());
        probe.set_alive(141, 141, 100);
        let lease = manager.acquire(request("fence", 141)).unwrap();
        assert!(
            manager
                .fencing_valid(&lease.lease_id, lease.fencing_token)
                .unwrap()
        );
        assert!(
            !manager
                .fencing_valid(&lease.lease_id, lease.fencing_token + 1)
                .unwrap()
        );
        manager.release(&lease.lease_id).unwrap();
        assert!(
            !manager
                .fencing_valid(&lease.lease_id, lease.fencing_token)
                .unwrap()
        );
    }

    #[test]
    fn quarantine_emits_a_revocation_for_the_in_daemon_executor() {
        let (manager, _, probe, _) = manager(Store::default());
        probe.set_alive(142, 142, 100);
        let lease = manager.acquire(request("quarantine-notify", 142)).unwrap();
        manager
            .quarantine_override(&lease.lease_id, true, true)
            .unwrap();
        assert_eq!(manager.drain_revocations(), vec![lease.lease_id]);
        assert!(manager.drain_revocations().is_empty());
    }

    #[test]
    fn pid_reuse_start_time_mismatch_is_proven_death_for_pid_and_group() {
        let (manager, clock, probe, _) = manager(Store::default());
        probe.set_alive(16, 16, 100);
        let first = manager.acquire(request("old", 16)).unwrap();
        clock.advance(120_001);
        probe.set_alive(16, 16, 999);
        assert_eq!(
            manager.prove_dead(&manager.lease_by_id(&first.lease_id).unwrap().unwrap()),
            DeathCheck::ProvenDead
        );
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
        assert!(
            manager
                .force_release(&lease.lease_id, true)
                .unwrap()
                .released
        );

        let uncertain = manager.acquire(request("uncertain-force", 27)).unwrap();
        let error = manager
            .force_release(&uncertain.lease_id, true)
            .unwrap_err();
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
        assert!(matches!(
            manager.acquire(request("successor", 19)),
            Err(LockError::Held)
        ));
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
    fn quarantine_fences_acquires_until_a_bound_receipt_and_fresh_death_proof_clear_it() {
        let (manager, _, probe, _) = manager(Store::default());
        probe.set_alive(22, 22, 100);
        let first = manager.acquire(request("uncertain", 22)).unwrap();
        manager
            .quarantine_override(&first.lease_id, true, true)
            .unwrap();
        probe.set_alive(23, 23, 100);
        let error = manager.acquire(request("fenced", 23)).unwrap_err();
        assert_eq!(error.code(), Some(1208));
        assert!(manager.status().unwrap().quarantined);
        let error = manager
            .clear_quarantine("git-verify-00000000000000000000000000000000", true)
            .unwrap_err();
        assert_eq!(error.code(), Some(1207));
        let error = manager
            .record_quarantine_receipt(
                &first.lease_id,
                "corpus",
                QuarantineReceiptEvidence {
                    process_inspected: true,
                    git_status_checked: true,
                    git_log_checked: true,
                    git_fsck_checked: true,
                    remote_verified: true,
                },
            )
            .unwrap_err();
        assert_eq!(error.code(), Some(1207));
        probe.set_dead(22, 22);
        let receipt = manager
            .record_quarantine_receipt(
                &first.lease_id,
                "corpus",
                QuarantineReceiptEvidence {
                    process_inspected: true,
                    git_status_checked: true,
                    git_log_checked: true,
                    git_fsck_checked: true,
                    remote_verified: true,
                },
            )
            .unwrap();
        assert!(receipt.starts_with("git-verify-"));
        manager.clear_quarantine(&receipt, true).unwrap();
        let second = manager.acquire(request("reopened", 23)).unwrap();
        assert!(second.fencing_token > first.fencing_token);
    }

    #[test]
    fn quarantine_receipt_is_bound_to_the_quarantined_lease_and_cannot_clear_a_successor_quarantine()
     {
        let (manager, _, probe, _) = manager(Store::default());
        probe.set_alive(28, 28, 100);
        let first = manager.acquire(request("first-quarantine", 28)).unwrap();
        manager
            .quarantine_override(&first.lease_id, true, true)
            .unwrap();
        probe.set_dead(28, 28);
        let first_receipt = manager
            .record_quarantine_receipt(
                &first.lease_id,
                "corpus",
                QuarantineReceiptEvidence {
                    process_inspected: true,
                    git_status_checked: true,
                    git_log_checked: true,
                    git_fsck_checked: true,
                    remote_verified: true,
                },
            )
            .unwrap();
        manager.clear_quarantine(&first_receipt, true).unwrap();

        probe.set_alive(29, 29, 100);
        let second = manager.acquire(request("second-quarantine", 29)).unwrap();
        manager
            .quarantine_override(&second.lease_id, true, true)
            .unwrap();
        probe.set_dead(29, 29);
        let error = manager.clear_quarantine(&first_receipt, true).unwrap_err();
        assert!(matches!(error, LockError::InvalidHolder(_)));
        assert!(manager.status().unwrap().quarantined);
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
    fn startup_recovery_kills_a_matching_in_daemon_group_before_admitting_a_successor() {
        let state_dir = temporary_state_dir("startup-revocation");
        let store = Store::open(&state_dir).unwrap();
        let (first_manager, _, first_probe, _) = manager(store.clone());
        first_probe.set_alive(241, 241, 100);
        let first = first_manager
            .acquire(request("daemon-killed", 241))
            .unwrap();
        drop(first_manager);
        drop(store);

        let reopened = Store::open(&state_dir).unwrap();
        let (second_manager, _, second_probe, second_revoker) = manager(reopened);
        second_probe.set_alive(241, 241, 100);
        second_probe.set_alive(242, 242, 100);
        let successor = second_manager
            .acquire(request("after-startup-reap", 242))
            .unwrap();
        assert!(successor.fencing_token > first.fencing_token);
        assert_eq!(second_revoker.calls.load(Ordering::Relaxed), 1);
        fs::remove_dir_all(state_dir).unwrap();
    }

    #[test]
    fn startup_recovery_reaps_a_leaderless_residual_group() {
        let state_dir = temporary_state_dir("startup-leaderless-group");
        let store = Store::open(&state_dir).unwrap();
        let (first_manager, _, first_probe, _) = manager(store.clone());
        first_probe.set_alive(251, 251, 100);
        let first = first_manager
            .acquire(request("daemon-descendant", 251))
            .unwrap();
        drop(first_manager);
        drop(store);

        let reopened = Store::open(&state_dir).unwrap();
        let (second_manager, _, second_probe, second_revoker) = manager(reopened);
        second_probe
            .processes
            .lock()
            .unwrap()
            .insert(251, ProcessObservation::Absent);
        second_probe
            .groups
            .lock()
            .unwrap()
            .insert(251, ProcessObservation::Present { start_time: None });
        second_probe.set_alive(252, 252, 100);
        let successor = second_manager
            .acquire(request("after-descendant-reap", 252))
            .unwrap();
        assert!(successor.fencing_token > first.fencing_token);
        assert_eq!(second_revoker.calls.load(Ordering::Relaxed), 1);
        fs::remove_dir_all(state_dir).unwrap();
    }

    #[test]
    fn batch_head_wins_after_three_interactive_grants_or_sixty_seconds() {
        let mut queue = QueueState::default();
        queue.waiters.push_back(super::Waiter {
            ticket: 0,
            class: LockClass::Batch,
            label: "batch".to_owned(),
            enqueued_at: 0,
        });
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
            let index = queue
                .waiters
                .iter()
                .position(|waiter| waiter.ticket == ticket)
                .unwrap();
            let waiter = queue.waiters.remove(index).unwrap();
            queue.record_grant(waiter.class);
        }
        assert_eq!(queue.selected_ticket(0), Some(0));

        let mut aged = QueueState::default();
        aged.waiters.push_back(super::Waiter {
            ticket: 4,
            class: LockClass::Batch,
            label: "aged".to_owned(),
            enqueued_at: 0,
        });
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
        let path =
            std::env::temp_dir().join(format!("gajae-way-lock-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        path
    }
}
