//! N-API surface for the gajae-way durable runtime.
//!
//! P1 keeps TypeScript thin: SQLite state, fenced lock ownership, durable
//! journal settlement, and idempotency are all executed in Rust.

use std::{str::FromStr, sync::OnceLock, time::SystemTime};

use napi_derive::napi;

use crate::{
	events::{ConsumerClaim, Cursor, DeliveryProof, EventJournal, JournalError, JournalGap, JournalRead, OutboxRow},
	lock::{
		AcquireRequest, AcquireResult, HolderKind, Lease, LeaseHolder, LockClass, LockError, LockManager, LockStatus,
		QueueEntry, ReleaseResult,
	},
	store::{unix_epoch_ms, Store, StoreError},
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

/// N-API handle over a real, migrated SQLite state directory.
#[napi]
pub struct WayCore {
	state_dir: String,
	store: Store,
	locks: LockManager,
	journal: EventJournal,
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
		Ok(Self { state_dir, store, locks, journal })
	}

	#[napi(getter, js_name = "stateDir")]
	pub fn state_dir(&self) -> String {
		self.state_dir.clone()
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
