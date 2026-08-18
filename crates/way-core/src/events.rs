//! Durable journal, retention, and consumer settlement protocol.

use std::{
	collections::HashSet,
	fmt,
	str::FromStr,
	sync::Arc,
};

use rusqlite::{params, params_from_iter, OptionalExtension, ToSql, Transaction};
use serde_json::Value;

use crate::store::{meta_get_tx, meta_set_tx, Clock, Store, StoreError, SystemClock};

pub const MAX_RETAINED_EVENTS: i64 = 50_000;
pub const EVENT_RETENTION_MS: i64 = 14 * 24 * 60 * 60 * 1_000;
pub const DEFAULT_CLAIM_TTL_MS: u64 = 120_000;
pub const MIN_CLAIM_TTL_MS: u64 = 5_000;
pub const MAX_CLAIM_TTL_MS: u64 = 600_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventFrame {
	pub seq: u64,
	pub ts: i64,
	pub kind: String,
	pub payload_json: String,
}

/// An opaque journal position. The generation changes only when the journal is
/// explicitly rebuilt, never when the process restarts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cursor {
	pub journal_generation: u64,
	pub seq: u64,
}

impl Cursor {
	pub const fn new(journal_generation: u64, seq: u64) -> Self {
		Self { journal_generation, seq }
	}
}

impl fmt::Display for Cursor {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(formatter, "{}:{}", self.journal_generation, self.seq)
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorParseError;

impl fmt::Display for CursorParseError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str("cursor must have the form journal_generation:seq")
	}
}

impl std::error::Error for CursorParseError {}

impl FromStr for Cursor {
	type Err = CursorParseError;

	fn from_str(value: &str) -> Result<Self, Self::Err> {
		let (generation, seq) = value.split_once(':').ok_or(CursorParseError)?;
		if generation.is_empty() || seq.is_empty() || seq.contains(':') {
			return Err(CursorParseError);
		}
		Ok(Self {
			journal_generation: generation.parse().map_err(|_| CursorParseError)?,
			seq: seq.parse().map_err(|_| CursorParseError)?,
		})
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalGap {
	pub missing_from: Cursor,
	pub missing_to: Cursor,
	pub resync_cursor: Cursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalRead {
	pub events: Vec<EventFrame>,
	pub next_cursor: Cursor,
	pub gap: Option<JournalGap>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerClaim {
	pub claim_id: String,
	pub cursor: Cursor,
	pub expires_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryProof {
	pub seq: u64,
	pub platform_msg_id: Option<String>,
	pub dedupe_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxRow {
	pub consumer_id: String,
	pub seq: u64,
	pub state: String,
	pub platform_msg_id: Option<String>,
	pub dedupe_key: Option<String>,
}

#[derive(Debug)]
pub enum JournalError {
	Store(StoreError),
	InvalidPayload,
	InvalidKind,
	InvalidLimit,
	InvalidClaimTtl,
	InvalidCursor,
	CursorBeforeRetention(JournalGap),
	ConsumerClaimHeld,
	ClaimNotHeld,
	ClaimExpired,
	CursorRegression,
	CursorAheadOfHead,
	InvalidProof(String),
	Overflow,
}

impl JournalError {
	pub const fn code(&self) -> Option<u16> {
		match self {
			Self::CursorBeforeRetention(_) => Some(1600),
			Self::ConsumerClaimHeld => Some(1601),
			_ => None,
		}
	}
}

impl fmt::Display for JournalError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Store(error) => write!(formatter, "journal store error: {error}"),
			Self::InvalidPayload => formatter.write_str("event payload_json must contain valid JSON"),
			Self::InvalidKind => formatter.write_str("event kind must not be empty"),
			Self::InvalidLimit => formatter.write_str("event read limit must be in 1..=500"),
			Self::InvalidClaimTtl => formatter.write_str("consumer claim TTL must be in 5000..=600000 ms"),
			Self::InvalidCursor => formatter.write_str("invalid journal cursor"),
			Self::CursorBeforeRetention(gap) => write!(formatter, "cursor is before retention floor; resync at {}", gap.resync_cursor),
			Self::ConsumerClaimHeld => formatter.write_str("consumer claim is already held"),
			Self::ClaimNotHeld => formatter.write_str("consumer claim is not held by this claim id"),
			Self::ClaimExpired => formatter.write_str("consumer claim has expired"),
			Self::CursorRegression => formatter.write_str("consumer commit cursor regresses its checkpoint"),
			Self::CursorAheadOfHead => formatter.write_str("consumer commit cursor is beyond journal head"),
			Self::InvalidProof(message) => write!(formatter, "invalid delivery proof: {message}"),
			Self::Overflow => formatter.write_str("journal timestamp or sequence overflow"),
		}
	}
}

impl std::error::Error for JournalError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Store(error) => Some(error),
			_ => None,
		}
	}
}

impl From<StoreError> for JournalError {
	fn from(error: StoreError) -> Self {
		Self::Store(error)
	}
}

impl From<rusqlite::Error> for JournalError {
	fn from(error: rusqlite::Error) -> Self {
		Self::Store(StoreError::from(error))
	}
}

pub type JournalResult<T> = Result<T, JournalError>;

/// The synchronous P1 journal. Appends deliberately own a blocking WAL
/// transaction so an event is durable before the TS subscriber resumes.
#[derive(Clone)]
pub struct EventJournal {
	store: Store,
	clock: Arc<dyn Clock>,
}

impl fmt::Debug for EventJournal {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.debug_struct("EventJournal").field("store", &self.store).finish_non_exhaustive()
	}
}

impl EventJournal {
	pub fn new(store: Store) -> Self {
		Self::with_clock(store, Arc::new(SystemClock))
	}

	pub fn with_clock(store: Store, clock: Arc<dyn Clock>) -> Self {
		Self { store, clock }
	}

	pub fn store(&self) -> &Store {
		&self.store
	}

	pub fn append(&self, kind: &str, payload_json: &str) -> JournalResult<Cursor> {
		validate_event(kind, payload_json)?;
		let now = self.clock.now_ms();
		let mut connection = self.store.connection()?;
		let transaction = connection.transaction()?;
		let cursor = append_in_transaction(&transaction, kind, payload_json, now)?;
		transaction.commit()?;
		Ok(cursor)
	}

	pub fn read(&self, cursor: Option<Cursor>, limit: u32) -> JournalResult<JournalRead> {
		self.read_filtered(cursor, limit, &[])
	}

	/// Reads journal frames after `cursor`, optionally filtering to selected kinds.
	/// A filtered read advances past nonmatching frames so a consumer cannot spin
	/// forever on events it explicitly elected not to receive.
	pub fn read_filtered(&self, cursor: Option<Cursor>, limit: u32, kinds: &[String]) -> JournalResult<JournalRead> {
		if !(1..=500).contains(&limit) {
			return Err(JournalError::InvalidLimit);
		}
		let connection = self.store.connection()?;
		let generation = current_generation(&connection)?;
		let floor = retention_floor(&connection)?;
		let requested = cursor.unwrap_or_else(|| Cursor::new(generation, 0));
		if requested.journal_generation != generation {
			let gap = JournalGap {
				missing_from: requested,
				missing_to: Cursor::new(generation, floor),
				resync_cursor: Cursor::new(generation, floor),
			};
			return Ok(JournalRead { events: Vec::new(), next_cursor: gap.resync_cursor, gap: Some(gap) });
		}
		if requested.seq < floor {
			let gap = JournalGap {
				missing_from: Cursor::new(generation, requested.seq.checked_add(1).ok_or(JournalError::Overflow)?),
				missing_to: Cursor::new(generation, floor),
				resync_cursor: Cursor::new(generation, floor),
			};
			return Ok(JournalRead { events: Vec::new(), next_cursor: gap.resync_cursor, gap: Some(gap) });
		}

		let requested_seq = i64::try_from(requested.seq).map_err(|_| JournalError::Overflow)?;
		let limit = i64::from(limit);
		let sql = if kinds.is_empty() {
			"SELECT seq, ts, kind, payload_json FROM events WHERE seq > ?1 ORDER BY seq ASC LIMIT ?2".to_owned()
		} else {
			let placeholders = (0..kinds.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
			format!(
				"SELECT seq, ts, kind, payload_json FROM events WHERE seq > ? AND kind IN ({placeholders}) ORDER BY seq ASC LIMIT ?"
			)
		};
		let mut statement = connection.prepare(&sql)?;
		let events = if kinds.is_empty() {
			statement
				.query_map(params![requested_seq, limit], event_frame_from_row)?
				.collect::<Result<Vec<_>, _>>()?
		} else {
			let mut values: Vec<&dyn ToSql> = Vec::with_capacity(kinds.len() + 2);
			values.push(&requested_seq);
			for kind in kinds {
				values.push(kind);
			}
			values.push(&limit);
			statement
				.query_map(params_from_iter(values), event_frame_from_row)?
				.collect::<Result<Vec<_>, _>>()?
		};
		let head: i64 = connection.query_row("SELECT COALESCE(MAX(seq), 0) FROM events", [], |row| row.get(0))?;
		let next_cursor = if events.len() < usize::try_from(limit).map_err(|_| JournalError::Overflow)? {
			Cursor::new(generation, head.try_into().map_err(|_| JournalError::Overflow)?)
		} else {
			events.last().map(|event| Cursor::new(generation, event.seq)).unwrap_or(requested)
		};
		Ok(JournalRead { events, next_cursor, gap: None })
	}

	pub fn head_cursor(&self) -> JournalResult<Cursor> {
		let connection = self.store.connection()?;
		let generation = current_generation(&connection)?;
		let sequence = connection.query_row("SELECT COALESCE(MAX(seq), 0) FROM events", [], |row| row.get::<_, i64>(0))?;
		Ok(Cursor::new(generation, sequence as u64))
	}

	pub fn claim_consumer(&self, consumer_id: &str, claim_ttl_ms: Option<u64>) -> JournalResult<ConsumerClaim> {
		if consumer_id.trim().is_empty() {
			return Err(JournalError::InvalidProof("consumer_id must not be empty".to_owned()));
		}
		let ttl_ms = claim_ttl_ms.unwrap_or(DEFAULT_CLAIM_TTL_MS);
		if !(MIN_CLAIM_TTL_MS..=MAX_CLAIM_TTL_MS).contains(&ttl_ms) {
			return Err(JournalError::InvalidClaimTtl);
		}
		let now = self.clock.now_ms();
		let expires_at = now.checked_add(ttl_ms.try_into().map_err(|_| JournalError::Overflow)?).ok_or(JournalError::Overflow)?;
		let mut connection = self.store.connection()?;
		let transaction = connection.transaction()?;
		let generation = current_generation_tx(&transaction)?;
		let existing = transaction
			.query_row(
				"SELECT cursor, claim_id, claim_expires_at FROM consumer_checkpoints WHERE consumer_id = ?1",
				[consumer_id],
				|row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?, row.get::<_, Option<i64>>(2)?)),
			)
			.optional()?;
		if let Some((_, Some(_), Some(existing_expiry))) = &existing
			&& *existing_expiry > now
		{
			return Err(JournalError::ConsumerClaimHeld);
		}
		let cursor = existing
			.as_ref()
			.map(|(cursor, _, _)| Cursor::from_str(cursor).map_err(|_| JournalError::InvalidCursor))
			.transpose()?
			.unwrap_or_else(|| Cursor::new(generation, 0));
		let claim_id: String = transaction.query_row("SELECT lower(hex(randomblob(16)))", [], |row| row.get(0))?;
		transaction.execute(
			"INSERT INTO consumer_checkpoints(consumer_id, cursor, claim_id, claim_expires_at, updated_at)
			 VALUES (?1, ?2, ?3, ?4, ?5)
			 ON CONFLICT(consumer_id) DO UPDATE SET
			 claim_id = excluded.claim_id,
			 claim_expires_at = excluded.claim_expires_at,
			 updated_at = excluded.updated_at",
			params![consumer_id, cursor.to_string(), claim_id, expires_at, now],
		)?;
		transaction.commit()?;
		Ok(ConsumerClaim { claim_id, cursor, expires_at })
	}

	/// Settles confirmed sends. The outbox inserts and checkpoint advance are in
	/// one transaction, so a crash cannot produce a committed cursor without its
	/// proof rows.
	pub fn commit_consumer(
		&self,
		consumer_id: &str,
		claim_id: &str,
		cursor: Cursor,
		proofs: &[DeliveryProof],
	) -> JournalResult<Cursor> {
		if consumer_id.trim().is_empty() || claim_id.trim().is_empty() {
			return Err(JournalError::ClaimNotHeld);
		}
		validate_proofs(proofs)?;
		let now = self.clock.now_ms();
		let mut connection = self.store.connection()?;
		let transaction = connection.transaction()?;
		let generation = current_generation_tx(&transaction)?;
		if cursor.journal_generation != generation {
			return Err(JournalError::InvalidCursor);
		}
		let checkpoint = transaction
			.query_row(
				"SELECT cursor, claim_id, claim_expires_at FROM consumer_checkpoints WHERE consumer_id = ?1",
				[consumer_id],
				|row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?, row.get::<_, Option<i64>>(2)?)),
			)
			.optional()?
			.ok_or(JournalError::ClaimNotHeld)?;
		let checkpoint_cursor = Cursor::from_str(&checkpoint.0).map_err(|_| JournalError::InvalidCursor)?;
		if checkpoint.1.as_deref() != Some(claim_id) {
			return Err(JournalError::ClaimNotHeld);
		}
		if checkpoint.2.unwrap_or_default() <= now {
			return Err(JournalError::ClaimExpired);
		}
		if checkpoint_cursor.journal_generation != generation || cursor.seq < checkpoint_cursor.seq {
			return Err(JournalError::CursorRegression);
		}
		let head: i64 = transaction.query_row("SELECT COALESCE(MAX(seq), 0) FROM events", [], |row| row.get(0))?;
		if cursor.seq > head as u64 {
			return Err(JournalError::CursorAheadOfHead);
		}
		if cursor.seq > checkpoint_cursor.seq && proofs.is_empty() {
			return Err(JournalError::InvalidProof("advancing a checkpoint requires confirmed send proofs".to_owned()));
		}
		for proof in proofs {
			if proof.seq <= checkpoint_cursor.seq || proof.seq > cursor.seq {
				return Err(JournalError::InvalidProof("proof sequence is outside the committed cursor range".to_owned()));
			}
			transaction.execute(
				"INSERT INTO outbox(consumer_id, seq, state, platform_msg_id, dedupe_key, created_at, updated_at)
				 VALUES (?1, ?2, 'sent', ?3, ?4, ?5, ?5)
				 ON CONFLICT(consumer_id, seq) DO UPDATE SET
				 state = 'sent', platform_msg_id = excluded.platform_msg_id,
				 dedupe_key = excluded.dedupe_key, updated_at = excluded.updated_at",
				params![consumer_id, proof.seq as i64, proof.platform_msg_id, proof.dedupe_key, now],
			)?;
		}
		transaction.execute(
			"UPDATE consumer_checkpoints SET cursor = ?2, claim_id = NULL, claim_expires_at = NULL, updated_at = ?3
			 WHERE consumer_id = ?1",
			params![consumer_id, cursor.to_string(), now],
		)?;
		transaction.commit()?;
		Ok(cursor)
	}

	pub fn consumer_cursor(&self, consumer_id: &str) -> JournalResult<Option<Cursor>> {
		let connection = self.store.connection()?;
		let cursor = connection
			.query_row("SELECT cursor FROM consumer_checkpoints WHERE consumer_id = ?1", [consumer_id], |row| row.get::<_, String>(0))
			.optional()?;
		cursor
			.map(|value| Cursor::from_str(&value).map_err(|_| JournalError::InvalidCursor))
			.transpose()
	}

	pub fn outbox_rows(&self, consumer_id: &str) -> JournalResult<Vec<OutboxRow>> {
		let connection = self.store.connection()?;
		let mut statement = connection.prepare(
			"SELECT consumer_id, seq, state, platform_msg_id, dedupe_key
			 FROM outbox WHERE consumer_id = ?1 ORDER BY seq ASC",
		)?;
		let rows = statement.query_map([consumer_id], |row| {
			Ok(OutboxRow {
				consumer_id: row.get(0)?,
				seq: row.get::<_, i64>(1)? as u64,
				state: row.get(2)?,
				platform_msg_id: row.get(3)?,
				dedupe_key: row.get(4)?,
			})
		})?;
		Ok(rows.collect::<Result<Vec<_>, _>>()?)
	}

	/// Deliberate journal rebuild used by an operator repair. It invalidates old
	/// cursors by changing the persistent generation.
	pub fn truncate_and_bump_generation(&self) -> JournalResult<u64> {
		let mut connection = self.store.connection()?;
		let transaction = connection.transaction()?;
		let generation = current_generation_tx(&transaction)?;
		let next = generation.checked_add(1).ok_or(JournalError::Overflow)?;
		transaction.execute("DELETE FROM outbox", [])?;
		transaction.execute("DELETE FROM events", [])?;
		meta_set_tx(&transaction, "journal_generation", &next.to_string())?;
		meta_set_tx(&transaction, "journal_floor_seq", "0")?;
		transaction.commit()?;
		Ok(next)
	}
}

fn event_frame_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EventFrame> {
	Ok(EventFrame {
		seq: row.get::<_, i64>(0)? as u64,
		ts: row.get(1)?,
		kind: row.get(2)?,
		payload_json: row.get(3)?,
	})
}

pub(crate) fn append_in_transaction(
	transaction: &Transaction<'_>,
	kind: &str,
	payload_json: &str,
	now: i64,
) -> JournalResult<Cursor> {
	validate_event(kind, payload_json)?;
	let generation = current_generation_tx(transaction)?;
	transaction.execute(
		"INSERT INTO events(ts, kind, payload_json) VALUES (?1, ?2, ?3)",
		params![now, kind, payload_json],
	)?;
	let sequence: i64 = transaction.last_insert_rowid();
	prune_in_transaction(transaction, now)?;
	Ok(Cursor::new(generation, sequence.try_into().map_err(|_| JournalError::Overflow)?))
}

fn validate_event(kind: &str, payload_json: &str) -> JournalResult<()> {
	if kind.trim().is_empty() {
		return Err(JournalError::InvalidKind);
	}
	serde_json::from_str::<Value>(payload_json).map_err(|_| JournalError::InvalidPayload)?;
	Ok(())
}

fn validate_proofs(proofs: &[DeliveryProof]) -> JournalResult<()> {
	let mut seen = HashSet::new();
	for proof in proofs {
		if !seen.insert(proof.seq) {
			return Err(JournalError::InvalidProof("proof sequences must be unique".to_owned()));
		}
		if proof.dedupe_key.as_deref().is_some_and(str::is_empty) {
			return Err(JournalError::InvalidProof("dedupe_key must not be empty when present".to_owned()));
		}
	}
	Ok(())
}

fn current_generation(connection: &rusqlite::Connection) -> JournalResult<u64> {
	let value = connection
		.query_row("SELECT v FROM gateway_meta WHERE k = 'journal_generation'", [], |row| row.get::<_, String>(0))
		.optional()?
		.ok_or_else(|| JournalError::Store(StoreError::InvalidMetadata("journal_generation is missing".to_owned())))?;
	value
		.parse()
		.map_err(|_| JournalError::Store(StoreError::InvalidMetadata("journal_generation is not a u64".to_owned())))
}

fn current_generation_tx(transaction: &Transaction<'_>) -> JournalResult<u64> {
	let value = meta_get_tx(transaction, "journal_generation")?
		.ok_or_else(|| JournalError::Store(StoreError::InvalidMetadata("journal_generation is missing".to_owned())))?;
	value
		.parse()
		.map_err(|_| JournalError::Store(StoreError::InvalidMetadata("journal_generation is not a u64".to_owned())))
}

fn retention_floor(connection: &rusqlite::Connection) -> JournalResult<u64> {
	let value = connection
		.query_row("SELECT v FROM gateway_meta WHERE k = 'journal_floor_seq'", [], |row| row.get::<_, String>(0))
		.optional()?
		.ok_or_else(|| JournalError::Store(StoreError::InvalidMetadata("journal_floor_seq is missing".to_owned())))?;
	value
		.parse()
		.map_err(|_| JournalError::Store(StoreError::InvalidMetadata("journal_floor_seq is not a u64".to_owned())))
}

fn prune_in_transaction(transaction: &Transaction<'_>, now: i64) -> JournalResult<()> {
	let cutoff = now.checked_sub(EVENT_RETENTION_MS).ok_or(JournalError::Overflow)?;
	let count: i64 = transaction.query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))?;
	let count_floor = if count > MAX_RETAINED_EVENTS {
		Some(transaction.query_row(
			"SELECT seq FROM events ORDER BY seq DESC LIMIT 1 OFFSET ?1",
			[MAX_RETAINED_EVENTS - 1],
			|row| row.get::<_, i64>(0),
		)?)
	} else {
		None
	};
	let max_deleted: Option<i64> = transaction.query_row(
		"SELECT MAX(seq) FROM events
		 WHERE ts < ?1 OR (?2 IS NOT NULL AND seq < ?2)",
		params![cutoff, count_floor],
		|row| row.get(0),
	)?;
	let Some(max_deleted) = max_deleted else {
		return Ok(());
	};
	transaction.execute(
		"DELETE FROM outbox WHERE seq IN (
			SELECT seq FROM events WHERE ts < ?1 OR (?2 IS NOT NULL AND seq < ?2)
		)",
		params![cutoff, count_floor],
	)?;
	transaction.execute(
		"DELETE FROM events WHERE ts < ?1 OR (?2 IS NOT NULL AND seq < ?2)",
		params![cutoff, count_floor],
	)?;
	let old_floor = meta_get_tx(transaction, "journal_floor_seq")?
		.ok_or_else(|| JournalError::Store(StoreError::InvalidMetadata("journal_floor_seq is missing".to_owned())))?
		.parse::<u64>()
		.map_err(|_| JournalError::Store(StoreError::InvalidMetadata("journal_floor_seq is not a u64".to_owned())))?;
	let floor = old_floor.max(max_deleted.try_into().map_err(|_| JournalError::Overflow)?);
	meta_set_tx(transaction, "journal_floor_seq", &floor.to_string())?;
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::{
		fs,
		path::PathBuf,
		sync::{
			atomic::{AtomicI64, AtomicU64, Ordering},
			Arc,
		},
	};

	use proptest::prelude::*;

	use super::{
		append_in_transaction, Clock, ConsumerClaim, Cursor, DeliveryProof, EventJournal, JournalError, EVENT_RETENTION_MS,
		MAX_RETAINED_EVENTS,
	};
	use crate::store::Store;

	#[derive(Default)]
	struct FakeClock(AtomicI64);

	impl FakeClock {
		fn set(&self, value: i64) {
			self.0.store(value, Ordering::Relaxed);
		}
	}

	impl Clock for FakeClock {
		fn now_ms(&self) -> i64 {
			self.0.load(Ordering::Relaxed)
		}
	}

	fn journal() -> (EventJournal, Arc<FakeClock>) {
		let clock = Arc::new(FakeClock::default());
		(EventJournal::with_clock(Store::default(), clock.clone()), clock)
	}

	static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

	fn temporary_state_dir(name: &str) -> PathBuf {
		let unique = NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed);
		let path = std::env::temp_dir().join(format!("gajae-way-events-{name}-{}-{unique}", std::process::id()));
		let _ = fs::remove_dir_all(&path);
		fs::create_dir_all(&path).unwrap();
		path
	}

	#[test]
	fn cursors_persist_across_reopen_and_generation_changes_only_on_rebuild() {
		let state_dir = temporary_state_dir("cursor");
		let store = Store::open(&state_dir).unwrap();
		let journal = EventJournal::new(store.clone());
		let first = journal.append("turn_start", "{\"session\":\"s\"}").unwrap();
		drop(journal);
		drop(store);

		let reopened_store = Store::open(&state_dir).unwrap();
		let reopened = EventJournal::new(reopened_store.clone());
		let second = reopened.append("turn_end", "{} ").unwrap();
		assert_eq!(first.to_string(), "1:1");
		assert_eq!(second.to_string(), "1:2");
		assert_eq!(reopened.read(Some(first), 10).unwrap().events[0].seq, 2);
		assert_eq!("1:2".parse::<Cursor>().unwrap(), second);

		assert_eq!(reopened.truncate_and_bump_generation().unwrap(), 2);
		let rebuilt = reopened.read(Some(second), 10).unwrap();
		assert_eq!(rebuilt.gap.unwrap().resync_cursor, Cursor::new(2, 0));
		drop(reopened);
		drop(reopened_store);
		fs::remove_dir_all(state_dir).unwrap();
	}

	#[test]
	fn retention_returns_a_typed_gap_and_keeps_a_resync_cursor() {
		let (journal, clock) = journal();
		journal.append("message", "{} ").unwrap();
		clock.set(EVENT_RETENTION_MS + 1);
		let retained = journal.append("message", "{} ").unwrap();
		let read = journal.read(Some(Cursor::new(1, 0)), 10).unwrap();
		let gap = read.gap.expect("cursor below the retention floor must receive a gap");
		assert_eq!(gap.missing_from, Cursor::new(1, 1));
		assert_eq!(gap.missing_to, Cursor::new(1, 1));
		assert_eq!(gap.resync_cursor, Cursor::new(1, 1));
		assert_eq!(journal.read(Some(read.next_cursor), 10).unwrap().events[0].seq, retained.seq);
	}

	#[test]
	fn retention_enforces_the_fifty_thousand_event_cap() {
		let (journal, clock) = journal();
		let mut connection = journal.store().connection().unwrap();
		let transaction = connection.transaction().unwrap();
		for _ in 0..MAX_RETAINED_EVENTS {
			append_in_transaction(&transaction, "cap", "{}", clock.now_ms()).unwrap();
		}
		transaction.commit().unwrap();
		drop(connection);
		journal.append("cap", "{}").unwrap();
		let connection = journal.store().connection().unwrap();
		let count: i64 = connection.query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0)).unwrap();
		assert_eq!(count, MAX_RETAINED_EVENTS);
	}

	#[test]
	fn claim_is_exclusive_and_checkpoint_advances_only_when_committed() {
		let (journal, _) = journal();
		let event = journal.append("surface_reply", "{\"text\":\"hi\"}").unwrap();
		let claim = journal.claim_consumer("discord", Some(5_000)).unwrap();
		assert!(matches!(journal.claim_consumer("discord", Some(5_000)), Err(JournalError::ConsumerClaimHeld)));
		assert_eq!(journal.consumer_cursor("discord").unwrap(), Some(Cursor::new(1, 0)));

		journal
			.commit_consumer(
				"discord",
				&claim.claim_id,
				event,
				&[DeliveryProof {
					seq: event.seq,
					platform_msg_id: Some("message-1".to_owned()),
					dedupe_key: Some("event-1".to_owned()),
				}],
			)
			.unwrap();
		assert_eq!(journal.consumer_cursor("discord").unwrap(), Some(event));
		assert_eq!(journal.outbox_rows("discord").unwrap()[0].state, "sent");
	}

	#[test]
	fn outbox_and_checkpoint_are_written_in_the_same_transaction() {
		let (journal, _) = journal();
		let event = journal.append("surface_reply", "{} ").unwrap();
		let ConsumerClaim { claim_id, .. } = journal.claim_consumer("consumer", Some(5_000)).unwrap();
		journal
			.commit_consumer(
				"consumer",
				&claim_id,
				event,
				&[DeliveryProof { seq: event.seq, platform_msg_id: None, dedupe_key: Some("d".to_owned()) }],
			)
			.unwrap();
		let connection = journal.store().connection().unwrap();
		let tuple: (String, i64) = connection
			.query_row(
				"SELECT c.cursor, COUNT(o.seq) FROM consumer_checkpoints c
				 LEFT JOIN outbox o ON o.consumer_id = c.consumer_id WHERE c.consumer_id = 'consumer'",
				[],
				|row| Ok((row.get(0)?, row.get(1)?)),
			)
			.unwrap();
		assert_eq!(tuple.0, event.to_string());
		assert_eq!(tuple.1, 1);
	}

	#[test]
	fn committed_checkpoint_and_outbox_are_durable_across_reopen() {
		let state_dir = temporary_state_dir("consumer");
		let store = Store::open(&state_dir).unwrap();
		let journal = EventJournal::new(store.clone());
		let event = journal.append("surface_reply", "{}").unwrap();
		let claim = journal.claim_consumer("durable-consumer", Some(5_000)).unwrap();
		journal
			.commit_consumer(
				"durable-consumer",
				&claim.claim_id,
				event,
				&[DeliveryProof { seq: event.seq, platform_msg_id: Some("platform-1".to_owned()), dedupe_key: Some("d-1".to_owned()) }],
			)
			.unwrap();
		drop(journal);
		drop(store);

		let reopened = EventJournal::new(Store::open(&state_dir).unwrap());
		assert_eq!(reopened.consumer_cursor("durable-consumer").unwrap(), Some(event));
		assert_eq!(reopened.outbox_rows("durable-consumer").unwrap().len(), 1);
		drop(reopened);
		fs::remove_dir_all(state_dir).unwrap();
	}

	proptest! {
		#[test]
		fn cursor_parser_rejects_non_canonical_shapes(value in ".{0,32}") {
			if value.parse::<Cursor>().is_ok() {
				prop_assert!(value.contains(':'));
			}
		}
	}
}
