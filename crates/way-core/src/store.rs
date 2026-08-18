//! Durable SQLite state for the gateway.
//!
//! The store owns the only SQLite connection used by the P1 core.  It is
//! serialized behind a mutex because the lock and journal protocols rely on
//! short, blocking write transactions rather than an async executor.

use std::{
	fmt,
	fs,
	io,
	path::{Path, PathBuf},
	sync::{Arc, Mutex, MutexGuard},
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, Transaction};

pub const DATABASE_FILENAME: &str = "way-core.sqlite3";
pub const SCHEMA_VERSION: u32 = 1;
pub const IDEMPOTENCY_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;

/// A clock is injected into protocol tests so expiry and retention behavior
/// never depends on wall-clock sleeps.
pub trait Clock: Send + Sync {
	fn now_ms(&self) -> i64;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
	fn now_ms(&self) -> i64 {
		unix_epoch_ms()
	}
}

pub fn unix_epoch_ms() -> i64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("system time must not precede the Unix epoch")
		.as_millis()
		.try_into()
		.expect("Unix epoch milliseconds fit in i64")
}

#[derive(Debug)]
pub enum StoreError {
	Io(io::Error),
	Sql(rusqlite::Error),
	Poisoned,
	Integrity(String),
	UnsupportedSchema(u32),
	InvalidMetadata(String),
	IdempotencyConflict,
}

impl fmt::Display for StoreError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Io(error) => write!(formatter, "store I/O error: {error}"),
			Self::Sql(error) => write!(formatter, "SQLite error: {error}"),
			Self::Poisoned => formatter.write_str("SQLite connection mutex was poisoned"),
			Self::Integrity(result) => write!(formatter, "SQLite integrity_check failed: {result}"),
			Self::UnsupportedSchema(version) => {
				write!(formatter, "database schema version {version} is newer than this gateway")
			}
			Self::InvalidMetadata(message) => write!(formatter, "invalid gateway metadata: {message}"),
			Self::IdempotencyConflict => formatter.write_str("idempotency key was reused for a different request"),
		}
	}
}

impl std::error::Error for StoreError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Io(error) => Some(error),
			Self::Sql(error) => Some(error),
			_ => None,
		}
	}
}

impl From<io::Error> for StoreError {
	fn from(error: io::Error) -> Self {
		Self::Io(error)
	}
}

impl From<rusqlite::Error> for StoreError {
	fn from(error: rusqlite::Error) -> Self {
		Self::Sql(error)
	}
}

pub type StoreResult<T> = Result<T, StoreError>;

struct StoreInner {
	connection: Mutex<Connection>,
	path: Option<PathBuf>,
}

/// A cloneable handle to the durable gateway state database.
#[derive(Clone)]
pub struct Store {
	inner: Arc<StoreInner>,
}

impl fmt::Debug for Store {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter
			.debug_struct("Store")
			.field("path", &self.inner.path)
			.finish_non_exhaustive()
	}
}

impl Default for Store {
	fn default() -> Self {
		Self::open_in_memory().expect("in-memory SQLite store must initialize")
	}
}

impl Store {
	/// Opens (and migrates) the state database in `state_dir`.
	pub fn open(state_dir: impl AsRef<Path>) -> StoreResult<Self> {
		let state_dir = state_dir.as_ref();
		fs::create_dir_all(state_dir)?;
		let path = state_dir.join(DATABASE_FILENAME);
		let connection = Connection::open(&path)?;
		Self::from_connection(connection, Some(path))
	}

	/// Creates an isolated store for unit tests and small in-process callers.
	pub fn open_in_memory() -> StoreResult<Self> {
		Self::from_connection(Connection::open_in_memory()?, None)
	}

	fn from_connection(mut connection: Connection, path: Option<PathBuf>) -> StoreResult<Self> {
		configure_connection(&mut connection)?;
		migrate(&mut connection)?;
		integrity_check(&connection)?;
		Ok(Self { inner: Arc::new(StoreInner { connection: Mutex::new(connection), path }) })
	}

	pub fn database_path(&self) -> Option<&Path> {
		self.inner.path.as_deref()
	}

	pub(crate) fn connection(&self) -> StoreResult<MutexGuard<'_, Connection>> {
		self.inner.connection.lock().map_err(|_| StoreError::Poisoned)
	}

	pub fn get_meta(&self, key: &str) -> StoreResult<Option<String>> {
		let connection = self.connection()?;
		meta_get(&connection, key)
	}

	pub fn set_meta(&self, key: &str, value: &str) -> StoreResult<()> {
		let connection = self.connection()?;
		connection.execute(
			"INSERT INTO gateway_meta(k, v) VALUES (?1, ?2)
			 ON CONFLICT(k) DO UPDATE SET v = excluded.v",
			rusqlite::params![key, value],
		)?;
		Ok(())
	}

	/// P0-compatible convenience metadata round-trip. New protocol code should
	/// use `set_meta` so it can propagate storage failures.
	pub fn put(&self, key: impl AsRef<str>, value: impl AsRef<str>) {
		self.set_meta(key.as_ref(), value.as_ref())
			.expect("metadata write to an initialized SQLite store must succeed");
	}

	/// P0-compatible convenience metadata lookup.
	pub fn get(&self, key: &str) -> Option<String> {
		self.get_meta(key).ok().flatten()
	}

	pub fn journal_generation(&self) -> StoreResult<u64> {
		let raw = self
			.get_meta("journal_generation")?
			.ok_or_else(|| StoreError::InvalidMetadata("journal_generation is missing".to_owned()))?;
		raw.parse().map_err(|_| StoreError::InvalidMetadata("journal_generation is not a u64".to_owned()))
	}

	/// Returns a replayed response for the exact request, or records no state
	/// when this key is unseen. Call `store_idempotency_response` after the
	/// mutation has committed.
	pub fn replay_idempotency(
		&self,
		scope: &str,
		key: &str,
		request_json: &str,
		now_ms: i64,
	) -> StoreResult<Option<String>> {
		let mut connection = self.connection()?;
		let transaction = connection.transaction()?;
		transaction.execute("DELETE FROM idempotency WHERE expires_at <= ?1", [now_ms])?;
		let existing = transaction
			.query_row(
				"SELECT request_json, response_json FROM idempotency WHERE scope = ?1 AND idempotency_key = ?2",
				rusqlite::params![scope, key],
				|row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
			)
			.optional()?;
		transaction.commit()?;

		match existing {
			Some((stored_request, _)) if stored_request != request_json => Err(StoreError::IdempotencyConflict),
			Some((_, response)) => Ok(Some(response)),
			None => Ok(None),
		}
	}

	pub fn store_idempotency_response(
		&self,
		scope: &str,
		key: &str,
		request_json: &str,
		response_json: &str,
		now_ms: i64,
	) -> StoreResult<()> {
		let expires_at = now_ms.checked_add(IDEMPOTENCY_WINDOW_MS).ok_or_else(|| {
			StoreError::InvalidMetadata("idempotency expiration overflowed i64".to_owned())
		})?;
		let mut connection = self.connection()?;
		let transaction = connection.transaction()?;
		transaction.execute("DELETE FROM idempotency WHERE expires_at <= ?1", [now_ms])?;
		let existing = transaction
			.query_row(
				"SELECT request_json FROM idempotency WHERE scope = ?1 AND idempotency_key = ?2",
				rusqlite::params![scope, key],
				|row| row.get::<_, String>(0),
			)
			.optional()?;
		if let Some(existing_request) = existing {
			if existing_request != request_json {
				return Err(StoreError::IdempotencyConflict);
			}
			transaction.commit()?;
			return Ok(());
		}
		transaction.execute(
			"INSERT INTO idempotency(scope, idempotency_key, request_json, response_json, created_at, expires_at)
			 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
			rusqlite::params![scope, key, request_json, response_json, now_ms, expires_at],
		)?;
		transaction.commit()?;
		Ok(())
	}
}

pub(crate) fn meta_get(connection: &Connection, key: &str) -> StoreResult<Option<String>> {
	connection
		.query_row("SELECT v FROM gateway_meta WHERE k = ?1", [key], |row| row.get(0))
		.optional()
		.map_err(StoreError::from)
}

pub(crate) fn meta_get_tx(transaction: &Transaction<'_>, key: &str) -> StoreResult<Option<String>> {
	transaction
		.query_row("SELECT v FROM gateway_meta WHERE k = ?1", [key], |row| row.get(0))
		.optional()
		.map_err(StoreError::from)
}

pub(crate) fn meta_set_tx(transaction: &Transaction<'_>, key: &str, value: &str) -> StoreResult<()> {
	transaction.execute(
		"INSERT INTO gateway_meta(k, v) VALUES (?1, ?2)
		 ON CONFLICT(k) DO UPDATE SET v = excluded.v",
		rusqlite::params![key, value],
	)?;
	Ok(())
}

fn configure_connection(connection: &mut Connection) -> StoreResult<()> {
	connection.busy_timeout(Duration::from_secs(5))?;
	// WAL is deliberately configured before every open. SQLite will preserve it
	// for a file-backed database, and reports `memory` harmlessly for :memory:.
	connection.execute_batch(
		"PRAGMA journal_mode = WAL;
		 PRAGMA foreign_keys = ON;
		 PRAGMA synchronous = NORMAL;",
	)?;
	Ok(())
}

fn migrate(connection: &mut Connection) -> StoreResult<()> {
	connection.execute_batch(
		"CREATE TABLE IF NOT EXISTS gateway_meta (
			k TEXT PRIMARY KEY NOT NULL,
			v TEXT NOT NULL
		);",
	)?;

	let current_version = meta_get(connection, "schema_version")?
		.map(|value| {
			value
				.parse::<u32>()
				.map_err(|_| StoreError::InvalidMetadata("schema_version is not a u32".to_owned()))
		})
		.transpose()?
		.unwrap_or(0);
	if current_version > SCHEMA_VERSION {
		return Err(StoreError::UnsupportedSchema(current_version));
	}

	if current_version < 1 {
		let transaction = connection.transaction()?;
		transaction.execute_batch(
			"CREATE TABLE sessions (
				session_id TEXT PRIMARY KEY NOT NULL,
				kind TEXT NOT NULL DEFAULT 'unknown' CHECK (kind IN ('main', 'conversation', 'lane', 'job', 'unknown')),
				purpose TEXT,
				brief TEXT,
				status TEXT NOT NULL DEFAULT 'discovered' CHECK (status IN ('discovered', 'starting', 'active', 'idle', 'closing', 'closed', 'lost')),
				surface_id TEXT,
				locator TEXT,
				endpoint_generation TEXT,
				host_incarnation TEXT,
				identity_provenance TEXT,
				index_seq INTEGER,
				live INTEGER,
				deleted INTEGER NOT NULL DEFAULT 0,
				terminal_uncertain INTEGER NOT NULL DEFAULT 0,
				ambiguous INTEGER NOT NULL DEFAULT 0,
				activity_state TEXT,
				activity_at INTEGER,
				last_heartbeat_at INTEGER,
				meta_name TEXT,
				meta_cwd TEXT,
				meta_kind TEXT,
				metadata_state TEXT NOT NULL DEFAULT 'pending' CHECK (metadata_state IN ('pending', 'enriched', 'unavailable')),
				metadata_at INTEGER,
				source TEXT NOT NULL CHECK (source IN ('gateway', 'reconciler')),
				created_at INTEGER NOT NULL,
				last_seen_at INTEGER,
				closed_at INTEGER,
				registry_rev INTEGER NOT NULL DEFAULT 0,
				FOREIGN KEY (surface_id) REFERENCES surfaces(surface_id) DEFERRABLE INITIALLY DEFERRED
			);

			CREATE TABLE surfaces (
				surface_id TEXT PRIMARY KEY NOT NULL,
				platform TEXT NOT NULL,
				kind TEXT NOT NULL,
				session_id TEXT,
				is_owner_surface INTEGER NOT NULL DEFAULT 0 CHECK (is_owner_surface IN (0, 1)),
				created_at INTEGER NOT NULL,
				updated_at INTEGER NOT NULL,
				FOREIGN KEY (session_id) REFERENCES sessions(session_id) DEFERRABLE INITIALLY DEFERRED
			);

			CREATE TABLE leases (
				lease_id TEXT PRIMARY KEY NOT NULL,
				lock_name TEXT NOT NULL DEFAULT 'corpus',
				holder_kind TEXT NOT NULL CHECK (holder_kind IN ('in_daemon', 'external')),
				session_id TEXT NOT NULL,
				label TEXT NOT NULL,
				pid INTEGER NOT NULL,
				pid_start_time TEXT NOT NULL,
				pgid INTEGER NOT NULL,
				pgid_start_time TEXT,
				conn_id TEXT,
				class TEXT NOT NULL CHECK (class IN ('interactive', 'batch')),
				state TEXT NOT NULL CHECK (state IN ('active', 'expiring', 'stuck', 'quarantined', 'released', 'revoked')),
				fencing_token TEXT NOT NULL,
				ttl_ms INTEGER NOT NULL,
				acquired_at INTEGER NOT NULL,
				expires_at INTEGER NOT NULL,
				hard_expires_at INTEGER NOT NULL,
				released_at INTEGER,
				release_reason TEXT,
				quarantine_receipt_id TEXT
			);
			CREATE UNIQUE INDEX one_unresolved_corpus_lease
				ON leases(lock_name)
				WHERE state IN ('active', 'expiring', 'stuck', 'quarantined');
			CREATE INDEX leases_state_idx ON leases(lock_name, state, expires_at);

			CREATE TABLE events (
				seq INTEGER PRIMARY KEY AUTOINCREMENT,
				ts INTEGER NOT NULL,
				kind TEXT NOT NULL,
				payload_json TEXT NOT NULL
			);
			CREATE INDEX events_ts_idx ON events(ts);

			CREATE TABLE consumer_checkpoints (
				consumer_id TEXT PRIMARY KEY NOT NULL,
				cursor TEXT NOT NULL,
				claim_id TEXT,
				claim_expires_at INTEGER,
				updated_at INTEGER NOT NULL
			);

			CREATE TABLE outbox (
				consumer_id TEXT NOT NULL,
				seq INTEGER NOT NULL,
				state TEXT NOT NULL CHECK (state IN ('pending', 'sent')),
				platform_msg_id TEXT,
				dedupe_key TEXT,
				created_at INTEGER NOT NULL,
				updated_at INTEGER NOT NULL,
				PRIMARY KEY (consumer_id, seq),
				FOREIGN KEY (consumer_id) REFERENCES consumer_checkpoints(consumer_id) ON DELETE CASCADE,
				FOREIGN KEY (seq) REFERENCES events(seq)
			);
			CREATE INDEX outbox_pending_idx ON outbox(consumer_id, state, seq);

			CREATE TABLE idempotency (
				scope TEXT NOT NULL,
				idempotency_key TEXT NOT NULL,
				request_json TEXT NOT NULL,
				response_json TEXT NOT NULL,
				created_at INTEGER NOT NULL,
				expires_at INTEGER NOT NULL,
				PRIMARY KEY (scope, idempotency_key)
			);
			CREATE INDEX idempotency_expiry_idx ON idempotency(expires_at);",
		)?;
		meta_set_tx(&transaction, "schema_version", "1")?;
		transaction.commit()?;
	}

	let defaults = [
		("bootstrap_state", "ABSENT"),
		("bootstrap_intent", "null"),
		("main_identity", "null"),
		("growth_intent", "null"),
		("profile_digest", "null"),
		("profile_digest_version", "0"),
		("profile_projection", "null"),
		("profile_tunables_revision", "0"),
		("profile_approved_at", "null"),
		("profile_approval_receipt", "null"),
		("failed_closed_reason", "null"),
		("journal_generation", "1"),
		("boot_epoch", "0"),
		("journal_floor_seq", "0"),
		("lock_fencing_token", "0"),
		("write_mode", "on"),
	];
	for (key, value) in defaults {
		connection.execute(
			"INSERT INTO gateway_meta(k, v) VALUES (?1, ?2) ON CONFLICT(k) DO NOTHING",
			rusqlite::params![key, value],
		)?;
	}
	Ok(())
}

fn integrity_check(connection: &Connection) -> StoreResult<()> {
	let mut statement = connection.prepare("PRAGMA integrity_check")?;
	let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
	for row in rows {
		let result = row?;
		if result != "ok" {
			return Err(StoreError::Integrity(result));
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::{
		fs,
		path::PathBuf,
		sync::atomic::{AtomicU64, Ordering},
	};

	use super::{DATABASE_FILENAME, IDEMPOTENCY_WINDOW_MS, SCHEMA_VERSION, Store};
	use rusqlite::OptionalExtension;

	static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

	fn temporary_state_dir(test_name: &str) -> PathBuf {
		let unique = NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed);
		let path = std::env::temp_dir().join(format!(
			"gajae-way-store-{test_name}-{}-{unique}",
			std::process::id()
		));
		fs::create_dir_all(&path).unwrap();
		path
	}

	#[test]
	fn fresh_open_creates_the_full_schema_in_wal_mode() {
		let state_dir = temporary_state_dir("fresh");
		let store = Store::open(&state_dir).unwrap();
		assert!(state_dir.join(DATABASE_FILENAME).is_file());
		assert_eq!(store.get_meta("schema_version").unwrap(), Some(SCHEMA_VERSION.to_string()));

		let connection = store.connection().unwrap();
		let journal_mode: String = connection.query_row("PRAGMA journal_mode", [], |row| row.get(0)).unwrap();
		assert_eq!(journal_mode.to_lowercase(), "wal");
		for table in ["sessions", "surfaces", "gateway_meta", "leases", "events", "consumer_checkpoints", "outbox", "idempotency"] {
			let found: Option<String> = connection
				.query_row(
					"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?1",
					[table],
					|row| row.get(0),
				)
				.optional()
				.unwrap();
			assert_eq!(found.as_deref(), Some(table));
		}
		drop(connection);
		fs::remove_dir_all(state_dir).unwrap();
	}

	#[test]
	fn reopen_is_idempotent_and_preserves_metadata() {
		let state_dir = temporary_state_dir("reopen");
		let first = Store::open(&state_dir).unwrap();
		first.set_meta("bootstrap_state", "COMMITTED").unwrap();
		drop(first);

		let second = Store::open(&state_dir).unwrap();
		assert_eq!(second.get_meta("bootstrap_state").unwrap().as_deref(), Some("COMMITTED"));
		assert_eq!(second.get_meta("schema_version").unwrap(), Some(SCHEMA_VERSION.to_string()));
		drop(second);
		fs::remove_dir_all(state_dir).unwrap();
	}

	#[test]
	fn round_trips_metadata_and_idempotency() {
		let store = Store::default();
		store.put("health", "healthy");
		assert_eq!(store.get("health").as_deref(), Some("healthy"));
		assert_eq!(store.get("missing"), None);

		assert_eq!(store.replay_idempotency("lock.acquire", "k", "{\"a\":1}", 10).unwrap(), None);
		store
			.store_idempotency_response("lock.acquire", "k", "{\"a\":1}", "{\"lease\":1}", 10)
			.unwrap();
		assert_eq!(
			store.replay_idempotency("lock.acquire", "k", "{\"a\":1}", 11).unwrap().as_deref(),
			Some("{\"lease\":1}")
		);
		assert!(store.replay_idempotency("lock.acquire", "k", "{\"a\":2}", 11).is_err());
		assert!(store
			.store_idempotency_response("lock.acquire", "k", "{\"a\":2}", "{\"lease\":2}", 11)
			.is_err());
		assert_eq!(
			store
				.replay_idempotency("lock.acquire", "k", "{\"a\":2}", 10 + IDEMPOTENCY_WINDOW_MS)
				.unwrap(),
			None
		);
	}
}
