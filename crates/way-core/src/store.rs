//! Durable SQLite state for the gateway.
//!
//! The store owns the only SQLite connection used by the P1 core.  It is
//! serialized behind a mutex because the lock and journal protocols rely on
//! short, blocking write transactions rather than an async executor.

use std::{
    fmt, fs, io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, Transaction};

pub const DATABASE_FILENAME: &str = "way-core.sqlite3";
pub const SCHEMA_VERSION: u32 = 7;
pub const IDEMPOTENCY_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;
pub const CLOSURE_OPERATION_META_KEY: &str = "gitlock_closure_operation";

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
    ClosureOperationInProgress,
    ClosureOperationMissing,
    ClosureOperationChanged,
    MainAdmissionOperationMissing,
    MainAdmissionOperationChanged,
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "store I/O error: {error}"),
            Self::Sql(error) => write!(formatter, "SQLite error: {error}"),
            Self::Poisoned => formatter.write_str("SQLite connection mutex was poisoned"),
            Self::Integrity(result) => write!(formatter, "SQLite integrity_check failed: {result}"),
            Self::UnsupportedSchema(version) => {
                write!(
                    formatter,
                    "database schema version {version} is newer than this gateway"
                )
            }
            Self::InvalidMetadata(message) => {
                write!(formatter, "invalid gateway metadata: {message}")
            }
            Self::IdempotencyConflict => {
                formatter.write_str("idempotency key was reused for a different request")
            }
            Self::ClosureOperationInProgress => formatter.write_str("another corpus closure operation is pending recovery"),
            Self::ClosureOperationMissing => formatter.write_str("closure operation intent is missing"),
            Self::ClosureOperationChanged => formatter.write_str("closure operation intent changed before finalization"),
            Self::MainAdmissionOperationMissing => formatter.write_str("main admission operation intent is missing"),
            Self::MainAdmissionOperationChanged => formatter.write_str("main admission operation intent changed before finalization"),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClosureOperationClaim {
    Claimed,
    Existing { response_json: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MainAdmissionOperationClaim {
    Claimed,
    Existing { response_json: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingMainAdmissionOperation {
    pub scope: String,
    pub key: String,
    pub request_json: String,
    pub intent_json: String,
}

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
        Ok(Self {
            inner: Arc::new(StoreInner {
                connection: Mutex::new(connection),
                path,
            }),
        })
    }

    pub fn database_path(&self) -> Option<&Path> {
        self.inner.path.as_deref()
    }

    pub(crate) fn connection(&self) -> StoreResult<MutexGuard<'_, Connection>> {
        self.inner
            .connection
            .lock()
            .map_err(|_| StoreError::Poisoned)
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

    pub fn journal_generation(&self) -> StoreResult<u64> {
        let raw = self.get_meta("journal_generation")?.ok_or_else(|| {
            StoreError::InvalidMetadata("journal_generation is missing".to_owned())
        })?;
        raw.parse()
            .map_err(|_| StoreError::InvalidMetadata("journal_generation is not a u64".to_owned()))
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
        expire_idempotency_tx(&transaction, now_ms)?;
        let existing = transaction
			.query_row(
				"SELECT request_json, response_json FROM idempotency WHERE scope = ?1 AND idempotency_key = ?2",
				rusqlite::params![scope, key],
				|row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
			)
			.optional()?;
        transaction.commit()?;

        match existing {
            Some((stored_request, _)) if stored_request != request_json => {
                Err(StoreError::IdempotencyConflict)
            }
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
        expire_idempotency_tx(&transaction, now_ms)?;
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

    /// Atomically reserves a corpus-closure idempotency key and writes the
    /// matching durable operation intent before Git is allowed to mutate.
    pub fn claim_closure_operation(
        &self,
        scope: &str,
        key: &str,
        request_json: &str,
        intent_json: &str,
        operation_json: &str,
        now_ms: i64,
    ) -> StoreResult<ClosureOperationClaim> {
        let expires_at = now_ms.checked_add(IDEMPOTENCY_WINDOW_MS).ok_or_else(|| {
            StoreError::InvalidMetadata("idempotency expiration overflowed i64".to_owned())
        })?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        expire_idempotency_tx(&transaction, now_ms)?;
        let existing = transaction
            .query_row(
                "SELECT request_json, response_json FROM idempotency WHERE scope = ?1 AND idempotency_key = ?2",
                rusqlite::params![scope, key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        match existing {
            Some((stored_request, _)) if stored_request != request_json => return Err(StoreError::IdempotencyConflict),
            Some((_, response_json)) => {
                transaction.commit()?;
                return Ok(ClosureOperationClaim::Existing { response_json });
            }
            None => {}
        }
        if meta_get_tx(&transaction, CLOSURE_OPERATION_META_KEY)?.is_some() {
            return Err(StoreError::ClosureOperationInProgress);
        }
        transaction.execute(
            "INSERT INTO idempotency(scope, idempotency_key, request_json, response_json, created_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![scope, key, request_json, intent_json, now_ms, expires_at],
        )?;
        meta_set_tx(&transaction, CLOSURE_OPERATION_META_KEY, operation_json)?;
        transaction.commit()?;
        Ok(ClosureOperationClaim::Claimed)
    }

    /// Atomically replaces a previously claimed intent with its completed
    /// response and removes its active-operation record.
    pub fn finalize_closure_operation(
        &self,
        scope: &str,
        key: &str,
        request_json: &str,
        intent_json: &str,
        operation_json: &str,
        response_json: &str,
        now_ms: i64,
    ) -> StoreResult<String> {
        let expires_at = now_ms.checked_add(IDEMPOTENCY_WINDOW_MS).ok_or_else(|| {
            StoreError::InvalidMetadata("idempotency expiration overflowed i64".to_owned())
        })?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let existing = transaction
            .query_row(
                "SELECT request_json, response_json FROM idempotency WHERE scope = ?1 AND idempotency_key = ?2",
                rusqlite::params![scope, key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((stored_request, stored_response)) = existing else {
            return Err(StoreError::ClosureOperationMissing);
        };
        if stored_request != request_json {
            return Err(StoreError::IdempotencyConflict);
        }
        if stored_response != intent_json {
            transaction.commit()?;
            return Ok(stored_response);
        }
        if meta_get_tx(&transaction, CLOSURE_OPERATION_META_KEY)?.as_deref() != Some(operation_json) {
            return Err(StoreError::ClosureOperationChanged);
        }
        transaction.execute(
            "UPDATE idempotency SET response_json = ?1, expires_at = ?2 WHERE scope = ?3 AND idempotency_key = ?4",
            rusqlite::params![response_json, expires_at, scope, key],
        )?;
        transaction.execute("DELETE FROM gateway_meta WHERE k = ?1", [CLOSURE_OPERATION_META_KEY])?;
        transaction.commit()?;
        Ok(response_json.to_owned())
    }

    /// Atomically reserves a main-admission idempotency key and its broker operation
    /// reference before any external broker command is allowed to run.
    pub fn claim_main_admission_operation(
        &self,
        scope: &str,
        key: &str,
        request_json: &str,
        intent_json: &str,
        now_ms: i64,
    ) -> StoreResult<MainAdmissionOperationClaim> {
        let expires_at = now_ms.checked_add(IDEMPOTENCY_WINDOW_MS).ok_or_else(|| {
            StoreError::InvalidMetadata("idempotency expiration overflowed i64".to_owned())
        })?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        expire_idempotency_tx(&transaction, now_ms)?;
        let existing = transaction
            .query_row(
                "SELECT request_json, response_json FROM idempotency WHERE scope = ?1 AND idempotency_key = ?2",
                rusqlite::params![scope, key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        match existing {
            Some((stored_request, _)) if stored_request != request_json => return Err(StoreError::IdempotencyConflict),
            Some((_, response_json)) => {
                transaction.commit()?;
                return Ok(MainAdmissionOperationClaim::Existing { response_json });
            }
            None => {}
        }
        transaction.execute(
            "INSERT INTO idempotency(scope, idempotency_key, request_json, response_json, created_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![scope, key, request_json, intent_json, now_ms, expires_at],
        )?;
        transaction.execute(
            "INSERT INTO main_admission_operations(scope, idempotency_key, request_json, intent_json)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![scope, key, request_json, intent_json],
        )?;
        transaction.commit()?;
        Ok(MainAdmissionOperationClaim::Claimed)
    }

    /// Atomically replaces a claimed pre-effect intent with its accepted response.
    pub fn finalize_main_admission_operation(
        &self,
        scope: &str,
        key: &str,
        request_json: &str,
        intent_json: &str,
        response_json: &str,
        now_ms: i64,
    ) -> StoreResult<String> {
        let expires_at = now_ms.checked_add(IDEMPOTENCY_WINDOW_MS).ok_or_else(|| {
            StoreError::InvalidMetadata("idempotency expiration overflowed i64".to_owned())
        })?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let existing = transaction
            .query_row(
                "SELECT request_json, response_json FROM idempotency WHERE scope = ?1 AND idempotency_key = ?2",
                rusqlite::params![scope, key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((stored_request, stored_response)) = existing else {
            return Err(StoreError::MainAdmissionOperationMissing);
        };
        if stored_request != request_json {
            return Err(StoreError::IdempotencyConflict);
        }
        if stored_response != intent_json {
            transaction.commit()?;
            return Ok(stored_response);
        }
        let operation_intent = transaction
            .query_row(
                "SELECT intent_json FROM main_admission_operations WHERE scope = ?1 AND idempotency_key = ?2",
                rusqlite::params![scope, key],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if operation_intent.as_deref() != Some(intent_json) {
            return Err(if operation_intent.is_some() {
                StoreError::MainAdmissionOperationChanged
            } else {
                StoreError::MainAdmissionOperationMissing
            });
        }
        transaction.execute(
            "UPDATE idempotency SET response_json = ?1, expires_at = ?2 WHERE scope = ?3 AND idempotency_key = ?4",
            rusqlite::params![response_json, expires_at, scope, key],
        )?;
        transaction.execute(
            "DELETE FROM main_admission_operations WHERE scope = ?1 AND idempotency_key = ?2",
            rusqlite::params![scope, key],
        )?;
        transaction.commit()?;
        Ok(response_json.to_owned())
    }

    /// Atomically abandons a pre-effect admission after the broker returned a
    /// definitive rejection envelope, leaving the idempotency key retryable.
    pub fn abandon_main_admission_operation(
        &self,
        scope: &str,
        key: &str,
        request_json: &str,
        intent_json: &str,
    ) -> StoreResult<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let existing = transaction
            .query_row(
                "SELECT request_json, response_json FROM idempotency WHERE scope = ?1 AND idempotency_key = ?2",
                rusqlite::params![scope, key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((stored_request, stored_response)) = existing else {
            return Err(StoreError::MainAdmissionOperationMissing);
        };
        if stored_request != request_json {
            return Err(StoreError::IdempotencyConflict);
        }
        if stored_response != intent_json {
            return Err(StoreError::MainAdmissionOperationChanged);
        }
        let operation_intent = transaction
            .query_row(
                "SELECT intent_json FROM main_admission_operations WHERE scope = ?1 AND idempotency_key = ?2",
                rusqlite::params![scope, key],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if operation_intent.as_deref() != Some(intent_json) {
            return Err(if operation_intent.is_some() {
                StoreError::MainAdmissionOperationChanged
            } else {
                StoreError::MainAdmissionOperationMissing
            });
        }
        transaction.execute(
            "DELETE FROM main_admission_operations WHERE scope = ?1 AND idempotency_key = ?2",
            rusqlite::params![scope, key],
        )?;
        transaction.execute(
            "DELETE FROM idempotency WHERE scope = ?1 AND idempotency_key = ?2",
            rusqlite::params![scope, key],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Lists only unresolved pre-effect claims so startup can prove broker acceptance without resending.
    pub fn pending_main_admission_operations(&self) -> StoreResult<Vec<PendingMainAdmissionOperation>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT scope, idempotency_key, request_json, intent_json
             FROM main_admission_operations ORDER BY scope, idempotency_key",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(PendingMainAdmissionOperation {
                scope: row.get(0)?,
                key: row.get(1)?,
                request_json: row.get(2)?,
                intent_json: row.get(3)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::from)
    }
}

fn expire_idempotency_tx(transaction: &Transaction<'_>, now_ms: i64) -> StoreResult<()> {
    transaction.execute(
        "DELETE FROM idempotency
         WHERE expires_at <= ?1
           AND NOT EXISTS (
               SELECT 1 FROM main_admission_operations AS pending
               WHERE pending.scope = idempotency.scope
                 AND pending.idempotency_key = idempotency.idempotency_key
           )",
        [now_ms],
    )?;
    Ok(())
}

pub(crate) fn meta_get(connection: &Connection, key: &str) -> StoreResult<Option<String>> {
    connection
        .query_row("SELECT v FROM gateway_meta WHERE k = ?1", [key], |row| {
            row.get(0)
        })
        .optional()
        .map_err(StoreError::from)
}

pub(crate) fn meta_get_tx(transaction: &Transaction<'_>, key: &str) -> StoreResult<Option<String>> {
    transaction
        .query_row("SELECT v FROM gateway_meta WHERE k = ?1", [key], |row| {
            row.get(0)
        })
        .optional()
        .map_err(StoreError::from)
}

pub(crate) fn meta_set_tx(
    transaction: &Transaction<'_>,
    key: &str,
    value: &str,
) -> StoreResult<()> {
    transaction.execute(
        "INSERT INTO gateway_meta(k, v) VALUES (?1, ?2)
		 ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        rusqlite::params![key, value],
    )?;
    Ok(())
}

fn configure_connection(connection: &mut Connection) -> StoreResult<()> {
    connection.busy_timeout(Duration::from_secs(5))?;
    // Lease transitions, journal receipts, and consumer settlements are durable
    // proofs. FULL synchronizes each committing WAL transaction before success,
    // trading throughput for a power-loss durability boundary operators can rely on.
    connection.execute_batch(
        "PRAGMA journal_mode = WAL;
		 PRAGMA foreign_keys = ON;
		 PRAGMA synchronous = FULL;",
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

    if current_version < 2 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
			"CREATE TABLE verification_receipts (
				receipt_id TEXT PRIMARY KEY NOT NULL,
				lease_id TEXT NOT NULL,
				lock_name TEXT NOT NULL,
				pid INTEGER NOT NULL,
				pid_start_time TEXT NOT NULL,
				pgid INTEGER NOT NULL,
				pgid_start_time TEXT,
				process_inspected INTEGER NOT NULL CHECK (process_inspected IN (0, 1)),
				git_status_checked INTEGER NOT NULL CHECK (git_status_checked IN (0, 1)),
				git_log_checked INTEGER NOT NULL CHECK (git_log_checked IN (0, 1)),
				git_fsck_checked INTEGER NOT NULL CHECK (git_fsck_checked IN (0, 1)),
				remote_verified INTEGER NOT NULL CHECK (remote_verified IN (0, 1)),
				verified_at INTEGER NOT NULL,
				consumed_at INTEGER,
				FOREIGN KEY (lease_id) REFERENCES leases(lease_id)
			);
			CREATE INDEX verification_receipts_lease_idx ON verification_receipts(lease_id, lock_name, consumed_at);",
		)?;
        meta_set_tx(&transaction, "schema_version", "2")?;
        transaction.commit()?;
    }

    if current_version < 3 {
        let transaction = connection.transaction()?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS main_admission_operations (
                scope TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                request_json TEXT NOT NULL,
                intent_json TEXT NOT NULL,
                PRIMARY KEY (scope, idempotency_key),
                FOREIGN KEY (scope, idempotency_key) REFERENCES idempotency(scope, idempotency_key)
            );",
        )?;
        meta_set_tx(&transaction, "schema_version", "3")?;
        transaction.commit()?;
    }

    if current_version < 4 {
        let transaction = connection.transaction()?;
        if meta_get_tx(&transaction, "transcript_proof")?.is_none() {
            let transcript_proof = meta_get_tx(&transaction, "main_identity")?
                .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
                .and_then(|identity| identity.as_object().is_some_and(|identity| identity.contains_key("transcript")).then_some("proven"))
                .unwrap_or("pending");
            meta_set_tx(&transaction, "transcript_proof", transcript_proof)?;
        }
        meta_set_tx(&transaction, "schema_version", "4")?;
        transaction.commit()?;
    }

    if current_version < 5 {
        let transaction = connection.transaction()?;
        if meta_get_tx(&transaction, "tail_ring_rotation_count")?.is_none() {
            meta_set_tx(&transaction, "tail_ring_rotation_count", "0")?;
        }
        meta_set_tx(&transaction, "schema_version", "5")?;
        transaction.commit()?;
    }

    // v5 used session.checkpoint coordinates as an event-ring watermark. Those
    // coordinate systems are unrelated, so discard every v5 ring watermark and
    // let the first actual tail envelope establish a new one.
    if current_version < 6 {
        let transaction = connection.transaction()?;
        meta_set_tx(&transaction, "tail_checkpoint", "null")?;
        meta_set_tx(&transaction, "tail_ring_rotation_count", "0")?;
        meta_set_tx(&transaction, "schema_version", "6")?;
        transaction.commit()?;
    }

    if current_version < 7 {
        let transaction = connection.transaction()?;
        if meta_get_tx(&transaction, "transcript_delivery_gap_count")?.is_none() {
            meta_set_tx(&transaction, "transcript_delivery_gap_count", "0")?;
        }
        meta_set_tx(&transaction, "schema_version", "7")?;
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
        ("tail_checkpoint", "null"),
        ("transcript_delivery_progress", "null"),
        ("transcript_proof", "pending"),
        ("tail_ring_rotation_count", "0"),
        ("transcript_delivery_gap_count", "0"),
        ("journal_generation", "1"),
        ("boot_epoch", "0"),
        ("journal_floor_seq", "0"),
        ("lock_fencing_token", "0"),
        ("write_mode", "on"),
        ("reconcile_last_ok_at", "null"),
        ("reconcile_cycle_ms", "null"),
        ("reconcile_drift_count", "0"),
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

    use super::{
        CLOSURE_OPERATION_META_KEY, DATABASE_FILENAME, IDEMPOTENCY_WINDOW_MS, SCHEMA_VERSION,
        ClosureOperationClaim, MainAdmissionOperationClaim, Store, StoreError,
    };
    use rusqlite::{Connection, OptionalExtension};

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
        assert_eq!(
            store.get_meta("schema_version").unwrap(),
            Some(SCHEMA_VERSION.to_string())
        );
        assert_eq!(store.get_meta("transcript_proof").unwrap().as_deref(), Some("pending"));
        assert_eq!(store.get_meta("tail_ring_rotation_count").unwrap().as_deref(), Some("0"));
        assert_eq!(store.get_meta("transcript_delivery_gap_count").unwrap().as_deref(), Some("0"));

        let connection = store.connection().unwrap();
        let journal_mode: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode.to_lowercase(), "wal");
        let synchronous: i64 = connection
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            synchronous, 2,
            "durable proof tables require synchronous=FULL"
        );
        for table in [
            "sessions",
            "surfaces",
            "gateway_meta",
            "leases",
            "verification_receipts",
            "events",
            "consumer_checkpoints",
            "outbox",
            "idempotency",
            "main_admission_operations",
        ] {
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
        let mut statement = connection.prepare("PRAGMA table_info(sessions)").unwrap();
        let session_columns = statement
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for column in [
            "session_id",
            "kind",
            "purpose",
            "brief",
            "status",
            "surface_id",
            "locator",
            "endpoint_generation",
            "host_incarnation",
            "identity_provenance",
            "index_seq",
            "live",
            "deleted",
            "terminal_uncertain",
            "ambiguous",
            "activity_state",
            "activity_at",
            "last_heartbeat_at",
            "meta_name",
            "meta_cwd",
            "meta_kind",
            "metadata_state",
            "metadata_at",
            "source",
            "created_at",
            "last_seen_at",
            "closed_at",
            "registry_rev",
        ] {
            assert!(
                session_columns.iter().any(|candidate| candidate == column),
                "sessions.{column} is missing"
            );
        }
        drop(statement);
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
        assert_eq!(
            second.get_meta("bootstrap_state").unwrap().as_deref(),
            Some("COMMITTED")
        );
        assert_eq!(
            second.get_meta("schema_version").unwrap(),
            Some(SCHEMA_VERSION.to_string())
        );
        drop(second);
        fs::remove_dir_all(state_dir).unwrap();
    }

    #[test]
    fn v1_state_directory_migrates_to_bound_verification_receipts() {
        let state_dir = temporary_state_dir("v1-migration");
        let database_path = state_dir.join(DATABASE_FILENAME);
        drop(Store::open(&state_dir).unwrap());
        let connection = Connection::open(&database_path).unwrap();
        connection
			.execute_batch("DROP TABLE verification_receipts; UPDATE gateway_meta SET v = '1' WHERE k = 'schema_version';")
			.unwrap();
        drop(connection);

        let migrated = Store::open(&state_dir).unwrap();
        assert_eq!(
            migrated.get_meta("schema_version").unwrap(),
            Some(SCHEMA_VERSION.to_string())
        );
        let connection = migrated.connection().unwrap();
        let found: Option<String> = connection
			.query_row(
				"SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'verification_receipts'",
				[],
				|row| row.get(0),
			)
			.optional()
			.unwrap();
        assert_eq!(found.as_deref(), Some("verification_receipts"));
        drop(connection);
        drop(migrated);
        fs::remove_dir_all(state_dir).unwrap();
    }

    #[test]
    fn v3_identity_with_a_fingerprint_migrates_to_a_proven_transcript_proof() {
        let state_dir = temporary_state_dir("v3-transcript-proof");
        let database_path = state_dir.join(DATABASE_FILENAME);
        let store = Store::open(&state_dir).unwrap();
        store
            .set_meta(
                "main_identity",
                r#"{"version":1,"sessionId":"main","locator":{"repo":"/repo","stateRoot":"/repo/.gjc/state"},"endpointGeneration":1,"transcript":{"entryCount":0,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}"#,
            )
            .unwrap();
        drop(store);

        let connection = Connection::open(&database_path).unwrap();
        connection
            .execute_batch("DELETE FROM gateway_meta WHERE k = 'transcript_proof'; UPDATE gateway_meta SET v = '3' WHERE k = 'schema_version';")
            .unwrap();
        drop(connection);

        let migrated = Store::open(&state_dir).unwrap();
        assert_eq!(migrated.get_meta("transcript_proof").unwrap().as_deref(), Some("proven"));
        assert_eq!(migrated.get_meta("schema_version").unwrap(), Some(SCHEMA_VERSION.to_string()));
        drop(migrated);
        fs::remove_dir_all(state_dir).unwrap();
    }

    #[test]
    fn v4_state_directory_initializes_the_durable_tail_ring_rotation_count() {
        let state_dir = temporary_state_dir("v4-tail-ring-rotation");
        let database_path = state_dir.join(DATABASE_FILENAME);
        drop(Store::open(&state_dir).unwrap());

        let connection = Connection::open(&database_path).unwrap();
        connection
            .execute_batch("DELETE FROM gateway_meta WHERE k = 'tail_ring_rotation_count'; UPDATE gateway_meta SET v = '4' WHERE k = 'schema_version';")
            .unwrap();
        drop(connection);

        let migrated = Store::open(&state_dir).unwrap();
        assert_eq!(migrated.get_meta("tail_ring_rotation_count").unwrap().as_deref(), Some("0"));
        assert_eq!(migrated.get_meta("transcript_delivery_gap_count").unwrap().as_deref(), Some("0"));
        assert_eq!(migrated.get_meta("schema_version").unwrap(), Some(SCHEMA_VERSION.to_string()));
        drop(migrated);
        fs::remove_dir_all(state_dir).unwrap();
    }

    #[test]
    fn v5_state_directory_discards_foreign_session_checkpoint_coordinates() {
        let state_dir = temporary_state_dir("v5-foreign-checkpoint");
        let database_path = state_dir.join(DATABASE_FILENAME);
        let store = Store::open(&state_dir).unwrap();
        store.set_meta("tail_checkpoint", r#"{"revision":1696,"generation":0,"seq":0}"#).unwrap();
        store.set_meta("tail_ring_rotation_count", "7").unwrap();
        drop(store);

        let connection = Connection::open(&database_path).unwrap();
        connection.execute_batch("UPDATE gateway_meta SET v = '5' WHERE k = 'schema_version';").unwrap();
        drop(connection);

        let migrated = Store::open(&state_dir).unwrap();
        assert_eq!(migrated.get_meta("tail_checkpoint").unwrap().as_deref(), Some("null"));
        assert_eq!(migrated.get_meta("tail_ring_rotation_count").unwrap().as_deref(), Some("0"));
        assert_eq!(migrated.get_meta("transcript_delivery_gap_count").unwrap().as_deref(), Some("0"));
        assert_eq!(migrated.get_meta("schema_version").unwrap(), Some(SCHEMA_VERSION.to_string()));
        drop(migrated);
        fs::remove_dir_all(state_dir).unwrap();
    }

    #[test]
    fn round_trips_metadata_and_idempotency() {
        let store = Store::default();
        store.set_meta("health", "healthy").unwrap();
        assert_eq!(
            store.get_meta("health").unwrap().as_deref(),
            Some("healthy")
        );
        assert_eq!(store.get_meta("missing").unwrap(), None);

        assert_eq!(
            store
                .replay_idempotency("lock.acquire", "k", "{\"a\":1}", 10)
                .unwrap(),
            None
        );
        store
            .store_idempotency_response("lock.acquire", "k", "{\"a\":1}", "{\"lease\":1}", 10)
            .unwrap();
        assert_eq!(
            store
                .replay_idempotency("lock.acquire", "k", "{\"a\":1}", 11)
                .unwrap()
                .as_deref(),
            Some("{\"lease\":1}")
        );
        assert!(
            store
                .replay_idempotency("lock.acquire", "k", "{\"a\":2}", 11)
                .is_err()
        );
        assert!(
            store
                .store_idempotency_response("lock.acquire", "k", "{\"a\":2}", "{\"lease\":2}", 11)
                .is_err()
        );
        assert_eq!(
            store
                .replay_idempotency("lock.acquire", "k", "{\"a\":2}", 10 + IDEMPOTENCY_WINDOW_MS)
                .unwrap(),
            None
        );
    }

    #[test]
    fn closure_operation_intent_binds_idempotency_before_final_response() {
        let store = Store::default();
        let scope = "main.corpus.close";
        let request = r#"{"commit_message":"close","idempotency_key":"key","paths":["file.txt"]}"#;
        let intent = r#"{"operationId":"operation","requestHash":"hash"}"#;
        let operation = r#"{"intentJson":"{\"operationId\":\"operation\",\"requestHash\":\"hash\"}","state":"intent"}"#;
        let response = r#"{"committed":true,"fencing_token":"2","lease_id":"lease"}"#;

        assert_eq!(
            store
                .claim_closure_operation(scope, "key", request, intent, operation, 100)
                .unwrap(),
            ClosureOperationClaim::Claimed
        );
        assert_eq!(
            store.get_meta(CLOSURE_OPERATION_META_KEY).unwrap().as_deref(),
            Some(operation)
        );
        assert_eq!(
            store
                .claim_closure_operation(scope, "key", request, intent, operation, 101)
                .unwrap(),
            ClosureOperationClaim::Existing { response_json: intent.to_owned() }
        );
        assert!(matches!(
            store.claim_closure_operation(scope, "key", r#"{"different":true}"#, intent, operation, 101),
            Err(StoreError::IdempotencyConflict)
        ));

        assert_eq!(
            store
                .finalize_closure_operation(scope, "key", request, intent, operation, response, 102)
                .unwrap(),
            response
        );
        assert_eq!(store.get_meta(CLOSURE_OPERATION_META_KEY).unwrap(), None);
        assert_eq!(store.replay_idempotency(scope, "key", request, 103).unwrap().as_deref(), Some(response));
    }

    #[test]
    fn main_admission_claim_is_pre_effect_and_finalizes_without_replay_resend() {
        let store = Store::default();
        let scope = "main.submit";
        let request = r#"{"idempotency_key":"key","surface_id":"owner","text":"send once"}"#;
        let intent = r#"{"delivered_as":"prompt","op_ref":"operation","request_hash":"hash","state":"claimed","version":1}"#;
        let response = r#"{"accepted":true,"delivered_as":"prompt","op_ref":"operation"}"#;

        assert_eq!(
            store.claim_main_admission_operation(scope, "key", request, intent, 100).unwrap(),
            MainAdmissionOperationClaim::Claimed
        );
        assert_eq!(
            store.claim_main_admission_operation(scope, "key", request, intent, 101).unwrap(),
            MainAdmissionOperationClaim::Existing { response_json: intent.to_owned() }
        );
        assert_eq!(
            store.pending_main_admission_operations().unwrap(),
            vec![super::PendingMainAdmissionOperation {
                scope: scope.to_owned(),
                key: "key".to_owned(),
                request_json: request.to_owned(),
                intent_json: intent.to_owned(),
            }]
        );
        assert!(matches!(
            store.claim_main_admission_operation(scope, "key", r#"{"different":true}"#, intent, 101),
            Err(StoreError::IdempotencyConflict)
        ));

        assert_eq!(
            store.finalize_main_admission_operation(scope, "key", request, intent, response, 102).unwrap(),
            response
        );
        assert!(store.pending_main_admission_operations().unwrap().is_empty());
        assert_eq!(store.replay_idempotency(scope, "key", request, 103).unwrap().as_deref(), Some(response));
    }

    #[test]
    fn definitive_main_admission_rejection_abandons_only_the_matching_claim() {
        let store = Store::default();
        let scope = "main.submit";
        let rejected_request = r#"{"idempotency_key":"rejected","surface_id":"owner","text":"retry"}"#;
        let rejected_intent = r#"{"delivered_as":"prompt","op_ref":"rejected-op","request_hash":"hash","state":"claimed","version":1}"#;
        let unrelated_request = r#"{"idempotency_key":"other","surface_id":"owner","text":"other"}"#;
        let unrelated_intent = r#"{"delivered_as":"prompt","op_ref":"other-op","request_hash":"hash","state":"claimed","version":1}"#;
        assert_eq!(
            store.claim_main_admission_operation(scope, "rejected", rejected_request, rejected_intent, 100).unwrap(),
            MainAdmissionOperationClaim::Claimed
        );
        assert_eq!(
            store.claim_main_admission_operation(scope, "other", unrelated_request, unrelated_intent, 100).unwrap(),
            MainAdmissionOperationClaim::Claimed
        );
        store.abandon_main_admission_operation(scope, "rejected", rejected_request, rejected_intent).unwrap();
        assert_eq!(
            store.pending_main_admission_operations().unwrap(),
            vec![super::PendingMainAdmissionOperation {
                scope: scope.to_owned(),
                key: "other".to_owned(),
                request_json: unrelated_request.to_owned(),
                intent_json: unrelated_intent.to_owned(),
            }]
        );
        assert_eq!(store.replay_idempotency(scope, "rejected", rejected_request, 101).unwrap(), None);
        assert_eq!(
            store.claim_main_admission_operation(scope, "rejected", rejected_request, rejected_intent, 101).unwrap(),
            MainAdmissionOperationClaim::Claimed
        );
    }
}
