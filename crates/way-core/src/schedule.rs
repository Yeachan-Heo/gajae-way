//! Durable scheduler tables for the gateway's in-process scheduler.
//!
//! This module owns persistence only. Cron and interval arithmetic, IANA
//! timezone resolution, and the tick loop live in TypeScript; the store keeps
//! exactly one absolute `next_fire_at_ms` per job so due selection here is a
//! single indexed comparison and no calendar logic is duplicated.
//!
//! Two invariants are load-bearing and enforced here rather than by convention:
//!
//! 1. The durable failed-closed marker is read *inside* the claim transaction.
//!    Reading it before the transaction would be a TOCTOU: the gateway could
//!    enter failed-closed between the check and the claim, and the scheduler
//!    would then admit a turn the dispatcher would have refused.
//! 2. In-flight runs (`outcome IS NULL`) are reconciled before any overdue
//!    collapse, and a job holding one is excluded from that collapse, so a
//!    collapse can never swallow an in-flight occurrence or miscount it.

use std::fmt;

use rusqlite::{OptionalExtension, Row, Transaction, params};

use crate::store::{Store, StoreError, meta_get_tx};

pub const DEFAULT_LIST_LIMIT: u32 = 50;
pub const MAX_LIST_LIMIT: u32 = 200;

#[derive(Debug)]
pub enum ScheduleError {
    Store(StoreError),
    NotFound,
    InvalidInput(String),
    Overflow,
}

impl fmt::Display for ScheduleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => write!(formatter, "schedule store error: {error}"),
            Self::NotFound => formatter.write_str("schedule job not found"),
            Self::InvalidInput(detail) => write!(formatter, "invalid schedule input: {detail}"),
            Self::Overflow => formatter.write_str("schedule arithmetic overflowed"),
        }
    }
}

impl std::error::Error for ScheduleError {}

impl From<StoreError> for ScheduleError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<rusqlite::Error> for ScheduleError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(StoreError::Sql(error))
    }
}

pub type ScheduleResult<T> = Result<T, ScheduleError>;

macro_rules! string_enum {
    ($name:ident, $($variant:ident => $text:literal),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name {
            $($variant,)+
        }

        impl $name {
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text,)+
                }
            }

            pub fn parse(value: &str) -> ScheduleResult<Self> {
                match value {
                    $($text => Ok(Self::$variant),)+
                    other => Err(ScheduleError::InvalidInput(format!(
                        concat!("unsupported ", stringify!($name), ": {}"),
                        other
                    ))),
                }
            }
        }
    };
}

string_enum!(ScheduleKind, At => "at", Every => "every", Cron => "cron");
string_enum!(SchedulePayloadKind, SystemEvent => "system_event", Submit => "submit");
string_enum!(
    ScheduleJobState,
    Active => "active",
    Backoff => "backoff",
    Suspended => "suspended",
    Completed => "completed",
);
string_enum!(ScheduleTrigger, Timer => "timer", Manual => "manual", Overdue => "overdue");
string_enum!(
    ScheduleRunOutcome,
    Ok => "ok",
    Failed => "failed",
    DeferredBusy => "deferred_busy",
    ExpiredDeferred => "expired_deferred",
    Missed => "missed",
    SkippedOverdue => "skipped_overdue",
    RefusedFailedClosed => "refused_failed_closed",
    RefusedQuarantined => "refused_quarantined",
    RefusedNotReady => "refused_not_ready",
    RefusedSurfaceQuarantined => "refused_surface_quarantined",
    InterruptedUnknown => "interrupted_unknown",
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleJob {
    pub job_id: String,
    pub name: String,
    pub kind: ScheduleKind,
    pub spec: String,
    pub timezone: String,
    pub payload_kind: SchedulePayloadKind,
    pub payload_json: String,
    pub surface_id: Option<String>,
    pub state: ScheduleJobState,
    pub next_fire_at_ms: Option<i64>,
    pub backoff_until_ms: Option<i64>,
    pub failure_count: i64,
    pub max_consecutive_failures: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleRun {
    pub run_id: String,
    pub job_id: String,
    pub trigger: ScheduleTrigger,
    pub scheduled_for_ms: i64,
    pub claimed_at: i64,
    pub finished_at: Option<i64>,
    pub attempt: i64,
    pub outcome: Option<ScheduleRunOutcome>,
    pub missed_count: i64,
    pub op_ref: Option<String>,
    pub detail_json: Option<String>,
}

/// One durable job definition written by `schedule.create` / `schedule.update`.
#[derive(Debug, Clone)]
pub struct ScheduleJobUpsert {
    pub job_id: String,
    pub name: String,
    pub kind: ScheduleKind,
    pub spec: String,
    pub timezone: String,
    pub payload_kind: SchedulePayloadKind,
    pub payload_json: String,
    pub surface_id: Option<String>,
    pub next_fire_at_ms: Option<i64>,
    pub max_consecutive_failures: i64,
    pub now_ms: i64,
}

/// Result of one due-claim attempt.
///
/// `RefusedFailedClosed` is distinct from `NothingDue` on purpose: the caller
/// records a durable refusal run for the former and simply idles for the latter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DueClaim {
    Claimed { job: ScheduleJob, run: ScheduleRun },
    NothingDue,
    RefusedFailedClosed { reason: String },
}

/// Terminal update applied to one claimed run plus its owning job.
#[derive(Debug, Clone)]
pub struct RunFinalize {
    pub run_id: String,
    pub outcome: ScheduleRunOutcome,
    pub finished_at: i64,
    pub op_ref: Option<String>,
    pub detail_json: Option<String>,
    /// `None` leaves job scheduling state untouched (used by pure refusals).
    pub job_state: Option<ScheduleJobState>,
    pub failure_count: Option<i64>,
    pub backoff_until_ms: Option<i64>,
    /// Journal payload appended in the same transaction as the finalize.
    pub journal_payload_json: Option<String>,
}

const JOB_COLUMNS: &str = "job_id, name, kind, spec, timezone, payload_kind, payload_json, surface_id, state, \
     next_fire_at_ms, backoff_until_ms, failure_count, max_consecutive_failures, created_at, updated_at";

const RUN_COLUMNS: &str = "run_id, job_id, trigger, scheduled_for_ms, claimed_at, finished_at, attempt, outcome, \
     missed_count, op_ref, detail_json";

fn job_from_row(row: &Row<'_>) -> rusqlite::Result<ScheduleJob> {
    let kind: String = row.get(2)?;
    let payload_kind: String = row.get(5)?;
    let state: String = row.get(8)?;
    let invalid = |error: ScheduleError| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    };
    Ok(ScheduleJob {
        job_id: row.get(0)?,
        name: row.get(1)?,
        kind: ScheduleKind::parse(&kind).map_err(invalid)?,
        spec: row.get(3)?,
        timezone: row.get(4)?,
        payload_kind: SchedulePayloadKind::parse(&payload_kind).map_err(invalid)?,
        payload_json: row.get(6)?,
        surface_id: row.get(7)?,
        state: ScheduleJobState::parse(&state).map_err(invalid)?,
        next_fire_at_ms: row.get(9)?,
        backoff_until_ms: row.get(10)?,
        failure_count: row.get(11)?,
        max_consecutive_failures: row.get(12)?,
        created_at: row.get(13)?,
        updated_at: row.get(14)?,
    })
}

fn run_from_row(row: &Row<'_>) -> rusqlite::Result<ScheduleRun> {
    let trigger: String = row.get(2)?;
    let outcome: Option<String> = row.get(7)?;
    let invalid = |error: ScheduleError| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    };
    Ok(ScheduleRun {
        run_id: row.get(0)?,
        job_id: row.get(1)?,
        trigger: ScheduleTrigger::parse(&trigger).map_err(invalid)?,
        scheduled_for_ms: row.get(3)?,
        claimed_at: row.get(4)?,
        finished_at: row.get(5)?,
        attempt: row.get(6)?,
        outcome: match outcome {
            Some(value) => Some(ScheduleRunOutcome::parse(&value).map_err(invalid)?),
            None => None,
        },
        missed_count: row.get(8)?,
        op_ref: row.get(9)?,
        detail_json: row.get(10)?,
    })
}

fn clamp_limit(limit: Option<u32>) -> u32 {
    limit.unwrap_or(DEFAULT_LIST_LIMIT).clamp(1, MAX_LIST_LIMIT)
}

/// Reads the durable failed-closed marker. `failed_closed_reason` is stored as
/// canonical JSON, so a cleared marker is the literal `null` rather than an
/// absent key.
fn failed_closed_reason_tx(transaction: &Transaction<'_>) -> ScheduleResult<Option<String>> {
    let raw = meta_get_tx(transaction, "failed_closed_reason")?;
    Ok(match raw {
        None => None,
        Some(value) if value.trim() == "null" => None,
        Some(value) => Some(value),
    })
}

fn job_get_tx(transaction: &Transaction<'_>, job_id: &str) -> ScheduleResult<Option<ScheduleJob>> {
    let sql = format!("SELECT {JOB_COLUMNS} FROM schedule_jobs WHERE job_id = ?1");
    Ok(transaction
        .query_row(&sql, params![job_id], job_from_row)
        .optional()?)
}

pub fn job_upsert(store: &Store, upsert: ScheduleJobUpsert) -> ScheduleResult<ScheduleJob> {
    if upsert.job_id.trim().is_empty() {
        return Err(ScheduleError::InvalidInput("job_id must not be empty".to_owned()));
    }
    if upsert.name.trim().is_empty() {
        return Err(ScheduleError::InvalidInput("name must not be empty".to_owned()));
    }
    if upsert.max_consecutive_failures < 1 {
        return Err(ScheduleError::InvalidInput(
            "max_consecutive_failures must be positive".to_owned(),
        ));
    }
    match (upsert.payload_kind, upsert.surface_id.as_deref()) {
        (SchedulePayloadKind::Submit, None) => {
            return Err(ScheduleError::InvalidInput(
                "submit payloads require a surface_id".to_owned(),
            ));
        }
        (SchedulePayloadKind::SystemEvent, Some(_)) => {
            return Err(ScheduleError::InvalidInput(
                "system_event payloads must not carry a surface_id".to_owned(),
            ));
        }
        _ => {}
    }
    let mut connection = store.connection()?;
    let transaction = connection.transaction()?;
    transaction.execute(
        "INSERT INTO schedule_jobs(
             job_id, name, kind, spec, timezone, payload_kind, payload_json, surface_id, state,
             next_fire_at_ms, backoff_until_ms, failure_count, max_consecutive_failures, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'active', ?9, NULL, 0, ?10, ?11, ?11)
         ON CONFLICT(job_id) DO UPDATE SET
             name = excluded.name,
             kind = excluded.kind,
             spec = excluded.spec,
             timezone = excluded.timezone,
             payload_json = excluded.payload_json,
             surface_id = excluded.surface_id,
             next_fire_at_ms = excluded.next_fire_at_ms,
             max_consecutive_failures = excluded.max_consecutive_failures,
             updated_at = excluded.updated_at",
        params![
            upsert.job_id,
            upsert.name,
            upsert.kind.as_str(),
            upsert.spec,
            upsert.timezone,
            upsert.payload_kind.as_str(),
            upsert.payload_json,
            upsert.surface_id,
            upsert.next_fire_at_ms,
            upsert.max_consecutive_failures,
            upsert.now_ms,
        ],
    )?;
    let job = job_get_tx(&transaction, &upsert.job_id)?.ok_or(ScheduleError::NotFound)?;
    transaction.commit()?;
    Ok(job)
}

pub fn job_get(store: &Store, job_id: &str) -> ScheduleResult<ScheduleJob> {
    let mut connection = store.connection()?;
    let transaction = connection.transaction()?;
    let job = job_get_tx(&transaction, job_id)?.ok_or(ScheduleError::NotFound)?;
    transaction.commit()?;
    Ok(job)
}

pub fn job_list(
    store: &Store,
    state: Option<ScheduleJobState>,
    cursor: Option<String>,
    limit: Option<u32>,
) -> ScheduleResult<(Vec<ScheduleJob>, Option<String>)> {
    let limit = clamp_limit(limit);
    let cursor = cursor.unwrap_or_default();
    let connection = store.connection()?;
    let sql = format!(
        "SELECT {JOB_COLUMNS} FROM schedule_jobs
         WHERE job_id > ?1 AND (?2 IS NULL OR state = ?2)
         ORDER BY job_id
         LIMIT ?3"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(
        params![cursor, state.map(ScheduleJobState::as_str), i64::from(limit) + 1],
        job_from_row,
    )?;
    let mut jobs = rows.collect::<Result<Vec<_>, _>>()?;
    let next_cursor = if jobs.len() > limit as usize {
        jobs.truncate(limit as usize);
        jobs.last().map(|job| job.job_id.clone())
    } else {
        None
    };
    Ok((jobs, next_cursor))
}

pub fn job_delete(store: &Store, job_id: &str) -> ScheduleResult<bool> {
    let mut connection = store.connection()?;
    let transaction = connection.transaction()?;
    let deleted = transaction.execute("DELETE FROM schedule_jobs WHERE job_id = ?1", params![job_id])?;
    transaction.commit()?;
    Ok(deleted > 0)
}

/// Claims the earliest due job and records its pre-effect run row.
///
/// The claim row and the advanced `next_fire_at_ms` commit together, before any
/// admission is attempted, so a crash after this transaction leaves exactly one
/// durable in-flight run to reconcile rather than an ambiguous replay.
pub fn due_claim(store: &Store, now_ms: i64, run_id: &str) -> ScheduleResult<DueClaim> {
    if run_id.trim().is_empty() {
        return Err(ScheduleError::InvalidInput("run_id must not be empty".to_owned()));
    }
    let mut connection = store.connection()?;
    let transaction = connection.transaction()?;

    if let Some(reason) = failed_closed_reason_tx(&transaction)? {
        transaction.commit()?;
        return Ok(DueClaim::RefusedFailedClosed { reason });
    }

    let sql = format!(
        "SELECT {JOB_COLUMNS} FROM schedule_jobs
         WHERE state IN ('active','backoff')
           AND next_fire_at_ms IS NOT NULL
           AND next_fire_at_ms <= ?1
           AND (backoff_until_ms IS NULL OR backoff_until_ms <= ?1)
         ORDER BY next_fire_at_ms, job_id
         LIMIT 1"
    );
    let job = transaction
        .query_row(&sql, params![now_ms], job_from_row)
        .optional()?;
    let Some(job) = job else {
        transaction.commit()?;
        return Ok(DueClaim::NothingDue);
    };

    let attempt = transaction
        .query_row(
            "SELECT COUNT(*) FROM schedule_runs WHERE job_id = ?1",
            params![job.job_id],
            |row| row.get::<_, i64>(0),
        )?
        .checked_add(1)
        .ok_or(ScheduleError::Overflow)?;
    let scheduled_for_ms = job.next_fire_at_ms.unwrap_or(now_ms);
    transaction.execute(
        "INSERT INTO schedule_runs(run_id, job_id, trigger, scheduled_for_ms, claimed_at, attempt, missed_count)
         VALUES (?1, ?2, 'timer', ?3, ?4, ?5, 0)",
        params![run_id, job.job_id, scheduled_for_ms, now_ms, attempt],
    )?;
    // The caller recomputes the next occurrence; clearing it here prevents a
    // second claim of the same instant before that update lands.
    transaction.execute(
        "UPDATE schedule_jobs SET next_fire_at_ms = NULL, updated_at = ?2 WHERE job_id = ?1",
        params![job.job_id, now_ms],
    )?;
    let run_sql = format!("SELECT {RUN_COLUMNS} FROM schedule_runs WHERE run_id = ?1");
    let run = transaction.query_row(&run_sql, params![run_id], run_from_row)?;
    transaction.commit()?;
    Ok(DueClaim::Claimed { job, run })
}

/// Finalizes one claimed run, optionally appending its journal event in the
/// same transaction so the terminal outcome and its audit record share a commit.
pub fn run_finalize(store: &Store, finalize: RunFinalize) -> ScheduleResult<ScheduleRun> {
    let mut connection = store.connection()?;
    let transaction = connection.transaction()?;
    let updated = transaction.execute(
        "UPDATE schedule_runs
            SET outcome = ?2, finished_at = ?3, op_ref = ?4, detail_json = ?5
          WHERE run_id = ?1 AND outcome IS NULL",
        params![
            finalize.run_id,
            finalize.outcome.as_str(),
            finalize.finished_at,
            finalize.op_ref,
            finalize.detail_json,
        ],
    )?;
    if updated == 0 {
        return Err(ScheduleError::NotFound);
    }
    let run_sql = format!("SELECT {RUN_COLUMNS} FROM schedule_runs WHERE run_id = ?1");
    let run = transaction.query_row(&run_sql, params![finalize.run_id], run_from_row)?;
    if finalize.job_state.is_some() || finalize.failure_count.is_some() || finalize.backoff_until_ms.is_some() {
        transaction.execute(
            "UPDATE schedule_jobs
                SET state = COALESCE(?2, state),
                    failure_count = COALESCE(?3, failure_count),
                    backoff_until_ms = ?4,
                    updated_at = ?5
              WHERE job_id = ?1",
            params![
                run.job_id,
                finalize.job_state.map(ScheduleJobState::as_str),
                finalize.failure_count,
                finalize.backoff_until_ms,
                finalize.finished_at,
            ],
        )?;
    }
    if let Some(payload) = finalize.journal_payload_json.as_deref() {
        crate::events::append_in_transaction(&transaction, "schedule_run", payload, finalize.finished_at)
            .map_err(|error| ScheduleError::InvalidInput(error.to_string()))?;
    }
    transaction.commit()?;
    Ok(run)
}

pub fn run_list(
    store: &Store,
    job_id: &str,
    cursor: Option<String>,
    limit: Option<u32>,
) -> ScheduleResult<(Vec<ScheduleRun>, Option<String>)> {
    let limit = clamp_limit(limit);
    let connection = store.connection()?;
    let sql = format!(
        "SELECT {RUN_COLUMNS} FROM schedule_runs
         WHERE job_id = ?1 AND (?2 IS NULL OR run_id < ?2)
         ORDER BY claimed_at DESC, run_id DESC
         LIMIT ?3"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params![job_id, cursor, i64::from(limit) + 1], run_from_row)?;
    let mut runs = rows.collect::<Result<Vec<_>, _>>()?;
    let next_cursor = if runs.len() > limit as usize {
        runs.truncate(limit as usize);
        runs.last().map(|run| run.run_id.clone())
    } else {
        None
    };
    Ok((runs, next_cursor))
}

/// Lists in-flight runs so the caller can resolve them from durable admission
/// evidence before any overdue collapse is considered.
pub fn in_flight_runs(store: &Store) -> ScheduleResult<Vec<ScheduleRun>> {
    let connection = store.connection()?;
    let sql = format!("SELECT {RUN_COLUMNS} FROM schedule_runs WHERE outcome IS NULL ORDER BY claimed_at");
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], run_from_row)?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

/// Collapses missed occurrences for one job into a single durable row.
///
/// N missed occurrences never produce N runs. A job that still holds an
/// in-flight run is refused here so the collapse cannot swallow or miscount
/// that occurrence; the caller reconciles the in-flight run first.
pub fn overdue_collapse(
    store: &Store,
    job_id: &str,
    run_id: &str,
    missed_count: i64,
    next_fire_at_ms: Option<i64>,
    now_ms: i64,
    journal_payload_json: Option<String>,
) -> ScheduleResult<Option<ScheduleRun>> {
    if missed_count < 1 {
        return Err(ScheduleError::InvalidInput(
            "missed_count must be positive for an overdue collapse".to_owned(),
        ));
    }
    let mut connection = store.connection()?;
    let transaction = connection.transaction()?;
    let job = job_get_tx(&transaction, job_id)?.ok_or(ScheduleError::NotFound)?;
    let in_flight: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM schedule_runs WHERE job_id = ?1 AND outcome IS NULL",
        params![job_id],
        |row| row.get(0),
    )?;
    if in_flight > 0 {
        transaction.commit()?;
        return Ok(None);
    }
    let attempt = transaction
        .query_row(
            "SELECT COUNT(*) FROM schedule_runs WHERE job_id = ?1",
            params![job_id],
            |row| row.get::<_, i64>(0),
        )?
        .checked_add(1)
        .ok_or(ScheduleError::Overflow)?;
    let outcome = match job.kind {
        ScheduleKind::At => ScheduleRunOutcome::Missed,
        ScheduleKind::Every | ScheduleKind::Cron => ScheduleRunOutcome::SkippedOverdue,
    };
    transaction.execute(
        "INSERT INTO schedule_runs(
             run_id, job_id, trigger, scheduled_for_ms, claimed_at, finished_at, attempt, outcome, missed_count)
         VALUES (?1, ?2, 'overdue', ?3, ?4, ?4, ?5, ?6, ?7)",
        params![
            run_id,
            job_id,
            job.next_fire_at_ms.unwrap_or(now_ms),
            now_ms,
            attempt,
            outcome.as_str(),
            missed_count,
        ],
    )?;
    let next_state = match job.kind {
        ScheduleKind::At => ScheduleJobState::Completed,
        ScheduleKind::Every | ScheduleKind::Cron => job.state,
    };
    transaction.execute(
        "UPDATE schedule_jobs SET state = ?2, next_fire_at_ms = ?3, updated_at = ?4 WHERE job_id = ?1",
        params![job_id, next_state.as_str(), next_fire_at_ms, now_ms],
    )?;
    let run_sql = format!("SELECT {RUN_COLUMNS} FROM schedule_runs WHERE run_id = ?1");
    let run = transaction.query_row(&run_sql, params![run_id], run_from_row)?;
    if let Some(payload) = journal_payload_json.as_deref() {
        crate::events::append_in_transaction(&transaction, "schedule_run", payload, now_ms)
            .map_err(|error| ScheduleError::InvalidInput(error.to_string()))?;
    }
    transaction.commit()?;
    Ok(Some(run))
}

/// Rewrites one job's next occurrence after the caller recomputes it.
pub fn set_next_fire(store: &Store, job_id: &str, next_fire_at_ms: Option<i64>, now_ms: i64) -> ScheduleResult<()> {
    let mut connection = store.connection()?;
    let transaction = connection.transaction()?;
    let updated = transaction.execute(
        "UPDATE schedule_jobs SET next_fire_at_ms = ?2, updated_at = ?3 WHERE job_id = ?1",
        params![job_id, next_fire_at_ms, now_ms],
    )?;
    if updated == 0 {
        return Err(ScheduleError::NotFound);
    }
    transaction.commit()?;
    Ok(())
}
