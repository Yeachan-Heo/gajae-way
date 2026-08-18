//! Authority-complete broker session registry and reconciliation diff.
//!
//! The broker supplies only authority facts.  This module deliberately keeps
//! reconciler-discovered rows at `kind = unknown` / `status = discovered` until
//! an authoritative source says more, while preserving gateway-owned kind and
//! purpose values.

use std::{
	collections::HashSet,
	fmt,
};

use rusqlite::{params, OptionalExtension, Row, Transaction};
use serde_json::{json, Value};

use crate::{
	events::append_in_transaction,
	store::{Store, StoreError},
};

pub const DEFAULT_LIST_LIMIT: u32 = 100;
pub const MAX_LIST_LIMIT: u32 = 500;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerSessionRow {
	pub session_id: String,
	/// Canonical JSON for the credential-free `{ repo, stateRoot }` SDK locator.
	pub locator: String,
	pub endpoint_generation: u64,
	pub host_incarnation: Option<String>,
	pub identity_provenance: Option<String>,
	pub index_seq: u64,
	pub live: bool,
	pub deleted: bool,
	pub terminal_uncertain: bool,
	pub ambiguous: bool,
	pub activity_state: Option<String>,
	pub activity_at: Option<i64>,
	pub last_heartbeat_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerSnapshot {
	pub observed_at: i64,
	pub rows: Vec<BrokerSessionRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotApplyResult {
	pub new_session_ids: Vec<String>,
	pub changed_session_ids: Vec<String>,
	pub changed_index_seq_session_ids: Vec<String>,
	pub drift_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryRow {
	pub session_id: String,
	pub kind: String,
	pub purpose: Option<String>,
	pub brief: Option<String>,
	pub status: String,
	pub surface_id: Option<String>,
	pub locator: Option<String>,
	pub endpoint_generation: Option<u64>,
	pub host_incarnation: Option<String>,
	pub identity_provenance: Option<String>,
	pub index_seq: Option<u64>,
	pub live: bool,
	pub deleted: bool,
	pub terminal_uncertain: bool,
	pub ambiguous: bool,
	pub activity_state: Option<String>,
	pub activity_at: Option<i64>,
	pub last_heartbeat_at: Option<i64>,
	pub meta_name: Option<String>,
	pub meta_cwd: Option<String>,
	pub meta_kind: Option<String>,
	pub metadata_state: String,
	pub metadata_at: Option<i64>,
	pub source: String,
	pub created_at: i64,
	pub last_seen_at: Option<i64>,
	pub closed_at: Option<i64>,
	pub registry_rev: u64,
	pub quarantined: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryListFilter {
	pub kind: Option<String>,
	pub status: Option<String>,
	pub surface_id: Option<String>,
	pub limit: u32,
	pub offset: u64,
}

impl Default for RegistryListFilter {
	fn default() -> Self {
		Self { kind: None, status: None, surface_id: None, limit: DEFAULT_LIST_LIMIT, offset: 0 }
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryList {
	pub rows: Vec<RegistryRow>,
	pub total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryAnnotation {
	pub session_id: String,
	pub purpose: Option<String>,
	pub brief: Option<String>,
	pub observed_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataEnrichment {
	pub session_id: String,
	pub name: String,
	pub cwd: String,
	pub kind: String,
	pub observed_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurfaceRecord {
	pub surface_id: String,
	pub platform: String,
	pub kind: String,
	pub is_owner_surface: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewaySession {
	pub session_id: String,
	pub kind: String,
	pub purpose: Option<String>,
	pub brief: Option<String>,
	pub status: String,
	pub surface_id: Option<String>,
	pub observed_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurfaceResolution {
	pub surface: SurfaceRecord,
	pub session_id: Option<String>,
	pub quarantined: bool,
}

#[derive(Debug)]
pub enum RegistryError {
	Store(StoreError),
	InvalidInput(String),
	UnknownSession,
	UnknownSurface,
	SessionQuarantined,
	Overflow,
}

impl RegistryError {
	pub const fn code(&self) -> Option<u16> {
		match self {
			Self::UnknownSurface => Some(1300),
			Self::UnknownSession => Some(1301),
			Self::SessionQuarantined => Some(1302),
			_ => None,
		}
	}
}

impl fmt::Display for RegistryError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Store(error) => write!(formatter, "registry store error: {error}"),
			Self::InvalidInput(message) => write!(formatter, "invalid registry input: {message}"),
			Self::UnknownSession => formatter.write_str("unknown session"),
			Self::UnknownSurface => formatter.write_str("unknown surface"),
			Self::SessionQuarantined => formatter.write_str("session is quarantined"),
			Self::Overflow => formatter.write_str("registry numeric value overflow"),
		}
	}
}

impl std::error::Error for RegistryError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Store(error) => Some(error),
			_ => None,
		}
	}
}

impl From<StoreError> for RegistryError {
	fn from(error: StoreError) -> Self {
		Self::Store(error)
	}
}

impl From<rusqlite::Error> for RegistryError {
	fn from(error: rusqlite::Error) -> Self {
		Self::Store(error.into())
	}
}

pub type RegistryResult<T> = Result<T, RegistryError>;

const ROW_COLUMNS: &str = "
	s.session_id, s.kind, s.purpose, s.brief, s.status, s.surface_id,
	s.locator, s.endpoint_generation, s.host_incarnation, s.identity_provenance,
	s.index_seq, s.live, s.deleted, s.terminal_uncertain, s.ambiguous,
	s.activity_state, s.activity_at, s.last_heartbeat_at,
	s.meta_name, s.meta_cwd, s.meta_kind, s.metadata_state, s.metadata_at,
	s.source, s.created_at, s.last_seen_at, s.closed_at, s.registry_rev,
	(s.ambiguous = 1 OR s.terminal_uncertain = 1 OR (
		s.deleted = 1 AND (
			s.surface_id IS NOT NULL OR EXISTS(SELECT 1 FROM surfaces bound WHERE bound.session_id = s.session_id)
		)
	)) AS quarantined";

fn select_rows_sql(where_clause: &str) -> String {
	format!("SELECT {ROW_COLUMNS} FROM sessions s {where_clause}")
}


fn row_to_registry(row: &Row<'_>) -> rusqlite::Result<RegistryRow> {
	let endpoint_generation = row
		.get::<_, Option<String>>(7)?
		.map(|value| value.parse::<u64>().map_err(|_| rusqlite::Error::InvalidQuery))
		.transpose()?;
	let index_seq = row
		.get::<_, Option<i64>>(10)?
		.map(|value| u64::try_from(value).map_err(|_| rusqlite::Error::InvalidQuery))
		.transpose()?;
	let registry_rev = u64::try_from(row.get::<_, i64>(27)?).map_err(|_| rusqlite::Error::InvalidQuery)?;
	Ok(RegistryRow {
		session_id: row.get(0)?,
		kind: row.get(1)?,
		purpose: row.get(2)?,
		brief: row.get(3)?,
		status: row.get(4)?,
		surface_id: row.get(5)?,
		locator: row.get(6)?,
		endpoint_generation,
		host_incarnation: row.get(8)?,
		identity_provenance: row.get(9)?,
		index_seq,
		live: row.get::<_, Option<i64>>(11)?.unwrap_or(0) != 0,
		deleted: row.get::<_, i64>(12)? != 0,
		terminal_uncertain: row.get::<_, i64>(13)? != 0,
		ambiguous: row.get::<_, i64>(14)? != 0,
		activity_state: row.get(15)?,
		activity_at: row.get(16)?,
		last_heartbeat_at: row.get(17)?,
		meta_name: row.get(18)?,
		meta_cwd: row.get(19)?,
		meta_kind: row.get(20)?,
		metadata_state: row.get(21)?,
		metadata_at: row.get(22)?,
		source: row.get(23)?,
		created_at: row.get(24)?,
		last_seen_at: row.get(25)?,
		closed_at: row.get(26)?,
		registry_rev,
		quarantined: row.get::<_, i64>(28)? != 0,
	})
}

fn get_tx(transaction: &Transaction<'_>, session_id: &str) -> RegistryResult<Option<RegistryRow>> {
	transaction
		.query_row(&select_rows_sql("WHERE s.session_id = ?1"), [session_id], row_to_registry)
		.optional()
		.map_err(RegistryError::from)
}

fn validate_identifier(value: &str, field: &str) -> RegistryResult<()> {
	if value.trim().is_empty() || value.len() > 4_096 {
		return Err(RegistryError::InvalidInput(format!("{field} must be a non-empty string up to 4096 bytes")));
	}
	Ok(())
}

fn validate_kind(value: &str) -> RegistryResult<()> {
	if matches!(value, "main" | "conversation" | "lane" | "job" | "unknown") {
		Ok(())
	} else {
		Err(RegistryError::InvalidInput("kind must be main, conversation, lane, job, or unknown".to_owned()))
	}
}

fn validate_status(value: &str) -> RegistryResult<()> {
	if matches!(value, "discovered" | "starting" | "active" | "idle" | "closing" | "closed" | "lost") {
		Ok(())
	} else {
		Err(RegistryError::InvalidInput(
			"status must be discovered, starting, active, idle, closing, closed, or lost".to_owned(),
		))
	}
}


fn validate_broker_row(row: &BrokerSessionRow) -> RegistryResult<()> {
	validate_identifier(&row.session_id, "session_id")?;
	let locator = serde_json::from_str::<Value>(&row.locator)
		.map_err(|_| RegistryError::InvalidInput("locator must be JSON".to_owned()))?;
	let locator = locator
		.as_object()
		.ok_or_else(|| RegistryError::InvalidInput("locator must be an object".to_owned()))?;
	for field in ["repo", "stateRoot"] {
		if !locator.get(field).is_some_and(|value| value.as_str().is_some_and(|value| !value.is_empty())) {
			return Err(RegistryError::InvalidInput(format!("locator.{field} must be a non-empty string")));
		}
	}
	if let Some(identity_provenance) = &row.identity_provenance
		&& !matches!(identity_provenance.as_str(), "composite" | "legacy")
	{
		return Err(RegistryError::InvalidInput("identity_provenance must be composite or legacy".to_owned()));
	}
	if let Some(activity_state) = &row.activity_state
		&& !matches!(activity_state.as_str(), "active" | "idle")
	{
		return Err(RegistryError::InvalidInput("activity_state must be active or idle".to_owned()));
	}
	if row.activity_at.is_some() != row.activity_state.is_some() {
		return Err(RegistryError::InvalidInput("activity_state and activity_at must be present together".to_owned()));
	}
	Ok(())
}

fn validate_snapshot(snapshot: &BrokerSnapshot) -> RegistryResult<()> {
	if snapshot.observed_at < 0 {
		return Err(RegistryError::InvalidInput("observed_at must not be negative".to_owned()));
	}
	let mut session_ids = HashSet::new();
	for row in &snapshot.rows {
		validate_broker_row(row)?;
		if !session_ids.insert(&row.session_id) {
			return Err(RegistryError::InvalidInput(format!("snapshot contains duplicate session_id {}", row.session_id)));
		}
	}
	Ok(())
}

fn status_from_broker(previous: &str, row: &BrokerSessionRow) -> String {
	if row.deleted {
		return "closed".to_owned();
	}
	if let Some(activity_state) = &row.activity_state {
		return activity_state.clone();
	}
	if previous == "lost" {
		return "discovered".to_owned();
	}
	previous.to_owned()
}

fn row_authority_changed(previous: &RegistryRow, next_status: &str, row: &BrokerSessionRow) -> bool {
	previous.status != next_status
		|| previous.locator.as_deref() != Some(row.locator.as_str())
		|| previous.endpoint_generation != Some(row.endpoint_generation)
		|| previous.host_incarnation != row.host_incarnation
		|| previous.identity_provenance != row.identity_provenance
		|| previous.index_seq != Some(row.index_seq)
		|| previous.live != row.live
		|| previous.deleted != row.deleted
		|| previous.terminal_uncertain != row.terminal_uncertain
		|| previous.ambiguous != row.ambiguous
		|| previous.activity_state != row.activity_state
		|| previous.activity_at != row.activity_at
		|| previous.last_heartbeat_at != row.last_heartbeat_at
}

fn transition_payload(
	session_id: &str,
	reason: &str,
	previous: Option<&RegistryRow>,
	status: &str,
	quarantined: bool,
	registry_rev: u64,
) -> String {
	json!({
		"session_id": session_id,
		"reason": reason,
		"previous": previous.map(|row| json!({
			"status": row.status,
			"quarantined": row.quarantined,
			"registry_rev": row.registry_rev,
		})),
		"current": {
			"status": status,
			"quarantined": quarantined,
			"registry_rev": registry_rev,
		},
	})
	.to_string()
}

fn append_change(
	transaction: &Transaction<'_>,
	session_id: &str,
	reason: &str,
	previous: Option<&RegistryRow>,
	status: &str,
	quarantined: bool,
	registry_rev: u64,
	observed_at: i64,
) -> RegistryResult<()> {
	let payload = transition_payload(session_id, reason, previous, status, quarantined, registry_rev);
	append_in_transaction(transaction, "registry_change", &payload, observed_at)
		.map_err(|error| RegistryError::InvalidInput(format!("could not append registry journal event: {error}")))?;
	Ok(())
}

fn surface_bound_tx(transaction: &Transaction<'_>, session_id: &str, surface_id: Option<&str>) -> RegistryResult<bool> {
	if surface_id.is_some() {
		return Ok(true);
	}
	let bound = transaction.query_row(
		"SELECT EXISTS(SELECT 1 FROM surfaces WHERE session_id = ?1)",
		[session_id],
		|row| row.get::<_, i64>(0),
	)?;
	Ok(bound != 0)
}

fn quarantine_for(row: &BrokerSessionRow, surface_bound: bool) -> bool {
	row.ambiguous || row.terminal_uncertain || (row.deleted && surface_bound)
}

/// Applies one fully parsed `sdk session list` result and every resulting journal
/// transition in a single SQLite transaction.  Callers must not invoke this
/// after a broker command or DTO parse failure.
pub fn apply_broker_snapshot(store: &Store, snapshot: BrokerSnapshot) -> RegistryResult<SnapshotApplyResult> {
	validate_snapshot(&snapshot)?;
	let mut connection = store.connection()?;
	let transaction = connection.transaction()?;
	let mut result = SnapshotApplyResult {
		new_session_ids: Vec::new(),
		changed_session_ids: Vec::new(),
		changed_index_seq_session_ids: Vec::new(),
		drift_count: 0,
	};
	let mut seen = HashSet::with_capacity(snapshot.rows.len());

	for broker_row in &snapshot.rows {
		seen.insert(broker_row.session_id.clone());
		let existing = get_tx(&transaction, &broker_row.session_id)?;
		match existing {
			None => {
				let status = if broker_row.deleted { "closed" } else { "discovered" };
				let quarantined = quarantine_for(broker_row, false);
				transaction.execute(
					"INSERT INTO sessions (
						session_id, kind, purpose, brief, status, surface_id,
						locator, endpoint_generation, host_incarnation, identity_provenance,
						index_seq, live, deleted, terminal_uncertain, ambiguous,
						activity_state, activity_at, last_heartbeat_at,
						metadata_state, source, created_at, last_seen_at, closed_at, registry_rev
					) VALUES (
						?1, 'unknown', NULL, NULL, ?2, NULL,
						?3, ?4, ?5, ?6,
						?7, ?8, ?9, ?10, ?11,
						?12, ?13, ?14,
						'pending', 'reconciler', ?15, ?15, CASE WHEN ?2 = 'closed' THEN ?15 ELSE NULL END, 1
					)",
					params![
						broker_row.session_id,
						status,
						broker_row.locator,
						broker_row.endpoint_generation.to_string(),
						broker_row.host_incarnation,
						broker_row.identity_provenance,
						i64::try_from(broker_row.index_seq).map_err(|_| RegistryError::Overflow)?,
						i64::from(broker_row.live),
						i64::from(broker_row.deleted),
						i64::from(broker_row.terminal_uncertain),
						i64::from(broker_row.ambiguous),
						broker_row.activity_state,
						broker_row.activity_at,
						broker_row.last_heartbeat_at,
						snapshot.observed_at,
					],
				)?;
				append_change(
					&transaction,
					&broker_row.session_id,
					"discovered",
					None,
					status,
					quarantined,
					1,
					snapshot.observed_at,
				)?;
				result.new_session_ids.push(broker_row.session_id.clone());
				result.changed_session_ids.push(broker_row.session_id.clone());
				result.changed_index_seq_session_ids.push(broker_row.session_id.clone());
				result.drift_count += 1;
			}
			Some(previous) => {
				let status = status_from_broker(&previous.status, broker_row);
				let changed = row_authority_changed(&previous, &status, broker_row);
				let closed_at = if matches!(status.as_str(), "closed" | "lost") {
					previous.closed_at.or(Some(snapshot.observed_at))
				} else {
					None
				};
				if changed {
					let next_revision = previous.registry_rev.checked_add(1).ok_or(RegistryError::Overflow)?;
					transaction.execute(
						"UPDATE sessions SET
							status = ?2, locator = ?3, endpoint_generation = ?4, host_incarnation = ?5,
							identity_provenance = ?6, index_seq = ?7, live = ?8, deleted = ?9,
							terminal_uncertain = ?10, ambiguous = ?11, activity_state = ?12,
							activity_at = ?13, last_heartbeat_at = ?14, last_seen_at = ?15,
							closed_at = ?16, registry_rev = ?17
						WHERE session_id = ?1",
						params![
							broker_row.session_id,
							status,
							broker_row.locator,
							broker_row.endpoint_generation.to_string(),
							broker_row.host_incarnation,
							broker_row.identity_provenance,
							i64::try_from(broker_row.index_seq).map_err(|_| RegistryError::Overflow)?,
							i64::from(broker_row.live),
							i64::from(broker_row.deleted),
							i64::from(broker_row.terminal_uncertain),
							i64::from(broker_row.ambiguous),
							broker_row.activity_state,
							broker_row.activity_at,
							broker_row.last_heartbeat_at,
							snapshot.observed_at,
							closed_at,
							i64::try_from(next_revision).map_err(|_| RegistryError::Overflow)?,
						],
					)?;
					let surface_bound = surface_bound_tx(&transaction, &broker_row.session_id, previous.surface_id.as_deref())?;
					append_change(
						&transaction,
						&broker_row.session_id,
						"broker_snapshot",
						Some(&previous),
						&status,
						quarantine_for(broker_row, surface_bound),
						next_revision,
						snapshot.observed_at,
					)?;
					if previous.index_seq != Some(broker_row.index_seq) {
						result.changed_index_seq_session_ids.push(broker_row.session_id.clone());
					}
					result.changed_session_ids.push(broker_row.session_id.clone());
					result.drift_count += 1;
				} else {
					transaction.execute(
						"UPDATE sessions SET last_seen_at = ?2 WHERE session_id = ?1",
						params![broker_row.session_id, snapshot.observed_at],
					)?;
				}
			}
		}
	}

	let absent = transaction
		.prepare(&select_rows_sql("WHERE s.status <> 'closed'"))?
		.query_map([], row_to_registry)?
		.collect::<Result<Vec<_>, _>>()?;
	for previous in absent {
		if seen.contains(&previous.session_id) || previous.status == "lost" {
			continue;
		}
		let next_revision = previous.registry_rev.checked_add(1).ok_or(RegistryError::Overflow)?;
		transaction.execute(
			"UPDATE sessions SET status = 'lost', closed_at = COALESCE(closed_at, ?2), registry_rev = ?3 WHERE session_id = ?1",
			params![
				previous.session_id,
				snapshot.observed_at,
				i64::try_from(next_revision).map_err(|_| RegistryError::Overflow)?,
			],
		)?;
		append_change(
			&transaction,
			&previous.session_id,
			"absent_from_broker",
			Some(&previous),
			"lost",
			previous.quarantined,
			next_revision,
			snapshot.observed_at,
		)?;
		result.changed_session_ids.push(previous.session_id);
		result.drift_count += 1;
	}

	transaction.commit()?;
	Ok(result)
}

pub fn list(store: &Store, filter: RegistryListFilter) -> RegistryResult<RegistryList> {
	if filter.limit == 0 || filter.limit > MAX_LIST_LIMIT {
		return Err(RegistryError::InvalidInput(format!("limit must be in 1..={MAX_LIST_LIMIT}")));
	}
	if let Some(kind) = &filter.kind {
		validate_kind(kind)?;
	}
	if let Some(status) = &filter.status {
		validate_status(status)?;
	}
	if let Some(surface_id) = &filter.surface_id {
		validate_identifier(surface_id, "surface_id")?;
	}
	let connection = store.connection()?;
	let rows = connection
		.prepare(&select_rows_sql("ORDER BY s.created_at ASC, s.session_id ASC"))?
		.query_map([], row_to_registry)?
		.collect::<Result<Vec<_>, _>>()?;
	let filtered = rows
		.into_iter()
		.filter(|row| filter.kind.as_ref().is_none_or(|kind| &row.kind == kind))
		.filter(|row| filter.status.as_ref().is_none_or(|status| &row.status == status))
		.filter(|row| filter.surface_id.as_ref().is_none_or(|surface_id| row.surface_id.as_ref() == Some(surface_id)))
		.collect::<Vec<_>>();
	let total = u64::try_from(filtered.len()).map_err(|_| RegistryError::Overflow)?;
	let offset = usize::try_from(filter.offset).unwrap_or(usize::MAX);
	let limit = usize::try_from(filter.limit).expect("u32 always fits usize on supported targets");
	let rows = filtered.into_iter().skip(offset).take(limit).collect();
	Ok(RegistryList { rows, total })
}

pub fn get(store: &Store, session_id: &str) -> RegistryResult<RegistryRow> {
	validate_identifier(session_id, "session_id")?;
	let connection = store.connection()?;
	connection
		.query_row(&select_rows_sql("WHERE s.session_id = ?1"), [session_id], row_to_registry)
		.optional()?
		.ok_or(RegistryError::UnknownSession)
}

pub fn annotate(store: &Store, annotation: RegistryAnnotation) -> RegistryResult<RegistryRow> {
	validate_identifier(&annotation.session_id, "session_id")?;
	if annotation.observed_at < 0 {
		return Err(RegistryError::InvalidInput("observed_at must not be negative".to_owned()));
	}
	for (field, value) in [("purpose", annotation.purpose.as_deref()), ("brief", annotation.brief.as_deref())] {
		if let Some(value) = value {
			validate_identifier(value, field)?;
		}
	}
	if annotation.purpose.is_none() && annotation.brief.is_none() {
		return Err(RegistryError::InvalidInput("at least one of purpose or brief is required".to_owned()));
	}
	let mut connection = store.connection()?;
	let transaction = connection.transaction()?;
	let previous = get_tx(&transaction, &annotation.session_id)?.ok_or(RegistryError::UnknownSession)?;
	let purpose = annotation.purpose.or_else(|| previous.purpose.clone());
	let brief = annotation.brief.or_else(|| previous.brief.clone());
	if purpose != previous.purpose || brief != previous.brief {
		let next_revision = previous.registry_rev.checked_add(1).ok_or(RegistryError::Overflow)?;
		transaction.execute(
			"UPDATE sessions SET purpose = ?2, brief = ?3, registry_rev = ?4 WHERE session_id = ?1",
			params![
				annotation.session_id,
				purpose,
				brief,
				i64::try_from(next_revision).map_err(|_| RegistryError::Overflow)?,
			],
		)?;
		append_change(
			&transaction,
			&annotation.session_id,
			"annotation",
			Some(&previous),
			&previous.status,
			previous.quarantined,
			next_revision,
			annotation.observed_at,
		)?;
	}
	let output = get_tx(&transaction, &annotation.session_id)?.ok_or(RegistryError::UnknownSession)?;
	transaction.commit()?;
	Ok(output)
}

pub fn apply_metadata(store: &Store, enrichment: MetadataEnrichment) -> RegistryResult<RegistryRow> {
	validate_identifier(&enrichment.session_id, "session_id")?;
	for (field, value) in [("name", enrichment.name.as_str()), ("cwd", enrichment.cwd.as_str()), ("kind", enrichment.kind.as_str())] {
		validate_identifier(value, field)?;
	}
	if enrichment.observed_at < 0 {
		return Err(RegistryError::InvalidInput("observed_at must not be negative".to_owned()));
	}
	let mut connection = store.connection()?;
	let transaction = connection.transaction()?;
	let previous = get_tx(&transaction, &enrichment.session_id)?.ok_or(RegistryError::UnknownSession)?;
	let changed = previous.meta_name.as_deref() != Some(enrichment.name.as_str())
		|| previous.meta_cwd.as_deref() != Some(enrichment.cwd.as_str())
		|| previous.meta_kind.as_deref() != Some(enrichment.kind.as_str())
		|| previous.metadata_state != "enriched";
	if changed {
		let next_revision = previous.registry_rev.checked_add(1).ok_or(RegistryError::Overflow)?;
		transaction.execute(
			"UPDATE sessions SET meta_name = ?2, meta_cwd = ?3, meta_kind = ?4,
				metadata_state = 'enriched', metadata_at = ?5, registry_rev = ?6 WHERE session_id = ?1",
			params![
				enrichment.session_id,
				enrichment.name,
				enrichment.cwd,
				enrichment.kind,
				enrichment.observed_at,
				i64::try_from(next_revision).map_err(|_| RegistryError::Overflow)?,
			],
		)?;
		append_change(
			&transaction,
			&enrichment.session_id,
			"metadata_enriched",
			Some(&previous),
			&previous.status,
			previous.quarantined,
			next_revision,
			enrichment.observed_at,
		)?;
	} else {
		transaction.execute(
			"UPDATE sessions SET metadata_at = ?2 WHERE session_id = ?1",
			params![enrichment.session_id, enrichment.observed_at],
		)?;
	}
	let output = get_tx(&transaction, &enrichment.session_id)?.ok_or(RegistryError::UnknownSession)?;
	transaction.commit()?;
	Ok(output)
}

pub fn mark_metadata_unavailable(store: &Store, session_id: &str, observed_at: i64) -> RegistryResult<RegistryRow> {
	validate_identifier(session_id, "session_id")?;
	if observed_at < 0 {
		return Err(RegistryError::InvalidInput("observed_at must not be negative".to_owned()));
	}
	let mut connection = store.connection()?;
	let transaction = connection.transaction()?;
	let previous = get_tx(&transaction, session_id)?.ok_or(RegistryError::UnknownSession)?;
	if previous.metadata_state != "unavailable" {
		let next_revision = previous.registry_rev.checked_add(1).ok_or(RegistryError::Overflow)?;
		transaction.execute(
			"UPDATE sessions SET metadata_state = 'unavailable', metadata_at = ?2, registry_rev = ?3 WHERE session_id = ?1",
			params![session_id, observed_at, i64::try_from(next_revision).map_err(|_| RegistryError::Overflow)?],
		)?;
		append_change(
			&transaction,
			session_id,
			"metadata_unavailable",
			Some(&previous),
			&previous.status,
			previous.quarantined,
			next_revision,
			observed_at,
		)?;
	} else {
		transaction.execute("UPDATE sessions SET metadata_at = ?2 WHERE session_id = ?1", params![session_id, observed_at])?;
	}
	let output = get_tx(&transaction, session_id)?.ok_or(RegistryError::UnknownSession)?;
	transaction.commit()?;
	Ok(output)
}

/// Registers static v1 profile surfaces.  Configuration updates never replace
/// an already-bound session id, which belongs to the later P11 binding flow.
pub fn configure_surfaces(store: &Store, surfaces: &[SurfaceRecord], observed_at: i64) -> RegistryResult<()> {
	if observed_at < 0 {
		return Err(RegistryError::InvalidInput("observed_at must not be negative".to_owned()));
	}
	let mut identifiers = HashSet::new();
	for surface in surfaces {
		for (field, value) in [
			("surface_id", surface.surface_id.as_str()),
			("platform", surface.platform.as_str()),
			("kind", surface.kind.as_str()),
		] {
			validate_identifier(value, field)?;
		}
		if !identifiers.insert(&surface.surface_id) {
			return Err(RegistryError::InvalidInput(format!("duplicate surface_id {}", surface.surface_id)));
		}
	}
	let mut connection = store.connection()?;
	let transaction = connection.transaction()?;
	for surface in surfaces {
		transaction.execute(
			"INSERT INTO surfaces (surface_id, platform, kind, session_id, is_owner_surface, created_at, updated_at)
			 VALUES (?1, ?2, ?3, NULL, ?4, ?5, ?5)
			 ON CONFLICT(surface_id) DO UPDATE SET
				platform = excluded.platform,
				kind = excluded.kind,
				is_owner_surface = excluded.is_owner_surface,
				updated_at = excluded.updated_at",
			params![
				surface.surface_id,
				surface.platform,
				surface.kind,
				i64::from(surface.is_owner_surface),
				observed_at,
			],
		)?;
	}
	transaction.commit()?;
	Ok(())
}

/// P11 consumes this lower-level primitive.  Keeping it here lets the
/// quarantine rule reject a bind before any relationship is committed.
pub fn bind_surface(store: &Store, surface_id: &str, session_id: &str, observed_at: i64) -> RegistryResult<()> {
	validate_identifier(surface_id, "surface_id")?;
	validate_identifier(session_id, "session_id")?;
	let mut connection = store.connection()?;
	let transaction = connection.transaction()?;
	let previous = get_tx(&transaction, session_id)?.ok_or(RegistryError::UnknownSession)?;
	let surface_exists = transaction.query_row("SELECT EXISTS(SELECT 1 FROM surfaces WHERE surface_id = ?1)", [surface_id], |row| {
		row.get::<_, i64>(0)
	})? != 0;
	if !surface_exists {
		return Err(RegistryError::UnknownSurface);
	}
	if previous.quarantined || previous.ambiguous || previous.terminal_uncertain || previous.deleted {
		return Err(RegistryError::SessionQuarantined);
	}
	let displaced = transaction
		.prepare(&select_rows_sql("WHERE s.surface_id = ?1 AND s.session_id <> ?2"))?
		.query_map(params![surface_id, session_id], row_to_registry)?
		.collect::<Result<Vec<_>, _>>()?;
	for row in displaced {
		let next_revision = row.registry_rev.checked_add(1).ok_or(RegistryError::Overflow)?;
		transaction.execute(
			"UPDATE sessions SET surface_id = NULL, registry_rev = ?2 WHERE session_id = ?1",
			params![row.session_id, i64::try_from(next_revision).map_err(|_| RegistryError::Overflow)?],
		)?;
		append_change(
			&transaction,
			&row.session_id,
			"surface_unbound",
			Some(&row),
			&row.status,
			row.quarantined,
			next_revision,
			observed_at,
		)?;
	}
	let changed = previous.surface_id.as_deref() != Some(surface_id);
	if let Some(previous_surface_id) = previous.surface_id.as_deref()
		&& previous_surface_id != surface_id
	{
		transaction.execute(
			"UPDATE surfaces SET session_id = NULL, updated_at = ?2 WHERE surface_id = ?1 AND session_id = ?3",
			params![previous_surface_id, observed_at, session_id],
		)?;
	}
	transaction.execute(
		"UPDATE surfaces SET session_id = ?2, updated_at = ?3 WHERE surface_id = ?1",
		params![surface_id, session_id, observed_at],
	)?;
	if changed {
		let next_revision = previous.registry_rev.checked_add(1).ok_or(RegistryError::Overflow)?;
		transaction.execute(
			"UPDATE sessions SET surface_id = ?2, registry_rev = ?3 WHERE session_id = ?1",
			params![session_id, surface_id, i64::try_from(next_revision).map_err(|_| RegistryError::Overflow)?],
		)?;
		append_change(
			&transaction,
			session_id,
			"surface_bound",
			Some(&previous),
			&previous.status,
			previous.quarantined,
			next_revision,
			observed_at,
		)?;
	}
	transaction.commit()?;
	Ok(())
}

pub fn register_gateway_session(store: &Store, session: GatewaySession) -> RegistryResult<RegistryRow> {
	validate_identifier(&session.session_id, "session_id")?;
	validate_kind(&session.kind)?;
	validate_status(&session.status)?;
	if session.kind == "unknown" {
		return Err(RegistryError::InvalidInput("gateway session kind must be authoritative".to_owned()));
	}
	if let Some(purpose) = &session.purpose {
		validate_identifier(purpose, "purpose")?;
	}
	if let Some(brief) = &session.brief {
		validate_identifier(brief, "brief")?;
	}
	if let Some(surface_id) = &session.surface_id {
		validate_identifier(surface_id, "surface_id")?;
	}
	let mut connection = store.connection()?;
	let transaction = connection.transaction()?;
	let previous = get_tx(&transaction, &session.session_id)?;
	if let Some(surface_id) = &session.surface_id {
		let exists = transaction.query_row("SELECT EXISTS(SELECT 1 FROM surfaces WHERE surface_id = ?1)", [surface_id], |row| {
			row.get::<_, i64>(0)
		})? != 0;
		if !exists {
			return Err(RegistryError::UnknownSurface);
		}
	}
	match &previous {
		None => {
			transaction.execute(
				"INSERT INTO sessions (
					session_id, kind, purpose, brief, status, surface_id, metadata_state,
					source, created_at, last_seen_at, closed_at, registry_rev
				) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', 'gateway', ?7, ?7,
					CASE WHEN ?5 IN ('closed', 'lost') THEN ?7 ELSE NULL END, 1)",
				params![
					session.session_id,
					session.kind,
					session.purpose,
					session.brief,
					session.status,
					session.surface_id,
					session.observed_at,
				],
			)?;
			append_change(
				&transaction,
				&session.session_id,
				"gateway_registered",
				None,
				&session.status,
				false,
				1,
				session.observed_at,
			)?;
		}
		Some(previous) => {
			let next_revision = previous.registry_rev.checked_add(1).ok_or(RegistryError::Overflow)?;
			transaction.execute(
				"UPDATE sessions SET kind = ?2, purpose = ?3, brief = ?4, status = ?5,
					surface_id = ?6, source = 'gateway', last_seen_at = ?7,
					closed_at = CASE WHEN ?5 IN ('closed', 'lost') THEN COALESCE(closed_at, ?7) ELSE NULL END,
					registry_rev = ?8 WHERE session_id = ?1",
				params![
					session.session_id,
					session.kind,
					session.purpose,
					session.brief,
					session.status,
					session.surface_id,
					session.observed_at,
					i64::try_from(next_revision).map_err(|_| RegistryError::Overflow)?,
				],
			)?;
			append_change(
				&transaction,
				&session.session_id,
				"gateway_registered",
				Some(previous),
				&session.status,
				previous.quarantined,
				next_revision,
				session.observed_at,
			)?;
		}
	}
	if let Some(surface_id) = &session.surface_id {
		transaction.execute(
			"UPDATE surfaces SET session_id = ?2, updated_at = ?3 WHERE surface_id = ?1",
			params![surface_id, session.session_id, session.observed_at],
		)?;
	}
	let output = get_tx(&transaction, &session.session_id)?.ok_or(RegistryError::UnknownSession)?;
	transaction.commit()?;
	Ok(output)
}

pub fn resolve_surface(store: &Store, surface_id: &str) -> RegistryResult<SurfaceResolution> {
	validate_identifier(surface_id, "surface_id")?;
	let connection = store.connection()?;
	let (platform, kind, is_owner_surface, session_id): (String, String, i64, Option<String>) = connection
		.query_row(
			"SELECT platform, kind, is_owner_surface, session_id FROM surfaces WHERE surface_id = ?1",
			[surface_id],
			|row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
		)
		.optional()?
		.ok_or(RegistryError::UnknownSurface)?;
	let quarantined = match &session_id {
		Some(session_id) => connection
			.query_row(&select_rows_sql("WHERE s.session_id = ?1"), [session_id], row_to_registry)
			.optional()?
			.is_some_and(|row| row.quarantined),
		None => false,
	};
	Ok(SurfaceResolution {
		surface: SurfaceRecord { surface_id: surface_id.to_owned(), platform, kind, is_owner_surface: is_owner_surface != 0 },
		session_id,
		quarantined,
	})
}

#[cfg(test)]
mod tests {
	use super::{
		apply_broker_snapshot, bind_surface, configure_surfaces, get, list, register_gateway_session, BrokerSessionRow, BrokerSnapshot,
		GatewaySession, RegistryError, RegistryListFilter, SurfaceRecord,
	};
	use crate::{events::EventJournal, store::Store};

	fn broker_row(session_id: &str) -> BrokerSessionRow {
		BrokerSessionRow {
			session_id: session_id.to_owned(),
			locator: r#"{"repo":"/repo","stateRoot":"/repo/.gjc/state"}"#.to_owned(),
			endpoint_generation: 1,
			host_incarnation: Some("host:1".to_owned()),
			identity_provenance: Some("composite".to_owned()),
			index_seq: 1,
			live: true,
			deleted: false,
			terminal_uncertain: false,
			ambiguous: false,
			activity_state: Some("active".to_owned()),
			activity_at: Some(10),
			last_heartbeat_at: Some(10),
		}
	}

	#[test]
	fn snapshot_discovers_unknown_rows_and_diffs_authority_in_one_journalled_transaction() {
		let store = Store::default();
		let first = apply_broker_snapshot(&store, BrokerSnapshot { observed_at: 10, rows: vec![broker_row("broker-1")] }).unwrap();
		assert_eq!(first.new_session_ids, ["broker-1"]);
		let discovered = get(&store, "broker-1").unwrap();
		assert_eq!(discovered.kind, "unknown");
		assert_eq!(discovered.status, "discovered");
		assert_eq!(discovered.source, "reconciler");
		assert_eq!(EventJournal::new(store.clone()).read(None, 10).unwrap().events.len(), 1);

		let mut changed = broker_row("broker-1");
		changed.live = false;
		changed.index_seq = 2;
		changed.activity_state = Some("idle".to_owned());
		changed.activity_at = Some(20);
		let diff = apply_broker_snapshot(&store, BrokerSnapshot { observed_at: 20, rows: vec![changed] }).unwrap();
		assert_eq!(diff.changed_index_seq_session_ids, ["broker-1"]);
		let row = get(&store, "broker-1").unwrap();
		assert!(!row.live);
		assert_eq!(row.activity_state.as_deref(), Some("idle"));
		assert_eq!(row.index_seq, Some(2));
		assert_eq!(row.status, "idle");
		assert_eq!(EventJournal::new(store).read(None, 10).unwrap().events.len(), 2);
	}

	#[test]
	fn broker_snapshots_never_replace_gateway_kind_or_purpose() {
		let store = Store::default();
		register_gateway_session(
			&store,
			GatewaySession {
				session_id: "gateway-1".to_owned(),
				kind: "conversation".to_owned(),
				purpose: Some("operator thread".to_owned()),
				brief: None,
				status: "starting".to_owned(),
				surface_id: None,
				observed_at: 1,
			},
		)
		.unwrap();
		apply_broker_snapshot(&store, BrokerSnapshot { observed_at: 2, rows: vec![broker_row("gateway-1")] }).unwrap();
		let row = get(&store, "gateway-1").unwrap();
		assert_eq!(row.source, "gateway");
		assert_eq!(row.kind, "conversation");
		assert_eq!(row.purpose.as_deref(), Some("operator thread"));
	}

	#[test]
	fn routing_quarantine_requires_a_bound_deleted_session_and_refuses_bind() {
		let store = Store::default();
		configure_surfaces(
			&store,
			&[SurfaceRecord {
				surface_id: "surface-1".to_owned(),
				platform: "test".to_owned(),
				kind: "dm".to_owned(),
				is_owner_surface: true,
			}],
			1,
		)
		.unwrap();
		let row = broker_row("session-1");
		apply_broker_snapshot(&store, BrokerSnapshot { observed_at: 2, rows: vec![row.clone()] }).unwrap();
		bind_surface(&store, "surface-1", "session-1", 3).unwrap();
		let mut deleted = row;
		deleted.deleted = true;
		deleted.index_seq = 2;
		apply_broker_snapshot(&store, BrokerSnapshot { observed_at: 4, rows: vec![deleted] }).unwrap();
		assert!(get(&store, "session-1").unwrap().quarantined);
		assert!(matches!(bind_surface(&store, "surface-1", "session-1", 5), Err(RegistryError::SessionQuarantined)));
		let mut unbound_deleted = broker_row("unbound-deleted");
		unbound_deleted.deleted = true;
		apply_broker_snapshot(&store, BrokerSnapshot { observed_at: 6, rows: vec![unbound_deleted] }).unwrap();
		assert!(matches!(
			bind_surface(&store, "surface-1", "unbound-deleted", 7),
			Err(RegistryError::SessionQuarantined)
		));
		assert_eq!(list(&store, RegistryListFilter::default()).unwrap().total, 2);
	}
}
