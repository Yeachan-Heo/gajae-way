//! Durable alert transitions evaluated on a daemon-owned cadence.
//!
//! Every transition writes its meta flag and its journal event in ONE
//! transaction, so an alert and the state it reports are a single durable fact.
//! Writes happen only on a change, so a restart with the condition still true
//! re-announces nothing.
//!
//! Evaluation is deliberately independent of the metrics HTTP listener: an
//! operator who leaves the endpoint disabled must still get alerts, so tying
//! this cadence to the listener would silently disable it.

use rusqlite::OptionalExtension;

use crate::{
    events::append_in_transaction,
    store::{Store, StoreError, StoreResult, meta_get_tx, meta_set_tx},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertTransition {
    pub condition: String,
    pub raised: bool,
}

/// Raises or clears one condition, writing the flag and its event atomically.
///
/// Returns `None` when the observed state already matches the durable flag,
/// which is what keeps a steady-state condition from re-announcing on every
/// evaluation tick.
pub fn record_transition(
    store: &Store,
    condition: &str,
    raised: bool,
    reason: &str,
    now_ms: i64,
) -> StoreResult<Option<AlertTransition>> {
    let key = format!("alert_{condition}");
    let mut connection = store.connection()?;
    let transaction = connection.transaction()?;
    let current = meta_get_tx(&transaction, &key)?.unwrap_or_else(|| "clear".to_owned());
    let already = current == "raised";
    if already == raised {
        transaction.commit()?;
        return Ok(None);
    }
    meta_set_tx(&transaction, &key, if raised { "raised" } else { "clear" })?;
    let kind = if raised { "alert_raised" } else { "alert_cleared" };
    let payload = format!(
        "{{\"condition\":\"{}\",\"reason\":\"{}\"}}",
        sanitize_token(condition),
        sanitize_token(reason)
    );
    append_in_transaction(&transaction, kind, &payload, now_ms)
        .map_err(|error| StoreError::InvalidMetadata(error.to_string()))?;
    transaction.commit()?;
    Ok(Some(AlertTransition { condition: condition.to_owned(), raised }))
}

/// Evaluates adapter liveness from the durable consumer checkpoints.
///
/// A consumer whose checkpoint has not advanced within `threshold_ms` is
/// treated as disconnected. This reads the same durable rows the gateway
/// already keeps, so it needs no adapter cooperation: an adapter that dies
/// without saying goodbye is still detected.
pub fn evaluate_adapter_disconnects(
    store: &Store,
    threshold_ms: i64,
    now_ms: i64,
) -> StoreResult<Vec<AlertTransition>> {
    let observed: Vec<(String, i64)> = {
        let connection = store.connection()?;
        let mut statement =
            connection.prepare("SELECT consumer_id, updated_at FROM consumer_checkpoints ORDER BY consumer_id")?;
        let rows = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)))?;
        rows.collect::<Result<Vec<_>, _>>()?
    };

    let mut transitions = Vec::new();
    for (consumer_id, updated_at) in observed {
        let stale = now_ms.saturating_sub(updated_at) > threshold_ms;
        let condition = format!("adapter_disconnected:{consumer_id}");
        if let Some(transition) = record_transition(store, &condition, stale, "checkpoint_stale", now_ms)? {
            transitions.push(transition);
        }
    }
    Ok(transitions)
}

/// Lists currently raised alert conditions for `way.status`.
pub fn raised_conditions(store: &Store) -> StoreResult<Vec<String>> {
    let connection = store.connection()?;
    let mut statement =
        connection.prepare("SELECT k FROM gateway_meta WHERE k LIKE 'alert_%' AND v = 'raised' ORDER BY k")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    Ok(rows
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|key| key.trim_start_matches("alert_").to_owned())
        .collect())
}

/// Reads one durable alert flag, used by tests and status projections.
pub fn is_raised(store: &Store, condition: &str) -> StoreResult<bool> {
    let connection = store.connection()?;
    let value: Option<String> = connection
        .query_row(
            "SELECT v FROM gateway_meta WHERE k = ?1",
            [format!("alert_{condition}")],
            |row| row.get(0),
        )
        .optional()?;
    Ok(value.as_deref() == Some("raised"))
}

/// Alert payloads reach chat surfaces, so only a bounded token is embedded.
fn sanitize_token(value: &str) -> String {
    let cleaned: String = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' || character == '-' || character == ':' {
                character
            } else {
                '_'
            }
        })
        .take(96)
        .collect();
    if cleaned.is_empty() { "unspecified".to_owned() } else { cleaned }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_transition_writes_its_flag_and_event_once() {
        let store = Store::default();

        let first = record_transition(&store, "journal_degraded", true, "degraded", 1_000).unwrap();
        assert!(first.is_some());
        assert!(is_raised(&store, "journal_degraded").unwrap());

        // Steady state must not re-announce on every tick.
        assert!(record_transition(&store, "journal_degraded", true, "degraded", 2_000).unwrap().is_none());

        let connection = store.connection().unwrap();
        let raised: i64 = connection
            .query_row("SELECT COUNT(*) FROM events WHERE kind = 'alert_raised'", [], |row| row.get(0))
            .unwrap();
        assert_eq!(raised, 1);
    }

    #[test]
    fn clearing_emits_once_and_only_from_raised() {
        let store = Store::default();
        assert!(record_transition(&store, "journal_degraded", false, "ok", 1_000).unwrap().is_none());
        record_transition(&store, "journal_degraded", true, "degraded", 1_000).unwrap();
        assert!(record_transition(&store, "journal_degraded", false, "recovered", 2_000).unwrap().is_some());
        assert!(!is_raised(&store, "journal_degraded").unwrap());

        let connection = store.connection().unwrap();
        let cleared: i64 = connection
            .query_row("SELECT COUNT(*) FROM events WHERE kind = 'alert_cleared'", [], |row| row.get(0))
            .unwrap();
        assert_eq!(cleared, 1);
    }

    #[test]
    fn a_stale_consumer_checkpoint_raises_and_recovery_clears() {
        let store = Store::default();
        {
            let connection = store.connection().unwrap();
            connection
                .execute(
                    "INSERT INTO consumer_checkpoints(consumer_id, cursor, updated_at) VALUES ('gajaeway-discord', '1:0', 1000)",
                    [],
                )
                .unwrap();
        }

        // Fresh enough: no alert.
        assert!(evaluate_adapter_disconnects(&store, 60_000, 30_000).unwrap().is_empty());

        // Past the threshold: raised exactly once.
        let raised = evaluate_adapter_disconnects(&store, 60_000, 100_000).unwrap();
        assert_eq!(raised.len(), 1);
        assert!(raised[0].raised);
        assert!(evaluate_adapter_disconnects(&store, 60_000, 200_000).unwrap().is_empty());
        assert!(is_raised(&store, "adapter_disconnected:gajaeway-discord").unwrap());

        // The adapter checkpoints again, so the alert clears.
        {
            let connection = store.connection().unwrap();
            connection
                .execute("UPDATE consumer_checkpoints SET updated_at = 250000 WHERE consumer_id = 'gajaeway-discord'", [])
                .unwrap();
        }
        let cleared = evaluate_adapter_disconnects(&store, 60_000, 260_000).unwrap();
        assert_eq!(cleared.len(), 1);
        assert!(!cleared[0].raised);
    }

    /// Alert evaluation must not depend on the metrics HTTP listener. An
    /// operator who leaves the endpoint disabled still needs to be told when an
    /// adapter goes silent, so the cadence is daemon-owned.
    #[test]
    fn adapter_alerts_are_evaluated_without_any_metrics_listener() {
        let store = Store::default();
        {
            let connection = store.connection().unwrap();
            connection
                .execute(
                    "INSERT INTO consumer_checkpoints(consumer_id, cursor, updated_at) VALUES ('gajaeway-telegram', '1:0', 0)",
                    [],
                )
                .unwrap();
        }
        // No listener is started anywhere in this test.
        let raised = evaluate_adapter_disconnects(&store, 1_000, 60_000).unwrap();
        assert_eq!(raised.len(), 1);
        assert!(raised[0].raised);
        assert!(is_raised(&store, "adapter_disconnected:gajaeway-telegram").unwrap());
    }

    #[test]
    fn raised_conditions_lists_only_active_alerts() {
        let store = Store::default();
        record_transition(&store, "failed_closed", true, "profile_drift", 1_000).unwrap();
        record_transition(&store, "journal_degraded", true, "degraded", 1_000).unwrap();
        record_transition(&store, "journal_degraded", false, "recovered", 2_000).unwrap();

        assert_eq!(raised_conditions(&store).unwrap(), vec!["failed_closed".to_owned()]);
    }

    #[test]
    fn a_hostile_reason_cannot_inject_payload_syntax() {
        let store = Store::default();
        record_transition(&store, "failed_closed", true, "\" } bad {\"x\":1", 1_000).unwrap();

        let connection = store.connection().unwrap();
        let payload: String = connection
            .query_row("SELECT payload_json FROM events WHERE kind = 'alert_raised'", [], |row| row.get(0))
            .unwrap();
        // The reason text may survive as characters; what must NOT survive is
        // JSON syntax, so the payload stays a single well-formed object with
        // exactly the two expected fields.
        let parsed: serde_json::Value = serde_json::from_str(&payload).expect("payload must remain valid JSON");
        let object = parsed.as_object().expect("payload must be an object");
        assert_eq!(object.len(), 2, "no injected field may appear");
        assert_eq!(object["condition"], "failed_closed");
        let reason = object["reason"].as_str().unwrap();
        assert!(!reason.contains('"'), "a quote must never survive into the payload");
        assert!(!reason.contains('{') && !reason.contains('}'), "braces must never survive");
    }
}
