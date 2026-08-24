//! Optional loopback Prometheus exposition.
//!
//! This is a second, weaker authentication surface than the product's UDS
//! peer-credential model: while enabled, any local process of any user can
//! scrape it. That residual is accepted deliberately, and the compensating
//! controls are structural rather than aspirational:
//!
//! - the bind address is asserted loopback and a non-loopback address is
//!   REFUSED, so the dangerous dimension cannot be introduced by configuration;
//! - the listener is disabled by default, so exposure is always an affirmative
//!   operator act; and
//! - only `GET /metrics` is served, with no request echo, so the endpoint
//!   cannot be turned into a reflector.
//!
//! Holder identity, session ids, lease ids, and free-text reasons are excluded
//! upstream in the `way.metrics` projection, which is asserted by test.

use std::{fmt, net::SocketAddr};

use rusqlite::OptionalExtension;
use serde_json::Value;

#[derive(Debug)]
pub enum MetricsError {
    NonLoopbackBind(SocketAddr),
    Bind(std::io::Error),
}

impl fmt::Display for MetricsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonLoopbackBind(addr) => write!(
                formatter,
                "refusing to bind the metrics listener to non-loopback address {addr}"
            ),
            Self::Bind(error) => write!(formatter, "could not bind the metrics listener: {error}"),
        }
    }
}

impl std::error::Error for MetricsError {}

/// Binds the metrics listener, refusing any non-loopback address.
///
/// Production passes a compile-time loopback constant and there is no
/// `bind_addr` profile key, so this cannot currently be reached with a routable
/// address. It is enforced anyway: a future change that introduces a
/// configurable address must fail closed rather than silently publish operator
/// telemetry to the network.
pub fn bind_metrics_listener(addr: SocketAddr) -> Result<std::net::TcpListener, MetricsError> {
    if !addr.ip().is_loopback() {
        return Err(MetricsError::NonLoopbackBind(addr));
    }
    // An occupied port is refused rather than rebound: silently moving would
    // leave the operator scraping nothing.
    std::net::TcpListener::bind(addr).map_err(MetricsError::Bind)
}

/// Durable metrics projection for the HTTP endpoint.
///
/// Built from durable state plus the lock manager only, so it needs no access
/// to the RPC dispatcher's in-memory session status. That is a deliberate
/// split, not an oversight: the in-memory fields (turn activity, follow-up queue
/// depth, journal degraded) stay on the authenticated UDS `way.metrics`, while
/// the unauthenticated endpoint carries durable counters an external scraper
/// needs. Nothing here is an identifier.
pub fn durable_projection(
    store: &crate::store::Store,
    locks: &crate::lock::LockManager,
    now_ms: i64,
) -> Result<Value, crate::store::StoreError> {
    let lock_status = locks.status().ok();
    let raised = crate::alerts::raised_conditions(store)?;
    let connection = store.connection()?;
    let journal_head: i64 = connection.query_row("SELECT COALESCE(MAX(seq), 0) FROM events", [], |row| row.get(0))?;

    let mut consumers = serde_json::Map::new();
    {
        let mut statement =
            connection.prepare("SELECT consumer_id, cursor, updated_at FROM consumer_checkpoints ORDER BY consumer_id")?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?))
        })?;
        for row in rows {
            let (consumer_id, cursor, updated_at) = row?;
            let settled: i64 = cursor.split(':').nth(1).and_then(|seq| seq.parse().ok()).unwrap_or(0);
            consumers.insert(
                consumer_id,
                serde_json::json!({
                    "lag_events": (journal_head - settled).max(0),
                    "idle_seconds": ((now_ms - updated_at).max(0)) / 1_000,
                }),
            );
        }
    }

    let job_count = |state: &str| -> Result<i64, crate::store::StoreError> {
        connection
            .query_row(
                "SELECT COUNT(*) FROM schedule_jobs WHERE state = ?1",
                rusqlite::params![state],
                |row| row.get::<_, i64>(0),
            )
            .map_err(crate::store::StoreError::from)
    };
    let failed_closed_since: Option<String> = connection
        .query_row("SELECT v FROM gateway_meta WHERE k = 'failed_closed_since_ms'", [], |row| row.get::<_, String>(0))
        .optional()?;
    let failed_closed_seconds = failed_closed_since
        .and_then(|raw| raw.trim().parse::<i64>().ok())
        .map(|since| ((now_ms - since).max(0)) / 1_000)
        .unwrap_or(0);

    Ok(serde_json::json!({
        "gajaeway_failed_closed_seconds": failed_closed_seconds,
        "gajaeway_journal_head_seq": journal_head,
        "gajaeway_alerts_raised": raised.len(),
        "gajaeway_lock_held": lock_status.as_ref().map(|status| status.held).unwrap_or(false),
        "gajaeway_lock_queue_len": lock_status.as_ref().map(|status| status.queue.len()).unwrap_or(0),
        "gajaeway_lock_stuck": lock_status.as_ref().map(|status| status.stuck).unwrap_or(false),
        "gajaeway_lock_quarantined": lock_status.as_ref().map(|status| status.quarantined).unwrap_or(false),
        "gajaeway_schedule": {
            "active_jobs": job_count("active")?,
            "backoff_jobs": job_count("backoff")?,
            "suspended_jobs": job_count("suspended")?,
        },
        "gajaeway_consumers": Value::Object(consumers),
    }))
}

/// Renders the `way.metrics` projection as Prometheus text exposition.
///
/// Only numbers and booleans are emitted. A string value is skipped rather than
/// rendered as a label, so a future field carrying free text cannot leak into
/// this surface by accident.
pub fn render_prometheus(metrics: &Value) -> String {
    let mut out = String::new();
    let Some(object) = metrics.as_object() else {
        return out;
    };
    for (key, value) in object {
        match value {
            Value::Bool(flag) => {
                out.push_str(&format!("{key} {}\n", if *flag { 1 } else { 0 }));
            }
            Value::Number(number) => {
                out.push_str(&format!("{key} {number}\n"));
            }
            Value::Object(nested) => {
                for (nested_key, nested_value) in nested {
                    match nested_value {
                        Value::Bool(flag) => out.push_str(&format!(
                            "{key}{{name=\"{}\"}} {}\n",
                            sanitize_label(nested_key),
                            if *flag { 1 } else { 0 }
                        )),
                        Value::Number(number) => out.push_str(&format!(
                            "{key}{{name=\"{}\"}} {number}\n",
                            sanitize_label(nested_key)
                        )),
                        Value::Object(inner) => {
                            for (inner_key, inner_value) in inner {
                                if let Some(number) = inner_value.as_i64() {
                                    out.push_str(&format!(
                                        "{key}_{inner_key}{{name=\"{}\"}} {number}\n",
                                        sanitize_label(nested_key)
                                    ));
                                }
                            }
                        }
                        // Strings are intentionally not rendered.
                        _ => {}
                    }
                }
            }
            // `gajaeway_state` and any other string stay out of the exposition.
            _ => {}
        }
    }
    out
}

/// Label values are restricted to a conservative character set so a configured
/// consumer id cannot inject exposition syntax.
fn sanitize_label(value: &str) -> String {
    value
        .chars()
        .map(|character| if character.is_ascii_alphanumeric() || character == '-' || character == '_' { character } else { '_' })
        .collect()
}

/// The single response the endpoint serves. Every other path and method gets a
/// bare 404 with no request echo.
pub fn http_response(method: &str, path: &str, body: &str) -> String {
    if method == "GET" && path == "/metrics" {
        return format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
    }
    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_owned()
}

/// Parses a request line, rejecting anything that is not a simple method/path.
pub fn parse_request_line(line: &str) -> Option<(String, String)> {
    let mut parts = line.split_whitespace();
    let method = parts.next()?;
    let target = parts.next()?;
    if !method.chars().all(|character| character.is_ascii_uppercase()) {
        return None;
    }
    // Strip any query string; only the path selects a response.
    let path = target.split('?').next().unwrap_or(target);
    Some((method.to_owned(), path.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_non_loopback_bind_is_refused() {
        let error = bind_metrics_listener("0.0.0.0:0".parse().unwrap()).unwrap_err();
        assert!(matches!(error, MetricsError::NonLoopbackBind(_)));
        // Loopback binds normally.
        assert!(bind_metrics_listener("127.0.0.1:0".parse().unwrap()).is_ok());
    }

    #[test]
    fn an_occupied_port_is_refused_rather_than_rebound() {
        let first = bind_metrics_listener("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = first.local_addr().unwrap();
        let error = bind_metrics_listener(addr).unwrap_err();
        assert!(matches!(error, MetricsError::Bind(_)));
    }

    #[test]
    fn only_get_metrics_is_served_and_nothing_is_echoed() {
        let body = "gajaeway_lock_held 0\n";
        assert!(http_response("GET", "/metrics", body).contains("200 OK"));
        for (method, path) in [("POST", "/metrics"), ("GET", "/healthz"), ("GET", "/metrics/../x")] {
            let response = http_response(method, path, body);
            assert!(response.contains("404 Not Found"), "{method} {path} must be refused");
            assert!(!response.contains(path), "a refusal must not echo the request target");
            assert!(!response.contains(body), "a refusal must not leak the exposition");
        }
    }

    #[test]
    fn rendering_emits_numbers_and_booleans_but_never_strings() {
        let rendered = render_prometheus(&json!({
            "gajaeway_state": "failed_closed",
            "gajaeway_lock_held": true,
            "gajaeway_journal_head_seq": 42,
            "gajaeway_consumers": { "gajaeway-discord": { "lag_events": 3 } },
        }));
        assert!(rendered.contains("gajaeway_lock_held 1\n"));
        assert!(rendered.contains("gajaeway_journal_head_seq 42\n"));
        assert!(rendered.contains("gajaeway_consumers_lag_events{name=\"gajaeway-discord\"} 3\n"));
        // The state string must not appear: strings are never rendered.
        assert!(!rendered.contains("failed_closed"));
    }

    #[test]
    fn a_label_cannot_inject_exposition_syntax() {
        let rendered = render_prometheus(&json!({
            "gajaeway_consumers": { "evil\" } injected 1\n#": { "lag_events": 1 } },
        }));
        assert!(!rendered.contains("injected 1"));
        assert!(rendered.contains("lag_events"));
    }

    #[test]
    fn a_request_line_must_be_a_simple_method_and_path() {
        assert_eq!(parse_request_line("GET /metrics HTTP/1.1"), Some(("GET".to_owned(), "/metrics".to_owned())));
        assert_eq!(parse_request_line("GET /metrics?x=1 HTTP/1.1"), Some(("GET".to_owned(), "/metrics".to_owned())));
        assert_eq!(parse_request_line("get /metrics"), None);
        assert_eq!(parse_request_line("GET"), None);
    }
}
