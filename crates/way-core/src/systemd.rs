//! P0 systemd integration boundary.

/// P0 has no notifier yet; P8 owns hardened systemd readiness integration.
pub const SYSTEMD_NOTIFY_AVAILABLE: bool = false;
