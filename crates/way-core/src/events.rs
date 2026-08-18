//! P0 event-journal boundary types.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventFrame {
	pub kind: String,
	pub payload_json: String,
}
