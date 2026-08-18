//! P0 registry boundary types.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryRow {
	pub session_id: String,
	pub kind: String,
}
