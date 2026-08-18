//! P0 shape for the exclusive corpus lock state machine.

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LockState {
	pub held: bool,
	pub quarantined: bool,
}
