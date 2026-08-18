//! Peer-auth boundary reserved for P2 UDS credential validation.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerAuth {
	Unverified,
	Verified,
}
