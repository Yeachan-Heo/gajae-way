//! Method dispatch boundary reserved for P2.

pub fn unavailable(method: &str) -> String {
	format!("RPC method {method:?} is unavailable before P2")
}
