//! P0 in-memory stand-in for the P1 SQLite WAL store.

use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct Store {
	values: BTreeMap<String, String>,
}

impl Store {
	pub fn put(&mut self, key: impl Into<String>, value: impl Into<String>) {
		self.values.insert(key.into(), value.into());
	}

	pub fn get(&self, key: &str) -> Option<&str> {
		self.values.get(key).map(String::as_str)
	}
}

#[cfg(test)]
mod tests {
	use super::Store;

	#[test]
	fn round_trips_a_value() {
		let mut store = Store::default();
		store.put("health", "healthy");

		assert_eq!(store.get("health"), Some("healthy"));
		assert_eq!(store.get("missing"), None);
	}
}
