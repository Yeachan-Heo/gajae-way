//! N-API surface for the gajae-way durable runtime.
//!
//! P0 establishes the single-addon boundary. Durable state, lock, event,
//! registry, RPC, and systemd behavior land in later phases behind this crate.

use std::{sync::OnceLock, time::SystemTime};

use napi_derive::napi;

pub mod events;
pub mod lock;
pub mod registry;
pub mod rpc;
pub mod store;
pub mod systemd;

static BOOT_EPOCH: OnceLock<u32> = OnceLock::new();

#[napi(object)]
pub struct HealthInfo {
	pub version: String,
	#[napi(js_name = "bootEpoch")]
	pub boot_epoch: u32,
}

/// Returns the process-local boot identity used by the P0 health probe.
#[napi(js_name = "healthInfo")]
pub fn health_info() -> HealthInfo {
	let boot_epoch = *BOOT_EPOCH.get_or_init(|| {
		SystemTime::now()
			.duration_since(SystemTime::UNIX_EPOCH)
			.expect("system time must not precede the Unix epoch")
			.as_secs()
			.try_into()
			.expect("P0 boot epoch fits in u32")
	});

	HealthInfo { version: env!("CARGO_PKG_VERSION").to_string(), boot_epoch }
}

/// P0 handle proving that TypeScript owns one stable `WayCore` N-API surface.
#[napi]
pub struct WayCore {
	state_dir: String,
}

#[napi]
impl WayCore {
	/// Opens a future state store. P0 records the requested directory only;
	/// migrations and SQLite ownership arrive in P1.
	#[napi(factory)]
	pub fn open(state_dir: String) -> napi::Result<Self> {
		if state_dir.trim().is_empty() {
			return Err(napi::Error::from_reason("stateDir must not be empty"));
		}
		Ok(Self { state_dir })
	}

	#[napi(getter, js_name = "stateDir")]
	pub fn state_dir(&self) -> String {
		self.state_dir.clone()
	}
}
