//! Rust-owned UDS JSON-RPC server.

use std::{
	path::PathBuf,
	sync::{
		Arc, OnceLock,
		atomic::{AtomicBool, Ordering},
	},
};

use tokio::{
	net::UnixListener,
	runtime::{Builder, Runtime},
	sync::{Notify, watch},
	task::JoinHandle,
};

pub mod auth;
pub mod dispatch;
pub mod framing;

use auth::{PeerAuth, SocketBootError, bind_socket, verify_peer};
use dispatch::{RpcBridgeStats, RpcDispatcher, TsfnBridge};
use framing::{ConnectionCounters, serve_connection};

static RPC_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn rpc_runtime() -> &'static Runtime {
	RPC_RUNTIME.get_or_init(|| {
		Builder::new_multi_thread()
			.enable_all()
			.thread_name("way-rpc")
			.build()
			.expect("way RPC runtime must initialize")
	})
}

/// Per-request cancellation shared by the transport, bridge, and lock waiter.
#[derive(Clone, Debug)]
pub struct CancellationToken {
	cancelled: Arc<AtomicBool>,
	changed: watch::Sender<bool>,
	notify: Arc<Notify>,
}

impl CancellationToken {
	pub fn new() -> Self {
		let (changed, _) = watch::channel(false);
		Self { cancelled: Arc::new(AtomicBool::new(false)), changed, notify: Arc::new(Notify::new()) }
	}

	pub fn cancel(&self) {
		if !self.cancelled.swap(true, Ordering::AcqRel) {
			let _ = self.changed.send(true);
			self.notify.notify_waiters();
		}
	}

	pub fn is_cancelled(&self) -> bool {
		self.cancelled.load(Ordering::Acquire)
	}

	pub fn atomic_flag(&self) -> Arc<AtomicBool> {
		self.cancelled.clone()
	}

	pub async fn cancelled(&self) {
		if self.is_cancelled() {
			return;
		}
		let mut changed = self.changed.subscribe();
		loop {
			if *changed.borrow() || self.is_cancelled() {
				return;
			}
			if changed.changed().await.is_err() {
				return;
			}
		}
	}
}

pub struct RpcServerHandle {
	shutdown: watch::Sender<bool>,
	bridge: TsfnBridge,
	dispatcher: RpcDispatcher,
	_task: JoinHandle<()>,
	counters: ConnectionCounters,
}

impl RpcServerHandle {
	pub fn start(socket_path: PathBuf, state_dir: PathBuf, dispatcher: RpcDispatcher) -> Result<Self, SocketBootError> {
		let listener = bind_socket(&state_dir, &socket_path)?;
		listener.set_nonblocking(true)?;
		let listener = {
			let _runtime_guard = rpc_runtime().enter();
			UnixListener::from_std(listener)?
		};
		let (shutdown, shutdown_receiver) = watch::channel(false);
		let bridge = dispatcher.bridge();
		let counters = ConnectionCounters::default();
		let listener_counters = counters.clone();
		let dispatcher_handle = dispatcher.clone();
		let task = rpc_runtime().spawn(async move {
			run_listener(listener, dispatcher, shutdown_receiver, listener_counters, socket_path).await;
		});
		Ok(Self { shutdown, bridge, dispatcher: dispatcher_handle, _task: task, counters })
	}

	pub fn shutdown(&self) {
		self.bridge.shutdown();
		let _ = self.shutdown.send(true);
	}

	pub fn bridge_complete(&self, request_id: u64, result_json: &str) -> bool {
		self.bridge.complete_json(request_id, result_json)
	}

	pub fn bridge_stats(&self) -> RpcBridgeStats {
		self.bridge.stats()
	}

	pub fn dropped_notification_count(&self) -> u64 {
		self.counters.dropped_notification_count()
	}

	pub fn set_gateway_state(&self, state: dispatch::GatewayState, reason: Option<String>) {
		self.dispatcher.set_gateway_state(state, reason);
	}

	pub fn set_main_session_status(&self, turn_state: String, follow_up_queue_depth: u64) {
		self.dispatcher.set_main_session_status(turn_state, follow_up_queue_depth);
	}

	pub fn reset_main_session_status(&self) {
		self.dispatcher.reset_main_session_status();
	}

	pub fn set_journal_degraded(&self, degraded: bool) {
		self.dispatcher.set_journal_degraded(degraded);
	}
}

impl Drop for RpcServerHandle {
	fn drop(&mut self) {
		self.shutdown();
	}
}

async fn run_listener(
	listener: UnixListener,
	dispatcher: RpcDispatcher,
	mut shutdown: watch::Receiver<bool>,
	counters: ConnectionCounters,
	socket_path: PathBuf,
) {
	loop {
		tokio::select! {
			changed = shutdown.changed() => {
				if changed.is_err() || *shutdown.borrow() {
					break;
				}
			}
			accepted = listener.accept() => match accepted {
				Ok((stream, _)) => match verify_peer(&stream) {
					Ok(PeerAuth::Verified) => {
						let dispatcher = dispatcher.clone();
						let connection_shutdown = shutdown.clone();
						let counters = counters.clone();
						rpc_runtime().spawn(async move {
							serve_connection(stream, dispatcher, connection_shutdown, counters).await;
						});
					}
					Ok(PeerAuth::Rejected | PeerAuth::Unverified) | Err(_) => {
						// Drop before parsing even one byte. Do not emit an oracle response.
						drop(stream);
					}
				},
				Err(error) => {
					eprintln!("way-core rpc accept failure: {error}");
				}
			}
		}
	}
	let _ = std::fs::remove_file(socket_path);
}

#[cfg(test)]
mod tests {
	use std::{
		fs,
		path::{Path, PathBuf},
		sync::atomic::{AtomicU64, Ordering},
		time::Duration,
	};

	use serde_json::{Value, json};
	use tokio::{
		io::{AsyncReadExt, AsyncWriteExt},
		net::UnixStream,
		time::timeout,
	};

	use super::{RpcServerHandle, dispatch::RpcDispatcher, dispatch::TsfnBridge};
	use crate::{events::EventJournal, lock::LockManager, store::Store};

	static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

	fn state_dir(name: &str) -> PathBuf {
		let state = std::env::temp_dir().join(format!(
			"gajae-way-rpc-server-{name}-{}-{}",
			std::process::id(),
			NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed)
		));
		fs::create_dir_all(&state).unwrap();
		state
	}

	fn start_server(state: &Path) -> (RpcServerHandle, LockManager) {
		let store = Store::open(state).unwrap();
		let locks = LockManager::new(store.clone());
		let dispatcher = RpcDispatcher::new(store.clone(), locks.clone(), EventJournal::new(store), TsfnBridge::for_tests());
		let handle = RpcServerHandle::start(state.join("rpc.sock"), state.to_path_buf(), dispatcher).unwrap();
		(handle, locks)
	}

	async fn connect(state: &Path) -> UnixStream {
		UnixStream::connect(state.join("rpc.sock")).await.unwrap()
	}

	async fn read_line(stream: &mut UnixStream) -> String {
		let mut bytes = Vec::new();
		loop {
			let mut byte = [0_u8; 1];
			let count = stream.read(&mut byte).await.unwrap();
			assert_ne!(count, 0, "socket closed before an expected response");
			if byte[0] == b'\n' {
				return String::from_utf8(bytes).unwrap();
			}
			bytes.push(byte[0]);
		}
	}

	fn event_read_request(id: u64) -> String {
		format!(
			"{}\n",
			json!({
				"jsonrpc": "2.0",
				"id": id,
				"method": "main.events.read",
				"params": { "cursor": "1:0", "wait_ms": 60_000 },
			})
		)
	}

	async fn stop(handle: RpcServerHandle, state: PathBuf) {
		handle.shutdown();
		drop(handle);
		tokio::time::sleep(Duration::from_millis(30)).await;
		let _ = fs::remove_dir_all(state);
	}

	#[tokio::test]
	async fn batch_and_notification_handling_use_the_real_socket() {
		let state = state_dir("batch");
		let (handle, _) = start_server(&state);
		let mut stream = connect(&state).await;
		stream.write_all(b"[]\n").await.unwrap();
		let batch: Value = serde_json::from_str(&read_line(&mut stream).await).unwrap();
		assert_eq!(batch["error"]["code"], -32600);
		assert_eq!(batch["error"]["message"], "batch_unsupported");
		stream
			.write_all(
				b"{\"jsonrpc\":\"2.0\",\"method\":\"way.health\",\"params\":{}}\n{\"jsonrpc\":\"2.0\",\"id\":9,\"method\":\"way.health\",\"params\":{}}\n",
			)
			.await
			.unwrap();
		let health: Value = serde_json::from_str(&read_line(&mut stream).await).unwrap();
		assert_eq!(health["id"], 9);
		assert_eq!(health["result"]["status"], "healthy");
		tokio::time::sleep(Duration::from_millis(20)).await;
		assert_eq!(handle.dropped_notification_count(), 1);
		drop(stream);
		stop(handle, state).await;
	}

	#[tokio::test]
	async fn duplicate_ids_and_sixteen_request_backpressure_are_connection_scoped() {
		let state = state_dir("pipeline");
		let (handle, _) = start_server(&state);
		let mut duplicate_stream = connect(&state).await;
		duplicate_stream.write_all(event_read_request(1).as_bytes()).await.unwrap();
		duplicate_stream
			.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"way.health\",\"params\":{}}\n")
			.await
			.unwrap();
		let duplicate: Value = serde_json::from_str(&read_line(&mut duplicate_stream).await).unwrap();
		assert_eq!(duplicate["error"]["code"], -32600);
		assert_eq!(duplicate["error"]["message"], "duplicate_id");
		drop(duplicate_stream);

		let mut pipeline_stream = connect(&state).await;
		let mut pipeline = String::new();
		for id in 2..=18 {
			pipeline.push_str(&event_read_request(id));
		}
		pipeline_stream.write_all(pipeline.as_bytes()).await.unwrap();
		assert!(timeout(Duration::from_millis(100), read_line(&mut pipeline_stream)).await.is_err());
		drop(pipeline_stream);
		tokio::time::sleep(Duration::from_millis(250)).await;
		// Closing the connection cancels all pending event long-polls.
		stop(handle, state).await;
	}

	#[tokio::test]
	async fn rpc_cancel_notification_cancels_an_in_memory_event_waiter() {
		let state = state_dir("cancel");
		let (handle, _) = start_server(&state);
		let mut stream = connect(&state).await;
		stream.write_all(event_read_request(44).as_bytes()).await.unwrap();
		tokio::time::sleep(Duration::from_millis(30)).await;
		stream
			.write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"rpc.cancel\",\"params\":{\"id\":44}}\n")
			.await
			.unwrap();
		let cancelled: Value = serde_json::from_str(&read_line(&mut stream).await).unwrap();
		assert_eq!(cancelled["id"], 44);
		assert_eq!(cancelled["error"]["code"], -32603);
		drop(stream);
		stop(handle, state).await;
	}
}
