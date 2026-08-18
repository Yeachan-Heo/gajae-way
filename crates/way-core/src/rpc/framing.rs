//! Strict NDJSON JSON-RPC 2.0 framing.
//!
//! A connection reads one UTF-8 JSON object per newline, starts requests in
//! arrival order, and deliberately stops reading when sixteen requests are
//! outstanding. That leaves excess bytes in the kernel receive buffer rather
//! than accumulating an application-level queue.

use std::{
	collections::HashMap,
	io,
	os::fd::{AsRawFd, RawFd},
	sync::{
		atomic::{AtomicU64, Ordering},
		Arc,
	},
	time::Duration,
};

use serde_json::{json, Map, Value};
use tokio::{
	io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
	net::UnixStream,
	sync::{mpsc, watch},
};

use super::{
	dispatch::{RpcDispatcher, RpcError},
	CancellationToken,
};

pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
pub const MAX_IN_FLIGHT_REQUESTS: usize = 16;

#[derive(Debug, PartialEq)]
pub enum FrameReadError {
	PayloadTooLarge,
	InvalidUtf8,
	InvalidJson,
	InvalidRequest(RpcError),
	Io(io::ErrorKind),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RpcRequest {
	pub id: Option<Value>,
	pub method: String,
	pub params: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DecodedFrame {
	Blank,
	Batch,
	Request(RpcRequest),
}

/// A bounded line reader. It never accumulates more than one frame plus one
/// socket read chunk before reporting an over-size payload and closing.
pub struct NdjsonReader<R> {
	reader: R,
	pending: Vec<u8>,
}

impl<R> NdjsonReader<R>
where
	R: AsyncRead + Unpin,
{
	pub fn new(reader: R) -> Self {
		Self { reader, pending: Vec::with_capacity(8 * 1024) }
	}

	pub async fn next_frame(&mut self) -> Result<Option<DecodedFrame>, FrameReadError> {
		loop {
			if let Some(newline) = self.pending.iter().position(|byte| *byte == b'\n') {
				let line = self.pending.drain(..=newline).collect::<Vec<_>>();
				let content = &line[..line.len().saturating_sub(1)];
				if content.len() > MAX_FRAME_BYTES {
					return Err(FrameReadError::PayloadTooLarge);
				}
				return decode_line(content).map(Some);
			}
			if self.pending.len() > MAX_FRAME_BYTES {
				return Err(FrameReadError::PayloadTooLarge);
			}
			let mut buffer = [0_u8; 8192];
			let read = self.reader.read(&mut buffer).await.map_err(|error| FrameReadError::Io(error.kind()))?;
			if read == 0 {
				return if self.pending.is_empty() {
					Ok(None)
				} else {
					Err(FrameReadError::InvalidJson)
				};
			};
			self.pending.extend_from_slice(&buffer[..read]);
		}
	}
}

impl<R> NdjsonReader<R>
where
	R: AsRawFd,
{
	/// Observes a hang-up without consuming queued request bytes. This keeps the
	/// full-pipeline backpressure invariant while still cancelling long-polls as
	/// soon as the peer closes a socket whose receive buffer is already full.
	fn peer_hung_up(&self) -> bool {
		let mut descriptor = libc::pollfd { fd: self.reader.as_raw_fd(), events: libc::POLLIN, revents: 0 };
		let result = unsafe { libc::poll(&mut descriptor, 1, 0) };
		result > 0 && (descriptor.revents & (libc::POLLHUP | libc::POLLERR)) != 0
	}
}

impl NdjsonReader<UnixStream> {
	/// Reads exactly one line from a UDS without prefetching a following request.
	/// Peeking leaves later frames in the kernel while the pipeline is full.
	async fn next_socket_frame(&mut self) -> Result<Option<DecodedFrame>, FrameReadError> {
		loop {
			if let Some(newline) = self.pending.iter().position(|byte| *byte == b'\n') {
				let line = self.pending.drain(..=newline).collect::<Vec<_>>();
				let content = &line[..line.len().saturating_sub(1)];
				if content.len() > MAX_FRAME_BYTES {
					return Err(FrameReadError::PayloadTooLarge);
				}
				return decode_line(content).map(Some);
			}
			if self.pending.len() > MAX_FRAME_BYTES {
				return Err(FrameReadError::PayloadTooLarge);
			}
			self.reader.readable().await.map_err(|error| FrameReadError::Io(error.kind()))?;
			let available = socket_available_bytes(self.reader.as_raw_fd()).map_err(|error| FrameReadError::Io(error.kind()))?;
			if available == 0 {
				let mut byte = [0_u8; 1];
				match self.reader.try_read(&mut byte) {
					Ok(0) => return if self.pending.is_empty() { Ok(None) } else { Err(FrameReadError::InvalidJson) },
					Ok(read) => self.pending.extend_from_slice(&byte[..read]),
					Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
					Err(error) => return Err(FrameReadError::Io(error.kind())),
				}
				continue;
			}
			let mut buffer = [0_u8; 8192];
			let peek_len = available.min(buffer.len());
			let peeked = unsafe {
				libc::recv(
					self.reader.as_raw_fd(),
					buffer.as_mut_ptr().cast(),
					peek_len,
					libc::MSG_PEEK | libc::MSG_DONTWAIT,
				)
			};
			if peeked < 0 {
				let error = io::Error::last_os_error();
				if error.kind() == io::ErrorKind::WouldBlock {
					continue;
				}
				return Err(FrameReadError::Io(error.kind()));
			}
			if peeked == 0 {
				return if self.pending.is_empty() { Ok(None) } else { Err(FrameReadError::InvalidJson) };
			}
			let peeked = peeked as usize;
			let read_len = buffer[..peeked]
				.iter()
				.position(|byte| *byte == b'\n')
				.map(|newline| newline + 1)
				.unwrap_or(peeked);
			match self.reader.try_read(&mut buffer[..read_len]) {
				Ok(0) => return if self.pending.is_empty() { Ok(None) } else { Err(FrameReadError::InvalidJson) },
				Ok(read) => self.pending.extend_from_slice(&buffer[..read]),
				Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
				Err(error) => return Err(FrameReadError::Io(error.kind())),
			}
		}
	}
}

fn socket_available_bytes(fd: RawFd) -> io::Result<usize> {
	let mut available = 0_i32;
	if unsafe { libc::ioctl(fd, libc::FIONREAD, &mut available) } != 0 {
		return Err(io::Error::last_os_error());
	}
	Ok(usize::try_from(available).unwrap_or(0))
}

pub fn decode_line(line: &[u8]) -> Result<DecodedFrame, FrameReadError> {
	if line.iter().all(u8::is_ascii_whitespace) {
		return Ok(DecodedFrame::Blank);
	}
	let text = std::str::from_utf8(line).map_err(|_| FrameReadError::InvalidUtf8)?;
	let value = serde_json::from_str::<Value>(text).map_err(|_| FrameReadError::InvalidJson)?;
	match value {
		Value::Array(_) => Ok(DecodedFrame::Batch),
		Value::Object(object) => parse_request(object).map(DecodedFrame::Request).map_err(FrameReadError::InvalidRequest),
		_ => Err(FrameReadError::InvalidRequest(RpcError::invalid_request("request must be an object"))),
	}
}

fn parse_request(mut object: Map<String, Value>) -> Result<RpcRequest, RpcError> {
	if object.remove("jsonrpc") != Some(Value::String("2.0".to_owned())) {
		return Err(RpcError::invalid_request("jsonrpc must be exactly 2.0"));
	}
	let method = object
		.remove("method")
		.and_then(|value| value.as_str().map(ToOwned::to_owned))
		.filter(|method| !method.is_empty())
		.ok_or_else(|| RpcError::invalid_request("method must be a non-empty string"))?;
	let id = object.remove("id");
	if id.as_ref().is_some_and(|id| !matches!(id, Value::String(_) | Value::Number(_) | Value::Null)) {
		return Err(RpcError::invalid_request("id must be a string, number, or null"));
	}
	let params = object.remove("params").unwrap_or(Value::Null);
	if !matches!(params, Value::Null | Value::Object(_) | Value::Array(_)) {
		return Err(RpcError::invalid_request("params must be structured"));
	}
	if let Some(unexpected) = object.keys().next() {
		return Err(RpcError::invalid_request(format!("unknown request member: {unexpected}")));
	}
	Ok(RpcRequest { id, method, params })
}

pub fn result_response(id: Value, result: Value) -> Vec<u8> {
	serialize_response(json!({ "jsonrpc": "2.0", "id": id, "result": result }))
}

pub fn error_response(id: Value, error: RpcError) -> Vec<u8> {
	serialize_response(json!({ "jsonrpc": "2.0", "id": id, "error": error.as_json() }))
}

fn serialize_response(value: Value) -> Vec<u8> {
	let mut encoded = serde_json::to_vec(&value).expect("JSON-RPC response values must serialize");
	encoded.push(b'\n');
	encoded
}

#[derive(Debug)]
struct Completion {
	key: String,
	id: Value,
	outcome: Result<Value, RpcError>,
}

#[derive(Debug)]
struct InFlight {
	id: Value,
	cancellation: CancellationToken,
}

#[derive(Clone)]
pub struct ConnectionCounters {
	pub dropped_notifications: Arc<AtomicU64>,
}

impl Default for ConnectionCounters {
	fn default() -> Self {
		Self { dropped_notifications: Arc::new(AtomicU64::new(0)) }
	}
}

impl ConnectionCounters {
	pub fn dropped_notification_count(&self) -> u64 {
		self.dropped_notifications.load(Ordering::Relaxed)
	}
}

/// Serves one authenticated UDS connection until peer EOF, a framing failure,
/// or daemon shutdown.
pub async fn serve_connection(
	stream: tokio::net::UnixStream,
	dispatcher: RpcDispatcher,
	mut shutdown: watch::Receiver<bool>,
	counters: ConnectionCounters,
) {
	let stream = match stream.into_std() {
		Ok(stream) => stream,
		Err(_) => return,
	};
	let write_stream = match stream.try_clone() {
		Ok(stream) => stream,
		Err(_) => return,
	};
	let reader_stream = match UnixStream::from_std(stream) {
		Ok(stream) => stream,
		Err(_) => return,
	};
	let mut write_half = match UnixStream::from_std(write_stream) {
		Ok(stream) => stream,
		Err(_) => return,
	};
	let mut reader = NdjsonReader::new(reader_stream);
	let (completion_sender, mut completions) = mpsc::unbounded_channel::<Completion>();
	let mut in_flight = HashMap::<String, InFlight>::new();

	loop {
		if *shutdown.borrow() {
			shutdown_connection(&mut write_half, &mut in_flight).await;
			return;
		}
		if in_flight.len() >= MAX_IN_FLIGHT_REQUESTS {
			tokio::select! {
				changed = shutdown.changed() => {
					if changed.is_err() || *shutdown.borrow() {
						shutdown_connection(&mut write_half, &mut in_flight).await;
						return;
					}
				}
				completion = completions.recv() => {
					if let Some(completion) = completion {
						write_completion(&mut write_half, &mut in_flight, completion).await;
					}
				}
				_ = tokio::time::sleep(Duration::from_millis(25)) => {
					if reader.peer_hung_up() {
						cancel_in_flight(&in_flight);
						return;
					}
				}
			}
			continue;
		}

		tokio::select! {
			changed = shutdown.changed() => {
				if changed.is_err() || *shutdown.borrow() {
					shutdown_connection(&mut write_half, &mut in_flight).await;
					return;
				}
			}
			completion = completions.recv(), if !in_flight.is_empty() => {
				if let Some(completion) = completion {
					write_completion(&mut write_half, &mut in_flight, completion).await;
				}
			}
			frame = reader.next_socket_frame() => {
				match frame {
					Ok(None) => {
						cancel_in_flight(&in_flight);
						return;
					}
					Ok(Some(DecodedFrame::Blank)) => {}
					Ok(Some(DecodedFrame::Batch)) => {
						if write_half.write_all(&error_response(Value::Null, RpcError::invalid_request("batch_unsupported"))).await.is_err() {
							cancel_in_flight(&in_flight);
							return;
						}
					}
					Ok(Some(DecodedFrame::Request(request))) => {
						if !handle_request(request, &dispatcher, &mut write_half, &mut in_flight, &completion_sender, &counters).await {
							cancel_in_flight(&in_flight);
							return;
						}
					}
					Err(FrameReadError::PayloadTooLarge) => {
						let _ = write_half.write_all(&error_response(Value::Null, RpcError::invalid_request("payload_too_large"))).await;
						cancel_in_flight(&in_flight);
						return;
					}
					Err(FrameReadError::InvalidRequest(error)) => {
						if write_half.write_all(&error_response(Value::Null, error)).await.is_err() {
							cancel_in_flight(&in_flight);
							return;
						}
					}
					Err(FrameReadError::InvalidUtf8 | FrameReadError::InvalidJson | FrameReadError::Io(_)) => {
						cancel_in_flight(&in_flight);
						return;
					}
				}
			}
		}
	}
}

async fn handle_request(
	request: RpcRequest,
	dispatcher: &RpcDispatcher,
	write_half: &mut UnixStream,
	in_flight: &mut HashMap<String, InFlight>,
	completion_sender: &mpsc::UnboundedSender<Completion>,
	counters: &ConnectionCounters,
) -> bool {
	let RpcRequest { id, method, params } = request;
	if id.is_none() {
		if method == "rpc.cancel" {
			cancel_requested(&params, in_flight);
		} else {
			counters.dropped_notifications.fetch_add(1, Ordering::Relaxed);
		}
		return true;
	}
	let id = id.expect("checked above");
	if method == "rpc.cancel" {
		let cancelled = cancel_requested(&params, in_flight);
		return write_half.write_all(&result_response(id, json!({ "cancelled": cancelled }))).await.is_ok();
	}
	let key = request_key(&id);
	if in_flight.contains_key(&key) {
		return write_half
			.write_all(&error_response(id, RpcError::invalid_request("duplicate_id")))
			.await
			.is_ok();
	}
	let cancellation = CancellationToken::new();
	in_flight.insert(key.clone(), InFlight { id: id.clone(), cancellation: cancellation.clone() });
	let dispatcher = dispatcher.clone();
	let completion_sender = completion_sender.clone();
	tokio::spawn(async move {
		let outcome = dispatcher.dispatch(method, params, cancellation).await;
		let _ = completion_sender.send(Completion { key, id, outcome });
	});
	true
}

fn cancel_requested(params: &Value, in_flight: &HashMap<String, InFlight>) -> bool {
	let Some(id) = params.as_object().and_then(|object| object.get("id")) else {
		return false;
	};
	let Some(token) = in_flight.get(&request_key(id)) else {
		return false;
	};
	token.cancellation.cancel();
	true
}

fn request_key(id: &Value) -> String {
	serde_json::to_string(id).expect("a parsed JSON-RPC id must serialize")
}

async fn write_completion(
	write_half: &mut UnixStream,
	in_flight: &mut HashMap<String, InFlight>,
	completion: Completion,
) {
	if in_flight.remove(&completion.key).is_none() {
		return;
	}
	let bytes = match completion.outcome {
		Ok(result) => result_response(completion.id, result),
		Err(error) => error_response(completion.id, error),
	};
	let _ = write_half.write_all(&bytes).await;
}

fn cancel_in_flight(in_flight: &HashMap<String, InFlight>) {
	for request in in_flight.values() {
		request.cancellation.cancel();
	}
}

async fn shutdown_connection(write_half: &mut UnixStream, in_flight: &mut HashMap<String, InFlight>) {
	let requests = in_flight.drain().map(|(_, request)| request).collect::<Vec<_>>();
	for request in &requests {
		request.cancellation.cancel();
	}
	for request in requests {
		if write_half
			.write_all(&error_response(request.id, RpcError::internal("shutting_down")))
			.await
			.is_err()
		{
			break;
		}
	}
	let _ = write_half.shutdown().await;
}

#[cfg(test)]
mod tests {
	use tokio::io::AsyncWriteExt;

	use super::{decode_line, DecodedFrame, FrameReadError, NdjsonReader, MAX_FRAME_BYTES};

	#[tokio::test]
	async fn blank_lines_are_ignored_and_one_object_is_decoded() {
		let (client, server) = tokio::io::duplex(4096);
		let mut client = client;
		client.write_all(b" \t\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"way.health\",\"params\":{}}\n").await.unwrap();
		let mut reader = NdjsonReader::new(server);
		assert_eq!(reader.next_frame().await.unwrap(), Some(DecodedFrame::Blank));
		assert!(matches!(reader.next_frame().await.unwrap(), Some(DecodedFrame::Request(request)) if request.method == "way.health"));
	}

	#[test]
	fn batch_is_explicitly_detected() {
		assert_eq!(decode_line(br#"[]"#).unwrap(), DecodedFrame::Batch);
	}

	#[tokio::test]
	async fn oversize_line_is_rejected_before_unbounded_buffering() {
		let (mut client, server) = tokio::io::duplex(MAX_FRAME_BYTES + 16 * 1024);
		let payload = vec![b'x'; MAX_FRAME_BYTES + 1];
		let mut reader = NdjsonReader::new(server);
		let (write, read) = tokio::join!(client.write_all(&payload), reader.next_frame());
		write.unwrap();
		assert_eq!(read.unwrap_err(), FrameReadError::PayloadTooLarge);
	}

	#[test]
	fn invalid_utf8_is_not_json() {
		assert_eq!(decode_line(&[0xff, b'\n']), Err(FrameReadError::InvalidUtf8));
	}
}
