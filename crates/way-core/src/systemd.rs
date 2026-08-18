//! Minimal sd_notify support. The gateway deliberately treats an absent
//! NOTIFY_SOCKET as ordinary non-systemd execution.

use std::{env, io};

#[cfg(unix)]
use std::os::unix::net::UnixDatagram;

/// Sends `STATUS=<status>` to systemd when `NOTIFY_SOCKET` is present.
pub fn notify_status(status: &str) -> io::Result<()> {
	notify(&format!("STATUS={status}"))
}

/// Marks the process ready after its UDS RPC endpoint and strict-resumed main
/// session are both available. A status accompanies readiness for `systemctl`
/// and journal observability.
pub fn notify_ready(status: &str) -> io::Result<()> {
	notify(&format!("READY=1\nSTATUS={status}"))
}

fn notify(payload: &str) -> io::Result<()> {
	let Ok(target) = env::var("NOTIFY_SOCKET") else {
		return Ok(());
	};
	if target.is_empty() {
		return Ok(());
	}
	#[cfg(unix)]
	{
		let socket = UnixDatagram::unbound()?;
		if let Some(abstract_name) = target.strip_prefix('@') {
			return send_abstract(&socket, abstract_name, payload.as_bytes());
		}
		socket.send_to(payload.as_bytes(), target)?;
		return Ok(());
	}
	#[cfg(not(unix))]
	{
		let _ = payload;
		Ok(())
	}
}

#[cfg(target_os = "linux")]
fn send_abstract(socket: &UnixDatagram, name: &str, payload: &[u8]) -> io::Result<()> {
	use std::{mem, os::fd::AsRawFd};

	if name.is_empty() || name.as_bytes().contains(&0) {
		return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid abstract NOTIFY_SOCKET"));
	}
	let mut address: libc::sockaddr_un = unsafe { mem::zeroed() };
	let capacity = address.sun_path.len().saturating_sub(1);
	if name.len() > capacity {
		return Err(io::Error::new(io::ErrorKind::InvalidInput, "abstract NOTIFY_SOCKET is too long"));
	}
	address.sun_family = libc::AF_UNIX as libc::sa_family_t;
	for (index, byte) in name.as_bytes().iter().enumerate() {
		address.sun_path[index + 1] = *byte as libc::c_char;
	}
	let address_length = (mem::size_of::<libc::sa_family_t>() + 1 + name.len()) as libc::socklen_t;
	let sent = unsafe {
		libc::sendto(
			socket.as_raw_fd(),
			payload.as_ptr().cast(),
			payload.len(),
			0,
			(&address as *const libc::sockaddr_un).cast(),
			address_length,
		)
	};
	if sent < 0 {
		return Err(io::Error::last_os_error());
	}
	Ok(())
}

#[cfg(all(unix, not(target_os = "linux")))]
fn send_abstract(_socket: &UnixDatagram, _name: &str, _payload: &[u8]) -> io::Result<()> {
	// systemd's abstract namespace is Linux-specific. A service manager on other
	// Unix platforms can still use a filesystem socket.
	Ok(())
}