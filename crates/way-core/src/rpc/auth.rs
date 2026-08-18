//! Unix-domain socket lifecycle and peer credential validation.
//!
//! Authentication happens before any bytes are parsed. The listener is only
//! created below the daemon-owned state directory, and a pre-existing path is
//! never removed unless it is a same-UID stale socket.

use std::{
	fmt,
	fs,
	io,
	os::{
		fd::{AsRawFd, RawFd},
		unix::{
			fs::{FileTypeExt, MetadataExt, PermissionsExt},
			net::{UnixListener, UnixStream},
		},
	},
	path::Path,
};

/// Result of checking the operating-system credentials attached to a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerAuth {
	Unverified,
	Verified,
	Rejected,
}

/// A boot-time socket safety failure. Callers surface these as typed startup
/// failures instead of trying to repair a path they do not own.
#[derive(Debug)]
pub enum SocketBootError {
	Io(io::Error),
	StateDirectoryIsSymlink,
	StateDirectoryNotDirectory,
	StateDirectoryForeign { uid: u32 },
	SocketOutsideStateDirectory,
	SocketParentIsSymlink,
	SocketPathIsSymlink,
	ExistingPathNotSocket,
	ExistingSocketForeign { uid: u32 },
	ActiveSocket,
}

impl fmt::Display for SocketBootError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Io(error) => write!(formatter, "RPC socket I/O error: {error}"),
			Self::StateDirectoryIsSymlink => formatter.write_str("RPC state directory must not be a symlink"),
			Self::StateDirectoryNotDirectory => formatter.write_str("RPC state directory is not a directory"),
			Self::StateDirectoryForeign { uid } => {
				write!(formatter, "RPC state directory is owned by uid {uid}, not this daemon")
			}
			Self::SocketOutsideStateDirectory => formatter.write_str("RPC socket must be directly inside the state directory"),
			Self::SocketParentIsSymlink => formatter.write_str("RPC socket parent must not be a symlink"),
			Self::SocketPathIsSymlink => formatter.write_str("RPC socket path must not be a symlink"),
			Self::ExistingPathNotSocket => formatter.write_str("RPC socket path is occupied by a non-socket file"),
			Self::ExistingSocketForeign { uid } => {
				write!(formatter, "RPC socket is owned by uid {uid}, not this daemon")
			}
			Self::ActiveSocket => formatter.write_str("RPC socket is already accepting connections"),
		}
	}
}

impl std::error::Error for SocketBootError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Io(error) => Some(error),
			_ => None,
		}
	}
}

impl From<io::Error> for SocketBootError {
	fn from(error: io::Error) -> Self {
		Self::Io(error)
	}
}

pub fn current_uid() -> u32 {
	unsafe { libc::geteuid() as u32 }
}

/// Creates a hardened listener at `socket_path`.
///
/// `socket_path` must be a direct child of `state_dir`; this makes every
/// component below the application-owned root explicit and rejects symlink
/// substitution rather than attempting to canonicalize through it.
pub fn bind_socket(state_dir: &Path, socket_path: &Path) -> Result<UnixListener, SocketBootError> {
	let state_metadata = fs::symlink_metadata(state_dir)?;
	if state_metadata.file_type().is_symlink() {
		return Err(SocketBootError::StateDirectoryIsSymlink);
	}
	if !state_metadata.is_dir() {
		return Err(SocketBootError::StateDirectoryNotDirectory);
	}
	let uid = current_uid();
	if state_metadata.uid() != uid {
		return Err(SocketBootError::StateDirectoryForeign { uid: state_metadata.uid() });
	}

	let parent = socket_path.parent().ok_or(SocketBootError::SocketOutsideStateDirectory)?;
	let parent_metadata = fs::symlink_metadata(parent)?;
	if parent_metadata.file_type().is_symlink() {
		return Err(SocketBootError::SocketParentIsSymlink);
	}
	let canonical_state = fs::canonicalize(state_dir)?;
	let canonical_parent = fs::canonicalize(parent)?;
	if canonical_parent != canonical_state {
		return Err(SocketBootError::SocketOutsideStateDirectory);
	}
	fs::set_permissions(state_dir, fs::Permissions::from_mode(0o700))?;

	match fs::symlink_metadata(socket_path) {
		Ok(metadata) => {
			if metadata.file_type().is_symlink() {
				return Err(SocketBootError::SocketPathIsSymlink);
			}
			if !metadata.file_type().is_socket() {
				return Err(SocketBootError::ExistingPathNotSocket);
			}
			if metadata.uid() != uid {
				return Err(SocketBootError::ExistingSocketForeign { uid: metadata.uid() });
			}
			match UnixStream::connect(socket_path) {
				Ok(_) => return Err(SocketBootError::ActiveSocket),
				Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => fs::remove_file(socket_path)?,
				Err(error) if error.kind() == io::ErrorKind::NotFound => {}
				Err(error) => return Err(SocketBootError::Io(error)),
			}
		}
		Err(error) if error.kind() == io::ErrorKind::NotFound => {}
		Err(error) => return Err(SocketBootError::Io(error)),
	}

	let listener = UnixListener::bind(socket_path)?;
	fs::set_permissions(socket_path, fs::Permissions::from_mode(0o600))?;
	Ok(listener)
}

/// Reads the peer's UID from the platform credential mechanism.
pub fn peer_uid(fd: RawFd) -> io::Result<u32> {
	#[cfg(target_os = "linux")]
	{
		let mut credential = std::mem::MaybeUninit::<libc::ucred>::zeroed();
		let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
		let result = unsafe {
			libc::getsockopt(
				fd,
				libc::SOL_SOCKET,
				libc::SO_PEERCRED,
				credential.as_mut_ptr().cast(),
				&mut length,
			)
		};
		if result != 0 {
			return Err(io::Error::last_os_error());
		}
		if length != std::mem::size_of::<libc::ucred>() as libc::socklen_t {
			return Err(io::Error::new(io::ErrorKind::InvalidData, "SO_PEERCRED returned an unexpected credential length"));
		}
		return Ok(unsafe { credential.assume_init().uid });
	}

	#[cfg(target_os = "macos")]
	{
		let mut uid = 0 as libc::uid_t;
		let mut gid = 0 as libc::gid_t;
		let result = unsafe { libc::getpeereid(fd, &mut uid, &mut gid) };
		if result != 0 {
			return Err(io::Error::last_os_error());
		}
		return Ok(uid);
	}

	#[cfg(not(any(target_os = "linux", target_os = "macos")))]
	{
		let _ = fd;
		Err(io::Error::new(io::ErrorKind::Unsupported, "peer credentials are unsupported on this platform"))
	}
}

pub fn peer_auth_for_uid(expected_uid: u32, actual_uid: u32) -> PeerAuth {
	if expected_uid == actual_uid {
		PeerAuth::Verified
	} else {
		PeerAuth::Rejected
	}
}

/// Checks a Tokio UDS connection before its first frame is read.
pub fn verify_peer(stream: &tokio::net::UnixStream) -> io::Result<PeerAuth> {
	peer_uid(stream.as_raw_fd()).map(|uid| peer_auth_for_uid(current_uid(), uid))
}

#[cfg(test)]
mod tests {
	use std::{
		fs,
		os::{
			fd::AsRawFd,
			unix::fs::{symlink, FileTypeExt, PermissionsExt},
		},
		path::PathBuf,
		sync::atomic::{AtomicU64, Ordering},
	};

	use super::{bind_socket, current_uid, peer_auth_for_uid, peer_uid, PeerAuth, SocketBootError};

	static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

	fn state_dir(name: &str) -> PathBuf {
		let path = std::env::temp_dir().join(format!(
			"gajae-way-rpc-auth-{name}-{}-{}",
			std::process::id(),
			NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed)
		));
		fs::create_dir_all(&path).unwrap();
		path
	}

	#[test]
	fn mismatch_branch_rejects_a_foreign_uid() {
		assert_eq!(peer_auth_for_uid(1000, 1000), PeerAuth::Verified);
		assert_eq!(peer_auth_for_uid(1000, 1001), PeerAuth::Rejected);
	}

	#[test]
	fn same_uid_peer_is_accepted_by_the_real_platform_credential_path() {
		let state = state_dir("peer");
		let socket = state.join("rpc.sock");
		let listener = bind_socket(&state, &socket).unwrap();
		let client = std::os::unix::net::UnixStream::connect(&socket).unwrap();
		let (server, _) = listener.accept().unwrap();
		assert_eq!(peer_auth_for_uid(current_uid(), peer_uid(server.as_raw_fd()).unwrap()), PeerAuth::Verified);
		drop(client);
		drop(server);
		drop(listener);
		fs::remove_dir_all(state).unwrap();
	}

	#[test]
	fn foreign_file_and_symlink_are_refused_without_deletion() {
		let state = state_dir("foreign");
		let socket = state.join("rpc.sock");
		fs::write(&socket, "do not remove").unwrap();
		assert!(matches!(bind_socket(&state, &socket), Err(SocketBootError::ExistingPathNotSocket)));
		assert_eq!(fs::read_to_string(&socket).unwrap(), "do not remove");
		fs::remove_file(&socket).unwrap();

		let target = state.join("target.sock");
		fs::write(&target, "target").unwrap();
		symlink(&target, &socket).unwrap();
		assert!(matches!(bind_socket(&state, &socket), Err(SocketBootError::SocketPathIsSymlink)));
		assert!(fs::symlink_metadata(&socket).unwrap().file_type().is_symlink());
		fs::remove_dir_all(state).unwrap();
	}

	#[test]
	fn active_same_uid_socket_is_never_replaced() {
		let state = state_dir("active");
		let socket = state.join("rpc.sock");
		let listener = bind_socket(&state, &socket).unwrap();
		assert!(matches!(bind_socket(&state, &socket), Err(SocketBootError::ActiveSocket)));
		assert!(fs::symlink_metadata(&socket).unwrap().file_type().is_socket());
		drop(listener);
		fs::remove_dir_all(state).unwrap();
	}

	#[test]
	fn stale_same_uid_socket_is_replaced_with_a_private_socket() {
		let state = state_dir("stale");
		let socket = state.join("rpc.sock");
		let stale = std::os::unix::net::UnixListener::bind(&socket).unwrap();
		drop(stale);
		let listener = bind_socket(&state, &socket).unwrap();
		assert_eq!(fs::metadata(&state).unwrap().permissions().mode() & 0o777, 0o700);
		assert_eq!(fs::metadata(&socket).unwrap().permissions().mode() & 0o777, 0o600);
		drop(listener);
		fs::remove_dir_all(state).unwrap();
	}

	#[cfg(target_os = "linux")]
	#[test]
	fn linux_second_os_user_is_rejected_by_real_so_peercred_when_enabled() {
		if std::env::var_os("WAY_CORE_CROSS_UID_TEST").is_none() {
			return;
		}
		assert_eq!(unsafe { libc::geteuid() }, 0, "cross-UID peer test must run as root");
		let state = state_dir("cross-uid");
		fs::set_permissions(&state, fs::Permissions::from_mode(0o777)).unwrap();
		let socket = state.join("rpc.sock");
		let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
		fs::set_permissions(&socket, fs::Permissions::from_mode(0o666)).unwrap();

		let child = unsafe { libc::fork() };
		assert!(child >= 0, "fork failed: {}", std::io::Error::last_os_error());
		if child == 0 {
			unsafe {
				if libc::setgid(65_534) != 0 || libc::setuid(65_534) != 0 {
					libc::_exit(2);
				}
				let status = if std::os::unix::net::UnixStream::connect(&socket).is_ok() { 0 } else { 3 };
				libc::_exit(status);
			}
		}
		let (server, _) = listener.accept().unwrap();
		let peer = peer_uid(server.as_raw_fd()).unwrap();
		assert_ne!(peer, current_uid());
		assert_eq!(peer_auth_for_uid(current_uid(), peer), PeerAuth::Rejected);
		let mut status = 0;
		assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);
		assert!(libc::WIFEXITED(status));
		assert_eq!(libc::WEXITSTATUS(status), 0);
		drop(server);
		drop(listener);
		fs::remove_dir_all(state).unwrap();
	}
}
