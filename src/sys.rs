//! Cross-platform type and trait aliases.

pub(crate) use self::sys::*;
use std::io;

/// Cross-platform alias to `AsRawFd` (Unix) or `AsRawSocket` (Windows).
///
/// Note: this is a slight hack around the rust type system. You should not
/// implement this trait directly, e.g. for a wrapper type, that will not work.
/// Instead you have to implement both `AsRawFd` and `AsRawSocket` separately.
pub trait AsRawSource {
  /// Cross-platform alias to `AsRawFd::as_raw_fd` (Unix) or `AsRawSocket` (Windows).
  fn as_raw_source(&self) -> RawSource;
}

/// Cross-platform alias to `FromRawFd` (Unix) or `FromRawSocket` (Windows).
pub trait FromRawSource {
  /// Cross-platform alias to `FromRawFd::from_raw_fd` (Unix) or `FromRawSocket::from_raw_socket` (Windows).
  unsafe fn from_raw_source(h: RawSource) -> Self;
}

#[cfg(unix)]
mod sys {
  use super::*;
  use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};

  pub(crate) type RawSource = RawFd;

  impl<T> AsRawSource for T where T: AsRawFd {
    fn as_raw_source(&self) -> RawSource {
      self.as_raw_fd()
    }
  }

  impl<T> FromRawSource for T where T: FromRawFd {
    unsafe fn from_raw_source(h: RawSource) -> Self {
      Self::from_raw_fd(h)
    }
  }

  /// Calls a libc function and results in `io::Result`.
  macro_rules! syscall {
      ($fn:ident $args:tt) => {{
          let res = unsafe { libc::$fn $args };
          if res == -1 {
              Err(std::io::Error::last_os_error())
          } else {
              Ok(res)
          }
      }};
  }

  pub fn set_non_blocking(fd: RawSource) -> io::Result<()> {
    // Put the file descriptor in non-blocking mode.
    let flags = syscall!(fcntl(fd, libc::F_GETFL))?;
    syscall!(fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK))?;
    Ok(())
  }
}

#[cfg(windows)]
mod sys {
  use super::*;
  use std::os::windows::io::{AsRawSocket, FromRawSocket, RawSocket};
  use winapi::um::winsock2;

  pub(crate) type RawSource = RawSocket;

  impl<T> AsRawSource for T where T: AsRawSocket {
    fn as_raw_source(&self) -> RawSource {
      self.as_raw_socket()
    }
  }

  impl<T> FromRawSource for T where T: FromRawSocket {
    unsafe fn from_raw_source(h: RawSource) -> Self {
      Self::from_raw_socket(h)
    }
  }

  pub fn set_non_blocking(sock: RawSource) -> io::Result<()> {
    unsafe {
      let mut nonblocking = true as libc::c_ulong;
      let res = winsock2::ioctlsocket(
        sock as winsock2::SOCKET,
        winsock2::FIONBIO,
        &mut nonblocking,
      );
      if res != 0 {
        return Err(io::Error::last_os_error());
      }
    }

    Ok(())
  }
}
