use std::{fmt, io};

/// Errors returned by the client. New variants can be added, so a `match` needs a wildcard arm.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
  /// The server answered with an error frame. `code` is a POSIX name such as ENOENT.
  Server { code: String, message: String },
  /// The peer broke the frame or message contract.
  Protocol(String),
  /// The socket failed.
  Io(io::Error),
  /// A call did not finish within `Options::call_timeout`. On a pipelining connection only this
  /// call fails and the late reply is dropped. Otherwise the connection was dropped, so a late
  /// reply cannot reach another call, and its open files and watches are gone. The code is
  /// `DEADLINE_EXCEEDED` and the `io::ErrorKind` is `TimedOut`.
  Timeout(String),
  /// A request is larger than the frame limits. Nothing was sent and the connection is intact.
  TooLarge(String),
  /// A directory helper hit its bound: `read_dir_all` found more entries than its limit, or
  /// `walk` queued more pending directories than its limit. The message names the limit.
  TooManyEntries(String),
  /// A write that covers more than one frame failed after `written` bytes were stored.
  /// `source` is the failure. `code` and the `io::ErrorKind` come from `source`.
  Partial { written: usize, source: Box<Error> },
  /// Notices were lost, so the end of an operation cannot be known. The server may still run it.
  /// Returned by `Operation::wait`.
  OperationStateUnknown,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
  /// Server error code, if this is a server error. A timeout has the code `DEADLINE_EXCEEDED`.
  /// A partial write has the code of its cause.
  pub fn code(&self) -> Option<&str> {
    match self {
      Error::Server { code, .. } => Some(code),
      Error::Timeout(_) => Some("DEADLINE_EXCEEDED"),
      Error::Partial { source, .. } => source.code(),
      _ => None,
    }
  }

  /// Copies the error. An I/O error keeps its kind and text.
  pub(crate) fn dup(&self) -> Error {
    match self {
      Error::Server { code, message } => Error::Server {
        code: code.clone(),
        message: message.clone(),
      },
      Error::Protocol(m) => Error::Protocol(m.clone()),
      Error::Io(e) => Error::Io(io::Error::new(e.kind(), e.to_string())),
      Error::Timeout(m) => Error::Timeout(m.clone()),
      Error::TooLarge(m) => Error::TooLarge(m.clone()),
      Error::TooManyEntries(m) => Error::TooManyEntries(m.clone()),
      Error::Partial { written, source } => Error::Partial {
        written: *written,
        source: Box::new(source.dup()),
      },
      Error::OperationStateUnknown => Error::OperationStateUnknown,
    }
  }

  pub(crate) fn server(code: &str, message: &str) -> Error {
    Error::Server {
      code: code.into(),
      message: message.into(),
    }
  }
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Error::Server { code, message } if message.is_empty() => write!(f, "{code}"),
      Error::Server { code, message } => write!(f, "{code}: {message}"),
      Error::Protocol(m) => write!(f, "protocol error: {m}"),
      Error::Io(e) => write!(f, "io error: {e}"),
      Error::Timeout(m) => write!(f, "DEADLINE_EXCEEDED: {m}"),
      Error::TooLarge(m) => write!(f, "request too large: {m}"),
      Error::TooManyEntries(m) => write!(f, "too many entries: {m}"),
      Error::Partial { written, source } => write!(f, "partial write of {written} bytes: {source}"),
      Error::OperationStateUnknown => write!(f, "operation state unknown, notices were lost"),
    }
  }
}

impl std::error::Error for Error {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    match self {
      Error::Io(e) => Some(e),
      Error::Partial { source, .. } => Some(source.as_ref()),
      _ => None,
    }
  }
}

impl From<io::Error> for Error {
  fn from(e: io::Error) -> Error {
    Error::Io(e)
  }
}

impl From<Error> for io::Error {
  fn from(e: Error) -> io::Error {
    let kind = match &e {
      Error::TooLarge(_) | Error::TooManyEntries(_) => io::ErrorKind::InvalidInput,
      e => e.code().map_or(io::ErrorKind::Other, kind_for_code),
    };
    match e {
      Error::Io(e) => e,
      e => io::Error::new(kind, e),
    }
  }
}

/// Maps a server error code to the closest `io::ErrorKind`. A code without a close kind maps to
/// `Other`. The code string stays available through `Error::code`.
pub(crate) fn kind_for_code(code: &str) -> io::ErrorKind {
  use io::ErrorKind as K;
  match code {
    "ENOENT" => K::NotFound,
    "EACCES" | "EPERM" => K::PermissionDenied,
    "EEXIST" => K::AlreadyExists,
    "EINVAL" => K::InvalidInput,
    "ENOTDIR" => K::NotADirectory,
    "EISDIR" => K::IsADirectory,
    "ENOTEMPTY" => K::DirectoryNotEmpty,
    "EROFS" => K::ReadOnlyFilesystem,
    "ENOSPC" => K::StorageFull,
    "EFBIG" => K::FileTooLarge,
    "EXDEV" => K::CrossesDevices,
    "EMLINK" => K::TooManyLinks,
    "EBUSY" => K::ResourceBusy,
    "ENAMETOOLONG" => K::InvalidFilename,
    "EDQUOT" => K::QuotaExceeded,
    "ESTALE" => K::StaleNetworkFileHandle,
    "EAGAIN" => K::WouldBlock,
    "EINTR" => K::Interrupted,
    "ETIMEDOUT" | "DEADLINE_EXCEEDED" => K::TimedOut,
    "ENOTSUP" | "ENOSYS" | "UNSUPPORTED_OPERATION" | "UNSUPPORTED_VERSION" => K::Unsupported,
    "SESSION_CLOSED" => K::ConnectionAborted,
    "FRAME_TOO_LARGE" => K::InvalidInput,
    "MALFORMED_FRAME" => K::InvalidData,
    "PERMISSION_DENIED_PEER" => K::PermissionDenied,
    "TOO_MANY_INFLIGHT" => K::WouldBlock,
    _ => K::Other,
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use io::ErrorKind as K;

  #[test]
  fn every_spec_code_maps() {
    let want = [
      ("ENOENT", K::NotFound),
      ("EACCES", K::PermissionDenied),
      ("EPERM", K::PermissionDenied),
      ("EEXIST", K::AlreadyExists),
      ("EINVAL", K::InvalidInput),
      ("ENOTDIR", K::NotADirectory),
      ("EISDIR", K::IsADirectory),
      ("ENOTEMPTY", K::DirectoryNotEmpty),
      ("EROFS", K::ReadOnlyFilesystem),
      ("ENOSPC", K::StorageFull),
      ("EFBIG", K::FileTooLarge),
      ("EXDEV", K::CrossesDevices),
      ("EMLINK", K::TooManyLinks),
      ("EBUSY", K::ResourceBusy),
      ("ENAMETOOLONG", K::InvalidFilename),
      ("EDQUOT", K::QuotaExceeded),
      ("ESTALE", K::StaleNetworkFileHandle),
      ("EAGAIN", K::WouldBlock),
      ("UNSUPPORTED_OPERATION", K::Unsupported),
      ("UNSUPPORTED_VERSION", K::Unsupported),
      ("DEADLINE_EXCEEDED", K::TimedOut),
      ("ETIMEDOUT", K::TimedOut),
      ("EINTR", K::Interrupted),
      ("ENOTSUP", K::Unsupported),
      ("ENOSYS", K::Unsupported),
      ("SESSION_CLOSED", K::ConnectionAborted),
      ("FRAME_TOO_LARGE", K::InvalidInput),
      ("MALFORMED_FRAME", K::InvalidData),
      ("PERMISSION_DENIED_PEER", K::PermissionDenied),
      ("TOO_MANY_INFLIGHT", K::WouldBlock),
      ("EIO", K::Other),
      ("EBADF", K::Other),
      ("ELOOP", K::Other),
      ("EMFILE", K::Other),
      ("EUNKNOWN", K::Other),
    ];
    for (code, kind) in want {
      let e: io::Error = Error::server(code, "m").into();
      assert_eq!(e.kind(), kind, "{code}");
    }
  }

  #[test]
  fn code_survives_conversion() {
    let e: io::Error = Error::server("ENOSPC", "disk full").into();
    let inner = e.get_ref().unwrap().downcast_ref::<Error>().unwrap();
    assert_eq!(inner.code(), Some("ENOSPC"));
  }

  #[test]
  fn timeout_too_large_and_partial_convert() {
    let e: io::Error = Error::Timeout("t".into()).into();
    assert_eq!(e.kind(), K::TimedOut);
    assert_eq!(Error::Timeout("t".into()).code(), Some("DEADLINE_EXCEEDED"));
    let e: io::Error = Error::TooLarge("x".into()).into();
    assert_eq!(e.kind(), K::InvalidInput);
    let p = Error::Partial {
      written: 5,
      source: Box::new(Error::server("ENOSPC", "full")),
    };
    assert_eq!(p.code(), Some("ENOSPC"));
    let e: io::Error = p.dup().into();
    assert_eq!(e.kind(), K::StorageFull);
  }

  #[test]
  fn non_server_errors_keep_other_kind() {
    let e: io::Error = Error::Protocol("x".into()).into();
    assert_eq!(e.kind(), K::Other);
  }
}
