use std::{fmt, io};

/// Errors returned by the client.
#[derive(Debug)]
pub enum Error {
  /// The server answered with an error frame. `code` is a POSIX name such as ENOENT.
  Server { code: String, message: String },
  /// The peer broke the frame or message contract.
  Protocol(String),
  /// The socket failed.
  Io(io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
  /// Server error code, if this is a server error.
  pub fn code(&self) -> Option<&str> {
    match self {
      Error::Server { code, .. } => Some(code),
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
    }
  }
}

impl std::error::Error for Error {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    match self {
      Error::Io(e) => Some(e),
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
    let kind = match e.code() {
      Some("ENOENT") => io::ErrorKind::NotFound,
      Some("EACCES") | Some("EPERM") => io::ErrorKind::PermissionDenied,
      Some("EEXIST") => io::ErrorKind::AlreadyExists,
      Some("EINVAL") => io::ErrorKind::InvalidInput,
      _ => io::ErrorKind::Other,
    };
    match e {
      Error::Io(e) => e,
      e => io::Error::new(kind, e),
    }
  }
}
