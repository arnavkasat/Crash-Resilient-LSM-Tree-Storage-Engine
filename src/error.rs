use std::fmt;
use std::io;

/// Errors returned by the storage engine.
#[derive(Debug)]
pub enum Error {
    /// An underlying filesystem operation failed.
    Io(io::Error),
    /// On-disk state failed a checksum or structural check.
    Corruption(String),
    /// The caller passed something the engine cannot accept.
    InvalidArgument(String),
    /// A background job failed; the database is read-only until reopened.
    Background(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Corruption(m) => write!(f, "corruption: {m}"),
            Error::InvalidArgument(m) => write!(f, "invalid argument: {m}"),
            Error::Background(m) => write!(f, "background error: {m}"),
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
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

pub(crate) fn corrupt<T>(msg: impl Into<String>) -> Result<T> {
    Err(Error::Corruption(msg.into()))
}
