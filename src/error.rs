#[derive(Debug)]
pub enum Error {
    WrongResults,
    MissingResults,
    WrongMessageType,
    MethodUnimplemented(&'static str),
    RemoteError(String),
    TransportError(String),
    InternalError(String),
    IO(std::io::Error),
}

use std::fmt;
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self)
    }
}

impl std::error::Error for Error {}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(e: std::sync::PoisonError<T>) -> Self {
        Error::InternalError(format!("sync error: {:#?}", e))
    }
}

impl From<std::sync::mpsc::RecvError> for Error {
    fn from(e: std::sync::mpsc::RecvError) -> Self {
        Error::InternalError(format!("receive error: {:#?}", e))
    }
}

impl<T> From<std::sync::mpsc::SendError<T>> for Error {
    fn from(e: std::sync::mpsc::SendError<T>) -> Self {
        Error::InternalError(format!("send error: {:#?}", e))
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IO(e)
    }
}
