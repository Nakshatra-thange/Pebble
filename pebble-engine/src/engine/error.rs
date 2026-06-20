use std::fmt;
use std::io;

#[derive(Debug)]
pub enum EngineError {
    Io(io::Error),
    Corruption(String),
    KeyNotFound,
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineError::Io(e) => write!(f, "IO error: {}", e),
            EngineError::Corruption(msg) => write!(f, "Corruption: {}", msg),
            EngineError::KeyNotFound => write!(f, "Key not found"),
        }
    }
}

impl From<io::Error> for EngineError {
    fn from(e: io::Error) -> Self {
        EngineError::Io(e)
    }
}