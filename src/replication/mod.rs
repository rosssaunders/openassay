pub mod apply;
pub mod client;
pub mod initial_sync;
pub mod pgoutput;
pub mod schema_sync;
pub mod slot;
pub mod subscription;
pub mod tuple_decoder;

use std::fmt;
use std::sync::OnceLock;

use tokio::runtime::{Builder, Runtime};

#[derive(Debug, Clone)]
pub struct ReplicationError {
    pub message: String,
}

impl fmt::Display for ReplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ReplicationError {}

impl From<std::io::Error> for ReplicationError {
    fn from(err: std::io::Error) -> Self {
        Self {
            message: err.to_string(),
        }
    }
}

impl From<tokio_postgres::Error> for ReplicationError {
    fn from(err: tokio_postgres::Error) -> Self {
        Self {
            message: err.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub u64);

impl Lsn {
    pub fn parse(raw: &str) -> Result<Self, ReplicationError> {
        let trimmed = raw.trim();
        let (hi, lo) = trimmed.split_once('/').ok_or_else(|| ReplicationError {
            message: format!("invalid LSN format: {raw}"),
        })?;
        let hi = u64::from_str_radix(hi, 16).map_err(|_| ReplicationError {
            message: format!("invalid LSN format: {raw}"),
        })?;
        let lo = u64::from_str_radix(lo, 16).map_err(|_| ReplicationError {
            message: format!("invalid LSN format: {raw}"),
        })?;
        Ok(Lsn((hi << 32) | lo))
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }

    pub fn format(self) -> String {
        format!("{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

static REPLICATION_RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub(crate) fn replication_runtime() -> &'static Runtime {
    REPLICATION_RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .thread_name("postrust-repl")
            .build()
            .expect("replication runtime should build")
    })
}
