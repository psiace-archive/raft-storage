use raft::prelude::*;
use raft::Result;

pub mod mem_storage;
#[cfg(feature = "sled-storage")]
pub mod sled_storage;
pub mod state;
pub mod utils;

pub type CustomStorageError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type Error = CustomStorageError;

pub trait RaftStorage: Storage {
    fn initialize_with_conf_state<T>(&self, conf_state: T)
    where
        ConfState: From<T>;

    fn set_hard_state(&mut self, hs: HardState);

    fn set_conf_state(&mut self, cs: ConfState);

    fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()>;

    fn hard_state(&self) -> HardState;

    fn commit_to(&mut self, index: u64) -> Result<()>;

    fn compact(&mut self, compact_index: u64) -> Result<()>;

    fn append(&mut self, ents: &[Entry]) -> Result<()>;
}
