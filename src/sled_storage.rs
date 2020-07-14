use std::path::Path;

use prost::Message;
use raft::prelude::*;
use raft::{Error as RaftError, RaftState, Storage, StorageError};
use sled::{open, Db};
use slog::{info, Logger};

use crate::error::CustomStorageError;
use crate::state::StorageRaftState;
use crate::utils::*;

pub struct SledStorage {
    state: StorageRaftState,
    snapshot_metadata: SnapshotMetadata,
    db: Db,
    logger: Logger,
    last_idx: u64,
}

impl SledStorage {
    pub fn new(path: &str, cfg: raft::Config, logger: Logger) -> Result<Self, CustomStorageError> {
        info!(logger, "Init sled storage db at: {}", path);
        let db = open(Path::new(path))?;
        let mut hard_state: HardState = get(&db, b"hard_state")?;
        let mut conf_state: ConfState = get(&db, b"conf_state")?;
        let last_idx_be = db.get(b"last_idx").unwrap_or(None);

        if !conf_state.voters.contains(&cfg.id) {
            conf_state.voters = vec![cfg.id];
        }
        let last_idx: u64 = if let Some(libe) = last_idx_be {
            read_be_u64(&mut libe.as_ref())
        } else {
            hard_state.commit
        };

        if hard_state.commit != last_idx {
            hard_state.commit = last_idx;
        }

        if !db.contains_key(b"hard_state")? {
            insert(&db, b"hard_state", hard_state.clone())?;
        }
        if !db.contains_key(b"conf_state")? {
            insert(&db, b"conf_state", conf_state.clone())?;
        }

        let state = StorageRaftState::new(hard_state, conf_state);
        info!(logger, "Initial HardState = {:?}", state.hard_state);
        info!(logger, "Initial ConfState = {:?}", state.conf_state);
        info!(logger, "Last IDX = {:?}", last_idx);
        info!(logger, "Initial DB = {:?}", db);
        Ok(Self {
            snapshot_metadata: SnapshotMetadata::default(),
            state,
            db,
            logger,
            last_idx,
        })
    }

    pub fn append(&mut self, entries: &[Entry]) -> Result<(), CustomStorageError> {
        if entries.is_empty() {
            return Ok(());
        }
        let entry_tree = self.db.open_tree("entries")?;

        for (i, e) in entries.into_iter().enumerate() {
            if e.data.is_empty() || e.context.is_empty() {
                continue;
            }
            let idx = e.index.to_be_bytes();
            let b = encode(e.clone())?;
            entry_tree.insert(idx, &b[..])?;
            self.last_idx = i as u64;
        }
        let last_idx_be = self.last_idx.to_be_bytes();
        self.db.insert(b"last_idx", &last_idx_be)?;
        Ok(())
    }

    pub fn set_conf_state(&mut self, conf: ConfState) {
        self.state.conf_state = conf;
    }

    pub fn commit(&mut self) -> Result<(), CustomStorageError> {
        info!(
            self.logger,
            "Commit HardState = {:?}", self.state.hard_state
        );
        info!(
            self.logger,
            "Commit ConfState = {:?}", self.state.conf_state
        );

        insert(&self.db, b"hard_state", self.state.hard_state.clone())?;
        insert(&self.db, b"conf_state", self.state.conf_state.clone())?;
        let idx = self.last_idx.to_be_bytes();
        self.db.insert(b"last_idx", &idx[..])?;
        let flush = self.db.flush()?;
        info!(self.logger, "Flushed: {} bytes ", flush);
        Ok(())
    }

    pub fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> Result<(), CustomStorageError> {
        let mut meta: SnapshotMetadata = snapshot.take_metadata();
        let term = meta.term;
        let index = meta.index;

        if self.first_index()? > index {
            return Err(Box::new(RaftError::Store(StorageError::SnapshotOutOfDate)));
        }

        self.snapshot_metadata = meta.clone();
        self.state.hard_state.term = term;
        self.state.hard_state.commit = index;
        self.state.conf_state = meta.take_conf_state();

        Ok(())
    }
}

impl Storage for SledStorage {
    fn initial_state(&self) -> Result<RaftState, raft::Error> {
        Ok(self.state.clone().into())
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> Result<Vec<Entry>, raft::Error> {
        if high > self.last_idx {
            return Ok(vec![]);
        }
        let max: u64 = max_size.into().unwrap_or(high - low);
        let lower = low.to_be_bytes();
        let upper = max.to_be_bytes();
        let tree = self.db.open_tree(b"entries").unwrap();
        let mut results = vec![];
        for i in tree.range(lower..upper) {
            match i {
                Ok((_, v)) => {
                    let dec: Entry = decode(v.as_ref()).unwrap();
                    results.push(dec);
                }
                Err(e) => panic!(e),
            }
        }
        Ok(results.into_iter().take(max as usize).collect())
    }

    fn term(&self, idx: u64) -> Result<u64, raft::Error> {
        if idx == self.snapshot_metadata.index {
            return Ok(self.snapshot_metadata.term);
        }
        let idx_bytes = idx.to_be_bytes();
        let tree = self.db.open_tree("entries").unwrap();
        let term = if let Ok(Some(e)) = tree.get(idx_bytes) {
            let msg = Entry::decode(e.as_ref()).unwrap();
            msg.term
        } else {
            1
        };
        info!(self.logger, "Term = {}, Idx = {}", term, idx);

        Ok(term)
    }

    fn first_index(&self) -> Result<u64, raft::Error> {
        Ok(1)
    }

    fn last_index(&self) -> Result<u64, raft::Error> {
        Ok(self.last_idx)
    }

    fn snapshot(&self, _request_index: u64) -> Result<Snapshot, raft::Error> {
        let mut snapshot = Snapshot::default();
        let applied_idx = self.state.hard_state.commit;
        let term = self.state.hard_state.term;
        let meta = snapshot.mut_metadata();
        meta.index = applied_idx;
        meta.term = term;

        meta.set_conf_state(self.state.conf_state.clone());
        Ok(snapshot)
    }
}

pub fn create_sled_storage(path: &str, cfg: raft::Config, logger: Logger) -> SledStorage {
    SledStorage::new(path, cfg, logger).unwrap()
}
