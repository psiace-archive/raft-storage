use raft::prelude::*;

#[derive(Debug, Clone, Default)]
pub struct StorageRaftState {
    pub hard_state: HardState,
    pub conf_state: ConfState,
    pub pending_conf_state: Option<ConfState>,
    pub pending_conf_state_start_index: Option<u64>,
}

impl StorageRaftState {
    pub fn new(hard_state: HardState, conf_state: ConfState) -> StorageRaftState {
        StorageRaftState {
            hard_state,
            conf_state,
            pending_conf_state: None,
            pending_conf_state_start_index: None,
        }
    }
    pub fn initialized(&self) -> bool {
        self.conf_state != ConfState::default()
    }
}

impl Into<RaftState> for StorageRaftState {
    fn into(self) -> RaftState {
        RaftState::new(self.hard_state, self.conf_state)
    }
}
