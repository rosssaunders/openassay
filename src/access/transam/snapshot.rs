use crate::tcop::engine::{EngineStateSnapshot, snapshot_state};

#[derive(Debug, Clone)]
pub struct TransactionSnapshots {
    base: EngineStateSnapshot,
    working: EngineStateSnapshot,
}

impl TransactionSnapshots {
    pub fn capture() -> Self {
        let base = snapshot_state();
        let working = base.clone();
        Self { base, working }
    }

    pub fn base(&self) -> &EngineStateSnapshot {
        &self.base
    }

    pub fn working(&self) -> &EngineStateSnapshot {
        &self.working
    }

    pub fn set_working(&mut self, snapshot: EngineStateSnapshot) {
        self.working = snapshot;
    }
}
