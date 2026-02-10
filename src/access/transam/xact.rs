use crate::tcop::engine::EngineStateSnapshot;

use super::snapshot::TransactionSnapshots;
use super::visibility::VisibilityMode;

#[derive(Debug, Clone)]
struct SavepointFrame {
    name: String,
    snapshot: EngineStateSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Idle,
    InTransaction,
    Failed,
}

#[derive(Debug, Clone)]
pub struct TransactionContext {
    state: TransactionState,
    snapshots: Option<TransactionSnapshots>,
    savepoints: Vec<SavepointFrame>,
}

impl Default for TransactionContext {
    fn default() -> Self {
        Self {
            state: TransactionState::Idle,
            snapshots: None,
            savepoints: Vec::new(),
        }
    }
}

impl TransactionContext {
    pub fn begin(&mut self) {
        if self.state != TransactionState::Idle {
            return;
        }
        self.state = TransactionState::InTransaction;
        self.snapshots = Some(TransactionSnapshots::capture());
        self.savepoints.clear();
    }

    pub fn commit(&mut self) -> Option<EngineStateSnapshot> {
        match self.state {
            TransactionState::Idle => None,
            TransactionState::InTransaction => {
                let committed = self
                    .snapshots
                    .as_ref()
                    .map(|snapshots| snapshots.working().clone());
                self.clear();
                committed
            }
            TransactionState::Failed => {
                let restored = self
                    .snapshots
                    .as_ref()
                    .map(|snapshots| snapshots.base().clone());
                self.clear();
                restored
            }
        }
    }

    pub fn rollback(&mut self) -> Option<EngineStateSnapshot> {
        if self.state == TransactionState::Idle {
            return None;
        }
        let restored = self
            .snapshots
            .as_ref()
            .map(|snapshots| snapshots.base().clone());
        self.clear();
        restored
    }

    pub fn savepoint(&mut self, name: String) -> Result<(), String> {
        match self.state {
            TransactionState::InTransaction => {}
            TransactionState::Idle => {
                return Err("SAVEPOINT can only be used in transaction blocks".to_string())
            }
            TransactionState::Failed => {
                return Err(
                    "current transaction is aborted, commands ignored until end of transaction block"
                        .to_string(),
                )
            }
        }
        let Some(snapshots) = &self.snapshots else {
            return Err("transaction state missing working snapshot".to_string());
        };
        self.savepoints.push(SavepointFrame {
            name: name.to_ascii_lowercase(),
            snapshot: snapshots.working().clone(),
        });
        Ok(())
    }

    pub fn release_savepoint(&mut self, name: String) -> Result<(), String> {
        match self.state {
            TransactionState::InTransaction => {}
            TransactionState::Idle => {
                return Err("RELEASE SAVEPOINT can only be used in transaction blocks".to_string())
            }
            TransactionState::Failed => {
                return Err(
                    "current transaction is aborted, commands ignored until end of transaction block"
                        .to_string(),
                )
            }
        }
        let normalized = name.to_ascii_lowercase();
        let Some(idx) = self
            .savepoints
            .iter()
            .rposition(|frame| frame.name == normalized)
        else {
            return Err(format!("savepoint \"{}\" does not exist", name));
        };
        self.savepoints.truncate(idx);
        Ok(())
    }

    pub fn rollback_to_savepoint(
        &mut self,
        name: String,
    ) -> Result<Option<EngineStateSnapshot>, String> {
        if self.state == TransactionState::Idle {
            return Err("ROLLBACK TO SAVEPOINT can only be used in transaction blocks".to_string());
        }
        let normalized = name.to_ascii_lowercase();
        let Some(idx) = self
            .savepoints
            .iter()
            .rposition(|frame| frame.name == normalized)
        else {
            return Err(format!("savepoint \"{}\" does not exist", name));
        };
        let snapshot = self.savepoints[idx].snapshot.clone();
        if let Some(snapshots) = self.snapshots.as_mut() {
            snapshots.set_working(snapshot.clone());
        }
        self.savepoints.truncate(idx + 1);
        self.state = TransactionState::InTransaction;
        Ok(Some(snapshot))
    }

    pub fn set_working_snapshot(&mut self, snapshot: EngineStateSnapshot) {
        if let Some(snapshots) = self.snapshots.as_mut() {
            snapshots.set_working(snapshot);
        }
    }

    pub fn working_snapshot(&self) -> Option<&EngineStateSnapshot> {
        self.snapshots.as_ref().map(|snapshots| snapshots.working())
    }

    pub fn base_snapshot(&self) -> Option<&EngineStateSnapshot> {
        self.snapshots.as_ref().map(|snapshots| snapshots.base())
    }

    pub fn mark_failed(&mut self) {
        if self.state == TransactionState::InTransaction {
            self.state = TransactionState::Failed;
        }
    }

    pub fn clear_failed(&mut self) {
        if self.state == TransactionState::Failed {
            self.state = TransactionState::InTransaction;
        }
    }

    pub fn in_explicit_block(&self) -> bool {
        self.state != TransactionState::Idle
    }

    pub fn is_aborted(&self) -> bool {
        self.state == TransactionState::Failed
    }

    pub fn visibility_mode(&self) -> VisibilityMode {
        if self.state != TransactionState::Idle {
            VisibilityMode::TransactionLocal
        } else {
            VisibilityMode::Global
        }
    }

    fn clear(&mut self) {
        self.state = TransactionState::Idle;
        self.snapshots = None;
        self.savepoints.clear();
    }
}
