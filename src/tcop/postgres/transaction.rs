#[allow(clippy::wildcard_imports)]
use super::*;

impl PostgresSession {
    pub(super) fn apply_transaction_command(
        &mut self,
        command: TransactionCommand,
    ) -> Result<(), SessionError> {
        match command {
            TransactionCommand::Begin => {
                self.tx_state.begin();
                Ok(())
            }
            TransactionCommand::Commit => {
                if let Some(snapshot) = self.tx_state.commit() {
                    restore_state(snapshot);
                }
                Ok(())
            }
            TransactionCommand::Rollback => {
                if let Some(snapshot) = self.tx_state.rollback() {
                    restore_state(snapshot);
                }
                Ok(())
            }
            TransactionCommand::Savepoint(name) => {
                self.tx_state
                    .savepoint(name)
                    .map_err(|message| SessionError { message })?;
                Ok(())
            }
            TransactionCommand::ReleaseSavepoint(name) => {
                self.tx_state
                    .release_savepoint(name)
                    .map_err(|message| SessionError { message })?;
                Ok(())
            }
            TransactionCommand::RollbackToSavepoint(name) => {
                self.tx_state
                    .rollback_to_savepoint(name)
                    .map_err(|message| SessionError { message })?;
                Ok(())
            }
        }
    }

    pub(super) fn start_xact_command(&mut self) {
        self.xact_started = true;
    }

    pub(super) fn finish_xact_command(&mut self) {
        self.xact_started = false;
    }

    pub(super) fn is_aborted_transaction_block(&self) -> bool {
        self.tx_state.is_aborted()
    }

    pub(super) fn ready_status(&self) -> ReadyForQueryStatus {
        if self.is_aborted_transaction_block() {
            ReadyForQueryStatus::FailedTransaction
        } else if self.tx_state.in_explicit_block() || self.xact_started {
            ReadyForQueryStatus::InTransaction
        } else {
            ReadyForQueryStatus::Idle
        }
    }
    pub(super) fn handle_error_recovery(&mut self) {
        if self.doing_extended_query_message {
            self.ignore_till_sync = true;
        } else {
            self.send_ready_for_query = true;
        }

        self.xact_started = false;
        self.copy_in_state = None;

        // Auto-rollback for implicit transactions to prevent cascade failures
        if !self.tx_state.in_explicit_block() {
            // If we're not in an explicit BEGIN/COMMIT block, rollback and restore state
            if let Some(snapshot) = self.tx_state.rollback() {
                restore_state(snapshot);
            }
        } else {
            // In explicit transaction, mark as failed but don't rollback
            self.tx_state.mark_failed();
        }
    }
}
