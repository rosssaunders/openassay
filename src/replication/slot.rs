use crate::replication::client::{ReplicationClient, ReplicationSlot};
use crate::replication::ReplicationError;

pub async fn ensure_replication_slot(
    client: &mut ReplicationClient,
    slot_name: &str,
) -> Result<ReplicationSlot, ReplicationError> {
    match client.create_replication_slot(slot_name).await {
        Ok(slot) => Ok(slot),
        Err(err) if slot_exists_error(&err) => Err(err),
        Err(err) => Err(err),
    }
}

pub fn slot_exists_error(error: &ReplicationError) -> bool {
    let message = error.message.to_ascii_lowercase();
    message.contains("already exists") || message.contains("duplicate")
}
