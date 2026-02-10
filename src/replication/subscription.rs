use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};
use std::thread;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio_postgres::NoTls;

use crate::replication::apply::ApplyWorker;
use crate::replication::client::{ConnectionConfig, ReplicationClient, ReplicationMessage};
use crate::replication::initial_sync::copy_tables;
use crate::replication::pgoutput::decode_stream;
use crate::replication::schema_sync::{ensure_local_table, fetch_publication_schema};
use crate::replication::slot::slot_exists_error;
use crate::replication::{Lsn, ReplicationError};

#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    pub name: String,
    pub conninfo: String,
    pub publication: String,
    pub slot_name: String,
    pub copy_data: bool,
}

struct SubscriptionHandle {
    stop: Option<oneshot::Sender<()>>,
}

static SUBSCRIPTIONS: OnceLock<RwLock<HashMap<String, SubscriptionHandle>>> = OnceLock::new();

fn subscriptions() -> &'static RwLock<HashMap<String, SubscriptionHandle>> {
    SUBSCRIPTIONS.get_or_init(|| RwLock::new(HashMap::new()))
}

pub fn create_subscription(config: SubscriptionConfig) -> Result<(), ReplicationError> {
    ConnectionConfig::parse(&config.conninfo)?;
    let mut registry = subscriptions()
        .write()
        .map_err(|_| ReplicationError {
            message: "subscription registry lock poisoned".to_string(),
        })?;
    if registry.contains_key(&config.name) {
        return Err(ReplicationError {
            message: format!("subscription \"{}\" already exists", config.name),
        });
    }
    let (stop_tx, stop_rx) = oneshot::channel();
    let thread_name = format!("postrust-subscription-{}", config.name);
    let config_clone = config.clone();
    thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            match runtime {
                Ok(runtime) => {
                    runtime.block_on(run_subscription(config_clone, stop_rx));
                }
                Err(err) => {
                    eprintln!("replication runtime error: {}", err);
                }
            }
        })
        .map_err(|err| ReplicationError {
            message: err.to_string(),
        })?;
    registry.insert(
        config.name.clone(),
        SubscriptionHandle {
            stop: Some(stop_tx),
        },
    );
    Ok(())
}

pub fn drop_subscription(name: &str) -> Result<(), ReplicationError> {
    let mut registry = subscriptions()
        .write()
        .map_err(|_| ReplicationError {
            message: "subscription registry lock poisoned".to_string(),
        })?;
    let Some(handle) = registry.remove(name) else {
        return Err(ReplicationError {
            message: format!("subscription \"{}\" does not exist", name),
        });
    };
    if let Some(stop) = handle.stop {
        let _ = stop.send(());
    }
    Ok(())
}

async fn run_subscription(config: SubscriptionConfig, mut stop: oneshot::Receiver<()>) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let attempt = tokio::select! {
            _ = &mut stop => return,
            result = run_subscription_once(&config) => result,
        };
        match attempt {
            Ok(()) => return,
            Err(err) => {
                eprintln!("replication subscription {} error: {}", config.name, err.message);
                let delay = backoff;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {},
                    _ = &mut stop => return,
                }
            }
        }
    }
}

async fn run_subscription_once(config: &SubscriptionConfig) -> Result<(), ReplicationError> {
    let mut client = ReplicationClient::connect(&config.conninfo).await?;
    let system = client.identify_system().await?;

    let connection_config = ConnectionConfig::parse(&config.conninfo)?;
    let standard_conninfo = connection_config.conninfo_string(false);
    let (standard_client, connection) = tokio_postgres::connect(&standard_conninfo, NoTls).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("replication schema connection error: {}", err);
        }
    });

    let start_lsn = match client.create_replication_slot(&config.slot_name).await {
        Ok(slot) => slot.consistent_point,
        Err(err) if slot_exists_error(&err) => {
            fetch_slot_lsn(&standard_client, &config.slot_name).await?.unwrap_or(system.xlog_pos)
        }
        Err(err) => return Err(err),
    };

    let tables = fetch_publication_schema(&standard_client, &config.publication).await?;
    let mut table_oids = Vec::new();
    for table in &tables {
        let local_oid = ensure_local_table(table)?;
        table_oids.push((table.relation_id, local_oid));
    }

    if config.copy_data {
        copy_tables(&standard_client, &tables, &table_oids).await?;
    }

    let mut applier = ApplyWorker::new();
    for (table, (_, oid)) in tables.iter().zip(table_oids.iter()) {
        applier.register_table_schema(table, *oid);
    }

    let mut stream = client
        .start_replication(&config.slot_name, start_lsn, &config.publication)
        .await?;
    let mut last_lsn = start_lsn;
    let mut status_interval = tokio::time::interval(Duration::from_secs(10));

    loop {
        tokio::select! {
            _ = status_interval.tick() => {
                stream.send_standby_status_update(last_lsn, false).await?;
            }
            message = stream.next_message() => {
                let Some(message) = message? else { return Ok(()); };
                match message {
                    ReplicationMessage::XLogData { wal_end, data, .. } => {
                        let messages = decode_stream(&data)?;
                        for message in messages {
                            if let Err(err) = applier.apply_message(&message) {
                                eprintln!("replication apply error: {}", err.message);
                            }
                        }
                        last_lsn = wal_end;
                    }
                    ReplicationMessage::PrimaryKeepalive { wal_end, reply_requested, .. } => {
                        if wal_end > last_lsn {
                            last_lsn = wal_end;
                        }
                        if reply_requested {
                            stream.send_standby_status_update(last_lsn, true).await?;
                        }
                    }
                }
            }
        }
    }
}

async fn fetch_slot_lsn(
    client: &tokio_postgres::Client,
    slot_name: &str,
) -> Result<Option<Lsn>, ReplicationError> {
    let rows = client
        .query(
            "SELECT confirmed_flush_lsn::text, restart_lsn::text \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let confirmed: Option<String> = row.get(0);
    let restart: Option<String> = row.get(1);
    let lsn = confirmed
        .as_ref()
        .or(restart.as_ref())
        .map(|raw| Lsn::parse(raw))
        .transpose()?;
    Ok(lsn)
}
