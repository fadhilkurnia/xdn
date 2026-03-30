use rocksdb::{Options, TransactionDB, TransactionDBOptions, TransactionOptions, WriteOptions};
use rusqlite::{params, Connection};
use std::sync::Mutex;
use std::time::Duration;
use tikv_client::TransactionClient;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{mpsc, oneshot};
use tokio::task;

pub trait KeyValueStore {
    fn new(db_path: &str) -> Result<Self, String>
    where
        Self: Sized;
    fn set(&self, key: &str, value: &str) -> Result<(), String>;
    fn get(&self, key: &str) -> Result<Option<String>, String>;
    fn delete(&self, key: &str) -> Result<(), String>;
}

#[allow(dead_code)]
pub struct SqliteKeyValueStore {
    conn: Mutex<Connection>,
}

impl KeyValueStore for SqliteKeyValueStore {
    fn new(db_path: &str) -> Result<Self, String> {
        let conn = Connection::open(db_path).map_err(|e| e.to_string())?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
            [],
        )
        .map_err(|e| e.to_string())?;
        Ok(SqliteKeyValueStore {
            conn: Mutex::new(conn),
        })
    }

    fn set(&self, key: &str, value: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        conn.execute(
            "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?1, ?2)",
            params![key, value],
        )
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<String>, String> {
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        let mut stmt = conn
            .prepare("SELECT value FROM kv_store WHERE key = ?1")
            .map_err(|e| e.to_string())?;
        let mut rows = stmt.query(params![key]).map_err(|e| e.to_string())?;

        if let Some(row) = rows.next().map_err(|e| e.to_string())? {
            Ok(Some(row.get(0).map_err(|e| e.to_string())?))
        } else {
            Ok(None)
        }
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        conn.execute("DELETE FROM kv_store WHERE key = ?1", params![key])
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

pub struct RocksDbKeyValueStore {
    conn: TransactionDB,
}

impl KeyValueStore for RocksDbKeyValueStore {
    fn new(db_path: &str) -> Result<Self, String> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_use_fsync(true);
        let txn_opts = TransactionDBOptions::default();
        let db = TransactionDB::open(&opts, &txn_opts, db_path).map_err(|e| e.to_string())?;
        Ok(RocksDbKeyValueStore { conn: db })
    }

    fn set(&self, key: &str, value: &str) -> Result<(), String> {
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(true);
        let txn = self
            .conn
            .transaction_opt(&write_opts, &TransactionOptions::default());
        txn.put(key, value.as_bytes()).map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())
    }

    fn get(&self, key: &str) -> Result<Option<String>, String> {
        let txn = self.conn.transaction();
        let value = txn.get(key).map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
        match value {
            Some(value) => {
                let as_string = String::from_utf8(value).map_err(|e| e.to_string())?;
                Ok(Some(as_string))
            }
            None => Ok(None),
        }
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(true);
        let txn = self
            .conn
            .transaction_opt(&write_opts, &TransactionOptions::default());
        txn.delete(key).map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())
    }
}

// Write operations are funneled through a single background writer task via
// an mpsc channel. This serializes TiKV transactions so only one is in-flight
// at a time, eliminating optimistic 2PC write conflicts on hot keys.
enum WriteOp {
    Set {
        key: String,
        value: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Delete {
        key: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
}

pub struct TiKeyValueStore {
    client: TransactionClient,
    runtime: Runtime,
    write_tx: mpsc::UnboundedSender<WriteOp>,
}

const TIKV_MAX_READ_RETRIES: u32 = 20;
const TIKV_RETRY_BASE_MS: u64 = 1;

impl KeyValueStore for TiKeyValueStore {
    fn new(pd_endpoints: &str) -> Result<Self, String> {
        let endpoints: Vec<String> = pd_endpoints
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if endpoints.is_empty() {
            return Err(
                "No PD endpoints provided for TiKV (expected comma-separated host:port)"
                    .to_string(),
            );
        }

        let runtime = Runtime::new().map_err(|e| e.to_string())?;
        let client = runtime
            .block_on(TransactionClient::new(endpoints.clone()))
            .map_err(|e| e.to_string())?;

        // Spawn the background writer task that serializes all write transactions
        let (write_tx, write_rx) = mpsc::unbounded_channel::<WriteOp>();
        let writer_client = runtime
            .block_on(TransactionClient::new(endpoints))
            .map_err(|e| e.to_string())?;
        runtime.spawn(Self::writer_loop(writer_client, write_rx));

        Ok(TiKeyValueStore {
            client,
            runtime,
            write_tx,
        })
    }

    fn set(&self, key: &str, value: &str) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.write_tx
            .send(WriteOp::Set {
                key: key.to_owned(),
                value: value.to_owned(),
                reply: tx,
            })
            .map_err(|_| "writer task gone".to_string())?;
        self.wait_reply(rx)
    }

    fn get(&self, key: &str) -> Result<Option<String>, String> {
        let key = key.to_owned();
        let result: Option<Vec<u8>> = self.retry_read(move |client| {
            let k = key.clone();
            Box::pin(async move {
                let mut txn = client.begin_optimistic().await?;
                let result = txn.get(k).await?;
                txn.commit().await?;
                Ok(result)
            })
        })?;

        match result {
            Some(val) => {
                let value = String::from_utf8(val).map_err(|e| e.to_string())?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.write_tx
            .send(WriteOp::Delete {
                key: key.to_owned(),
                reply: tx,
            })
            .map_err(|_| "writer task gone".to_string())?;
        self.wait_reply(rx)
    }
}

impl TiKeyValueStore {
    /// Background writer task: drains all pending ops and executes them in a
    /// single batched transaction. Serialization eliminates optimistic 2PC
    /// write conflicts; batching amortizes per-transaction overhead.
    async fn writer_loop(
        client: TransactionClient,
        mut rx: mpsc::UnboundedReceiver<WriteOp>,
    ) {
        while let Some(first) = rx.recv().await {
            let mut batch = vec![first];
            while let Ok(op) = rx.try_recv() {
                batch.push(op);
            }
            let result = Self::execute_batch(&client, &batch).await;
            for op in batch {
                let reply = match op {
                    WriteOp::Set { reply, .. } | WriteOp::Delete { reply, .. } => reply,
                };
                let _ = reply.send(result.clone());
            }
        }
    }

    async fn execute_batch(
        client: &TransactionClient,
        ops: &[WriteOp],
    ) -> Result<(), String> {
        let mut txn = client.begin_optimistic().await.map_err(|e| e.to_string())?;
        for op in ops {
            match op {
                WriteOp::Set { key, value, .. } => {
                    txn.put(key.clone(), value.clone())
                        .await
                        .map_err(|e| e.to_string())?;
                }
                WriteOp::Delete { key, .. } => {
                    txn.delete(key.clone())
                        .await
                        .map_err(|e| e.to_string())?;
                }
            }
        }
        txn.commit().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    fn wait_reply(&self, rx: oneshot::Receiver<Result<(), String>>) -> Result<(), String> {
        match Handle::try_current() {
            Ok(handle) => task::block_in_place(|| {
                handle
                    .block_on(rx)
                    .map_err(|_| "writer dropped reply".to_string())?
            }),
            Err(_) => self
                .runtime
                .block_on(rx)
                .map_err(|_| "writer dropped reply".to_string())?,
        }
    }

    fn run<F, T>(&self, fut: F) -> Result<T, String>
    where
        F: std::future::Future<Output = Result<T, tikv_client::Error>>,
    {
        match Handle::try_current() {
            Ok(handle) => task::block_in_place(|| handle.block_on(fut)).map_err(|e| e.to_string()),
            Err(_) => self.runtime.block_on(fut).map_err(|e| e.to_string()),
        }
    }

    fn is_retryable(e: &tikv_client::Error) -> bool {
        let msg = e.to_string();
        msg.contains("WriteConflict")
            || msg.contains("write conflict")
            || msg.contains("KeyIsLocked")
            || msg.contains("key is locked")
            || msg.contains("TxnLockNotFound")
    }

    fn retry_read<F, T>(&self, make_txn: F) -> Result<T, String>
    where
        F: Fn(&TransactionClient) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T, tikv_client::Error>> + Send + '_>,
        >,
    {
        let do_retry = async {
            for attempt in 0..TIKV_MAX_READ_RETRIES {
                match make_txn(&self.client).await {
                    Ok(v) => return Ok(v),
                    Err(e) => {
                        if !Self::is_retryable(&e) || attempt + 1 == TIKV_MAX_READ_RETRIES {
                            return Err(e);
                        }
                        let backoff =
                            Duration::from_millis(TIKV_RETRY_BASE_MS << attempt.min(6));
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
            unreachable!()
        };
        match Handle::try_current() {
            Ok(handle) => {
                task::block_in_place(|| handle.block_on(do_retry)).map_err(|e| e.to_string())
            }
            Err(_) => self.runtime.block_on(do_retry).map_err(|e| e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{KeyValueStore, TiKeyValueStore};

    #[test]
    fn tikv_txn_round_trip_when_env_set() {
        let pd = match std::env::var("TIKV_PD_ADDR") {
            Ok(v) => v,
            Err(_) => {
                eprintln!(
                    "skipping TiKV round-trip test; set TIKV_PD_ADDR to a reachable PD (host:port)"
                );
                return;
            }
        };

        let store = <TiKeyValueStore as KeyValueStore>::new(&pd).expect("connect to TiKV via PD");
        let key = "tikv_test_key";
        let value = "tikv_test_value";

        store.set(key, value).expect("set value");
        let fetched = store.get(key).expect("get value");
        assert_eq!(fetched.as_deref(), Some(value));
        store.delete(key).expect("delete value");
    }
}
