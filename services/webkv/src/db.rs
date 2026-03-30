use rocksdb::{Options, TransactionDB, TransactionDBOptions, TransactionOptions, WriteOptions};
use rusqlite::{params, Connection};
use std::sync::Mutex;
use std::time::Duration;
use tikv_client::TransactionClient;
use tokio::runtime::{Handle, Runtime};
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

pub struct TiKeyValueStore {
    client: TransactionClient,
    runtime: Runtime,
}

const TIKV_MAX_RETRIES: u32 = 20;
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
            .block_on(TransactionClient::new(endpoints))
            .map_err(|e| e.to_string())?;

        Ok(TiKeyValueStore { client, runtime })
    }

    fn set(&self, key: &str, value: &str) -> Result<(), String> {
        let key = key.to_owned();
        let value = value.to_owned();
        self.retry_write(move |client| {
            let k = key.clone();
            let v = value.clone();
            Box::pin(async move {
                let mut txn = client.begin_optimistic().await?;
                txn.put(k, v).await?;
                txn.commit().await?;
                Ok(())
            })
        })
    }

    fn get(&self, key: &str) -> Result<Option<String>, String> {
        let key = key.to_owned();
        let result = self.run(async {
            let mut txn = self.client.begin_optimistic().await?;
            let result = txn.get(key).await?;
            txn.commit().await?;
            Ok(result)
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
        let key = key.to_owned();
        self.retry_write(move |client| {
            let k = key.clone();
            Box::pin(async move {
                let mut txn = client.begin_optimistic().await?;
                txn.delete(k).await?;
                txn.commit().await?;
                Ok(())
            })
        })
    }
}

impl TiKeyValueStore {
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

    fn retry_write<F>(&self, make_txn: F) -> Result<(), String>
    where
        F: Fn(&TransactionClient) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), tikv_client::Error>> + Send + '_>,
        >,
    {
        let do_retry = async {
            for attempt in 0..TIKV_MAX_RETRIES {
                match make_txn(&self.client).await {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        if !Self::is_retryable(&e) || attempt + 1 == TIKV_MAX_RETRIES {
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
