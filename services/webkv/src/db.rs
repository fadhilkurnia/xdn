use rocksdb::DB;
use rusqlite::{params, Connection};
use std::sync::Mutex;
use tikv_client::RawClient;
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
    conn: DB,
}

impl KeyValueStore for RocksDbKeyValueStore {
    fn new(db_path: &str) -> Result<Self, String> {
        let db = DB::open_default(db_path);
        match db {
            Ok(db) => Ok(RocksDbKeyValueStore { conn: db }),
            Err(e) => Err(e.to_string()),
        }
    }

    fn set(&self, key: &str, value: &str) -> Result<(), String> {
        match self.conn.put(key, value.as_bytes()) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }

    fn get(&self, key: &str) -> Result<Option<String>, String> {
        match self.conn.get(key) {
            Ok(Some(value)) => {
                let as_string = String::from_utf8(value.to_vec()).map_err(|e| e.to_string())?;
                Ok(Some(as_string))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        match self.conn.delete(key) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }
}

pub struct TiKeyValueStore {
    client: RawClient,
    runtime: Runtime,
}

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
            .block_on(RawClient::new(endpoints))
            .map_err(|e| e.to_string())?;

        Ok(TiKeyValueStore { client, runtime })
    }

    fn set(&self, key: &str, value: &str) -> Result<(), String> {
        self.run(self.client.put(key.to_owned(), value.to_owned()))
    }

    fn get(&self, key: &str) -> Result<Option<String>, String> {
        let result = self.run(self.client.get(key.to_owned()))?;

        match result {
            Some(val) => {
                let bytes: Vec<u8> = val.into();
                let value = String::from_utf8(bytes).map_err(|e| e.to_string())?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        self.run(self.client.delete(key.to_owned()))
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
}

#[cfg(test)]
mod tests {
    use super::{KeyValueStore, TiKeyValueStore};

    #[test]
    fn tikv_round_trip_when_env_set() {
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
