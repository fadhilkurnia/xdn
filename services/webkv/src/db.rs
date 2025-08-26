use rocksdb::{DBWithThreadMode, SingleThreaded, DB};
use rusqlite::{params, Connection};

pub trait KeyValueStore {
    fn new(db_path: &str) -> Result<Self, String> where Self: Sized;
    fn set(&self, key: &str, value: &str) -> Result<(), String>;
    fn get(&self, key: &str) -> Result<Option<String>, String>;
    fn delete(&self, key: &str) -> Result<(), String>;
}

pub struct SqliteKeyValueStore {
    conn: Connection,
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
        ).map_err(|e| e.to_string())?;
        Ok(SqliteKeyValueStore { conn })
    }

    fn set(&self, key: &str, value: &str) -> Result<(), String> {
        self.conn.execute(
            "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?1, ?2)",
            params![key, value],
        ).map_err(|e| e.to_string())?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<String>, String> {
        let mut stmt = self.conn
            .prepare("SELECT value FROM kv_store WHERE key = ?1")
            .map_err(|e| e.to_string())?;
        let mut rows = stmt.query(params![key])
            .map_err(|e| e.to_string())?;

        if let Some(row) = rows.next().map_err(|e| e.to_string())? {
            Ok(Some(row.get(0).map_err(|e| e.to_string())?))
        } else {
            Ok(None)
        }
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        self.conn
            .execute("DELETE FROM kv_store WHERE key = ?1", params![key])
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

pub struct RocksDbKeyValueStore {
    conn: DBWithThreadMode<SingleThreaded>,
}

impl KeyValueStore for RocksDbKeyValueStore {
    fn new(db_path: &str) -> Result<Self, String> {
        let db = DB::open_default(db_path);
        match db {
            Ok(db) => Ok(RocksDbKeyValueStore { conn : db }),
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
            Ok(Some(value)) => Ok(Some(String::from_utf8(value.to_vec()).unwrap())),
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
