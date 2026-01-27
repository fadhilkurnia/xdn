#[macro_use]
extern crate rocket;

use std::env;
use rocket::State;
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::serde::Serialize;
use rocket::fs::{FileServer, relative};
use serde::Deserialize;
use std::net::{IpAddr, Ipv4Addr};
use crate::db::{KeyValueStore, RocksDbKeyValueStore, TiKeyValueStore};

mod db;

#[derive(Serialize, Deserialize)]
struct KeyValue {
    key: String,
    value: String,
}

#[derive(Serialize)]
struct Response {
    status: String,
    message: String,
}

type SharedStore = Box<dyn KeyValueStore + Send + Sync>;

#[post("/kv/<key>", format = "json", data = "<key_value>")]
async fn set_kv(key: &str, key_value: Json<KeyValue>, db: &State<SharedStore>)
                -> (Status, Json<Response>) {
    // Check if the key in the URL matches the key in the body
    if key != key_value.key {
        return (Status::BadRequest, Json(Response {
            status: "error".to_string(),
            message: "Key in URL does not match key in body".to_string(),
        }));
    }

    let db = db.inner();
    match db.set(&key_value.key, &key_value.value) {
        Ok(_) => (Status::Ok, Json(Response {
            status: "success".to_string(),
            message: "Key-value pair set successfully".to_string(),
        })),
        Err(e) => (Status::InternalServerError, Json(Response {
            status: "error".to_string(),
            message: format!("Failed to set key-value pair: {}", e),
        })),
    }
}

#[put("/kv/<key>", format = "json", data = "<key_value>")]
async fn set_kv2(key: &str, key_value: Json<KeyValue>, db: &State<SharedStore>)
                 -> (Status, Json<Response>) {
    set_kv(key, key_value, db).await
}

#[get("/kv/<key>")]
async fn get_kv(key: &str, db: &State<SharedStore>) -> (Status, Json<KeyValue>) {
    let db = db.inner();
    match db.get(key) {
        Ok(Some(value)) => (Status::Ok, Json(KeyValue { key: key.to_string(), value })),
        Ok(None) => (Status::Ok, Json(KeyValue { key: key.to_string(), value: "".to_string() })),
        Err(e) => (
            Status::InternalServerError,
            Json(KeyValue { key: key.to_string(), value: format!("error: {}", e) }),
        ),
    }
}

#[delete("/kv/<key>")]
async fn delete_kv(key: &str, db: &State<SharedStore>) -> (Status, Json<Response>) {
    let db = db.inner();
    match db.delete(key) {
        Ok(_) => (Status::Ok, Json(Response {
            status: "success".to_string(),
            message: "Key deleted successfully".to_string(),
        })),
        Err(e) => (Status::InternalServerError, Json(Response {
            status: "error".to_string(),
            message: format!("Failed to delete key: {}", e),
        })),
    }
}

fn init_store_from_env() -> Result<Box<dyn KeyValueStore + Send + Sync>, String> {
    let db_type = env::var("DB_TYPE").unwrap_or_else(|_| "rocksdb".to_string()).to_lowercase();
    match db_type.as_str() {
        "rocksdb" => {
            let path = env::var("DB_HOST").unwrap_or_else(|_| "data".to_string());
            Ok(Box::new(RocksDbKeyValueStore::new(&path)?))
        }
        "tikv" => {
            let pd = env::var("DB_HOST")
                .map_err(|_| "DB_HOST must be set when DB_TYPE=tikv (PD endpoints comma-separated)".to_string())?;
            Ok(Box::new(TiKeyValueStore::new(&pd)?))
        }
        other => Err(format!(
            "Unsupported DB_TYPE '{}'. Use 'rocksdb' (default), 'sqlite', or 'tikv'.",
            other
        )),
    }
}

#[launch]
fn rocket() -> _ {
    let db = init_store_from_env()
        .expect("Failed to initialize key-value store from env (DB_TYPE/DB_HOST)");

    let host = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
    let port = 80;

    rocket::build()
        .manage(db)
        .mount("/", FileServer::from(relative!("/static")))
        .mount("/api/", routes![set_kv, set_kv2, get_kv, delete_kv])
        .configure(rocket::Config {
            address: host,
            port,
            ..rocket::Config::default()
        })
}
