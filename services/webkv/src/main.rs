#[macro_use]
extern crate rocket;

use std::env;
use rocket::State;
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::serde::Serialize;
use rocket::fs::{FileServer, relative};
use serde::Deserialize;
use std::sync::Mutex;
use std::net::{IpAddr, Ipv4Addr};
use crate::db::{KeyValueStore, RocksDbKeyValueStore};

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

#[post("/kv/<key>", format = "json", data = "<key_value>")]
async fn set_kv(key: &str, key_value: Json<KeyValue>, db: &State<Mutex<RocksDbKeyValueStore>>)
                -> (Status, Json<Response>) {
    // Check if the key in the URL matches the key in the body
    if key != key_value.key {
        return (Status::BadRequest, Json(Response {
            status: "error".to_string(),
            message: "Key in URL does not match key in body".to_string(),
        }));
    }

    let db = db.lock().unwrap();
    if db.set(&key_value.key, &key_value.value).is_ok() {
        (Status::Ok, Json(Response {
            status: "success".to_string(),
            message: "Key-value pair set successfully".to_string(),
        }))
    } else {
        (Status::InternalServerError, Json(Response {
            status: "error".to_string(),
            message: "Failed to set key-value pair".to_string(),
        }))
    }
}

#[put("/kv/<key>", format = "json", data = "<key_value>")]
async fn set_kv2(key: &str, key_value: Json<KeyValue>, db: &State<Mutex<RocksDbKeyValueStore>>)
                 -> (Status, Json<Response>) {
    set_kv(key, key_value, db).await
}

#[get("/kv/<key>")]
async fn get_kv(key: &str, db: &State<Mutex<RocksDbKeyValueStore>>) -> (Status, Json<KeyValue>) {
    let db = db.lock().unwrap();
    if let Ok(Some(value)) = db.get(key) {
        (Status::Ok, Json(KeyValue {
            key: key.to_string(),
            value,
        }))
    } else {
        // Return an empty string as value if the key doesn't exist
        (Status::Ok, Json(KeyValue {
            key: key.to_string(),
            value: "".to_string(),
        }))
    }
}

#[delete("/kv/<key>")]
async fn delete_kv(key: &str, db: &State<Mutex<RocksDbKeyValueStore>>) -> (Status, Json<Response>) {
    let db = db.lock().unwrap();
    if db.delete(key).is_ok() {
        (Status::Ok, Json(Response {
            status: "success".to_string(),
            message: "Key deleted successfully".to_string(),
        }))
    } else {
        (Status::InternalServerError, Json(Response {
            status: "error".to_string(),
            message: "Failed to delete key".to_string(),
        }))
    }
}

#[launch]
fn rocket() -> _ {
    let db: RocksDbKeyValueStore = KeyValueStore::new("data").unwrap();

    let host = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
    let port = 80;

    rocket::build()
        .manage(Mutex::new(db))
        .mount("/", FileServer::from(relative!("/static")))
        .mount("/api/", routes![set_kv, set_kv2, get_kv, delete_kv])
        .configure(rocket::Config {
            address: host,
            port,
            ..rocket::Config::default()
        })
}
