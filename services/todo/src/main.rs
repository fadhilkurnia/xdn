use actix_web::{delete, get, post, web, App, HttpResponse, HttpServer, Responder};
use actix_web::http::header;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    Pool, Row, Sqlite,
};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Hash)]
struct Task {
    item: String,
}

enum Backend {
    Sqlite(Pool<Sqlite>),
    Rqlite(RqliteClient),
}

struct AppState {
    backend: Backend,
}

struct RqliteClient {
    base_url: String,
    http: reqwest::Client,
}

#[derive(Deserialize)]
struct RqliteQueryResponse {
    results: Option<Vec<RqliteQueryResult>>,
}

#[derive(Deserialize)]
struct RqliteQueryResult {
    values: Option<Vec<Vec<serde_json::Value>>>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct RqliteExecResponse {
    results: Option<Vec<RqliteExecResult>>,
}

#[derive(Deserialize)]
struct RqliteExecResult {
    error: Option<String>,
}

// Redirect '/' to '/view'
#[get("/")]
async fn index() -> HttpResponse {
    HttpResponse::Found()
        .append_header((header::LOCATION, "/view"))
        .finish()
}

// Serve the 'index.html' file from the 'static' folder
#[get("/view")]
async fn view() -> impl Responder {
    // Define the path to the 'index.html' file
    let path: PathBuf = ["static", "index.html"].iter().collect();

    // Try to read the file, handle potential errors
    match fs::read_to_string(path) {
        Ok(contents) => HttpResponse::Ok().content_type("text/html").body(contents),
        Err(_) => HttpResponse::InternalServerError().body("Error loading index.html"),
    }
}

#[get("/api/todo/tasks")]
async fn get_tasks(data: web::Data<AppState>) -> impl Responder {
    match &data.backend {
        Backend::Sqlite(pool) => {
            let tasks = sqlx::query("SELECT item FROM tasks WHERE counter > 0 ORDER BY item ASC")
                .fetch_all(pool)
                .await;

            match tasks {
                Ok(rows) => {
                    let active_tasks: Vec<String> = rows.into_iter().map(|row| row.get("item")).collect();
                    HttpResponse::Ok().json(&active_tasks)
                }
                Err(err) => {
                    eprintln!("Database query error: {:?}", err);
                    HttpResponse::InternalServerError().body("Error fetching tasks")
                }
            }
        }
        Backend::Rqlite(client) => {
            match client
                .query(
                    "SELECT item FROM tasks WHERE counter > 0 ORDER BY item ASC",
                    &[],
                )
                .await
            {
                Ok(values) => {
                    let mut active_tasks = Vec::new();
                    for row in values {
                        if let Some(val) = row.get(0) {
                            if let Some(item) = val.as_str() {
                                active_tasks.push(item.to_string());
                            }
                        }
                    }
                    HttpResponse::Ok().json(&active_tasks)
                }
                Err(err) => {
                    eprintln!("Rqlite query error: {}", err);
                    HttpResponse::InternalServerError().body("Error fetching tasks")
                }
            }
        }
    }
}

#[post("/api/todo/tasks")]
async fn add_task(data: web::Data<AppState>, task: web::Json<Task>) -> impl Responder {
    match &data.backend {
        Backend::Sqlite(pool) => {
            let result = sqlx::query(
                "INSERT INTO tasks (item, counter) VALUES (?, 1)
                 ON CONFLICT(item) DO UPDATE SET counter = counter + 1",
            )
            .bind(&task.item)
            .execute(pool)
            .await;

            match result {
                Ok(_) => HttpResponse::Ok().body("Task added"),
                Err(err) => {
                    eprintln!("Database insert error: {:?}", err);
                    HttpResponse::InternalServerError().body("Error adding task")
                }
            }
        }
        Backend::Rqlite(client) => {
            match client
                .execute(
                    "INSERT INTO tasks (item, counter) VALUES (?, 1) \
                     ON CONFLICT(item) DO UPDATE SET counter = counter + 1",
                    &[json!(task.item.as_str())],
                )
                .await
            {
                Ok(_) => HttpResponse::Ok().body("Task added"),
                Err(err) => {
                    eprintln!("Rqlite insert error: {}", err);
                    HttpResponse::InternalServerError().body("Error adding task")
                }
            }
        }
    }
}

#[delete("/api/todo/tasks")]
async fn delete_task(data: web::Data<AppState>, task: web::Json<Task>) -> impl Responder {
    match &data.backend {
        Backend::Sqlite(pool) => {
            let result = sqlx::query(
                "INSERT INTO tasks (item, counter) VALUES (?, -1)
                 ON CONFLICT(item) DO UPDATE SET counter = counter - 1",
            )
            .bind(&task.item)
            .execute(pool)
            .await;

            match result {
                Ok(_) => HttpResponse::Ok().body("Task removed"),
                Err(err) => {
                    eprintln!("Database delete error: {:?}", err);
                    HttpResponse::InternalServerError().body("Error removing task")
                }
            }
        }
        Backend::Rqlite(client) => {
            match client
                .execute(
                    "INSERT INTO tasks (item, counter) VALUES (?, -1) \
                     ON CONFLICT(item) DO UPDATE SET counter = counter - 1",
                    &[json!(task.item.as_str())],
                )
                .await
            {
                Ok(_) => HttpResponse::Ok().body("Task removed"),
                Err(err) => {
                    eprintln!("Rqlite delete error: {}", err);
                    HttpResponse::InternalServerError().body("Error removing task")
                }
            }
        }
    }
}

#[actix_web::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_type = env::var("DB_TYPE").unwrap_or_else(|_| "sqlite".to_string());
    if db_type != "sqlite" && db_type != "rqlite" {
        return Err(format!("invalid DB_TYPE {}, options: sqlite, rqlite", db_type).into());
    }

    let backend = if db_type == "rqlite" {
        let db_host = env::var("DB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let base_url = normalize_rqlite_base(&db_host);
        println!("Using datastore: rqlite ({})", base_url);
        let client = RqliteClient::new(base_url);
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS tasks (item TEXT PRIMARY KEY, counter INTEGER NOT NULL)",
                &[],
            )
            .await?;
        Backend::Rqlite(client)
    } else {
        println!("Using datastore: sqlite");
        // Ensure the data directory exists
        fs::create_dir_all("data")?;

        // Path to the database file
        let database_path = Path::new("data").join("tasks.db");

        // Check if WAL mode is enabled via environment variable
        let enable_wal = env::var("ENABLE_WAL")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false);

        // Configure the SQLite connection options
        let mut connect_options = SqliteConnectOptions::from_str(database_path.to_str().unwrap())?
            .create_if_missing(true);

        // Enable WAL mode with synchronous=FULL for durability
        if enable_wal {
            println!("Enabling WAL mode with synchronous=FULL");
            connect_options = connect_options
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(SqliteSynchronous::Full);
        }

        // Initialize the database connection pool
        let db_pool = SqlitePoolOptions::new()
            .connect_with(connect_options)
            .await?;

        // Create the tasks table if it doesn't exist
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS tasks (
                item TEXT PRIMARY KEY,
                counter INTEGER NOT NULL
            )",
        )
        .execute(&db_pool)
        .await?;

        Backend::Sqlite(db_pool)
    };

    // Prepare the app state, with the underlying backend
    let app_state = web::Data::new(AppState { backend });

    // Configure the listening host and port
    let host = "0.0.0.0";
    let port = env::var("PORT").unwrap_or_else(|_| "80".to_string());

    // Log the host and port where the server will run
    println!("Starting server at http://{}:{}", host, port);

    // Start the HTTP server
    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .service(index)
            .service(view)
            .service(get_tasks)
            .service(add_task)
            .service(delete_task)
    })
    .bind((host, port.parse::<u16>()?))?
    .run()
    .await?;

    Ok(())
}

impl RqliteClient {
    fn new(base_url: String) -> Self {
        Self {
            base_url,
            http: reqwest::Client::new(),
        }
    }

    async fn execute(&self, statement: &str, params: &[serde_json::Value]) -> Result<(), String> {
        let mut stmt = Vec::with_capacity(params.len() + 1);
        stmt.push(json!(statement));
        for param in params {
            stmt.push(param.clone());
        }
        let body = json!([stmt]);
        let url = format!(
            "{}/db/execute?transaction&timings",
            self.base_url.trim_end_matches('/')
        );
        let resp = self
            .http
            .post(url)
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = resp.status();
        let resp_body = resp.json::<RqliteExecResponse>().await.map_err(|e| e.to_string())?;
        if !status.is_success() {
            return Err(format!("rqlite execute HTTP {}", status));
        }
        if let Some(results) = resp_body.results {
            for result in results {
                if let Some(err) = result.error {
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    async fn query(
        &self,
        statement: &str,
        params: &[serde_json::Value],
    ) -> Result<Vec<Vec<serde_json::Value>>, String> {
        let mut stmt = Vec::with_capacity(params.len() + 1);
        stmt.push(json!(statement));
        for param in params {
            stmt.push(param.clone());
        }
        let body = json!([stmt]);
        let url = format!("{}/db/query?transaction&timings", self.base_url.trim_end_matches('/'));
        let resp = self
            .http
            .post(url)
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = resp.status();
        let resp_body = resp.json::<RqliteQueryResponse>().await.map_err(|e| e.to_string())?;
        if !status.is_success() {
            return Err(format!("rqlite query HTTP {}", status));
        }
        if let Some(results) = resp_body.results {
            if let Some(first) = results.get(0) {
                if let Some(err) = &first.error {
                    return Err(err.clone());
                }
                if let Some(values) = &first.values {
                    return Ok(values.clone());
                }
            }
        }
        Ok(Vec::new())
    }
}

fn normalize_rqlite_base(db_host: &str) -> String {
    if db_host.starts_with("http://") || db_host.starts_with("https://") {
        return db_host.to_string();
    }
    if db_host.contains(':') {
        return format!("http://{}", db_host);
    }
    format!("http://{}:4001", db_host)
}
