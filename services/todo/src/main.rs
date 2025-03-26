use actix_web::{delete, get, post, web, App, HttpResponse, HttpServer, Responder};
use actix_web::http::header;
use serde::{Deserialize, Serialize};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Pool, Row, Sqlite,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Hash)]
struct Task {
    item: String,
}

struct AppState {
    db_pool: Pool<Sqlite>,
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
    let tasks = sqlx::query("SELECT item FROM tasks WHERE counter > 0 ORDER BY item ASC")
        .fetch_all(&data.db_pool)
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

#[post("/api/todo/tasks")]
async fn add_task(data: web::Data<AppState>, task: web::Json<Task>) -> impl Responder {
    let result = sqlx::query(
        "INSERT INTO tasks (item, counter) VALUES (?, 1)
         ON CONFLICT(item) DO UPDATE SET counter = counter + 1",
    )
    .bind(&task.item)
    .execute(&data.db_pool)
    .await;

    match result {
        Ok(_) => HttpResponse::Ok().body("Task added"),
        Err(err) => {
            eprintln!("Database insert error: {:?}", err);
            HttpResponse::InternalServerError().body("Error adding task")
        }
    }
}

#[delete("/api/todo/tasks")]
async fn delete_task(data: web::Data<AppState>, task: web::Json<Task>) -> impl Responder {
    let result = sqlx::query(
        "INSERT INTO tasks (item, counter) VALUES (?, -1)
         ON CONFLICT(item) DO UPDATE SET counter = counter - 1",
    )
    .bind(&task.item)
    .execute(&data.db_pool)
    .await;

    match result {
        Ok(_) => HttpResponse::Ok().body("Task removed"),
        Err(err) => {
            eprintln!("Database delete error: {:?}", err);
            HttpResponse::InternalServerError().body("Error removing task")
        }
    }
}

#[actix_web::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Ensure the data directory exists
    fs::create_dir_all("data")?;

    // Path to the database file
    let database_path = Path::new("data").join("tasks.db");

    // Configure the SQLite connection options
    let connect_options = SqliteConnectOptions::from_str(database_path.to_str().unwrap())?
        .create_if_missing(true);

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

    // Prepare the app state, with the underlying SQLite data
    let app_state = web::Data::new(AppState { db_pool });

    // Configure the listening host and port
    let host = "0.0.0.0";
    let port = 80;

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
    .bind((host, port))?
    .run()
    .await?;

    Ok(())
}
