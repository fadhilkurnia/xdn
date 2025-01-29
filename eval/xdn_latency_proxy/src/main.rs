use hyper::{
    client::{Client, HttpConnector},
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};
use std::{
    collections::HashMap,
    convert::Infallible,
    net::SocketAddr,
    sync::Arc,
    time::Instant,
};
use tokio::time::Duration;

mod utils;
use utils::ServerLocation;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Usage: xdn_latency_proxy <config_file>");
        return Err("Expecting a valid config file.".into());
    }

    // Initialize logger and its level, defaulting to INFO.
    use std::env;
    let log_level = env::var("RUST_LOG");
    if log_level.is_err() || log_level.unwrap() == "" {
        env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    // Our proxy will bind to 0.0.0.0:8080
    let address = SocketAddr::from(([0, 0, 0, 0], 8080));

    // Create a shared Hyper client to forward requests
    let client = Client::new();

    // Read the configuration file
    let mut config_file = "";
    if args.len() >= 2 {
        config_file = args[1].as_str();
    }
    let server_locations = utils::get_server_locations(config_file)?;
    let server_locations = Arc::new(server_locations);
    let slowdown_factor = utils::get_slowdown_factor(config_file);

    // Create the "make_service" to handle each connection
    let make_svc = make_service_fn(move |_| {
        let client = client.clone();
        let server_location_ptr = Arc::clone(&server_locations);
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                let client = client.clone();
                let server_location_ptr = server_location_ptr.clone();
                async move {
                    proxy_handler(req, &client, server_location_ptr, slowdown_factor).await
                }
            }))
        }
    });

    let server = Server::bind(&address).serve(make_svc);
    log::info!("Latency proxy is listening on http://{}", address);

    // Run until server is stopped (e.g., ctrl-c)
    server.await?;

    Ok(())
}

async fn proxy_handler(
    req: Request<Body>,
    client: &Client<HttpConnector>,
    server_locations: Arc<HashMap<String, ServerLocation>>,
    slowdown_factor: f64,
) -> Result<Response<Body>, hyper::Error> {
    let processing_time_start = Instant::now();

    // Look for the "X-Request-Delay" header in the incoming request (in ms).
    let request_delay_ms = req
        .headers()
        .get("X-Request-Delay")
        .and_then(|val| val.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let mut request_delay_ns = request_delay_ms * 1_000_000;

    // Look for the "X-Client-Location" header if "X-Request-Delay" doesn't exist.
    // The expected format is "X-Client-Location: 39.9526;-75.1652".
    if request_delay_ns == 0 && req.headers().contains_key("X-Client-Location") {

        // Parses client location
        let client_location = req.headers()
            .get("X-Client-Location").unwrap()
            .to_str().unwrap();
        let mut client_latitude: Option<f64> = None;
        let mut client_longitude: Option<f64> = None;
        if client_location.trim().split(";").collect::<Vec<&str>>().len() == 2 {
            let client_location = client_location.split(";").collect::<Vec<&str>>();
            client_latitude = client_location[0].trim().parse::<f64>().ok();
            client_longitude = client_location[1].trim().parse::<f64>().ok();
        }

        // Calculates the injected client -> replica latency (given the slowdown factor).
        //   The injected latency is the (ping latency / 2).
        let mut server_latitude: Option<f64> = None;
        let mut server_longitude: Option<f64> = None;
        let target_server = req.uri().host().unwrap();
        let target_server = target_server.to_string();
        let server_location = server_locations.get(&target_server);
        if server_location.is_some() {
            let server_location = server_location.unwrap();
            server_latitude = Some(server_location.latitude);
            server_longitude = Some(server_location.longitude);
        }

        // Calculates the emulated latency, given the client and server location
        if client_latitude.is_some() && client_longitude.is_some() &&
            server_latitude.is_some() && server_longitude.is_some() {
            let client_server_distance =
                utils::calculate_haversine_distance(
                    client_latitude.unwrap(), client_longitude.unwrap(),
                    server_latitude.unwrap(), server_longitude.unwrap());
            let emulated_latency_ms =
                utils::get_emulated_latency(client_server_distance, slowdown_factor)
                    .unwrap_or(0.0);
            request_delay_ns = (emulated_latency_ms * 1_000_000.0) as u64;

            log::debug!("Path: Client({:.2};{:.2}) <=> Replica:{}({:.2};{:.2}) \
            dist={:.2}m slw={:.2}x",
                client_latitude.unwrap(),
                client_longitude.unwrap(),
                server_location.unwrap().name,
                server_latitude.unwrap(),
                server_longitude.unwrap(),
                client_server_distance,
                slowdown_factor
            );
        }
    }

    let delay_calculation_end_time = Instant::now();

    // Builds a new request to forward (reusing headers, method, and URI).
    let (mut parts, body) = req.into_parts();
    parts.headers.remove("X-Request-Delay");
    parts.headers.remove("X-Client-Location");
    let forward_req = Request::from_parts(parts, body);
    let request_creation_end_time = Instant::now();

    // If we have a valid delay, sleeps for that duration.
    if request_delay_ns > 0 {
        log::debug!("Delaying request by {}ms",
            (request_delay_ns as f64) / 1_000_000.0);
        let start = processing_time_start;
        let delay_duration = Duration::from_nanos(request_delay_ns);
        while Instant::now() - start < delay_duration {
            // delay with busy loop
        }
    }
    let delay_emulation_end_time = Instant::now();

    // Forwards the request to the upstream server using Hyper's client.
    let response = client.request(forward_req).await?;
    let request_response_end_time = Instant::now();

    let total_duration = processing_time_start.elapsed();
    log::debug!("Spent {:?} in proxy, pre={:?} req={:?} dly={:?} proc={:?}",
        total_duration,
        delay_calculation_end_time - processing_time_start,
        request_creation_end_time - delay_calculation_end_time,
        delay_emulation_end_time - request_creation_end_time,
        request_response_end_time - delay_emulation_end_time,
    );
    Ok(response)
}
