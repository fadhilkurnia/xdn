use std::collections::HashMap;
use std::io::BufReader;
use std::net::SocketAddr;
use java_properties::read;

pub struct ServerLocation {
    pub(crate) name: String,
    pub(crate) address: Option<SocketAddr>,
    pub(crate) latitude: f64,
    pub(crate) longitude: f64,
}

/// Opens Gigapaxos config file (.properties), reads the file to find the server location,
/// then return the server location in HashMap with hostname/IP as the key. Expected format
/// for server data:
///    active.xxx=10.10.1.1:2000
///    active.xxx.geolocation="10.35,-90.20"
/// Returns a String error otherwise.
pub fn get_server_locations(config_file: &str) -> Result<HashMap<String, ServerLocation>, String> {
    if config_file.is_empty() {
        return Ok(HashMap::new());
    }
    log::info!("Loading server locations from '{}'", config_file);

    let file = std::fs::File::open(config_file).expect("Could not open the config file");
    let config_map = read(BufReader::new(file)).expect("Could not read the config file");

    // temporary HashMap, mapping server name into ServerLocation struct
    let mut location_map = HashMap::<String, ServerLocation>::new();
    for (key, value) in config_map.into_iter() {

        // handle server host IP and port
        if key.starts_with("active.") && key.split('.').collect::<Vec<&str>>().len() == 2 {
            let server_name = key.split('.').collect::<Vec<&str>>()[1];
            let address = value.parse::<SocketAddr>();
            if address.is_err() {
                log::warn!("Could not parse server address {}", server_name);
                continue;
            }
            let address = address.unwrap();
            if !location_map.contains_key(server_name) {
                location_map.insert(server_name.to_string(), ServerLocation {
                    name: server_name.to_string(),
                    address: Some(address),
                    latitude: 0.0,
                    longitude: 0.0,
                });
            } else {
                let sl = location_map.get_mut(server_name).unwrap();
                sl.address = Some(address);
            }
        }

        // handle server geolocation
        if key.starts_with("active.")
            && key.split('.').collect::<Vec<&str>>().len() == 3
            && key.split('.').collect::<Vec<&str>>()[2]
            .eq_ignore_ascii_case("geolocation") {
            let server_name = key.split('.').collect::<Vec<&str>>()[1];

            let locations = value.trim().trim_matches('"').split(',')
                .collect::<Vec<&str>>();
            let locations = locations.into_iter()
                .map(|s| s.parse::<f64>().unwrap())
                .collect::<Vec<f64>>();
            if locations.len() != 2 {
                log::warn!("Invalid geolocation data for {}", server_name);
                continue;
            }

            let latitude = locations[0];
            let longitude = locations[1];

            if !location_map.contains_key(server_name) {
                location_map.insert(server_name.to_string(), ServerLocation {
                    name: server_name.to_string(),
                    address: None,
                    latitude,
                    longitude,
                });
            } else {
                let sl = location_map.get_mut(server_name).unwrap();
                sl.latitude = latitude;
                sl.longitude = longitude;
            }
        }
    };

    let mut server_location_by_hostname = HashMap::<String, ServerLocation>::new();
    let server_names = location_map.keys().cloned().collect::<Vec<String>>();
    for sn in server_names {
        let loc = location_map.remove(&sn).unwrap();
        if loc.address.is_none() {
            continue;
        }
        log::info!("- parsed location: {}\t{}\t{}\t{}",
            loc.name, loc.address.unwrap(), loc.latitude, loc.longitude);
        server_location_by_hostname.insert(loc.address.unwrap().ip().to_string(), loc);
    }

    Ok(server_location_by_hostname)
}


/// Returns the `XDN_EVAL_LATENCY_SLOWDOWN_FACTOR` from the given `.properties` file,
/// or `1.0` if the file is missing, empty, or invalid.
///
/// # Arguments
/// * `config_file` - Path to the `.properties` file.
///
/// # Returns
/// The slowdown factor as `f64`.
pub fn get_slowdown_factor(config_file: &str) -> f64 {
    if config_file.is_empty() {
        log::info!("Unspecified config file, using default latency slowdown factor: 1.0x");
        return 1.0;
    }

    // Open the .properties config file
    let file = std::fs::File::open(config_file);
    if file.is_err() {
        log::warn!("Failed to open config file, using default latency slowdown factor: 1.0x");
        return 1.0;
    }
    let file = file.unwrap();

    // Read and parse the config file
    let config_map = read(BufReader::new(file));
    if config_map.is_err() {
        log::warn!("Failed to read config file, using default latency slowdown factor: 1.0x");
        return 1.0;
    }
    let config_map = config_map.unwrap();

    for (key, value) in config_map.into_iter() {
        if key.eq("XDN_EVAL_LATENCY_SLOWDOWN_FACTOR") {
            let slowdown = value.parse::<f64>().unwrap_or(1.0);
            log::info!("Parsed slowdown factor: {}x", slowdown);
            return slowdown;
        }
    }

    log::warn!("Unknown value of `XDN_EVAL_LATENCY_SLOWDOWN_FACTOR`, using the default: 1.0x");
    1.0
}

/// Calculates the Haversine distance (in meters) between two points on Earth.
///
/// # Arguments
/// * `lat1` - Latitude of the first point in degrees
/// * `lon1` - Longitude of the first point in degrees
/// * `lat2` - Latitude of the second point in degrees
/// * `lon2` - Longitude of the second point in degrees
///
/// # Returns
/// * `f64` - The distance between the two points in meters
///
pub fn calculate_haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    // Earth radius in kilometers
    let r_km = 6371.0_f64;

    // Convert degrees to radians
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();

    // Haversine formula
    let a = (d_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    // Distance in kilometers, then convert to meters
    (r_km * c) * 1_000.0
}

/// Calculates an emulated latency (in milliseconds) given a distance (in meters)
/// and a slowdown factor between 0 (exclusive) and 1 (inclusive).
///
/// # Arguments
/// * `distance` - The distance in meters.
/// * `slowdown_factor` - The factor by which speed is reduced (0 < slowdown_factor <= 1).
///
/// # Returns
/// * `Ok(f64)` - The emulated latency in milliseconds.
/// * `Err(String)` - An error if the slowdown factor is invalid.
pub(crate) fn get_emulated_latency(distance: f64, slowdown_factor: f64) -> Result<f64, String> {
    // Validate the slowdown factor
    if slowdown_factor <= 0.0 || slowdown_factor > 1.0 {
        return Err(format!(
            "Invalid slowdown factor: {}. Must be > 0 and <= 1.",
            slowdown_factor
        ));
    }

    // Speed of light in meters per second
    let speed_of_light_m_s = 299_792_458.0_f64;

    // Time in seconds = distance / (speed_of_light * slowdown_factor)
    let time_s = distance / (speed_of_light_m_s * slowdown_factor);

    // Convert to milliseconds
    let time_ms = time_s * 1000.0;

    Ok(time_ms)
}
