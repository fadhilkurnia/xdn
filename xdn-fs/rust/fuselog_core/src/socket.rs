use crate::statediff::{StateDiffAction, StateDiffLog};
use crate::STATEDIFF_LOG;
use bincode::config;
use log::{error, info, warn};
use std::collections::{HashMap, HashSet};
use std::io::{ErrorKind, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::env;
use std::fs;
use std::thread;
use std::time::Duration;

// PRODUCTION
const MIN_SAMPLES: usize = 200;
const MIN_TOTAL_BYTES: usize = 10 * 1024 * 1024; // 10MB
const DICT_SIZE: usize = 128 * 1024; // 128KB
const MIN_SAMPLE_SIZE: usize = 1024; // 1KB
const MAX_TRAINING_BUFFER_SIZE: usize = 250;
const SAMPLES_TO_KEEP_AFTER_TRAINING: usize = 100;
const COMPRESSION_LEVEL: i32 = 3;

// DEVELOPMENT (for testing purposes)
const DEV_MIN_SAMPLES: usize = 5;
const DEV_MIN_TOTAL_BYTES: usize = 2 * 1024; // 2KB
const DEV_MIN_SAMPLE_SIZE: usize = 100; // 100 bytes

const DICT_PATH: &str = "/var/cache/fuselog/statediff.dict";

#[derive(Default)]
struct PruneState {
    creation_idx: Option<usize>,
    last_chmod_idx: Option<usize>,
    last_chown_idx: Option<usize>,
}

struct AdaptiveState {
    first_statediff_seen: bool,
    training_buffer: Vec<Vec<u8>>,
    encoder_dict: Option<Arc<Vec<u8>>>,
    training_in_progress: bool,
    new_dict_needs_sending: bool,
}

impl Default for AdaptiveState {
    fn default() -> Self {
        Self {
            first_statediff_seen: false,
            training_buffer: Vec::new(),
            encoder_dict: None,
            training_in_progress: false,
            new_dict_needs_sending: false,
        }
    }
}

static ADAPTIVE_STATE: once_cell::sync::Lazy<Mutex<AdaptiveState>> = once_cell::sync::Lazy::new(|| Mutex::new(AdaptiveState::default()));

fn load_existing_dictionary() {
    if let Ok(dict_data) = std::fs::read(DICT_PATH) {
        let mut state = ADAPTIVE_STATE.lock().unwrap();
        let dict_arc = Arc::new(dict_data);
        state.encoder_dict = Some(dict_arc);
        state.first_statediff_seen = true;
        state.training_in_progress = false;
        info!("Loaded existing dictionary from {} (size: {} bytes)", 
              DICT_PATH, 
              state.encoder_dict.as_ref().unwrap().len());
    } else {
        info!("No existing dictionary found at {}", DICT_PATH);
    }
}

fn handle_client(mut stream: UnixStream) -> Result<(), Box<dyn std::error::Error>> {
    info!("Socket: Client connected");

    let mut buffer = [0; 1];

    loop {
        match stream.read_exact(&mut buffer) {
            Ok(_) => {
                let result = match buffer[0] {
                    b'g' => send_statediff(stream.try_clone()?),
                    b'c' => clear_statediff(),
                    b'm' => {
                        println!("[]==========[] CHECKPOINT []==========[] ");
                        Ok(())
                    },
                    _ => {
                        warn!("Socket: Received unknown command: {}", buffer[0] as char);
                        Ok(())
                    }
                };

                if let Err(e) = result {
                    error!("Socket: Command error: {}", e);
                }
            }
            Err(e) => {
                eprintln!("Read error or client disconnected: {}", e);
                break;
            }
        }
    }
    Ok(())
}

pub fn start_listener(socket_path: &str, shutdown_rx: Receiver<()>) -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::fs::remove_file(socket_path);
    
    let listener = UnixListener::bind(socket_path)?;
    listener.set_nonblocking(true)?;
    
    info!("Socket listener started at {}", socket_path);

    load_existing_dictionary();

    loop {
        // Check for shutdown signal without blocking
        if shutdown_rx.try_recv().is_ok() {
            info!("Shutdown signal received. Stopping listener.");
            break;
        }

        match listener.accept() {
            Ok((stream, _addr)) => {
                println!("Client connected");

                if let Err(e) = handle_client(stream) {
                    error!("Socket: Error handling client: {}", e);
                }

                info!("Socket: Client disconnected");
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                // No client right now — sleep briefly to avoid busy waiting
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
            Err(e) => {
                error!("Socket: Listener error: {}", e);
                break;
            }
        }
    }

    Ok(())
}

fn prune_log(log: &mut StateDiffLog) {
    if log.actions.is_empty() {
        return;
    }

    let original_action_count = log.actions.len();
    let original_fid_count = log.fid_map.len();

    let mut actions: Vec<Option<StateDiffAction>> = log.actions.drain(..).map(Some).collect();
    let mut file_states: HashMap<u64, PruneState> = HashMap::new();
    let mut fids_to_purge: HashSet<u64> = HashSet::new();

    for i in 0..actions.len() {
        let action = match &actions[i] {
            Some(a) => a,
            None => continue,
        };

        match action {
            StateDiffAction::Create { fid, .. }
            | StateDiffAction::Mkdir { fid }
            | StateDiffAction::Symlink { link_fid: fid, .. } => {
                file_states.entry(*fid).or_default().creation_idx = Some(i);
            }
            StateDiffAction::Chmod { fid, .. } => {
                let state = file_states.entry(*fid).or_default();
                if let Some(prev_idx) = state.last_chmod_idx.replace(i) {
                    actions[prev_idx] = None;
                }
            }
            StateDiffAction::Chown { fid, .. } => {
                let state = file_states.entry(*fid).or_default();
                if let Some(prev_idx) = state.last_chown_idx.replace(i) {
                    actions[prev_idx] = None; 
                }
            }
            StateDiffAction::Unlink { fid } | StateDiffAction::Rmdir { fid } => {
                if let Some(state) = file_states.get(fid) {
                    if state.creation_idx.is_some() {
                        fids_to_purge.insert(*fid);
                    }
                }
            }
            _ => {}
        }
    }

    let mut final_actions = Vec::new();
    let mut used_fids = HashSet::new();

    for action_opt in actions.into_iter() {
        if let Some(action) = action_opt {
            let mut action_fids = Vec::new();
            match &action {
                StateDiffAction::Create { fid, .. }
                | StateDiffAction::Write { fid, .. }
                | StateDiffAction::Unlink { fid }
                | StateDiffAction::Truncate { fid, .. }
                | StateDiffAction::Chown { fid, .. }
                | StateDiffAction::Chmod { fid, .. }
                | StateDiffAction::Mkdir { fid }
                | StateDiffAction::Rmdir { fid } => action_fids.push(*fid),
                StateDiffAction::Symlink { link_fid, .. } => action_fids.push(*link_fid),
                StateDiffAction::Rename { from_fid, to_fid } => {
                    action_fids.push(*from_fid);
                    action_fids.push(*to_fid);
                }
                StateDiffAction::Link {
                    source_fid,
                    new_link_fid,
                } => {
                    action_fids.push(*source_fid);
                    action_fids.push(*new_link_fid);
                }
            };

            if action_fids.iter().any(|fid| fids_to_purge.contains(fid)) {
                continue;
            }

            for fid in action_fids {
                used_fids.insert(fid);
            }
            final_actions.push(action);
        }
    }

    log.actions = final_actions;
    log.fid_map.retain(|fid, _| used_fids.contains(fid));

    if log.actions.len() < original_action_count {
        info!(
            "Log pruned: {} actions -> {} actions, {} fids -> {} fids",
            original_action_count,
            log.actions.len(),
            original_fid_count,
            log.fid_map.len()
        );
    }
}

fn try_train_dictionary_async(samples: Vec<Vec<u8>>) {
    thread::spawn(move || {
        let total_bytes: usize = samples.iter().map(|v| v.len()).sum();
        let sample_count = samples.len();

        info!(
            "Starting async dictionary training with {} samples ({} bytes total)",
            sample_count, total_bytes
        );

        match zstd::dict::from_samples(&samples, DICT_SIZE) {
            Ok(dict_content) => {
                info!(
                    "Successfully trained dictionary of size {} bytes",
                    dict_content.len()
                );

                // Save dictionary to disk
                if let Some(parent) = std::path::Path::new(DICT_PATH).parent() {
                    if let Err(e) = fs::create_dir_all(parent) {
                        error!("Failed to create dictionary directory: {}", e);
                    }
                }

                if let Err(e) = fs::write(DICT_PATH, &dict_content) {
                    error!("Failed to save dictionary to disk: {}", e);
                }

                // Update state with new dictionary
                let mut state = ADAPTIVE_STATE.lock().unwrap();
                state.encoder_dict = Some(Arc::new(dict_content));
                state.training_in_progress = false;
                state.new_dict_needs_sending = true;

                // Trim buffer after successful training
                if state.training_buffer.len() > MAX_TRAINING_BUFFER_SIZE {
                    let drain_count = state.training_buffer.len() - SAMPLES_TO_KEEP_AFTER_TRAINING;
                    state.training_buffer.drain(0..drain_count);
                    info!("Trimmed training buffer to {} samples", state.training_buffer.len());
                }
            }
            Err(e) => {
                error!("Failed to train dictionary: {}", e);
                let mut state = ADAPTIVE_STATE.lock().unwrap();
                state.training_in_progress = false;
            }
        }
    });
}

fn send_statediff(mut stream: UnixStream) -> Result<(), Box<dyn std::error::Error>> {
    info!("Socket: Received 'get' command");

    let serialized_data = {
        let mut log = STATEDIFF_LOG.lock().map_err(|e| {
            error!("Socket: Failed to lock statediff log: {}", e);
            std::io::Error::new(std::io::ErrorKind::Other, "Lock poisoned")
        })?;

        let original_action_count = log.actions.len();
        let original_fid_count = log.fid_map.len();

        // Pruning is disabled by default
        let is_prune_enabled = env::var("FUSELOG_PRUNE")
            .map_or(false, |val| val.to_lowercase() == "true" || val == "1");

        if is_prune_enabled {
            info!("=========================================");
            info!("Pruning enabled. Pruning statediff log...");
            info!("==========================================");
            prune_log(&mut log);
        } else {
            info!("Pruning is disabled. Skipping pruning of statediff log.");
        }

        let bincode_data = bincode::encode_to_vec(&*log, config::standard()).map_err(|e| {
            error!("Socket: Failed to serialize statediff log: {}", e);
            std::io::Error::new(std::io::ErrorKind::Other, format!("Serialization failed: {}", e))
        })?;

        // Adaptive compression is disabled by default
        let adaptive_enabled = env::var("ADAPTIVE_COMPRESSION")
            .map_or(false, |val| val.to_lowercase() == "true" || val == "1");

        if adaptive_enabled && !bincode_data.is_empty() {
            let mut state = ADAPTIVE_STATE.lock().unwrap();

            if !state.first_statediff_seen {
                // Skipping the first statediff (database initialization)
                state.first_statediff_seen = true;
                info!("Skipping first statediff for dictionary training (initialization data)");
            } else {
                let dev_mode = env::var("ADAPTIVE_DEV_MODE")
                    .map_or(false, |val| val.to_lowercase() == "true" || val == "1");

                let min_sample_size = if dev_mode { DEV_MIN_SAMPLE_SIZE } else { MIN_SAMPLE_SIZE };

                if bincode_data.len() >= min_sample_size {
                    // Collect sample for training
                    state.training_buffer.push(bincode_data.clone());
                    info!("Collected sample for dictionary training: {} bytes (total samples: {})",
                          bincode_data.len(), state.training_buffer.len());

                    // Let's check if we should train
                    let total_bytes: usize = state.training_buffer.iter().map(|v| v.len()).sum();
                    let (min_samples, min_total_bytes) = if dev_mode {
                        (DEV_MIN_SAMPLES, DEV_MIN_TOTAL_BYTES)
                    } else {
                        (MIN_SAMPLES, MIN_TOTAL_BYTES)
                    };

                    if state.encoder_dict.is_none() &&
                       state.training_buffer.len() >= min_samples &&
                       total_bytes >= min_total_bytes &&
                       !state.training_in_progress {
                        info!("Threshold reached. Starting async dictionary training...");
                        state.training_in_progress = true;
                        let samples = state.training_buffer.clone();
                        drop(state);
                        try_train_dictionary_async(samples);
                    }
                } else {
                    info!("Sample too small ({} bytes), skipping collection", bincode_data.len());
                }
            }
        }

        let compression_enabled = env::var("FUSELOG_COMPRESSION")
            .map_or(false, |val| val.to_lowercase() == "true" || val == "1");

        let final_payload = if compression_enabled && !bincode_data.is_empty() {
            if adaptive_enabled {
                let state = ADAPTIVE_STATE.lock().unwrap();

                if let Some(dict_arc) = state.encoder_dict.as_ref().map(Arc::clone) {
                    drop(state);

                    let normal_compressed = zstd::encode_all(&bincode_data[..], COMPRESSION_LEVEL)?;
                    let dict_compressed = {
                        let mut compressor = zstd::bulk::Compressor::with_dictionary(COMPRESSION_LEVEL, &dict_arc)?;
                        compressor.compress(&bincode_data)?
                    };

                    if dict_compressed.len() < normal_compressed.len() {
                        info!("Dictionary compression chosen: {} bytes vs {} bytes (normal)",
                              dict_compressed.len(), normal_compressed.len());
                        
                        let mut state = ADAPTIVE_STATE.lock().unwrap();
                        let should_include_dict = state.new_dict_needs_sending;
                        
                        if should_include_dict {
                            info!("Including dictionary in payload (first time after training)");
                            state.new_dict_needs_sending = false;
                            let mut payload = Vec::new();
                            payload.push(b'd');
                            payload.extend_from_slice(&(dict_arc.len() as u32).to_le_bytes());
                            payload.extend_from_slice(&dict_arc);
                            payload.push(b'z');
                            payload.extend(dict_compressed);
                            payload
                        } else {
                            info!("Not including dictionary in payload (must have been sent before)");
                            let mut payload = Vec::with_capacity(1 + dict_compressed.len());
                            payload.push(b'z');
                            payload.extend(dict_compressed);
                            payload
                        }
                    } else {
                        info!("Normal compression chosen: {} bytes vs {} bytes (dictionary)",
                              normal_compressed.len(), dict_compressed.len());
                        let mut payload = Vec::with_capacity(1 + normal_compressed.len());
                        payload.push(b'z');
                        payload.extend(normal_compressed);
                        payload
                    }
                } else {
                    info!("Adaptive mode enabled but no dictionary trained yet. Using normal compression.");
                    let compressed_data = zstd::encode_all(&bincode_data[..], COMPRESSION_LEVEL)?;
                    let mut payload = Vec::with_capacity(1 + compressed_data.len());
                    payload.push(b'z');
                    payload.extend(compressed_data);
                    payload
                }
            } else {
                info!("Standard compression enabled.");
                let compressed_data = zstd::encode_all(&bincode_data[..], COMPRESSION_LEVEL)?;
                info!("Data compressed from {} to {} bytes.", bincode_data.len(), compressed_data.len());
                let mut payload = Vec::with_capacity(1 + compressed_data.len());
                payload.push(b'z');
                payload.extend(compressed_data);
                payload
            }
        } else {
            info!("Compression is disabled or data is empty. Sending raw data.");
            let mut payload = Vec::with_capacity(1 + bincode_data.len());
            payload.push(b'n');
            payload.extend(bincode_data);
            payload
        };

        let action_count = log.actions.len();
        let fid_count = log.fid_map.len();
        log.actions.clear();
        log.fid_map.clear();

        info!("Socket: Original statediff log had {} actions, {} fids. Pruned to {} actions, {} fids.",
            original_action_count, original_fid_count, action_count, fid_count);

        final_payload
    };

    stream.write_all(&serialized_data.len().to_le_bytes())?;

    stream.write_all(&serialized_data)?;
    info!("Socket: Successfully sent data to client");
    Ok(())
}

fn clear_statediff() -> Result<(), Box<dyn std::error::Error>> {
    info!("Socket: Received 'clear' command");

    let mut log = STATEDIFF_LOG.lock().map_err(|e| {
        error!("Socket: Failed to lock statediff log: {}", e);
        std::io::Error::new(std::io::ErrorKind::Other, "Lock poisoned")
    })?;

    let action_count = log.actions.len();
    let fid_count = log.fid_map.len();
    log.actions.clear();
    log.fid_map.clear();

    info!("Socket: Cleared statediff log (had {} actions, {} fids).", 
        action_count, fid_count);

    Ok(())
}