use fuser::MountOption;
use fuselog_core::socket::start_listener;
use fuselog_core::FuseLogFS;
use std::path::PathBuf;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::fs::File;
use daemonize::Daemonize;

static SHUTDOWN_FLAG: AtomicBool = AtomicBool::new(false);

const SOCKET_PATH: &str = "/tmp/fuselog.sock";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    
    let foreground = args.iter().any(|arg| arg == "-f" || arg == "--foreground");
    let multi_threaded = args.iter().any(|arg| arg == "--multi-threaded");

    let filtered_args: Vec<String> = args.into_iter()
        .filter(|arg| arg != "-f" && arg != "--foreground" && arg != "--multi-threaded")
        .collect();
    
    if filtered_args.len() != 2 {
        eprintln!("Usage: {} [-f|--foreground] [--multi-threaded] <directory>", filtered_args[0]);
        std::process::exit(1);
    }

    let root_dir = PathBuf::from(&filtered_args[1]);

    if !root_dir.exists() {
        if let Err(e) = std::fs::create_dir_all(&root_dir) {
            eprintln!("Failed to create directory '{}': {}", root_dir.display(), e);
            std::process::exit(1);
        }
        println!("Created directory: {}", root_dir.display());
    } else if !root_dir.is_dir() {
        eprintln!("Path '{}' exists but is not a directory", root_dir.display());
        std::process::exit(1);
    }

    if foreground {
        env_logger::init();
        log::info!("Starting Fuselog in foreground mode on directory: '{}' (multi-threaded: {})", root_dir.display(), multi_threaded);
        let exit_code = run_fuse_logic(root_dir, multi_threaded);
        std::process::exit(exit_code);
    } else {
        // Check if daemon logs are enabled (default: false)
        let enable_daemon_logs = env::var("FUSELOG_DAEMON_LOGS")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        let (stdout, stderr) = if enable_daemon_logs {
            // Try to create log files with better error handling
            let stdout = match std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open("/tmp/fuselog.out")
            {
                Ok(file) => file,
                Err(e) => {
                    eprintln!("Warning: Failed to create /tmp/fuselog.out: {}. Falling back to /dev/null", e);
                    File::create("/dev/null").expect("Failed to open /dev/null")
                }
            };

            let stderr = match std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open("/tmp/fuselog.err")
            {
                Ok(file) => file,
                Err(e) => {
                    eprintln!("Warning: Failed to create /tmp/fuselog.err: {}. Falling back to /dev/null", e);
                    File::create("/dev/null").expect("Failed to open /dev/null")
                }
            };

            (stdout, stderr)
        } else {
            // Default: redirect to /dev/null
            let devnull_out = File::create("/dev/null").expect("Failed to open /dev/null");
            let devnull_err = File::create("/dev/null").expect("Failed to open /dev/null");
            (devnull_out, devnull_err)
        };

        // let pid_file = format!("/tmp/fuselog_{}.pid",
        //     root_dir.to_string_lossy().replace("/", "_").replace(" ", "_"));

        let daemonize = Daemonize::new()
            // .pid_file(pid_file)
            // .chown_pid_file(true)
            .working_directory(&root_dir)
            .stdout(stdout)
            .stderr(stderr);

        match daemonize.start() {
            Ok(_) => {
                env_logger::init();
                log::info!("Successfully daemonized fuselog for directory: '{}' (multi-threaded: {})", root_dir.display(), multi_threaded);
                let exit_code = run_fuse_logic(root_dir, multi_threaded);
                std::process::exit(exit_code);
            }
            Err(e) => {
                eprintln!("Error daemonizing: {}", e);
                std::process::exit(1);
            }
        }
    }
}

fn run_fuse_logic(root_dir: PathBuf, multi_threaded: bool) -> i32 {
    log::info!("Starting Fuselog on directory: '{}' (multi-threaded: {})", root_dir.display(), multi_threaded);
    let (shutdown_tx, shutdown_rx) = mpsc::channel();

    let socket_file = env::var("FUSELOG_SOCKET_FILE").unwrap_or_else(|_| SOCKET_PATH.to_string());

    let listener_handle = thread::spawn({
        let socket_file = socket_file.clone();
        move || {
            if let Err(e) = start_listener(&socket_file[..], shutdown_rx) {
                log::error!("Failed to start socket listener: {}", e);
                std::process::exit(1);
            }
        }
    });

    if let Err(e) = std::env::set_current_dir(&root_dir) {
        log::error!("Failed to change directory to '{}': {}", root_dir.display(), e);
        std::process::exit(1);
    }

    let options = vec![
        MountOption::FSName("fuselog".to_string()),
        MountOption::AutoUnmount,
        MountOption::AllowOther,
        MountOption::DefaultPermissions,
    ];

    let fs = FuseLogFS::new(root_dir.clone());

    let exit_code = if multi_threaded {
        // Multi-threaded mode: spawn_mount2 returns a BackgroundSession that runs
        // FUSE in background threads, allowing concurrent callbacks.
        match fuser::spawn_mount2(fs, &root_dir, &options) {
            Ok(session) => {
                log::info!("FUSE filesystem mounted in multi-threaded mode. Waiting for unmount...");
                // Install signal handler for graceful shutdown
                unsafe {
                    extern "C" fn sig_handler(_: libc::c_int) {
                        SHUTDOWN_FLAG.store(true, Ordering::SeqCst);
                    }
                    libc::signal(libc::SIGINT, sig_handler as *const () as libc::sighandler_t);
                    libc::signal(libc::SIGTERM, sig_handler as *const () as libc::sighandler_t);
                }
                // Wait until signal received
                while !SHUTDOWN_FLAG.load(Ordering::SeqCst) {
                    thread::sleep(std::time::Duration::from_millis(100));
                }
                log::info!("Received shutdown signal, dropping FUSE session...");
                drop(session);
                0
            }
            Err(e) => {
                log::error!("Failed to mount FUSE filesystem (multi-threaded): {}", e);
                1
            }
        }
    } else {
        // Single-threaded mode (default): mount2 blocks on the current thread.
        match fuser::mount2(fs, &root_dir, &options) {
            Ok(_) => {
                log::info!("FUSE filesystem has been unmounted.");
                0
            }
            Err(e) => {
                log::error!("Failed to mount FUSE filesystem: {}", e);
                1
            }
        }
    };

    let _ = shutdown_tx.send(());

    if let Err(e) = listener_handle.join() {
        log::error!("Listener thread panicked: {:?}", e);
    }

    exit_code
}

