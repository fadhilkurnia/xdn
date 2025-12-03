pub mod socket;
pub mod statediff;

use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, ReplyCreate, Request, TimeOrNow,
};
use libc::{ENOENT, EIO, EEXIST};
use log::{debug, info, error, warn, trace};
use bincode::{config, encode_to_vec};
use statediff::{StateDiffAction, StateDiffLog};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH, SystemTime};
use std::io::{Read, Seek, SeekFrom, Write, ErrorKind};
use std::fs::{File, OpenOptions};

const TTL: Duration = Duration::from_secs(1);

static STATEDIFF_LOG: once_cell::sync::Lazy<Arc<Mutex<StateDiffLog>>> = once_cell::sync::Lazy::new(|| Arc::new(Mutex::new(StateDiffLog::default())));

fn get_fid(log: &mut StateDiffLog, path: &str) -> u64 {
    if let Some((fid, _)) = log.fid_map.iter().find(|(_, p)| p == &path) {
        return *fid;
    }
    
    let new_fid = log.fid_map.len() as u64 + 1;
    log.fid_map.insert(new_fid, path.to_string());
    new_fid
}

fn metadata_to_file_attr(ino: u64, metadata: &std::fs::Metadata) -> FileAttr {
    let file_type = if metadata.is_dir() {
        FileType::Directory
    } else if metadata.is_file() {
        FileType::RegularFile
    } else if metadata.file_type().is_symlink() {
        FileType::Symlink
    } else {
        FileType::RegularFile 
    };

    FileAttr {
        ino,
        size: metadata.len(),
        blocks: metadata.blocks(),
        atime: metadata.accessed().unwrap_or(UNIX_EPOCH),
        mtime: metadata.modified().unwrap_or(UNIX_EPOCH),
        ctime: SystemTime::UNIX_EPOCH + Duration::from_secs(metadata.ctime() as u64),
        crtime: metadata.created().unwrap_or(UNIX_EPOCH),
        kind: file_type,
        perm: (metadata.mode() & 0o7777) as u16,
        nlink: metadata.nlink() as u32,
        uid: metadata.uid(),
        gid: metadata.gid(),
        rdev: metadata.rdev() as u32,
        flags: 0,
        blksize: metadata.blksize() as u32,
    }
}

struct InodeManager {
    ino_to_path: HashMap<u64, PathBuf>,
    path_to_ino: HashMap<PathBuf, u64>,
    next_ino: u64,
}

impl InodeManager {
    fn new() -> Self {
        let mut manager = Self {
            ino_to_path: HashMap::new(),
            path_to_ino: HashMap::new(),
            next_ino: 2,
        };
        
        let root_path = PathBuf::from(".");
        manager.ino_to_path.insert(1, root_path.clone());
        manager.path_to_ino.insert(root_path, 1);
        
        manager
    }
    
    fn get_path(&self, ino: u64) -> Option<&PathBuf> {
        self.ino_to_path.get(&ino)
    }
    
    fn get_or_create_ino(&mut self, path: &Path) -> u64 {
        if let Some(&ino) = self.path_to_ino.get(path) {
            return ino;
        }
        
        let ino = self.next_ino;
        self.next_ino += 1;
        self.ino_to_path.insert(ino, path.to_path_buf());
        self.path_to_ino.insert(path.to_path_buf(), ino);
        ino
    }
    
    fn remove_path(&mut self, path: &Path) -> Option<u64> {
        if let Some(ino) = self.path_to_ino.remove(path) {
            self.ino_to_path.remove(&ino);
            Some(ino)
        } else {
            None
        }
    }
}

pub struct FuseLogFS {
    inodes: Mutex<InodeManager>,
    write_coalescing: bool,
}

impl FuseLogFS {
    pub fn new(_root: PathBuf) -> Self {

        // Write coalesing is disabled by default
        let coalescing_enabled = std::env::var("WRITE_COALESCING")
            .map_or(false, |val| val.to_lowercase() == "true" || val == "1");

        if coalescing_enabled {
            info!("Write coalescing is enabled.");
        } else {
            info!("Write coalescing is disabled.");
        }

        Self {
            inodes: Mutex::new(InodeManager::new()),
            write_coalescing : coalescing_enabled,
        }
    }
    
    fn get_relative_path(&self, full_path: &Path) -> String {
        full_path.strip_prefix("./").unwrap_or(full_path).to_string_lossy().to_string()
    }
}

impl Filesystem for FuseLogFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("lookup(parent={}, name={:?})", parent, name);
        
        let mut inodes = self.inodes.lock().unwrap();
        
        let parent_path = match inodes.get_path(parent) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        
        let child_path = parent_path.join(name);
        
        // Use symlink_metadata to avoid following symlinks
        match std::fs::symlink_metadata(&child_path) {
            Ok(metadata) => {
                let ino = inodes.get_or_create_ino(&child_path);
                let attrs = metadata_to_file_attr(ino, &metadata);
                reply.entry(&TTL, &attrs, 0);
            }
            Err(_) => reply.error(ENOENT),
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        debug!("getattr(ino={})", ino);
        
        let inodes = self.inodes.lock().unwrap();
        
        let path = match inodes.get_path(ino) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        
        // Use symlink_metadata to avoid following symlinks
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) => {
                let attrs = metadata_to_file_attr(ino, &metadata);
                reply.attr(&TTL, &attrs);
            }
            Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        debug!("readlink(ino={})", ino);
        let inodes = self.inodes.lock().unwrap();
        let path = match inodes.get_path(ino) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        match std::fs::read_link(&path) {
            Ok(target_path) => {
                reply.data(target_path.as_os_str().as_encoded_bytes());
            }
            Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        debug!("readdir(ino={}, offset={})", ino, offset);
        
        let mut inodes = self.inodes.lock().unwrap();
        
        let path = match inodes.get_path(ino) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        
        let mut entries = vec![];
        
        entries.push((ino, FileType::Directory, ".".to_string()));
        let parent_ino = if path == Path::new(".") {
            1
        } else {
            path.parent()
                .and_then(|p| inodes.path_to_ino.get(p))
                .copied()
                .unwrap_or(1)
        };
        entries.push((parent_ino, FileType::Directory, "..".to_string()));
        
        if let Ok(dir_iter) = std::fs::read_dir(&path) {
            for entry in dir_iter.filter_map(Result::ok) {
                let entry_path = if path == Path::new(".") {
                    PathBuf::from(entry.file_name())
                } else {
                    path.join(entry.file_name())
                };
                
                let entry_ino = inodes.get_or_create_ino(&entry_path);
                
                let file_type = if entry.file_type().map_or(false, |ft| ft.is_dir()) {
                    FileType::Directory
                } else if entry.file_type().map_or(false, |ft| ft.is_symlink()) {
                    FileType::Symlink
                } else {
                    FileType::RegularFile
                };
                
                if let Some(name) = entry.file_name().to_str() {
                    entries.push((entry_ino, file_type, name.to_string()));
                }
            }
        }
        
        for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(ino, (i + 1) as i64, kind, &name) {
                break;
            }
        }
        reply.ok();
    }

    fn mkdir(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, _umask: u32, reply: ReplyEntry) {
        debug!("mkdir(parent={}, name={:?}, mode={:o}, uid={}, gid={})", parent, name, mode, req.uid(), req.gid());
        
        let mut inodes = self.inodes.lock().unwrap();
        
        let parent_path = match inodes.get_path(parent) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        
        let dir_path = parent_path.join(name);
        let relative_path = self.get_relative_path(&dir_path);
        
        if dir_path.exists() {
            reply.error(EEXIST);
            return;
        }
        
        match std::fs::create_dir(&dir_path) {
            Ok(_) => {
                if let Err(e) = std::fs::set_permissions(&dir_path, std::fs::Permissions::from_mode(mode)) {
                    warn!("Warning: failed to set directory permissions: {}", e);
                }

                if let Err(e) = std::os::unix::fs::chown(&dir_path, Some(req.uid()), Some(req.gid())) {
                    error!("Failed to chown new directory {:?}: {}. Cleaning up.", &dir_path, e);
                    let _ = std::fs::remove_dir(&dir_path);
                    reply.error(e.raw_os_error().unwrap_or(EIO));
                    return;
                }
                
                let ino = inodes.get_or_create_ino(&dir_path);
                
                {
                    let mut log = STATEDIFF_LOG.lock().unwrap();
                    let fid = get_fid(&mut log, &relative_path);
                    log.actions.push(StateDiffAction::Mkdir { fid });
                    log.actions.push(StateDiffAction::Chown { fid, uid: req.uid(), gid: req.gid() });
                }
                info!("Created and logged directory: {:?} with owner {}:{}", dir_path, req.uid(), req.gid());

                match std::fs::metadata(&dir_path) {
                    Ok(metadata) => {
                        let attrs = metadata_to_file_attr(ino, &metadata);
                        reply.entry(&TTL, &attrs, 0);
                    }
                    Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
                }
            }
            Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("rmdir(parent={}, name={:?})", parent, name);
        
        let mut inodes = self.inodes.lock().unwrap();
        
        let parent_path = match inodes.get_path(parent) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        
        let dir_path = parent_path.join(name);
        let relative_path = self.get_relative_path(&dir_path);
        
        match std::fs::remove_dir(&dir_path) {
            Ok(_) => {
                if let Some(ino) = inodes.remove_path(&dir_path) {
                     debug!("Removed inode {} for path {:?}", ino, dir_path);
                }
                {
                    let mut log = STATEDIFF_LOG.lock().unwrap();
                    let fid = get_fid(&mut log, &relative_path);
                    log.actions.push(StateDiffAction::Rmdir { fid });
                }
                info!("Removed and logged directory: {:?}", dir_path);
                reply.ok();
            }
            Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
        }
    }

    fn symlink(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, link: &Path, reply: ReplyEntry) {
        debug!("symlink(parent={}, name={:?}, target={:?})", parent, name, link);

        let mut inodes = self.inodes.lock().unwrap();

        let parent_path = match inodes.get_path(parent) {
            Some(p) => p.clone(),
            None => { reply.error(ENOENT); return; }
        };

        let link_path = parent_path.join(name);
        let relative_link_path = self.get_relative_path(&link_path);
        let target_path_str = link.to_string_lossy().to_string();

        match std::os::unix::fs::symlink(link, &link_path) {
            Ok(_) => {
                // Use lchown to set ownership of the link itself, not the target
                if let Err(e) = std::os::unix::fs::chown(&link_path, Some(req.uid()), Some(req.gid())) {
                    error!("Failed to chown new symlink {:?}: {}. Cleaning up.", &link_path, e);
                    let _ = std::fs::remove_file(&link_path);
                    reply.error(e.raw_os_error().unwrap_or(EIO));
                    return;
                }

                let ino = inodes.get_or_create_ino(&link_path);
                
                {
                    let mut log = STATEDIFF_LOG.lock().unwrap();
                    let link_fid = get_fid(&mut log, &relative_link_path);
                    log.actions.push(StateDiffAction::Symlink {
                        link_fid,
                        target_path: target_path_str,
                        uid: req.uid(),
                        gid: req.gid(),
                    });
                }
                info!("Created and logged symlink: {:?} -> {:?}", link_path, link);

                // Use symlink_metadata to get attributes of the link itself
                match std::fs::symlink_metadata(&link_path) {
                    Ok(metadata) => {
                        let attrs = metadata_to_file_attr(ino, &metadata);
                        reply.entry(&TTL, &attrs, 0);
                    }
                    Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
                }
            }
            Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        debug!("open(ino={})", ino);

        let mut open_flag = 0;

        if (flags & libc::O_DIRECT as i32) != 0 {
            info!("O_DIRECT flag detected for ino {}, enabling FOPEN_DIRECT_IO", ino);
            open_flag |= fuser::consts::FOPEN_DIRECT_IO;
        }

        reply.opened(0, open_flag);
        trace!("open(ino={}) - EXIT (OK)", ino);
    }

    fn create(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, _umask: u32, flags: i32, reply: ReplyCreate) {
        trace!("create(parent={}, name={:?}, flags=0x{:x}) - ENTER", parent, name, flags);
        
        let mut inodes = self.inodes.lock().unwrap();
        
        let parent_path = match inodes.get_path(parent) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                trace!("create({:?}) - EXIT (ENOENT parent)", name);
                return;
            }
        };
        
        let file_path = parent_path.join(name);
        let relative_path = self.get_relative_path(&file_path);

        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true);

        if (flags & libc::O_EXCL as i32) != 0 {
            // O_EXCL (fail if file already exists)
            options.create_new(true); 
        } else if (flags & libc::O_TRUNC as i32) != 0 {
            options.truncate(true);
        }

        match options.open(&file_path) {
            Ok(_file) => { 
                if let Err(e) = std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(mode)) {
                    warn!("Warning: failed to set file permissions for {:?}: {}", &file_path, e);
                }

                if let Err(e) = std::os::unix::fs::chown(&file_path, Some(req.uid()), Some(req.gid())) {
                    error!("Failed to chown new file {:?}: {}. Cleaning up.", &file_path, e);
                    let _ = std::fs::remove_file(&file_path);
                    reply.error(e.raw_os_error().unwrap_or(EIO));
                    trace!("create({:?}) - EXIT (EIO on chown)", name);
                    return;
                }

                let ino = inodes.get_or_create_ino(&file_path);
                
                {
                    let mut log = STATEDIFF_LOG.lock().unwrap();
                    let fid = get_fid(&mut log, &relative_path);
                    log.actions.push(StateDiffAction::Create { 
                        fid, 
                        uid: req.uid(), 
                        gid: req.gid(),
                        mode,
                    });
                }
                info!("Logged create for file: {:?} with owner {}:{}", file_path, req.uid(), req.gid());

                if let Ok(metadata) = std::fs::metadata(&file_path) {
                    let attrs = metadata_to_file_attr(ino, &metadata);
                    
                    // FIX : Handle O_DIRECT flag correctly
                    let mut open_flags = 0;
                    if (flags & libc::O_DIRECT as i32) != 0 {
                        info!("O_DIRECT flag detected on create for {:?}, enabling FOPEN_DIRECT_IO", file_path);
                        open_flags |= fuser::consts::FOPEN_DIRECT_IO;
                    }
                    
                    reply.created(&TTL, &attrs, 0, 0, open_flags);
                    trace!("create({:?}) - EXIT (OK)", name);
                } else {
                    reply.error(EIO);
                    trace!("create({:?}) - EXIT (EIO on metadata)", name);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                reply.error(EEXIST);
                trace!("create({:?}) - EXIT (EEXIST)", name);
            }
            Err(e) => {
                reply.error(e.raw_os_error().unwrap_or(EIO));
                trace!("create({:?}) - EXIT (EIO on open)", name);
            },
        }
    }

    fn read(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, size: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyData) {
        debug!("read(ino={}, offset={}, size={})", ino, offset, size);
        
        let inodes = self.inodes.lock().unwrap();
        
        let path = match inodes.get_path(ino) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        
        match File::open(&path) {
            Ok(mut file) => {
                if let Err(e) = file.seek(SeekFrom::Start(offset as u64)) {
                    reply.error(e.raw_os_error().unwrap_or(EIO));
                    return;
                }
                
                let mut buffer = vec![0u8; size as usize];
                match file.read(&mut buffer) {
                    Ok(bytes_read) => {
                        reply.data(&buffer[..bytes_read]);
                    }
                    Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
                }
            }
            Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
        }
    }

    fn write(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, data: &[u8], _write_flags: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyWrite) {
        debug!("write(ino={}, offset={}, size={}, coalescing={})", ino, offset, data.len(), self.write_coalescing);

        let inodes = self.inodes.lock().unwrap();
        let path = match inodes.get_path(ino) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        if self.write_coalescing {
            // 1. Read the old data
            let old_data: Vec<u8> = match File::open(&path) {
                Ok(mut old_file) => {
                    // Seek the offset
                    if old_file.seek(SeekFrom::Start(offset as u64)).is_ok() {
                        let mut buffer = vec![0; data.len()];
                        match old_file.read(&mut buffer) {
                            Ok(bytes_read) => {
                                buffer.truncate(bytes_read);
                                buffer
                            }
                            Err(_) => Vec::new(),
                        }
                    } else {
                        Vec::new()
                    }
                }
                Err(e) if e.kind() == ErrorKind::NotFound => Vec::new(),
                Err(_) => Vec::new(),
            };

            // 2. Perform the actual write to the underlying filesystem.
            match OpenOptions::new().write(true).create(true).open(&path) {
                Ok(mut file) => {
                    if let Err(e) = file.seek(SeekFrom::Start(offset as u64)) {
                        reply.error(e.raw_os_error().unwrap_or(EIO));
                        return;
                    }

                    match file.write_all(data) {
                        Ok(_) => {

                            // Compute per action overhead using an empty write action
                            let fid_for_overhead = {
                                let mut log = STATEDIFF_LOG.lock().unwrap();
                                get_fid(&mut log, &self.get_relative_path(&path))
                            };
                            let overhead_probe = StateDiffAction::Write {
                                fid: fid_for_overhead,
                                offset: 0,
                                data: Vec::new(),
                            };
                            let overhead_bytes = encode_to_vec(&overhead_probe, config::standard())
                                .map(|v| v.len())
                                .unwrap_or(0)
                                .max(1); 

                            // 3. Comparing old and new data to find and log differences.
                            let mut coalesced_writes = Vec::new();
                            let mut i = 0;
                            while i < data.len() {
                                let old_byte = old_data.get(i);
                                let new_byte = data[i];
        
                                if old_byte.map_or(true, |&b| b != new_byte) {
                                    let chunk_start_index = i;
                                    let mut chunk_data = vec![new_byte];
                                    i += 1;

                                    while i < data.len() {
                                        let next_old_byte = old_data.get(i);
                                        let next_new_byte = data[i];
                                        if next_old_byte.map_or(true, |&b| b != next_new_byte) {
                                            chunk_data.push(next_new_byte);
                                            i += 1;
                                        } else {
                                            let mut match_run = 1;
                                            while i + match_run < data.len() {
                                                let further_old = old_data.get(i + match_run);
                                                let further_new = data[i + match_run];
                                                if further_old.map_or(true, |&b| b != further_new) {
                                                    break;
                                                }
                                                match_run += 1;
                                            }

                                            if match_run < overhead_bytes {
                                                for k in 0..match_run {
                                                    chunk_data.push(data[i + k]);
                                                }
                                                i += match_run;
                                                continue;
                                            } else {
                                                break;
                                            }
                                        }
                                    }
                                    
                                    coalesced_writes.push((
                                        offset as u64 + chunk_start_index as u64,
                                        chunk_data,
                                    ));
                                } else {
                                    i += 1;
                                }
                            }

                            if !coalesced_writes.is_empty() {
                                let total_coalesced_bytes = coalesced_writes.iter().map(|(_, d)| d.len()).sum::<usize>();
                                info!(
                                    "Coalesced write of {} bytes into {} chunk(s) ({} total bytes) for {:?}",
                                    data.len(),
                                    coalesced_writes.len(),
                                    total_coalesced_bytes,
                                    &path
                                );

                                let relative_path = self.get_relative_path(&path);
                                let mut log = STATEDIFF_LOG.lock().unwrap();
                                let fid = get_fid(&mut log, &relative_path);

                                for (chunk_offset, chunk_data) in coalesced_writes {
                                    log.actions.push(StateDiffAction::Write {
                                        fid,
                                        offset: chunk_offset,
                                        data: chunk_data,
                                    });
                                }
                            } else {
                                info!("Redundant write to {:?} (no changes detected), not logging.", &path);
                            }
                            
                            reply.written(data.len() as u32);
                        }
                        Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
                    }
                }
                Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
            }
        } else {
            info!("Write coalescing disabled. Logging full write of {} bytes to {:?}", data.len(), &path);

            match OpenOptions::new().write(true).create(true).open(&path) {
                Ok(mut file) => {
                    if let Err(e) = file.seek(SeekFrom::Start(offset as u64)) {
                        reply.error(e.raw_os_error().unwrap_or(EIO));
                        return;
                    }

                    match file.write_all(data) {
                        Ok(_) => {
                            let relative_path = self.get_relative_path(&path);
                            let mut log = STATEDIFF_LOG.lock().unwrap();
                            let fid = get_fid(&mut log, &relative_path);
                            
                            log.actions.push(StateDiffAction::Write {
                                fid,
                                offset: offset as u64,
                                data: data.to_vec(),
                            });

                            reply.written(data.len() as u32);
                        }
                        Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
                    }
                }
                Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
            }
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("unlink(parent={}, name={:?})", parent, name);
        
        let mut inodes = self.inodes.lock().unwrap();
        
        let parent_path = match inodes.get_path(parent) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        
        let file_path = parent_path.join(name);
        let relative_path = self.get_relative_path(&file_path);
        
        match std::fs::remove_file(&file_path) {
            Ok(_) => {
                if let Some(ino) = inodes.remove_path(&file_path) {
                    debug!("Removed inode {} for path {:?}", ino, file_path);
                }
                let mut log = STATEDIFF_LOG.lock().unwrap();
                let fid = get_fid(&mut log, &relative_path);
                log.actions.push(StateDiffAction::Unlink { fid });
                info!("Unlinked and logged file: {:?}", file_path);
                reply.ok();
            }
            Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
        }
    }

    fn setattr(&mut self, _req: &Request<'_>, ino: u64, mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, size: Option<u64>, _atime: Option<TimeOrNow>, _mtime: Option<TimeOrNow>, _ctime: Option<SystemTime>, _fh: Option<u64>, _crtime: Option<SystemTime>, _chgtime: Option<SystemTime>, _bkuptime: Option<SystemTime>, _flags: Option<u32>, reply: ReplyAttr) {
        debug!("setattr(ino={}, mode={:?}, uid={:?}, gid={:?}, size={:?})", ino, mode, uid, gid, size);
    
        let inodes = self.inodes.lock().unwrap();
        let path = match inodes.get_path(ino) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let relative_path = self.get_relative_path(&path);

        if let Some(new_mode) = mode {
            match std::fs::set_permissions(&path, std::fs::Permissions::from_mode(new_mode)) {
                Ok(_) => {
                    let mut log = STATEDIFF_LOG.lock().unwrap();
                    let fid = get_fid(&mut log, &relative_path);
                    log.actions.push(StateDiffAction::Chmod { fid, mode: new_mode });
                    info!("Logged chmod for {:?} to {:o}", path, new_mode);
                }
                Err(e) => {
                    error!("Failed to chmod {:?} to {:o}: {}", path, new_mode, e);
                    reply.error(e.raw_os_error().unwrap_or(EIO));
                    return;
                }
            }
        }

        if let Some(new_size) = size {
            match std::fs::OpenOptions::new().write(true).open(&path) {
                Ok(file) => {
                    if let Err(e) = file.set_len(new_size) {
                        reply.error(e.raw_os_error().unwrap_or(EIO));
                        return;
                    }
                    let mut log = STATEDIFF_LOG.lock().unwrap();
                    let fid = get_fid(&mut log, &relative_path);
                    log.actions.push(StateDiffAction::Truncate { fid, size: new_size });
                    info!("Logged truncate for {:?} to {}", path, new_size);
                }
                Err(e) => {
                    reply.error(e.raw_os_error().unwrap_or(EIO));
                    return;
                }
            }
        }

        if uid.is_some() || gid.is_some() {
            let current_meta = match std::fs::symlink_metadata(&path) {
                Ok(meta) => meta,
                Err(e) => {
                     reply.error(e.raw_os_error().unwrap_or(EIO));
                     return;
                }
            };
            let final_uid = uid.unwrap_or_else(|| current_meta.uid());
            let final_gid = gid.unwrap_or_else(|| current_meta.gid());
            
            // Use lchown for symlinks, chown for other file types
            let chown_result = if current_meta.file_type().is_symlink() {
                std::os::unix::fs::lchown(&path, Some(final_uid), Some(final_gid))
            } else {
                std::os::unix::fs::chown(&path, Some(final_uid), Some(final_gid))
            };

            match chown_result {
                Ok(_) => {
                    let mut log = STATEDIFF_LOG.lock().unwrap();
                    let fid = get_fid(&mut log, &relative_path);
                    log.actions.push(StateDiffAction::Chown { fid, uid: final_uid, gid: final_gid });
                    info!("Logged chown for {:?} to {}:{}", path, final_uid, final_gid);
                }
                Err(e) => {
                    error!("Failed to chown {:?} to uid={:?}, gid={:?}: {}", path, uid, gid, e);
                    reply.error(e.raw_os_error().unwrap_or(EIO));
                    return;
                }
            }
        }
    
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) => {
                let attrs = metadata_to_file_attr(ino, &metadata);
                reply.attr(&TTL, &attrs);
            }
            Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
        }
    }    

    fn release(&mut self, _req: &Request, _ino: u64, _fh: u64, _flags: i32, _lock_owner: Option<u64>, _flush: bool, reply: ReplyEmpty) {
        debug!("release called");
        reply.ok();
    }

    fn flush(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        debug!("flush called");
        reply.ok();
    }

    fn rename(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, newparent: u64, newname: &OsStr, _flags: u32, reply: ReplyEmpty) {
        debug!("rename(parent={}, name={:?}, newparent={}, newname={:?})", parent, name, newparent, newname);

        let mut inodes = self.inodes.lock().unwrap();

        let from_parent_path = match inodes.get_path(parent) {
            Some(p) => p.clone(),
            None => { reply.error(ENOENT); return; }
        };
        let from_path = from_parent_path.join(name);

        let to_parent_path = match inodes.get_path(newparent) {
            Some(p) => p.clone(),
            None => { reply.error(ENOENT); return; }
        };
        let to_path = to_parent_path.join(newname);

        match std::fs::rename(&from_path, &to_path) {
            Ok(_) => {
                if let Some(ino) = inodes.remove_path(&from_path) {
                    inodes.ino_to_path.insert(ino, to_path.clone());
                    inodes.path_to_ino.insert(to_path.clone(), ino);
                    info!("Updated inode mapping: ino {} from {:?} to {:?}", ino, from_path, to_path);
                }

                let relative_from_path = self.get_relative_path(&from_path);
                let relative_to_path = self.get_relative_path(&to_path);

                let mut log = STATEDIFF_LOG.lock().unwrap();
                let from_fid = get_fid(&mut log, &relative_from_path);
                let to_fid = get_fid(&mut log, &relative_to_path);

                log.actions.push(StateDiffAction::Rename { from_fid, to_fid });
                info!("Renamed {:?} to {:?}, logging action", from_path, to_path);

                reply.ok();
            }
            Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
        }
    }

    fn link(&mut self, _req: &Request<'_>, ino: u64, newparent: u64, newname: &OsStr, reply: ReplyEntry) {
        debug!("link(ino={}, newparent={}, newname={:?})", ino, newparent, newname);

        let mut inodes = self.inodes.lock().unwrap();

        let source_path = match inodes.get_path(ino) {
            Some(p) => p.clone(),
            None => { reply.error(ENOENT); return; }
        };

        let dest_parent_path = match inodes.get_path(newparent) {
            Some(p) => p.clone(),
            None => { reply.error(ENOENT); return; }
        };

        let dest_path = dest_parent_path.join(newname);

        match std::fs::hard_link(&source_path, &dest_path) {
            Ok(_) => {
                info!("Created hard link from {:?} to {:?}", source_path, dest_path);
                
                inodes.path_to_ino.insert(dest_path.clone(), ino);

                let relative_source_path = self.get_relative_path(&source_path);
                let relative_dest_path = self.get_relative_path(&dest_path);
                
                let mut log = STATEDIFF_LOG.lock().unwrap();
                let source_fid = get_fid(&mut log, &relative_source_path);
                let new_link_fid = get_fid(&mut log, &relative_dest_path);
                
                log.actions.push(StateDiffAction::Link { source_fid, new_link_fid });

                match std::fs::metadata(&dest_path) {
                    Ok(metadata) => {
                        let attrs = metadata_to_file_attr(ino, &metadata);
                        reply.entry(&TTL, &attrs, 0);
                    }
                    Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
                }
            }
            Err(e) => reply.error(e.raw_os_error().unwrap_or(EIO)),
        }
    }

    fn fsync(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, datasync: bool, reply: ReplyEmpty) {
        // I think I don't need to log it in the StateDiffLog
        // because fsync doesn't change file content or metadata;
        debug!("fsync(ino={}, datasync={})", ino, datasync);

        let inodes = self.inodes.lock().unwrap();
        let path = match inodes.get_path(ino) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                warn!("fsync called on non-existent file (ino {}): {:?}", ino, path);
                reply.error(ENOENT);
                return;
            }
            Err(e) => {
                error!("Failed to open file for fsync {:?}: {}", path, e);
                reply.error(e.raw_os_error().unwrap_or(EIO));
                return;
            }
        };

        let sync_result = if datasync {
            // Corresponds to fdatasync(): syncs file data, but not necessarily metadata.
            file.sync_data()
        } else {
            // Corresponds to fsync(): syncs both file data and metadata.
            file.sync_all()
        };

        match sync_result {
            Ok(_) => {
                info!("Successfully synced file: {:?}", path);
                reply.ok();
            }
            Err(e) => {
                error!("Failed to sync file {:?}: {}", path, e);
                reply.error(e.raw_os_error().unwrap_or(EIO));
            }
        }
    }

}
