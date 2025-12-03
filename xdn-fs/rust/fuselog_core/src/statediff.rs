use bincode::{Decode, Encode};
use std::collections::HashMap;

// Make sure it schema based serialization
// Maybe try cap and proto  
// Only capture operation that change the state  

#[derive(Encode, Decode, Debug)]
pub enum StateDiffAction {
    Create {
        fid: u64,
        uid: u32,
        gid: u32,
        mode: u32,
    },
    Write {
        fid: u64,
        offset: u64,
        data: Vec<u8>,
    },
    Unlink {
        fid: u64,
    },
    Rename {
        from_fid: u64,
        to_fid: u64,
    },
    Truncate {
        fid: u64,
        size: u64,
    },
    Link {
        source_fid: u64,
        new_link_fid: u64,
    },
    Chown {
        fid: u64,
        uid: u32,
        gid: u32,
    },
    Chmod {
        fid: u64,
        mode: u32,
    },
    Mkdir {
        fid: u64,
    },
    Rmdir {
        fid: u64,
    },
    Symlink {
        link_fid: u64,
        target_path: String,
        uid: u32,
        gid: u32,
    },
}

#[derive(Encode, Decode, Debug, Default)]
pub struct StateDiffLog {
    pub fid_map: HashMap<u64, String>,
    pub actions: Vec<StateDiffAction>,
}