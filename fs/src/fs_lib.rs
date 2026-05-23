use fuser::{Filesystem, MountOption};
use std::path::Path;

pub struct NifsiFS;

impl Filesystem for NifsiFS {
}

pub fn mount_fs<P: AsRef<Path>>(_mountpoint: P) -> Result<(), std::io::Error> {
    let _options=vec![MountOption::FSName("nifsi".to_string())];
    Ok(())
}

