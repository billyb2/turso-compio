//! Platform filesystem capabilities, locks, metadata, and durability barriers.

use std::{
    io,
    os::{fd::AsRawFd, unix::fs::MetadataExt},
    sync::{Arc, atomic::AtomicU64},
};

use anyhow::{Context, ensure};
use compio::fs::{File, OpenOptions};
#[cfg(target_os = "linux")]
use compio::runtime::Runtime;
use turso::core::io::FileId;

pub(super) struct FileState {
    pub(super) path: String,
    pub(super) id: FileId,
    pub(super) size: AtomicU64,
}

#[cfg(target_os = "linux")]
pub(crate) fn check_native_filesystem_support() -> anyhow::Result<()> {
    Runtime::try_with_current(|runtime| {
        ensure!(
            runtime.driver_type().is_iouring(),
            "Turso requires CompIO's io_uring driver, not its blocking-pool filesystem fallback"
        );
        Ok(())
    })
    .map_err(|_| anyhow::anyhow!("Turso must be opened inside the CompIO runtime"))??;
    // CompIO does not expose its cached opcode probe, and silently sends
    // unsupported operations to its blocking pool. Probe first, before any
    // filesystem operation. This temporary ring never performs database I/O.
    let ring = io_uring::IoUring::new(1).context("probing native io_uring filesystem support")?;
    let mut probe = io_uring::Probe::new();
    ring.submitter().register_probe(&mut probe)?;
    use io_uring::opcode;
    for (name, code) in [
        ("OPENAT", opcode::OpenAt::CODE),
        ("STATX", opcode::Statx::CODE),
        ("READ", opcode::Read::CODE),
        ("WRITE", opcode::Write::CODE),
        ("FSYNC", opcode::Fsync::CODE),
        ("FTRUNCATE", opcode::Ftruncate::CODE),
        ("CLOSE", opcode::Close::CODE),
    ] {
        ensure!(
            probe.is_supported(code),
            "Turso requires native io_uring {name}; FTRUNCATE requires Linux 6.9+ (no blocking fallback permitted)"
        );
    }
    Ok(())
}

fn lock_exclusively(file: &File) -> anyhow::Result<()> {
    let mut lock = libc::flock {
        l_type: libc::F_WRLCK as _,
        l_whence: libc::SEEK_SET as _,
        l_start: 0,
        l_len: 0,
        l_pid: 0,
    };
    // SAFETY: only borrows the managed descriptor for this nonblocking
    // syscall. OFD locks conflict with SQLite/Turso's POSIX locks, including
    // locks acquired through other descriptors in this same process, and
    // are released by the owning descriptor's close, not unrelated closes.
    let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_SETLK, &mut lock) };
    if result == -1 {
        return Err(io::Error::last_os_error()).context("acquiring exclusive Turso file lock");
    }
    Ok(())
}

pub(super) async fn preopen(path: &str) -> anyhow::Result<(File, Arc<FileState>)> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .await
        .with_context(|| format!("opening Turso file {path}"))?;
    lock_exclusively(&file)?;
    let metadata = file.metadata().await?;
    ensure!(
        metadata.is_file(),
        "Turso path is not a regular file: {path}"
    );
    let state = Arc::new(FileState {
        path: path.to_owned(),
        id: FileId {
            dev: metadata.dev(),
            ino: metadata.ino(),
        },
        size: AtomicU64::new(metadata.len()),
    });
    Ok((file, state))
}
