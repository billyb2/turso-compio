//! Turso trait implementations and asynchronous database construction.

mod bridge;
mod filesystem;

use std::{
    os::fd::AsRawFd,
    path::Path,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use anyhow::{Context, ensure};
use compio::{fs::OpenOptions, runtime::Runtime};
use turso::core::{
    Buffer, Completion, IO, LimboError, OpenFlags,
    io::{
        Clock, File as TursoFile, FileId, FileSyncType,
        clock::{DefaultClock, MonotonicInstant, WallClockInstant},
    },
};

use bridge::{Endpoint, Operation, Reservation, WriteBuffer};
#[cfg(target_os = "linux")]
pub(super) use filesystem::check_native_filesystem_support;
use filesystem::{FileState, preopen};

const MAX_WRITE_BUFFERS: usize = 1024;

fn unsupported(message: impl Into<String>) -> LimboError {
    LimboError::InvalidArgument(message.into())
}

struct DiskFile {
    endpoint: Arc<Endpoint>,
    state: Arc<FileState>,
    index: usize,
}

impl DiskFile {
    fn submit(
        &self,
        operation: Operation,
        completion: Completion,
        reservation: Reservation,
    ) -> turso::core::Result<Completion> {
        self.endpoint
            .enqueue(self.index, operation, completion, reservation)
    }

    fn write(
        &self,
        offset: u64,
        buffers: Vec<Arc<Buffer>>,
        completion: Completion,
    ) -> turso::core::Result<Completion> {
        self.endpoint.check()?;
        if buffers.len() > MAX_WRITE_BUFFERS {
            return Err(unsupported("Turso write exceeds 1024 buffers"));
        }
        let (length, retained) =
            buffers
                .iter()
                .try_fold((0usize, 0usize), |(length, retained), buffer| {
                    let allocation = match buffer.as_ref() {
                        Buffer::HeapView { data, .. } => data.len(),
                        _ => buffer.len(),
                    };
                    Ok::<_, LimboError>((
                        length
                            .checked_add(buffer.len())
                            .ok_or_else(|| unsupported("Turso write length overflow"))?,
                        retained
                            .checked_add(allocation)
                            .ok_or_else(|| unsupported("Turso write allocation overflow"))?,
                    ))
                })?;
        check_range(offset, length)?;
        let reservation = self.endpoint.reserve(retained)?;
        let buffers = buffers.into_iter().map(WriteBuffer::from_buffer).collect();
        self.submit(
            Operation::Write { offset, buffers },
            completion,
            reservation,
        )
    }
}

fn check_range(offset: u64, length: usize) -> turso::core::Result<()> {
    if offset
        .checked_add(length as u64)
        .is_none_or(|end| end > i64::MAX as u64)
        || length > i32::MAX as usize
    {
        return Err(unsupported(
            "Turso I/O range exceeds file/completion limits",
        ));
    }
    Ok(())
}

impl TursoFile for DiskFile {
    fn lock_file(&self, exclusive: bool) -> turso::core::Result<()> {
        self.endpoint.check()?;
        if !exclusive {
            return Err(unsupported(
                "this adapter holds an exclusive database lock for the file lifetime",
            ));
        }
        // An OFD write lock was acquired before handing the file to Turso.
        // No other descriptor can silently release it.
        Ok(())
    }

    fn unlock_file(&self) -> turso::core::Result<()> {
        Err(unsupported(
            "the database lock is released only when its file is closed",
        ))
    }

    fn pread(&self, offset: u64, completion: Completion) -> turso::core::Result<Completion> {
        self.endpoint.check()?;
        let buffer = completion.as_read().buf();
        if matches!(buffer, Buffer::Shared(_)) {
            return Err(unsupported("cannot read into an immutable Turso buffer"));
        }
        let length = buffer.len();
        check_range(offset, length)?;
        let reservation = self.endpoint.reserve_read(length)?;
        self.submit(Operation::Read { offset, length }, completion, reservation)
    }

    fn pwrite(
        &self,
        offset: u64,
        buffer: Arc<Buffer>,
        completion: Completion,
    ) -> turso::core::Result<Completion> {
        self.write(offset, vec![buffer], completion)
    }

    fn pwritev(
        &self,
        offset: u64,
        buffers: Vec<Arc<Buffer>>,
        completion: Completion,
    ) -> turso::core::Result<Completion> {
        // One FIFO job and one terminal callback: a failed constituent
        // write cannot leave a completion group waiting forever.
        self.write(offset, buffers, completion)
    }

    fn sync(
        &self,
        completion: Completion,
        _sync_type: FileSyncType,
    ) -> turso::core::Result<Completion> {
        let reservation = self.endpoint.reserve(0)?;
        self.submit(Operation::Sync, completion, reservation)
    }

    fn size(&self) -> turso::core::Result<u64> {
        self.endpoint.check()?;
        Ok(self.state.size.load(Ordering::Acquire))
    }

    fn truncate(&self, length: u64, completion: Completion) -> turso::core::Result<Completion> {
        check_range(length, 0)?;
        let reservation = self.endpoint.reserve(0)?;
        self.submit(Operation::Truncate(length), completion, reservation)
    }

    fn has_hole(&self, _offset: usize, _length: usize) -> turso::core::Result<bool> {
        Err(unsupported(
            "Turso sync-engine sparse files are not supported",
        ))
    }

    fn punch_hole(&self, _offset: usize, _length: usize) -> turso::core::Result<()> {
        Err(unsupported(
            "Turso sync-engine sparse files are not supported",
        ))
    }
}

struct CompioIo {
    files: [Arc<DiskFile>; 2],
}

impl Clock for CompioIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}

impl CompioIo {
    fn file(&self, path: &str) -> turso::core::Result<&Arc<DiskFile>> {
        self.files[0].endpoint.check()?;
        self.files
            .iter()
            .find(|file| file.state.path == path)
            .ok_or_else(|| {
                unsupported(format!(
                    "Turso 0.7.2 requires synchronous open_file; only the asynchronously preopened database and WAL are available: {path}"
                ))
            })
    }
}

impl IO for CompioIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        _direct: bool,
    ) -> turso::core::Result<Arc<dyn TursoFile>> {
        if flags.contains(OpenFlags::ReadOnly) {
            return Err(unsupported(
                "this adapter preopens read-write files; read-only attachments require an asynchronous Turso open contract",
            ));
        }
        // `direct` is a hint. Buffered I/O permits Turso's unaligned
        // header buffers while sync still supplies the durability barrier.
        Ok(self.file(path)?.clone())
    }

    fn open_shared_wal_file(&self, _path: &str) -> turso::core::Result<Arc<dyn TursoFile>> {
        self.files[0].endpoint.check()?;
        Err(unsupported(
            "Turso 0.7.2 shared WAL coordination requires synchronous open, resize, memory mapping, and lock backoff; opening the ordinary WAL does not implement this contract",
        ))
    }

    fn supports_shared_wal_coordination(&self) -> bool {
        false
    }

    fn remove_file(&self, _path: &str) -> turso::core::Result<()> {
        self.files[0].endpoint.check()?;
        Err(unsupported(
            "Turso 0.7.2 IO::remove_file has no completion or pending result; asynchronous unlink requires an upstream API change",
        ))
    }

    fn file_id(&self, path: &str) -> turso::core::Result<FileId> {
        Ok(self.file(path)?.state.id)
    }

    fn step(&self) -> turso::core::Result<()> {
        // CompIO drives the owned futures and their callbacks wake Turso.
        // Never submit_and_wait, poll a ring, or spin here.
        self.files[0].endpoint.check()
    }

    fn cancel(&self, completions: &[Completion]) -> turso::core::Result<()> {
        self.files[0].endpoint.check()?;
        for completion in completions {
            if !completion.finished() {
                completion.abort();
            }
        }
        Ok(())
    }

    fn drain_completions(&self, completions: &[Completion]) -> turso::core::Result<()> {
        self.files[0].endpoint.check()?;
        // Cancellation can synchronously finish callbacks because all
        // kernel buffers are independently owned. Already submitted disk
        // writes still retire in FIFO order before later operations.
        if completions
            .iter()
            .all(|completion| completion.finished() || completion.failed())
        {
            Ok(())
        } else {
            Err(unsupported(
                "synchronous Turso completion draining is not supported; await the async API",
            ))
        }
    }

    fn wait_for_completion(&self, completion: Completion) -> turso::core::Result<()> {
        self.files[0].endpoint.check()?;
        if let Some(error) = completion.get_error() {
            return Err(error.into());
        }
        if !completion.finished() {
            return Err(unsupported(
                "synchronous Turso I/O waiting is not supported; await the async API",
            ));
        }
        Ok(())
    }

    fn yield_now(&self) {
        panic!("synchronous Turso lock backoff is not supported on the CompIO adapter");
    }

    fn sleep(&self, _duration: Duration) {
        panic!("synchronous Turso lock backoff is not supported on the CompIO adapter");
    }
}

pub(super) async fn open(
    path: &Path,
    options: crate::Options,
) -> anyhow::Result<turso::Connection> {
    let runtime_fd = Runtime::try_with_current(|runtime| runtime.as_raw_fd())
        .map_err(|_| anyhow::anyhow!("Turso must be opened inside the CompIO runtime"))?;
    let path_string = path.to_str().context("Turso database path must be UTF-8")?;
    ensure!(
        !path_string.is_empty() && path.file_name().is_some(),
        "Turso requires a database file path"
    );
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY)
        .open(parent)
        .await
        .context("opening existing Turso parent directory")?;
    // Lock the DB before touching its WAL. A second opener cannot mutate
    // either file or attach a competing WAL to the same database inode.
    let (database, database_state) = preopen(path_string).await?;
    let (wal, wal_state) = preopen(&format!("{path_string}-wal")).await?;
    ensure!(
        database_state.id != wal_state.id,
        "Turso database and WAL must be distinct files"
    );
    database
        .sync_all()
        .await
        .context("syncing Turso database creation")?;
    wal.sync_all().await.context("syncing Turso WAL creation")?;
    directory
        .sync_all()
        .await
        .context("syncing Turso directory entries")?;
    directory.close().await?;

    let endpoint = Arc::new(Endpoint::new(runtime_fd, options));
    let states = [database_state, wal_state];
    let io = Arc::new(CompioIo {
        files: std::array::from_fn(|index| {
            Arc::new(DiskFile {
                endpoint: endpoint.clone(),
                state: states[index].clone(),
                index,
            })
        }),
    });
    // Construct the shutdown guard before spawning: even an unpolled task
    // dropped with its runtime must abort queued callbacks and mark it dead.
    endpoint.start([database, wal], states);
    let database = turso::Builder::new_local(path_string)
        .with_io_impl(io)
        .build()
        .await?;
    database
        .connect()
        .context("connecting to the Turso database")
}
