//! Runtime-affine FIFO execution, buffer ownership, and completion lifetimes.

use std::{
    collections::VecDeque,
    io,
    mem::MaybeUninit,
    os::fd::AsRawFd,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{self, ThreadId},
};

use compio::{
    BufResult,
    buf::{IntoInner, IoBuf, IoBufMut, SetLen},
    fs::File,
    io::{AsyncReadAt, AsyncWriteAt},
    runtime::Runtime,
};
use synchrony::sync::{event::Event, mutex::Mutex};
use turso::core::{Buffer, Completion, CompletionError, io::SharedBufferData};

use super::{filesystem::FileState, unsupported};

fn completion_error(error: io::Error, operation: &'static str) -> CompletionError {
    CompletionError::IOError(error.kind(), operation)
}

struct Budget {
    operations: AtomicUsize,
    bytes: AtomicUsize,
    limits: crate::Options,
}

impl Budget {
    fn can_retain(&self, bytes: usize) -> bool {
        self.bytes
            .load(Ordering::Acquire)
            .checked_add(bytes)
            .is_some_and(|total| total <= self.limits.max_retained_bytes.unwrap_or(usize::MAX))
    }
}

pub(super) struct Reservation {
    budget: Arc<Budget>,
    bytes: usize,
    read_buffer: Option<ReadBuffer>,
}

impl Reservation {
    fn release_bytes(&mut self) {
        let bytes = std::mem::take(&mut self.bytes);
        if bytes != 0 {
            self.budget.bytes.fetch_sub(bytes, Ordering::AcqRel);
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.read_buffer.take();
        self.release_bytes();
        self.budget.operations.fetch_sub(1, Ordering::AcqRel);
    }
}

// The accounting owner travels with the allocation into CompIO. Cancelling
// a future cannot release its charge while the kernel still owns the buffer.
struct ReadBuffer {
    data: Vec<u8>,
    budget: Arc<Budget>,
}

impl Drop for ReadBuffer {
    fn drop(&mut self) {
        let capacity = self.data.capacity();
        drop(std::mem::take(&mut self.data));
        self.budget.bytes.fetch_sub(capacity, Ordering::AcqRel);
    }
}

impl IoBuf for ReadBuffer {
    fn as_init(&self) -> &[u8] {
        &self.data
    }
}

impl IoBufMut for ReadBuffer {
    fn as_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
        self.data.as_uninit()
    }
}

impl SetLen for ReadBuffer {
    unsafe fn set_len(&mut self, length: usize) {
        // SAFETY: the caller guarantees initialized bytes within capacity,
        // exactly as required by the underlying Vec implementation.
        unsafe { self.data.set_len(length) };
    }
}

#[derive(Default)]
struct Queue {
    jobs: VecDeque<Job>,
    read_buffer: Option<ReadBuffer>,
}

struct Bridge {
    queue: Mutex<Queue>,
    ready: Event,
    alive: AtomicBool,
    closed: AtomicBool,
    budget: Arc<Budget>,
}

// This is genuinely Send + Sync: it contains no CompIO handles, runtime
// clones, Rc values, or thread-affine buffers disguised as Send.
pub(super) struct Endpoint {
    bridge: Arc<Bridge>,
    thread: ThreadId,
    runtime_fd: i32,
}

impl Endpoint {
    pub(super) fn new(runtime_fd: i32, limits: crate::Options) -> Self {
        let bridge = Arc::new(Bridge {
            queue: Mutex::new(Queue::default()),
            ready: Event::new(),
            alive: AtomicBool::new(true),
            closed: AtomicBool::new(false),
            budget: Arc::new(Budget {
                operations: AtomicUsize::new(0),
                bytes: AtomicUsize::new(0),
                limits,
            }),
        });
        Self {
            bridge,
            thread: thread::current().id(),
            runtime_fd,
        }
    }

    pub(super) fn start(&self, files: [File; 2], states: [Arc<FileState>; 2]) {
        compio::runtime::spawn(drive(files, states, DriverLifetime(self.bridge.clone()))).detach();
    }

    pub(super) fn check(&self) -> turso::core::Result<()> {
        if thread::current().id() != self.thread
            || !self.bridge.alive.load(Ordering::Acquire)
            || !Runtime::try_with_current(|runtime| runtime.as_raw_fd() == self.runtime_fd)
                .unwrap_or(false)
        {
            return Err(unsupported(
                "Turso I/O must run on its original live CompIO runtime and thread",
            ));
        }
        Ok(())
    }

    pub(super) fn reserve(&self, bytes: usize) -> turso::core::Result<Reservation> {
        self.check()?;
        let budget = &self.bridge.budget;
        let operations_fit = budget
            .operations
            .load(Ordering::Acquire)
            .checked_add(1)
            .is_some_and(|count| {
                count
                    <= budget
                        .limits
                        .max_outstanding_operations
                        .unwrap_or(usize::MAX)
            });
        let mut bytes_fit = budget.can_retain(bytes);
        if operations_fit && !bytes_fit {
            // An idle cache must not reject an otherwise admissible read or
            // write. Drop it before charging the incoming reservation.
            let cached = self
                .bridge
                .queue
                .try_lock()
                .ok_or_else(|| unsupported("reentrant Turso I/O queue access"))?
                .read_buffer
                .take();
            drop(cached);
            bytes_fit = budget.can_retain(bytes);
        }
        if !operations_fit || !bytes_fit {
            return Err(unsupported(format!(
                "Turso I/O submission exceeds configured limits ({:?} outstanding operations / {:?} retained bytes) or accounting capacity",
                budget.limits.max_outstanding_operations, budget.limits.max_retained_bytes,
            )));
        }
        // Only the owner thread can reserve; drops may only reduce usage.
        // The checked totals above therefore also bound these atomic adds.
        budget.operations.fetch_add(1, Ordering::AcqRel);
        budget.bytes.fetch_add(bytes, Ordering::AcqRel);
        Ok(Reservation {
            budget: budget.clone(),
            bytes,
            read_buffer: None,
        })
    }

    pub(super) fn reserve_read(&self, length: usize) -> turso::core::Result<Reservation> {
        self.check()?;
        let cached = {
            let mut queue = self
                .bridge
                .queue
                .try_lock()
                .ok_or_else(|| unsupported("reentrant Turso I/O queue access"))?;
            if queue
                .read_buffer
                .as_ref()
                .is_some_and(|buffer| buffer.data.capacity() >= length)
            {
                queue.read_buffer.take()
            } else {
                None
            }
        };
        if let Some(buffer) = cached {
            // Its full capacity is already charged. Transfer ownership rather
            // than charging the requested length twice (especially at the cap).
            let mut reservation = self.reserve(0)?;
            reservation.read_buffer = Some(buffer);
            Ok(reservation)
        } else {
            self.reserve(length)
        }
    }

    pub(super) fn enqueue(
        &self,
        file: usize,
        operation: Operation,
        completion: Completion,
        reservation: Reservation,
    ) -> turso::core::Result<Completion> {
        let job = Job {
            file,
            operation: Some(operation),
            completion,
            reservation: Some(reservation),
        };
        let completion = job.completion.clone();
        {
            let mut queue = self
                .bridge
                .queue
                .try_lock()
                .ok_or_else(|| unsupported("reentrant Turso I/O queue access"))?;
            queue.jobs.push_back(job);
        }
        self.bridge.ready.notify(1);
        Ok(completion)
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        // Safe even if the final Turso handle was dropped on another thread.
        // The local task drains accepted operations, then closes both files.
        self.bridge.closed.store(true, Ordering::Release);
        self.bridge.ready.notify(1);
    }
}

pub(super) enum WriteBuffer {
    Owned(Buffer),
    Shared(SharedBufferData),
    Snapshot(Vec<u8>),
}

impl WriteBuffer {
    pub(super) fn from_buffer(buffer: Arc<Buffer>) -> Self {
        // A shared view does not expose the allocation it retains, so its
        // backing size cannot be charged against our memory budget.
        if matches!(buffer.as_ref(), Buffer::Shared(SharedBufferData::View(_))) {
            return Self::Snapshot(buffer.as_slice().to_vec());
        }
        match Arc::try_unwrap(buffer) {
            Ok(buffer) => Self::Owned(buffer),
            Err(buffer) => match buffer.as_ref() {
                Buffer::Shared(data) => Self::Shared(data.clone()),
                // Turso cancellation may immediately reuse a mutable page.
                // Holding an Arc alone would not prevent that reuse while
                // the kernel is still reading it.
                _ => Self::Snapshot(buffer.as_slice().to_vec()),
            },
        }
    }
}

impl IoBuf for WriteBuffer {
    fn as_init(&self) -> &[u8] {
        match self {
            Self::Owned(buffer) => buffer.as_slice(),
            Self::Shared(buffer) => buffer.as_slice(),
            Self::Snapshot(buffer) => buffer,
        }
    }
}

pub(super) enum Operation {
    Read {
        offset: u64,
        length: usize,
    },
    Write {
        offset: u64,
        buffers: Vec<WriteBuffer>,
    },
    Sync,
    Truncate(u64),
}

struct Job {
    file: usize,
    operation: Option<Operation>,
    completion: Completion,
    reservation: Option<Reservation>,
}

impl Drop for Job {
    fn drop(&mut self) {
        // Runtime shutdown, an unwinding task, or a rejected submission
        // must not strand Turso futures. Kernel buffers belong to CompIO,
        // never to the read callback or a shared mutable Turso page.
        self.operation.take();
        self.reservation.take();
        if !self.completion.finished() {
            self.completion.abort();
        }
    }
}

struct DriverLifetime(Arc<Bridge>);

impl Drop for DriverLifetime {
    fn drop(&mut self) {
        self.0.alive.store(false, Ordering::Release);
        // No queue guard is ever held across an await or callback.
        let queued = self
            .0
            .queue
            .try_lock()
            .map(|mut queue| std::mem::take(&mut *queue));
        drop(queued);
    }
}

async fn drive(files: [File; 2], states: [Arc<FileState>; 2], lifetime: DriverLifetime) {
    let bridge = &lifetime.0;
    loop {
        // Register before checking the queue to avoid a lost wakeup.
        let ready = bridge.ready.listen();
        let job = bridge
            .queue
            .try_lock()
            .expect("I/O queue guard escaped a synchronous scope")
            .jobs
            .pop_front();

        if let Some(mut job) = job {
            if !job.completion.finished() && !job.completion.failed() {
                let operation = job.operation.take().expect("one operation per I/O job");
                let result = execute(
                    &files[job.file],
                    &states[job.file],
                    operation,
                    &job.completion,
                    bridge,
                    job.reservation
                        .as_mut()
                        .expect("one reservation per I/O job"),
                )
                .await;
                // Release the operation's reservation before invoking a
                // reentrant callback. Any cached read capacity stays charged.
                job.reservation.take();
                if !job.completion.finished() && !job.completion.failed() {
                    match result {
                        Ok(bytes) => job.completion.complete(bytes),
                        Err(error) => job.completion.error(error),
                    }
                }
            }
        } else if bridge.closed.load(Ordering::Acquire) {
            break;
        } else {
            ready.await;
        }
    }
    // Keep the database's process lock until the WAL handle is closed.
    let [database, wal] = files;
    if let Err(error) = wal.close().await {
        log::error!("closing Turso WAL: {error}");
    }
    if let Err(error) = database.close().await {
        log::error!("closing Turso database: {error}");
    }
}

async fn execute(
    file: &File,
    state: &FileState,
    operation: Operation,
    completion: &Completion,
    bridge: &Bridge,
    reservation: &mut Reservation,
) -> Result<i32, CompletionError> {
    match operation {
        Operation::Read { offset, length } => {
            let cached = reservation.read_buffer.take().or_else(|| {
                bridge
                    .queue
                    .try_lock()
                    .expect("I/O queue guard escaped a synchronous scope")
                    .read_buffer
                    .take()
            });
            let mut buffer = match cached {
                Some(buffer) if buffer.data.capacity() >= length => {
                    // The allocation is already charged independently; retire
                    // this queued read's hypothetical allocation reservation.
                    reservation.release_bytes();
                    buffer
                }
                cached => {
                    drop(cached);
                    // vec![0; n] has exactly n capacity. Allocate only when the
                    // one cached allocation is absent or too small, without
                    // geometric growth beyond the reserved byte count.
                    let data = vec![0; length];
                    let buffer = ReadBuffer {
                        data,
                        budget: reservation.budget.clone(),
                    };
                    reservation.bytes = 0; // Transfer the existing charge.
                    buffer
                }
            };
            buffer.data.resize(length, 0);
            let mut read = 0;
            let result = loop {
                if read == length {
                    break Ok(read as i32);
                }
                // Bound by the requested length, not the reused capacity.
                let BufResult(result, slice) = file
                    .read_at(buffer.slice(read..length), offset + read as u64)
                    .await;
                buffer = slice.into_inner();
                match result {
                    Ok(0) => break Ok(read as i32),
                    Ok(bytes) => read += bytes,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => break Err(completion_error(error, "Turso pread")),
                }
            };
            if result.is_ok() && !completion.finished() && !completion.failed() {
                // Cached data beyond a short read is stale, never valid page
                // content. Preserve zero-fill and the actual completion count.
                buffer.data[read..length].fill(0);
                completion
                    .as_read()
                    .buf()
                    .as_mut_slice()
                    .copy_from_slice(&buffer.data);
            }
            // Only completed kernel I/O reaches the cache, including errors
            // and cancellation. Keep at most one idle allocation.
            let previous = bridge
                .queue
                .try_lock()
                .expect("I/O queue guard escaped a synchronous scope")
                .read_buffer
                .replace(buffer);
            drop(previous);
            result
        }
        Operation::Write {
            mut offset,
            buffers,
        } => {
            let start = offset;
            for mut buffer in buffers {
                let length = buffer.buf_len();
                let mut written = 0;
                while written < length {
                    let mut output = file;
                    let BufResult(result, slice) =
                        output.write_at(buffer.slice(written..), offset).await;
                    buffer = slice.into_inner();
                    match result {
                        Ok(0) => return Err(CompletionError::ShortWrite),
                        Ok(bytes) => {
                            written += bytes;
                            offset += bytes as u64;
                            // Preserve actual partial growth even if the
                            // next write fails or its callback is cancelled.
                            state.size.fetch_max(offset, Ordering::AcqRel);
                        }
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                        Err(error) => return Err(completion_error(error, "Turso pwrite")),
                    }
                }
            }
            Ok((offset - start) as i32)
        }
        Operation::Sync => {
            file.sync_all()
                .await
                .map_err(|error| completion_error(error, "Turso fsync"))?;
            Ok(0)
        }
        Operation::Truncate(length) => {
            file.set_len(length)
                .await
                .map_err(|error| completion_error(error, "Turso ftruncate"))?;
            state.size.store(length, Ordering::Release);
            Ok(0)
        }
    }
}
