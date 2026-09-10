//! Disk-backed Turso I/O on the caller's CompIO runtime (Linux and macOS).
//!
//! Select a backend through this crate's `io-uring` / `polling` features or
//! the application's CompIO dependency. No backend is enabled by default.
//! Ordinary [`open`] permits CompIO's filesystem blocking-pool fallback;
//! [`open_strict`] rejects it before submitting filesystem operations.
//!
//! The database and WAL are preopened asynchronously and exclusively owned
//! until accepted operations drain and their descriptors close. Operations
//! must run on the opening runtime and thread. No operation-count or retained-
//! byte limits are imposed by default. Use [`Options`] with [`open_with_options`]
//! or [`open_strict_with_options`] to opt into limits.
//!
//! Durability barriers use CompIO's `File::sync_all` for every Turso sync type.
//! On macOS this provides fsync semantics, not an additional F_FULLFSYNC
//! device-cache flush.
//!
//! # Turso 0.7.2 boundaries
//!
//! Turso's synchronous `IO::open_file` can only return already-open handles.
//! This adapter therefore supports one database and its WAL, not dynamic
//! attachments, disk-backed temporary storage, or the sync engine. Configure
//! temporary SQL storage in memory before running application queries.
//!
//! `IO::remove_file` has neither a completion nor a pending result. Returning
//! success before an asynchronous unlink completes would be incorrect.
//! Shared WAL coordination additionally requires synchronous resizing,
//! mapping, byte locks, and lock backoff, not merely opening a WAL handle.
//! Those operations are explicitly rejected; shared coordination is not
//! advertised. Synchronous waits reject pending I/O instead of spinning,
//! blocking the event loop, or nesting a runtime.

use std::path::Path;

/// Per-connection limits on accepted I/O, including the executing operation.
///
/// Both limits default to `None` (unlimited). Explicit limits must be nonzero.
/// Integer overflow is rejected even when no policy limit is configured.
/// Limits do not preallocate memory. Exceeding a limit rejects submission
/// rather than blocking the runtime or waiting for queue capacity.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Options {
    /// Maximum queued plus executing operations, or `None` for no limit.
    pub max_outstanding_operations: Option<usize>,
    /// Maximum bytes retained for read staging and write buffers, or no limit.
    ///
    /// Includes the capacity of the single idle cached read buffer, which is
    /// released when needed to admit new I/O. This is not a process-memory
    /// limit: queue bookkeeping and Turso's own allocations are not included.
    pub max_retained_bytes: Option<usize>,
}

/// Open an exclusively owned database using the current CompIO backend.
///
/// Filesystem operations use whatever native or blocking-pool implementation
/// the caller selected. Use [`open_strict`] when worker-thread fallback is
/// prohibited. Linux and macOS require open-file-description lock support.
///
/// Uses [`Options::default`], which imposes no operation-count or retained-byte
/// limits. Use [`open_with_options`] to opt into these limits.
pub async fn open(path: &Path) -> anyhow::Result<turso::Connection> {
    open_with_options(path, Options::default()).await
}

/// Open using the current CompIO backend and caller-selected I/O limits.
///
/// Like [`open`], this permits the selected backend's blocking-pool fallback.
/// Invalid zero limits are rejected before opening any files.
pub async fn open_with_options(path: &Path, options: Options) -> anyhow::Result<turso::Connection> {
    anyhow::ensure!(
        options.max_outstanding_operations != Some(0) && options.max_retained_bytes != Some(0),
        "Turso I/O limits must be nonzero"
    );
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        adapter::open(path, options).await
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = path;
        anyhow::bail!("turso-compio currently supports Linux and macOS")
    }
}

/// Verify that the current runtime can execute this adapter's filesystem
/// operations without CompIO's blocking-pool fallback.
///
/// Requires Linux, the io_uring driver, and all probed opcodes (including
/// FTRUNCATE, introduced in Linux 6.9). This does not enable a backend, create
/// a CompIO runtime, or change the caller's runtime configuration.
pub fn check_native_filesystem_support() -> anyhow::Result<()> {
    #[cfg(target_os = "linux")]
    {
        adapter::check_native_filesystem_support()
    }
    #[cfg(not(target_os = "linux"))]
    {
        anyhow::bail!(
            "native filesystem mode requires Linux io_uring; other backends use filesystem worker threads"
        )
    }
}

/// Open only when this runtime supports filesystem I/O without worker threads.
///
/// The check runs before any filesystem operation on the same runtime that
/// will own the database. Ordinary [`open`] does not impose this policy.
///
/// Uses [`Options::default`], which imposes no operation-count or retained-byte
/// limits. Use [`open_strict_with_options`] to opt into these limits.
pub async fn open_strict(path: &Path) -> anyhow::Result<turso::Connection> {
    open_strict_with_options(path, Options::default()).await
}

/// Open with caller-selected I/O limits, rejecting filesystem worker fallback.
///
/// Combines [`check_native_filesystem_support`] with [`open_with_options`].
pub async fn open_strict_with_options(
    path: &Path,
    options: Options,
) -> anyhow::Result<turso::Connection> {
    check_native_filesystem_support()?;
    open_with_options(path, options).await
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod adapter;
