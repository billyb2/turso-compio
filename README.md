# turso-compio

Run embedded Turso's file I/O on your CompIO runtime. Linux and macOS, currently pinned to Turso 0.7.2.

## Why?

I like using CompIO, and want to use Turso along with it.

## How?

Inside your existing CompIO runtime:

```rust
let connection = turso_compio::open(std::path::Path::new("data.db")).await?;
```

Enable the backend you want through your CompIO dependency. Keep the connection on the runtime and thread that opened it.

No I/O limits by default. Use `open_with_options` to cap outstanding operations or retained buffer bytes. Hitting a limit returns an error; it doesn't wait for space. This doesn't cap Turso's own memory use.

`open_strict` rejects filesystem worker-thread fallback. It requires Linux io_uring with the needed operations, including `FTRUNCATE` (Linux 6.9+).

One exclusively owned database and its WAL. No dynamic attachments, disk-backed temporary storage, or shared WAL coordination. Syncs use CompIO's `sync_all()`.
