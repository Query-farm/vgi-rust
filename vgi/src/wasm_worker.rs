//! Canonical browser (`worker:`) entry points for a VGI worker.
//!
//! A VGI worker served over DuckDB-WASM's SharedArrayBuffer channel needs a
//! fixed set of C ABI exports that the page-side boot script
//! (`vgi-worker-boot.js`) calls, plus the ring shims that talk to the
//! `--js-library` (`vgi_worker_lib.js`). That glue is **identical for every
//! worker** — only the choice of which functions the [`Worker`](crate::Worker)
//! registers differs — so it lives here rather than being hand-copied into each
//! worker crate (the same reason the ring ops are a shared js-library, not
//! vendored per worker).
//!
//! [`wasm_worker!`](crate::wasm_worker) generates the three canonical exports —
//! `vgi_worker_init`, `vgi_worker_serve_sab_slot`, `vgi_worker_serve_pool` —
//! wired to a builder you supply. The macro expands to nothing off `wasm32`, so
//! a crate can invoke it unconditionally.
//!
//! # Example
//!
//! ```ignore
//! fn build() -> vgi::Worker {
//!     let mut w = vgi::Worker::new();
//!     w.register_scalar(MyScalar);
//!     w
//! }
//!
//! vgi::wasm_worker! { build = build }
//! ```
//!
//! With optional hooks:
//!
//! ```ignore
//! vgi::wasm_worker! {
//!     // Runs once on the module's main thread inside `vgi_worker_init`, before
//!     // any serve thread spawns. For process-global setup that is safe off a
//!     // serve thread — e.g. selecting an in-memory storage backend.
//!     init = my_init,
//!     // Runs exactly once, on the first serve thread, before its first serve.
//!     // For setup that MUST run on a serve thread — e.g. mounting OPFS, whose
//!     // operations are proxied to the event-loop thread and would deadlock if
//!     // called from the main thread.
//!     first_serve = my_first_serve,
//!     // Builds a fully-registered worker; called once per serve lifecycle.
//!     build = build,
//! }
//! ```
//!
//! Paths passed to the macro resolve in the invocation scope, so a sibling
//! function (`build`), an extern-crate path (`fixedformat_worker::build`), or a
//! `crate::`-rooted path all work.

/// Generate the canonical browser `worker:` entry points for a VGI worker.
///
/// See the [module docs](crate::wasm_worker) for the fields and hook semantics.
/// `build` is required; `init` and `first_serve` are optional and must appear in
/// the order shown (`init`, then `first_serve`, then `build`). Expands to
/// nothing on non-`wasm32` targets.
///
/// The macro emits a few private helper items (a `__VgiSab*` reader/writer and
/// helper fns) plus the three `#[no_mangle]` exports into the invocation scope;
/// invoke it at most once per crate.
#[macro_export]
macro_rules! wasm_worker {
    (
        $( init = $init:path, )?
        $( first_serve = $first_serve:path, )?
        build = $build:path
        $(,)?
    ) => {
        // Worker-side ring ops, implemented in the emscripten `--js-library`
        // (`vgi_worker_lib.js`) — the page half of the transport ABI.
        #[cfg(target_arch = "wasm32")]
        extern "C" {
            fn vgi_sab_worker_read(slot: i32, dst: *mut u8, n: i32) -> i32;
            fn vgi_sab_worker_write(slot: i32, src: *const u8, n: i32) -> i32;
            fn vgi_sab_worker_close(slot: i32);
            fn vgi_worker_await_slot(slot: i32);
            fn vgi_worker_await_release(slot: i32);
        }

        /// `Read` over a ring slot's client→worker direction.
        #[cfg(target_arch = "wasm32")]
        struct __VgiSabReader {
            slot: i32,
        }
        #[cfg(target_arch = "wasm32")]
        impl ::std::io::Read for __VgiSabReader {
            fn read(&mut self, buf: &mut [u8]) -> ::std::io::Result<usize> {
                let n = unsafe { vgi_sab_worker_read(self.slot, buf.as_mut_ptr(), buf.len() as i32) };
                if n < 0 {
                    return Err(::std::io::Error::other("vgi sab ring read failed"));
                }
                Ok(n as usize)
            }
        }

        /// `Write` over a ring slot's worker→client direction.
        #[cfg(target_arch = "wasm32")]
        struct __VgiSabWriter {
            slot: i32,
        }
        #[cfg(target_arch = "wasm32")]
        impl ::std::io::Write for __VgiSabWriter {
            fn write(&mut self, buf: &[u8]) -> ::std::io::Result<usize> {
                let n = unsafe { vgi_sab_worker_write(self.slot, buf.as_ptr(), buf.len() as i32) };
                if n < 0 {
                    return Err(::std::io::Error::other("vgi sab ring write failed"));
                }
                Ok(n as usize)
            }
            fn flush(&mut self) -> ::std::io::Result<()> {
                // The ring has no buffering of its own — writes land in shared memory.
                Ok(())
            }
        }

        #[cfg(target_arch = "wasm32")]
        fn __vgi_first_serve_once() {
            $(
                static ONCE: ::std::sync::Once = ::std::sync::Once::new();
                ONCE.call_once(|| { $first_serve(); });
            )?
        }

        #[cfg(target_arch = "wasm32")]
        fn __vgi_serve_slot(slot: i32) {
            // A panic must not escape the `extern "C"` frame below: that is a
            // nounwind context, so Rust turns it into an immediate `abort()` that
            // tears down the whole module — every serve thread and the DuckDB
            // engine with it. Catch it, log it, and close the slot so the client
            // sees a failed stream instead.
            let result = ::std::panic::catch_unwind(::std::panic::AssertUnwindSafe(|| {
                __vgi_first_serve_once();
                let worker: $crate::Worker = $build();
                worker.serve_reader_writer(__VgiSabReader { slot }, __VgiSabWriter { slot });
            }));
            if let Err(payload) = result {
                let msg = payload
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "<non-string panic payload>".to_string());
                ::std::eprintln!("[vgi] worker panic on slot {slot}: {msg}");
            }
            unsafe { vgi_sab_worker_close(slot) };
        }

        /// Main-thread init. The page boot calls this once before the serve pool
        /// spawns.
        #[cfg(target_arch = "wasm32")]
        #[no_mangle]
        pub extern "C" fn vgi_worker_init() {
            $( $init(); )?
        }

        /// Serve a single request lifecycle on one ring slot.
        #[cfg(target_arch = "wasm32")]
        #[no_mangle]
        pub extern "C" fn vgi_worker_serve_sab_slot(slot: i32) {
            __vgi_serve_slot(slot);
        }

        /// Spawn one serve thread per ring slot and return immediately: each
        /// parks until claimed, serves, parks until released, repeats. Threads
        /// are pre-spawned because `pthread_create` after `dlopen` is unreliable
        /// under emscripten. `n_slots` must not exceed `PTHREAD_POOL_SIZE`.
        #[cfg(target_arch = "wasm32")]
        #[no_mangle]
        pub extern "C" fn vgi_worker_serve_pool(n_slots: i32) {
            for slot in 0..n_slots {
                ::std::thread::spawn(move || loop {
                    unsafe { vgi_worker_await_slot(slot) };
                    __vgi_serve_slot(slot);
                    unsafe { vgi_worker_await_release(slot) };
                });
            }
        }
    };
}
