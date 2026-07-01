//! Async→sync bridge.
//!
//! ADBC's API is synchronous but `cosmos-client` is async. Each database owns one
//! multi-threaded Tokio runtime (shared via `Arc` into its connections and statements),
//! and blocking calls at the ADBC boundary go through [`Runtime::block_on`]. Multi-thread
//! is deliberate: a current-thread runtime can deadlock when the transport spawns tasks.
//!
//! `block_on` must only be called from a non-async context (the ADBC/FFI boundary), never
//! from inside another runtime's worker thread.

use std::future::Future;

pub struct Runtime {
    inner: tokio::runtime::Runtime,
}

impl Runtime {
    pub fn new_multi_thread() -> std::io::Result<Self> {
        let inner = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        Ok(Self { inner })
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.inner.block_on(future)
    }
}
