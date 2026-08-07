//! A lightweight tracing allocator that wraps the system allocator to track
//! current and peak heap usage.  Gated behind the `mem-trace` feature flag.
//!
//! Inspired by the `trace-alloc` crate from the diamond-types / eg-walker project.
//!
//! ## Limitations
//!
//! - Tracking is **thread-local** (single-threaded benchmarks only).
//! - `realloc` is not overridden, so the default alloc→copy→dealloc path is
//!   used; this may slightly overcount transient peak usage.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::RefCell;

// ---------------------------------------------------------------------------
// Per-thread counters
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default)]
pub struct AllocStats {
    pub num_allocations: usize,
    pub current_bytes: usize,
    pub peak_bytes: usize,
}

thread_local! {
    static STATS: RefCell<AllocStats> = RefCell::new(AllocStats::default());
}

// ---------------------------------------------------------------------------
// The allocator
// ---------------------------------------------------------------------------

pub struct TracingAlloc;

unsafe impl GlobalAlloc for TracingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            STATS.with(|s| {
                let mut r = s.borrow_mut();
                r.num_allocations += 1;
                r.current_bytes += layout.size();
                if r.current_bytes > r.peak_bytes {
                    r.peak_bytes = r.current_bytes;
                }
            });
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        STATS.with(|s| {
            let mut r = s.borrow_mut();
            r.current_bytes = r.current_bytes.saturating_sub(layout.size());
        });
        unsafe { System.dealloc(ptr, layout) };
    }
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------

/// Current live heap bytes on this thread.
pub fn current_bytes() -> usize {
    STATS.with(|s| s.borrow().current_bytes)
}

/// Peak heap bytes observed since the last [`reset_peak`].
pub fn peak_bytes() -> usize {
    STATS.with(|s| s.borrow().peak_bytes)
}

/// Reset the peak counter to the current level.
pub fn reset_peak() {
    STATS.with(|s| {
        let mut r = s.borrow_mut();
        r.peak_bytes = r.current_bytes;
    });
}

/// Run `f`, returning `(peak_above_baseline, steady_state_above_baseline, f's return value)`.
///
/// "Baseline" is the current allocation level at the moment this function is
/// called.  Peak is the high-water mark *during* `f`, and steady-state is the
/// level after `f` returns — both relative to that baseline.
pub fn measure_memusage<F: FnOnce() -> R, R>(f: F) -> (usize, usize, R) {
    let before = current_bytes();
    reset_peak();

    let result = f();

    let peak = peak_bytes().saturating_sub(before);
    let steady = current_bytes().saturating_sub(before);
    (peak, steady, result)
}

// ---------------------------------------------------------------------------
// Conditional global-allocator registration
// ---------------------------------------------------------------------------
//
// When the `mem-trace` feature is enabled the TracingAlloc is installed as the
// global allocator.  This lives here so that *any* binary in the crate (tests,
// benches, bins) picks it up automatically — but it can only be active when the
// feature is on (otherwise the normal system allocator is used).

#[cfg(feature = "mem-trace")]
#[global_allocator]
static A: TracingAlloc = TracingAlloc;