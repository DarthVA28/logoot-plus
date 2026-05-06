use std::cell::RefCell;
use std::collections::BTreeMap;

thread_local! {
    static STATS: RefCell<ComparisonStats> = RefCell::new(ComparisonStats::new());
}

struct ComparisonStats {
    /// How many comparisons hit the fast path (same base offset)
    same_base: u64,

    /// For different-base comparisons: histogram of the depth at which
    /// the two slices first diverge. Key = depth index (0-based),
    /// Value = count.
    diverge_at_depth: BTreeMap<usize, u64>,

    /// Comparisons where one slice is a prefix of the other
    /// (divergence happens at the "extra" / offset level)
    diverge_at_extra: u64,

    /// Total number of u32 element comparisons performed across all calls
    total_element_cmps: u64,

    /// Total different-base comparisons
    total_diff_base: u64,

    /// Distribution of slice lengths seen (max of the two per comparison)
    max_depth_histogram: BTreeMap<usize, u64>,
}

impl ComparisonStats {
    fn new() -> Self {
        ComparisonStats {
            same_base: 0,
            diverge_at_depth: BTreeMap::new(),
            diverge_at_extra: 0,
            total_element_cmps: 0,
            total_diff_base: 0,
            max_depth_histogram: BTreeMap::new(),
        }
    }
}

/// Call this at the top of `compare_refs` (or `compare_intervals_raw`).
/// Pass the two base Identifiers being compared.
///
/// ```rust
/// // Inside IdArena::compare_refs or compare_intervals_raw:
/// cmp_profiler::record_comparison(self, a_base, b_base);
/// ```
pub fn record_comparison(
    arena: &crate::idarena::IdArena,
    a: crate::idarena::Identifier,
    b: crate::idarena::Identifier,
) {
    STATS.with(|cell| {
        let stats = &mut *cell.borrow_mut();

        // Fast path: same interned base
        if a == b {
            stats.same_base += 1;
            return;
        }

        stats.total_diff_base += 1;

        let sa = arena.get_path(a);
        let sb = arena.get_path(b);

        let max_depth = sa.len().max(sb.len());
        *stats.max_depth_histogram.entry(max_depth).or_insert(0) += 1;

        let min_len = sa.len().min(sb.len());

        // Walk element by element to find divergence point
        let mut diverged = false;
        for i in 0..min_len {
            stats.total_element_cmps += 1;
            if sa[i] != sb[i] {
                *stats.diverge_at_depth.entry(i).or_insert(0) += 1;
                diverged = true;
                break;
            }
        }

        if !diverged {
            // Slices matched on their shared prefix.
            // Divergence is at the length boundary / extra level.
            stats.diverge_at_extra += 1;
            // Count the prefix comparisons that were equal
            stats.total_element_cmps += 0; // already counted in loop
        }
    });
}

/// Print a human-readable report of comparison statistics.
pub fn report() {
    STATS.with(|cell| {
        let stats = &*cell.borrow();

        let total = stats.same_base + stats.total_diff_base;
        if total == 0 {
            eprintln!("[cmp_profiler] No comparisons recorded.");
            return;
        }

        eprintln!("\n{{'=':.>60}}");
        eprintln!("  IdArena Comparison Profile");
        eprintln!("{{'=':.>60}}");
        eprintln!();
        eprintln!("  Total comparisons:        {:>10}", total);
        eprintln!("  Same-base (fast path):    {:>10}  ({:.1}%)",
            stats.same_base, stats.same_base as f64 / total as f64 * 100.0);
        eprintln!("  Different-base:           {:>10}  ({:.1}%)",
            stats.total_diff_base, stats.total_diff_base as f64 / total as f64 * 100.0);
        eprintln!();

        if stats.total_diff_base > 0 {
            eprintln!("  --- Divergence depth (different-base only) ---");
            eprintln!("  {:>8}  {:>10}  {:>8}  {:>10}", "Depth", "Count", "%", "Cumul %");
            eprintln!("  {:->8}  {:->10}  {:->8}  {:->10}", "", "", "", "");

            let diff = stats.total_diff_base as f64;
            let mut cumulative = 0u64;

            // Merge diverge_at_depth and diverge_at_extra into one view
            let max_key = stats.diverge_at_depth.keys().last().copied().unwrap_or(0);

            for depth in 0..=max_key {
                let count = stats.diverge_at_depth.get(&depth).copied().unwrap_or(0);
                if count == 0 { continue; }
                cumulative += count;
                eprintln!("  {:>8}  {:>10}  {:>7.1}%  {:>9.1}%",
                    depth, count, count as f64 / diff * 100.0,
                    cumulative as f64 / diff * 100.0);
            }

            if stats.diverge_at_extra > 0 {
                cumulative += stats.diverge_at_extra;
                eprintln!("  {:>8}  {:>10}  {:>7.1}%  {:>9.1}%",
                    "extra", stats.diverge_at_extra,
                    stats.diverge_at_extra as f64 / diff * 100.0,
                    cumulative as f64 / diff * 100.0);
            }

            eprintln!();
            eprintln!("  Avg element comparisons per different-base call: {:.2}",
                stats.total_element_cmps as f64 / stats.total_diff_base as f64);
            eprintln!();

            eprintln!("  --- Max identifier depth per comparison ---");
            eprintln!("  {:>8}  {:>10}  {:>8}", "Depth", "Count", "%");
            eprintln!("  {:->8}  {:->10}  {:->8}", "", "", "");
            for (depth, count) in &stats.max_depth_histogram {
                eprintln!("  {:>8}  {:>10}  {:>7.1}%",
                    depth, count, *count as f64 / diff * 100.0);
            }
        }
    });
}

/// Reset all counters.
pub fn reset() {
    STATS.with(|cell| {
        *cell.borrow_mut() = ComparisonStats::new();
    });
}

/// Return raw stats as a structured summary (for programmatic use).
pub fn snapshot() -> ComparisonSnapshot {
    STATS.with(|cell| {
        let stats = &*cell.borrow();
        ComparisonSnapshot {
            total: stats.same_base + stats.total_diff_base,
            same_base: stats.same_base,
            diff_base: stats.total_diff_base,
            diverge_at_depth: stats.diverge_at_depth.clone(),
            diverge_at_extra: stats.diverge_at_extra,
            avg_element_cmps: if stats.total_diff_base > 0 {
                stats.total_element_cmps as f64 / stats.total_diff_base as f64
            } else {
                0.0
            },
        }
    })
}

#[derive(Debug)]
pub struct ComparisonSnapshot {
    pub total: u64,
    pub same_base: u64,
    pub diff_base: u64,
    pub diverge_at_depth: BTreeMap<usize, u64>,
    pub diverge_at_extra: u64,
    pub avg_element_cmps: f64,
}