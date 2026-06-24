//! Dot Index: maps (site, sequence_number) → AVL tree node index.
//!
//! Every character in the document has a unique "dot" `(site_id, seq)`.
//! A node in the block tree holds a contiguous run of characters from
//! one creator, so its dot range is `[offset, offset + size)` for that
//! creator's site id.
//!
//! The dot index maintains these mappings, enabling:
//!
//! 1. **O(log k) delete resolution** — replaces `find_by_id_exact` which
//!    costs O(p · log n) with path-length p and tree size n. The dot
//!    index lookup is O(log k) where k = blocks per site, with cheap
//!    u32 comparisons and no arena reads.
//!
//! 2. **O(log k) insertion hints** — given an `origin_left` dot, look up
//!    the predecessor node and start `insert_rec` from its right subtree
//!    instead of from root, reducing comparisons from O(log n) to O(log d)
//!    where d = rank distance between hint and target.
//!
//! # Backend abstraction
//!
//! Per-site storage is abstracted behind the [`DotMap`] trait.
//! The default implementation [`BTreeDotMap`] uses `BTreeMap<u32, (u32, usize)>`
//! giving O(log k) for all operations including splits. A sorted-`Vec`
//! implementation can be swapped in by implementing the same trait.
//!
//! ```text
//! // Default (BTreeMap backend):
//! let index: DotIndex = DotIndex::new();
//!
//! // Future Vec backend:
//! let index: DotIndex<VecDotMap> = DotIndex::new();
//! ```
//!
//! # Maintenance contract
//!
//! The caller must invoke the appropriate `on_*` method after every tree
//! mutation. The mapping from tree operations to dot index callbacks:
//!
//! | Tree operation              | Dot index callback(s)                           |
//! |-----------------------------|--------------------------------------------------|
//! | `insert_first`              | `on_block_inserted`                              |
//! | `insert_after`              | `on_block_inserted`                              |
//! | `insert_before`             | `on_block_inserted`                              |
//! | `extend_content`            | `on_block_extended`                               |
//! | `truncate_content(Start)`   | `on_block_truncated_start`                       |
//! | `truncate_content(End)`     | `on_block_truncated_end`                         |
//! | `split_and_insert_middle`   | `on_block_split` + `on_block_inserted`           |
//! | `delete_target` (leaf/1kid) | `on_block_deleted`                               |
//! | `delete_target` (2 kids)    | `on_block_deleted` + `on_node_remapped`          |
//! | `delete_middle_at_target`   | `on_block_middle_deleted`                        |
//! | `insert_rec` leaf attach    | `on_block_inserted`                              |
//! | `insert_rec` B1InsideB2     | `on_block_split` + `on_block_inserted`           |
//! | `insert_rec` B2ConcatB1     | `on_block_extended`                               |
//! | `insert_rec` B2InsideB1     | (handled by recursive calls)                     |

use std::collections::BTreeMap;
use ahash::AHashMap as HashMap;

// ─── Per-site storage trait ─────────────────────────────────────────────

/// Per-site dot range storage.
///
/// Stores non-overlapping, sorted ranges `[seq_lo, seq_hi) → node_idx`
/// where `seq_lo` and `seq_hi` are sequence numbers for a single site,
/// and `node_idx` is the index into the AVL tree's node arena.
pub trait DotMap: Clone + std::fmt::Debug {
    fn new() -> Self;
    fn clear(&mut self);

    /// Find the node index whose range contains `seq`.
    /// Returns `None` if `seq` falls in a gap or beyond all ranges.
    fn lookup(&self, seq: u32) -> Option<usize>;

    /// Find the full range containing `seq`.
    /// Returns `(seq_lo, seq_hi, node_idx)` or `None`.
    fn lookup_range(&self, seq: u32) -> Option<(u32, u32, usize)>;

    /// Insert a new range `[seq_lo, seq_hi) → node_idx`.
    fn insert(&mut self, seq_lo: u32, seq_hi: u32, node_idx: usize);

    /// Remove the range keyed by `seq_lo`.
    /// Returns `(seq_hi, node_idx)` if found.
    fn remove(&mut self, seq_lo: u32) -> Option<(u32, usize)>;

    /// Split `[seq_lo, old_hi) → old_node` into two ranges:
    ///   - `[seq_lo, split_seq)   → old_node`     (left, keeps original)
    ///   - `[split_seq, old_hi)   → new_node_idx`  (right, new node)
    fn split(&mut self, seq_lo: u32, split_seq: u32, new_node_idx: usize);

    /// Shrink from the start: `[old_lo, hi)` becomes `[new_lo, hi)`.
    fn truncate_start(&mut self, old_lo: u32, new_lo: u32);

    /// Shrink from the end: `[lo, old_hi)` becomes `[lo, new_hi)`.
    fn truncate_end(&mut self, seq_lo: u32, new_hi: u32);

    /// Grow the range: `[lo, old_hi)` becomes `[lo, new_hi)`.
    fn extend(&mut self, seq_lo: u32, new_hi: u32);

    /// Change which node index a range points to, without
    /// altering the range boundaries.
    fn remap(&mut self, seq_lo: u32, new_node_idx: usize);

    /// Number of ranges stored for this site.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ─── BTreeMap implementation ────────────────────────────────────────────

/// [`DotMap`] backed by `BTreeMap<u32, (u32, usize)>`.
///
/// Keys are `seq_lo`, values are `(seq_hi, node_idx)`.
/// All mutating operations are O(log k) where k = number of ranges.
///
/// Lookup uses `range(..=seq).next_back()` to find the greatest key
/// ≤ the query, then verifies the query falls within `[key, seq_hi)`.
#[derive(Clone, Debug)]
pub struct BTreeDotMap {
    ranges: BTreeMap<u32, (u32, usize)>,
}

impl DotMap for BTreeDotMap {
    fn new() -> Self {
        BTreeDotMap {
            ranges: BTreeMap::new(),
        }
    }

    fn clear(&mut self) {
        self.ranges.clear();
    }

    #[inline]
    fn lookup(&self, seq: u32) -> Option<usize> {
        let (_, &(hi, node_idx)) = self.ranges.range(..=seq).next_back()?;
        if seq < hi {
            Some(node_idx)
        } else {
            None
        }
    }

    #[inline]
    fn lookup_range(&self, seq: u32) -> Option<(u32, u32, usize)> {
        let (&lo, &(hi, node_idx)) = self.ranges.range(..=seq).next_back()?;
        if seq < hi {
            Some((lo, hi, node_idx))
        } else {
            None
        }
    }

    fn insert(&mut self, seq_lo: u32, seq_hi: u32, node_idx: usize) {
        debug_assert!(
            seq_lo < seq_hi,
            "insert: empty range [{}, {})",
            seq_lo,
            seq_hi
        );
        self.ranges.insert(seq_lo, (seq_hi, node_idx));
    }

    fn remove(&mut self, seq_lo: u32) -> Option<(u32, usize)> {
        self.ranges.remove(&seq_lo)
    }

    fn split(&mut self, seq_lo: u32, split_seq: u32, new_node_idx: usize) {
        let (old_hi, old_node) = self
            .ranges
            .remove(&seq_lo)
            .expect("split: no range at seq_lo");
        debug_assert!(
            split_seq > seq_lo && split_seq < old_hi,
            "split: split_seq {} outside [{}, {})",
            split_seq,
            seq_lo,
            old_hi
        );
        self.ranges.insert(seq_lo, (split_seq, old_node));
        self.ranges.insert(split_seq, (old_hi, new_node_idx));
    }

    fn truncate_start(&mut self, old_lo: u32, new_lo: u32) {
        let (hi, node_idx) = self
            .ranges
            .remove(&old_lo)
            .expect("truncate_start: no range at old_lo");
        debug_assert!(
            new_lo > old_lo && new_lo < hi,
            "truncate_start: new_lo {} not in ({}, {})",
            new_lo,
            old_lo,
            hi
        );
        self.ranges.insert(new_lo, (hi, node_idx));
    }

    fn truncate_end(&mut self, seq_lo: u32, new_hi: u32) {
        let entry = self
            .ranges
            .get_mut(&seq_lo)
            .expect("truncate_end: no range at seq_lo");
        debug_assert!(
            new_hi > seq_lo && new_hi < entry.0,
            "truncate_end: new_hi {} not in ({}, {})",
            new_hi,
            seq_lo,
            entry.0
        );
        entry.0 = new_hi;
    }

    fn extend(&mut self, seq_lo: u32, new_hi: u32) {
        let entry = self
            .ranges
            .get_mut(&seq_lo)
            .expect("extend: no range at seq_lo");
        debug_assert!(
            new_hi >= entry.0,
            "extend: new_hi {} < old_hi {}",
            new_hi,
            entry.0
        );
        entry.0 = new_hi;
    }

    fn remap(&mut self, seq_lo: u32, new_node_idx: usize) {
        let entry = self
            .ranges
            .get_mut(&seq_lo)
            .expect("remap: no range at seq_lo");
        entry.1 = new_node_idx;
    }

    fn len(&self) -> usize {
        self.ranges.len()
    }
}

// ─── Multi-site wrapper ─────────────────────────────────────────────────

/// Maps `(site_id, seq)` → `node_idx` across all sites.
///
/// Internally holds one [`DotMap`] per site, keyed by site id.
/// The default type parameter uses [`BTreeDotMap`]; swap in an
/// alternative implementation by specifying a different `M`.
#[derive(Clone, Debug)]
pub struct DotIndex<M: DotMap = BTreeDotMap> {
    sites: HashMap<u32, M>,
}

impl<M: DotMap> DotIndex<M> {
    pub fn new() -> Self {
        DotIndex {
            sites: HashMap::new(),
        }
    }

    pub fn clear(&mut self) {
        self.sites.clear();
    }

    // ── Lookups ─────────────────────────────────────────────────────

    /// Find the tree node index containing the character at `(site, seq)`.
    #[inline]
    pub fn lookup(&self, site: u32, seq: u32) -> Option<usize> {
        self.sites.get(&site)?.lookup(seq)
    }

    /// Find the full range containing `(site, seq)`.
    /// Returns `(seq_lo, seq_hi, node_idx)`.
    #[inline]
    pub fn lookup_range(&self, site: u32, seq: u32) -> Option<(u32, u32, usize)> {
        self.sites.get(&site)?.lookup_range(seq)
    }

    // ── Internal helpers ────────────────────────────────────────────

    #[inline]
    fn site_mut(&mut self, site: u32) -> &mut M {
        self.sites.entry(site).or_insert_with(M::new)
    }

    // ── Tree mutation callbacks ─────────────────────────────────────
    //
    // Call exactly one of these after each tree structural change.
    // The names mirror the tree operations they correspond to.

    /// A new block was inserted into the tree.
    ///
    /// Called after: `insert_first`, `insert_after`, `insert_before`,
    /// or a leaf-attach case in `insert_rec`.
    pub fn on_block_inserted(
        &mut self,
        creator: u32,
        seq_lo: u32,
        seq_hi: u32,
        node_idx: usize,
    ) {
        self.site_mut(creator).insert(seq_lo, seq_hi, node_idx);
    }

    /// A block's content was extended in place.
    ///
    /// Called after: `extend_content`, or `B2ConcatB1` merge in `insert_rec`.
    pub fn on_block_extended(&mut self, creator: u32, seq_lo: u32, new_hi: u32) {
        self.site_mut(creator).extend(seq_lo, new_hi);
    }

    /// A block was split into left (original node) and right (new node).
    ///
    /// Called after: `split_and_insert_middle` (for the existing block's
    /// split), or `B1InsideB2` in `insert_rec`.
    pub fn on_block_split(
        &mut self,
        creator: u32,
        seq_lo: u32,
        split_seq: u32,
        new_node_idx: usize,
    ) {
        self.site_mut(creator).split(seq_lo, split_seq, new_node_idx);
    }

    /// An entire block was removed from the tree.
    ///
    /// Called after: `delete_target` (for the block being deleted).
    /// For the two-children case, call this for the *old* target content
    /// and then `on_node_remapped` for the successor that takes its place.
    pub fn on_block_deleted(&mut self, creator: u32, seq_lo: u32) {
        self.site_mut(creator).remove(seq_lo);
    }

    /// A block was truncated from the start (`DelLocation::Start`).
    ///
    /// Called after: `truncate_content(node, n, Start)`.
    pub fn on_block_truncated_start(&mut self, creator: u32, old_lo: u32, new_lo: u32) {
        self.site_mut(creator).truncate_start(old_lo, new_lo);
    }

    /// A block was truncated from the end (`DelLocation::End`).
    ///
    /// Called after: `truncate_content(node, n, End)`.
    pub fn on_block_truncated_end(&mut self, creator: u32, seq_lo: u32, new_hi: u32) {
        self.site_mut(creator).truncate_end(seq_lo, new_hi);
    }

    /// A node's arena index changed without its content changing.
    ///
    /// This happens in `delete_target` with two children: the successor's
    /// content is copied into the target node slot, and the successor's
    /// original slot is freed. The dot entry must follow the content
    /// to its new slot.
    pub fn on_node_remapped(&mut self, creator: u32, seq_lo: u32, new_node_idx: usize) {
        self.site_mut(creator).remap(seq_lo, new_node_idx);
    }

    /// A block had its middle section deleted, producing two remnants.
    ///
    /// Called after: `delete_middle_at_target(target, start, count)`.
    ///
    /// - Left remnant `[seq_lo, left_end)` stays at the original node.
    /// - Right remnant `[right_start, right_end)` goes to `right_node`.
    /// - Middle `[left_end, right_start)` is gone.
    pub fn on_block_middle_deleted(
        &mut self,
        creator: u32,
        seq_lo: u32,
        left_end: u32,
        right_start: u32,
        right_end: u32,
        right_node: usize,
    ) {
        let site = self.site_mut(creator);
        site.truncate_end(seq_lo, left_end);
        site.insert(right_start, right_end, right_node);
    }

    // ── Bulk construction and verification ──────────────────────────
    pub fn build_from<I>(iter: I) -> Self
    where
        I: Iterator<Item = (usize, u32, u32, u32)>,
    {
        let mut index = Self::new();
        for (node_idx, creator, seq_lo, seq_hi) in iter {
            index.on_block_inserted(creator, seq_lo, seq_hi, node_idx);
        }
        index
    }

    /// Check this index against ground truth from the tree.
    ///
    /// `iter` yields `(node_idx, creator, seq_lo, seq_hi)` for each
    /// live node. Returns a list of discrepancies (empty = valid).
    pub fn verify<I>(&self, iter: I) -> Vec<String>
    where
        I: Iterator<Item = (usize, u32, u32, u32)>,
    {
        let mut errors = Vec::new();
        for (node_idx, creator, seq_lo, seq_hi) in iter {
            match self.lookup_range(creator, seq_lo) {
                Some((found_lo, found_hi, found_node)) => {
                    if found_lo != seq_lo || found_hi != seq_hi || found_node != node_idx {
                        errors.push(format!(
                            "site {} seq_lo {}: expected [{}, {}) → node {}, \
                             found [{}, {}) → node {}",
                            creator, seq_lo, seq_lo, seq_hi, node_idx,
                            found_lo, found_hi, found_node
                        ));
                    }
                }
                None => {
                    errors.push(format!(
                        "site {} seq_lo {}: expected [{}, {}) → node {}, not found",
                        creator, seq_lo, seq_lo, seq_hi, node_idx
                    ));
                }
            }
        }
        errors
    }

    // ── Diagnostics ─────────────────────────────────────────────────

    /// Total ranges across all sites.
    pub fn total_ranges(&self) -> usize {
        self.sites.values().map(|m| m.len()).sum()
    }

    /// Number of tracked sites.
    pub fn num_sites(&self) -> usize {
        self.sites.len()
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────

// #[cfg(test)]
// mod tests {
//     use super::*;

//     // Shorthand: all tests use BTreeDotMap via the default type parameter.
//     type Idx = DotIndex;

//     // ── Basic insert + lookup ───────────────────────────────────────

//     #[test]
//     fn single_insert_and_lookup() {
//         let mut idx = Idx::new();
//         // Site 0 created chars with seq 10..15 at node 42
//         idx.on_block_inserted(0, 10, 15, 42);

//         assert_eq!(idx.lookup(0, 10), Some(42));
//         assert_eq!(idx.lookup(0, 12), Some(42));
//         assert_eq!(idx.lookup(0, 14), Some(42));
//         // seq_hi is exclusive
//         assert_eq!(idx.lookup(0, 15), None);
//         // Before range
//         assert_eq!(idx.lookup(0, 9), None);
//         // Wrong site
//         assert_eq!(idx.lookup(1, 12), None);
//     }

//     #[test]
//     fn multiple_ranges_same_site() {
//         let mut idx = Idx::new();
//         idx.on_block_inserted(0, 1, 5, 10);    // [1,5) → node 10
//         idx.on_block_inserted(0, 10, 20, 11);   // [10,20) → node 11
//         idx.on_block_inserted(0, 25, 30, 12);   // [25,30) → node 12

//         assert_eq!(idx.lookup(0, 3), Some(10));
//         assert_eq!(idx.lookup(0, 15), Some(11));
//         assert_eq!(idx.lookup(0, 27), Some(12));
//         // Gaps
//         assert_eq!(idx.lookup(0, 7), None);
//         assert_eq!(idx.lookup(0, 22), None);
//     }

//     #[test]
//     fn multiple_sites() {
//         let mut idx = Idx::new();
//         idx.on_block_inserted(0, 1, 10, 100);
//         idx.on_block_inserted(1, 1, 10, 200);
//         idx.on_block_inserted(2, 5, 8, 300);

//         assert_eq!(idx.lookup(0, 5), Some(100));
//         assert_eq!(idx.lookup(1, 5), Some(200));
//         assert_eq!(idx.lookup(2, 5), Some(300));
//         assert_eq!(idx.lookup(2, 4), None);
//     }

//     // ── lookup_range ────────────────────────────────────────────────

//     #[test]
//     fn lookup_range_returns_boundaries() {
//         let mut idx = Idx::new();
//         idx.on_block_inserted(0, 10, 20, 42);

//         assert_eq!(idx.lookup_range(0, 15), Some((10, 20, 42)));
//         assert_eq!(idx.lookup_range(0, 10), Some((10, 20, 42)));
//         assert_eq!(idx.lookup_range(0, 19), Some((10, 20, 42)));
//         assert_eq!(idx.lookup_range(0, 20), None);
//     }

//     // ── Split ───────────────────────────────────────────────────────

//     #[test]
//     fn split_divides_range() {
//         let mut idx = Idx::new();
//         idx.on_block_inserted(0, 10, 20, 42);

//         // Split at seq 15: [10,20) → [10,15) + [15,20)
//         idx.on_block_split(0, 10, 15, 99);

//         assert_eq!(idx.lookup(0, 10), Some(42));  // left half, original node
//         assert_eq!(idx.lookup(0, 14), Some(42));
//         assert_eq!(idx.lookup(0, 15), Some(99));  // right half, new node
//         assert_eq!(idx.lookup(0, 19), Some(99));
//         assert_eq!(idx.lookup(0, 20), None);

//         assert_eq!(idx.lookup_range(0, 12), Some((10, 15, 42)));
//         assert_eq!(idx.lookup_range(0, 17), Some((15, 20, 99)));
//     }

//     // ── Truncate ────────────────────────────────────────────────────

//     #[test]
//     fn truncate_start_shrinks_from_left() {
//         let mut idx = Idx::new();
//         idx.on_block_inserted(0, 10, 20, 42);

//         // Delete first 3 chars: [10,20) → [13,20)
//         idx.on_block_truncated_start(0, 10, 13);

//         assert_eq!(idx.lookup(0, 12), None);
//         assert_eq!(idx.lookup(0, 13), Some(42));
//         assert_eq!(idx.lookup(0, 19), Some(42));
//     }

//     #[test]
//     fn truncate_end_shrinks_from_right() {
//         let mut idx = Idx::new();
//         idx.on_block_inserted(0, 10, 20, 42);

//         // Delete last 5 chars: [10,20) → [10,15)
//         idx.on_block_truncated_end(0, 10, 15);

//         assert_eq!(idx.lookup(0, 14), Some(42));
//         assert_eq!(idx.lookup(0, 15), None);
//     }

//     // ── Extend ──────────────────────────────────────────────────────

//     #[test]
//     fn extend_grows_range() {
//         let mut idx = Idx::new();
//         idx.on_block_inserted(0, 10, 15, 42);

//         // Append 5 chars: [10,15) → [10,20)
//         idx.on_block_extended(0, 10, 20);

//         assert_eq!(idx.lookup(0, 17), Some(42));
//         assert_eq!(idx.lookup(0, 19), Some(42));
//         assert_eq!(idx.lookup(0, 20), None);
//     }

//     // ── Remove ──────────────────────────────────────────────────────

//     #[test]
//     fn delete_removes_range() {
//         let mut idx = Idx::new();
//         idx.on_block_inserted(0, 10, 20, 42);

//         idx.on_block_deleted(0, 10);

//         assert_eq!(idx.lookup(0, 15), None);
//     }

//     // ── Remap ───────────────────────────────────────────────────────

//     #[test]
//     fn remap_changes_node_index() {
//         let mut idx = Idx::new();
//         idx.on_block_inserted(0, 10, 20, 42);

//         // Successor swap: content moved from node 42 to node 7
//         idx.on_node_remapped(0, 10, 7);

//         assert_eq!(idx.lookup(0, 15), Some(7));
//     }

//     // ── Middle delete ───────────────────────────────────────────────

//     #[test]
//     fn middle_delete_creates_two_remnants() {
//         let mut idx = Idx::new();
//         // Block [10, 30) at node 42, creator 0
//         idx.on_block_inserted(0, 10, 30, 42);

//         // Delete chars at positions 5..10 within block → seq 15..20
//         // Left remnant: [10, 15) → node 42
//         // Right remnant: [20, 30) → node 99
//         idx.on_block_middle_deleted(0, 10, 15, 20, 30, 99);

//         assert_eq!(idx.lookup(0, 10), Some(42));
//         assert_eq!(idx.lookup(0, 14), Some(42));
//         assert_eq!(idx.lookup(0, 15), None);  // deleted
//         assert_eq!(idx.lookup(0, 19), None);  // deleted
//         assert_eq!(idx.lookup(0, 20), Some(99));
//         assert_eq!(idx.lookup(0, 29), Some(99));
//         assert_eq!(idx.lookup(0, 30), None);
//     }

//     // ── build_from + verify ─────────────────────────────────────────

//     #[test]
//     fn build_from_and_verify() {
//         let nodes = vec![
//             (0_usize, 0_u32, 1_u32, 5_u32),   // node 0, site 0, [1,5)
//             (1, 0, 5, 10),                       // node 1, site 0, [5,10)
//             (2, 1, 1, 8),                        // node 2, site 1, [1,8)
//         ];

//         let idx: Idx = DotIndex::build_from(nodes.clone().into_iter());

//         assert_eq!(idx.lookup(0, 3), Some(0));
//         assert_eq!(idx.lookup(0, 7), Some(1));
//         assert_eq!(idx.lookup(1, 4), Some(2));

//         let errors = idx.verify(nodes.into_iter());
//         assert!(errors.is_empty(), "verify errors: {:?}", errors);
//     }

//     #[test]
//     fn verify_detects_mismatch() {
//         let mut idx = Idx::new();
//         idx.on_block_inserted(0, 1, 5, 42);

//         // Claim node 99 is at [1,5) — doesn't match
//         let errors = idx.verify(vec![(99, 0, 1, 5)].into_iter());
//         assert_eq!(errors.len(), 1);
//     }

//     // ── Diagnostics ─────────────────────────────────────────────────

//     #[test]
//     fn diagnostics() {
//         let mut idx = Idx::new();
//         idx.on_block_inserted(0, 1, 5, 10);
//         idx.on_block_inserted(0, 10, 20, 11);
//         idx.on_block_inserted(1, 1, 3, 20);

//         assert_eq!(idx.total_ranges(), 3);
//         assert_eq!(idx.num_sites(), 2);
//     }

//     // ── clear ───────────────────────────────────────────────────────

//     #[test]
//     fn clear_removes_everything() {
//         let mut idx = Idx::new();
//         idx.on_block_inserted(0, 1, 5, 10);
//         idx.on_block_inserted(1, 1, 5, 20);

//         idx.clear();

//         assert_eq!(idx.lookup(0, 3), None);
//         assert_eq!(idx.lookup(1, 3), None);
//         assert_eq!(idx.total_ranges(), 0);
//         assert_eq!(idx.num_sites(), 0);
//     }

//     // ── Compound operation sequences ────────────────────────────────

//     #[test]
//     fn insert_split_delete_sequence() {
//         let mut idx = Idx::new();

//         // Site 0 inserts "hello" → [1,6) at node 0
//         idx.on_block_inserted(0, 1, 6, 0);
//         assert_eq!(idx.lookup(0, 3), Some(0));

//         // Remote insert splits it: "hel|X|lo"
//         // [1,6) splits into [1,4) → node 0 and [4,6) → node 2
//         idx.on_block_split(0, 1, 4, 2);
//         // Plus the new insert from site 1
//         idx.on_block_inserted(1, 1, 2, 1);

//         assert_eq!(idx.lookup(0, 2), Some(0));   // "hel"
//         assert_eq!(idx.lookup(0, 4), Some(2));   // "lo"
//         assert_eq!(idx.lookup(1, 1), Some(1));   // "X"

//         // Delete "lo" (site 0, [4,6))
//         idx.on_block_deleted(0, 4);
//         assert_eq!(idx.lookup(0, 4), None);
//         assert_eq!(idx.lookup(0, 2), Some(0));   // "hel" still there
//     }

//     #[test]
//     fn successor_swap_in_delete() {
//         let mut idx = Idx::new();

//         // Three blocks from different creators
//         idx.on_block_inserted(0, 1, 5, 10);   // target to delete
//         idx.on_block_inserted(1, 1, 4, 11);   // successor
//         idx.on_block_inserted(2, 1, 3, 12);

//         // delete_target with two children: node 10 is deleted,
//         // successor (node 11, creator 1) takes node 10's arena slot
//         idx.on_block_deleted(0, 1);            // old content gone
//         idx.on_node_remapped(1, 1, 10);        // successor content now at node 10

//         assert_eq!(idx.lookup(0, 3), None);     // deleted
//         assert_eq!(idx.lookup(1, 2), Some(10)); // remapped to target's slot
//         assert_eq!(idx.lookup(2, 2), Some(12)); // untouched
//     }

//     #[test]
//     fn extend_then_split() {
//         let mut idx = Idx::new();

//         // User types "abc" → [1,4) at node 0
//         idx.on_block_inserted(0, 1, 4, 0);

//         // User types "de" → extend to [1,6)
//         idx.on_block_extended(0, 1, 6);
//         assert_eq!(idx.lookup(0, 5), Some(0));

//         // Remote split: "ab|X|cde"
//         idx.on_block_split(0, 1, 3, 2);        // [1,3) → node 0, [3,6) → node 2
//         idx.on_block_inserted(1, 1, 2, 1);     // "X" from site 1

//         assert_eq!(idx.lookup(0, 1), Some(0));
//         assert_eq!(idx.lookup(0, 2), Some(0));
//         assert_eq!(idx.lookup(0, 3), Some(2));
//         assert_eq!(idx.lookup(0, 5), Some(2));
//         assert_eq!(idx.lookup(1, 1), Some(1));
//     }
// }