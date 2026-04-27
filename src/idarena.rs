///! Arena-interned identifiers for LogootSplit.
///!
///! All identifier paths are stored contiguously in a single `Vec<u32>`.
///! Each unique path is stored exactly once (interned).  An identifier
///! becomes a lightweight `ArenaId` — 12 bytes, `Copy` — holding an
///! offset into the arena plus an inline prefix cache.
///!
///! ## Performance characteristics
///!
///! | Operation        | Arc<[u32]> (old) | Trie (attempted) | Arena (this)      |
///! |------------------|------------------|-------------------|-------------------|
///! | Equality         | O(depth)         | O(1)              | O(1)              |
///! | Hash             | O(depth)         | O(1)              | O(1)              |
///! | Comparison       | O(depth) seq     | O(log D) random   | O(depth) seq      |
///! | Clone/Drop       | atomic refcount  | free (Copy)       | free (Copy)       |
///! | Memory per ID    | 16+ bytes heap   | 28 bytes inline   | 12 bytes inline   |
///! | Comparison cache | L1-unfriendly    | L1-unfriendly     | L1-friendly       |
///!
///! The key insight: O(depth) with sequential memory access beats O(log D)
///! with random access for typical CRDT identifier depths (5-20 components).
///! LLVM auto-vectorises `[u32]` slice comparison, so we get ~4 components
///! compared per cycle on AVX2.

use std::cmp::Ordering;
use ahash::AHashMap as HashMap;

// ───────────────────────── Constants ──────────────────────────

pub const MIN_VALUE: u32 = 0;
pub const MAX_VALUE: u32 = 100000;
pub type Range = (u32, u32);

/// Sentinel offset meaning "empty identifier".
const EMPTY_OFFSET: u32 = u32::MAX;

// ───────────────────────── ArenaId ───────────────────────────

/// Lightweight handle to an interned identifier.  12 bytes, `Copy`.
///
/// Two `ArenaId`s with the same `offset` are guaranteed to represent
/// the same path (interning ensures uniqueness), so equality and
/// hashing are O(1).
#[derive(Clone, Copy, Debug)]
pub struct Identifier {
    /// Byte offset into `IdArena::data`.  EMPTY_OFFSET = empty path.
    offset: u32,
    /// Number of `u32` components in this path.
    len: u16,
    // Padding: 2 bytes free here due to alignment.
}

impl Identifier {
    pub const EMPTY: Identifier = Identifier { offset: EMPTY_OFFSET, len: 0 };

    #[inline(always)]
    pub fn is_empty(self) -> bool { self.offset == EMPTY_OFFSET }

    #[inline(always)]
    pub fn depth(self) -> u16 { self.len }
}

impl PartialEq for Identifier {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool { self.offset == other.offset }
}
impl Eq for Identifier {}

impl std::hash::Hash for Identifier {
    #[inline(always)]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) { self.offset.hash(state); }
}

// ───────────────────────── ArenaIdRef ────────────────────────

/// An identifier extended by one extra component (the offset into an
/// `IdentifierInterval`).  Replaces the old `IdentifierRef`.
#[derive(Clone, Copy, Debug)]
pub struct IdentifierRef {
    pub base: Identifier,
    pub extra: u32,
}

impl IdentifierRef {
    #[inline(always)]
    pub fn new(base: Identifier, extra: u32) -> Self { IdentifierRef { base, extra } }
    pub fn doc_start() -> Self { IdentifierRef { base: Identifier::EMPTY, extra: MIN_VALUE } }
    pub fn doc_end()   -> Self { IdentifierRef { base: Identifier::EMPTY, extra: MAX_VALUE } }
}

// ───────────────────────── IdentifierInterval ────────────────

#[derive(Clone, Copy, Debug)]
pub struct IdentifierInterval {
    pub base: Identifier,
    pub lo: u32,
    pub hi: u32,
}

impl IdentifierInterval {
    pub fn new(base: Identifier, lo: u32, hi: u32) -> Self { IdentifierInterval { base, lo, hi } }
    pub fn id_begin(&self) -> IdentifierRef { IdentifierRef::new(self.base, self.lo) }
    pub fn id_end(&self)   -> IdentifierRef { IdentifierRef::new(self.base, self.hi - 1) }
}

// ───────────────────────── IdOrderingRelation ────────────────

pub enum IdOrderingRelation {
    B1BeforeB2,
    B1AfterB2,
    B1InsideB2,
    B2InsideB1,
    B1ConcatB2,
    B2ConcatB1,
    B1EqualsB2,
}

// ───────────────────────── IdArena ────────────────────────────

/// Central store for all identifier paths.  Owned by `Document`.
///
/// Paths are appended to a single contiguous `Vec<u32>`.  Deduplication
/// ensures each unique path is stored exactly once.
#[derive(Clone, Debug)]
pub struct IdArena {
    /// All path components, packed contiguously.
    /// Path at offset `o` with length `l` = `data[o..o+l]`.
    data: Vec<u32>,
    /// Deduplication index: content hash → list of (offset, len).
    /// On insert, we hash the input slice, find candidates, and
    /// compare content to confirm.
    dedup: HashMap<u64, smallvec::SmallVec<[(u32, u16); 1]>>,
}

impl IdArena {
    pub fn new() -> Self {
        IdArena {
            data: Vec::with_capacity(4096),
            dedup: HashMap::with_capacity(1024),
        }
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.dedup.clear();
    }

    // ── Interning ────────────────────────────────────────────

    /// Intern a path, returning a deduplicated `ArenaId`.
    /// If this exact path was previously interned, returns the same
    /// `ArenaId` (same offset).
    pub fn intern(&mut self, path: &[u32]) -> Identifier {
        if path.is_empty() { return Identifier::EMPTY; }

        let hash = self.hash_slice(path);
        let len = path.len() as u16;

        // Check for existing entry with same hash
        if let Some(candidates) = self.dedup.get(&hash) {
            for &(offset, cand_len) in candidates {
                if cand_len == len {
                    let stored = &self.data[offset as usize..(offset as usize + len as usize)];
                    if stored == path {
                        return Identifier { offset, len };
                    }
                }
            }
        }

        // Not found — append to arena
        let offset = self.data.len() as u32;
        self.data.extend_from_slice(path);
        self.dedup.entry(hash).or_default().push((offset, len));
        Identifier { offset, len }
    }

    #[inline]
    fn hash_slice(&self, path: &[u32]) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = ahash::AHasher::default();
        path.hash(&mut hasher);
        hasher.finish()
    }

    // ── Slice access ─────────────────────────────────────────

    /// Get the path components for an `ArenaId`.
    /// This is an O(1) slice lookup — no allocation, no copying.
    #[inline(always)]
    pub fn get_slice(&self, id: Identifier) -> &[u32] {
        if id.is_empty() { return &[]; }
        &self.data[id.offset as usize..(id.offset as usize + id.len as usize)]
    }

    // ── Comparison ───────────────────────────────────────────

    /// Compare two identifiers lexicographically.
    /// O(1) if same id, O(depth) sequential scan otherwise.
    #[inline]
    pub fn compare_ids(&self, a: Identifier, b: Identifier) -> Ordering {
        if a.offset == b.offset { return Ordering::Equal; }
        self.get_slice(a).cmp(self.get_slice(b))
    }

    /// Compare two `ArenaIdRef`s (base path + extra) lexicographically.
    ///
    /// Conceptual paths: `slice(a.base) ++ [a.extra]` vs `slice(b.base) ++ [b.extra]`.
    ///
    /// Optimised to avoid iterator chaining in the hot path.
    #[inline]
    pub fn compare_refs(&self, a: IdentifierRef, b: IdentifierRef) -> Ordering {
        // Fast path: same base → just compare extras
        if a.base.offset == b.base.offset {
            return a.extra.cmp(&b.extra);
        }

        let sa = self.get_slice(a.base);
        let sb = self.get_slice(b.base);

        let min_len = sa.len().min(sb.len());

        // Compare the shared prefix.  LLVM will auto-vectorise this
        // slice comparison on x86-64 with SSE2/AVX2.
        match sa[..min_len].cmp(&sb[..min_len]) {
            Ordering::Equal => {}
            ord => return ord,
        }

        // Shared prefix is equal.  Now compare the tails.
        // The conceptual sequences are:
        //   a: sa[0..sa.len()] ++ [a.extra]
        //   b: sb[0..sb.len()] ++ [b.extra]
        //
        // We've matched [0..min_len).  Position min_len:
        //   - If sa.len() == sb.len(): both have extra → compare extras
        //   - If sa.len() < sb.len():  a has extra, b has sb[min_len]
        //   - If sa.len() > sb.len():  a has sa[min_len], b has extra
        match sa.len().cmp(&sb.len()) {
            Ordering::Equal => a.extra.cmp(&b.extra),
            Ordering::Less => {
                // a: extra at position min_len;  b: sb[min_len]
                match a.extra.cmp(&sb[min_len]) {
                    Ordering::Equal => {
                        // a is shorter (min_len + 1 components) vs b has more
                        Ordering::Less
                    }
                    ord => ord,
                }
            }
            Ordering::Greater => {
                // a: sa[min_len];  b: extra at position min_len
                match sa[min_len].cmp(&b.extra) {
                    Ordering::Equal => {
                        // b is shorter
                        Ordering::Greater
                    }
                    ord => ord,
                }
            }
        }
    }

    // ── Interval comparison ──────────────────────────────────

    /// Compare two identifier intervals.  Restructured to use at most
    /// 2 `compare_refs` calls instead of up to 4.
    #[inline(always)]
    pub fn compare_intervals_raw(
        &self,
        b1_base: Identifier, b1_lo: u32, b1_hi: u32,
        b2_base: Identifier, b2_lo: u32, b2_hi: u32,
    ) -> IdOrderingRelation {
        // Fast path: same base → pure offset arithmetic, no comparison needed
        if b1_base == b2_base {
            if b1_lo == b2_lo && b1_hi == b2_hi {
                return IdOrderingRelation::B1EqualsB2;
            } else if b1_hi == b2_lo {
                return IdOrderingRelation::B1ConcatB2;
            } else if b2_hi == b1_lo {
                return IdOrderingRelation::B2ConcatB1;
            } else if b1_lo >= b2_lo && b1_hi <= b2_hi {
                return IdOrderingRelation::B1InsideB2;
            } else if b2_lo >= b1_lo && b2_hi <= b1_hi {
                return IdOrderingRelation::B2InsideB1;
            } else if b1_lo < b2_lo {
                return IdOrderingRelation::B1BeforeB2;
            } else {
                return IdOrderingRelation::B1AfterB2;
            }
        }

        // Different bases: compare begin points first (1 comparison),
        // then conditionally check containment (1 more comparison).
        // Total: at most 2 compare_refs calls.
        let b1_begin = IdentifierRef::new(b1_base, b1_lo);
        let b2_begin = IdentifierRef::new(b2_base, b2_lo);

        match self.compare_refs(b1_begin, b2_begin) {
            Ordering::Less => {
                // b1 starts before b2.  Check if b2_begin < b1_end (containment).
                let b1_end = IdentifierRef::new(b1_base, b1_hi - 1);
                if self.compare_refs(b2_begin, b1_end) == Ordering::Less {
                    IdOrderingRelation::B2InsideB1
                } else {
                    IdOrderingRelation::B1BeforeB2
                }
            }
            Ordering::Greater => {
                // b2 starts before b1.  Check if b1_begin < b2_end (containment).
                let b2_end = IdentifierRef::new(b2_base, b2_hi - 1);
                if self.compare_refs(b1_begin, b2_end) == Ordering::Less {
                    IdOrderingRelation::B1InsideB2
                } else {
                    IdOrderingRelation::B1AfterB2
                }
            }
            Ordering::Equal => {
                // Same begin but different bases — shouldn't happen in
                // well-formed LogootSplit, but handle gracefully.
                IdOrderingRelation::B1BeforeB2
            }
        }
    }

    #[inline(always)]
    pub fn compare_intervals(&self, b1: &IdentifierInterval, b2: &IdentifierInterval) -> IdOrderingRelation {
        self.compare_intervals_raw(b1.base, b1.lo, b1.hi, b2.base, b2.lo, b2.hi)
    }

    // ── num_insertable ───────────────────────────────────────

    pub fn num_insertable(&self, id_insert: IdentifierRef, id_next: IdentifierRef, length: u32) -> u32 {
        let insert_slice = self.get_slice(id_insert.base);
        let next_slice = self.get_slice(id_next.base);
        let l = insert_slice.len();

        // next's full path length = next_slice.len() + 1 (the extra)
        if l >= next_slice.len() + 1 { return length; }

        // Check: insert's base must be a prefix of next's full path
        let next_full_iter = next_slice.iter().chain(std::iter::once(&id_next.extra));
        for (&a, &b) in insert_slice.iter().zip(next_full_iter) {
            if a != b { return length; }
        }

        let next_at_l = if l < next_slice.len() { next_slice[l] } else { id_next.extra };
        next_at_l + 1 - id_insert.extra
    }

    // ── find_split_point ─────────────────────────────────────

    pub fn find_split_point(&self, idi_short: &IdentifierInterval, id_long: Identifier) -> u32 {
        let long_slice = self.get_slice(id_long);
        let text_len = idi_short.hi - idi_short.lo;
        let short_slice = self.get_slice(idi_short.base);
        let mut sp = 0;
        for i in 0..text_len {
            // Compare short_slice ++ [lo + i] against long_slice
            // let ref_i = IdentifierRef::new(idi_short.base, idi_short.lo + i);
            // Inline the comparison to avoid function call overhead in the loop
            let cmp = short_slice.iter().chain(std::iter::once(&(idi_short.lo + i)))
                .cmp(long_slice.iter());
            if cmp != Ordering::Less { break; }
            sp += 1;
        }
        sp
    }

    // ── Path access (for serialisation / debug) ──────────────

    /// Get the full path as a slice.  Zero allocation.
    #[inline(always)]
    pub fn get_path(&self, id: Identifier) -> &[u32] {
        self.get_slice(id)
    }

    /// Get the full path as an owned Vec (for serialisation).
    pub fn get_path_owned(&self, id: Identifier) -> Vec<u32> {
        self.get_slice(id).to_vec()
    }

    pub fn to_string(&self, id: Identifier) -> String {
        self.get_slice(id).iter().map(|x| x.to_string()).collect::<Vec<_>>().join(".")
    }

    pub fn node_count(&self) -> usize {
        self.dedup.values().map(|v| v.len()).sum()
    }

    pub fn arena_size(&self) -> usize {
        self.data.len()
    }
}

// ───────────────────────── generate_base ──────────────────────

use crate::state::State;
use rand::RngExt;

pub fn generate_base(
    arena: &mut IdArena,
    id_low: IdentifierRef,
    id_high: IdentifierRef,
    state: &mut State,
) -> Identifier {
    let low_slice = arena.get_slice(id_low.base);
    let high_slice = arena.get_slice(id_high.base);

    let mut new_path: Vec<u32> = Vec::new();
    let mut low_iter = low_slice.iter().copied().chain(std::iter::once(id_low.extra));
    let mut high_iter = high_slice.iter().copied().chain(std::iter::once(id_high.extra));

    let mut l = low_iter.next().unwrap_or(MIN_VALUE);
    let mut h = high_iter.next().unwrap_or(MAX_VALUE);

    while (h as i32) - (l as i32) < 2 {
        new_path.push(l);
        l = low_iter.next().unwrap_or(MIN_VALUE);
        h = high_iter.next().unwrap_or(MAX_VALUE);
    }

    let nxt = state.rng.random_range(l + 1..h);
    new_path.push(nxt);
    new_path.push(state.replica);
    new_path.push(state.local_clock);

    arena.intern(&new_path)
}

// ───────────────────────── Tests ──────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intern_deduplication() {
        let mut arena = IdArena::new();
        let a = arena.intern(&[5, 12, 3, 99]);
        let b = arena.intern(&[5, 12, 3, 99]);
        assert_eq!(a, b);
        assert_eq!(a.offset, b.offset); // same storage
        assert_eq!(arena.arena_size(), 4); // stored once

        let c = arena.intern(&[5, 12, 3, 120]);
        assert_ne!(a, c);
        assert_eq!(arena.arena_size(), 8); // two paths stored
    }

    #[test]
    fn test_get_path() {
        let mut arena = IdArena::new();
        let id = arena.intern(&[10, 20, 30]);
        assert_eq!(arena.get_path(id), &[10, 20, 30]);
        assert_eq!(arena.get_path(Identifier::EMPTY), &[] as &[u32]);
    }

    #[test]
    fn test_compare_ids() {
        let mut arena = IdArena::new();
        let a = arena.intern(&[1, 2, 3]);
        let b = arena.intern(&[1, 2, 3]);
        assert_eq!(arena.compare_ids(a, b), Ordering::Equal);

        let c = arena.intern(&[1, 2, 4]);
        assert_eq!(arena.compare_ids(a, c), Ordering::Less);

        let d = arena.intern(&[1, 2]);
        assert_eq!(arena.compare_ids(d, a), Ordering::Less); // prefix < longer
    }

    #[test]
    fn test_compare_refs_same_base() {
        let mut arena = IdArena::new();
        let base = arena.intern(&[5, 12, 3]);
        let a = IdentifierRef::new(base, 99);
        let b = IdentifierRef::new(base, 120);
        assert_eq!(arena.compare_refs(a, b), Ordering::Less);
    }

    #[test]
    fn test_compare_refs_different_base() {
        let mut arena = IdArena::new();
        let ba = arena.intern(&[5, 12]);
        let bb = arena.intern(&[5, 12, 3]);
        // [5,12,3] vs [5,12,3,99] → Less (prefix)
        assert_eq!(arena.compare_refs(IdentifierRef::new(ba, 3), IdentifierRef::new(bb, 99)), Ordering::Less);
        // [5,12,4] vs [5,12,3,99] → Greater (4 > 3 at position 2)
        assert_eq!(arena.compare_refs(IdentifierRef::new(ba, 4), IdentifierRef::new(bb, 99)), Ordering::Greater);
    }

    #[test]
    fn test_compare_refs_empty_base() {
        let mut arena = IdArena::new();
        let base = arena.intern(&[5, 10]);
        // [0] vs [5,10,99]
        assert_eq!(
            arena.compare_refs(IdentifierRef::doc_start(), IdentifierRef::new(base, 99)),
            Ordering::Less
        );
        // [100000] vs [5,10,99]
        assert_eq!(
            arena.compare_refs(IdentifierRef::doc_end(), IdentifierRef::new(base, 99)),
            Ordering::Greater
        );
    }

    #[test]
    fn test_interval_same_base() {
        let mut arena = IdArena::new();
        let base = arena.intern(&[5, 10]);
        let r = arena.compare_intervals_raw(base, 0, 5, base, 5, 10);
        assert!(matches!(r, IdOrderingRelation::B1ConcatB2));
    }

    #[test]
    fn test_interval_different_base() {
        let mut arena = IdArena::new();
        let a = arena.intern(&[5]);
        let b = arena.intern(&[10]);
        let r = arena.compare_intervals_raw(a, 0, 5, b, 0, 5);
        assert!(matches!(r, IdOrderingRelation::B1BeforeB2));
    }

    #[test]
    fn test_interval_containment() {
        let mut arena = IdArena::new();
        let outer = arena.intern(&[5]);
        let inner = arena.intern(&[5, 3, 7]);
        // outer interval [5]+[0..5), inner begin = [5,3,7]+[0] = [5,3,7,0]
        // [5,0] < [5,3,7,0] < [5,4] → B2InsideB1
        let r = arena.compare_intervals_raw(outer, 0, 5, inner, 0, 1);
        assert!(matches!(r, IdOrderingRelation::B2InsideB1));
    }

    #[test]
    fn test_num_insertable() {
        let mut arena = IdArena::new();
        let base = arena.intern(&[5, 10]);
        let ins = IdentifierRef::new(base, 3);
        let nxt = IdentifierRef::new(base, 7);
        assert_eq!(arena.num_insertable(ins, nxt, 100), 5); // 7+1-3
    }

    #[test]
    fn test_compare_intervals_max_2_comparisons() {
        // This test verifies the restructured logic handles all cases.
        let mut arena = IdArena::new();
        let a = arena.intern(&[10]);
        let b = arena.intern(&[20]);
        // let c = arena.intern(&[30]);

        // a before b
        assert!(matches!(
            arena.compare_intervals_raw(a, 0, 5, b, 0, 5),
            IdOrderingRelation::B1BeforeB2
        ));
        // b after a
        assert!(matches!(
            arena.compare_intervals_raw(b, 0, 5, a, 0, 5),
            IdOrderingRelation::B1AfterB2
        ));
    }

    /// Exhaustive cross-check against raw slice comparison.
    #[test]
    fn test_ref_comparison_exhaustive() {
        let paths: Vec<Vec<u32>> = vec![
            vec![], vec![1], vec![1, 2], vec![1, 2, 3],
            vec![1, 3], vec![2], vec![2, 1],
        ];
        let extras = [0u32, 1, 5, 100];

        let mut arena = IdArena::new();
        let arena_ids: Vec<Identifier> = paths.iter().map(|p| arena.intern(p)).collect();

        for (i, pa) in paths.iter().enumerate() {
            for &ea in &extras {
                for (j, pb) in paths.iter().enumerate() {
                    for &eb in &extras {
                        let ra = IdentifierRef::new(arena_ids[i], ea);
                        let rb = IdentifierRef::new(arena_ids[j], eb);
                        let mut sa = pa.clone(); sa.push(ea);
                        let mut sb = pb.clone(); sb.push(eb);
                        let expected = sa.cmp(&sb);
                        let got = arena.compare_refs(ra, rb);
                        assert_eq!(got, expected,
                            "Mismatch: {:?}+{} vs {:?}+{}: expected {:?}, got {:?}",
                            pa, ea, pb, eb, expected, got);
                    }
                }
            }
        }
    }
}