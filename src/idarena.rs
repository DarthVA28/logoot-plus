///! Chunked arena-interned identifiers for LogootSplit.
///!
///! Each path is stored as a sequence of u128 chunks, where each chunk
///! packs 8 × u16 components.  Comparison reduces to lexicographic
///! comparison of u128 sequences — ~8× fewer compares than per-component,
///! all on contiguous prefetcher-friendly memory.
///!
///! Layout of a u128 chunk (MSB to LSB):
///!   bits 127..112: position 0 (16 bits, value+1 encoded)
///!   bits 111..96:  position 1
///!   bits 95..80:   position 2
///!   bits 79..64:   position 3
///!   bits 63..48:   position 4
///!   bits 47..32:   position 5
///!   bits 31..16:   position 6
///!   bits 15..0:    position 7
///!
///! Value+1 encoding: stored as (v + 1) so 0 means "no component here",
///! which sorts before any real value.  Lets shorter paths sort before
///! longer paths with the same prefix automatically.
///!
///! Note: MAX_VALUE is 65534 (u16::MAX - 1) to leave room for value+1.

use std::cmp::Ordering;
use ahash::AHashMap as HashMap;
use smallvec::SmallVec;

// ───────────────────────── Constants ──────────────────────────

pub const MIN_VALUE: u32 = 0;
/// One less than u16::MAX to allow value+1 encoding without overflow.
pub const MAX_VALUE: u32 = 65534;
pub type Range = (u32, u32);

const COMPONENTS_PER_CHUNK: usize = 8;

const EMPTY_CHUNKS_OFFSET: u32 = u32::MAX;

// ───────────────────────── Encoding helpers ───────────────────

/// Pack up to 8 components into a u128 chunk (MSB-first, value+1 encoded).
///
/// Slot k (0-indexed from start of components) goes in bits
/// [16*(7-k), 16*(7-k)+15].  So position 0 is in bits 127..112 (highest).
#[inline(always)]
fn pack_chunk(components: &[u32]) -> u128 {
    debug_assert!(components.len() <= COMPONENTS_PER_CHUNK);
    let mut chunk: u128 = 0;
    for (i, &v) in components.iter().enumerate() {
        debug_assert!(v <= MAX_VALUE);
        let shift = (COMPONENTS_PER_CHUNK - 1 - i) * 16;
        chunk |= ((v as u128) + 1) << shift;
    }
    chunk
}

/// Unpack a u128 chunk back into up to 8 components, appending to `out`.
#[inline]
fn unpack_chunk(chunk: u128, count: usize, out: &mut Vec<u32>) {
    debug_assert!(count <= COMPONENTS_PER_CHUNK);
    for i in 0..count {
        let shift = (COMPONENTS_PER_CHUNK - 1 - i) * 16;
        let encoded = ((chunk >> shift) & 0xFFFF) as u32;
        debug_assert!(encoded > 0, "unpack on empty slot");
        out.push(encoded - 1);
    }
}

/// Pack a path into a sequence of u128 chunks.
fn pack_path(path: &[u32]) -> SmallVec<[u128; 4]> {
    let mut chunks = SmallVec::new();
    for chunk_components in path.chunks(COMPONENTS_PER_CHUNK) {
        chunks.push(pack_chunk(chunk_components));
    }
    chunks
}

/// Number of chunks needed to hold `len` components.
#[inline(always)]
fn chunks_needed(len: usize) -> usize {
    (len + COMPONENTS_PER_CHUNK - 1) / COMPONENTS_PER_CHUNK
}

// ───────────────────────── ArenaId ────────────────────────────

/// Lightweight handle to an interned identifier.  32 bytes, `Copy`.
///
/// The first chunk is stored inline so common-case comparison
/// (resolves in first 8 components) requires zero arena access.
#[derive(Clone, Copy, Debug)]
pub struct Identifier {
    /// Offset into `IdArena::extra_chunks` for chunk 1 onward.
    /// EMPTY_CHUNKS_OFFSET if path has 0 or 1 chunks.
    chunks_offset: u32,
    /// Total number of components in the full path.
    len: u16,
    /// Number of chunks beyond the inline first chunk.
    extra_chunks: u16,
    /// First chunk packed inline.  0 for empty path.
    first_chunk: u128,
}

impl Identifier {
    pub const EMPTY: Identifier = Identifier {
        chunks_offset: EMPTY_CHUNKS_OFFSET,
        len: 0,
        extra_chunks: 0,
        first_chunk: 0,
    };

    #[inline(always)]
    pub fn is_empty(self) -> bool { self.len == 0 }

    #[inline(always)]
    pub fn depth(self) -> u16 { self.len }

    #[inline(always)]
    fn total_chunks(self) -> usize {
        if self.len == 0 { 0 } else { 1 + self.extra_chunks as usize }
    }
}

impl PartialEq for Identifier {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        // For interned IDs, identical paths share storage:
        // - len matches
        // - first_chunk matches
        // - chunks_offset matches (or both are EMPTY_CHUNKS_OFFSET)
        self.len == other.len
            && self.first_chunk == other.first_chunk
            && self.chunks_offset == other.chunks_offset
    }
}
impl Eq for Identifier {}

impl std::hash::Hash for Identifier {
    #[inline(always)]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // chunks_offset alone uniquely identifies long paths; first_chunk
        // uniquely identifies short paths.  Hash both for safety + speed.
        self.first_chunk.hash(state);
        self.chunks_offset.hash(state);
    }
}

// ───────────────────────── ArenaIdRef ────────────────────────

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
    pub fn new(base: Identifier, lo: u32, hi: u32) -> Self {
        IdentifierInterval { base, lo, hi }
    }
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

#[derive(Clone, Debug)]
pub struct IdArena {
    /// Chunks beyond the first (the first is inlined on ArenaId).
    /// All paths' extra chunks are concatenated; ArenaId::chunks_offset
    /// points to where each path's chunk-1-onward starts.
    extra_chunks: Vec<u128>,
    /// Dedup index: hash → list of (chunks_offset, len, first_chunk).
    dedup: HashMap<u64, SmallVec<[(u32, u16, u128); 1]>>,
}

impl IdArena {
    pub fn new() -> Self {
        IdArena {
            extra_chunks: Vec::with_capacity(2048),
            dedup: HashMap::with_capacity(1024),
        }
    }

    pub fn clear(&mut self) {
        self.extra_chunks.clear();
        self.dedup.clear();
    }

    // ── Interning ────────────────────────────────────────────

    pub fn intern(&mut self, path: &[u32]) -> Identifier {
        if path.is_empty() { return Identifier::EMPTY; }

        let len = path.len() as u16;
        let chunks = pack_path(path);
        debug_assert!(!chunks.is_empty());

        let first_chunk = chunks[0];
        let extras_count = chunks.len() - 1;

        let hash = compute_hash(first_chunk, len, &chunks[1..]);

        // Check existing entries
        if let Some(candidates) = self.dedup.get(&hash) {
            for &(co, clen, cfirst) in candidates {
                if clen != len || cfirst != first_chunk { continue; }
                let stored_extras = if extras_count > 0 {
                    &self.extra_chunks[co as usize..co as usize + extras_count]
                } else {
                    &[][..]
                };
                if stored_extras == &chunks[1..] {
                    return Identifier {
                        chunks_offset: co,
                        len,
                        extra_chunks: extras_count as u16,
                        first_chunk,
                    };
                }
            }
        }

        // Not found — append to arena
        let chunks_offset = if extras_count > 0 {
            let off = self.extra_chunks.len() as u32;
            self.extra_chunks.extend_from_slice(&chunks[1..]);
            off
        } else {
            EMPTY_CHUNKS_OFFSET
        };

        self.dedup.entry(hash).or_default().push((chunks_offset, len, first_chunk));

        Identifier {
            chunks_offset,
            len,
            extra_chunks: extras_count as u16,
            first_chunk,
        }
    }

    /// Get the n-th chunk (0-indexed).
    #[inline(always)]
    fn get_chunk(&self, id: Identifier, n: usize) -> u128 {
        if n == 0 { return id.first_chunk; }
        debug_assert!(n <= id.extra_chunks as usize);
        self.extra_chunks[id.chunks_offset as usize + n - 1]
    }

    /// Slice of all extra chunks for an ID.
    #[inline(always)]
    fn extras_slice(&self, id: Identifier) -> &[u128] {
        if id.extra_chunks == 0 { return &[]; }
        let start = id.chunks_offset as usize;
        let end = start + id.extra_chunks as usize;
        &self.extra_chunks[start..end]
    }

    // ── Comparison: ArenaId vs ArenaId ───────────────────────

    /// Compare two interned IDs lexicographically.  O(D/8) chunk compares.
    #[inline]
    pub fn compare_ids(&self, a: Identifier, b: Identifier) -> Ordering {
        if a == b { return Ordering::Equal; }
        if a.is_empty() && b.is_empty() { return Ordering::Equal; }
        if a.is_empty() { return Ordering::Less; }
        if b.is_empty() { return Ordering::Greater; }

        // Fast path: most comparisons resolve in first 8 components
        if a.first_chunk != b.first_chunk {
            return a.first_chunk.cmp(&b.first_chunk);
        }

        // First chunks equal.  Compare extra chunks lexicographically.
        let ea = self.extras_slice(a);
        let eb = self.extras_slice(b);
        let min_len = ea.len().min(eb.len());

        // Sequential u128 comparison — prefetcher-friendly
        for i in 0..min_len {
            if ea[i] != eb[i] {
                return ea[i].cmp(&eb[i]);
            }
        }

        // All shared chunks equal.  Shorter wins (lex prefix rule).
        // BUT: if one has more chunks, the trailing chunks may have
        // empty slots (zero-padded), and value+1 encoding makes those
        // sort before any real component.  So checking len directly works.
        a.len.cmp(&b.len)
    }

    // ── Comparison: ArenaIdRef vs ArenaIdRef ─────────────────

    /// Compare two ArenaIdRefs (base path + extra component) lexicographically.
    ///
    /// Conceptually: path(a.base) ++ [a.extra] vs path(b.base) ++ [b.extra].
    ///
    /// We compare base chunks normally, then handle the extras at whatever
    /// position they fall.  Most comparisons resolve in the bases' first
    /// chunks without ever reading the extras.
    pub fn compare_refs(&self, a: IdentifierRef, b: IdentifierRef) -> Ordering {
        // Fast path: same base → compare extras directly
        if a.base == b.base {
            return a.extra.cmp(&b.extra);
        }

        // To compare path(a) ++ [extra_a] vs path(b) ++ [extra_b], we need
        // to find where they first diverge.  The bases may diverge first,
        // or they may share a long prefix and diverge in the extras.
        //
        // Strategy: pretend both refs are "virtual chunked sequences" of
        // length len+1, and compare chunk-by-chunk.  For each chunk index:
        //  - If neither has the extra in this chunk, compare base chunks directly.
        //  - If exactly one has its extra in this chunk, build that chunk
        //    on the fly with the extra inserted at the right slot.
        //  - If both have extras in this chunk (only when both have same
        //    length and we're in the final chunk), build both chunks.

        let a_total_len = a.base.len as usize + 1; // including the extra
        let b_total_len = b.base.len as usize + 1;
        let a_chunks = chunks_needed(a_total_len);
        let b_chunks = chunks_needed(b_total_len);
        let min_chunks = a_chunks.min(b_chunks);

        // Position of the extra component within its full path:
        let a_extra_pos = a.base.len as usize;
        let b_extra_pos = b.base.len as usize;
        let a_extra_chunk = a_extra_pos / COMPONENTS_PER_CHUNK;
        let b_extra_chunk = b_extra_pos / COMPONENTS_PER_CHUNK;
        let a_extra_slot = a_extra_pos % COMPONENTS_PER_CHUNK;
        let b_extra_slot = b_extra_pos % COMPONENTS_PER_CHUNK;

        for i in 0..min_chunks {
            let ca = self.chunk_with_extra(a, i, a_extra_chunk, a_extra_slot);
            let cb = self.chunk_with_extra(b, i, b_extra_chunk, b_extra_slot);
            if ca != cb {
                return ca.cmp(&cb);
            }
        }

        // All shared chunks equal — shorter total path is lexicographically less
        a_total_len.cmp(&b_total_len)
    }

    /// Get the i-th chunk of the conceptual path `path(ref.base) ++ [ref.extra]`,
    /// inserting the extra into the appropriate slot if i is the chunk that
    /// contains the extra.
    #[inline]
    fn chunk_with_extra(
        &self,
        r: IdentifierRef,
        i: usize,
        extra_chunk: usize,
        extra_slot: usize,
    ) -> u128 {
        // Get the base chunk if it exists, else 0.
        let base_chunk = if i < r.base.total_chunks() {
            self.get_chunk(r.base, i)
        } else {
            0
        };

        // If this chunk doesn't contain the extra, return base chunk unchanged.
        if i != extra_chunk {
            return base_chunk;
        }

        // Insert the extra at extra_slot.  The slot is currently 0 in
        // base_chunk (since the base path didn't reach that position).
        let shift = (COMPONENTS_PER_CHUNK - 1 - extra_slot) * 16;
        let encoded = (r.extra as u128 + 1) << shift;
        base_chunk | encoded
    }

    // ── Interval comparison ──────────────────────────────────

    #[inline(always)]
    pub fn compare_intervals_raw(
        &self,
        b1_base: Identifier, b1_lo: u32, b1_hi: u32,
        b2_base: Identifier, b2_lo: u32, b2_hi: u32,
    ) -> IdOrderingRelation {
        // Same base → pure offset arithmetic, no comparison
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

        // Different bases: at most 2 ref comparisons
        let b1_begin = IdentifierRef::new(b1_base, b1_lo);
        let b2_begin = IdentifierRef::new(b2_base, b2_lo);

        match self.compare_refs(b1_begin, b2_begin) {
            Ordering::Less => {
                let b1_end = IdentifierRef::new(b1_base, b1_hi - 1);
                if self.compare_refs(b2_begin, b1_end) == Ordering::Less {
                    IdOrderingRelation::B2InsideB1
                } else {
                    IdOrderingRelation::B1BeforeB2
                }
            }
            Ordering::Greater => {
                let b2_end = IdentifierRef::new(b2_base, b2_hi - 1);
                if self.compare_refs(b1_begin, b2_end) == Ordering::Less {
                    IdOrderingRelation::B1InsideB2
                } else {
                    IdOrderingRelation::B1AfterB2
                }
            }
            Ordering::Equal => IdOrderingRelation::B1BeforeB2,
        }
    }

    #[inline(always)]
    pub fn compare_intervals(&self, b1: &IdentifierInterval, b2: &IdentifierInterval) -> IdOrderingRelation {
        self.compare_intervals_raw(b1.base, b1.lo, b1.hi, b2.base, b2.lo, b2.hi)
    }

    // ── num_insertable ───────────────────────────────────────

    /// How many chars insertable at id_insert before colliding with id_next.
    pub fn num_insertable(&self, id_insert: IdentifierRef, id_next: IdentifierRef, length: u32) -> u32 {
        let l = id_insert.base.len as usize;
        let next_full_len = id_next.base.len as usize + 1;

        if l >= next_full_len { return length; }

        // Need: insert's base path is a prefix of next's full path.
        // Walk through and check.
        let insert_path = self.get_path_owned(id_insert.base);
        let next_path = self.get_path_owned(id_next.base);

        let next_full_iter = next_path.iter().chain(std::iter::once(&id_next.extra));
        for (&a, &b) in insert_path.iter().zip(next_full_iter) {
            if a != b { return length; }
        }

        let next_at_l = if l < next_path.len() { next_path[l] } else { id_next.extra };
        next_at_l + 1 - id_insert.extra
    }

    // ── find_split_point (O(1) arithmetic version) ───────────

    pub fn find_split_point(&self, idi_short: &IdentifierInterval, id_long: Identifier) -> u32 {
        if id_long.is_empty() { return 0; }

        let text_len = idi_short.hi - idi_short.lo;
        if text_len == 0 { return 0; }

        let long_path = self.get_path_owned(id_long);
        let short_path = self.get_path_owned(idi_short.base);

        let min_len = short_path.len().min(long_path.len());

        // Compare prefix once
        match short_path[..min_len].cmp(&long_path[..min_len]) {
            Ordering::Less => return text_len,
            Ordering::Greater => return 0,
            Ordering::Equal => {}
        }

        if short_path.len() < long_path.len() {
            let pivot = long_path[min_len];
            let extras_below = if long_path.len() > min_len + 1 {
                pivot.saturating_add(1).saturating_sub(idi_short.lo)
            } else {
                pivot.saturating_sub(idi_short.lo)
            };
            extras_below.min(text_len)
        } else {
            // short_path.len() >= long_path.len(); since prefixes equal,
            // long is a prefix of short, so any ref > long.
            0
        }
    }

    // ── Path materialisation ─────────────────────────────────

    /// Reconstruct the full path as a Vec<u32>.
    pub fn get_path_owned(&self, id: Identifier) -> Vec<u32> {
        if id.is_empty() { return Vec::new(); }
        let mut path = Vec::with_capacity(id.len as usize);
        let total = id.total_chunks();
        for i in 0..total {
            let chunk = self.get_chunk(id, i);
            let in_this_chunk = if i + 1 < total {
                COMPONENTS_PER_CHUNK
            } else {
                // Last chunk: only the components that fit
                let remaining = id.len as usize - i * COMPONENTS_PER_CHUNK;
                remaining
            };
            unpack_chunk(chunk, in_this_chunk, &mut path);
        }
        path
    }

    /// Backwards-compat alias.
    #[inline]
    pub fn get_path(&self, id: Identifier) -> Vec<u32> {
        self.get_path_owned(id)
    }

    pub fn to_string(&self, id: Identifier) -> String {
        self.get_path_owned(id).iter().map(|x| x.to_string()).collect::<Vec<_>>().join(".")
    }

    pub fn arena_size(&self) -> usize { self.extra_chunks.len() }
    pub fn node_count(&self) -> usize { self.dedup.values().map(|v| v.len()).sum() }
}

#[inline]
fn compute_hash(first: u128, len: u16, extras: &[u128]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = ahash::AHasher::default();
    first.hash(&mut h);
    len.hash(&mut h);
    for c in extras { c.hash(&mut h); }
    h.finish()
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
    let low_path = arena.get_path_owned(id_low.base);
    let high_path = arena.get_path_owned(id_high.base);

    let mut new_path: Vec<u32> = Vec::new();
    let mut low_iter = low_path.iter().copied().chain(std::iter::once(id_low.extra));
    let mut high_iter = high_path.iter().copied().chain(std::iter::once(id_high.extra));

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
    fn test_pack_unpack_roundtrip() {
        let path = [10, 20, 30, 40, 50, 60, 70, 80];
        let chunk = pack_chunk(&path);
        let mut out = Vec::new();
        unpack_chunk(chunk, 8, &mut out);
        assert_eq!(out, path);
    }

    #[test]
    fn test_pack_short() {
        let path = [10, 20, 30];
        let chunk = pack_chunk(&path);
        let mut out = Vec::new();
        unpack_chunk(chunk, 3, &mut out);
        assert_eq!(out, path);
    }

    #[test]
    fn test_pack_chunk_ordering() {
        // [1,2,3] < [1,2,4] should give ck1 < ck2
        let c1 = pack_chunk(&[1, 2, 3]);
        let c2 = pack_chunk(&[1, 2, 4]);
        assert!(c1 < c2);

        // [1,2] (zero-padded in slots 2..7) < [1,2,1] because
        // zero (encoded as 0) is less than 1+1 (encoded as 2)
        let c3 = pack_chunk(&[1, 2]);
        let c4 = pack_chunk(&[1, 2, 1]);
        assert!(c3 < c4);

        // [2] should be greater than [1, 99, 99, 99...]
        let c5 = pack_chunk(&[2]);
        let c6 = pack_chunk(&[1, 99, 99, 99, 99, 99, 99, 99]);
        assert!(c5 > c6);
    }

    #[test]
    fn test_intern_dedup() {
        let mut arena = IdArena::new();
        let a = arena.intern(&[5, 12, 3, 99]);
        let b = arena.intern(&[5, 12, 3, 99]);
        assert_eq!(a, b);
        let c = arena.intern(&[5, 12, 3, 120]);
        assert_ne!(a, c);
    }

    #[test]
    fn test_get_path_short() {
        let mut arena = IdArena::new();
        let id = arena.intern(&[10, 20, 30]);
        assert_eq!(arena.get_path_owned(id), vec![10, 20, 30]);
        assert_eq!(arena.get_path_owned(Identifier::EMPTY), Vec::<u32>::new());
    }

    #[test]
    fn test_get_path_long() {
        let mut arena = IdArena::new();
        let path: Vec<u32> = (0..25).collect();
        let id = arena.intern(&path);
        assert_eq!(arena.get_path_owned(id), path);
        assert_eq!(id.total_chunks(), 4); // 25/8 = 4 chunks
    }

    #[test]
    fn test_compare_ids_basic() {
        let mut arena = IdArena::new();
        let a = arena.intern(&[1, 2, 3]);
        let b = arena.intern(&[1, 2, 3]);
        assert_eq!(arena.compare_ids(a, b), Ordering::Equal);

        let c = arena.intern(&[1, 2, 4]);
        assert_eq!(arena.compare_ids(a, c), Ordering::Less);

        let d = arena.intern(&[1, 2]);
        assert_eq!(arena.compare_ids(d, a), Ordering::Less); // prefix < longer

        let e = arena.intern(&[5, 99, 1]);
        let f = arena.intern(&[10, 1, 1]);
        assert_eq!(arena.compare_ids(e, f), Ordering::Less);
    }

    #[test]
    fn test_compare_ids_deep() {
        let mut arena = IdArena::new();
        let path_a: Vec<u32> = (0..30).collect();
        let mut path_b = path_a.clone();
        path_b[25] = 999;
        let a = arena.intern(&path_a);
        let b = arena.intern(&path_b);
        assert_eq!(arena.compare_ids(a, b), Ordering::Less);

        // Differ in last chunk only
        let mut path_c = path_a.clone();
        path_c[28] = 999;
        let c = arena.intern(&path_c);
        assert_eq!(arena.compare_ids(a, c), Ordering::Less);

        // Prefix
        let d = arena.intern(&path_a[..15]);
        assert_eq!(arena.compare_ids(d, a), Ordering::Less);
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
        // [5,12,4] vs [5,12,3,99] → Greater
        assert_eq!(arena.compare_refs(IdentifierRef::new(ba, 4), IdentifierRef::new(bb, 99)), Ordering::Greater);
    }

    #[test]
    fn test_compare_refs_empty_base() {
        let mut arena = IdArena::new();
        let base = arena.intern(&[5, 10]);
        assert_eq!(
            arena.compare_refs(IdentifierRef::doc_start(), IdentifierRef::new(base, 99)),
            Ordering::Less
        );
        assert_eq!(
            arena.compare_refs(IdentifierRef::doc_end(), IdentifierRef::new(base, 99)),
            Ordering::Greater
        );
    }

    #[test]
    fn test_compare_refs_extra_in_second_chunk() {
        let mut arena = IdArena::new();
        // Base length 8 → extra is in chunk 1, slot 0
        let ba = arena.intern(&[1, 2, 3, 4, 5, 6, 7, 8]);
        // Base length 8, different last component → extras compared in chunk 1
        let bb = arena.intern(&[1, 2, 3, 4, 5, 6, 7, 8]);
        // Same base → just compare extras
        assert_eq!(
            arena.compare_refs(IdentifierRef::new(ba, 5), IdentifierRef::new(bb, 10)),
            Ordering::Less
        );

        // Different bases of length 8, extras pushed into chunk 1
        let bc = arena.intern(&[1, 2, 3, 4, 5, 6, 7, 9]);
        // Now ba ends in 8, bc ends in 9; with extras [5] and [5]:
        //   ba+[5] = [1..7, 8, 5]
        //   bc+[5] = [1..7, 9, 5]
        // Diverges at position 7 (the 8 vs 9), so ba < bc.
        assert_eq!(
            arena.compare_refs(IdentifierRef::new(ba, 5), IdentifierRef::new(bc, 5)),
            Ordering::Less
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
    fn test_num_insertable() {
        let mut arena = IdArena::new();
        let base = arena.intern(&[5, 10]);
        let ins = IdentifierRef::new(base, 3);
        let nxt = IdentifierRef::new(base, 7);
        assert_eq!(arena.num_insertable(ins, nxt, 100), 5);
    }

    /// Exhaustive cross-check against raw slice comparison.
    #[test]
    fn test_ref_comparison_exhaustive() {
        let paths: Vec<Vec<u32>> = vec![
            vec![],
            vec![1],
            vec![1, 2],
            vec![1, 2, 3],
            vec![1, 3],
            vec![2],
            vec![2, 1],
            vec![1, 2, 3, 4, 5, 6, 7],     // exactly fills 1 chunk minus one
            vec![1, 2, 3, 4, 5, 6, 7, 8],  // exactly fills 1 chunk
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9], // spills into chunk 2
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

    /// Cross-check compare_ids exhaustively too.
    #[test]
    fn test_id_comparison_exhaustive() {
        let paths: Vec<Vec<u32>> = vec![
            vec![],
            vec![1], vec![2],
            vec![1, 2], vec![1, 3], vec![2, 1],
            vec![1, 2, 3, 4, 5, 6, 7, 8],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 10],
            // Deep paths
            (0..20).collect(),
            (0..20).map(|i| if i == 18 { 999 } else { i }).collect(),
        ];

        let mut arena = IdArena::new();
        let ids: Vec<Identifier> = paths.iter().map(|p| arena.intern(p)).collect();

        for (i, pa) in paths.iter().enumerate() {
            for (j, pb) in paths.iter().enumerate() {
                let expected = pa.cmp(pb);
                let got = arena.compare_ids(ids[i], ids[j]);
                assert_eq!(got, expected,
                    "compare_ids mismatch: {:?} vs {:?}: expected {:?}, got {:?}",
                    pa, pb, expected, got);
            }
        }
    }
}