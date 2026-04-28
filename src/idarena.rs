use std::cmp::Ordering;
use ahash::AHashMap as HashMap;
use crate::state::State;
use rand::RngExt;

pub const MIN_VALUE: u32 = 0;
pub const MAX_VALUE: u32 = 100000;
pub type Range = (u32, u32);

const EMPTY_OFFSET: u32 = u32::MAX;

#[derive(Clone, Copy, Debug)]
pub struct Identifier {
    offset: u32,
    len: u32,
}

impl Identifier {
    pub const EMPTY: Identifier = Identifier { offset: EMPTY_OFFSET, len: 0 };

    #[inline(always)]
    pub fn is_empty(self) -> bool { self.offset == EMPTY_OFFSET }

    #[inline(always)]
    pub fn depth(self) -> u32 { self.len }
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

pub enum IdOrderingRelation {
    B1BeforeB2,
    B1AfterB2,
    B1InsideB2,
    B2InsideB1,
    B1ConcatB2,
    B2ConcatB1,
    B1EqualsB2,
}

#[derive(Clone, Debug)]
pub struct IdArena {
    data: Vec<u32>,
    dedup: HashMap<u64, smallvec::SmallVec<[(u32, u32); 1]>>,
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
        let len = path.len() as u32;

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

    #[inline]
    pub fn compare_ids(&self, a: Identifier, b: Identifier) -> Ordering {
        if a.offset == b.offset { return Ordering::Equal; }
        self.get_slice(a).cmp(self.get_slice(b))
    }

    #[inline(always)]
    fn get_slice_unchecked(&self, id: Identifier) -> &[u32] {
        debug_assert!(!id.is_empty());
        unsafe {
            self.data.get_unchecked(id.offset as usize..(id.offset as usize + id.len as usize))
        }
    }

    #[inline]
    pub fn compare_refs(&self, a: IdentifierRef, b: IdentifierRef) -> Ordering {
        // Fast path: same base → just compare extras
        if a.base.offset == b.base.offset {
            return a.extra.cmp(&b.extra);
        }

        let sa = self.get_slice_unchecked(a.base);
        let sb = self.get_slice_unchecked(b.base);

        let sa_len = sa.len();
        let sb_len = sb.len();

        let min_len = sa_len.min(sb_len);

        let sa_prefix = unsafe { sa.get_unchecked(..min_len) };
        let sb_prefix = unsafe { sb.get_unchecked(..min_len) };
        match sa_prefix.cmp(sb_prefix) {
            Ordering::Equal => {}
            ord => return ord,
        }

        match sa_len.cmp(&sb_len) {
            Ordering::Equal => a.extra.cmp(&b.extra),
            Ordering::Less => {
                let sb_at = unsafe { *sb.get_unchecked(min_len) };
                match a.extra.cmp(&sb_at) {
                    Ordering::Equal => {
                        // a is shorter (min_len + 1 components) vs b has more
                        Ordering::Less
                    }
                    ord => ord,
                }
            }
            Ordering::Greater => {
                // a: sa[min_len];  b: extra at position min_len
                let sa_at = unsafe { *sa.get_unchecked(min_len) };
                match sa_at.cmp(&b.extra) {
                    Ordering::Equal => {
                        // b is shorter
                        Ordering::Greater
                    }
                    ord => ord,
                }
            }
        }
    }

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

        if l >= next_slice.len() + 1 { return length; }

        // Check: insert's base must be a prefix of next's full path
        let next_full_iter = next_slice.iter().chain(std::iter::once(&id_next.extra));
        for (&a, &b) in insert_slice.iter().zip(next_full_iter) {
            if a != b { return length; }
        }

        let next_at_l = if l < next_slice.len() { next_slice[l] } else { id_next.extra };
        next_at_l + 1 - id_insert.extra
    }

    pub fn find_split_point(&self, idi_short: &IdentifierInterval, id_long: Identifier) -> u32 {
        if id_long.is_empty() { return 0; }

        let text_len = idi_short.hi - idi_short.lo;
        if text_len == 0 { return 0; }

        let long_slice = self.get_slice(id_long);
        let short_slice = self.get_slice(idi_short.base);
        let min_len = short_slice.len().min(long_slice.len());

        // Compare the shared prefix once, outside the binary search.
        match short_slice[..min_len].cmp(&long_slice[..min_len]) {
            Ordering::Less => {
                return text_len;
            }
            Ordering::Greater => {
                return 0;
            }
            Ordering::Equal => {}
        }

        // Prefixes match. Now ordering depends on what comes after position min_len.
        if short_slice.len() < long_slice.len() {
            let pivot = long_slice[min_len];
            let extras_below = if long_slice.len() > min_len + 1 {
                pivot.saturating_add(1).saturating_sub(idi_short.lo)
            } else {
                // ref < long whenever extra < pivot
                pivot.saturating_sub(idi_short.lo)
            };
            return extras_below.min(text_len);
        } else if short_slice.len() == long_slice.len() {
            return 0;
        } else {
            return 0;
        }
    }

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