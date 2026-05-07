use std::cmp::Ordering;
use ahash::AHashMap as HashMap;
use crate::state::State;
use rand::RngExt;

pub const MIN_VALUE: u32 = 0;
pub const MAX_VALUE: u32 = 100000;
pub type Range = (u32, u32);

const EMPTY_OFFSET: u32 = u32::MAX;

/// Only engage the cache when the first DEPTH_THRESHOLD elements of
/// both bases match.  Divergences shallower than this are cheap enough
/// to redo every time.  Tune to taste: 6 means ~45% of comparisons
/// (depth 0-5) skip the cache entirely.
const DEPTH_THRESHOLD: usize = 6;

/// Direct-mapped cache: 2^CACHE_BITS entries, ~32 KB at 16 B/entry.
/// Fits comfortably in L1.  Collisions silently evict — correctness
/// never depends on a hit.
const CACHE_BITS: usize = 11;
const CACHE_SIZE: usize = 1 << CACHE_BITS;
const CACHE_MASK: usize = CACHE_SIZE - 1;

// ── Identifier types ─────────────────────────────────────

#[derive(Clone, Copy, Debug)]
pub struct Identifier {
    offset: u32,
    len: u32,
    extends: Option<u32>,
}

impl Identifier {
    pub const EMPTY: Identifier = Identifier { offset: EMPTY_OFFSET, len: 0, extends: None };

    #[inline(always)]
    pub fn is_empty(self) -> bool { self.offset == EMPTY_OFFSET }

    #[inline(always)]
    pub fn depth(self) -> u32 { self.len }

    pub fn set_extends(&mut self, base: Identifier) {
        self.extends = Some(base.offset);
    }
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

// ── BaseRelation ─────────────────────────────────────────

#[derive(Clone, Copy, Debug)]
enum BaseRelation {
    Diverged(Ordering),
    Equal,
    B1Prefix { discriminant: u32 },
    B2Prefix { discriminant: u32 },
}

impl BaseRelation {
    #[inline(always)]
    fn compare(self, b1_extra: u32, b2_extra: u32) -> Ordering {
        match self {
            BaseRelation::Diverged(ord) => ord,
            BaseRelation::Equal => b1_extra.cmp(&b2_extra),
            BaseRelation::B1Prefix { discriminant } => {
                match b1_extra.cmp(&discriminant) {
                    Ordering::Equal => Ordering::Less,
                    ord => ord,
                }
            }
            BaseRelation::B2Prefix { discriminant } => {
                match discriminant.cmp(&b2_extra) {
                    Ordering::Equal => Ordering::Greater,
                    ord => ord,
                }
            }
        }
    }

    #[inline(always)]
    fn flip(self) -> BaseRelation {
        match self {
            BaseRelation::Diverged(ord) => BaseRelation::Diverged(ord.reverse()),
            BaseRelation::Equal => BaseRelation::Equal,
            BaseRelation::B1Prefix { discriminant } => BaseRelation::B2Prefix { discriminant },
            BaseRelation::B2Prefix { discriminant } => BaseRelation::B1Prefix { discriminant },
        }
    }
}

// ── Flat direct-mapped cache ─────────────────────────────

/// A single cache slot.  Keys are stored in canonical order
/// (lo ≤ hi) so that (a,b) and (b,a) share the same slot.
#[derive(Clone, Copy, Debug)]
struct CacheEntry {
    key_lo: u32,
    key_hi: u32,
    relation: BaseRelation, // stored in (lo, hi) direction
}

impl CacheEntry {
    const EMPTY: CacheEntry = CacheEntry {
        key_lo: u32::MAX,
        key_hi: u32::MAX,
        relation: BaseRelation::Equal,
    };
}

#[derive(Clone, Debug)]
struct RelationCache {
    entries: Box<[CacheEntry; CACHE_SIZE]>,
}

impl RelationCache {
    fn new() -> Self {
        RelationCache {
            entries: Box::new([CacheEntry::EMPTY; CACHE_SIZE]),
        }
    }

    #[inline(always)]
    fn index(lo: u32, hi: u32) -> usize {
        let h = (lo as usize).wrapping_mul(0x9E3779B9) ^ (hi as usize).wrapping_mul(0x517CC1B7);
        h & CACHE_MASK
    }

    /// Look up, returning the relation in (a, b) direction.
    #[inline(always)]
    fn lookup(&self, a: u32, b: u32) -> Option<BaseRelation> {
        let (lo, hi, flipped) = if a <= b { (a, b, false) } else { (b, a, true) };
        let idx = Self::index(lo, hi);
        let entry = unsafe { self.entries.get_unchecked(idx) };
        if entry.key_lo == lo && entry.key_hi == hi {
            Some(if flipped { entry.relation.flip() } else { entry.relation })
        } else {
            None
        }
    }

    /// Store, canonicalising keys so (a,b) and (b,a) share one slot.
    #[inline(always)]
    fn store(&mut self, a: u32, b: u32, rel: BaseRelation) {
        let (lo, hi, canonical_rel) = if a <= b {
            (a, b, rel)
        } else {
            (b, a, rel.flip())
        };
        let idx = Self::index(lo, hi);
        let entry = unsafe { self.entries.get_unchecked_mut(idx) };
        entry.key_lo = lo;
        entry.key_hi = hi;
        entry.relation = canonical_rel;
    }

    fn clear(&mut self) {
        self.entries.fill(CacheEntry::EMPTY);
    }
}

// ── Arena ────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct IdArena {
    data: Vec<u32>,
    dedup: HashMap<u64, smallvec::SmallVec<[(u32, u32); 1]>>,
    rel_cache: RelationCache,
}

impl IdArena {
    pub fn new() -> Self {
        IdArena {
            data: Vec::with_capacity(4096),
            dedup: HashMap::with_capacity(1024),
            rel_cache: RelationCache::new(),
        }
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.dedup.clear();
        self.rel_cache.clear();
    }

    pub fn intern(&mut self, path: &[u32]) -> Identifier {
        if path.is_empty() { return Identifier::EMPTY; }

        let hash = self.hash_slice(path);
        let len = path.len() as u32;

        if let Some(candidates) = self.dedup.get(&hash) {
            for &(offset, cand_len) in candidates {
                if cand_len == len {
                    let stored = &self.data[offset as usize..(offset as usize + len as usize)];
                    if stored == path {
                        return Identifier { offset, len, extends: None };
                    }
                }
            }
        }

        let offset = self.data.len() as u32;
        self.data.extend_from_slice(path);
        self.dedup.entry(hash).or_default().push((offset, len));
        Identifier { offset, len, extends: None }
    }

    #[inline]
    fn hash_slice(&self, path: &[u32]) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = ahash::AHasher::default();
        path.hash(&mut hasher);
        hasher.finish()
    }

    #[inline(always)]
    pub fn get_slice(&self, id: Identifier) -> &[u32] {
        if id.is_empty() { return &[]; }
        &self.data[id.offset as usize..(id.offset as usize + id.len as usize)]
    }

    #[inline(always)]
    fn get_slice_unchecked(&self, id: Identifier) -> &[u32] {
        debug_assert!(!id.is_empty());
        unsafe {
            self.data.get_unchecked(id.offset as usize..(id.offset as usize + id.len as usize))
        }
    }

    // ── Extends shortcut ─────────────────────────────────

    #[inline(always)]
    fn try_extends_shortcut(&self, b1: Identifier, b2: Identifier) -> Option<BaseRelation> {
        if let Some(orig) = b1.extends {
            if orig == b2.offset {
                let discriminant = unsafe {
                    *self.data.get_unchecked(b1.offset as usize + b2.len as usize)
                };
                return Some(BaseRelation::B2Prefix { discriminant });
            }
        }
        if let Some(orig) = b2.extends {
            if orig == b1.offset {
                let discriminant = unsafe {
                    *self.data.get_unchecked(b2.offset as usize + b1.len as usize)
                };
                return Some(BaseRelation::B1Prefix { discriminant });
            }
        }
        None
    }

    // ── Base relation with threshold + flat cache ────────

    #[inline]
    fn base_relation(&mut self, b1: Identifier, b2: Identifier) -> BaseRelation {
        if b1.offset == b2.offset {
            return BaseRelation::Equal;
        }

        // Extends shortcut — two integer comparisons, no cache
        if let Some(rel) = self.try_extends_shortcut(b1, b2) {
            return rel;
        }

        let sa = self.get_slice_unchecked(b1);
        let sb = self.get_slice_unchecked(b2);
        let sa_len = sa.len();
        let sb_len = sb.len();
        let min_len = sa_len.min(sb_len);

        // ── Phase 1: compare first DEPTH_THRESHOLD elements inline ──
        // Resolves ~45% of comparisons with zero cache overhead.
        let phase1_end = min_len.min(DEPTH_THRESHOLD);

        match unsafe { sa.get_unchecked(..phase1_end) }
            .cmp(unsafe { sb.get_unchecked(..phase1_end) })
        {
            Ordering::Equal => {}
            ord => return BaseRelation::Diverged(ord),
        }

        // If either slice is shorter than the threshold, we've already
        // compared everything we need — resolve without cache.
        if min_len <= DEPTH_THRESHOLD {
            return match sa_len.cmp(&sb_len) {
                Ordering::Equal => BaseRelation::Equal,
                Ordering::Less => BaseRelation::B1Prefix {
                    discriminant: unsafe { *sb.get_unchecked(min_len) },
                },
                Ordering::Greater => BaseRelation::B2Prefix {
                    discriminant: unsafe { *sa.get_unchecked(min_len) },
                },
            };
        }

        // ── Phase 2: both bases ≥ DEPTH_THRESHOLD, prefix matches ──
        // Now the remaining comparison is expensive enough to justify
        // a cache probe (~3-5 ns for the flat array).
        if let Some(rel) = self.rel_cache.lookup(b1.offset, b2.offset) {
            return rel;
        }

        // ── Phase 3: cache miss — compare remaining elements ────────
        match unsafe { sa.get_unchecked(DEPTH_THRESHOLD..min_len) }
            .cmp(unsafe { sb.get_unchecked(DEPTH_THRESHOLD..min_len) })
        {
            Ordering::Equal => {}
            ord => {
                let rel = BaseRelation::Diverged(ord);
                self.rel_cache.store(b1.offset, b2.offset, rel);
                return rel;
            }
        }

        let rel = match sa_len.cmp(&sb_len) {
            Ordering::Equal => BaseRelation::Equal,
            Ordering::Less => BaseRelation::B1Prefix {
                discriminant: unsafe { *sb.get_unchecked(min_len) },
            },
            Ordering::Greater => BaseRelation::B2Prefix {
                discriminant: unsafe { *sa.get_unchecked(min_len) },
            },
        };
        self.rel_cache.store(b1.offset, b2.offset, rel);
        rel
    }

    // ── Public comparison API ────────────────────────────

    #[inline]
    pub fn compare_ids(&self, a: Identifier, b: Identifier) -> Ordering {
        if a.offset == b.offset { return Ordering::Equal; }
        self.get_slice(a).cmp(self.get_slice(b))
    }

    #[inline]
    pub fn compare_refs(&mut self, a: IdentifierRef, b: IdentifierRef) -> Ordering {
        if a.base.offset == b.base.offset {
            return a.extra.cmp(&b.extra);
        }
        self.base_relation(a.base, b.base).compare(a.extra, b.extra)
    }

    pub fn compare_intervals_raw(
        &mut self,
        b1_base: Identifier, b1_lo: u32, b1_hi: u32,
        b2_base: Identifier, b2_lo: u32, b2_hi: u32,
    ) -> IdOrderingRelation {
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

        let rel = self.base_relation(b1_base, b2_base);

        match rel.compare(b1_lo, b2_lo) {
            Ordering::Less => {
                if rel.compare(b1_hi - 1, b2_lo) == Ordering::Greater {
                    IdOrderingRelation::B2InsideB1
                } else {
                    IdOrderingRelation::B1BeforeB2
                }
            }
            Ordering::Greater => {
                if rel.compare(b1_lo, b2_hi - 1) == Ordering::Less {
                    IdOrderingRelation::B1InsideB2
                } else {
                    IdOrderingRelation::B1AfterB2
                }
            }
            Ordering::Equal => {
                IdOrderingRelation::B1BeforeB2
            }
        }
    }

    pub fn compare_intervals(&mut self, b1: &IdentifierInterval, b2: &IdentifierInterval) -> IdOrderingRelation {
        self.compare_intervals_raw(b1.base, b1.lo, b1.hi, b2.base, b2.lo, b2.hi)
    }

    // ── num_insertable / find_split_point ────────────────

    pub fn num_insertable(&self, id_insert: IdentifierRef, id_next: IdentifierRef, length: u32) -> u32 {
        let insert_slice = self.get_slice(id_insert.base);
        let next_slice = self.get_slice(id_next.base);
        let l = insert_slice.len();

        if l >= next_slice.len() + 1 { return length; }

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

        match short_slice[..min_len].cmp(&long_slice[..min_len]) {
            Ordering::Less  => return text_len,
            Ordering::Greater => return 0,
            Ordering::Equal => {}
        }

        if short_slice.len() < long_slice.len() {
            let pivot = long_slice[min_len];
            let extras_below = if long_slice.len() > min_len + 1 {
                pivot.saturating_add(1).saturating_sub(idi_short.lo)
            } else {
                pivot.saturating_sub(idi_short.lo)
            };
            return extras_below.min(text_len);
        } else {
            return 0;
        }
    }

    // ── Accessors ────────────────────────────────────────

    #[inline(always)]
    pub fn get_path(&self, id: Identifier) -> &[u32] {
        self.get_slice(id)
    }

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