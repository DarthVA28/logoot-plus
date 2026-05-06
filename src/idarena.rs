use std::cmp::Ordering;
use ahash::AHashMap as HashMap;
use crate::{state::State};
use rand::RngExt;

pub const MIN_VALUE: u32 = 0;
pub const MAX_VALUE: u32 = 100000;
pub type Range = (u32, u32);

const EMPTY_OFFSET: u32 = u32::MAX;

#[derive(Clone, Copy, Debug)]
pub struct Identifier {
    offset: u32,
    len: u32,
    extends: Option<u32>
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

#[derive(Clone, Copy, Debug)]
enum BaseRelation {
    Diverged(Ordering),
    Equal,
    /// b1's base is a proper prefix of b2's base.
    /// `discriminant` = b2_base[b1_base.len()] — the value b1's extra
    /// is compared against.
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
}

#[derive(Clone, Debug)]
pub struct IdCmpCache {
    cache: HashMap<(u32, u32), BaseRelation>
}

impl IdCmpCache {
    fn store(&mut self, a: u32, b: u32, ord: BaseRelation) {
        // FIXME: For now insert both relations into the cache 
        // (a,b) and (b,a) this is simple but doubles memory usage.
        // We can optimize by only storing one direction and flipping the relation on lookup.
        self.cache.insert((a, b), ord);
        self.cache.insert((b,a), match ord {
            BaseRelation::Diverged(ord) => BaseRelation::Diverged(ord.reverse()),
            BaseRelation::Equal => BaseRelation::Equal,
            BaseRelation::B1Prefix { discriminant } => BaseRelation::B2Prefix { discriminant },
            BaseRelation::B2Prefix { discriminant } => BaseRelation::B1Prefix { discriminant },
        });
    }

    fn lookup(&self, a: u32, b: u32) -> Option<&BaseRelation> {
        self.cache.get(&(a, b))
    }
}

#[derive(Clone, Debug)]
pub struct IdArena {
    data: Vec<u32>,
    dedup: HashMap<u64, smallvec::SmallVec<[(u32, u32); 1]>>,
    id_cmp_cache: IdCmpCache,
}

impl IdArena {
    pub fn new() -> Self {
        IdArena {
            data: Vec::with_capacity(4096),
            dedup: HashMap::with_capacity(1024),
            id_cmp_cache: IdCmpCache { cache: HashMap::with_capacity(4096) },
        }
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.dedup.clear();
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

    // ── Base relation ────────────────────────────────────────

    /// Compare two base slices once and return a reusable descriptor.
    /// Callers can then resolve any number of `(base, extra)` pairs
    /// against each other without re-walking the slices.
    #[inline]
    fn base_relation(&mut self, b1: Identifier, b2: Identifier) -> BaseRelation {
        // Same interned identity → Equal (fast path)
        if b1.offset == b2.offset {
            return BaseRelation::Equal;
        }

        // Check cache for previously computed relation
        if let Some(cached) = self.id_cmp_cache.lookup(b1.offset, b2.offset) {
            // println!("Cache hit for comparing {:?} and {:?}: {:?}", b1, b2, cached);
            return *cached;
        }

        // It is possible that the new identifier extends an existing one, see if we can get a relation from that 
        if let Some(orig) = b1.extends {
            // Check the cache for the relation between the original identifier and b2
            if let Some(cached) = self.id_cmp_cache.lookup(orig, b2.offset) {
                // If we find a cached relation, we can try to use it to infer the relation between b1 and b2
                let inferred = match cached {
                    BaseRelation::Diverged(ord) => Some(BaseRelation::Diverged(*ord)),
                    BaseRelation::Equal => Some(BaseRelation::B2Prefix { discriminant: b2.len }),
                    BaseRelation::B2Prefix { discriminant } => Some(BaseRelation::B2Prefix { discriminant: *discriminant }),
                    BaseRelation::B1Prefix { .. } => None, // Can't infer if b1 is a prefix of b2
                };
                if let Some(rel) = inferred {
                    // println!("Inferred hit:  {:?} extends {:?} and {:?} {:?}", b1, orig, b2, rel);
                    // Print inferred hit and the original identifiers too (just for testing)
                    // println!("Inferred hit:  {:?} extends {:?} and {:?} {:?}", b1, orig, b2, rel);
                    // println!("Identifier arrays are {:?} and {:?}", self.get_slice(b1), self.get_slice(b2));
                    self.id_cmp_cache.store(b1.offset, b2.offset, rel);
                    return rel;
                }
            }

        }

        let sa = self.get_slice_unchecked(b1);
        let sb = self.get_slice_unchecked(b2);
        let sa_len = sa.len();
        let sb_len = sb.len();
        let min_len = sa_len.min(sb_len);

        let sa_prefix = unsafe { sa.get_unchecked(..min_len) };
        let sb_prefix = unsafe { sb.get_unchecked(..min_len) };

        match sa_prefix.cmp(sb_prefix) {
            Ordering::Equal => {}
            ord => {
                // Add to cache 
                let rel = BaseRelation::Diverged(ord);
                self.id_cmp_cache.store(b1.offset, b2.offset, rel);
                return rel;
                // return BaseRelation::Diverged(ord)
            },
        }

        match sa_len.cmp(&sb_len) {
            Ordering::Equal => {
                // Same content, different offsets (shouldn't happen with
                // proper interning, but handle gracefully).
                // let rel = BaseRelation::Equal;
                self.id_cmp_cache.store(b1.offset, b2.offset, BaseRelation::Equal);
                BaseRelation::Equal
                // BaseRelation::Equal
            }
            Ordering::Less => {
                let rel = BaseRelation::B1Prefix {
                    discriminant: unsafe { *sb.get_unchecked(min_len) },
                };
                self.id_cmp_cache.store(b1.offset, b2.offset, rel);
                rel
            },
            Ordering::Greater => {
                let rel = BaseRelation::B2Prefix {
                    discriminant: unsafe { *sa.get_unchecked(min_len) },
                };
                self.id_cmp_cache.store(b1.offset, b2.offset, rel);
                rel
            },
        }
    }

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
        // Fast path: same base → pure offset arithmetic
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

        // ── Compare bases once, reuse for all ref comparisons ──
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
                // Same begin position but different bases — shouldn't
                // happen in well-formed LogootSplit, but handle gracefully.
                IdOrderingRelation::B1BeforeB2
            }
        }
    }

    pub fn compare_intervals(&mut self, b1: &IdentifierInterval, b2: &IdentifierInterval) -> IdOrderingRelation {
        self.compare_intervals_raw(b1.base, b1.lo, b1.hi, b2.base, b2.lo, b2.hi)
    }

    // ── num_insertable ───────────────────────────────────────

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

    // ── Accessors ────────────────────────────────────────────

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