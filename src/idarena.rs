use std::cmp::Ordering;
use ahash::AHashMap as HashMap;
use crate::state::State;
use rand::RngExt;

pub const MIN_VALUE: u32 = 0;
pub const MAX_VALUE: u32 = 100000;
pub const MAX_AGENTS: u32 = 1000;
pub type Range = (u32, u32);
pub const ID_SIZE: usize = 3;

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

#[derive(Debug)]
pub enum IdOrderingRelation {
    B1BeforeB2,
    B1AfterB2,
    B1InsideB2,
    B2InsideB1,
    B1ConcatB2,
    B2ConcatB1,
    B1EqualsB2,
    /* Two special cases: Equal bases */
    B1BeforeB2E,
    B1AfterB2E,
}

pub enum RunComparision {
    SameBase,
    Adjacent,
    Different
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

    /* 
        The new intern function is only called when we have a *new* identifier 
        which does not exist in the tree already
        This prevents unnecessary double work
    */

    pub fn intern(&mut self, path: &[u32]) -> Identifier {
        let offset = self.data.len() as u32;
        let len = path.len() as u32;
        self.data.extend_from_slice(path);
        Identifier { offset, len }
    }

    #[inline(always)]
    pub fn get_slice(&self, id: Identifier) -> &[u32] {
        if id.is_empty() { return &[]; }
        &self.data[id.offset as usize..(id.offset as usize + id.len as usize)]
    }

    #[inline(always)]
    pub fn get_slice_unchecked(&self, id: Identifier) -> &[u32] {
        debug_assert!(!id.is_empty());
        unsafe {
            self.data.get_unchecked(id.offset as usize..(id.offset as usize + id.len as usize))
        }
    }

    #[inline(always)]
    pub fn compare_ids_raw(&self, x: &[u32], x_offset: u32, y: &[u32], y_offset: u32) -> Ordering {
        let x_tuples = x.len() / ID_SIZE;
        let y_tuples = y.len() / ID_SIZE;
        let min_tuples = x_tuples.min(y_tuples);

        for t in 0..min_tuples {
            let i = t * ID_SIZE;            
            let x1 = unsafe {x.get_unchecked(i)} + if t == x_tuples - 1 { x_offset } else { 0 };
            let y1 = unsafe{y.get_unchecked(i)} + if t == y_tuples - 1 { y_offset } else { 0 };
            let x2 = unsafe {x.get_unchecked(i + 1)};
            let y2 = unsafe {y.get_unchecked(i + 1)};

            let lhs = x1 as u64 * (*y2 as u64);
            let rhs = y1 as u64 * (*x2 as u64);

            match lhs.cmp(&rhs) {
                Ordering::Equal => {
                    let x_extra = unsafe {x.get_unchecked(i + 2)};
                    let y_extra = unsafe {y.get_unchecked(i + 2)};
                    match x_extra.cmp(&y_extra) {
                        Ordering::Equal => continue,
                        cmp => return cmp,
                    }
                }
                cmp => return cmp,
            }
        }

        x_tuples.cmp(&y_tuples)
    }

    fn compare_runs(sa: &[u32], a_off: u32, sb: &[u32], b_off: u32) -> RunComparision {
        if sa.len() != sb.len() { return RunComparision::Different; }
        let d = sa.len() - ID_SIZE; // index of deepest tuple
        // All elements must match except the numerator at index d.
        for i in 0..sa.len() {
            if i == d { continue; }
            if sa[i] != sb[i] { return RunComparision::Different; }
        }
        // The effective numerators must be equal 
        if sa[d].wrapping_add(a_off) == sb[d].wrapping_add(b_off) {
            RunComparision::Adjacent
        } else if sa[d] == sb[d] {
            RunComparision::SameBase
        } else {
            RunComparision::Different
        }
    }

    /// Compare two (base, extra) pairs. Replaces the old compare_refs(IdentifierRef, IdentifierRef).
    #[inline]
    pub fn compare_refs(&self, a_base: Identifier, a_extra: u32, b_base: Identifier, b_extra: u32) -> Ordering {
        if a_base.offset == b_base.offset {
            return a_extra.cmp(&b_extra);
        }
        self.compare_ids_raw(self.get_slice_unchecked(a_base), a_extra, self.get_slice_unchecked(b_base), b_extra)
    }
    
    // Function to compare against a raw identifier slice without interning 
    pub fn compare_intervals(&self, 
        b1_base: &[u32], b1_lo: u32, b1_hi: u32,
        b2_base: Identifier, b2_lo: u32, b2_hi: u32)
        -> IdOrderingRelation
    {
        let b2_base_slice = self.get_slice_unchecked(b2_base);
        let lo_cmp = self.compare_ids_raw(b1_base, b1_lo, b2_base_slice, b2_lo);

        match lo_cmp {
            Ordering::Less => {
                let hi_lo_cmp = self.compare_ids_raw(b1_base, b1_hi-1, b2_base_slice, b2_lo);
                match hi_lo_cmp {
                    Ordering::Less => {
                        // Check for run successorship 
                        match Self::compare_runs(b1_base, b1_hi, b2_base_slice, b2_lo) {
                            RunComparision::Adjacent => IdOrderingRelation::B1ConcatB2,
                            RunComparision::Different => IdOrderingRelation::B1BeforeB2,
                            RunComparision::SameBase => IdOrderingRelation::B1BeforeB2E,
                        }
                    }
                    _ => IdOrderingRelation::B2InsideB1
                }
            } 
            Ordering::Greater => {
                let lo_hi_cmo = self.compare_ids_raw(b1_base, b1_lo, b2_base_slice, b2_hi-1);
                match lo_hi_cmo {
                    Ordering::Greater => {
                        match Self::compare_runs(b2_base_slice, b2_hi, b1_base, b1_lo) {
                            RunComparision::Adjacent => IdOrderingRelation::B2ConcatB1,
                            RunComparision::Different => IdOrderingRelation::B1AfterB2,
                            RunComparision::SameBase => IdOrderingRelation::B1AfterB2E,
                        }
                    }
                    _ => IdOrderingRelation::B1InsideB2
                }
            }
            Ordering::Equal => {
                let hi_cmp = self.compare_ids_raw(b1_base, b1_hi-1, b2_base_slice, b2_hi-1);
                match hi_cmp {
                    Ordering::Less => IdOrderingRelation::B1InsideB2,
                    Ordering::Greater => IdOrderingRelation::B2InsideB1,
                    Ordering::Equal => IdOrderingRelation::B1EqualsB2,
                }
            }
        }
    } 

    /// Count how many k ∈ {0, …, length−1} make (ins, ins_extra+k) < (nxt, nxt_extra).
    /// nxt_extra is applied to nxt's deepest numerator only when depths match.
    fn count_before(
        ins: &[u32], ins_extra: u32,
        nxt: &[u32], nxt_extra: u32,
        length: u32,
    ) -> u32 {
        let ins_depth = ins.len() / ID_SIZE;
        let nxt_depth = nxt.len() / ID_SIZE;
 
        if ins_depth > nxt_depth { return length; }
 
        // Compare prefix tuples (before insert's deepest).
        for t in 0..(ins_depth - 1) {
            let i = t * ID_SIZE;
            let lhs = ins[i] as u64 * nxt[i + 1] as u64;
            let rhs = nxt[i] as u64 * ins[i + 1] as u64;
            if lhs != rhs { return if lhs < rhs { length } else { 0 }; }
            if ins[i + 2] != nxt[i + 2] { return if ins[i + 2] < nxt[i + 2] { length } else { 0 }; }
        }
 
        // At insert's deepest tuple.
        let d = (ins_depth - 1) * ID_SIZE;
        let ins_num = ins[d] as u64 + ins_extra as u64;
        let nxt_num = nxt[d] as u64 + if ins_depth == nxt_depth { nxt_extra as u64 } else { 0 };
 
        let lhs = nxt_num * ins[d + 1] as u64; // nxt × ins_den
        let rhs = ins_num * nxt[d + 1] as u64; // ins × nxt_den
        if lhs < rhs { return 0; }
 
        let gap = lhs - rhs;
        let b_nxt = nxt[d + 1] as u64;
        let q = gap / b_nxt;
        let r = gap % b_nxt;
 
        let mut k = q + 1;
        if r == 0 {
            // Rationals exactly equal at offset q — tiebreak on agent_info then depth.
            if ins[d + 2] > nxt[d + 2]
                || (ins[d + 2] == nxt[d + 2] && ins_depth >= nxt_depth)
            {
                k = q;
            }
        }
 
        length.min(k as u32)
    }
 
    /// How many characters from the run at `insert` can be placed before `next`.
    pub fn num_insertable(
        &self,
        insert_base: Identifier, insert_extra: u32,
        next_base: Identifier, next_extra: u32,
        length: u32,
    ) -> u32 {
        if insert_base == next_base {
            if insert_extra >= next_extra { return 0; }
            return length.min(next_extra - insert_extra);
        }
        Self::count_before(
            self.get_slice_unchecked(insert_base), insert_extra,
            self.get_slice_unchecked(next_base), next_extra,
            length,
        )
    }
 
    /// Find where to split the interval (short_slice, short_lo..short_hi)
    /// when the point long_slice falls inside it.
    pub fn find_split_point(
        &self,
        short_slice: &[u32], short_lo: u32, short_hi: u32,
        long_slice: &[u32], long_offset: u32
    ) -> u32 {
        if short_hi <= short_lo || long_slice.is_empty() { return 0; }
        Self::count_before(short_slice, short_lo, long_slice, long_offset, short_hi - short_lo)
    }

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
    low_base: Identifier, low_offset: u32,
    high_base: Identifier, high_offset: u32,
    state: &mut State,
    count: u32,
) -> Identifier {
    // Materialise the effective low/high identifiers.
    let low_s: Vec<u32> = if low_base.is_empty() {
        vec![]
    } else {
        let s = arena.get_slice(low_base);
        let mut v = s.to_vec();
        let d = v.len() - ID_SIZE;
        v[d] += low_offset;
        v
    };
    let high_s: Vec<u32> = if high_base.is_empty() {
        vec![]
    } else {
        let s = arena.get_slice(high_base);
        let mut v = s.to_vec();
        let d = v.len() - ID_SIZE;
        v[d] += high_offset;
        v
    };
 
    let max_tuples = low_s.len().max(high_s.len()) / ID_SIZE + 2;
    let mut path: Vec<u32> = Vec::with_capacity(max_tuples * ID_SIZE);
 
    let agent_info = state.replica + state.local_clock * MAX_AGENTS;
 
    for t in 0..max_tuples {
        let i = t * ID_SIZE;
 
        // Low bound at this depth (default: 0/1 = 0)
        let (al, bl) = if i + 1 < low_s.len() {
            (low_s[i], low_s[i + 1])
        } else {
            (0u32, 1u32)
        };
 
        // High bound at this depth (default: MAX_VALUE/1)
        let (ah, bh) = if i + 1 < high_s.len() {
            (high_s[i], high_s[i + 1])
        } else {
            (MAX_VALUE, 1u32)
        };
 
        let cross_l = al as u64 * bh as u64;
        let cross_h = ah as u64 * bl as u64;
 
        if cross_l < cross_h {
            // There is room between al/bl and ah/bh.
            let (al, bl, ah, bh) = (al as u64, bl as u64, ah as u64, bh as u64);
            let cnt = count as u64;
 
            // Smallest denominator bm such that `count` distinct integer
            // numerators fit in the open interval (al/bl, ah/bh):
            //   ⌊ah·bm/bh⌋ − ⌈al·bm/bl⌉ ≥ count
            // Conservative: bm = cnt·bl·bh / gap + 1  (always sufficient).
            let gap = ah * bl - al * bh;   // > 0
            let bm = cnt * bl * bh / gap + 1;
 
            // First valid numerator: smallest am with am/bm > al/bl
            // i.e. am·bl > al·bm  ⟹  am ≥ ⌊al·bm/bl⌋ + 1
            let am_min = al * bm / bl + 1;
 
            // Largest valid numerator end: we need (am + cnt − 1)·bh < ah·bm
            // i.e. am + cnt − 1 ≤ ⌊(ah·bm − 1) / bh⌋
            let run_end_max = (ah * bm - 1) / bh; // largest x with x·bh < ah·bm
            let am_max = run_end_max.saturating_sub(cnt - 1);
 
            if am_min <= am_max && am_max <= u32::MAX as u64 && bm <= u32::MAX as u64 {
                // Inject randomness: pick am uniformly in [am_min, am_max].
                let am = if am_min == am_max {
                    am_min
                } else {
                    state.rng.random_range(am_min..=am_max)
                };
 
                path.extend_from_slice(&[am as u32, bm as u32, agent_info]);
                return arena.intern(&path);
            }
            // Overflow — fall through to go deeper.
        }
 
        // No room (or overflow) at this depth.  Copy low's tuple and descend.
        let info = if i + 2 < low_s.len() { low_s[i + 2] } else { 0 };
        path.extend_from_slice(&[al, bl, info]);
    }
 
    unreachable!("could not allocate identifier between low and high")
}
