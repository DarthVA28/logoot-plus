use std::cmp::Ordering;
use crate::state::State;

pub const MIN_VALUE: u32 = 0;
pub const MAX_VALUE: u32 = 100000;
pub const MAX_AGENTS: u32 = 1000;
pub type Range = (u32, u32);
pub const TUPLE_SIZE: usize = 4;

const EMPTY_OFFSET: u32 = u32::MAX;

#[derive(Clone, Copy, Debug)]
pub struct Identifier {
    offset: u32,
    len: u32
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

pub enum IdOrderingRelation {
    B1BeforeB2,
    B1AfterB2,
    B1InsideB2,
    B2InsideB1,
    B1ConcatB2,
    B2ConcatB1,
    B1EqualsB2,
}

// #[derive(Clone, Copy, Debug)]
// enum BaseRelation {
//     Diverged(Ordering),
//     Equal,
//     B1Prefix { discriminant: u64 },
//     B2Prefix { discriminant: u64 },
// }

#[derive(Clone, Debug)]
pub struct IdArena {
    data: Vec<u64>,
}

/* For representing a block of identifiers 
These have identifiers 
a/b, a+1/b, ... and so on (for the lowest 4 tuple)
*/
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct IdBlock {
    pub low: Identifier, 
    pub high: Identifier, 
    pub count: u32, // how many identifiers are in the block (including low and high)
}

impl IdBlock {
    pub fn new(low: Identifier, count: u32, arena: &mut IdArena) -> Self {
        let high = IdBlock::id_with_offset(arena, low, count-1);
        IdBlock { low, high, count }
    }

    pub fn low<'a>(&self, arena: &'a IdArena) -> &'a [u64] {
        arena.get_slice_unchecked(self.low)
    }

    pub fn high<'a>(&self, arena: &'a IdArena) -> &'a [u64] {
        arena.get_slice_unchecked(self.high)
    }

    // Update lo by n 
    pub fn truncate_start(&mut self, arena: &mut IdArena, n: u32) {
        let low_s = arena.get_slice_unchecked(self.low);
        let mut new_low_s = low_s.to_vec();
        let last_idx = new_low_s.len() - TUPLE_SIZE;
        new_low_s[last_idx] += n as u64;
        self.low = arena.push_id(&new_low_s);
        self.count -= n;
    }

    pub fn truncate_end(&mut self, arena: &mut IdArena, n: u32) {
        let high_s = arena.get_slice_unchecked(self.high);
        let mut new_high_s = high_s.to_vec();
        let last_idx = new_high_s.len() - TUPLE_SIZE;
        new_high_s[last_idx] -= n as u64;
        self.high = arena.push_id(&new_high_s);
        self.count -= n;
    }

    pub fn extend_end(&mut self, arena: &mut IdArena, n: u32) {
        let high_s = arena.get_slice_unchecked(self.high);
        let mut new_high_s = high_s.to_vec();
        let last_idx = new_high_s.len() - TUPLE_SIZE;
        new_high_s[last_idx] += n as u64;
        self.high = arena.push_id(&new_high_s);
        self.count += n;
    }

    pub fn id_with_offset(arena: &mut IdArena, id: Identifier, offset: u32) -> Identifier {
        if offset == 0 {
            return id;
        }
        let low_s = arena.get_slice_unchecked(id);
        let low_len = low_s.len();
        let mut high_s = low_s.to_vec();
        // In the last level, we add count to the numerator, keeping the same denominator, replica and clock.
        let last_idx = low_len - TUPLE_SIZE;
        high_s[last_idx] += offset as u64;
        arena.push_id(&high_s)
    }
}


impl IdArena {
    pub fn new() -> Self {
        IdArena {
            data: Vec::with_capacity(4096)
        }
    }

    pub fn push_id(&mut self, path: &[u64]) -> Identifier {
        // debug_assert!(path.len() % TUPLE_SIZE == 0);
        let offset = self.data.len() as u32;
        let len = path.len() as u32;
        self.data.extend_from_slice(path);
        Identifier { offset, len }
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    #[inline(always)]
    pub fn get_slice_unchecked(&self, id: Identifier) -> &[u64] {
        debug_assert!(!id.is_empty());
        unsafe {
            self.data.get_unchecked(id.offset as usize..(id.offset as usize + id.len as usize))
        }
    }

    /* 
    Identifier format is now: 
    Tuples of [a, b, replica, clk] where the first two components represent the rational number a/b
    To compare two identifiers we must compare by a1*b2 and a2*b1 (as i128s)
    If these are same, move to replica clk, and if needed more components 
    Invariant: Size of identifier is always 4k
    */
    #[inline]
    fn compare_ids_raw(&self, sa: &[u64], sb: &[u64]) -> Ordering {
        let sa_len = sa.len();
        let sb_len = sb.len();
        let min_len = sa_len.min(sb_len);

        // Iterate in groups of 4 over both
        for i in (0..min_len).step_by(TUPLE_SIZE) {
            let a1 = sa[i] as u128;
            let b1 = sa[i + 1] as u128;
            let a2 = sb[i] as u128;
            let b2 = sb[i + 1] as u128;

            let left = a1 * b2;
            let right = a2 * b1;

            match left.cmp(&right) {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                Ordering::Equal => {},
            }

            // If rational numbers are equal, compare replica and clock
            let r_cmp = sa[i + 2].cmp(&sb[i + 2]);
            if r_cmp != Ordering::Equal {
                return r_cmp;
            }

            let clk_cmp = sa[i + 3].cmp(&sb[i + 3]);
            if clk_cmp != Ordering::Equal {
                return clk_cmp;
            }
        }

        return sa_len.cmp(&sb_len)
    }

    pub fn compare_ids(&self, a: Identifier, b: Identifier) -> Ordering {
        let sa = self.get_slice_unchecked(a);
        let sb = self.get_slice_unchecked(b);
        self.compare_ids_raw(sa, sb)
    }

    fn is_run_successor(&self, s1: &[u64], s2: &[u64]) -> bool {
        if s1.len() != s2.len() { return false; }
        let len = s2.len();
        let num_idx = len - 4;
        if s1[num_idx] + 1 != s2[num_idx] { return false; }
        for i in 0..len {
            if i == num_idx { continue; }
            if s2[i] != s1[i] { return false; }
        }
        true
    }

    pub fn compare_intervals(&self, b1: &IdBlock, b2: &IdBlock) -> IdOrderingRelation {
        let s1_lo = self.get_slice_unchecked(b1.low);
        let s2_lo = self.get_slice_unchecked(b2.low);

        // Slow path: different runs
        let lo_cmp = self.compare_ids_raw(s1_lo, s2_lo);

        match lo_cmp {
            Ordering::Less => {
                // b1 starts before b2. Either fully before, concat, or b2 inside b1.
                let s1_hi = self.get_slice_unchecked(b1.high);
                let hi_lo = self.compare_ids_raw(s1_hi, s2_lo);
                match hi_lo {
                    Ordering::Less => {
                        if self.is_run_successor(s1_hi, s2_lo) {
                            IdOrderingRelation::B1ConcatB2
                        } else {
                            IdOrderingRelation::B1BeforeB2
                        }
                    }
                    _ => IdOrderingRelation::B2InsideB1,
                }
            }
            Ordering::Greater => {
                // b1 starts after b2. Either fully after, concat, or b1 inside b2.
                let s2_hi = self.get_slice_unchecked(b2.high);
                let lo_hi = self.compare_ids_raw(s1_lo, s2_hi);
                match lo_hi {
                    Ordering::Greater => {
                        if self.is_run_successor(s2_hi, s1_lo) {
                            IdOrderingRelation::B2ConcatB1
                        } else {
                            IdOrderingRelation::B1AfterB2
                        }
                    }
                    _ => IdOrderingRelation::B1InsideB2,
                }
            }
            Ordering::Equal => {
                // Same start. Whoever has the longer range contains the other.
                let s1_hi = self.get_slice_unchecked(b1.high);
                let s2_hi = self.get_slice_unchecked(b2.high);
                match self.compare_ids_raw(s1_hi, s2_hi) {
                    Ordering::Equal => IdOrderingRelation::B1EqualsB2,
                    Ordering::Less => IdOrderingRelation::B1InsideB2,
                    Ordering::Greater => IdOrderingRelation::B2InsideB1,
                }
            }
        }
    }

    /// How many identifiers in the run starting at `insert` (incrementing
    /// deepest numerator by 0, 1, 2, ...) are ordered before `next`?
    /// Capped at `length`.
    pub fn num_insertable(&self, insert: Identifier, next: Identifier, length: u32) -> u32 {
        let ins = self.get_slice_unchecked(insert);
        let nxt = self.get_slice_unchecked(next);
        let ins_depth = ins.len() / TUPLE_SIZE;
        let nxt_depth = nxt.len() / TUPLE_SIZE;

        if ins_depth > nxt_depth { return length; }

        for t in 0..(ins_depth - 1) {
            let i = t * TUPLE_SIZE;
            let lhs = ins[i] as i128 * nxt[i + 1] as i128;
            let rhs = nxt[i] as i128 * ins[i + 1] as i128;
            if lhs != rhs {
                return if lhs < rhs { length } else { 0 };
            }
            if ins[i + 2] != nxt[i + 2] || ins[i + 3] != nxt[i + 3] {
                return if (ins[i + 2], ins[i + 3]) < (nxt[i + 2], nxt[i + 3]) { length } else { 0 };
            }
        }

        let d = (ins_depth - 1) * TUPLE_SIZE;
        let gap = nxt[d] as i128 * ins[d + 1] as i128
                - ins[d] as i128 * nxt[d + 1] as i128;

        if gap < 0 { return 0; }

        let b_nxt = nxt[d + 1] as i128;
        let q = gap / b_nxt;
        let r = gap % b_nxt;

        let k = if r != 0 {
            q + 1
        } else if ins_depth < nxt_depth {
            q + 1
        } else if (ins[d + 2], ins[d + 3]) < (nxt[d + 2], nxt[d + 3]) {
            q + 1
        } else {
            q
        };

        length.min(k as u32)
    }

    // Precondition: point is inside block.
    pub fn find_split_point(&self, block: &IdBlock, point: Identifier) -> u32 {
        self.num_insertable(block.low, point, block.count)
    }   

    // [2,3, r1, c1], [2,3, r1, c1, 5, 7, r2, c2]
    // [2,3, r1, c1, .., .., r3, c3] < 5, 7 
    // [7/10, r3, c3]
    // FIXME
    // pub fn generate_id(&mut self, low: Identifier, high: Identifier, state: &mut State) -> Identifier {
    //     let low_s = if low.is_empty() { &[] as &[u64] } else { self.get_slice_unchecked(low) };
    //     let high_s = if high.is_empty() { &[] as &[u64] } else { self.get_slice_unchecked(high) };

    //     let mut path: Vec<u64> = Vec::with_capacity(TUPLE_SIZE);
    //     let max_tuples = low_s.len().max(high_s.len()) / TUPLE_SIZE + 2;

    //     for t in 0..max_tuples {
    //         let i = t * TUPLE_SIZE;
    //         let (a_l, b_l) = if i + 1 < low_s.len() { (low_s[i], low_s[i + 1]) } else { (0, 1) };
    //         let (a_h, b_h) = if i + 1 < high_s.len() { (high_s[i], high_s[i + 1]) } else { (MAX_VALUE as u64, 1) };

    //         let cross_l = a_l as u128 * b_h as u128;
    //         let cross_h = a_h as u128 * b_l as u128;

    //         if cross_l < cross_h {
    //             let a_m = (a_l as u128) + (a_h as u128);
    //             let b_m = (b_l as u128) + (b_h as u128);

    //             if a_m <= u64::MAX as u128 && b_m <= u64::MAX as u128 {
    //                 path.extend_from_slice(&[
    //                     a_m as u64, b_m as u64,
    //                     state.replica as u64, state.local_clock as u64,
    //                 ]);
    //                 let id = self.push_id(&path);
    //                 return id;
    //             }
    //         }

    //         // No room or mediant overflows 
    //         // go deeper
    //         path.extend_from_slice(&[
    //             a_l, b_l,
    //             if i + 2 < low_s.len() { low_s[i + 2] } else { 0 },
    //             if i + 3 < low_s.len() { low_s[i + 3] } else { 0 },
    //         ]);
    //     }

    //     unreachable!()
    // }

    // /// Check whether `id` is an actual member of the run represented by `block`.
    // /// Same depth, same prefix (all tuples except last numerator), and last
    // /// numerator in [lo_num, hi_num].
    pub fn id_in_block(&self, id: Identifier, block: &IdBlock) -> bool {
        let id_s = self.get_slice_unchecked(id);
        let lo_s = self.get_slice_unchecked(block.low);
        let hi_s = self.get_slice_unchecked(block.high);

        if id_s.len() != lo_s.len() {
            return false;
        }

        let num_idx = id_s.len() - TUPLE_SIZE;
        for i in 0..id_s.len() {
            if i == num_idx { continue; }
            if id_s[i] != lo_s[i] {
                return false;
            }
        }

        id_s[num_idx] >= lo_s[num_idx] && id_s[num_idx] <= hi_s[num_idx]
    }

    pub fn generate_id(
        &mut self,
        low: Identifier,
        high: Identifier,
        state: &mut State,
        count: u32,
    ) -> Identifier {
        let low_s = if low.is_empty() { &[] as &[u64] } else { self.get_slice_unchecked(low) };
        let high_s = if high.is_empty() { &[] as &[u64] } else { self.get_slice_unchecked(high) };

        let mut path: Vec<u64> = Vec::with_capacity(TUPLE_SIZE);
        let max_tuples = low_s.len().max(high_s.len()) / TUPLE_SIZE + 2;

        for t in 0..max_tuples {
            let i = t * TUPLE_SIZE;
            let (al, bl) = if i + 1 < low_s.len()  { (low_s[i],  low_s[i + 1])  } else { (0, 1) };
            let (ah, bh) = if i + 1 < high_s.len() { (high_s[i], high_s[i + 1]) } else { (MAX_VALUE as u64, 1) };

            let cross_l = al as u128 * bh as u128;
            let cross_h = ah as u128 * bl as u128;

            if cross_l < cross_h {
                let (al, bl, ah, bh) = (al as u128, bl as u128, ah as u128, bh as u128);
                let cnt = count as u128;
                let gap = ah * bl - al * bh;

                // Exact: smallest b_m such that count integers fit in (al/bl, ah/bh)
                let b_m = cnt * bl * bh / gap + 1;
                let a_m = al * b_m / bl + 1;  
                let end = a_m + cnt - 1;

                if end <= u64::MAX as u128 && b_m <= u64::MAX as u128 {
                    path.extend_from_slice(&[
                        a_m as u64, b_m as u64,
                        state.replica as u64, state.local_clock as u64,
                    ]);
                    return self.push_id(&path);
                }
            }

            // No room at this depth — go deeper
            path.extend_from_slice(&[
                al, bl,
                if i + 2 < low_s.len() { low_s[i + 2] } else { 0 },
                if i + 3 < low_s.len() { low_s[i + 3] } else { 0 },
            ]);
        }

        unreachable!()
    }
}

