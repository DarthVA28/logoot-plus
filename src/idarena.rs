use std::cmp::Ordering;
use ahash::AHashMap as HashMap;
use crate::state::State;
use rand::RngExt;

pub const MIN_VALUE: u32 = 0;
pub const MAX_VALUE: u32 = 100000;
pub const MAX_AGENTS: u32 = 1000;
pub type Range = (u32, u32);

#[inline(always)]
pub fn encode_combined(priority: u32, replica: u32) -> u32 {
    (priority << 16) | replica
}

#[inline(always)]
pub fn decode_priority(combined: u32) -> u32 {
    combined >> 16
}

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

    #[inline]
    fn base_relation(&self, b1: Identifier, b2: Identifier) -> BaseRelation {
        if b1.offset == b2.offset {
            return BaseRelation::Equal;
        }

        let sa = self.get_slice_unchecked(b1);
        let sb = self.get_slice_unchecked(b2);
        self.base_relation_raw(sa, sb)
    }

    fn base_relation_raw(&self, sa: &[u32], sb: &[u32]) -> BaseRelation {
        let sa_len = sa.len();
        let sb_len = sb.len();
        let min_len = sa_len.min(sb_len);

        let sa_prefix = unsafe { sa.get_unchecked(..min_len) };
        let sb_prefix = unsafe { sb.get_unchecked(..min_len) };

        match sa_prefix.cmp(sb_prefix) {
            Ordering::Equal => {}
            ord => return BaseRelation::Diverged(ord),
        }

        match sa_len.cmp(&sb_len) {
            Ordering::Equal => BaseRelation::Equal,
            Ordering::Less => BaseRelation::B1Prefix {
                discriminant: unsafe { *sb.get_unchecked(min_len) },
            },
            Ordering::Greater => BaseRelation::B2Prefix {
                discriminant: unsafe { *sa.get_unchecked(min_len) },
            },
        }
    }

    #[inline]
    pub fn compare_ids(&self, a: Identifier, b: Identifier) -> Ordering {
        if a.offset == b.offset { return Ordering::Equal; }
        self.get_slice_unchecked(a).cmp(self.get_slice_unchecked(b))
    }

    /// Compare two (base, extra) pairs. Replaces the old compare_refs(IdentifierRef, IdentifierRef).
    #[inline]
    pub fn compare_refs(&self, a_base: Identifier, a_extra: u32, b_base: Identifier, b_extra: u32) -> Ordering {
        if a_base.offset == b_base.offset {
            return a_extra.cmp(&b_extra);
        }
        self.base_relation(a_base, b_base).compare(a_extra, b_extra)
    }

    /// Compare two intervals given as (base, lo, hi). This is the only interval
    /// comparison function — the old wrapper taking IdentifierInterval is removed.
    pub fn compare_intervals(
        &self,
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

        let rel = self.base_relation(b1_base, b2_base);

        if rel == BaseRelation::Equal {
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

        match rel.compare(b1_lo, b2_lo) {
            Ordering::Less => {
                if rel.compare(b1_hi - 1, b2_lo) == Ordering::Greater {
                    IdOrderingRelation::B2InsideB1
                } else {
                    // Check if bases equal 
                    if rel == BaseRelation::Equal {
                        IdOrderingRelation::B1BeforeB2E
                    } else {
                        IdOrderingRelation::B1BeforeB2
                    }
                }
            }
            Ordering::Greater => {
                if rel.compare(b1_lo, b2_hi - 1) == Ordering::Less {
                    IdOrderingRelation::B1InsideB2
                } else {
                    // Check if bases equal
                    if rel == BaseRelation::Equal {
                        IdOrderingRelation::B1AfterB2E
                    } else {
                        IdOrderingRelation::B1AfterB2
                    }
                }
            }
            Ordering::Equal => {
                if rel.compare(b1_hi-1, b2_hi-1) == Ordering::Equal {
                    return IdOrderingRelation::B1EqualsB2;
                }
                // Random, check!
                IdOrderingRelation::B1BeforeB2
            }
        }
    }
    

    // Function to compare against a raw identifier slice without interning 
    pub fn compare_intervals_first_raw(&self, 
        b1_base: &[u32], b1_lo: u32, b1_hi: u32,
        b2_base: Identifier, b2_lo: u32, b2_hi: u32)
        -> IdOrderingRelation
    {
        let rel = self.base_relation_raw(b1_base, self.get_slice_unchecked(b2_base));

        if rel == BaseRelation::Equal {
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

        match rel.compare(b1_lo, b2_lo) {
            Ordering::Less => {
                if rel.compare(b1_hi - 1, b2_lo) == Ordering::Greater {
                    IdOrderingRelation::B2InsideB1
                } else {
                    // Check if bases equal 
                    if rel == BaseRelation::Equal {
                        IdOrderingRelation::B1BeforeB2E
                    } else {
                        IdOrderingRelation::B1BeforeB2
                    }
                }
            }
            Ordering::Greater => {
                if rel.compare(b1_lo, b2_hi - 1) == Ordering::Less {
                    IdOrderingRelation::B1InsideB2
                } else {
                    // Check if bases equal
                    if rel == BaseRelation::Equal {
                        IdOrderingRelation::B1AfterB2E
                    } else {
                        IdOrderingRelation::B1AfterB2
                    }
                }
            }
            Ordering::Equal => {
                // if rel.compare(b1_hi-1, b2_hi-1) == Ordering::Equal {
                //     return IdOrderingRelation::B1EqualsB2;
                // }
                // Random, check!
                IdOrderingRelation::B1BeforeB2
            }

        }
    } 

    /// How many characters from `insert` can be placed before `next`.
    /// Replaces the old num_insertable(IdentifierRef, IdentifierRef, u32).
    // pub fn num_insertable(
    //     &self,
    //     insert_base: Identifier, insert_extra: u32,
    //     next_base: Identifier, next_extra: u32,
    //     length: u32,
    // ) -> u32 {
    //     let insert_slice = self.get_slice_unchecked(insert_base);
    //     let next_slice = self.get_slice_unchecked(next_base);

    //     let l = insert_slice.len();

    //     if l >= next_slice.len() + 1 { return length; }

    //     let next_full_iter = next_slice.iter().chain(std::iter::once(&next_extra));
    //     for (&a, &b) in insert_slice.iter().zip(next_full_iter) {
    //         if a != b { return length; }
    //     }

    //     let next_at_l = if l < next_slice.len() { next_slice[l] } else { next_extra };
    //     next_at_l + 1 - insert_extra
    // }

    pub fn num_insertable(
        &self,
        insert_base: Identifier, insert_seq: u32,
        next_base: Identifier, next_seq: u32,
        length: u32,
    ) -> u32 {
        let insert_slice = self.get_slice_unchecked(insert_base);
        let next_slice = self.get_slice_unchecked(next_base);
        let ilen = insert_slice.len();

        // If insert base is at least as deep as next's full position,
        // insert can never be a prefix of next → all chars fit.
        // next_full has length next_slice.len() + 1.
        if ilen >= next_slice.len() + 1 {
            return length;
        }

        // Check that insert_base is a prefix of next_full = next_slice ++ [next_seq]
        let next_full_iter = next_slice.iter().chain(std::iter::once(&next_seq));
        for (&a, &b) in insert_slice.iter().zip(next_full_iter) {
            if a != b { return length; }
        }

        // insert_base IS a prefix of next_full.
        // The constraining element is next_full[ilen].
        let constraining = if ilen < next_slice.len() {
            next_slice[ilen]
        } else {
            next_seq
        };

        // If ilen < next_slice.len(): the constraining element is a seq from a
        // prefix tuple, and there are more elements after it in next_full.
        // insert_seq + k == constraining means insert's position is a prefix of next → fits.
        // So count = constraining + 1 - insert_seq.
        //
        // If ilen == next_slice.len(): constraining == next_seq, and this is the LAST
        // element. insert_seq + k == next_seq means EQUAL positions → does NOT fit.
        // So count = constraining - insert_seq = next_seq - insert_seq.

        let count = if ilen < next_slice.len() {
            constraining + 1 - insert_seq
        } else {
            // Same depth: strict less-than needed
            constraining - insert_seq
        };

        count.min(length)
    }

    /// Find where to split `idi_short` (base, lo, hi) when `id_long` falls inside it.
    // pub fn find_split_point(
    //     &self,
    //     short_slice: &[u32], short_lo: u32, short_hi: u32,
    //     long_slice: &[u32],
    // ) -> u32 {
    //     if long_slice.is_empty() { return 0; }

    //     let text_len = short_hi - short_lo;
    //     if text_len == 0 { return 0; }

    //     // let long_slice = self.get_slice_unchecked(id_long);
    //     // let short_slice = self.get_slice_unchecked(short_base);

    //     let min_len = short_slice.len().min(long_slice.len());

    //     let short_prefix = unsafe { short_slice.get_unchecked(..min_len) };
    //     let long_prefix = unsafe { long_slice.get_unchecked(..min_len) };
    //     match short_prefix.cmp(long_prefix) {
    //         Ordering::Less  => return text_len,
    //         Ordering::Greater => return 0,
    //         Ordering::Equal => {}
    //     }

    //     if short_slice.len() < long_slice.len() {
    //         let pivot = unsafe { *long_slice.get_unchecked(min_len) };
    //         let extras_below = if long_slice.len() > min_len + 1 {
    //             pivot.saturating_add(1).saturating_sub(short_lo)
    //         } else {
    //             pivot.saturating_sub(short_lo)
    //         };
    //         return extras_below.min(text_len);
    //     } else {
    //         return 0;
    //     }
    // }

    pub fn find_split_point(
        &self,
        short_base: &[u32], short_lo: u32, short_hi: u32,
        long_base: &[u32],
    ) -> u32 {
        let text_len = short_hi - short_lo;
        if long_base.is_empty() || text_len == 0 { return 0; }

        let min_len = short_base.len().min(long_base.len());
        // Verify prefix match
        if short_base[..min_len] != long_base[..min_len] {
            return if short_base[..min_len] < long_base[..min_len] {
                text_len
            } else {
                0
            };
        }

        if short_base.len() < long_base.len() {
            // long extends short. Element at short_base.len() is a seq number.
            debug_assert!(long_base.len() - short_base.len() >= 3,
                "tuple structure violated: bases differ by {} elements",
                long_base.len() - short_base.len());

            let seq_value = long_base[short_base.len()];
            // Position (…, seq_value, next_priority, next_replica, next_seq)
            // falls between character at seq_value and seq_value+1 in the block.
            // Characters [short_lo, seq_value] go left → count = seq_value + 1 - short_lo
            let chars_left = seq_value.saturating_add(1).saturating_sub(short_lo);
            chars_left.min(text_len)
        } else {
            // short_base.len() >= long_base.len() → long is a prefix of short.
            // The long position comes before all positions in the short block.
            0
        }
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

// pub fn generate_base(
//     arena: &mut IdArena,
//     low_base: Identifier, low_extra: u32,
//     high_base: Identifier, high_extra: u32,
//     state: &mut State,
// ) -> Identifier {
//     let low_slice = arena.get_slice(low_base);
//     let high_slice = arena.get_slice(high_base);

//     let mut new_path: Vec<u32> = Vec::new();
//     let mut low_iter = low_slice.iter().copied().chain(std::iter::once(low_extra));
//     let mut high_iter = high_slice.iter().copied().chain(std::iter::once(high_extra));

//     let mut l = low_iter.next().unwrap_or(MIN_VALUE);
//     let mut h = high_iter.next().unwrap_or(MAX_VALUE);

//     while (h as i32) - (l as i32) < 2 {
//         new_path.push(l);
//         l = low_iter.next().unwrap_or(MIN_VALUE);
//         h = high_iter.next().unwrap_or(MAX_VALUE);
//     }

//     let nxt = state.rng.random_range(l + 1..h);
//     new_path.push(nxt);
//     new_path.push(state.replica + state.local_clock * MAX_AGENTS);

//     arena.intern(&new_path)
// }

pub fn generate_base(
    arena: &mut IdArena,
    low_base: Identifier, low_extra: u32,
    high_base: Identifier, high_extra: u32,
    state: &mut State,
) -> Identifier {
    let low_slice = arena.get_slice(low_base);
    let high_slice = arena.get_slice(high_base);

    let mut low_full: Vec<u32> = Vec::with_capacity(low_slice.len() + 1);
    low_full.extend_from_slice(low_slice);
    low_full.push(low_extra);

    let mut high_full: Vec<u32> = Vec::with_capacity(high_slice.len() + 1);
    high_full.extend_from_slice(high_slice);
    high_full.push(high_extra);

    let mut new_path: Vec<u32> = Vec::new();
    let mut depth = 0;
    let mut high_unconstrained = false;

    loop {
        let l_c = low_full.get(depth * 2).copied().unwrap_or(0);
        let h_c = if high_unconstrained {
            MAX_VALUE << 16
        } else {
            high_full.get(depth * 2).copied().unwrap_or(MAX_VALUE << 16)
        };

        let l_p = decode_priority(l_c) + 1;
        let h_p = decode_priority(h_c);

        if l_p < h_p {
            let chosen = state.rng.random_range(l_p..h_p);
            new_path.push(encode_combined(chosen, state.replica));
            return arena.intern(&new_path);
        }

        let l_s = low_full.get(depth * 2 + 1).copied().unwrap_or(MIN_VALUE);
        new_path.push(l_c);
        new_path.push(l_s);

        if !high_unconstrained {
            let h_s = high_full.get(depth * 2 + 1).copied().unwrap_or(u32::MAX);
            if (l_c, l_s) < (h_c, h_s) {
                high_unconstrained = true;
            }
        }

        depth += 1;
    }
}