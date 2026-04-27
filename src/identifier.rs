use std::sync::Arc;

use rand::RngExt;
use crate::state::State;

pub type Range = (u32, u32);
pub const MIN_VALUE: u32 = 0;
pub const MAX_VALUE: u32 = 100000;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize)]
pub struct Identifier {
    pub id: Arc<[u32]>
}

impl Identifier {
    pub fn new(id: Vec<u32>) -> Self {
        Identifier { id: Arc::from(id.as_slice()) }
    }
    
    pub fn to_string(&self) -> String {
        self.id.iter().map(|x| x.to_string()).collect::<Vec<String>>().join(".")
    }
}

pub type Id = Identifier;

impl PartialOrd for Identifier {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Identifier {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

#[derive(Clone, Copy)]
pub struct IdentifierRef<'a> {
    pub base: &'a [u32],
    pub extra: u32
}

impl<'a> IdentifierRef<'a> {
    pub fn new(id: &'a Identifier, extra: u32) -> Self {
        IdentifierRef { base: &id.id, extra }
    }

    pub fn from_slice(base: &'a [u32]) -> Self {
        let (extra, base) = base.split_last().expect("empty identifier");
        IdentifierRef { base, extra: *extra }
    }

    pub fn doc_start() -> IdentifierRef<'static> {
        IdentifierRef { base: &[], extra: MIN_VALUE }
    }

    pub fn doc_end() -> IdentifierRef<'static> {
        IdentifierRef { base: &[], extra: MAX_VALUE }
    }

    pub fn cmp_slice(&self, other: &[u32]) -> std::cmp::Ordering {
        self.base.iter()
            .chain(std::iter::once(&self.extra))
            .cmp(other.iter())
    }

}

impl PartialEq for IdentifierRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.base == other.base && self.extra == other.extra
    }
}

impl Eq for IdentifierRef<'_> {}

impl PartialOrd for IdentifierRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// impl Ord for IdentifierRef<'_> {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         self.base.iter()
//             .chain(std::iter::once(&self.extra))
//             .cmp(other.base.iter().chain(std::iter::once(&other.extra)))
//     }
// }

// impl Ord for IdentifierRef<'_> {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         match self.base.cmp(other.base) {
//             std::cmp::Ordering::Equal => self.extra.cmp(&other.extra),
//             ord => ord,
//         }
//     }
// }

impl Ord for IdentifierRef<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let min_base = self.base.len().min(other.base.len());
        // Fast path
        match self.base[..min_base].cmp(&other.base[..min_base]) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }

        // Shared prefix equal 
        match self.base.len().cmp(&other.base.len()) {
            std::cmp::Ordering::Equal => {
                self.extra.cmp(&other.extra)
            }
            std::cmp::Ordering::Less => {
                match self.extra.cmp(&other.base[min_base]) {
                    std::cmp::Ordering::Equal => std::cmp::Ordering::Less, 
                    ord => ord,
                }
            }
            std::cmp::Ordering::Greater => {
                match self.base[min_base].cmp(&other.extra) {
                    std::cmp::Ordering::Equal => std::cmp::Ordering::Greater, 
                    ord => ord,
                }
            }
        }
    }
}

pub fn generate_base(id_low: IdentifierRef<'_>, id_high: IdentifierRef<'_>, state: &mut State) -> Identifier {
    let mut base = Vec::new();
    let mut low_iter = id_low.base.iter().copied().chain(std::iter::once(id_low.extra));
    let mut high_iter = id_high.base.iter().copied().chain(std::iter::once(id_high.extra));
    
    let mut l = low_iter.next().unwrap_or(MIN_VALUE);
    let mut h = high_iter.next().unwrap_or(MAX_VALUE);

    while (h as i32) - (l as i32) < 2 {
        base.push(l);
        l = low_iter.next().unwrap_or(MIN_VALUE);
        h = high_iter.next().unwrap_or(MAX_VALUE);
    }

    // Random number between l and h
    let nxt = state.rng.random_range(l+1..h);
    base.push(nxt);
    base.push(state.replica);
    base.push(state.local_clock);
    Identifier { id: Arc::from(base.as_slice()) }
}

// We will also refer to this as a "block"
#[derive(Clone, Debug)]
pub struct IdentifierInterval {
    pub base: Identifier,
    pub lo: u32,
    pub hi: u32
}

impl IdentifierInterval {
    pub fn new(base: Identifier, lo: u32, hi: u32) -> Self {
        IdentifierInterval { base, lo, hi }
    }

    pub fn id_begin(&self) -> IdentifierRef<'_> {
        IdentifierRef::new(&self.base, self.lo)
        // self.base.with_offset(self.lo)
    }

    pub fn id_end(&self) -> IdentifierRef<'_> {
        IdentifierRef::new(&self.base, self.hi-1)
        // self.base.with_offset(self.hi-1)
    }

    // fn contains(&self, id: IdentifierRef<'_>) -> bool {
    //     self.id_begin() < id && id < self.id_end()
    // }
}

pub enum IdOrderingRelation {
    B1BeforeB2,
    B1AfterB2,
    B1InsideB2,
    B2InsideB1,
    B1ConcatB2,
    B2ConcatB1,
    B1EqualsB2
}

#[inline(always)]
pub fn compare_intervals_raw(
    b1_base: &Identifier, b1_lo: u32, b1_hi: u32,
    b2_base: &Identifier, b2_lo: u32, b2_hi: u32,
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

    // Different bases.
    // _b1_end and _b2_end are computed in the original but never used.
    // b1_start and b1_begin are the same value in the original (id_begin called twice).
    // We compute each once.
    let b1_begin = IdentifierRef::new(b1_base, b1_lo);       // = b1.id_begin()
    let b1_end   = IdentifierRef::new(b1_base, b1_hi - 1);   // = b1.id_end(), used in contains()
    let b2_begin = IdentifierRef::new(b2_base, b2_lo);       // = b2.id_begin()
    let b2_end   = IdentifierRef::new(b2_base, b2_hi - 1);   // = b2.id_end(), used in contains()

    // b1.contains(b2_begin): b1.id_begin() < b2_begin && b2_begin < b1.id_end()
    if b1_begin < b2_begin && b2_begin < b1_end {
        return IdOrderingRelation::B2InsideB1;
    }
    // b2.contains(b1_begin): b2.id_begin() < b1_begin && b1_begin < b2.id_end()
    if b2_begin < b1_begin && b1_begin < b2_end {
        return IdOrderingRelation::B1InsideB2;
    }

    // Final: b1_start < b2_start in original, which is b1_begin < b2_begin
    if b1_begin < b2_begin {
        IdOrderingRelation::B1BeforeB2
    } else {
        IdOrderingRelation::B1AfterB2
    }
}

#[inline(always)]
pub fn compare_intervals(b1: &IdentifierInterval, b2: &IdentifierInterval) -> IdOrderingRelation {
    compare_intervals_raw(&b1.base, b1.lo, b1.hi, &b2.base, b2.lo, b2.hi)
}

// pub fn compare_intervals(b1: &IdentifierInterval, b2: &IdentifierInterval) -> IdOrderingRelation {
//     if b1.base == b2.base {
//         if b1.lo == b2.lo && b1.hi == b2.hi {
//             return IdOrderingRelation::B1EqualsB2
//         } else if b1.hi == b2.lo {
//             return IdOrderingRelation::B1ConcatB2
//         } else if b2.hi == b1.lo {
//             return IdOrderingRelation::B2ConcatB1
//         } else if b1.lo >= b2.lo && b1.hi <= b2.hi {
//             return IdOrderingRelation::B1InsideB2
//         } else if b2.lo >= b1.lo && b2.hi <= b1.hi {
//             return IdOrderingRelation::B2InsideB1
//         } else if b1.lo < b2.lo {
//             return IdOrderingRelation::B1BeforeB2
//         } else {
//             return IdOrderingRelation::B1AfterB2
//         }
//     }

//     // Different bases -- check if bases fall in each other's range
//     let b1_start = b1.id_begin();
//     let _b1_end = b1.id_end();
//     let b2_start = b2.id_begin();
//     let _b2_end = b2.id_end();

//     // Containment checks 
//     let b1_begin = b1.id_begin();
//     let b2_begin = b2.id_begin();

//     if b1.contains(b2_begin) {
//         return IdOrderingRelation::B2InsideB1
//     } else if b2.contains(b1_begin) {
//         return IdOrderingRelation::B1InsideB2
//     }

//     if b1_start < b2_start {
//         return IdOrderingRelation::B1BeforeB2
//     } else {
//         return IdOrderingRelation::B1AfterB2
//     }
// }

pub fn num_insertable(id_insert: IdentifierRef<'_>, id_next: IdentifierRef<'_>, length: u32) -> u32 { 
    let l = id_insert.base.len(); 
    if l >= id_next.base.len() + 1 { return length; }

    // Check all prefix components match
    for (a, b) in id_insert.base.iter().zip(id_next.base.iter().chain(std::iter::once(&id_next.extra))) {
        if a != b { return length; }
    }

    // Compare the extra (last) components
    let next_at_l = if l < id_next.base.len() { id_next.base[l] } else { id_next.extra };
    next_at_l + 1 - id_insert.extra
}
