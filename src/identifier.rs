use std::sync::Arc;

use rand::RngExt;
use crate::state::State;

pub type Range = (u32, u32);
pub const MIN_VALUE: u32 = 0;
pub const MAX_VALUE: u32 = 100;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize)]
pub struct Identifier {
    pub id: Arc<[u32]>
}

impl Identifier {
    pub fn new(id: Vec<u32>) -> Self {
        Identifier { id: Arc::from(id.as_slice()) }
    }

    // pub fn with_offset(&self, offset: u32) -> Self {
    //     let mut new_id = self.id.clone();
    //     new_id.push(offset);
    //     Identifier { id: new_id }
    // }

    // pub fn is_base_same(&self, other: &Identifier) -> bool {
    //     // FIXME: 
    //     self.id == other.id
    // }
    
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

impl Ord for IdentifierRef<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.base.iter()
            .chain(std::iter::once(&self.extra))
            .cmp(other.base.iter().chain(std::iter::once(&other.extra)))
    }
}

pub fn generate_base(id_low: IdentifierRef<'_>, id_high: IdentifierRef<'_>, state: &mut State) -> Identifier {
    let mut base = Vec::new();
    let mut low_iter = id_low.base.iter().copied().chain(std::iter::once(id_low.extra));
    let mut high_iter = id_high.base.iter().copied().chain(std::iter::once(id_high.extra));

    // let mut low_iter = low_full.iter();
    // let mut high_iter = id_high.id.iter();
    // println!("Generating base between {:?} and {:?}", id_low, id_high);
    
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

    fn contains(&self, id: IdentifierRef<'_>) -> bool {
        self.id_begin() < id && id < self.id_end()
    }
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

pub fn compare_intervals(b1: &IdentifierInterval, b2: &IdentifierInterval) -> IdOrderingRelation {
    if b1.base == b2.base {
        if b1.lo == b2.lo && b1.hi == b2.hi {
            return IdOrderingRelation::B1EqualsB2
        } else if b1.hi == b2.lo {
            return IdOrderingRelation::B1ConcatB2
        } else if b2.hi == b1.lo {
            return IdOrderingRelation::B2ConcatB1
        } else if b1.lo >= b2.lo && b1.hi <= b2.hi {
            return IdOrderingRelation::B1InsideB2
        } else if b2.lo >= b1.lo && b2.hi <= b1.hi {
            return IdOrderingRelation::B2InsideB1
        } else if b1.lo < b2.lo {
            return IdOrderingRelation::B1BeforeB2
        } else {
            return IdOrderingRelation::B1AfterB2
        }
    }

    // Different bases -- check if bases fall in each other's range
    let b1_start = b1.id_begin();
    let _b1_end = b1.id_end();
    let b2_start = b2.id_begin();
    let _b2_end = b2.id_end();

    // Containment checks 
    let b1_begin = b1.id_begin();
    let b2_begin = b2.id_begin();

    if b1.contains(b2_begin) {
        return IdOrderingRelation::B2InsideB1
    } else if b2.contains(b1_begin) {
        return IdOrderingRelation::B1InsideB2
    }

    if b1_start < b2_start {
        return IdOrderingRelation::B1BeforeB2
    } else {
        return IdOrderingRelation::B1AfterB2
    }
}

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
