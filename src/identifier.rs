use rand::RngExt;
use crate::state::State;

pub type Range = (u32, u32);
pub const MIN_VALUE: u32 = 0;
pub const MAX_VALUE: u32 = 100;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize)]
pub struct Identifier {
    pub id: Vec<u32>
}

impl Identifier {
    pub fn new(id: Vec<u32>) -> Self {
        Identifier { id }
    }

    pub fn with_offset(&self, offset: u32) -> Self {
        let mut new_id = self.id.clone();
        new_id.push(offset);
        Identifier { id: new_id }
    }

    pub fn is_base_same(&self, other: &Identifier) -> bool {
        // FIXME: 
        self.id == other.id
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

pub fn generate_base(id_low: &Id, id_high: &Id, state: &mut State) -> Id {
    let mut base = Vec::new();
    let mut low_iter = id_low.id.iter();
    let mut high_iter = id_high.id.iter();
    // println!("Generating base between {:?} and {:?}", id_low, id_high);
    
    let mut l = *low_iter.next().unwrap_or(&MIN_VALUE);
    let mut h = *high_iter.next().unwrap_or(&MAX_VALUE);

    while (h as i32) - (l as i32) < 2 {
        base.push(l);
        l = *low_iter.next().unwrap_or(&MIN_VALUE);
        h = *high_iter.next().unwrap_or(&MAX_VALUE);
    }

    // Random number between l and h
    let nxt = state.rng.random_range(l+1..h);
    base.push(nxt);
    base.push(state.replica);
    base.push(state.local_clock);
    Identifier { id: base }
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

    pub fn id_begin(&self) -> Id {
        self.base.with_offset(self.lo)
    }

    pub fn id_end(&self) -> Id {
        self.base.with_offset(self.hi-1)
    }

    pub fn contains(&self, id: &Id) -> bool {
        self.id_begin() < *id && *id < self.id_end()
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
    if b1.base.is_base_same(&b2.base) {
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
    let b1_start: Identifier = b1.id_begin();
    let _b1_end = b1.id_end();
    let b2_start = b2.id_begin();
    let _b2_end: Identifier = b2.id_end();

    // Containment checks 
    if b1.contains(&b2.id_begin()) {
        return IdOrderingRelation::B2InsideB1
    } else if b2.contains(&b1.id_begin()) {
        return IdOrderingRelation::B1InsideB2
    }

    if b1_start < b2_start {
        return IdOrderingRelation::B1BeforeB2
    } else {
        return IdOrderingRelation::B1AfterB2
    }
}

pub fn num_insertable(id_insert: &Id, id_next: &Id, length: u32) -> u32 { 
    // println!("Calculating num_insertable between {:?} and {:?} with length {}", id_insert, id_next, length);
    let l = id_insert.id.len()-1;
    if l >= id_next.id.len() {
        return length
    }
    for i in 0..l {
        if id_insert.id[i] != id_next.id[i] {
            return length
        }
    }
    // println!("id_insert.id[l] is {}, id_next.id[l] is {}, so num_insertable is {}", id_insert.id[l], id_next.id[l], id_next.id[l] + 1 - id_insert.id[l]);
    id_next.id[l] + 1 - id_insert.id[l]
}
