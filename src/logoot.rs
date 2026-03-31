/* 
An implementation of the Logoot CRDT for collaborative text editing.
*/

use std::{collections::HashMap, iter, u32::MIN, vec};

use rand::RngExt;

const MIN_VALUE: u32 = 0;
const MAX_VALUE: u32 = 50;

type Id = Vec<u32>; 
type Range = (u32, u32);

struct State { 
    local_clock: u32, 
    replica: u32
}

struct InfiniteIterator<T: Copy> {
    elements: Vec<T>,
    current: usize,
    infinity: T    
}

impl <T: Copy> InfiniteIterator<T> {
    fn new(elements: Vec<T>, infinity: T) -> Self {
        InfiniteIterator { elements, current: 0, infinity }
    }

    fn next(&mut self) -> T {
        if self.current < self.elements.len() {
            let element = self.elements[self.current];
            self.current += 1;
            element
        } else {
            self.infinity
        }
    }
}

enum OperationType {
    Insert,
    Delete
}   

struct Operation { 
    op_type: OperationType,
    ids: Vec<Id>,
    offsets: Vec<usize>,
    payload: Option<String>,
    site: u32, 
    clock: u32
}

struct Block {
    base: Id,
    range: Range,
    value: String,
    size: usize,
    creator: u32
}

struct Document { 
    blocks: Vec<Block>,
    state: State,
    // TODO::: Clear performance bottleneck: we compute hash of Ids which is an array
    used_ranges_for_id: HashMap<Id, Range>,
    snapshot: String,
    operations: Vec<Operation>, 
    // TODO:: Assumes causal messages, fix later
    last_seen: HashMap<u32, u32>
}

impl Document {
    fn new(id: u32) -> Self {
        Document {
            blocks: Vec::new(),
            state: State { local_clock: 0, replica: id },
            used_ranges_for_id: HashMap::new(),
            snapshot: String::new(),
            operations: Vec::new(),
            last_seen: HashMap::new()
        }
    }

    fn ins(&mut self, pos: u32, text: String) -> Operation {
        // TODO
        todo!()
    }

    fn del(&mut self, from: u32, to: u32) -> Operation {
        // TODO
        todo!()
    }

    fn read(&self) -> String {
        let mut res = String::new();
        for block in &self.blocks {
            res.push_str(&block.value);
        }
        res
    }

    fn merge_from(&mut self, other: &Document) {
        // TODO
        todo!()
    }

    fn reset (&mut self) {
        self.blocks.clear();
        self.used_ranges_for_id.clear();
        self.snapshot.clear();
        self.operations.clear();
        self.last_seen.clear();
    }
}

struct Position {
    idx: usize,
    offset: usize 
}

enum PosInfo { 
    Found(Position), 
    NotFound
}


/* Generate a new base between idLow and idHigh */
fn generate_base(id_low: &Id, id_high: &Id, state: &State) -> Id {
    let mut base = Vec::new();
    let mut low_iter = id_low.iter();
    let mut high_iter = id_high.iter();
    
    let mut l = low_iter.next().unwrap_or(&MIN_VALUE);
    let mut h = high_iter.next().unwrap_or(&MAX_VALUE);

    while (h-l < 2) {
        base.push(*l);
        l = low_iter.next().unwrap_or(&MIN_VALUE);
        h = high_iter.next().unwrap_or(&MAX_VALUE);
    }

    // Random number between l and h
    let nxt = rand::rng().random_range(l+1..*h);
    base.push(nxt);
    base.push(state.replica);
    base.push(state.local_clock);
    base
}

fn num_insertable(id_insert: &Id, id_next: &Id, length: u32) -> u32 { 
    let l = id_insert.len()-1;
    if l >= id_next.len() {
        return length
    }
    for i in 0..l {
        if id_insert[i] != id_next[i] {
            return length
        }
    }
    id_next[l] - id_insert[l] - 1
}


// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn it_works() {
//         let result = add(2, 2);
//         assert_eq!(result, 4);
//     }
// }
