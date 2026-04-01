/* 
An implementation of the Logoot CRDT for collaborative text editing.
*/

pub mod blocktree;
pub mod identifiers;

use crate::identifiers::{Id, Range, get_combined_id};
use crate::blocktree::{BlockTree, BlockNode};

use std::{collections::HashMap};

use rand::RngExt;

const MIN_VALUE: u32 = 0;
const MAX_VALUE: u32 = 50;

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
    offsets: Vec<u32>,
    payload: Option<String>,
    site: u32, 
    clock: u32
}

struct Document { 
    blocks: BlockTree,
    state: State,
    used_ranges_for_id: HashMap<Id, Range>,
    snapshot: String,
    operations: Vec<Operation>, 
    // TODO:: Assumes causal messages, fix later
    last_seen: HashMap<u32, u32>
}

impl Document {
    fn new(id: u32) -> Self {
        Document {
            blocks: BlockTree::new(),
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
        for block in self.blocks.inorder_iter() {
            res.push_str(&block.content());
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

fn extend_block(doc: &mut Document, text:String, block: usize, path: &Vec<usize>, site: u32) -> Operation {
    // Check if we can extend the block without clashing with the next block 
    let next = doc.blocks.next(block, path);
    let insert_base = doc.blocks.base_id(block);
    let insert_offsets = doc.blocks.ranges(block);

    if !next.is_none() {
        let text_len = text.chars().count() as u32;
        let nxt_block = next.unwrap();
        // Get bases and offsets for the block and the next block
        let next_base = doc.blocks.base_id(nxt_block);
        let next_offsets: (u32, u32) = doc.blocks.ranges(nxt_block);
        // Get final IDs
        let id_insert = get_combined_id(insert_base, insert_offsets.1);
        let id_next = get_combined_id(next_base, next_offsets.0);
        let n = num_insertable(&id_insert, &id_next, text_len);
        if n < text_len {
            // Cannot extend the block without clashing with the next block
            // Insert n chars here and then insert a new block
            // Remaining text to be inserted in a new block
            // Get substring n..
            // let rest = text.chars().skip(n as usize).collect::<String>();
            // doc.blocks.extend_content(block, &text); 
            let id_low = get_combined_id(insert_base, insert_offsets.1 + n);
            let id_high = get_combined_id(next_base, next_offsets.0);
            return insert_new_block(doc, &id_low, &id_high, text, site);   
        }
    }
    doc.blocks.extend_content(block, &text);
    return Operation 
    { op_type: OperationType::Insert, 
        ids: vec![doc.blocks.base_id(block).clone()], 
        offsets: vec![insert_offsets.1], 
        payload: Some(text), 
        site: site, 
        clock: doc.state.local_clock 
    }
}

fn insert_new_block(doc: &mut Document, idLow: &Id, idHigh: &Id, text: String, site: u32) -> Operation {
    let base = generate_base(idLow, idHigh, &doc.state);
    let size = text.chars().count() as u32;
    let base_block = doc.blocks.create_base_block(base.clone(), (0, size), site);
    doc.blocks.insert(text.clone(), base_block, 0);
    return Operation 
    { op_type: OperationType::Insert, 
        ids: vec![base], 
        offsets: vec![0], 
        payload: Some(text), 
        site: site, 
        clock: doc.state.local_clock 
    }
}

fn split_and_insert_block(doc: &mut Document, text: String, block: usize, path: &Vec<usize>, sp: u32, site: u32) -> Operation {
    // sp is the split point 
    let base = doc.blocks.base(block);
    let offsets = doc.blocks.ranges(block);

    // Create 2 new blocks with the content split at sp 
    let content = doc.blocks.content(block);
    let lcontent = content.chars().take(sp as usize).collect::<String>();
    let rcontent = content.chars().skip(sp as usize).collect::<String>();
    
    let res = doc.blocks.delete(base, 0);
    if res.is_err() {
        panic!("Error deleting block during split");
    }

    doc.blocks.insert(lcontent, base, offsets.0);
    doc.blocks.insert(rcontent, base, offsets.0 + sp);

    // Insert the new block in between
    let base_id = doc.blocks.base_id(block);
    let id_low = get_combined_id(base_id, offsets.0 + sp-1);
    let id_high = get_combined_id(base_id, offsets.0 + sp);

    let new_id = generate_base(&id_low, &id_high, &doc.state);
    let new_base_block = doc.blocks.create_base_block(new_id.clone(), (0, text.chars().count() as u32), site);
    doc.blocks.insert(text.clone(), new_base_block, 0);
    
    return Operation 
    { op_type: OperationType::Insert, 
        ids: vec![new_id], 
        offsets: vec![0], 
        payload: Some(text), 
        site: site, 
        clock: doc.state.local_clock 
     }
}

fn local_insert(doc: &mut Document, pos: u32, text: String) -> Operation {
    // Invariant: Size of text passed to the localInsert is less than MAXVALUE 
    assert!(text.chars().count() as u32 <= MAX_VALUE);

    let p = doc.blocks.find_by_position(pos);
    if p.is_none() {
        // Document is empty
        return insert_new_block(doc, &vec![], &vec![], text, doc.state.replica);
    }

    let path = p.unwrap();
    let block = path.last().expect("Path should not be empty");

    // If we are inserting at the end of the block 
    // And we are the creator and the block endpoint is maximal 
    if (pos == doc.blocks.left_count(Some(*block)) + doc.blocks.size(Some(*block))) {
       if (doc.blocks.creator(*block) == doc.state.replica) {
            // Check if the offset is maximal for the block 
            let block_ranges = doc.blocks.ranges(*block);
            let base_ranges = doc.blocks.base_offsets(*block);
            if block_ranges.1 == base_ranges.1 {
                // We can extend this block 
            }
       }
    }
    todo!()
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
