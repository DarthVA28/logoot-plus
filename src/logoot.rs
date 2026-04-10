pub mod tree;
pub mod identifier;
pub mod node;
pub mod operation;
pub mod state;
pub mod wasm;

use std::collections::HashMap;

use crate::tree::{DelLocation, Tree};
use crate::identifier::{Id, Identifier, MAX_VALUE, Range, generate_base, num_insertable};
use crate::state::State;
use crate::operation::{OpLog, Operation, OperationType};
// use rand::{RngExt, SeedableRng};

#[derive(Clone, Debug)]
pub struct Document { 
    blocks: Tree,
    state: State,
    used_ranges_for_id: HashMap<Identifier, Range>,
    snapshot: String,
    oplog: OpLog,
    debug: bool,
}

impl Document {
    pub fn new(id: u32) -> Self {
        Document {
            blocks: Tree::new(),
            state: State::new(id),
            used_ranges_for_id: HashMap::new(),
            snapshot: String::new(),
            oplog: OpLog::new(),
            debug: false,
        }
    }

    pub fn ins(&mut self, pos: usize, text: String) {
        let op = local_insert(self, pos, text);
        if self.debug {
            if !self.blocks.check_tree() {
                self.blocks.print_tree();
                panic!("Tree structure is invalid after local insert of {} at pos {} at site {}", &op.payload.unwrap().clone(), pos, self.state.replica);
            }
        }
        self.oplog.record_op(op);
        self.state.local_clock += 1;
    }

    pub fn del(&mut self, _from: usize, _to: usize) {
        let op = local_delete(self, _from, _to);
        if self.debug {
            if !self.blocks.check_tree() {
                self.blocks.print_tree();
                panic!("Tree structure is invalid after local delete from {} to {} at site {}", _from, _to, self.state.replica);
            }
        }
        self.oplog.record_op(op);
        self.state.local_clock += 1;
    }

    pub fn read(&self) -> String {
        let mut res = String::new();
        for block in self.blocks.inorder_iter() {
            res.push_str(&block.content);
        }
        res
    }

    pub fn merge_from(&mut self, other: &Document) {
        for op in &other.oplog.ops {
            if self.oplog.is_recorded(op) {
                continue;
            }

            if !self.oplog.is_ready(&op) {
                self.oplog.add_pending(op.clone());
                continue;
            }

            // We are ready to apply this operation, first record it in the oplog and then apply it
            match op.op_type {
                OperationType::Insert => remote_insert(self, &op),
                OperationType::Delete => remote_delete(self, &op)
            }
            
            self.oplog.record_op(op.clone());
        }
    }

    pub fn reset (&mut self) {
        self.blocks.clear();
        self.used_ranges_for_id.clear();
        self.snapshot.clear();
        self.oplog.clear();
    }
}

fn extend_block(doc: &mut Document, text:String, block: usize, path: &Vec<usize>, site: u32) -> Operation {
    // Check if we can extend the block without clashing with the next block 
    let next = doc.blocks.next(block, path);
    let insert_base = doc.blocks.node_base_id(block);
    let insert_offsets = doc.blocks.node_ranges(block);

    if !next.is_none() {
        let text_len = text.chars().count() as u32;
        let nxt_block = next.unwrap();
        // Get bases and offsets for the block and the next block
        let next_base = doc.blocks.node_base_id(nxt_block);
        let next_offsets: (u32, u32) = doc.blocks.node_ranges(nxt_block);
        // Get final IDs
        let id_insert = insert_base.with_offset(insert_offsets.1);
        let id_next = next_base.with_offset(next_offsets.0);
        let n = num_insertable(&id_insert, &id_next, text_len);
        if n < text_len {
            let id_low = insert_base.with_offset(insert_offsets.1-1);
            let id_high = next_base.with_offset(next_offsets.0);
            return insert_new_block(doc, &id_low, &id_high, text, site, None);   
        }
    }
    doc.blocks.extend_content(block, &text, path);
    return Operation 
    { op_type: OperationType::Insert, 
        ids: vec![(doc.blocks.node_base_id(block).clone(), vec![insert_offsets.1])],
        payload: Some(text), 
        site: site, 
        clock: doc.state.local_clock 
    }
}

fn insert_new_block(doc: &mut Document, id_low: &Id, id_high: &Id, text: String, site: u32, id: Option<&Id>) -> Operation {
    let base = {
        if id.is_none() { generate_base(id_low, id_high, &mut doc.state) }
        else { id.unwrap().clone() }
    };
    let _size = text.chars().count() as u32;
    doc.blocks.insert_by_id(site, base.clone(), 0, text.clone());
    return Operation 
    { op_type: OperationType::Insert, 
        ids: vec![(base, vec![0])], 
        payload: Some(text), 
        site: site, 
        clock: doc.state.local_clock 
    }
}

fn split_and_insert_block(doc: &mut Document, text: String, block: usize, _path: &Vec<usize>, sp: u32, site: u32, id: Option<&Id>) -> Operation {
    // sp is the split point 
    let base_id = doc.blocks.node_base_id(block).clone();
    let offsets = doc.blocks.node_ranges(block);
    let owner = doc.blocks.node_creator(block);

    // Create 2 new blocks with the content split at sp 
    let content = doc.blocks.node_content(Some(block));
    let lcontent = content.chars().take(sp as usize).collect::<String>();
    let rcontent = content.chars().skip(sp as usize).collect::<String>();
    
    let res = doc.blocks.delete_by_id(base_id.clone(), offsets.0);
    if res.is_err() {
        panic!("Error deleting block during split");
    }

    doc.blocks.insert_by_id(owner, base_id.clone(), offsets.0, lcontent.clone());    
    doc.blocks.insert_by_id(owner, base_id.clone(), offsets.0 + sp, rcontent.clone());

    // Insert the new block in between
    let id_low = base_id.with_offset(offsets.0 + sp - 1);
    let id_high = base_id.with_offset(offsets.0 + sp);

    let new_id: Identifier = if let Some(id) = id {
        id.clone()
    } else {
        generate_base(&id_low, &id_high, &mut doc.state)
    };
    doc.blocks.insert_by_id(site, new_id.clone(), 0, text.clone());
    return Operation 
    { op_type: OperationType::Insert, 
        ids: vec![(new_id, vec![0])], 
        payload: Some(text), 
        site: site, 
        clock: doc.state.local_clock 
     }
}

fn local_insert(doc: &mut Document, pos: usize, text: String) -> Operation {
    // Invariant: Size of text passed to the localInsert is less than MAXVALUE 
    assert!(text.chars().count() as u32 <= MAX_VALUE);

    let (path, covered) = doc.blocks.find_by_pos(pos);
    if path.is_empty() {
        return insert_new_block(doc, &Identifier {id: vec![]}, &Identifier {id: vec![]} , text, doc.state.replica, None);
    }

    let block = path.last().expect("Path should not be empty");
    let block_base = doc.blocks.node_base_id(*block);
    let block_ranges = doc.blocks.node_ranges(*block);

    // If we are inserting at the end of the block 
    // And we are the creator and the block endpoint is maximal 
    let block_start = covered;
    let block_end = block_start + doc.blocks.node_size(Some(*block));
    if pos == block_end {
       if doc.blocks.node_creator(*block) == doc.state.replica {
            let block_ranges = doc.blocks.node_ranges(*block);
            // FIXME MAYBE, changed node_base_offsets
            let base_ranges = doc.blocks.node_base_offsets(*block);
            if block_ranges.1 == base_ranges.1 {
                return extend_block(doc, text, *block, &path, doc.state.replica);
            }
       }

       // It cannot be extended, insert one new block after this block
       let id_low = block_base.with_offset(block_ranges.1 - 1);
       let next = doc.blocks.next(*block, &path);
       let id_high = if next.is_none() {
            Identifier { id: vec![] }
        } else {
            let next_block = next.unwrap();
            let next_base = doc.blocks.node_base_id(next_block);
            let next_ranges = doc.blocks.node_ranges(next_block);
            next_base.with_offset(next_ranges.0)
        };
        return insert_new_block(doc, &id_low, &id_high, text, doc.state.replica, None);
    }

    // If we are inserting at the start of the block, create a new block and insert before this block
    if pos == block_start {
        let prev_block = doc.blocks.prev(*block, &path);
        let id_low = if prev_block.is_none() {
            Identifier { id: vec![] }
        } else {
            let prev_block = prev_block.unwrap();
            let prev_base = doc.blocks.node_base_id(prev_block);
            let prev_ranges = doc.blocks.node_ranges(prev_block);
            prev_base.with_offset(prev_ranges.1-1)
        };
        let id_high = block_base.with_offset(block_ranges.0);
        return insert_new_block(doc, &id_low, &id_high, text, doc.state.replica, None);
    }

    // Split the block at the position and insert a new block in between
    let sp = (pos - block_start) as u32;
    if (sp > block_ranges.1 - block_ranges.0) || (sp == 0) {
        panic!("Invalid split point - split point: {}, block size: {}", sp, block_ranges.1 - block_ranges.0);
    }
    return split_and_insert_block(doc, text, *block, &path, sp, doc.state.replica, None);
}

fn remote_insert(doc: &mut Document, op: &Operation) {
    let val = op.ids[0].clone();
    let base  = val.0;
    let offset = val.1[0];
    let text = op.payload.as_ref().expect("No payload for insert operation");
    let site = op.site;

    // Find and insert this id 
    doc.blocks.insert_by_id(site, base, offset, text.to_string());
}

fn delete_and_split(doc: &mut Document, block: usize, _path: &Vec<usize>, left: usize, n: usize) {
    // Prepare the 2 blocks after the split 
    let base_id = doc.blocks.node_base_id(block).clone();
    let offsets = doc.blocks.node_ranges(block);
    let creator = doc.blocks.node_creator(block);

    let content = doc.blocks.node_content(Some(block));
    let lcontent = content.chars().take(left as usize).collect::<String>();
    let rcontent = content.chars().skip((left+n) as usize).collect::<String>();

    let res = doc.blocks.delete_by_id(base_id.clone(), offsets.0);
    if res.is_err() {
        panic!("Error deleting block during delete and split");
    }

    doc.blocks.insert_by_id(creator, base_id.clone(), offsets.0, lcontent, );
    doc.blocks.insert_by_id(creator, base_id.clone(), offsets.0 + (left+n) as u32, rcontent);
}

fn local_delete(doc: &mut Document, from: usize, to: usize) -> Operation {
    // Collect all the IDs of the elements to be deleted 
    // Cases: 
    // 1. The entire block is deleted
    // 2. We are deleting at the end of the block
    // 3. We are deleting at the start of the block
    // 4. We are deleting in the middle of the block
    // Find the index of from 

    let mut num_delete = to - from;
    let mut del_info: Vec<(Id, Vec<u32>)> = vec![];

    let curr = from;

    while num_delete > 0 {
        let (path, covered) = doc.blocks.find_by_pos_delete(curr);
        if path.is_empty() {
            panic!("Cannot delete from an empty document");
        }

        let block = *path.last().unwrap();
        let mut indices : Vec<(Id, Vec<u32>)> = vec![];
        let offset = covered;
        let block_size = doc.blocks.node_size(Some(block));
        
        let start_del = curr - offset;
        let end_del = start_del + num_delete;

        let base_id = doc.blocks.node_base_id(block);
        let block_ranges = doc.blocks.node_ranges(block);

        if start_del == 0 && end_del >= block_size {
            let del_offsets = (block_ranges.0..block_ranges.1).collect::<Vec<u32>>();
            indices.push((base_id.clone(), del_offsets));
            num_delete -= block_size;
            let res = doc.blocks.delete_by_id(base_id.clone(), block_ranges.0);
            if res.is_err() {
                panic!("Error deleting block");
            }
        } else if start_del == 0 { 
            // Case 2: delete some chars from the start of the block 
            let del_offsets = (block_ranges.0..block_ranges.0+end_del as u32).collect::<Vec<u32>>();
            indices.push((base_id.clone(), del_offsets));
            doc.blocks.truncate_content(block, num_delete, DelLocation::Start, &path);
            num_delete = 0;
        } else if end_del >= block_size {
            // Case 3: delete some chars from the end of the block
            let del_offsets = (block_ranges.0+start_del as u32..block_ranges.1).collect::<Vec<u32>>();
            indices.push((base_id.clone(), del_offsets));
            doc.blocks.truncate_content(block, block_size - start_del, DelLocation::End, &path);
            num_delete -= block_size - start_del;
        } else {
            let del_offsets = (block_ranges.0+start_del as u32..block_ranges.0+end_del as u32).collect::<Vec<u32>>();
            indices.push((base_id.clone(), del_offsets));
            delete_and_split(doc, block, &path, start_del, num_delete);
            num_delete = 0;
        }
        del_info.extend(indices);
    }

    return Operation 
    { op_type: OperationType::Delete, 
        ids: del_info, 
        payload: None, 
        site: doc.state.replica, 
        clock: doc.state.local_clock 
    };
}

fn remote_delete(doc: &mut Document, op: &Operation) {
    let del_ids = &op.ids;

    for (id, offsets) in del_ids {
        
        let mut processed = 0;
        while processed < offsets.len() {
            // Find the block with this base ID + first (smallest) offset
            let path = doc.blocks.find_by_id(id.clone(), offsets[processed]);
            if path.is_empty() {
                // Assume already 
                processed += 1;
                continue;
            }
            // Verify if the base id of the blocks are the same else continue 
            if doc.blocks.node_base_id(*path.last().unwrap()) != id {
                // FIXME: May need to be handled separately
                eprintln!("Base ID of the block does not match the ID in delete operation, skipping this offset");
                processed += 1;
                continue;
            }
            let block: usize = *path.last().unwrap();
            let base_id = doc.blocks.node_base_id(block);
            let block_ranges = doc.blocks.node_ranges(block);
            let block_size = block_ranges.1 - block_ranges.0;
            let offset = offsets[processed];
            // let n_delete = offsets.len();

            // Count how many of the remaining offsets fall inside this node.
            // Offsets are always a contiguous range, and this node covers
            // [block_ranges.0, block_ranges.1), so a simple take_while works.
            let n_in_block = offsets[processed..]
                .iter()
                .take_while(|&&o| o >= block_ranges.0 && o < block_ranges.1)
                .count();
            
            // Same 4 cases as local delete
            if offset == block_ranges.0 && n_in_block >= block_size as usize {
                // Case 1: delete the entire block 
                let res = doc.blocks.delete_by_id(base_id.clone(), block_ranges.0);
                if res.is_err() {
                    panic!("Error deleting block during remote delete");
                }
            } else if offset == block_ranges.0 {
                doc.blocks.truncate_content(block, n_in_block, DelLocation::Start, &path);
            } else if offset + n_in_block as u32 >= block_ranges.1 {
                doc.blocks.truncate_content(block, n_in_block, DelLocation::End, &path);
            } else {
                let sp = (offset - block_ranges.0) as usize;
                delete_and_split(doc, block, &path, sp, n_in_block);
            }
            processed += n_in_block;
        }
    }
}

#[test]
fn ab() {
    let mut doc = Document::new(0);
    doc.ins(0,"a".to_string());
    doc.ins(1,"b".to_string());
    assert_eq!(doc.read(), "ab".to_string());
}

#[test]
fn abc() {
    let mut doc = Document::new(0);
    doc.ins(0,"a".to_string());
    doc.ins(1,"b".to_string());
    doc.ins(2,"c".to_string());

    assert_eq!(doc.read(), "abc".to_string());
    // panic!("just to debug...");
}

#[test]
fn simple_test_1() {
    let mut d0 = Document::new(0);
    let mut d1 = Document::new(1);

    d1.ins(0, "c".to_string());
    d0.ins(0, "b".to_string());

    d1.ins(0, "b".to_string());
    d0.ins(1, "c".to_string());

    d0.merge_from(&d1);
    d1.merge_from(&d0);
    assert_eq!(d0.read(), d1.read());

    d0.ins(1, "b".to_string());

    d0.merge_from(&d1);
    d1.merge_from(&d0);
    assert_eq!(d0.read(), d1.read());
}

#[test]
fn test_interleaved_inserts() {
    let mut a = Document::new(0);
    let mut b = Document::new(1);

    a.ins(0, "A".to_string());
    a.ins(1, "B".to_string());

    a.blocks.print_tree();

    b.ins(0, "X".to_string());
    b.ins(1, "Y".to_string());

    b.blocks.print_tree();

    a.merge_from(&b);
    a.blocks.print_tree();
    b.merge_from(&a);

    assert_eq!(a.read(), b.read());
}

#[allow(dead_code)]
fn run_insert_delete(seed: u64) {
    use rand::{SeedableRng, RngExt};
    use rand::rngs::StdRng;

    let mut rng = StdRng::seed_from_u64(seed);

    let mut docs = vec![
        Document::new(0),
        Document::new(1),
        // Document::new(2),
    ];

    let alphabet: Vec<char> = "abcdefghijklmnopqrstuvwxyz".chars().collect();

    for j in 0..200 {
        let i = rng.random_range(0..docs.len());
        let doc = &mut docs[i];
        let len = doc.read().chars().count();

        // 30% chance to delete if there's something to delete
        if len > 0 && rng.random_range(0..10) < 3 {
            let from = rng.random_range(0..len);
            let to = rng.random_range(from+1..=len);
            doc.del(from, to);
        } else {
            let pos = if len == 0 { 0 } else { rng.random_range(0..=len) };
            let ch = alphabet[rng.random_range(0..alphabet.len())].to_string();
            doc.ins(pos, ch);
        }

        // random merge
        let a = rng.random_range(0..docs.len());
        let b = rng.random_range(0..docs.len());
        if a == b { continue; }

        let (left, right) = if a < b {
            let (l, r) = docs.split_at_mut(b);
            (&mut l[a], &mut r[0])
        } else {
            let (l, r) = docs.split_at_mut(a);
            (&mut r[0], &mut l[b])
        };

        let clone = right.clone();
        left.merge_from(&clone);

        let clone2 = left.clone();
        right.merge_from(&clone2);

        if left.read() != right.read() {
            println!("Divergence detected at seed {}!", seed);
            left.blocks.print_tree();   
            println!("---");
            right.blocks.print_tree();
        }

        assert_eq!(
            left.read(),
            right.read(),
            "Seed {} diverged\n'{}' vs '{}'",
            seed,
            left.read(),
            right.read()
        );
    }
}

#[test]
fn test_insert_delete_heavy() {
    for i in 0..1000 {
        println!("Running seed {}", i); 
        run_insert_delete(i);
    }
}