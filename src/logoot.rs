/* 
An implementation of the Logoot CRDT for collaborative text editing.
*/

pub mod blocktree;
pub mod identifiers;

use crate::identifiers::{Id, Range, get_combined_id};
use crate::blocktree::{BlockTree, DelLocation};

use std::{collections::HashMap};

// use rand::RngExt;
use rand::{RngExt, SeedableRng};
use rand::rngs::StdRng;
use rand_chacha::ChaCha8Rng;

const MIN_VALUE: u32 = 0;
const MAX_VALUE: u32 = 50;

#[derive(Clone, Debug,)]
struct State { 
    local_clock: u32, 
    replica: u32
}

#[derive(Clone, Debug, PartialEq, Eq,)]
pub enum OperationType {
    Insert,
    Delete
}   

#[derive(Clone)]
pub struct Operation { 
    op_type: OperationType,
    ids: Vec<(Id, Vec<u32>)>,
    payload: Option<String>,
    site: u32, 
    clock: u32
}

#[derive(Clone)]
pub struct Document { 
    blocks: BlockTree,
    state: State,
    used_ranges_for_id: HashMap<Id, Range>,
    snapshot: String,
    operations: Vec<Operation>, 
    // TODO:: Assumes causal messages, fix later
    last_seen: HashMap<u32, u32>,
    // rng: StdRng
    rng: ChaCha8Rng,
}


impl Document {
    pub fn new(id: u32) -> Self {
        Document {
            blocks: BlockTree::new(),
            state: State { local_clock: 1, replica: id },
            used_ranges_for_id: HashMap::new(),
            snapshot: String::new(),
            operations: Vec::new(),
            last_seen: HashMap::new(),
            rng: ChaCha8Rng::seed_from_u64(42)
        }
    }

    pub fn ins(&mut self, pos: u32, text: String) {
        let op = local_insert(self, pos, text);
        self.operations.push(op);
        self.state.local_clock += 1;
        // println!("Blocktree @ site {} after local insert:", self.state.replica);
        // self.blocks.print_tree();
    }

    pub fn del(&mut self, _from: u32, _to: u32) {
        let op = local_delete(self, _from, _to);
        self.operations.push(op);
        self.state.local_clock += 1;
        // println!("Blocktree @ site {} after local delete:", self.state.replica);
        // self.blocks.print_tree();
    }

    pub fn read(&self) -> String {
        let mut res = String::new();
        for block in self.blocks.inorder_iter() {
            res.push_str(&block.content());
        }
        res
    }

    pub fn merge_from(&mut self, other: &Document) {
        for op in &other.operations {
            if self.last_seen.get(&op.site).unwrap_or(&0) >= &op.clock {
                continue;
            }
            // print the whole operation for debugging
            // println!("Merging Operation type: {:?}, ids: {:?}, payload: {:?}, site: {}, clock: {}", op.op_type, op.ids, op.payload, op.site, op.clock);
            // println!("Merging operation from site {} with clock {}", op.site, op.clock);
            self.last_seen.insert(op.site, op.clock);
            match op.op_type {
                OperationType::Insert => remote_insert(self, &op),
                OperationType::Delete => remote_delete(self, &op)
            }
            // println!("After merging, blocktree is:");
            // self.blocks.print_tree();
        }
    }

    pub fn reset (&mut self) {
        self.blocks.clear();
        self.used_ranges_for_id.clear();
        self.snapshot.clear();
        self.operations.clear();
        self.last_seen.clear();
    }
}

/* Generate a new base between idLow and idHigh */
fn generate_base(doc: &mut Document, id_low: &Id, id_high: &Id) -> Id {
    let mut base = Vec::new();
    let mut low_iter = id_low.iter();
    let mut high_iter = id_high.iter();
    let state = &doc.state;
    
    let mut l = *low_iter.next().unwrap_or(&MIN_VALUE);
    let mut h = *high_iter.next().unwrap_or(&MAX_VALUE);

    while (h as i32) - (l as i32) < 2 {
        base.push(l);
        l = *low_iter.next().unwrap_or(&MIN_VALUE);
        h = *high_iter.next().unwrap_or(&MAX_VALUE);
    }

    // Random number between l and h
    // let mut rng = StdR
    // let mut rng = StdRng::seed_from_u64(42);
    let rng = &mut doc.rng;
    let nxt = rng.random_range(l+1..h);
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
    id_next[l] - id_insert[l] + 1
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
            let id_low = get_combined_id(insert_base, insert_offsets.1-1);
            let id_high: Vec<u32> = get_combined_id(next_base, next_offsets.0);
            return insert_new_block(doc, &id_low, &id_high, text, site, None);   
        }
    }
    doc.blocks.extend_content(block, &text, path);
    return Operation 
    { op_type: OperationType::Insert, 
        ids: vec![(doc.blocks.base_id(block).clone(), vec![insert_offsets.1])],
        payload: Some(text), 
        site: site, 
        clock: doc.state.local_clock 
    }
}

fn insert_new_block(doc: &mut Document, id_low: &Id, id_high: &Id, text: String, site: u32, id: Option<&Id>) -> Operation {
    let base = {
        if id.is_none() { generate_base(doc, id_low, id_high) }
        else { id.unwrap().clone() }
    };
    let size = text.chars().count() as u32;
    let base_block = doc.blocks.create_base_block(base.clone(), (0, size), site);
    doc.blocks.insert(text.clone(), base_block, 0);
    return Operation 
    { op_type: OperationType::Insert, 
        ids: vec![(base, vec![0])], 
        payload: Some(text), 
        site: site, 
        clock: doc.state.local_clock 
    }
}

fn split_and_insert_block(doc: &mut Document, text: String, block: usize, _path: &Vec<usize>, sp: u32, site: u32) -> Operation {
    // sp is the split point 
    let base = doc.blocks.base(block);
    let base_id = doc.blocks.base_id(block).clone();
    let offsets = doc.blocks.ranges(block);

    // Create 2 new blocks with the content split at sp 
    let content = doc.blocks.content(block);
    let lcontent = content.chars().take(sp as usize).collect::<String>();
    let rcontent = content.chars().skip(sp as usize).collect::<String>();
    
    let res = doc.blocks.delete(base, offsets.0);
    if res.is_err() {
        panic!("Error deleting block during split");
    }

    doc.blocks.insert(lcontent, base, offsets.0);
    doc.blocks.insert(rcontent, base, offsets.0 + sp);
    // doc.blocks.print_tree();


    // Insert the new block in between
    let id_low = get_combined_id(&base_id, offsets.0 + sp-1);
    let id_high = get_combined_id(&base_id, offsets.0 + sp);

    let new_id = generate_base(doc, &id_low, &id_high);
    let new_base_block = doc.blocks.create_base_block(new_id.clone(), (0, text.chars().count() as u32), site);
    doc.blocks.insert(text.clone(), new_base_block, 0);
    
    return Operation 
    { op_type: OperationType::Insert, 
        ids: vec![(new_id, vec![0])], 
        payload: Some(text), 
        site: site, 
        clock: doc.state.local_clock 
     }
}

fn local_insert(doc: &mut Document, pos: u32, text: String) -> Operation {
    // Invariant: Size of text passed to the localInsert is less than MAXVALUE 
    assert!(text.chars().count() as u32 <= MAX_VALUE);

    // println!("Local insert on site {} at position {} with text '{}'", doc.state.replica, pos, text);

    let (path, covered) = doc.blocks.find_by_position(pos);
    if path.is_empty() {
        // Document is empty
        // println!("Document is empty, inserting at the start");
        return insert_new_block(doc, &vec![], &vec![], text, doc.state.replica, None);
    }

    let block = path.last().expect("Path should not be empty");
    let block_base = doc.blocks.base_id(*block);
    let block_ranges = doc.blocks.ranges(*block);

    // If we are inserting at the end of the block 
    // And we are the creator and the block endpoint is maximal 
    let block_start = covered;
    let block_end = block_start + doc.blocks.size(Some(*block));
    // println!("blockstart: {}, blockend: {}, pos: {}", block_start, block_end, pos);
    if pos == block_end {
       if doc.blocks.creator(*block) == doc.state.replica {
            // Check if the offset is maximal for the block 
            let block_ranges = doc.blocks.ranges(*block);
            let base_ranges = doc.blocks.base_offsets(*block);
            if block_ranges.1 == base_ranges.1 {
                // We can extend this block 
                // println!("Extending block {} with id {:?} ", block, block_base);
                return extend_block(doc, text, *block, &path, doc.state.replica);
            }
       }

       // It cannot be extended, insert one new block after this block
       let id_low = get_combined_id(block_base, block_ranges.1-1); 
       let next = doc.blocks.next(*block, &path);
       let id_high = if next.is_none() {
            vec![]
        } else {
            let next_block = next.unwrap();
            let next_base = doc.blocks.base_id(next_block);
            let next_ranges = doc.blocks.ranges(next_block);
            get_combined_id(next_base, next_ranges.0)
        };
        // println!("Inserting new block between {:?} and {:?}", id_low, id_high);
        return insert_new_block(doc, &id_low, &id_high, text, doc.state.replica, None);
    }

    // If we are inserting at the start of the block, create a new block and insert before this block
    if pos == block_start {
        let prev_block = doc.blocks.prev(*block, &path);
        let id_low = if prev_block.is_none() {
            vec![]
        } else {
            let prev_block = prev_block.unwrap();
            let prev_base = doc.blocks.base_id(prev_block);
            let prev_ranges = doc.blocks.ranges(prev_block);
            get_combined_id(prev_base, prev_ranges.1)
        };
        let id_high = get_combined_id(block_base, block_ranges.0);
        // println!("Inserting at start, new block between {:?} and {:?}", id_low, id_high);
        return insert_new_block(doc, &id_low, &id_high, text, doc.state.replica, None);
    }

    // Split the block at the position and insert a new block in between
    let sp = pos - block_start;
    if (sp > block_ranges.1 - block_ranges.0) || (sp == 0) {
        panic!("Invalid split point - split point: {}, block size: {}", sp, block_ranges.1 - block_ranges.0);
    }
    return split_and_insert_block(doc, text, *block, &path, sp, doc.state.replica);
}

fn find_split_point(doc: &Document, block: usize, id: Id) -> u32 {
    let mut sp = 0;
    let block_base = doc.blocks.base_id(block);
    let block_ranges = doc.blocks.ranges(block);
    let text_len = block_ranges.1 - block_ranges.0;
    for i in 0..text_len {
        let id_elem = get_combined_id(block_base, block_ranges.0 + i);
        if id_elem >= id {
            break;
        }
        sp+=1;
    }
    return sp;
}

fn remote_insert(doc: &mut Document, op: &Operation) {
    let val = &op.ids[0];
    let base  = &val.0;
    let offset = val.1[0];
    let text = op.payload.as_ref().expect("Insert operation should have payload");
    let site = op.site;

    // println!("Remote insert received from site {} at site {} with clock {}: inserting '{}' at base {:?} and offset {}", site, doc.state.replica, op.clock, text, base, offset);
    
    let id = get_combined_id(base, offset);
    
    // Try to find this ID 
    let path = doc.blocks.find_by_id(base, offset);

    if path.is_empty() {
        // Insert at the start
        // println!("ID not found, inserting at the start");
        insert_new_block(doc, &vec![], &vec![], text.clone(), site, Some(&base));
        return;
    }

    let block = *path.last().unwrap();
    let block_base = doc.blocks.base_id(block);
    let block_ranges = doc.blocks.ranges(block);
    let block_min_id = get_combined_id(block_base, block_ranges.0);
    let block_max_id = get_combined_id(block_base, block_ranges.1-1);

    // Check if we can extend the block: offset should be at the end of the block
    if base == block_base && offset == block_ranges.1 {
        // println!("Extending block {} with id {:?} and new text '{}'", block, block_base, text);
        extend_block(doc, text.clone(), block, &path, site);
        return
    }

    // If we are at the start of the block, insert a new block before this block:
    if id < block_min_id {
        // println!("Inserting before the block {} with id {:?} and new text '{}'", block, block_base, text);
        let id_high = block_min_id;
        let prev = doc.blocks.prev(block, &path);
        let id_low = if prev.is_none() {
            vec![]
        } else {
            let prev_block = prev.unwrap();
            let prev_base = doc.blocks.base_id(prev_block);
            let prev_ranges = doc.blocks.ranges(prev_block);
            get_combined_id(prev_base, prev_ranges.1)
        };
        insert_new_block(doc, &id_low, &id_high, text.clone(), site, Some(&base));
        return
    }

    // If we are at the end of the block, insert a new block 
    if block_max_id < id {
        // println!("Inserting after the block {} with new text '{}'", block, text);
        let id_low = block_max_id; 
        let next = doc.blocks.next(block, &path);
        let id_high = if next.is_none() {
            vec![]
        } else {
            let next_block = next.unwrap();
            let next_base = doc.blocks.base_id(next_block);
            let next_ranges: (u32, u32) = doc.blocks.ranges(next_block);
            get_combined_id(next_base, next_ranges.0)
        };
        insert_new_block(doc, &id_low, &id_high, text.clone(), site, Some(&base));
        return
    }

    // Insert in the middle of the block 
    // Find the point in the block where the new ID should be inserted 
    // println!("Inserting in the middle of the block {} with new text '{}'", block, text);
    let sp = find_split_point(doc, block, id);
    split_and_insert_block(doc, text.clone(), block, &path, sp, site);
}

fn delete_and_split(doc: &mut Document, block: usize, _path: &Vec<usize>, left: u32, n: u32) {
    // Prepare the 2 blocks after the split 
    let base = doc.blocks.base(block);
    let _base_id = doc.blocks.base_id(block).clone();
    let offsets = doc.blocks.ranges(block);

    let content = doc.blocks.content(block);
    let lcontent = content.chars().take(left as usize).collect::<String>();
    let rcontent = content.chars().skip((left+n) as usize).collect::<String>();

    let res = doc.blocks.delete(base, offsets.0);
    if res.is_err() {
        panic!("Error deleting block during delete and split");
    }

    doc.blocks.insert(lcontent, base, offsets.0);
    doc.blocks.insert(rcontent, base, offsets.0 + left+n);
}

fn local_delete(doc: &mut Document, from: u32, to: u32) -> Operation {
    // Collect all the IDs of the elements to be deleted 
    // Cases: 
    // 1. The entire block is deleted
    // 2. We are deleting at the end of the block
    // 3. We are deleting at the start of the block
    // 4. We are deleting in the middle of the block
    // Find the index of from 

    let mut num_delete = to - from;
    let mut del_info: Vec<(Id, Vec<u32>)> = vec![];

    let mut curr = from;

    while num_delete > 0 {
        let (path, covered) = doc.blocks.find_by_position(curr);
        if path.is_empty() {
            panic!("Cannot delete from an empty document");
        }

        let block = *path.last().unwrap();
        let mut indices : Vec<(Id, Vec<u32>)> = vec![];
        let offset = covered;
        let block_size = doc.blocks.size(Some(block));

        // doc.blocks.print_tree();
        
        let start_del = curr - offset;
        let end_del = to - offset;

        // println!("start_del: {}, end_del: {}", start_del, end_del);

        let id = doc.blocks.base(block);
        let base_id = doc.blocks.base_id(block);
        let block_ranges = doc.blocks.ranges(block);

        if start_del == 0 && end_del >= block_size {
            // Delete the entire block 
            // for i in 0..block_size {
            //     indices.push(base_id.clone());
            //     offsets.push(block_ranges.0 + i);
            // }
            let del_offsets = (block_ranges.0..block_ranges.1).collect::<Vec<u32>>();
            indices.push((base_id.clone(), del_offsets));
            num_delete -= block_size;
            // curr += block_size;
            let res = doc.blocks.delete(id, block_ranges.0);
            if res.is_err() {
                panic!("Error deleting block");
            }
        } else if start_del == 0 { 
            // Case 2: delete some chars from the start of the block 
            let del_offsets = (block_ranges.0..block_ranges.0+end_del).collect::<Vec<u32>>();
            indices.push((base_id.clone(), del_offsets));
            doc.blocks.truncate_content(block, num_delete, DelLocation::Start, &path);
            num_delete = 0;
        } else if end_del >= block_size {
            // Case 3: delete some chars from the end of the block
            // for i in start_del..block_size {
            //     indices.push(base_id.clone());
            //     offsets.push(block_ranges.0 + i);
            // }
            let del_offsets = (block_ranges.0+start_del..block_ranges.1).collect::<Vec<u32>>();
            indices.push((base_id.clone(), del_offsets));
            doc.blocks.truncate_content(block, block_size - start_del, DelLocation::End, &path);
            num_delete -= block_size - start_del;
        } else {
            let del_offsets = (block_ranges.0+start_del..block_ranges.0+end_del).collect::<Vec<u32>>();
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
        // Find the block with this base ID + first (smallest) offset
        let path = doc.blocks.find_by_id(&id, offsets[0]);
        if path.is_empty() {
            continue;
        }
        // Verify if the base id of the blocks are the same else continue 
        if doc.blocks.base_id(*path.last().unwrap()) != id {
            continue;
        }
        let block: usize = *path.last().unwrap();
        let base = doc.blocks.base(block);
        let block_ranges = doc.blocks.ranges(block);
        let block_size = block_ranges.1 - block_ranges.0;
        let offset = offsets[0];
        let n_delete: u32 = offsets.len() as u32;
        
        // Same 4 cases as local delete
        if offset == block_ranges.0 && offsets.len() as u32 >= block_size {
            // Case 1: delete the entire block 
            let res = doc.blocks.delete(base, block_ranges.0);
            if res.is_err() {
                panic!("Error deleting block during remote delete");
            }
        } else if offset == block_ranges.0 {
            doc.blocks.truncate_content(block, n_delete, DelLocation::Start, &path);
        } else if offset + n_delete >= block_ranges.1 {
            doc.blocks.truncate_content(block, n_delete, DelLocation::End, &path);
        } else {
            let sp = offset - block_ranges.0;
            delete_and_split(doc, block, &path, sp, n_delete);
        }
    }
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

fn run_insert_only(seed: u64) {
    use rand::{SeedableRng, RngExt};
    use rand::rngs::StdRng;

    let mut rng = StdRng::seed_from_u64(seed);

    let mut docs = vec![
        Document::new(0),
        Document::new(1),
        // Document::new(2),
    ];

    let alphabet: Vec<char> = "abc".chars().collect();

    for _ in 0..200 {
        // random insert
        let i = rng.random_range(0..docs.len());
        let doc = &mut docs[i];

        let len = doc.read().chars().count();
        let pos = if len == 0 { 0 } else { rng.random_range(0..=len) };

        let ch = alphabet[rng.random_range(0..alphabet.len())].to_string();

        doc.ins(pos as u32, ch);

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

        assert_eq!(
            left.read(),
            right.read(),
            "Seed {} diverged\n{} vs {}",
            seed,
            left.read(),
            right.read()
        );
    }
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
fn test_insert_100_seeds() {
    // run_insert_only(1);
    for i in 0..100 {
        run_insert_only(i);
    }
}

fn run_insert_delete(seed: u64) {
    use rand::{SeedableRng, RngExt};
    use rand::rngs::StdRng;

    let mut rng = StdRng::seed_from_u64(seed);

    let mut docs = vec![
        Document::new(0),
        Document::new(1),
    ];

    let alphabet: Vec<char> = "abc".chars().collect();

    for _ in 0..200 {
        let i = rng.random_range(0..docs.len());
        let doc = &mut docs[i];
        let len = doc.read().chars().count();

        // 30% chance to delete if there's something to delete
        if len > 0 && rng.random_range(0..10) < 3 {
            let from = rng.random_range(0..len);
            let to = rng.random_range(from+1..=len);
            doc.del(from as u32, to as u32);
        } else {
            let pos = if len == 0 { 0 } else { rng.random_range(0..=len) };
            let ch = alphabet[rng.random_range(0..alphabet.len())].to_string();
            doc.ins(pos as u32, ch);
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
    for i in 0..100 {
        run_insert_delete(i);
    }
}