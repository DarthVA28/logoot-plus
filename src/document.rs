use std::collections::HashMap;

use crate::idarena::{IdArena, IdBlock, Identifier, Range, TUPLE_SIZE};
use crate::node::Node;
use crate::tree::{DelLocation, Path, Tree};
use crate::state::State;
use crate::operation::{OpLog, Operation, OperationType, WireOperation};

#[derive(Clone, Debug)]
pub struct Document { 
    pub blocks: Tree,
    pub id_arena: IdArena,
    state: State,
    used_ranges_for_id: HashMap<Identifier, Range>,
    snapshot: String,
    pub oplog: OpLog,
    debug: bool,
    fresh: bool,
}

impl Document {
    pub fn new(id: u32) -> Self {
        Document {
            blocks: Tree::new(),
            id_arena: IdArena::new(),
            state: State::new(id),
            used_ranges_for_id: HashMap::new(),
            snapshot: String::new(),
            oplog: OpLog::new(),
            debug: false,
            fresh: true,
        }
    }

    pub fn set_replica(&mut self, replica_id: u32) {
        self.state.replica = replica_id;
    }

    pub fn site_id(&self) -> u32 {
        self.state.replica
    }

    pub fn ins(&mut self, pos: usize, text: String) -> Option<WireOperation>{
        if text == "" {
            // For empty inserts   
            return None;
        }
        // println!("Inserting '{}' at pos {} at site {}", text, pos, self.state.replica);
        let op = local_insert(self, pos, text);
        if self.debug {
            if !self.blocks.check_tree(&self.id_arena) {
                self.blocks.print_tree();
                panic!("Tree structure is invalid after local insert of {} at pos {} at site {}", &op.payload.unwrap().clone(), pos, self.state.replica);
            }
        }
        // println!("After local insert at replica {}", self.state.replica);
        // self.blocks.print_tree();

        self.oplog.record_op(&op);
        self.state.local_clock += 1;
        self.fresh = false;
        Some(op.to_wire(&self.id_arena))
    }

    pub fn del(&mut self, from: usize, to: usize) -> WireOperation {
        // println!("Deleting from {} to {} at site {}", from, to, self.state.replica);
        let op = local_delete(self, from, to);
        if self.debug {
            if !self.blocks.check_tree(&self.id_arena) {
                self.blocks.print_tree();
                panic!("Tree structure is invalid after local delete from {} to {} at site {}", from, to, self.state.replica);
            }
        }
        // println!("After local delete at replica {}", self.state.replica);
        // self.blocks.print_tree();
        self.oplog.record_op(&op);
        self.state.local_clock += 1;
        self.fresh = false;
        op.to_wire(&self.id_arena)
    }

    pub fn read(&mut self) -> String {
        if self.fresh {
            return self.snapshot.clone();
        }
        let mut res = String::with_capacity(self.blocks.tree_size());
        for block in self.blocks.inorder_iter() {
            res.push_str(&block.content);
        }
        self.snapshot = res.clone();
        self.fresh = true;
        res
    }

    pub fn apply_remote_op(&mut self, wire_op: &WireOperation) {
        let op = Operation::from_wire(wire_op, &mut self.id_arena);
        self.apply_op(&op);
    }

    pub fn apply_op(&mut self, op: &Operation) {
        // We are ready to apply this operation, first record it in the oplog and then apply it
        match op.op_type {
            OperationType::Insert => {
                // println!("Applying remote insert of '{}' with id {:?} at site {} at site {}", op.payload.as_ref().unwrap(), op.ids, op.site, self.state.replica);
                remote_insert(self, &op)
            },
            OperationType::Delete => {
                // println!("Applying remote delete of ids {:?} at site {} at site {}", op.ids, op.site, self.state.replica);
                remote_delete(self, &op)
            }
        }
        
        if self.debug {
            if !self.blocks.check_tree(&self.id_arena) {
                self.blocks.print_tree();
                panic!("Tree structure is invalid after merging op {:?} from site {} at site {}", op, op.site, self.state.replica);
            }
        }

        // Some operations can now possibly be applied!
        if op.op_type == OperationType::Insert {
            for id  in &op.ids {
                let pending_ops = self.oplog.get_pending_for_id(id);
                for op in pending_ops {
                    self.apply_op(&op);
                }
            }
        }

        self.fresh = false;
    }

    pub fn enable_debug(&mut self) {
        self.debug = true;
    }

    pub fn disable_debug(&mut self) {
        self.debug = false;
    }

    /* Public API for benchmarking */
    pub fn reset (&mut self) {
        self.blocks.clear();
        self.id_arena.clear();
        self.used_ranges_for_id.clear();
        self.snapshot.clear();
        self.oplog.clear();
        self.state.local_clock = 1;
        self.fresh = true;
    }

}

fn extend_block(doc: &mut Document, text: String, block: usize, path: &Path, site: u32) -> Operation {
    let next = doc.blocks.next(block, path);
    // let insert_base = doc.blocks.node_base_id(block);
    // let insert_offsets = doc.blocks.node_ranges(block);
    let insert_block = doc.blocks.node_block(block);
    let text_len = text.chars().count() as u32;
 
    if let Some(nxt_block) = next {
        let next_block = doc.blocks.node_block(nxt_block);
        let n = doc.id_arena.num_insertable(insert_block.high, next_block.low, text_len);
        if n < text_len {
            // Can't extend, not enough room before the next block.
            let new_id = doc.id_arena.generate_id(insert_block.high, next_block.low, &mut doc.state);
            let new_block = IdBlock::new(new_id, text_len, &mut doc.id_arena);
            let node = Node::new(text.clone(), new_block,site);
            doc.blocks.insert_after(path, node);
            return Operation {
                op_type: OperationType::Insert,
                ids: vec![new_block],
                payload: Some(text),
                site,
                clock: doc.state.local_clock,
            };
        }
    }
 
    doc.blocks.extend_content(&mut doc.id_arena, block, &text, path);
    let new_lo = IdBlock::id_with_offset(&mut doc.id_arena, insert_block.low, insert_block.count);
    let new_block = IdBlock::new(new_lo, text_len+1, &mut doc.id_arena);
    Operation {
        op_type: OperationType::Insert,
        ids: vec![new_block],
        payload: Some(text),
        site,
        clock: doc.state.local_clock,
    }
}

fn local_insert(doc: &mut Document, pos: usize, text: String) -> Operation {
    let doc_size = doc.blocks.tree_size();
    let pos = if pos > doc_size { doc_size } else { pos };
 
    let (path, covered) = doc.blocks.find_by_pos(pos);
 
    if path.is_empty() {
        let node_id = doc.id_arena.generate_id(Identifier::EMPTY, Identifier::EMPTY, &mut doc.state);
        let node_block = IdBlock::new(node_id, 1, &mut doc.id_arena);
        let node = Node::new(text.clone(), node_block, doc.state.replica);
        doc.blocks.insert_first(node);
        return Operation {
            op_type: OperationType::Insert,
            ids: vec![node_block],
            payload: Some(text),
            site: doc.state.replica,
            clock: doc.state.local_clock,
        };
    }
 
    let block = *path.last().unwrap();
    let block_base  = doc.blocks.node_block(block);
    let block_start = covered;
    let block_end = block_start + doc.blocks.node_size(Some(block));
 
    if pos == block_end {
        // Try extending in-place first.
        if doc.blocks.node_creator(block) == doc.state.replica {
            return extend_block(doc, text, block, &path, doc.state.replica);
        }
 
        let node_id = match doc.blocks.next(block, &path) {
            Some(next_block) => {
                let next_block = doc.blocks.node_block(next_block);
                doc.id_arena.generate_id(block_base.high, next_block.low, &mut doc.state)
            }
            None => {
                doc.id_arena.generate_id(block_base.high, Identifier::EMPTY, &mut doc.state)
            }
        };

        let node_block = IdBlock::new(node_id, text.len() as u32, &mut doc.id_arena);
        let node = Node::new(text.clone(), node_block, doc.state.replica);
 
        doc.blocks.insert_after(&path, node);
 
        return Operation {
            op_type: OperationType::Insert,
            ids: vec![node_block],
            payload: Some(text),
            site: doc.state.replica,
            clock: doc.state.local_clock,
        };
    }
 
    if pos == block_start {
        let base = match doc.blocks.prev(block, &path) {
            Some(prev_block) => {
                let prev_block = doc.blocks.node_block(prev_block);
                doc.id_arena.generate_id(prev_block.high, block_base.low, &mut doc.state)
            }
            None => {
                doc.id_arena.generate_id(Identifier::EMPTY, block_base.low, &mut doc.state)
            }
        };

        let node_block = IdBlock::new(base, text.len() as u32, &mut doc.id_arena);
        let node = Node::new(text.clone(), node_block, doc.state.replica);
 
        doc.blocks.insert_before(&path, node);
 
        return Operation {
            op_type: OperationType::Insert,
            ids: vec![node_block],
            payload: Some(text),
            site: doc.state.replica,
            clock: doc.state.local_clock,
        };
    }
 
    let sp = (pos - block_start) as u32;
    // debug_assert!(
    //     sp > 0 && sp < block_ranges.1 - block_ranges.0,
    //     "Invalid split point: sp={}, block_size={}",
    //     sp,
    //     block_ranges.1 - block_ranges.0
    // );
    
    let sp_low = IdBlock::id_with_offset(&mut doc.id_arena, block_base.low, sp-1);
    let sp_high = IdBlock::id_with_offset(&mut doc.id_arena, block_base.low, sp);

    let middle_id = doc.id_arena.generate_id(sp_low, sp_high, &mut doc.state);
    let middle_block = IdBlock::new(middle_id, text.len() as u32, &mut doc.id_arena );
    let middle = Node::new(text.clone(), middle_block, doc.state.replica);
 
    doc.blocks.split_and_insert_middle(&mut doc.id_arena, &path, sp as usize, middle);
 
    Operation {
        op_type: OperationType::Insert,
        ids: vec![middle_block],
        payload: Some(text),
        site: doc.state.replica,
        clock: doc.state.local_clock,
    }
}

fn remote_insert(doc: &mut Document, op: &Operation) {
    let mut id_block = op.ids[0];
    let text = op.payload.as_ref().expect("No payload for insert operation");
    let site = op.site;
    // Find and insert this id 
    doc.blocks.insert_by_id(site, &mut doc.id_arena, &mut id_block, text.to_string());
}

fn local_delete(doc: &mut Document, from: usize, to: usize) -> Operation {
    let mut num_delete = to - from;
    let mut del_info: Vec<IdBlock> = vec![];
    let curr = from;
 
    while num_delete > 0 {
        let (path, covered) = doc.blocks.find_by_pos_delete(curr);
        if path.is_empty() {
            panic!("Cannot delete from an empty document");
        }
 
        let target = *path.last().unwrap();
        let target_size = doc.blocks.node_size(Some(target));
        let start_del  = curr - covered;
        let end_del    = start_del + num_delete;
        let target_block    = doc.blocks.node_block(target);
 
        if start_del == 0 && end_del >= target_size {
            // Case 1: delete entire block 
            del_info.push(target_block);
            num_delete -= target_size;
 
            // *** DIRECT: uses path, no find_by_id ***
            doc.blocks.delete_at_path(&path);
 
        } else if start_del == 0 {
            // ── Case 2: delete from start of block ──────────────────────
            del_info.push(IdBlock::new(target_block.low, end_del as u32, &mut doc.id_arena));
            // del_info.push((base_id, block_ranges.0, block_ranges.0 + end_del as u32));
            doc.blocks.truncate_content(&mut doc.id_arena, target, num_delete, DelLocation::Start, &path);
            num_delete = 0;
 
        } else if end_del >= target_size {
            // Case 3: Delete from end of the block
            let n = target_size - start_del;
            let id_lo = IdBlock::id_with_offset(&mut doc.id_arena, target_block.low, start_del as u32);
            del_info.push(IdBlock::new(id_lo, n as u32, &mut doc.id_arena));
            doc.blocks.truncate_content(&mut doc.id_arena, target, n, DelLocation::End, &path);
            num_delete -= n;
 
        } else {
            // Case 4: Delete in the middle of the block
            let id_lo = IdBlock::id_with_offset(&mut doc.id_arena, target_block.low, start_del as u32);
            let _id_hi = IdBlock::id_with_offset(&mut doc.id_arena, target_block.low, end_del as u32);
            del_info.push(IdBlock::new(id_lo, num_delete as u32, &mut doc.id_arena));
            doc.blocks.delete_middle_at_path(&mut doc.id_arena, &path, start_del, num_delete);
            num_delete = 0;
        }
    }
 
    Operation {
        op_type: OperationType::Delete,
        ids: del_info,
        payload: None,
        site: doc.state.replica,
        clock: doc.state.local_clock,
    }
}

// fn remote_delete(doc: &mut Document, op: &Operation) {
//     let del_ids = &op.ids;
//     for id_block in del_ids {
//         // start is inclusive, end is exclusive
//         let offsets_len = id_block.count;
//         let mut processed = 0;
//         while processed < offsets_len {
//             // FIXME: place of inefficiency
//             let id = IdBlock::id_with_offset(&mut doc.id_arena, id_block.low, processed);
//             let path = doc.blocks.find_by_id_exact(&doc.id_arena, id);
//             if path.is_empty() {
//                 // base id exists but this offset is missing 
//                 let missing_start = processed;
//                 processed += 1;
//                 while processed < offsets_len {
//                     let id = IdBlock::id_with_offset(&mut doc.id_arena, id_block.low, processed);
//                     if doc.blocks.find_by_id_exact(&doc.id_arena, id).is_empty() {
//                         processed += 1;
//                     } else {
//                         break;
//                     }
//                 }
//                 let buffer_lo = IdBlock::id_with_offset(&mut doc.id_arena, id_block.low, missing_start);
//                 let buffer_count = processed - missing_start;
//                 let partial_op = Operation {
//                     op_type: OperationType::Delete,
//                     ids: vec![IdBlock::new(buffer_lo, buffer_count, &mut doc.id_arena)],
//                     payload: None,
//                     site: op.site,
//                     clock: op.clock,
//                 };
//                 doc.oplog.add_to_pending(partial_op);
//                 continue;

//             }
     
//             let target: usize = *path.last().unwrap();
//             let target_block = doc.blocks.node_block(target);
//             let offset = processed;

//             let target_lo_s = doc.id_arena.get_slice_unchecked(target_block.low);
//             let id_s = doc.id_arena.get_slice_unchecked(id);
//             let num_idx = id_s.len() - TUPLE_SIZE;
//             let offset_in_node = (id_s[num_idx] - target_lo_s[num_idx]) as u32;
//             let n_to_delete = (target_size - offset_in_node).min(id_block.count - processed);

//             // Same 4 cases as local delete
//             if offset == block_ranges.0 && n_in_block >= block_size {
//                 // Case 1: delete the entire block 
//                 doc.blocks.delete_at_path(&path);
//             } else if offset == block_ranges.0 {
//                 doc.blocks.truncate_content(target, n_in_block as usize, DelLocation::Start, &path);
//             } else if offset + n_in_block as u32 >= block_ranges.1 {
//                 doc.blocks.truncate_content(target, n_in_block as usize, DelLocation::End, &path);
//             } else {
//                 let sp = (offset - block_ranges.0) as usize;
//                 doc.blocks.delete_middle_at_path(&path, sp, n_in_block as usize);             
//             }
//             processed += n_in_block;
//         }
//     }
// }

fn remote_delete(doc: &mut Document, op: &Operation) {
    for id_block in &op.ids {
        let mut processed: u32 = 0;

        while processed < id_block.count {
            let id = IdBlock::id_with_offset(&mut doc.id_arena, id_block.low, processed);
            let path = doc.blocks.find_by_id_exact(&mut doc.id_arena, id);

            if path.is_empty() {
                let missing_start = processed;
                processed += 1;
                while processed < id_block.count {
                    let next_id = IdBlock::id_with_offset(&mut doc.id_arena, id_block.low, processed);
                    if doc.blocks.find_by_id_exact(&mut doc.id_arena, next_id).is_empty() {
                        processed += 1;
                    } else {
                        break;
                    }
                }
                let buffer_lo = IdBlock::id_with_offset(&mut doc.id_arena, id_block.low, missing_start);
                let partial_op = Operation {
                    op_type: OperationType::Delete,
                    ids: vec![IdBlock::new(buffer_lo, processed - missing_start, &mut doc.id_arena)],
                    payload: None,
                    site: op.site,
                    clock: op.clock,
                };
                doc.oplog.add_to_pending(partial_op);
                continue;
            }

            let target = *path.last().unwrap();
            let target_block = doc.blocks.node_block(target);
            let target_size = doc.blocks.node_size(Some(target)) as u32;

            let target_lo_s = doc.id_arena.get_slice_unchecked(target_block.low);
            let id_s = doc.id_arena.get_slice_unchecked(id);
            let num_idx = id_s.len() - TUPLE_SIZE;
            let offset_in_node = (id_s[num_idx] - target_lo_s[num_idx]) as u32;
            let chars_to_delete = (target_size - offset_in_node).min(id_block.count - processed);

            if offset_in_node == 0 && chars_to_delete >= target_size {
                doc.blocks.delete_at_path(&path);
            } else if offset_in_node == 0 {
                doc.blocks.truncate_content(&mut doc.id_arena, target, chars_to_delete as usize, DelLocation::Start, &path);
            } else if offset_in_node + chars_to_delete >= target_size {
                doc.blocks.truncate_content(&mut doc.id_arena, target, chars_to_delete as usize, DelLocation::End, &path);
            } else {
                doc.blocks.delete_middle_at_path(&mut doc.id_arena, &path, offset_in_node as usize, chars_to_delete as usize);
            }

            processed += chars_to_delete;
        }
    }
}