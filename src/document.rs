use crate::idarena::{IdArena, Identifier, MAX_VALUE, MIN_VALUE, generate_base};
use crate::node::Node;
use crate::tree::{DelLocation, Tree};
use crate::state::State;
use crate::delta::{Delta, OperationType, WireDelta};
use crate::dotstore::{DotStore, Dot};
use crate::dotindex::DotIndex;

#[derive(Clone, Debug)]
pub struct Document { 
    pub blocks: Tree,
    pub id_arena: IdArena,
    state: State,
    snapshot: String,
    pub dot_index: DotIndex,
    pub dotstore: DotStore,
    debug: bool,
    fresh: bool,
}

impl Document {
    pub fn new(id: u32) -> Self {
        Document {
            blocks: Tree::new(),
            id_arena: IdArena::new(),
            state: State::new(id),
            snapshot: String::new(),
            dot_index: DotIndex::new(),
            dotstore: DotStore::new(),
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

    pub fn ins(&mut self, pos: usize, text: String) -> Option<WireDelta>{
        let text_len = text.len() as u32;
        if text_len == 0 {
            // For empty inserts   
            return None;
        }
        // println!("Inserting '{}' at pos {} at site {}", text, pos, self.state.replica);
        let op = local_insert(self, pos, text);
        if self.debug {
            if !self.blocks.check_tree(&self.id_arena) {
                self.blocks.print_tree(&self.id_arena);
                panic!("Tree structure is invalid after local insert of {} at pos {} at site {}", &op.payload.unwrap().clone(), pos, self.state.replica);
            }
        }
        // println!("After local insert at replica {}", self.state.replica);
        // self.blocks.print_tree(&self.id_arena);

        self.dotstore.record_delta(&op);
        self.state.local_clock += text_len;
        self.fresh = false;
        Some(op.to_wire(&self.id_arena))
    }

    pub fn del(&mut self, from: usize, to: usize) -> WireDelta {
        // println!("Deleting from {} to {} at site {}", from, to, self.state.replica);
        let op = local_delete(self, from, to);
        if self.debug {
            if !self.blocks.check_tree(&self.id_arena) {
                self.blocks.print_tree(&self.id_arena);
                panic!("Tree structure is invalid after local delete from {} to {} at site {}", from, to, self.state.replica);
            }
        }
        // println!("After local delete at replica {}", self.state.replica);
        // self.blocks.print_tree(&self.id_arena);
        self.fresh = false;
        // self.state.local_clock += 1;
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

    pub fn apply_remote_op(&mut self, wire_op: &WireDelta) {
        // let op = Operation::from_wire(wire_op, &mut self.id_arena);
        self.apply_op(wire_op);
    }

    pub fn apply_op(&mut self, op: &WireDelta) {
        match &op.op_type {
            OperationType::Insert => {
                remote_insert(self, &op);
            },
            OperationType::Delete => {
                remote_delete(self, &op)
            }
        }
        
        if self.debug {
            if !self.blocks.check_tree(&self.id_arena) {
                self.blocks.print_tree(&self.id_arena);
                panic!("Tree structure is invalid after merging op {:?} from site {} at site {}", op, op.site, self.state.replica);
            }
        }

        // Some operations can now possibly be applied!
        if op.op_type == OperationType::Insert {
            for (dot, _, _, _) in &op.ids {
                let pending_ops = self.dotstore.get_pending_for_block(dot.site, dot.b_idx);
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
        self.snapshot.clear();
        self.dotstore.clear();
        self.dot_index.clear();
        self.state.local_clock = 1;
        self.fresh = true;
    }

}

fn extend_block(doc: &mut Document, text: String, block: usize, site: u32) -> Delta {
    let next = doc.blocks.next(block);
    let insert_base = doc.blocks.node_base_id(block);
    let insert_offsets = doc.blocks.node_ranges(block);
    let text_len = text.len() as u32;
    let seq = doc.state.local_clock;
 
    if let Some(nxt_block) = next {
        let next_base = doc.blocks.node_base_id(nxt_block);
        let next_offsets = doc.blocks.node_ranges(nxt_block);
        let n = doc.id_arena.num_insertable(insert_base, insert_offsets.1, next_base, next_offsets.0, text_len);
        if n < text_len {
            // Can't extend — not enough room before the next block.
            let base = generate_base(&mut doc.id_arena, insert_base, insert_offsets.1-1, next_base, next_offsets.0, &mut doc.state);
            let block_idx = doc.state.block_idx;
            doc.state.block_idx += 1;
            let node = Node::new(text.clone(), base, seq, site, block_idx);
            doc.blocks.insert_after(&mut doc.dot_index, block, node);
            return Delta {
                op_type: OperationType::Insert,
                ids: vec![(Dot{ site, seq: doc.state.local_clock, b_idx: block_idx }, base, seq, seq + text_len)],
                payload: Some(text),
                site,
            };
        }
    }
 
    doc.blocks.extend_content(&mut doc.dot_index, block, &text);
    Delta {
        op_type: OperationType::Insert,
        ids: vec![(Dot{ site, seq: doc.state.local_clock, b_idx: doc.blocks.node_block_idx(block) }, insert_base, insert_offsets.1, insert_offsets.1 + text_len)],
        payload: Some(text),
        site,
    }
}

fn local_insert(doc: &mut Document, pos: usize, text: String) -> Delta {
    let doc_size = doc.blocks.tree_size();
    let text_len = text.len() as u32;
    let pos = if pos > doc_size { doc_size } else { pos };
 
    let (node, covered) = doc.blocks.find_by_pos(pos);
 
    let seq = doc.state.local_clock;
    // ── Empty tree ──────────────────────────────────────────────────────
    if node.is_none() {
        let block_idx = doc.state.block_idx;
        doc.state.block_idx += 1;
        let base = generate_base(&mut doc.id_arena, Identifier::EMPTY, MIN_VALUE, Identifier::EMPTY, MAX_VALUE, &mut doc.state);
        let node = Node::new(text.clone(), base, seq, doc.state.replica, block_idx);
        doc.blocks.insert_first(&mut doc.dot_index, node);
        return Delta {
            op_type: OperationType::Insert,
            ids: vec![(Dot{ site: doc.state.replica, seq: doc.state.local_clock, b_idx: block_idx }, base, seq, seq + text_len)],
            payload: Some(text),
            site: doc.state.replica,
        };
    }
 
    let block = node.unwrap();
    let block_base  = doc.blocks.node_base_id(block);
    let block_ranges = doc.blocks.node_ranges(block);
    let block_start = covered;
    let block_end   = block_start + doc.blocks.node_size(Some(block));
 
    // ── Insert at end of block ──────────────────────────────────────────
    if pos == block_end {
        // Try extending in-place first.
        if doc.blocks.node_creator(block) == doc.state.replica {
            // let base_ranges = doc.blocks.node_base_offsets(block);
            // CHECK 
            if block_ranges.1 == seq {
                return extend_block(doc, text, block, doc.state.replica);
            }
        }
 
        // Can't extend — create a new block after this one.
        // let id_low = IdentifierRef::new(block_base, block_ranges.1 - 1);
        // let base;
        let base = match doc.blocks.next(block) {
            Some(next_block) => {
                let next_base   = doc.blocks.node_base_id(next_block);
                let next_ranges = doc.blocks.node_ranges(next_block);
                generate_base(&mut doc.id_arena, block_base, block_ranges.1 - 1, next_base, next_ranges.0, &mut doc.state)
            }
            None => generate_base(&mut doc.id_arena, block_base, block_ranges.1 - 1, Identifier::EMPTY, MAX_VALUE, &mut doc.state)
        };
        // let base = generate_base(&mut doc.id_arena, id_low, id_high, &mut doc.state);
        let block_idx = doc.state.block_idx;
        doc.state.block_idx += 1;
        let node = Node::new(text.clone(), base, seq, doc.state.replica, block_idx);
        doc.blocks.insert_after(&mut doc.dot_index, block, node);
 
        return Delta {
            op_type: OperationType::Insert,
            ids: vec![(Dot{ site: doc.state.replica, seq: doc.state.local_clock, b_idx: block_idx }, base, seq, seq + text_len)],
            payload: Some(text),
            site: doc.state.replica,
            // clock: doc.state.local_clock,
        };
    }
 
    if pos == block_start {
        let base = match doc.blocks.prev(block) {
            Some(prev_block) => {
                let prev_base   = doc.blocks.node_base_id(prev_block);
                let prev_ranges = doc.blocks.node_ranges(prev_block);
                // IdentifierRef::new(prev_base, prev_ranges.1 - 1)
                generate_base(&mut doc.id_arena, prev_base, prev_ranges.1 - 1, block_base, block_ranges.0, &mut doc.state)
            }
            None => generate_base(&mut doc.id_arena, Identifier::EMPTY, MIN_VALUE, block_base, block_ranges.0, &mut doc.state),
        };
        let block_idx = doc.state.block_idx;
        doc.state.block_idx += 1;
        let node = Node::new(text.clone(), base, seq, doc.state.replica, block_idx);
        doc.blocks.insert_before(&mut doc.dot_index, block, node);
        return Delta {
            op_type: OperationType::Insert,
            ids: vec![(Dot{ site: doc.state.replica, seq: doc.state.local_clock, b_idx: block_idx }, base, seq, seq + text_len)],
            payload: Some(text),
            site: doc.state.replica,
        };
    }
 
    // ── Insert in the middle of a block (split) ─────────────────────────
    let sp = (pos - block_start) as u32;
    debug_assert!(
        sp > 0 && sp < block_ranges.1 - block_ranges.0,
        "Invalid split point: sp={}, block_size={}",
        sp,
        block_ranges.1 - block_ranges.0
    );

    let block_idx = doc.state.block_idx;
    doc.state.block_idx += 1;
 
    let base = generate_base(&mut doc.id_arena, block_base, block_ranges.0 + sp - 1, block_base, block_ranges.0 + sp, &mut doc.state);
    let middle = Node::new(text.clone(), base, seq, doc.state.replica, block_idx);
 
    doc.blocks.split_and_insert_middle(&mut doc.dot_index, block, sp as usize, middle);
 
    Delta {
        op_type: OperationType::Insert,
        ids: vec![(Dot{ site: doc.state.replica, seq: doc.state.local_clock, b_idx: block_idx }, base, seq, seq + text_len)],
        payload: Some(text),
        site: doc.state.replica,
    }
}

fn remote_insert(doc: &mut Document, op: &WireDelta) -> Identifier {
    let val = &op.ids[0];
    let block_id = val.0.b_idx;
    let base = &val.1;
    let offset = val.2;
    let text = op.payload.as_ref().expect("No payload for insert operation");
    let site = op.site;

    // Find and insert this id 
    doc.blocks.insert_by_id(site, &mut doc.id_arena, &mut doc.dot_index, base, offset, block_id, text.to_string())
}

fn local_delete(doc: &mut Document, from: usize, to: usize) -> Delta {
    let mut num_delete = to - from;
    let mut del_info = vec![];
    let curr = from;
 
    while num_delete > 0 {
        let (node, covered) = doc.blocks.find_by_pos_delete(curr);
        if node.is_none() {
            panic!("Cannot delete from an empty document");
        }
 
        let block = node.unwrap();
        let block_size = doc.blocks.node_size(Some(block));
        let start_del  = curr - covered;
        let end_del    = start_del + num_delete;
        let base_id    = doc.blocks.node_base_id(block);
        let block_ranges = doc.blocks.node_ranges(block);
        let creator = doc.blocks.node_creator(block);
        let block_idx = doc.blocks.node_block_idx(block);
 
        if start_del == 0 && end_del >= block_size {
            // ── Case 1: delete entire block ─────────────────────────────
            del_info.push((Dot{ site: creator, seq: block_ranges.0, b_idx: block_idx }, base_id, block_ranges.0, block_ranges.1));
            num_delete -= block_size;
 
            // *** DIRECT: uses path, no find_by_id ***
            doc.blocks.delete_target(&mut doc.dot_index, Some(block));
 
        } else if start_del == 0 {
            // ── Case 2: delete from start of block ──────────────────────
            del_info.push((Dot{ site: creator, seq: block_ranges.0, b_idx: block_idx }, base_id, block_ranges.0, block_ranges.0 + end_del as u32));
            doc.blocks.truncate_content(&mut doc.dot_index, block, num_delete, DelLocation::Start);
            num_delete = 0;
 
        } else if end_del >= block_size {
            let n = block_size - start_del;
            del_info.push((Dot{ site: creator, seq: block_ranges.0, b_idx: block_idx }, base_id, block_ranges.0 + start_del as u32, block_ranges.1));
            doc.blocks.truncate_content(&mut doc.dot_index, block, n, DelLocation::End);
            num_delete -= n;
 
        } else {
            del_info.push((Dot{ site: creator, seq: block_ranges.0, b_idx: block_idx }, base_id, block_ranges.0 + start_del as u32, block_ranges.0 + end_del as u32));
            doc.blocks.delete_middle_at_target(&mut doc.dot_index, block, start_del, num_delete);
            num_delete = 0;
        }
    }
 
    Delta {
        op_type: OperationType::Delete,
        ids: del_info,
        payload: None,
        site: doc.state.replica,
    }
}

fn remote_delete(doc: &mut Document, op: &WireDelta) {
    for (dot, id, start, end) in &op.ids {
        let ranges = doc.dot_index.overlapping_ranges(dot.site, dot.b_idx, *start, *end);

        if ranges.is_empty() {
            doc.dotstore.add_to_pending(dot, WireDelta {
                op_type: OperationType::Delete,
                ids: vec![(dot.clone(), id.clone(), *start, *end)],
                payload: None,
                site: op.site,
            });
            continue;
        }

        let mut cursor = *end;
        for &(r_lo, r_hi, block) in ranges.iter().rev() {
            if cursor > r_hi {
                doc.dotstore.add_to_pending(dot, WireDelta {
                    op_type: OperationType::Delete,
                    ids: vec![(dot.clone(), id.clone(), r_hi, cursor)],
                    payload: None,
                    site: op.site,
                });
            }

            let ov_lo = (*start).max(r_lo);
            let ov_hi = cursor.min(r_hi);
            cursor = ov_lo;
            if ov_lo >= ov_hi { continue; }

            let block_ranges = doc.blocks.node_ranges(block);
            let block_size = block_ranges.1 - block_ranges.0;
            let n = ov_hi - ov_lo;

            if ov_lo == block_ranges.0 && n >= block_size {
                doc.blocks.delete_target(&mut doc.dot_index, Some(block));
            } else if ov_lo == block_ranges.0 {
                doc.blocks.truncate_content(&mut doc.dot_index, block, n as usize, DelLocation::Start);
            } else if ov_hi >= block_ranges.1 {
                doc.blocks.truncate_content(&mut doc.dot_index, block, n as usize, DelLocation::End);
            } else {
                let sp = (ov_lo - block_ranges.0) as usize;
                doc.blocks.delete_middle_at_target(&mut doc.dot_index, block, sp, n as usize);
            }
        }

        if cursor > *start {
            doc.dotstore.add_to_pending(dot, WireDelta {
                op_type: OperationType::Delete,
                ids: vec![(dot.clone(), id.clone(), *start, cursor)],
                payload: None,
                site: op.site,
            });
        }
    }
}