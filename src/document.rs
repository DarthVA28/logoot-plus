use std::collections::HashMap;

use crate::idarena::{IdArena, Range, Identifier, IdentifierRef, generate_base};
use crate::node::Node;
use crate::tree::{DelLocation, Path, Tree};
use crate::state::State;
use crate::operation::{OpLog, Operation, OperationType, WireOperation};

#[derive(Clone, Debug)]
pub struct Document { 
    pub blocks: Tree,
    pub id_trie: IdArena,
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
            id_trie: IdArena::new(),
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
            if !self.blocks.check_tree(&self.id_trie) {
                self.blocks.print_tree();
                panic!("Tree structure is invalid after local insert of {} at pos {} at site {}", &op.payload.unwrap().clone(), pos, self.state.replica);
            }
        }
        // println!("After local insert at replica {}", self.state.replica);
        // self.blocks.print_tree();

        self.oplog.record_op(&op);
        self.state.local_clock += 1;
        self.fresh = false;
        Some(op.to_wire(&self.id_trie))
    }

    pub fn del(&mut self, from: usize, to: usize) -> WireOperation {
        // println!("Deleting from {} to {} at site {}", from, to, self.state.replica);
        let op = local_delete(self, from, to);
        if self.debug {
            if !self.blocks.check_tree(&self.id_trie) {
                self.blocks.print_tree();
                panic!("Tree structure is invalid after local delete from {} to {} at site {}", from, to, self.state.replica);
            }
        }
        // println!("After local delete at replica {}", self.state.replica);
        // self.blocks.print_tree();
        self.oplog.record_op(&op);
        self.state.local_clock += 1;
        self.fresh = false;
        op.to_wire(&self.id_trie)
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
        let op = Operation::from_wire(wire_op, &mut self.id_trie);
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
            if !self.blocks.check_tree(&self.id_trie) {
                self.blocks.print_tree();
                panic!("Tree structure is invalid after merging op {:?} from site {} at site {}", op, op.site, self.state.replica);
            }
        }

        // Some operations can now possibly be applied!
        if op.op_type == OperationType::Insert {
            for (id, _, _) in &op.ids {
                let pending_ops = self.oplog.get_pending_for_id(id);
                for op in pending_ops {
                    // println!("Applying pending op {:?} for id {:?} at site {}", op, id, self.state.replica);
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
        self.id_trie.clear();
        self.used_ranges_for_id.clear();
        self.snapshot.clear();
        self.oplog.clear();
        self.state.local_clock = 1;
        self.fresh = true;
    }

}

fn extend_block(doc: &mut Document, text: String, block: usize, path: &Path, site: u32) -> Operation {
    let next = doc.blocks.next(block, path);
    let insert_base = doc.blocks.node_base_id(block);
    let insert_offsets = doc.blocks.node_ranges(block);
 
    if let Some(nxt_block) = next {
        let text_len = text.chars().count() as u32;
        let next_base = doc.blocks.node_base_id(nxt_block);
        let next_offsets = doc.blocks.node_ranges(nxt_block);
        let id_insert = IdentifierRef::new(insert_base, insert_offsets.1);
        let id_next   = IdentifierRef::new(next_base, next_offsets.0);
        let n = doc.id_trie.num_insertable(id_insert, id_next, text_len);
        if n < text_len {
            // Can't extend — not enough room before the next block.
            // Fall back to creating a new block between this and next.
            let id_low  = IdentifierRef::new(insert_base, insert_offsets.1 - 1);
            let id_high = IdentifierRef::new(next_base, next_offsets.0);
            let base = generate_base(&mut doc.id_trie, id_low, id_high, &mut doc.state);
            let node = Node::new(text.clone(), base, 0, site);
            doc.blocks.insert_after(path, node);
            return Operation {
                op_type: OperationType::Insert,
                ids: vec![(base, 0, 1)],
                payload: Some(text),
                site,
                clock: doc.state.local_clock,
            };
        }
    }
 
    doc.blocks.extend_content(block, &text, path);
    Operation {
        op_type: OperationType::Insert,
        ids: vec![(insert_base, insert_offsets.1, insert_offsets.1 + 1)],
        payload: Some(text),
        site,
        clock: doc.state.local_clock,
    }
}

fn local_insert(doc: &mut Document, pos: usize, text: String) -> Operation {
    let doc_size = doc.blocks.tree_size();
    let pos = if pos > doc_size { doc_size } else { pos };
 
    let (path, covered) = doc.blocks.find_by_pos(pos);
 
    // ── Empty tree ──────────────────────────────────────────────────────
    if path.is_empty() {
        let base = generate_base(
            &mut doc.id_trie,
            IdentifierRef::doc_start(),
            IdentifierRef::doc_end(),
            &mut doc.state,
        );
        let node = Node::new(text.clone(), base, 0, doc.state.replica);
        doc.blocks.insert_first(node);
        return Operation {
            op_type: OperationType::Insert,
            ids: vec![(base, 0, 1)],
            payload: Some(text),
            site: doc.state.replica,
            clock: doc.state.local_clock,
        };
    }
 
    let block       = *path.last().unwrap();
    let block_base  = doc.blocks.node_base_id(block);
    let block_ranges = doc.blocks.node_ranges(block);
    let block_start = covered;
    let block_end   = block_start + doc.blocks.node_size(Some(block));
 
    // ── Insert at end of block ──────────────────────────────────────────
    if pos == block_end {
        // Try extending in-place first.
        if doc.blocks.node_creator(block) == doc.state.replica {
            let base_ranges = doc.blocks.node_base_offsets(block);
            if block_ranges.1 == base_ranges.1 {
                return extend_block(doc, text, block, &path, doc.state.replica);
            }
        }
 
        // Can't extend — create a new block after this one.
        let id_low = IdentifierRef::new(block_base, block_ranges.1 - 1);
        let id_high = match doc.blocks.next(block, &path) {
            Some(next_block) => {
                let next_base   = doc.blocks.node_base_id(next_block);
                let next_ranges = doc.blocks.node_ranges(next_block);
                IdentifierRef::new(next_base, next_ranges.0)
            }
            None => IdentifierRef::doc_end(),
        };
        let base = generate_base(&mut doc.id_trie, id_low, id_high, &mut doc.state);
        let node = Node::new(text.clone(), base, 0, doc.state.replica);
 
        // *** DIRECT: insert_after uses the path, no find_by_id ***
        doc.blocks.insert_after(&path, node);
 
        return Operation {
            op_type: OperationType::Insert,
            ids: vec![(base, 0, 1)],
            payload: Some(text),
            site: doc.state.replica,
            clock: doc.state.local_clock,
        };
    }
 
    if pos == block_start {
        let id_low = match doc.blocks.prev(block, &path) {
            Some(prev_block) => {
                let prev_base   = doc.blocks.node_base_id(prev_block);
                let prev_ranges = doc.blocks.node_ranges(prev_block);
                IdentifierRef::new(prev_base, prev_ranges.1 - 1)
            }
            None => IdentifierRef::doc_start(),
        };
        let id_high = IdentifierRef::new(block_base, block_ranges.0);
        let base = generate_base(&mut doc.id_trie, id_low, id_high, &mut doc.state);
        let node = Node::new(text.clone(), base, 0, doc.state.replica);
 
        // *** DIRECT: insert_before uses the path, no find_by_id ***
        doc.blocks.insert_before(&path, node);
 
        return Operation {
            op_type: OperationType::Insert,
            ids: vec![(base, 0, 1)],
            payload: Some(text),
            site: doc.state.replica,
            clock: doc.state.local_clock,
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
 
    let id_low  = IdentifierRef::new(block_base, block_ranges.0 + sp - 1);
    let id_high = IdentifierRef::new(block_base, block_ranges.0 + sp);
    let base = generate_base(&mut doc.id_trie, id_low, id_high, &mut doc.state);
    let middle = Node::new(text.clone(), base, 0, doc.state.replica);
 
    doc.blocks.split_and_insert_middle(&path, sp as usize, middle);
 
    Operation {
        op_type: OperationType::Insert,
        ids: vec![(base, 0, 1)],
        payload: Some(text),
        site: doc.state.replica,
        clock: doc.state.local_clock,
    }
}

fn remote_insert(doc: &mut Document, op: &Operation) {
    let val = op.ids[0].clone();
    let base  = val.0;
    let offset = val.1;
    let text = op.payload.as_ref().expect("No payload for insert operation");
    let site = op.site;

    // Find and insert this id 
    doc.blocks.insert_by_id(site, &doc.id_trie, base, offset, text.to_string());
}

fn local_delete(doc: &mut Document, from: usize, to: usize) -> Operation {
    let mut num_delete = to - from;
    let mut del_info: Vec<(Identifier, u32, u32)> = vec![];
    let curr = from;
 
    while num_delete > 0 {
        let (path, covered) = doc.blocks.find_by_pos_delete(curr);
        if path.is_empty() {
            panic!("Cannot delete from an empty document");
        }
 
        let block      = *path.last().unwrap();
        let block_size = doc.blocks.node_size(Some(block));
        let start_del  = curr - covered;
        let end_del    = start_del + num_delete;
        let base_id    = doc.blocks.node_base_id(block);
        let block_ranges = doc.blocks.node_ranges(block);
 
        if start_del == 0 && end_del >= block_size {
            // ── Case 1: delete entire block ─────────────────────────────
            del_info.push((base_id, block_ranges.0, block_ranges.1));
            num_delete -= block_size;
 
            // *** DIRECT: uses path, no find_by_id ***
            doc.blocks.delete_at_path(&path);
 
        } else if start_del == 0 {
            // ── Case 2: delete from start of block ──────────────────────
            del_info.push((base_id, block_ranges.0, block_ranges.0 + end_del as u32));
            doc.blocks.truncate_content(block, num_delete, DelLocation::Start, &path);
            num_delete = 0;
 
        } else if end_del >= block_size {
            // ── Case 3: delete from end of block ────────────────────────
            let n = block_size - start_del;
            del_info.push((base_id, block_ranges.0 + start_del as u32, block_ranges.1));
            doc.blocks.truncate_content(block, n, DelLocation::End, &path);
            num_delete -= n;
 
        } else {
            // ── Case 4: delete from middle (split around hole) ──────────
            del_info.push((base_id, block_ranges.0 + start_del as u32, block_ranges.0 + end_del as u32));
 
            // *** DIRECT: one call replaces delete_by_id + 2× insert_by_id ***
            doc.blocks.delete_middle_at_path(&path, start_del, num_delete);
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

fn remote_delete(doc: &mut Document, op: &Operation) {
    let del_ids = &op.ids;
    for (id, start, end) in del_ids {
        let mut cursor = *start;
        // Surviving neighbor from the last iteration — used as a
        // linked-list anchor to skip gaps without tree traversals.
        let mut ll_anchor: Option<usize> = None;

        while cursor < *end {
            let path = doc.blocks.find_by_id_exact(&doc.id_trie, *id, cursor);

            if path.is_empty() {
                // ── Fast exit: no offsets for this base beyond cursor ────
                if doc.blocks.base_id_max_offset(*id).map_or(true, |hi| hi <= cursor) {
                    let partial_op = Operation {
                        op_type: OperationType::Delete,
                        ids: vec![(*id, cursor, *end)],
                        payload: None,
                        site: op.site,
                        clock: op.clock,
                    };
                    doc.oplog.add_to_pending(partial_op);
                    break;
                }

                // ── Find where the gap ends ─────────────────────────────
                let gap_end = if let Some(anchor) = ll_anchor {
                    // O(few hops) — same-base nodes cluster in the LL
                    doc.blocks.ll_scan_for_base(anchor, *id, cursor, *end)
                } else {
                    // No anchor yet (gap at the very start of the range).
                    // Fall back to linear probe — this is rare and short.
                    let mut probe = cursor + 1;
                    while probe < *end {
                        if !doc.blocks.find_by_id_exact(&doc.id_trie, *id, probe).is_empty() {
                            break;
                        }
                        probe += 1;
                    }
                    probe
                };

                let partial_op = Operation {
                    op_type: OperationType::Delete,
                    ids: vec![(*id, cursor, gap_end)],
                    payload: None,
                    site: op.site,
                    clock: op.clock,
                };
                doc.oplog.add_to_pending(partial_op);
                cursor = gap_end;
                continue;
            }

            // ── Found the node ──────────────────────────────────────────
            let block = *path.last().unwrap();
            if doc.blocks.node_base_id(block) != *id {
                panic!(
                    "remote_delete: expected base {:?} at offset {}, found {:?} at site {}",
                    id, cursor, doc.blocks.node_base_id(block), doc.state.replica
                );
            }

            let block_ranges = doc.blocks.node_ranges(block);
            let block_size = block_ranges.1 - block_ranges.0;
            let overlap_lo = cursor.max(block_ranges.0);
            let overlap_hi = (*end).min(block_ranges.1);
            let n_in_block = overlap_hi - overlap_lo;

            // ── Capture LL neighbors BEFORE mutating ────────────────────
            let ll_prev = doc.blocks.nodes[block].ll_prev;
            let ll_next = doc.blocks.nodes[block].ll_next;

            if overlap_lo == block_ranges.0 && n_in_block >= block_size {
                // Full delete — block is freed, use a surviving neighbor
                doc.blocks.delete_at_path(&path);
                ll_anchor = ll_prev.or(ll_next);
            } else if overlap_lo == block_ranges.0 {
                // Truncate start — block survives
                doc.blocks.truncate_content(block, n_in_block as usize, DelLocation::Start, &path);
                ll_anchor = Some(block);
            } else if overlap_hi >= block_ranges.1 {
                // Truncate end — block survives
                doc.blocks.truncate_content(block, n_in_block as usize, DelLocation::End, &path);
                ll_anchor = Some(block);
            } else {
                // Middle delete (split) — block becomes left half, survives
                let sp = (overlap_lo - block_ranges.0) as usize;
                doc.blocks.delete_middle_at_path(&path, sp, n_in_block as usize);
                ll_anchor = Some(block);
            }

            cursor = overlap_hi;
        }
    }
}