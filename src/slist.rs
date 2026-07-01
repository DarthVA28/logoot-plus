use std::cmp::Ordering;
use ahash::AHashMap as HashMap;
use smallvec::SmallVec;
use rand::{RngExt, SeedableRng};
use crate::dotindex::DotIndex;
use crate::idarena::{Identifier, IdArena, IdOrderingRelation};
use crate::dotstore::Dot;
use crate::delta::{OperationType, WireDelta};
use crate::node::{Node, MAX_HEIGHT, HEAD, Level};

pub type Path = SmallVec<[usize; 32]>;

pub enum DelLocation { Start, End }
 
#[derive(Clone, Debug)]
pub struct Tree {
    pub nodes: Vec<Node>,
    free_list: Vec<usize>,
    /// Current maximum level in use (0-based, so 1 means only level 0 is used).
    max_level: usize,
    total_size: usize,
    base_to_offsets: HashMap<Identifier, (u32, u32)>,
    rng: rand::rngs::SmallRng,
}
 
// ── Allocation & basics ────────────────────────────────────────────────────
 
impl Tree {
    pub fn new() -> Self {
        let mut nodes = Vec::with_capacity(4096);
        nodes.push(Node::sentinel()); 
        Tree {
            nodes,
            free_list: Vec::new(),
            max_level: 1,
            total_size: 0,
            base_to_offsets: HashMap::new(),
            rng: rand::rngs::SmallRng::seed_from_u64(0xCAFE),
        }
    }
 
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.nodes.push(Node::sentinel());
        self.free_list.clear();
        self.max_level = 1;
        self.total_size = 0;
        self.base_to_offsets.clear();
    }
 
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.nodes[HEAD].levels[0].next.is_none()
    }
 
    #[inline(always)]
    pub fn tree_size(&self) -> usize {
        self.total_size
    }
 
    fn alloca(&mut self, node: Node) -> usize {
        match self.free_list.pop() {
            Some(idx) => { self.nodes[idx] = node; idx }
            None      => { self.nodes.push(node); self.nodes.len() - 1 }
        }
    }
 
    fn free(&mut self, idx: usize) {
        self.free_list.push(idx);
    }
 
    fn random_height(&mut self) -> usize {
        let bits: u32 = self.rng.random();
        (bits.trailing_zeros() as usize + 1).min(MAX_HEIGHT)
    }
}
 
// ── Node accessors (API-compatible) ────────────────────────────────────────
 
impl Tree {
    #[inline(always)]
    pub fn node_size(&self, node: Option<usize>) -> usize {
        node.map_or(0, |i| self.nodes[i].size)
    }
 
    pub fn node_content(&self, node: Option<usize>) -> &str {
        node.map_or("", |i| &self.nodes[i].content)
    }
 
    #[inline(always)]
    pub fn node_creator(&self, node: usize) -> u32 {
        self.nodes[node].creator
    }
 
    #[inline(always)]
    pub fn node_base_id(&self, node: usize) -> Identifier {
        self.nodes[node].base_id
    }
 
    #[inline(always)]
    pub fn node_ranges(&self, node: usize) -> (u32, u32) {
        let n = &self.nodes[node];
        (n.offset, n.offset + n.size as u32)
    }
 
    pub fn node_base_offsets(&self, node: usize) -> (u32, u32) {
        let base_id = self.nodes[node].base_id;
        *self.base_to_offsets.get(&base_id)
            .expect("base offsets not found")
    }
 
    pub fn base_id_max_offset(&self, id: Identifier) -> Option<u32> {
        self.base_to_offsets.get(&id).map(|(_, hi)| *hi)
    }
 
    pub fn node_set_base_id(&mut self, node: usize, new_base: Identifier) {
        self.nodes[node].base_id = new_base;
    }
 
    #[inline(always)]
    pub fn node_block_idx(&self, node: usize) -> u32 {
        self.nodes[node].block_idx
    }
}
 
// ── O(1) next / prev ──────────────────────────────────────────────────────
 
impl Tree {
    /// Inorder successor — O(1).
    #[inline(always)]
    pub fn next(&self, node: usize) -> Option<usize> {
        self.nodes[node].levels[0].next
    }
 
    /// Inorder predecessor — O(1). Returns `None` if `node` is the first data node.
    #[inline(always)]
    pub fn prev(&self, node: usize) -> Option<usize> {
        match self.nodes[node].prev {
            Some(p) if p != HEAD => Some(p),
            _ => None,
        }
    }
}
 
// ── Position lookup ───────────────────────────────────────────────────────
 
impl Tree {
    /// Find the node containing character at `pos`.
    /// Returns `(Some(node_idx), covered)` where `covered` = characters before the
    /// node's content, so `pos - covered` is the local offset within the node.
    /// Returns `(None, total_size)` if `pos >= total_size` or the list is empty.
    pub fn find_by_pos(&self, pos: usize) -> (Option<usize>, usize) {
        let mut curr = HEAD;
        let mut remaining = pos;
 
        // Descend through levels, advancing while width ≤ remaining
        for level in (0..self.max_level).rev() {
            while let Some(nxt) = self.nodes[curr].levels[level].next {
                let w = self.nodes[curr].levels[level].width;
                if w <= remaining {
                    remaining -= w;
                    curr = nxt;
                } else {
                    break;
                }
            }
        }
 
        if curr == HEAD {
            // remaining >= 0 but sentinel has size 0, so check first real node
            return match self.nodes[HEAD].levels[0].next {
                Some(first) if remaining < self.nodes[first].size => (Some(first), 0),
                _ => (None, 0),
            };
        }
 
        // curr is a data node; remaining is the local offset
        if remaining < self.nodes[curr].size {
            let covered = pos - remaining;
            (Some(curr), covered)
        } else if remaining == self.nodes[curr].size {
            // Exactly at the end of this node (used for end-of-block insertions)
            let covered = pos - remaining;
            (Some(curr), covered)
        } else {
            (None, pos)
        }
    }
 
    /// Deletion variant: uses strict `<` instead of `<=` for the right edge
    /// (there is no valid deletion position at the right boundary of a node).
    pub fn find_by_pos_delete(&self, pos: usize) -> (Option<usize>, usize) {
        let mut curr = HEAD;
        let mut remaining = pos;
        let mut last = None;
 
        for level in (0..self.max_level).rev() {
            while let Some(nxt) = self.nodes[curr].levels[level].next {
                let w = self.nodes[curr].levels[level].width;
                if w <= remaining {
                    remaining -= w;
                    curr = nxt;
                } else {
                    break;
                }
            }
        }
 
        if curr == HEAD {
            if let Some(first) = self.nodes[HEAD].levels[0].next {
                last = Some(first);
                if remaining < self.nodes[first].size {
                    return (Some(first), 0);
                }
            }
            return (last, 0);
        }
 
        let covered = pos - remaining;
        if remaining < self.nodes[curr].size {
            (Some(curr), covered)
        } else {
            (Some(curr), covered)
        }
    }
}
 
// ── Low-level skip list operations ─────────────────────────────────────────
 
impl Tree {
    /// Build the update array by walking backward from `pred` at level 0.
    ///
    /// Returns a vec of `(update_node_idx, distance_to_insertion_point)` for
    /// each level from 0 to `num_levels - 1`.
    ///
    /// `pred` is the level-0 predecessor of the insertion point.
    /// `num_levels` should be `max(new_height, self.max_level)`.
    fn build_update_array(&self, pred: usize, num_levels: usize) -> SmallVec<[(usize, usize); 20]> {
        let mut updates = SmallVec::with_capacity(num_levels);
        let mut curr = pred;
        let mut dist = self.nodes[pred].size;
 
        for level in 0..num_levels {
            // Walk backward until finding a node that participates at `level`
            while self.nodes[curr].levels.len() <= level {
                // prev is always Some because sentinel has MAX_HEIGHT levels
                curr = self.nodes[curr].prev.unwrap_or(HEAD);
                dist += self.nodes[curr].size;
            }
            updates.push((curr, dist));
        }
 
        updates
    }
 
    /// Insert a pre-allocated node right after `pred` in the skip list.
    /// Generates a random height, wires all level pointers, updates widths.
    /// Returns the index of the inserted node.
    fn sl_insert_after(&mut self, pred: usize, idx: usize) {
        let h = self.random_height();
        let num_levels = h.max(self.max_level);
        let updates = self.build_update_array(pred, num_levels);
        let new_size = self.nodes[idx].size;
 
        // Initialise levels for the new node
        self.nodes[idx].levels.clear();
        for _ in 0..h {
            self.nodes[idx].levels.push(Level { next: None, width: 0 });
        }
 
        // Wire pointers and update widths
        for level in 0..num_levels {
            let (upd, d) = updates[level];
            if level < h {
                // New node participates at this level — split the pointer
                self.nodes[idx].levels[level].next  = self.nodes[upd].levels[level].next;
                self.nodes[idx].levels[level].width  = self.nodes[upd].levels[level].width
                    .saturating_sub(d) + new_size;
                self.nodes[upd].levels[level].next  = Some(idx);
                self.nodes[upd].levels[level].width = d;
            } else {
                // New node doesn't reach this level — just widen
                self.nodes[upd].levels[level].width += new_size;
            }
        }
 
        // Wire level-0 prev pointers
        let old_next = self.nodes[idx].levels[0].next;
        self.nodes[idx].prev = Some(pred);
        if let Some(nxt) = old_next {
            self.nodes[nxt].prev = Some(idx);
        }
 
        // Possibly grow max_level
        if h > self.max_level {
            self.max_level = h;
        }
 
        self.total_size += new_size;
    }
 
    /// Remove `idx` from the skip list, updating all level pointers and widths.
    fn sl_remove(&mut self, idx: usize) {
        let h = self.nodes[idx].levels.len();
        let node_size = self.nodes[idx].size;
        let pred = self.nodes[idx].prev.unwrap_or(HEAD);
        let num_levels = h.max(self.max_level);
        let updates = self.build_update_array(pred, num_levels);
 
        for level in 0..num_levels {
            let (upd, _) = updates[level];
            if level < h {
                // Splice out at this level
                debug_assert_eq!(self.nodes[upd].levels[level].next, Some(idx));
                self.nodes[upd].levels[level].next = self.nodes[idx].levels[level].next;
                self.nodes[upd].levels[level].width += self.nodes[idx].levels[level].width
                    .saturating_sub(node_size);
            } else {
                // Just shrink
                self.nodes[upd].levels[level].width -= node_size;
            }
        }
 
        // Fix prev pointer of successor
        if let Some(nxt) = self.nodes[idx].levels[0].next {
            self.nodes[nxt].prev = Some(pred);
        }
 
        self.total_size -= node_size;
        self.free(idx);
 
        // Possibly shrink max_level
        while self.max_level > 1 && self.nodes[HEAD].levels[self.max_level - 1].next.is_none() {
            self.max_level -= 1;
        }
    }
 
    /// Update all level widths when a node's content size changes by `delta`.
    /// Positive delta = grew, negative = shrank.
    fn update_widths_for_size_change(&mut self, node: usize, delta: isize) {
        let h = self.nodes[node].levels.len();
 
        // Levels the node participates in
        for k in 0..h {
            self.nodes[node].levels[k].width =
                (self.nodes[node].levels[k].width as isize + delta) as usize;
        }
 
        // Levels above: walk backward to find enclosing nodes
        let mut curr = node;
        for level in h..self.max_level {
            while self.nodes[curr].levels.len() <= level {
                curr = self.nodes[curr].prev.unwrap_or(HEAD);
            }
            self.nodes[curr].levels[level].width =
                (self.nodes[curr].levels[level].width as isize + delta) as usize;
        }
 
        self.total_size = (self.total_size as isize + delta) as usize;
    }
}
 
// ── Content mutation ──────────────────────────────────────────────────────
 
impl Tree {
    pub fn extend_content(&mut self, dot_index: &mut DotIndex, node_idx: usize, text: &str) {
        let added = text.len();
        self.nodes[node_idx].content.push_str(text);
        self.nodes[node_idx].size += added;
 
        // Update base_to_offsets
        let base_id = self.nodes[node_idx].base_id;
        if let Some((lo, hi)) = self.base_to_offsets.get(&base_id) {
            let new_hi = hi + added as u32;
            self.base_to_offsets.insert(base_id, (*lo, new_hi));
        }
 
        dot_index.on_block_extended(
            self.nodes[node_idx].creator,
            self.nodes[node_idx].block_idx,
            self.nodes[node_idx].offset,
            self.nodes[node_idx].offset + self.nodes[node_idx].size as u32,
        );
 
        self.update_widths_for_size_change(node_idx, added as isize);
    }
 
    pub fn truncate_content(
        &mut self, dot_index: &mut DotIndex, node_idx: usize,
        num_delete: usize, location: DelLocation,
    ) {
        let creator  = self.nodes[node_idx].creator;
        let old_off  = self.nodes[node_idx].offset;
        let is_start = matches!(&location, DelLocation::Start);
 
        {
            let n = &mut self.nodes[node_idx];
            match location {
                DelLocation::Start => {
                    let byte_off = n.content.char_indices()
                        .nth(num_delete)
                        .map(|(i, _)| i)
                        .unwrap_or(n.content.len());
                    let kept = n.content.split_off(byte_off);
                    n.content = kept;
                    n.offset += num_delete as u32;
                }
                DelLocation::End => {
                    let keep_chars = n.size - num_delete;
                    let byte_off = n.content.char_indices()
                        .nth(keep_chars)
                        .map(|(i, _)| i)
                        .unwrap_or(n.content.len());
                    n.content.truncate(byte_off);
                }
            }
            n.size -= num_delete;
        }
 
        if is_start {
            dot_index.on_block_truncated_start(
                creator, self.nodes[node_idx].block_idx,
                old_off, old_off + num_delete as u32,
            );
        } else {
            let new_hi = self.nodes[node_idx].offset + self.nodes[node_idx].size as u32;
            dot_index.on_block_truncated_end(
                creator, self.nodes[node_idx].block_idx,
                old_off, new_hi,
            );
        }
 
        self.update_widths_for_size_change(node_idx, -(num_delete as isize));
    }
}
 
// ── Insertion helpers ─────────────────────────────────────────────────────
 
impl Tree {
    #[inline(always)]
    fn register_base_offsets(&mut self, base: Identifier, offset: u32, size: u32) {
        if let Some((lo, hi)) = self.base_to_offsets.get(&base) {
            let new_lo = (*lo).min(offset);
            let new_hi = (*hi).max(offset + size);
            self.base_to_offsets.insert(base, (new_lo, new_hi));
        } else {
            self.base_to_offsets.insert(base, (offset, offset + size));
        }
    }
 
    /// Insert the very first data node (empty document).
    pub fn insert_first(&mut self, dot_index: &mut DotIndex, node: Node) -> usize {
        let idx = self.alloca(node);
        let n = &self.nodes[idx];
        dot_index.on_block_inserted(n.creator, n.block_idx, n.offset, n.offset + n.size as u32, idx);
        self.register_base_offsets(n.base_id, n.offset, n.size as u32);
        self.sl_insert_after(HEAD, idx);
        idx
    }
 
    /// Insert `node` immediately after `target` in document order.
    pub fn insert_after(&mut self, dot_index: &mut DotIndex, target: usize, node: Node) -> usize {
        let idx = self.alloca(node);
        let n = &self.nodes[idx];
        dot_index.on_block_inserted(n.creator, n.block_idx, n.offset, n.offset + n.size as u32, idx);
        self.register_base_offsets(n.base_id, n.offset, n.size as u32);
        self.sl_insert_after(target, idx);
        idx
    }
 
    /// Insert `node` immediately before `target` in document order.
    pub fn insert_before(&mut self, dot_index: &mut DotIndex, target: usize, node: Node) -> usize {
        // Predecessor at level 0 is target.prev (or HEAD if target is first)
        let pred = self.nodes[target].prev.unwrap_or(HEAD);
        let idx = self.alloca(node);
        let n = &self.nodes[idx];
        dot_index.on_block_inserted(n.creator, n.block_idx, n.offset, n.offset + n.size as u32, idx);
        self.register_base_offsets(n.base_id, n.offset, n.size as u32);
        self.sl_insert_after(pred, idx);
        idx
    }
}
 
// ── Split & delete-middle ──────────────────────────────────────────────────
 
impl Tree {
    /// Split `target` at character offset `sp`, insert `middle` between the halves.
    /// Returns the index of the inserted middle node.
    pub fn split_and_insert_middle(
        &mut self,
        dot_index: &mut DotIndex,
        target: usize,
        sp: usize,
        middle: Node,
    ) -> usize {
        let base_id   = self.nodes[target].base_id;
        let offset    = self.nodes[target].offset;
        let creator   = self.nodes[target].creator;
        let block_idx = self.nodes[target].block_idx;
 
        // Split content
        let content = std::mem::take(&mut self.nodes[target].content);
        let byte_idx = content.char_indices()
            .nth(sp)
            .map(|(i, _)| i)
            .unwrap_or(content.len());
        let left_content  = content[..byte_idx].to_string();
        let right_content = content[byte_idx..].to_string();
 
        let old_size = self.nodes[target].size;
        let right_size = old_size - sp;
 
        // Shrink target to left half
        self.nodes[target].content = left_content;
        self.nodes[target].size = sp;
        self.update_widths_for_size_change(target, -(right_size as isize));
 
        // Create right-half node
        let right_node = Node::new(right_content, base_id, offset + sp as u32, creator, block_idx);
        let right_idx = self.alloca(right_node);
 
        // Capture middle metadata before moving it
        let middle_creator   = middle.creator;
        let middle_base      = middle.base_id;
        let middle_offset    = middle.offset;
        let middle_size      = middle.size as u32;
        let middle_block_idx = middle.block_idx;
        let middle_idx = self.alloca(middle);
 
        self.register_base_offsets(middle_base, middle_offset, middle_size);
 
        // Update DotIndex: split the original block, then record the new insertion
        dot_index.on_block_split(creator, block_idx, offset, offset + sp as u32, right_idx);
        dot_index.on_block_inserted(
            middle_creator, middle_block_idx,
            middle_offset, middle_offset + middle_size, middle_idx,
        );
 
        // Insert middle after target, then right after middle
        self.sl_insert_after(target, middle_idx);
        self.sl_insert_after(middle_idx, right_idx);
 
        middle_idx
    }
 
    /// Delete a range from the middle of a node's content, producing left and
    /// right halves with the gap removed.
    pub fn delete_middle_at_target(
        &mut self,
        dot_index: &mut DotIndex,
        target: usize,
        start: usize,
        count: usize,
    ) {
        let base_id   = self.nodes[target].base_id;
        let offset    = self.nodes[target].offset;
        let creator   = self.nodes[target].creator;
        let block_idx = self.nodes[target].block_idx;
        let old_size  = self.nodes[target].size;
 
        let content = std::mem::take(&mut self.nodes[target].content);
 
        // Find byte boundaries
        let mut indices = content.char_indices();
        let left_byte = indices.nth(start)
            .map(|(i, _)| i)
            .unwrap_or(content.len());
        let mid_byte = indices.nth(count - 1)
            .map(|(i, _)| i)
            .unwrap_or(content.len());
 
        let left_content  = content[..left_byte].to_string();
        let right_content = content[mid_byte..].to_string();
 
        // Shrink target to left half
        let removed = old_size - start; // everything from start onwards
        self.nodes[target].content = left_content;
        self.nodes[target].size = start;
        self.update_widths_for_size_change(target, -(removed as isize));
 
        // Create right-half node
        let right_node = Node::new(
            right_content, base_id,
            offset + (start + count) as u32,
            creator, block_idx,
        );
        let right_idx = self.alloca(right_node);
 
        // Update DotIndex
        dot_index.on_block_middle_deleted(
            creator, block_idx, offset,
            offset + start as u32,
            offset + (start + count) as u32,
            offset + old_size as u32,
            right_idx,
        );
 
        // Insert right half after target
        self.sl_insert_after(target, right_idx);
    }
 
    /// Remove the node at `target` from the list entirely.
    pub fn delete_target(&mut self, dot_index: &mut DotIndex, target: Option<usize>) {
        let Some(idx) = target else { return };
        let creator   = self.nodes[idx].creator;
        let offset    = self.nodes[idx].offset;
        let block_idx = self.nodes[idx].block_idx;
 
        dot_index.on_block_deleted(creator, block_idx, offset);
        self.sl_remove(idx);
    }
}
 
// ── Identifier-based insertion (CRDT layer) ───────────────────────────────
 
impl Tree {
    /// Top-down skip list search by identifier.
    /// Returns the level-0 predecessor of where `(base, lo)` should be inserted.
    fn find_position_by_id(&self, id_arena: &IdArena, base: &[u32], lo: u32, hi: u32) -> (usize, IdOrderingRelation) {
        let mut curr = HEAD;
        let mut last_relation = IdOrderingRelation::B1BeforeB2; // default
 
        for level in (0..self.max_level).rev() {
            while let Some(nxt) = self.nodes[curr].levels[level].next {
                let n = &self.nodes[nxt];
                let rel = id_arena.compare_intervals_first_raw(
                    base, lo, hi,
                    n.base_id, n.offset, n.offset + n.size as u32,
                );
                match rel {
                    IdOrderingRelation::B1AfterB2
                    | IdOrderingRelation::B1AfterB2E
                    | IdOrderingRelation::B2ConcatB1 => {
                        // New id comes after this node — advance
                        last_relation = rel;
                        curr = nxt;
                    }
                    _ => {
                        // New id is before, inside, or equal — stop at this level
                        last_relation = rel;
                        break;
                    }
                }
            }
        }
 
        // curr is now the last node (at level 0) that is strictly before the
        // insertion point, or HEAD if the new id comes first.
        // last_relation is the relation with curr's level-0 successor.
        (curr, last_relation)
    }
 
    /// Insert a remote block by its CRDT identifier.
    ///
    /// `origin_left`: optional dot of the character immediately left of the
    /// insertion at the originating site — used as a finger hint.
    ///
    /// Returns the interned Identifier for the base.
    pub fn insert_by_id(
        &mut self,
        site: u32,
        id_arena: &mut IdArena,
        dot_index: &mut DotIndex,
        base: &[u32],
        offset: u32,
        block_idx: u32,
        content: String,
        origin_left: Option<Dot>,
    ) -> Identifier {
        let len = content.len() as u32;
        let lo = offset;
        let hi = offset + len;
 
        // Allocate the node (levels assigned during sl_insert_after)
        let idx = self.alloca(Node::new(content, Identifier::EMPTY, offset, site, block_idx));
 
        // ── Empty list fast path ──
        if self.is_empty() {
            let base_id = id_arena.intern(base);
            self.node_set_base_id(idx, base_id);
            self.register_base_offsets(base_id, lo, len);
            dot_index.on_block_inserted(site, block_idx, lo, hi, idx);
            self.sl_insert_after(HEAD, idx);
            return base_id;
        }
 
        // ── Find insertion position ──
        //
        // Strategy:
        //   1. If origin_left is provided, look it up in DotIndex → finger node.
        //      Walk right at level 0 comparing identifiers.
        //   2. Otherwise, top-down skip list search by identifier.
        //
        // In both cases we end up with a `pred` (level-0 predecessor) and may
        // need to handle CRDT-specific cases (split, extend, etc.).
 
        let (pred, relation) = match origin_left {
            Some(dot) => self.find_by_finger(id_arena, dot_index, base, lo, hi, dot),
            None      => self.find_position_by_id(id_arena, base, lo, hi),
        };
 
        // ── Handle CRDT relation with the successor ──
        self.insert_rec(
            pred, relation, idx, base, lo, hi, len,
            site, block_idx, id_arena, dot_index,
        )
    }
 
    /// Finger search: resolve `origin_left` via DotIndex, walk right at level 0.
    fn find_by_finger(
        &self,
        id_arena: &IdArena,
        dot_index: &DotIndex,
        base: &[u32], lo: u32, hi: u32,
        origin_left: Dot,
    ) -> (usize, IdOrderingRelation) {
        // Try to resolve the finger
        if let Some(finger_node) = dot_index.lookup(origin_left.site, origin_left.b_idx, origin_left.seq) {
            // Walk right from finger_node at level 0 until we find the right spot
            let mut curr = finger_node;
            loop {
                let nxt = self.nodes[curr].levels[0].next;
                match nxt {
                    Some(n) => {
                        let nn = &self.nodes[n];
                        let rel = id_arena.compare_intervals_first_raw(
                            base, lo, hi,
                            nn.base_id, nn.offset, nn.offset + nn.size as u32,
                        );
                        match rel {
                            IdOrderingRelation::B1AfterB2
                            | IdOrderingRelation::B1AfterB2E
                            | IdOrderingRelation::B2ConcatB1 => {
                                // Keep walking right
                                curr = n;
                            }
                            _ => return (curr, rel),
                        }
                    }
                    None => {
                        // End of list; insert after curr
                        return (curr, IdOrderingRelation::B1AfterB2);
                    }
                }
            }
        } else {
            // Finger miss — fall back to top-down search
            self.find_position_by_id(id_arena, base, lo, hi)
        }
    }
 
    /// Handle the CRDT-specific insertion logic based on the relation between
    /// the incoming block and the existing block at the insertion point.
    ///
    /// `pred` is the level-0 predecessor found during the search.
    /// `relation` is the id-ordering relation with pred's successor.
    fn insert_rec(
        &mut self,
        pred: usize,
        relation: IdOrderingRelation,
        idx: usize,         // pre-allocated new node
        base: &[u32],
        lo: u32, hi: u32,
        len: u32,
        site: u32,
        block_idx: u32,
        id_arena: &mut IdArena,
        dot_index: &mut DotIndex,
    ) -> Identifier {
        // The successor of pred (the node our new block relates to)
        let succ = self.nodes[pred].levels[0].next;
 
        match relation {
            // ── Simple before/after: insert between pred and succ ──
            IdOrderingRelation::B1BeforeB2
            | IdOrderingRelation::B1BeforeB2E
            | IdOrderingRelation::B1ConcatB2
            | IdOrderingRelation::B1AfterB2
            | IdOrderingRelation::B1AfterB2E => {
                let base_id = if let Some(s) = succ {
                    // Try to reuse the base_id if relation says same base
                    match relation {
                        IdOrderingRelation::B1BeforeB2E
                        | IdOrderingRelation::B1AfterB2E => self.nodes[s].base_id,
                        _ => id_arena.intern(base),
                    }
                } else {
                    id_arena.intern(base)
                };
 
                self.node_set_base_id(idx, base_id);
                self.register_base_offsets(base_id, lo, len);
                dot_index.on_block_inserted(site, block_idx, lo, hi, idx);
                self.sl_insert_after(pred, idx);
                base_id
            }
 
            // ── New block lands inside an existing block → split ──
            IdOrderingRelation::B1InsideB2 => {
                let target = succ.expect("B1InsideB2 requires a successor");
                let target_base   = self.nodes[target].base_id;
                let target_offset = self.nodes[target].offset;
                let target_size   = self.nodes[target].size as u32;
                let target_creator = self.nodes[target].creator;
                let target_block_idx = self.nodes[target].block_idx;
 
                // Determine the split point within the target
                let target_slice = id_arena.get_slice_unchecked(target_base);
                let sp = id_arena.find_split_point(
                    target_slice, target_offset, target_offset + target_size,
                    base,
                );
 
                // Split content
                let content = std::mem::take(&mut self.nodes[target].content);
                let byte_idx = content.char_indices()
                    .nth(sp as usize)
                    .map(|(i, _)| i)
                    .unwrap_or(content.len());
                let left_content  = content[..byte_idx].to_string();
                let right_content = content[byte_idx..].to_string();
 
                let right_size = self.nodes[target].size - sp as usize;
 
                // Shrink target to left half
                self.nodes[target].content = left_content;
                self.nodes[target].size = sp as usize;
                self.update_widths_for_size_change(target, -(right_size as isize));
 
                // Create right-half node
                let right_node = Node::new(
                    right_content, target_base,
                    target_offset + sp, target_creator, target_block_idx,
                );
                let right_idx = self.alloca(right_node);
 
                // Intern and set base for the new node
                let base_id = id_arena.intern(base);
                self.node_set_base_id(idx, base_id);
                self.register_base_offsets(base_id, lo, len);
 
                // DotIndex updates
                dot_index.on_block_split(
                    target_creator, target_block_idx,
                    target_offset, target_offset + sp, right_idx,
                );
                dot_index.on_block_inserted(site, block_idx, lo, hi, idx);
 
                // Insert: target → idx → right_idx
                self.sl_insert_after(target, idx);
                self.sl_insert_after(idx, right_idx);
 
                base_id
            }
 
            // ── Existing block is inside the new block → split new, recurse ──
            IdOrderingRelation::B2InsideB1 => {
                let target = succ.expect("B2InsideB1 requires a successor");
                let target_base = self.nodes[target].base_id;
                let target_slice = id_arena.get_slice_unchecked(target_base);
 
                let sp = id_arena.find_split_point(base, lo, hi, target_slice);
 
                let content = std::mem::take(&mut self.nodes[idx].content);
                let byte_idx = content.char_indices()
                    .nth(sp as usize)
                    .map(|(i, _)| i)
                    .unwrap_or(content.len());
                let left_content  = content[..byte_idx].to_string();
                let right_content = content[byte_idx..].to_string();
 
                // Recursive insert of both halves
                let id1 = self.insert_by_id(
                    site, id_arena, dot_index,
                    base, lo, block_idx, left_content, None,
                );
                let _id2 = self.insert_by_id(
                    site, id_arena, dot_index,
                    base, lo + sp, block_idx, right_content, None,
                );
 
                self.free(idx); // original pre-alloc no longer needed
                id1
            }
 
            // ── Successor can be extended (concat) ──
            IdOrderingRelation::B2ConcatB1 => {
                let target = succ.unwrap_or(pred);
                // The new block's identifiers immediately follow target's.
                // Check if we can extend target in-place.
                let target_base = self.nodes[target].base_id;
 
                // Check if same creator + block and contiguous
                if self.nodes[target].creator == site
                    && self.nodes[target].block_idx == block_idx
                {
                    // Check room via num_insertable against the next node
                    let can_extend = match self.nodes[target].levels[0].next {
                        Some(nxt) => {
                            let r_base   = self.node_base_id(nxt);
                            let r_offset = self.node_ranges(nxt).0;
                            id_arena.num_insertable(target_base, lo, r_base, r_offset, len) >= len
                        }
                        None => true,
                    };
 
                    if can_extend {
                        let content = std::mem::take(&mut self.nodes[idx].content);
                        self.nodes[target].content.push_str(&content);
                        self.nodes[target].size += len as usize;
                        self.update_widths_for_size_change(target, len as isize);
                        dot_index.on_block_extended(
                            site, self.nodes[target].block_idx,
                            self.nodes[target].offset,
                            self.nodes[target].offset + self.nodes[target].size as u32,
                        );
                        self.free(idx);
                        return target_base;
                    }
                }
 
                // Can't extend — insert as a new node after pred
                let base_id = target_base; // same base
                self.node_set_base_id(idx, base_id);
                self.register_base_offsets(base_id, lo, len);
                dot_index.on_block_inserted(site, block_idx, lo, hi, idx);
                self.sl_insert_after(pred, idx);
                base_id
            }
 
            // ── Exact duplicate (idempotent) ──
            IdOrderingRelation::B1EqualsB2 => {
                let base_id = if let Some(s) = succ {
                    self.nodes[s].base_id
                } else {
                    id_arena.intern(base)
                };
                self.free(idx); // duplicate, discard
                base_id
            }
        }
    }
}
 
// ── Exact-id lookup (for DotIndex bypass / legacy) ─────────────────────────
 
impl Tree {
    pub fn find_by_id_exact(
        &self, id_arena: &IdArena, base: &[u32], offset: u32,
    ) -> Option<usize> {
        let mut curr = HEAD;
 
        for level in (0..self.max_level).rev() {
            while let Some(nxt) = self.nodes[curr].levels[level].next {
                let n = &self.nodes[nxt];
                let rel = id_arena.compare_intervals_first_raw(
                    base, offset, offset + 1,
                    n.base_id, n.offset, n.offset + n.size as u32,
                );
                match rel {
                    IdOrderingRelation::B1AfterB2 | IdOrderingRelation::B2ConcatB1 => {
                        curr = nxt;
                    }
                    IdOrderingRelation::B1InsideB2 | IdOrderingRelation::B1EqualsB2 => {
                        let curr_slice = id_arena.get_slice_unchecked(n.base_id);
                        return if curr_slice == base { Some(nxt) } else { None };
                    }
                    _ => break,
                }
            }
        }
 
        None
    }
}
 
// ── Iteration & diagnostics ───────────────────────────────────────────────
 
pub struct InOrderIter<'a> {
    tree: &'a Tree,
    current: Option<usize>,
}
 
impl<'a> InOrderIter<'a> {
    fn new(tree: &'a Tree) -> Self {
        InOrderIter {
            current: tree.nodes[HEAD].levels[0].next,
            tree,
        }
    }
}
 
impl<'a> Iterator for InOrderIter<'a> {
    type Item = &'a Node;
    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.current?;
        let node = &self.tree.nodes[idx];
        self.current = node.levels[0].next;
        Some(node)
    }
}
 
impl Tree {
    pub fn inorder_iter(&self) -> InOrderIter<'_> {
        InOrderIter::new(self)
    }
 
    pub fn read(&self) -> String {
        self.inorder_iter().map(|n| n.content.as_str()).collect()
    }
 
    pub fn print_tree(&self, id_arena: &IdArena) {
        println!("\n===== SKIP LIST =====");
        let mut curr = self.nodes[HEAD].levels[0].next;
        let mut pos = 0usize;
        while let Some(idx) = curr {
            let n = &self.nodes[idx];
            let base = id_arena.get_slice(n.base_id);
            let content = if n.content.len() > 20 {
                format!("{}...", &n.content[..20])
            } else {
                n.content.clone()
            };
            println!(
                "  [{:3}] h={} base={:?} off={} size={} creator={} | \"{}\"",
                idx, n.levels.len(), base, n.offset, n.size, n.creator, content,
            );
            // Print level widths
            for (lv, l) in n.levels.iter().enumerate() {
                println!(
                    "         L{}: next={:?} width={}",
                    lv, l.next, l.width,
                );
            }
            pos += n.size;
            curr = n.levels[0].next;
        }
        println!("  total_size={} (walked={})", self.total_size, pos);
        println!("======================\n");
    }
 
    /// Verify that all nodes are in sorted identifier order.
    pub fn check_tree(&self, id_arena: &IdArena) -> bool {
        let mut prev_id: Option<Identifier> = None;
        let mut prev_offsets: Option<(u32, u32)> = None;
 
        for node in self.inorder_iter() {
            let curr_id = node.base_id;
            let (lo, hi) = (node.offset, node.offset + node.size as u32);
            if let Some(prev) = prev_id {
                let cmp = id_arena.compare_refs(
                    curr_id, lo, prev, prev_offsets.unwrap().1 - 1,
                );
                if cmp != Ordering::Greater {
                    eprintln!(
                        "check_tree failed: {:?} [{},{}] not > {:?} [{},{}]",
                        curr_id, lo, hi, prev,
                        prev_offsets.unwrap().0, prev_offsets.unwrap().1,
                    );
                    return false;
                }
            }
            prev_id = Some(curr_id);
            prev_offsets = Some((lo, hi));
        }
        true
    }
 
    /// Verify skip list structural invariants:
    /// - Level-0 chain is consistent with prev pointers.
    /// - Higher-level forward pointers skip over the correct set of nodes.
    /// - Widths are consistent with node sizes.
    pub fn check_skiplist(&self) -> bool {
        // Check level-0 chain
        let mut curr = HEAD;
        let mut walked_size = 0usize;
        while let Some(nxt) = self.nodes[curr].levels[0].next {
            if self.nodes[nxt].prev != Some(curr) {
                eprintln!("check_skiplist: prev mismatch at node {}", nxt);
                return false;
            }
            if self.nodes[curr].levels[0].width != self.nodes[curr].size {
                eprintln!(
                    "check_skiplist: level-0 width mismatch at node {}: width={} size={}",
                    curr, self.nodes[curr].levels[0].width, self.nodes[curr].size,
                );
                return false;
            }
            walked_size += self.nodes[nxt].size;
            curr = nxt;
        }
        // Check last node's level-0 width
        if curr != HEAD && self.nodes[curr].levels[0].width != self.nodes[curr].size {
            eprintln!("check_skiplist: last node level-0 width mismatch");
            return false;
        }
 
        if walked_size != self.total_size {
            eprintln!(
                "check_skiplist: total_size mismatch: walked={} stored={}",
                walked_size, self.total_size,
            );
            return false;
        }
 
        // Check higher-level widths
        for level in 1..self.max_level {
            let mut curr = HEAD;
            while let Some(nxt) = self.nodes[curr].levels[level].next {
                // Sum level-0 sizes from curr to nxt
                let mut sum = self.nodes[curr].size;
                let mut walk = self.nodes[curr].levels[0].next;
                while let Some(w) = walk {
                    if w == nxt { break; }
                    sum += self.nodes[w].size;
                    walk = self.nodes[w].levels[0].next;
                }
                if sum != self.nodes[curr].levels[level].width {
                    eprintln!(
                        "check_skiplist: level-{} width mismatch at node {}: expected={} got={}",
                        level, curr, sum, self.nodes[curr].levels[level].width,
                    );
                    return false;
                }
                curr = nxt;
            }
        }
 
        true
    }
}
