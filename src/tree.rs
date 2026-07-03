use core::panic;
use std::cmp::Ordering;
use ahash::AHashMap as HashMap;
use crate::dotindex::DotIndex;
use crate::dotstore::Dot;
use crate::node::Node;
use crate::idarena::{Identifier, IdOrderingRelation, IdArena};
use smallvec::SmallVec;

pub type Path = SmallVec<[usize; 32]>;

#[derive(Clone, Debug)]
pub struct Tree {
    pub nodes: Vec<Node>, 
    pub root: Option<usize>,
    free_list: Vec<usize>,
    base_to_offsets: HashMap<Identifier, (u32, u32)>
}

pub enum DelLocation {
    Start, 
    End
}

/* Basic helper functions */
impl Tree { 
    pub fn new() -> Self {
        Tree {
            root: None, 
            nodes: Vec::new(),
            free_list: Vec::new(),
            base_to_offsets: HashMap::new()
        }
    }

    pub fn clear(&mut self) {
        self.root = None;
        self.nodes.clear();
        self.free_list.clear();
        self.base_to_offsets.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    pub fn tree_size(&self) -> usize {
        if let Some(r) = self.root {
            self.nodes[r].subtree_count
        } else {
            0
        } 
    }

    fn alloca(&mut self, node: Node) -> usize {
        match self.free_list.pop() {
            Some(idx) => { self.nodes[idx] = node; idx },
            None => { self.nodes.push(node); self.nodes.len() - 1 }
        }
    }

    fn free(&mut self, idx: usize) {
        self.free_list.push(idx);
    }

    #[inline(always)]
    fn node_height(&self, node: Option<usize>) -> i32 { 
        node.map_or(0, |index| self.nodes[index].height)
    }

    #[inline(always)]
    fn node_subtree_count(&self, node: Option<usize>) -> usize { 
        node.map_or(0, |index| self.nodes[index].subtree_count)
    }

    pub fn node_content(&self, node: Option<usize>) -> &str { 
        node.map_or("", |index| &self.nodes[index].content)
    }

    #[inline(always)]
    pub fn node_size(&self, node: Option<usize>) -> usize { 
        node.map_or(0, |index| self.nodes[index].size)
    }

    #[inline(always)]
    pub fn node_left_count(&self, node: Option<usize>) -> usize { 
        if node.is_none() { return 0; }
        let left= self.nodes[node.unwrap()].left;
        left.map_or(0, |index| self.nodes[index].subtree_count)
    }

    pub fn node_creator(&self, node: usize) -> u32 { 
        self.nodes[node].creator
    }

    pub fn node_base_id(&self, node: usize) -> Identifier { 
        self.nodes[node].base_id
    }

    pub fn node_ranges(&self, node: usize) -> (u32, u32) { 
        let n = &self.nodes[node];
        let range_left= n.offset;
        let range_right= n.offset + n.size as u32;
        (range_left, range_right)
    }

    pub fn node_base_offsets(&self, node: usize) -> (u32, u32) { 
        // Get the offsets from the map
        let base_id = self.nodes[node].base_id;
        if let Some((lo, hi)) = self.base_to_offsets.get(&base_id) {
            return (*lo, *hi)
        } else {
            panic!("Base offsets not found for node {}, this should not happen", node);
        }
    }

    pub fn base_id_max_offset(&self, id: Identifier) -> Option<u32> {
        self.base_to_offsets.get(&id).map(|(_, hi)| *hi)
    }

    pub fn node_set_base_id(&mut self, node: usize, new_base: Identifier) {
        self.nodes[node].base_id = new_base;
    }

    pub fn node_block_idx(&self, node: usize) -> u32 {
        self.nodes[node].block_idx
    }

    pub fn extend_content(&mut self, dot_index: &mut DotIndex, node_idx: usize, text: &str) {
        let node = &mut self.nodes[node_idx];
        node.content.push_str(text);
        let added_size = text.len();
        node.size += added_size;
        // update the offsets of the base 
        let base_id = node.base_id;
        if let Some((lo, hi)) = self.base_to_offsets.get(&base_id) {
            let new_hi = hi + added_size as u32;
            self.base_to_offsets.insert(base_id, (*lo, new_hi));
        } 
        dot_index.on_block_extended(node.creator, node.block_idx, node.offset, node.offset + node.size as u32);
        self.propagate_size_delta(node_idx, added_size as isize);
    }

    pub fn truncate_content(&mut self, dot_index: &mut DotIndex, node_idx: usize, num_delete: usize, location: DelLocation) {
        let creator = self.nodes[node_idx].creator;
        let old_offset = self.nodes[node_idx].offset;
        let is_start = matches!(&location, DelLocation::Start);

        {
            let n = &mut self.nodes[node_idx];
            debug_assert!(n.content.is_ascii(), "non-ASCII content in truncate");
            match location {
                DelLocation::Start => {
                    let kept = n.content.split_off(num_delete);
                    n.content = kept;
                    n.offset += num_delete as u32;
                }
                DelLocation::End => {
                    n.content.truncate(n.size - num_delete);
                }
            }
            n.size -= num_delete;
        }

        if is_start {
            dot_index.on_block_truncated_start(creator, self.nodes[node_idx].block_idx, old_offset, old_offset + num_delete as u32);
        } else {
            let new_hi = self.nodes[node_idx].offset + self.nodes[node_idx].size as u32;
            dot_index.on_block_truncated_end(creator, self.nodes[node_idx].block_idx, old_offset, new_hi);
        }
        self.propagate_size_delta(node_idx, -(num_delete as isize));
    }
}

/* Linked List Operations */
impl Tree {
    #[inline]
    fn list_link_after(&mut self, existing: usize, new_idx: usize) {
        let old_next = self.nodes[existing].list_next;
        self.nodes[new_idx].list_prev = Some(existing);
        self.nodes[new_idx].list_next = old_next;
        self.nodes[existing].list_next = Some(new_idx);
        if let Some(n) = old_next {
            self.nodes[n].list_prev = Some(new_idx);
        }
    }

    #[inline]
    fn list_link_before(&mut self, existing: usize, new_idx: usize) {
        let old_prev = self.nodes[existing].list_prev;
        self.nodes[new_idx].list_next = Some(existing);
        self.nodes[new_idx].list_prev = old_prev;
        self.nodes[existing].list_prev = Some(new_idx);
        if let Some(p) = old_prev {
            self.nodes[p].list_next = Some(new_idx);
        }
    }

    #[inline]
    fn list_unlink(&mut self, idx: usize) {
        let p = self.nodes[idx].list_prev;
        let n = self.nodes[idx].list_next;
        if let Some(pi) = p { self.nodes[pi].list_next = n; }
        if let Some(ni) = n { self.nodes[ni].list_prev = p; }
        self.nodes[idx].list_prev = None;
        self.nodes[idx].list_next = None;
    }
}

/* Rotation and Rebalancing Functions */
impl Tree {    
    #[inline(always)]
    fn update_node(&mut self, idx: usize) {
        let left = self.nodes[idx].left;
        let right = self.nodes[idx].right;
        let lh = self.node_height(left);
        let rh = self.node_height(right);
        let lc = self.node_subtree_count(left);
        let rc = self.node_subtree_count(right);
        let node = &mut self.nodes[idx];
        node.height = 1 + lh.max(rh);
        node.subtree_count = node.size + lc + rc;
    }

    #[inline(always)]
    fn propagate_size_delta(&mut self, mut node: usize, delta: isize) {
        loop {
            let n = &mut self.nodes[node];
            n.subtree_count = (n.subtree_count as isize + delta) as usize;
            match n.parent {
                Some(p) => node = p,
                None => break,
            }
        }
    }

    fn balance_factor(&self, node: usize) -> i32 { 
        let n = &self.nodes[node];
        self.node_height(n.right) - self.node_height(n.left)
    }

    fn rotate_right(&mut self, y: usize) -> usize { 
        let x = self.nodes[y].left.expect("rotate_right: No left child");
        let b  = self.nodes[x].right;
        self.nodes[x].right = Some(y);
        self.nodes[y].left = b;
        self.nodes[x].parent = self.nodes[y].parent;
        self.nodes[y].parent = Some(x);
        if let Some(b_idx) = b {
            self.nodes[b_idx].parent = Some(y);
        }
        self.update_node(y);
        self.update_node(x);
        x
    }

    fn rotate_left(&mut self, x: usize) -> usize {
        let y = self.nodes[x].right.expect("rotate_left: No right child");
        let b = self.nodes[y].left;
        self.nodes[y].left = Some(x);
        self.nodes[x].right = b;
        self.nodes[y].parent = self.nodes[x].parent;
        self.nodes[x].parent = Some(y);
        if let Some(b_idx) = b {
            self.nodes[b_idx].parent = Some(x);
        }
        self.update_node(x);
        self.update_node(y);
        y
    }

    /* Avl Fix Function */
    fn avl_fix(&mut self, idx: usize) -> usize {
        self.update_node(idx);
        let bf = self.balance_factor(idx);

        if bf > 1 { 
            // Right heavy 
            let r = self.nodes[idx].right.unwrap();
            if self.balance_factor(r) < 0 {
                let rn = self.rotate_right(r);
                self.nodes[idx].right = Some(rn);
                // self.nodes[rn].parent = Some(idx);
            }
            self.rotate_left(idx)
        } else if bf < -1 {
            // left heavy 
            let l = self.nodes[idx].left.unwrap();
            if self.balance_factor(l) > 0 {
                let ln = self.rotate_left(l);
                self.nodes[idx].left = Some(ln);
                // self.nodes[ln].parent = Some(idx);
            }
            self.rotate_right(idx)
        } else { 
            idx
        }
    }

    fn rebalance(&mut self, node: Option<usize>, size_delta: Option<isize>) {
        let mut curr = match node {
            Some(idx) => idx,
            None => return,
        };

        loop {
            let par = self.nodes[curr].parent;
            let old_h = self.nodes[curr].height;
            let fixed = self.avl_fix(curr);

            // Update parent's child pointer if rotation changed the subtree root
            if let Some(parent) = self.nodes[fixed].parent {
                if self.nodes[parent].left == Some(curr) {
                    self.nodes[parent].left = Some(fixed);
                } else {
                    self.nodes[parent].right = Some(fixed);
                }
            } else {
                self.root = Some(fixed);
            }

            // No rotation + height unchanged
            if fixed == curr && self.nodes[curr].height == old_h {
            match (par, size_delta) {
                (Some(p), Some(delta)) => self.propagate_size_delta(p, delta),
                (Some(p), None) => {
                    let mut walk = Some(p);
                    while let Some(pi) = walk {
                        self.update_node(pi);
                        walk = self.nodes[pi].parent;
                    }
                }
                _ => {}
            }
                return;
            }

            match par {
                Some(p) => curr = p,
                None => return,
            }
        }
    }

}

/* Inorder Predecessor and Successor Functions */
// Replace next() and prev()
impl Tree {
    #[inline(always)]
    pub fn next(&self, node: usize) -> Option<usize> {
        self.nodes[node].list_next
    }

    #[inline(always)]
    pub fn prev(&self, node: usize) -> Option<usize> {
        self.nodes[node].list_prev
    }

    /// Leftmost node in the tree (list head).
    fn leftmost(&self) -> Option<usize> {
        let mut curr = self.root?;
        while let Some(l) = self.nodes[curr].left {
            curr = l;
        }
        Some(curr)
    }
}

impl Tree {
    pub fn find_by_pos(&self, pos: usize) -> (Option<usize>, usize) {
        // let mut path_to_root = Path::new(); 
        let nodes = &self.nodes;
        let mut i = self.root;
        let mut curr = pos;
        let mut covered: usize = 0;
        while let Some(index) = i { 
            let node = &nodes[index];
            // path_to_root.push(index);
            let left = node.left;
            let left_count = if let Some(left_index) = left {
                nodes[left_index].subtree_count
            } else {
                0
            };
            if curr < left_count {
                i = left;
            } else if curr <= left_count + node.size {
                covered += left_count;
                return (Some(index), covered);
            } else { 
                curr -= left_count + node.size;
                covered += left_count + node.size;
                i = node.right;
            }
        }
        (None, covered)
    }

    pub fn find_by_pos_delete(&self, pos: usize) -> (Option<usize>, usize) {
        // let mut path_to_root = Path::new();
        let nodes = &self.nodes;
        let mut i = self.root;
        let mut curr = pos;
        let mut covered: usize = 0;
        let mut last = None;
        while let Some(index) = i {
            last = Some(index);
            let node = &nodes[index];
            // path_to_root.push(index);
            let left = node.left;
            let left_count = if let Some(left_index) = left {
                nodes[left_index].subtree_count
            } else {
                0
            };
            if curr < left_count {
                i = left;
            } else if curr < left_count + node.size {
                // Deletion pos lands on a character *within* this node.
                // Unlike insertion, we use strict `<` because there is no
                // valid deletion position at the right edge of the node
                // (that would be the first character of the right subtree).
                covered += left_count;
                return (Some(index), covered);
            } else {
                curr -= left_count + node.size;
                covered += left_count + node.size;
                i = node.right;
            }
        }
        (last, covered)
    }

    /// Insert the node by identifier  
    /// Return the interned identifier
    pub fn insert_by_id(&mut self, 
        site: u32, 
        id_arena: &mut IdArena, 
        dot_index: &mut DotIndex, 
        base: &[u32], 
        offset: u32, 
        block_idx: u32, 
        content: String, 
        origin_left: Option<Dot>
    ) -> Identifier {
        let len = content.len() as u32;

        // Try finger-based insertion first
        if let Some(origin) = origin_left {
            if let Some(finger_node) = dot_index.lookup(origin.site, origin.b_idx, origin.seq) {
                let base_id = self.insert_by_finger(
                    site, id_arena, dot_index,
                    finger_node, origin.seq,
                    base, offset, block_idx, content,
                );
                if let Some((lo, hi)) = self.base_to_offsets.get(&base_id) {
                    let new_hi = std::cmp::max(*hi, offset + len);
                    self.base_to_offsets.insert(base_id, (*lo, new_hi));
                } else {
                    self.base_to_offsets.insert(base_id, (offset, offset + len));
                }
                return base_id;
            }
        }

        let idx = self.alloca(Node::new(content, Identifier::EMPTY, offset, site, block_idx));
        if self.is_empty() {
            let base_id = id_arena.intern(base);
            self.node_set_base_id(idx, base_id);
            self.root = Some(idx);
            self.base_to_offsets.insert(base_id, (offset, offset + len));
            dot_index.on_block_inserted(site, block_idx, offset, offset + len, idx);
            return base_id;
        }
        let from = self.root.unwrap();
        let base_id = self.insert_rec(id_arena, dot_index, idx, base, offset, offset + len, from, len, site, block_idx);
        if let Some((lo, hi)) = self.base_to_offsets.get(&base_id) {
            let new_hi = std::cmp::max(*hi, offset + len);
            self.base_to_offsets.insert(base_id.clone(), (*lo, new_hi));
        } else {
            self.base_to_offsets.insert(base_id.clone(), (offset, offset + len));
        }
        base_id
    }

    pub fn insert_rec(&mut self, id_arena: &mut IdArena, dot_index: &mut DotIndex, node: usize, node_base: &[u32], node_lo: u32, node_hi: u32, mut from: usize, len: u32, site: u32, block_idx: u32) -> Identifier {
        // let mut path = Path::new();
        let mut con = true;
        let mut rec = false;
        let mut inserted_id = Identifier::EMPTY;
        let mut last = None;

        while con {
            last = Some(from);
            let relation = {
                let n = &self.nodes[from];
                id_arena.compare_intervals_first_raw(node_base, node_lo, node_hi, n.base_id, n.offset, n.offset + n.size as u32)
            };

            match relation {
                IdOrderingRelation::B1AfterB2E => {
                    let from_node = &mut self.nodes[from];
                    inserted_id = from_node.base_id;
                    if let Some(r) = from_node.right {
                        from = r;
                        continue;
                    } else {
                        from_node.right = Some(node);
                        self.nodes[node].parent = Some(from);
                        self.node_set_base_id(node, inserted_id);
                        self.list_link_after(from, node);
                        dot_index.on_block_inserted(site, block_idx, node_lo, node_hi, node);
                        con = false;
                    }
                }
                IdOrderingRelation::B1AfterB2 => {
                    let from_node = &mut self.nodes[from];
                    if let Some(r) = from_node.right {
                        from = r;
                    } else {
                        from_node.right = Some(node);
                        self.nodes[node].parent = Some(from);
                        // Intern the identifier if not already done 
                        if inserted_id == Identifier::EMPTY {
                            let base_id = id_arena.intern(node_base);
                            self.node_set_base_id(node, base_id);
                            inserted_id = base_id;
                        } else {
                            self.node_set_base_id(node, inserted_id);
                        }
                        self.list_link_after(from, node);
                        dot_index.on_block_inserted(site, block_idx, node_lo, node_hi, node);
                        con = false;
                    }
                },
                IdOrderingRelation::B1BeforeB2 => {
                    let from_node = &mut self.nodes[from];
                    if let Some(l) = from_node.left {
                        from = l;
                    } else {
                        from_node.left = Some(node);
                        self.nodes[node].parent = Some(from);
                        dot_index.on_block_inserted(site, block_idx, node_lo, node_hi, node);
                        // Intern the identifier
                        if inserted_id == Identifier::EMPTY {
                            let base_id = id_arena.intern(node_base);
                            self.node_set_base_id(node, base_id);
                            inserted_id = base_id;
                        } else {
                            self.node_set_base_id(node, inserted_id);
                        }
                        self.list_link_before(from, node); 
                        con = false;
                    }
                },
                IdOrderingRelation::B1BeforeB2E => {
                    let from_node = &mut self.nodes[from];
                    inserted_id = from_node.base_id;
                    if let Some(l) = from_node.left {
                        from = l;
                    } else {
                        from_node.left = Some(node);
                        self.nodes[node].parent = Some(from);
                        dot_index.on_block_inserted(site, block_idx, node_lo, node_hi, node);
                        self.node_set_base_id(node, inserted_id);
                        self.list_link_before(from, node); 
                        con = false;
                    }
                },
                IdOrderingRelation::B1InsideB2 => {
                    let (sp, from_base_id, from_offset, from_creator, mut from_content, from_block_idx) = {
                        let from_node = &self.nodes[from];
                        let f_offset = from_node.offset;
                        let from_slice = id_arena.get_slice_unchecked(from_node.base_id);
                        let sp = id_arena.find_split_point(from_slice, f_offset, f_offset + from_node.size as u32, node_base);
                        let from_node = &mut self.nodes[from];
                        let from_content = std::mem::take(&mut from_node.content);
                        (sp, &from_node.base_id, from_node.offset, from_node.creator, from_content, from_node.block_idx)
                    };

                    let rcontent = from_content.split_off(sp as usize);
                    let right_node = Node::new(rcontent, from_base_id.clone(), from_offset + sp, from_creator, from_block_idx);
                    let right_idx = self.alloca(right_node);

                    let original_right = self.nodes[from].right;

                    let from_node = &mut self.nodes[from];
                    from_node.content = from_content;
                    from_node.size = from_node.content.len();

                    // Intern identifier for the new node
                    if inserted_id == Identifier::EMPTY {
                        let base_id = id_arena.intern(node_base);
                        self.node_set_base_id(node, base_id);
                        inserted_id = base_id;
                    } else {
                        self.node_set_base_id(node, inserted_id);
                    }

                    self.list_link_after(from, right_idx);
                    self.list_link_after(from, node);
                    
                    let joined = self.join(Some(node), right_idx, original_right);
                    self.nodes[joined].parent = Some(from);
                    self.nodes[from].right = Some(joined);
                    
                    // Update dot index 
                    let from_block_idx = self.nodes[from].block_idx;
                    dot_index.on_block_split(from_creator, from_block_idx, from_offset, from_offset+sp, right_idx);
                    dot_index.on_block_inserted(site, block_idx, node_lo, node_hi, node);
                    
                    con = false;
                },
                IdOrderingRelation::B2ConcatB1 => {
                    // We know that the base identifier of the node is same as base of the from node 
                    let b2_base = {
                        self.nodes[from].base_id
                    };
                    let node_base_id = b2_base;
                    inserted_id = node_base_id;
                    if let Some((_, hi)) = self.base_to_offsets.get(&b2_base) {
                        if node_lo < *hi {
                            let from_node = &mut self.nodes[from];
                            if let Some(r) = from_node.right {
                                from = r;
                                continue;
                            } else {
                                from_node.right = Some(node);
                                self.nodes[node].parent = Some(from);
                                self.node_set_base_id(node, node_base_id);
                                dot_index.on_block_inserted(site, block_idx, node_lo, node_hi, node);
                                self.list_link_after(from, node); 
                                break;
                            }
                        }
                    }
                    if self.nodes[from].creator != site || self.nodes[from].block_idx != block_idx {
                        let from_node = &mut self.nodes[from];
                        if let Some(r) = from_node.right {
                            from = r;
                            continue;
                        } else {
                            from_node.right = Some(node);
                            self.nodes[node].parent = Some(from);
                            self.node_set_base_id(node, node_base_id);
                            dot_index.on_block_inserted(site, block_idx, node_lo, node_hi, node);
                            self.list_link_after(from, node);
                            break;
                        }
                    }
                    let nxt = self.next(from);
                    if let Some(r) = nxt {
                        let r_base = self.node_base_id(r);
                        let r_offset = self.node_ranges(r).0;
                        // let id_insert = IdentifierRef::new(node_idi.base, node_idi.lo);
                        // let id_next = IdentifierRef::new(r_base, r_offset);
                        let n_insertable = id_arena.num_insertable(node_base_id, node_lo, r_base, r_offset, len);
                        // let n_insertable = id_arena.num_insertable(id_insert, id_next, len);
                        if n_insertable < len {
                            from = self.nodes[from].right.unwrap();
                        } else {
                            // take content first, then borrow from_node
                            let content = std::mem::take(&mut self.nodes[node].content);
                            let from_node = &mut self.nodes[from];
                            from_node.content.push_str(&content);
                            from_node.size += len as usize;
                            // Update dot_index
                            dot_index.on_block_extended(site, from_node.block_idx, from_node.offset, from_node.offset + from_node.size as u32);
                            self.free(node);
                            con = false;
                        }
                    } else {
                        let content = std::mem::take(&mut self.nodes[node].content);
                        let from_node = &mut self.nodes[from];
                        from_node.content.push_str(&content);
                        from_node.size = from_node.content.len();
                        // Update dot_index
                        dot_index.on_block_extended(site, from_node.block_idx, from_node.offset, from_node.offset + from_node.size as u32);
                        self.free(node);
                        con = false;
                    }
                },

                IdOrderingRelation::B1EqualsB2 => {
                    inserted_id = {
                        let from_node = &self.nodes[from];
                        from_node.base_id
                    };
                    con = false;
                }
                IdOrderingRelation::B2InsideB1 => {
                    let sp = {
                        let b2_base = self.nodes[from].base_id;
                        let b2_slice = id_arena.get_slice_unchecked(b2_base);
                        id_arena.find_split_point(node_base, node_lo, node_hi, b2_slice)
                    };
                    let content = std::mem::take(&mut self.nodes[node].content);
                    let left_content  = content[..sp as usize].to_string();
                    let right_content = content[sp as usize..].to_string();
                    
                    // FIXME!!
                    let id1 = self.insert_by_id(site, id_arena, dot_index, node_base, node_lo, block_idx, left_content, None);
                    let id2 = self.insert_by_id(site, id_arena, dot_index, node_base, node_lo + sp, block_idx, right_content, None);

                    // id1 and id2 should be equal 
                    debug_assert_eq!(id1, id2);
                    inserted_id = id1;

                    self.free(node);

                    con = false;
                    rec = true;
                },
                IdOrderingRelation::B1ConcatB2 => {
                    let from_node = &mut self.nodes[from];
                    if let Some(l) = from_node.left {
                        from = l;
                    } else {
                        from_node.left = Some(node);
                        self.nodes[node].parent = Some(from);
                        // Intern the identifier
                        if inserted_id == Identifier::EMPTY {
                            let base_id = id_arena.intern(node_base);
                            self.node_set_base_id(node, base_id);
                            inserted_id = base_id;
                        } else {
                            self.node_set_base_id(node, inserted_id);
                        }
                        self.list_link_before(from, node);
                        dot_index.on_block_inserted(site, block_idx, node_lo, node_hi, node);
                        con = false;
                    }
                },
            }
        }
        if !rec {
            self.rebalance(last, Some(len as isize));
        }
        return inserted_id;
    }

    pub fn insert_by_finger(
        &mut self,
        site: u32,
        id_arena: &mut IdArena,
        dot_index: &mut DotIndex,
        finger: usize,
        finger_seq: u32,
        base: &[u32],
        offset: u32,
        block_idx: u32,
        content: String,
    ) -> Identifier {
        let len = content.len() as u32;
        let (f_lo, f_hi) = self.node_ranges(finger);
        debug_assert!(
            finger_seq >= f_lo && finger_seq < f_hi,
            "finger node does not contain origin seq"
        );

        // If origin points mid-fragment, split the finger first so that
        // the walk starts at the right boundary.
        let prev = if finger_seq < f_hi - 1 {
            let sp = (finger_seq + 1 - f_lo) as usize;
            let base_id = self.nodes[finger].base_id;
            let f_offset = self.nodes[finger].offset;
            let creator = self.nodes[finger].creator;
            let f_block_idx = self.nodes[finger].block_idx;
            let target_right = self.nodes[finger].right;

            let full_content = std::mem::take(&mut self.nodes[finger].content);
            let left_content = full_content[..sp].to_string();
            let right_content = full_content[sp..].to_string();

            self.nodes[finger].content = left_content;
            self.nodes[finger].size = sp;

            let right_node = Node::new(right_content, base_id, f_offset + sp as u32, creator, f_block_idx);
            let right_idx = self.alloca(right_node);

            dot_index.on_block_split(creator, f_block_idx, f_offset, f_offset + sp as u32, right_idx);
            self.list_link_after(finger, right_idx);

            let joined = self.join(None, right_idx, target_right);
            self.nodes[finger].right = Some(joined);
            self.nodes[joined].parent = Some(finger);

            self.rebalance(Some(finger), None);
            finger
        } else {
            finger
        };

        // ── Walk right comparing IDs ───────────────────────────────────
        let mut prev = prev;
        let mut resolved: Option<Identifier> = None;

        loop {
            let cand = match self.nodes[prev].list_next {
                Some(c) => c,
                None => break,
            };

            let rel = {
                let c = &self.nodes[cand];
                id_arena.compare_intervals_first_raw(
                    base, offset, offset + len,
                    c.base_id, c.offset, c.offset + c.size as u32,
                )
            };

            match rel {
                IdOrderingRelation::B1AfterB2 => {
                    prev = cand;
                }
                IdOrderingRelation::B1AfterB2E => {
                    resolved = Some(self.nodes[cand].base_id);
                    prev = cand;
                }
                IdOrderingRelation::B2ConcatB1 => {
                    resolved = Some(self.nodes[cand].base_id);
                    let same_block = self.nodes[cand].creator == site
                        && self.nodes[cand].block_idx == block_idx;
                    let overlaps_existing = self
                        .base_id_max_offset(self.nodes[cand].base_id)
                        .map_or(false, |hi| offset < hi);

                    if same_block && !overlaps_existing {
                        let fits = match self.nodes[cand].list_next {
                            None => true,
                            Some(nxt) => {
                                let n_base = self.nodes[nxt].base_id;
                                let n_lo = self.nodes[nxt].offset;
                                id_arena.num_insertable(
                                    self.nodes[cand].base_id, offset,
                                    n_base, n_lo, len,
                                ) >= len
                            }
                        };
                        if fits {
                            self.extend_content(dot_index, cand, &content);
                            return self.nodes[cand].base_id;
                        }
                    }
                    prev = cand;
                }
                IdOrderingRelation::B1BeforeB2
                | IdOrderingRelation::B1ConcatB2 => break,
                IdOrderingRelation::B1BeforeB2E => {
                    resolved = Some(self.nodes[cand].base_id);
                    break;
                }
                IdOrderingRelation::B1InsideB2 => {
                    let sp = {
                        let c = &self.nodes[cand];
                        let c_slice = id_arena.get_slice_unchecked(c.base_id);
                        id_arena.find_split_point(
                            c_slice, c.offset, c.offset + c.size as u32, base,
                        )
                    };
                    let base_id = resolved.unwrap_or_else(|| id_arena.intern(base));
                    let middle = Node::new(content, base_id, offset, site, block_idx);
                    self.split_and_insert_middle(dot_index, cand, sp as usize, middle);
                    return base_id;
                }
                IdOrderingRelation::B1EqualsB2 => {
                    return self.nodes[cand].base_id;
                }
                IdOrderingRelation::B2InsideB1 => {
                    return self.insert_by_id(
                        site, id_arena, dot_index, base, offset, block_idx, content, None,
                    );
                }
            }
        }

        // Insert between prev and prev.list_next
        let base_id = resolved.unwrap_or_else(|| id_arena.intern(base));
        let node = Node::new(content, base_id, offset, site, block_idx);
        self.insert_after(dot_index, prev, node);
        base_id
    }

    pub fn splice(&mut self, target: usize, replacement: Option<usize>) {
        self.list_unlink(target);
        let target_node = &self.nodes[target];
        let target_size = target_node.size;
        // let parent_idx = target_node.parent;

        if let Some(parent_idx) = target_node.parent {
            let parent = &mut self.nodes[parent_idx];
            if parent.left == Some(target) {
                parent.left = replacement;
            } else if parent.right == Some(target) {
                parent.right = replacement;
            } else { 
                panic!("splice: invalid path, target not a child of its parent");
            }

            // Also update the parent of the replacement node, if it exists
            if let Some(replacement_idx) = replacement {
                self.nodes[replacement_idx].parent = Some(parent_idx);
            }

            self.free(target);
            self.rebalance(Some(parent_idx), Some(-(target_size as isize)));
        } else {
            // Target is the root 
            self.root = replacement;
            if let Some(replacement_idx) = replacement {
                self.nodes[replacement_idx].parent = None;
            }
            self.free(target);
            return;
        }
        
    }

    pub fn find_by_id_exact(&mut self, id_arena: &IdArena, base: &[u32], offset: u32) -> Option<usize> {
        if self.is_empty() {
            return None;
        }
        let mut curr = self.root.unwrap();

        loop {
            // path.push(curr);
            let cmp = {
                let b1_base = base;
                let b1_lo = offset;
                let b1_hi = offset + 1;
                let curr_node = &self.nodes[curr];
                let b2_base = curr_node.base_id;
                let b2_lo = curr_node.offset;
                let b2_hi = b2_lo + curr_node.size as u32;
                id_arena.compare_intervals_first_raw(b1_base, b1_lo, b1_hi, b2_base, b2_lo, b2_hi)
            };

            match cmp {
                IdOrderingRelation::B1AfterB2 | IdOrderingRelation::B2ConcatB1 => {
                    if let Some(r) = self.nodes[curr].right {
                        curr = r;
                    } else {
                        // return Path::new();
                        return None;
                    }
                }
                IdOrderingRelation::B1BeforeB2 | IdOrderingRelation::B1ConcatB2 => {
                    if let Some(l) = self.nodes[curr].left {
                        curr = l;
                    } else {
                        // return Path::new();
                        return None;
                    }
                }
                IdOrderingRelation::B1EqualsB2 => {
                    // Exact interval match — still verify base
                    // println!("Probe matches node range exactly, checking base for exact match");
                    let curr_slice = id_arena.get_slice_unchecked(self.nodes[curr].base_id);
                    if curr_slice == base {
                        return Some(curr);
                    }
                    return None;
                    // return path;
                }
                IdOrderingRelation::B1InsideB2 => {
                    // println!("Probe inside node range, checking base for exact match");
                    // Probe falls inside this node's range.
                    // Only a real match if the base is identical.
                    // Cannot exist elsewhere in the tree, so return empty if base differs.
                    let curr_slice = id_arena.get_slice_unchecked(self.nodes[curr].base_id);
                    if curr_slice == base {
                        return Some(curr);
                    }
                    return None;
                }
                _ => panic!("Unexpected relation in find_by_id_exact"),
            }
        }
    }
}

impl Tree {
    #[inline(always)]
    fn register_base_offsets(&mut self, base: Identifier, offset: u32, size: u32) {
        if let Some((lo, hi)) = self.base_to_offsets.get(&base) {
            let new_lo = std::cmp::min(*lo, offset);
            let new_hi = std::cmp::max(*hi, offset + size);
            self.base_to_offsets.insert(base, (new_lo, new_hi));
        } else {
            self.base_to_offsets.insert(base, (offset, offset + size));
        }
    }

    pub fn insert_after(&mut self, dot_index: &mut DotIndex, target: usize, node: Node) -> usize {
        let new_idx = self.alloca(node);
        let n = &self.nodes[new_idx];
        let new_size = n.size as isize;
        dot_index.on_block_inserted(n.creator, n.block_idx, n.offset, n.offset + n.size as u32, new_idx);
        self.register_base_offsets(n.base_id, n.offset, n.size as u32);

        let last;

        if self.nodes[target].right.is_none() {
            self.nodes[target].right = Some(new_idx);
            self.nodes[new_idx].parent = Some(target);
            last = Some(target);
        } else {
            // Walk to the leftmost node in the right subtree.
            let mut curr = self.nodes[target].right.unwrap();
            while let Some(l) = self.nodes[curr].left {
                curr = l;
            }
            self.nodes[curr].left = Some(new_idx);
            self.nodes[new_idx].parent = Some(curr);
            last = Some(curr);
        }
        self.list_link_after(target, new_idx);
        self.rebalance(last, Some(new_size));
        new_idx
    }

    pub fn insert_before(&mut self, dot_index: &mut DotIndex, target: usize, node: Node) -> usize {
        let new_idx = self.alloca(node);
        let n = &self.nodes[new_idx];
        let new_size = n.size as isize;
        dot_index.on_block_inserted(n.creator, n.block_idx, n.offset, n.offset + n.size as u32, new_idx);
        self.register_base_offsets(n.base_id, n.offset, n.size as u32);

        let last;

        if self.nodes[target].left.is_none() {
            self.nodes[target].left = Some(new_idx);
            self.nodes[new_idx].parent = Some(target);
            last = Some(target);
        } else {
            // Walk to the rightmost node in the left subtree.
            let mut curr = self.nodes[target].left.unwrap();
            // extended_path.push(curr);
            while let Some(r) = self.nodes[curr].right {
                curr = r;
            }
            self.nodes[curr].right = Some(new_idx);
            self.nodes[new_idx].parent = Some(curr);
            last = Some(curr);
        }
        self.list_link_before(target, new_idx);
        self.rebalance(last, Some(new_size));
        new_idx
    }

    pub fn split_and_insert_middle(
        &mut self,
        dot_index: &mut DotIndex,
        target: usize,
        sp: usize,         
        middle: Node,
    ) -> usize {
        let base_id = self.nodes[target].base_id;
        let offset  = self.nodes[target].offset;
        let creator = self.nodes[target].creator;
        let target_right = self.nodes[target].right;
        let target_block_idx = self.nodes[target].block_idx;

        let content = std::mem::take(&mut self.nodes[target].content);
        let left_content  = content[..sp].to_string();
        let right_content = content[sp..].to_string();

        // middle_idx
        self.nodes[target].content = left_content;
        self.nodes[target].size = sp;

        let right_node = Node::new(right_content, base_id, offset + sp as u32, creator, target_block_idx);
        let right_idx = self.alloca(right_node);
        
        let middle_creator = middle.creator;    
        let middle_base   = middle.base_id;
        let middle_offset = middle.offset;
        let middle_size   = middle.size as u32;
        let middle_idx = self.alloca(middle);
        let middle_block_idx = self.nodes[middle_idx].block_idx;
        self.register_base_offsets(middle_base, middle_offset, middle_size);

        // Update dot index
        dot_index.on_block_split(creator, target_block_idx, offset, offset + sp as u32, right_idx);
        dot_index.on_block_inserted(middle_creator, middle_block_idx, middle_offset, middle_offset + middle_size as u32, middle_idx);

        self.list_link_after(target, right_idx);
        self.list_link_after(target, middle_idx);

        let joined = self.join(Some(middle_idx), right_idx, target_right);
        self.nodes[target].right = Some(joined);
        self.nodes[joined].parent = Some(target);

        self.rebalance(Some(target), Some(middle_size as isize));  // path ends at target, NOT at right_idx
        middle_idx
    }

    /// Delete the node at `path.last()` using the path directly.
    /// This is the same algorithm as delete_by_id's second half, but skips
    /// the find_by_id traversal since we already have the path.

    pub fn delete_target(&mut self, dot_index: &mut DotIndex, target: Option<usize>) {
        if target.is_none() {
            return;
        }
        let curr = target.unwrap();
        let left  = self.nodes[curr].left;
        let right = self.nodes[curr].right;
        let target_creator = self.nodes[curr].creator;
        let target_offset = self.nodes[curr].offset;
        let target_block_idx = self.nodes[curr].block_idx;

        match (left, right) {
            (None, None) => {
                self.splice(curr, None);
                dot_index.on_block_deleted(target_creator, target_block_idx, target_offset);
            }
            (Some(child), None) | (None, Some(child)) => {
                self.splice(curr, Some(child));
                dot_index.on_block_deleted(target_creator, target_block_idx, target_offset);
            }
            (Some(l), Some(r)) => {
                let succ = self.nodes[curr].list_next.unwrap();
                debug_assert_eq!(self.nodes[curr].list_next, Some(succ));
                self.list_unlink(curr);

                let succ_right = self.nodes[succ].right;
                let target_parent = self.nodes[curr].parent;
                let rebalance_from;

                if succ == r {
                    // Successor is the direct right child of target.
                    // Just give it target's left child.
                    self.nodes[succ].left = Some(l);
                    self.nodes[l].parent = Some(succ);
                    rebalance_from = succ;
                } else {
                    // Successor is deeper. Detach it from its parent.
                    let succ_parent = self.nodes[succ].parent.unwrap();
                    self.nodes[succ_parent].left = succ_right;
                    if let Some(sr) = succ_right {
                        self.nodes[sr].parent = Some(succ_parent);
                    }

                    // Wire successor into target's position
                    self.nodes[succ].left = Some(l);
                    self.nodes[succ].right = Some(r);
                    self.nodes[l].parent = Some(succ);
                    self.nodes[r].parent = Some(succ);
                    rebalance_from = succ_parent;
                }

                // Rewire target's parent to point to successor
                self.nodes[succ].parent = target_parent;
                if let Some(p) = target_parent {
                    if self.nodes[p].left == Some(curr) {
                        self.nodes[p].left = Some(succ);
                    } else {
                        self.nodes[p].right = Some(succ);
                    }
                } else {
                    self.root = Some(succ);
                }
                
                self.free(curr);
                dot_index.on_block_deleted(target_creator, target_block_idx, target_offset);
                self.rebalance(Some(rebalance_from), None);
            }
        }
    }

    // TODO: Check , possible correctness bug in left byte / right byte computation
    pub fn delete_middle_at_target(
        &mut self,
        dot_index: &mut DotIndex,
        target: usize,
        start: usize,
        count: usize,
    ) {
        let base_id = self.nodes[target].base_id;
        let offset  = self.nodes[target].offset;
        let creator = self.nodes[target].creator;
        let target_right = self.nodes[target].right;
        let target_size = self.nodes[target].size;
        let target_block_idx = self.nodes[target].block_idx;

        let content = std::mem::take(&mut self.nodes[target].content);
        let left_content  = content[..start].to_string();
        let right_content = content[start + count..].to_string();

        self.nodes[target].content = left_content;
        self.nodes[target].size = start;

        let right_node = Node::new(
            right_content, base_id, offset + (start + count) as u32, creator, target_block_idx
        );
        let right_idx = self.alloca(right_node);

        // In-order: target(left half), right_idx, original_right...
        // No middle element here — right_idx is the separator
        let joined = self.join(None, right_idx, target_right);
        self.nodes[target].right = Some(joined);
        self.nodes[joined].parent = Some(target);

        // Update dot index
        dot_index.on_block_middle_deleted(creator, target_block_idx, offset, offset+start as u32, 
            offset+ (start+count) as u32, offset + target_size as u32, right_idx);

        self.list_link_after(target, right_idx);

        self.rebalance(Some(target), Some(-(count as isize)));
    }

    pub fn insert_first(&mut self, dot_index: &mut DotIndex, node: Node) -> usize {
        let idx = self.alloca(node);
        let n = &self.nodes[idx];
        dot_index.on_block_inserted(n.creator, n.block_idx, n.offset, n.offset + n.size as u32, idx);
        self.register_base_offsets(n.base_id, n.offset, n.size as u32);
        self.root = Some(idx);
        idx
    }
}

// Replace InOrderIter — no stack needed anymore
pub struct InOrderIter<'a> {
    tree: &'a Tree,
    current: Option<usize>,
}

impl<'a> InOrderIter<'a> {
    pub fn new(tree: &'a Tree) -> Self {
        InOrderIter {
            tree,
            current: tree.leftmost(),
        }
    }
}

impl<'a> Iterator for InOrderIter<'a> {
    type Item = &'a Node;

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.current?;
        let node = &self.tree.nodes[idx];
        self.current = node.list_next;
        Some(node)
    }
}

impl Tree {
    pub fn print_tree(&self, id_arena: &IdArena) {
        println!("\n===== BLOCK TREE =====");
        match self.root {
            Some(root) => self.print_node(id_arena, root, "", true),
            None => println!("(empty)"),
        }
        println!("======================\n");
    }

    fn print_node(&self, id_arena: &IdArena, idx: usize, prefix: &str, is_last: bool) {
        let node = &self.nodes[idx];

        // formatting helpers
        let left = node.left.map_or("·".to_string(), |x| x.to_string());
        let right = node.right.map_or("·".to_string(), |x| x.to_string());

        let base = &node.base_id;
        let base = id_arena.get_slice_unchecked(*base);

        // trim content for readability
        let content = if node.content.len() > 10 {
            format!("{}...", &node.content[..10])
        } else {
            node.content.clone()
        };

        println!(
            "{}{}[{}] base={:?} off={} size={} cnt={} h={} | L:{} R:{} | \"{}\" | creator={}",
            prefix,
            if is_last { "└──" } else { "├──" },
            idx,
            base,
            node.offset,
            node.size,
            node.subtree_count,
            node.height,
            left,
            right,
            content,
            node.creator
        );

        let new_prefix = format!(
            "{}{}",
            prefix,
            if is_last { "    " } else { "│   " }
        );

        match (node.left, node.right) {
            (Some(l), Some(r)) => {
                self.print_node(id_arena, l, &new_prefix, false);
                self.print_node(id_arena, r, &new_prefix, true);
            }
            (Some(l), None) => {
                self.print_node(id_arena, l, &new_prefix, true);
            }
            (None, Some(r)) => {
                self.print_node(id_arena, r, &new_prefix, true);
            }
            (None, None) => {}
        }
    }

    pub fn inorder_iter(&self) -> InOrderIter<'_> {
        InOrderIter::new(self)
    }

    pub fn read(&self) -> String {
        let mut res = String::with_capacity(self.tree_size());
        let mut curr = self.leftmost();
        while let Some(idx) = curr {
            res.push_str(&self.nodes[idx].content);
            curr = self.nodes[idx].list_next;
        }
        res
    }

    /* Function to check whether all the keys in the tree are sorted or not */
    /// collect all the keys inorder and check if they are sorted
    pub fn check_tree(&self, id_arena: &IdArena) -> bool {
        let mut prev_id: Option<Identifier> = None;
        let mut prev_offsets: Option<(u32, u32)> = None;
        for node in self.inorder_iter() {
            let curr_id = node.base_id.clone();
            let (lo, hi) = (node.offset, node.offset + node.size as u32);
            if let Some(prev) = prev_id {
                // let curr_lo = IdentifierRef::new(curr_id, lo);
                // let prev_hi = IdentifierRef::new(prev, prev_offsets.unwrap().1-1);
                let cmp = id_arena.compare_refs(curr_id, lo, prev, prev_offsets.unwrap().1 - 1);
                if cmp != Ordering::Greater {
                    eprintln!("Tree check failed: current id {:?} with offsets {}-{} is not greater than previous id {:?} with offsets {}-{}", curr_id, lo, hi, prev, prev_offsets.unwrap().0, prev_offsets.unwrap().1);
                    return false;
                }
            }
            prev_id = Some(curr_id);
            prev_offsets = Some((lo, hi));
        }
        true
    }

    pub fn check_avl(&self) -> bool {
        fn check_avl_rec(tree: &Tree, idx: usize) -> Result<usize, String> {
            let node = &tree.nodes[idx];
            let left_height = if let Some(l) = node.left {
                check_avl_rec(tree, l)?
            } else {
                0
            };
            let right_height = if let Some(r) = node.right {
                check_avl_rec(tree, r)?
            } else {
                0
            };
            if (left_height as isize - right_height as isize).abs() > 1 {
                return Err(format!("AVL violation at node {}: left height {}, right height {}", idx, left_height, right_height));
            }
            Ok(1 + std::cmp::max(left_height, right_height))
        }

        if let Some(root) = self.root {
            match check_avl_rec(self, root) {
                Ok(_) => true,
                Err(e) => {
                    println!("{}", e);
                    false
                }
            }
        } else {
            true
        }
    }
}

impl Tree {
    /// AVL join: merge left subtree `l`, separator node `mid`, and right subtree `r`
    /// into a single balanced AVL tree.
    /// Precondition: all keys in l < mid.key < all keys in r.
    /// TODO: Check correctness of impact of parent pointer 
    pub fn join(&mut self, l: Option<usize>, mid: usize, r: Option<usize>) -> usize {
        let lh = self.node_height(l);
        let rh = self.node_height(r);

        if (lh - rh).abs() <= 1 {
            // Heights are close — mid becomes the root directly
            self.nodes[mid].left = l;
            self.nodes[mid].right = r;
            if let Some(li) = l {
                self.nodes[li].parent = Some(mid);
            }
            if let Some(ri) = r {
                self.nodes[ri].parent = Some(mid);
            }
            self.update_node(mid);
            return mid;
        }

        if lh > rh + 1 {
            // Left tree is taller: walk down its right spine
            let li = l.unwrap();
            let lr = self.nodes[li].right;
            let new_right = self.join(lr, mid, r);
            self.nodes[li].right = Some(new_right);
            self.nodes[new_right].parent = Some(li);
            self.avl_fix(li)
        } else {
            // Right tree is taller: walk down its left spine
            let ri = r.unwrap();
            let rl = self.nodes[ri].left;
            let new_left = self.join(l, mid, rl);
            self.nodes[ri].left = Some(new_left);
            self.nodes[new_left].parent = Some(ri);
            self.avl_fix(ri)
        }
    }
}