use core::panic;
use std::cmp::Ordering;
// use std::collections::HashMap;
use ahash::AHashMap as HashMap;
use crate::dotindex::DotIndex;
use crate::node::Node;
// use crate::identifier::{Id, IdOrderingRelation, Identifier, IdentifierInterval, IdentifierRef, compare_intervals, compare_intervals_raw, num_insertable};
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
        dot_index.on_block_extended(node.creator, node.offset, node.offset + node.size as u32);
        // update everything till root 
        let mut curr = Some(node_idx);
        while let Some(idx) = curr {
            self.update_node(idx);
            curr = self.nodes[idx].parent;
        }
    }

    pub fn truncate_content(&mut self, dot_index: &mut DotIndex, node_idx: usize, num_delete: usize, location: DelLocation) {
        let creator = self.nodes[node_idx].creator;
        let old_offset = self.nodes[node_idx].offset;
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
        // n borrow is dead — safe to access self.nodes and dot_index

        if is_start {
            dot_index.on_block_truncated_start(creator, old_offset, old_offset + num_delete as u32);
        } else {
            let new_hi = self.nodes[node_idx].offset + self.nodes[node_idx].size as u32;
            dot_index.on_block_truncated_end(creator, old_offset, new_hi);
        }

        let mut curr = Some(node_idx);
        while let Some(idx) = curr {
            self.update_node(idx);
            curr = self.nodes[idx].parent;
        }
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

    // fn rebalance(&mut self, node: Option<usize>) {
    //     let mut curr = node;
    //     while let Some(idx) = curr {
    //         let par = self.nodes[idx].parent;
    //         let fixed = self.avl_fix(idx);
    //         if let Some(parent) = self.nodes[fixed].parent {
    //             if self.nodes[parent].left == Some(idx) {
    //                 self.nodes[parent].left = Some(fixed);
    //             } else {
    //                 self.nodes[parent].right = Some(fixed);
    //             }
    //         } else {
    //             self.root = Some(fixed);
    //         }
    //         curr = par;
    //     }
    // }

    fn rebalance(&mut self, node: Option<usize>) {
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
                let mut p = par;
                while let Some(pi) = p {
                    self.update_node(pi);
                    p = self.nodes[pi].parent;
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
impl Tree {
    // Function to get inorder successor of a node
    pub fn next(&self, node: usize) -> Option<usize> {
        let nodes = &self.nodes;

        // Case 1: right subtree: leftmost node
        if let Some(mut r) = nodes[node].right {
            while let Some(l) = nodes[r].left {
                r = l;
            }
            return Some(r);
        }

        // Case 2: go up until we come from the left
        let mut curr = node;
        while let Some(parent) = nodes[curr].parent {
            if nodes[parent].left == Some(curr) {
                return Some(parent);
            }
            curr = parent; // Move up the tree
        }
        
        None
    }

    // Function to get inorder predecessor of a node
    pub fn prev(&self, node: usize) -> Option<usize> {
        let nodes = &self.nodes;

        // Case 1: left subtree: rightmost node
        if let Some(mut l) = nodes[node].left {
            while let Some(r) = nodes[l].right {
                l = r;
            }
            return Some(l);
        }

        // Case 2: go up until we come from the right
        let mut curr = node;
        while let Some(parent) = nodes[curr].parent {
            if nodes[parent].right == Some(curr) {
                return Some(parent);
            }
            curr = parent; // Move up the tree
        }
        
        None
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
    pub fn insert_by_id(&mut self, site: u32, id_arena: &mut IdArena, dot_index: &mut DotIndex, base: &[u32], offset: u32, content: String) -> Identifier {
        let len = content.len() as u32;
        let idx = self.alloca(Node::new(content, Identifier::EMPTY, offset, site));
        if self.is_empty() {
            let base_id = id_arena.intern(base);
            self.node_set_base_id(idx, base_id);
            self.root = Some(idx);
            self.base_to_offsets.insert(base_id, (offset, offset + len));
            dot_index.on_block_inserted(site, offset, offset + len, idx);
            return base_id;
        }
        let from = self.root.unwrap();
        let base_id = self.insert_rec(id_arena, dot_index, idx, base, offset, offset + len, from, len, site);
        if let Some((lo, hi)) = self.base_to_offsets.get(&base_id) {
            let new_hi = std::cmp::max(*hi, offset + len);
            self.base_to_offsets.insert(base_id.clone(), (*lo, new_hi));
        } else {
            self.base_to_offsets.insert(base_id.clone(), (offset, offset + len));
        }
        base_id
    }

    pub fn insert_rec(&mut self, id_arena: &mut IdArena, dot_index: &mut DotIndex, node: usize, node_base: &[u32], node_lo: u32, node_hi: u32, mut from: usize, len: u32, site: u32) -> Identifier {
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
                        dot_index.on_block_inserted(site, node_lo, node_hi, node);
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
                        dot_index.on_block_inserted(site, node_lo, node_hi, node);
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
                        dot_index.on_block_inserted(site, node_lo, node_hi, node);
                        // Intern the identifier
                        if inserted_id == Identifier::EMPTY {
                            let base_id = id_arena.intern(node_base);
                            self.node_set_base_id(node, base_id);
                            inserted_id = base_id;
                        } else {
                            self.node_set_base_id(node, inserted_id);
                        }
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
                        dot_index.on_block_inserted(site, node_lo, node_hi, node);
                        self.node_set_base_id(node, inserted_id);
                        con = false;
                    }
                },
                IdOrderingRelation::B1InsideB2 => {
                    let (sp, b_idx, from_base_id, from_offset, from_creator, mut from_content) = {
                        let from_node = &self.nodes[from];
                        let f_offset = from_node.offset;
                        let from_slice = id_arena.get_slice_unchecked(from_node.base_id);
                        let sp = id_arena.find_split_point(from_slice, f_offset, f_offset + from_node.size as u32, node_base);
                        // let sp = id_arena.find_split_point(&self.node_get_identifier_interval(from), node_idi.base);
                        let from_node = &mut self.nodes[from];
                        let from_content_ref = &from_node.content;
                        let b_idx = from_content_ref.char_indices()
                            .nth(sp as usize)
                            .map(|(idx, _)| idx)
                            .unwrap_or(from_content_ref.len());
                        let from_content = std::mem::take(&mut from_node.content);
                        (sp, b_idx, &from_node.base_id, from_node.offset, from_node.creator, from_content)
                    };

                    let rcontent = from_content.split_off(b_idx);
                    let right_node = Node::new(rcontent, from_base_id.clone(), from_offset + sp, from_creator);
                    let right_idx = self.alloca(right_node);

                    // Detach original_right BEFORE mutating from
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
                    
                    let joined = self.join(Some(node), right_idx, original_right);
                    self.nodes[joined].parent = Some(from);
                    self.nodes[from].right = Some(joined);
                    
                    // Update dot index 
                    dot_index.on_block_split(from_creator, from_offset, from_offset+sp, right_idx);
                    dot_index.on_block_inserted(site, node_lo, node_hi, node);
                    
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
                                dot_index.on_block_inserted(site, node_lo, node_hi, node);
                                break;
                            }
                        }
                    }
                    if self.node_creator(from) != site {
                        let from_node = &mut self.nodes[from];
                        if let Some(r) = from_node.right {
                            from = r;
                            continue;
                        } else {
                            from_node.right = Some(node);
                            self.nodes[node].parent = Some(from);
                            self.node_set_base_id(node, node_base_id);
                            dot_index.on_block_inserted(site, node_lo, node_hi, node);
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
                            dot_index.on_block_extended(site, from_node.offset, from_node.offset + from_node.size as u32);
                            self.free(node);
                            con = false;
                        }
                    } else {
                        let content = std::mem::take(&mut self.nodes[node].content);
                        let from_node = &mut self.nodes[from];
                        from_node.content.push_str(&content);
                        from_node.size = from_node.content.len();
                        // Update dot_index
                        dot_index.on_block_extended(site, from_node.offset, from_node.offset + from_node.size as u32);
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
                    let byte_idx = content
                        .char_indices()
                        .nth(sp as usize)
                        .map(|(i, _)| i)
                        .unwrap_or(content.len());
                    let left_content  = content[..byte_idx].to_string();
                    let right_content = content[byte_idx..].to_string();
                    
                    let id1 = self.insert_by_id(site, id_arena, dot_index, node_base, node_lo, left_content);
                    let id2 = self.insert_by_id(site, id_arena, dot_index, node_base, node_lo + sp, right_content);

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
                        dot_index.on_block_inserted(site, node_lo, node_hi, node);
                        con = false;
                    }
                },
            }
        }
        if !rec {
            self.rebalance(last);
        }
        return inserted_id;

    }

    pub fn splice(&mut self, target: usize, replacement: Option<usize>) {
        let target_node = &self.nodes[target];
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
            self.rebalance(Some(parent_idx));
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
        dot_index.on_block_inserted(n.creator, n.offset, n.offset + n.size as u32, new_idx);
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

        self.rebalance(last);
        new_idx
    }

    pub fn insert_before(&mut self, dot_index: &mut DotIndex, target: usize, node: Node) -> usize {
        let new_idx = self.alloca(node);
        let n = &self.nodes[new_idx];
        dot_index.on_block_inserted(n.creator, n.offset, n.offset + n.size as u32, new_idx);
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

        self.rebalance(last);
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
        let original_right = self.nodes[target].right;

        let content = std::mem::take(&mut self.nodes[target].content);
        let byte_idx = content
            .char_indices()
            .nth(sp)
            .map(|(i, _)| i)
            .unwrap_or(content.len());
        let left_content  = content[..byte_idx].to_string();
        let right_content = content[byte_idx..].to_string();

        // middle_idx
        self.nodes[target].content = left_content;
        self.nodes[target].size = sp;

        let right_node = Node::new(right_content, base_id, offset + sp as u32, creator);
        let right_idx = self.alloca(right_node);

        
        let middle_creator = middle.creator;    
        let middle_base   = middle.base_id;
        let middle_offset = middle.offset;
        let middle_size   = middle.size as u32;
        let middle_idx = self.alloca(middle);
        self.register_base_offsets(middle_base, middle_offset, middle_size);

        // Update dot index
        dot_index.on_block_split(creator, offset, offset + sp as u32, right_idx);
        dot_index.on_block_inserted(middle_creator, middle_offset, middle_offset + middle_size as u32, middle_idx);

        let joined = self.join(Some(middle_idx), right_idx, original_right);
        self.nodes[target].right = Some(joined);
        self.nodes[joined].parent = Some(target);

        self.rebalance(Some(target));  // path ends at target, NOT at right_idx
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

        match (left, right) {
            (None, None) => {
                self.splice(curr, None);
                dot_index.on_block_deleted(target_creator, target_offset);
            }
            (Some(child), None) | (None, Some(child)) => {
                self.splice(curr, Some(child));
                dot_index.on_block_deleted(target_creator, target_offset);
            }
            (Some(_), Some(r)) => {
                // Two children: replace with in-order successor, then delete successor.
                // let mut succ_path: Path = Path::from_slice(path);
                let mut succ = r;
                while let Some(l) = self.nodes[succ].left {
                    succ = l;
                }

                // Copy successor's payload into target.
                // let succ_data = self.nodes[succ].clone();
                let succ_content = std::mem::take(&mut self.nodes[succ].content);
                let succ_base    = self.nodes[succ].base_id;
                let succ_offset  = self.nodes[succ].offset;
                let succ_size    = self.nodes[succ].size;
                let succ_creator = self.nodes[succ].creator;

                let tn = &mut self.nodes[curr];
                tn.content = succ_content;
                tn.base_id = succ_base;
                tn.offset  = succ_offset;
                tn.size    = succ_size;
                tn.creator = succ_creator;

                // Delete successor 
                let succ_right = self.nodes[succ].right;
                self.splice(succ, succ_right);

                // Update dot index 
                dot_index.on_block_deleted(target_creator, target_offset);
                dot_index.on_node_remapped(succ_creator, succ_offset, curr);
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
        let original_right = self.nodes[target].right;
        let original_size = self.nodes[target].size;

        let content = std::mem::take(&mut self.nodes[target].content);

        // Find the two byte boundaries with a single pass
        let mut indices = content.char_indices();
        let left_byte = indices.nth(start)
            .map(|(i, _)| i)
            .unwrap_or(content.len());
        let mid_byte = indices.nth(count - 1)
            .map(|(i, _)| i)
            .unwrap_or(content.len());

        let left_content  = content[..left_byte].to_string();
        let right_content = content[mid_byte..].to_string();

        self.nodes[target].content = left_content;
        self.nodes[target].size = start;

        let right_node = Node::new(
            right_content, base_id, offset + (start + count) as u32, creator,
        );
        let right_idx = self.alloca(right_node);

        // In-order: target(left half), right_idx, original_right...
        // No middle element here — right_idx is the separator
        let joined = self.join(None, right_idx, original_right);
        self.nodes[target].right = Some(joined);
        self.nodes[joined].parent = Some(target);

        // Update dot index
        dot_index.on_block_middle_deleted(creator, offset, offset+start as u32, 
            offset+ (start+count) as u32, offset + original_size as u32, right_idx);

        self.rebalance(Some(target));
    }

    pub fn insert_first(&mut self, dot_index: &mut DotIndex, node: Node) -> usize {
        let idx = self.alloca(node);
        let n = &self.nodes[idx];
        dot_index.on_block_inserted(n.creator, n.offset, n.offset + n.size as u32, idx);
        self.register_base_offsets(n.base_id, n.offset, n.size as u32);
        self.root = Some(idx);
        idx
    }
}

pub struct InOrderIter<'a> {
    tree: &'a Tree,
    stack: SmallVec<[usize; 32]>,
    current: Option<usize>,
}

impl<'a> InOrderIter<'a> {
    pub fn new(tree: &'a Tree) -> Self {
        InOrderIter {
            tree,
            stack: SmallVec::new(),
            current: tree.root,
        }
    }
}

impl<'a> Iterator for InOrderIter<'a> {
    type Item = &'a Node;
    
    fn next(&mut self) -> Option<Self::Item> {
        let nodes = &self.tree.nodes;
        
        // Go as left as possible
        while let Some(curr_idx) = self.current {
            self.stack.push(curr_idx);
            self.current = nodes[curr_idx].left;
        }
        
        // Pop from stack
        let node_idx = self.stack.pop()?;
        let node = &nodes[node_idx];
        
        // Move to right subtree
        self.current = node.right;
        
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
        self.inorder_iter().map(|n| n.content.clone()).collect::<Vec<String>>().join("")
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