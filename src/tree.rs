use core::panic;
use std::cmp::Ordering;
use crate::node::Node;
use crate::idarena::{IdArena, IdBlock, IdOrderingRelation, Identifier};
use smallvec::SmallVec;

pub type Path = SmallVec<[usize; 32]>;

#[derive(Clone, Debug)]
pub struct Tree {
    pub nodes: Vec<Node>, 
    pub root: Option<usize>,
    free_list: Vec<usize>,
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
        }
    }

    pub fn clear(&mut self) {
        self.root = None;
        self.nodes.clear();
        self.free_list.clear();
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

    pub fn node_block(&self, node: usize) -> IdBlock { 
        self.nodes[node].block
    }

    pub fn extend_content(&mut self, arena: &mut IdArena, node: usize, text: &str, path_to_root: &[usize]) {
        let node = &mut self.nodes[node];
        node.content.push_str(text);
        let added_size = text.chars().count();
        node.size += added_size;
        node.block.extend_end(arena, added_size as u32);
        for idx in path_to_root.iter().rev() {
            self.update_node(*idx);
        }
    }

    pub fn truncate_content(&mut self, arena: &mut IdArena, node: usize, num_delete: usize, location: DelLocation, path_to_root: &[usize]) {
        let n = &mut self.nodes[node];
        match location {
            DelLocation::Start => {
                let byte_off = n.content.char_indices()
                    .nth(num_delete)
                    .map(|(i, _)| i)
                    .unwrap_or(n.content.len());
                let kept = n.content.split_off(byte_off);
                n.content = kept;
                n.block.truncate_start(arena, num_delete as u32);
                // n.offset += num_delete as u32;
                /* Change the lo ID */
                
            }
            DelLocation::End => {
                let keep_chars = n.size - num_delete;
                let byte_off = n.content.char_indices()
                    .nth(keep_chars)
                    .map(|(i, _)| i)
                    .unwrap_or(n.content.len());
                n.content.truncate(byte_off); // truncate in-place, no allocation
                n.block.truncate_end(arena, num_delete as u32);
            }
        }
        n.size -= num_delete;
        for idx in path_to_root.iter().rev() {
            self.update_node(*idx);
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
        self.update_node(y);
        self.update_node(x);
        x
    }

    fn rotate_left(&mut self, x: usize) -> usize {
        let y = self.nodes[x].right.expect("rotate_left: No right child");
        let b = self.nodes[y].left;
        self.nodes[y].left = Some(x);
        self.nodes[x].right = b;
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
            }
            self.rotate_left(idx)
        } else if bf < -1 {
            // left heavy 
            let l = self.nodes[idx].left.unwrap();
            if self.balance_factor(l) > 0 {
                let ln = self.rotate_left(l);
                self.nodes[idx].left = Some(ln);
            }
            self.rotate_right(idx)
        } else { 
            idx
        }
    }

    /* Rebalance the tree all along a path to root */
    fn rebalance(&mut self, path_to_root: &[usize]) {
        if path_to_root.is_empty() { return; }

        // The deepest node in the path 
        let mut curr = *path_to_root.last().unwrap(); 
        let path_len = path_to_root.len();

        for i in (0..path_to_root.len()).rev() {
            let idx = path_to_root[i];
            let node = &self.nodes[idx];

            // Update its children 
            if i+1 < path_len { 
                let old_child = path_to_root[i+1];
                if node.left == Some(old_child) {
                    self.nodes[idx].left = Some(curr);
                } else { 
                    self.nodes[idx].right = Some(curr);
                }
            }

            let old_h = self.nodes[idx].height;
            curr = self.avl_fix(idx);
            if curr == idx && self.nodes[idx].height == old_h {
                for j in (0..i).rev() {
                    self.update_node(path_to_root[j]);
                }
                if i > 0 { 
                    self.root = Some(path_to_root[0]); 
                }
                else { 
                    self.root = Some(curr); 
                }
            return;
        }

        }
        self.root = Some(curr);
    }
}

/* Inorder Predecessor and Successor Functions */
impl Tree {
    // Function to get inorder successor of a node
    pub fn next(&self, node: usize, path: &[usize]) -> Option<usize> {
        let nodes = &self.nodes;
        let curr = node;

        // Case 1: right subtree: leftmost node
        if let Some(mut r) = nodes[curr].right {
            while let Some(l) = nodes[r].left {
                r = l;
            }
            return Some(r);
        }

        // Case 2: go up until we come from left
        for i in (1..path.len()).rev() {
            let parent = path[i - 1];
            if nodes[parent].left == Some(path[i]) {
                return Some(parent);
            }
        }
        None
    }

    // Function to get inorder predecessor of a node
    pub fn prev(&self, node: usize, path: &[usize]) -> Option<usize> {
        let nodes = &self.nodes;
        let curr = node;

        // Case 1: left subtree → rightmost node
        if let Some(mut l) = nodes[curr].left {
            while let Some(r) = nodes[l].right {
                l = r;
            }
            return Some(l);
        }

        // Case 2: go up until we come from right
        for i in (1..path.len()).rev() {
            let parent = path[i - 1];
            if nodes[parent].right == Some(path[i]) {
                return Some(parent);
            }
        }
        None
    }
}

impl Tree {
    pub fn find_by_pos(&self, pos: usize) -> (Path, usize) {
        let mut path_to_root = Path::new(); 
        let nodes = &self.nodes;
        let mut i = self.root;
        let mut curr = pos;
        let mut covered: usize = 0;
        while let Some(index) = i { 
            let node = &nodes[index];
            path_to_root.push(index);
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
                return (path_to_root, covered);
            } else { 
                curr -= left_count + node.size;
                covered += left_count + node.size;
                i = node.right;
            }
        }
        (path_to_root, covered)
    }

    pub fn find_by_pos_delete(&self, pos: usize) -> (Path, usize) {
        let mut path_to_root = Path::new();
        let nodes = &self.nodes;
        let mut i = self.root;
        let mut curr = pos;
        let mut covered: usize = 0;
        while let Some(index) = i {
            let node = &nodes[index];
            path_to_root.push(index);
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
                return (path_to_root, covered);
            } else {
                curr -= left_count + node.size;
                covered += left_count + node.size;
                i = node.right;
            }
        }
        (path_to_root, covered)
    }

    /// Insert the node by identifier  
    pub fn insert_by_id(&mut self, site: u32, id_arena: &mut IdArena, block: &mut IdBlock, content: String) {
        let len = content.chars().count() as u32;
        let idx = self.alloca(Node::new(content, *block, site));
        if self.is_empty() {
            self.root = Some(idx);
            // FIXME: Need to ensure causality 
            return;
        }
        let from = self.root.unwrap();
        self.insert_rec(id_arena, idx, block, from, len, site);
    }

    pub fn insert_rec(&mut self, id_arena: &mut IdArena, node: usize, node_block: &mut IdBlock, mut from: usize, len: u32, site: u32) {
        let mut path = Path::new();
        let mut con = true;
        let mut rec = false;

        while con {
            path.push(from);

            let relation = {
                let n = &self.nodes[from];
                id_arena.compare_intervals(node_block, &n.block)
            };

            match relation {
                IdOrderingRelation::B1AfterB2 => {
                    let from_node = &mut self.nodes[from];
                    if let Some(r) = from_node.right {
                        from = r;
                    } else {
                        from_node.right = Some(node);
                        con = false;
                    }
                },
                IdOrderingRelation::B1BeforeB2 => {
                    let from_node = &mut self.nodes[from];
                    if let Some(l) = from_node.left {
                        from = l;
                    } else {
                        from_node.left = Some(node);
                        con = false;
                    }
                },
                IdOrderingRelation::B1InsideB2 => {
                    let sp = id_arena.find_split_point(&self.nodes[from].block, node_block.low);
                    let from_block_low = self.nodes[from].block.low;
                    let from_block_count = self.nodes[from].block.count;
                    let from_creator = self.nodes[from].creator;

                    let from_content_ref = &self.nodes[from].content;
                    let b_idx = from_content_ref.char_indices()
                        .nth(sp as usize)
                        .map(|(idx, _)| idx)
                        .unwrap_or(from_content_ref.len());

                    let mut from_content = std::mem::take(&mut self.nodes[from].content);
                    let rcontent = from_content.split_off(b_idx);

                    let right_lo = IdBlock::id_with_offset(id_arena, from_block_low, sp);
                    let right_block = IdBlock::new(right_lo, from_block_count - sp, id_arena);
                    let right_idx = self.alloca(Node::new(rcontent, right_block, from_creator));

                    let original_right = self.nodes[from].right;
                    self.nodes[from].content = from_content;
                    self.nodes[from].size = self.nodes[from].content.chars().count();
                    self.nodes[from].block = IdBlock::new(from_block_low, sp, id_arena);
                    self.nodes[from].right = Some(right_idx);

                    self.nodes[right_idx].right = original_right;
                    self.nodes[right_idx].left = Some(node);

                    path.push(right_idx);
                    con = false;
                },
                IdOrderingRelation::B2ConcatB1 => {
                    if self.node_creator(from) != site {
                        let from_node = &mut self.nodes[from];
                        if let Some(r) = from_node.right {
                            from = r;
                            continue;
                        } else {
                            from_node.right = Some(node);
                            break;
                        }
                    }
                    let nxt = self.next(from, &path);
                    if let Some(r) = nxt {
                        let r_node = &self.nodes[r];
                        let n_insertable = id_arena.num_insertable(node_block.low, r_node.block.low, len);
                        if n_insertable < len {
                            from = self.nodes[from].right.unwrap();
                        } else {
                            // take content first, then borrow from_node
                            let content = std::mem::take(&mut self.nodes[node].content);
                            let from_node = &mut self.nodes[from];
                            from_node.content.push_str(&content);
                            from_node.size += len as usize;
                            from_node.block.extend_end(id_arena, len);
                            self.free(node);
                            con = false;
                        }
                    } else {
                        let content = std::mem::take(&mut self.nodes[node].content);
                        let from_node = &mut self.nodes[from];
                        from_node.content.push_str(&content);
                        from_node.size = from_node.content.chars().count();
                        from_node.block.extend_end(id_arena, len);
                        self.free(node);
                        con = false;
                    }
                },

                IdOrderingRelation::B1EqualsB2 => {
                    con = false;
                }
                IdOrderingRelation::B2InsideB1 => {
                    let sp = id_arena.find_split_point(node_block, self.nodes[from].block.low);
                    let content = std::mem::take(&mut self.nodes[node].content);
                    let byte_idx = content
                        .char_indices()
                        .nth(sp as usize)
                        .map(|(i, _)| i)
                        .unwrap_or(content.len());
                    let left_content  = content[..byte_idx].to_string();
                    let right_content = content[byte_idx..].to_string();

                    // Create left and right block 
                    let right_lo = IdBlock::id_with_offset(id_arena, node_block.low, sp);
                    let mut right_block = IdBlock::new(right_lo, node_block.count - sp, id_arena);
                    
                    node_block.truncate_end(id_arena, node_block.count - sp);

                    // FIXME?
                    self.insert_by_id(site, id_arena, node_block, left_content);
                    self.insert_by_id(site, id_arena, &mut right_block, right_content);

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
                        con = false;
                    }
                },
            }
        }
        if !rec {
            self.rebalance(&path);
        }
    }

    pub fn splice(&mut self, path: &[usize], target: usize, replacement: Option<usize>) {
        if path.len() == 1 {
            // Target is root 
            self.root = replacement;
            self.free(target);
            return;
        }

        let parent_idx = path[path.len() - 2];
        let parent = &mut self.nodes[parent_idx];
        if parent.left == Some(target) {
            parent.left = replacement;
        } else if parent.right == Some(target) {
            parent.right = replacement;
        } else { 
            panic!("splice: invalid path, target not a child of its parent");
        }

        self.free(target);
        self.rebalance(&path[..path.len()-1]);
    }

    pub fn delete_by_id(&mut self, id_arena: &mut IdArena, base: Identifier, _offset: u32) -> Result<(), ()> {
        // let mut path: Vec<usize> = vec![];
        if self.is_empty() {
            return Err(())
        }
        let path= self.find_by_id(id_arena, base);
        if path.is_empty() {
            return Err(());
        } 

        let curr = *path.last().unwrap();

        // Found the block to delete, delete the entire thing 
        let target = &self.nodes[curr];
        let left = target.left;
        let right: Option<usize> = target.right;

        match (left, right) {
            (None, None) => {
                // No children, just delete
                self.splice(&path, curr, None);
            },

            (Some(child), None) | (None, Some(child)) => {
                self.splice(&path, curr, Some(child));
            },

            (Some(_), Some(r)) => {
                let delete_idx = curr;
                let mut succ_path = path.clone();
                succ_path.push(r);
                let mut curr = r;

                while let Some(l) = self.nodes[curr].left {
                    succ_path.push(l);
                    curr = l;
                }

                let succ = curr;
                let succ_payload = self.nodes[succ].clone();
                let tn = &mut self.nodes[delete_idx];
                tn.content = succ_payload.content;
                tn.block = succ_payload.block;
                tn.size    = succ_payload.size;
                tn.creator = succ_payload.creator;

                let succ_right = self.nodes[succ].right;
                self.splice(&succ_path, succ, succ_right);
            }
        }
         Ok(())
    }

    pub fn find_by_id(&mut self, id_arena: &mut IdArena, base: Identifier) -> Path {
        let mut path = Path::new();
        if self.is_empty() {
            return Path::new();
        }
        let mut curr = self.root.unwrap();
 
        loop {
            path.push(curr);
            let cmp = {
                let b1_block = IdBlock::new(base, 1, id_arena);
                let b2_block = self.nodes[curr].block;
                id_arena.compare_intervals(&b1_block, &b2_block)
            };

            match cmp {
                IdOrderingRelation::B1AfterB2 | IdOrderingRelation::B2ConcatB1 => {
                    let from_node = &mut self.nodes[curr];
                    if let Some(r) = from_node.right {
                        curr = r;
                    } else {
                        break;
                    } 
                },
                IdOrderingRelation::B1BeforeB2 | IdOrderingRelation::B1ConcatB2 => {
                    let from_node = &mut self.nodes[curr];
                    if let Some(l) = from_node.left {
                        curr = l;
                    } else {
                        break;    
                    }
                },
                IdOrderingRelation::B1InsideB2 | IdOrderingRelation::B1EqualsB2 => {
                    // Found the block, return the path to it 
                    return path;
                }
                _ => panic!("Unexpected relation between B1 and B2 during find_by_id")
            }
        }
        return Path::new();
    }

    pub fn find_by_id_exact(&mut self, id_arena: &mut IdArena, base: Identifier) -> Path {
        let mut path = Path::new();
        if self.is_empty() {
            return Path::new();
        }
        let mut curr = self.root.unwrap();

        loop {
            path.push(curr);
            let cmp = {
                let b1_block = IdBlock::new(base, 1, id_arena);
                let b2_block = self.nodes[curr].block;
                id_arena.compare_intervals(&b1_block, &b2_block)

            };

            match cmp {
                IdOrderingRelation::B1AfterB2 | IdOrderingRelation::B2ConcatB1 => {
                    if let Some(r) = self.nodes[curr].right {
                        curr = r;
                    } else {
                        return Path::new();
                    }
                }
                IdOrderingRelation::B1BeforeB2 | IdOrderingRelation::B1ConcatB2 => {
                    if let Some(l) = self.nodes[curr].left {
                        curr = l;
                    } else {
                        return Path::new();
                    }
                }
                IdOrderingRelation::B1EqualsB2 => {
                    // Exact interval match -- still verify
                    // if self.nodes[curr].block.low == base {
                    //     return path;
                    // }
                    let node_block = self.nodes[curr].block;
                    if id_arena.id_in_block(base, &node_block) {
                        return path;
                    }
                    return Path::new();
                }
                IdOrderingRelation::B1InsideB2 => {
                    // Probe falls inside this node's range.
                    // Only a real match if the base is identical.
                    // Cannot exist elsewhere in the tree, so return empty if base differs.
                    // if self.nodes[curr].block.low == base {
                    //     return path;
                    // }
                    let node_block = self.nodes[curr].block;
                    if id_arena.id_in_block(base, &node_block) {
                        return path;
                    }
                    return Path::new();
                }
                _ => panic!("Unexpected relation in find_by_id_exact"),
            }
        }
    }
}

impl Tree {
    // #[inline(always)]
    // fn register_base_offsets(&mut self, base: Identifier, offset: u32, size: u32) {
    //     if let Some((lo, hi)) = self.base_to_offsets.get(&base) {
    //         let new_lo = std::cmp::min(*lo, offset);
    //         let new_hi = std::cmp::max(*hi, offset + size);
    //         self.base_to_offsets.insert(base, (new_lo, new_hi));
    //     } else {
    //         self.base_to_offsets.insert(base, (offset, offset + size));
    //     }
    // }

    pub fn insert_after(&mut self, path: &[usize], node: Node) -> usize {
        let new_idx = self.alloca(node);
        let _n = &self.nodes[new_idx];
        // self.register_base_offsets(n.base_id, n.offset, n.size as u32);

        let target = *path.last().unwrap();
        let mut extended_path: Path = Path::from_slice(path);

        if self.nodes[target].right.is_none() {
            self.nodes[target].right = Some(new_idx);
        } else {
            // Walk to the leftmost node in the right subtree.
            let mut curr = self.nodes[target].right.unwrap();
            extended_path.push(curr);
            while let Some(l) = self.nodes[curr].left {
                curr = l;
                extended_path.push(curr);
            }
            self.nodes[curr].left = Some(new_idx);
        }

        self.rebalance(&extended_path);
        new_idx
    }

    pub fn insert_before(&mut self, path: &[usize], node: Node) -> usize {
        let new_idx = self.alloca(node);
        let _n = &self.nodes[new_idx];
        // self.register_base_offsets(n.base_id, n.offset, n.size as u32);

        let target = *path.last().unwrap();
        let mut extended_path: Path = Path::from_slice(path);

        if self.nodes[target].left.is_none() {
            self.nodes[target].left = Some(new_idx);
        } else {
            // Walk to the rightmost node in the left subtree.
            let mut curr = self.nodes[target].left.unwrap();
            extended_path.push(curr);
            while let Some(r) = self.nodes[curr].right {
                curr = r;
                extended_path.push(curr);
            }
            self.nodes[curr].right = Some(new_idx);
        }

        self.rebalance(&extended_path);
        new_idx
    }

    pub fn split_and_insert_middle(
        &mut self,
        id_arena: &mut IdArena,
        path: &[usize],
        sp: usize,         
        middle: Node,
    ) -> usize {
        let target = *path.last().unwrap();
        
        let creator = self.nodes[target].creator;
        
        let content = std::mem::take(&mut self.nodes[target].content);
        let byte_idx = content
        .char_indices()
        .nth(sp)
        .map(|(i, _)| i)
        .unwrap_or(content.len());
        let left_content  = content[..byte_idx].to_string();
        let right_content = content[byte_idx..].to_string();
        let left_size = sp;
        
        let target_node = &mut self.nodes[target];
        let original_right = target_node.right;
        target_node.content = left_content;
        target_node.size = left_size;
        
        let node_block = &mut target_node.block;
        // Create right block 
        let right_lo = IdBlock::id_with_offset(id_arena, node_block.low, sp as u32);
        let right_block = IdBlock::new(right_lo, node_block.count - (sp as u32), id_arena);
        let right_node = Node::new(
            right_content,
            right_block,
            creator,
        );
        node_block.truncate_end(id_arena, node_block.count - sp as u32);
        let right_idx = self.alloca(right_node);
        // Right half inherits target's original right subtree.
        self.nodes[right_idx].right = original_right;

        let _middle_block   = middle.block;
        // let middle_offset = middle.offset;
        let _middle_size   = middle.size as u32;
        let middle_idx = self.alloca(middle);

        self.nodes[right_idx].left = Some(middle_idx);
        self.nodes[target].right   = Some(right_idx);

        // rebalance from the deepest new node up to root 
        let mut extended_path: Path = Path::from_slice(path);
        extended_path.push(right_idx);
        extended_path.push(middle_idx);
        self.rebalance(&extended_path);

        middle_idx
    }

    /// Delete the node at `path.last()` using the path directly.
    /// This is the same algorithm as delete_by_id's second half, but skips
    /// the find_by_id traversal since we already have the path.
    pub fn delete_at_path(&mut self, path: &[usize]) {
        if path.is_empty() {
            return;
        }
        let curr = *path.last().unwrap();
        let left  = self.nodes[curr].left;
        let right = self.nodes[curr].right;

        match (left, right) {
            (None, None) => {
                self.splice(path, curr, None);
            }
            (Some(child), None) | (None, Some(child)) => {
                self.splice(path, curr, Some(child));
            }
            (Some(_), Some(r)) => {
                // Two children: replace with in-order successor, then delete successor.
                let mut succ_path: Path = Path::from_slice(path);
                succ_path.push(r);
                let mut succ = r;
                while let Some(l) = self.nodes[succ].left {
                    succ = l;
                    succ_path.push(succ);
                }

                // Copy successor's payload into target.
                // let succ_data = self.nodes[succ].clone();
                let succ_content = std::mem::take(&mut self.nodes[succ].content);
                let succ_block    = self.nodes[succ].block;
                // let succ_offset  = self.nodes[succ].offset;
                let succ_size    = self.nodes[succ].size;
                let succ_creator = self.nodes[succ].creator;

                let tn = &mut self.nodes[curr];
                tn.content = succ_content;
                tn.block = succ_block;
                // tn.offset  = succ_offset;
                tn.size    = succ_size;
                tn.creator = succ_creator;

                // Delete successor 
                let succ_right = self.nodes[succ].right;
                self.splice(&succ_path, succ, succ_right);
            }
        }
    }

    pub fn delete_middle_at_path(
        &mut self,
        id_arena: &mut IdArena,
        path: &[usize],
        start: usize,
        count: usize,
    ) {
        let target = *path.last().unwrap();
        let node_block = self.nodes[target].block;
        let creator = self.nodes[target].creator;
        let original_right = self.nodes[target].right;

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

        // Update the blocks of left and right nodes

        self.nodes[target].content = left_content;
        self.nodes[target].size = start;
        self.nodes[target].block = IdBlock::new(node_block.low, start as u32, id_arena);

        let right_lo = IdBlock::id_with_offset(id_arena, node_block.low, (start+count) as u32);
        let right_block = IdBlock::new(right_lo, node_block.count - ((start + count) as u32), id_arena);

        let right_node = Node::new(
            right_content,
            right_block,
            creator,
        );
        let right_idx = self.alloca(right_node);

        self.nodes[right_idx].right = original_right;
        self.nodes[target].right = Some(right_idx);

        let mut extended_path: Path = Path::from_slice(path);
        extended_path.push(right_idx);
        self.rebalance(&extended_path);
    }

    pub fn insert_first(&mut self, node: Node) -> usize {
        let idx = self.alloca(node);
        let _n = &self.nodes[idx];
        // self.register_base_offsets(n.base_id, n.offset, n.size as u32);
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
    pub fn print_tree(&self) {
        println!("\n===== BLOCK TREE =====");
        match self.root {
            Some(root) => self.print_node(root, "", true),
            None => println!("(empty)"),
        }
        println!("======================\n");
    }

    fn print_node(&self, idx: usize, prefix: &str, is_last: bool) {
        let node = &self.nodes[idx];

        // formatting helpers
        let left = node.left.map_or("·".to_string(), |x| x.to_string());
        let right = node.right.map_or("·".to_string(), |x| x.to_string());

        let base = &node.block;

        // trim content for readability
        let content = if node.content.len() > 10 {
            format!("{}...", &node.content[..10])
        } else {
            node.content.clone()
        };

        println!(
            "{}{}[{}] base={:?} size={} cnt={} h={} | L:{} R:{} | \"{}\" | creator={}",
            prefix,
            if is_last { "└──" } else { "├──" },
            idx,
            base,
            // node.offset,
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
                self.print_node(l, &new_prefix, false);
                self.print_node(r, &new_prefix, true);
            }
            (Some(l), None) => {
                self.print_node(l, &new_prefix, true);
            }
            (None, Some(r)) => {
                self.print_node(r, &new_prefix, true);
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
        for node in self.inorder_iter() {
            let curr_block = node.block.clone();
            let (_lo, _hi) = (curr_block.low, curr_block.high);
            if let Some(prev) = prev_id {
                let cmp = id_arena.compare_ids(curr_block.low, prev);
                if cmp != Ordering::Greater {
                    eprintln!("Tree check failed: current id {:?} is not greater than previous id {:?}", curr_block.low, prev);
                    return false;
                }
            }
            prev_id = Some(curr_block.high);
            // prev_offsets = Some((lo, hi));
        }
        true
    }
}