use core::panic;
use std::collections::HashMap;
use crate::node::Node;
use crate::identifier::{Id, IdOrderingRelation, Identifier, IdentifierInterval, compare_intervals, num_insertable};

#[derive(Clone, Debug)]
pub struct Tree {
    pub nodes: Vec<Node>, 
    pub root: Option<usize>,
    free_list: Vec<usize>,
    base_to_offsets: HashMap<String, (u32, u32)>
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

    fn node_height(&self, node: Option<usize>) -> i32 { 
        node.map_or(0, |index| self.nodes[index].height)
    }

    fn node_subtree_count(&self, node: Option<usize>) -> usize { 
        node.map_or(0, |index| self.nodes[index].subtree_count)
    }

    pub fn node_content(&self, node: Option<usize>) -> &str { 
        node.map_or("", |index| &self.nodes[index].content)
    }

    pub fn node_size(&self, node: Option<usize>) -> usize { 
        node.map_or(0, |index| self.nodes[index].size)
    }

    pub fn node_left_count(&self, node: Option<usize>) -> usize { 
        if node.is_none() { return 0; }
        let left= self.nodes[node.unwrap()].left;
        left.map_or(0, |index| self.nodes[index].subtree_count)
    }

    pub fn node_creator(&self, node: usize) -> u32 { 
        self.nodes[node].creator
    }

    pub fn node_base_id(&self, node: usize) -> &Identifier { 
        &self.nodes[node].base_id
    }

    pub fn node_ranges(&self, node: usize) -> (u32, u32) { 
        let n = &self.nodes[node];
        let range_left= n.offset;
        let range_right= n.offset + n.size as u32;
        (range_left, range_right)
    }

    pub fn node_base_offsets(&self, node: usize) -> (u32, u32) { 
        // Get the offsets from the map
        let base_str = self.nodes[node].base_id.to_string();
        if let Some((lo, hi)) = self.base_to_offsets.get(&base_str) {
            return (*lo, *hi)
        } else {
            panic!("Base offsets not found for node {}, this should not happen", node);
        }
    }

    pub fn node_get_identifier_interval(&self, node: usize) -> IdentifierInterval { 
        let base = self.nodes[node].base_id.clone();
        let offset = self.nodes[node].offset;
        IdentifierInterval::new(base, offset, offset + self.nodes[node].size as u32)
    }

    pub fn extend_content(&mut self, node: usize, text: &str, path_to_root: &[usize]) {
        let node = &mut self.nodes[node];
        node.content.push_str(text);
        let added_size = text.chars().count();
        node.size += added_size;
        // update the offsets of the base 
        let base_str = node.base_id.to_string();
        if let Some((lo, hi)) = self.base_to_offsets.get(&base_str) {
            let new_hi = hi + added_size as u32;
            self.base_to_offsets.insert(base_str, (*lo, new_hi));
        } 
        for idx in path_to_root.iter().rev() {
            self.update_node(*idx);
        }
    }

    pub fn truncate_content(&mut self, node: usize, num_delete: usize, location: DelLocation, path_to_root: &[usize]) {
        let node = &mut self.nodes[node];
        let content_len = node.content.chars().count();
        match location {
            DelLocation::Start => {
                let new_content: String = node.content.chars().skip(num_delete as usize).collect();
                node.content = new_content;
            }
            DelLocation::End => {
                let new_content: String = node.content.chars().take(content_len - num_delete).collect();
                node.content = new_content;
            }
        }
        node.size -= num_delete as usize;
        // update offsets 
        node.offset = match location {
            DelLocation::Start => node.offset + num_delete as u32,
            DelLocation::End => node.offset
        };
        for idx in path_to_root.iter().rev() {
            self.update_node(*idx);
        }
    }
}

/* Rotation and Rebalancing Functions */
impl Tree {
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
    fn rebalance(&mut self, path_to_root: Vec<usize>) {
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
            curr = self.avl_fix(idx);
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
    pub fn find_by_pos(&self, pos: usize) -> (Vec<usize>, usize) {
        let mut path_to_root: Vec<usize> = vec![]; 
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

    pub fn find_by_pos_delete(&self, pos: usize) -> (Vec<usize>, usize) {
        let mut path_to_root: Vec<usize> = vec![];
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
    pub fn insert_by_id(&mut self, site: u32, base: Id, offset: u32, content: String) {
        let idx = self.alloca(Node::new(content.clone(), base.clone(), offset, site));
        if self.is_empty() {
            self.root = Some(idx);
            self.base_to_offsets.insert(base.to_string(), (offset, offset + content.chars().count() as u32));
            return;
        }
        let from = self.root.unwrap();
        let len = content.chars().count() as u32;
        let insert_interval = IdentifierInterval::new(base.clone(), offset, offset + len);
        self.insert_rec(idx, insert_interval, from, content, site);
        // Lookup in base to offsets map and modify the offsets accordingly
        if let Some((lo, hi)) = self.base_to_offsets.get(&base.to_string()) {
            // Modify the offsets accordingly
            let new_hi = std::cmp::max(*hi, offset + len);
            self.base_to_offsets.insert(base.to_string(), (*lo, new_hi));
        } else {
            self.base_to_offsets.insert(base.to_string(), (offset, offset + len));
        }
    }

    fn find_split_point(idi_short: &IdentifierInterval, id_long: &Id) -> u32 {
        let mut sp = 0;
        let text_len = idi_short.hi - idi_short.lo;
        for i in 0..text_len {
            let id_elem = idi_short.base.with_offset(idi_short.lo + i);
            if id_elem >= *id_long {
                break;
            }
            sp+=1;
        }
        return sp;
    }

    pub fn insert_rec(&mut self, node: usize, mut node_idi: IdentifierInterval, mut from: usize, content: String, site: u32) {
        let mut path = vec![];
        let mut con = true;
        let mut rec = false;

        while con {
            path.push(from);
            
            // B1 is the block we are adding 
            // B2 is the block we are comparing with
            let b1 = &mut node_idi;
            let b2 = &self.node_get_identifier_interval(from);

            match compare_intervals(b1, &b2) {
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
                    // Split the node and insert in the middle
                    // Find the split point 
                    let (sp, b_idx, from_base_id, from_offset, from_creator, mut from_content) = {
                        let sp = Self::find_split_point(&self.node_get_identifier_interval(from), &node_idi.base);
                        // println!("Splitting node {} at char offset {})", from, sp);
                        let from_node = &mut self.nodes[from];
                        let from_content = &from_node.content;
                        // Find the byte position of the character offset 'sp'
                        let b_idx = from_content.char_indices()
                            .nth(sp as usize)
                            .map(|(idx, _)| idx)
                            .unwrap_or(from_content.len());
                        let from_content = std::mem::take(&mut from_node.content);
                        (sp, b_idx, &from_node.base_id, from_node.offset, from_node.creator, from_content)
                    };

                    let rcontent = from_content.split_off(b_idx); 
                    
                    let right_node = Node::new(rcontent, from_base_id.clone(), from_offset + sp, from_creator);
                    let right_idx = &self.alloca(right_node);

                    let from_node = &mut self.nodes[from];
                    let original_right = from_node.right;
                    from_node.content = from_content;
                    from_node.size = from_node.content.chars().count();
                    from_node.right = Some(*right_idx);

                    let right_node = &mut self.nodes[*right_idx];
                    right_node.right = original_right;
                    right_node.left = Some(node);

                    path.push(*right_idx);
                    con = false;
                },
                IdOrderingRelation::B2ConcatB1 => {
                    // Concat at the end
                    // We should also check if we are at upper edge of offsets for this block
                    // As a rule, we will not reinsert IDs which have already been inserted & deleted 
                    // check base to offsets map 
                    if let Some((_, hi)) = self.base_to_offsets.get(&b2.base.to_string()) {
                        if node_idi.lo < *hi {
                            let from_node = &mut self.nodes[from];
                            if let Some(r) = from_node.right {
                                from = r;
                                continue;
                            } else {
                                from_node.right = Some(node);
                                break;
                            }
                        }
                    }
                    // Also check if we actually own this 
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
                        // See how much we can insert before clashing with the next ID
                        let r_base = self.node_base_id(r);
                        let r_offset = self.node_ranges(r).0;
                        let len = content.chars().count() as u32;
                        let n_insertable= num_insertable(&node_idi.base.with_offset(node_idi.lo), &r_base.with_offset(r_offset), len);
                        let from_node = &mut self.nodes[from];
                        if n_insertable < len {
                            // FIXME: just go right, don't bother splitting for now
                            from = from_node.right.unwrap();
                        } else {
                            // CHECK 
                            from_node.content.push_str(&content);
                            from_node.size = from_node.content.chars().count();
                            con = false;
                        }
                    } else {
                        // We can extend as much as we want, just concat at the end
                        let from_node = &mut self.nodes[from];
                        from_node.content.push_str(&content);
                        from_node.size = from_node.content.chars().count();
                        con = false;
                    }
                },
                IdOrderingRelation::B1EqualsB2 => {
                    con = false;
                }
                IdOrderingRelation::B2InsideB1 => {
                    // println!("Covered...");
                    // Split the incoming node 
                    let sp = Self::find_split_point(b1, &b2.base);
                    let left_content: String = content.chars().take(sp as usize).collect();
                    let right_content: String = content.chars().skip(sp as usize).collect();

                    // Insert both recursively 
                    self.insert_by_id(site, b1.base.clone(), b1.lo, left_content);
                    self.insert_by_id(site, std::mem::take(&mut b1.base), b1.lo + sp, right_content);

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
            self.rebalance(path);
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
        self.rebalance(path[..path.len()-1].to_vec());
    }

    pub fn delete_by_id(&mut self, base: Id, offset: u32) -> Result<(), ()> {
        // let mut path: Vec<usize> = vec![];
        if self.is_empty() {
            return Err(())
        }
        let path= self.find_by_id(base, offset);
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
                tn.base_id = succ_payload.base_id;
                tn.offset  = succ_payload.offset;
                tn.size    = succ_payload.size;
                tn.creator = succ_payload.creator;

                let succ_right = self.nodes[succ].right;
                self.splice(&succ_path, succ, succ_right);
            }
        }
         Ok(())
    }

    pub fn find_by_id(&mut self, base: Id, offset: u32) -> Vec<usize> {
        let mut path = vec![];
        if self.is_empty() {
            return Vec::new();
        }
        let node_idi = IdentifierInterval::new(base, offset, offset+1);
        let mut curr = self.root.unwrap();
 
        loop {
            path.push(curr);
            let b1 = &node_idi;
            let b2 = &self.node_get_identifier_interval(curr);

            match compare_intervals(b1, &b2) {
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
        return vec![];
    }

    pub fn find_by_id_exact(&mut self, base: Id, offset: u32) -> Vec<usize> {
        let mut path = vec![];
        if self.is_empty() {
            return Vec::new();
        }
        let node_idi = IdentifierInterval::new(base.clone(), offset, offset + 1);
        let mut curr = self.root.unwrap();

        loop {
            path.push(curr);
            let b1 = &node_idi;
            let b2 = &self.node_get_identifier_interval(curr);

            match compare_intervals(b1, &b2) {
                IdOrderingRelation::B1AfterB2 | IdOrderingRelation::B2ConcatB1 => {
                    if let Some(r) = self.nodes[curr].right {
                        curr = r;
                    } else {
                        return vec![];
                    }
                }
                IdOrderingRelation::B1BeforeB2 | IdOrderingRelation::B1ConcatB2 => {
                    if let Some(l) = self.nodes[curr].left {
                        curr = l;
                    } else {
                        return vec![];
                    }
                }
                IdOrderingRelation::B1EqualsB2 => {
                    // Exact interval match — still verify base
                    if self.nodes[curr].base_id == base {
                        return path;
                    }
                    return vec![];
                }
                IdOrderingRelation::B1InsideB2 => {
                    // Probe falls inside this node's range.
                    // Only a real match if the base is identical.
                    // Cannot exist elsewhere in the tree, so return empty if base differs.
                    if self.nodes[curr].base_id == base {
                        return path;
                    }
                    return vec![];
                }
                _ => panic!("Unexpected relation in find_by_id_exact"),
            }
        }
    }


}

pub struct InOrderIter<'a> {
    tree: &'a Tree,
    stack: Vec<usize>,
    current: Option<usize>,
}

impl<'a> InOrderIter<'a> {
    pub fn new(tree: &'a Tree) -> Self {
        InOrderIter {
            tree,
            stack: Vec::new(),
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

        let base = &node.base_id;

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
    pub fn check_tree(&self) -> bool {
        let mut prev_id: Option<Identifier> = None;
        let mut prev_offsets: Option<(u32, u32)> = None;
        for node in self.inorder_iter() {
            let curr_id = node.base_id.clone();
            let (lo, hi) = (node.offset, node.offset + node.size as u32);
            if let Some(prev) = prev_id {
                if curr_id.with_offset(lo) <= prev.with_offset(prev_offsets.unwrap().1-1) {
                    eprintln!("Tree check failed: current id {:?} with offsets {}-{} is not greater than previous id {:?} with offsets {}-{}", curr_id, lo, hi, prev, prev_offsets.unwrap().0, prev_offsets.unwrap().1);
                    return false;
                }
            }
            prev_id = Some(curr_id);
            prev_offsets = Some((lo, hi));
        }
        true
    }
}

/* 
Test cases for AVL TREE
*/

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::identifier::{Id};

//     fn make_id(x: u32) -> Id {
//         Identifier::new(vec![x])
//     }

//     fn check_avl(tree: &Tree, node: Option<usize>) -> (i32, u32) {
//         /* Check sorted Ids too! */

//         if let Some(idx) = node {
//             let n = &tree.nodes[idx];

//             let (lh, lc) = check_avl(tree, n.left);
//             let (rh, rc) = check_avl(tree, n.right);

//             // Height correctness
//             assert_eq!(n.height, 1 + lh.max(rh));

//             // AVL balance
//             assert!((rh - lh).abs() <= 1, "AVL violated at node {}", idx);

//             let sz = n.size as u32;
//             let stree_c = n.subtree_count as u32;

//             // Count correctness
//             assert_eq!(stree_c, sz + lc + rc);

//             (n.height, stree_c)
//         } else {
//             (0, 0)
//         }
//     }

//     /* Complete check AVL function which checks that IDs are sorted in the AVL tree as well as balance heights etc. */

//     fn collect_inorder(tree: &Tree) -> Vec<(String, u32, String)> {
//         tree.inorder_iter()
//             .map(|n| (n.base_id.to_string(), n.offset, n.content.clone()))
//             .collect()
//     }

//     #[test]
//     fn test_basic_append() {
//         let mut tree = Tree::new();

//         tree.insert_by_id(0, Identifier {id: vec![1,0,1]}, 0, "ABC".to_string());
//         tree.insert_by_id(0, Identifier {id: vec![1,0,1]}, 3, "DEF".to_string());
//         tree.insert_by_id(0, Identifier {id: vec![1,0,1]}, 6, "GHI".to_string());
        
//         let inorder = collect_inorder(&tree);
        
//         tree.print_tree();

//         assert_eq!(inorder, vec![
//             ("1.0.1".to_string(), 0, "ABCDEFGHI".to_string()),
//         ]);

//         check_avl(&tree, tree.root);
//     }

//     #[test]
//     fn test_basic_split_1() {
//         let mut tree = Tree::new();

//         tree.insert_by_id(0, Identifier {id: vec![1,0,1]}, 0, "Hello".to_string());
//         tree.insert_by_id(0, Identifier {id: vec![7,0,0]}, 0, "World".to_string());
//         tree.insert_by_id(0, Identifier {id: vec![1,0,1,2,3,4]}, 0, "AAAAAAA".to_string());
        
//         let inorder = collect_inorder(&tree);
        
//         tree.print_tree();

//         println!("{}", tree.read());

//         check_avl(&tree, tree.root);
//     }

//     #[test]
//     fn test_basic_split_2() {
//         let mut tree = Tree::new();

//         tree.insert_by_id(0, Identifier {id: vec![1,0,1]}, 0, "Hello".to_string());
//         // tree.print_tree();
//         tree.insert_by_id(0, Identifier {id: vec![7,0,0]}, 0, "World".to_string());
//         // tree.print_tree();
//         tree.insert_by_id(0, Identifier {id: vec![1,0,1,2,3,4]}, 0, "AAAAAAA".to_string());
//         // tree.print_tree();
//         tree.insert_by_id(0, Identifier {id: vec![1,0,1,2,3,4,2,1,5,7]}, 0, "p".to_string());
//         tree.print_tree();

//         let inorder = collect_inorder(&tree);
        
//         tree.print_tree();

//         println!("{}", tree.read());

//         check_avl(&tree, tree.root);
//     }

//     #[test]
//     fn test_split_in_middle() {
//         let mut tree = Tree::new();
//         tree.insert_by_id(0, Identifier { id: vec![5] }, 0, "ABCDE".to_string());
//         tree.insert_by_id(1, Identifier { id: vec![5, 2, 7] }, 0, "X".to_string());
//         assert_eq!(tree.read(), "ABCXDE");
//         check_avl(&tree, tree.root);
//     }

//     #[test]
//     fn test_split_near_beginning() {
//         let mut tree = Tree::new();
//         tree.insert_by_id(0, Identifier { id: vec![5] }, 0, "ABCDE".to_string());
//         tree.insert_by_id(1, Identifier { id: vec![5, 0, 9, 9] }, 0, "X".to_string());
//         assert_eq!(tree.read(), "AXBCDE");
//         check_avl(&tree, tree.root);
//     }

//         #[test]
//     fn test_split_near_end() {
//         let mut tree = Tree::new();
//         tree.insert_by_id(0, Identifier { id: vec![5] }, 0, "ABCDE".to_string());
//         tree.insert_by_id(1, Identifier { id: vec![5, 3, 9, 9] }, 0, "X".to_string());
//         assert_eq!(tree.read(), "ABCDXE");
//         check_avl(&tree, tree.root);
//     }

//     #[test]
//     fn test_two_successive_splits() {
//         let mut tree = Tree::new();
//         tree.insert_by_id(0, Identifier { id: vec![5] }, 0, "ABCDE".to_string());
//         tree.insert_by_id(1, Identifier { id: vec![5, 2, 7] }, 0, "X".to_string());
//         tree.insert_by_id(2, Identifier { id: vec![5, 1, 8] }, 0, "Y".to_string());
//         assert_eq!(tree.read(), "ABYCXDE");
//         check_avl(&tree, tree.root);
//     }

//     #[test]
//     fn test_three_concurrent_sites() {
//         let mut tree = Tree::new();
//         tree.insert_by_id(2, Identifier { id: vec![70] }, 0, "Rust".to_string());
//         tree.insert_by_id(0, Identifier { id: vec![10] }, 0, "Hello".to_string());
//         tree.insert_by_id(1, Identifier { id: vec![40] }, 0, "from".to_string());
//         assert_eq!(tree.read(), "HellofromRust");
//         check_avl(&tree, tree.root);
//     }

//     #[test]
//     fn test_avl_ascending_insertion() {
//         let mut tree = Tree::new();
//         for i in 0..10u32 {
//             tree.insert_by_id(
//                 0,
//                 Identifier { id: vec![i * 10 + 10] },
//                 0,
//                 char::from_u32('A' as u32 + i).unwrap().to_string(),
//             );
//         }
//         assert_eq!(tree.read(), "ABCDEFGHIJ");
//         check_avl(&tree, tree.root);
//         // AVL height should be at most 5 for 10 nodes
//         let height = tree.nodes[tree.root.unwrap()].height;
//         assert!(height <= 5, "Expected height ≤ 5, got {}", height);
//     }

//     #[test]
//     fn test_mixed_splits_and_prepend() {
//         let mut tree = Tree::new();
//         tree.insert_by_id(0, Identifier { id: vec![50] }, 0, "WXYZ".to_string());
//         tree.insert_by_id(1, Identifier { id: vec![50, 1, 9] }, 0, "M".to_string());
//         tree.insert_by_id(2, Identifier { id: vec![50, 0, 5] }, 0, "N".to_string());
//         tree.insert_by_id(0, Identifier { id: vec![10] }, 0, "START".to_string());
//         assert_eq!(tree.read(), "STARTWN XM YZ".replace(' ', ""));
//         // Written out explicitly so the intent is obvious:
//         assert_eq!(tree.read(), "STARTWNXMyz".replace("yz","YZ"));
//         // Unambiguous form:
//         assert_eq!(tree.read(), "STARTWN XMYZ".replace(" ",""));
//         check_avl(&tree, tree.root);
//     }

// }
