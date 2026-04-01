/* 
An implementation of Block Trees in the form of AVL Trees
*/
use std::{cmp::Ordering};
use crate::identifiers::{Id, Range}; 

fn cmp_key(node_base: &Id, node_offset: u32, search_base: &Id,search_offset: u32, node_size: u32) -> Ordering {
    match node_base.cmp(search_base) {
        Ordering::Less => Ordering::Less,
        Ordering::Greater => Ordering::Greater,
        Ordering::Equal => {
            if search_offset < node_offset {
                Ordering::Greater // go left
            } else if search_offset >= node_offset + node_size {
                Ordering::Less 
            } else {
                Ordering::Equal 
            }
        }
    }
}

#[derive(Clone)]
pub struct BlockNode { 
    /* indices of left and right children */
    left: Option<usize>,
    right: Option<usize>,
    /* content and size of the block */
    content: String, 
    size: u32,
    /* Order statistic tree metadata */
    count: u32, // total count of chars in the subtree 
    height: i32,
    /* index of the base block */
    base: usize, 
    offset: u32
}

impl BlockNode { 
    pub fn new(content: String, base: usize, offset: u32) -> Self { 
        let size = content.chars().count() as u32;
        BlockNode { left: None, right: None, content, size, count: size, height: 1, base, offset }
    }

    pub fn content(&self) -> &str {
        &self.content
    }
}

pub struct BaseBlock { 
    base: Id,
    range: Range,
    creator: u32
}

pub struct BlockTree { 
    nodes: Vec<BlockNode>,
    base_blocks: Vec<BaseBlock>,
    root: Option<usize>, 
    free_list: Vec<usize>
}

impl BlockTree { 
    pub fn new() -> Self { 
        BlockTree { root: None, nodes: Vec::new(), free_list: Vec::new(), base_blocks: Vec::new() }
    }

    pub fn is_empty(&self) -> bool { 
        self.root.is_none()
    }

    pub fn clear(&mut self) { 
        self.nodes.clear();
        self.base_blocks.clear();
        self.root = None;
        self.free_list.clear();
    }

    fn alloca(&mut self, node: BlockNode) -> usize { 
        match self.free_list.pop() {
            Some(idx) => { self.nodes[idx] = node; idx },
            None => { self.nodes.push(node); self.nodes.len() - 1 }
        }
    }

    fn free(&mut self, idx: usize) { 
        self.free_list.push(idx);
    }

    fn height(&self, node: Option<usize>) -> i32 { 
        node.map_or(0, |index| self.nodes[index].height)
    }

    fn subtree_count(&self, node: Option<usize>) -> u32 { 
        node.map_or(0, |index| self.nodes[index].count)
    }

    pub fn content(&self, node: usize) -> &str { 
        &self.nodes[node].content
    }

    pub fn size(&self, node: Option<usize>) -> u32 { 
        node.map_or(0, |index| self.nodes[index].size)
    }

    pub fn left_count(&self, node: Option<usize>) -> u32 { 
        node.map_or(0, |index| self.nodes[index].count)
    }

    pub fn creator(&self, node: usize) -> u32 { 
        self.base_blocks[self.nodes[node].base].creator
    }

    pub fn base(&self, node: usize) -> usize { 
        return self.nodes[node].base
    }

    pub fn base_id(&self, node: usize) -> &Id { 
        let base_idx = self.nodes[node].base;
        &self.base_blocks[base_idx].base
    }

    pub fn ranges(&self, node: usize) -> (u32, u32) { 
        let node = &self.nodes[node];
        let range_left = node.offset;
        let range_right = node.offset + node.size;
        (range_left, range_right)
    }

    pub fn base_offsets(&self, node: usize) -> (u32, u32) { 
        let node = &self.nodes[node];
        let base_block = &self.base_blocks[node.base];
        let base_left = base_block.range.0;
        let base_right = base_block.range.1;
        (base_left, base_right)
    }

    pub fn extend_content(&mut self, node: usize, text: &str) {
        let node = &mut self.nodes[node];
        node.content.push_str(text);
        let added_size = text.chars().count() as u32;
        node.size += added_size;
        node.count += added_size;
    }

    fn update_node(&mut self, idx: usize) {
        let left = self.nodes[idx].left;
        let right = self.nodes[idx].right;
        let lh = self.height(left);
        let rh = self.height(right);
        let lc = self.subtree_count(left);
        let rc = self.subtree_count(right);
        let node = &mut self.nodes[idx];
        node.height = 1 + lh.max(rh);
        node.count = node.size + lc + rc;
    }

    fn balance_factor(&self, node: usize) -> i32 { 
        let n = &self.nodes[node];
        self.height(n.right) - self.height(n.left)
    }

    /* Rotation Functions */

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
            let node = &self.nodes[i];

            // Update its children 
            if i+1 < path_len { 
                let old_child = path_to_root[i+1];
                if (node.left == Some(old_child)) {
                    self.nodes[idx].left = Some(curr);
                } else { 
                    self.nodes[idx].right = Some(curr);
                }
            }
            curr = self.avl_fix(idx);
        }
    }
 
    /* Find block by position function */
    pub fn find_by_position(&self, pos: u32) -> Option<Vec<usize>> { 
        let mut path_to_root: Vec<usize> = vec![]; 
        let nodes = &self.nodes;
        let mut i = self.root;
        let mut curr = pos;
        while let Some(index) = i { 
            let node: &BlockNode = &nodes[index];
            path_to_root.push(index);
            let left = node.left;
            let left_count = if let Some(left_index) = left {
                nodes[left_index].count
            } else {
                0
            };
            if curr < left_count {
                i = left;
            } else if curr < left_count + node.size {
                return Some(path_to_root);
            } else { 
                curr -= left_count + node.size;
                i = node.right;
            }
        }
        None
    }

    /* Find block by ID function */ 
    pub fn find_by_id(&self, base: &Id, offset: u32) -> Option<Vec<usize>> { 
        let mut path_to_root: Vec<usize> = vec![];
        let nodes = &self.nodes;
        let mut i = self.root;  
        while let Some(index) = i { 
            let node: &BlockNode = &nodes[index];
            path_to_root.push(index);
            // Check if bases match and offset is contained 
            let base_block = &self.base_blocks[node.base];
            let node_base = &base_block.base;
            match cmp_key(node_base, node.offset, base, offset, node.size) { 
                Ordering::Less => i = node.left,
                Ordering::Greater => i = node.right,
                Ordering::Equal => return Some(path_to_root)
            }
        }
        None
    }

    /* Public function to create a new base block */
    pub fn create_base_block(&mut self, base: Id, range: Range, creator: u32) -> usize {
        let idx = self.base_blocks.len();
        self.base_blocks.push(BaseBlock { base, range, creator });
        idx
    }

    /* Insert block function */
    pub fn insert(&mut self, content: String, base: usize, offset: u32) {
        let idx = self.alloca(BlockNode::new(content,base, offset));
        
        let Some(root) = self.root else {
            self.root = Some(idx);
            return;
        };

        let search_id = &self.base_blocks[base].base;
        let mut path: Vec<usize> = Vec::new();
        let mut cursor = Some(root);

        while let Some(curr) = cursor {
            path.push(curr);

            let node = &mut self.nodes[curr];
            let node_base = node.base;
            let node_offset = node.offset;
            let node_size = node.size;
            let node_left = node.left;
            let node_right = node.right;
            let node_base_id = &self.base_blocks[node_base].base;

            match cmp_key(node_base_id, node_offset, search_id, offset, node_size) {
                Ordering::Less => {
                    if node.left.is_none() {
                        node.left = Some(idx);
                        break;
                    }
                    cursor = node_left
                } 
                Ordering::Greater => {
                    if node.right.is_none() {
                        node.right = Some(idx);
                        break;
                    }
                    cursor = node_right
                }
                Ordering::Equal => {
                    // This should not happen in a well-formed tree
                    panic!("Duplicate block ID detected during insertion");
                }
            }
        }

        path.push(idx);
        self.rebalance(path);
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
    
    pub fn delete(&mut self, base_id: usize, offset: u32) -> Result<(), String> {
        let base = &self.base_blocks[base_id].base;
        if let Some(path) = self.find_by_id(base, offset) {
            let target = *path.last().unwrap();
            let target_node = &self.nodes[target];
            let left = target_node.left;
            let right = target_node.right;

            match (left, right) {
                (None, None) => {
                    // Leaf Node 
                    self.splice(&path, target, None);
                }

                (Some(child), None) | (None, Some(child)) => {
                    self.splice(&path, target, Some(child));
                }

                (Some(_), Some(r)) => {
                    let mut succ_path = path.clone();
                    succ_path.push(r);
                    let mut curr = r;

                    while let Some(l) = self.nodes[curr].left {
                        succ_path.push(l);
                        curr = l;
                    }

                    let succ = curr;
                    let succ_payload = self.nodes[succ].clone();
                    let tn = &mut self.nodes[target];
                    tn.content = succ_payload.content;
                    tn.base    = succ_payload.base;
                    tn.offset  = succ_payload.offset;
                    tn.size    = succ_payload.size;

                    let succ_right = self.nodes[succ].right;
                    self.splice(&succ_path, succ, succ_right);
                }
            }
            Ok(())
        } else { 
            return Err("Block to delete not found".into());
        }
    }

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

pub struct InOrderIter<'a> {
    tree: &'a BlockTree,
    stack: Vec<usize>,
    current: Option<usize>,
}

impl<'a> InOrderIter<'a> {
    pub fn new(tree: &'a BlockTree) -> Self {
        InOrderIter {
            tree,
            stack: Vec::new(),
            current: tree.root,
        }
    }
}

impl<'a> Iterator for InOrderIter<'a> {
    type Item = &'a BlockNode;

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

impl BlockTree {
    pub fn inorder_iter(&self) -> InOrderIter {
        InOrderIter::new(self)
    }
}

