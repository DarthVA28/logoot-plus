/* 
An implementation of Block Trees in the form of AVL Trees
*/
use std::cmp::Ordering;
use crate::identifiers::{Id, Range, get_combined_id}; 

fn compare_ids(search_base: &Id, search_offset: u32, node_base: &Id, node_offset: u32) -> Ordering {
    let node_id = get_combined_id(node_base, node_offset);
    let search_id = get_combined_id(search_base, search_offset);
    search_id.cmp(&node_id)
}

/* FIXME */
/*
 Detailed bug description:: 
 - Find by ID is being used for 2 purposes simultaneously:
 - If a block with the same ID exists, we want to return it 
 - Else we want to return a block in which that ID is contained (needed for splitting etc)
 - These conflict in the search_id > node_id_end condition: if we put this requirement then we end up picking up _smaller_ blocks which are prefixes of our ID
 - If we relax this condition and make it search_id >= node_id then we miss out on the first case -- same ID does exist and we can extend it but we don't do it properly
 - TO BE FIXED!
*/
fn search_cmp_key(search_base: &Id, search_offset: u32, node_base: &Id, node_offset: u32, node_size: u32) -> Ordering {
    let node_id_start = get_combined_id(node_base, node_offset);
    let node_id_end = get_combined_id(node_base, node_offset + node_size);
    let search_id = get_combined_id(search_base, search_offset);

    if search_id < node_id_start {
        Ordering::Less
    } else if search_id > node_id_end {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

fn ins_cmp_key(search_base: &Id, search_offset: u32, node_base: &Id, node_offset: u32) -> Ordering {
    compare_ids(search_base, search_offset, node_base, node_offset)
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

#[derive(Clone)]
pub struct BaseBlock { 
    base: Id,
    range: Range,
    creator: u32
}

#[derive(Clone)]
pub struct BlockTree { 
    nodes: Vec<BlockNode>,
    base_blocks: Vec<BaseBlock>,
    pub root: Option<usize>, 
    free_list: Vec<usize>
}

pub enum DelLocation {
    Start, 
    End
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
        if node.is_none() { return 0; }
        let left= self.nodes[node.unwrap()].left;
        left.map_or(0, |index| self.nodes[index].count)
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

    pub fn extend_content(&mut self, node: usize, text: &str, path_to_root: &[usize]) {
        let node = &mut self.nodes[node];
        node.content.push_str(text);
        let added_size = text.chars().count() as u32;
        node.size += added_size;
        // update the offsets of the base 
        let base_block = &mut self.base_blocks[node.base];
        base_block.range.1 += added_size;
        for idx in path_to_root.iter().rev() {
            self.update_node(*idx);
        }
    }

    pub fn truncate_content(&mut self, node: usize, num_delete: u32, location: DelLocation, path_to_root: &[usize]) {
        let node = &mut self.nodes[node];
        let content_len = node.content.chars().count() as u32;
        match location {
            DelLocation::Start => {
                let new_content: String = node.content.chars().skip(num_delete as usize).collect();
                node.content = new_content;
            }
            DelLocation::End => {
                let new_content: String = node.content.chars().take((content_len - num_delete) as usize).collect();
                node.content = new_content;
            }
        }
        node.size -= num_delete;
        // update offsets 
        node.offset = match location {
            DelLocation::Start => node.offset + num_delete,
            DelLocation::End => node.offset
        };
        for idx in path_to_root.iter().rev() {
            self.update_node(*idx);
        }
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
 
    /* Find block by position function */
    pub fn find_by_position(&self, pos: u32) -> (Vec<usize>, u32) { 
        let mut path_to_root: Vec<usize> = vec![]; 
        let nodes = &self.nodes;
        let mut i = self.root;
        let mut curr = pos;
        let mut covered: u32 = 0;
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

    /* Find block by ID function */ 
    pub fn find_by_id(&self, base: &Id, offset: u32) -> Vec<usize> {
        if self.root.is_none() { return vec![]; } 
        let mut path_to_root: Vec<usize> = vec![];
        let nodes = &self.nodes;
        let mut i = self.root;  
        while let Some(index) = i { 
            let node: &BlockNode = &nodes[index];
            path_to_root.push(index);
            // Check if bases match and offset is contained 
            let base_block = &self.base_blocks[node.base];
            let node_base = &base_block.base;
            if *base == [11, 0, 1] {
                self.print_tree();  
            }
            match search_cmp_key(base, offset, node_base, node.offset, node.size) { 
                Ordering::Less => i = node.left,
                Ordering::Greater => i = node.right,
                Ordering::Equal => return path_to_root
            }
        }
        path_to_root
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
            let _node_size = node.size;
            let node_left = node.left;
            let node_right = node.right;
            let node_base_id = &self.base_blocks[node_base].base;

            match ins_cmp_key(search_id, offset, node_base_id, node_offset) {
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
                    println!("Tried to insert duplicate block with base {:?} and offset {}", search_id, offset);
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
        let path = self.find_by_id(base, offset);
        if !path.is_empty() {
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

    // Check the tree in terms of Ids
    pub fn check_tree(&self) -> bool {
        fn check_node(tree: &BlockTree, node_idx: Option<usize>, min_id: Option<(Id, u32)>, max_id: Option<(Id, u32)>) -> bool {
            if let Some(idx) = node_idx {
                let node = &tree.nodes[idx];
                let base_block = &tree.base_blocks[node.base];
                let _node_id = get_combined_id(&base_block.base, node.offset);

                // Check BST property
                if let Some((min_base, min_offset)) = &min_id {
                    if compare_ids(min_base, *min_offset, &base_block.base, node.offset) != Ordering::Less {
                        return false;
                    }
                }
                if let Some((max_base, max_offset)) = &max_id {
                    if compare_ids(max_base, *max_offset, &base_block.base, node.offset) != Ordering::Greater {
                        return false;
                    }
                }

                // Check left and right subtrees
                check_node(tree, node.left, min_id.clone(), Some((base_block.base.clone(), node.offset))) &&
                check_node(tree, node.right, Some((base_block.base.clone(), node.offset)), max_id.clone())
            } else {
                true
            }
        }

        check_node(self, self.root, None, None) 
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
    pub fn inorder_iter(&self) -> InOrderIter<'_> {
        InOrderIter::new(self)
    }
}

impl BlockTree {
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

        let base = &self.base_blocks[node.base].base;

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
            node.count,
            node.height,
            left,
            right,
            content,
            self.base_blocks[node.base].creator
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
}

/* Testing */

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::identifiers::{Id, Range};

//     fn make_id(x: u32) -> Id {
//         vec![x]
//     }

//     fn check_avl(tree: &BlockTree, node: Option<usize>) -> (i32, u32) {
//         if let Some(idx) = node {
//             let n = &tree.nodes[idx];

//             let (lh, lc) = check_avl(tree, n.left);
//             let (rh, rc) = check_avl(tree, n.right);

//             // Height correctness
//             assert_eq!(n.height, 1 + lh.max(rh));

//             // AVL balance
//             assert!((rh - lh).abs() <= 1, "AVL violated at node {}", idx);

//             // Count correctness
//             assert_eq!(n.count, n.size + lc + rc);

//             (n.height, n.count)
//         } else {
//             (0, 0)
//         }
//     }

//     fn collect_inorder(tree: &BlockTree) -> Vec<(usize, u32, String)> {
//         tree.inorder_iter()
//             .map(|n| (n.base, n.offset, n.content.clone()))
//             .collect()
//     }

//     #[test]
//     fn test_basic_insert_inorder() {
//         let mut tree = BlockTree::new();

//         let b0 = tree.create_base_block(make_id(1), (0, 100), 0);

//         tree.insert("A".into(), b0, 10);
//         tree.insert("B".into(), b0, 20);
//         tree.insert("C".into(), b0, 30);

//         let inorder = collect_inorder(&tree);

//         assert_eq!(inorder.len(), 3);
//         assert_eq!(inorder[0].1, 10);
//         assert_eq!(inorder[1].1, 20);
//         assert_eq!(inorder[2].1, 30);

//         check_avl(&tree, tree.root);
//     }

//     #[test]
//     fn test_find_by_position() {
//         let mut tree = BlockTree::new();
//         let b = tree.create_base_block(make_id(1), (0, 100), 0);

//         tree.insert("hello".into(), b, 0);   // len 5
//         tree.insert("world".into(), b, 5);   // len 5

//         // pos inside first block
//         let path = tree.find_by_position(2);
//         let node = *path.last().unwrap();
//         assert_eq!(tree.content(node), "hello");

//         // pos inside second block
//         let path = tree.find_by_position(7);
//         let node = *path.last().unwrap();
//         assert_eq!(tree.content(node), "world");

//         check_avl(&tree, tree.root);
//     }

//     #[test]
//     fn test_find_by_id() {
//         let mut tree = BlockTree::new();
//         let b = tree.create_base_block(make_id(42), (0, 100), 0);

//         tree.insert("abc".into(), b, 10);

//         let id = tree.base_blocks[b].base.clone();
//         let path = tree.find_by_id(&id, 10);

//         assert!(!path.is_empty());
//         let node = *path.last().unwrap();
//         assert_eq!(tree.content(node), "abc");
//     }

//     #[test]
//     fn test_next_prev() {
//         let mut tree = BlockTree::new();
//         let b = tree.create_base_block(make_id(1), (0, 100), 0);

//         tree.insert("A".into(), b, 10);
//         tree.insert("B".into(), b, 20);
//         tree.insert("C".into(), b, 30);

//         let id = tree.base_blocks[b].base.clone();
//         let path = tree.find_by_id(&id, 20);
//         let node = *path.last().unwrap();

//         let next = tree.next(node, &path).unwrap();
//         assert_eq!(tree.content(next), "C");

//         let prev = tree.prev(node, &path).unwrap();
//         assert_eq!(tree.content(prev), "A");
//     }

//     #[test]
//     fn test_delete_cases() {
//         let mut tree = BlockTree::new();
//         let b = tree.create_base_block(make_id(1), (0, 100), 0);

//         // build tree
//         tree.insert("A".into(), b, 10);
//         tree.insert("B".into(), b, 20);
//         tree.insert("C".into(), b, 30);

//         // delete leaf
//         tree.delete(b, 30).unwrap();

//         // delete node with one child
//         tree.delete(b, 20).unwrap();

//         // delete root
//         tree.delete(b, 10).unwrap();

//         assert!(tree.is_empty());

//         check_avl(&tree, tree.root);
//     }

//     #[test]
//     fn test_random_stress() {
//         use rand::RngExt;

//         // let mut rng = rand::make_rng();
//         let mut tree = BlockTree::new();

//         let b = tree.create_base_block(make_id(1), (0, 10_000), 0);

//         let mut offsets = vec![];

//         // insert random blocks
//         for _ in 0..200 {
//             let offset = rand::rng().random_range
//             (0..10_000);

//             // avoid duplicates
//             if offsets.contains(&offset) {
//                 continue;
//             }

//             offsets.push(offset);
//             tree.insert("x".into(), b, offset);
//         }

//         // sort ground truth
//         offsets.sort();

//         let inorder: Vec<u32> = tree
//             .inorder_iter()
//             .map(|n| n.offset)
//             .collect();

//         assert_eq!(offsets, inorder);

//         // validate AVL + counts
//         check_avl(&tree, tree.root);

//         // random deletions
//         for offset in offsets.iter().take(50) {
//             tree.delete(b, *offset).unwrap();
//             check_avl(&tree, tree.root);
//         }
//     }
// }

