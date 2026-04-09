use crate::node::Node;
use crate::identifier::{Id, IdOrderingRelation, Identifier, IdentifierInterval, compare_intervals, num_insertable};

pub struct Tree {
    pub nodes: Vec<Node>, 
    pub root: Option<usize>,
    free_list: Vec<usize>
}

/* Basic helper functions */
impl Tree { 
    pub fn new() -> Self {
        Tree {
            root: None, 
            nodes: Vec::new(),
            free_list: Vec::new()
        }
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
        let n = &self.nodes[node];
        let offset_left = n.offset;
        let offset_right = n.offset + n.size as u32;
        (offset_left, offset_right)
    }

    pub fn node_get_identifier_interval(&self, node: usize) -> IdentifierInterval { 
        let base = self.nodes[node].base_id.clone();
        let offset = self.nodes[node].offset;
        IdentifierInterval::new(base, offset, offset + self.nodes[node].size as u32)
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
    fn next(&self, node: usize, path: &[usize]) -> Option<usize> {
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
    /// Insert the node by identifier  
    pub fn insert_by_id(&mut self, site: u32, base: Id, offset: u32, content: String) {
        let idx = self.alloca(Node::new(content.clone(), base.clone(), offset, site));
        if self.is_empty() {
            self.root = Some(idx);
            return;
        }
        let from = self.root.unwrap();
        let len = content.chars().count() as u32;
        let insert_interval = IdentifierInterval::new(base, offset, offset + len);
        return self.insert_rec(idx, insert_interval, from, content);
    }

    fn find_split_point(node_idi: &IdentifierInterval, id_insert: &Id) -> u32 {
        // println!("Finding split point for insert id {:?} in Id {:?} (offsets: {}-{})", id_insert, node_idi.base, node_idi.lo, node_idi.hi);
        let mut sp = 0;
        let text_len = node_idi.hi - node_idi.lo;
        for i in 0..text_len {
            let id_elem = node_idi.base.with_offset(node_idi.lo + i);
            if id_elem >= *id_insert {
                break;
            }
            sp+=1;
        }
        // println!("Split point found at offset {} ({} chars into the node)", sp, sp);
        return sp;
    }

    pub fn insert_rec(&mut self, node: usize, node_idi: IdentifierInterval, mut from: usize, content: String) {
        let mut path = vec![];
        let mut con = true;

        while con {
            path.push(from);
            
            // B1 is the block we are adding 
            // B2 is the block we are comparing with
            let b1 = &node_idi;
            let b2 = &self.node_get_identifier_interval(from);

            match compare_intervals(b1, &b2) {
                IdOrderingRelation::B1AfterB2 => {
                    // println!("B1 {:?} is after B2 {:?}, going right", b1.base, b2.base);
                    let from_node = &mut self.nodes[from];
                    if let Some(r) = from_node.right {
                        from = r;
                    } else {
                        from_node.right = Some(node);
                        con = false;
                    } 
                },
                IdOrderingRelation::B1BeforeB2 => {
                    // println!("B1 {:?} is before B2 {:?}, going left", b1.base, b2.base);
                    let from_node = &mut self.nodes[from];
                    if let Some(l) = from_node.left {
                        from = l;
                    } else {
                        from_node.left = Some(node);
                        con = false;    
                    }
                },
                IdOrderingRelation::B1InsideB2 => {
                    // println!("B1 {:?} is inside B2 {:?}, splitting", b1.base, b2.base);
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
                    // println!("B2 {:?} is a concat of B1 {:?}, trying to concat", b2.base, b1.base);
                    // Concat at the end
                    if let Some(r) = self.nodes[from].right {
                        // See how much we can insert before clashing with the next ID
                        let r_base = self.node_base_id(r);
                        let r_offset = self.node_ranges(r).0;
                        let len = content.chars().count() as u32;
                        let n_insertable= num_insertable(&node_idi.base.with_offset(node_idi.lo), &r_base.with_offset(r_offset), len);
                        if n_insertable < len {
                            // FIXME: just go right, don't bother splitting for now
                            from = r;
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
                    panic!("Oops...");
                },
                IdOrderingRelation::B1ConcatB2 => {
                    panic!("Oops 2, we never generate this operation")
                },
            }
        }
        self.rebalance(path);
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

}

/// Test cases for insert by ID 
#[cfg(test)]
mod tests {
    use super::*;
    use crate::identifier::{Id, Range};

    fn make_id(x: u32) -> Id {
        Identifier::new(vec![x])
    }

    fn check_avl(tree: &Tree, node: Option<usize>) -> (i32, u32) {
        /* Check sorted Ids too! */

        if let Some(idx) = node {
            let n = &tree.nodes[idx];

            let (lh, lc) = check_avl(tree, n.left);
            let (rh, rc) = check_avl(tree, n.right);

            // Height correctness
            assert_eq!(n.height, 1 + lh.max(rh));

            // AVL balance
            assert!((rh - lh).abs() <= 1, "AVL violated at node {}", idx);

            let sz = n.size as u32;
            let stree_c = n.subtree_count as u32;

            // Count correctness
            assert_eq!(stree_c, sz + lc + rc);

            (n.height, stree_c)
        } else {
            (0, 0)
        }
    }

    /* Complete check AVL function which checks that IDs are sorted in the AVL tree as well as balance heights etc. */

    fn collect_inorder(tree: &Tree) -> Vec<(String, u32, String)> {
        tree.inorder_iter()
            .map(|n| (n.base_id.to_string(), n.offset, n.content.clone()))
            .collect()
    }

    #[test]
    fn test_basic_append() {
        let mut tree = Tree::new();

        tree.insert_by_id(0, Identifier {id: vec![1,0,1]}, 0, "ABC".to_string());
        tree.insert_by_id(0, Identifier {id: vec![1,0,1]}, 3, "DEF".to_string());
        tree.insert_by_id(0, Identifier {id: vec![1,0,1]}, 6, "GHI".to_string());
        
        let inorder = collect_inorder(&tree);
        
        tree.print_tree();

        assert_eq!(inorder, vec![
            ("1.0.1".to_string(), 0, "ABCDEFGHI".to_string()),
        ]);

        check_avl(&tree, tree.root);
    }

    #[test]
    fn test_basic_split_1() {
        let mut tree = Tree::new();

        tree.insert_by_id(0, Identifier {id: vec![1,0,1]}, 0, "Hello".to_string());
        tree.insert_by_id(0, Identifier {id: vec![7,0,0]}, 0, "World".to_string());
        tree.insert_by_id(0, Identifier {id: vec![1,0,1,2,3,4]}, 0, "AAAAAAA".to_string());
        
        let inorder = collect_inorder(&tree);
        
        tree.print_tree();

        println!("{}", tree.read());

        check_avl(&tree, tree.root);
    }

    #[test]
    fn test_basic_split_2() {
        let mut tree = Tree::new();

        tree.insert_by_id(0, Identifier {id: vec![1,0,1]}, 0, "Hello".to_string());
        // tree.print_tree();
        tree.insert_by_id(0, Identifier {id: vec![7,0,0]}, 0, "World".to_string());
        // tree.print_tree();
        tree.insert_by_id(0, Identifier {id: vec![1,0,1,2,3,4]}, 0, "AAAAAAA".to_string());
        // tree.print_tree();
        tree.insert_by_id(0, Identifier {id: vec![1,0,1,2,3,4,2,1,5,7]}, 0, "p".to_string());
        tree.print_tree();

        let inorder = collect_inorder(&tree);
        
        tree.print_tree();

        println!("{}", tree.read());

        check_avl(&tree, tree.root);
    }

}

