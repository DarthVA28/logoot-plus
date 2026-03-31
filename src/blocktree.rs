/* 
An implementation of Block Trees in the form of AVL Trees
*/

use std::{cell::RefCell, rc::Rc};

use rand::seq::index;

type Id = Vec<u32>; 
type Range = (u32, u32);

use std::cmp::Ordering;

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
    free_list: Vec<u32>
}

impl BlockTree { 
    pub fn new() -> Self { 
        BlockTree { root: None, nodes: Vec::new(), free_list: Vec::new(), base_blocks: Vec::new() }
    }

    pub fn is_empty(&self) -> bool { 
        self.root.is_none()
    }
}

/* Find block by position function */
fn find_by_position(tree: &BlockTree, pos: u32) -> Option<Vec<usize>> { 
    let mut path_to_root: Vec<usize> = vec![]; 
    let nodes = &tree.nodes;
    let mut i = tree.root;
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
            curr -= (left_count + node.size);
            i = node.right;
        }
    }
    None
}

fn find_by_id(tree: &BlockTree, base: &Id, offset: u32) -> Option<Vec<usize>> { 
    let mut path_to_root: Vec<usize> = vec![];
    let nodes = &tree.nodes;
    let mut i = tree.root;  
    while let Some(index) = i { 
        let node: &BlockNode = &nodes[index];
        path_to_root.push(index);
        // Check if bases match and offset is contained 
        let base_block = &tree.base_blocks[node.base];
        let node_base = &base_block.base;
        match cmp_key(node_base, node.offset, base, offset, node.size) { 
            Ordering::Less => i = node.left,
            Ordering::Greater => i = node.right,
            Ordering::Equal => return Some(path_to_root)
        }
    }
    None
}