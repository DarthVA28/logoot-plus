// use crate::identifier::Range;
    use crate::idarena::{Identifier};

#[derive(Clone, Debug)]
pub struct Node {
    pub left: Option<usize>, 
    pub right: Option<usize>,
    pub parent: Option<usize>,
    pub list_next: Option<usize>,
    pub list_prev: Option<usize>,
    pub height: i32,
    pub size: usize,
    pub subtree_count: usize, // subtree count is the number of chars in the subtree rooted at this node
    pub block_idx: u32,
    pub creator: u32, // replica id of the creator
    pub base_id: Identifier,
    pub offset: u32, // starting offset 
    pub content: String,
}

impl Node {
    pub fn new(content: String, base_id: Identifier, offset: u32, creator: u32, block_idx: u32) -> Self {
        let size = content.len();
        Node { 
            left: None, 
            right: None, 
            list_next: None,
            list_prev: None,
            content: content, 
            height: 1, 
            size: size, 
            subtree_count: size, 
            base_id: base_id,
            offset: offset,
            creator: creator,
            block_idx: block_idx,
            parent: None
        }
    }
}