// use crate::identifier::Range;
    use crate::idtrie::{TrieId};

#[derive(Clone, Debug)]
pub struct Node {
    pub left: Option<usize>, 
    pub right: Option<usize>,
    pub content: String,
    pub height: i32,
    pub size: usize,
    pub subtree_count: usize, // subtree count is the number of chars in the subtree rooted at this node
    pub creator: u32, // replica id of the creator
    pub base_id: TrieId,
    pub offset: u32 // starting offset 
}

impl Node {
    pub fn new(content: String, base_id: TrieId, offset: u32, creator: u32) -> Self {
        let size = content.chars().count();
        Node { 
            left: None, 
            right: None, 
            content: content, 
            height: 1, 
            size: size, 
            subtree_count: size, 
            base_id: base_id,
            offset: offset,
            creator: creator
        }
    }
}

// pub struct BaseBlock {
//     pub base: TrieId, 
//     pub range: Range, 
//     pub creator: u32
// }