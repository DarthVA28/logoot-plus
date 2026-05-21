use crate::idarena::IdBlock;

#[derive(Clone, Debug)]
pub struct Node {
    pub left: Option<usize>, 
    pub right: Option<usize>,
    pub content: String,
    pub height: i32,
    pub size: usize,
    pub subtree_count: usize, // subtree count is the number of chars in the subtree rooted at this node
    pub creator: u32, // replica id of the creator
    pub block: IdBlock, // the identifier block for this node
    pub marked: bool // whether this node can be extended or not, we set it to false if we delete from the edge of this node
}

impl Node {
    pub fn new(content: String, block: IdBlock, creator: u32) -> Self {
        let size = content.len();
        Node { 
            left: None, 
            right: None, 
            content: content, 
            height: 1, 
            size: size, 
            subtree_count: size, 
            block: block,
            creator: creator,
            marked: false,
        }
    }
}