use smallvec::SmallVec;
use crate::idarena::{Identifier};

/// Maximum skip list height
pub const MAX_HEIGHT: usize = 20;
pub const HEAD: usize = 0;

#[derive(Clone, Debug)]
pub struct Level {
    pub next: Option<usize>,
    /// Characters spanned from this node (inclusive) to `next` (exclusive).
    /// At level 0 this equals `node.size`.
    pub width: usize,
}
 
#[derive(Clone, Debug)]
pub struct Node {
    pub content: String,
    pub size: usize,
    pub base_id: Identifier,
    pub offset: u32,
    pub creator: u32,
    pub block_idx: u32,
    /// Level-0 backward pointer. `None` only for the sentinel.
    pub prev: Option<usize>,
    /// Forward pointers, one per level this node participates in.
    pub levels: SmallVec<[Level; 4]>,
}
 
impl Node {
    /// Create a data node with zero levels (levels assigned on insertion).
    pub fn new(content: String, base_id: Identifier, offset: u32, creator: u32, block_idx: u32) -> Self {
        let size = content.len();
        Node {
            content,
            size,
            base_id,
            offset,
            creator,
            block_idx,
            prev: None,
            levels: SmallVec::new(),
        }
    }
 
    /// Create the sentinel node with `MAX_HEIGHT` levels.
    pub fn sentinel() -> Self {
        let mut levels = SmallVec::new();
        for _ in 0..MAX_HEIGHT {
            levels.push(Level { next: None, width: 0 });
        }
        Node {
            content: String::new(),
            size: 0,
            base_id: Identifier::EMPTY,
            offset: 0,
            creator: u32::MAX,
            block_idx: u32::MAX,
            prev: None,
            levels,
        }
    }
}
