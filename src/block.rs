use crate::identifier::{Identifier};

/* A block owns base + [lo, hi) */
#[derive(Debug, Clone)]
pub struct Block {
    pub base: Identifier,
    pub lo: u32,
    pub hi: u32
}

impl Block {
    pub fn new(base: Identifier, lo: u32, hi: u32) -> Self {
        assert!(lo < hi, "lo must be less than hi");
        Block { base, lo, hi }
    }

    pub fn len(&self) -> u32 {
        self.hi - self.lo
    }

    pub fn id_begin(&self) -> Identifier {
        self.base.with_offset(self.lo)
    }

    pub fn id_end(&self) -> Identifier {
        self.base.with_offset(self.hi-1)
    }
}

pub enum BlockOrderingRelation {
    B1BeforeB2,
    B1AfterB2,
    B1InsideB2,
    B2InsideB1,
    B1ConcatB2,
    B2ConcatB1,
    B1EqualsB2
}

pub fn compare_blocks(b1: &Block, b2: &Block) -> BlockOrderingRelation {
    if b1.base.is_base_same(&b2.base) {
        if b1.lo == b2.lo && b1.hi == b2.hi {
            return BlockOrderingRelation::B1EqualsB2
        } else if b1.hi == b2.lo {
            return BlockOrderingRelation::B1ConcatB2
        } else if b2.hi == b1.lo {
            return BlockOrderingRelation::B2ConcatB1
        } else if b1.lo >= b2.lo && b1.hi <= b2.hi {
            return BlockOrderingRelation::B1InsideB2
        } else if b2.lo >= b1.lo && b2.hi <= b1.hi {
            return BlockOrderingRelation::B2InsideB1
        } else if b1.lo < b2.lo {
            return BlockOrderingRelation::B1BeforeB2
        } else {
            return BlockOrderingRelation::B1AfterB2
        }
    }

    // Different bases -- check if bases fall in each other's range
    let b1_start = b1.id_begin();
    let b1_end = b1.id_end();
    let b2_start = b2.id_begin();
    let b2_end = b2.id_end();

    if b1.base >= b2_start && b1.base <= b2_end {
        return BlockOrderingRelation::B1InsideB2
    } else if b2.base >= b1_start && b2.base <= b1_end {
        return BlockOrderingRelation::B2InsideB1
    } 
    
    if b1.base < b2.base {
        return BlockOrderingRelation::B1BeforeB2
    } else {
        return BlockOrderingRelation::B1AfterB2
    }
}

pub fn split_block(block: &Block, split_point: u32) -> (Block, Block) {
    assert!(split_point > block.lo && split_point < block.hi, "split_point must be within the block range");
    let left = Block::new(block.base.clone(), block.lo, split_point);
    let right = Block::new(block.base.clone(), split_point, block.hi);
    (left, right)
}