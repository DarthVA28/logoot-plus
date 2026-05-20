pub const MIN_VALUE: u32 = 0;
pub const MAX_VALUE: u32 = 100000;
pub const MAX_AGENTS: u32 = 1000;
pub type Range = (u32, u32);

pub struct Identifier {
    offset: u32,
    range: Range,
}