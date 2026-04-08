#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identifier {
    pub id: Vec<u32>,
    pub is_base: bool,
}

impl Identifier {
    pub fn new(base: Vec<u32>) -> Self {
        Identifier { id: base, is_base: false }
    }

    pub fn with_offset(&self, offset: u32) -> Identifier {
        if !self.is_base {
            panic!("Cannot create an identifier with offset from a non-base identifier");
        }
        let mut new_id: Vec<u32> = self.id.clone();
        new_id.push(offset);
        Identifier { id: new_id, is_base: false }
    }

    pub fn is_base_same(&self, other: &Identifier) -> bool {
        return self.id == other.id;
    }
}

impl PartialOrd for Identifier {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Identifier {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}
