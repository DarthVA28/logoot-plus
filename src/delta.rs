use crate::dotstore::{Dot};

use crate::idarena::{IdArena, Identifier};
// use crate::identifier::Identifier;

#[derive(Copy, Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OperationType {
    Insert,
    Delete
}   

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delta { 
    pub op_type: OperationType,
    pub ids: Vec<(Dot, Identifier, u32, u32)>,
    pub payload: Option<String>,
    pub site: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WireDelta {
    pub op_type: OperationType,
    pub ids: Vec<(Dot, Vec<u32>, u32, u32)>,
    pub payload: Option<String>,
    pub site: u32,
}

impl Delta {
    pub fn to_wire(&self, arena: &IdArena) -> WireDelta {
        WireDelta {
            op_type: self.op_type,
            ids: self.ids.iter()
                .map(|(dot, id, lo, hi)| (dot.clone(), arena.get_slice_unchecked(*id).to_vec(), *lo, *hi))
                .collect(),
            payload: self.payload.clone(),
            site: self.site,
        }
    }
}