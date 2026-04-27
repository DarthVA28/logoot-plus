use std::collections::{HashSet, HashMap};
use crate::identifier::Identifier;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OperationType {
    Insert,
    Delete
}   

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Operation { 
    pub op_type: OperationType,
    pub ids: Vec<(Identifier, u32, u32)>,
    pub payload: Option<String>,
    pub site: u32, 
    pub clock: u32
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OpId { 
    pub site: u32,
    pub clock: u32
}

#[derive(Clone, Debug)]
pub struct OpLog { 
    index: HashSet<OpId>, 
    v_clock: HashMap<u32, u32>,
    // pub pending: Vec<Operation>
    pub pending: HashMap<Identifier, Vec<Operation>>
}

impl OpLog { 
    pub fn new() -> Self {
        OpLog { index: HashSet::new(), v_clock: HashMap::new(), pending: HashMap::new() }
    }

    pub fn is_recorded(&self, op: &Operation) -> bool {
        let id = OpId { site: op.site, clock: op.clock };
        self.index.contains(&id)
    }
    
    pub fn record_op(&mut self, op: &Operation) {
        let id = OpId { site: op.site, clock: op.clock };
        self.index.insert(id);
        self.v_clock.insert(op.site, op.clock);
    }

    pub fn add_to_pending(&mut self, op: Operation) {
        // println!("Adding op {:?} to pending at site {}", op, op.site);
        let id = op.ids.first().unwrap().0.clone();
        self.pending.entry(id).or_default().push(op);   
    }

    pub fn get_pending_for_id(&mut self, id: &Identifier) -> Vec<Operation> {
        self.pending.remove(id).unwrap_or_default()
    }

    pub fn clear(&mut self) {
        self.index.clear();
        self.v_clock.clear();
        self.pending.clear();
    }
}