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
    pub ids: Vec<(Identifier, Vec<u32>)>,
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
    pub pending: Vec<Operation>
}

impl OpLog { 
    pub fn new() -> Self {
        OpLog { index: HashSet::new(), v_clock: HashMap::new(), pending: vec![] }
    }

    pub fn is_recorded(&self, op: &Operation) -> bool {
        let id = OpId { site: op.site, clock: op.clock };
        self.index.contains(&id)
    }

    pub fn is_ready(&self, op: &Operation) -> bool {
        let clk = self.v_clock.get(&op.site).unwrap_or(&1);
        op.clock <= clk + 1
    }

    pub fn record_op(&mut self, op: &Operation) {
        let id = OpId { site: op.site, clock: op.clock };
        self.index.insert(id);
        self.v_clock.insert(op.site, op.clock);
    }

    pub fn add_pending(&mut self, op: Operation) {
        self.pending.push(op);
    }   

    pub fn drain_pending(&mut self) -> Vec<Operation> {
        let mut ready = vec![];
        loop {
            let candidates = std::mem::take(&mut self.pending);
            let mut found = false;

            for op in candidates {
                if self.index.contains(&OpId { site: op.site, clock: op.clock }) {
                    continue; // duplicate
                }
                if self.is_ready(&op) {
                    self.record_op(&op);
                    ready.push(op);
                    found = true;
                } else {
                    self.pending.push(op); // re-queue for next pass
                }
            }
            if !found { break; }
        }
        ready
    }

    pub fn clear(&mut self) {
        self.index.clear();
        self.v_clock.clear();
        self.pending.clear();
    }
}