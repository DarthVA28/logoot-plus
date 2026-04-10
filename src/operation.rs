use std::collections::{HashSet, HashMap};

use crate::identifier::Id;

#[derive(Clone, Debug, PartialEq, Eq)]

pub enum OperationType {
    Insert,
    Delete
}   

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Operation { 
    pub op_type: OperationType,
    pub ids: Vec<(Id, Vec<u32>)>,
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
    pub ops: Vec<Operation>, 
    index: HashSet<OpId>, 
    v_clock: HashMap<u32, u32>,
    pending: Vec<Operation>
}

impl OpLog { 
    pub fn new() -> Self {
        OpLog { ops: vec![], index: HashSet::new(), v_clock: HashMap::new(), pending: vec![] }
    }

    pub fn is_recorded(&self, op: &Operation) -> bool {
        let id = OpId { site: op.site, clock: op.clock };
        self.index.contains(&id)
    }

    pub fn is_ready(&self, op: &Operation) -> bool {
        let clk = self.v_clock.get(&op.site).unwrap_or(&1);
        op.clock <= clk + 1
    }

    pub fn record_op(&mut self, op: Operation) {
        let id = OpId { site: op.site, clock: op.clock };
        self.index.insert(id);
        self.v_clock.insert(op.site, op.clock);
        self.ops.push(op);
        self.drain_pending();
    }

    pub fn add_pending(&mut self, op: Operation) {
        self.pending.push(op);
    }   

    pub fn drain_pending(&mut self) -> Vec<Operation> {
        let mut ready_ops = vec![];
        let mut still_pending = vec![];
        let _changed = false;
        for op in self.pending.drain(..) {
            let id = OpId { site: op.site, clock: op.clock };
            if self.index.contains(&id) {
                continue;
            }
            let clk = self.v_clock.get(&op.site).unwrap_or(&1);
            if op.clock == clk + 1 {
                self.index.insert(id);
                self.v_clock.insert(op.site, op.clock);
                ready_ops.push(op);
            } else {
                still_pending.push(op);
            }
        }

        self.pending = still_pending;
        ready_ops
    }

    pub fn clear(&mut self) {
        self.ops.clear();
        self.index.clear();
        self.v_clock.clear();
        self.pending.clear();
    }
}

