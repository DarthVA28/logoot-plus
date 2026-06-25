// use std::collections::BTreeMap;
use btree_experiment::BTreeMap;
use core::panic;
use std::ops::Bound;

#[derive(Clone, Debug)]
pub struct DotIndex {
    ranges: BTreeMap<(u32, u32), (u32, usize)>,
}

impl DotIndex {
    pub fn new() -> Self {
        DotIndex { ranges: BTreeMap::new() }
    }

    pub fn clear(&mut self) {
        self.ranges.clear();
    }

    #[inline]
    pub fn lookup(&self, site: u32, seq: u32) -> Option<usize> {
        let (&(s, _), &(hi, node_idx)) = self.ranges.range(..=(site, seq)).next_back()?;
        if s == site && seq < hi { Some(node_idx) } else { None }
    }

    #[inline]
    pub fn lookup_range(&self, site: u32, seq: u32) -> Option<(u32, u32, usize)> {
        let (&(s, lo), &(hi, node_idx)) = self.ranges.range(..=(site, seq)).next_back()?;
        if s == site && seq < hi { Some((lo, hi, node_idx)) } else { None }
    }

    pub fn on_block_inserted(&mut self, creator: u32, seq_lo: u32, seq_hi: u32, node_idx: usize) {
        self.ranges.insert((creator, seq_lo), (seq_hi, node_idx));
    }

    pub fn on_block_extended(&mut self, creator: u32, seq_lo: u32, new_hi: u32) {
        let entry = self.ranges.get_mut(&(creator, seq_lo)).expect("extend: not found");
        entry.0 = new_hi;
    }

    pub fn on_block_split(&mut self, creator: u32, seq_lo: u32, split_seq: u32, new_node_idx: usize) {
        // Modify left half in place
        let entry = self.ranges.get_mut(&(creator, seq_lo))
            .expect("split: not found");
        let old_hi = entry.0;
        entry.0 = split_seq;
        // Insert right half using cursor — positioned near the key we just modified
        let mut cursor = self.ranges.upper_bound_mut(Bound::Included(&(creator, seq_lo)));
        let res =cursor.insert_after((creator, split_seq), (old_hi, new_node_idx));
        if res.is_err() {
            panic!("split: insert_after failed");
        }
    }

    pub fn on_block_deleted(&mut self, creator: u32, seq_lo: u32) {
        self.ranges.remove(&(creator, seq_lo));
    }

    pub fn on_block_truncated_start(&mut self, creator: u32, old_lo: u32, new_lo: u32) {
        let mut cursor = self.ranges.upper_bound_mut(Bound::Included(&(creator, old_lo)));
        let (_, (hi, node_idx)) = cursor.remove_prev()
            .expect("trunc_start: not found");
        let res = cursor.insert_before((creator, new_lo), (hi, node_idx));
        if res.is_err() {
            panic!("trunc_start: insert_before failed");
        }
    }

    pub fn on_block_truncated_end(&mut self, creator: u32, seq_lo: u32, new_hi: u32) {
        let entry = self.ranges.get_mut(&(creator, seq_lo)).expect("trunc_end: not found");
        entry.0 = new_hi;
    }

    pub fn on_node_remapped(&mut self, creator: u32, seq_lo: u32, new_node_idx: usize) {
        let entry = self.ranges.get_mut(&(creator, seq_lo)).expect("remap: not found");
        entry.1 = new_node_idx;
    }

    pub fn on_block_middle_deleted(
        &mut self, creator: u32, seq_lo: u32,
        left_end: u32, right_start: u32, right_end: u32, right_node: usize,
    ) {
        let entry = self.ranges.get_mut(&(creator, seq_lo)).expect("mid_del: not found");
        entry.0 = left_end;
        self.ranges.insert((creator, right_start), (right_end, right_node));
    }

    pub fn total_ranges(&self) -> usize { self.ranges.len() }
}