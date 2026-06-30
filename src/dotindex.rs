use ahash::AHashMap as HashMap;
use smallvec::SmallVec;

type RangeList = SmallVec<[(u32, u32, usize); 4]>; // (seq_lo, seq_hi, node_idx)

#[derive(Clone, Debug)]
pub struct DotIndex {
    replicas: HashMap<u32, Vec<RangeList>>,
}

impl DotIndex {
    pub fn new() -> Self {
        DotIndex { replicas: HashMap::new() }
    }

    pub fn clear(&mut self) {
        self.replicas.clear();
    }

    fn ensure_slot(&mut self, creator: u32, block_idx: u32) -> &mut RangeList {
        let blocks = self.replicas.entry(creator).or_default();
        let idx = block_idx as usize;
        if blocks.len() <= idx {
            blocks.resize(idx + 1, SmallVec::new());
        }
        &mut blocks[idx]
    }

    #[inline]
    pub fn lookup(&self, site: u32, block_idx: u32, seq: u32) -> Option<usize> {
        let blocks = self.replicas.get(&site)?;
        let ranges = blocks.get(block_idx as usize)?;
        for &(lo, hi, node_idx) in ranges {
            if seq >= lo && seq < hi {
                return Some(node_idx);
            }
        }
        None
    }

    pub fn on_block_inserted(&mut self, creator: u32, block_idx: u32, seq_lo: u32, seq_hi: u32, node_idx: usize) {
        self.ensure_slot(creator, block_idx).push((seq_lo, seq_hi, node_idx));
    }

    pub fn on_block_extended(&mut self, creator: u32, block_idx: u32, seq_lo: u32, new_hi: u32) {
        let ranges = self.ensure_slot(creator, block_idx);
        for entry in ranges.iter_mut() {
            if entry.0 == seq_lo {
                entry.1 = new_hi;
                return;
            }
        }
        panic!("extend: range starting at {} not found for site {} block {}", seq_lo, creator, block_idx);
    }

    pub fn on_block_split(&mut self, creator: u32, block_idx: u32, seq_lo: u32, split_seq: u32, new_node_idx: usize) {
        let ranges = self.ensure_slot(creator, block_idx);
        let mut old_hi = 0;
        let mut found = false;
        for entry in ranges.iter_mut() {
            if entry.0 == seq_lo {
                old_hi = entry.1;
                entry.1 = split_seq; // left half shrinks
                found = true;
                break;
            }
        }
        assert!(found, "split: not found");
        ranges.push((split_seq, old_hi, new_node_idx)); // right half
    }

    pub fn on_block_deleted(&mut self, creator: u32, block_idx: u32, seq_lo: u32) {
        let ranges = self.ensure_slot(creator, block_idx);
        if let Some(pos) = ranges.iter().position(|&(lo, _, _)| lo == seq_lo) {
            ranges.swap_remove(pos);
        }
    }

    pub fn on_block_truncated_start(&mut self, creator: u32, block_idx: u32, old_lo: u32, new_lo: u32) {
        let ranges = self.ensure_slot(creator, block_idx);
        for entry in ranges.iter_mut() {
            if entry.0 == old_lo {
                entry.0 = new_lo;
                return;
            }
        }
        panic!("trunc_start: not found");
    }

    pub fn on_block_truncated_end(&mut self, creator: u32, block_idx: u32, seq_lo: u32, new_hi: u32) {
        let ranges = self.ensure_slot(creator, block_idx);
        for entry in ranges.iter_mut() {
            if entry.0 == seq_lo {
                entry.1 = new_hi;
                return;
            }
        }
        panic!("trunc_end: not found");
    }

    pub fn on_node_remapped(&mut self, creator: u32, block_idx: u32, seq_lo: u32, new_node_idx: usize) {
        let ranges = self.ensure_slot(creator, block_idx);
        for entry in ranges.iter_mut() {
            if entry.0 == seq_lo {
                entry.2 = new_node_idx;
                return;
            }
        }
        panic!("remap: not found");
    }

    pub fn on_block_middle_deleted(
        &mut self, creator: u32, block_idx: u32, seq_lo: u32,
        left_end: u32, right_start: u32, right_end: u32, right_node: usize,
    ) {
        let ranges = self.ensure_slot(creator, block_idx);
        for entry in ranges.iter_mut() {
            if entry.0 == seq_lo {
                entry.1 = left_end; // shrink left half
                break;
            }
        }
        ranges.push((right_start, right_end, right_node)); // right half
    }

    pub fn total_ranges(&self) -> usize {
        self.replicas.values().map(|blocks| blocks.iter().map(|r| r.len()).sum::<usize>()).sum()
    }

    /// Returns all ranges in the given block that overlap [query_lo, query_hi),
    /// sorted by seq_lo. Gaps in coverage become pending deletes.
    pub fn overlapping_ranges(&self, site: u32, block_idx: u32, query_lo: u32, query_hi: u32) -> SmallVec<[(u32, u32, usize); 4]> {
        let mut result = SmallVec::new();
        if let Some(blocks) = self.replicas.get(&site) {
            if let Some(ranges) = blocks.get(block_idx as usize) {
                for &(lo, hi, node_idx) in ranges {
                    if lo < query_hi && hi > query_lo {
                        result.push((lo, hi, node_idx));
                    }
                }
            }
        }
        // Sort by lo so we can walk left-to-right and detect gaps
        result.sort_unstable_by_key(|&(lo, _, _)| lo);
        result
    }
}