use std::collections::{HashSet, HashMap};

use crate::{delta::{Delta, WireDelta}};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Dot { 
    pub site: u32,
    pub b_idx: u32,
    pub seq: u32
}

#[derive(Clone, Debug)]
pub struct DotStore { 
    /* TODO: Compare Hashmap vs Ordered Tree */
    pub versions: HashMap<u32, u32>,
    pub missing: HashMap<u32, HashSet<u32>>,
    pub pending: HashMap<Vec<u32>, Vec<WireDelta>>,
}

impl DotStore { 
    pub fn new() -> Self {
        DotStore { versions: HashMap::new(), missing: HashMap::new(), pending: HashMap::new() }
    }

    pub fn record_delta(&mut self, op: &Delta) {
        // Update versions and missing for each dot in the delta
        // Note: The dot for each operation is the dot for the *first* ID in the operation. 
        // We need to increment by # of ids in the operation
        for (dot, _id, lo, hi) in &op.ids {
            let site_version = self.versions.entry(dot.site).or_insert(0);
            if dot.seq > *site_version + 1 {
                // There are missing dots
                let missing_set = self.missing.entry(dot.site).or_insert_with(HashSet::new);
                for seq in (*site_version + 1)..dot.seq {
                    missing_set.insert(seq);
                }
            }
            // Update the version to the highest seen dot
            *site_version = (*site_version).max(dot.seq + (hi - lo));
        }
    }

    pub fn is_recorded(&self, dot: &Dot) -> bool {
        // check if dot is <= versions[dot.site] AND not in missing[dot.site]
        let site_version = self.versions.get(&dot.site).copied().unwrap_or(0);
        if dot.seq <= site_version {
            if let Some(missing_set) = self.missing.get(&dot.site) {
                return !missing_set.contains(&dot.seq);
            } else {
                return true;
            }
        }
        false
    }

    pub fn add_to_pending(&mut self, op: WireDelta) {
        // println!("Adding op {:?} to pending at site {}", op, op.site);
        let id = op.ids.first().unwrap().1.clone();
        self.pending.entry(id).or_default().push(op);   
    }

    pub fn get_pending_for_id(&mut self, id: &[u32]) -> Vec<WireDelta> {
        self.pending.remove(id).unwrap_or_default()
    }

    pub fn clear(&mut self) {
        self.versions.clear();
        self.missing.clear();
        self.pending.clear();
    }

}