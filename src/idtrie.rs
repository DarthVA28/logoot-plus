///! Prefix-trie representation for LogootSplit identifiers.
///!
///! Instead of storing each identifier as a full `Arc<[u32]>` path, we store
///! them in a shared prefix trie.  An identifier becomes a lightweight
///! `TrieId` (a u32 index + small prefix cache) and all comparison,
///! hashing, and path operations go through the central `IdentifierTrie`.

use std::cmp::Ordering;
use ahash::AHashMap as HashMap;

// ───────────────────────── Constants ──────────────────────────

const NO_PARENT: u32 = u32::MAX;
const PREFIX_CACHE_SIZE: usize = 2;

pub const MIN_VALUE: u32 = 0;
pub const MAX_VALUE: u32 = 100000;

pub type Range = (u32, u32);

// ───────────────────────── TrieNode (internal) ───────────────

#[derive(Clone, Debug)]
struct TrieNode {
    value: u32,
    parent: u32,
    depth: u16,
}

// ───────────────────────── TrieId (replaces Identifier) ──────

#[derive(Clone, Copy, Debug)]
pub struct TrieId {
    pub node: u32,
    pub depth: u16,
    prefix_len: u8,
    prefix: [u32; PREFIX_CACHE_SIZE],
}

impl TrieId {
    pub const EMPTY: TrieId = TrieId {
        node: NO_PARENT,
        depth: 0,
        prefix_len: 0,
        prefix: [0; PREFIX_CACHE_SIZE],
    };

    #[inline]
    pub fn is_empty(self) -> bool {
        self.node == NO_PARENT
    }
}

impl PartialEq for TrieId {
    #[inline]
    fn eq(&self, other: &Self) -> bool { self.node == other.node }
}
impl Eq for TrieId {}

impl std::hash::Hash for TrieId {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) { self.node.hash(state); }
}

// ───────────────────────── TrieIdRef (replaces IdentifierRef) ─

#[derive(Clone, Copy, Debug)]
pub struct TrieIdRef {
    pub base: TrieId,
    pub extra: u32,
}

impl TrieIdRef {
    #[inline]
    pub fn new(base: TrieId, extra: u32) -> Self { TrieIdRef { base, extra } }

    pub fn doc_start() -> Self { TrieIdRef { base: TrieId::EMPTY, extra: MIN_VALUE } }
    pub fn doc_end()   -> Self { TrieIdRef { base: TrieId::EMPTY, extra: MAX_VALUE } }
}

// ───────────────────────── IdentifierInterval ─────────────────

#[derive(Clone, Copy, Debug)]
pub struct IdentifierInterval {
    pub base: TrieId,
    pub lo: u32,
    pub hi: u32,
}

impl IdentifierInterval {
    pub fn new(base: TrieId, lo: u32, hi: u32) -> Self {
        IdentifierInterval { base, lo, hi }
    }

    pub fn id_begin(&self) -> TrieIdRef { TrieIdRef::new(self.base, self.lo) }
    pub fn id_end(&self)   -> TrieIdRef { TrieIdRef::new(self.base, self.hi - 1) }
}

// ───────────────────────── IdOrderingRelation ─────────────────

pub enum IdOrderingRelation {
    B1BeforeB2,
    B1AfterB2,
    B1InsideB2,
    B2InsideB1,
    B1ConcatB2,
    B2ConcatB1,
    B1EqualsB2,
}

// ───────────────────────── IdentifierTrie ─────────────────────

#[derive(Clone, Debug)]
pub struct IdentifierTrie {
    nodes: Vec<TrieNode>,
    children: HashMap<(u32, u32), u32>,
}

impl IdentifierTrie {
    pub fn new() -> Self {
        IdentifierTrie {
            nodes: Vec::with_capacity(1024),
            children: HashMap::with_capacity(1024),
        }
    }

    pub fn clear(&mut self) {
        self.nodes.clear();
        self.children.clear();
    }

    // ── Insertion ────────────────────────────────────────────

    pub fn insert_path(&mut self, path: &[u32]) -> TrieId {
        if path.is_empty() { return TrieId::EMPTY; }
        let mut parent = NO_PARENT;
        let mut node_idx = NO_PARENT;
        for (i, &value) in path.iter().enumerate() {
            let key = (parent, value);
            node_idx = match self.children.get(&key) {
                Some(&existing) => existing,
                None => {
                    let idx = self.nodes.len() as u32;
                    self.nodes.push(TrieNode { value, parent, depth: (i + 1) as u16 });
                    self.children.insert(key, idx);
                    idx
                }
            };
            parent = node_idx;
        }
        self.make_trie_id(node_idx)
    }

    pub fn extend(&mut self, base: TrieId, value: u32) -> TrieId {
        let parent = if base.is_empty() { NO_PARENT } else { base.node };
        let key = (parent, value);
        let node_idx = match self.children.get(&key) {
            Some(&existing) => existing,
            None => {
                let depth = if base.is_empty() { 1 } else { base.depth + 1 };
                let idx = self.nodes.len() as u32;
                self.nodes.push(TrieNode { value, parent, depth });
                self.children.insert(key, idx);
                idx
            }
        };
        self.make_trie_id(node_idx)
    }

    fn make_trie_id(&self, node_idx: u32) -> TrieId {
        if node_idx == NO_PARENT { return TrieId::EMPTY; }
        let depth = self.nodes[node_idx as usize].depth;
        let mut prefix = [0u32; PREFIX_CACHE_SIZE];
        let prefix_len = (depth as usize).min(PREFIX_CACHE_SIZE);
        if prefix_len > 0 {
            let target = self.ancestor_at_depth(node_idx, prefix_len as u16);
            let mut cur = target;
            for i in (0..prefix_len).rev() {
                prefix[i] = self.nodes[cur as usize].value;
                cur = self.nodes[cur as usize].parent;
            }
        }
        TrieId { node: node_idx, depth, prefix_len: prefix_len as u8, prefix }
    }

    // ── Ancestry ─────────────────────────────────────────────

    fn ancestor_at_depth(&self, mut node_idx: u32, target_depth: u16) -> u32 {
        let mut d = self.nodes[node_idx as usize].depth;
        while d > target_depth {
            node_idx = self.nodes[node_idx as usize].parent;
            d -= 1;
        }
        node_idx
    }

    fn value_at_depth(&self, node_idx: u32, target_depth: u16) -> u32 {
        self.nodes[self.ancestor_at_depth(node_idx, target_depth) as usize].value
    }

    // ── compare_ids ──────────────────────────────────────────

    pub fn compare_ids(&self, a: TrieId, b: TrieId) -> Ordering {
        if a.node == b.node { return Ordering::Equal; }
        if a.is_empty() && b.is_empty() { return Ordering::Equal; }
        if a.is_empty() { return Ordering::Less; }
        if b.is_empty() { return Ordering::Greater; }

        let common_prefix = (a.prefix_len as usize).min(b.prefix_len as usize);
        for i in 0..common_prefix {
            match a.prefix[i].cmp(&b.prefix[i]) {
                Ordering::Equal => {}
                ord => return ord,
            }
        }
        self.compare_nodes(a.node, a.depth, b.node, b.depth)
    }

    fn compare_nodes(&self, a: u32, da: u16, b: u32, db: u16) -> Ordering {
        let (mut wa, mut wb) = match da.cmp(&db) {
            Ordering::Greater => (self.ancestor_at_depth(a, db), b),
            Ordering::Less    => (a, self.ancestor_at_depth(b, da)),
            Ordering::Equal   => (a, b),
        };
        if wa == wb {
            return da.cmp(&db); // shorter path < longer path
        }
        while self.nodes[wa as usize].parent != self.nodes[wb as usize].parent {
            wa = self.nodes[wa as usize].parent;
            wb = self.nodes[wb as usize].parent;
        }
        self.nodes[wa as usize].value.cmp(&self.nodes[wb as usize].value)
    }

    // ── compare_refs ─────────────────────────────────────────

    pub fn compare_refs(&self, a: TrieIdRef, b: TrieIdRef) -> Ordering {
        if a.base == b.base { return a.extra.cmp(&b.extra); }
        if a.base.is_empty() && b.base.is_empty() { return a.extra.cmp(&b.extra); }
        if a.base.is_empty() {
            return self.compare_single_vs_extended(a.extra, b.base.node, b.base.depth);
        }
        if b.base.is_empty() {
            return self.compare_single_vs_extended(b.extra, a.base.node, a.base.depth).reverse();
        }

        let common_prefix = (a.base.prefix_len as usize).min(b.base.prefix_len as usize);
        for i in 0..common_prefix {
            match a.base.prefix[i].cmp(&b.base.prefix[i]) {
                Ordering::Equal => {}
                ord => return ord,
            }
        }

        let da = a.base.depth;
        let db = b.base.depth;
        let min_base_depth = da.min(db);

        if min_base_depth > 0 {
            let wa = self.ancestor_at_depth(a.base.node, min_base_depth);
            let wb = self.ancestor_at_depth(b.base.node, min_base_depth);
            if wa != wb {
                return self.compare_nodes(wa, min_base_depth, wb, min_base_depth);
            }
        }

        match da.cmp(&db) {
            Ordering::Equal => a.extra.cmp(&b.extra),
            Ordering::Less => {
                let b_val = self.value_at_depth(b.base.node, da + 1);
                match a.extra.cmp(&b_val) {
                    Ordering::Equal => Ordering::Less,
                    ord => ord,
                }
            }
            Ordering::Greater => {
                let a_val = self.value_at_depth(a.base.node, db + 1);
                match a_val.cmp(&b.extra) {
                    Ordering::Equal => Ordering::Greater,
                    ord => ord,
                }
            }
        }
    }

    fn compare_single_vs_extended(&self, single: u32, node: u32, _depth: u16) -> Ordering {
        let first = self.value_at_depth(node, 1);
        match single.cmp(&first) {
            Ordering::Equal => Ordering::Less,
            ord => ord,
        }
    }

    // ── Interval comparison ──────────────────────────────────

    #[inline(always)]
    pub fn compare_intervals_raw(
        &self,
        b1_base: TrieId, b1_lo: u32, b1_hi: u32,
        b2_base: TrieId, b2_lo: u32, b2_hi: u32,
    ) -> IdOrderingRelation {
        if b1_base == b2_base {
            if b1_lo == b2_lo && b1_hi == b2_hi {
                return IdOrderingRelation::B1EqualsB2;
            } else if b1_hi == b2_lo {
                return IdOrderingRelation::B1ConcatB2;
            } else if b2_hi == b1_lo {
                return IdOrderingRelation::B2ConcatB1;
            } else if b1_lo >= b2_lo && b1_hi <= b2_hi {
                return IdOrderingRelation::B1InsideB2;
            } else if b2_lo >= b1_lo && b2_hi <= b1_hi {
                return IdOrderingRelation::B2InsideB1;
            } else if b1_lo < b2_lo {
                return IdOrderingRelation::B1BeforeB2;
            } else {
                return IdOrderingRelation::B1AfterB2;
            }
        }

        let b1_begin = TrieIdRef::new(b1_base, b1_lo);
        let b1_end   = TrieIdRef::new(b1_base, b1_hi - 1);
        let b2_begin = TrieIdRef::new(b2_base, b2_lo);
        let b2_end   = TrieIdRef::new(b2_base, b2_hi - 1);

        if self.compare_refs(b1_begin, b2_begin) == Ordering::Less
            && self.compare_refs(b2_begin, b1_end) == Ordering::Less
        {
            return IdOrderingRelation::B2InsideB1;
        }
        if self.compare_refs(b2_begin, b1_begin) == Ordering::Less
            && self.compare_refs(b1_begin, b2_end) == Ordering::Less
        {
            return IdOrderingRelation::B1InsideB2;
        }
        if self.compare_refs(b1_begin, b2_begin) == Ordering::Less {
            IdOrderingRelation::B1BeforeB2
        } else {
            IdOrderingRelation::B1AfterB2
        }
    }

    #[inline(always)]
    pub fn compare_intervals(&self, b1: &IdentifierInterval, b2: &IdentifierInterval) -> IdOrderingRelation {
        self.compare_intervals_raw(b1.base, b1.lo, b1.hi, b2.base, b2.lo, b2.hi)
    }

    // ── num_insertable ───────────────────────────────────────

    pub fn num_insertable(&self, id_insert: TrieIdRef, id_next: TrieIdRef, length: u32) -> u32 {
        let insert_path = self.get_path(id_insert.base);
        let next_path = self.get_path(id_next.base);
        let l = insert_path.len();
        if l >= next_path.len() + 1 { return length; }
        let next_full_iter = next_path.iter().chain(std::iter::once(&id_next.extra));
        for (a, b) in insert_path.iter().zip(next_full_iter) {
            if a != b { return length; }
        }
        let next_at_l = if l < next_path.len() { next_path[l] } else { id_next.extra };
        next_at_l + 1 - id_insert.extra
    }

    // ── find_split_point ─────────────────────────────────────

    pub fn find_split_point(&self, idi_short: &IdentifierInterval, id_long: TrieId) -> u32 {
        let long_path = self.get_path(id_long);
        let text_len = idi_short.hi - idi_short.lo;
        let mut sp = 0;
        for i in 0..text_len {
            let ref_i = TrieIdRef::new(idi_short.base, idi_short.lo + i);
            if self.ref_cmp_slice(ref_i, &long_path) != Ordering::Less {
                break;
            }
            sp += 1;
        }
        sp
    }

    // ── Path materialisation ─────────────────────────────────

    pub fn get_path(&self, id: TrieId) -> Vec<u32> {
        if id.is_empty() { return Vec::new(); }
        let mut path = Vec::with_capacity(id.depth as usize);
        let mut cur = id.node;
        while cur != NO_PARENT {
            path.push(self.nodes[cur as usize].value);
            cur = self.nodes[cur as usize].parent;
        }
        path.reverse();
        path
    }

    pub fn from_slice(&mut self, slice: &[u32]) -> TrieId {
        self.insert_path(slice)
    }

    pub fn to_string(&self, id: TrieId) -> String {
        self.get_path(id).iter().map(|x| x.to_string()).collect::<Vec<_>>().join(".")
    }

    pub fn ref_cmp_slice(&self, r: TrieIdRef, other: &[u32]) -> Ordering {
        let path = self.get_path(r.base);
        path.iter().chain(std::iter::once(&r.extra)).cmp(other.iter())
    }

    pub fn node_count(&self) -> usize { self.nodes.len() }
}

// ───────────────────────── generate_base ──────────────────────

use crate::state::State;
use rand::RngExt;

pub fn generate_base_trie(
    trie: &mut IdentifierTrie,
    id_low: TrieIdRef,
    id_high: TrieIdRef,
    state: &mut State,
) -> TrieId {
    let low_path = {
        let mut p = trie.get_path(id_low.base);
        p.push(id_low.extra);
        p
    };
    let high_path = {
        let mut p = trie.get_path(id_high.base);
        p.push(id_high.extra);
        p
    };

    let mut new_path: Vec<u32> = Vec::new();
    let mut low_iter = low_path.iter().copied();
    let mut high_iter = high_path.iter().copied();

    let mut l = low_iter.next().unwrap_or(MIN_VALUE);
    let mut h = high_iter.next().unwrap_or(MAX_VALUE);

    while (h as i32) - (l as i32) < 2 {
        new_path.push(l);
        l = low_iter.next().unwrap_or(MIN_VALUE);
        h = high_iter.next().unwrap_or(MAX_VALUE);
    }

    let nxt = state.rng.random_range(l + 1..h);
    new_path.push(nxt);
    new_path.push(state.replica);
    new_path.push(state.local_clock);

    trie.insert_path(&new_path)
}

// ───────────────────────── Tests ──────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_deduplication() {
        let mut trie = IdentifierTrie::new();
        let a = trie.insert_path(&[5, 12, 3, 99]);
        let b = trie.insert_path(&[5, 12, 3, 99]);
        assert_eq!(a, b);
        assert_eq!(trie.node_count(), 4);
        let c = trie.insert_path(&[5, 12, 3, 120]);
        assert_ne!(a, c);
        assert_eq!(trie.node_count(), 5);
    }

    #[test]
    fn test_get_path() {
        let mut trie = IdentifierTrie::new();
        let id = trie.insert_path(&[10, 20, 30]);
        assert_eq!(trie.get_path(id), vec![10, 20, 30]);
        assert_eq!(trie.get_path(TrieId::EMPTY), Vec::<u32>::new());
    }

    #[test]
    fn test_compare_ids_basic() {
        let mut trie = IdentifierTrie::new();
        let a = trie.insert_path(&[1, 2, 3]);
        let b = trie.insert_path(&[1, 2, 3]);
        assert_eq!(trie.compare_ids(a, b), Ordering::Equal);

        let c = trie.insert_path(&[1, 2, 4]);
        assert_eq!(trie.compare_ids(a, c), Ordering::Less);

        let d = trie.insert_path(&[1, 2]);
        assert_eq!(trie.compare_ids(d, a), Ordering::Less); // prefix < longer

        let e = trie.insert_path(&[5, 99, 1]);
        let f = trie.insert_path(&[10, 1, 1]);
        assert_eq!(trie.compare_ids(e, f), Ordering::Less);
    }

    #[test]
    fn test_compare_refs_same_base() {
        let mut trie = IdentifierTrie::new();
        let base = trie.insert_path(&[5, 12, 3]);
        let a = TrieIdRef::new(base, 99);
        let b = TrieIdRef::new(base, 120);
        assert_eq!(trie.compare_refs(a, b), Ordering::Less);
    }

    #[test]
    fn test_compare_refs_different_base() {
        let mut trie = IdentifierTrie::new();
        let ba = trie.insert_path(&[5, 12]);
        let bb = trie.insert_path(&[5, 12, 3]);
        // [5,12,3] vs [5,12,3,99]
        assert_eq!(trie.compare_refs(TrieIdRef::new(ba, 3), TrieIdRef::new(bb, 99)), Ordering::Less);
        // [5,12,4] vs [5,12,3,99]
        assert_eq!(trie.compare_refs(TrieIdRef::new(ba, 4), TrieIdRef::new(bb, 99)), Ordering::Greater);
    }
    #[test]
    fn test_interval_same_base() {
        let mut trie = IdentifierTrie::new();
        let base = trie.insert_path(&[5, 10]);
        let r = trie.compare_intervals_raw(base, 0, 5, base, 5, 10);
        assert!(matches!(r, IdOrderingRelation::B1ConcatB2));
    }

    #[test]
    fn test_interval_different_base() {
        let mut trie = IdentifierTrie::new();
        let a = trie.insert_path(&[5]);
        let b = trie.insert_path(&[10]);
        let r = trie.compare_intervals_raw(a, 0, 5, b, 0, 5);
        assert!(matches!(r, IdOrderingRelation::B1BeforeB2));
    }

    /// Exhaustive cross-check against raw slice comparison.
    #[test]
    fn test_ref_comparison_matches_slice_comparison() {
        let paths: Vec<Vec<u32>> = vec![
            vec![], vec![1], vec![1, 2], vec![1, 2, 3],
            vec![1, 3], vec![2], vec![2, 1],
        ];
        let extras = [0u32, 1, 5, 100];

        let mut trie = IdentifierTrie::new();
        let trie_ids: Vec<TrieId> = paths.iter().map(|p| trie.insert_path(p)).collect();

        for (i, pa) in paths.iter().enumerate() {
            for &ea in &extras {
                for (j, pb) in paths.iter().enumerate() {
                    for &eb in &extras {
                        let ra = TrieIdRef::new(trie_ids[i], ea);
                        let rb = TrieIdRef::new(trie_ids[j], eb);
                        let mut sa = pa.clone(); sa.push(ea);
                        let mut sb = pb.clone(); sb.push(eb);
                        let expected = sa.cmp(&sb);
                        let got = trie.compare_refs(ra, rb);
                        assert_eq!(got, expected,
                            "Mismatch: {:?}+{} vs {:?}+{}: expected {:?}, got {:?}",
                            pa, ea, pb, eb, expected, got);
                    }
                }
            }
        }
    }
}