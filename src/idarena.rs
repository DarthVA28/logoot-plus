use std::cmp::Ordering;
use smallvec::SmallVec;
use crate::state::State;
use rand::RngExt;

pub const MIN_VALUE: u32 = 0;
pub const MAX_VALUE: u32 = 100000;
pub type Range = (u32, u32);

const EMPTY_INDEX: u32 = u32::MAX;
const ROOT_INDEX: u32 = 0;

// ── Identifier types ─────────────────────────────────────────

/// An identifier is a pointer to the last node of its path in the prefix tree.
/// `index` is the node index in `IdArena::nodes`; `len` is the depth (path length).
#[derive(Clone, Copy, Debug)]
pub struct Identifier {
    index: u32,
    len: u32,
}

impl Identifier {
    pub const EMPTY: Identifier = Identifier { index: EMPTY_INDEX, len: 0 };

    #[inline(always)]
    pub fn is_empty(self) -> bool { self.index == EMPTY_INDEX }

    #[inline(always)]
    pub fn depth(self) -> u32 { self.len }
}

impl PartialEq for Identifier {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool { self.index == other.index }
}
impl Eq for Identifier {}

impl std::hash::Hash for Identifier {
    #[inline(always)]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) { self.index.hash(state); }
}

#[derive(Clone, Copy, Debug)]
pub struct IdentifierRef {
    pub base: Identifier,
    pub extra: u32,
}

impl IdentifierRef {
    #[inline(always)]
    pub fn new(base: Identifier, extra: u32) -> Self { IdentifierRef { base, extra } }
    pub fn doc_start() -> Self { IdentifierRef { base: Identifier::EMPTY, extra: MIN_VALUE } }
    pub fn doc_end()   -> Self { IdentifierRef { base: Identifier::EMPTY, extra: MAX_VALUE } }
}

#[derive(Clone, Copy, Debug)]
pub struct IdentifierInterval {
    pub base: Identifier,
    pub lo: u32,
    pub hi: u32,
}

impl IdentifierInterval {
    pub fn new(base: Identifier, lo: u32, hi: u32) -> Self { IdentifierInterval { base, lo, hi } }
    pub fn id_begin(&self) -> IdentifierRef { IdentifierRef::new(self.base, self.lo) }
    pub fn id_end(&self)   -> IdentifierRef { IdentifierRef::new(self.base, self.hi - 1) }
}

pub enum IdOrderingRelation {
    B1BeforeB2,
    B1AfterB2,
    B1InsideB2,
    B2InsideB1,
    B1ConcatB2,
    B2ConcatB1,
    B1EqualsB2,
}

// ── Trie node ────────────────────────────────────────────────

/// A single node in the prefix tree.
///
/// Each node stores exactly one `u32` value — the path element at this depth.
/// The full identifier path is reconstructed by walking parent pointers to the root.
///
/// `ancestors[k]` = the node index of the 2^k-th ancestor (binary lifting table),
/// computed at insertion time and used for O(log depth) LCA queries.
#[derive(Clone, Debug)]
struct TrieNode {
    value: u32,
    parent: u32,
    depth: u32,
    /// Binary lifting table: `ancestors[k]` is the 2^k-th ancestor.
    /// Length = floor(log2(depth)) + 1.
    ancestors: SmallVec<[u32; 10]>,
    /// Sparse children list: (child_value, child_node_index).
    /// Most CRDT paths branch minimally, so a linear-scanned SmallVec
    /// outperforms a HashMap here.
    children: SmallVec<[(u32, u32); 4]>,
}

// ── IdArena — prefix tree with binary lifting ────────────────

#[derive(Clone, Debug)]
pub struct IdArena {
    nodes: Vec<TrieNode>,
}

impl IdArena {
    pub fn new() -> Self {
        // Node 0 is the virtual root (depth 0, no value, no parent).
        let root = TrieNode {
            value: 0,
            parent: ROOT_INDEX,
            depth: 0,
            ancestors: SmallVec::new(),
            children: SmallVec::new(),
        };
        let mut nodes = Vec::with_capacity(4096);
        nodes.push(root);
        IdArena { nodes }
    }

    pub fn clear(&mut self) {
        self.nodes.truncate(1);
        self.nodes[0].children.clear();
    }

    // ── Interning ────────────────────────────────────────────

    /// Walk `path` through the trie, creating nodes as needed, and return
    /// an `Identifier` pointing to the terminal node.
    /// Identical paths always yield the same node index (deduplication is
    /// automatic — the trie only creates a new node when no matching child
    /// exists).
    pub fn intern(&mut self, path: &[u32]) -> Identifier {
        if path.is_empty() { return Identifier::EMPTY; }

        let mut current = ROOT_INDEX;
        for &val in path {
            current = self.get_or_create_child(current, val);
        }

        Identifier { index: current, len: path.len() as u32 }
    }

    /// Find or create a child of `parent` with the given `value`.
    /// When creating, the binary lifting table is computed on the fly.
    fn get_or_create_child(&mut self, parent: u32, value: u32) -> u32 {
        // Linear scan over children — fast for typical branching factors (1–4).
        for &(v, idx) in &self.nodes[parent as usize].children {
            if v == value { return idx; }
        }

        let child_idx = self.nodes.len() as u32;
        let child_depth = self.nodes[parent as usize].depth + 1;

        // ── Build the binary lifting table ───────────────────
        // ancestors[0] = parent                           (2^0 = 1 step)
        // ancestors[k] = ancestors[k-1]'s ancestors[k-1]  (2^k steps)
        let mut ancestors: SmallVec<[u32; 10]> = SmallVec::new();
        ancestors.push(parent);
        let mut k = 0;
        loop {
            let prev_anc = ancestors[k];
            if let Some(&next) = self.nodes[prev_anc as usize].ancestors.get(k) {
                ancestors.push(next);
                k += 1;
            } else {
                break;
            }
        }

        let node = TrieNode {
            value,
            parent,
            depth: child_depth,
            ancestors,
            children: SmallVec::new(),
        };
        self.nodes.push(node);
        self.nodes[parent as usize].children.push((value, child_idx));

        child_idx
    }

    // ── Binary lifting primitives ────────────────────────────

    /// Lift `node` upward by `steps` edges.  O(log steps).
    #[inline]
    fn lift(&self, mut node: u32, mut steps: u32) -> u32 {
        let mut k = 0usize;
        while steps > 0 {
            if steps & 1 != 0 {
                node = self.nodes[node as usize].ancestors[k];
            }
            steps >>= 1;
            k += 1;
        }
        node
    }

    /// Lowest Common Ancestor of two trie nodes.  O(log depth).
    fn lca(&self, mut a: u32, mut b: u32) -> u32 {
        let da = self.nodes[a as usize].depth;
        let db = self.nodes[b as usize].depth;

        // 1. Bring both to the same depth.
        if da > db {
            a = self.lift(a, da - db);
        } else if db > da {
            b = self.lift(b, db - da);
        }

        if a == b { return a; }

        // 2. Binary-lift both until their ancestors converge.
        let max_k = self.nodes[a as usize].ancestors.len();
        for k in (0..max_k).rev() {
            let aa = self.nodes[a as usize].ancestors.get(k).copied();
            let bb = self.nodes[b as usize].ancestors.get(k).copied();
            if let (Some(aa), Some(bb)) = (aa, bb) {
                if aa != bb {
                    a = aa;
                    b = bb;
                }
            }
        }

        // a and b are now distinct children of the LCA.
        self.nodes[a as usize].parent
    }

    /// Return the value of the child of `ancestor` that lies on the
    /// path from the root to `descendant`.
    /// Precondition: `ancestor` is a strict ancestor of `descendant`.
    #[inline]
    fn child_value_on_path(&self, ancestor: u32, descendant: u32) -> u32 {
        let anc_depth = self.nodes[ancestor as usize].depth;
        let desc_depth = self.nodes[descendant as usize].depth;
        debug_assert!(desc_depth > anc_depth);
        let child = self.lift(descendant, desc_depth - anc_depth - 1);
        self.nodes[child as usize].value
    }

    // ── Comparisons (O(log depth) via LCA) ───────────────────

    /// Compare two `Identifier` bases lexicographically.
    /// (Shorter prefix is Less, matching Rust's `&[u32]` ordering.)
    #[inline]
    pub fn compare_ids(&self, a: Identifier, b: Identifier) -> Ordering {
        if a.index == b.index { return Ordering::Equal; }

        // Handle empties: [] < [anything].
        match (a.is_empty(), b.is_empty()) {
            (true, true)  => return Ordering::Equal,
            (true, false) => return Ordering::Less,
            (false, true) => return Ordering::Greater,
            _ => {}
        }

        let lca_idx = self.lca(a.index, b.index);

        if lca_idx == a.index {
            // a's path is a strict prefix of b's → a < b.
            return Ordering::Less;
        }
        if lca_idx == b.index {
            // b's path is a strict prefix of a's → a > b.
            return Ordering::Greater;
        }

        // Paths diverge below the LCA — compare the diverging values.
        let va = self.child_value_on_path(lca_idx, a.index);
        let vb = self.child_value_on_path(lca_idx, b.index);
        va.cmp(&vb)
    }

    /// Compare two `IdentifierRef`s lexicographically.
    ///
    /// An `IdentifierRef` represents the sequence `base_path ++ [extra]`.
    ///
    /// This is the hot comparison used for document ordering and is
    /// O(log depth) thanks to binary lifting.
    #[inline]
    pub fn compare_refs(&self, a: IdentifierRef, b: IdentifierRef) -> Ordering {
        // ── Fast path: same base → just compare extras ───────
        if a.base.index == b.base.index {
            return a.extra.cmp(&b.extra);
        }

        // ── Handle empty bases ───────────────────────────────
        // Empty base means the full sequence is just [extra].
        let a_empty = a.base.is_empty();
        let b_empty = b.base.is_empty();

        if a_empty && b_empty {
            return a.extra.cmp(&b.extra);
        }

        if a_empty {
            // a = [extra_a],  b = [path_b…, extra_b]
            // Compare extra_a against the first element of b's path.
            let b_first = self.child_value_on_path(ROOT_INDEX, b.base.index);
            return match a.extra.cmp(&b_first) {
                Ordering::Equal => Ordering::Less, // a is shorter
                ord => ord,
            };
        }

        if b_empty {
            // Symmetric.
            let a_first = self.child_value_on_path(ROOT_INDEX, a.base.index);
            return match a_first.cmp(&b.extra) {
                Ordering::Equal => Ordering::Greater, // b is shorter
                ord => ord,
            };
        }

        // ── Both bases non-empty — use LCA ───────────────────
        let lca_idx = self.lca(a.base.index, b.base.index);

        if lca_idx == a.base.index {
            // a.base is a strict ancestor of b.base.
            // At depth = a's depth, a's sequence has `extra_a`,
            // while b's sequence has the next path element toward b.base.
            let b_child = self.child_value_on_path(a.base.index, b.base.index);
            return match a.extra.cmp(&b_child) {
                Ordering::Equal => Ordering::Less, // a's full path is shorter
                ord => ord,
            };
        }

        if lca_idx == b.base.index {
            // b.base is a strict ancestor of a.base.  (Symmetric.)
            let a_child = self.child_value_on_path(b.base.index, a.base.index);
            return match a_child.cmp(&b.extra) {
                Ordering::Equal => Ordering::Greater, // b's full path is shorter
                ord => ord,
            };
        }

        // Neither is an ancestor of the other — they diverge below the LCA.
        let va = self.child_value_on_path(lca_idx, a.base.index);
        let vb = self.child_value_on_path(lca_idx, b.base.index);
        va.cmp(&vb) // guaranteed unequal
    }

    // ── Interval comparisons ─────────────────────────────────

    #[inline(always)]
    pub fn compare_intervals_raw(
        &self,
        b1_base: Identifier, b1_lo: u32, b1_hi: u32,
        b2_base: Identifier, b2_lo: u32, b2_hi: u32,
    ) -> IdOrderingRelation {
        // Fast path: same base → pure offset arithmetic.
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

        let b1_begin = IdentifierRef::new(b1_base, b1_lo);
        let b2_begin = IdentifierRef::new(b2_base, b2_lo);

        match self.compare_refs(b1_begin, b2_begin) {
            Ordering::Less => {
                let b1_end = IdentifierRef::new(b1_base, b1_hi - 1);
                if self.compare_refs(b2_begin, b1_end) == Ordering::Less {
                    IdOrderingRelation::B2InsideB1
                } else {
                    IdOrderingRelation::B1BeforeB2
                }
            }
            Ordering::Greater => {
                let b2_end = IdentifierRef::new(b2_base, b2_hi - 1);
                if self.compare_refs(b1_begin, b2_end) == Ordering::Less {
                    IdOrderingRelation::B1InsideB2
                } else {
                    IdOrderingRelation::B1AfterB2
                }
            }
            Ordering::Equal => {
                IdOrderingRelation::B1BeforeB2
            }
        }
    }

    #[inline(always)]
    pub fn compare_intervals(&self, b1: &IdentifierInterval, b2: &IdentifierInterval) -> IdOrderingRelation {
        self.compare_intervals_raw(b1.base, b1.lo, b1.hi, b2.base, b2.lo, b2.hi)
    }

    // ── Path reconstruction (O(depth), for serialisation) ────

    /// Reconstruct the full path from root to this node.
    /// Only needed for serialisation and the non-hot helpers below.
    pub fn get_path_owned(&self, id: Identifier) -> Vec<u32> {
        if id.is_empty() { return Vec::new(); }
        let len = id.len as usize;
        let mut path = vec![0u32; len];
        let mut cur = id.index;
        for i in (0..len).rev() {
            path[i] = self.nodes[cur as usize].value;
            cur = self.nodes[cur as usize].parent;
        }
        path
    }

    /// Fill a caller-provided buffer with the path (avoids allocation
    /// when called in a loop with a reusable buffer).
    pub fn fill_path(&self, id: Identifier, buf: &mut Vec<u32>) {
        buf.clear();
        if id.is_empty() { return; }
        let len = id.len as usize;
        buf.resize(len, 0);
        let mut cur = id.index;
        for i in (0..len).rev() {
            buf[i] = self.nodes[cur as usize].value;
            cur = self.nodes[cur as usize].parent;
        }
    }

    /// Read a single path element at a given depth.  O(log depth).
    #[inline]
    pub fn value_at_depth(&self, id: Identifier, target_depth: u32) -> u32 {
        debug_assert!(!id.is_empty());
        debug_assert!(target_depth >= 1 && target_depth <= id.len);
        let node = self.lift(id.index, id.len - target_depth);
        self.nodes[node as usize].value
    }

    // ── Compatibility helpers (delegate to path reconstruction) ──

    /// Legacy-compatible slice accessor.  Returns an owned `Vec` because the
    /// trie does not store paths contiguously.
    #[inline]
    pub fn get_path(&self, id: Identifier) -> Vec<u32> {
        self.get_path_owned(id)
    }

    pub fn to_string(&self, id: Identifier) -> String {
        self.get_path_owned(id)
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(".")
    }

    // ── num_insertable ───────────────────────────────────────

    pub fn num_insertable(
        &self,
        id_insert: IdentifierRef,
        id_next: IdentifierRef,
        length: u32,
    ) -> u32 {
        let insert_path = self.get_path_owned(id_insert.base);
        let next_path   = self.get_path_owned(id_next.base);
        let l = insert_path.len();

        if l >= next_path.len() + 1 { return length; }

        // insert's base must be a prefix of next's full path (path ++ [extra]).
        let next_full_iter = next_path.iter().copied()
            .chain(std::iter::once(id_next.extra));
        for (&a, b) in insert_path.iter().zip(next_full_iter) {
            if a != b { return length; }
        }

        let next_at_l = if l < next_path.len() { next_path[l] } else { id_next.extra };
        next_at_l + 1 - id_insert.extra
    }

    pub fn find_split_point(
        &self,
        idi_short: &IdentifierInterval,
        id_long: Identifier,
    ) -> u32 {
        if id_long.is_empty() { return 0; }

        let text_len = idi_short.hi - idi_short.lo;
        if text_len == 0 { return 0; }

        let long_path  = self.get_path_owned(id_long);
        let short_path = self.get_path_owned(idi_short.base);
        let min_len = short_path.len().min(long_path.len());

        match short_path[..min_len].cmp(&long_path[..min_len]) {
            Ordering::Less    => return text_len,
            Ordering::Greater => return 0,
            Ordering::Equal   => {}
        }

        if short_path.len() < long_path.len() {
            let pivot = long_path[min_len];
            let extras_below = if long_path.len() > min_len + 1 {
                pivot.saturating_add(1).saturating_sub(idi_short.lo)
            } else {
                pivot.saturating_sub(idi_short.lo)
            };
            extras_below.min(text_len)
        } else {
            0
        }
    }

    // ── Stats ────────────────────────────────────────────────

    /// Number of interned identifier nodes (excluding the root).
    pub fn node_count(&self) -> usize {
        self.nodes.len() - 1
    }

    /// Total number of trie nodes (including the root sentinel).
    pub fn arena_size(&self) -> usize {
        self.nodes.len()
    }
}

// ── Base generation ──────────────────────────────────────────

pub fn generate_base(
    arena: &mut IdArena,
    id_low: IdentifierRef,
    id_high: IdentifierRef,
    state: &mut State,
) -> Identifier {
    let low_path  = arena.get_path_owned(id_low.base);
    let high_path = arena.get_path_owned(id_high.base);

    let mut new_path: Vec<u32> = Vec::new();
    let mut low_iter  = low_path.iter().copied().chain(std::iter::once(id_low.extra));
    let mut high_iter = high_path.iter().copied().chain(std::iter::once(id_high.extra));

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

    arena.intern(&new_path)
}