use std::cmp::Ordering;

use ahash::AHashMap as HashMap;
use rand::RngExt;

use crate::state::State;

pub const MIN_VALUE: u32 = 0;
pub const MAX_VALUE: u32 = 100000;
pub type Range = (u32, u32);

// Binary-lifting levels: supports trie depth up to 2^8 = 256.
const LOG: usize = 8;
// Trie root index.  Also serves as the "empty" identifier.
const ROOT: u32 = 0;
// Number of u32 path elements packed into the order key.
const KEY_SLOTS: u32 = 4; // 4 × 32 = 128 bits = u128

// ─── Public types ────────────────────────────────────────────────────────────

/// Lightweight trie-node handle (4 bytes).
#[derive(Clone, Copy, Debug)]
pub struct Identifier(u32);

impl Identifier {
    pub const EMPTY: Identifier = Identifier(ROOT);
    #[inline(always)]
    pub fn is_empty(self) -> bool { self.0 == ROOT }
}

impl PartialEq for Identifier {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool { self.0 == other.0 }
}
impl Eq for Identifier {}

impl std::hash::Hash for Identifier {
    #[inline(always)]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) { self.0.hash(state); }
}

/// Reference = trie node + one trailing value.
/// Conceptual path: `path(base) ++ [extra]`.
#[derive(Clone, Copy, Debug)]
pub struct IdentifierRef {
    pub base: Identifier,
    pub extra: u32,
}

impl IdentifierRef {
    #[inline(always)]
    pub fn new(base: Identifier, extra: u32) -> Self { Self { base, extra } }
    #[inline(always)]
    pub fn doc_start() -> Self { Self { base: Identifier::EMPTY, extra: MIN_VALUE } }
    #[inline(always)]
    pub fn doc_end() -> Self { Self { base: Identifier::EMPTY, extra: MAX_VALUE } }
}

#[derive(Clone, Copy, Debug)]
pub struct IdentifierInterval {
    pub base: Identifier,
    pub lo: u32,
    pub hi: u32,
}

impl IdentifierInterval {
    #[inline(always)]
    pub fn new(base: Identifier, lo: u32, hi: u32) -> Self { Self { base, lo, hi } }
    #[inline(always)]
    pub fn id_begin(&self) -> IdentifierRef { IdentifierRef::new(self.base, self.lo) }
    #[inline(always)]
    pub fn id_end(&self) -> IdentifierRef { IdentifierRef::new(self.base, self.hi - 1) }
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

// ─── Trie node ───────────────────────────────────────────────────────────────
//
// Exactly one cache line (64 bytes).  A single access to any field pulls the
// full node into L1, so compare_refs that reads order_key and depth gets the
// lifting table for free.

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct TrieNode {
    /// First min(depth, 4) path values packed big-endian into a u128.
    /// Position i occupies bits `(3-i)*32 .. (3-i)*32+31`.
    /// Enables O(1) lexicographic comparison for paths with ≤ 4 elements.
    order_key: u128, // 16 B
    /// Binary-lifting table: `up[k]` = ancestor 2^k levels above.
    up: [u32; LOG], // 32 B
    /// This node's value in the trie.
    value: u32, // 4 B
    /// Parent node index.
    parent: u32, // 4 B
    /// Depth in the trie (root = 0).
    depth: u32, // 4 B
    _pad: u32,  // 4 B  →  total 64 B
}

// ─── IdArena ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct IdArena {
    nodes: Vec<TrieNode>,
    children: HashMap<u64, u32>,
}

/// Pack `(parent_index, child_value)` into a single u64 hash-map key.
#[inline(always)]
fn child_key(parent: u32, value: u32) -> u64 {
    (parent as u64) << 32 | value as u64
}

impl IdArena {
    pub fn new() -> Self {
        let root = TrieNode {
            order_key: 0,
            up: [ROOT; LOG],
            value: 0,
            parent: ROOT,
            depth: 0,
            _pad: 0,
        };
        let mut nodes = Vec::with_capacity(4096);
        nodes.push(root);
        IdArena {
            nodes,
            children: HashMap::with_capacity(4096),
        }
    }

    pub fn clear(&mut self) {
        self.nodes.truncate(1);
        self.children.clear();
    }

    // ── helpers ──────────────────────────────────────────────

    #[inline(always)]
    fn node(&self, idx: u32) -> &TrieNode {
        unsafe { self.nodes.get_unchecked(idx as usize) }
    }

    // ── Node creation ────────────────────────────────────────

    fn add_node(&mut self, parent: u32, value: u32) -> u32 {
        let idx = self.nodes.len() as u32;

        // Copy parent data before pushing (TrieNode is Copy).
        let p = self.nodes[parent as usize];
        let depth = p.depth + 1;

        let order_key = if depth <= KEY_SLOTS {
            p.order_key | ((value as u128) << ((KEY_SLOTS - depth) * 32))
        } else {
            p.order_key
        };

        let mut up = [ROOT; LOG];
        up[0] = parent;
        for k in 1..LOG {
            let anc = up[k - 1];
            up[k] = self.nodes[anc as usize].up[k - 1];
        }

        self.nodes.push(TrieNode {
            order_key,
            up,
            value,
            parent,
            depth,
            _pad: 0,
        });
        self.children.insert(child_key(parent, value), idx);
        idx
    }

    // ── Interning ────────────────────────────────────────────

    pub fn intern(&mut self, path: &[u32]) -> Identifier {
        if path.is_empty() {
            return Identifier::EMPTY;
        }
        let mut node = ROOT;
        for &val in path {
            let key = child_key(node, val);
            node = match self.children.get(&key) {
                Some(&child) => child,
                None => self.add_node(node, val),
            };
        }
        Identifier(node)
    }

    // ── Binary lifting ──────────────────────────────────────

    #[inline]
    fn lift(&self, mut node: u32, target_depth: u32) -> u32 {
        let mut diff = self.node(node).depth - target_depth;
        let mut k = 0usize;
        while diff > 0 {
            if diff & 1 != 0 {
                node = self.node(node).up[k];
            }
            diff >>= 1;
            k += 1;
        }
        node
    }

    #[inline]
    fn lca(&self, mut a: u32, mut b: u32) -> u32 {
        let da = self.node(a).depth;
        let db = self.node(b).depth;
        if da > db {
            a = self.lift(a, db);
        } else if db > da {
            b = self.lift(b, da);
        }
        if a == b {
            return a;
        }
        for k in (0..LOG).rev() {
            let ua = self.node(a).up[k];
            let ub = self.node(b).up[k];
            if ua != ub {
                a = ua;
                b = ub;
            }
        }
        self.node(a).up[0]
    }

    // ── Depth accessor ──────────────────────────────────────

    #[inline(always)]
    pub fn depth(&self, id: Identifier) -> u32 {
        self.node(id.0).depth
    }

    // ── Order-key helpers ───────────────────────────────────

    /// Build the packed order key for a full ref path.
    /// Returns `(key, ref_depth)`.
    #[inline(always)]
    fn ref_key(&self, r: IdentifierRef) -> (u128, u32) {
        if r.base.is_empty() {
            return ((r.extra as u128) << 96, 1);
        }
        let n = self.node(r.base.0);
        let d = n.depth;
        if d < KEY_SLOTS {
            let key = n.order_key | ((r.extra as u128) << ((KEY_SLOTS - 1 - d) * 32));
            (key, d + 1)
        } else {
            (n.order_key, d + 1)
        }
    }

    // ── compare_refs ────────────────────────────────────────

    /// O(1) in the common case (ref paths ≤ 4 elements).
    /// Falls back to O(log d) LCA for deeper paths.
    #[inline]
    pub fn compare_refs(&self, a: IdentifierRef, b: IdentifierRef) -> Ordering {
        if a.base == b.base {
            return a.extra.cmp(&b.extra);
        }

        let (ka, da) = self.ref_key(a);
        let (kb, db) = self.ref_key(b);

        match ka.cmp(&kb) {
            Ordering::Equal => {
                if da <= KEY_SLOTS && db <= KEY_SLOTS {
                    // Both ref paths fully captured: shorter = less.
                    da.cmp(&db)
                } else {
                    self.compare_refs_lca(a, b)
                }
            }
            ord => ord,
        }
    }

    #[cold]
    fn compare_refs_lca(&self, a: IdentifierRef, b: IdentifierRef) -> Ordering {
        let an = a.base.0;
        let bn = b.base.0;
        let lca = self.lca(an, bn);

        if lca == an {
            let ad = self.node(an).depth;
            let bc = self.lift(bn, ad + 1);
            match a.extra.cmp(&self.node(bc).value) {
                Ordering::Equal => Ordering::Less,
                ord => ord,
            }
        } else if lca == bn {
            let bd = self.node(bn).depth;
            let ac = self.lift(an, bd + 1);
            match self.node(ac).value.cmp(&b.extra) {
                Ordering::Equal => Ordering::Greater,
                ord => ord,
            }
        } else {
            let ld = self.node(lca).depth;
            let ac = self.lift(an, ld + 1);
            let bc = self.lift(bn, ld + 1);
            self.node(ac).value.cmp(&self.node(bc).value)
        }
    }

    // ── compare_ids ─────────────────────────────────────────

    #[inline]
    pub fn compare_ids(&self, a: Identifier, b: Identifier) -> Ordering {
        if a == b {
            return Ordering::Equal;
        }
        let na = self.node(a.0);
        let nb = self.node(b.0);
        match na.order_key.cmp(&nb.order_key) {
            Ordering::Equal => {
                if na.depth <= KEY_SLOTS && nb.depth <= KEY_SLOTS {
                    na.depth.cmp(&nb.depth)
                } else {
                    self.compare_ids_lca(a, b)
                }
            }
            ord => ord,
        }
    }

    #[cold]
    fn compare_ids_lca(&self, a: Identifier, b: Identifier) -> Ordering {
        let lca = self.lca(a.0, b.0);
        if lca == a.0 {
            return Ordering::Less;
        }
        if lca == b.0 {
            return Ordering::Greater;
        }
        let ld = self.node(lca).depth;
        let ac = self.lift(a.0, ld + 1);
        let bc = self.lift(b.0, ld + 1);
        self.node(ac).value.cmp(&self.node(bc).value)
    }

    // ── Interval comparisons ────────────────────────────────

    #[inline(always)]
    pub fn compare_intervals_raw(
        &self,
        b1_base: Identifier,
        b1_lo: u32,
        b1_hi: u32,
        b2_base: Identifier,
        b2_lo: u32,
        b2_hi: u32,
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
            Ordering::Equal => IdOrderingRelation::B1BeforeB2,
        }
    }

    #[inline(always)]
    pub fn compare_intervals(
        &self,
        b1: &IdentifierInterval,
        b2: &IdentifierInterval,
    ) -> IdOrderingRelation {
        self.compare_intervals_raw(b1.base, b1.lo, b1.hi, b2.base, b2.lo, b2.hi)
    }

    // ── num_insertable ──────────────────────────────────────

    pub fn num_insertable(
        &self,
        id_insert: IdentifierRef,
        id_next: IdentifierRef,
        length: u32,
    ) -> u32 {
        let ins_d = self.node(id_insert.base.0).depth;
        let nxt_d = self.node(id_next.base.0).depth;

        if ins_d > nxt_d {
            return length;
        }

        // Ancestor check: insert.base must be an ancestor of next.base.
        if ins_d > 0 {
            let lifted = self.lift(id_next.base.0, ins_d);
            if lifted != id_insert.base.0 {
                return length;
            }
        }

        let next_at = if ins_d < nxt_d {
            self.node(self.lift(id_next.base.0, ins_d + 1)).value
        } else {
            id_next.extra
        };

        next_at + 1 - id_insert.extra
    }

    // ── find_split_point ────────────────────────────────────

    pub fn find_split_point(
        &self,
        idi_short: &IdentifierInterval,
        id_long: Identifier,
    ) -> u32 {
        if id_long.is_empty() {
            return 0;
        }
        let text_len = idi_short.hi - idi_short.lo;
        if text_len == 0 {
            return 0;
        }

        let sn = idi_short.base.0;
        let ln = id_long.0;
        let s_d = self.node(sn).depth;
        let l_d = self.node(ln).depth;

        let lca = self.lca(sn, ln);

        if lca == sn {
            if s_d >= l_d {
                return 0;
            }
            let pivot = self.node(self.lift(ln, s_d + 1)).value;
            let extras_below = if l_d > s_d + 1 {
                pivot.saturating_add(1).saturating_sub(idi_short.lo)
            } else {
                pivot.saturating_sub(idi_short.lo)
            };
            extras_below.min(text_len)
        } else if lca == ln {
            0
        } else {
            let ld = self.node(lca).depth;
            let sc = self.lift(sn, ld + 1);
            let lc = self.lift(ln, ld + 1);
            if self.node(sc).value < self.node(lc).value {
                text_len
            } else {
                0
            }
        }
    }

    // ── Path extraction ─────────────────────────────────────

    #[inline]
    fn fill_path(&self, id: Identifier, buf: &mut Vec<u32>) {
        buf.clear();
        if id.is_empty() {
            return;
        }
        let d = self.node(id.0).depth as usize;
        buf.reserve(d);
        let mut n = id.0;
        for _ in 0..d {
            let nd = self.node(n);
            buf.push(nd.value);
            n = nd.parent;
        }
        buf.reverse();
    }

    pub fn get_path_owned(&self, id: Identifier) -> Vec<u32> {
        let mut buf = Vec::new();
        self.fill_path(id, &mut buf);
        buf
    }

    // ── Display / diagnostics ───────────────────────────────

    pub fn to_string(&self, id: Identifier) -> String {
        self.get_path_owned(id)
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(".")
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn arena_size(&self) -> usize {
        self.nodes.len() * std::mem::size_of::<TrieNode>()
    }
}

impl std::fmt::Debug for IdArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdArena")
            .field("node_count", &self.nodes.len())
            .finish()
    }
}

// ─── generate_base ──────────────────────────────────────────────────────────

pub fn generate_base(
    arena: &mut IdArena,
    id_low: IdentifierRef,
    id_high: IdentifierRef,
    state: &mut State,
) -> Identifier {
    let mut low_buf: Vec<u32> = Vec::new();
    let mut high_buf: Vec<u32> = Vec::new();
    arena.fill_path(id_low.base, &mut low_buf);
    low_buf.push(id_low.extra);
    arena.fill_path(id_high.base, &mut high_buf);
    high_buf.push(id_high.extra);

    let mut new_path: Vec<u32> = Vec::new();
    let mut i = 0usize;

    loop {
        let l = low_buf.get(i).copied().unwrap_or(MIN_VALUE);
        let h = high_buf.get(i).copied().unwrap_or(MAX_VALUE);
        if (h as i64) - (l as i64) >= 2 {
            break;
        }
        new_path.push(l);
        i += 1;
    }

    let l = low_buf.get(i).copied().unwrap_or(MIN_VALUE);
    let h = high_buf.get(i).copied().unwrap_or(MAX_VALUE);
    let nxt = state.rng.random_range(l + 1..h);
    new_path.push(nxt);
    new_path.push(state.replica);
    new_path.push(state.local_clock);

    arena.intern(&new_path)
}