use std::cmp::Ordering;

use ahash::AHashMap as HashMap;
use rand::RngExt;

use crate::state::State;

pub const MIN_VALUE: u32 = 0;
pub const MAX_VALUE: u32 = 100000;
pub type Range = (u32, u32);

/// Maximum binary-lifting levels. Supports trie depth up to 2^16 = 65 536.
const LOG: usize = 16;
/// The root node index – also doubles as the "empty" identifier.
const ROOT: u32 = 0;

// ─── Identifier ──────────────────────────────────────────────────────────────

/// A lightweight handle into the trie.  Just a node index (4 bytes).
#[derive(Clone, Copy, Debug)]
pub struct Identifier(u32);

impl Identifier {
    /// The empty / document-boundary identifier (maps to the trie root).
    pub const EMPTY: Identifier = Identifier(ROOT);

    #[inline(always)]
    pub fn is_empty(self) -> bool {
        self.0 == ROOT
    }

    #[inline(always)]
    pub(crate) fn idx(self) -> u32 {
        self.0
    }
}

impl PartialEq for Identifier {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for Identifier {}

impl std::hash::Hash for Identifier {
    #[inline(always)]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

// ─── IdentifierRef ───────────────────────────────────────────────────────────

/// A reference = base path (trie node) + one trailing value.
/// Conceptual path: path(base) ++ [extra].
#[derive(Clone, Copy, Debug)]
pub struct IdentifierRef {
    pub base: Identifier,
    pub extra: u32,
}

impl IdentifierRef {
    #[inline(always)]
    pub fn new(base: Identifier, extra: u32) -> Self {
        IdentifierRef { base, extra }
    }
    pub fn doc_start() -> Self {
        IdentifierRef { base: Identifier::EMPTY, extra: MIN_VALUE }
    }
    pub fn doc_end() -> Self {
        IdentifierRef { base: Identifier::EMPTY, extra: MAX_VALUE }
    }
}

// ─── IdentifierInterval ─────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug)]
pub struct IdentifierInterval {
    pub base: Identifier,
    pub lo: u32,
    pub hi: u32,
}

impl IdentifierInterval {
    pub fn new(base: Identifier, lo: u32, hi: u32) -> Self {
        IdentifierInterval { base, lo, hi }
    }
    pub fn id_begin(&self) -> IdentifierRef {
        IdentifierRef::new(self.base, self.lo)
    }
    pub fn id_end(&self) -> IdentifierRef {
        IdentifierRef::new(self.base, self.hi - 1)
    }
}

// ─── Ordering relation ──────────────────────────────────────────────────────

pub enum IdOrderingRelation {
    B1BeforeB2,
    B1AfterB2,
    B1InsideB2,
    B2InsideB1,
    B1ConcatB2,
    B2ConcatB1,
    B1EqualsB2,
}

// ─── IdArena — prefix trie with binary lifting ──────────────────────────────

/// Struct-of-arrays trie.  Each node is an index `0..node_count`.
/// Node 0 is the root (depth 0, no value).
#[derive(Clone, Debug)]
pub struct IdArena {
    /// Per-node value (root's value is unused).
    values: Vec<u32>,
    /// Parent index.
    parents: Vec<u32>,
    /// Depth in the trie (root = 0).
    depths: Vec<u32>,
    /// Flat binary-lifting table: `up[node * LOG + k]` = 2^k-th ancestor.
    up: Vec<u32>,
    /// Child lookup: key = `(parent as u64) << 32 | value`, value = child index.
    children: HashMap<u64, u32>,
    /// Total number of nodes (including root).
    node_count: u32,
}

impl IdArena {
    pub fn new() -> Self {
        let mut arena = IdArena {
            values: Vec::with_capacity(4096),
            parents: Vec::with_capacity(4096),
            depths: Vec::with_capacity(4096),
            up: Vec::with_capacity(4096 * LOG),
            children: HashMap::with_capacity(4096),
            node_count: 0,
        };
        // Allocate root (index 0).
        arena.values.push(0);
        arena.parents.push(ROOT);
        arena.depths.push(0);
        arena.up.extend(std::iter::repeat_n(ROOT, LOG));
        arena.node_count = 1;
        arena
    }

    pub fn clear(&mut self) {
        self.values.truncate(1);
        self.parents.truncate(1);
        self.depths.truncate(1);
        self.up.truncate(LOG);
        self.children.clear();
        self.node_count = 1;
    }

    // ── Node creation ────────────────────────────────────────

    /// Insert a child node under `parent` with the given `value`.
    /// Sets up the binary-lifting table for the new node.
    fn add_node(&mut self, parent: u32, value: u32) -> u32 {
        let idx = self.node_count;
        self.node_count += 1;

        let depth = unsafe { *self.depths.get_unchecked(parent as usize) } + 1;
        self.values.push(value);
        self.parents.push(parent);
        self.depths.push(depth);

        // Binary lifting: up[0] = parent
        debug_assert_eq!(self.up.len(), idx as usize * LOG);
        self.up.push(parent);
        let base = idx as usize * LOG;
        for k in 1..LOG {
            let prev = unsafe { *self.up.get_unchecked(base + k - 1) };
            let anc = unsafe { *self.up.get_unchecked(prev as usize * LOG + k - 1) };
            self.up.push(anc);
        }

        // Register child edge.
        let key = (parent as u64) << 32 | value as u64;
        self.children.insert(key, idx);

        idx
    }

    // ── Interning ────────────────────────────────────────────

    /// Intern a path, deduplicating via the trie structure.
    /// Returns the node at the end of the path.
    pub fn intern(&mut self, path: &[u32]) -> Identifier {
        if path.is_empty() {
            return Identifier::EMPTY;
        }
        let mut node = ROOT;
        for &val in path {
            let key = (node as u64) << 32 | val as u64;
            node = match self.children.get(&key) {
                Some(&child) => child,
                None => self.add_node(node, val),
            };
        }
        Identifier(node)
    }

    // ── Binary lifting primitives ────────────────────────────

    /// Lift `node` up to `target_depth`.
    /// Pre: `target_depth <= depth(node)`.
    #[inline]
    fn lift(&self, mut node: u32, target_depth: u32) -> u32 {
        let mut diff = unsafe { *self.depths.get_unchecked(node as usize) } - target_depth;
        let mut k = 0usize;
        while diff > 0 {
            if diff & 1 != 0 {
                node = unsafe { *self.up.get_unchecked(node as usize * LOG + k) };
            }
            diff >>= 1;
            k += 1;
        }
        node
    }

    /// Lowest Common Ancestor.
    #[inline]
    fn lca(&self, mut a: u32, mut b: u32) -> u32 {
        let da = unsafe { *self.depths.get_unchecked(a as usize) };
        let db = unsafe { *self.depths.get_unchecked(b as usize) };

        if da > db {
            a = self.lift(a, db);
        } else if db > da {
            b = self.lift(b, da);
        }

        if a == b {
            return a;
        }

        // Walk from highest bit downward.
        for k in (0..LOG).rev() {
            let ua = unsafe { *self.up.get_unchecked(a as usize * LOG + k) };
            let ub = unsafe { *self.up.get_unchecked(b as usize * LOG + k) };
            if ua != ub {
                a = ua;
                b = ub;
            }
        }

        // Now a and b are children of the LCA.
        unsafe { *self.up.get_unchecked(a as usize * LOG) }
    }

    // ── Depth accessor ──────────────────────────────────────

    /// Depth of an identifier in the trie (= path length).
    #[inline(always)]
    pub fn depth(&self, id: Identifier) -> u32 {
        if id.is_empty() {
            0
        } else {
            unsafe { *self.depths.get_unchecked(id.0 as usize) }
        }
    }

    // ── Comparisons ─────────────────────────────────────────

    /// Compare two bare identifiers (path-only, no extra).
    #[inline]
    pub fn compare_ids(&self, a: Identifier, b: Identifier) -> Ordering {
        if a == b {
            return Ordering::Equal;
        }
        // EMPTY (ROOT) has depth 0 and is ancestor of everything.
        let lca = self.lca(a.0, b.0);
        if lca == a.0 {
            return Ordering::Less;
        }
        if lca == b.0 {
            return Ordering::Greater;
        }
        let lca_d = unsafe { *self.depths.get_unchecked(lca as usize) };
        let ac = self.lift(a.0, lca_d + 1);
        let bc = self.lift(b.0, lca_d + 1);
        let va = unsafe { *self.values.get_unchecked(ac as usize) };
        let vb = unsafe { *self.values.get_unchecked(bc as usize) };
        va.cmp(&vb)
    }

    /// Compare two `IdentifierRef`s lexicographically.
    ///
    /// Conceptual comparison of `path(a.base) ++ [a.extra]`
    /// vs `path(b.base) ++ [b.extra]`.
    ///
    /// O(log depth) via binary lifting.
    #[inline]
    pub fn compare_refs(&self, a: IdentifierRef, b: IdentifierRef) -> Ordering {
        // Fast path: same base → just compare extras.
        if a.base == b.base {
            return a.extra.cmp(&b.extra);
        }

        let an = a.base.0;
        let bn = b.base.0;

        let lca = self.lca(an, bn);

        if lca == an {
            // a.base is a strict ancestor of b.base.
            // They match up to depth(a.base).  Next element:
            //   a → a.extra
            //   b → value at depth(a.base) + 1
            let a_d = unsafe { *self.depths.get_unchecked(an as usize) };
            let bc = self.lift(bn, a_d + 1);
            let bv = unsafe { *self.values.get_unchecked(bc as usize) };
            match a.extra.cmp(&bv) {
                Ordering::Equal => Ordering::Less, // a is shorter → a < b
                ord => ord,
            }
        } else if lca == bn {
            // b.base is a strict ancestor of a.base (mirror).
            let b_d = unsafe { *self.depths.get_unchecked(bn as usize) };
            let ac = self.lift(an, b_d + 1);
            let av = unsafe { *self.values.get_unchecked(ac as usize) };
            match av.cmp(&b.extra) {
                Ordering::Equal => Ordering::Greater, // b is shorter → a > b
                ord => ord,
            }
        } else {
            // Paths diverge below LCA.
            let lca_d = unsafe { *self.depths.get_unchecked(lca as usize) };
            let ac = self.lift(an, lca_d + 1);
            let bc = self.lift(bn, lca_d + 1);
            let av = unsafe { *self.values.get_unchecked(ac as usize) };
            let bv = unsafe { *self.values.get_unchecked(bc as usize) };
            av.cmp(&bv) // can never be Equal for distinct children
        }
    }

    // ── Interval comparisons ─────────────────────────────────

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
        let ins_d = self.depth(id_insert.base);
        let nxt_d = self.depth(id_next.base);

        // insert's base path must be strictly shorter than next's full path.
        if ins_d > nxt_d {
            return length;
        }

        // Check prefix: insert.base must be an ancestor of next.base
        // (or ROOT if ins_d == 0, which is always an ancestor).
        if ins_d > 0 {
            let lifted = self.lift(id_next.base.0, ins_d);
            if lifted != id_insert.base.0 {
                return length;
            }
        }

        // Paths match up to ins_d.  Get the value at position ins_d in
        // next's full path:  if ins_d < nxt_d it's a trie value,
        //                    if ins_d == nxt_d it's next.extra.
        let next_at = if ins_d < nxt_d {
            let node = self.lift(id_next.base.0, ins_d + 1);
            unsafe { *self.values.get_unchecked(node as usize) }
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
        let s_d = unsafe { *self.depths.get_unchecked(sn as usize) };
        let l_d = unsafe { *self.depths.get_unchecked(ln as usize) };

        let lca = self.lca(sn, ln);

        if lca == sn {
            // short.base is an ancestor of (or equal to) long.
            if s_d >= l_d {
                return 0;
            }
            // Strict ancestor: get pivot value at depth s_d + 1 on long's path.
            let pivot_node = self.lift(ln, s_d + 1);
            let pivot = unsafe { *self.values.get_unchecked(pivot_node as usize) };
            let extras_below = if l_d > s_d + 1 {
                // long continues past the pivot → refs with extra ≤ pivot
                // are all strictly before long.
                pivot.saturating_add(1).saturating_sub(idi_short.lo)
            } else {
                // long ends right at the pivot → ref with extra == pivot
                // has the same full path as long (equal, not less).
                pivot.saturating_sub(idi_short.lo)
            };
            extras_below.min(text_len)
        } else if lca == ln {
            // long is an ancestor of short → short > long.
            0
        } else {
            // Paths diverge.
            let lca_d = unsafe { *self.depths.get_unchecked(lca as usize) };
            let sc = self.lift(sn, lca_d + 1);
            let lc = self.lift(ln, lca_d + 1);
            let sv = unsafe { *self.values.get_unchecked(sc as usize) };
            let lv = unsafe { *self.values.get_unchecked(lc as usize) };
            if sv < lv {
                text_len
            } else {
                0
            }
        }
    }

    // ── Path extraction ─────────────────────────────────────

    /// Reconstruct the full path (owned).
    /// Walk from the node to root, then reverse.
    pub fn get_path_owned(&self, id: Identifier) -> Vec<u32> {
        if id.is_empty() {
            return Vec::new();
        }
        let d = unsafe { *self.depths.get_unchecked(id.0 as usize) } as usize;
        let mut path = Vec::with_capacity(d);
        let mut node = id.0;
        for _ in 0..d {
            path.push(unsafe { *self.values.get_unchecked(node as usize) });
            node = unsafe { *self.parents.get_unchecked(node as usize) };
        }
        path.reverse();
        path
    }

    /// Fill a caller-provided buffer with the path (avoids allocation
    /// in hot loops like `generate_base`).
    #[inline]
    fn fill_path(&self, id: Identifier, buf: &mut Vec<u32>) {
        buf.clear();
        if id.is_empty() {
            return;
        }
        let d = unsafe { *self.depths.get_unchecked(id.0 as usize) } as usize;
        buf.reserve(d);
        let mut node = id.0;
        for _ in 0..d {
            buf.push(unsafe { *self.values.get_unchecked(node as usize) });
            node = unsafe { *self.parents.get_unchecked(node as usize) };
        }
        buf.reverse();
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
        self.node_count as usize
    }

    pub fn arena_size(&self) -> usize {
        self.values.len() + self.up.len()
    }
}

// ─── generate_base ──────────────────────────────────────────────────────────

pub fn generate_base(
    arena: &mut IdArena,
    id_low: IdentifierRef,
    id_high: IdentifierRef,
    state: &mut State,
) -> Identifier {
    // Materialise the full ref paths once (O(depth)).
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