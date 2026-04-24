pub mod tree;
pub mod identifier;
pub mod node;
pub mod operation;
pub mod state;
pub mod wasm;
pub mod document;
pub mod network;
pub mod trace_bench;

use document::Document;
use network::Network;

struct LogootSplitSystem { 
    pub network: Network
}

impl LogootSplitSystem {
    pub fn new(n: usize) -> Self {
        let docs = (0..n as u32).map(Document::new).collect();
        LogootSplitSystem { network: Network::new(docs) }
    }

    pub fn ins(&mut self, doc_id: u32, pos: usize, text: String) {
        let idx = self.network.index_of(doc_id);
        let op = self.network.documents[idx].ins(pos, text);
        if let Some(op) = op {
            self.network.broadcast(op, doc_id);
        }
    }

    pub fn del(&mut self, doc_id: u32, from: usize, to: usize) {
        let idx = self.network.index_of(doc_id);
        let op = self.network.documents[idx].del(from, to);
        self.network.broadcast(op, doc_id);
    }

    pub fn read(&mut self, doc_id: u32) -> String {
        let idx = self.network.index_of(doc_id);
        self.network.documents[idx].read()
    }   

    pub fn merge_from(&mut self, into: u32, from: u32) {
        self.network.sync_from(into, from);
    }

    pub fn reset(&mut self) {
        self.network.reset();
    }
}

#[test]
fn ab() {
    let mut sys = LogootSplitSystem::new(1);

    sys.ins(0, 0, "a".to_string());
    sys.ins(0, 1, "b".to_string());

    assert_eq!(sys.read(0), "ab".to_string());
}

#[test]
fn abc() {
    let mut sys = LogootSplitSystem::new(1);

    sys.ins(0, 0, "a".to_string());
    sys.ins(0, 1, "b".to_string());
    sys.ins(0, 2, "c".to_string());

    assert_eq!(sys.read(0), "abc".to_string());
}

#[test]
fn simple_test_1() {
    let mut sys = LogootSplitSystem::new(2);

    sys.ins(1, 0, "c".to_string());
    sys.ins(0, 0, "b".to_string());

    sys.ins(1, 0, "b".to_string());
    sys.ins(0, 1, "c".to_string());

    sys.merge_from(0, 1);
    sys.merge_from(1, 0);

    assert_eq!(sys.read(0), sys.read(1));

    sys.ins(0, 1, "b".to_string());

    sys.merge_from(0, 1);
    sys.merge_from(1, 0);

    assert_eq!(sys.read(0), sys.read(1));
}

#[test]
fn test_interleaved_inserts() {
    let mut sys = LogootSplitSystem::new(2);

    sys.ins(0, 0, "A".to_string());
    sys.ins(0, 1, "B".to_string());

    sys.ins(1, 0, "X".to_string());
    sys.ins(1, 1, "Y".to_string());

    sys.merge_from(0, 1);
    sys.merge_from(1, 0);

    assert_eq!(sys.read(0), sys.read(1));
}

#[allow(dead_code)]
fn run_insert_delete(seed: u64) {
    use rand::{SeedableRng, RngExt};
    use rand::rngs::StdRng;

    let mut rng = StdRng::seed_from_u64(seed);

    let mut sys = LogootSplitSystem::new(2);
    let doc_ids = vec![0u32, 1u32];

    let alphabet: Vec<char> = "abcdefghijklmnopqrstuvwxyz".chars().collect();

    for _ in 0..200 {
        let i = rng.random_range(0..doc_ids.len());
        let doc_id = doc_ids[i];

        let content = sys.read(doc_id);
        let len = content.chars().count();

        // 30% delete
        if len > 0 && rng.random_range(0..10) < 3 {
            let from = rng.random_range(0..len);
            let to = rng.random_range(from + 1..=len);
            // println!("Deleting from {} to {} in doc {}", from, to, doc_id);
            sys.del(doc_id, from, to);
        } else {
            let pos = if len == 0 { 0 } else { rng.random_range(0..=len) };
            let ch = alphabet[rng.random_range(0..alphabet.len())].to_string();
            // println!("Inserting '{}' at {} in doc {}", ch, pos, doc_id);
            sys.ins(doc_id, pos, ch);
        }

        // random merge
        let a = doc_ids[rng.random_range(0..doc_ids.len())];
        let b = doc_ids[rng.random_range(0..doc_ids.len())];

        if a != b {
            sys.merge_from(a, b);
            sys.merge_from(b, a);
        }

        let r0 = sys.read(a);
        let r1 = sys.read(b);

        if r0 != r1 {
            println!("Divergence detected at seed {}!", seed);
            // sys.network.documents[sys.network.index_of(a)].blocks.print_tree();
            // sys.network.documents[sys.network.index_of(b)].blocks.print_tree();
        }

        assert_eq!(
            r0,
            r1,
            "Seed {} diverged\n'{}' vs '{}'",
            seed,
            r0,
            r1
        );
    }
}

#[test]
fn test_insert_delete_heavy() {
    for i in 0..100000 {
        // println!("Running seed {}", i);
        run_insert_delete(i);
    }
}

#[allow(dead_code)]
fn run_async_ops(seed: u64) {
    use rand::{SeedableRng, RngExt};
    use rand::rngs::StdRng;

    let mut rng = StdRng::seed_from_u64(seed);

    let n_agents = 100;
    let mut sys = LogootSplitSystem::new(n_agents);
    let doc_ids: Vec<u32> = (0..n_agents).map(|i| i as u32).collect();

    let alphabet: Vec<char> = "abcdefghijklmnopqrstuvwxyz".chars().collect();

    for _ in 0..100 {
        let i = rng.random_range(0..doc_ids.len());
        let doc_id = doc_ids[i];

        let content = sys.read(doc_id);
        let len = content.chars().count();

        // 30% delete
        if len > 0 && rng.random_range(0..10) < 3 {
            let from = rng.random_range(0..len);
            let to = rng.random_range(from + 1..=len);
            // println!("Deleting from {} to {} in doc {}", from, to, doc_id);
            sys.del(doc_id, from, to);
        } else {
            let pos = if len == 0 { 0 } else { rng.random_range(0..=len) };
            let ch = alphabet[rng.random_range(0..alphabet.len())].to_string();
            // println!("Inserting '{}' at {} in doc {}", ch, pos, doc_id);
            sys.ins(doc_id, pos, ch);
        }

        // do periodic sync_all merges (generate rng between 1-10, if 5 do a full sync)
        if rng.random_range(1..=100) == 50 {
            // println!("Performing full sync at seed {}", seed);
            sys.network.sync_all();

            // Check for convergence
            for &a in &doc_ids {
                for &b in &doc_ids {
                    if a != b {
                        let r0 = sys.read(a);
                        let r1 = sys.read(b);

                        if r0 != r1 {
                            println!("Divergence detected between doc {} and {} at seed {}!", a, b, seed);
                        }

                        assert_eq!(
                            r0,
                            r1,
                            "Seed {} diverged between doc {} and {}\n'{}' vs '{}'",
                            seed,
                            a,
                            b,
                            r0,
                            r1
                        );
                    }
                }
            }
        }

    }
}

#[test]
fn test_async_ops() {
    for i in 0..10 {
        println!("Running async ops seed {}", i);
        run_async_ops(i);
    }
}

#[test]
fn test_contrived_async() {
    let mut sys = LogootSplitSystem::new(3);

    // Insert aaaaaaaaaa in doc 0
    sys.ins(0, 0, "aaaaaaaaaa".to_string());

    // Sync with doc 1
    sys.merge_from(1, 0);

    // Insert b into doc 1
    sys.ins(1, 5, "b".to_string());

    // Sync 2 with doc 1 (before doc 2 has seen the insert of aaaa..)
    sys.merge_from(2, 1);

    // Now sync 2 with 0
    sys.merge_from(2, 0);

    // Finally sync 0 with 1 to see the insert of b
    sys.merge_from(0, 1);

    // All docs should converge to aaaaaabaaaa
    let expected = "aaaaabaaaaa".to_string();

    // // Print tree for all 
    // for i in 0..3 {
    //     println!("Doc {} content: '{}'", i, sys.read(i));
    //     sys.network.documents[sys.network.index_of(i)].blocks.print_tree();
    // }

    assert_eq!(sys.read(0), expected);
    assert_eq!(sys.read(1), expected);
    assert_eq!(sys.read(2), expected);
}

#[test]
fn test_b2_inside_b1_two_children_before_parent() {
    // More stress: two different child blocks from two different sites
    // both arrive at site 2 before the parent block does.
    // Both children sit inside the parent's identifier space.

    let mut sys = LogootSplitSystem::new(3);

    sys.ins(0, 0, "ABCDEFGH".to_string());

    // Site 1 splits near the start
    sys.merge_from(1, 0);
    sys.ins(1, 1, "P".to_string());

    // Site 2 splits near the end — before receiving from site 0
    // (site 2 hasn't seen the base block yet, it will receive children first)
    sys.merge_from(2, 1);   // child from site 1 arrives on site 2
    // Site 2 now does its own insert — it only knows about site 1's child
    sys.ins(2, 0, "Q".to_string());
    
    // Now the parent from site 0 arrives on site 2 last
    sys.merge_from(2, 0);   // parent arrives → B2InsideB1 for site 1's child

    // Full convergence
    sys.merge_from(0, 1);
    sys.merge_from(0, 2);
    sys.merge_from(1, 2);
    sys.merge_from(1, 0);
    sys.merge_from(2, 0);
    sys.merge_from(2, 1);

    let r0 = sys.read(0);
    let r1 = sys.read(1);
    let r2 = sys.read(2);

    assert_eq!(r0, r1, "0 vs 1: '{}' vs '{}'", r0, r1);
    assert_eq!(r1, r2, "1 vs 2: '{}' vs '{}'", r1, r2);
}

#[test]
fn test_b2_inside_b1_with_delete() {
    // Same out-of-order scenario, but site 2 also deletes from the child
    // before the parent arrives. This is the nastiest variant because
    // remote_delete may have already modified the child node's offsets,
    // and then the parent insert tries to split around it.

    let mut sys = LogootSplitSystem::new(3);

    sys.ins(0, 0, "ABCDE".to_string());

    sys.merge_from(1, 0);
    sys.ins(1, 2, "XYZ".to_string());  // child block with 3 chars

    // Site 2 gets the child first, then deletes one char from it
    sys.merge_from(2, 1);
    sys.del(2, 2, 3);                  // delete 'Y' from site 2's view

    // Parent arrives late on site 2
    sys.merge_from(2, 0);

    // Full sync
    sys.merge_from(0, 1);
    sys.merge_from(0, 2);
    sys.merge_from(1, 2);
    sys.merge_from(1, 0);
    sys.merge_from(2, 1);

    let r0 = sys.read(0);
    let r1 = sys.read(1);
    let r2 = sys.read(2);

    assert_eq!(r0, r1, "0 vs 1: '{}' vs '{}'", r0, r1);
    assert_eq!(r1, r2, "1 vs 2: '{}' vs '{}'", r1, r2);
}

#[cfg(test)]
mod b2_inside_b1_stress {
    use super::*;
    use rand::{SeedableRng, RngExt};
    use rand::rngs::StdRng;
    use crate::operation::Operation;

    /// A network where ops are queued and can be delivered in any order.
    /// This lets us specifically engineer child-before-parent scenarios.
    struct ManualNetwork {
        docs: Vec<Document>,
        /// pending[i] = ops queued for delivery to doc i, not yet applied
        pending: Vec<Vec<Operation>>,
    }

    impl ManualNetwork {
        fn new(n: usize) -> Self {
            let docs = (0..n as u32).map(Document::new).collect();
            let pending = vec![vec![]; n];
            ManualNetwork { docs, pending }
        }

        /// Perform a local insert on site `site`, queue the op for all others
        fn ins(&mut self, site: usize, pos: usize, text: String) -> Operation {
            let op = self.docs[site].ins(pos, text).unwrap();
            for (i, q) in self.pending.iter_mut().enumerate() {
                if i != site {
                    q.push(op.clone());
                }
            }
            op
        }

        fn del(&mut self, site: usize, from: usize, to: usize) -> Operation {
            let op = self.docs[site].del(from, to);
            for (i, q) in self.pending.iter_mut().enumerate() {
                if i != site {
                    q.push(op.clone());
                }
            }
            op
        }

        /// Deliver the op at index `op_idx` in site `site`'s queue
        fn deliver(&mut self, site: usize, op_idx: usize) {
            let op = self.pending[site].remove(op_idx);
            // println!("Delivering op from site {} to site {}: {:?}", op.site, site, op);
            self.docs[site].apply_op(&op);
        }

        /// Deliver all pending ops to all sites in a random order
        fn drain_random(&mut self, rng: &mut impl rand::Rng) {
            loop {
                // collect (site, queue_len) for all sites with pending ops
                let candidates: Vec<usize> = self.pending.iter()
                    .enumerate()
                    .filter(|(_, q)| !q.is_empty())
                    .map(|(i, _)| i)
                    .collect();

                if candidates.is_empty() { break; }

                let site = candidates[rng.random_range(0..candidates.len())];
                let q_len = self.pending[site].len();
                let op_idx = rng.random_range(0..q_len);
                self.deliver(site, op_idx);
            }
        }

        fn read(&mut self, site: usize) -> String {
            self.docs[site].read()
        }

        fn assert_convergence(&mut self, seed: u64) {
            let contents: Vec<String> = (0..self.docs.len())
                .map(|i| self.docs[i].read())
                .collect();
            for i in 1..contents.len() {
                assert_eq!(
                    contents[0], contents[i],
                    "Seed: {} -- Divergence between site 0 and site {}: '{}' vs '{}'",
                    seed, i, contents[0], contents[i]
                );
            }
        }
    }

    /// Core scenario: site 0 inserts a wide block, site 1 sees it and inserts
    /// a child inside it, site 2 gets the child FIRST then the parent.
    /// Permute the order of the remaining deliveries randomly.
    fn run_child_before_parent(seed: u64) {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut net = ManualNetwork::new(3);

        // Site 0: insert a wide block — this becomes the "parent"
        net.ins(0, 0, "ABCDEFGHIJ".to_string());

        // Deliver parent to site 1 only (site 2 stays ignorant for now)
        // Sites 1's queue has the op at index 0, deliver it
        net.deliver(1, 0); // site 1 now knows about "ABCDEFGHIJ"
        // Site 2's queue still has the parent op pending (not delivered yet)

        // Site 1: insert a child inside the parent's identifier space
        let split_pos = rng.random_range(1..10);
        net.ins(1, split_pos, "X".to_string());

        // Now site 2 gets site 1's child op BEFORE the parent from site 0
        // Site 2's pending queue: [parent_from_0, child_from_1]
        // We want to deliver child_from_1 (index 1) before parent_from_0 (index 0)
        net.deliver(2, 1); // child arrives on site 2 first — B2InsideB1 triggered later
        net.deliver(2, 0); // parent arrives — insert_rec hits B2InsideB1

        // Drain everything else randomly
        net.drain_random(&mut rng);
        // Check if anybody still has something in pending!
        net.assert_convergence(seed);
    }

    /// Deeper stress: multiple rounds of wide-block + child insertions with
    /// random out-of-order delivery across N sites.
    fn run_deep_stress(seed: u64) {
        let mut rng = StdRng::seed_from_u64(seed);
        let n = 3;
        let mut net = ManualNetwork::new(n);

        let alphabet: Vec<char> = "abcdefghijklmnopqrstuvwxyz".chars().collect();

        for round in 0..40 {
            let site = rng.random_range(0..n);
            let content = net.read(site);
            let len = content.chars().count();

            if len > 1 && rng.random_range(0..10) < 3 {
                let from = rng.random_range(0..len);
                let to = rng.random_range(from+1..=len);
                // println!("Round {}: Deleting from {} to {} in site {}", round, from, to, site);
                net.del(site, from, to);
            } else {
                let pos = if len == 0 { 0 } else { rng.random_range(0..=len) };
                let ch = alphabet[rng.random_range(0..26)].to_string();
                // println!("Round {}: Inserting '{}' at pos {} in site {}", round, ch, pos, site);
                net.ins(site, pos, ch);
            }

            // Every ~5 rounds, randomly drain some (not all) pending ops
            // to simulate partial, out-of-order delivery
            if round % 5 == 0 {
                for _ in 0..rng.random_range(0..8) {
                    let candidates: Vec<usize> = net.pending.iter()
                        .enumerate()
                        .filter(|(_, q)| !q.is_empty())
                        .map(|(i, _)| i)
                        .collect();
                    if candidates.is_empty() { break; }
                    let s = candidates[rng.random_range(0..candidates.len())];
                    let qi = rng.random_range(0..net.pending[s].len());
                    net.deliver(s, qi);
                }
                // Print all docs
                // for i in 0..n {
                //     println!("Site {} content: '{}'", i, net.read(i));
                //     net.docs[i].blocks.print_tree();
                // }
            }
        }

        // Final full drain in random order, then check convergence
        net.drain_random(&mut rng);
        net.assert_convergence(seed);
    }

    #[test]
    fn test_child_before_parent_exhaustive() {
        for seed in 100000..200000 {
            run_child_before_parent(seed);
        }
    }

    #[test]
    fn test_deep_stress_random_delivery() {
        for seed in 100000..200000 {
            run_deep_stress(seed);
        }
        // run_deep_stress(1219);
    }
}