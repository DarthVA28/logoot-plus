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
            println!("Deleting from {} to {} in doc {}", from, to, doc_id);
            sys.del(doc_id, from, to);
        } else {
            let pos = if len == 0 { 0 } else { rng.random_range(0..=len) };
            let ch = alphabet[rng.random_range(0..alphabet.len())].to_string();
            println!("Inserting '{}' at {} in doc {}", ch, pos, doc_id);
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
    for i in 0..1000 {
        println!("Running seed {}", i);
        run_insert_delete(i);
    }
    // run_insert_delete(4);
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

    for _ in 0..1000 {
        let i = rng.random_range(0..doc_ids.len());
        let doc_id = doc_ids[i];

        let content = sys.read(doc_id);
        let len = content.chars().count();

        // 30% delete
        if len > 0 && rng.random_range(0..10) < 3 {
            let from = rng.random_range(0..len);
            let to = rng.random_range(from + 1..=len);
            println!("Deleting from {} to {} in doc {}", from, to, doc_id);
            sys.del(doc_id, from, to);
        } else {
            let pos = if len == 0 { 0 } else { rng.random_range(0..=len) };
            let ch = alphabet[rng.random_range(0..alphabet.len())].to_string();
            println!("Inserting '{}' at {} in doc {}", ch, pos, doc_id);
            sys.ins(doc_id, pos, ch);
        }

        // do periodic sync_all merges (generate rng between 1-10, if 5 do a full sync)
        if rng.random_range(1..=100) == 50 {
            println!("Performing full sync at seed {}", seed);
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