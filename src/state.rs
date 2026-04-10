use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

#[derive(Clone, Debug,)]
pub struct State { 
    pub local_clock: u32, 
    pub replica: u32,
    pub rng: ChaCha8Rng
}

impl State {
    pub fn new(replica: u32) -> Self {
        State { 
            local_clock: 1, 
            replica, 
            rng: ChaCha8Rng::seed_from_u64(replica as u64) 
        }
    }
}
