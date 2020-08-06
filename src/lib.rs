pub mod testing;

use std::time::Instant;

#[derive(Debug)]
pub struct BWStats {
  start: Instant,
  /** cumulative time, cumulative space */
  stats: Vec<(u128, u128)>,
}

impl BWStats {
  pub fn new() -> BWStats {
    let mut stats = Vec::new();
    stats.push((0, 0));
    BWStats {
      start: Instant::now(),
      stats: stats,
    }
  }

  pub fn add(self: &mut Self, n: usize) {
    let prev = self.stats.last().unwrap().1;
    self.stats.push((self.start.elapsed().as_micros(), prev + (n as u128)));
  }

  pub fn last(self: &Self) -> &(u128, u128) {
    self.stats.last().unwrap()
  }
}
