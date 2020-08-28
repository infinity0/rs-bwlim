/*! Reactor for rate-limited streams.

This is only a partial reactor to add rate-limiting to byte streams; it does
not cover other types of events like connect/listen. For that, use a "normal"
reactor like the one in async-io.

Currently this reactor runs as an asynchronous task inside the full reactor of
async-io. This could in theory be changed, and we could run the main loop as a
standalone thread with blocking sleeps. It's not clear that this would give a
great benefit however, so keeping the current solution works OK for now.

*/

use std::collections::HashMap;
use std::panic;
use std::sync::{atomic::*, Arc, Mutex};
use std::time::{Duration, Instant};
use std::io;
use once_cell::sync::Lazy;
use vec_arena::Arena;

use async_io::Timer;
use async_io::reactor::*;
use smol::Task;

use crate::limit::{RateLimited, derive_allowance};
use crate::util::RorW;


#[derive(Debug, PartialEq, Eq, Hash)]
enum SourceDirection {
  Read,
  Write,
}

#[derive(Debug)]
pub(crate) struct Reactor {
  /// Last tick that we rate-limited on.
  last_tick: Mutex<Instant>,

  /// Ticker bumped before polling.
  ticker: AtomicUsize,

  /// Registered sources.
  sources: Mutex<Arena<Arc<Source<dyn RorW + Send + Sync>>>>,
}

impl Reactor {
  pub(crate) fn get() -> &'static Reactor {
      static REACTOR: Lazy<(Reactor, Task<()>)> = Lazy::new(|| {
          let reactor = Reactor {
            last_tick: Mutex::new(Instant::now()),
            ticker: AtomicUsize::new(0),
            sources: Mutex::new(Arena::new()),
          };

          let task = Task::spawn(async {
            Reactor::get().main_loop_async().await
          });

          (reactor, task)
      });
      &(REACTOR.0)
  }

  /// Registers an I/O source in the reactor.
  pub(crate) fn insert_io<T>(
      &self,
      inner: T,
  ) -> io::Result<Arc<Source<T>>>
  where T: RorW + Send + Sync + 'static
  {
      let mut sources = self.sources.lock().unwrap();
      let key = sources.next_vacant();
      let source = Arc::new(Source::new(
          Mutex::new(RateLimited::new_lb(inner, 65536)),
          key
      ));
      sources.insert(source.clone());
      Ok(source)
  }

  /// Deregisters an I/O source from the reactor.
  pub(crate) fn remove_io<T>(&self, source: &Source<T>) -> io::Result<()> {
      let mut sources = self.sources.lock().unwrap();
      sources.remove(source.key);
      Ok(())
  }

  pub(crate) async fn main_loop_async(&self) {
    loop {
      let mut wakers = Vec::new();

      let tick_length = Duration::from_millis(1); // rate-limit every 1 ms
      let target = *self.last_tick.lock().unwrap() + tick_length;
      let now = Instant::now();
      if target > now {
        Timer::after(target - now).await;
      } else {
        println!("rwlim reactor running slow: {:?} {:?} {:?}", tick_length, now, target);
      }
      let mut last_tick = self.last_tick.lock().unwrap();
      if Instant::now() >= target {
        let tick = self
            .ticker
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(1);
        let mut sources = self.sources.lock().unwrap();
        let mut total_r = 0;
        let mut total_w = 0;

        // calculate demand from buffers
        let mut demand = HashMap::new();
        for (key, source) in sources.iter_mut() {
          let rl = &mut *source.raw.lock().unwrap();
          if rl.inner.can_read() {
            rl.pre_read();
            let (_wasted, used) = rl.rbuf.reset_usage();
            total_r += used;
            // TODO: do something with the waste, e.g. to give more allowance
            demand.insert((key, SourceDirection::Read), rl.rbuf.get_demand());
          }
          if rl.inner.can_write() {
            let (_wasted, used) = rl.wbuf.reset_usage();
            total_w += used;
            // TODO: do something with the waste, e.g. to give more allowance
            demand.insert((key, SourceDirection::Write), rl.wbuf.get_demand());
          }
        }
        if total_r > 0 || total_w > 0 {
          log::trace!("main_loop_async: tick {}: RX {}, TX {}", tick, total_r, total_w);
        }

        // calculate allowance & make it effective
        let allowance = derive_allowance(demand);
        for (key, source) in sources.iter_mut() {
          let rl = &mut *source.raw.lock().unwrap();
          if rl.inner.can_read() {
            rl.rbuf.add_allowance(*allowance.get(&(key, SourceDirection::Read)).unwrap());
            if rl.is_readable() {
              let _ = source.wake(&mut wakers, true, false, tick);
            }
          }
          if rl.inner.can_write() {
            rl.wbuf.add_allowance(*allowance.get(&(key, SourceDirection::Write)).unwrap());
            rl.post_write();
            if rl.is_writable() {
              let _ = source.wake(&mut wakers, false, true, tick);
            }
          }
        }

        *last_tick = Instant::now();
      }
      drop(last_tick);

      // Wake up ready tasks.
      for waker in wakers {
          // Don't let a panicking waker blow everything up.
          let _ = panic::catch_unwind(|| waker.wake());
      }
    }
  }
}

pub(crate) type Source<T> = GSource<Reactor, Mutex<RateLimited<T>>>;

impl<T: ?Sized> SourceReactor<T> for Reactor {
    fn get_current_tick() -> usize {
        Reactor::get().ticker.load(Ordering::SeqCst)
    }

    fn poller_interest(_raw: &T, _key: usize, _readable: bool, _writable: bool) -> io::Result<()> {
        Ok(())
    }
}
