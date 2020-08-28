/*! Reactor for rate-limited streams.

This is only a partial reactor to add rate-limiting to byte streams; it does
not cover other types of events like connect/listen. For that, use a "normal"
reactor like the one in async-io.

Currently this reactor runs as an asynchronous task inside the full reactor of
async-io. This could in theory be changed, and we could run the main loop as a
standalone thread with blocking sleeps. It's not clear that this would give a
great benefit however, so keeping the current solution works OK for now.

*/

use std::panic;
use std::sync::{atomic::*, Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};
use std::io;
use once_cell::sync::Lazy;
use vec_arena::Arena;

use futures_lite::*;
use async_io::Timer;
use smol::Task;

use crate::limit::RateLimited;
use crate::util::RorW;


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
      let source = Arc::new(Source {
          inner: Mutex::new(RateLimited::new_lb(inner, 65536)),
          key,
          wakers: Mutex::new(Wakers {
              tick_readable: 0,
              tick_writable: 0,
              readers: Vec::new(),
              writers: Vec::new(),
          }),
          wakers_registered: AtomicU8::new(0),
      });
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

        for (key, source) in self.sources.lock().unwrap().iter_mut() {
          let rl = &mut *source.inner.lock().unwrap();
          if rl.inner.can_read() {
            rl.pre_read();
            rl.rbuf.reset_usage();
            // TODO: actually perform rate-limiting. the current code ought not
            // to be (but is) much slower than the async-io version.
            rl.rbuf.add_allowance(rl.rbuf.get_demand());
            if rl.is_readable() {
              self.react_evt(&mut wakers, &**source, true, false, tick);
            }
          }
          if rl.inner.can_write() {
            rl.wbuf.reset_usage();
            // TODO: actually perform rate-limiting. the current code ought not
            // to be (but is) much slower than the async-io version.
            rl.wbuf.add_allowance(rl.wbuf.get_demand());
            rl.post_write();
            if rl.is_writable() {
              self.react_evt(&mut wakers, &**source, false, true, tick);
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

  // copied from async-io Reactor.react, except references to poller removed
  fn react_evt(&self, wakers: &mut Vec<Waker>, source: &Source<dyn RorW>, ev_readable: bool, ev_writable: bool, tick: usize) {
      let mut w = source.wakers.lock().unwrap();

      // Wake readers if a readability event was emitted.
      if ev_readable {
          w.tick_readable = tick;
          wakers.append(&mut w.readers);
          source
              .wakers_registered
              .fetch_and(!READERS_REGISTERED, Ordering::SeqCst);
      }

      // Wake writers if a writability event was emitted.
      if ev_writable {
          w.tick_writable = tick;
          wakers.append(&mut w.writers);
          source
              .wakers_registered
              .fetch_and(!WRITERS_REGISTERED, Ordering::SeqCst);
      }
  }
}

// copied from async-io, except inner field
#[derive(Debug)]
pub struct Source<T> where T: ?Sized {
  /// The key of this source obtained during registration.
  key: usize,

  /// Tasks interested in events on this source.
  wakers: Mutex<Wakers>,

  /// Whether there are wakers interrested in events on this source.
  wakers_registered: AtomicU8,

  pub(crate) inner: Mutex<RateLimited<T>>,
}

// copied from async-io. TODO: figure out a way to deduplicate
/// Tasks interested in events on a source.
#[derive(Debug)]
struct Wakers {
  /// Last reactor tick that delivered a readability event.
  tick_readable: usize,

  /// Last reactor tick that delivered a writability event.
  tick_writable: usize,

  /// Tasks waiting for the next readability event.
  readers: Vec<Waker>,

  /// Tasks waiting for the next writability event.
  writers: Vec<Waker>,
}

const READERS_REGISTERED: u8 = 1 << 0;
const WRITERS_REGISTERED: u8 = 1 << 1;

// copied from async-io, except references to reactor.poller removed
// TODO: figure out a way to deduplicate
impl<T> Source<T> {
    /// Waits until the I/O source is readable.
    pub(crate) async fn readable(&self) -> io::Result<()> {
        let mut ticks = None;

        future::poll_fn(|cx| {
            let mut w = self.wakers.lock().unwrap();

            // Check if the reactor has delivered a readability event.
            if let Some((a, b)) = ticks {
                // If `tick_readable` has changed to a value other than the old reactor tick, that
                // means a newer reactor tick has delivered a readability event.
                if w.tick_readable != a && w.tick_readable != b {
                    return Poll::Ready(Ok(()));
                }
            }

            // If there are no other readers, re-register in the reactor.
            if w.readers.is_empty() {
                self.wakers_registered
                    .fetch_or(READERS_REGISTERED, Ordering::SeqCst);
            }

            // Register the current task's waker if not present already.
            if w.readers.iter().all(|w| !w.will_wake(cx.waker())) {
                w.readers.push(cx.waker().clone());
                if limit_waker_list(&mut w.readers) {
                    self.wakers_registered
                        .fetch_and(!READERS_REGISTERED, Ordering::SeqCst);
                }
            }

            // Remember the current ticks.
            if ticks.is_none() {
                ticks = Some((
                    Reactor::get().ticker.load(Ordering::SeqCst),
                    w.tick_readable,
                ));
            }

            Poll::Pending
        })
        .await
    }

    pub(crate) fn readers_registered(&self) -> bool {
        self.wakers_registered.load(Ordering::SeqCst) & READERS_REGISTERED != 0
    }

    /// Waits until the I/O source is writable.
    pub(crate) async fn writable(&self) -> io::Result<()> {
        let mut ticks = None;

        future::poll_fn(|cx| {
            let mut w = self.wakers.lock().unwrap();

            // Check if the reactor has delivered a writability event.
            if let Some((a, b)) = ticks {
                // If `tick_writable` has changed to a value other than the old reactor tick, that
                // means a newer reactor tick has delivered a writability event.
                if w.tick_writable != a && w.tick_writable != b {
                    return Poll::Ready(Ok(()));
                }
            }

            // If there are no other writers, re-register in the reactor.
            if w.writers.is_empty() {
                self.wakers_registered
                    .fetch_or(WRITERS_REGISTERED, Ordering::SeqCst);
            }

            // Register the current task's waker if not present already.
            if w.writers.iter().all(|w| !w.will_wake(cx.waker())) {
                w.writers.push(cx.waker().clone());
                if limit_waker_list(&mut w.writers) {
                    self.wakers_registered
                        .fetch_and(!WRITERS_REGISTERED, Ordering::SeqCst);
                }
            }

            // Remember the current ticks.
            if ticks.is_none() {
                ticks = Some((
                    Reactor::get().ticker.load(Ordering::SeqCst),
                    w.tick_writable,
                ));
            }

            Poll::Pending
        })
        .await
    }

    pub(crate) fn writers_registered(&self) -> bool {
        self.wakers_registered.load(Ordering::SeqCst) & WRITERS_REGISTERED != 0
    }
}

/// Wakes up all wakers in the list if it grew too big and returns whether it did.
///
/// The waker list keeps growing in pathological cases where a single async I/O handle has lots of
/// different reader or writer tasks. If the number of interested wakers crosses some threshold, we
/// clear the list and wake all of them at once.
///
/// This strategy prevents memory leaks by bounding the number of stored wakers. However, since all
/// wakers get woken, tasks might simply re-register their interest again, thus creating an
/// infinite loop and burning CPU cycles forever.
///
/// However, we don't worry about such scenarios because it's very unlikely to have more than two
/// actually concurrent tasks operating on a single async I/O handle. If we happen to cross the
/// aforementioned threshold, we have bigger problems to worry about.
fn limit_waker_list(wakers: &mut Vec<Waker>) -> bool {
    if wakers.len() > 50 {
        for waker in wakers.drain(..) {
            // Don't let a panicking waker blow everything up.
            let _ = panic::catch_unwind(|| waker.wake());
        }
        true
    } else {
        false
    }
}
