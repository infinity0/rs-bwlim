//! Data structures to help perform rate limiting.

use std::collections::{HashMap, VecDeque};
use std::cmp;
use std::fmt::Debug;
use std::io::{self, Read, Write, ErrorKind};
use std::result::Result;

use bytes::{BytesMut, Buf, BufMut};

use crate::util::RorW;
use self::Status::*;

/// Generic buffer for rate-limiting, both reading and writing.
#[derive(Debug)]
pub struct RLBuf {
  /// Buffer to help determine demand, for rate-limiting.
  buf: BytesMut,
  /// Index into `buf`, of the first data not allowed to be used. Everything
  /// before it will be used upon request.
  ///
  /// "Used" means `read` by a higher layer, or `write` by a lower layer.
  allowance: usize,
  /// Amount of data read out since last call to `reset_usage`.
  last_used: usize,
}

impl RLBuf {
  /** Create a new `RLBuf` with the given lower bound on the initial capacity.

  The actual capacity can be got later with `get_demand_cap`.
  */
  pub fn new_lb(init: usize) -> RLBuf {
    RLBuf {
      buf: BytesMut::with_capacity(init),
      allowance: 0,
      last_used: 0,
    }
  }

  /** Get the current demand.

  For higher-level rate-limiting logic, to determine how to rate-limit.
  */
  pub fn get_demand(&self) -> usize {
    self.buf.len()
  }

  /** Get the current buffer capacity, i.e. allocated memory.

  For higher-level rate-limiting logic, to monitor resource usage, to help it
  analyse how efficient it is.
  */
  pub fn get_demand_cap(&self) -> usize {
    self.buf.capacity()
  }

  pub fn get_demand_remaining(&self) -> usize {
    self.buf.capacity() - self.buf.len()
  }

  /** Add the allowance, which must not be greater than the demand.

  For higher-level rate-limiting logic, as it performs the rate-limiting.
  */
  pub fn add_allowance(&mut self, allowance: usize) {
    if self.allowance + allowance > self.get_demand() {
      panic!("allowance > demand");
    }
    self.allowance += allowance
  }

  /** Return the latest usage figures & reset them back to zero.

  The first number is the number of allowed bytes that were unused.
  The second number is the number of allowed bytes that were used.

  For higher-level rate-limiting logic, before rate-limiting is performed, to
  detect consumers that consumed even more slowly than the rate limit in the
  previous cycle. In response to this, the higher-level logic should give less
  allowance for this consumer, to avoid waste.
  */
  pub fn reset_usage(&mut self) -> (usize, usize) {
    let wasted = self.allowance;
    let used = self.last_used;
    self.allowance = 0;
    self.last_used = 0;
    (wasted, used)
  }

  fn record_demand(&mut self, buf: &[u8]) {
    self.buf.extend_from_slice(buf);
  }

  fn add_demand_cap(&mut self, more: usize) {
    self.buf.reserve(more + self.get_demand_remaining());
  }

  fn take_allowance(&mut self, taken: usize) {
    if taken > self.allowance {
      panic!("taken > allowance");
    }
    self.allowance -= taken;
    self.last_used += taken;
  }

  fn consume_read(&mut self, buf: &mut [u8]) -> usize {
    let to_drain = cmp::min(buf.len(), self.allowance);
    self.buf.copy_to_slice(&mut buf[..to_drain]);
    self.buf.reserve(to_drain);
    self.take_allowance(to_drain);
    to_drain
  }

  fn consume_write<F, E>(&mut self, sz: usize, mut write: F) -> (usize, Option<E>)
  where F: FnMut (&[u8]) -> Result<usize, E> {
    let mut used = 0;
    let mut err = None;
    let to_drain = cmp::min(self.buf.len(), sz);
    match write(&self.buf[..to_drain]) {
      Ok(n) => used += n,
      Err(e) => err = Some(e),
    }
    self.buf.advance(used);
    self.add_demand_cap(used);
    self.take_allowance(used);
    (used, err)
  }
}

fn unwrap_err_or<T, E>(r: Result<T, E>, de: E) -> E {
  match r {
    Ok(_) => de,
    Err(e) => e,
  }
}

#[derive(Debug, PartialEq, Eq)]
enum Status {
  SOpen,
  SOk, // eof
  SErr
}

/** Rate-limited asynchronous analogue of `std::io::BufReader` + `std::io::BufWriter`.

You **must** call `flush()` before dropping this (which closes the stream).
This is even more important than doing so on `BufWriter` - if not, you may lose
data. See https://internals.rust-lang.org/t/asynchronous-destructors/11127/49
for an in-depth explanation.
*/
#[derive(Debug)]
pub struct RateLimited<T> where T: ?Sized {
    rstatus: Status,
    pub(crate) rbuf: RLBuf,
    wstatus: Status,
    pub(crate) wbuf: RLBuf,
    pub(crate) inner: T,
}

impl<T> RateLimited<T> {
  /** Create a new `RateLimited` with the given initial capacity.

  The inner stream must already be in non-blocking mode.
  */
  pub fn new_lb(inner: T, init: usize) -> RateLimited<T> {
    RateLimited {
      inner: inner,
      rstatus: SOpen,
      rbuf: RLBuf::new_lb(init),
      wstatus: SOpen,
      wbuf: RLBuf::new_lb(init),
    }
  }
}

impl<T> RateLimited<T> where T: RorW + ?Sized {
  /** Do a pre-read.

  That is, do a non-blocking read from the underlying handle, filling up the
  remaining part of `rbuf`.

  This is to be used by higher-level code, before it performs the rate-limiting.
  */
  pub fn pre_read(&mut self) {
    match self.rstatus {
      SOpen => {
        let remain = self.rbuf.get_demand_remaining();
        if remain == 0 {
          return;
        }
        // TODO: replace with https://github.com/rust-lang/rfcs/pull/2930
        let mut buf: &mut [u8] = unsafe { std::mem::transmute(self.rbuf.buf.bytes_mut()) };
        match self.inner.read(&mut buf) { // TODO: assert non-blocking
          Ok(0) => {
            self.rstatus = SOk;
          },
          Ok(n) => {
            unsafe {
              self.rbuf.buf.advance_mut(n);
            }
            if n >= remain {
              // TODO: automatically grow the buffer capacity
              log::debug!("rbuf pre_read filled buffer");
            }
          },
          Err(e) => match e.kind() {
            ErrorKind::WouldBlock => (),
            ErrorKind::Interrupted => (),
            _ => {
              // println!("pre_read: {:?}", e);
              self.rstatus = SErr;
            }
          },
        }
      },
      _ => (), // already finished
    }
  }

  pub fn is_readable(&self) -> bool {
    self.rstatus != SOpen || self.rbuf.allowance > 0
  }

  /** Do a post-write.

  That is, do a non-blocking write to the underlying handle, up to the current
  allowance of `wbuf`.

  This is to be used by higher-level code, after it performs the rate-limiting.
  */
  pub fn post_write(&mut self) {
    self.post_write_exact(self.wbuf.allowance);
  }

  pub fn is_writable(&self) -> bool {
    self.wstatus != SOpen || self.wbuf.get_demand_remaining() > 0
  }

  // extra param is exposed for testing only
  fn post_write_exact(&mut self, sz: usize) -> Option<io::Error> {
    match self.wbuf.get_demand() {
      0 => None,
      _ => match self.wbuf.allowance {
        0 => None,
        _ => {
          let w = &mut self.inner;
          let (_, err) = self.wbuf.consume_write(sz, |b| w.write(b));
          if let Some(e) = err.as_ref() {
            match e.kind() {
              ErrorKind::WouldBlock => (),
              ErrorKind::Interrupted => (),
              _ => {
                self.wstatus = SErr;
              },
            }
          }
          err
        }
      }
    }
  }
}

impl<T> Read for RateLimited<T> where T: Read {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    match self.rbuf.get_demand() {
      0 => match self.rstatus {
        SOpen => Err(io::Error::new(ErrorKind::WouldBlock, "")),
        SOk => Ok(0),
        SErr => Err(unwrap_err_or(self.inner.read(&mut []), io::Error::new(ErrorKind::Other, "Ok after Err"))),
      },
      _ => match self.rbuf.allowance {
        0 => Err(io::Error::new(ErrorKind::WouldBlock, "")),
        _ => Ok(self.rbuf.consume_read(buf)),
      }
    }
  }
}

impl<T> Write for RateLimited<T> where T: Write {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    match self.wstatus {
      SOpen => {
        // TODO: figure out when it's appropriate to automatically grow the buffer capacity
        let remain = self.wbuf.get_demand_remaining();
        match remain {
          0 => Err(io::Error::new(ErrorKind::WouldBlock, "")),
          _ => {
            let n = cmp::min(buf.len(), remain);
            self.wbuf.record_demand(&buf[..n]);
            Ok(n)
          }
        }
      },
      SOk => Ok(0),
      SErr => Err(unwrap_err_or(self.inner.write(&mut []), io::Error::new(ErrorKind::Other, "Ok after Err"))),
    }
  }

  fn flush(&mut self) -> io::Result<()> {
    match self.wstatus {
      SErr =>
        // if there was an error, wbuf might not have been consumed, so output error even if wbuf is non-empty
        Err(unwrap_err_or(self.inner.write(&mut []), io::Error::new(ErrorKind::Other, "Ok after Err"))),
      _ => match self.wbuf.get_demand() {
        0 => {
          //println!("flush OK");
          Ok(())
        },
        _ => {
          //println!("flush waiting :( {} {}", self.wbuf.get_demand(), self.wbuf.allowance);
          Err(io::Error::new(ErrorKind::WouldBlock, ""))
        }, // something else is responsible for calling post_write
      }
    }
  }
}

#[derive(Debug)]
pub struct UsageStats {
  samples: VecDeque<(usize, usize)>,
  max_samples: usize,
  current_usage: (usize, usize), // (waste, used)
}

impl UsageStats {
  pub fn new() -> UsageStats {
    UsageStats {
      samples: VecDeque::new(),
      max_samples: 4096, // TODO: make configurable
      current_usage: (0, 0),
    }
  }

  pub fn add_current_usage(&mut self, usage: (usize, usize)) {
    self.current_usage.0 += usage.0;
    self.current_usage.1 += usage.1;
  }

  pub fn finalise_current_usage(&mut self) -> (usize, usize) {
    while self.samples.len() >= self.max_samples {
      self.samples.pop_front();
    }
    let usage = self.current_usage;
    self.samples.push_back(usage);
    self.current_usage = (0, 0);
    usage
  }

  pub fn estimate_next_usage(&mut self) -> usize {
    // TODO: something smarter
    // TODO: do something with the waste, e.g. to give more allowance
    self.samples.back().unwrap().1
  }
}

pub fn derive_allowance<K>(demand: HashMap<K, usize>) -> HashMap<K, usize> {
  // TODO: actually perform rate-limiting. the current code ought not
  // to be (but is) much slower than the async-io version, however
  // this only noticeable on localhost-localhost transfers.
  demand
}

#[cfg(test)]
mod tests {
  use std::fs::*;
  use std::fmt::Debug;
  use std::io;
  use std::io::*;
  use std::assert;

  use crate::sys::*;
  use crate::util::*;

  use super::*;

  fn assert_would_block<T>(res: io::Result<T>) where T: Debug {
    match res {
      Err(e) => assert_eq!(e.kind(), ErrorKind::WouldBlock),
      x => {
        println!("{:?}", x);
        assert!(false);
      },
    }
  }

  fn assert_error<T>(res: io::Result<T>) where T: Debug {
    match res {
      Err(e) => match e.kind() {
        ErrorKind::WouldBlock => assert!(false),
        ErrorKind::Interrupted => assert!(false),
        _ => (),
      },
      x => {
        println!("{:?}", x);
        assert!(false);
      },
    }
  }

  fn assert_num_bytes(res: io::Result<usize>, s: usize) {
    match res {
      Ok(n) => assert_eq!(n, s),
      x => {
        println!("{:?}", x);
        assert!(false);
      },
    }
  }

  // TODO: /dev/null etc is not a RawSocket in windows

  #[test]
  fn read_eof_ok() -> io::Result<()> {
    let file = File::open("/dev/null")?;
    set_non_blocking(file.as_raw_source())?;
    let mut bf = RateLimited::new_lb(RO(file), 1);
    let mut buf = [0].repeat(1);
    assert_would_block(bf.read(&mut buf));
    bf.pre_read();
    assert_num_bytes(bf.read(&mut buf), 0); // eof
    Ok(())
  }

  #[test]
  fn read_zero_err() -> io::Result<()> {
    let file = File::open("/dev/zero")?;
    set_non_blocking(file.as_raw_source())?;
    let unsafe_f = unsafe { File::from_raw_source(file.as_raw_source()) };

    let sd = 4095; // in case VecDeque changes implementation, this needs to be changed
    let sx = 1024;
    let sy = 1024;
    let mut bf = RateLimited::new_lb(RO(file), sd);
    assert_eq!(sd, bf.rbuf.get_demand_cap());
    assert_eq!(0, bf.rbuf.get_demand());
    let mut buf = [0].repeat(sx);

    assert_would_block(bf.read(&mut buf));
    bf.pre_read();
    assert_eq!(sd, bf.rbuf.get_demand());
    assert_would_block(bf.read(&mut buf));

    bf.rbuf.add_allowance(sx);
    assert_num_bytes(bf.read(&mut buf), sx);
    assert_eq!(sd - sx, bf.rbuf.get_demand());

    bf.rbuf.add_allowance(sx + sy);
    assert_num_bytes(bf.read(&mut buf), sx);
    assert_eq!(sd - sx - sx, bf.rbuf.get_demand());

    assert_eq!(bf.rbuf.reset_usage(), (sy, sx + sy));
    // sy bytes of allowance were wasted
    assert_would_block(bf.read(&mut buf));

    assert_eq!(bf.rbuf.reset_usage(), (0, 0));
    assert_eq!(sd - sx - sx, bf.rbuf.get_demand());
    assert_eq!(SOpen, bf.rstatus);

    drop(unsafe_f); // close f, to force an error on the underlying stream
    bf.pre_read();
    assert_eq!(sd - sx - sx, bf.rbuf.get_demand());
    assert_eq!(SErr, bf.rstatus);
    bf.rbuf.add_allowance(sd - sx - sx);
    assert_num_bytes(bf.read(&mut buf), sx);
    assert!(sd - sx - sx - sx <= sx); // otherwise next step fails
    assert_num_bytes(bf.read(&mut buf), sd - sx - sx - sx);
    assert_error(bf.read(&mut buf));
    assert_error(bf.read(&mut buf));
    assert_error(bf.read(&mut buf));

    Ok(())
  }

  #[test]
  fn write_eof_err() -> io::Result<()> {
    let file = File::open("/dev/zero")?;
    set_non_blocking(file.as_raw_source())?;
    let mut bf = RateLimited::new_lb(WO(file), 1);
    let buf = [0].repeat(1);
    assert_num_bytes(bf.write(&buf), 1);
    bf.post_write();
    assert_eq!(bf.wstatus, SOpen);
    bf.wbuf.add_allowance(1);
    bf.post_write();
    assert_eq!(bf.wstatus, SErr);
    assert_error(bf.flush());
    assert_error(bf.flush());
    assert_error(bf.flush());
    Ok(())
  }

  #[test]
  fn write_null_ok() -> io::Result<()> {
    let file = OpenOptions::new().write(true).open("/dev/null")?;
    set_non_blocking(file.as_raw_source())?;

    let sd = 4095; // in case VecDeque changes implementation, this needs to be changed
    let sx = 1024;
    let sy = 1024;
    let mut bf = RateLimited::new_lb(WO(file), sd);
    assert_eq!(sd, bf.wbuf.get_demand_cap());
    assert_eq!(sd, bf.wbuf.get_demand_remaining());
    assert_eq!(0, bf.wbuf.get_demand());
    let buf = [0].repeat(sd + sx);

    bf.flush()?;
    assert_num_bytes(bf.write(&buf), sd);
    assert_eq!(sd, bf.wbuf.get_demand());
    assert_would_block(bf.write(&buf[sd..]));

    bf.wbuf.add_allowance(sx);
    bf.post_write();
    assert_eq!(sd - sx, bf.wbuf.get_demand());

    bf.wbuf.add_allowance(sx + sy);
    bf.post_write_exact(sx);
    assert_eq!(sd - sx - sx, bf.wbuf.get_demand());

    assert_eq!(bf.wbuf.reset_usage(), (sy, sx + sy));
    // sy bytes of allowance were wasted
    assert!(bf.post_write_exact(0).is_none());

    assert_eq!(bf.wbuf.reset_usage(), (0, 0));
    assert_eq!(sd - sx - sx, bf.wbuf.get_demand());
    assert_eq!(SOpen, bf.wstatus);

    assert_num_bytes(bf.write(&buf), sx + sx);
    assert_eq!(sd, bf.wbuf.get_demand());
    assert_eq!(SOpen, bf.wstatus);
    bf.wbuf.add_allowance(sd);
    assert_would_block(bf.flush());
    assert_would_block(bf.flush());
    assert_would_block(bf.flush());
    bf.post_write();
    assert_eq!(0, bf.wbuf.get_demand());
    bf.flush()
  }
}
