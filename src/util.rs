//! Various utils.

use std::fmt::Debug;
use std::io::{self, Read, Write};
use crate::sys::{AsRawSource, RawSource};

/// Trait that unifies `Read` + `Write` to make code easier to write.
pub trait RorW: Read + Write + Debug {
  fn can_read(&self) -> bool;
  fn can_write(&self) -> bool;
}

#[derive(Debug)]
#[repr(transparent)]
pub struct RW<T>(pub T);

impl<T> AsRawSource for RW<T> where T: AsRawSource {
  fn as_raw_source(&self) -> RawSource {
    self.0.as_raw_source()
  }
}

pub fn as_rw_ref<T>(x: &T) -> &RW<T> {
  unsafe {
    std::mem::transmute(x)
  }
}

impl<T> Read for RW<T> where T: Read {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    self.0.read(buf)
  }
}

impl<T> Write for RW<T> where T: Write {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    self.0.write(buf)
  }
  fn flush(&mut self) -> io::Result<()> {
    self.0.flush()
  }
}

impl<T> RorW for RW<T> where T: Read + Write + Debug {
  fn can_read(&self) -> bool {
    true
  }
  fn can_write(&self) -> bool {
    true
  }
}

#[derive(Debug)]
#[repr(transparent)]
pub struct RO<T>(pub T);

impl<T> Read for RO<T> where T: Read {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    self.0.read(buf)
  }
}

impl<T> Write for RO<T> where T: Write {
  fn write(&mut self, _: &[u8]) -> io::Result<usize> {
    panic!("tried to write a RO")
  }
  fn flush(&mut self) -> io::Result<()> {
    panic!("tried to flush a RO")
  }
}

impl<T> RorW for RO<T> where T: Read + Write + Debug {
  fn can_read(&self) -> bool {
    true
  }
  fn can_write(&self) -> bool {
    false
  }
}

#[derive(Debug)]
#[repr(transparent)]
pub struct WO<T>(pub T);

impl<T> Read for WO<T> where T: Read {
  fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
    panic!("tried to read a WO")
  }
}

impl<T> Write for WO<T> where T: Write {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    self.0.write(buf)
  }
  fn flush(&mut self) -> io::Result<()> {
    self.0.flush()
  }
}

impl<T> RorW for WO<T> where T: Read + Write + Debug {
  fn can_read(&self) -> bool {
    false
  }
  fn can_write(&self) -> bool {
    true
  }
}
