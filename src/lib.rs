#![feature(generic_associated_types)]

pub mod limit;
pub mod reactor;
pub mod stats;
pub mod testing;
pub mod util;
pub mod sys;

use std::borrow::*;
use std::fmt::Debug;
use std::io::{self, IoSlice, IoSliceMut, Read, Write};
use std::pin::Pin;
use std::mem::ManuallyDrop;
use std::net::{Shutdown, TcpStream};
use std::sync::{Arc, MutexGuard};
use std::task::{Context, Poll};

use async_io::*;
use async_trait::async_trait;

use futures_lite::io::{AsyncRead, AsyncWrite};

use crate::limit::RateLimited;
use crate::util::RorW;
use crate::reactor::{Reactor, Source};
use crate::sys::*;


#[derive(Debug)]
pub struct RLAsync<T> {
  source: Arc<Source<T>>,
}

impl<T> RLAsync<T> {
    pub fn new(io: T) -> io::Result<RLAsync<T>> where T: RorW + Send + Sync + 'static {
        Ok(RLAsync {
            source: Reactor::get().insert_io(io)?,
        })
    }
}

impl<T> Drop for RLAsync<T> {
    fn drop(&mut self) {
        // Deregister and ignore errors because destructors should not panic.
        let _ = Reactor::get().remove_io(&self.source);
    }
}

/// Wrapper for `MutexGuard` that implements `Borrow`/`BorrowMut`.
pub struct MGW<'a, T: ?Sized>(pub MutexGuard<'a, T>);

impl<T: ?Sized> Borrow<T> for MGW<'_, T> {
    fn borrow(&self) -> &T {
        &*self.0
    }
}

impl<T: ?Sized> BorrowMut<T> for MGW<'_, T> {
    fn borrow_mut(&mut self) -> &mut T {
        &mut *self.0
    }
}

#[async_trait]
impl<T> AnAsync<RateLimited<T>> for RLAsync<T> where T: Send + Sync {
    type Borrow<'a> where T: 'a = MGW<'a, RateLimited<T>>;
    type BorrowMut<'a> where T: 'a = MGW<'a, RateLimited<T>>;

    fn get_ref<'a>(&'a self) -> MGW<RateLimited<T>> {
        MGW(self.source.raw.lock().unwrap())
    }

    fn get_mut<'a>(&'a mut self) -> MGW<RateLimited<T>> {
        MGW(self.source.raw.lock().unwrap())
    }

    async fn readable(&self) -> io::Result<()> {
        self.source.readable().await
    }

    async fn writable(&self) -> io::Result<()> {
        self.source.writable().await
    }

    fn readers_registered(&self) -> bool {
        self.source.readers_registered()
    }

    fn writers_registered(&self) -> bool {
        self.source.writers_registered()
    }
}

// copied from async-io
impl<T: Read + Send + Sync> AsyncRead for RLAsync<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        poll_future(cx, self.read_with_mut(|io| io.read(buf)))
    }

    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        poll_future(cx, self.read_with_mut(|io| io.read_vectored(bufs)))
    }
}

// copied from async-io
impl<T: Write + Send + Sync> AsyncWrite for RLAsync<T>
where
    T: AsRawSource
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        poll_future(cx, self.write_with_mut(|io| io.write(buf)))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        poll_future(cx, self.write_with_mut(|io| io.write_vectored(bufs)))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        poll_future(cx, self.write_with_mut(|io| io.flush()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let inner = self.source.raw.lock().unwrap();
        Poll::Ready(shutdown_write(inner.inner.as_raw_source()))
    }
}

// copied from async-io
fn shutdown_write(raw: RawSource) -> io::Result<()> {
    // This may not be a TCP stream, but that's okay. All we do is attempt a `shutdown()` on the
    // raw descriptor and ignore errors.
    let stream = unsafe {
        ManuallyDrop::new(
            TcpStream::from_raw_source(raw),
        )
    };

    // If the socket is a TCP stream, the only actual error can be ENOTCONN.
    match stream.shutdown(Shutdown::Write) {
        Err(err) if err.kind() == io::ErrorKind::NotConnected => Err(err),
        _ => Ok(()),
    }
}
