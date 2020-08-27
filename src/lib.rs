pub mod limit;
pub mod reactor;
pub mod stats;
pub mod testing;
pub mod util;
pub mod sys;

use std::fmt::Debug;
use std::future::Future;
use std::io::{self, IoSlice, IoSliceMut, Read, Write};
use std::pin::Pin;
use std::mem::ManuallyDrop;
use std::net::{Shutdown, TcpStream};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_lite::io::{AsyncRead, AsyncWrite};
use futures_lite::{future, pin};

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

// copied from async-io, except:
// self.get_mut() replaced with lock() / RateLimited
// TODO: figure out a way to de-duplicate with them
impl<T> RLAsync<T> {
    pub async fn readable(&self) -> io::Result<()> {
        self.source.readable().await
    }
    pub async fn writable(&self) -> io::Result<()> {
        self.source.writable().await
    }
    pub async fn read_with_mut<R>(
        &mut self,
        op: impl FnMut(&mut RateLimited<T>) -> io::Result<R>,
    ) -> io::Result<R> {
        let mut op = op;
        loop {
            // If there are no blocked readers, attempt the read operation.
            if !self.source.readers_registered() {
                let mut inner = self.source.inner.lock().unwrap();
                match op(&mut inner) {
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                    res => return res,
                }
            }
            // Wait until the I/O handle becomes readable.
            optimistic(self.readable()).await?;
        }
    }
    pub async fn write_with_mut<R>(
        &mut self,
        op: impl FnMut(&mut RateLimited<T>) -> io::Result<R>,
    ) -> io::Result<R> {
        let mut op = op;
        loop {
            // If there are no blocked readers, attempt the write operation.
            if !self.source.writers_registered() {
                let mut inner = self.source.inner.lock().unwrap();
                match op(&mut inner) {
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                    res => return res,
                }
            }
            // Wait until the I/O handle becomes writable.
            optimistic(self.writable()).await?;
        }
    }
}

// copied from async-io
impl<T: Read> AsyncRead for RLAsync<T> {
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
impl<T: Write> AsyncWrite for RLAsync<T>
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
        let inner = self.source.inner.lock().unwrap();
        Poll::Ready(shutdown_write(inner.inner.as_raw_source()))
    }
}

// copied from async-io
fn poll_future<T>(cx: &mut Context<'_>, fut: impl Future<Output = T>) -> Poll<T> {
    pin!(fut);
    fut.poll(cx)
}

// copied from async-io
async fn optimistic(fut: impl Future<Output = io::Result<()>>) -> io::Result<()> {
    let mut polled = false;
    pin!(fut);

    future::poll_fn(|cx| {
        if !polled {
            polled = true;
            fut.as_mut().poll(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    })
    .await
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
