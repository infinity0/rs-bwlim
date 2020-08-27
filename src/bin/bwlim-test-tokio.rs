use bwlim::testing::*;

use std::net::{SocketAddr, Shutdown};
use std::process::Stdio;
use std::thread;
use std::time::Duration;

use futures::future;
use tokio::net::{TcpStream, TcpListener};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_util::compat::*;

#[tokio::main]
async fn main() {
  let (test_bytes, listen, connect, host, _) = get_args();

  let listen_addr = SocketAddr::new("127.0.0.1".parse().unwrap(), listen);
  let connect_addr = SocketAddr::new("127.0.0.1".parse().unwrap(), connect);
  println!("main: server listen {}, client connect {}", listen_addr, connect_addr);

  // ssh loopback proxy
  let ssh = host.map(|host| {
    let ssh = Command::new("ssh")
      .args(get_ssh_args(host, connect, listen))
      .stdin(Stdio::piped())
      .spawn().unwrap();
    thread::sleep(Duration::from_millis(1500)); // give some time to set up the tunnels
    println!("main: set up ssh loopback proxy from {} to {}", connect, listen);
    ssh
  });

  // server thread
  let mut listener = TcpListener::bind(listen_addr).await.unwrap();
  let (mut shutdown, mut is_shutdown) = mpsc::channel(1);
  // ^ can't use oneshot because we need to repeatedly await on the receive handle
  let server = tokio::spawn(async move {
    let mut workers = Vec::new();
    loop {
      tokio::select! {
        val = listener.accept() => {
          let (socket, _) = val.unwrap();
          workers.push(tokio::spawn(server_thread(Tokio02AsyncReadCompatExt::compat(socket))));
        },
        _ = is_shutdown.recv() => {
          break;
        }
      }
    }
    println!("server: waiting for {} workers to close...", workers.len());
    for w in workers.into_iter() {
      w.await.unwrap();
    }
    println!("server: shutting down");
  });

  // client threads
  let clients = [0; 2].iter().map(|_| {
    tokio::spawn(async move {
      let mut stream = Tokio02AsyncWriteCompatExt::compat_write(TcpStream::connect(connect_addr).await.unwrap());
      client_thread(&mut stream, test_bytes).await;
      stream.into_inner().shutdown(Shutdown::Both).unwrap();
    })
  }).collect::<Vec<_>>();
  future::join_all(clients).await.into_iter().for_each(Result::unwrap);

  shutdown.send(()).await.unwrap();
  server.await.unwrap();

  if let Some(ssh) = ssh {
    // wait_with_output closes stdin internally, so we don't need to
    //ssh.stdin.as_mut().unwrap().shutdown().await.unwrap();
    ssh.wait_with_output().await.unwrap();
    println!("main: torn down ssh loopback proxy from {} to {}", connect, listen);
    println!("main: if clients were much quicker than servers, that's probably due to buffering in your ssh process");
    println!("main: trying increasing --bytes and the effect will probably go away");
  }
}
