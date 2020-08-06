use bwlim::testing::*;

use std::net::{SocketAddr, TcpStream, TcpListener};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use futures::prelude::*;
use futures::future;
use futures::channel::mpsc;
use async_io::Async;
use smol::Task;

async fn async_main() {
  let (test_bytes, listen, connect, host) = get_args();

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
  let listener = Async::<TcpListener>::bind(listen_addr).unwrap();
  let (mut shutdown, mut is_shutdown) = mpsc::channel(1);
  // ^ can't use oneshot because we need to repeatedly await on the receive handle
  let server = Task::spawn(async move {
    let mut workers = Vec::new();
    loop {
      futures::select! {
        val = listener.accept().fuse() => {
          let (socket, _) = val.unwrap();
          workers.push(Task::spawn(server_thread(socket)));
        },
        _ = is_shutdown.next() => {
          break;
        }
      }
    }
    println!("server: waiting for {} workers to close...", workers.len());
    for w in workers.into_iter() {
      w.await;
    }
    println!("server: shutting down");
  });

  // client threads
  let clients = [0; 2].iter().map(|_| {
    Task::spawn(async move {
      let mut stream = Async::<TcpStream>::connect(connect_addr).await.unwrap();
      client_thread(&mut stream, test_bytes).await;
      stream.close().await.unwrap();
    })
  }).collect::<Vec<_>>();
  future::join_all(clients).await;

  shutdown.send(()).await.unwrap();
  server.await;

  if let Some(mut ssh) = ssh {
    // wait closes stdin internally, so we don't need to
    ssh.wait().unwrap();
    println!("main: torn down ssh loopback proxy from {} to {}", connect, listen);
    println!("main: if clients were much quicker than servers, that's probably due to buffering in your ssh process");
    println!("main: trying increasing --bytes and the effect will probably go away");
  }
}

fn main() {
  smol::run(async_main())
}