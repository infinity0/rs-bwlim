// TODO: measure latency between client and corresponding server-worker

use crate::stats::BWStats;

use futures::io::{AsyncReadExt, AsyncWriteExt};
use clap::{App, Arg};

const TEST_CHUNK: &[u8] = &[0; 65536];

pub async fn server_thread<R: Unpin + AsyncReadExt>(idx: u64, mut socket: R) {
  println!("server {}: stream connected", idx);
  let mut stats = BWStats::new();

  let mut buf = [0; 65536];
  loop {
    let n = match socket.read(&mut buf).await {
      Ok(n) if n == 0 => break, // eof
      Ok(n) => {
        //println!("server {}: recv {} B", idx, n);
        n
      },
      Err(e) => {
        println!("server {}: failed to read from socket; err = {:?}", idx, e);
        break;
      }
    };
    stats.add(n);
  }

  //println!("server: stats: {:?}", stats);
  let (t, s) = stats.last();
  println!("server {}: total {} B recv in {} us: ~{} KBps", idx, s, t, if *t == 0 { 0 } else { s * 1024 / t });
}

pub async fn client_thread<W: Unpin + AsyncWriteExt>(idx: u64, stream: &mut W, len: usize) {
  println!("client {}: connected stream", idx);
  let mut stats = BWStats::new();

  let mut remain = len;
  while remain > 0 {
    let bytes_to_write = if remain >= TEST_CHUNK.len() { TEST_CHUNK.len() } else { remain };
    match stream.write_all(&TEST_CHUNK[0..bytes_to_write]).await {
      Ok(()) => (),
      Err(e) => {
        println!("client {}: failed to write to socket; err = {:?}", idx, e);
        break;
      },
    }
    //println!("client {}: wrote; starting flush", idx);
    match stream.flush().await {
      Ok(()) => {
        remain -= bytes_to_write;
        //println!("client {}: sent {} B, remain {} B", idx, bytes_to_write, remain);
      },
      Err(e) => {
        println!("client {}: failed to flush to socket; err = {:?}", idx, e);
        break;
      },
    }
    stats.add(bytes_to_write);
  }

  //println!("client: stats: {:?}", stats);
  let (t, s) = stats.last();
  println!("client: total {} B sent in {} us: ~{} KBps", s, t, if *t == 0 { 0 } else { s * 1024 / t });
}

pub fn get_args() -> (usize, u16, u16, Option<String>, bool) {
  let m = App::new("Bandwidth limit tester")
        .version("0.0")
        .author("Ximin Luo <ximin@web3.foundation>")
        .about("Testing bandwidth-limiting strategies")
        .arg(Arg::with_name("listen")
             .short("p")
             .long("port")
             .value_name("PORT")
             .default_value("6397")
             .help("Local port for listening"))
        .arg(Arg::with_name("bytes")
             .short("b")
             .long("bytes")
             .value_name("NUM")
             .default_value("16777216")
             .help("Number of bytes to send per client"))
        .arg(Arg::with_name("connect")
             .short("c")
             .long("connect")
             .value_name("PORT")
             .help("Local port for connecting to an ssh loopback proxy"))
        .arg(Arg::with_name("host")
             .short("h")
             .long("host")
             .value_name("HOST")
             .help("Remote ssh host for setting up an ssh loopback proxy. \
                   Use ssh_config if you need to configure more things."))
        .arg(Arg::with_name("rate_limit")
             .short("r")
             .long("rate-limit")
             .value_name("BOOL")
             .default_value("true")
             .help("Whether to perform rate-limiting"))
        .get_matches();

  let test_bytes = m.value_of("bytes").unwrap().parse::<usize>().unwrap();
  let listen = m.value_of("listen").unwrap().parse::<u16>().unwrap();
  let (connect, host) = match (m.value_of("connect"), m.value_of("host")) {
    (None, None) => (listen, None),
    (Some(connect), Some(host)) => {
      let connect = connect.parse::<u16>().unwrap();
      (connect, Some(host))
    },
    _ => panic!("--host and --connect must be both set or unset"),
  };
  (test_bytes, listen, connect, host.map(str::to_string), m.value_of("rate_limit").unwrap().parse::<bool>().unwrap())
}

pub fn get_ssh_args(host: String, connect: u16, listen: u16) -> Vec<String> {
  let remote = connect; // ideally this would choose a temp port, but awkward
  vec![
    "-T".to_string(),
    host,
    format!("-Llocalhost:{}:localhost:{}", connect, remote),
    format!("-Rlocalhost:{}:localhost:{}", remote, listen),
    "cat".to_string() // so it responds properly to EOF on stdin; -N ignores it*/
  ]
  // FIXME: for some reason ssh fails to connect with --bytes < ~2.8MB, for both tokio/asyncio
}
