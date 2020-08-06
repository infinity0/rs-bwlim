// TODO: measure latency between client and corresponding server-worker

use crate::BWStats;

use futures::io::{AsyncReadExt, AsyncWriteExt};
use clap::{App, Arg};

const TEST_CHUNK: &[u8] = &[0; 65536];

pub async fn server_thread<R: Unpin + AsyncReadExt>(mut socket: R) {
  println!("server: stream connected");
  let mut stats = BWStats::new();

  let mut buf = [0; 65536];
  loop {
    let n = match socket.read(&mut buf).await {
      Ok(n) if n == 0 => break, // eof
      Ok(n) => n,
      Err(e) => {
        println!("server: failed to read from socket; err = {:?}", e);
        break;
      }
    };
    stats.add(n);
  }

  //println!("server: stats: {:?}", stats);
  let (t, s) = stats.last();
  println!("server: {} B recv in {} us: ~{} MBps", s, t, if *t == 0 { 0 } else { s / t });
}

pub async fn client_thread<W: Unpin + AsyncWriteExt>(stream: &mut W, len: usize) {
  println!("client: connected stream");
  let mut stats = BWStats::new();

  let mut remain = len;
  while remain > 0 {
    let bytes_to_write = if remain >= TEST_CHUNK.len() { TEST_CHUNK.len() } else { remain };
    stream.write_all(&TEST_CHUNK[0..bytes_to_write]).await.unwrap();
    //println!("client: wrote to stream; success={:?}", result.is_ok());
    remain -= bytes_to_write;
    stats.add(bytes_to_write);
  }

  //println!("client: stats: {:?}", stats);
  let (t, s) = stats.last();
  println!("client: {} B sent in {} us: ~{} MBps", s, t, if *t == 0 { 0 } else { s / t });
}

pub fn get_args() -> (usize, u16, u16, Option<String>) {
  let m = App::new("Bandwidth limit tester")
        .version("0.0")
        .author("Ximin Luo <ximin@web3.foundation>")
        .about("Testing bandwidth-limiting strategies")
        .arg(Arg::with_name("listen")
             .short("p")
             .long("port")
             .value_name("PORT")
             .help("Local port for listening")
             .default_value("6397"))
        .arg(Arg::with_name("bytes")
             .short("b")
             .long("bytes")
             .value_name("NUM")
             .help("Number of bytes to send per client")
             .default_value("16777216"))
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
  (test_bytes, listen, connect, host.map(str::to_string))
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
}
