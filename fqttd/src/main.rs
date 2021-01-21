use std::{
    collections::HashMap,
    convert::TryInto,
    io::{self, Read, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{bail, Result};
use argh::FromArgs;
use mio::{
    event::Event,
    net::{TcpListener, TcpStream},
    Events, Interest, Poll, Registry, Token,
};

const SERVER: Token = Token(0);

fn main() -> Result<()> {
    let opts: Opts = argh::from_env();

    let counters = Counters::default();

    std::thread::spawn({
        let counters = counters.clone();
        move || print_metrics(counters)
    });

    // Create a poll instance.
    let mut poll = Poll::new()?;
    // Create storage for events.
    let mut events = Events::with_capacity(128);

    let mut server = TcpListener::bind(([0, 0, 0, 0], opts.port).try_into()?)?;
    println!("Listening on: {}", server.local_addr()?);

    // Register the server with poll we can receive events for it.
    poll.registry()
        .register(&mut server, SERVER, Interest::READABLE)?;

    // Map of `Token` -> `TcpStream`.
    let mut connections = HashMap::new();
    // Unique token for each incoming connection.
    let mut unique_token = Token(SERVER.0 + 1);

    loop {
        poll.poll(&mut events, None)?;

        for event in events.iter() {
            match event.token() {
                SERVER => loop {
                    let (mut connection, address) = match server.accept() {
                        Ok((connection, address)) => (connection, address),
                        Err(e) if would_block(&e) => {
                            break;
                        }
                        Err(e) => {
                            return Err(e.into());
                        }
                    };

                    println!("Accepted connection from: {}", address);

                    let token = next(&mut unique_token);
                    poll.registry().register(
                        &mut connection,
                        token,
                        Interest::READABLE.add(Interest::WRITABLE),
                    )?;

                    connections.insert(token, connection);
                },
                token => {
                    // Maybe received an event for a TCP connection.
                    let done = if let Some(connection) = connections.get_mut(&token) {
                        handle_connection_event(poll.registry(), connection, event, &counters)?
                    } else {
                        // Sporadic events happen, we can safely ignore them.
                        false
                    };
                    if done {
                        connections.remove(&token);
                    }
                }
            }
        }
    }

    Ok(())
}

fn next(current: &mut Token) -> Token {
    let next = current.0;
    current.0 += 1;
    Token(next)
}

/// Returns `true` if the connection is done.
fn handle_connection_event(
    registry: &Registry,
    connection: &mut TcpStream,
    event: &Event,
    counters: &Counters,
) -> io::Result<bool> {
    if event.is_writable() {
        // write CONNACK
        let connack = [0x20, 0x02, 0x00, 0x00];
        connection.write_all(&connack)?;

        // After we've written CONNACK we'll reregister the connection
        // to only respond to readable events.
        registry.reregister(connection, event.token(), Interest::READABLE)?;
    }

    if event.is_readable() {
        let mut connection_closed = false;
        let mut received_data = vec![0; 4096];
        let mut bytes_read = 0;
        // We can (maybe) read from the connection.
        loop {
            match connection.read(&mut received_data[bytes_read..]) {
                Ok(0) => {
                    // Reading 0 bytes means the other side has closed the
                    // connection or is done writing, then so are we.
                    connection_closed = true;
                    break;
                }
                Ok(n) => {
                    // bytes_read += n;
                    // if bytes_read == received_data.len() {
                    //     received_data.resize(received_data.len() + 1024, 0);
                    // }
                    counters.bytes.fetch_add(n as u64, Ordering::Relaxed);
                }

                // Would block "errors" are the OS's way of saying that the
                // connection is not actually ready to perform this I/O operation.
                Err(ref err) if would_block(err) => break,
                Err(ref err) if interrupted(err) => continue,
                // Other errors we'll consider fatal.
                Err(err) => return Err(err),
            }
        }

        // if bytes_read != 0 {

        //     // let received_data = &received_data[..bytes_read];
        //     // if let Ok(str_buf) = std::str::from_utf8(received_data) {
        //     //     println!("Received data: {}", str_buf.trim_end());
        //     // } else {
        //     //     println!("Received (none UTF-8) data: {:?}", received_data);
        //     // }
        // }

        if connection_closed {
            println!("Connection closed");
            return Ok(true);
        }
    }

    Ok(false)
}

fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

fn interrupted(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Interrupted
}

// async fn process_incoming(socket: TcpStream, counters: Counters) -> Result<()> {
//     let mut buffer = vec![0_u8; 8 * 1024 * 1024];

//     socket.readable().await?;
//     let _n = socket.try_read(&mut buffer[..])?;
//     // println!("recv: {:?}", &buffer[..n]);

//     let connack = [0x20, 0x02, 0x00, 0x00];
//     socket.try_write(&connack)?;

//     loop {
//         let ready = socket.ready(Interest::READABLE).await?;

//         if ready.is_readable() {
//             let n = match socket.try_read(&mut buffer[..]) {
//                 Ok(0) => break,
//                 Ok(n) => n,
//                 Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
//                     continue;
//                 }
//                 Err(e) => return Err(e.into()),
//             };

//             counters.messages.fetch_add(1, Ordering::Relaxed);
//             counters.bytes.fetch_add(n as u64, Ordering::Relaxed);

//             // println!("recv: {:?}", &buffer[..n]);
//         }

//         if counters.messages.load(Ordering::Relaxed) % 1000 == 0 {
//             tokio::task::yield_now().await;
//         }
//     }

//     Ok(())
// }

// async fn process_incoming(socket: TcpStream, counters: Counters) -> Result<()> {
//     let mut codec = Framed::new(socket, PacketCodec::default());

//     match codec.try_next().await? {
//         Some(Packet::Connect(_connect)) => {
//             let connack = Packet::ConnAck(ConnAck {
//                 session_present: false,
//                 return_code: ConnectReturnCode::Accepted,
//             });
//             codec.send(connack).await?
//         }
//         _ => bail!("not CONNECT packet"),
//     };

//     while let Some(packet) = codec.try_next().await? {
//         if let Packet::Publish(p) = packet {
//             let len = p.topic_name.len() + p.payload.len();
//             counters.bytes.fetch_add(len as u64, Ordering::Relaxed);

//             counters.messages.fetch_add(1, Ordering::Relaxed);
//         }

//         if counters.messages.load(Ordering::Relaxed) % 1000 == 0 {
//             tokio::task::yield_now().await;
//         }
//     }

//     Ok(())
// }

fn print_metrics(counters: Counters) {
    let mut now = std::time::Instant::now();

    loop {
        std::thread::sleep(Duration::from_secs(1));
        let tick = std::time::Instant::now();

        let elapsed = tick.duration_since(now);

        let messages = counters.messages.swap(0, Ordering::Relaxed);
        let messages = messages as f64 / elapsed.as_secs_f64();

        let bytes = counters.bytes.swap(0, Ordering::Relaxed);
        let bytes = bytes as f64 / elapsed.as_secs_f64();

        let throughput = match number_prefix::NumberPrefix::decimal(bytes) {
            number_prefix::NumberPrefix::Standalone(bytes) => format!("{} B/sec", bytes),
            number_prefix::NumberPrefix::Prefixed(prefix, n) => {
                format!("{:.1} {}B/sec ", n, prefix)
            }
        };

        println!("{} msg/sec, {} ", messages, throughput);

        now = tick;
    }
}

/// Fast MQTT broker.
#[derive(FromArgs)]
struct Opts {
    /// port number.
    #[argh(option, short = 'p', default = "1883")]
    port: u16,
}

#[derive(Debug, Default, Clone)]
struct Counters {
    messages: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}
