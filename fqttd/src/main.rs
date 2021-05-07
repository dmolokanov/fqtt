use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{bail, Result};
use argh::FromArgs;
use futures_util::{sink::SinkExt, StreamExt, TryStreamExt};
use mqtt3::proto::{ConnAck, ConnectReturnCode, Packet, PacketCodec};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

fn setup_global_subscriber() -> impl Drop {
    use std::{fs::File, io::BufWriter};
    use tracing_flame::FlameLayer;
    use tracing_subscriber::{fmt, prelude::*, registry::Registry};

    let fmt_layer = fmt::Layer::default();

    let (flame_layer, _guard) = FlameLayer::with_file("./tracing.folded").unwrap();

    let subscriber = Registry::default().with(fmt_layer).with(flame_layer);

    tracing::subscriber::set_global_default(subscriber).expect("Could not set global default");
    _guard
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts: Opts = argh::from_env();

    let _ = setup_global_subscriber();

    let mut listener = TcpListener::bind(("0.0.0.0", opts.port)).await?;
    println!("Listening on: {}", listener.local_addr()?);

    let counters = Counters::default();
    tokio::spawn(print_metrics(counters.clone()));

    while let Ok(Some(socket)) = listener.try_next().await {
        let counters = counters.clone();
        tokio::spawn(async move {
            if let Err(e) = process_incoming(socket, counters).await {
                println!("error = {}", e);
            }
        });
    }

    // let local = tokio::task::LocalSet::new();

    // local
    //     .run_until(async move {
    //         let (socket, _) = listener.accept().await.expect("accept");
    //         let connack = connack.clone();

    //         println!("{:?}", socket.local_addr());
    //     })
    //     .await;

    Ok(())
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

async fn process_incoming(socket: TcpStream, counters: Counters) -> Result<()> {
    let mut codec = Framed::new(socket, PacketCodec::default());

    match codec.try_next().await? {
        Some(Packet::Connect(_connect)) => {
            let connack = Packet::ConnAck(ConnAck {
                session_present: false,
                return_code: ConnectReturnCode::Accepted,
            });
            codec.send(connack).await?
        }
        _ => bail!("not CONNECT packet"),
    };

    while let Some(packet) = codec.try_next().await? {
        if let Packet::Publish(p) = packet {
            let len = p.topic_name.len() + p.payload.len();
            counters.bytes.fetch_add(len as u64, Ordering::Relaxed);

            counters.messages.fetch_add(1, Ordering::Relaxed);
        }

        if counters.messages.load(Ordering::Relaxed) % 1000 == 0 {
            tokio::task::yield_now().await;
        }
    }

    Ok(())
}

async fn print_metrics(counters: Counters) {
    let mut now = tokio::time::Instant::now();
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    while let Some(tick) = interval.next().await {
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
