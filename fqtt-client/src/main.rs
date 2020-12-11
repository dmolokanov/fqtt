use anyhow::{Context, Result};
use argh::FromArgs;
use futures_util::future;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<()> {
    let opts: Opts = argh::from_env();

    match opts.mode {
        Mode::Publisher(config) => {
            let publishers: Vec<_> = (0..100)
                .map(|_| tokio::spawn(publisher(config.clone())))
                .collect();
            let results = future::try_join_all(publishers).await?;
            for res in results {
                if let Err(e) = res {
                    println!("{}", e);
                }
            }
        }
        Mode::Subscriber(_) => {}
    }

    Ok(())
}

async fn publisher(config: Publisher) -> Result<()> {
    let socket = TcpStream::connect(&config.broker)
        .await
        .with_context(|| format!("unable to connect to broker {}", config.broker))?;

    let mut buffer = vec![0; 8 * 1024];

    socket.writable().await?;
    socket.try_write(b"connect")?;

    socket.readable().await?;
    let _ = socket.try_read(&mut buffer[..])?;

    let packet = [
        0x30, 0x6a, 0x00, 0x05, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30,
        0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30,
        0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30,
        0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30,
        0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30,
        0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30,
        0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30,
        0x30, 0x30, 0x30,
    ];

    loop {
        socket.writable().await?;

        match socket.try_write(&packet) {
            Ok(0) => {
                break;
            }
            Ok(_n) => {
                // println!("{}", n);
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }

    Ok(())
}

/// A simple MQTT client supports subscriber and publisher modes.
#[derive(FromArgs, PartialEq, Debug)]
struct Opts {
    #[argh(subcommand)]
    mode: Mode,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum Mode {
    Publisher(Publisher),
    Subscriber(Subscriber),
}

/// A simple MQTT client will publish the same messages on a given topic until stopped.
#[derive(FromArgs, PartialEq, Debug, Clone)]
#[argh(subcommand, name = "pub")]
struct Publisher {
    /// MQTT broker endpoint.
    #[argh(option, short = 'b', default = "String::from(\"locahost:1883\")")]
    broker: String,
}

/// A simple MQTT client will subscribe to a topic and will receive messages until stopped.
#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "sub")]
struct Subscriber {}
