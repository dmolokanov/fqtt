use std::{
    num::{NonZeroU16, NonZeroUsize},
    time::Duration,
};

use anyhow::{Context, Result};
use argh::FromArgs;
use bytes::BytesMut;
use futures_util::future;
use mqtt3::proto::{ClientId, Connect, Packet, PacketCodec, PacketIdentifierDupQoS, Publish};
use tokio::net::TcpStream;
use tokio_util::codec::Encoder;

#[tokio::main]
async fn main() -> Result<()> {
    let opts: Opts = argh::from_env();

    match opts.mode {
        Mode::Publisher(config) => {
            let publishers: Vec<_> = (0..config.clients.get())
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

    socket.writable().await?;

    let connect = Connect {
        username: None,
        password: None,
        will: None,
        client_id: ClientId::ServerGenerated,
        keep_alive: Duration::from_secs(5),
        protocol_name: mqtt3::PROTOCOL_NAME.into(),
        protocol_level: mqtt3::PROTOCOL_LEVEL,
    };
    let mut packet = BytesMut::with_capacity(8 * 1024);
    let mut codec = PacketCodec::default();
    codec.encode(Packet::Connect(connect), &mut packet)?;

    socket.try_write(&packet)?;

    socket.readable().await?;

    let mut buffer = vec![0; 8 * 1024];
    let _ = socket.try_read(&mut buffer[..])?;

    let publish = Publish {
        packet_identifier_dup_qos: PacketIdentifierDupQoS::AtMostOnce,
        retain: false,
        topic_name: config.topic,
        payload: vec![0; config.message_size.get()].into(),
    };

    let mut packet = BytesMut::with_capacity(8 * 1024);
    let mut codec = PacketCodec::default();
    codec.encode(Packet::Publish(publish), &mut packet)?;

    let mut offset = 0;
    loop {
        socket.writable().await?;

        match socket.try_write(&packet[offset..]) {
            Ok(0) => {
                break;
            }
            Ok(written) => {
                if offset + written == packet.len() {
                    offset = 0;
                } else {
                    offset += written;
                }
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

    /// number of clients.
    #[argh(option, short = 'c', default = "NonZeroU16::new(5).unwrap()")]
    clients: NonZeroU16,

    /// MQTT topic to publish to.
    #[argh(option, short = 't')]
    topic: String,

    /// MQTT message payload size.
    #[argh(option, short = 'm')]
    message_size: NonZeroUsize,
}

/// A simple MQTT client will subscribe to a topic and will receive messages until stopped.
#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "sub")]
struct Subscriber {}
