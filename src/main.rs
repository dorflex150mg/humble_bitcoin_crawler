#![allow(unused)]
mod codec;

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;
use std::collections::HashSet;

use anyhow::Context;
use bitcoin::p2p::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::p2p::message_network::VersionMessage;
use bitcoin::p2p::{Address, ServiceFlags};
use bitcoin::Network;
use clap::Parser;
use codec::BitcoinCodec;
use futures::{stream, SinkExt, StreamExt, TryFutureExt};
use rand::Rng;
use tokio::net::TcpStream;
use tokio::time::error::Elapsed;
use tokio::time::timeout;
use tokio_util::codec::Framed;

const RECV_TIMEOUT: u64 = 2_000;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The address of the node to reach to.
    /// `dig seed.bitcoin.sipa.be +short` may provide a fresh list of nodes.
    #[arg(short, long, default_value = "localhost:8333")]
    remote_address: String,

    /// The connection timeout, in milliseconds.
    /// Most nodes will quickly respond. If they don't, we'll probably want to talk to other nodes instead.
    #[arg(short, long, default_value = "500")]
    connection_timeout_ms: u64,

    ///Batch size limit -- the smaller the size, the "deeper" the search,
    ///i.e., more hops before the crawl limit gets reached.
    #[arg(short, long, default_value = "500")]
    batch_limit: usize,


    ///crawl limit -- the crawl ends when at least this many addresses have been added.
    #[arg(short, long, default_value = "5000")]
    max_crawl: usize,

}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();

    let remote_address = args
        .remote_address
        .parse::<SocketAddr>()
        .context("Invalid remote address")?;
    let mut stream: Framed<TcpStream, BitcoinCodec> =
        connect(&remote_address, args.connection_timeout_ms).await?;

    perform_handshake(&mut stream, remote_address).await?;
    let mut batch_addresses = vec![remote_address];
    let mut crawled_addresses: Vec<SocketAddr> = vec![];
    while crawled_addresses.len() < args.max_crawl {
        let mut new_addresses = get_addresses(&mut batch_addresses, args.connection_timeout_ms).await?;
        if new_addresses.len() == 0 {
            batch_addresses = crawled_addresses[crawled_addresses.len() - args.batch_limit..crawled_addresses.len()].to_vec();
        } else if new_addresses.len() < args.batch_limit {
            batch_addresses = new_addresses;
        } else {
            batch_addresses = new_addresses[0..args.batch_limit].to_vec();
        }
        crawled_addresses.extend(&batch_addresses);
        crawled_addresses = remove_duplicates(crawled_addresses);
        tracing::debug!("New Addresses: {:?}, addresses so far: {}", 
            batch_addresses, 
            crawled_addresses.len()); //Big data dump
    }
    Ok(())
}

async fn get_addresses(batch_addresses: &mut Vec<SocketAddr>, timeout: u64) -> anyhow::Result<Vec<SocketAddr>> {
    let address_list: Vec<Result<Vec<SocketAddr>, Error>> = stream::iter(batch_addresses)
        .map(|remote_address| process_address(remote_address, timeout)) //get addresses
        .buffer_unordered(256) //It would be good to find a sweet spot here 
        .collect::<Vec<_>>()
        .await; 
    let unique: Vec<SocketAddr> = address_list
        .into_iter() 
        .filter_map(Result::ok)
        .flatten()
        .collect();
    Ok(unique)
}

async fn process_address(remote_address: &mut SocketAddr, timeout: u64) -> Result<Vec<SocketAddr>, Error> {
    let mut stream: Framed<TcpStream, BitcoinCodec> =
        connect(&remote_address, timeout).await?;
    perform_handshake(&mut stream, *remote_address).await?;
    Ok(getaddr(&mut stream, *remote_address).await?)
}

async fn getaddr(
    stream: &mut Framed<TcpStream, BitcoinCodec>,
    remote_address: SocketAddr
) -> Result<Vec<SocketAddr>, Error> {

    let getaddr_message = RawNetworkMessage::new(
        Network::Bitcoin.magic(),
        NetworkMessage::GetAddr,
    );
    stream
        .send(getaddr_message)
        .await
        .map_err(Error::SendingFailed)?;


    while let Some(result) = timeout(Duration::from_millis(RECV_TIMEOUT), stream.next())
        .map_err(Error::ConnectionTimedOut)
        .await? {
        match result {
            Ok(message) => match message.payload() {
                NetworkMessage::Addr(addresses) => {
                    let results = addresses.iter().map(|addr_struct| {
                        addr_struct.1.socket_addr().unwrap()
                    }).collect();
                    return Ok(results);
                },
                other_message => (),
            },
            Err(err) => tracing::error!("Decoding error: {}", err),
        }
    }
    Err(Error::ConnectionLost)
}

fn remove_duplicates(address_list: Vec<SocketAddr>) -> Vec<SocketAddr>{
    let deduped_set: HashSet<SocketAddr> = address_list.into_iter().collect();
    let deduped_vec: Vec<SocketAddr> = deduped_set.into_iter().collect();
    deduped_vec
}

async fn connect(
    remote_address: &SocketAddr,
    connection_timeout: u64,
) -> Result<Framed<TcpStream, BitcoinCodec>, Error> {
    let connection = TcpStream::connect(remote_address).map_err(Error::ConnectionFailed);
    let stream = timeout(Duration::from_millis(connection_timeout), connection)
        .map_err(Error::ConnectionTimedOut)
        .await??;
    //let framed = Framed::new(stream, BitcoinCodec {});
    Ok(Framed::new(stream, BitcoinCodec {}))
}

/// Perform a Bitcoin handshake as per [this protocol documentation](https://en.bitcoin.it/wiki/Protocol_documentation)
async fn perform_handshake(
    stream: &mut Framed<TcpStream, BitcoinCodec>,
    peer_address: SocketAddr,
) -> Result<(), Error> {
    let version_message = RawNetworkMessage::new(
        Network::Bitcoin.magic(),
        NetworkMessage::Version(build_version_message(&peer_address)),
    );

    stream
        .send(version_message)
        .await
        .map_err(Error::SendingFailed)?;

    while let Some(result) = timeout(Duration::from_millis(RECV_TIMEOUT), stream.next())
        .map_err(Error::ConnectionTimedOut)
        .await? {
        match result {
            Ok(message) => match message.payload() {
                NetworkMessage::Version(remote_version) => {
                    //tracing::info!("Version message: {:?}", remote_version);

                    stream
                        .send(RawNetworkMessage::new(
                            Network::Bitcoin.magic(),
                            NetworkMessage::Verack,
                        ))
                        .await
                        .map_err(Error::SendingFailed)?;

                    return Ok(());
                }
                other_message => {
                    // We're only interested in the version message right now. Keep the loop running.
                    tracing::debug!("Unsupported message: {:?}", other_message);
                }
            },
            Err(err) => {
                tracing::error!("Decoding error: {}", err);
            }
        }
    }

    Err(Error::ConnectionLost)
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Connection failed: {0:?}")]
    ConnectionFailed(std::io::Error),
    #[error("Connection timed out")]
    ConnectionTimedOut(Elapsed),
    #[error("Connection lost")]
    ConnectionLost,
    #[error("Sending failed")]
    SendingFailed(std::io::Error),
}

fn build_version_message(receiver_address: &SocketAddr) -> VersionMessage {
    /// The height of the block that the node is currently at.
    /// We are always at the genesis block. because our implementation is not a real node.
    const START_HEIGHT: i32 = 0;
    /// The most popular user agent. See https://bitnodes.io/nodes/
    const USER_AGENT: &str = "/Satoshi:25.0.0/";
    const SERVICES: ServiceFlags = ServiceFlags::NONE;
    /// The address of this local node.
    /// This address doesn't matter much as it will be ignored by the bitcoind node in most cases.
    let sender_address: SocketAddr =
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0));

    let sender = Address::new(&sender_address, SERVICES);
    let timestamp = chrono::Utc::now().timestamp();
    let receiver = Address::new(&receiver_address, SERVICES);
    let nonce = rand::thread_rng().gen();
    let user_agent = USER_AGENT.to_string();

    VersionMessage::new(
        SERVICES,
        timestamp,
        receiver,
        sender,
        nonce,
        user_agent,
        START_HEIGHT,
    )
}

pub fn init_tracing() {
    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::EnvFilter;

    let env = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .with_env_var("RUST_LOG")
        .from_env_lossy();

    let fmt_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(false)
        .with_target(false);
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(env)
        .init();
}
