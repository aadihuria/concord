use std::collections::HashMap;
use std::net::SocketAddr;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use concord_server::ConcordServer;

#[derive(Parser)]
#[command(name = "concord", about = "concord distributed state store")]
struct Args {
    #[arg(long)]
    id: String,

    #[arg(long, default_value = "127.0.0.1:50051")]
    addr: String,

    /// peers in format id=addr,id=addr
    #[arg(long, value_delimiter = ',')]
    peers: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("concord=info".parse()?))
        .init();

    let args = Args::parse();

    let addr: SocketAddr = args.addr.parse()?;

    let mut peer_addrs = HashMap::new();
    for peer in &args.peers {
        if let Some((id, peer_addr)) = peer.split_once('=') {
            let full_addr = if peer_addr.starts_with("http") {
                peer_addr.to_string()
            } else {
                format!("http://{}", peer_addr)
            };
            peer_addrs.insert(id.to_string(), full_addr);
        }
    }

    let server = ConcordServer::new(&args.id, addr, peer_addrs);
    server.run().await
}
