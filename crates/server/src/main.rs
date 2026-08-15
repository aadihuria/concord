use std::collections::HashMap;
use std::net::SocketAddr;

use clap::Parser;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
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

    /// enable opentelemetry export (set OTEL_EXPORTER_OTLP_ENDPOINT)
    #[arg(long, default_value_t = false)]
    otel: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.otel {
        init_otel_tracing(&args.id)?;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env().add_directive("concord=info".parse()?))
            .init();
    }

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

fn init_otel_tracing(service_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::SpanExporter;
    use opentelemetry_sdk::trace::TracerProvider;
    use opentelemetry_sdk::Resource;

    let exporter = SpanExporter::builder().with_tonic().build()?;

    let provider = TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_resource(Resource::new(vec![KeyValue::new(
            "service.name",
            service_name.to_string(),
        )]))
        .build();

    use opentelemetry::trace::TracerProvider as _;
    let telemetry = tracing_opentelemetry::layer().with_tracer(provider.tracer("concord"));

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env().add_directive("concord=info".parse()?))
        .with(tracing_subscriber::fmt::layer())
        .with(telemetry)
        .init();

    Ok(())
}
