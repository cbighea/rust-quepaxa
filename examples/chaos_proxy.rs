use rust_quepaxa::network::{ReproducibleWanProxy, WanLinkProfile};
use rust_quepaxa::{QuePaxaError, Result};
use std::net::SocketAddr;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn argument<T: std::str::FromStr>(args: &[String], index: usize, name: &str) -> Result<T> {
    args.get(index)
        .ok_or_else(|| QuePaxaError::InvalidProposal(format!("missing {name}")))?
        .parse()
        .map_err(|_| QuePaxaError::InvalidProposal(format!("invalid {name}")))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 6 {
        return Err(QuePaxaError::InvalidProposal(
            "usage: chaos_proxy LISTEN TARGET SEED BASE_DELAY_MS JITTER_MS [BYTES_PER_SECOND] [FAIL_EVERY] [RESET_AFTER_BYTES]".into(),
        ));
    }
    let listen: SocketAddr = argument(&args, 1, "listen address")?;
    let target: SocketAddr = argument(&args, 2, "target address")?;
    let seed = argument(&args, 3, "seed")?;
    let base_ms = argument(&args, 4, "base delay")?;
    let jitter_ms = argument(&args, 5, "jitter")?;
    let optional = |index, name| {
        args.get(index)
            .map(|_| argument(&args, index, name))
            .transpose()
    };
    let profile = WanLinkProfile {
        seed,
        base_delay: Duration::from_millis(base_ms),
        jitter: Duration::from_millis(jitter_ms),
        bandwidth_bytes_per_second: optional(6, "bandwidth")?,
        fail_every: optional(7, "connection failure interval")?,
        reset_after_bytes: optional(8, "reset byte count")?,
        blackhole_after_bytes: None,
    };
    let proxy = ReproducibleWanProxy::bind(listen, target, profile).await?;
    println!("quepaxa chaos proxy listening on {}", proxy.local_addr()?);
    proxy.run(CancellationToken::new()).await
}
