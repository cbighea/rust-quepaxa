use rust_quepaxa::network::{ReproducibleWanHarness, WanHarnessLink, WanLinkProfile};
use rust_quepaxa::{QuePaxaError, Result};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn parse_link(value: &str) -> Result<WanHarnessLink> {
    let fields = value.split(',').collect::<Vec<_>>();
    if fields.len() != 7 {
        return Err(QuePaxaError::InvalidProposal(
            "link must be NAME,LISTEN,TARGET,SEED,BASE_MS,JITTER_MS,BYTES_PER_SECOND".into(),
        ));
    }
    let parse = |index: usize, name: &str| {
        fields[index]
            .parse::<u64>()
            .map_err(|_| QuePaxaError::InvalidProposal(format!("invalid {name}")))
    };
    Ok(WanHarnessLink {
        name: fields[0].to_owned(),
        listen: fields[1]
            .parse::<SocketAddr>()
            .map_err(|_| QuePaxaError::InvalidProposal("invalid listen address".into()))?,
        target: fields[2]
            .parse::<SocketAddr>()
            .map_err(|_| QuePaxaError::InvalidProposal("invalid target address".into()))?,
        profile: WanLinkProfile {
            seed: parse(3, "seed")?,
            base_delay: Duration::from_millis(parse(4, "base delay")?),
            jitter: Duration::from_millis(parse(5, "jitter")?),
            bandwidth_bytes_per_second: Some(parse(6, "bandwidth")?),
            ..WanLinkProfile::default()
        },
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 4 {
        return Err(QuePaxaError::InvalidProposal(
            "usage: wan_harness DURATION_SECS REPORT.json NAME,LISTEN,TARGET,SEED,BASE_MS,JITTER_MS,BYTES_PER_SECOND [...]".into(),
        ));
    }
    let duration = Duration::from_secs(
        args[1]
            .parse()
            .map_err(|_| QuePaxaError::InvalidProposal("invalid duration".into()))?,
    );
    let report_path = PathBuf::from(&args[2]);
    let links = args[3..]
        .iter()
        .map(|link| parse_link(link))
        .collect::<Result<Vec<_>>>()?;
    let harness = ReproducibleWanHarness::bind(links).await?;
    for (name, address) in harness.endpoints() {
        println!("{name}={address}");
    }
    let shutdown = CancellationToken::new();
    let cancel = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(duration).await;
        cancel.cancel();
    });
    let report = harness.run(shutdown).await?;
    report.write_json(report_path)?;
    Ok(())
}
