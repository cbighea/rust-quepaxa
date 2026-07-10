use rust_quepaxa::{
    LaneId, ProposerCore, RecorderCore, RecorderHandle, ReplicaId, SlotIndex, XorShift64,
};
use std::time::{Duration, Instant};

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let index = (sorted.len() * percentile)
        .div_ceil(100)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[index]
}

fn main() {
    let slots = std::env::var("QUEPAXA_BENCH_SLOTS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(2_000)
        .max(1);
    let batch_size = std::env::var("QUEPAXA_BENCH_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(50)
        .max(1);
    let members = (1..=3).map(ReplicaId::new).collect::<Vec<_>>();
    let recorders = members
        .iter()
        .map(|replica| {
            RecorderHandle::new(
                *replica,
                RecorderCore::permissive_for_tests(*replica, members.clone()).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let mut proposer = ProposerCore::with_rng(
        ReplicaId::new(1),
        LaneId::new(1),
        XorShift64::new_for_stream(1, ReplicaId::new(1), LaneId::new(1)),
    );
    let mut latencies = Vec::with_capacity(slots as usize);
    let benchmark_started = Instant::now();

    for slot in 1..=slots {
        let first_value = (slot - 1) * batch_size;
        let values = (1..=batch_size)
            .map(|offset| first_value + offset)
            .collect::<Vec<_>>();
        let started = Instant::now();
        let decision = proposer
            .propose(
                SlotIndex::new(slot),
                values,
                Some(ReplicaId::new(1)),
                &recorders,
                &[],
            )
            .unwrap();
        latencies.push(started.elapsed());
        for recorder in &recorders {
            let decision = decision.clone();
            recorder
                .with_client(move |core| core.inform_decision(decision))
                .unwrap()
                .unwrap();
        }
    }

    let elapsed = benchmark_started.elapsed();
    latencies.sort_unstable();
    let values = slots * batch_size;
    let throughput = values as f64 / elapsed.as_secs_f64();
    println!(
        "slots={slots} batch_size={batch_size} values={values} elapsed={elapsed:?} throughput_values_per_sec={throughput:.0} p50={:?} p99={:?}",
        percentile(&latencies, 50),
        percentile(&latencies, 99),
    );
}
