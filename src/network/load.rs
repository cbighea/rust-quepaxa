use crate::error::{QuePaxaError, Result};
use crate::network::report::write_json_atomic;
use crate::network::transport::TlsSubmitClient;
use crate::network::wire::SubmissionOutcome;
use futures_util::future::BoxFuture;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

pub trait U64SubmissionClient: Clone + Send + Sync + 'static {
    fn submit_u64(
        &self,
        request_id: u64,
        value_id: u64,
    ) -> BoxFuture<'_, Result<SubmissionOutcome<u64>>>;
}

impl U64SubmissionClient for TlsSubmitClient {
    fn submit_u64(
        &self,
        request_id: u64,
        value_id: u64,
    ) -> BoxFuture<'_, Result<SubmissionOutcome<u64>>> {
        Box::pin(self.submit(request_id, vec![value_id]))
    }
}

#[derive(Debug, Clone)]
pub struct OpenLoopLoadConfig {
    pub seed: u64,
    pub requests_per_second: u64,
    pub duration: Duration,
    pub max_in_flight: usize,
}

impl OpenLoopLoadConfig {
    pub fn validate(&self) -> Result<()> {
        if self.requests_per_second == 0 || self.duration.is_zero() || self.max_in_flight == 0 {
            return Err(QuePaxaError::InvalidProposal(
                "load rate, duration, and max in-flight requests must be non-zero".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OpenLoopLoadReport {
    pub seed: u64,
    pub requested_rate: u64,
    pub elapsed_millis: u64,
    pub offered: u64,
    pub committed: u64,
    pub accepted: u64,
    pub duplicates: u64,
    pub errors: u64,
    pub dropped_overload: u64,
    pub achieved_success_per_second: f64,
    pub latency_p50_micros: u64,
    pub latency_p99_micros: u64,
}

impl OpenLoopLoadReport {
    pub fn write_json(&self, path: impl Into<PathBuf>) -> Result<()> {
        write_json_atomic(self, path, "load report")
    }
}

pub struct OpenLoopLoadGenerator<C> {
    client: C,
    config: OpenLoopLoadConfig,
}

impl<C> OpenLoopLoadGenerator<C>
where
    C: U64SubmissionClient,
{
    pub fn new(client: C, config: OpenLoopLoadConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { client, config })
    }

    pub async fn run(self) -> Result<OpenLoopLoadReport> {
        let started = Instant::now();
        let deadline = started + self.config.duration;
        let permits = Arc::new(Semaphore::new(self.config.max_in_flight));
        let mut tasks = JoinSet::new();
        let mut random = self.config.seed.max(1);
        let mut request_id = 0_u64;
        let mut offered = 0_u64;
        let mut dropped = 0_u64;
        let mut next_at = started;

        while next_at < deadline {
            tokio::time::sleep_until(next_at.into()).await;
            offered = offered.saturating_add(1);
            request_id = request_id.checked_add(1).ok_or_else(|| {
                QuePaxaError::InvalidProposal("load request ID overflowed".into())
            })?;
            if let Ok(permit) = Arc::clone(&permits).try_acquire_owned() {
                let client = self.client.clone();
                let value_id = mix64(self.config.seed ^ request_id);
                tasks.spawn(async move {
                    let started = Instant::now();
                    let outcome = client.submit_u64(request_id, value_id).await;
                    drop(permit);
                    (started.elapsed(), outcome)
                });
            } else {
                dropped = dropped.saturating_add(1);
            }
            random = xorshift(random);
            let unit = ((random as f64) + 1.0) / ((u64::MAX as f64) + 2.0);
            let interval = -unit.ln() / self.config.requests_per_second as f64;
            next_at += Duration::from_secs_f64(interval);
        }

        let mut committed = 0_u64;
        let mut accepted = 0_u64;
        let mut duplicates = 0_u64;
        let mut errors = 0_u64;
        let mut latencies = Vec::new();
        while let Some(result) = tasks.join_next().await {
            let (latency, outcome) = result.map_err(|error| {
                QuePaxaError::TransportError(format!("load task failed: {error}"))
            })?;
            match outcome {
                Ok(SubmissionOutcome::Committed(_)) => committed += 1,
                Ok(SubmissionOutcome::Accepted) => accepted += 1,
                Ok(SubmissionOutcome::Duplicate(_)) => duplicates += 1,
                Err(_) => errors += 1,
            }
            latencies.push(latency.as_micros().min(u64::MAX as u128) as u64);
        }
        latencies.sort_unstable();
        let elapsed = started.elapsed();
        let successes = committed + accepted + duplicates;
        Ok(OpenLoopLoadReport {
            seed: self.config.seed,
            requested_rate: self.config.requests_per_second,
            elapsed_millis: elapsed.as_millis().min(u64::MAX as u128) as u64,
            offered,
            committed,
            accepted,
            duplicates,
            errors,
            dropped_overload: dropped,
            achieved_success_per_second: successes as f64 / elapsed.as_secs_f64(),
            latency_p50_micros: percentile(&latencies, 50),
            latency_p99_micros: percentile(&latencies, 99),
        })
    }
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let index = (values.len() - 1) * percentile / 100;
    values[index]
}

fn xorshift(mut value: u64) -> u64 {
    value = value.max(1);
    value ^= value << 13;
    value ^= value >> 7;
    value ^ (value << 17)
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct ImmediateClient;

    impl U64SubmissionClient for ImmediateClient {
        fn submit_u64(
            &self,
            _request_id: u64,
            value_id: u64,
        ) -> BoxFuture<'_, Result<SubmissionOutcome<u64>>> {
            Box::pin(async move {
                Ok(SubmissionOutcome::Committed(crate::Decision::new(
                    crate::SlotIndex::new(value_id),
                    vec![value_id],
                    crate::ReplicaId::new(1),
                    crate::Step::ROUND_ONE_PHASE_ZERO,
                )?))
            })
        }
    }

    #[tokio::test]
    async fn open_loop_generator_reports_reproducible_successes() {
        let report = OpenLoopLoadGenerator::new(
            ImmediateClient,
            OpenLoopLoadConfig {
                seed: 7,
                requests_per_second: 100,
                duration: Duration::from_millis(30),
                max_in_flight: 16,
            },
        )
        .unwrap()
        .run()
        .await
        .unwrap();
        assert!(report.offered > 0);
        assert_eq!(report.committed, report.offered - report.dropped_overload);
        assert_eq!(report.errors, 0);
    }
}
