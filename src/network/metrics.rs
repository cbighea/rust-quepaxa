use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct NetworkMetrics {
    accepted_connections: AtomicU64,
    authentication_failures: AtomicU64,
    requests: AtomicU64,
    request_errors: AtomicU64,
    bytes_received: AtomicU64,
    bytes_sent: AtomicU64,
    cancelled_after_quorum: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetworkMetricsSnapshot {
    pub accepted_connections: u64,
    pub authentication_failures: u64,
    pub requests: u64,
    pub request_errors: u64,
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub cancelled_after_quorum: u64,
}

impl NetworkMetrics {
    pub fn snapshot(&self) -> NetworkMetricsSnapshot {
        NetworkMetricsSnapshot {
            accepted_connections: self.accepted_connections.load(Ordering::Relaxed),
            authentication_failures: self.authentication_failures.load(Ordering::Relaxed),
            requests: self.requests.load(Ordering::Relaxed),
            request_errors: self.request_errors.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            cancelled_after_quorum: self.cancelled_after_quorum.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn connection_accepted(&self) {
        self.accepted_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn authentication_failed(&self) {
        self.authentication_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn request_received(&self, bytes: usize) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.bytes_received
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn response_sent(&self, bytes: usize) {
        self.bytes_sent.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn request_failed(&self) {
        self.request_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn quorum_cancelled(&self, count: usize) {
        self.cancelled_after_quorum
            .fetch_add(count as u64, Ordering::Relaxed);
    }
}
