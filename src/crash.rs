use crate::Result;

/// Literal durable-write boundaries exposed to subprocess crash harnesses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CrashPoint {
    RecorderTemporaryOpened,
    RecorderTemporaryWritten,
    RecorderTemporarySynced,
    RecorderRenamed,
    RecorderDirectorySynced,
    RuntimeTemporaryOpened,
    RuntimeTemporaryWritten,
    RuntimeTemporarySynced,
    RuntimeRenamed,
    RuntimeDirectorySynced,
    BatchTemporaryOpened,
    BatchTemporaryWritten,
    BatchTemporarySynced,
    BatchRenamed,
    BatchDirectorySynced,
    SubmissionJournalTemporaryOpened,
    SubmissionJournalTemporaryWritten,
    SubmissionJournalTemporarySynced,
    SubmissionJournalRenamed,
    SubmissionJournalDirectorySynced,
    ExactlyOnceTemporaryOpened,
    ExactlyOnceTemporaryWritten,
    ExactlyOnceTemporarySynced,
    ExactlyOnceRenamed,
    ExactlyOnceDirectorySynced,
}

/// Hook invoked synchronously at a durable-write boundary. Production uses
/// the no-op default. A subprocess test can signal its parent and block here;
/// the parent then sends a real process kill before reopening the store.
pub trait CrashInjector: Send + Sync + 'static {
    fn reached(&self, point: CrashPoint) -> Result<()>;
}

impl<F> CrashInjector for F
where
    F: Fn(CrashPoint) -> Result<()> + Send + Sync + 'static,
{
    fn reached(&self, point: CrashPoint) -> Result<()> {
        self(point)
    }
}

#[derive(Debug, Default)]
pub struct NoopCrashInjector;

impl CrashInjector for NoopCrashInjector {
    fn reached(&self, _point: CrashPoint) -> Result<()> {
        Ok(())
    }
}
