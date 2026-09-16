//! Absolute deadlines for caller-owned asynchronous work.

use std::future::Future;

use anyhow::{Result, anyhow, ensure};
use tokio::time::{Instant, timeout_at};
use tokio_util::sync::CancellationToken;

/// A caller-owned deadline and cancellation signal shared by related operations.
#[derive(Clone)]
pub struct Budget {
    pub deadline: Instant,
    pub stop: CancellationToken,
}

impl Budget {
    /// Check synchronous work boundaries, including before starting another request.
    pub fn check(&self) -> Result<()> {
        ensure!(!self.stop.is_cancelled(), "operation cancelled");

        ensure!(
            Instant::now() < self.deadline,
            "operation deadline exceeded"
        );

        Ok(())
    }

    /// Drop pending work on cancellation or expiry, and reject late ready results.
    pub async fn run<F: Future>(&self, future: F) -> Result<F::Output> {
        self.check()?;

        let result = tokio::select! {
            biased;

            _ = self.stop.cancelled() => return Err(anyhow!("operation cancelled")),

            result = timeout_at(self.deadline, future) => result,
        };

        self.check()?;

        result.map_err(|_| anyhow!("operation deadline exceeded"))
    }
}
