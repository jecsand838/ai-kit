use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures_util::FutureExt;
use tokio::time::Instant;
use tokio::time::error::Elapsed;

/// Test-owned cleanup evidence; no backend or service behavior is substituted.
pub struct NativeCleanup {
    phase_timeout: Duration,
    first_failure: Option<Box<dyn Any + Send>>,
    diagnostics: Vec<String>,
}

impl NativeCleanup {
    pub fn new(phase_timeout: Duration) -> Self {
        Self {
            phase_timeout,
            first_failure: None,
            diagnostics: Vec::new(),
        }
    }

    pub fn observe<T>(
        deadline: Instant,
        observation: impl Future<Output = T>,
    ) -> impl Future<Output = Result<Result<T, Elapsed>, Box<dyn Any + Send>>> {
        let observation = Box::pin(observation);
        async move {
            AssertUnwindSafe(tokio::time::timeout_at(deadline, observation))
                .catch_unwind()
                .await
        }
    }

    pub async fn settle<E: std::fmt::Debug>(
        &mut self,
        cleanup: impl Future<Output = Result<(), E>>,
    ) {
        let result = Self::observe(Instant::now() + self.phase_timeout, cleanup).await;
        let (failure, diagnostic): (Box<dyn Any + Send>, String) = match result {
            Ok(Ok(Ok(()))) => return,
            Ok(Ok(Err(error))) => {
                let diagnostic = format!("native cleanup failed: {error:?}");
                (Box::new(diagnostic.clone()), diagnostic)
            }
            Ok(Err(_)) => {
                let diagnostic = "native cleanup deadline elapsed".to_owned();
                (Box::new(diagnostic.clone()), diagnostic)
            }
            Err(panic) => {
                let message = panic
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                    .unwrap_or("non-string panic payload");
                let diagnostic = format!("native cleanup panicked: {message}");
                (panic, diagnostic)
            }
        };
        self.diagnostics.push(diagnostic);
        if self.first_failure.is_none() {
            self.first_failure = Some(failure);
        }
    }

    pub fn diagnostics(&self) -> &[String] {
        &self.diagnostics
    }

    pub fn finish<T>(self, observation: Result<Result<T, Elapsed>, Box<dyn Any + Send>>) -> T {
        // Report every phase failure even when an earlier observation remains primary.
        for diagnostic in self.diagnostics() {
            eprintln!("{diagnostic}");
        }
        let output = match observation {
            Ok(Ok(output)) => output,
            Ok(Err(_)) => panic!("native observation deadline elapsed"),
            Err(panic) => std::panic::resume_unwind(panic),
        };
        if let Some(panic) = self.first_failure {
            std::panic::resume_unwind(panic);
        }
        output
    }
}
