use std::io;
use std::process::{Child, ExitStatus};
use std::time::{Duration, Instant};

/// Test-local ownership of one real operating-system child through reaping.
pub(super) struct OwnedChild {
    child: Child,
    reaped: bool,
    fallback_attempted: bool,
}

impl OwnedChild {
    pub(super) fn new(child: Child) -> Self {
        Self {
            child,
            reaped: false,
            fallback_attempted: false,
        }
    }

    pub(super) fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        let status = self.child.try_wait()?;
        self.reaped |= status.is_some();
        Ok(status)
    }

    pub(super) async fn stop(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.try_wait()? {
            return Ok(status);
        }
        self.child.kill()?;
        let deadline = Instant::now() + Duration::from_secs(10);
        // Give the runtime a scheduling opportunity after real termination.
        // Cancelling this observer retains the exact child's wait authority.
        tokio::task::yield_now().await;
        loop {
            if let Some(status) = self.try_wait()? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "owned child reaping exceeded its ten-second bound",
                ));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Returns true only after an actual exit receipt, never from a kill alone.
    pub(super) fn settle_on_drop(&mut self) -> bool {
        if self.reaped {
            return true;
        }
        if self.fallback_attempted {
            return false;
        }
        self.fallback_attempted = true;
        match self.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => {}
            Err(error) => eprintln!("owned child fallback status failed: {error}"),
        }
        if let Err(error) = self.child.kill() {
            eprintln!("owned child fallback termination failed: {error}");
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            match self.try_wait() {
                Ok(Some(_)) => return true,
                Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                Err(error) => {
                    eprintln!("owned child fallback reaping failed: {error}");
                    return false;
                }
            }
        }
        eprintln!("owned child fallback reaping exceeded its two-second bound");
        false
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        self.settle_on_drop();
    }
}
