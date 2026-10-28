use crate::native_cleanup::NativeCleanup;

impl NativeCleanup {
    pub async fn join<E: std::fmt::Debug>(
        &mut self,
        task: &mut tokio::task::JoinHandle<Result<(), E>>,
    ) {
        let mut joined = false;
        self.settle(async {
            let result = (&mut *task).await;
            joined = true;
            match result {
                Ok(result) => result.map_err(|error| format!("native task failed: {error:?}")),
                Err(error) => Err(format!("native task join failed: {error:?}")),
            }
        })
        .await;
        if !joined {
            if !task.is_finished() {
                task.abort();
            }
            // is_finished only observes readiness. Even a task that finished
            // after the deadline still owns an unconsumed terminal result.
            self.settle(async {
                match (&mut *task).await {
                    Ok(result) => result.map_err(|error| format!("native task failed: {error:?}")),
                    Err(error) if error.is_cancelled() => Ok(()),
                    Err(error) => Err(format!("native task join failed: {error:?}")),
                }
            })
            .await;
        }
    }

    pub async fn reap(&mut self, child: &mut tokio::process::Child) {
        let running = match child.try_wait() {
            Ok(status) => status.is_none(),
            Err(error) => {
                self.settle(async { Err(error) }).await;
                true
            }
        };
        if running {
            self.settle(async { child.start_kill() }).await;
        }
        // A failed kill observation never skips the separate wait/reap attempt.
        self.settle(async { child.wait().await.map(|_| ()) }).await;
    }
}
