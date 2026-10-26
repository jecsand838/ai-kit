use tokio::select;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub async fn run_workers() {
    let token = CancellationToken::new();
    let tracker = TaskTracker::new();

    for worker_id in 0..4 {
        let token = token.clone();
        tracker.spawn(async move {
            loop {
                select! {
                    _ = token.cancelled() => break,
                    _ = do_one_job(worker_id) => {}
                }
            }
        });
    }

    wait_for_shutdown_signal().await;
    token.cancel();
    tracker.close();
    tracker.wait().await;
}

async fn do_one_job(_worker_id: usize) {}
async fn wait_for_shutdown_signal() {}
