use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use tokio::sync::Semaphore;
use tool_sdk::{
    ToolError, ToolHandlerBackend, Toolset, ToolsetBackend,
    declaration::{ToolInfo, ToolName},
    execution::{ToolHandlerInput, ToolHandlerOutput, ToolResult},
};

const DEADLINE: Duration = Duration::from_secs(5);

struct Handler;

impl ToolHandlerBackend for Handler {
    fn step(
        &mut self,
        _input: ToolHandlerInput,
    ) -> impl std::future::Future<Output = Result<ToolHandlerOutput, ToolError>> + Send + '_ {
        std::future::ready(Ok(ToolHandlerOutput::Completed(ToolResult::text("ok"))))
    }
}

struct Control {
    cleanup_count: AtomicUsize,
    cleanup_started: Semaphore,
    cleanup_release: Semaphore,
    cleanup_finished: Semaphore,
    list_started: Semaphore,
    list_release: Semaphore,
    handler_started: Semaphore,
    handler_release: Semaphore,
    admission_gate: AdmissionGate,
    terminal: Result<(), ToolError>,
}

#[derive(Clone, Copy)]
enum AdmissionGate {
    None,
    ListTools,
    ToolHandler,
}

struct Backend {
    control: Arc<Control>,
}

impl ToolsetBackend for Backend {
    type Handler = Handler;

    fn list_tools(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<ToolInfo>, ToolError>> + Send + '_ {
        let control = Arc::clone(&self.control);
        async move {
            if matches!(control.admission_gate, AdmissionGate::ListTools) {
                control.list_started.add_permits(1);
                control.list_release.acquire().await.unwrap().forget();
            }
            Ok(Vec::new())
        }
    }

    fn tool_handler(
        &self,
        _name: ToolName,
    ) -> impl std::future::Future<Output = Result<Self::Handler, ToolError>> + Send + '_ {
        let control = Arc::clone(&self.control);
        async move {
            if matches!(control.admission_gate, AdmissionGate::ToolHandler) {
                control.handler_started.add_permits(1);
                control.handler_release.acquire().await.unwrap().forget();
            }
            Ok(Handler)
        }
    }

    fn shutdown(&self) -> impl std::future::Future<Output = Result<(), ToolError>> + Send + '_ {
        let control = Arc::clone(&self.control);
        async move {
            control.cleanup_count.fetch_add(1, Ordering::SeqCst);
            control.cleanup_started.add_permits(1);
            control.cleanup_release.acquire().await.unwrap().forget();
            let terminal = control.terminal.clone();
            control.cleanup_finished.add_permits(1);
            terminal
        }
    }
}

impl Control {
    fn new(terminal: Result<(), ToolError>, admission_gate: AdmissionGate) -> Arc<Self> {
        Arc::new(Self {
            cleanup_count: AtomicUsize::new(0),
            cleanup_started: Semaphore::new(0),
            cleanup_release: Semaphore::new(0),
            cleanup_finished: Semaphore::new(0),
            list_started: Semaphore::new(0),
            list_release: Semaphore::new(0),
            handler_started: Semaphore::new(0),
            handler_release: Semaphore::new(0),
            admission_gate,
            terminal,
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_completes_without_waiters_and_replays_once() {
    tokio::time::timeout(DEADLINE, async {
        let control = Control::new(Ok(()), AdmissionGate::None);
        let toolset = Toolset::from_backend(Backend {
            control: Arc::clone(&control),
        });
        let first = tokio::spawn({
            let toolset = toolset.clone();
            async move { toolset.shutdown().await }
        });
        control
            .cleanup_started
            .acquire()
            .await
            .expect("cleanup observation should remain open")
            .forget();
        first.abort();
        let cancelled = first
            .await
            .expect_err("aborted shutdown waiter should report cancellation");
        assert!(cancelled.is_cancelled());
        control.cleanup_release.add_permits(1);
        control
            .cleanup_finished
            .acquire()
            .await
            .expect("cleanup completion observation should remain open")
            .forget();
        toolset
            .shutdown()
            .await
            .expect("late shutdown should replay success");
        assert_eq!(control.cleanup_count.load(Ordering::SeqCst), 1);
        assert!(matches!(
            toolset.list_tools().await,
            Err(ToolError::Closed { .. })
        ));
    })
    .await
    .expect("toolset waiter-loss shutdown scenario should be bounded");
}

#[tokio::test]
async fn shutdown_replays_the_exact_terminal_error() {
    tokio::time::timeout(DEADLINE, async {
        let terminal = ToolError::Transport {
            tool: None,
            message: "cleanup failed".to_owned(),
        };
        let control = Control::new(Err(terminal.clone()), AdmissionGate::None);
        let toolset = Toolset::from_backend(Backend {
            control: Arc::clone(&control),
        });
        let shutdown = tokio::spawn({
            let toolset = toolset.clone();
            async move { toolset.shutdown().await }
        });
        control
            .cleanup_started
            .acquire()
            .await
            .expect("cleanup observation should remain open")
            .forget();
        control.cleanup_release.add_permits(1);
        let observed = shutdown.await.expect("shutdown task should join");
        assert_eq!(observed, Err(terminal.clone()));
        let replayed = toolset.shutdown().await;
        assert_eq!(replayed, Err(terminal));
    })
    .await
    .expect("toolset terminal-error replay scenario should be bounded");
}

#[tokio::test]
async fn shutdown_drains_admitted_work_before_cleanup() {
    tokio::time::timeout(DEADLINE, async {
        let control = Control::new(Ok(()), AdmissionGate::ListTools);
        let toolset = Toolset::from_backend(Backend {
            control: Arc::clone(&control),
        });
        let listing = tokio::spawn({
            let toolset = toolset.clone();
            async move { toolset.list_tools().await }
        });
        control
            .list_started
            .acquire()
            .await
            .expect("list admission observation should remain open")
            .forget();
        let shutdown = tokio::spawn({
            let toolset = toolset.clone();
            async move { toolset.shutdown().await }
        });
        let later_name = ToolName::new("later").expect("tool name should validate");
        loop {
            match toolset.tool_handler(later_name.clone()).await {
                Err(ToolError::Closed { .. }) => break,
                Ok(_) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected tool admission error: {error}"),
            }
        }
        assert_eq!(control.cleanup_count.load(Ordering::SeqCst), 0);
        control.list_release.add_permits(1);
        listing
            .await
            .expect("listing task should join")
            .expect("admitted listing should complete");
        control
            .cleanup_started
            .acquire()
            .await
            .expect("cleanup observation should remain open")
            .forget();
        control.cleanup_release.add_permits(1);
        shutdown
            .await
            .expect("shutdown task should join")
            .expect("shutdown should succeed");
    })
    .await
    .expect("toolset list-admission drain scenario should be bounded");
}

#[tokio::test]
async fn shutdown_drains_tool_handler_admission_before_cleanup() {
    tokio::time::timeout(DEADLINE, async {
        let control = Control::new(Ok(()), AdmissionGate::ToolHandler);
        let toolset = Toolset::from_backend(Backend {
            control: Arc::clone(&control),
        });
        let handler = tokio::spawn({
            let toolset = toolset.clone();
            async move {
                toolset
                    .tool_handler(ToolName::new("gated").expect("tool name should validate"))
                    .await
            }
        });
        control
            .handler_started
            .acquire()
            .await
            .expect("handler admission observation should remain open")
            .forget();
        let shutdown = tokio::spawn({
            let toolset = toolset.clone();
            async move { toolset.shutdown().await }
        });
        loop {
            match toolset.list_tools().await {
                Err(ToolError::Closed { .. }) => break,
                Ok(_) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected tool admission error: {error}"),
            }
        }
        assert_eq!(control.cleanup_count.load(Ordering::SeqCst), 0);

        control.handler_release.add_permits(1);
        handler
            .await
            .expect("handler task should join")
            .expect("admitted handler acquisition should complete");
        control
            .cleanup_started
            .acquire()
            .await
            .expect("cleanup observation should remain open")
            .forget();
        control.cleanup_release.add_permits(1);
        shutdown
            .await
            .expect("shutdown task should join")
            .expect("shutdown should succeed");
    })
    .await
    .expect("toolset handler-admission drain scenario should be bounded");
}

#[tokio::test]
async fn dropping_clones_does_not_start_async_cleanup() {
    let control = Control::new(Ok(()), AdmissionGate::None);
    drop(Toolset::from_backend(Backend {
        control: Arc::clone(&control),
    }));
    tokio::task::yield_now().await;
    assert_eq!(control.cleanup_count.load(Ordering::SeqCst), 0);
}
