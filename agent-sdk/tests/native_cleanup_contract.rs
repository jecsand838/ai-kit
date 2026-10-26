mod native_cleanup;
mod native_process;
mod native_stream;

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use agent_sdk::SdkError;
use agent_sdk::error::{ErrorDetail, ErrorPhase, Operation};
use agent_sdk::prompt::{PromptEvent, PromptResponse, PromptStream};
use futures_util::{StreamExt, stream};
use native_cleanup::NativeCleanup;
use tokio::sync::oneshot;
use tokio::time::Instant;

#[tokio::test]
async fn cleanup_settles_later_owners_and_retains_observation_panic() {
    let observed = NativeCleanup::observe(Instant::now() + Duration::from_secs(1), async {
        std::panic::panic_any(17_u32);
    })
    .await;
    let settled = AtomicUsize::new(0);
    let mut cleanup = NativeCleanup::new(Duration::from_millis(20));
    cleanup.settle(async { Err("child close failed") }).await;
    cleanup
        .settle::<&str>(async { std::panic::panic_any(29_u32) })
        .await;
    cleanup
        .settle(std::future::pending::<Result<(), &str>>())
        .await;
    cleanup
        .settle(async {
            settled.fetch_add(1, Ordering::SeqCst);
            Ok::<(), &str>(())
        })
        .await;
    assert_eq!(
        cleanup.diagnostics(),
        [
            "native cleanup failed: \"child close failed\"",
            "native cleanup panicked: non-string panic payload",
            "native cleanup deadline elapsed",
        ]
    );
    let failure =
        std::panic::catch_unwind(AssertUnwindSafe(|| cleanup.finish(observed))).unwrap_err();
    assert_eq!(failure.downcast_ref::<u32>(), Some(&17));
    assert_eq!(settled.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cleanup_retains_observation_timeout_until_parent_settles() {
    let observed = NativeCleanup::observe(Instant::now(), std::future::pending::<()>()).await;
    let settled = AtomicUsize::new(0);
    let mut cleanup = NativeCleanup::new(Duration::from_secs(1));
    cleanup
        .settle(async {
            settled.fetch_add(1, Ordering::SeqCst);
            Err("cleanup secondary")
        })
        .await;
    let failure =
        std::panic::catch_unwind(AssertUnwindSafe(|| cleanup.finish(observed))).unwrap_err();
    let message = failure
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| failure.downcast_ref::<String>().map(String::as_str))
        .unwrap();
    assert_eq!(message, "native observation deadline elapsed");
    assert_eq!(settled.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cleanup_reports_first_failure_only_after_all_owners_settle() {
    let observed =
        NativeCleanup::observe(Instant::now() + Duration::from_secs(1), async { 7 }).await;
    let settled = AtomicUsize::new(0);
    let mut cleanup = NativeCleanup::new(Duration::from_secs(1));
    cleanup.settle(async { Err("first child failure") }).await;
    cleanup
        .settle(async {
            settled.fetch_add(1, Ordering::SeqCst);
            Err("later parent failure")
        })
        .await;
    let failure =
        std::panic::catch_unwind(AssertUnwindSafe(|| cleanup.finish(observed))).unwrap_err();
    assert_eq!(
        failure.downcast_ref::<String>().map(String::as_str),
        Some("native cleanup failed: \"first child failure\"")
    );
    assert_eq!(settled.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cleanup_preserves_success_after_settling_every_owner() {
    let observed =
        NativeCleanup::observe(Instant::now() + Duration::from_secs(1), async { 7 }).await;
    let mut cleanup = NativeCleanup::new(Duration::from_secs(1));
    cleanup.settle(async { Ok::<(), &str>(()) }).await;
    assert_eq!(cleanup.finish(observed), 7);
}

#[tokio::test]
async fn cleanup_reaps_a_real_child_before_restoring_observation_failure() {
    let mut child = tokio::process::Command::new("sh")
        .args(["-c", "exec sleep 60"])
        .kill_on_drop(true)
        .spawn()
        .expect("real cleanup child must start");
    let observed = NativeCleanup::observe(Instant::now() + Duration::from_secs(1), async {
        std::panic::panic_any(43_u32);
    })
    .await;
    let mut cleanup = NativeCleanup::new(Duration::from_secs(5));
    cleanup.reap(&mut child).await;
    assert!(
        child
            .try_wait()
            .expect("child status must remain observable")
            .is_some()
    );
    assert!(cleanup.diagnostics().is_empty());
    let failure = std::panic::catch_unwind(AssertUnwindSafe(|| cleanup.finish(observed)))
        .expect_err("the original observation must remain failed");
    assert_eq!(failure.downcast_ref::<u32>(), Some(&43));
}

#[tokio::test]
async fn cleanup_reaps_an_already_exited_child_without_changing_observation() {
    let mut child = tokio::process::Command::new("sh")
        .args(["-c", "exit 7"])
        .kill_on_drop(true)
        .spawn()
        .expect("real cleanup child must start");
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("child exit must be bounded")
        .expect("child must exit");
    let observed =
        NativeCleanup::observe(Instant::now() + Duration::from_secs(1), async { 17 }).await;
    let mut cleanup = NativeCleanup::new(Duration::from_secs(5));
    cleanup.reap(&mut child).await;
    assert_eq!(status.code(), Some(7));
    assert_eq!(child.try_wait().unwrap().unwrap().code(), Some(7));
    assert_eq!(cleanup.finish(observed), 17);
}

#[tokio::test]
async fn cleanup_consumes_a_completed_task_failure() {
    let mut task = tokio::spawn(async { Err::<(), _>("shutdown failed") });
    let mut cleanup = NativeCleanup::new(Duration::from_secs(1));
    cleanup.join(&mut task).await;
    assert!(task.is_finished());
    assert_eq!(cleanup.diagnostics().len(), 1);
    assert!(cleanup.diagnostics()[0].contains("shutdown failed"));
}

#[tokio::test]
async fn cleanup_joins_task_cancellation_after_observation_timeout() {
    let released = std::sync::Arc::new(AtomicUsize::new(0));
    let mut task = tokio::spawn({
        let released = std::sync::Arc::clone(&released);
        async move {
            let _owned = released;
            std::future::pending::<Result<(), &str>>().await
        }
    });
    let mut cleanup = NativeCleanup::new(Duration::from_millis(20));
    cleanup.join(&mut task).await;
    assert!(task.is_finished());
    assert_eq!(std::sync::Arc::strong_count(&released), 1);
    assert_eq!(cleanup.diagnostics(), ["native cleanup deadline elapsed"]);
}

#[derive(Clone, Copy)]
enum InlineCleanupCase {
    ObservationPanic,
    SettlementTimeout,
}

struct InlineCleanupFuture<'a> {
    payload: [u8; 256 * 1024],
    polls: &'a AtomicUsize,
    drops: &'a AtomicUsize,
    case: InlineCleanupCase,
}

impl<'a> InlineCleanupFuture<'a> {
    fn new(polls: &'a AtomicUsize, drops: &'a AtomicUsize, case: InlineCleanupCase) -> Self {
        Self {
            payload: [37; 256 * 1024],
            polls,
            drops,
            case,
        }
    }

    async fn observation() {
        let polls = AtomicUsize::new(0);
        let drops = AtomicUsize::new(0);
        let observed = NativeCleanup::observe(
            Instant::now() + Duration::from_secs(1),
            InlineCleanupFuture::new(&polls, &drops, InlineCleanupCase::ObservationPanic),
        )
        .await;
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(1));
        let later = AtomicUsize::new(0);
        cleanup
            .settle(async {
                later.fetch_add(1, Ordering::SeqCst);
                Err("cleanup secondary")
            })
            .await;
        let primary = std::panic::catch_unwind(AssertUnwindSafe(|| cleanup.finish(observed)))
            .expect_err("observation panic must survive cleanup");
        assert_eq!(primary.downcast_ref::<u32>(), Some(&71));
        assert_eq!(later.load(Ordering::SeqCst), 1);
    }

    async fn settlement() {
        let polls = AtomicUsize::new(0);
        let drops = AtomicUsize::new(0);
        let observed = NativeCleanup::observe(Instant::now() + Duration::from_secs(1), async {
            std::panic::panic_any(73_u32);
        })
        .await;
        let mut cleanup = NativeCleanup::new(Duration::from_millis(20));
        cleanup
            .settle(InlineCleanupFuture::new(
                &polls,
                &drops,
                InlineCleanupCase::SettlementTimeout,
            ))
            .await;
        assert!(polls.load(Ordering::SeqCst) > 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(cleanup.diagnostics(), ["native cleanup deadline elapsed"]);
        let later = AtomicUsize::new(0);
        cleanup
            .settle(async {
                later.fetch_add(1, Ordering::SeqCst);
                Ok::<(), &str>(())
            })
            .await;
        let primary = std::panic::catch_unwind(AssertUnwindSafe(|| cleanup.finish(observed)))
            .expect_err("the original panic must survive the cleanup timeout");
        assert_eq!(primary.downcast_ref::<u32>(), Some(&73));
        assert_eq!(later.load(Ordering::SeqCst), 1);
    }
}

impl std::future::Future for InlineCleanupFuture<'_> {
    type Output = Result<(), &'static str>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        assert!(self.payload.iter().all(|byte| *byte == 37));
        self.polls.fetch_add(1, Ordering::SeqCst);
        match self.case {
            InlineCleanupCase::ObservationPanic => std::panic::panic_any(71_u32),
            InlineCleanupCase::SettlementTimeout => std::task::Poll::Pending,
        }
    }
}

impl Drop for InlineCleanupFuture<'_> {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cleanup_large_inline_futures_preserve_outcomes_and_drop_before_return() {
    const CHILD_MODE: &str = "AI_KIT_NATIVE_CLEANUP_INLINE_CHILD";
    match std::env::var(CHILD_MODE).as_deref() {
        Ok("observation") => return Box::pin(InlineCleanupFuture::observation()).await,
        Ok("settlement") => return Box::pin(InlineCleanupFuture::settlement()).await,
        Err(std::env::VarError::NotPresent) => {}
        other => panic!("unexpected inline cleanup child mode: {other:?}"),
    }
    // Stack overflow aborts cannot unwind. Keep each regression in an owned
    // child using the ordinary test-thread stack, so the parent can report red
    // evidence and still join the child without aborting the entire target.
    let mut outcomes = Vec::new();
    for mode in ["observation", "settlement"] {
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "cleanup_large_inline_futures_preserve_outcomes_and_drop_before_return",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_MODE, mode)
            .kill_on_drop(true)
            .spawn()
            .expect("owned inline cleanup child must start");
        let observed = NativeCleanup::observe(Instant::now() + Duration::from_secs(15), async {
            child.wait().await
        })
        .await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(5));
        cleanup.reap(&mut child).await;
        let status = cleanup
            .finish(observed)
            .expect("owned child wait must succeed");
        eprintln!("inline cleanup {mode}: child joined with {status}");
        outcomes.push((mode, status));
    }
    assert!(
        outcomes.iter().all(|(_, status)| status.success()),
        "inline cleanup outcomes: {outcomes:?}"
    );
}

#[tokio::test]
async fn abort_stream_drains_while_abort_is_pending() {
    let (entered, entry) = oneshot::channel();
    let (drained, drainage) = oneshot::channel();
    let (settled, settlement) = oneshot::channel();
    let fragments = Arc::new(AtomicUsize::new(0));
    let observed_fragments = Arc::clone(&fragments);
    let aborts = AtomicUsize::new(0);
    let chunks = stream::once(async move {
        entry
            .await
            .expect("abort must enter before output progresses");
        observed_fragments.fetch_add(1, Ordering::SeqCst);
        drained.send(()).unwrap();
        Ok(PromptEvent::OutputDelta {
            text: "chunk".to_owned(),
        })
    });
    let mut output = PromptStream::from_stream(chunks.chain(stream::once(async move {
        settlement
            .await
            .expect("abort must settle before its terminal");
        Err(SdkError::cancelled("accepted prompt cancelled"))
    })));
    let abort = async {
        entered.send(()).unwrap();
        drainage
            .await
            .expect("observation must progress while abort is pending");
        aborts.fetch_add(1, Ordering::SeqCst);
        settled.send(()).unwrap();
        Ok(())
    };
    // No task is spawned: the two owned futures and their signal endpoints are
    // dropped before assertions, including when this watchdog diagnoses a stall.
    let observed = NativeCleanup::observe(
        Instant::now() + Duration::from_secs(1),
        NativeCleanup::abort_stream(&mut output, abort),
    )
    .await;
    drop(output);
    observed
        .expect("drain/abort coordination must not panic")
        .expect("receiver must progress before abort can settle");
    assert_eq!(fragments.load(Ordering::SeqCst), 1);
    assert_eq!(aborts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn abort_stream_preserves_overflow_and_settles_abort() {
    let (observed, observation) = oneshot::channel();
    let aborts = AtomicUsize::new(0);
    let mut output = PromptStream::from_stream(stream::once(async move {
        observed.send(()).unwrap();
        Err(SdkError::integration("prompt output observation overflow")
            .with_detail(ErrorDetail::ResponseLimit { limit: 64 })
            .with_context(Operation::Prompt, ErrorPhase::Publication))
    }));
    let abort = async {
        observation
            .await
            .expect("terminal observation must run while abort is pending");
        aborts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    };
    let observed = NativeCleanup::observe(
        Instant::now() + Duration::from_secs(1),
        NativeCleanup::abort_stream(&mut output, abort),
    )
    .await;
    drop(output);
    assert_eq!(
        aborts.load(Ordering::SeqCst),
        1,
        "observation failure must not discard abort"
    );
    let panic = observed.expect_err("overflow remains a failed live observation");
    let message = panic
        .downcast_ref::<String>()
        .expect("retained formatted panic");
    assert!(message.contains("prompt output observation overflow"));
    assert!(message.contains("ResponseLimit { limit: 64 }"));
}

#[tokio::test]
async fn abort_stream_drains_before_reporting_abort_failure() {
    let items = Arc::new(AtomicUsize::new(0));
    let observed_items = Arc::clone(&items);
    let mut output = PromptStream::from_stream(
        stream::iter([
            Ok(PromptEvent::OutputDelta {
                text: "partial".to_owned(),
            }),
            Err(SdkError::cancelled("prompt terminal")),
        ])
        .inspect(move |_| {
            observed_items.fetch_add(1, Ordering::SeqCst);
        }),
    );
    let aborts = AtomicUsize::new(0);
    let abort = async {
        aborts.fetch_add(1, Ordering::SeqCst);
        Err(SdkError::integration("abort receipt failed"))
    };
    let observed = NativeCleanup::observe(
        Instant::now() + Duration::from_secs(1),
        NativeCleanup::abort_stream(&mut output, abort),
    )
    .await;
    drop(output);
    assert_eq!(aborts.load(Ordering::SeqCst), 1);
    assert_eq!(
        items.load(Ordering::SeqCst),
        2,
        "abort failure must not discard stream observation"
    );
    let panic = observed.expect_err("abort failure remains primary");
    let message = panic
        .downcast_ref::<String>()
        .expect("retained formatted panic");
    assert!(message.contains("explicit abort joins the current stream generation"));
    assert!(message.contains("abort receipt failed"));
}

#[tokio::test]
async fn abort_stream_accepts_cancellation_and_fuses() {
    let items = Arc::new(AtomicUsize::new(0));
    let observed_items = Arc::clone(&items);
    let mut output = PromptStream::from_stream(
        stream::iter([
            Ok(PromptEvent::OutputDelta {
                text: "partial".to_owned(),
            }),
            Err(SdkError::cancelled("prompt terminal")),
            Ok(PromptEvent::OutputDelta {
                text: "must never be observed".to_owned(),
            }),
        ])
        .inspect(move |_| {
            observed_items.fetch_add(1, Ordering::SeqCst);
        }),
    );
    let observed = NativeCleanup::observe(
        Instant::now() + Duration::from_secs(1),
        NativeCleanup::abort_stream(&mut output, async { Ok(()) }),
    )
    .await;
    let fused = output.recv().await.is_none();
    drop(output);
    observed
        .expect("cancellation observation must not panic")
        .expect("bounded cancellation");
    assert!(fused);
    assert_eq!(items.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn abort_stream_accepts_natural_completion_and_fuses() {
    let items = Arc::new(AtomicUsize::new(0));
    let observed_items = Arc::clone(&items);
    let mut output = PromptStream::from_stream(
        stream::iter([
            Ok(PromptEvent::OutputDelta {
                text: "partial".to_owned(),
            }),
            Ok(PromptEvent::Completed(PromptResponse::new(
                "native completion".to_owned(),
                None,
            ))),
            Err(SdkError::integration("must never be observed")),
        ])
        .inspect(move |_| {
            observed_items.fetch_add(1, Ordering::SeqCst);
        }),
    );
    let observed = NativeCleanup::observe(
        Instant::now() + Duration::from_secs(1),
        NativeCleanup::abort_stream(&mut output, async { Ok(()) }),
    )
    .await;
    let fused = output.recv().await.is_none();
    drop(output);
    observed
        .expect("natural completion must not panic")
        .expect("bounded completion");
    assert!(fused);
    assert_eq!(items.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn abort_stream_rejects_empty_natural_completion() {
    let mut output = PromptStream::from_stream(stream::iter([Ok(PromptEvent::Completed(
        PromptResponse::new(String::new(), None),
    ))]));
    let observed = NativeCleanup::observe(
        Instant::now() + Duration::from_secs(1),
        NativeCleanup::abort_stream(&mut output, async { Ok(()) }),
    )
    .await;
    drop(output);
    assert!(
        observed.is_err(),
        "empty natural completion must remain a failed assertion"
    );
}

#[tokio::test]
async fn abort_stream_rejects_premature_eof() {
    let mut output = PromptStream::from_stream(stream::empty());
    let observed = NativeCleanup::observe(
        Instant::now() + Duration::from_secs(1),
        NativeCleanup::abort_stream(&mut output, async { Ok(()) }),
    )
    .await;
    drop(output);
    let panic = observed.expect_err("missing terminal cannot pass");
    let message = panic
        .downcast_ref::<String>()
        .expect("retained formatted panic");
    assert!(message.contains("prompt output ended before its terminal response"));
}

#[tokio::test]
async fn stream_observation_rejects_terminal_only_with_bounded_counts() {
    for terminal in [
        String::new(),
        "nonempty".to_owned(),
        "secret\n\0😀".repeat(16_384),
    ] {
        let bytes = terminal.len();
        let empty = terminal.is_empty();
        let output = PromptStream::from_stream(stream::iter([Ok(PromptEvent::Completed(
            PromptResponse::new(terminal, None),
        ))]));
        let observed = NativeCleanup::observe(
            Instant::now() + Duration::from_secs(1),
            NativeCleanup::observe_stream(output),
        )
        .await;
        let panic = observed.expect_err("terminal-only output must remain a failed assertion");
        let message = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .expect("stream assertion must retain its diagnostic");
        assert_eq!(
            message,
            format!(
                "real source must emit text before its terminal event; fragments=0; terminal_output_bytes={bytes}; terminal_output_empty={empty}"
            )
        );
        assert!(message.len() <= 256);
        assert!(message.is_ascii());
        assert!(!message.chars().any(char::is_control));
        assert!(!message.contains("secret"));
    }
}

#[tokio::test]
async fn stream_observation_preserves_terminal_bytes_and_fusion() {
    let items = Arc::new(AtomicUsize::new(0));
    let observed_items = Arc::clone(&items);
    let terminal = "untouched Ω\n terminal";
    let output = PromptStream::from_stream(
        stream::iter([
            Ok(PromptEvent::OutputDelta {
                text: "one".to_owned(),
            }),
            Ok(PromptEvent::OutputDelta {
                text: "two".to_owned(),
            }),
            Ok(PromptEvent::Completed(PromptResponse::new(
                terminal.to_owned(),
                None,
            ))),
            Err(SdkError::integration("must never be observed")),
        ])
        .inspect(move |_| {
            observed_items.fetch_add(1, Ordering::SeqCst);
        }),
    );
    let response = NativeCleanup::observe(
        Instant::now() + Duration::from_secs(1),
        NativeCleanup::observe_stream(output),
    )
    .await
    .expect("valid stream must not panic")
    .expect("bounded stream observation");
    assert_eq!(response.output(), terminal);
    assert_eq!(items.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn stream_observation_preserves_empty_fragment_and_terminal_failures() {
    for (fragment, terminal, expected) in [
        ("", "terminal", "native output fragment must contain text"),
        ("fragment", "", "terminal response remains authoritative"),
    ] {
        let output = PromptStream::from_stream(stream::iter([
            Ok(PromptEvent::OutputDelta {
                text: fragment.to_owned(),
            }),
            Ok(PromptEvent::Completed(PromptResponse::new(
                terminal.to_owned(),
                None,
            ))),
        ]));
        let observed = NativeCleanup::observe(
            Instant::now() + Duration::from_secs(1),
            NativeCleanup::observe_stream(output),
        )
        .await;
        let panic = observed.expect_err("original empty-output assertions must still fail");
        let message = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .expect("original assertion must remain visible");
        assert_eq!(message, expected);
    }
}
