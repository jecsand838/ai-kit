use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use agents_kit::KitError;
use agents_kit::error::ErrorKind;
use agents_kit::prompt::{PromptEvent, PromptResponse, PromptStream};
use futures_util::FutureExt;

use crate::native_cleanup::NativeCleanup;

impl NativeCleanup {
    pub async fn observe_stream(mut stream: PromptStream) -> PromptResponse {
        let mut fragments = 0;
        loop {
            match tokio::time::timeout(Duration::from_secs(300), stream.recv())
                .await
                .expect("real incremental observation must remain bounded")
                .expect("stream must publish its authoritative terminal")
                .expect("real incremental observation must succeed")
            {
                PromptEvent::OutputDelta { text } => {
                    assert!(!text.is_empty(), "native output fragment must contain text");
                    fragments += 1;
                }
                PromptEvent::Completed(response) => {
                    assert!(
                        fragments > 0,
                        "real source must emit text before its terminal event; fragments={fragments}; terminal_output_bytes={}; terminal_output_empty={}",
                        response.output().len(),
                        response.output().is_empty()
                    );
                    assert!(
                        !response.output().is_empty(),
                        "terminal response remains authoritative"
                    );
                    assert!(
                        stream.recv().await.is_none(),
                        "one terminal response must fuse the stream"
                    );
                    return response;
                }
            }
        }
    }

    /// Observes cancellation without taking ownership of the accepted prompt.
    /// The caller has already required its first real nonempty text fragment.
    pub async fn abort_stream(
        stream: &mut PromptStream,
        abort: impl Future<Output = Result<(), KitError>>,
    ) {
        let abort = AssertUnwindSafe(async {
            abort
                .await
                .expect("explicit abort joins the current stream generation");
        })
        .catch_unwind();
        let observation = AssertUnwindSafe(async {
            loop {
                match tokio::time::timeout(Duration::from_secs(90), stream.recv())
                    .await
                    .expect("aborted stream terminal")
                {
                    Some(Ok(PromptEvent::OutputDelta { .. })) => {}
                    Some(Err(error)) if error.kind() == ErrorKind::Cancelled => break,
                    Some(Ok(PromptEvent::Completed(response))) => {
                        // Remote natural completion may race the abort request.
                        assert!(!response.output().is_empty());
                        break;
                    }
                    event => {
                        panic!("stream abort must preserve its exact terminal authority: {event:?}")
                    }
                }
            }
            assert!(stream.recv().await.is_none());
        })
        .catch_unwind();
        // Both branches remain owned through error or panic. In particular,
        // abort settlement must not prevent observation from releasing capacity.
        let (abort, observation) = tokio::join!(abort, observation);
        if let Err(primary) = abort {
            if let Err(secondary) = observation {
                let message = secondary
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| secondary.downcast_ref::<String>().map(String::as_str))
                    .unwrap_or("non-string panic payload");
                eprintln!("prompt stream observation also failed: {message}");
            }
            std::panic::resume_unwind(primary);
        }
        if let Err(panic) = observation {
            std::panic::resume_unwind(panic);
        }
    }
}
