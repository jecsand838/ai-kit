use agents_kit::KitError;
use agents_kit::error::{DeadlineKind, ErrorDetail, ErrorKind, ErrorPhase, Operation};
use std::error::Error;
use std::time::Duration;
use tools_kit::ToolError;
use tools_kit::declaration::ToolName;

#[test]
fn tool_timeout_remains_a_timeout() {
    let error = KitError::from(ToolError::Timeout {
        tool: ToolName::new("private-tool-sentinel").expect("valid tool name"),
        timeout_secs: 7,
    });
    assert_eq!(error.kind(), ErrorKind::Timeout, "{error:?}");
    assert_eq!(
        error.detail(),
        Some(&ErrorDetail::DeadlineElapsed {
            kind: DeadlineKind::Request,
            timeout: Some(Duration::from_secs(7)),
        })
    );
    assert!(!format!("{error:?}").contains("private-tool-sentinel"));
}

#[test]
fn normalized_sources_and_cleanup_preserve_safe_evidence() {
    let error = KitError::from(ToolError::BuildCleanup {
        primary: Box::new(ToolError::Timeout {
            tool: ToolName::new("secret-tool").unwrap(),
            timeout_secs: 3,
        }),
        cleanup: Box::new(ToolError::Transport {
            tool: None,
            message: "secret-credential".into(),
        }),
    })
    .with_context(Operation::Build, ErrorPhase::InventoryVerification)
    .with_cause(
        KitError::integration("normalized failure").with_detail(ErrorDetail::ServiceUnavailable),
    );
    let copied = error.clone();
    assert_eq!(copied.kind(), ErrorKind::Timeout);
    assert_eq!(copied.context().operation, Operation::Build);
    assert_eq!(copied.context().phase, ErrorPhase::InventoryVerification);
    assert_eq!(copied.supplementary().len(), 1);
    assert_eq!(
        copied.supplementary()[0].context().phase,
        ErrorPhase::Cleanup
    );
    assert_eq!(
        copied.supplementary()[0].detail(),
        Some(&ErrorDetail::TransportFailure)
    );
    let source = copied.source().expect("normalized source");
    assert!(source.downcast_ref::<KitError>().is_some());
    assert!(source.downcast_ref::<ToolError>().is_none());
    for text in [
        copied.to_string(),
        format!("{copied:?}"),
        source.to_string(),
    ] {
        assert!(!text.contains("secret-"));
    }
}

#[test]
fn dependency_closure_and_cancellation_do_not_invent_agent_terminals() {
    let closed = KitError::from(ToolError::Closed {
        operation: "private-route".into(),
    });
    assert_eq!(closed.kind(), ErrorKind::Integration);
    assert_eq!(closed.detail(), Some(&ErrorDetail::DependencyClosed));
    let cancelled = KitError::from(ToolError::Cancelled {
        tool: ToolName::new("secret-tool").unwrap(),
        reason: Some("secret-reason".into()),
    });
    assert_eq!(cancelled.kind(), ErrorKind::Integration);
    assert_eq!(cancelled.detail(), Some(&ErrorDetail::DependencyCancelled));
}

#[test]
fn retained_trees_are_bounded_and_clones_keep_truncation() {
    let mut deep = KitError::integration("leaf");
    for _ in 0..32 {
        deep = KitError::integration("parent").with_cause(deep);
    }
    let mut depth = 1;
    let mut node = &deep;
    while let Some(cause) = node.cause() {
        depth += 1;
        node = cause;
    }
    assert_eq!(depth, 8);
    assert!(deep.clone().diagnostics_truncated());

    let mut wide = KitError::integration("primary");
    for _ in 0..40 {
        wide = wide.with_supplementary(KitError::integration("secondary"));
    }
    assert_eq!(wide.supplementary().len(), 31);
    assert!(wide.diagnostics_truncated());
    assert_eq!(wide.message(), "primary");

    let error = KitError::integration("é".repeat(4097));
    assert_eq!(error.message().len(), 4096);
    assert!(error.diagnostics_truncated());
    let mut total = error;
    for _ in 0..25 {
        total = total.with_supplementary(KitError::integration("x".repeat(4096)));
    }
    let size = total.message().len()
        + total
            .supplementary()
            .iter()
            .map(|error| error.message().len())
            .sum::<usize>();
    assert_eq!(size, 64 * 1024);
    assert!(total.diagnostics_truncated());
}

#[test]
fn caller_construction_is_clone_send_sync_and_preserves_observations() {
    fn assert_traits<T: Clone + Send + Sync + std::error::Error>() {}
    assert_traits::<KitError>();
    let error = KitError::new(ErrorKind::Integration, "authentication rejected")
        .with_context(Operation::Prompt, ErrorPhase::ModelGeneration)
        .with_detail(ErrorDetail::AuthenticationRejected)
        .with_http_status(401);
    assert_eq!(error.clone().http_status(), Some(401));
    assert!(error.to_string().contains("AuthenticationRejected"));
    assert!(error.to_string().contains("Prompt/ModelGeneration"));
}

struct TimedOutStore;
impl storage_kit::StateStoreBackend for TimedOutStore {
    type Transaction = storage_kit::state::transaction::StateTransaction;
    async fn load(
        &self,
        _key: storage_kit::state::record::StateKey,
    ) -> Result<Option<storage_kit::state::record::StateRecordSnapshot>, storage_kit::StateStoreError>
    {
        Err(storage_kit::StateStoreError::TimedOut {
            operation: "secret-store-operation".into(),
            timeout_ms: 73,
        })
    }
    async fn list(
        &self,
        _request: storage_kit::state::record::StateListRequest,
    ) -> Result<storage_kit::state::record::StateListResponse, storage_kit::StateStoreError> {
        Err(storage_kit::StateStoreError::TimedOut {
            operation: "secret-store-operation".into(),
            timeout_ms: 73,
        })
    }
    async fn begin_transaction(&self) -> Result<Self::Transaction, storage_kit::StateStoreError> {
        Err(storage_kit::StateStoreError::TimedOut {
            operation: "secret-store-operation".into(),
            timeout_ms: 73,
        })
    }
}

#[tokio::test]
async fn storage_timeouts_preserve_duration_and_public_read_operation() {
    let storage = agents_kit::AgentStorage::new(TimedOutStore);
    let error = storage.list_agents().await.expect_err("store timeout");
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.context().operation, Operation::ListAgents);
    assert_eq!(error.context().phase, ErrorPhase::StateRead);
    assert_eq!(
        error.detail(),
        Some(&ErrorDetail::DeadlineElapsed {
            kind: DeadlineKind::Operation,
            timeout: Some(Duration::from_millis(73))
        })
    );
    assert!(!format!("{error:?}").contains("secret-store-operation"));
}
