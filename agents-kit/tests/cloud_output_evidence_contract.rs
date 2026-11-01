mod cloud_output_evidence;
mod native_cleanup;

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cloud_output_evidence::CloudOutputEvidence;
use native_cleanup::NativeCleanup;
use serde_json::json;
use tokio::time::Instant;

struct EvidenceFixture {
    path: PathBuf,
}

impl EvidenceFixture {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!(
                "cloud-evidence-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )),
        }
    }

    fn evidence(&self) -> CloudOutputEvidence {
        CloudOutputEvidence::new("model".into(), "opaque123".into(), self.path.clone()).unwrap()
    }
}

impl Drop for EvidenceFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[test]
fn partial_observation_survives_missing_terminal() {
    let fixture = EvidenceFixture::new();
    let mut evidence = fixture.evidence();
    assert_eq!(evidence.expected(), "opaque123");
    assert_eq!(evidence.model(), "model");
    assert_eq!(
        evidence.prompt(),
        "Do not call tools. Return only this exact token: opaque123"
    );
    let _request = evidence.request();
    let mut config = json!({"plugin":[]});
    evidence.configure(&mut config);
    assert_eq!(config["plugin"].as_array().unwrap().len(), 1);
    evidence.begin_unary("unary");
    evidence.unary_result("unary");
    evidence.delta("opaque");
    evidence.delta("");
    let summary = evidence.public_summary();
    assert_eq!(summary["unary"]["portable_equals_expected"], true);
    assert_eq!(summary["stream"]["state"], "partial");
    assert_eq!(summary["stream"]["incremental_bytes"], 6);
    assert_eq!(summary["stream"]["invalid_delta"], true);
    assert_eq!(summary["stream"]["capture"], "incomplete");
}

#[tokio::test]
async fn terminal_comparisons_survive_readback_failure() {
    let fixture = EvidenceFixture::new();
    let mut evidence = fixture.evidence();
    evidence.begin_unary("unary");
    evidence.unary_result("unary");
    evidence.delta("opaque123");
    evidence.terminal("opaque13");
    evidence.fused(true);
    evidence
        .observe_native(Instant::now() + Duration::from_secs(1), async {
            Err("readback_failed")
        })
        .await;
    let summary = evidence.public_summary();
    assert_eq!(summary["stream"]["portable_equals_expected"], false);
    assert_eq!(summary["stream"]["incremental_equals_portable"], false);
    assert_eq!(summary["stream"]["incremental_equals_expected"], true);
    assert_eq!(summary["native"]["reason"], "readback_failed");
}

#[tokio::test]
async fn diagnostic_and_cleanup_failures_preserve_original_panic() {
    let fixture = EvidenceFixture::new();
    let mut evidence = fixture.evidence();
    let primary = NativeCleanup::observe(Instant::now() + Duration::from_secs(1), async {
        panic!("original assertion")
    })
    .await;
    evidence
        .observe_native(Instant::now() + Duration::from_secs(1), async {
            panic!("diagnostic failure")
        })
        .await;
    let mut cleanup = NativeCleanup::new(Duration::from_secs(1));
    cleanup.settle(async { Err("cleanup failure") }).await;
    assert_eq!(cleanup.diagnostics().len(), 1);
    let failure =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cleanup.finish(primary)))
            .unwrap_err();
    assert_eq!(failure.downcast_ref::<&str>(), Some(&"original assertion"));
    assert_eq!(
        evidence.public_summary()["native"]["reason"],
        "readback_panic"
    );
}

#[test]
fn private_staging_retains_partial_observations_without_public_text() {
    let fixture = EvidenceFixture::new();
    let mut evidence = fixture.evidence();
    evidence.begin_unary("unary_secret_literal");
    evidence.delta("opaque");
    evidence
        .publish(
            false,
            true,
            json!({"capture":"incomplete"}),
            &["foreground_missing"],
        )
        .unwrap();
    let summary = std::fs::read_to_string(evidence.directory().join("summary.json")).unwrap();
    assert!(!summary.contains("unary_secret_literal"));
    assert!(!summary.contains("opaque123"));
    let path = evidence.directory().join("private-pending/case-text.json");
    let text: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(text["phases"]["unary"]["expected"], "unary_secret_literal");
    assert_eq!(text["phases"]["stream"]["public_deltas"], "opaque");
    assert!(
        evidence.publish(false, true, json!({}), &[]).is_err(),
        "staged evidence cannot be overwritten"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(evidence.directory())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }
}

#[test]
fn oversized_terminals_remain_explicit_incomplete_observations() {
    let fixture = EvidenceFixture::new();
    let mut evidence = fixture.evidence();
    let large = "x".repeat(64 * 1024 + 1);
    evidence.begin_unary("unary");
    evidence.unary_result(&large);
    evidence.delta("opaque");
    evidence.terminal(&large);
    let summary = evidence.public_summary();
    for phase in ["unary", "stream"] {
        assert_eq!(summary[phase]["capture"], "incomplete");
        assert_eq!(summary[phase]["state"], "terminal_bound");
        assert_eq!(summary[phase]["portable_bytes"], large.len());
    }
    assert_eq!(summary["stream"]["incremental_bytes"], 6);
    evidence.publish(false, true, json!({}), &[]).unwrap();
    assert!(
        !std::fs::read_to_string(evidence.directory().join("private-pending/case-text.json"))
            .unwrap()
            .contains(&large)
    );
}
