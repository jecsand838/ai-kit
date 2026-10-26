use std::path::{Component, PathBuf};

use serde_json::Value;
use sha2::{Digest, Sha256};
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_sdk::state::record::{StateListRequest, StateRecordSnapshot};
use storage_sdk::{StateStore, StateStoreBackend};

/// A checksum-verified copy of a complete cold store authored by the frozen v6 App.
/// Declare this before all App/Storage owners and join them before it is dropped.
pub(crate) struct AppV6Fixture {
    directory: PathBuf,
    expected: Value,
}

impl AppV6Fixture {
    pub(crate) fn copy(name: &str) -> Self {
        assert!(matches!(
            name,
            "notification-history" | "unknown-history" | "sealed-deletion" | "current-recovery"
        ));
        let original = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/app-v6")
            .join(name);
        let provenance: Value = serde_json::from_slice(
            &std::fs::read(original.join("provenance.json")).expect("checked-in v6 provenance"),
        )
        .expect("v6 provenance JSON");
        let fixture = Self {
            directory: std::env::temp_dir()
                .join(format!("app-v6-migration-{}", uuid::Uuid::new_v4())),
            expected: serde_json::from_slice(
                &std::fs::read(original.join("expected.json"))
                    .expect("v6 expected public observations"),
            )
            .expect("v6 observations JSON"),
        };
        std::fs::create_dir(&fixture.directory).expect("new fixture copy");
        for (relative, identity) in provenance["files"]
            .as_object()
            .expect("complete file inventory")
        {
            let relative = PathBuf::from(relative);
            assert!(
                relative
                    .components()
                    .all(|part| matches!(part, Component::Normal(_)))
            );
            let bytes =
                std::fs::read(original.join(&relative)).expect("complete original cold file");
            assert_eq!(bytes.len() as u64, identity["bytes"].as_u64().unwrap());
            assert_eq!(
                format!("{:x}", Sha256::digest(&bytes)),
                identity["sha256"].as_str().unwrap()
            );
            let target = fixture.directory.join(relative);
            std::fs::create_dir_all(target.parent().unwrap()).expect("cold file parent");
            std::fs::write(target, bytes).expect("unchanged cold file copy");
        }
        fixture
    }

    pub(crate) fn config(&self) -> StateStoreConfig {
        StateStoreConfig::Lance(LanceStateStoreConfig::directory(
            self.directory.join("state"),
        ))
    }

    pub(crate) fn expected(&self) -> &Value {
        &self.expected
    }

    pub(crate) async fn records(&self, native: &StateStore) -> Vec<StateRecordSnapshot> {
        let mut result = Vec::new();
        let mut cursor = None;
        for _ in 0..64 {
            let mut request = StateListRequest::new("app-sdk")
                .unwrap()
                .with_limit(256)
                .unwrap();
            if let Some(after) = cursor.take() {
                request = request.start_after(after).unwrap();
            }
            let page = native
                .list(request)
                .await
                .expect("complete public App inventory");
            result.extend(page.records);
            match page.next_start_after {
                Some(next) => cursor = Some(next),
                None => return result,
            }
        }
        panic!("bounded golden inventory exceeded 16,384 records");
    }
}

impl Drop for AppV6Fixture {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.directory) {
            eprintln!("secondary cold-fixture directory cleanup: {error}");
        }
    }
}
