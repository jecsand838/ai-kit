use std::time::Duration;

use agent_sdk::error::{ErrorKind, ErrorPhase, Operation};
use agent_sdk::model::ModelInfo;
use agent_sdk::workspace::WorkspaceDefaults;
use agent_sdk::{AgentBackend, AgentBuilder, SdkError, WorkspaceBackend};
use serde_json::{Value, json};

use crate::OpenCodeEventProbe;
use crate::native_cleanup::NativeCleanup;

impl OpenCodeEventProbe {
    pub(super) fn provider_inventory() -> Value {
        crate::opencode_release::ReleaseFixture::default().inventory()
    }

    async fn discover_inventories(payloads: Vec<Value>) -> Vec<Result<Vec<ModelInfo>, SdkError>> {
        let probe = Self::start(0).await;
        let built = AgentBuilder::create(
            probe.config(Duration::from_secs(2), Duration::from_secs(3)),
            Self::storage().await,
        )
        .build()
        .await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(3));
        let observation = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(10),
            async {
                let agent = built.as_ref().expect("inventory Agent must build");
                let mut results = Vec::new();
                for payload in payloads {
                    assert!(
                        payload.to_string().len() < 65_536,
                        "fixture body stays bounded"
                    );
                    probe.stats.release.lock().unwrap().inventory = Some(payload);
                    results.push(agent.list_models().await);
                }
                results
            },
        )
        .await;
        if let Ok(agent) = built {
            cleanup.settle(agent.shutdown()).await;
        }
        cleanup
            .settle(async {
                probe.shutdown().await;
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup.finish(observation)
    }

    fn provider_pair() -> Value {
        let mut inventory = Self::provider_inventory();
        let mut disconnected = inventory["all"][0].clone();
        disconnected["id"] = json!("disconnected");
        disconnected["source"] = json!("api");
        disconnected["key"] = json!("PRIVATE_KEY");
        disconnected["models"]["model"]["providerID"] = json!("disconnected");
        disconnected["models"]["model"]["name"] = json!("Disconnected Model");
        inventory["all"].as_array_mut().unwrap().push(disconnected);
        inventory["default"]["disconnected"] = json!("model");
        inventory["all"][0]["source"] = json!("env");
        inventory
    }
}

#[tokio::test]
async fn provider_inventory_accepts_boolean_and_arbitrary_string_interleaving() {
    let fields = [
        json!(false),
        json!(true),
        json!({"field":"reasoning"}),
        json!({"field":"reasoning_content"}),
        json!({"field":"reasoning_details"}),
        json!({"field":"reasoning_text"}),
        json!({"field":"PRIVATE_CUSTOM_FIELD"}),
        json!({"field":""}),
        json!({"field":" \t\n"}),
    ];
    let mut payloads = Vec::new();
    let mut labels = Vec::new();
    for field in fields {
        for provider in 0..2 {
            let mut inventory = OpenCodeEventProbe::provider_pair();
            inventory["all"][provider]["models"]["model"]["capabilities"]["interleaved"] =
                field.clone();
            labels.push((provider, field.clone()));
            payloads.push(inventory);
        }
    }
    let results = OpenCodeEventProbe::discover_inventories(payloads).await;
    let failures: Vec<_> = labels
        .iter()
        .zip(&results)
        .filter(|(_, result)| result.is_err())
        .collect();
    assert!(
        failures.is_empty(),
        "source-valid inventory rejected: {failures:?}"
    );
    for models in results {
        let models = models.unwrap();
        assert_eq!(
            models.len(),
            1,
            "only reported connected providers are selectable"
        );
        assert_eq!(models[0].display_name(), Some("Probe Model"));
        assert!(!models[0].is_default());
        assert!(models[0].reasoning_efforts().is_empty());
        assert!(models[0].personalities().is_empty());
        for secret in [
            "PRIVATE_KEY",
            "PRIVATE_CUSTOM_FIELD",
            "interleaved",
            "source",
            "https://example.invalid",
        ] {
            assert!(!format!("{models:?}").contains(secret));
        }
    }
}

#[tokio::test]
async fn provider_inventory_rejects_malformed_interleaving_before_connected_projection() {
    let fields = [
        Value::Null,
        json!(7),
        json!([]),
        json!(["reasoning"]),
        json!(["reasoning_text"]),
        json!([false]),
        json!({"field":"reasoning", "extra":true}),
        json!("reasoning_text"),
        json!({}),
        json!({"field":null}),
        json!({"field":false}),
        json!({"field":7}),
        json!({"field":[]}),
        json!({"field":{}}),
    ];
    let mut payloads = Vec::new();
    let mut labels = Vec::new();
    for field in fields {
        for provider in 0..2 {
            let mut inventory = OpenCodeEventProbe::provider_pair();
            inventory["all"][provider]["models"]["model"]["capabilities"]["interleaved"] =
                field.clone();
            labels.push((provider, Some(field.clone())));
            payloads.push(inventory);
        }
    }
    for provider in 0..2 {
        let mut inventory = OpenCodeEventProbe::provider_pair();
        inventory["all"][provider]["models"]["model"]["capabilities"]
            .as_object_mut()
            .unwrap()
            .remove("interleaved");
        labels.push((provider, None));
        payloads.push(inventory);
    }
    let results = OpenCodeEventProbe::discover_inventories(payloads).await;
    assert_eq!(
        results.len(),
        labels.len(),
        "every malformed inventory is observed"
    );
    for ((provider, interleaved), result) in labels.into_iter().zip(results) {
        let error = result.expect_err(&format!(
            "invalid HTTP shapes cannot become model inventories: provider={provider}, interleaved={interleaved:?}"
        ));
        assert_eq!(error.kind(), ErrorKind::Protocol);
        assert_eq!(error.context().operation, Operation::ListModels);
        assert_eq!(error.context().phase, ErrorPhase::ResponseDecode);
    }
}

#[tokio::test]
async fn provider_inventory_rejects_inconsistent_indexes_and_disconnected_models() {
    let mut payloads = Vec::new();
    for connected in [json!(["provider", "provider"]), json!(["missing"])] {
        let mut inventory = OpenCodeEventProbe::provider_pair();
        inventory["connected"] = connected;
        payloads.push(inventory);
    }
    let mut duplicate = OpenCodeEventProbe::provider_pair();
    let provider = duplicate["all"][0].clone();
    duplicate["all"].as_array_mut().unwrap().push(provider);
    payloads.push(duplicate);
    for provider in ["provider", "disconnected"] {
        let mut missing_default = OpenCodeEventProbe::provider_pair();
        missing_default["default"]
            .as_object_mut()
            .unwrap()
            .remove(provider);
        payloads.push(missing_default);
        let mut invalid_default = OpenCodeEventProbe::provider_pair();
        invalid_default["default"][provider] = json!("missing");
        payloads.push(invalid_default);
    }
    let mut foreign_default = OpenCodeEventProbe::provider_pair();
    foreign_default["default"]["missing"] = json!("model");
    payloads.push(foreign_default);
    for provider in 0..2 {
        for (field, value) in [("id", "mismatched-key"), ("providerID", "foreign")] {
            let mut inventory = OpenCodeEventProbe::provider_pair();
            inventory["all"][provider]["models"]["model"][field] = json!(value);
            payloads.push(inventory);
        }
    }
    for result in OpenCodeEventProbe::discover_inventories(payloads).await {
        let error = result.expect_err("contradictory complete inventory must fail");
        assert_eq!(error.kind(), ErrorKind::Protocol);
        assert_eq!(error.context().operation, Operation::ListModels);
        assert_eq!(error.context().phase, ErrorPhase::InventoryVerification);
    }
}

#[tokio::test]
async fn provider_inventory_uses_connected_membership_and_stable_portable_order() {
    let mut inventory = OpenCodeEventProbe::provider_pair();
    let mut second = inventory["all"][0].clone();
    second["id"] = json!("another");
    second["source"] = json!("config");
    second["models"]["model"]["providerID"] = json!("another");
    second["models"]["model"]["name"] = json!("Probe Model");
    inventory["all"].as_array_mut().unwrap().push(second);
    inventory["connected"] = json!(["provider", "another"]);
    inventory["default"]["another"] = json!("model");
    let mut reversed = inventory.clone();
    reversed["all"].as_array_mut().unwrap().reverse();
    reversed["connected"].as_array_mut().unwrap().reverse();
    let results = OpenCodeEventProbe::discover_inventories(vec![inventory, reversed]).await;
    let mut selections = Vec::new();
    for result in results {
        let models = result.expect("connected membership is independent of keys and source");
        assert_eq!(models.len(), 2);
        assert!(
            models
                .windows(2)
                .all(|pair| pair[0].selector() < pair[1].selector())
        );
        assert!(
            models
                .iter()
                .all(|model| model.display_name() == Some("Probe Model")),
            "equal display names do not merge distinct provider/model identities"
        );
        assert!(models.iter().all(|model| !model.is_default()));
        selections.push(
            models
                .into_iter()
                .map(|model| model.selector().clone())
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(selections[0], selections[1]);
}

#[tokio::test]
async fn provider_inventory_preserves_exact_selection_without_default_fallback() {
    let probe = OpenCodeEventProbe::start(0).await;
    let built = AgentBuilder::create(
        probe.config(Duration::from_secs(2), Duration::from_secs(3)),
        OpenCodeEventProbe::storage().await,
    )
    .build()
    .await;
    let mut workspace = None;
    let observation = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(10),
        async {
            let agent = built.as_ref().expect("inventory Agent must build");
            let mut inventory = OpenCodeEventProbe::provider_pair();
            let mut selected = inventory["all"][0]["models"]["model"].clone();
            selected["id"] = json!("selected");
            selected["name"] = json!("Configured Model");
            inventory["all"][0]["models"]["selected"] = selected;
            probe.stats.release.lock().unwrap().inventory = Some(inventory.clone());
            let models = agent.list_models().await.unwrap();
            let selector = models
                .iter()
                .find(|model| model.display_name() == Some("Configured Model"))
                .expect("configured model must be present exactly")
                .selector()
                .clone();
            let defaults = WorkspaceDefaults::new(Some(selector), None, None).unwrap();
            workspace = Some(
                agent
                    .new_workspace(OpenCodeEventProbe::workspace_request_with_defaults(
                        defaults,
                    ))
                    .await
                    .unwrap(),
            );
            workspace.as_ref().unwrap().list_tools().await.unwrap();
            assert_eq!(
                *probe.stats.tool_models.lock().unwrap(),
                vec![("provider".to_owned(), "selected".to_owned())]
            );
            let mut disconnected = inventory.clone();
            disconnected["connected"] = json!(["disconnected"]);
            probe.stats.release.lock().unwrap().inventory = Some(disconnected);
            let remaining = agent.list_models().await.unwrap();
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].display_name(), Some("Disconnected Model"));
            let error =
                workspace.as_ref().unwrap().list_tools().await.expect_err(
                    "disconnected selected provider cannot fall back to another provider",
                );
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            assert_eq!(error.context().operation, Operation::ListTools);
            assert_eq!(
                probe.stats.tool_models.lock().unwrap().len(),
                1,
                "disconnected selection cannot describe a fallback model"
            );

            inventory["all"][0]["models"]
                .as_object_mut()
                .unwrap()
                .remove("selected");
            probe.stats.release.lock().unwrap().inventory = Some(inventory);
            let error = workspace
                .as_ref()
                .unwrap()
                .list_tools()
                .await
                .expect_err("missing selected model cannot fall back to inventory default");
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            assert_eq!(error.context().operation, Operation::ListTools);
            assert_eq!(
                probe.stats.tool_models.lock().unwrap().len(),
                1,
                "failed selection cannot describe a fallback model"
            );
        },
    )
    .await;
    let mut cleanup = NativeCleanup::new(Duration::from_secs(3));
    if let Some(workspace) = workspace {
        cleanup.settle(workspace.close()).await;
    }
    if let Ok(agent) = built {
        cleanup.settle(agent.shutdown()).await;
    }
    cleanup
        .settle(async {
            probe.shutdown().await;
            Ok::<(), SdkError>(())
        })
        .await;
    cleanup.finish(observation);
}
