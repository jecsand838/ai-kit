use crate::support;

struct OpenCodeAuthority {
    specifications: String,
    vendor_lock: String,
    protocol: String,
    backend: String,
}

impl OpenCodeAuthority {
    fn load() -> Self {
        Self {
            specifications: support::read_workspace_file("agent-sdk/specs/SPECIFICATIONS.md"),
            vendor_lock: support::read_workspace_file("agent-sdk/vendor/vendor-lock.toml"),
            protocol: support::read_workspace_file("agent-sdk/specs/opencode/PROTOCOL.md"),
            backend: support::read_workspace_file("agent-sdk/specs/opencode/BACKEND.md"),
        }
    }

    fn vendor_source(&self, path: &str) -> String {
        support::read_workspace_file(format!("agent-sdk/vendor/opencode-1.19.3/{path}"))
    }
}

#[test]
fn selected_opencode_release_has_one_pinned_authority() {
    let authority = OpenCodeAuthority::load();

    assert!(
        authority
            .specifications
            .contains("OpenCode Server: `v1.19.3`")
    );
    for fact in [
        "tag = \"v1.19.3\"",
        "archive = \"opencode-1.19.3.zip\"",
        "extracted_dir = \"opencode-1.19.3\"",
        "sha256 = \"662ef890f51c49c364bc8e91ab04f25c8a3abf8833c86d42d9d0e5c31df34b0b\"",
    ] {
        assert!(
            authority.vendor_lock.contains(fact),
            "OpenCode vendor lock lost `{fact}`"
        );
    }
    assert!(
        !authority.protocol.contains("v1.19.3"),
        "the protocol contract must defer release selection to SPECIFICATIONS.md"
    );
}

#[test]
fn selected_opencode_wire_facts_are_sourced_without_becoming_public_api() {
    let authority = OpenCodeAuthority::load();
    let session_routes = authority
        .vendor_source("packages/opencode/src/server/routes/instance/httpapi/handlers/session.ts");
    let global_events = authority
        .vendor_source("packages/opencode/src/server/routes/instance/httpapi/handlers/global.ts");
    let question_schema = authority.vendor_source("packages/schema/src/v1/question.ts");

    for sourced in ["/session", "/prompt", ".handle(\"abort\", abort)"] {
        assert!(
            session_routes.contains(sourced),
            "selected session authority lost `{sourced}`"
        );
    }
    for sourced in [".handleRaw(\"event\", event)", "server.heartbeat"] {
        assert!(
            global_events.contains(sourced),
            "selected global-event authority lost `{sourced}`"
        );
    }
    for sourced in [
        "QuestionRequest",
        "export const Reply",
        "export const Rejected",
    ] {
        assert!(
            question_schema.contains(sourced),
            "selected question authority lost `{sourced}`"
        );
    }

    for contract in [
        "`GET /global/event` is the sole live event subscription.",
        "The selected foreground message HTTP response is prompt-terminal authority.",
        "Known structured non-success envelopes map through the route",
    ] {
        assert!(
            authority.protocol.contains(contract) || authority.backend.contains(contract),
            "OpenCode protocol contract lost `{contract}`"
        );
    }
}

#[test]
fn selected_generated_opencode_schema_keeps_foreground_and_interaction_envelopes() {
    let authority = OpenCodeAuthority::load();
    let schema: serde_json::Value =
        serde_json::from_str(&authority.vendor_source("packages/sdk/openapi.json")).unwrap();
    let model_config = &schema["components"]["schemas"]["ProviderConfig"]["properties"]["models"]["additionalProperties"];
    assert!(
        !model_config["required"]
            .as_array()
            .is_some_and(|required| required.contains(&serde_json::json!("reasoning_options")))
    );
    let options = &model_config["properties"]["reasoning_options"];
    assert_eq!(options["type"], "array");
    let alternatives = options["items"]["anyOf"].as_array().unwrap();
    assert_eq!(alternatives.len(), 3);
    for (alternative, kind, required) in [
        (
            &alternatives[0],
            "effort",
            serde_json::json!(["type", "values"]),
        ),
        (&alternatives[1], "toggle", serde_json::json!(["type"])),
        (
            &alternatives[2],
            "budget_tokens",
            serde_json::json!(["type"]),
        ),
    ] {
        assert_eq!(alternative["type"], "object");
        assert_eq!(
            alternative["properties"]["type"]["enum"],
            serde_json::json!([kind])
        );
        assert_eq!(alternative["required"], required);
        assert_eq!(alternative["additionalProperties"], false);
    }
    assert_eq!(
        alternatives[0]["properties"]["values"],
        serde_json::json!({"type": "array", "items": {"type": "string"}}),
        "generated HTTP efforts remain string-valued despite nullable config-source values"
    );
    for bound in ["min", "max"] {
        assert_eq!(
            alternatives[2]["properties"][bound],
            serde_json::json!({"type": "number"})
        );
    }
    assert!(
        schema["components"]["schemas"]["Model"]["properties"]
            .get("reasoning_options")
            .is_none(),
        "configuration does not widen model inventory responses"
    );
    let interleaved = &schema["components"]["schemas"]["Model"]["properties"]["capabilities"]["properties"]
        ["interleaved"]["anyOf"];
    assert_eq!(interleaved.as_array().unwrap().len(), 2);
    assert_eq!(interleaved[0], serde_json::json!({"type":"boolean"}));
    assert_eq!(interleaved[1]["type"], "object");
    assert_eq!(interleaved[1]["required"], serde_json::json!(["field"]));
    assert_eq!(interleaved[1]["additionalProperties"], false);
    assert!(
        interleaved[1]["properties"]["field"]["anyOf"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!({"type":"string"})),
        "the response field vocabulary is open"
    );

    for (path, method, info) in [
        ("/session/{sessionID}/message", "post", "AssistantMessage"),
        ("/session/{sessionID}/message/{messageID}", "get", "Message"),
    ] {
        let response = &schema["paths"][path][method]["responses"]["200"]["content"]["application/json"]
            ["schema"];
        assert_eq!(response["required"], serde_json::json!(["info", "parts"]));
        assert_eq!(
            response["properties"]["info"]["$ref"],
            format!("#/components/schemas/{info}")
        );
        assert_eq!(
            response["properties"]["parts"]["items"]["$ref"],
            "#/components/schemas/Part"
        );
    }
    let create = &schema["paths"]["/session"]["post"];
    assert_eq!(
        create["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/Session"
    );
    let properties = &create["requestBody"]["content"]["application/json"]["schema"]["properties"];
    assert!(properties.get("metadata").is_some());
    assert!(properties.get("permission").is_some());
    for path in [
        "/session/{sessionID}/abort",
        "/permission/{requestID}/reply",
        "/question/{requestID}/reply",
    ] {
        assert_eq!(
            schema["paths"][path]["post"]["responses"]["200"]["content"]["application/json"]["schema"]
                ["type"],
            "boolean"
        );
    }
    let reply = &schema["paths"]["/permission/{requestID}/reply"]["post"]["requestBody"]["content"]
        ["application/json"]["schema"];
    assert_eq!(
        reply["properties"]["reply"]["enum"],
        serde_json::json!(["once", "always", "reject"])
    );
    let question = &schema["paths"]["/question/{requestID}/reply"]["post"]["requestBody"]["content"]
        ["application/json"]["schema"];
    assert_eq!(question["required"], serde_json::json!(["answers"]));
    assert_eq!(
        question["properties"]["answers"]["items"]["$ref"],
        "#/components/schemas/QuestionAnswer"
    );
}

#[test]
fn selected_opencode_configured_variants_and_discontinued_sharing_are_sourced() {
    let authority = OpenCodeAuthority::load();
    for (path, anchors) in [
        (
            "packages/core/src/v1/config/provider.ts",
            vec![
                "reasoning_options: Schema.optional(",
                "Schema.Array(ReasoningOption)",
                "values: Schema.Array(Schema.NullOr(Schema.String))",
            ],
        ),
        (
            "packages/opencode/src/provider/provider.ts",
            vec![
                "model.reasoning_options !== undefined",
                "ProviderTransform.reasoningVariants(model, parsedModel) ?? ProviderTransform.variants(parsedModel)",
                "mergeDeep(variants, model.variants ?? {})",
                "pickBy(merged, (v) => !v.disabled)",
                "(v) => omit(v, [\"disabled\"])",
            ],
        ),
        (
            "packages/opencode/src/provider/transform.ts",
            vec![
                "const options = model.reasoning_options",
                "if (options === undefined) return",
                "if (options.length === 0) return {}",
            ],
        ),
        (
            "packages/opencode/src/share/session.ts",
            vec![
                "SessionShare.create",
                "return yield* session.create(input)",
                "SessionShare.share",
                "Effect.fail(new Error(\"Session sharing has been discontinued\"))",
                "SessionShare.unshare",
                "session.setShare({ sessionID, share: undefined })",
            ],
        ),
        (
            "packages/opencode/src/config/v2-compat.ts",
            vec![
                "Object.hasOwn(parsed.value, \"permissions\")",
                "[\"agents\", \"agent\", \"mode\"]",
                "V2 permissions are not supported by OpenCode V1",
            ],
        ),
        (
            "packages/opencode/src/permission/index.ts",
            vec!["findLast", "export function merge"],
        ),
    ] {
        let source = authority.vendor_source(path);
        for anchor in anchors {
            assert!(
                source.contains(anchor),
                "selected {path} lost upstream fact `{anchor}`"
            );
        }
    }
}

#[test]
fn opencode_contract_owns_semantics_not_private_rust_choreography() {
    let authority = OpenCodeAuthority::load();

    for forbidden in [
        "src/backends/opencode/",
        "OpenCodeAgentRuntime::",
        "OpenCodeConversationRuntime::",
        "fn handle_",
        "match AgentInner",
    ] {
        assert!(
            !authority.protocol.contains(forbidden) && !authority.backend.contains(forbidden),
            "OpenCode contracts freeze private implementation spelling `{forbidden}`"
        );
    }
}
