//! Direct public-trait mocks. No supplied Toolset or extension ingress is used.
mod mock_support;

use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::task::{Context, Poll, Waker};

use mock_support::{CallerHandler, CallerTools, HandlerScript, ScriptedTools, Transition};
use serde_json::json;
use tools_kit::declaration::{ToolAnnotations, ToolIcon, ToolInfo, ToolName};
use tools_kit::execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult};
use tools_kit::interaction::{
    ToolInteractionCapabilities, ToolInteractionId, ToolInteractionKind, ToolInteractionRequest,
    ToolInteractionResponse,
};
use tools_kit::schema::JsonSchema;
use tools_kit::{ToolError, ToolHandlerBackend, ToolsetBackend};

struct Case;

impl Case {
    fn name() -> ToolName {
        ToolName::new("lookup").unwrap()
    }

    fn info() -> ToolInfo {
        let mut info = ToolInfo::new(Self::name(), JsonSchema::strict(json!({"type":"object"})));
        info.title = Some("Lookup".into());
        info.description = Some("Returns a caller-selected record".into());
        info.output_schema = Some(JsonSchema::strict(json!({"type":"object"})));
        info.annotations = ToolAnnotations {
            title: Some("Read record".into()),
            read_only_hint: Some(true),
            destructive_hint: Some(false),
            idempotent_hint: Some(true),
            open_world_hint: Some(false),
        };
        info.icons = vec![ToolIcon {
            uri: "data:image/png;base64,aA==".into(),
            mime_type: Some("image/png".into()),
        }];
        info.interactions = ToolInteractionCapabilities {
            text: true,
            multiline_text: true,
            form: true,
            form_schema_validation: true,
        };
        info.metadata.insert("source".into(), json!({"version":2}));
        info
    }

    fn input(id: &str) -> ToolHandlerInput {
        ToolHandlerInput::Invoke(ToolInvocation {
            call_id: Some(id.into()),
            arguments: json!({"record":"42"}),
        })
    }

    fn script(
        input: ToolHandlerInput,
        output: Result<ToolHandlerOutput, ToolError>,
    ) -> HandlerScript {
        HandlerScript {
            name: Self::name(),
            transitions: VecDeque::from([Transition { input, output }]),
        }
    }

    fn request(call: &str, kind: ToolInteractionKind) -> ToolInteractionRequest {
        ToolInteractionRequest {
            id: ToolInteractionId::new(),
            tool_name: Self::name(),
            call_id: Some(call.into()),
            prompt: "Confirm record".into(),
            kind,
        }
    }

    fn ready<F: Future>(future: F) -> F::Output {
        let mut pinned = std::pin::pin!(future);
        match pinned
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("immediate fixture unexpectedly suspended"),
        }
    }
}

#[tokio::test]
async fn struct_inventory_exact_requests_unique_children_and_completed_results() {
    let info = Case::info();
    let mut result = ToolResult::json(json!({"answer":42}));
    result.metadata.insert("trace".into(), json!("owned"));
    let scripts = vec![
        Case::script(
            Case::input("first"),
            Ok(ToolHandlerOutput::Completed(result.clone())),
        ),
        Case::script(
            Case::input("second"),
            Ok(ToolHandlerOutput::Completed(
                ToolResult::text("missing").with_error(true),
            )),
        ),
    ];
    let (tools, observed) = ScriptedTools::new(Ok(vec![info.clone()]), scripts, Ok(()));
    let first_inventory = tools.list_tools().await.unwrap();
    assert_eq!(first_inventory, vec![info]);
    let mut altered = first_inventory.clone();
    altered[0].description = None;
    assert_eq!(tools.list_tools().await.unwrap(), first_inventory);
    let unknown = ToolName::new("missing").unwrap();
    assert_eq!(
        tools.tool_handler(unknown.clone()).await.err(),
        Some(ToolError::UnknownTool { name: unknown })
    );
    let mut first = tools.tool_handler(Case::name()).await.unwrap();
    let mut second = tools.tool_handler(Case::name()).await.unwrap();
    assert_eq!(
        second.step(Case::input("second")).await.unwrap(),
        ToolHandlerOutput::Completed(ToolResult::text("missing").with_error(true))
    );
    assert_eq!(
        first.step(Case::input("first")).await.unwrap(),
        ToolHandlerOutput::Completed(result)
    );
    assert!(matches!(
        first.step(Case::input("again")).await,
        Err(ToolError::Closed { .. })
    ));
    assert_eq!(
        observed.inputs(),
        vec![(1, Case::input("second")), (0, Case::input("first"))]
    );
    tools.assert_complete();
    drop(first);
    drop(second);
    let (a, b) = tokio::join!(tools.shutdown(), tools.shutdown());
    a.unwrap();
    b.unwrap();
    assert!(matches!(
        tools.list_tools().await,
        Err(ToolError::Closed { .. })
    ));
    assert!(matches!(
        tools.tool_handler(Case::name()).await,
        Err(ToolError::Closed { .. })
    ));
    observed.assert_cleanup(2);
}

#[tokio::test]
async fn caller_enum_branches_preserve_children_inventory_inputs_and_errors() {
    for backup in [false, true] {
        let failure = ToolError::Transport {
            tool: Some(Case::name()),
            message: "fixture transport failed".into(),
        };
        let (tools, observed) = ScriptedTools::new(
            Ok(vec![Case::info()]),
            vec![Case::script(Case::input("enum"), Err(failure.clone()))],
            Ok(()),
        );
        let caller = if backup {
            CallerTools::Backup(tools)
        } else {
            CallerTools::Primary(tools)
        };
        assert_eq!(caller.list_tools().await.unwrap(), vec![Case::info()]);
        let mut handler = caller.tool_handler(Case::name()).await.unwrap();
        assert!(matches!(
            (&handler, backup),
            (CallerHandler::Primary(_), false) | (CallerHandler::Backup(_), true)
        ));
        assert_eq!(handler.step(Case::input("enum")).await, Err(failure));
        assert!(matches!(
            handler.step(Case::input("enum")).await,
            Err(ToolError::Closed { .. })
        ));
        observed.assert_complete();
        drop(handler);
        caller.shutdown().await.unwrap();
        caller.shutdown().await.unwrap();
        observed.assert_cleanup(1);
    }
}

#[tokio::test]
async fn repeated_interactions_preserve_correlation_and_reject_stale_or_wrong_ids() {
    let text = Case::request("interact", ToolInteractionKind::Text { multiline: true });
    let form = Case::request(
        "interact",
        ToolInteractionKind::Form {
            schema: json!({"type":"object"}),
            schema_validation: true,
        },
    );
    let first_response = ToolHandlerInput::Interaction(ToolInteractionResponse::submit(
        text.id.clone(),
        json!("continue\nplease"),
    ));
    let second_response = ToolHandlerInput::Interaction(ToolInteractionResponse::submit(
        form.id.clone(),
        json!({"confirm":true}),
    ));
    let script = HandlerScript {
        name: Case::name(),
        transitions: VecDeque::from([
            Transition {
                input: Case::input("interact"),
                output: Ok(ToolHandlerOutput::Interaction(text.clone())),
            },
            Transition {
                input: first_response.clone(),
                output: Ok(ToolHandlerOutput::Interaction(form.clone())),
            },
            Transition {
                input: second_response.clone(),
                output: Ok(ToolHandlerOutput::Completed(ToolResult::json(
                    json!({"confirmed":true}),
                ))),
            },
        ]),
    };
    let (tools, observed) = ScriptedTools::new(Ok(vec![Case::info()]), vec![script], Ok(()));
    let mut handler = tools.tool_handler(Case::name()).await.unwrap();
    assert!(matches!(
        handler.step(first_response.clone()).await,
        Err(ToolError::InvalidInteraction { .. })
    ));
    assert_eq!(
        handler.step(Case::input("interact")).await.unwrap(),
        ToolHandlerOutput::Interaction(text)
    );
    let wrong = ToolHandlerInput::Interaction(ToolInteractionResponse::submit(
        ToolInteractionId::new(),
        json!("wrong"),
    ));
    assert!(matches!(
        handler.step(wrong).await,
        Err(ToolError::InvalidInteraction { .. })
    ));
    assert!(matches!(
        handler.step(Case::input("duplicate")).await,
        Err(ToolError::Protocol { .. })
    ));
    assert_eq!(
        handler.step(first_response.clone()).await.unwrap(),
        ToolHandlerOutput::Interaction(form)
    );
    assert!(matches!(
        handler.step(first_response).await,
        Err(ToolError::InvalidInteraction { .. })
    ));
    assert!(
        matches!(handler.step(second_response).await.unwrap(),ToolHandlerOutput::Completed(result) if !result.is_error)
    );
    tools.assert_complete();
    drop(handler);
    tools.shutdown().await.unwrap();
    observed.assert_cleanup(1);
}

#[tokio::test]
async fn both_enum_arms_forward_interactions_success_metadata_and_model_visible_failures() {
    for backup in [false, true] {
        let text = Case::request(
            "enum-interact",
            ToolInteractionKind::Text { multiline: true },
        );
        let form = Case::request(
            "enum-interact",
            ToolInteractionKind::Form {
                schema: json!({"type":"object","properties":{"confirmed":{"type":"boolean"}},"required":["confirmed"]}),
                schema_validation: true,
            },
        );
        let text_response = ToolHandlerInput::Interaction(ToolInteractionResponse::submit(
            text.id.clone(),
            json!("exact\nanswer"),
        ));
        let form_response = ToolHandlerInput::Interaction(ToolInteractionResponse::submit(
            form.id.clone(),
            json!({"confirmed":true}),
        ));
        let mut successful = ToolResult::json(
            json!({"confirmed":true,"family":if backup {"backup"} else {"primary"}}),
        );
        successful.metadata.insert(
            "trace".into(),
            json!({"request":"enum-interact","attempt":1}),
        );
        let model_failure = ToolResult::text("record unavailable").with_error(true);
        let scripts = vec![
            HandlerScript {
                name: Case::name(),
                transitions: VecDeque::from([
                    Transition {
                        input: Case::input("enum-interact"),
                        output: Ok(ToolHandlerOutput::Interaction(text.clone())),
                    },
                    Transition {
                        input: text_response.clone(),
                        output: Ok(ToolHandlerOutput::Interaction(form.clone())),
                    },
                    Transition {
                        input: form_response.clone(),
                        output: Ok(ToolHandlerOutput::Completed(successful.clone())),
                    },
                ]),
            },
            Case::script(
                Case::input("enum-failure"),
                Ok(ToolHandlerOutput::Completed(model_failure.clone())),
            ),
        ];
        let (tools, observed) = ScriptedTools::new(Ok(vec![Case::info()]), scripts, Ok(()));
        let caller = if backup {
            CallerTools::Backup(tools)
        } else {
            CallerTools::Primary(tools)
        };
        let mut handler = caller.tool_handler(Case::name()).await.unwrap();
        let mut failure_handler = caller.tool_handler(Case::name()).await.unwrap();
        assert_eq!(
            handler.step(Case::input("enum-interact")).await.unwrap(),
            ToolHandlerOutput::Interaction(text)
        );
        assert_eq!(
            failure_handler
                .step(Case::input("enum-failure"))
                .await
                .unwrap(),
            ToolHandlerOutput::Completed(model_failure)
        );
        assert_eq!(
            handler.step(text_response.clone()).await.unwrap(),
            ToolHandlerOutput::Interaction(form)
        );
        assert!(matches!(
            handler.step(text_response.clone()).await,
            Err(ToolError::InvalidInteraction { .. })
        ));
        assert_eq!(
            handler.step(form_response.clone()).await.unwrap(),
            ToolHandlerOutput::Completed(successful)
        );
        assert!(matches!(
            handler.step(form_response.clone()).await,
            Err(ToolError::Closed { .. })
        ));
        assert_eq!(
            observed.inputs(),
            vec![
                (0, Case::input("enum-interact")),
                (1, Case::input("enum-failure")),
                (0, text_response),
                (0, form_response)
            ]
        );
        observed.assert_complete();
        drop(handler);
        drop(failure_handler);
        caller.shutdown().await.unwrap();
        observed.assert_cleanup(2);
    }
}

#[tokio::test]
async fn decline_interaction_cancel_and_direct_cancel_are_terminal_typed_failures() {
    for mode in 0..3 {
        let request = Case::request("cancel", ToolInteractionKind::Text { multiline: false });
        let (input, error) = match mode {
            0 => (
                ToolHandlerInput::Interaction(ToolInteractionResponse::decline(
                    request.id.clone(),
                    Some("no".into()),
                )),
                ToolError::InteractionDeclined {
                    tool: Case::name(),
                    reason: Some("no".into()),
                },
            ),
            1 => (
                ToolHandlerInput::Interaction(ToolInteractionResponse::cancel(
                    request.id.clone(),
                    Some("stop".into()),
                )),
                ToolError::Cancelled {
                    tool: Case::name(),
                    reason: Some("stop".into()),
                },
            ),
            2 => (
                ToolHandlerInput::Cancel { reason: None },
                ToolError::Cancelled {
                    tool: Case::name(),
                    reason: None,
                },
            ),
            _ => unreachable!(),
        };
        let script = HandlerScript {
            name: Case::name(),
            transitions: VecDeque::from([
                Transition {
                    input: Case::input("cancel"),
                    output: Ok(ToolHandlerOutput::Interaction(request)),
                },
                Transition {
                    input: input.clone(),
                    output: Err(error.clone()),
                },
            ]),
        };
        let (tools, observed) = ScriptedTools::new(Ok(vec![Case::info()]), vec![script], Ok(()));
        let mut handler = tools.tool_handler(Case::name()).await.unwrap();
        handler.step(Case::input("cancel")).await.unwrap();
        assert_eq!(handler.step(input.clone()).await, Err(error));
        assert!(matches!(
            handler.step(input).await,
            Err(ToolError::Closed { .. })
        ));
        tools.assert_complete();
        drop(handler);
        tools.shutdown().await.unwrap();
        observed.assert_cleanup(1);
    }
}

#[tokio::test]
async fn empty_inventory_failure_fidelity_and_shutdown_failure_replay() {
    let (empty, observed) = ScriptedTools::new(Ok(vec![]), vec![], Ok(()));
    assert!(empty.list_tools().await.unwrap().is_empty());
    empty.shutdown().await.unwrap();
    observed.assert_cleanup(0);
    let failure = ToolError::Protocol {
        message: "inventory unavailable".into(),
    };
    let cleanup = ToolError::Transport {
        tool: None,
        message: "cleanup failed".into(),
    };
    let (tools, observed) = ScriptedTools::new(Err(failure.clone()), vec![], Err(cleanup.clone()));
    assert_eq!(tools.list_tools().await, Err(failure.clone()));
    assert_eq!(tools.tool_handler(Case::name()).await.err(), Some(failure));
    assert_eq!(tools.shutdown().await, Err(cleanup.clone()));
    assert_eq!(tools.shutdown().await, Err(cleanup));
    observed.assert_cleanup(0);
}

#[test]
fn fixture_detects_wrong_arguments_exhaustion_unexpected_and_unconsumed_calls() {
    let script = Case::script(
        Case::input("required"),
        Ok(ToolHandlerOutput::Completed(ToolResult::json(json!({})))),
    );
    let (tools, observed) = ScriptedTools::new(Ok(vec![Case::info()]), vec![script], Ok(()));
    assert!(catch_unwind(AssertUnwindSafe(|| observed.assert_complete())).is_err());
    let mut handler = Case::ready(tools.tool_handler(Case::name())).unwrap();
    assert!(
        catch_unwind(AssertUnwindSafe(|| Case::ready(
            handler.step(Case::input("wrong"))
        )))
        .is_err()
    );
    Case::ready(handler.step(Case::input("required"))).unwrap();
    observed.assert_complete();
    drop(handler);
    Case::ready(tools.shutdown()).unwrap();
    let (empty, _) = ScriptedTools::new(Ok(vec![Case::info()]), vec![], Ok(()));
    assert!(
        catch_unwind(AssertUnwindSafe(|| Case::ready(
            empty.tool_handler(Case::name())
        )))
        .is_err()
    );
    let (exhausted, _) = ScriptedTools::new(
        Ok(vec![Case::info()]),
        vec![HandlerScript {
            name: Case::name(),
            transitions: VecDeque::new(),
        }],
        Ok(()),
    );
    let mut handler = Case::ready(exhausted.tool_handler(Case::name())).unwrap();
    assert!(
        catch_unwind(AssertUnwindSafe(|| Case::ready(
            handler.step(Case::input("extra"))
        )))
        .is_err()
    );
}

#[test]
fn immediate_shutdown_seals_existing_handlers_without_background_work() {
    let (tools, observed) = ScriptedTools::new(
        Ok(vec![Case::info()]),
        vec![HandlerScript {
            name: Case::name(),
            transitions: VecDeque::new(),
        }],
        Ok(()),
    );
    // A future that was never polled never accepted the shutdown command.
    drop(tools.shutdown());
    assert_eq!(Case::ready(tools.list_tools()).unwrap(), vec![Case::info()]);
    let mut handler = Case::ready(tools.tool_handler(Case::name())).unwrap();
    Case::ready(tools.shutdown()).unwrap();
    assert!(matches!(
        Case::ready(handler.step(Case::input("sealed"))),
        Err(ToolError::Closed { .. })
    ));
    Case::ready(tools.shutdown()).unwrap();
    tools.assert_complete();
    drop(handler);
    observed.assert_cleanup(1);
}
