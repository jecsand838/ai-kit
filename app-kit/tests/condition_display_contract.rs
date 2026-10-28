//! Source-derived condition labels from pinned rurr action/condition.rs:
//! 323–333 preserves caller names, 397–400 exposes them, and 467–509 composes
//! them with ~, & and |. These unbound graph tests supply no callable inventory.

use app_kit::AppError;
use app_kit::workflow::{GraphComposition, GraphFormat, GraphImport, WorkflowDefinition};
use serde_json::{Value, json};

struct ConditionDisplay;

impl ConditionDisplay {
    fn custom(name: Option<Value>) -> Value {
        let mut condition = json!({"kind":"custom","condition":"choose","revision":"r1"});
        if let Some(name) = name {
            condition["name"] = name;
        }
        condition
    }

    fn source(condition: Value) -> String {
        json!({
            "schema":"app-kit.workflow/v2","id":"display","revision":"r1",
            "state":{},"required":[],"reducers":{},"entry":"a",
            "nodes":[
                {"id":"a","step":{"kind":"state","mutations":[]}},
                {"id":"b","step":{"kind":"state","mutations":[]}}
            ],
            "routes":{"a":{"mode":"first","branches":[
                {"when":condition,"target":{"kind":"node","id":"b"}}
            ]}}
        })
        .to_string()
    }

    fn parse(condition: Value) -> WorkflowDefinition {
        WorkflowDefinition::parse(&Self::source(condition)).expect("authored condition syntax")
    }

    fn label(condition: Value, expected: &str) {
        let definition = Self::parse(condition);
        let graph = definition.graph();
        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.edges[0].condition.as_deref(), Some(expected));
        let exported: Value = serde_json::from_str(&definition.export(GraphFormat::Json).unwrap())
            .expect("lossless graph JSON");
        assert_eq!(exported["edges"][0]["condition"], expected);
        assert_eq!(
            graph.tracking_record().unwrap()["transitions"][0]["condition"],
            expected
        );
    }
}

#[test]
fn built_in_condition_composition_uses_pinned_names_without_invented_parentheses() {
    let yes = json!({"kind":"constant","value":true});
    let no = json!({"kind":"constant","value":false});
    for (condition, expected) in [
        (json!({"kind":"not","condition":yes.clone()}), "~True"),
        (
            json!({"kind":"all","conditions":[yes.clone(),no.clone()]}),
            "True & False",
        ),
        (
            json!({"kind":"any","conditions":[yes.clone(),no.clone()]}),
            "True | False",
        ),
        (
            json!({"kind":"not","condition":{"kind":"any","conditions":[yes.clone(),no.clone()]}}),
            "~True | False",
        ),
        (
            json!({"kind":"any","conditions":[{"kind":"all","conditions":[yes.clone(),{"kind":"not","condition":no.clone()}]},no]}),
            "True & ~False | False",
        ),
        (json!({"kind":"all","conditions":[yes]}), "True"),
        (json!({"kind":"all","conditions":[]}), "True"),
        (json!({"kind":"any","conditions":[]}), "False"),
    ] {
        ConditionDisplay::label(condition, expected);
    }
}

#[test]
fn value_expression_operators_keep_expression_labels() {
    ConditionDisplay::label(
        json!({"kind":"equal",
            "left":{"kind":"and","values":[{"kind":"literal","value":true},{"kind":"literal","value":false}]},
            "right":{"kind":"literal","value":false}
        }),
        "(True) and (False) == False",
    );
}

#[test]
fn custom_names_preserve_arbitrary_display_text_without_binding_or_evaluation() {
    let long_name = "Display label outside canonical binding grammar ".repeat(4);
    for name in [
        "",
        "  \t  ",
        "任意 label 🦀 — a/b? 12.5 > 7",
        "quote \" slash \\ newline\nreturn\rtab\tnul\0 pipe | & [] #",
        long_name.as_str(),
    ] {
        ConditionDisplay::label(ConditionDisplay::custom(Some(json!(name))), name);
    }
    ConditionDisplay::label(ConditionDisplay::custom(None), "choose");
}

#[test]
fn composed_custom_names_follow_pinned_name_concatenation() {
    let left = " left | (literal) ";
    let right = "right & [display]";
    let condition = json!({"kind":"any","conditions":[
        {"kind":"not","condition":ConditionDisplay::custom(Some(json!(left)))},
        {"kind":"all","conditions":[ConditionDisplay::custom(Some(json!(right))),ConditionDisplay::custom(None)]}
    ]});
    ConditionDisplay::label(condition, &format!("~{left} | {right} & choose"));
}

#[test]
fn caller_condition_labels_are_escaped_by_each_existing_graph_exporter() {
    let name = "name \"quoted\"\\line\n] --> injected[<script>&|#";
    let definition = ConditionDisplay::parse(ConditionDisplay::custom(Some(json!(name))));
    let dot = definition.export(GraphFormat::Dot).expect("bounded DOT");
    assert!(dot.contains("name \\\"quoted\\\"\\\\line\\n] --> injected[<script>&|#"));
    assert!(!dot.contains(name));
    assert_eq!(dot.matches(" -> ").count(), 1);
    let mermaid = definition
        .export(GraphFormat::Mermaid)
        .expect("bounded Mermaid");
    assert!(mermaid.contains(
        "name #34;quoted#34;#92;line#10;#93; --#62; injected#91;#60;script#62;#38;#124;#35;"
    ));
    assert!(!mermaid.contains(name) && !mermaid.contains("<script>"));
    assert_eq!(mermaid.matches(" -->|").count(), 1);
    assert_eq!(definition.export(GraphFormat::Dot).unwrap(), dot);
    assert_eq!(definition.export(GraphFormat::Mermaid).unwrap(), mermaid);
}

#[test]
fn graph_prefix_composition_keeps_the_condition_display_name() {
    let name = "caller label != canonical-key";
    let definition = WorkflowDefinition::compose(GraphComposition {
        id: "composed".parse().unwrap(),
        revision: "r1".into(),
        entry: "part-a".into(),
        imports: vec![GraphImport {
            definition: ConditionDisplay::parse(ConditionDisplay::custom(Some(json!(name)))),
            prefix: "part-".into(),
        }],
        connections: Vec::new(),
    })
    .expect("ordinary graph composition");
    let graph = definition.graph();
    assert_eq!(graph.edges[0].from, "part-a");
    assert_eq!(graph.edges[0].to.as_deref(), Some("part-b"));
    assert_eq!(graph.edges[0].condition.as_deref(), Some(name));
}

#[test]
fn a_present_custom_display_name_requires_a_string() {
    // The valid control must parse before malformed cases can prove rejection.
    ConditionDisplay::label(
        ConditionDisplay::custom(Some(json!("valid name"))),
        "valid name",
    );
    for name in [Value::Null, json!(true), json!(42), json!([]), json!({})] {
        assert!(matches!(
            WorkflowDefinition::parse(&ConditionDisplay::source(ConditionDisplay::custom(Some(
                name
            )))),
            Err(AppError::InvalidInput(_))
        ));
    }
}

#[test]
fn custom_display_names_use_the_existing_byte_bound_without_identity_restrictions() {
    for name in ["n".repeat(65_535), "n".repeat(65_536), "é".repeat(32_768)] {
        ConditionDisplay::label(ConditionDisplay::custom(Some(json!(name))), &name);
    }
    for name in ["n".repeat(65_537), format!("{}n", "é".repeat(32_768))] {
        assert!(matches!(
            WorkflowDefinition::parse(&ConditionDisplay::source(ConditionDisplay::custom(Some(
                json!(name)
            )))),
            Err(AppError::InvalidInput(_))
        ));
    }
}
