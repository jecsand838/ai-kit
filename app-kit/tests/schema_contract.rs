//! The published v2 schema is a syntax contract, supplemented by the compiler.
//!
//! WORKFLOW.md owns this hand-maintained vocabulary. Tests below deliberately
//! keep adopted Agent/child/map/pipeline syntax as positive obligations even
//! while its parser implementation is pending. No ignored/compatibility lane.
//! Graph references, duplicate identities, decoded UTF-8 byte limits and local
//! embedded-schema compilation remain semantic checks. Workflow loops are valid;
//! child-inventory cycle/depth policy belongs to App admission tests.

use std::path::PathBuf;

use app_kit::workflow::WorkflowDefinition;
use serde_json::{Value, json};

struct SchemaCorpus {
    validator: jsonschema::Validator,
}

impl SchemaCorpus {
    fn open() -> Self {
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docs/schema/workflow-v2.schema.json");
        let source = std::fs::read(&path).expect("the published workflow-v2 schema must exist");
        let schema: Value = serde_json::from_slice(&source).expect("schema must be valid JSON");
        assert_eq!(
            schema["$schema"],
            "https://json-schema.org/draft/2020-12/schema"
        );
        Self::local_references(&schema);
        jsonschema::draft202012::meta::validate(&schema).expect("valid draft 2020-12 schema");
        let validator = jsonschema::draft202012::options()
            .build(&schema)
            .expect("schema references resolve within this one document");
        Self { validator }
    }

    fn local_references(value: &Value) {
        match value {
            Value::Object(fields) => {
                for (key, value) in fields {
                    if matches!(key.as_str(), "$ref" | "$dynamicRef") {
                        assert!(
                            value
                                .as_str()
                                .is_some_and(|reference| reference.starts_with('#')),
                            "the published schema cannot retrieve another document"
                        );
                    }
                    Self::local_references(value);
                }
            }
            Value::Array(values) => values.iter().for_each(Self::local_references),
            _ => {}
        }
    }

    fn document(step: Value) -> Value {
        json!({
            "schema":"app-kit.workflow/v2", "id":"schema-corpus", "revision":"r1",
            "state":{"count":0,"items":[],"object":{},"text":"hello","output":null},
            "required":["count"],
            "reducers":{"count":"sum_integer","items":"append_array","object":"merge_object",
                "text":"reject_concurrent","output":"collect"},
            "entry":"work", "nodes":[{"id":"work","step":step}], "routes":{}
        })
    }

    fn literal(value: Value) -> Value {
        json!({"kind":"literal","value":value})
    }

    fn expression(value: Value) -> Value {
        Self::document(json!({"kind":"state","mutations":[{
            "kind":"assign","target":"output","value":value
        }]}))
    }

    fn condition(condition: Value) -> Value {
        let mut document = Self::document(json!({"kind":"state","mutations":[]}));
        document["routes"] = json!({"work":{"mode":"first","branches":[{
            "when":condition,"target":{"kind":"end"}
        }]}});
        document
    }

    fn accepted(&self, label: &str, document: &Value) {
        let errors: Vec<_> = self
            .validator
            .iter_errors(document)
            .map(|error| error.to_string())
            .collect();
        assert!(
            errors.is_empty(),
            "{label}: schema rejected valid syntax: {errors:?}"
        );
        assert!(
            WorkflowDefinition::parse(&document.to_string()).is_ok(),
            "{label}: compiler rejected adopted syntax"
        );
    }

    fn rejected(&self, label: &str, document: &Value) {
        assert!(
            !self.validator.is_valid(document),
            "{label}: schema accepted malformed syntax"
        );
        assert!(
            WorkflowDefinition::parse(&document.to_string()).is_err(),
            "{label}: compiler accepted malformed syntax"
        );
    }

    fn semantic_rejection(&self, label: &str, document: &Value) {
        assert!(
            self.validator.is_valid(document),
            "{label}: must remain structurally valid"
        );
        assert!(
            WorkflowDefinition::parse(&document.to_string()).is_err(),
            "{label}: semantic compiler rejection is required"
        );
    }

    fn closed_object(&self, label: &str, document: Value, pointer: &str) {
        self.accepted(label, &document);
        let mut unknown = document.clone();
        unknown
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("unexpected_field".into(), Value::Null);
        self.rejected(&format!("{label} unknown field"), &unknown);
        if document.pointer(pointer).unwrap().get("kind").is_some() {
            let mut wrong_tag = document;
            wrong_tag.pointer_mut(pointer).unwrap()["kind"] = json!(false);
            self.rejected(&format!("{label} non-string discriminator"), &wrong_tag);
        }
    }
}

#[test]
fn schema_current_step_variants_and_optional_fields() {
    let corpus = SchemaCorpus::open();
    let steps = [
        (
            "action minimum",
            json!({"kind":"action","action":"caller","revision":"r1"}),
        ),
        (
            "action projection",
            json!({"kind":"action","action":"caller","revision":"r1","project_result":true}),
        ),
        (
            "agent minimum",
            json!({"kind":"agent","agent":"writer","prompt":[{"kind":"text","text":"hello"}]}),
        ),
        (
            "agent all optional",
            json!({"kind":"agent","agent":"writer","prompt":[{"kind":"text","text":"read "},{"kind":"state","path":"text"}],
            "structured_schema":{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false},
            "text_target":"text","structured_target":"object"}),
        ),
        (
            "agent nullable fields",
            json!({"kind":"agent","agent":"writer","prompt":[{"kind":"text","text":"hello"}],"structured_schema":null,"text_target":null,"structured_target":null}),
        ),
        ("state empty", json!({"kind":"state","mutations":[]})),
        (
            "input minimum",
            json!({"kind":"input","message":[{"kind":"text","text":"answer?"}]}),
        ),
        (
            "input optional",
            json!({"kind":"input","message":[{"kind":"state","path":"text"}],"schema":{"type":"string"},"target":"text"}),
        ),
        (
            "input nullable fields",
            json!({"kind":"input","message":[{"kind":"text","text":"answer?"}],"schema":null,"target":null}),
        ),
        (
            "child",
            json!({"kind":"child","workflow":"child-flow","input":{"count":{"kind":"state","path":"count"}},"reductions":[{"source":"count","target":"count","reducer":"sum_integer"}]}),
        ),
        (
            "map minimum",
            json!({"kind":"map","workflow":"child-flow","items":{"kind":"state","path":"items"},"input_key":"item","max_parallel":2,"reductions":[]}),
        ),
        (
            "map optional",
            json!({"kind":"map","workflow":"child-flow","items":{"kind":"literal","value":[1,2]},"input_key":"item","index_key":"index","input":{"extra":{"kind":"literal","value":true}},"max_parallel":2,"reductions":[{"source":"count","target":"output","reducer":"collect"}]}),
        ),
        (
            "map nullable index",
            json!({"kind":"map","workflow":"child-flow","items":{"kind":"literal","value":[]},"input_key":"item","index_key":null,"max_parallel":1,"reductions":[]}),
        ),
        ("join all", json!({"kind":"join_all"})),
        ("join first", json!({"kind":"join_first"})),
        (
            "sleep",
            json!({"kind":"sleep","message":"Waiting","delay_ms":1}),
        ),
    ];
    for (label, step) in steps {
        corpus.closed_object(label, SchemaCorpus::document(step), "/nodes/0/step");
    }
    for part in [
        json!({"kind":"text","text":"prompt"}),
        json!({"kind":"state","path":"text"}),
    ] {
        let document = SchemaCorpus::document(json!({"kind":"input","message":[part]}));
        corpus.closed_object("rich message part", document, "/nodes/0/step/message/0");
    }
    let document =
        SchemaCorpus::document(json!({"kind":"child","workflow":"child-flow","input":{},
        "reductions":[{"source":"count","target":"count","reducer":"sum_integer"}]}));
    corpus.closed_object("child reduction", document, "/nodes/0/step/reductions/0");
}

#[test]
fn schema_every_ordered_state_mutation() {
    let corpus = SchemaCorpus::open();
    for mutation in [
        json!({"kind":"assign","target":"count","value":SchemaCorpus::literal(json!(1))}),
        json!({"kind":"append","target":"items","value":SchemaCorpus::literal(json!(1))}),
        json!({"kind":"extend","target":"items","values":SchemaCorpus::literal(json!([1,2]))}),
        json!({"kind":"increment","target":"count","value":SchemaCorpus::literal(json!(1))}),
        json!({"kind":"remove","target":"output"}),
        json!({"kind":"wipe","keep":["count","count"]}),
        json!({"kind":"merge","state":SchemaCorpus::literal(json!({"count":2}))}),
        json!({"kind":"subset","keys":["count","count"]}),
    ] {
        corpus.closed_object(
            "state mutation",
            SchemaCorpus::document(json!({"kind":"state","mutations":[mutation]})),
            "/nodes/0/step/mutations/0",
        );
    }
}

#[test]
fn schema_every_expression_and_nullable_slice_boundary() {
    let corpus = SchemaCorpus::open();
    let mut expressions = vec![
        SchemaCorpus::literal(json!({"arbitrary key": {"kind":"not-an-expression"}})),
        json!({"kind":"state","path":"object/a~1b"}),
        json!({"kind":"conditional","when":{"kind":"constant","value":true},"then":SchemaCorpus::literal(json!(1)),"else":SchemaCorpus::literal(json!(2))}),
        json!({"kind":"attribute","value":SchemaCorpus::literal(json!({"a":1})),"name":"a"}),
        json!({"kind":"index","value":SchemaCorpus::literal(json!([1,2])),"index":SchemaCorpus::literal(json!(0))}),
        json!({"kind":"slice","value":SchemaCorpus::literal(json!([1,2,3]))}),
        json!({"kind":"slice","value":SchemaCorpus::literal(json!([1,2,3])),"start":null,"stop":null,"stride":null}),
        json!({"kind":"slice","value":SchemaCorpus::literal(json!([1,2,3])),"start":SchemaCorpus::literal(json!(0)),"stop":SchemaCorpus::literal(json!(2)),"stride":SchemaCorpus::literal(json!(1))}),
    ];
    for kind in ["length", "positive", "negative", "not"] {
        expressions.push(json!({"kind":kind,"value":SchemaCorpus::literal(json!(1))}));
    }
    for kind in ["and", "or"] {
        expressions.push(json!({"kind":kind,"values":[SchemaCorpus::literal(json!(false)),SchemaCorpus::literal(json!(true))]}));
    }
    for kind in [
        "add",
        "subtract",
        "multiply",
        "divide",
        "remainder",
        "floor_divide",
        "power",
    ] {
        expressions.push(json!({"kind":kind,"left":SchemaCorpus::literal(json!(2)),"right":SchemaCorpus::literal(json!(1))}));
    }
    for expression in expressions {
        corpus.closed_object(
            "value expression",
            SchemaCorpus::expression(expression),
            "/nodes/0/step/mutations/0/value",
        );
    }
}

#[test]
fn schema_every_condition_and_comparison_operator() {
    let corpus = SchemaCorpus::open();
    let mut conditions = vec![
        json!({"kind":"custom","condition":"caller-predicate","revision":"r1"}),
        json!({"kind":"custom","condition":"caller-predicate","revision":"r1","name":""}),
        json!({"kind":"constant","value":false}),
        json!({"kind":"exists","path":"object/absent"}),
        json!({"kind":"not","condition":{"kind":"constant","value":true}}),
        json!({"kind":"all","conditions":[]}),
        json!({"kind":"any","conditions":[]}),
        json!({"kind":"all","conditions":[{"kind":"constant","value":true}]}),
        json!({"kind":"any","conditions":[{"kind":"constant","value":false}]}),
    ];
    for kind in [
        "equal",
        "not_equal",
        "less_than",
        "less_than_or_equal",
        "greater_than",
        "greater_than_or_equal",
        "in",
        "not_in",
        "contains",
        "is",
        "is_not",
    ] {
        conditions.push(json!({"kind":kind,"left":SchemaCorpus::literal(Value::Null),"right":SchemaCorpus::literal(Value::Null)}));
        conditions.push(json!({"kind":"compare","values":[SchemaCorpus::literal(Value::Null),SchemaCorpus::literal(Value::Null)],"operators":[kind]}));
    }
    for condition in conditions {
        corpus.closed_object(
            "route condition",
            SchemaCorpus::condition(condition),
            "/routes/work/branches/0/when",
        );
    }
}

#[test]
fn schema_root_node_route_metadata_retries_and_reducers() {
    let corpus = SchemaCorpus::open();
    let basic = SchemaCorpus::document(json!({"kind":"state","mutations":[]}));
    corpus.closed_object("root", basic.clone(), "");
    for key in [
        "schema", "id", "revision", "state", "required", "reducers", "entry", "nodes", "routes",
    ] {
        let mut missing = basic.clone();
        missing.as_object_mut().unwrap().remove(key);
        corpus.rejected(&format!("missing root {key}"), &missing);
    }
    let mut metadata = basic.clone();
    metadata["nodes"][0]["name"] = json!("Display name");
    metadata["nodes"][0]["description"] = json!("Description");
    metadata["nodes"][0]["tags"] = json!(["", "space tag", "space tag", "ユニコード"]);
    metadata["nodes"][0]["deadline_ms"] = json!(100);
    corpus.closed_object("node metadata", metadata.clone(), "/nodes/0");
    for key in ["name", "description", "deadline_ms", "retry"] {
        metadata["nodes"][0][key] = Value::Null;
    }
    metadata["nodes"][0]["tags"] = json!([]);
    corpus.accepted("nullable node optional fields", &metadata);
    for retry in [
        json!({"kind":"never"}),
        json!({"kind":"immediate","max_attempts":2}),
        json!({"kind":"fixed","max_attempts":2,"max_elapsed_ms":100,"delay_ms":1}),
        json!({"kind":"exponential","max_attempts":3,"max_elapsed_ms":100,"initial_ms":1,"maximum_ms":8,"multiplier":2}),
    ] {
        let mut document = SchemaCorpus::document(
            json!({"kind":"agent","agent":"writer","prompt":[{"kind":"text","text":"hello"}]}),
        );
        document["nodes"][0]["retry"] = retry;
        corpus.closed_object("retry", document, "/nodes/0/retry");
    }
    // All reducer enum values are already present in the initial document.
    for mode in ["first", "broadcast"] {
        let mut document = basic.clone();
        document["routes"] = json!({"work":{"mode":mode,"branches":[]}});
        corpus.closed_object("empty route", document.clone(), "/routes/work");
        document["routes"]["work"]["branches"] = json!([{"when":null,"target":{"kind":"end"}}]);
        corpus.closed_object(
            "default branch",
            document.clone(),
            "/routes/work/branches/0",
        );
        corpus.closed_object("end target", document, "/routes/work/branches/0/target");
    }
    let mut loop_document = basic;
    loop_document["routes"] =
        json!({"work":{"mode":"first","branches":[{"target":{"kind":"node","id":"work"}}]}});
    corpus.closed_object(
        "node target and valid workflow loop",
        loop_document,
        "/routes/work/branches/0/target",
    );
}

#[test]
fn schema_retry_objects_apply_only_to_agent_nodes() {
    let corpus = SchemaCorpus::open();
    for step in [
        json!({"kind":"action","action":"caller","revision":"r1"}),
        json!({"kind":"state","mutations":[]}),
        json!({"kind":"input","message":[{"kind":"text","text":"answer?"}]}),
        json!({"kind":"child","workflow":"child-flow","input":{},"reductions":[]}),
        json!({"kind":"map","workflow":"child-flow","items":{"kind":"literal","value":[]},"input_key":"item","max_parallel":1,"reductions":[]}),
        json!({"kind":"pipeline","children":[{"workflow":"child-flow","input":{}}]}),
        json!({"kind":"join_all"}),
        json!({"kind":"join_first"}),
        json!({"kind":"sleep","message":"Waiting","delay_ms":1}),
    ] {
        let kind = step["kind"].as_str().unwrap().to_owned();
        let mut document = SchemaCorpus::document(step);
        corpus.accepted(&format!("{kind} without retry"), &document);
        document["nodes"][0]["retry"] = Value::Null;
        corpus.accepted(&format!("{kind} with null retry"), &document);
        for retry in [
            json!({"kind":"never"}),
            json!({"kind":"immediate","max_attempts":2}),
            json!({"kind":"fixed","max_attempts":2,"max_elapsed_ms":100,"delay_ms":1}),
            json!({"kind":"exponential","max_attempts":3,"max_elapsed_ms":100,"initial_ms":1,"maximum_ms":8,"multiplier":2}),
        ] {
            let retry_kind = retry["kind"].as_str().unwrap().to_owned();
            document["nodes"][0]["retry"] = retry;
            corpus.rejected(&format!("{kind} with {retry_kind} retry"), &document);
        }
    }
}

#[test]
fn schema_input_documents_are_local_but_meta_validation_remains_semantic() {
    let corpus = SchemaCorpus::open();
    for schema in [
        json!(true),
        json!(false),
        json!({}),
        json!({
            "$defs":{"answer":{"type":"string","minLength":1}},"$ref":"#/$defs/answer"
        }),
    ] {
        corpus.accepted(
            "local input schema",
            &SchemaCorpus::document(
                json!({"kind":"input","message":[{"kind":"text","text":"input"}],"schema":schema}),
            ),
        );
    }
    for schema in [
        json!({"type":42}),
        json!({"$ref":"#/$defs/missing"}),
        json!({"$ref":"https://example.invalid/remote-schema"}),
    ] {
        corpus.semantic_rejection(
            "embedded schema must compile locally",
            &SchemaCorpus::document(
                json!({"kind":"input","message":[{"kind":"text","text":"input"}],"schema":schema}),
            ),
        );
    }
}

#[test]
fn schema_empty_payload_variants_reject_extra_fields_independently() {
    let corpus = SchemaCorpus::open();
    let mut cases = Vec::new();
    for kind in ["join_all", "join_first"] {
        cases.push((
            kind,
            SchemaCorpus::document(json!({"kind":kind,"unexpected_field":null})),
        ));
    }
    let mut retry = SchemaCorpus::document(
        json!({"kind":"agent","agent":"writer","prompt":[{"kind":"text","text":"hello"}]}),
    );
    retry["nodes"][0]["retry"] = json!({"kind":"never","unexpected_field":null});
    cases.push(("retry never", retry));
    let mut end = SchemaCorpus::document(json!({"kind":"state","mutations":[]}));
    end["routes"] = json!({"work":{"mode":"first","branches":[{
        "target":{"kind":"end","unexpected_field":null}
    }]}});
    cases.push(("route end", end));
    let mut admitted = Vec::new();
    for (label, document) in cases {
        assert!(
            !corpus.validator.is_valid(&document),
            "{label}: closed schema payload"
        );
        if WorkflowDefinition::parse(&document.to_string()).is_ok() {
            admitted.push(label);
        }
    }
    assert!(
        admitted.is_empty(),
        "compiler accepted extra fields on empty payload variants: {admitted:?}"
    );
}

#[test]
fn schema_wrong_payloads_and_unknown_discriminators_are_rejected() {
    let corpus = SchemaCorpus::open();
    for step in [
        json!({"kind":"unknown"}),
        json!({"kind":"join_all","value":1}),
        json!({"kind":"action","action":"caller","revision":"r1","project_result":null}),
        json!({"kind":"action","action":"caller"}),
        json!({"kind":"sleep","message":[{"kind":"text","text":"wrong"}],"delay_ms":1}),
        json!({"kind":"sleep","message":"wait","delay_ms":0}),
        json!({"kind":"sleep","message":"wait","delay_ms":1.5}),
        json!({"kind":"input","message":[]}),
        json!({"kind":"input","message":[{"kind":"text","text":"ok"}],"schema":[]}),
        json!({"kind":"state","mutations":[{"kind":"assign","target":"count"}]}),
        json!({"kind":"state","mutations":[{"kind":"extend","target":"items","value":SchemaCorpus::literal(json!([]))}]}),
        json!({"kind":"map","workflow":"child-flow","items":SchemaCorpus::literal(json!([])),"input_key":"item","max_parallel":0,"reductions":[]}),
    ] {
        corpus.rejected("wrong step payload", &SchemaCorpus::document(step));
    }
    for expression in [
        json!({"kind":"unknown"}),
        json!({"kind":"literal"}),
        json!({"kind":"add","left":1,"right":2}),
        json!({"kind":"and","values":[SchemaCorpus::literal(json!(true))]}),
        json!({"kind":"slice","value":SchemaCorpus::literal(json!([])),"stride":1}),
    ] {
        corpus.rejected(
            "wrong expression payload",
            &SchemaCorpus::expression(expression),
        );
    }
    for condition in [
        json!({"kind":"constant","value":1}),
        json!({"kind":"custom","condition":"caller","revision":"r1","name":null}),
        json!({"kind":"custom","condition":"caller"}),
        json!({"kind":"all","conditions":false}),
        json!({"kind":"compare","values":[SchemaCorpus::literal(Value::Null),SchemaCorpus::literal(Value::Null)],"operators":["unknown"]}),
    ] {
        corpus.rejected(
            "wrong condition payload",
            &SchemaCorpus::condition(condition),
        );
    }
    let mut document = SchemaCorpus::document(json!({"kind":"join_all"}));
    document["nodes"][0]["tags"] = Value::Null;
    corpus.rejected("present tags cannot be null", &document);
    document["nodes"][0]["tags"] = json!([1]);
    corpus.rejected("tag members must be strings", &document);
    document["nodes"][0]["tags"] = json!([]);
    document["schema"] = json!("app-kit.workflow/v1");
    corpus.rejected("old document version", &document);
}

#[test]
fn schema_semantic_compiler_rejections_are_not_syntax_restrictions() {
    let corpus = SchemaCorpus::open();
    let basic = SchemaCorpus::document(json!({"kind":"state","mutations":[]}));
    let mut duplicate = basic.clone();
    duplicate["nodes"]
        .as_array_mut()
        .unwrap()
        .push(json!({"id":"work","step":{"kind":"join_all"}}));
    corpus.semantic_rejection("different nodes cannot share one identity", &duplicate);
    let mut missing_entry = basic.clone();
    missing_entry["entry"] = json!("missing");
    corpus.semantic_rejection("entry reference", &missing_entry);
    let mut missing_target = basic.clone();
    missing_target["routes"] =
        json!({"work":{"mode":"first","branches":[{"target":{"kind":"node","id":"missing"}}]}});
    corpus.semantic_rejection("target reference", &missing_target);
    let mut missing_state = basic.clone();
    missing_state["required"] = json!(["missing"]);
    corpus.semantic_rejection("required state membership", &missing_state);
    let mut conflicting_retry = SchemaCorpus::document(
        json!({"kind":"agent","agent":"writer","prompt":[{"kind":"text","text":"hello"}]}),
    );
    conflicting_retry["nodes"][0]["retry"] = json!({"kind":"exponential","max_attempts":2,"max_elapsed_ms":10,"initial_ms":8,"maximum_ms":1,"multiplier":2});
    corpus.semantic_rejection("cross-field retry ordering", &conflicting_retry);
    corpus.semantic_rejection("comparison operand/operator arity", &SchemaCorpus::condition(json!({"kind":"compare",
        "values":[SchemaCorpus::literal(json!(1)),SchemaCorpus::literal(json!(2)),SchemaCorpus::literal(json!(3))],"operators":["less_than"]})));
    let mut utf8 = basic.clone();
    let key = "é".repeat(65); // 65 characters, 130 decoded UTF-8 bytes.
    utf8["state"]
        .as_object_mut()
        .unwrap()
        .insert(key.clone(), Value::Null);
    utf8["reducers"]
        .as_object_mut()
        .unwrap()
        .insert(key, json!("reject_concurrent"));
    corpus.semantic_rejection("decoded UTF-8 byte bound", &utf8);
    // A JSON value has already lost duplicate-field evidence. The strict text
    // parser must reject it even though the normalized value is valid syntax.
    let duplicate_text = basic.to_string().replacen(
        "\"revision\":\"r1\"",
        "\"revision\":\"r1\",\"revision\":\"r1\"",
        1,
    );
    let normalized: Value = serde_json::from_str(&duplicate_text).unwrap();
    assert!(corpus.validator.is_valid(&normalized));
    assert!(WorkflowDefinition::parse(&duplicate_text).is_err());
}

#[test]
fn schema_adopted_agent_stream_flag_is_a_positive_parser_obligation() {
    let corpus = SchemaCorpus::open();
    for stream in [false, true] {
        corpus.accepted(
            "adopted agent stream flag",
            &SchemaCorpus::document(json!({"kind":"agent","agent":"writer",
            "prompt":[{"kind":"text","text":"hello"}],"stream":stream})),
        );
    }
}

#[test]
fn schema_adopted_child_initializer_selection_and_history_are_positive_obligations() {
    let corpus = SchemaCorpus::open();
    for checkpoint in [
        json!({"kind":"latest"}),
        json!({"kind":"sequence","value":0}),
        json!({"kind":"action","value":0}),
    ] {
        for position in ["after_completed", "at_saved", "entrypoint"] {
            let mut child = json!({"kind":"child","workflow":"child-flow","revision":"r1","input":{},
                "task_key":SchemaCorpus::literal(json!("task")),"history":"delete_after_reduction",
                "initializer":{"source":SchemaCorpus::literal(json!("run_00000000-0000-4000-8000-000000000001")),
                    "checkpoint":checkpoint,"position":position,"overrides":{"count":SchemaCorpus::literal(json!(40))},
                    "fallback":SchemaCorpus::literal(json!({"count":0}))},"reductions":[]});
            corpus.accepted(
                "adopted initializer",
                &SchemaCorpus::document(child.clone()),
            );
            child["initializer"]["occurrence"] =
                SchemaCorpus::literal(json!("00000000000040008000000000000001"));
            corpus.accepted(
                "exact initializer occurrence",
                &SchemaCorpus::document(child.clone()),
            );
            child["initializer"]["occurrence"] = json!("00000000000040008000000000000001");
            corpus.rejected(
                "initializer occurrence must be an expression",
                &SchemaCorpus::document(child),
            );
        }
    }
    corpus.accepted("adopted dynamic child", &SchemaCorpus::document(json!({"kind":"child",
        "select":SchemaCorpus::literal(json!({"workflow":"child-flow","revision":"r1"})),"input":{},"history":"retain","reductions":[]})));
}

#[test]
fn schema_adopted_task_map_and_pipeline_are_positive_parser_obligations() {
    let corpus = SchemaCorpus::open();
    corpus.accepted(
        "pipeline defaults to final-state writes",
        &SchemaCorpus::document(json!({"kind":"pipeline",
        "children":[{"workflow":"child-flow","input":{}}]})),
    );
    corpus.accepted("adopted task map", &SchemaCorpus::document(json!({"kind":"map",
        "tasks":SchemaCorpus::literal(json!([{"task_key":"one","workflow":"child-flow","revision":"r1","input":{"count":1}}])),
        "max_parallel":2,"reductions":[]})));
    corpus.accepted(
        "task data can come from state",
        &SchemaCorpus::document(json!({"kind":"map",
        "tasks":{"kind":"state","path":"items"},"max_parallel":2,"reductions":[]})),
    );
    // Literal payloads remain arbitrary JSON. Resolved task validation belongs
    // to execution, not a second expression-specific syntax grammar.
    corpus.accepted(
        "resolved task payload validation is semantic",
        &SchemaCorpus::document(json!({"kind":"map",
        "tasks":SchemaCorpus::literal(json!([{"task_key":false,"workflow":123,"input":[]}])),
        "max_parallel":2,"reductions":[]})),
    );
    corpus.accepted("adopted pipeline", &SchemaCorpus::document(json!({"kind":"pipeline",
        "children":[{"workflow":"child-flow","revision":"r1","input":{"count":SchemaCorpus::literal(json!(1))},"history":"retain"}],
        "reductions":[]})));
}

#[test]
fn schema_adopted_child_and_pipeline_reject_ambiguous_payloads() {
    let corpus = SchemaCorpus::open();
    corpus.rejected("initializer overrides is a map, not one expression", &SchemaCorpus::document(json!({
        "kind":"child","workflow":"child-flow","input":{},"reductions":[],
        "initializer":{"source":SchemaCorpus::literal(json!("run_00000000-0000-4000-8000-000000000001")),
            "checkpoint":{"kind":"latest"},"position":"after_completed",
            "overrides":SchemaCorpus::literal(json!({"count":40}))}
    })));
    for field in ["kind", "reductions"] {
        let mut child = json!({"workflow":"child-flow","input":{}});
        child[field] = if field == "kind" {
            json!("child")
        } else {
            json!([])
        };
        corpus.rejected(
            "pipeline children cannot repeat enclosing fields",
            &SchemaCorpus::document(json!({
                "kind":"pipeline","children":[child],"reductions":[]
            })),
        );
    }
    corpus.rejected(
        "pipeline children are nonempty",
        &SchemaCorpus::document(json!({
            "kind":"pipeline","children":[],"reductions":[]
        })),
    );
    corpus.rejected("child selector must choose exactly one form", &SchemaCorpus::document(json!({
        "kind":"child","workflow":"child-flow","select":SchemaCorpus::literal(json!({"workflow":"child-flow"})),
        "input":{},"reductions":[]
    })));
    for (field, value) in [
        ("workflow", json!("child-flow")),
        ("revision", json!("r1")),
        (
            "select",
            SchemaCorpus::literal(json!({"workflow":"child-flow"})),
        ),
        ("input_key", json!("item")),
        ("index_key", json!("index")),
        ("input", json!({})),
        ("task_key", SchemaCorpus::literal(json!("one"))),
        (
            "initializer",
            json!({"source":SchemaCorpus::literal(json!("run_00000000-0000-4000-8000-000000000001")),
            "checkpoint":{"kind":"latest"},"position":"after_completed"}),
        ),
        ("history", json!("retain")),
    ] {
        let mut step = json!({"kind":"map","tasks":SchemaCorpus::literal(json!([])),
            "max_parallel":2,"reductions":[]});
        step[field] = value;
        corpus.rejected(
            "tasks node has no redundant policy or item layer",
            &SchemaCorpus::document(step),
        );
    }
}

#[test]
fn schema_child_runtime_inputs_are_expression_maps() {
    let corpus = SchemaCorpus::open();
    for (mut step, pointer) in [
        (
            json!({"kind":"child","workflow":"child-flow","input":{},"reductions":[]}),
            "/nodes/0/step",
        ),
        (
            json!({"kind":"map","workflow":"child-flow","items":{"kind":"literal","value":[1]},
            "input_key":"item","max_parallel":1,"reductions":[]}),
            "/nodes/0/step",
        ),
        (
            json!({"kind":"pipeline","children":[{"workflow":"child-flow","input":{}}]}),
            "/nodes/0/step/children/0",
        ),
    ] {
        let fields = if step["kind"] == "pipeline" {
            &mut step["children"][0]
        } else {
            &mut step
        };
        fields["inputs"] = json!({
            "question":{"kind":"state","path":"text"},
            "metadata":{"kind":"literal","value":{"kind":"state","path":"not evaluated again"}}
        });
        let document = SchemaCorpus::document(step);
        corpus.accepted("child transient expression map", &document);
        let mut empty = document.clone();
        empty.pointer_mut(pointer).unwrap()["inputs"] = json!({});
        corpus.accepted("empty child transient input map", &empty);
        for malformed in [
            Value::Null,
            json!([]),
            json!({"question":"raw authored value"}),
        ] {
            let mut invalid = document.clone();
            invalid.pointer_mut(pointer).unwrap()["inputs"] = malformed;
            corpus.rejected("malformed child transient input map", &invalid);
        }
    }
}
