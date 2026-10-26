//! Parse, compose and export metadata without opening an App or a lower service.
//! Run with `cargo run -p app-sdk --example graph`.

use app_sdk::AppError;
use app_sdk::workflow::{
    GraphComposition, GraphConnection, GraphFormat, GraphImport, WorkflowDefinition,
};
use serde_json::json;

fn main() -> Result<(), AppError> {
    let mut imports = Vec::new();
    for (id, prefix) in [("first-counter", "first_"), ("second-counter", "second_")] {
        let definition = WorkflowDefinition::parse(
            &json!({
                "schema":"app-sdk.workflow/v2", "id":id, "revision":"r1",
                "state":{"count":0}, "required":["count"],
                "reducers":{"count":"reject_concurrent"}, "entry":"add",
                "nodes":[{"id":"add", "name":"Add one", "step":{
                    "kind":"state", "mutations":[{
                        "kind":"increment", "target":"count",
                        "value":{"kind":"literal", "value":1}
                    }]
                }}],
                "routes":{}
            })
            .to_string(),
        )?;
        imports.push(GraphImport {
            definition,
            prefix: prefix.into(),
        });
    }
    let definition = WorkflowDefinition::compose(GraphComposition {
        id: "two-counters".parse()?,
        revision: "r1".into(),
        entry: "first_add".into(),
        imports,
        connections: vec![GraphConnection {
            from: "first_add".into(),
            to: "second_add".into(),
        }],
    })?;
    let graph = definition.graph();
    assert_eq!(graph.entry, "first_add");
    assert_eq!(graph.nodes.len(), 2);
    assert_eq!(graph.nodes[0].id, "first_add");
    assert_eq!(graph.nodes[1].id, "second_add");
    assert!(
        graph
            .edges
            .iter()
            .any(|edge| { edge.from == "first_add" && edge.to.as_deref() == Some("second_add") })
    );

    let inspection = definition.export(GraphFormat::Json)?;
    let inspection: serde_json::Value = serde_json::from_str(&inspection)
        .map_err(|_| AppError::Integration("example graph JSON was invalid".into()))?;
    assert_eq!(inspection["nodes"].as_array().map(Vec::len), Some(2));
    assert!(definition.export(GraphFormat::Dot)?.starts_with("digraph "));
    println!("{}", definition.export(GraphFormat::Mermaid)?);

    // Built-in nodes retain their canonical authored source. Tracking requires
    // actual source for every node and is a separate content-export format.
    let tracking = graph.tracking_record()?;
    assert_eq!(tracking["type"], "application");
    assert_eq!(tracking["actions"].as_array().map(Vec::len), Some(2));
    Ok(())
}
