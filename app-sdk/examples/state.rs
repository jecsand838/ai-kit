//! Immutable state changes, local typed data, and explicit JSON codecs.
//! Run with `cargo run -p app-sdk --example state`.

use std::rc::Rc;

use app_sdk::AppError;
use app_sdk::workflow::{StateChange, StateSerialization, StateTyping, WorkflowState};
use serde_json::{Map, Value, json};

struct CountData {
    count: i64,
    labels: Vec<Value>,
}

struct CountPolicy {
    maximum: i64,
}

impl StateTyping for CountPolicy {
    // The behavior owner is Send + Sync. Its locally constructed Data need not be.
    type Data = Rc<CountData>;

    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, AppError> {
        let count = state.values().get("count").and_then(Value::as_i64);
        let labels = state.values().get("labels").and_then(Value::as_array);
        match (count, labels) {
            (Some(count), Some(labels)) => Ok(Rc::new(CountData {
                count,
                labels: labels.clone(),
            })),
            _ => Err(AppError::InvalidInput(
                "count and labels have invalid types".into(),
            )),
        }
    }

    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, AppError> {
        WorkflowState::new(Map::from_iter([
            ("count".into(), json!(data.count)),
            ("labels".into(), json!(data.labels)),
        ]))
    }

    fn validate_state(&self, state: &WorkflowState) -> Result<(), AppError> {
        let data = self.construct_data(state)?;
        if !(0..=self.maximum).contains(&data.count) {
            return Err(AppError::InvalidInput(
                "count is outside its allowed range".into(),
            ));
        }
        Ok(())
    }
}

fn main() -> Result<(), AppError> {
    let policy = CountPolicy { maximum: 100 };
    let initial = WorkflowState::new(Map::from_iter([
        ("count".into(), json!(1)),
        ("labels".into(), json!([])),
    ]))?;
    let counted = initial.apply(
        &StateChange::Increment {
            values: vec![("count".into(), json!(3)), ("count".into(), json!(4))],
        },
        &policy,
    )?;
    let labeled = counted.apply(
        &StateChange::Append {
            values: vec![
                ("labels".into(), json!("draft")),
                ("labels".into(), json!("queued")),
            ],
        },
        &policy,
    )?;
    assert_eq!(initial.values()["count"], 1);
    assert_eq!(labeled.values()["count"], 8);
    assert_eq!(labeled.values()["labels"], json!(["draft", "queued"]));

    // A later invalid entry rejects the complete operation, including count + 1.
    let rejected = labeled.apply(
        &StateChange::Increment {
            values: vec![("count".into(), json!(1)), ("labels".into(), json!(1))],
        },
        &policy,
    );
    assert!(matches!(rejected, Err(AppError::InvalidInput(_))));
    assert_eq!(labeled.values()["count"], 8);

    // This registration is process-global. This standalone example owns its
    // process; an application should choose registrations during its setup.
    StateSerialization::global().register_field(
        "labels".into(),
        |value| Ok::<_, &'static str>(json!({"version": 1, "items": value})),
        |value| value.get("items").cloned().ok_or("missing labels.items"),
    )?;
    let encoded = labeled.serialize()?;
    assert_eq!(
        encoded["labels"],
        json!({"version":1,"items":["draft","queued"]})
    );
    let decoded = WorkflowState::deserialize(encoded.clone())?;
    assert_eq!(decoded, labeled);

    StateSerialization::global().register_tag("example-money".into(), |value| {
        value.get("minor").cloned().ok_or("missing money.minor")
    })?;
    let tagged = WorkflowState::deserialize(Map::from_iter([(
        "amount".into(),
        json!({"__burr_serde__":"example-money","minor":120}),
    )]))?;
    assert_eq!(tagged.values()["amount"], 120);
    println!("{}", Value::Object(encoded));
    Ok(())
}
