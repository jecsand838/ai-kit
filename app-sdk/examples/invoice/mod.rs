//! The invoice action shared by the action and execution-control examples.

use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use app_sdk::AppError;
use app_sdk::workflow::{
    ActionBackend, ActionDeclaration, ActionEvent, ActionMode, ActionRequest, StateChange,
    WorkflowState,
};
use futures_util::Stream;
use serde_json::{Map, Value, json};

pub(super) struct InvoiceAction {
    declaration: ActionDeclaration,
}

pub(super) enum InvoiceSource {
    Ready {
        state: WorkflowState,
        inputs: Map<String, Value>,
    },
    Completed {
        result: Map<String, Value>,
        total: u64,
    },
    Done,
}

impl InvoiceAction {
    pub(super) fn new() -> Self {
        Self {
            declaration: ActionDeclaration {
                key: "invoice".into(),
                revision: "r1".into(),
                reads: vec!["unit_price".into()],
                writes: vec!["total".into()],
                required_inputs: vec!["quantity".into()],
                optional_inputs: vec!["label".into()],
                bound_inputs: Map::from_iter([("currency".into(), json!("USD"))]),
                default_inputs: Map::from_iter([("label".into(), json!("order"))]),
                tags: vec!["billing".into()],
                source: Some("invoice: unit_price * quantity; explicit total assignment".into()),
                mode: ActionMode::Both,
            },
        }
    }
}

impl ActionBackend for InvoiceAction {
    type Stream = InvoiceSource;

    fn declaration(&self) -> &ActionDeclaration {
        &self.declaration
    }

    fn start(&self, request: ActionRequest) -> Self::Stream {
        InvoiceSource::Ready {
            state: request.state,
            inputs: request.inputs,
        }
    }
}

impl InvoiceSource {
    fn calculate(state: WorkflowState, inputs: Map<String, Value>) -> Result<Self, AppError> {
        let price = state
            .values()
            .get("unit_price")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                AppError::InvalidInput("unit_price must be an unsigned integer".into())
            })?;
        let quantity = inputs
            .get("quantity")
            .and_then(Value::as_u64)
            .ok_or_else(|| AppError::InvalidInput("quantity must be an unsigned integer".into()))?;
        let total = price
            .checked_mul(quantity)
            .ok_or_else(|| AppError::InvalidInput("invoice total overflowed".into()))?;
        let mut result = inputs;
        result.insert("total".into(), json!(total));
        Ok(Self::Completed { result, total })
    }
}

impl Stream for InvoiceSource {
    type Item = Result<ActionEvent, AppError>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let source = self.get_mut();
        Poll::Ready(match mem::replace(source, Self::Done) {
            Self::Ready { state, inputs } => match Self::calculate(state, inputs) {
                Ok(completed) => {
                    *source = completed;
                    Some(Ok(ActionEvent::Partial {
                        result: Map::from_iter([("progress".into(), json!("calculated"))]),
                    }))
                }
                Err(error) => Some(Err(error)),
            },
            Self::Completed { result, total } => Some(Ok(ActionEvent::Completed {
                result,
                writes: vec![StateChange::Assign {
                    values: Map::from_iter([("total".into(), json!(total))]),
                }],
            })),
            Self::Done => None,
        })
    }
}
