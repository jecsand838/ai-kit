//! Crate-local passive tracing records and exact interaction metadata checks.
//! Native commit ordering and callback failures belong to observability_contract.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use app_sdk::workflow::{Interaction, RunSnapshot};
use serde_json::Value;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

pub struct TraceRecord {
    pub name: &'static str,
    pub fields: BTreeMap<String, Value>,
    pub kind: TraceKind,
}

pub enum TraceKind {
    Span { enters: usize, exits: usize },
    Event,
}

impl TraceRecord {
    pub fn from_event(event: &Event<'_>) -> Self {
        let mut record = Self {
            name: event.metadata().name(),
            fields: BTreeMap::new(),
            kind: TraceKind::Event,
        };
        event.record(&mut record);
        record
    }
}

impl Visit for TraceRecord {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.fields
            .insert(field.name().into(), format!("{value:?}").into());
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields.insert(field.name().into(), value.into());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields.insert(field.name().into(), value.into());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields.insert(field.name().into(), value.into());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields.insert(field.name().into(), value.into());
    }
}

impl TraceRecord {
    pub fn matches_target(&self, waiting: &RunSnapshot, status: &str) -> bool {
        let (sequence, generation, category) = match waiting.interactions.as_slice() {
            [
                Interaction::Input {
                    sequence,
                    generation,
                    ..
                },
            ] => (*sequence, *generation, "input"),
            [
                Interaction::Inputs {
                    sequence,
                    generation,
                    ..
                },
            ] => (*sequence, *generation, "inputs"),
            [
                Interaction::Notification {
                    sequence,
                    generation,
                    ..
                },
            ] => (*sequence, *generation, "notification"),
            [
                Interaction::Unknown {
                    sequence,
                    generation,
                    ..
                },
            ] => (*sequence, *generation, "unknown"),
            _ => panic!("one actual public interaction supplies the metadata identity"),
        };
        matches!(self.kind, TraceKind::Event)
            && self.fields.get("event").and_then(Value::as_str) == Some("app.workflow.interaction")
            && self.fields.get("run_id").and_then(Value::as_str)
                == Some(waiting.id.to_string().as_str())
            && self.fields.get("occurrence").and_then(Value::as_str)
                == Some(waiting.occurrence.as_str())
            && self.fields.get("control").and_then(Value::as_u64) == Some(sequence)
            && self.fields.get("generation").and_then(Value::as_u64) == Some(generation)
            && self.fields.get("category").and_then(Value::as_str) == Some(category)
            && self.fields.get("status").and_then(Value::as_str) == Some(status)
    }

    pub fn assert_redacted(&self, private: &[&str]) {
        let allowed = [
            "event",
            "operation",
            "transition",
            "category",
            "status",
            "generation",
            "agents",
            "workflows",
            "workspace_id",
            "workflow_id",
            "run_id",
            "occurrence",
            "checkpoint",
            "superstep",
            "action_ordinal",
            "attempt",
            "control",
            "frontier",
            "count",
            "duration_ms",
            "elapsed_ms",
            "run_count",
        ];
        for field in self.fields.keys() {
            assert!(
                allowed.contains(&field.as_str()),
                "unexpected operational field: {field}"
            );
        }
        if self.fields.contains_key("run_id") {
            let occurrence = self
                .fields
                .get("occurrence")
                .and_then(Value::as_str)
                .expect("every run-correlated trace carries its exact public occurrence");
            assert_eq!(occurrence.len(), 32);
            assert!(
                occurrence
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            );
        }
        let encoded = serde_json::to_string(&self.fields).unwrap();
        for value in private {
            assert!(
                !encoded.contains(value),
                "operational metadata leaked caller content"
            );
        }
    }
}

#[derive(Clone)]
pub struct InteractionCapture {
    records: Arc<Mutex<Vec<TraceRecord>>>,
    // Keep the untraced host scope registered for this capture's lifetime.
    // tracing-core 0.1.36 otherwise caches the current thread's default when
    // only one local Dispatch exists; another test can initialize a callsite
    // outside that scope and suppress its later captured events. This Dispatch
    // is never installed as a thread-local or global default.
    _unscoped: tracing::Dispatch,
}

impl Default for InteractionCapture {
    fn default() -> Self {
        Self {
            records: Arc::default(),
            _unscoped: tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()),
        }
    }
}

impl InteractionCapture {
    pub fn assert_target(&self, waiting: &RunSnapshot, statuses: &[&str]) {
        let records = self.records.lock().unwrap();
        for status in statuses {
            assert!(
                records
                    .iter()
                    .any(|record| record.matches_target(waiting, status)),
                "missing {status} metadata for run {}; captured events: {:?}",
                waiting.id,
                records
                    .iter()
                    .map(|record| &record.fields)
                    .collect::<Vec<_>>()
            );
        }
    }

    pub fn assert_not_applied(&self, waiting: &RunSnapshot) {
        assert!(
            !self
                .records
                .lock()
                .unwrap()
                .iter()
                .any(|record| record.matches_target(waiting, "applied")),
            "an original unresolved lower receipt cannot produce applied metadata"
        );
    }

    pub fn assert_redacted(&self, private: &[&str]) {
        let records = self.records.lock().unwrap();
        assert!(
            !records.is_empty(),
            "the real workflow must emit operational events"
        );
        for record in records.iter() {
            record.assert_redacted(private);
        }
    }
}

impl Subscriber for InteractionCapture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.is_event()
            && (metadata.target() == "app_sdk" || metadata.target().starts_with("app_sdk::"))
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        unreachable!("this subscriber disables spans")
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        self.records
            .lock()
            .unwrap()
            .push(TraceRecord::from_event(event));
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}
