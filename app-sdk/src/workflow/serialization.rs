use std::collections::BTreeMap;
use std::fmt::Display;
use std::sync::{Arc, OnceLock, RwLock};

use serde_json::{Map, Value};

use crate::AppError;
use crate::workflow::definition::JsonBoundary;
use crate::workflow::{StateSerialization, WorkflowState};

#[derive(Clone, Default)]
pub(super) struct SerializationRegistry {
    fields: BTreeMap<String, FieldCodec>,
    tags: BTreeMap<String, Arc<dyn ValueCodec>>,
}

#[derive(Clone)]
struct FieldCodec {
    serialize: Arc<dyn ValueCodec>,
    deserialize: Arc<dyn ValueCodec>,
}

trait ValueCodec: Send + Sync {
    fn transform(&self, value: &Value) -> Result<Value, AppError>;
}

impl<F> ValueCodec for F
where
    F: Fn(&Value) -> Result<Value, AppError> + Send + Sync,
{
    fn transform(&self, value: &Value) -> Result<Value, AppError> {
        self(value)
    }
}

impl StateSerialization {
    /// Returns the one process-global registry, shared across all state owners.
    ///
    /// ```
    /// use app_sdk::workflow::{StateSerialization, WorkflowState};
    /// assert!(std::ptr::eq(StateSerialization::global(), StateSerialization::global()));
    /// StateSerialization::global().register_tag("global_counter".into(), |value| {
    ///     value.get("count").cloned().ok_or("missing count")
    /// })?;
    /// let encoded = serde_json::from_str(r#"{"count":{"__burr_serde__":"global_counter","count":3}}"#)?;
    /// assert_eq!(WorkflowState::deserialize(encoded)?.values()["count"], 3);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn global() -> &'static Self {
        static REGISTRY: OnceLock<StateSerialization> = OnceLock::new();
        REGISTRY.get_or_init(|| Self {
            registry: RwLock::new(Arc::new(SerializationRegistry::default())),
        })
    }

    /// Registers or replaces both transformations for one exact top-level field.
    ///
    /// Encoders must return objects; decoders may return any valid JSON value.
    /// Callback errors retain their Display text plus field and direction context.
    /// Neither callback invocation nor destruction of replaced captures holds
    /// the registry lock. An in-flight operation keeps its original callbacks.
    ///
    /// ```
    /// use app_sdk::workflow::{StateSerialization, WorkflowState};
    /// use serde_json::{json, Map, Value};
    /// StateSerialization::global().register_field(
    /// "distance_meters".into(),
    /// |value| Ok::<_, String>(json!({"meters": value})),
    /// |value| value.get("meters").cloned().ok_or("missing meters"),
    /// )?;
    /// let state = WorkflowState::new(Map::from_iter([
    /// ("distance_meters".into(), Value::from(12)),
    /// ]))?;
    /// let encoded = state.serialize()?;
    /// assert_eq!(encoded["distance_meters"], json!({"meters": 12}));
    /// assert_eq!(WorkflowState::deserialize(encoded)?, state);
    /// # Ok::<(), app_sdk::AppError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `Integration` if the registry lock is poisoned. Callback results are
    /// validated when encoding/decoding, not during registration; exact field names
    /// replace prior registrations for future operations.
    pub fn register_field<S, D, SE, DE>(
        &self,
        field: String,
        serialize: S,
        deserialize: D,
    ) -> Result<(), AppError>
    where
        S: Fn(&Value) -> Result<Value, SE> + Send + Sync + 'static,
        D: Fn(&Value) -> Result<Value, DE> + Send + Sync + 'static,
        SE: Display,
        DE: Display,
    {
        let encoding_field = field.clone();
        let decoding_field = field.clone();
        let codec = FieldCodec {
            serialize: Arc::new(move |value: &Value| {
                serialize(value).map_err(|error| {
                    AppError::InvalidInput(format!(
                        "field serialize handler for '{encoding_field}' failed: {error}"
                    ))
                })
            }),
            deserialize: Arc::new(move |value: &Value| {
                deserialize(value).map_err(|error| {
                    AppError::InvalidInput(format!(
                        "field deserialize handler for '{decoding_field}' failed: {error}"
                    ))
                })
            }),
        };
        let replaced = {
            let mut registry = self.registry.write().map_err(|_| {
                AppError::Integration("state serialization registry lock was poisoned".to_owned())
            })?;
            Arc::make_mut(&mut registry).fields.insert(field, codec)
        };
        drop(replaced);
        Ok(())
    }

    /// Registers or replaces a decoder for an exact `__burr_serde__` tag string.
    ///
    /// Empty strings are valid names. The decoder receives the complete original
    /// tagged object. Its output is terminal: it is not dispatched or recursively
    /// decoded again. Ordinary untagged containers are decoded recursively.
    ///
    /// ```
    /// use app_sdk::workflow::{StateSerialization, WorkflowState};
    /// use serde_json::{json, Map};
    /// StateSerialization::global().register_tag("distance".into(), |value| {
    /// value.get("meters").cloned().ok_or("missing meters")
    /// })?;
    /// let state = WorkflowState::deserialize(Map::from_iter([(
    /// "route".into(),
    /// json!([{"__burr_serde__": "distance", "meters": 12}]),
    /// )]))?;
    /// assert_eq!(state.values()["route"], json!([12]));
    /// # Ok::<(), app_sdk::AppError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `Integration` if the registry lock is poisoned. Decoder failures occur
    /// at decode time and preserve caller error text with tag context.
    pub fn register_tag<D, DE>(&self, tag: String, deserialize: D) -> Result<(), AppError>
    where
        D: Fn(&Value) -> Result<Value, DE> + Send + Sync + 'static,
        DE: Display,
    {
        let decoding_tag = tag.clone();
        let decoder = Arc::new(move |value: &Value| {
            deserialize(value).map_err(|error| {
                AppError::InvalidInput(format!(
                    "serde dispatch handler for '{decoding_tag}' failed: {error}"
                ))
            })
        });
        let replaced = {
            let mut registry = self.registry.write().map_err(|_| {
                AppError::Integration("state serialization registry lock was poisoned".to_owned())
            })?;
            Arc::make_mut(&mut registry).tags.insert(tag, decoder)
        };
        drop(replaced);
        Ok(())
    }

    fn snapshot(&self) -> Result<Arc<SerializationRegistry>, AppError> {
        self.registry
            .read()
            .map(|registry| Arc::clone(&registry))
            .map_err(|_| {
                AppError::Integration("state serialization registry lock was poisoned".to_owned())
            })
    }
}

impl WorkflowState {
    /// Encodes ordered state using one snapshot of the global field registry.
    ///
    /// Unregistered fields keep their JSON values, including tagged objects.
    /// Field encoders must produce objects. The complete encoded state retains
    /// the ordinary JSON byte/depth bounds; failure leaves this state unchanged.
    ///
    /// ```
    /// use app_sdk::workflow::WorkflowState;
    /// use serde_json::{Map, Value};
    /// let values = Map::from_iter([("count".into(), Value::from(2))]);
    /// let state = WorkflowState::new(values.clone())?;
    /// assert_eq!(state.serialize()?, values);
    /// # Ok::<(), app_sdk::AppError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for a codec error, a field encoder returning a nonobject,
    /// or exceeded JSON bounds; `Integration` reports registry lock poisoning.
    ///
    /// # Panics
    ///
    /// Panics if a registered field encoder panics. The registry lock is released
    /// before callbacks run.
    pub fn serialize(&self) -> Result<Map<String, Value>, AppError> {
        StateSerialization::global()
            .snapshot()?
            .encode(self.values())
    }

    /// Decodes state with one snapshot of global field and tag registrations.
    ///
    /// Top-level field decoders take precedence over recursive tagged decoding.
    /// Missing and malformed tags fail; handler outputs are not redispatched.
    /// Returned state preserves key order and passes the JSON boundary checks.
    ///
    /// ```
    /// use app_sdk::workflow::WorkflowState;
    /// let values = serde_json::from_str(r#"{"count":2,"ready":true}"#)?;
    /// let state = WorkflowState::deserialize(values)?;
    /// assert_eq!(state.values()["count"], 2);
    /// assert_eq!(state.values()["ready"], true);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for malformed state, a nonstring or unregistered tag,
    /// a decoder error, or invalid transformed values. Registry lock poisoning is
    /// `Integration`; field/tag decoder outputs are not recursively redispatched.
    ///
    /// # Panics
    ///
    /// Panics if a registered field or tag decoder panics. The registry lock is
    /// released before callbacks run.
    pub fn deserialize(values: Map<String, Value>) -> Result<Self, AppError> {
        JsonBoundary::validate_map(&values)?;
        let values = StateSerialization::global()
            .snapshot()?
            .decode_state(values)?;
        Self::new(values)
    }
}

impl SerializationRegistry {
    fn encode(&self, state: &Map<String, Value>) -> Result<Map<String, Value>, AppError> {
        let mut output = Map::with_capacity(state.len());
        for (field, value) in state {
            let value = match self.fields.get(field) {
                Some(codec) => {
                    let value = codec.serialize.transform(value)?;
                    if !value.is_object() {
                        return Err(AppError::InvalidInput(format!(
                            "field serialize handler for '{field}' must return an object, got {}",
                            Self::value_kind(&value)
                        )));
                    }
                    value
                }
                None => value.clone(),
            };
            output.insert(field.clone(), value);
        }
        JsonBoundary::validate_map(&output)?;
        Ok(output)
    }

    fn decode_state(&self, state: Map<String, Value>) -> Result<Map<String, Value>, AppError> {
        let mut output = Map::with_capacity(state.len());
        for (field, value) in state {
            let value = match self.fields.get(&field) {
                Some(codec) => codec.deserialize.transform(&value)?,
                None => self.decode_value(value)?,
            };
            output.insert(field, value);
        }
        Ok(output)
    }

    fn decode_value(&self, value: Value) -> Result<Value, AppError> {
        match value {
            Value::Array(values) => values
                .into_iter()
                .map(|value| self.decode_value(value))
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array),
            Value::Object(values) => {
                if let Some(tag) = values.get("__burr_serde__") {
                    let name = tag.as_str().ok_or_else(|| {
                        AppError::InvalidInput(format!(
                            "dispatch key '__burr_serde__' must be a string, got {}",
                            Self::value_kind(tag)
                        ))
                    })?;
                    let decoder = self.tags.get(name).ok_or_else(|| {
                        AppError::InvalidInput(format!("no function registered for key: {name}"))
                    })?;
                    return decoder.transform(&Value::Object(values));
                }
                let mut output = Map::with_capacity(values.len());
                for (key, value) in values {
                    output.insert(key, self.decode_value(value)?);
                }
                Ok(Value::Object(output))
            }
            value => Ok(value),
        }
    }

    fn value_kind(value: &Value) -> &'static str {
        match value {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Number(number) if number.is_i64() => "i64",
            Value::Number(number) if number.is_u64() => "u64",
            Value::Number(_) => "f64",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        }
    }
}
