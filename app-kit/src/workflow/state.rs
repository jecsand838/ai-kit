use std::collections::BTreeSet;

use serde_json::{Map, Number, Value};

use crate::AppError;
use crate::workflow::StateChange;
use crate::workflow::definition::MAX_COLLECTION_ITEMS;

impl StateChange {
    pub(super) fn apply_values(
        &self,
        before: &Map<String, Value>,
    ) -> Result<Map<String, Value>, AppError> {
        let mut values = if matches!(self, Self::Subset { .. }) {
            Map::new()
        } else {
            before.clone()
        };
        match self {
            Self::Assign { values: entries } => {
                Self::validate_count(entries.len())?;
                for (key, value) in entries {
                    values.insert(key.clone(), value.clone());
                }
            }
            Self::Append { values: entries } => {
                Self::validate_count(entries.len())?;
                for (key, value) in entries {
                    Self::array(&mut values, key)?.push(value.clone());
                }
            }
            Self::Extend { values: entries } => {
                Self::validate_count(entries.len())?;
                for (key, items) in entries {
                    Self::array(&mut values, key)?.extend(items.iter().cloned());
                }
            }
            Self::Increment { values: entries } => {
                Self::validate_count(entries.len())?;
                for (key, value) in entries {
                    let delta = value.as_i64().ok_or_else(|| {
                        AppError::InvalidInput(
                            "state increment requires a signed integer delta".to_owned(),
                        )
                    })?;
                    let prior = match values.get(key) {
                        None => 0,
                        Some(Value::Number(number)) => number.as_i128().ok_or_else(|| {
                            AppError::InvalidInput(
                                "state increment requires an integer value".to_owned(),
                            )
                        })?,
                        Some(_) => {
                            return Err(AppError::InvalidInput(
                                "state increment requires an integer value".to_owned(),
                            ));
                        }
                    };
                    let result = prior
                        .checked_add(i128::from(delta))
                        .and_then(Number::from_i128)
                        .ok_or_else(|| {
                            AppError::InvalidInput(
                                "state increment exceeded the JSON integer range".to_owned(),
                            )
                        })?;
                    values.insert(key.clone(), Value::Number(result));
                }
            }
            Self::Remove { keys } => {
                Self::validate_count(keys.len())?;
                let keys = keys.iter().map(String::as_str).collect::<BTreeSet<_>>();
                values.retain(|key, _| !keys.contains(key.as_str()));
            }
            Self::Wipe { .. } => {
                return Err(AppError::Integration(
                    "wipe must resolve to a removal before mutation".to_owned(),
                ));
            }
            Self::Merge { state } => {
                for (key, value) in state.values() {
                    values.insert(key.clone(), value.clone());
                }
            }
            Self::Subset { keys } => {
                Self::validate_count(keys.len())?;
                for key in keys {
                    if let Some(value) = before.get(key) {
                        values.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        Ok(values)
    }

    fn array<'a>(
        values: &'a mut Map<String, Value>,
        key: &str,
    ) -> Result<&'a mut Vec<Value>, AppError> {
        values
            .entry(key.to_owned())
            .or_insert_with(|| Value::Array(Vec::new()))
            .as_array_mut()
            .ok_or_else(|| {
                AppError::InvalidInput("state array operation requires an array value".to_owned())
            })
    }

    pub(super) fn validate_count(count: usize) -> Result<(), AppError> {
        if count > MAX_COLLECTION_ITEMS {
            return Err(AppError::InvalidInput(
                "state operation exceeds its item bound".to_owned(),
            ));
        }
        Ok(())
    }
}
