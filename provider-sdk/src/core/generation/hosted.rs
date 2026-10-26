use serde_json::{Map, Value};
use url::Url;

use crate::SdkError;
use crate::error::{JsonValueKind, NativeErrorDetails, ResponseDetails, ResponseViolation};
use crate::generation::{Artifact, FileReference};
use crate::transport::CheckedRequestSpec;

/// Provider-private cursor for validating one hosted-output JSON value.
pub(crate) struct HostedJsonCursor<'a> {
    context: String,
    body: HostedJsonBody<'a>,
    value: HostedJsonValue<'a>,
}

enum HostedJsonBody<'a> {
    Owned(Box<NativeErrorDetails>),
    Object(&'a Map<String, Value>),
}

enum HostedJsonValue<'a> {
    Value(&'a Value),
    Object(&'a Map<String, Value>),
}

impl<'a> HostedJsonCursor<'a> {
    pub(crate) fn new(
        context: impl Into<String>,
        body: NativeErrorDetails,
        value: &'a Value,
    ) -> Self {
        Self {
            context: context.into(),
            body: HostedJsonBody::Owned(Box::new(body)),
            value: HostedJsonValue::Value(value),
        }
    }

    pub(crate) fn for_object(
        context: impl Into<String>,
        body: NativeErrorDetails,
        object: &'a Map<String, Value>,
    ) -> Self {
        Self {
            context: context.into(),
            body: HostedJsonBody::Owned(Box::new(body)),
            value: HostedJsonValue::Object(object),
        }
    }

    pub(crate) fn for_borrowed_object(
        context: impl Into<String>,
        object: &'a Map<String, Value>,
    ) -> Self {
        Self {
            context: context.into(),
            body: HostedJsonBody::Object(object),
            value: HostedJsonValue::Object(object),
        }
    }

    pub(crate) fn object(&self) -> Result<&'a Map<String, Value>, SdkError> {
        match self.value {
            HostedJsonValue::Object(object) => Ok(object),
            HostedJsonValue::Value(value) => value.as_object().ok_or_else(|| {
                self.invalid(
                    ResponseViolation::InvalidField {
                        field: "hosted_output",
                    },
                    "must be an object",
                )
                .with_response_details(ResponseDetails::FieldType {
                    expected: JsonValueKind::Object,
                    actual: JsonValueKind::of(Some(value)),
                })
            }),
        }
    }

    pub(crate) fn optional_array_field(
        &self,
        field: &'static str,
    ) -> Result<Option<&'a [Value]>, SdkError> {
        self.object()?
            .get(field)
            .map(|value| {
                value.as_array().map(Vec::as_slice).ok_or_else(|| {
                    self.invalid(
                        ResponseViolation::InvalidField { field },
                        format!(".{field} must be an array"),
                    )
                    .with_response_details(ResponseDetails::FieldType {
                        expected: JsonValueKind::Array,
                        actual: JsonValueKind::of(Some(value)),
                    })
                })
            })
            .transpose()
    }

    pub(crate) fn required_str_field(&self, field: &'static str) -> Result<&'a str, SdkError> {
        match self.object()?.get(field) {
            Some(Value::String(value)) => Ok(value),
            Some(value) => Err(self
                .invalid(
                    ResponseViolation::InvalidField { field },
                    format!("{field} must be a string"),
                )
                .with_response_details(ResponseDetails::FieldType {
                    expected: JsonValueKind::String,
                    actual: JsonValueKind::of(Some(value)),
                })),
            None => Err(self
                .invalid(
                    ResponseViolation::MissingField { field },
                    format!("did not include {field}"),
                )
                .with_response_details(ResponseDetails::FieldType {
                    expected: JsonValueKind::String,
                    actual: JsonValueKind::Absent,
                })),
        }
    }

    pub(crate) fn optional_str_field(
        &self,
        field: &'static str,
    ) -> Result<Option<String>, SdkError> {
        match self.object()?.get(field) {
            Some(Value::String(value)) => Ok(Some(value.clone())),
            Some(Value::Null) | None => Ok(None),
            Some(value) => Err(self
                .invalid(
                    ResponseViolation::InvalidField { field },
                    format!(".{field} must be a string"),
                )
                .with_response_details(ResponseDetails::FieldType {
                    expected: JsonValueKind::String,
                    actual: JsonValueKind::of(Some(value)),
                })),
        }
    }

    pub(crate) fn required_string_field(&self, field: &'static str) -> Result<String, SdkError> {
        self.required_str_field(field).map(ToOwned::to_owned)
    }

    pub(crate) fn optional_number_field(
        &self,
        field: &'static str,
    ) -> Result<Option<f64>, SdkError> {
        match self.object()?.get(field) {
            Some(Value::Number(value)) => value
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or_else(|| {
                    self.invalid(
                        ResponseViolation::InvalidField { field },
                        format!(".{field} must be a finite number"),
                    )
                })
                .map(Some),
            Some(Value::Null) | None => Ok(None),
            Some(value) => Err(self
                .invalid(
                    ResponseViolation::InvalidField { field },
                    format!(".{field} must be a number"),
                )
                .with_response_details(ResponseDetails::FieldType {
                    expected: JsonValueKind::Number,
                    actual: JsonValueKind::of(Some(value)),
                })),
        }
    }

    pub(crate) fn optional_i64_field(&self, field: &'static str) -> Result<Option<i64>, SdkError> {
        match self.object()?.get(field) {
            Some(Value::Number(value)) => value
                .as_i64()
                .ok_or_else(|| {
                    self.invalid(
                        ResponseViolation::InvalidField { field },
                        format!(".{field} must be an integer"),
                    )
                })
                .map(Some),
            Some(Value::Null) | None => Ok(None),
            Some(value) => Err(self
                .invalid(
                    ResponseViolation::InvalidField { field },
                    format!(".{field} must be an integer"),
                )
                .with_response_details(ResponseDetails::FieldType {
                    expected: JsonValueKind::Number,
                    actual: JsonValueKind::of(Some(value)),
                })),
        }
    }

    pub(crate) fn string_array_field(&self, field: &'static str) -> Result<Vec<String>, SdkError> {
        let Some(values) = self.optional_array_field(field)? else {
            return Ok(Vec::new());
        };
        values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                value.as_str().map(ToOwned::to_owned).ok_or_else(|| {
                    self.invalid(
                        ResponseViolation::InvalidField { field },
                        format!(".{field}[{index}] must be a string"),
                    )
                    .with_response_details(ResponseDetails::FieldType {
                        expected: JsonValueKind::String,
                        actual: JsonValueKind::of(Some(value)),
                    })
                })
            })
            .collect()
    }

    pub(crate) fn objects_or_single(&self) -> Result<Vec<&'a Map<String, Value>>, SdkError> {
        match self.value {
            HostedJsonValue::Object(object) | HostedJsonValue::Value(Value::Object(object)) => {
                Ok(vec![object])
            }
            HostedJsonValue::Value(Value::Array(items)) => items
                .iter()
                .enumerate()
                .map(|(index, item)| {
                    item.as_object().ok_or_else(|| {
                        self.invalid(
                            ResponseViolation::InvalidField {
                                field: "hosted_output",
                            },
                            format!("[{index}] must be an object"),
                        )
                        .with_response_details(
                            ResponseDetails::FieldType {
                                expected: JsonValueKind::Object,
                                actual: JsonValueKind::of(Some(item)),
                            },
                        )
                    })
                })
                .collect(),
            _ => Err(self.invalid(
                ResponseViolation::InvalidField {
                    field: "hosted_output",
                },
                "must be an object or array",
            )),
        }
    }

    pub(crate) fn invalid(
        &self,
        reason: ResponseViolation,
        message: impl Into<String>,
    ) -> SdkError {
        let message = message.into();
        let separator = if message.starts_with(['[', '.']) {
            ""
        } else {
            " "
        };
        SdkError::invalid_response(reason)
            .with_message(format!("{}{separator}{message}", self.context))
            .with_diagnostic(self.body.render())
    }
}

impl HostedJsonBody<'_> {
    fn render(&self) -> Option<NativeErrorDetails> {
        match self {
            Self::Owned(body) => Some((**body).clone()),
            Self::Object(object) => {
                CheckedRequestSpec::diagnostic_json(&Value::Object((*object).clone()))
            }
        }
    }
}

pub(crate) fn hosted_url_artifact(
    url: &str,
    body: NativeErrorDetails,
) -> Result<Artifact, SdkError> {
    let reference = FileReference::Url {
        url: Url::parse(url).map_err(|err| {
            SdkError::invalid_response(ResponseViolation::InvalidField {
                field: "artifact.url",
            })
            .with_message(format!("hosted tool artifact URL was invalid: {err}"))
            .with_diagnostic(Some(body.clone()))
        })?,
        media_type: None,
    };
    reference.validate().map_err(|error| {
        SdkError::invalid_response(ResponseViolation::InvalidField {
            field: "artifact.url",
        })
        .with_message("hosted tool artifact URL was not a reusable HTTP(S) reference".to_string())
        .with_diagnostic(Some(body.clone()))
        .with_supplementary(error)
    })?;
    Ok(Artifact::File {
        reference,
        file_name: None,
        description: None,
    })
}
