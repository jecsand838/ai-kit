use http::{HeaderMap, HeaderName, HeaderValue};
use secrecy::ExposeSecret;

use crate::SdkError;
use crate::config::ApiKey;
use crate::error::ValidationViolation;

#[derive(Clone)]
pub(crate) enum AuthScheme {
    None,
    Bearer(ApiKey),
    Header {
        name: String,
        value: ApiKey,
        prefix: Option<String>,
    },
}

impl AuthScheme {
    pub(crate) fn apply(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self {
            Self::None => builder,
            Self::Bearer(token) => builder.bearer_auth(token.as_secret().expose_secret()),
            Self::Header {
                name,
                value,
                prefix,
            } => {
                let rendered = match prefix {
                    Some(prefix) => format!("{prefix} {}", value.as_secret().expose_secret()),
                    None => value.as_secret().expose_secret().to_string(),
                };
                builder.header(name.as_str(), rendered)
            }
        }
    }

    pub(crate) fn apply_headers(&self, headers: &mut HeaderMap) -> Result<(), SdkError> {
        match self {
            Self::None => Ok(()),
            Self::Bearer(token) => insert_header(
                headers,
                "authorization",
                format!("Bearer {}", token.as_secret().expose_secret()),
            ),
            Self::Header {
                name,
                value,
                prefix,
            } => {
                let rendered = match prefix {
                    Some(prefix) => format!("{prefix} {}", value.as_secret().expose_secret()),
                    None => value.as_secret().expose_secret().to_string(),
                };
                insert_header(headers, name, rendered)
            }
        }
    }
}

fn insert_header(headers: &mut HeaderMap, name: &str, value: String) -> Result<(), SdkError> {
    let name = HeaderName::try_from(name).map_err(|err| {
        SdkError::config(
            ValidationViolation::InvalidFormat {
                field: "authentication.header.name",
                rule: "valid HTTP header name",
            },
            format!("invalid provider authentication header name `{name}`: {err}"),
        )
    })?;
    let value = HeaderValue::from_str(&value).map_err(|err| {
        SdkError::config(
            ValidationViolation::InvalidFormat {
                field: "authentication.header.value",
                rule: "valid HTTP header value",
            },
            format!("invalid provider authentication header value for `{name}`: {err}"),
        )
    })?;
    headers.insert(name, value);
    Ok(())
}
