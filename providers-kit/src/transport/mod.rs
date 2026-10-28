//! Shared transport configuration and retry helpers for provider backends.
//!
//! Callers interact with [`TransportConfig`](crate::config::TransportConfig) and
//! [`RetryPolicy`](crate::config::RetryPolicy); the HTTP and SSE machinery stays
//! private to the crate.

mod auth;
mod http;
mod sse;

pub(crate) use auth::AuthScheme;
pub(crate) use http::{
    AuthMode, CheckedRequestSpec, HttpResponse, ProviderHttpContext, RequestExecution,
};
pub(crate) use sse::{SseEvent, SseParser};
