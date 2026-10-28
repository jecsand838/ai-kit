//! Private HTTP transport implementation used by provider adapters.
//!
//! The public transport surface stays in [`crate::config`], while request
//! execution details remain internal.
//! Provider clients own provider semantics such as responses_create,
//! messages_create, generate_content, chat, embed, files, and batches, then call
//! `ProviderHttpContext` / `CheckedRequestSpec` for reusable request execution.
//! `ProviderHttpContext` reuses a long-lived reqwest client instead of
//! constructing per-model/per-request clients.

mod context;
mod execution;
mod request;
mod response;

pub(crate) use context::{AuthMode, ProviderHttpContext};
pub(crate) use execution::RequestExecution;
pub(crate) use response::{CheckedRequestSpec, HttpResponse};
