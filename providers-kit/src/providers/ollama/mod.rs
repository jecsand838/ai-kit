mod api;
mod backend;
mod chat;
mod client;
mod embeddings;
mod models;
mod native;
mod streaming;
mod wire;

pub(crate) use backend::OllamaBackend;
pub(crate) use models::OllamaModel;
