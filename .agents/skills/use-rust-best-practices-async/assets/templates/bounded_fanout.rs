use futures::{stream, StreamExt, TryStreamExt};

pub async fn fetch_all(
    client: ApiClient,
    ids: Vec<u64>,
) -> Result<Vec<Record>, Error> {
    stream::iter(ids)
        .map(|id| {
            let client = client.clone();
            async move { client.fetch_record(id).await }
        })
        .buffer_unordered(16)
        .try_collect()
        .await
}

#[derive(Clone)]
pub struct ApiClient;

#[derive(Debug)]
pub struct Record;

#[derive(Debug, thiserror::Error)]
#[error("api failure")]
pub struct Error;

impl ApiClient {
    pub async fn fetch_record(&self, _id: u64) -> Result<Record, Error> {
        Ok(Record)
    }
}
