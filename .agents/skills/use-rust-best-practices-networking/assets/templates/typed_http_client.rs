use reqwest::{header::InvalidHeaderValue, Client, StatusCode, Url};

#[derive(Clone)]
pub struct OrdersClient {
    base_url: Url,
    http: Client,
}

impl OrdersClient {
    pub fn new(base_url: Url, bearer_token: String) -> Result<Self, OrdersClientBuildError> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {bearer_token}").parse::<reqwest::header::HeaderValue>()?,
        );

        let http = Client::builder()
            .user_agent("my-app/1.0")
            .default_headers(headers)
            .connect_timeout(std::time::Duration::from_secs(3))
            .timeout(std::time::Duration::from_secs(10))
            .build()?;

        Ok(Self { base_url, http })
    }

    pub async fn get_order(&self, id: u64) -> Result<OrderDto, OrdersClientError> {
        let url = self
            .base_url
            .join(&format!("orders/{id}"))
            .map_err(|_| OrdersClientError::InvalidBaseUrl)?;
        let response = self.http.get(url).send().await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            Err(OrdersClientError::HttpStatus { status, body })
        }
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct OrderDto {
    pub id: u64,
    pub status: String,
}

#[derive(Debug, thiserror::Error)]
pub enum OrdersClientBuildError {
    #[error("invalid authorization header: {0}")]
    InvalidAuthorizationHeader(#[from] InvalidHeaderValue),
    #[error("failed to build http client: {0}")]
    Build(#[from] reqwest::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum OrdersClientError {
    #[error("orders client base_url must be a valid absolute base URL")]
    InvalidBaseUrl,
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("unexpected status {status}: {body}")]
    HttpStatus {
        status: StatusCode,
        body: String,
    },
}
