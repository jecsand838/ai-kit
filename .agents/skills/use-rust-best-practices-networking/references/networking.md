# Rust networking best practices

Use this reference when designing HTTP/gRPC clients, network error handling, retries, backpressure, or database pooling.

## 1. Reuse clients, channels, and pools

### HTTP / REST

Create a `reqwest::Client` once and reuse it.

### Good

```rust
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
```

### Bad

```rust
pub async fn bad_fetch(base_url: &str, id: u64) -> Result<String, reqwest::Error> {
    let client = reqwest::Client::new();
    client
        .get(format!("{base_url}/orders/{id}"))
        .send()
        .await?
        .text()
        .await
}
```

Do not build a new client per request unless the task explicitly needs a distinct configuration and the call frequency is tiny.

## 2. Separate transport DTOs from domain types

Transport models often differ from domain models.

### Good

```rust
#[derive(Debug, serde::Deserialize)]
pub struct OrderDto {
    pub id: u64,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderStatus {
    Pending,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Order {
    pub id: u64,
    pub status: OrderStatus,
}

impl TryFrom<OrderDto> for Order {
    type Error = OrderMappingError;

    fn try_from(dto: OrderDto) -> Result<Self, Self::Error> {
        let status = match dto.status.as_str() {
            "pending" => OrderStatus::Pending,
            "complete" => OrderStatus::Complete,
            other => return Err(OrderMappingError::UnknownStatus(other.to_owned())),
        };

        Ok(Self { id: dto.id, status })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OrderMappingError {
    #[error("unknown order status: {0}")]
    UnknownStatus(String),
}
```

This keeps transport churn from leaking into domain logic.

## 3. Reuse tonic channels and client clones

### Good

```rust
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

#[derive(Clone)]
pub struct PaymentsGrpc {
    inner: payments::payments_client::PaymentsClient<Channel>,
}

impl PaymentsGrpc {
    pub fn new(endpoint: String) -> Result<Self, tonic::transport::Error> {
        let endpoint = Endpoint::from_shared(endpoint)?
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(10))
            .tcp_nodelay(true);

        let channel = endpoint.connect_lazy();

        Ok(Self {
            inner: payments::payments_client::PaymentsClient::new(channel),
        })
    }

    pub async fn capture(
        &self,
        request: payments::CaptureRequest,
    ) -> Result<payments::CaptureResponse, tonic::Status> {
        let mut client = self.inner.clone();
        let response = client.capture(request).await?;
        Ok(response.into_inner())
    }
}

mod payments {
    #[derive(Debug, Clone)]
    pub struct CaptureRequest;

    #[derive(Debug, Clone)]
    pub struct CaptureResponse;

    pub mod payments_client {
        use super::{CaptureRequest, CaptureResponse};
        use tonic::{transport::Channel, Response, Status};

        #[derive(Clone)]
        pub struct PaymentsClient<T> {
            _inner: T,
        }

        impl PaymentsClient<Channel> {
            pub fn new(channel: Channel) -> Self {
                Self { _inner: channel }
            }

            pub async fn capture(
                &mut self,
                _request: CaptureRequest,
            ) -> Result<Response<CaptureResponse>, Status> {
                Ok(Response::new(CaptureResponse))
            }
        }
    }
}
```

### Avoid

- reconnecting for every RPC
- wrapping a channel inside `Arc<Mutex<_>>`
- inventing a custom pool on top of tonic without a clear need

## 4. Use crate-native pools for databases

### Good: create one pool and share cheap clones

```rust
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;

pub async fn connect_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .min_connections(4)
        .max_connections(32)
        .acquire_timeout(Duration::from_secs(5))
        .connect(database_url)
        .await
}
```

`PgPool`, `MySqlPool`, and similar handles are meant to be created once and shared.

### Bad: open a new connection for every operation

```rust
pub async fn bad_insert(database_url: &str, email: &str) -> Result<(), sqlx::Error> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;

    sqlx::query("INSERT INTO users (email) VALUES ($1)")
        .bind(email)
        .execute(&pool)
        .await?;

    Ok(())
}
```

This recreates pool state for every call.

### Tune pool options for production load

Defaults are often fine for tests and light-duty apps, but production workloads usually need deliberate values for:

- `max_connections`
- `min_connections`
- `acquire_timeout`
- connection lifetime / idle time
- per-environment database limits

Use one pool per database / role unless a true isolation requirement says otherwise.

## 5. Retries must be safe and bounded

Retry only when the operation is safe to repeat.

### Usually safe to retry

- idempotent `GET`
- idempotent `PUT`
- reads against gRPC methods that are documented as safe to repeat
- transient connection establishment

### Usually unsafe to retry blindly

- payment capture
- order creation
- mutation endpoints without idempotency keys
- database writes after partial success is possible

### Good retry shape

```rust
use std::time::Duration;

pub async fn get_order_with_retry(
    client: &OrdersClient,
    id: u64,
) -> Result<OrderDto, OrdersClientError> {
    let mut delay = Duration::from_millis(100);

    for attempt in 0..3 {
        match client.get_order(id).await {
            Ok(order) => return Ok(order),
            Err(error) if attempt < 2 && error.is_retryable() => {
                tokio::time::sleep(delay).await;
                delay *= 2;
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("loop always returns");
}

impl OrdersClientError {
    pub fn is_retryable(&self) -> bool {
        match self {
            OrdersClientError::Transport(_) => true,
            OrdersClientError::HttpStatus { status, .. } => {
                status.is_server_error() || *status == StatusCode::TOO_MANY_REQUESTS
            }
        }
    }
}
```

Keep retry counts small and explicit. Add jitter in real production clients when many callers may retry together.

## 6. Set timeouts consciously

Use layered timeouts:

- connect timeout
- total request deadline
- per-request override when a specific call needs a different bound
- upstream/server deadlines when supported

```rust
pub async fn fetch_with_tighter_timeout(
    client: &OrdersClient,
    id: u64,
) -> Result<OrderDto, OrdersClientError> {
    let url = client
        .base_url
        .join(&format!("orders/{id}"))
        .map_err(|_| OrdersClientError::InvalidBaseUrl)?;

    let response = client
        .http
        .get(url)
        .timeout(std::time::Duration::from_secs(2))
        .send()
        .await?;

    if response.status().is_success() {
        Ok(response.json().await?)
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(OrdersClientError::HttpStatus { status, body })
    }
}
```

## 7. Apply backpressure and bounded concurrency

When issuing many network calls, cap concurrency.

```rust
use futures::{stream, StreamExt, TryStreamExt};

pub async fn fetch_many(
    client: OrdersClient,
    ids: Vec<u64>,
) -> Result<Vec<OrderDto>, OrdersClientError> {
    stream::iter(ids)
        .map(|id| {
            let client = client.clone();
            async move { client.get_order(id).await }
        })
        .buffer_unordered(16)
        .try_collect()
        .await
}
```

Unbounded fan-out can overload both your process and the upstream service.

## 8. Prefer custom pools only when the dependency lacks one

A custom pool or generic resource pool may be justified when:

- the driver has no built-in reuse story
- you are pooling something that is not a normal HTTP/gRPC/DB client
- you need strict resource partitioning not exposed by the client library

Even then:

- keep ownership simple
- define clear acquisition timeouts
- document fairness / queueing behavior
- prefer a small wrapper over an elaborate meta-pool architecture

## 9. Networking dos and don'ts

### Do

- reuse `reqwest::Client`
- reuse tonic `Channel` / clone cheap clients
- share one DB pool per DB role
- separate DTOs from domain types
- surface status/body context on network errors
- bound concurrency
- retry only when safe

### Don't

- create a client or pool for every call
- wrap cheap-clone handles in needless `Arc<Mutex<_>>`
- retry unsafe mutations blindly
- let transport models leak into core domain logic
- fan out unbounded requests
- bury timeouts and retry policy all over the codebase

## Further reading

- reqwest `Client`: <https://docs.rs/reqwest/latest/reqwest/struct.Client.html>
- tonic `Channel`: <https://docs.rs/tonic/latest/tonic/transport/struct.Channel.html>
- tonic client docs: <https://docs.rs/tonic/latest/tonic/client/index.html>
- tonic `Endpoint`: <https://docs.rs/tonic/latest/tonic/transport/channel/struct.Endpoint.html>
- sqlx `Pool`: <https://docs.rs/sqlx/latest/sqlx/struct.Pool.html>


## 10. Additional merged examples

### Bad: `Arc<Mutex<reqwest::Client>>`

```rust
#[derive(Clone)]
pub struct ApiClient {
    inner: std::sync::Arc<tokio::sync::Mutex<reqwest::Client>>,
}
```

This adds lock contention around a handle that is already designed to be cloned cheaply.

### Better: clone the cheap client handle directly

```rust
#[derive(Clone)]
pub struct ApiClient {
    inner: reqwest::Client,
}

impl ApiClient {
    pub fn new(inner: reqwest::Client) -> Self {
        Self { inner }
    }
}
```

### Bad: connect to gRPC on every request

```rust
pub async fn get_user(id: &str) -> Result<UserReply, tonic::Status> {
    let mut client = users::user_service_client::UserServiceClient::connect(
        "http://127.0.0.1:50051",
    )
    .await?;

    client
        .get_user(tonic::Request::new(GetUserRequest { id: id.into() }))
        .await
        .map(|response| response.into_inner())
}
```

### Better: build one channel, then clone clients cheaply

```rust
#[derive(Clone)]
pub struct UsersGrpc {
    channel: tonic::transport::Channel,
}

impl UsersGrpc {
    pub async fn connect(endpoint: &str) -> Result<Self, tonic::transport::Error> {
        let channel = tonic::transport::Endpoint::from_shared(endpoint.to_owned())?
            .connect()
            .await?;
        Ok(Self { channel })
    }

    pub async fn get_user(&self, id: String) -> Result<UserReply, tonic::Status> {
        let mut client = users::user_service_client::UserServiceClient::new(self.channel.clone());
        let response = client
            .get_user(tonic::Request::new(GetUserRequest { id }))
            .await?;
        Ok(response.into_inner())
    }
}
```

## Additional source links

- reqwest crate docs: <https://docs.rs/reqwest/latest/reqwest/>
- `reqwest::Client`: <https://docs.rs/reqwest/latest/reqwest/struct.Client.html>
- `reqwest::ClientBuilder`: <https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html>
- tonic crate docs: <https://docs.rs/tonic>
- tonic `Channel`: <https://docs.rs/tonic/latest/tonic/transport/struct.Channel.html>
- tonic `Endpoint`: <https://docs.rs/tonic/latest/tonic/transport/channel/struct.Endpoint.html>
- tonic client docs: <https://docs.rs/tonic/latest/tonic/client/>
- sqlx crate docs: <https://docs.rs/sqlx/latest/sqlx/>
- `sqlx::postgres::PgPoolOptions`: <https://docs.rs/sqlx/latest/sqlx/postgres/type.PgPoolOptions.html>
- `sqlx::PgPool`: <https://docs.rs/sqlx/latest/sqlx/type.PgPool.html>
- deadpool crate docs: <https://docs.rs/deadpool>
- deadpool managed pools: <https://docs.rs/deadpool/latest/deadpool/managed/>
