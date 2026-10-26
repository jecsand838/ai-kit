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
