use std::net::SocketAddr;
use std::time::Duration;

use crate::SdkError;
use crate::error::{DeadlineKind, ErrorDetail, ErrorPhase};

use super::jsonrpc::{CodexRuntimeBuilder, CodexTransportRuntime};

pub(crate) struct CodexTcp;

impl CodexTcp {
    pub(crate) async fn connect(
        address: SocketAddr,
        timeout: Duration,
    ) -> Result<CodexTransportRuntime, SdkError> {
        let endpoint = format!("ws://{address}/");
        let (stream, _) = tokio::time::timeout(
            timeout,
            tokio_tungstenite::connect_async_with_config(
                endpoint.as_str(),
                Some(CodexRuntimeBuilder::websocket_config()),
                false,
            ),
        )
        .await
        .map_err(|_| {
            SdkError::timeout("TCP connection deadline elapsed")
                .in_phase(ErrorPhase::ClientSetup)
                .with_detail(ErrorDetail::DeadlineElapsed {
                    kind: DeadlineKind::Lifecycle,
                    timeout: Some(timeout),
                })
        })?
        .map_err(|_| {
            SdkError::integration("TCP connection failed")
                .in_phase(ErrorPhase::ClientSetup)
                .with_detail(ErrorDetail::TransportFailure)
        })?;
        Ok(CodexRuntimeBuilder::websocket(endpoint, stream))
    }
}
