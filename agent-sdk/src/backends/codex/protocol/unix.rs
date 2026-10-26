use std::path::Path;
use std::time::Duration;

use crate::SdkError;
use crate::error::{DeadlineKind, ErrorDetail, ErrorPhase};

use super::jsonrpc::{CodexRuntimeBuilder, CodexTransportRuntime};

pub(crate) struct CodexUnix;

impl CodexUnix {
    pub(crate) async fn connect(
        path: &Path,
        timeout: Duration,
    ) -> Result<CodexTransportRuntime, SdkError> {
        #[cfg(unix)]
        {
            use tokio_tungstenite::tungstenite::client::IntoClientRequest;

            let stream = tokio::time::timeout(timeout, tokio::net::UnixStream::connect(path))
                .await
                .map_err(|_| {
                    SdkError::timeout("Unix connection deadline elapsed")
                        .in_phase(ErrorPhase::ClientSetup)
                        .with_detail(ErrorDetail::DeadlineElapsed {
                            kind: DeadlineKind::Lifecycle,
                            timeout: Some(timeout),
                        })
                })?
                .map_err(|_| {
                    SdkError::integration("Unix connection failed")
                        .in_phase(ErrorPhase::ClientSetup)
                        .with_detail(ErrorDetail::TransportFailure)
                })?;
            let request = "ws://localhost/rpc"
                .into_client_request()
                .map_err(|_| SdkError::protocol("Unix handshake URI is invalid"))?;
            let (stream, _) = tokio::time::timeout(
                timeout,
                tokio_tungstenite::client_async_with_config(
                    request,
                    stream,
                    Some(CodexRuntimeBuilder::websocket_config()),
                ),
            )
            .await
            .map_err(|_| {
                SdkError::timeout("Unix handshake deadline elapsed")
                    .in_phase(ErrorPhase::ClientSetup)
                    .with_detail(ErrorDetail::DeadlineElapsed {
                        kind: DeadlineKind::Lifecycle,
                        timeout: Some(timeout),
                    })
            })?
            .map_err(|_| {
                SdkError::integration("Unix handshake failed")
                    .in_phase(ErrorPhase::ClientSetup)
                    .with_detail(ErrorDetail::TransportFailure)
            })?;
            Ok(CodexRuntimeBuilder::websocket(
                format!("unix://{}", path.display()),
                stream,
            ))
        }

        #[cfg(windows)]
        {
            let _ = (path, timeout);
            Err(SdkError::unsupported(
                "Unix socket attachment is unsupported on Windows",
            ))
        }
    }
}
