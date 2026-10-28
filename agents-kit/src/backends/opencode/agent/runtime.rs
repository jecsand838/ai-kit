use crate::error::{DeadlineKind, ErrorDetail, ErrorPhase};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use tokio::io::AsyncRead;
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::backends::opencode::agent::events::{
    OpenCodeEventHandle, OpenCodeEventPolicy, OpenCodeEvents,
};
use crate::backends::opencode::protocol::http::OpenCodeHttp;
use crate::backends::opencode::state_store::OpenCodeStateStore;
use crate::backends::opencode::state_store::records::OpenCodeAttachmentMode;
use crate::backends::opencode::tools::OpenCodeTools;
use crate::config::OpenCodeMode;
use crate::core::hierarchy::HierarchyRuntime;
use crate::{AgentId, KitError, OpenCodeConfig};
use storage_kit::StateStoreBackend;

pub(crate) struct OpenCodeRuntime {
    http: OpenCodeHttp,
    process: Option<OpenCodeProcess>,
    events: Option<OpenCodeEvents>,
}

struct OpenCodeProcess {
    child: Child,
    drains: Vec<JoinHandle<Result<(), std::io::Error>>>,
}

impl OpenCodeRuntime {
    pub(crate) fn attachment(
        config: &OpenCodeConfig,
        id: &AgentId,
    ) -> Result<OpenCodeAttachmentMode, KitError> {
        match config.mode() {
            OpenCodeMode::Spawn { .. } => Ok(OpenCodeAttachmentMode::Spawn {
                database: Self::database_name(id),
            }),
            OpenCodeMode::External { base_url, .. } => Ok(OpenCodeAttachmentMode::External {
                base_url: base_url.as_str().to_owned(),
            }),
        }
    }

    pub(crate) async fn connect(
        config: &OpenCodeConfig,
        id: &AgentId,
        lifecycle_deadline: Instant,
        lifecycle_timeout: Duration,
    ) -> Result<Self, KitError> {
        if Instant::now() >= lifecycle_deadline {
            return Err(
                KitError::timeout("construction deadline expired before client setup")
                    .at_phase(ErrorPhase::ClientSetup)
                    .with_detail(ErrorDetail::DeadlineElapsed {
                        kind: DeadlineKind::Lifecycle,
                        timeout: Some(lifecycle_timeout),
                    }),
            );
        }
        let setup_config = config.clone();
        // The retained builder task owns this join across caller loss. Started
        // synchronous setup cannot be cancelled, so settle it before checking
        // whether its result may authorize another construction stage.
        let http = match tokio::task::spawn_blocking(move || {
            if Instant::now() >= lifecycle_deadline {
                None
            } else {
                Some(OpenCodeHttp::from_config(&setup_config))
            }
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Some(Err(KitError::integration(
                "HTTP client setup task terminated",
            )
            .at_phase(ErrorPhase::ClientSetup)
            .with_detail(ErrorDetail::TaskTerminated))),
        };
        let http = match http {
            Some(result) if Instant::now() < lifecycle_deadline => result?,
            late => {
                let error = KitError::timeout("client setup exceeded the construction deadline")
                    .at_phase(ErrorPhase::ClientSetup)
                    .with_detail(ErrorDetail::DeadlineElapsed {
                        kind: DeadlineKind::Lifecycle,
                        timeout: Some(lifecycle_timeout),
                    });
                return Err(match late {
                    Some(Err(setup)) => error.with_supplementary(setup),
                    Some(Ok(_)) | None => error,
                });
            }
        };
        let mut runtime = Self {
            http,
            process: None,
            events: None,
        };
        match config.mode() {
            OpenCodeMode::Spawn {
                executable,
                arguments,
                address,
                startup_timeout,
                ..
            } => {
                let database = Self::database_name(id);
                runtime.process =
                    Some(OpenCodeProcess::spawn(executable, arguments, *address, &database).await?);
                let startup_deadline = Instant::now()
                    .checked_add(*startup_timeout)
                    .map_or(lifecycle_deadline, |deadline| {
                        deadline.min(lifecycle_deadline)
                    });
                if let Err(error) = runtime.http.wait_until_healthy(startup_deadline).await {
                    let cleanup = runtime.shutdown(Instant::now() + lifecycle_timeout).await;
                    return Err(match cleanup {
                        Ok(()) => error,
                        Err(cleanup) => {
                            error.with_supplementary(cleanup.at_phase(ErrorPhase::Cleanup))
                        }
                    });
                }
                if let Err(error) = runtime.http.verify_capabilities(startup_deadline).await {
                    let cleanup = runtime.shutdown(Instant::now() + lifecycle_timeout).await;
                    return Err(match cleanup {
                        Ok(()) => error,
                        Err(cleanup) => {
                            error.with_supplementary(cleanup.at_phase(ErrorPhase::Cleanup))
                        }
                    });
                }
            }
            OpenCodeMode::External { .. } => runtime.http.verify_server(lifecycle_deadline).await?,
        }
        Ok(runtime)
    }

    pub(crate) fn http(&self) -> &OpenCodeHttp {
        &self.http
    }

    pub(crate) fn events(&self) -> Result<OpenCodeEventHandle, KitError> {
        self.events
            .as_ref()
            .map(OpenCodeEvents::handle)
            .ok_or_else(|| KitError::closed("event delivery is not running"))
    }

    pub(super) async fn start_events<S: StateStoreBackend>(
        &mut self,
        agent_id: AgentId,
        state: OpenCodeStateStore<S>,
        hierarchy: HierarchyRuntime,
        tools: OpenCodeTools,
        policy: OpenCodeEventPolicy,
    ) -> Result<(), KitError> {
        if self.events.is_some() {
            return Err(KitError::invalid_state("event delivery is already running"));
        }
        let (events, readiness) =
            OpenCodeEvents::start(self.http.clone(), state, hierarchy, agent_id, tools, policy)
                .await;
        self.events = Some(events);
        readiness
    }

    pub(crate) async fn shutdown(mut self, deadline: Instant) -> Result<(), KitError> {
        let events = self.events.take();
        let process = self.process.take();
        let (event_result, process_result) = tokio::join!(
            async move {
                match events {
                    Some(events) => events.shutdown(deadline).await,
                    None => Ok(()),
                }
            },
            async move {
                match process {
                    Some(process) => process.shutdown(deadline).await,
                    None => Ok(()),
                }
            },
        );
        match (event_result, process_result) {
            (Err(primary), Err(secondary)) => Err(primary.with_supplementary(secondary)),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
        .map_err(|error| error.in_phase(ErrorPhase::Cleanup))
    }

    fn database_name(id: &AgentId) -> PathBuf {
        PathBuf::from(format!("agents-kit-opencode-{}.db", id.as_str()))
    }
}

impl OpenCodeProcess {
    async fn spawn(
        executable: &std::path::Path,
        arguments: &[std::ffi::OsString],
        address: std::net::SocketAddr,
        database: &std::path::Path,
    ) -> Result<Self, KitError> {
        let mut command = Command::new(executable);
        command
            .arg("serve")
            .args(arguments)
            .arg("--hostname")
            .arg(address.ip().to_string())
            .arg("--port")
            .arg(address.port().to_string())
            .env("OPENCODE_AUTO_SHARE", "false")
            .env("OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS", "false")
            .env("OPENCODE_DB", database)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| {
            KitError::integration("server process could not start")
                .at_phase(ErrorPhase::ProcessStart)
                .with_cause(KitError::from(error).at_phase(ErrorPhase::ProcessStart))
        })?;
        let mut drains = Vec::new();
        if let Some(stdout) = child.stdout.take() {
            drains.push(tokio::spawn(Self::drain(stdout)));
        }
        if let Some(stderr) = child.stderr.take() {
            drains.push(tokio::spawn(Self::drain(stderr)));
        }
        Ok(Self { child, drains })
    }

    async fn shutdown(mut self, deadline: Instant) -> Result<(), KitError> {
        let now = Instant::now();
        let graceful_deadline =
            now + (deadline.saturating_duration_since(now) / 2).min(Duration::from_secs(1));
        let mut cleanup_error = None;
        let escalate = match tokio::time::timeout_at(graceful_deadline, self.child.wait()).await {
            Ok(Ok(_)) => false,
            Ok(Err(error)) => {
                Self::retain_cleanup_failure(&mut cleanup_error, KitError::from(error));
                true
            }
            Err(_) => true,
        };
        if escalate {
            if let Err(error) = self.child.start_kill()
                && !matches!(self.child.try_wait(), Ok(Some(_)))
            {
                Self::retain_cleanup_failure(&mut cleanup_error, KitError::from(error));
            }
            match tokio::time::timeout_at(deadline, self.child.wait()).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    Self::retain_cleanup_failure(&mut cleanup_error, KitError::from(error));
                }
                Err(_) => {
                    Self::retain_cleanup_failure(
                        &mut cleanup_error,
                        KitError::timeout("process termination timed out"),
                    );
                }
            }
        }
        for mut drain in self.drains {
            match tokio::time::timeout_at(deadline, &mut drain).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    Self::retain_cleanup_failure(&mut cleanup_error, KitError::from(error));
                }
                Ok(Err(_)) => {
                    Self::retain_cleanup_failure(
                        &mut cleanup_error,
                        KitError::integration("output drain task failed")
                            .with_detail(ErrorDetail::TaskTerminated),
                    );
                }
                Err(_) => {
                    drain.abort();
                    let _ = drain.await;
                    Self::retain_cleanup_failure(
                        &mut cleanup_error,
                        KitError::timeout("output drain timed out"),
                    );
                }
            }
        }
        match cleanup_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn retain_cleanup_failure(first: &mut Option<KitError>, error: KitError) {
        let error = error.in_phase(ErrorPhase::Cleanup);
        *first = Some(match first.take() {
            Some(primary) => primary.with_supplementary(error),
            None => error,
        });
    }

    async fn drain(mut source: impl AsyncRead + Unpin) -> Result<(), std::io::Error> {
        tokio::io::copy(&mut source, &mut tokio::io::sink())
            .await
            .map(|_| ())
    }
}
