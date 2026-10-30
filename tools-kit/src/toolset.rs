use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

use crate::{
    ToolError, ToolHandler,
    declaration::ToolInfo,
    local::ToolRegistration,
    mcp::McpClientConfig,
    name::ToolName,
    sources::{local::LocalToolset, mcp::McpToolset},
};

/// Public implementation contract for one shared executable tool source.
/// Use this trait directly with concrete caller implementations and their exact
/// associated handlers. [`Toolset::from_backend`] is optional normalization when a
/// supplied Toolset is needed. Shared shutdown obligations apply to both forms.
///
/// # Examples
///
/// ```
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), tools_kit::ToolError> {
/// use tools_kit::{Toolset, ToolsetBackend};
/// let tools = Toolset::local(Vec::new())?;
/// assert!(tools.list_tools().await?.is_empty());
/// tools.shutdown().await?;
///
/// # Ok(())
/// # }
/// ```
///
/// # Implement an executable caller Toolset
///
/// This runnable caller implementation executes a bounded string echo without tasks,
/// networking, or asynchronous cleanup. It has one unique terminal handler per call,
/// validates its advertised input shape itself, and seals root/handler admission on
/// shared shutdown. More complex implementations must own accepted asynchronous work
/// independently of a shutdown waiter. Neither this Toolset nor its Handler is Clone.
///
/// Direct use keeps the concrete associated child. Moving the same Toolset into
/// [`Toolset::from_backend`] is optional; normalization does not supply missing
/// backend validation, cancellation, or lifecycle behavior.
///
/// ```
/// use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
/// use tools_kit::{ToolError, ToolHandlerBackend, Toolset, ToolsetBackend};
/// use tools_kit::declaration::{ToolInfo, ToolName};
/// use tools_kit::execution::{
///     ToolContent, ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult,
/// };
/// use tools_kit::schema::JsonSchema;
///
/// struct EchoTools { info: ToolInfo, closed: Arc<AtomicBool> }
/// struct EchoHandler { name: ToolName, closed: Arc<AtomicBool>, terminal: bool }
/// impl EchoTools {
///     fn check(&self) -> Result<(), ToolError> {
///         if self.closed.load(Ordering::SeqCst) {
///             return Err(ToolError::Closed { operation: "echo toolset".into() });
///         }
///         Ok(())
///     }
/// }
/// impl ToolsetBackend for EchoTools {
///     type Handler = EchoHandler;
///     async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
///         self.check()?;
///         Ok(vec![self.info.clone()])
///     }
///     async fn tool_handler(&self, name: ToolName) -> Result<EchoHandler, ToolError> {
///         self.check()?;
///         if name != self.info.name { return Err(ToolError::UnknownTool { name }); }
///         Ok(EchoHandler { name, closed: Arc::clone(&self.closed), terminal: false })
///     }
///     async fn shutdown(&self) -> Result<(), ToolError> {
///         // No await: this synchronous toolset owns no unfinished external work.
///         self.closed.store(true, Ordering::SeqCst);
///         Ok(())
///     }
/// }
/// impl ToolHandlerBackend for EchoHandler {
///     async fn step(&mut self, input: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
///         if self.closed.load(Ordering::SeqCst) {
///             return Err(ToolError::Closed { operation: "echo step".into() });
///         }
///         if self.terminal {
///             return Err(ToolError::Protocol { message: "handler already terminal".into() });
///         }
///         match input {
///             ToolHandlerInput::Invoke(invocation) => {
///                 let Some(text) = invocation.arguments.as_str().filter(|text| text.chars().count() <= 1024) else {
///                     return Err(ToolError::InvalidArguments {
///                         tool: self.name.clone(), message: "expected a string of at most 1024 characters".into(), issues: Vec::new(),
///                     });
///                 };
///                 self.terminal = true;
///                 Ok(ToolHandlerOutput::Completed(ToolResult::text(text)))
///             }
///             ToolHandlerInput::Cancel { reason } => {
///                 self.terminal = true;
///                 Err(ToolError::Cancelled { tool: self.name.clone(), reason })
///             }
///             ToolHandlerInput::Interaction(_) => Err(ToolError::Protocol {
///                 message: "this tool never requests interaction".into(),
///             }),
///         }
///     }
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), ToolError> {
/// let name = ToolName::new("echo")?;
/// let concrete = EchoTools {
///     info: ToolInfo::new(name.clone(), JsonSchema::strict(serde_json::json!({"type":"string","maxLength":1024}))),
///     closed: Arc::new(AtomicBool::new(false)),
/// };
/// let mut direct = concrete.tool_handler(name.clone()).await?;
/// let input = ToolInvocation {
///     call_id: Some("call-1".into()),
///     arguments: serde_json::json!("unchanged"),
/// };
/// let ToolHandlerOutput::Completed(result) = direct.step(ToolHandlerInput::Invoke(input.clone())).await?
///     else { panic!("echo does not interact") };
/// assert!(!result.is_error);
/// assert!(
///     matches!(result.content.as_slice(), [ToolContent::Text { text, .. }] if text == "unchanged")
/// );
/// assert!(direct.step(ToolHandlerInput::Invoke(input.clone())).await.is_err());
/// let tools = Toolset::from_backend(concrete); // Sole public erased ingress.
/// let mut normalized = tools.tool_handler(name).await?;
/// let observed = normalized.step(ToolHandlerInput::Invoke(input)).await;
/// let cleanup = tools.shutdown().await;
/// if let Err(error) = &cleanup {
///     eprintln!("Cleanup failed: {error}");
/// }
/// let ToolHandlerOutput::Completed(result) = observed? else { panic!("echo does not interact") };
/// assert!(
///     matches!(result.content.as_slice(), [ToolContent::Text { text, .. }] if text == "unchanged")
/// );
/// cleanup?;
/// assert!(tools.list_tools().await.is_err());
/// tools.shutdown().await?;
/// # Ok(())
/// # }
/// ```
///
pub trait ToolsetBackend: Send + Sync + 'static {
    /// Unique resumable handler produced by this tool source.
    type Handler: crate::ToolHandlerBackend;

    /// Lists the complete executable inventory.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::Closed`] after sealing, or the underlying inventory failure.
    ///
    /// See [`crate::ToolsetBackend`] for a complete example.
    ///
    fn list_tools(&self) -> impl Future<Output = Result<Vec<ToolInfo>, ToolError>> + Send + '_;

    /// Creates a unique handler for one selected tool name.
    ///
    /// # Errors
    ///
    /// Returns an unknown-name error when no declared tool matches, [`ToolError::Closed`] after sealing, or the underlying handler-construction failure.
    ///
    /// See [`crate::local::ToolRegistration`] for a complete example.
    ///
    fn tool_handler(
        &self,
        name: ToolName,
    ) -> impl Future<Output = Result<Self::Handler, ToolError>> + Send + '_;

    /// Seals admission and performs shared exactly-once cleanup.
    ///
    /// This contract applies to direct caller implementations as well as supplied
    /// handles. On first acceptance, retain and track the cleanup independently of
    /// this awaiting future. Dropping a shutdown waiter or the last public value
    /// must not abandon accepted cleanup. Repeated calls observe its terminal
    /// result; they must not start another cleanup. Listing and handler admission
    /// reject after sealing. A consumer may bound its wait with a deadline, so
    /// putting all cleanup solely inside this borrowed future is insufficient.
    ///
    /// # Errors
    ///
    /// Replays the shared cleanup error, including a cleanup timeout, to concurrent and later callers. A failed observation does not authorize abandoning accepted cleanup.
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), tools_kit::ToolError> {
    /// use tools_kit::{Toolset, ToolsetBackend};
    /// let tools = Toolset::local(Vec::new())?;
    /// let shared = tools.clone();
    /// tools.shutdown().await?;
    /// shared.shutdown().await?;
    /// assert!(shared.list_tools().await.is_err());
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn shutdown(&self) -> impl Future<Output = Result<(), ToolError>> + Send + '_;
}

/// Opaque cloneable Toolset over Local, MCP, or downstream behavior.
/// Clones share admission and cleanup. Dropping a clone alone does not initiate
/// asynchronous cleanup; explicitly await [`ToolsetBackend::shutdown`].
///
/// See [`crate::ToolsetBackend`] for a complete example.
///
#[derive(Clone)]
pub struct Toolset {
    runtime: Arc<ToolsetRuntime>,
}

struct ToolsetRuntime {
    backend: InnerToolset,
    lifecycle: ToolsetLifecycle,
}

enum InnerToolset {
    Local(LocalToolset),
    Mcp(McpToolset),
    Extension(Arc<dyn ErasedToolsetBackend>),
}

impl Toolset {
    /// Builds an in-process Toolset from complete registrations.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::DuplicateName`] for duplicate names; an empty inventory is valid.
    ///
    /// See [`crate::ToolsetBackend`] for a complete example.
    ///
    pub fn local(registrations: Vec<ToolRegistration>) -> Result<Self, ToolError> {
        Ok(Self::from_local(LocalToolset::build(registrations)?))
    }

    /// Connects one MCP client Toolset and discovers its inventory.
    /// This example requires a running MCP service at the configured endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError`] for invalid connection settings, process startup, transport or initialization failure, malformed inventory, or an elapsed request deadline.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), tools_kit::ToolError> {
    /// use tools_kit::mcp::{McpClientConfig, McpConnectionConfig};
    /// let connection = McpConnectionConfig::StreamableHttp {
    ///     endpoint: "http://127.0.0.1:8000/mcp".parse().unwrap(),
    ///     headers: Default::default(),
    /// };
    /// let config = McpClientConfig::new(connection);
    /// use tools_kit::{Toolset, ToolsetBackend};
    /// let tools = Toolset::mcp(config).await?;
    /// let inventory = tools.list_tools().await;
    /// let cleanup = tools.shutdown().await;
    /// if let Err(error) = &cleanup {
    ///     eprintln!("Cleanup failed: {error}");
    /// }
    /// let _ = inventory?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    pub async fn mcp(config: McpClientConfig) -> Result<Self, ToolError> {
        Ok(Self::from_mcp(McpToolset::connect(config).await?))
    }

    /// Wraps a downstream Toolset implementation.
    /// The associated handler is normalized privately. Concrete callers may instead
    /// retain the backend directly; there is no public child normalization constructor.
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), tools_kit::ToolError> {
    /// use std::sync::atomic::{AtomicBool, Ordering};
    /// use tools_kit::{ToolError, Toolset, ToolsetBackend, ToolHandlerBackend};
    /// use tools_kit::declaration::{ToolInfo, ToolName};
    /// use tools_kit::execution::{ToolHandlerInput, ToolHandlerOutput};
    /// struct EmptyInventory { closed: AtomicBool }
    /// enum NoHandler {}
    /// impl ToolHandlerBackend for NoHandler {
    ///     async fn step(&mut self, _: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
    ///         match *self {}
    ///     }
    /// }
    /// impl ToolsetBackend for EmptyInventory {
    ///     type Handler = NoHandler;
    ///     async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
    ///         if self.closed.load(Ordering::Acquire) {
    ///             return Err(ToolError::Closed { operation: "list_tools".into() });
    ///         }
    ///         Ok(Vec::new())
    ///     }
    ///     async fn tool_handler(&self, name: ToolName) -> Result<NoHandler, ToolError> {
    ///         if self.closed.load(Ordering::Acquire) {
    ///             return Err(ToolError::Closed { operation: "tool_handler".into() });
    ///         }
    ///         Err(ToolError::UnknownTool { name })
    ///     }
    ///     async fn shutdown(&self) -> Result<(), ToolError> {
    ///         self.closed.store(true, Ordering::Release);
    ///         Ok(()) // This empty implementation owns no asynchronous resources.
    ///     }
    /// }
    /// let concrete = EmptyInventory { closed: AtomicBool::new(false) };
    /// assert!(concrete.list_tools().await?.is_empty());
    /// let normalized = Toolset::from_backend(concrete);
    /// normalized.shutdown().await?;
    /// assert!(normalized.list_tools().await.is_err());
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    #[must_use]
    pub fn from_backend<B: ToolsetBackend>(backend: B) -> Self {
        Self {
            runtime: Arc::new(ToolsetRuntime::new(InnerToolset::Extension(Arc::new(
                backend,
            )))),
        }
    }

    fn from_local(toolset: LocalToolset) -> Self {
        Self {
            runtime: Arc::new(ToolsetRuntime::new(InnerToolset::Local(toolset))),
        }
    }

    fn from_mcp(toolset: McpToolset) -> Self {
        Self {
            runtime: Arc::new(ToolsetRuntime::new(InnerToolset::Mcp(toolset))),
        }
    }
}

impl std::fmt::Debug for Toolset {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Toolset").finish_non_exhaustive()
    }
}

trait ErasedToolsetBackend: Send + Sync {
    fn list_tools_erased(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ToolInfo>, ToolError>> + Send + '_>>;

    fn tool_handler_erased(
        &self,
        name: ToolName,
    ) -> Pin<Box<dyn Future<Output = Result<ToolHandler, ToolError>> + Send + '_>>;

    fn shutdown_erased(&self) -> Pin<Box<dyn Future<Output = Result<(), ToolError>> + Send + '_>>;
}

impl<B: ToolsetBackend> ErasedToolsetBackend for B {
    fn list_tools_erased(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ToolInfo>, ToolError>> + Send + '_>> {
        Box::pin(self.list_tools())
    }

    fn tool_handler_erased(
        &self,
        name: ToolName,
    ) -> Pin<Box<dyn Future<Output = Result<ToolHandler, ToolError>> + Send + '_>> {
        Box::pin(async move {
            self.tool_handler(name)
                .await
                .map(ToolHandler::from_extension)
        })
    }

    fn shutdown_erased(&self) -> Pin<Box<dyn Future<Output = Result<(), ToolError>> + Send + '_>> {
        Box::pin(self.shutdown())
    }
}

impl ToolsetBackend for InnerToolset {
    type Handler = ToolHandler;

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
        match self {
            Self::Local(toolset) => toolset.list_tools().await,
            Self::Mcp(toolset) => toolset.list_tools().await,
            Self::Extension(toolset) => toolset.list_tools_erased().await,
        }
    }

    async fn tool_handler(&self, name: ToolName) -> Result<Self::Handler, ToolError> {
        match self {
            Self::Local(toolset) => toolset
                .tool_handler(name)
                .await
                .map(ToolHandler::from_local),
            Self::Mcp(toolset) => toolset.tool_handler(name).await.map(ToolHandler::from_mcp),
            Self::Extension(toolset) => toolset.tool_handler_erased(name).await,
        }
    }

    async fn shutdown(&self) -> Result<(), ToolError> {
        match self {
            Self::Local(toolset) => toolset.shutdown().await,
            Self::Mcp(toolset) => toolset.shutdown().await,
            Self::Extension(toolset) => toolset.shutdown_erased().await,
        }
    }
}

impl ToolsetBackend for Toolset {
    type Handler = ToolHandler;

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
        let _admission = self.runtime.admit("list_tools")?;
        self.runtime.backend.list_tools().await
    }

    async fn tool_handler(&self, name: ToolName) -> Result<Self::Handler, ToolError> {
        let _admission = self.runtime.admit("tool_handler")?;
        self.runtime.backend.tool_handler(name).await
    }

    async fn shutdown(&self) -> Result<(), ToolError> {
        self.runtime.shutdown().await
    }
}

struct ToolsetLifecycle {
    state: Mutex<ToolsetState>,
    changed: Notify,
}

enum ToolsetState {
    Open {
        admitted: usize,
    },
    Closing {
        admitted: usize,
        completion: watch::Receiver<Option<Result<(), ToolError>>>,
        task: JoinHandle<()>,
    },
    Closed {
        terminal: Result<(), ToolError>,
    },
}

struct ToolsetAdmission {
    runtime: Arc<ToolsetRuntime>,
}

impl ToolsetRuntime {
    fn new(backend: InnerToolset) -> Self {
        Self {
            backend,
            lifecycle: ToolsetLifecycle {
                state: Mutex::new(ToolsetState::Open { admitted: 0 }),
                changed: Notify::new(),
            },
        }
    }

    fn admit(self: &Arc<Self>, operation: &'static str) -> Result<ToolsetAdmission, ToolError> {
        let mut state = self.lifecycle.state();
        match &mut *state {
            ToolsetState::Open { admitted } => {
                *admitted = admitted
                    .checked_add(1)
                    .ok_or_else(|| ToolError::Transport {
                        tool: None,
                        message: "toolset admission is exhausted".to_owned(),
                    })?;
                Ok(ToolsetAdmission {
                    runtime: Arc::clone(self),
                })
            }
            ToolsetState::Closing { .. } | ToolsetState::Closed { .. } => {
                Err(ToolError::closed(operation))
            }
        }
    }

    async fn shutdown(self: &Arc<Self>) -> Result<(), ToolError> {
        self.start_shutdown()?;
        self.lifecycle.terminal().await
    }

    fn start_shutdown(self: &Arc<Self>) -> Result<(), ToolError> {
        let mut state = self.lifecycle.state();
        let admitted = match &*state {
            ToolsetState::Open { admitted } => *admitted,
            ToolsetState::Closing { .. } | ToolsetState::Closed { .. } => return Ok(()),
        };
        let handle = tokio::runtime::Handle::try_current().map_err(|_| ToolError::Transport {
            tool: None,
            message: "toolset shutdown requires a Tokio runtime".to_owned(),
        })?;
        let (terminal, completion) = watch::channel(None);
        let runtime = Arc::clone(self);
        let task = handle.spawn(async move {
            terminal.send_replace(Some(runtime.finish_shutdown().await));
        });
        *state = ToolsetState::Closing {
            admitted,
            completion,
            task,
        };
        Ok(())
    }

    async fn finish_shutdown(self: Arc<Self>) -> Result<(), ToolError> {
        self.lifecycle.drain().await;
        self.backend.shutdown().await
    }
}

impl ToolsetLifecycle {
    fn state(&self) -> std::sync::MutexGuard<'_, ToolsetState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn release(&self) {
        let should_notify = {
            let mut state = self.state();
            match &mut *state {
                ToolsetState::Open { admitted } => {
                    debug_assert_ne!(*admitted, 0);
                    *admitted = admitted.saturating_sub(1);
                    false
                }
                ToolsetState::Closing { admitted, .. } => {
                    debug_assert_ne!(*admitted, 0);
                    *admitted = admitted.saturating_sub(1);
                    *admitted == 0
                }
                ToolsetState::Closed { .. } => {
                    debug_assert!(false, "closed toolset retained an admission");
                    false
                }
            }
        };
        if should_notify {
            self.changed.notify_waiters();
        }
    }

    async fn drain(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let drained = match &*self.state() {
                ToolsetState::Open { admitted } | ToolsetState::Closing { admitted, .. } => {
                    *admitted == 0
                }
                ToolsetState::Closed { .. } => true,
            };
            if drained {
                return;
            }
            notified.await;
        }
    }

    async fn terminal(&self) -> Result<(), ToolError> {
        let mut completion = match &*self.state() {
            ToolsetState::Closing { completion, .. } => completion.clone(),
            ToolsetState::Closed { terminal } => return terminal.clone(),
            ToolsetState::Open { .. } => {
                return Err(ToolError::Transport {
                    tool: None,
                    message: "toolset shutdown lost lifecycle ownership".to_owned(),
                });
            }
        };
        let terminal = match completion.wait_for(Option::is_some).await {
            Ok(terminal) => terminal
                .as_ref()
                .cloned()
                .ok_or_else(|| ToolError::Transport {
                    tool: None,
                    message: "toolset shutdown result is unavailable".to_owned(),
                })?,
            Err(_) => Err(ToolError::Transport {
                tool: None,
                message: "toolset shutdown owner exited without a terminal result".to_owned(),
            }),
        };
        loop {
            let settled = {
                let mut state = self.state();
                match &mut *state {
                    ToolsetState::Closed { terminal } => return terminal.clone(),
                    ToolsetState::Closing { task, .. } if task.is_finished() => {
                        let waker = std::task::Waker::noop();
                        let mut context = Context::from_waker(waker);
                        match Pin::new(task).poll(&mut context) {
                            Poll::Ready(joined) => {
                                let terminal = if joined.is_ok() {
                                    terminal.clone()
                                } else {
                                    Err(ToolError::Transport {
                                        tool: None,
                                        message: "toolset shutdown owner could not be joined"
                                            .to_owned(),
                                    })
                                };
                                *state = ToolsetState::Closed {
                                    terminal: terminal.clone(),
                                };
                                Some(terminal)
                            }
                            Poll::Pending => None,
                        }
                    }
                    ToolsetState::Closing { .. } => None,
                    ToolsetState::Open { .. } => {
                        let terminal = Err(ToolError::Transport {
                            tool: None,
                            message: "toolset shutdown lost lifecycle ownership".to_owned(),
                        });
                        *state = ToolsetState::Closed {
                            terminal: terminal.clone(),
                        };
                        Some(terminal)
                    }
                }
            };
            if let Some(terminal) = settled {
                self.changed.notify_waiters();
                return terminal;
            }
            tokio::task::yield_now().await;
        }
    }
}

impl Drop for ToolsetAdmission {
    fn drop(&mut self) {
        self.runtime.lifecycle.release();
    }
}
