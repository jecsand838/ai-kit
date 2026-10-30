//! Durable conversation facts, exclusive delivery, and caller interaction resolutions.
//!
//! Prompt output stays on the prompt return path. Passive items are acknowledged
//! by advancing delivery; actionable interactions require an explicit resolution.
//! Receiver drop is not acknowledgment while the conversation is reopenable.
//!
use std::path::PathBuf;
use std::pin::Pin;
use std::time::SystemTime;

use futures_util::{Stream, StreamExt};

use crate::backends::custom::CustomNotificationReceiver;
use crate::core::notification_store::NotificationReceiver;
use crate::{KitError, NotificationId, WorkspaceContext};
use storage_kit::{DatabaseBackend, StateStoreBackend};

#[derive(Clone, Debug, PartialEq)]
/// One durable notification delivered for a conversation.
///
/// # Examples
///
/// ```
/// use agents_kit::notification::{ConversationNotification, ConversationNotificationKind};
/// let id = "notification_00000000000000000000000000000000".parse()?;
/// let notification = ConversationNotification::new(
///     id, std::time::SystemTime::UNIX_EPOCH,
///     ConversationNotificationKind::Message { text: "Ready".into() },
/// )?;
/// assert!(notification.id().as_str().starts_with("notification_"));
/// assert_eq!(notification.inserted_at(), std::time::SystemTime::UNIX_EPOCH);
/// assert!(matches!(notification.kind(), ConversationNotificationKind::Message { text } if text == "Ready"));
/// # Ok::<(), agents_kit::KitError>(())
/// ```
pub struct ConversationNotification {
    id: NotificationId,
    inserted_at: SystemTime,
    kind: ConversationNotificationKind,
}

impl ConversationNotification {
    /// Validates and constructs one portable notification.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for invalid content: blank required text, invalid relative paths, padded labels/titles, or a non-object interaction schema. This value constructor does not persist or publish a notification; the owning conversation validates the full schema before publication.
    ///
    /// See [`ConversationNotification`] for a complete example.
    pub fn new(
        id: NotificationId,
        inserted_at: SystemTime,
        kind: ConversationNotificationKind,
    ) -> Result<Self, KitError> {
        kind.validate()?;
        Ok(Self {
            id,
            inserted_at,
            kind,
        })
    }

    /// Returns the durable notification identity.
    ///
    /// See [`ConversationNotification`] for a complete example.
    pub fn id(&self) -> &NotificationId {
        &self.id
    }

    /// Returns when the notification entered durable delivery.
    ///
    /// See [`ConversationNotification`] for a complete example.
    pub fn inserted_at(&self) -> SystemTime {
        self.inserted_at
    }

    /// Returns the portable notification content.
    ///
    /// See [`ConversationNotification`] for a complete example.
    pub fn kind(&self) -> &ConversationNotificationKind {
        &self.kind
    }
}

/// Portable lifecycle state for one caller-visible backend-host process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProcessState {
    /// The process is currently running.
    Running,
    /// The process exited with an optional portable exit code.
    Exited {
        /// Native exit status when one is available without leaking platform details.
        exit_code: Option<i32>,
    },
}

#[derive(Clone, Debug, PartialEq)]
/// Portable content delivered through the conversation notification stream.
pub enum ConversationNotificationKind {
    /// Reports a passive text message.
    Message {
        /// Message text.
        text: String,
    },
    /// Reports that a workspace-relative file changed.
    FileChanged {
        /// Validated workspace-relative path, preserving its original spelling.
        path: PathBuf,
        /// Optional source-neutral change summary.
        summary: Option<String>,
    },
    /// Reports a safe caller-visible process lifecycle fact.
    Process {
        /// Source-neutral process label.
        label: String,
        /// Current portable process state.
        state: ProcessState,
    },
    /// Reports passive progress without completing a prompt.
    Progress {
        /// Short progress label.
        label: String,
        /// Optional progress detail.
        detail: Option<String>,
    },
    /// Reports a portable warning.
    Warning {
        /// Warning message.
        message: String,
    },
    /// Reports a portable asynchronous error.
    Error {
        /// Error message.
        message: String,
    },
    /// Requests one caller decision through the portable interaction path.
    Interaction {
        /// Caller-facing interaction title.
        title: String,
        /// Caller-facing interaction body.
        body: String,
        /// Optional JSON Schema describing a submitted response.
        response_schema: Option<serde_json::Value>,
        /// Whether the caller may dismiss the interaction.
        dismissible: bool,
    },
}

impl ConversationNotificationKind {
    fn validate(&self) -> Result<(), KitError> {
        match self {
            Self::Message { text }
            | Self::Warning { message: text }
            | Self::Error { message: text }
                if text.trim().is_empty() =>
            {
                Err(KitError::invalid_input(
                    "notification message must be nonblank",
                ))
            }
            Self::FileChanged { path, summary } => {
                WorkspaceContext::normalized_relative(path)?;
                if summary
                    .as_ref()
                    .is_some_and(|summary| summary.trim().is_empty())
                {
                    return Err(KitError::invalid_input(
                        "file notification summary must be absent or nonblank",
                    ));
                }
                Ok(())
            }
            Self::Process { label, .. }
                if label.is_empty()
                    || label.trim() != label
                    || label.chars().any(char::is_control) =>
            {
                Err(KitError::invalid_input(
                    "process notification label must be nonempty, unpadded, and control-free",
                ))
            }
            Self::Progress { label, detail } => {
                if label.is_empty() || label.trim() != label {
                    return Err(KitError::invalid_input(
                        "progress notification label must be nonempty and unpadded",
                    ));
                }
                if detail
                    .as_ref()
                    .is_some_and(|detail| detail.trim().is_empty())
                {
                    return Err(KitError::invalid_input(
                        "progress notification detail must be absent or nonblank",
                    ));
                }
                Ok(())
            }
            Self::Interaction {
                title,
                body,
                response_schema,
                ..
            } => {
                if title.is_empty() || title.trim() != title {
                    return Err(KitError::invalid_input(
                        "interaction title must be nonempty and unpadded",
                    ));
                }
                if body.trim().is_empty() {
                    return Err(KitError::invalid_input("interaction body must be nonblank"));
                }
                if response_schema
                    .as_ref()
                    .is_some_and(|schema| !schema.is_object())
                {
                    return Err(KitError::invalid_input(
                        "interaction response schema must be a JSON object",
                    ));
                }
                Ok(())
            }
            Self::Message { .. }
            | Self::Process { .. }
            | Self::Warning { .. }
            | Self::Error { .. } => Ok(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// Resolves one pending portable interaction.
pub enum NotificationResolution {
    /// Allows the requested action.
    Allow,
    /// Denies the requested action.
    Deny {
        /// Nonempty caller explanation.
        reason: String,
    },
    /// Submits structured interaction content.
    Submit {
        /// JSON value validated against the interaction schema when present.
        content: serde_json::Value,
    },
    /// Dismisses a dismissible interaction.
    Dismiss {
        /// Nonempty caller explanation.
        reason: String,
    },
}

impl NotificationResolution {
    pub(crate) fn validate(&self) -> Result<(), KitError> {
        match self {
            Self::Deny { reason } | Self::Dismiss { reason }
                if reason.is_empty() || reason.trim() != reason =>
            {
                Err(KitError::invalid_input(
                    "notification resolution reason must be nonempty and unpadded",
                ))
            }
            _ => Ok(()),
        }
    }
}

/// Exclusively receives durable notifications for one conversation.
///
/// Built-in conversations deliver in durable, at-least-once order. The next receive
/// acknowledges the previous passive item; interactions require a successful
/// `ConversationBackend::resolve_notification` instead. Dropping a receiver while
/// the conversation is reopenable releases its lease without acknowledging items.
///
/// Conversation close makes an existing receiver drain-only. Parent workspace close
/// or Agent shutdown preserves backlog for reopen; it does not acknowledge it.
///
/// # Examples
///
/// A finite caller source illustrates wrapper delivery and fused end-of-stream.
/// It does not itself implement durable storage or interaction acknowledgment.
///
/// ```
/// use agents_kit::notification::{ConversationNotification, ConversationNotificationKind, ConversationNotificationReceiver};
/// let runtime = tokio::runtime::Builder::new_current_thread().build()?;
/// runtime.block_on(async {
///     let notification = ConversationNotification::new(
///         "notification_00000000000000000000000000000000".parse()?,
///         std::time::SystemTime::UNIX_EPOCH,
///         ConversationNotificationKind::Message { text: "Ready".into() },
///     )?;
///     let id = notification.id().clone();
///     let mut receiver = ConversationNotificationReceiver::from_stream(
///         futures_util::stream::iter([Ok(notification)]),
///     );
///     assert_eq!(receiver.recv().await.transpose()?.unwrap().id(), &id);
///     assert!(receiver.recv().await.is_none());
///     assert!(receiver.recv().await.is_none());
///     Ok::<(), agents_kit::KitError>(())
/// })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct ConversationNotificationReceiver {
    inner: Pin<Box<dyn Stream<Item = Result<ConversationNotification, KitError>> + Send + 'static>>,
    terminal: bool,
}

impl ConversationNotificationReceiver {
    pub(crate) fn shared<S: StateStoreBackend>(inner: NotificationReceiver<S>) -> Self {
        Self::from_stream(futures_util::stream::unfold(
            inner,
            |mut receiver| async move { receiver.recv().await.map(|item| (item, receiver)) },
        ))
    }

    pub(crate) fn custom<S: StateStoreBackend, D: DatabaseBackend>(
        inner: CustomNotificationReceiver<S, D>,
    ) -> Self {
        Self::from_stream(futures_util::stream::unfold(
            inner,
            |mut receiver| async move { receiver.recv().await.map(|item| (item, receiver)) },
        ))
    }

    /// Adapts a downstream portable notification stream into the unique receiver wrapper.
    ///
    /// The caller source owns durable ordering, acknowledgment, cancellation safety and cleanup. The wrapper supplies portable delivery and terminal fusion, not a new journal.
    ///
    /// See [`ConversationNotificationReceiver`] for a complete example.
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<ConversationNotification, KitError>> + Send + 'static,
    {
        Self {
            inner: Box::pin(stream),
            terminal: false,
        }
    }

    /// Waits for the next delivery, error, or normal end of delivery.
    ///
    /// # Errors
    ///
    /// Yields source errors with notification/publication context. `Timeout` and
    /// `Integration` permit a later receive. `Conflict`, `Closed`, `Unsupported`,
    /// `NotYetImplemented`, `Protocol` and `InvalidState` are yielded once and fuse
    /// this receiver; subsequent calls return `None`. A source EOF also fuses it.
    /// Built-in delivery retains admitted storage work if one awaiting receive is cancelled.
    ///
    /// See [`ConversationNotificationReceiver`] for a complete example.
    pub async fn recv(&mut self) -> Option<Result<ConversationNotification, KitError>> {
        if self.terminal {
            return None;
        }
        let next = self.inner.next().await.map(|result| {
            result.map_err(|error| {
                error
                    .at_operation(crate::error::Operation::NotificationStream)
                    .in_phase(crate::error::ErrorPhase::Publication)
            })
        });
        if next.is_none()
            || matches!(
                &next, Some(Err(error)) if matches!(error.kind(),
                    crate::error::ErrorKind::Conflict | crate::error::ErrorKind::Closed |
                    crate::error::ErrorKind::Unsupported | crate::error::ErrorKind::NotYetImplemented |
                    crate::error::ErrorKind::Protocol | crate::error::ErrorKind::InvalidState)
            )
        {
            self.terminal = true;
        }
        next
    }
}
