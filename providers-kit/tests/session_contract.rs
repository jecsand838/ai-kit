//! Downstream construction and borrowed-half contract; no supplied provider or I/O.
use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use providers_kit::error::{ErrorKind, ErrorPhase, Operation};
use providers_kit::generation::{ContentPart, Message};
use providers_kit::session::{
    CommandId, OutputSegmentId, ResponseId, ResponseOutcome, SessionBackend, SessionCommand,
    SessionCommands, SessionConfig, SessionContinuity, SessionEvent, SessionEvents, SessionProfile,
    ToolRequestId,
};
use providers_kit::{KitError, ModelId};
use tokio::sync::Notify;

struct EventQueue {
    values: Mutex<VecDeque<SessionEvent>>,
    changed: Notify,
}

struct CallerSession {
    model: ModelId,
    commands: CallerCommands,
    events: CallerEvents,
}

struct CallerCommands {
    sequence: u64,
    queue: Arc<EventQueue>,
}

struct CallerEvents {
    queue: Arc<EventQueue>,
}

struct CommandBorrow<'a>(&'a mut CallerCommands);
struct EventBorrow<'a>(&'a mut CallerEvents);

impl CallerSession {
    fn new() -> Self {
        let queue = Arc::new(EventQueue {
            values: Mutex::new(VecDeque::new()),
            changed: Notify::new(),
        });
        Self {
            model: ModelId::new("caller-exact-model").unwrap(),
            commands: CallerCommands {
                sequence: 0,
                queue: Arc::clone(&queue),
            },
            events: CallerEvents { queue },
        }
    }
}

impl SessionBackend for CallerSession {
    type Commands<'a> = CommandBorrow<'a>;
    type Events<'a> = EventBorrow<'a>;

    fn id(&self) -> &str {
        "caller-session"
    }
    fn model_id(&self) -> &ModelId {
        &self.model
    }

    fn split(&mut self) -> (Self::Commands<'_>, Self::Events<'_>) {
        (
            CommandBorrow(&mut self.commands),
            EventBorrow(&mut self.events),
        )
    }

    async fn close(self) -> Result<(), KitError> {
        Ok(())
    }
}

impl SessionCommands for CommandBorrow<'_> {
    async fn submit(&mut self, command: SessionCommand) -> Result<CommandId, KitError> {
        let SessionCommand::Input(message) = command else {
            return Err(KitError::new(
                ErrorKind::Unsupported,
                Operation::SessionSend,
                ErrorPhase::InputValidation,
            ));
        };
        let next = self.0.sequence.checked_add(1).unwrap();
        let sequence = NonZeroU64::new(next).unwrap();
        let command = CommandId::new("caller-session", sequence)?;
        let response = ResponseId::new("caller-session", sequence)?;
        let mut events = self.0.queue.values.lock().unwrap();
        events.push_back(SessionEvent::ResponseStarted {
            command: Some(command.clone()),
            response: response.clone(),
        });
        for content in message.content() {
            events.push_back(SessionEvent::Output {
                response: response.clone(),
                segment: None,
                content: content.clone(),
            });
        }
        events.push_back(SessionEvent::ResponseFinished {
            response,
            outcome: ResponseOutcome::Completed,
            continuity: SessionContinuity::Reusable,
            output: None,
        });
        self.0.sequence = next;
        self.0.queue.changed.notify_one();
        Ok(command)
    }
}

impl SessionEvents for EventBorrow<'_> {
    async fn recv(&mut self) -> Option<SessionEvent> {
        loop {
            let changed = self.0.queue.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(event) = self.0.queue.values.lock().unwrap().pop_front() {
                return Some(event);
            }
            changed.await;
        }
    }
}

#[test]
fn explicit_profiles_and_downstream_tokens_are_constructible() {
    assert_eq!(
        SessionConfig::new(SessionProfile::Conversation).profile(),
        SessionProfile::Conversation
    );
    assert_eq!(
        SessionConfig::new(SessionProfile::Realtime).profile(),
        SessionProfile::Realtime
    );
    let sequence = NonZeroU64::new(1).unwrap();
    let command = CommandId::new("caller-session", sequence).unwrap();
    assert_eq!(command.session_id(), "caller-session");
    assert_eq!(command.sequence(), sequence);
    assert_ne!(
        command,
        CommandId::new("another-session", sequence).unwrap()
    );
    assert!(CommandId::new(" ", sequence).is_err());
    assert!(ResponseId::new("", sequence).is_err());
    assert!(ToolRequestId::new("\n", sequence).is_err());
    assert!(OutputSegmentId::new("\t", sequence).is_err());
    assert!(ResponseId::new("caller-session", sequence).is_ok());
    assert!(ToolRequestId::new("caller-session", sequence).is_ok());
    assert!(OutputSegmentId::new("caller-session", sequence).is_ok());
}

#[tokio::test]
async fn independent_borrowed_halves_progress_while_receive_is_pending() {
    let mut session = CallerSession::new();
    {
        let (mut commands, mut events) = session.split();
        let mut pending = Box::pin(events.recv());
        poll_fn(|cx| {
            assert!(
                pending.as_mut().poll(cx).is_pending(),
                "idle is not permanent drain"
            );
            Poll::Ready(())
        })
        .await;
        let command = commands
            .submit(SessionCommand::Input(Message::user_text("hello").unwrap()))
            .await
            .unwrap();
        let first = pending.await.unwrap();
        assert!(
            matches!(first, SessionEvent::ResponseStarted { command: Some(accepted), .. } if accepted == command)
        );
        assert!(
            matches!(events.recv().await, Some(SessionEvent::Output { content: ContentPart::Text { text }, .. }) if text == "hello")
        );
        assert!(matches!(
            events.recv().await,
            Some(SessionEvent::ResponseFinished {
                outcome: ResponseOutcome::Completed,
                continuity: SessionContinuity::Reusable,
                ..
            })
        ));
        let mut idle = Box::pin(events.recv());
        poll_fn(|cx| {
            assert!(idle.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(idle);
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn dropping_an_unpolled_submission_and_pending_receive_loses_no_event() {
    let mut session = CallerSession::new();
    {
        let (mut commands, mut events) = session.split();
        drop(commands.submit(SessionCommand::Input(
            Message::user_text("not accepted").unwrap(),
        )));
        let mut receive = Box::pin(events.recv());
        poll_fn(|cx| {
            assert!(receive.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(receive);
        let command = commands
            .submit(SessionCommand::Input(
                Message::user_text("accepted").unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(command.sequence().get(), 1);
        assert!(
            matches!(events.recv().await, Some(SessionEvent::ResponseStarted { command: Some(observed), .. }) if observed == command)
        );
    }
    session.close().await.unwrap();
}

#[test]
fn half_types_and_operation_futures_are_send() {
    struct SendProof;
    impl SendProof {
        fn value<T: Send>(_: T) {}
    }
    let mut session = CallerSession::new();
    let (mut commands, mut events) = session.split();
    SendProof::value(commands.submit(SessionCommand::Input(Message::user_text("hello").unwrap())));
    SendProof::value(events.recv());
    SendProof::value(commands);
    SendProof::value(events);
    SendProof::value(session.close());
}
