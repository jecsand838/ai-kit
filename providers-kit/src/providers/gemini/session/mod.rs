mod protocol;

use std::sync::Arc;

use crate::KitError;
use crate::core::{
    ManagedCommands, ManagedConversation, SessionControl, SessionEventPort, SessionSocket,
};
use crate::generation::{ContentPart, Message, Role};
use crate::models::{ModelBackend, ModelId};
use crate::provider::ProviderRuntime;
use crate::session::{
    CommandId, SessionBackend, SessionCommand, SessionCommands, SessionConfig, SessionProfile,
    SessionTurnDetection,
};

use super::generate_content::tool_result_content;
use super::models::GeminiModel;
use protocol::GeminiProtocol;

pub(crate) struct GeminiSession {
    model: ModelId,
    mode: GeminiMode,
}

enum GeminiMode {
    Conversation(ManagedConversation<GeminiModel>),
    Realtime(GeminiLive),
}

pub(crate) enum GeminiCommands<'a> {
    Conversation(ManagedCommands<'a, GeminiModel>),
    Realtime {
        control: &'a Arc<SessionControl>,
        capturing: &'a mut bool,
    },
}

struct GeminiLive {
    control: Arc<SessionControl>,
    driver: GeminiDriver,
    capturing: bool,
}

enum GeminiDriver {
    Prepared(Box<GeminiProtocol>),
    Running,
}

impl GeminiSession {
    pub(crate) async fn open(model: &GeminiModel, config: SessionConfig) -> Result<Self, KitError> {
        let timeout = model.api().transport().timeout();
        let mode = match config.profile() {
            SessionProfile::Conversation => {
                GeminiMode::Conversation(ManagedConversation::new(model.clone(), config, timeout)?)
            }
            SessionProfile::Realtime => {
                let control = SessionControl::new(config, timeout)?;
                let protocol = GeminiProtocol::open(model, control.config()).await?;
                GeminiMode::Realtime(GeminiLive {
                    control,
                    driver: GeminiDriver::Prepared(Box::new(protocol)),
                    capturing: false,
                })
            }
        };
        Ok(Self {
            model: model.model_id().clone(),
            mode,
        })
    }

    pub(crate) fn bind_provider(&mut self, runtime: Arc<ProviderRuntime>) -> Result<(), KitError> {
        match &mut self.mode {
            GeminiMode::Conversation(session) => session.bind_provider(runtime),
            GeminiMode::Realtime(session) => {
                session.control.bind_provider(runtime)?;
                let GeminiDriver::Prepared(driver) =
                    std::mem::replace(&mut session.driver, GeminiDriver::Running)
                else {
                    return Err(KitError::invalid_state("Gemini Live was already bound"));
                };
                let control = Arc::clone(&session.control);
                session
                    .control
                    .spawn(async move { driver.run(control).await })
            }
        }
    }
}

impl SessionBackend for GeminiSession {
    type Commands<'a> = GeminiCommands<'a>;
    type Events<'a> = SessionEventPort<'a>;

    fn id(&self) -> &str {
        match &self.mode {
            GeminiMode::Conversation(session) => session.control().id(),
            GeminiMode::Realtime(session) => session.control.id(),
        }
    }

    fn model_id(&self) -> &ModelId {
        &self.model
    }

    fn split(&mut self) -> (Self::Commands<'_>, Self::Events<'_>) {
        match &mut self.mode {
            GeminiMode::Conversation(session) => {
                let (commands, events) =
                    session.split(|model, result| tool_result_content(model, [result]).map(drop));
                (GeminiCommands::Conversation(commands), events)
            }
            GeminiMode::Realtime(session) => {
                // Command admission and the observation port borrow distinct owner fields.
                let GeminiLive {
                    control, capturing, ..
                } = session;
                let events = SessionEventPort::new(control);
                (GeminiCommands::Realtime { control, capturing }, events)
            }
        }
    }

    async fn close(self) -> Result<(), KitError> {
        match self.mode {
            GeminiMode::Conversation(session) => session.close().await,
            GeminiMode::Realtime(session) => session.control.close().await,
        }
    }
}

impl SessionCommands for GeminiCommands<'_> {
    async fn submit(&mut self, command: SessionCommand) -> Result<CommandId, KitError> {
        match self {
            Self::Conversation(commands) => commands.submit(command).await,
            Self::Realtime { control, capturing } => Self::accept_live(control, capturing, command),
        }
    }
}

impl GeminiCommands<'_> {
    fn accept_live(
        control: &SessionControl,
        capturing: &mut bool,
        command: SessionCommand,
    ) -> Result<CommandId, KitError> {
        control.ensure_open()?;
        let config = control.config();
        match &command {
            SessionCommand::Input(message) => {
                Self::validate_input(message)?;
                if *capturing {
                    return Err(KitError::invalid_state(
                        "Gemini Live input activity is still open",
                    ));
                }
            }
            SessionCommand::ToolResults(results) => {
                for result in results {
                    for content in result.result().content() {
                        if !matches!(content, ContentPart::Text { .. } | ContentPart::Json { .. }) {
                            return Err(GeminiProtocol::unsupported(
                                "session.tool_results",
                                "Gemini Live function responses require text or JSON",
                            ));
                        }
                    }
                }
            }
            SessionCommand::StartInput | SessionCommand::EndInput => {
                if config.turn_detection() != SessionTurnDetection::Manual
                    || config.input_audio_format().is_none()
                {
                    return Err(GeminiProtocol::unsupported(
                        "session.input_activity",
                        "explicit activity requires manual configured audio",
                    ));
                }
                if matches!(command, SessionCommand::StartInput) == *capturing {
                    return Err(KitError::invalid_state(
                        "Gemini Live activity boundary is out of order",
                    ));
                }
            }
            SessionCommand::Audio { media_type, data } => {
                let encoded = data
                    .len()
                    .checked_add(2)
                    .and_then(|bytes| bytes.checked_div(3))
                    .and_then(|bytes| bytes.checked_mul(4))
                    .and_then(|bytes| bytes.checked_add(media_type.len()))
                    .and_then(|bytes| bytes.checked_add(128))
                    .ok_or_else(|| KitError::invalid_state("Gemini Live audio size overflow"))?;
                SessionSocket::validate_message_bytes(encoded)?;
                if config.input_audio_format() != Some(media_type.as_str()) || data.is_empty() {
                    return Err(KitError::invalid_state(
                        "Gemini Live audio must use the configured format and contain bytes",
                    ));
                }
                if config.turn_detection() == SessionTurnDetection::Manual && !*capturing {
                    return Err(KitError::invalid_state(
                        "Gemini Live manual audio requires open input activity",
                    ));
                }
            }
            SessionCommand::EndAudioStream => {
                if config.turn_detection() != SessionTurnDetection::Automatic
                    || config.input_audio_format().is_none()
                {
                    return Err(GeminiProtocol::unsupported(
                        "session.audio_stream",
                        "audioStreamEnd requires automatic configured audio",
                    ));
                }
            }
            SessionCommand::Cancel(_) => {
                return Err(GeminiProtocol::unsupported(
                    "session.cancel",
                    "Gemini Live has no targeted response cancellation operation",
                ));
            }
            SessionCommand::DiscardInput => {
                return Err(GeminiProtocol::unsupported(
                    "session.discard_input",
                    "Gemini Live has no input-buffer discard operation",
                ));
            }
            SessionCommand::ReconcilePlayback { .. } => {
                return Err(GeminiProtocol::unsupported(
                    "session.playback",
                    "Gemini Live has no output-segment playback reconciliation operation",
                ));
            }
        }
        let start = matches!(command, SessionCommand::StartInput);
        let end = matches!(command, SessionCommand::EndInput);
        let command = control.accept(command)?;
        if start {
            *capturing = true;
        }
        if end {
            *capturing = false;
        }
        Ok(command)
    }

    fn validate_input(message: &Message<()>) -> Result<(), KitError> {
        if message.role() != Role::User
            || !message.function_calls().is_empty()
            || !message.function_results().is_empty()
        {
            return Err(GeminiProtocol::unsupported(
                "session.input",
                "Gemini Live accepts user content only",
            ));
        }
        for part in message.content() {
            if !matches!(part, ContentPart::Text { .. }) {
                return Err(GeminiProtocol::unsupported(
                    "session.input",
                    "Gemini Live message input requires text; audio uses the capture command",
                ));
            }
        }
        Ok(())
    }
}

impl Drop for GeminiLive {
    fn drop(&mut self) {
        self.control.stop();
    }
}
