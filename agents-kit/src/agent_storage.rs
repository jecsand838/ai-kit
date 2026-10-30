use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use storage_kit::{DatabaseBackend, StateStoreBackend, StateTransactionBackend};

use crate::{AgentId, AgentInfo, KitError, KnowledgeOwnerId, ManagedKnowledge};

const AGENT_NAMESPACE: &str = "agents_kit_agents_v1";
const AGENT_RECORD_VERSION: u32 = 1;
const MAX_AGENT_RECORDS: usize = 65_536;
const MAX_AGENT_INVENTORY_PAGES: usize = 1_024;
const KNOWLEDGE_FORGET_NAMESPACE: &str = "agents_kit_knowledge_forget_v1";
const KNOWLEDGE_FORGET_RECORD_VERSION: u32 = 1;
const MAX_KNOWLEDGE_FORGET_RECORDS: usize = 65_536;
const MAX_KNOWLEDGE_FORGET_PAGES: usize = 1_024;

/// Coordinates durable agent identities through a caller-supplied state store.
///
/// The concrete `S` is retained directly, and its associated `S::Transaction`
/// remains the only transaction type used for agent control-plane work. Clones
/// share the same store access and live-agent ownership domain without requiring
/// `S: Clone`.
///
/// # Examples
///
/// ```
/// use agents_kit::AgentStorage;
/// use storage_kit::{StateStore, state::config::{StateStoreConfig, LanceStateStoreConfig}};
/// let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// runtime.block_on(async {
///     let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
///     let storage = AgentStorage::new(store);
///     assert!(storage.list_agents().await?.is_empty());
///     let id = "agent_00000000000000000000000000000000".parse()?;
///     let error = storage.agent_info(id).await.unwrap_err();
///     assert_eq!(error.kind(), agents_kit::error::ErrorKind::NotFound);
///     drop(storage);
///     Ok::<(), Box<dyn std::error::Error>>(())
/// })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct AgentStorage<S: StateStoreBackend = storage_kit::StateStore> {
    inner: Arc<AgentStorageInner<S>>,
}

struct AgentStorageInner<S: StateStoreBackend> {
    store: S,
    live_agents: Arc<Mutex<BTreeSet<AgentId>>>,
}

pub(crate) struct AgentStorageClaim {
    live_agents: Arc<Mutex<BTreeSet<AgentId>>>,
    id: Option<AgentId>,
    release_on_drop: bool,
}

pub(crate) enum AgentStorageCreationError {
    TransactionConflict,
    Failure(KitError),
}

pub(crate) enum AgentStorageKnowledgeError {
    TransactionConflict(crate::error::ErrorPhase),
    Failure(KitError),
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct AgentRecord {
    version: u32,
    id: AgentId,
    display_name: Option<String>,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct KnowledgeForgetRecord {
    version: u32,
    owner: KnowledgeOwnerId,
}

impl<S: StateStoreBackend> Clone for AgentStorage<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S: StateStoreBackend> AgentStorage<S> {
    /// Creates the agent control-plane boundary from an already-built state store.
    ///
    /// Construction retains the supplied store; it neither creates an Agent nor requires `S: Clone`.
    ///
    /// See [`AgentStorage`] for a complete example.
    pub fn new(store: S) -> Self {
        Self {
            inner: Arc::new(AgentStorageInner {
                store,
                live_agents: Arc::new(Mutex::new(BTreeSet::new())),
            }),
        }
    }

    /// Lists every durable agent identity visible through this storage domain.
    ///
    /// The result is ordered by durable Agent ID. This reads storage, not native runtime inventory.
    ///
    /// # Errors
    ///
    /// Returns a typed storage failure or `InvalidState` for malformed records or an inventory that exceeds the bounded traversal.
    ///
    /// See [`AgentStorage`] for a complete example.
    pub async fn list_agents(&self) -> Result<Vec<AgentInfo>, KitError> {
        async {
            let mut records = Vec::new();
            let mut cursor = None;
            let mut pages = 0_usize;
            loop {
                pages += 1;
                if pages > MAX_AGENT_INVENTORY_PAGES {
                    return Err(KitError::invalid_state(
                        "agent inventory exceeds its private bound",
                    ));
                }
                let mut request =
                    storage_kit::state::record::StateListRequest::new(AGENT_NAMESPACE)
                        .map_err(|error| {
                            KitError::from_state_store(
                                error,
                                "agent inventory request could not be created",
                            )
                            .in_phase(crate::error::ErrorPhase::StateRead)
                        })?
                        .with_limit(storage_kit::state::record::StateListRequest::MAX_LIMIT)
                        .map_err(|error| {
                            KitError::from_state_store(error, "agent inventory limit was rejected")
                                .in_phase(crate::error::ErrorPhase::StateRead)
                        })?;
                if let Some(next) = cursor.take() {
                    request = request.start_after(next).map_err(|_| {
                        KitError::invalid_state("agent inventory cursor is invalid")
                    })?;
                }
                let page = self.store().list(request).await.map_err(|error| {
                    KitError::from_state_store(error, "agent inventory could not be loaded")
                        .in_phase(crate::error::ErrorPhase::StateRead)
                })?;
                for snapshot in page.records {
                    if records.len() == MAX_AGENT_RECORDS {
                        return Err(KitError::invalid_state(
                            "agent inventory exceeds its private bound",
                        ));
                    }
                    records.push(AgentRecord::decode(snapshot)?);
                }
                let Some(next) = page.next_start_after else {
                    break;
                };
                cursor = Some(next);
            }
            records.sort_by(|left, right| left.id.cmp(&right.id));
            records.into_iter().map(AgentRecord::into_info).collect()
        }
        .await
        .map_err(|error: KitError| {
            error
                .at_operation(crate::error::Operation::ListAgents)
                .in_phase(crate::error::ErrorPhase::StateRead)
        })
    }

    /// Loads descriptive information for one durable agent identity.
    ///
    /// This reads the durable description without opening a runtime.
    ///
    /// # Errors
    ///
    /// Returns `NotFound` for an absent identity, `InvalidState` for malformed records, or a typed storage access failure.
    ///
    /// See [`AgentStorage`] for a complete example.
    pub async fn agent_info(&self, id: AgentId) -> Result<AgentInfo, KitError> {
        async { self.load_record(&id).await?.into_info() }
            .await
            .map_err(|error: KitError| {
                error
                    .at_operation(crate::error::Operation::AgentInfo)
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })
    }

    pub(crate) async fn create_agent_in_transaction(
        &self,
        transaction: &mut S::Transaction,
        info: &AgentInfo,
    ) -> Result<(), AgentStorageCreationError> {
        let record = AgentRecord::from_info(info);
        let value = serde_json::to_value(&record).map_err(|_| {
            AgentStorageCreationError::Failure(KitError::invalid_state(
                "agent identity could not be encoded",
            ))
        })?;
        let key = AgentRecord::key(info.id()).map_err(AgentStorageCreationError::Failure)?;
        if transaction
            .load(key.clone())
            .await
            .map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    AgentStorageCreationError::TransactionConflict
                }
                error => AgentStorageCreationError::Failure(
                    KitError::from_state_store(error, "agent identity could not be checked")
                        .in_phase(crate::error::ErrorPhase::StateRead),
                ),
            })?
            .is_some()
        {
            return Err(AgentStorageCreationError::Failure(KitError::conflict(
                "agent identity already exists",
            )));
        }
        transaction
            .put(storage_kit::state::record::StateRecord::new(key, value))
            .await
            .map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    AgentStorageCreationError::TransactionConflict
                }
                error => AgentStorageCreationError::Failure(
                    KitError::from_state_store(error, "agent identity could not be stored")
                        .in_phase(crate::error::ErrorPhase::StateWrite),
                ),
            })?;
        Ok(())
    }

    pub(crate) fn store(&self) -> &S {
        &self.inner.store
    }

    pub(crate) async fn stage_knowledge_forget(
        &self,
        transaction: &mut S::Transaction,
        owner: &KnowledgeOwnerId,
    ) -> Result<(), AgentStorageKnowledgeError> {
        let record = KnowledgeForgetRecord::new(owner.clone())
            .map_err(AgentStorageKnowledgeError::Failure)?;
        let key = record.key().map_err(AgentStorageKnowledgeError::Failure)?;
        let current = transaction
            .load(key.clone())
            .await
            .map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    AgentStorageKnowledgeError::TransactionConflict(
                        crate::error::ErrorPhase::StateRead,
                    )
                }
                error => AgentStorageKnowledgeError::Failure(
                    KitError::from_state_store(
                        error,
                        "knowledge forget obligation could not be checked",
                    )
                    .in_phase(crate::error::ErrorPhase::StateRead),
                ),
            })?;
        if let Some(current) = current {
            let current = KnowledgeForgetRecord::decode(current)
                .map_err(AgentStorageKnowledgeError::Failure)?;
            return if current.owner == record.owner {
                Ok(())
            } else {
                Err(AgentStorageKnowledgeError::Failure(
                    KitError::invalid_state(
                        "knowledge forget obligation contradicts its durable key",
                    ),
                ))
            };
        }
        let value = record
            .encode()
            .map_err(AgentStorageKnowledgeError::Failure)?;
        transaction
            .put(storage_kit::state::record::StateRecord::new(key, value))
            .await
            .map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    AgentStorageKnowledgeError::TransactionConflict(
                        crate::error::ErrorPhase::StateWrite,
                    )
                }
                error => AgentStorageKnowledgeError::Failure(
                    KitError::from_state_store(
                        error,
                        "knowledge forget obligation could not be stored",
                    )
                    .in_phase(crate::error::ErrorPhase::StateWrite),
                ),
            })
    }

    pub(crate) async fn settle_knowledge_forget<D: DatabaseBackend>(
        &self,
        knowledge: Option<&ManagedKnowledge<S, D>>,
        owner: &KnowledgeOwnerId,
        deadline: tokio::time::Instant,
        observation_deadline: Option<tokio::time::Instant>,
    ) -> Result<(), KitError> {
        let settlement = async {
            let key = KnowledgeForgetRecord::key_for(owner)?;
            if tokio::time::Instant::now() >= deadline {
                return Err(KitError::timeout(
                    "knowledge reconciliation deadline elapsed",
                ));
            }
            let Some(snapshot) = self.store().load(key.clone()).await.map_err(|error| {
                KitError::from_state_store(error, "knowledge forget obligation could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            else {
                return Ok(());
            };
            let record = KnowledgeForgetRecord::decode(snapshot)?;
            if &record.owner != owner {
                return Err(KitError::invalid_state(
                    "knowledge forget obligation contradicts its owner",
                ));
            }
            let knowledge = knowledge.ok_or_else(|| {
                KitError::conflict(
                    "durable knowledge cleanup requires the supplied ManagedKnowledge",
                )
            })?;
            if tokio::time::Instant::now() >= deadline {
                return Err(KitError::timeout(
                    "knowledge reconciliation deadline elapsed",
                ));
            }
            knowledge.forget(owner.clone()).await?;

            if tokio::time::Instant::now() >= deadline {
                return Err(KitError::timeout(
                    "knowledge reconciliation deadline elapsed",
                ));
            }
            let mut transaction = self.store().begin_transaction().await.map_err(|error| {
                KitError::from_state_store(
                    error,
                    "knowledge forget obligation could not be settled",
                )
                .in_phase(crate::error::ErrorPhase::StateWrite)
            })?;
            let current = transaction.load(key.clone()).await.map_err(|error| {
                KitError::from_state_store(
                    error,
                    "knowledge forget obligation could not be settled",
                )
                .in_phase(crate::error::ErrorPhase::StateRead)
            })?;
            if let Some(current) = current {
                let current = KnowledgeForgetRecord::decode(current)?;
                if current.owner != record.owner {
                    return Err(KitError::conflict(
                        "knowledge forget obligation changed during settlement",
                    ));
                }
                transaction.delete(key).await.map_err(|error| {
                    KitError::from_state_store(
                        error,
                        "knowledge forget obligation could not be settled",
                    )
                    .in_phase(crate::error::ErrorPhase::StateWrite)
                })?;
            }
            transaction.commit().await.map(|_| ()).map_err(|error| {
                KitError::from_state_store(
                    error,
                    "knowledge forget obligation could not be settled",
                )
                .in_phase(crate::error::ErrorPhase::StateCommit)
            })
        };
        match observation_deadline {
            Some(observation_deadline) => tokio::time::timeout_at(observation_deadline, settlement)
                .await
                .map_err(|_| KitError::timeout("knowledge reconciliation deadline elapsed"))?,
            None => settlement.await,
        }
    }

    pub(crate) async fn settle_knowledge_forgets<D: DatabaseBackend>(
        &self,
        knowledge: Option<&ManagedKnowledge<S, D>>,
        agent_id: &AgentId,
        deadline: tokio::time::Instant,
    ) -> Result<(), KitError> {
        let records = self.knowledge_forget_records(agent_id, deadline).await?;
        if !records.is_empty() && knowledge.is_none() {
            return Err(KitError::conflict(
                "durable knowledge cleanup requires the supplied ManagedKnowledge",
            ));
        }
        for record in records {
            self.settle_knowledge_forget(knowledge, &record.owner, deadline, Some(deadline))
                .await?;
        }
        Ok(())
    }

    pub(crate) fn claim_agent(&self, id: &AgentId) -> Result<AgentStorageClaim, KitError> {
        let mut live = self
            .inner
            .live_agents
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !live.insert(id.clone()) {
            return Err(KitError::busy("agent identity is already live"));
        }
        drop(live);
        Ok(AgentStorageClaim {
            live_agents: self.inner.live_agents.clone(),
            id: Some(id.clone()),
            release_on_drop: true,
        })
    }

    async fn load_record(&self, id: &AgentId) -> Result<AgentRecord, KitError> {
        let key = AgentRecord::key(id)?;
        let snapshot = self
            .store()
            .load(key)
            .await
            .map_err(|error| {
                KitError::from_state_store(error, "agent identity could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?
            .ok_or_else(|| KitError::not_found("agent identity does not exist"))?;
        let record = AgentRecord::decode(snapshot)?;
        if &record.id != id {
            return Err(KitError::invalid_state(
                "stored agent identity does not match its key",
            ));
        }
        Ok(record)
    }

    async fn knowledge_forget_records(
        &self,
        agent_id: &AgentId,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<KnowledgeForgetRecord>, KitError> {
        let prefix =
            storage_kit::state::record::StateKeyPrefix::new(format!("{}:", agent_id.as_str()))
                .map_err(|_| KitError::invalid_state("knowledge forget prefix is invalid"))?;
        let mut records = Vec::new();
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        for _ in 0..MAX_KNOWLEDGE_FORGET_PAGES {
            let mut request =
                storage_kit::state::record::StateListRequest::new(KNOWLEDGE_FORGET_NAMESPACE)
                    .map_err(|_| {
                        KitError::invalid_state("knowledge forget inventory request is invalid")
                    })?
                    .with_key_prefix(prefix.clone())
                    .with_limit(storage_kit::state::record::StateListRequest::MAX_LIMIT)
                    .map_err(|_| {
                        KitError::invalid_state("knowledge forget inventory limit is invalid")
                    })?;
            if let Some(next) = cursor.take() {
                request = request.start_after(next).map_err(|_| {
                    KitError::invalid_state("knowledge forget inventory cursor is invalid")
                })?;
            }
            let page = tokio::time::timeout_at(deadline, self.store().list(request))
                .await
                .map_err(|_| KitError::timeout("knowledge reconciliation deadline elapsed"))?
                .map_err(|error| {
                    KitError::from_state_store(
                        error,
                        "knowledge forget inventory could not be loaded",
                    )
                    .in_phase(crate::error::ErrorPhase::StateRead)
                })?;
            if page.records.len() > MAX_KNOWLEDGE_FORGET_RECORDS.saturating_sub(records.len()) {
                return Err(KitError::invalid_state(
                    "knowledge forget inventory exceeds its private bound",
                ));
            }
            for snapshot in page.records {
                let record = KnowledgeForgetRecord::decode(snapshot)?;
                if record.agent_id() != Some(agent_id) {
                    return Err(KitError::invalid_state(
                        "knowledge forget obligation has a foreign owner",
                    ));
                }
                records.push(record);
            }
            let Some(next) = page.next_start_after else {
                return Ok(records);
            };
            if !cursors.insert(next.clone()) {
                return Err(KitError::invalid_state(
                    "knowledge forget inventory repeated its cursor",
                ));
            }
            cursor = Some(next);
        }
        Err(KitError::invalid_state(
            "knowledge forget inventory exceeds its private page bound",
        ))
    }
}

impl AgentStorageCreationError {
    pub(crate) fn into_sdk(self) -> KitError {
        match self {
            Self::TransactionConflict => {
                KitError::conflict("agent identity transaction conflicted")
            }
            Self::Failure(error) => error,
        }
    }
}

impl AgentStorageKnowledgeError {
    pub(crate) fn into_sdk(self) -> KitError {
        match self {
            Self::TransactionConflict(phase) => {
                KitError::conflict("knowledge forget transaction conflicted")
                    .with_detail(crate::error::ErrorDetail::TransactionConflict)
                    .in_phase(phase)
            }
            Self::Failure(error) => error,
        }
    }
}

impl Drop for AgentStorageClaim {
    fn drop(&mut self) {
        if !self.release_on_drop {
            return;
        }
        self.release();
    }
}

impl AgentStorageClaim {
    pub(crate) fn retain_until_shutdown(&mut self) {
        self.release_on_drop = false;
    }

    pub(crate) fn release(&mut self) {
        let Some(id) = self.id.take() else {
            return;
        };
        self.live_agents
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&id);
    }

    pub(crate) fn complete_shutdown(
        &mut self,
        result: Result<(), KitError>,
    ) -> Result<(), KitError> {
        if result.is_ok() {
            self.release();
        }
        result
    }
}

impl AgentRecord {
    fn from_info(info: &AgentInfo) -> Self {
        Self {
            version: AGENT_RECORD_VERSION,
            id: info.id().clone(),
            display_name: info.display_name().map(str::to_owned),
        }
    }

    fn into_info(self) -> Result<AgentInfo, KitError> {
        AgentInfo::new(self.id, self.display_name)
            .map_err(|_| KitError::invalid_state("stored agent identity is malformed"))
    }

    fn key(id: &AgentId) -> Result<storage_kit::state::record::StateKey, KitError> {
        storage_kit::state::record::StateKey::new(AGENT_NAMESPACE, id.as_str())
            .map_err(|_| KitError::invalid_state("agent identity key is invalid"))
    }

    fn decode(snapshot: storage_kit::state::record::StateRecordSnapshot) -> Result<Self, KitError> {
        let key = snapshot.record.key.key().to_owned();
        let record: Self = serde::Deserialize::deserialize(snapshot.record.value.as_ref())
            .map_err(|_| KitError::invalid_state("stored agent identity is malformed"))?;
        if record.version != AGENT_RECORD_VERSION || record.id.as_str() != key {
            return Err(KitError::invalid_state(
                "stored agent identity does not match its durable key",
            ));
        }
        if record
            .display_name
            .as_ref()
            .is_some_and(|name| name.is_empty() || name.trim() != name)
        {
            return Err(KitError::invalid_state(
                "stored agent display name is malformed",
            ));
        }
        Ok(record)
    }
}

impl KnowledgeForgetRecord {
    fn new(owner: KnowledgeOwnerId) -> Result<Self, KitError> {
        if matches!(owner, KnowledgeOwnerId::Database) {
            return Err(KitError::invalid_input(
                "Database knowledge is not owned by an agent hierarchy deletion",
            ));
        }
        Ok(Self {
            version: KNOWLEDGE_FORGET_RECORD_VERSION,
            owner,
        })
    }

    fn key(&self) -> Result<storage_kit::state::record::StateKey, KitError> {
        Self::key_for(&self.owner)
    }

    fn key_for(owner: &KnowledgeOwnerId) -> Result<storage_kit::state::record::StateKey, KitError> {
        let (agent_id, suffix) = match owner {
            KnowledgeOwnerId::Database => {
                return Err(KitError::invalid_input(
                    "Database knowledge has no agent hierarchy key",
                ));
            }
            KnowledgeOwnerId::Agent { agent_id } => (agent_id, "agent".to_owned()),
            KnowledgeOwnerId::Workspace {
                agent_id,
                workspace_id,
            } => (agent_id, format!("workspace:{}", workspace_id.as_str())),
            KnowledgeOwnerId::Conversation {
                agent_id,
                workspace_id,
                conversation_id,
            } => (
                agent_id,
                format!(
                    "conversation:{}:{}",
                    workspace_id.as_str(),
                    conversation_id.as_str()
                ),
            ),
        };
        storage_kit::state::record::StateKey::new(
            KNOWLEDGE_FORGET_NAMESPACE,
            format!("{}:{suffix}", agent_id.as_str()),
        )
        .map_err(|_| KitError::invalid_state("knowledge forget key is invalid"))
    }

    fn agent_id(&self) -> Option<&AgentId> {
        match &self.owner {
            KnowledgeOwnerId::Database => None,
            KnowledgeOwnerId::Agent { agent_id }
            | KnowledgeOwnerId::Workspace { agent_id, .. }
            | KnowledgeOwnerId::Conversation { agent_id, .. } => Some(agent_id),
        }
    }

    fn encode(&self) -> Result<serde_json::Value, KitError> {
        serde_json::to_value(self).map_err(|_| {
            KitError::invalid_state("knowledge forget obligation could not be encoded")
        })
    }

    fn decode(snapshot: storage_kit::state::record::StateRecordSnapshot) -> Result<Self, KitError> {
        let key = snapshot.record.key.clone();
        let record: Self = serde::Deserialize::deserialize(snapshot.record.value.as_ref())
            .map_err(|_| {
                KitError::invalid_state("stored knowledge forget obligation is malformed")
            })?;
        let expected_key = record.key().map_err(|_| {
            KitError::invalid_state("stored knowledge forget obligation has an invalid owner")
        })?;
        if record.version != KNOWLEDGE_FORGET_RECORD_VERSION || expected_key != key {
            return Err(KitError::invalid_state(
                "stored knowledge forget obligation contradicts its key",
            ));
        }
        Ok(record)
    }
}
