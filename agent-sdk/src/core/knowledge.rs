use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use storage_sdk::{DatabaseBackend, StateStoreBackend, StateTransactionBackend};
use tokio::sync::Semaphore;

use crate::{
    AgentId, AgentStorage, IngestKnowledgeRequest, KnowledgeContext, KnowledgeOwnerId,
    KnowledgePolicy, KnowledgeProvenance, KnowledgeRetrievalRequest, KnowledgeScope,
    KnowledgeSnippet, KnowledgeSource, SdkError,
};

const KNOWLEDGE_MANIFEST_NAMESPACE: &str = "agent_sdk_knowledge_manifests_v1";
const KNOWLEDGE_MANIFEST_VERSION: u32 = 1;
const MAX_KNOWLEDGE_MANIFESTS: usize = storage_sdk::state::record::StateListRequest::MAX_LIMIT;
const MAX_KNOWLEDGE_MANIFEST_PAGES: usize = 64;
const MAX_OWNER_SCOPE_PAGES: usize = 64;
const OWNER_SCOPE_PAGE_CHUNKS: usize = 4_096;
const MAX_DATABASE_SCOPE_CHUNKS: usize = 4_096;
const KNOWLEDGE_COLLECTION: &str = "agent_sdk_managed_knowledge_v1";
const DATABASE_SCOPE_DATABASE: &str = "agent_sdk_shared_database_scope_v1";
const DATABASE_SCOPE_VALUE: &str = "database";
pub(crate) struct ManagedKnowledge<S: StateStoreBackend, D: DatabaseBackend> {
    storage: AgentStorage<S>,
    database: Arc<D>,
    operations: Arc<Semaphore>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum KnowledgePartition {
    Owner(KnowledgeOwnerId),
    Database,
}

#[derive(Clone, Copy)]
enum KnowledgeCollectionSchema {
    Owner,
    Database,
}

#[derive(Clone, Copy)]
enum KnowledgeDatabaseAction {
    EnsureCollection,
    WriteCandidate,
    ReplaceDatabaseScope,
    DeleteCleanup,
    ForgetDatabaseScope,
    ReadOwnerScope,
    ReadDatabaseScope,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct KnowledgeManifest {
    version: u32,
    agent_id: AgentId,
    owner: KnowledgeOwnerId,
    sources: Vec<KnowledgeSourceReference>,
    cleanup: Vec<KnowledgeSourceReference>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct KnowledgeSourceReference {
    source_id: String,
    title: String,
    publication_id: String,
    chunks: Vec<KnowledgeChunkReference>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct KnowledgeChunkReference {
    chunk_id: String,
    content_integrity: String,
}

struct KnowledgeCandidate {
    reference: KnowledgeSourceReference,
    records: Vec<storage_sdk::database::data::DataRecord>,
}

struct KnowledgeStoredChunk {
    source_id: String,
    title: String,
    publication_id: String,
    chunk_id: String,
    text: String,
    content_integrity: String,
}

impl<S, D> Clone for ManagedKnowledge<S, D>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
{
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            database: self.database.clone(),
            operations: self.operations.clone(),
        }
    }
}

impl<S, D> ManagedKnowledge<S, D>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
{
    pub(crate) fn new(storage: AgentStorage<S>, database: D) -> Self {
        Self {
            storage,
            database: Arc::new(database),
            operations: Arc::new(Semaphore::new(1)),
        }
    }

    pub(crate) async fn ingest(
        &self,
        owner: KnowledgeOwnerId,
        request: IngestKnowledgeRequest,
    ) -> Result<(), SdkError> {
        self.ingest_managed(owner, request).await
    }

    pub(crate) async fn retrieve(
        &self,
        request: KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeContext, SdkError> {
        request.validate()?;
        let validation = request.clone();
        let context = self.retrieve_managed(request).await?;
        validation.validate_context(&context)?;
        Ok(context)
    }

    pub(crate) async fn verify(&self, owner: KnowledgeOwnerId) -> Result<(), SdkError> {
        self.verify_managed(owner).await
    }

    pub(crate) async fn forget(&self, owner: KnowledgeOwnerId) -> Result<(), SdkError> {
        self.forget_managed(owner).await
    }

    async fn admit_operation(&self) -> Result<tokio::sync::OwnedSemaphorePermit, SdkError> {
        self.operations
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| SdkError::closed("managed knowledge base is closed"))
    }

    async fn ingest_managed(
        &self,
        owner: KnowledgeOwnerId,
        request: IngestKnowledgeRequest,
    ) -> Result<(), SdkError> {
        request.validate()?;
        let (sources, selected_scope) = request.into_parts();
        let partition = owner.resolve_ingestion_partition(selected_scope)?;
        if sources.is_empty() {
            return Ok(());
        }
        let _operation = self.admit_operation().await?;
        match partition {
            KnowledgePartition::Owner(owner) => self.ingest_owner(owner, sources).await,
            KnowledgePartition::Database => self.ingest_database(sources).await,
        }
    }

    async fn ingest_owner(
        &self,
        owner: KnowledgeOwnerId,
        sources: Vec<KnowledgeSource>,
    ) -> Result<(), SdkError> {
        let locator = owner.owner_locator()?;
        let partition = KnowledgePartition::Owner(owner.clone());
        let candidates = sources
            .into_iter()
            .map(|source| self.candidate(&partition, source))
            .collect::<Result<Vec<_>, SdkError>>()?;
        self.ensure_collection(&locator, KnowledgeCollectionSchema::Owner)
            .await?;
        self.record_candidate_cleanup(
            &owner,
            candidates
                .iter()
                .map(|candidate| candidate.reference.clone())
                .collect(),
        )
        .await?;
        let records = candidates
            .iter()
            .flat_map(|candidate| candidate.records.iter().cloned())
            .collect();
        if let Err(error) = self
            .database
            .data(storage_sdk::database::data::DataRequest::Insert(
                storage_sdk::database::data::InsertRequest::append(locator, records),
            ))
            .await
        {
            let primary = KnowledgeDatabaseAction::WriteCandidate.map_error(error);
            return Err(match self.reconcile_cleanup_owned(&owner).await {
                Ok(_) => primary,
                Err(cleanup) => {
                    primary.with_supplementary(cleanup.at_phase(crate::error::ErrorPhase::Cleanup))
                }
            });
        }
        if let Err(error) = self.publish(owner.clone(), candidates).await {
            return Err(match self.reconcile_cleanup_owned(&owner).await {
                Ok(_) => error,
                Err(cleanup) => {
                    error.with_supplementary(cleanup.at_phase(crate::error::ErrorPhase::Cleanup))
                }
            });
        }
        let _ = self.reconcile_cleanup_owned(&owner).await;
        Ok(())
    }

    async fn ingest_database(&self, sources: Vec<KnowledgeSource>) -> Result<(), SdkError> {
        let locator = KnowledgeOwnerId::Database.database_locator();
        let partition = KnowledgePartition::Database;
        let candidates = sources
            .into_iter()
            .map(|source| self.candidate(&partition, source))
            .collect::<Result<Vec<_>, SdkError>>()?;
        self.ensure_collection(&locator, KnowledgeCollectionSchema::Database)
            .await?;
        let source_ids = candidates
            .iter()
            .map(|candidate| serde_json::Value::String(candidate.reference.source_id.clone()))
            .collect();
        let records = candidates
            .into_iter()
            .flat_map(|candidate| candidate.records)
            .collect();
        let request = storage_sdk::database::data::ReplacePartitionRequest::new(
            locator,
            storage_sdk::database::data::FilterExpr::eq("scope", DATABASE_SCOPE_VALUE).and(
                storage_sdk::database::data::FilterExpr::In {
                    field: "source_id".to_owned(),
                    values: source_ids,
                },
            ),
            "chunk_id",
            records,
        )
        .map_err(|error| KnowledgeDatabaseAction::ReplaceDatabaseScope.map_error(error))?;
        self.database
            .data(storage_sdk::database::data::DataRequest::ReplacePartition(
                request,
            ))
            .await
            .map_err(|error| KnowledgeDatabaseAction::ReplaceDatabaseScope.map_error(error))?;
        Ok(())
    }

    async fn verify_managed(&self, owner: KnowledgeOwnerId) -> Result<(), SdkError> {
        let _operation = self.admit_operation().await?;
        match &owner {
            KnowledgeOwnerId::Database => {
                self.database_chunks().await?;
            }
            KnowledgeOwnerId::Agent { agent_id } => {
                self.verify_agent(agent_id).await?;
            }
            KnowledgeOwnerId::Workspace { .. } | KnowledgeOwnerId::Conversation { .. } => {
                self.verify_owned(&owner).await?;
            }
        }
        Ok(())
    }

    async fn verify_agent(&self, agent_id: &AgentId) -> Result<bool, SdkError> {
        let manifests = self.manifests(agent_id).await?;
        let mut reconciled = false;
        for manifest in manifests {
            reconciled |= self.verify_owned(&manifest.owner).await?;
        }
        Ok(reconciled)
    }

    async fn verify_owned(&self, owner: &KnowledgeOwnerId) -> Result<bool, SdkError> {
        let reconciled = self.reconcile_cleanup_owned(owner).await?;
        let Some(manifest) = self.manifest(owner).await? else {
            return Ok(reconciled);
        };
        manifest.validate(owner)?;
        let locator = owner.owner_locator()?;
        self.ensure_collection(&locator, KnowledgeCollectionSchema::Owner)
            .await?;
        let actual = self.stored_chunks(owner, &manifest.sources).await?;
        manifest.verify_records(actual)?;
        Ok(reconciled)
    }

    async fn retrieve_managed(
        &self,
        request: KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeContext, SdkError> {
        let _operation = self.admit_operation().await?;
        if matches!(request.policy(), KnowledgePolicy::Disabled) {
            return KnowledgeContext::new(Vec::new());
        }
        let owners = request.owner().retrieval_owners(request.policy())?;
        let query = KnowledgeQuery::new(request.query());
        let budget = request.budget();
        let mut snippets = Vec::new();
        let mut used_bytes = 0_usize;
        for owner in owners {
            let active = match &owner {
                KnowledgeOwnerId::Database => self.database_chunks().await?,
                KnowledgeOwnerId::Agent { .. }
                | KnowledgeOwnerId::Workspace { .. }
                | KnowledgeOwnerId::Conversation { .. } => {
                    let Some(manifest) = self.manifest(&owner).await? else {
                        continue;
                    };
                    manifest.validate(&owner)?;
                    let records = self.stored_chunks(&owner, &manifest.sources).await?;
                    manifest.active_chunks(records)?
                }
            };
            for (score, chunk) in query.rank(active) {
                if snippets.len() == budget.chunks() {
                    return KnowledgeContext::new(snippets);
                }
                let Some(next_bytes) = used_bytes.checked_add(chunk.text.len()) else {
                    continue;
                };
                if next_bytes > budget.bytes() {
                    continue;
                }
                used_bytes = next_bytes;
                snippets.push(KnowledgeSnippet::new(
                    chunk.source_id,
                    chunk.title,
                    chunk.text,
                    owner.clone(),
                    Some(score as f64),
                    KnowledgeProvenance::default(),
                    BTreeMap::new(),
                )?);
            }
        }
        KnowledgeContext::new(snippets)
    }

    fn candidate(
        &self,
        partition: &KnowledgePartition,
        source: KnowledgeSource,
    ) -> Result<KnowledgeCandidate, SdkError> {
        let publication_id = uuid::Uuid::new_v4().simple().to_string();
        let (source_id, title, text) = match source {
            KnowledgeSource::Text {
                source_id,
                title,
                text,
            } => (source_id, title, text),
            KnowledgeSource::Bytes {
                source_id,
                title,
                bytes,
                mime_type,
            } => {
                if !mime_type.starts_with("text/") {
                    return Err(SdkError::unsupported(
                        "managed knowledge byte source MIME type is unsupported",
                    ));
                }
                let text = String::from_utf8(bytes).map_err(|_| {
                    SdkError::invalid_input("managed knowledge bytes must contain valid UTF-8")
                })?;
                (source_id, title, text)
            }
        };
        let chunk_id = format!("{source_id}:0:{publication_id}");
        let content_integrity = KnowledgeChunkReference::integrity(&source_id, &title, &text);
        let chunk = KnowledgeStoredChunk {
            source_id,
            title,
            publication_id,
            chunk_id,
            text,
            content_integrity,
        };
        let record = self.data_record(partition, &chunk)?;
        Ok(KnowledgeCandidate {
            reference: KnowledgeSourceReference {
                source_id: chunk.source_id,
                title: chunk.title,
                publication_id: chunk.publication_id,
                chunks: vec![KnowledgeChunkReference {
                    chunk_id: chunk.chunk_id,
                    content_integrity: chunk.content_integrity,
                }],
            },
            records: vec![record],
        })
    }

    fn data_record(
        &self,
        partition: &KnowledgePartition,
        chunk: &KnowledgeStoredChunk,
    ) -> Result<storage_sdk::database::data::DataRecord, SdkError> {
        let (record_agent_id, owner_kind, owner_id) = match partition {
            KnowledgePartition::Owner(owner) => {
                let (owner_kind, owner_id) = owner.storage_identity()?;
                (
                    owner
                        .agent_id()
                        .ok_or_else(|| {
                            SdkError::invalid_state(
                                "managed owner record requires an agent lineage",
                            )
                        })?
                        .as_str(),
                    owner_kind,
                    owner_id,
                )
            }
            KnowledgePartition::Database => (
                DATABASE_SCOPE_DATABASE,
                "database",
                DATABASE_SCOPE_DATABASE.to_owned(),
            ),
        };
        let record = storage_sdk::database::data::DataRecord::without_id()
            .field("agent_id", record_agent_id)
            .and_then(|record| record.field("owner_kind", owner_kind))
            .and_then(|record| record.field("owner_id", owner_id))
            .and_then(|record| record.field("source_id", &chunk.source_id))
            .and_then(|record| record.field("title", &chunk.title))
            .and_then(|record| record.field("publication_id", &chunk.publication_id))
            .and_then(|record| record.field("chunk_id", &chunk.chunk_id))
            .and_then(|record| record.field("text", &chunk.text))
            .map_err(|_| SdkError::integration("managed knowledge record could not be encoded"))?;
        match partition {
            KnowledgePartition::Owner(_) => Ok(record),
            KnowledgePartition::Database => record
                .field("scope", DATABASE_SCOPE_VALUE)
                .and_then(|record| record.field("content_integrity", &chunk.content_integrity))
                .map_err(|_| {
                    SdkError::integration("shared Database knowledge record could not be encoded")
                }),
        }
    }

    async fn publish(
        &self,
        owner: KnowledgeOwnerId,
        candidates: Vec<KnowledgeCandidate>,
    ) -> Result<(), SdkError> {
        let key = self.manifest_key(&owner)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "knowledge publication could not begin")
                .in_phase(crate::error::ErrorPhase::StateWrite)
        })?;
        let mut manifest = match transaction.load(key.clone()).await.map_err(|error| {
            SdkError::from_state_store(error, "knowledge manifest could not be loaded")
                .in_phase(crate::error::ErrorPhase::StateRead)
        })? {
            Some(snapshot) => KnowledgeManifest::decode(snapshot, &owner)?,
            None => KnowledgeManifest::new(owner.clone())?,
        };
        let replacements = candidates
            .into_iter()
            .map(|candidate| candidate.reference)
            .collect::<Vec<_>>();
        let replacement_ids = replacements
            .iter()
            .map(|source| source.source_id.as_str())
            .collect::<BTreeSet<_>>();
        manifest.cleanup.extend(
            manifest
                .sources
                .iter()
                .filter(|source| replacement_ids.contains(source.source_id.as_str()))
                .cloned(),
        );
        let publications = replacements
            .iter()
            .map(|source| source.publication_id.as_str())
            .collect::<BTreeSet<_>>();
        manifest
            .cleanup
            .retain(|source| !publications.contains(source.publication_id.as_str()));
        manifest.replace(replacements)?;
        manifest.normalize_cleanup();
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                manifest.encode()?,
            ))
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "knowledge manifest could not be stored")
                    .in_phase(crate::error::ErrorPhase::StateWrite)
            })?;
        transaction.commit().await.map_err(|error| {
            SdkError::from_state_store(error, "knowledge publication could not commit")
                .in_phase(crate::error::ErrorPhase::StateCommit)
        })?;
        Ok(())
    }

    async fn record_candidate_cleanup(
        &self,
        owner: &KnowledgeOwnerId,
        candidates: Vec<KnowledgeSourceReference>,
    ) -> Result<(), SdkError> {
        let key = self.manifest_key(owner)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "knowledge candidate cleanup could not begin")
                .in_phase(crate::error::ErrorPhase::StateWrite)
        })?;
        let mut manifest = match transaction.load(key.clone()).await.map_err(|error| {
            SdkError::from_state_store(error, "knowledge manifest could not be loaded")
                .in_phase(crate::error::ErrorPhase::StateRead)
        })? {
            Some(snapshot) => KnowledgeManifest::decode(snapshot, owner)?,
            None => KnowledgeManifest::new(owner.clone())?,
        };
        manifest.cleanup.extend(candidates);
        manifest.normalize_cleanup();
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                manifest.encode()?,
            ))
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "knowledge candidate cleanup could not be stored")
                    .in_phase(crate::error::ErrorPhase::StateWrite)
            })?;
        transaction.commit().await.map_err(|error| {
            SdkError::from_state_store(error, "knowledge candidate cleanup could not commit")
                .in_phase(crate::error::ErrorPhase::StateCommit)
        })?;
        Ok(())
    }

    async fn reconcile_cleanup_owned(&self, owner: &KnowledgeOwnerId) -> Result<bool, SdkError> {
        let Some(manifest) = self.manifest(owner).await? else {
            return Ok(false);
        };
        if manifest.cleanup.is_empty() {
            return Ok(false);
        }
        let locator = owner.owner_locator()?;
        self.ensure_collection(&locator, KnowledgeCollectionSchema::Owner)
            .await?;
        let agent_id = owner.agent_id().ok_or_else(|| {
            SdkError::invalid_state("managed cleanup requires an agent owner lineage")
        })?;
        let (owner_kind, owner_id) = owner.storage_identity()?;
        let publications = manifest
            .cleanup
            .iter()
            .map(|source| serde_json::Value::String(source.publication_id.clone()))
            .collect();
        let filter = storage_sdk::database::data::FilterExpr::eq("agent_id", agent_id.as_str())
            .and(storage_sdk::database::data::FilterExpr::eq(
                "owner_kind",
                owner_kind,
            ))
            .and(storage_sdk::database::data::FilterExpr::eq(
                "owner_id",
                owner_id.as_str(),
            ))
            .and(storage_sdk::database::data::FilterExpr::In {
                field: "publication_id".to_owned(),
                values: publications,
            });
        self.database
            .data(storage_sdk::database::data::DataRequest::Delete(
                storage_sdk::database::data::DeleteRequest { locator, filter },
            ))
            .await
            .map_err(|error| KnowledgeDatabaseAction::DeleteCleanup.map_error(error))?;

        let key = self.manifest_key(owner)?;
        let cleaned = manifest
            .cleanup
            .iter()
            .map(|source| source.publication_id.as_str())
            .collect::<BTreeSet<_>>();
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            SdkError::from_state_store(error, "knowledge cleanup settlement could not begin")
                .in_phase(crate::error::ErrorPhase::StateWrite)
        })?;
        let Some(snapshot) = transaction.load(key.clone()).await.map_err(|error| {
            SdkError::from_state_store(error, "knowledge manifest could not be loaded")
                .in_phase(crate::error::ErrorPhase::StateRead)
        })?
        else {
            transaction.rollback().await.map_err(|error| {
                SdkError::from_state_store(
                    error,
                    "knowledge cleanup settlement could not be rolled back",
                )
                .in_phase(crate::error::ErrorPhase::StateWrite)
            })?;
            return Ok(true);
        };
        let mut current = KnowledgeManifest::decode(snapshot, owner)?;
        current
            .cleanup
            .retain(|source| !cleaned.contains(source.publication_id.as_str()));
        if current.sources.is_empty() && current.cleanup.is_empty() {
            transaction.delete(key).await.map_err(|error| {
                SdkError::from_state_store(error, "knowledge manifest could not be removed")
                    .in_phase(crate::error::ErrorPhase::StateWrite)
            })?;
        } else {
            transaction
                .put(storage_sdk::state::record::StateRecord::new(
                    key,
                    current.encode()?,
                ))
                .await
                .map_err(|error| {
                    SdkError::from_state_store(error, "knowledge cleanup could not be settled")
                        .in_phase(crate::error::ErrorPhase::StateWrite)
                })?;
        }
        transaction.commit().await.map_err(|error| {
            SdkError::from_state_store(error, "knowledge cleanup settlement could not commit")
                .in_phase(crate::error::ErrorPhase::StateCommit)
        })?;
        Ok(true)
    }

    async fn manifest(
        &self,
        owner: &KnowledgeOwnerId,
    ) -> Result<Option<KnowledgeManifest>, SdkError> {
        let store = self.storage.store();
        let snapshot = store
            .load(self.manifest_key(owner)?)
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "knowledge manifest could not be loaded")
                    .in_phase(crate::error::ErrorPhase::StateRead)
            })?;
        snapshot
            .map(|snapshot| KnowledgeManifest::decode(snapshot, owner))
            .transpose()
    }

    async fn manifests(&self, agent_id: &AgentId) -> Result<Vec<KnowledgeManifest>, SdkError> {
        let prefix =
            storage_sdk::state::record::StateKeyPrefix::new(format!("{}:", agent_id.as_str()))
                .map_err(|_| SdkError::invalid_state("knowledge manifest prefix is invalid"))?;
        let store = self.storage.store();
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut manifests = Vec::new();
        for _ in 0..MAX_KNOWLEDGE_MANIFEST_PAGES {
            let mut request =
                storage_sdk::state::record::StateListRequest::new(KNOWLEDGE_MANIFEST_NAMESPACE)
                    .map_err(|_| SdkError::invalid_state("knowledge inventory request is invalid"))?
                    .with_key_prefix(prefix.clone())
                    .with_limit(storage_sdk::state::record::StateListRequest::MAX_LIMIT)
                    .map_err(|_| SdkError::invalid_state("knowledge inventory limit is invalid"))?;
            if let Some(next) = cursor.take() {
                request = request.start_after(next).map_err(|_| {
                    SdkError::invalid_state("knowledge inventory cursor is invalid")
                })?;
            }
            let page = store.list(request).await.map_err(|error| {
                SdkError::from_state_store(
                    error,
                    "knowledge manifest inventory could not be loaded",
                )
                .in_phase(crate::error::ErrorPhase::StateRead)
            })?;
            if page.records.len() > MAX_KNOWLEDGE_MANIFESTS - manifests.len() {
                return Err(SdkError::invalid_state(
                    "knowledge manifest inventory exceeds its private record bound",
                ));
            }
            for snapshot in page.records {
                manifests.push(KnowledgeManifest::decode_for_agent(snapshot, agent_id)?);
            }
            let Some(next) = page.next_start_after else {
                return Ok(manifests);
            };
            if !cursors.insert(next.clone()) {
                return Err(SdkError::invalid_state(
                    "knowledge manifest inventory repeated its cursor",
                ));
            }
            cursor = Some(next);
        }
        Err(SdkError::invalid_state(
            "knowledge manifest inventory exceeds its private page bound",
        ))
    }

    fn manifest_key(
        &self,
        owner: &KnowledgeOwnerId,
    ) -> Result<storage_sdk::state::record::StateKey, SdkError> {
        owner.manifest_key()
    }

    async fn ensure_collection(
        &self,
        locator: &storage_sdk::database::StorageLocator,
        schema: KnowledgeCollectionSchema,
    ) -> Result<(), SdkError> {
        self.database
            .catalog(
                storage_sdk::database::catalog::CatalogRequest::CreateCollection(
                    storage_sdk::database::catalog::CreateCollectionRequest {
                        locator: locator.clone(),
                        schema: Some(schema.into_storage_schema()),
                        records: Vec::new(),
                        mode: storage_sdk::database::catalog::CreateCollectionMode::CreateIfMissing,
                    },
                ),
            )
            .await
            .map_err(|error| KnowledgeDatabaseAction::EnsureCollection.map_error(error))?;
        Ok(())
    }

    async fn stored_chunks(
        &self,
        owner: &KnowledgeOwnerId,
        references: &[KnowledgeSourceReference],
    ) -> Result<Vec<KnowledgeStoredChunk>, SdkError> {
        let agent_id = owner.agent_id().ok_or_else(|| {
            SdkError::invalid_state("managed records require an agent owner lineage")
        })?;
        let (owner_kind, owner_id) = owner.storage_identity()?;
        let publications = references
            .iter()
            .map(|source| serde_json::Value::String(source.publication_id.clone()))
            .collect::<Vec<_>>();
        if publications.is_empty() {
            return Ok(Vec::new());
        }
        let expected_chunks = references.iter().try_fold(0_usize, |total, source| {
            total.checked_add(source.chunks.len()).ok_or_else(|| {
                SdkError::invalid_state("managed knowledge reference count overflowed")
            })
        })?;
        let expected_total = u64::try_from(expected_chunks).map_err(|_| {
            SdkError::invalid_state("managed knowledge reference count is not portable")
        })?;
        let record_bound = expected_chunks
            .checked_add(1)
            .ok_or_else(|| SdkError::invalid_state("managed knowledge record bound overflowed"))?;
        let filter = storage_sdk::database::data::FilterExpr::eq("agent_id", agent_id.as_str())
            .and(storage_sdk::database::data::FilterExpr::eq(
                "owner_kind",
                owner_kind,
            ))
            .and(storage_sdk::database::data::FilterExpr::eq(
                "owner_id",
                owner_id.as_str(),
            ))
            .and(storage_sdk::database::data::FilterExpr::In {
                field: "publication_id".to_owned(),
                values: publications,
            });
        let locator = owner.owner_locator()?;
        let mut page_token = None;
        let mut page_tokens = BTreeSet::new();
        let mut records = Vec::with_capacity(expected_chunks.min(OWNER_SCOPE_PAGE_CHUNKS));
        for _ in 0..MAX_OWNER_SCOPE_PAGES {
            let remaining = record_bound.saturating_sub(records.len());
            let mut request = storage_sdk::database::data::ScanRequest::new(locator.clone())
                .filter(filter.clone())
                .limit(remaining.clamp(1, OWNER_SCOPE_PAGE_CHUNKS));
            if let Some(token) = page_token.take() {
                request = request.page_token(token);
            }
            let response = self
                .database
                .data(storage_sdk::database::data::DataRequest::Scan(request))
                .await
                .map_err(|error| KnowledgeDatabaseAction::ReadOwnerScope.map_error(error))?;
            let page = match response {
                storage_sdk::database::data::DataResponse::Records(page) => page,
                _ => {
                    return Err(SdkError::integration(
                        "managed knowledge scan returned an unexpected response",
                    ));
                }
            };
            if page.total.is_some_and(|total| total != expected_total) {
                return Err(SdkError::conflict(
                    "managed knowledge records disagree with durable references",
                ));
            }
            if page.records.len() > record_bound.saturating_sub(records.len()) {
                return Err(SdkError::conflict(
                    "managed knowledge contains rows outside its durable references",
                ));
            }
            records.extend(page.records);
            if records.len() > expected_chunks {
                return Err(SdkError::conflict(
                    "managed knowledge contains rows outside its durable references",
                ));
            }
            let Some(next) = page.next_page else {
                return records
                    .into_iter()
                    .map(|record| {
                        KnowledgeStoredChunk::decode_owner(
                            record,
                            agent_id.as_str(),
                            owner_kind,
                            &owner_id,
                        )
                    })
                    .collect();
            };
            if !page_tokens.insert(next.clone()) {
                return Err(SdkError::invalid_state(
                    "managed knowledge scan repeated its page token",
                ));
            }
            page_token = Some(next);
        }
        Err(SdkError::unsupported(
            "owner-scoped managed knowledge exceeds its bounded scan contract",
        ))
    }

    async fn database_chunks(&self) -> Result<Vec<KnowledgeStoredChunk>, SdkError> {
        let locator = KnowledgeOwnerId::Database.database_locator();
        self.ensure_collection(&locator, KnowledgeCollectionSchema::Database)
            .await?;
        let request = storage_sdk::database::data::ScanRequest::new(locator)
            .filter(storage_sdk::database::data::FilterExpr::eq(
                "scope",
                DATABASE_SCOPE_VALUE,
            ))
            .limit(MAX_DATABASE_SCOPE_CHUNKS + 1);
        let response = self
            .database
            .data(storage_sdk::database::data::DataRequest::Scan(request))
            .await
            .map_err(|error| KnowledgeDatabaseAction::ReadDatabaseScope.map_error(error))?;
        let page = match response {
            storage_sdk::database::data::DataResponse::Records(page) => page,
            _ => {
                return Err(SdkError::integration(
                    "shared Database knowledge scan returned an unexpected response",
                ));
            }
        };
        if page.records.len() > MAX_DATABASE_SCOPE_CHUNKS
            || page.next_page.is_some()
            || page
                .total
                .is_some_and(|total| total > MAX_DATABASE_SCOPE_CHUNKS as u64)
        {
            return Err(SdkError::unsupported(
                "shared Database knowledge exceeds the bounded retrieval contract",
            ));
        }
        let mut publications = BTreeMap::<String, String>::new();
        let mut chunks = BTreeSet::<String>::new();
        let mut decoded = Vec::with_capacity(page.records.len());
        for record in page.records {
            let chunk = KnowledgeStoredChunk::decode_database(record)?;
            if publications
                .insert(chunk.source_id.clone(), chunk.publication_id.clone())
                .is_some_and(|existing| existing != chunk.publication_id)
                || !chunks.insert(chunk.chunk_id.clone())
            {
                return Err(SdkError::conflict(
                    "shared Database knowledge contains contradictory chunk identities",
                ));
            }
            decoded.push(chunk);
        }
        Ok(decoded)
    }

    async fn forget_managed(&self, owner: KnowledgeOwnerId) -> Result<(), SdkError> {
        let _operation = self.admit_operation().await?;
        match &owner {
            KnowledgeOwnerId::Database => self.forget_database().await,
            KnowledgeOwnerId::Agent { .. }
            | KnowledgeOwnerId::Workspace { .. }
            | KnowledgeOwnerId::Conversation { .. } => {
                if self.stage_owner_forget(&owner).await? {
                    self.reconcile_cleanup_owned(&owner).await?;
                }
                Ok(())
            }
        }
    }

    async fn stage_owner_forget(&self, owner: &KnowledgeOwnerId) -> Result<bool, SdkError> {
        let key = self.manifest_key(owner)?;
        let mut transaction = self
            .storage
            .store()
            .begin_transaction()
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "knowledge forget could not begin")
                    .in_phase(crate::error::ErrorPhase::StateWrite)
            })?;
        let Some(snapshot) = transaction.load(key.clone()).await.map_err(|error| {
            SdkError::from_state_store(error, "knowledge manifest could not be loaded")
                .in_phase(crate::error::ErrorPhase::StateRead)
        })?
        else {
            transaction.rollback().await.map_err(|error| {
                SdkError::from_state_store(error, "knowledge forget could not be rolled back")
                    .in_phase(crate::error::ErrorPhase::StateWrite)
            })?;
            return Ok(false);
        };
        let mut manifest = KnowledgeManifest::decode(snapshot, owner)?;
        manifest
            .cleanup
            .extend(std::mem::take(&mut manifest.sources));
        manifest.normalize_cleanup();
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                manifest.encode()?,
            ))
            .await
            .map_err(|error| {
                SdkError::from_state_store(error, "knowledge cleanup could not be recorded")
                    .in_phase(crate::error::ErrorPhase::StateWrite)
            })?;
        transaction.commit().await.map_err(|error| {
            SdkError::from_state_store(error, "knowledge forget could not commit")
                .in_phase(crate::error::ErrorPhase::StateCommit)
        })?;
        Ok(true)
    }

    async fn forget_database(&self) -> Result<(), SdkError> {
        let locator = KnowledgeOwnerId::Database.database_locator();
        self.ensure_collection(&locator, KnowledgeCollectionSchema::Database)
            .await?;
        let request = storage_sdk::database::data::ReplacePartitionRequest::new(
            locator,
            storage_sdk::database::data::FilterExpr::eq("scope", DATABASE_SCOPE_VALUE),
            "chunk_id",
            Vec::new(),
        )
        .map_err(|error| KnowledgeDatabaseAction::ForgetDatabaseScope.map_error(error))?;
        self.database
            .data(storage_sdk::database::data::DataRequest::ReplacePartition(
                request,
            ))
            .await
            .map_err(|error| KnowledgeDatabaseAction::ForgetDatabaseScope.map_error(error))?;
        Ok(())
    }
}

impl KnowledgeOwnerId {
    fn resolve_ingestion_partition(
        &self,
        selected: Option<KnowledgeScope>,
    ) -> Result<KnowledgePartition, SdkError> {
        match self.ingestion_owner(selected)? {
            Self::Database => Ok(KnowledgePartition::Database),
            owner @ (Self::Agent { .. } | Self::Workspace { .. } | Self::Conversation { .. }) => {
                Ok(KnowledgePartition::Owner(owner))
            }
        }
    }

    fn agent_id(&self) -> Option<&AgentId> {
        match self {
            Self::Database => None,
            Self::Agent { agent_id }
            | Self::Workspace { agent_id, .. }
            | Self::Conversation { agent_id, .. } => Some(agent_id),
        }
    }

    fn owner_locator(&self) -> Result<storage_sdk::database::StorageLocator, SdkError> {
        let agent_id = self.agent_id().ok_or_else(|| {
            SdkError::invalid_state("Database scope uses its fixed managed locator")
        })?;
        Ok(
            storage_sdk::database::StorageLocator::new(KNOWLEDGE_COLLECTION)
                .with_namespace(["agent_sdk"])
                .with_database(agent_id.as_str()),
        )
    }

    fn database_locator(&self) -> storage_sdk::database::StorageLocator {
        storage_sdk::database::StorageLocator::new(KNOWLEDGE_COLLECTION)
            .with_namespace(["agent_sdk"])
            .with_database(DATABASE_SCOPE_DATABASE)
    }

    fn manifest_key(&self) -> Result<storage_sdk::state::record::StateKey, SdkError> {
        let agent_id = self.agent_id().ok_or_else(|| {
            SdkError::invalid_state("Database scope has no managed knowledge manifest")
        })?;
        storage_sdk::state::record::StateKey::new(
            KNOWLEDGE_MANIFEST_NAMESPACE,
            format!("{}:{}", agent_id.as_str(), self.key_suffix()?),
        )
        .map_err(|_| SdkError::invalid_state("knowledge manifest key is invalid"))
    }

    fn key_suffix(&self) -> Result<String, SdkError> {
        match self {
            Self::Database => Err(SdkError::invalid_state(
                "Database scope has no managed knowledge manifest",
            )),
            Self::Agent { .. } => Ok("agent".to_owned()),
            Self::Workspace { workspace_id, .. } => {
                Ok(format!("workspace:{}", workspace_id.as_str()))
            }
            Self::Conversation {
                workspace_id,
                conversation_id,
                ..
            } => Ok(format!(
                "conversation:{}:{}",
                workspace_id.as_str(),
                conversation_id.as_str()
            )),
        }
    }

    fn storage_identity(&self) -> Result<(&'static str, String), SdkError> {
        match self {
            Self::Database => Err(SdkError::invalid_state(
                "Database scope uses its fixed managed partition",
            )),
            Self::Agent { agent_id } => Ok(("agent", agent_id.as_str().to_owned())),
            Self::Workspace {
                agent_id,
                workspace_id,
            } => Ok((
                "workspace",
                format!("{}:{}", agent_id.as_str(), workspace_id.as_str()),
            )),
            Self::Conversation {
                agent_id,
                workspace_id,
                conversation_id,
            } => Ok((
                "conversation",
                format!(
                    "{}:{}:{}",
                    agent_id.as_str(),
                    workspace_id.as_str(),
                    conversation_id.as_str()
                ),
            )),
        }
    }
}

impl KnowledgeManifest {
    fn new(owner: KnowledgeOwnerId) -> Result<Self, SdkError> {
        let agent_id = owner.agent_id().cloned().ok_or_else(|| {
            SdkError::invalid_state("Database scope has no managed knowledge manifest")
        })?;
        Ok(Self {
            version: KNOWLEDGE_MANIFEST_VERSION,
            agent_id,
            owner,
            sources: Vec::new(),
            cleanup: Vec::new(),
        })
    }

    fn replace(
        &mut self,
        replacements: impl IntoIterator<Item = KnowledgeSourceReference>,
    ) -> Result<(), SdkError> {
        let replacements = replacements.into_iter().collect::<Vec<_>>();
        let ids = replacements
            .iter()
            .map(|source| source.source_id.as_str())
            .collect::<BTreeSet<_>>();
        if ids.len() != replacements.len() {
            return Err(SdkError::invalid_input(
                "knowledge source ids must be unique within one publication",
            ));
        }
        self.sources
            .retain(|source| !ids.contains(source.source_id.as_str()));
        self.sources.extend(replacements);
        self.sources
            .sort_by(|left, right| left.source_id.cmp(&right.source_id));
        self.validate(&self.owner)
    }

    fn normalize_cleanup(&mut self) {
        self.cleanup.sort_by(|left, right| {
            left.publication_id
                .cmp(&right.publication_id)
                .then_with(|| left.source_id.cmp(&right.source_id))
        });
        self.cleanup
            .dedup_by(|left, right| left.publication_id == right.publication_id);
    }

    fn validate(&self, owner: &KnowledgeOwnerId) -> Result<(), SdkError> {
        let owner_agent_id = owner.agent_id().ok_or_else(|| {
            SdkError::invalid_state("Database scope has no managed knowledge manifest")
        })?;
        if self.version != KNOWLEDGE_MANIFEST_VERSION
            || &self.agent_id != owner_agent_id
            || &self.owner != owner
        {
            return Err(SdkError::invalid_state(
                "stored knowledge manifest contradicts its owner",
            ));
        }
        let mut source_ids = BTreeSet::new();
        for source in &self.sources {
            if !source_ids.insert(source.source_id.as_str()) {
                return Err(SdkError::invalid_state(
                    "stored knowledge source reference is malformed",
                ));
            }
        }
        let mut publications = BTreeSet::new();
        let mut chunks = BTreeSet::new();
        for source in self.sources.iter().chain(&self.cleanup) {
            source.validate()?;
            if !publications.insert(source.publication_id.as_str()) {
                return Err(SdkError::invalid_state(
                    "stored knowledge source reference is malformed",
                ));
            }
            for chunk in &source.chunks {
                if !chunks.insert(chunk.chunk_id.as_str()) {
                    return Err(SdkError::invalid_state(
                        "stored knowledge chunk reference is malformed",
                    ));
                }
            }
        }
        Ok(())
    }

    fn encode(&self) -> Result<serde_json::Value, SdkError> {
        serde_json::to_value(self)
            .map_err(|_| SdkError::invalid_state("knowledge manifest could not be encoded"))
    }

    fn decode(
        snapshot: storage_sdk::state::record::StateRecordSnapshot,
        owner: &KnowledgeOwnerId,
    ) -> Result<Self, SdkError> {
        let manifest: Self = serde::Deserialize::deserialize(snapshot.record.value.as_ref())
            .map_err(|_| SdkError::invalid_state("stored knowledge manifest is malformed"))?;
        manifest.validate(owner)?;
        Ok(manifest)
    }

    fn decode_for_agent(
        snapshot: storage_sdk::state::record::StateRecordSnapshot,
        agent_id: &AgentId,
    ) -> Result<Self, SdkError> {
        let key = snapshot.record.key.key().to_owned();
        let manifest: Self = serde::Deserialize::deserialize(snapshot.record.value.as_ref())
            .map_err(|_| SdkError::invalid_state("stored knowledge manifest is malformed"))?;
        manifest.validate(&manifest.owner)?;
        if manifest.owner.agent_id() != Some(agent_id) {
            return Err(SdkError::invalid_state(
                "stored knowledge manifest belongs to a foreign agent",
            ));
        }
        let expected = format!("{}:{}", agent_id.as_str(), manifest.owner.key_suffix()?);
        if key != expected {
            return Err(SdkError::invalid_state(
                "stored knowledge manifest key contradicts its owner",
            ));
        }
        Ok(manifest)
    }

    fn verify_records(&self, records: Vec<KnowledgeStoredChunk>) -> Result<(), SdkError> {
        self.active_chunks(records).map(|_| ())
    }

    fn active_chunks(
        &self,
        records: Vec<KnowledgeStoredChunk>,
    ) -> Result<Vec<KnowledgeStoredChunk>, SdkError> {
        let mut indexed = BTreeMap::new();
        for record in records {
            let identity = (record.publication_id.clone(), record.chunk_id.clone());
            if indexed.insert(identity, record).is_some() {
                return Err(SdkError::conflict(
                    "managed knowledge contains duplicate durable chunk identities",
                ));
            }
        }
        let mut active = Vec::new();
        for source in &self.sources {
            for reference in &source.chunks {
                let record = indexed
                    .remove(&(source.publication_id.clone(), reference.chunk_id.clone()))
                    .ok_or_else(|| {
                        SdkError::conflict("managed knowledge records are missing from Database")
                    })?;
                if record.source_id != source.source_id
                    || record.title != source.title
                    || record.content_integrity != reference.content_integrity
                {
                    return Err(SdkError::conflict(
                        "managed knowledge records disagree with durable references",
                    ));
                }
                active.push(record);
            }
        }
        if !indexed.is_empty() {
            return Err(SdkError::conflict(
                "managed knowledge contains rows outside its durable references",
            ));
        }
        Ok(active)
    }
}

impl KnowledgeSourceReference {
    fn validate(&self) -> Result<(), SdkError> {
        if self.source_id.is_empty()
            || self.source_id.trim() != self.source_id
            || self.title.is_empty()
            || self.title.trim() != self.title
            || !Self::publication_id_is_generated(&self.publication_id)
            || self.chunks.is_empty()
        {
            return Err(SdkError::invalid_state(
                "stored knowledge source reference is malformed",
            ));
        }
        for (index, chunk) in self.chunks.iter().enumerate() {
            chunk.validate(&format!(
                "{}:{index}:{}",
                self.source_id, self.publication_id
            ))?;
        }
        Ok(())
    }

    fn publication_id_is_generated(publication_id: &str) -> bool {
        if publication_id.len() != 32 {
            return false;
        }
        let bytes = publication_id.as_bytes();
        bytes
            .iter()
            .copied()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            && bytes[12] == b'4'
            && matches!(bytes[16], b'8' | b'9' | b'a' | b'b')
    }
}

impl KnowledgeChunkReference {
    fn validate(&self, expected_chunk_id: &str) -> Result<(), SdkError> {
        let integrity_is_generated = self.content_integrity.len() == 32
            && self
                .content_integrity
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'));
        if self.chunk_id != expected_chunk_id || !integrity_is_generated {
            return Err(SdkError::invalid_state(
                "stored knowledge chunk reference is malformed",
            ));
        }
        Ok(())
    }

    fn integrity(source_id: &str, title: &str, text: &str) -> String {
        let mut first = 0xcbf2_9ce4_8422_2325_u64;
        let mut second = 0x8422_2325_cbf2_9ce4_u64;
        for component in [source_id.as_bytes(), title.as_bytes(), text.as_bytes()] {
            for byte in component.len().to_be_bytes().iter().chain(component) {
                first ^= u64::from(*byte);
                first = first.wrapping_mul(0x0000_0100_0000_01b3);
                second ^= u64::from(*byte).rotate_left(1);
                second = second.rotate_left(7).wrapping_mul(0x9e37_79b1_85eb_ca87);
            }
        }
        format!("{first:016x}{second:016x}")
    }
}

impl Clone for KnowledgeStoredChunk {
    fn clone(&self) -> Self {
        Self {
            source_id: self.source_id.clone(),
            title: self.title.clone(),
            publication_id: self.publication_id.clone(),
            chunk_id: self.chunk_id.clone(),
            text: self.text.clone(),
            content_integrity: self.content_integrity.clone(),
        }
    }
}

impl KnowledgeStoredChunk {
    fn decode_owner(
        record: storage_sdk::database::data::DataRecord,
        agent_id: &str,
        owner_kind: &str,
        owner_id: &str,
    ) -> Result<Self, SdkError> {
        let identity_matches = record
            .fields
            .get("agent_id")
            .and_then(serde_json::Value::as_str)
            == Some(agent_id)
            && record
                .fields
                .get("owner_kind")
                .and_then(serde_json::Value::as_str)
                == Some(owner_kind)
            && record
                .fields
                .get("owner_id")
                .and_then(serde_json::Value::as_str)
                == Some(owner_id);
        if !identity_matches {
            return Err(SdkError::conflict(
                "managed knowledge record contradicts its owner lineage",
            ));
        }
        Self::decode(record)
    }

    fn decode_database(record: storage_sdk::database::data::DataRecord) -> Result<Self, SdkError> {
        let fixed_identity = record
            .fields
            .get("agent_id")
            .and_then(serde_json::Value::as_str)
            == Some(DATABASE_SCOPE_DATABASE)
            && record
                .fields
                .get("owner_kind")
                .and_then(serde_json::Value::as_str)
                == Some("database")
            && record
                .fields
                .get("owner_id")
                .and_then(serde_json::Value::as_str)
                == Some(DATABASE_SCOPE_DATABASE)
            && record
                .fields
                .get("scope")
                .and_then(serde_json::Value::as_str)
                == Some(DATABASE_SCOPE_VALUE);
        if !fixed_identity {
            return Err(SdkError::conflict(
                "shared Database knowledge record contradicts its fixed scope",
            ));
        }
        let embedded_integrity = Self::required(&record.fields, "content_integrity")?;
        let chunk = Self::decode(record)?;
        if embedded_integrity.len() != 32
            || !embedded_integrity
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || embedded_integrity != chunk.content_integrity
        {
            return Err(SdkError::conflict(
                "shared Database knowledge record failed its integrity check",
            ));
        }
        let expected_chunk_id = format!("{}:0:{}", chunk.source_id, chunk.publication_id);
        if chunk.source_id.trim() != chunk.source_id
            || chunk.title.trim() != chunk.title
            || !KnowledgeSourceReference::publication_id_is_generated(&chunk.publication_id)
            || chunk.chunk_id != expected_chunk_id
        {
            return Err(SdkError::conflict(
                "shared Database knowledge record has a contradictory chunk identity",
            ));
        }
        Ok(chunk)
    }

    fn decode(record: storage_sdk::database::data::DataRecord) -> Result<Self, SdkError> {
        let fields = record.fields;
        let source_id = Self::required(&fields, "source_id")?;
        let title = Self::required(&fields, "title")?;
        let publication_id = Self::required(&fields, "publication_id")?;
        let chunk_id = Self::required(&fields, "chunk_id")?;
        let text = Self::required(&fields, "text")?;
        let content_integrity = KnowledgeChunkReference::integrity(&source_id, &title, &text);
        Ok(Self {
            source_id,
            title,
            publication_id,
            chunk_id,
            text,
            content_integrity,
        })
    }

    fn required(
        fields: &serde_json::Map<String, serde_json::Value>,
        field: &'static str,
    ) -> Result<String, SdkError> {
        fields
            .get(field)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| {
                SdkError::conflict("managed knowledge record disagrees with durable references")
            })
    }
}

struct KnowledgeQuery {
    terms: Vec<String>,
}

impl KnowledgeQuery {
    fn new(text: &str) -> Self {
        let terms = text
            .split(|character: char| !character.is_alphanumeric() && character != '_')
            .filter(|term| term.len() >= 3)
            .map(str::to_lowercase)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Self { terms }
    }

    fn rank(&self, records: Vec<KnowledgeStoredChunk>) -> Vec<(usize, KnowledgeStoredChunk)> {
        let mut ranked = records
            .into_iter()
            .filter(|record| !record.text.trim().is_empty())
            .filter_map(|record| {
                let searchable =
                    format!("{} {} {}", record.source_id, record.title, record.text).to_lowercase();
                let score = self
                    .terms
                    .iter()
                    .filter(|term| searchable.contains(term.as_str()))
                    .count();
                (score != 0).then_some((score, record))
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|(left_score, left), (right_score, right)| {
            right_score
                .cmp(left_score)
                .then_with(|| left.source_id.cmp(&right.source_id))
                .then_with(|| left.chunk_id.cmp(&right.chunk_id))
        });
        ranked
    }
}

impl KnowledgeCollectionSchema {
    fn into_storage_schema(self) -> storage_sdk::database::catalog::CollectionSchema {
        let mut fields = vec![
            "agent_id",
            "owner_kind",
            "owner_id",
            "source_id",
            "title",
            "publication_id",
            "chunk_id",
            "text",
        ];
        if matches!(self, Self::Database) {
            fields.extend(["scope", "content_integrity"]);
        }
        storage_sdk::database::catalog::CollectionSchema {
            fields: fields
                .into_iter()
                .map(|name| storage_sdk::database::catalog::FieldSchema {
                    name: name.to_owned(),
                    field_type: storage_sdk::database::catalog::FieldType::String,
                    nullable: false,
                    metadata: BTreeMap::new(),
                })
                .collect(),
            metadata: BTreeMap::new(),
        }
    }
}

impl KnowledgeDatabaseAction {
    fn map_error(self, error: storage_sdk::StorageError) -> SdkError {
        match error {
            storage_sdk::StorageError::UnsupportedCapability { .. } => {
                SdkError::unsupported(self.unsupported_message())
            }
            storage_sdk::StorageError::NotYetImplemented { .. } => {
                SdkError::not_yet_implemented(self.not_yet_implemented_message())
            }
            storage_sdk::StorageError::InvalidConfig(_)
            | storage_sdk::StorageError::InvalidRequest(_) => {
                SdkError::integration(self.integration_message())
                    .with_detail(crate::error::ErrorDetail::InvalidData)
            }
            storage_sdk::StorageError::Serialization(_) => {
                SdkError::integration(self.integration_message())
                    .with_detail(crate::error::ErrorDetail::MalformedData)
            }
            storage_sdk::StorageError::Io(_) => SdkError::integration(self.integration_message())
                .with_detail(crate::error::ErrorDetail::TransportFailure),
            storage_sdk::StorageError::BackendOperation { .. } => {
                SdkError::integration(self.integration_message())
            }
        }
        .in_phase(match self {
            Self::ReadOwnerScope | Self::ReadDatabaseScope => {
                crate::error::ErrorPhase::KnowledgeRead
            }
            Self::EnsureCollection
            | Self::WriteCandidate
            | Self::ReplaceDatabaseScope
            | Self::DeleteCleanup
            | Self::ForgetDatabaseScope => crate::error::ErrorPhase::KnowledgeWrite,
        })
    }

    const fn unsupported_message(self) -> &'static str {
        match self {
            Self::EnsureCollection => {
                "managed knowledge collection setup is unsupported by the supplied Database"
            }
            Self::WriteCandidate => {
                "managed knowledge publication is unsupported by the supplied Database"
            }
            Self::ReplaceDatabaseScope => {
                "atomic shared Database knowledge replacement is unsupported"
            }
            Self::DeleteCleanup => {
                "managed knowledge cleanup is unsupported by the supplied Database"
            }
            Self::ForgetDatabaseScope => "atomic shared Database knowledge removal is unsupported",
            Self::ReadOwnerScope => {
                "owner-scoped managed knowledge reads are unsupported by the supplied Database"
            }
            Self::ReadDatabaseScope => {
                "shared Database knowledge reads are unsupported by the supplied Database"
            }
        }
    }

    const fn not_yet_implemented_message(self) -> &'static str {
        match self {
            Self::EnsureCollection => "managed knowledge collection setup is not implemented",
            Self::WriteCandidate => "managed knowledge publication is not implemented",
            Self::ReplaceDatabaseScope => {
                "atomic shared Database knowledge replacement is not implemented"
            }
            Self::DeleteCleanup => "managed knowledge cleanup is not implemented",
            Self::ForgetDatabaseScope => {
                "atomic shared Database knowledge removal is not implemented"
            }
            Self::ReadOwnerScope => "owner-scoped managed knowledge reads are not implemented",
            Self::ReadDatabaseScope => "shared Database knowledge reads are not implemented",
        }
    }

    const fn integration_message(self) -> &'static str {
        match self {
            Self::EnsureCollection => "managed knowledge collection is unavailable",
            Self::WriteCandidate => "managed knowledge candidate write failed",
            Self::ReplaceDatabaseScope => "shared Database knowledge replacement failed atomically",
            Self::DeleteCleanup => "managed knowledge cleanup failed",
            Self::ForgetDatabaseScope => "shared Database knowledge removal failed atomically",
            Self::ReadOwnerScope => "managed knowledge records could not be read",
            Self::ReadDatabaseScope => "shared Database knowledge could not be read",
        }
    }
}
