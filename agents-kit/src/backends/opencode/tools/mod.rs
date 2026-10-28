use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::backends::opencode::adapters::OpenCodeSelectedModel;
use crate::backends::opencode::protocol::http::OpenCodeHttp;
use crate::backends::opencode::protocol::requests::OpenCodeRouting;
use crate::backends::opencode::protocol::responses::{
    OpenCodeToolDescription, OpenCodeToolIds, OpenCodeToolInventory,
};
use crate::{KitError, ToolInfo};

const MAX_NATIVE_TOOLS: usize = 65_536;

#[derive(Clone)]
pub(crate) struct OpenCodeTools {
    runtime_id: Arc<str>,
    revision: Arc<AtomicU64>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodeCatalogGeneration {
    token: String,
}

pub(crate) struct OpenCodeToolCatalog {
    generation: OpenCodeCatalogGeneration,
    tools: Vec<ToolInfo>,
}

impl OpenCodeTools {
    pub(crate) fn new() -> Self {
        Self {
            runtime_id: Arc::from(uuid::Uuid::new_v4().simple().to_string()),
            revision: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn generation(&self) -> OpenCodeCatalogGeneration {
        OpenCodeCatalogGeneration::new(&self.runtime_id, self.revision.load(Ordering::Acquire))
    }

    pub(crate) fn invalidate(&self) -> Result<(), KitError> {
        self.revision
            .try_update(Ordering::AcqRel, Ordering::Acquire, |revision| {
                revision.checked_add(1)
            })
            .map(|_| ())
            .map_err(|_| KitError::invalid_state("tool catalog generation exhausted"))
    }

    pub(crate) async fn discover(
        &self,
        http: &OpenCodeHttp,
        routing: &OpenCodeRouting,
        selected_model: Option<&OpenCodeSelectedModel>,
    ) -> Result<OpenCodeToolCatalog, KitError> {
        let selected_model = selected_model.ok_or_else(|| {
            KitError::not_yet_implemented(
                "tool inventory requires a concrete selected workspace model",
            )
        })?;
        let generation = self.generation();

        if http.mcp_statuses(routing).await?.has_connected() {
            return Err(KitError::not_yet_implemented(
                "connected native tool sources lack a complete selected inventory route",
            ));
        }
        let first_ids = self.validate_ids(http.tool_ids(routing).await?)?;
        let descriptions = http.tools(routing, selected_model.native()).await?;
        let second_ids = self.validate_ids(http.tool_ids(routing).await?)?;
        if first_ids != second_ids {
            return Err(KitError::conflict(
                "tool inventory changed during discovery",
            ));
        }
        if http.mcp_statuses(routing).await?.has_connected() {
            return Err(KitError::not_yet_implemented(
                "connected native tool sources lack a complete selected inventory route",
            ));
        }
        if self.generation() != generation {
            return Err(KitError::conflict(
                "tool catalog was invalidated during discovery",
            ));
        }

        let tools = self.project(first_ids, descriptions)?;
        Ok(OpenCodeToolCatalog { generation, tools })
    }

    fn validate_ids(&self, native: OpenCodeToolIds) -> Result<Vec<String>, KitError> {
        if native.ids().len() > MAX_NATIVE_TOOLS {
            return Err(KitError::integration(
                "tool inventory exceeds its configured bound",
            ));
        }
        let mut ids = native.ids().to_vec();
        if ids.iter().any(|id| id.is_empty() || id.trim() != id) {
            return Err(KitError::protocol(
                "tool inventory contains a malformed name",
            ));
        }
        ids.sort();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(KitError::invalid_state(
                "tool inventory contains duplicate names",
            ));
        }
        Ok(ids)
    }

    fn project(
        &self,
        ids: Vec<String>,
        native: OpenCodeToolInventory,
    ) -> Result<Vec<ToolInfo>, KitError> {
        if native.descriptions().len() > MAX_NATIVE_TOOLS {
            return Err(KitError::integration(
                "tool descriptions exceed their configured bound",
            ));
        }
        let mut descriptions = BTreeMap::new();
        for description in native.descriptions() {
            let name = description.id();
            if name.is_empty() || name.trim() != name {
                return Err(KitError::protocol(
                    "tool description contains a malformed name",
                ));
            }
            if descriptions.insert(name.to_owned(), description).is_some() {
                return Err(KitError::invalid_state(
                    "tool descriptions contain duplicate names",
                ));
            }
        }
        if descriptions.is_empty() {
            return Err(KitError::invalid_state(
                "selected tool descriptions are unexpectedly empty",
            ));
        }
        for name in descriptions.keys() {
            if ids.binary_search(name).is_err() {
                return Err(KitError::invalid_state(
                    "tool description is absent from the candidate inventory",
                ));
            }
        }

        descriptions
            .into_iter()
            .map(|(id, description)| self.project_tool(id, description))
            .collect()
    }

    fn project_tool(
        &self,
        id: String,
        native: &OpenCodeToolDescription,
    ) -> Result<ToolInfo, KitError> {
        let parameters = native.parameters();
        if jsonschema::draft202012::meta::validate(parameters).is_err() {
            return Err(KitError::protocol("tool input schema is malformed"));
        }
        let description = native.description().trim();
        let description = if description.is_empty() {
            if native.description().is_empty() {
                None
            } else {
                return Err(KitError::protocol("tool description is malformed"));
            }
        } else {
            Some(description.to_owned())
        };
        ToolInfo::new(id, description, Some(parameters.clone()))
            .map_err(|_| KitError::protocol("tool projection is malformed"))
    }
}

impl OpenCodeCatalogGeneration {
    fn new(runtime_id: &str, revision: u64) -> Self {
        Self {
            token: format!("{runtime_id}:{revision}"),
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.token
    }
}

impl OpenCodeToolCatalog {
    pub(crate) fn generation(&self) -> &OpenCodeCatalogGeneration {
        &self.generation
    }

    pub(crate) fn into_tools(self) -> Vec<ToolInfo> {
        self.tools
    }
}
