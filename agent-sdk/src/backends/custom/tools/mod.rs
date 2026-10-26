use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use provider_sdk::generation::{FunctionCall, JsonSchema, ProviderFunctionTool, ProviderToolSpec};
use tool_sdk::ToolsetBackend;
use tool_sdk::declaration::ToolName;

use crate::backends::custom::state_store::records::{CustomCatalogFloor, CustomToolShape};
use crate::error::{ErrorDetail, ErrorPhase, Operation};
use crate::{SdkError, ToolInfo, ToolSelection};

const MAX_APPROVAL_SCHEMA_BYTES: usize = 1 << 20;
const MAX_APPROVAL_ARGUMENT_BYTES: usize = 64 << 10;
const MAX_APPROVAL_VALUE_NODES: usize = 65_536;
const MAX_APPROVAL_NESTING_DEPTH: usize = 64;

pub(crate) struct CustomTools<T: ToolsetBackend> {
    inner: Arc<CustomToolsInner<T>>,
}

struct CustomToolsInner<T: ToolsetBackend> {
    catalog: Vec<ToolInfo>,
    floor: CustomCatalogFloor,
    executable_names: Vec<String>,
    provider_tools: Vec<ProviderFunctionTool>,
    routes: BTreeMap<String, usize>,
    toolsets: Mutex<Option<Vec<Arc<T>>>>,
}

pub(super) struct CustomToolCatalog {
    entries: Vec<ToolInfo>,
    floor: CustomCatalogFloor,
    executable_names: Vec<String>,
    provider_tools: Vec<ProviderFunctionTool>,
    routes: BTreeMap<String, usize>,
}

impl<T: ToolsetBackend> CustomTools<T> {
    pub(super) fn new(toolsets: Vec<Arc<T>>, catalog: CustomToolCatalog) -> Self {
        Self {
            inner: Arc::new(CustomToolsInner {
                catalog: catalog.entries,
                floor: catalog.floor,
                executable_names: catalog.executable_names,
                provider_tools: catalog.provider_tools,
                routes: catalog.routes,
                toolsets: Mutex::new(Some(toolsets)),
            }),
        }
    }

    pub(crate) fn floor(&self) -> &CustomCatalogFloor {
        &self.inner.floor
    }

    pub(crate) fn catalog(&self) -> Vec<ToolInfo> {
        self.inner.catalog.clone()
    }

    pub(crate) fn has_actions(&self) -> bool {
        !self.inner.catalog.is_empty()
    }

    pub(crate) fn select(
        &self,
        requested: Option<&ToolSelection>,
        configured: &ToolSelection,
    ) -> Result<ToolSelection, SdkError> {
        let selection = requested.unwrap_or(configured);
        selection.validate()?;
        match selection {
            ToolSelection::Auto | ToolSelection::None => Ok(selection.clone()),
            ToolSelection::Required => Ok(selection.clone()),
            ToolSelection::Allowed(names) => {
                self.require_executable_names(names)?;
                Ok(selection.clone())
            }
            ToolSelection::RequiredOneOf(names) => {
                self.require_executable_names(names)?;
                Ok(selection.clone())
            }
        }
    }

    pub(crate) fn provider_tools(&self, selection: &ToolSelection) -> Vec<ProviderToolSpec> {
        match selection {
            ToolSelection::None => Vec::new(),
            ToolSelection::Allowed(names) | ToolSelection::RequiredOneOf(names) => self
                .inner
                .provider_tools
                .iter()
                .filter(|function| names.iter().any(|name| name == function.name()))
                .cloned()
                .map(ProviderToolSpec::Function)
                .collect(),
            ToolSelection::Auto | ToolSelection::Required => self
                .inner
                .provider_tools
                .iter()
                .cloned()
                .map(ProviderToolSpec::Function)
                .collect(),
        }
    }

    pub(crate) fn validate_calls(
        &self,
        selection: &ToolSelection,
        calls: &[FunctionCall],
    ) -> Result<(), SdkError> {
        for call in calls {
            let selected = match selection {
                ToolSelection::Auto | ToolSelection::Required => true,
                ToolSelection::None => false,
                ToolSelection::Allowed(names) | ToolSelection::RequiredOneOf(names) => {
                    names.iter().any(|name| name == call.name())
                }
            };
            if !selected || !self.inner.routes.contains_key(call.name()) {
                return Err(SdkError::protocol(
                    "model requested a tool outside the effective selection",
                )
                .with_detail(ErrorDetail::InvalidData)
                .with_context(Operation::Prompt, ErrorPhase::CompletionValidation));
            }
        }
        Ok(())
    }

    pub(crate) async fn handler(&self, name: &str) -> Result<T::Handler, SdkError> {
        let index = self
            .inner
            .routes
            .get(name)
            .copied()
            .ok_or_else(|| SdkError::invalid_input("model requested an unavailable tool"))?;
        let toolset = self
            .inner
            .toolsets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .and_then(|toolsets| toolsets.get(index))
            .cloned()
            .ok_or_else(|| SdkError::closed("tool catalog is closed"))?;
        let name = ToolName::new(name.to_owned()).map_err(SdkError::from)?;
        toolset.tool_handler(name).await.map_err(SdkError::from)
    }

    pub(crate) async fn present_approval_arguments(
        &self,
        visible_name: &str,
        arguments: &serde_json::Value,
    ) -> Result<Option<String>, SdkError> {
        if !Self::value_is_within_bound(arguments, MAX_APPROVAL_ARGUMENT_BYTES) {
            return Err(SdkError::integration(
                "tool approval arguments exceed the supported limit",
            ));
        }
        let tools = self.clone();
        let visible_name = visible_name.to_owned();
        let arguments = arguments.clone();
        tokio::task::spawn_blocking(move || {
            tools.present_approval_arguments_blocking(&visible_name, &arguments)
        })
        .await
        .map_err(|_| {
            SdkError::integration("tool approval presentation task failed")
                .with_detail(crate::error::ErrorDetail::TaskTerminated)
        })?
    }

    fn present_approval_arguments_blocking(
        &self,
        visible_name: &str,
        arguments: &serde_json::Value,
    ) -> Result<Option<String>, SdkError> {
        let encoded = serde_json::to_vec(arguments).map_err(|_| {
            SdkError::integration("tool approval arguments cannot be encoded safely")
        })?;
        if encoded.len() > MAX_APPROVAL_ARGUMENT_BYTES {
            return Err(SdkError::integration(
                "tool approval arguments exceed the supported limit",
            ));
        }
        let schema = self
            .inner
            .catalog
            .iter()
            .find(|tool| tool.name() == visible_name)
            .ok_or_else(|| SdkError::integration("tool approval catalog entry is unavailable"))?
            .input_schema()
            .ok_or_else(|| SdkError::integration("tool approval schema is unavailable"))?;
        let presentable = Self::approval_schema_is_presentable(schema)?;
        let validator = jsonschema::draft202012::options()
            .build(schema)
            .map_err(|_| SdkError::integration("tool approval schema is unusable"))?;
        if validator.validate(arguments).is_err() {
            return Err(SdkError::integration(
                "tool approval arguments do not match their schema",
            ));
        }
        if !presentable || !Self::approval_schema_covers_value(schema, arguments) {
            return Ok(None);
        }
        let encoded = serde_json::to_string(arguments).map_err(|_| {
            SdkError::integration("tool approval arguments cannot be encoded safely")
        })?;
        if encoded.len() > MAX_APPROVAL_ARGUMENT_BYTES {
            return Err(SdkError::integration(
                "tool approval arguments exceed the supported limit",
            ));
        }
        Ok(Some(encoded))
    }

    pub(crate) async fn shutdown(&self) -> Result<(), SdkError> {
        let toolsets = self
            .inner
            .toolsets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let Some(mut toolsets) = toolsets else {
            return Ok(());
        };
        let mut first_error: Option<SdkError> = None;
        while let Some(toolset) = toolsets.pop() {
            if let Err(error) = toolset.shutdown().await {
                let error = SdkError::from(error).at_phase(crate::error::ErrorPhase::Cleanup);
                first_error = Some(match first_error.take() {
                    Some(primary) => primary.with_supplementary(error),
                    None => error,
                });
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn require_executable_names(&self, names: &[String]) -> Result<(), SdkError> {
        let available = self
            .inner
            .executable_names
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if let Some(_missing) = names.iter().find(|name| !available.contains(name.as_str())) {
            return Err(SdkError::invalid_input(
                "tool selection contains an unavailable executable name",
            ));
        }
        Ok(())
    }

    fn approval_schema_is_presentable(schema: &serde_json::Value) -> Result<bool, SdkError> {
        if !schema.is_object()
            || !Self::value_is_within_bound(schema, MAX_APPROVAL_SCHEMA_BYTES)
            || serde_json::to_vec(schema)
                .ok()
                .is_none_or(|encoded| encoded.len() > MAX_APPROVAL_SCHEMA_BYTES)
        {
            return Err(SdkError::integration(
                "tool approval schema exceeds the supported limit",
            ));
        }
        if jsonschema::draft202012::meta::validate(schema).is_err() {
            return Err(SdkError::integration("tool approval schema is unusable"));
        }
        let mut pending = vec![schema];
        let mut visited = 0_usize;
        let mut presentable = true;
        while let Some(value) = pending.pop() {
            visited += 1;
            if visited > MAX_APPROVAL_VALUE_NODES {
                return Err(SdkError::integration(
                    "tool approval schema exceeds the supported limit",
                ));
            }
            match value {
                serde_json::Value::Array(values) => pending.extend(values),
                serde_json::Value::Object(values) => {
                    if values.get("writeOnly").and_then(serde_json::Value::as_bool) == Some(true)
                        || values.get("format").and_then(serde_json::Value::as_str)
                            == Some("password")
                    {
                        presentable = false;
                    }
                    for reference_key in ["$ref", "$dynamicRef"] {
                        if let Some(reference) = values.get(reference_key)
                            && reference
                                .as_str()
                                .is_none_or(|reference| !reference.starts_with('#'))
                        {
                            return Err(SdkError::integration(
                                "tool approval schema is not self-contained",
                            ));
                        }
                    }
                    pending.extend(values.values());
                }
                serde_json::Value::Null
                | serde_json::Value::Bool(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::String(_) => {}
            }
        }
        Ok(presentable)
    }

    fn approval_schema_covers_value(schema: &serde_json::Value, value: &serde_json::Value) -> bool {
        let mut pending = vec![(schema, value)];
        let mut visited = 0_usize;
        while let Some((schema, value)) = pending.pop() {
            visited += 1;
            if visited > MAX_APPROVAL_VALUE_NODES {
                return false;
            }
            let Some(schema) = schema.as_object() else {
                return false;
            };
            match value {
                serde_json::Value::Object(values) => {
                    if !Self::approval_schema_allows_type(schema, "object") {
                        return false;
                    }
                    let Some(properties) = schema
                        .get("properties")
                        .and_then(serde_json::Value::as_object)
                    else {
                        return values.is_empty();
                    };
                    for (name, value) in values {
                        let Some(property_schema) = properties.get(name) else {
                            return false;
                        };
                        pending.push((property_schema, value));
                    }
                }
                serde_json::Value::Array(values) => {
                    if !Self::approval_schema_allows_type(schema, "array") {
                        return false;
                    }
                    let Some(item_schema) = schema.get("items") else {
                        return values.is_empty();
                    };
                    pending.extend(values.iter().map(|value| (item_schema, value)));
                }
                serde_json::Value::String(_) => {
                    if !Self::approval_schema_allows_type(schema, "string") {
                        return false;
                    }
                }
                serde_json::Value::Number(number) => {
                    let number_is_covered = Self::approval_schema_allows_type(schema, "number")
                        || ((number.is_i64() || number.is_u64())
                            && Self::approval_schema_allows_type(schema, "integer"));
                    if !number_is_covered {
                        return false;
                    }
                }
                serde_json::Value::Bool(_) => {
                    if !Self::approval_schema_allows_type(schema, "boolean") {
                        return false;
                    }
                }
                serde_json::Value::Null => {
                    if !Self::approval_schema_allows_type(schema, "null") {
                        return false;
                    }
                }
            }
        }
        true
    }

    fn approval_schema_allows_type(
        schema: &serde_json::Map<String, serde_json::Value>,
        expected: &str,
    ) -> bool {
        match schema.get("type") {
            Some(serde_json::Value::String(schema_type)) => schema_type == expected,
            Some(serde_json::Value::Array(schema_types)) => schema_types
                .iter()
                .any(|schema_type| schema_type.as_str() == Some(expected)),
            Some(
                serde_json::Value::Null
                | serde_json::Value::Bool(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::Object(_),
            )
            | None => false,
        }
    }

    fn value_is_within_bound(value: &serde_json::Value, max_bytes: usize) -> bool {
        let mut pending = vec![(value, 0_usize)];
        let mut visited = 0_usize;
        let mut bytes = 0_usize;
        while let Some((value, depth)) = pending.pop() {
            visited += 1;
            if visited > MAX_APPROVAL_VALUE_NODES || depth > MAX_APPROVAL_NESTING_DEPTH {
                return false;
            }
            match value {
                serde_json::Value::Array(values) => {
                    for value in values {
                        if visited.saturating_add(pending.len()) >= MAX_APPROVAL_VALUE_NODES {
                            return false;
                        }
                        pending.push((value, depth.saturating_add(1)));
                    }
                }
                serde_json::Value::Object(values) => {
                    bytes = values
                        .keys()
                        .fold(bytes, |bytes, key| bytes.saturating_add(key.len()));
                    for value in values.values() {
                        if visited.saturating_add(pending.len()) >= MAX_APPROVAL_VALUE_NODES {
                            return false;
                        }
                        pending.push((value, depth.saturating_add(1)));
                    }
                }
                serde_json::Value::String(value) => {
                    bytes = bytes.saturating_add(value.len());
                }
                serde_json::Value::Number(_) => bytes = bytes.saturating_add(32),
                serde_json::Value::Bool(_) | serde_json::Value::Null => {
                    bytes = bytes.saturating_add(5);
                }
            }
            if bytes > max_bytes {
                return false;
            }
        }
        true
    }
}

impl CustomToolCatalog {
    pub(super) async fn from_sources<T: ToolsetBackend>(
        toolsets: &[Arc<T>],
    ) -> Result<Self, SdkError> {
        let mut catalog = Vec::new();
        let mut shapes = Vec::new();
        let mut executable_names = Vec::new();
        let mut provider_tools = Vec::new();
        let mut routes = BTreeMap::new();
        let mut all_names = BTreeSet::new();
        for (toolset_index, toolset) in toolsets.iter().enumerate() {
            for tool in toolset.list_tools().await? {
                let name = tool.name.as_str().to_owned();
                if !all_names.insert(name.clone()) {
                    return Err(SdkError::invalid_config(
                        "configured tool names must be unique",
                    ));
                }
                let input_schema = JsonSchema::new(tool.input_schema.schema).map_err(|_| {
                    SdkError::invalid_config("configured tool schema is not provider-compatible")
                })?;
                let shape = CustomToolShape::new(
                    name.clone(),
                    tool.description.clone(),
                    input_schema.schema().clone(),
                )?;
                catalog.push(ToolInfo::new(
                    name.clone(),
                    tool.description.clone(),
                    Some(input_schema.schema().clone()),
                )?);
                provider_tools.push(
                    ProviderFunctionTool::new(name.clone(), tool.description, input_schema)
                        .map_err(|_| {
                            SdkError::invalid_config(
                                "configured tool declaration is not provider-compatible",
                            )
                        })?,
                );
                shapes.push(shape);
                routes.insert(name.clone(), toolset_index);
                executable_names.push(name);
            }
        }
        catalog.sort_by(|left, right| left.name().cmp(right.name()));
        provider_tools.sort_by(|left, right| left.name().cmp(right.name()));
        executable_names.sort();
        let floor = CustomCatalogFloor::new(shapes)?;
        Ok(CustomToolCatalog {
            entries: catalog,
            floor,
            executable_names,
            provider_tools,
            routes,
        })
    }
}

impl<T: ToolsetBackend> Clone for CustomTools<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
