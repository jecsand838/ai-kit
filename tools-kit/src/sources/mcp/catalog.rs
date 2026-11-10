use std::collections::BTreeMap;
#[cfg(feature = "mcp-server")]
use std::{borrow::Cow, sync::Arc};

#[cfg(feature = "mcp-server")]
use rmcp::ErrorData as McpError;
use rmcp::model::{CallToolResult, ContentBlock, Tool};
use serde_json::Map;

use crate::{
    ToolError,
    declaration::{ToolAnnotations, ToolIcon, ToolInfo},
    execution::{ToolContent, ToolResult},
    interaction::ToolInteractionCapabilities,
    mcp::McpClientConfig,
    name::{McpToolNameContext, ToolName},
    schema::{JsonSchema, SchemaStrictness},
};

use super::client::McpClient;

#[derive(Clone)]
pub(crate) struct McpToolEntry {
    info: ToolInfo,
    client: McpClient,
}

impl McpToolEntry {
    pub(crate) fn info(&self) -> &ToolInfo {
        &self.info
    }

    pub(crate) fn client(&self) -> &McpClient {
        &self.client
    }
}

impl std::fmt::Debug for McpToolEntry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpToolEntry")
            .field("info", &self.info)
            .finish_non_exhaustive()
    }
}

pub(crate) struct McpProtocol;

impl McpProtocol {
    #[cfg(feature = "mcp-server")]
    pub(crate) fn tool_info_to_mcp(info: &ToolInfo) -> Result<Tool, ToolError> {
        let input_schema = Self::schema_to_mcp_object(&info.input_schema.schema)?;
        let output_schema = info
            .output_schema
            .as_ref()
            .map(|schema| Self::schema_to_mcp_object(&schema.schema))
            .transpose()?;
        let mut tool = Tool::new_with_raw(
            info.name.as_str().to_owned(),
            info.description.clone().map(Cow::Owned),
            Arc::new(input_schema),
        );
        tool.title.clone_from(&info.title);
        tool.output_schema = output_schema.map(Arc::new);
        tool.annotations = Some(Self::annotations_to_mcp(&info.annotations));
        if !info.icons.is_empty() {
            tool.icons = Some(
                info.icons
                    .iter()
                    .map(|icon| {
                        let mut mapped = rmcp::model::Icon::new(icon.uri.clone());
                        mapped.mime_type.clone_from(&icon.mime_type);
                        mapped
                    })
                    .collect(),
            );
        }
        if !info.metadata.is_empty() {
            tool.meta = Some(Self::metadata_to_meta(&info.metadata));
        }
        Ok(tool)
    }

    pub(crate) fn entry_from_mcp_tool(
        config: &McpClientConfig,
        client: McpClient,
        names: &mut McpToolNameContext,
        tool: Tool,
    ) -> Result<McpToolEntry, ToolError> {
        let native_name = tool.name.to_string();
        let name = ToolName::mcp(names, native_name)?;
        let input_schema = JsonSchema {
            schema: serde_json::Value::Object((*tool.input_schema).clone()),
            strictness: SchemaStrictness::Unspecified,
        };
        let output_schema = tool.output_schema.map(|schema| JsonSchema {
            schema: serde_json::Value::Object((*schema).clone()),
            strictness: SchemaStrictness::Unspecified,
        });
        let metadata = tool.meta.map(Self::meta_to_metadata).unwrap_or_default();
        let interactions = ToolInteractionCapabilities {
            form: config.supports_forms(),
            form_schema_validation: config.validates_forms(),
            text: false,
            multiline_text: false,
        };
        let info = ToolInfo {
            name,
            title: tool.title,
            description: tool.description.map(|description| description.into_owned()),
            input_schema,
            output_schema,
            annotations: Self::map_annotations(tool.annotations),
            icons: Self::map_icons(tool.icons),
            interactions,
            metadata,
        };
        Ok(McpToolEntry { info, client })
    }

    #[cfg(feature = "mcp-server")]
    pub(crate) fn tool_result_to_mcp(mut result: ToolResult) -> Result<CallToolResult, ToolError> {
        let mut content = result
            .content
            .into_iter()
            .map(Self::tool_content_to_mcp)
            .collect::<Result<Vec<_>, _>>()?;
        let structured_content = result.structured_content.take();
        if content.is_empty()
            && let Some(value) = &structured_content
        {
            content.push(ContentBlock::text(value.to_string()));
        }
        let mut mapped = if result.is_error {
            CallToolResult::error(content)
        } else {
            CallToolResult::success(content)
        };
        mapped.structured_content = structured_content;
        mapped.is_error = Some(result.is_error);
        if !result.metadata.is_empty() {
            mapped.meta = Some(Self::metadata_to_meta(&result.metadata));
        }
        Ok(mapped)
    }

    #[cfg(feature = "mcp-server")]
    pub(crate) fn tool_error_to_mcp_error(error: ToolError) -> McpError {
        if matches!(
            &error,
            ToolError::UnknownTool { .. }
                | ToolError::InvalidArguments { .. }
                | ToolError::InvalidInteraction { .. }
        ) {
            McpError::invalid_params(error.to_string(), None)
        } else {
            McpError::internal_error(error.to_string(), None)
        }
    }

    pub(crate) fn tool_result_from_mcp(result: CallToolResult) -> Result<ToolResult, ToolError> {
        Ok(ToolResult {
            content: result
                .content
                .into_iter()
                .map(Self::tool_content_from_mcp)
                .collect::<Result<Vec<_>, _>>()?,
            structured_content: result.structured_content,
            is_error: result.is_error.unwrap_or(false),
            metadata: result.meta.map(Self::meta_to_metadata).unwrap_or_default(),
        })
    }

    fn tool_content_from_mcp(content: ContentBlock) -> Result<ToolContent, ToolError> {
        match content {
            ContentBlock::Text(text) => Ok(ToolContent::Text {
                metadata: Self::content_metadata(text.meta, text.annotations),
                text: text.text,
            }),
            ContentBlock::Image(image) => Ok(ToolContent::Image {
                metadata: Self::content_metadata(image.meta, image.annotations),
                mime_type: image.mime_type,
                data_base64: image.data,
            }),
            ContentBlock::Audio(audio) => Ok(ToolContent::Audio {
                metadata: Self::content_metadata(audio.meta, audio.annotations),
                mime_type: audio.mime_type,
                data_base64: audio.data,
            }),
            ContentBlock::Resource(resource) => {
                let metadata = Self::content_metadata(resource.meta, resource.annotations);
                let value = serde_json::to_value(resource.resource).map_err(|error| {
                    ToolError::Protocol {
                        message: format!("failed to serialize MCP embedded resource: {error}"),
                    }
                })?;
                Ok(ToolContent::EmbeddedResource {
                    uri: value
                        .get("uri")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    contents: value,
                    metadata: metadata.unwrap_or(serde_json::Value::Null),
                })
            }
            ContentBlock::ResourceLink(resource) => Ok(ToolContent::ResourceLink {
                uri: resource.uri,
                title: Some(resource.name),
                metadata: Self::content_metadata(resource.meta, resource.annotations)
                    .unwrap_or(serde_json::Value::Null),
            }),
            _ => Err(ToolError::Unsupported {
                operation: "unrecognized MCP tool content".to_owned(),
            }),
        }
    }

    fn content_metadata(
        meta: Option<rmcp::model::MetaObject>,
        annotations: Option<rmcp::model::Annotations>,
    ) -> Option<serde_json::Value> {
        let mut object = Map::new();
        if let Some(meta) = meta {
            object.extend(Self::meta_to_metadata(meta));
        }
        if let Some(annotations) = annotations
            && let Ok(serde_json::Value::Object(annotation_object)) =
                serde_json::to_value(annotations)
        {
            object.extend(annotation_object);
        }
        if object.is_empty() {
            None
        } else {
            Some(serde_json::Value::Object(object))
        }
    }

    fn map_annotations(annotations: Option<rmcp::model::ToolAnnotations>) -> ToolAnnotations {
        let Some(annotations) = annotations else {
            return ToolAnnotations::default();
        };
        ToolAnnotations {
            title: annotations.title,
            read_only_hint: annotations.read_only_hint,
            destructive_hint: annotations.destructive_hint,
            idempotent_hint: annotations.idempotent_hint,
            open_world_hint: annotations.open_world_hint,
        }
    }

    fn map_icons(icons: Option<Vec<rmcp::model::Icon>>) -> Vec<ToolIcon> {
        icons
            .unwrap_or_default()
            .into_iter()
            .map(|icon| ToolIcon {
                uri: icon.src,
                mime_type: icon.mime_type,
            })
            .collect()
    }

    fn meta_to_metadata(meta: rmcp::model::MetaObject) -> BTreeMap<String, serde_json::Value> {
        match serde_json::to_value(meta) {
            Ok(serde_json::Value::Object(object)) => object.into_iter().collect(),
            _ => BTreeMap::new(),
        }
    }

    #[cfg(feature = "mcp-server")]
    fn schema_to_mcp_object(
        schema: &serde_json::Value,
    ) -> Result<rmcp::model::JsonObject, ToolError> {
        let serde_json::Value::Object(object) = schema else {
            return Err(ToolError::Protocol {
                message: "MCP tool schemas must be JSON objects".to_owned(),
            });
        };
        Ok(object.clone())
    }

    #[cfg(feature = "mcp-server")]
    fn annotations_to_mcp(annotations: &ToolAnnotations) -> rmcp::model::ToolAnnotations {
        rmcp::model::ToolAnnotations::from_raw(
            annotations.title.clone(),
            annotations.read_only_hint,
            annotations.destructive_hint,
            annotations.idempotent_hint,
            annotations.open_world_hint,
        )
    }

    #[cfg(feature = "mcp-server")]
    fn tool_content_to_mcp(content: ToolContent) -> Result<ContentBlock, ToolError> {
        use rmcp::model::{
            AudioContent, EmbeddedResource, ImageContent, Resource, ResourceContents, TextContent,
        };

        match content {
            ToolContent::Text { text, metadata } => {
                let mut mapped = TextContent::new(text);
                mapped.meta = metadata.and_then(Self::metadata_value_to_meta);
                Ok(ContentBlock::Text(mapped))
            }
            ToolContent::Json { value, metadata } => {
                let mut mapped = TextContent::new(value.to_string());
                mapped.meta = metadata.and_then(Self::metadata_value_to_meta);
                Ok(ContentBlock::Text(mapped))
            }
            ToolContent::Image {
                mime_type,
                data_base64,
                metadata,
            } => {
                let mut mapped = ImageContent::new(data_base64, mime_type);
                mapped.meta = metadata.and_then(Self::metadata_value_to_meta);
                Ok(ContentBlock::Image(mapped))
            }
            ToolContent::Audio {
                mime_type,
                data_base64,
                metadata,
            } => {
                let mut mapped = AudioContent::new(data_base64, mime_type);
                mapped.meta = metadata.and_then(Self::metadata_value_to_meta);
                Ok(ContentBlock::Audio(mapped))
            }
            ToolContent::ResourceLink {
                uri,
                title,
                metadata,
            } => {
                let name = title.clone().unwrap_or_else(|| uri.clone());
                let mut resource = Resource::new(uri, name);
                resource.title = title;
                resource.meta = Self::metadata_value_to_meta(metadata);
                Ok(ContentBlock::ResourceLink(resource))
            }
            ToolContent::EmbeddedResource {
                uri,
                contents,
                metadata,
            } => {
                let resource =
                    serde_json::from_value::<ResourceContents>(contents).map_err(|error| {
                        ToolError::Protocol {
                            message: format!(
                                "failed to map embedded resource content to MCP: {error}"
                            ),
                        }
                    })?;
                let mut mapped = EmbeddedResource::new(resource);
                mapped.meta = Self::metadata_value_to_meta(metadata).or_else(|| {
                    Some(rmcp::model::MetaObject(Map::from_iter([(
                        "uri".to_owned(),
                        serde_json::Value::String(uri),
                    )])))
                });
                Ok(ContentBlock::Resource(mapped))
            }
        }
    }

    #[cfg(feature = "mcp-server")]
    fn metadata_to_meta(metadata: &BTreeMap<String, serde_json::Value>) -> rmcp::model::MetaObject {
        rmcp::model::MetaObject(metadata.clone().into_iter().collect())
    }

    #[cfg(feature = "mcp-server")]
    fn metadata_value_to_meta(value: serde_json::Value) -> Option<rmcp::model::MetaObject> {
        match value {
            serde_json::Value::Object(object) if object.is_empty() => None,
            serde_json::Value::Object(object) => Some(rmcp::model::MetaObject(object)),
            serde_json::Value::Null => None,
            other => Some(rmcp::model::MetaObject(Map::from_iter([(
                "value".to_owned(),
                other,
            )]))),
        }
    }
}
