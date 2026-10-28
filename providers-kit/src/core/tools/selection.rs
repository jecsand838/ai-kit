use std::collections::BTreeSet;

use crate::KitError;
use crate::error::ValidationViolation;

use crate::generation::{FunctionCall, ProviderToolSpec, ToolChoice};

/// Returns the stable provider-visible name for a function tool.
pub(crate) fn tool_name(tool: &ProviderToolSpec) -> Option<&str> {
    match tool {
        ProviderToolSpec::Function(tool) => Some(tool.name()),
        ProviderToolSpec::Hosted(_) => None,
    }
}

/// Normalizes explicit provider-visible names requested by a tool choice.
pub(crate) fn selected_tool_names(
    choice: Option<&ToolChoice>,
) -> Result<Option<Vec<String>>, KitError> {
    match choice {
        Some(ToolChoice::Tool { name }) => {
            FunctionCall::validate_name("tool_choice.name", name)?;
            Ok(Some(vec![name.clone()]))
        }
        Some(ToolChoice::Allowed { names, .. }) => {
            if names.is_empty() {
                return Err(KitError::invalid_input(
                    ValidationViolation::Empty {
                        field: "tool_choice.names",
                    },
                    "ToolChoice::Allowed requires at least one tool name",
                ));
            }
            let mut seen = BTreeSet::new();
            for name in names {
                FunctionCall::validate_name("tool_choice.names", name)?;
                if !seen.insert(name.clone()) {
                    return Err(KitError::invalid_input(
                        ValidationViolation::Duplicate {
                            field: "tool_choice.names",
                        },
                        format!("duplicate allowed tool choice name `{name}`"),
                    ));
                }
            }
            Ok(Some(names.clone()))
        }
        _ => Ok(None),
    }
}

/// Filters declarations to the subset selected by a validated tool choice.
pub(crate) fn filter_tools_for_choice(
    tools: &[ProviderToolSpec],
    choice: Option<&ToolChoice>,
) -> Result<Vec<ProviderToolSpec>, KitError> {
    Ok(selected_tools_for_choice(tools, choice)?.0)
}

pub(crate) fn selected_tools_for_choice(
    tools: &[ProviderToolSpec],
    choice: Option<&ToolChoice>,
) -> Result<(Vec<ProviderToolSpec>, Option<Vec<String>>), KitError> {
    if matches!(choice, Some(ToolChoice::Required | ToolChoice::Any)) && tools.is_empty() {
        return Err(KitError::invalid_input(
            ValidationViolation::MissingField { field: "tools" },
            "required tool choice needs at least one tool declaration",
        ));
    }
    let Some(requested_names) = selected_tool_names(choice)? else {
        return Ok((tools.to_vec(), None));
    };

    let mut available_names = BTreeSet::new();
    for tool in tools {
        if let Some(name) = tool_name(tool)
            && !available_names.insert(name.to_string())
        {
            return Err(KitError::invalid_input(
                ValidationViolation::Duplicate {
                    field: "tools.name",
                },
                format!(
                    "tool choice is ambiguous because tool name `{name}` appears multiple times"
                ),
            ));
        }
    }

    let requested_lookup = requested_names.iter().cloned().collect::<BTreeSet<_>>();
    let filtered = tools
        .iter()
        .filter(|tool| {
            tool_name(tool)
                .map(|name| requested_lookup.contains(name))
                .unwrap_or(false)
        })
        .cloned()
        .collect::<Vec<_>>();

    let filtered_names = filtered
        .iter()
        .filter_map(tool_name)
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    let missing = requested_names
        .iter()
        .filter(|name| !filtered_names.contains(*name))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(KitError::invalid_input(
            ValidationViolation::InvalidValue {
                field: "tool_choice.names",
                rule: "must reference declared tools",
            },
            format!(
                "tool choice referenced unknown tool(s): {}",
                missing.join(", ")
            ),
        ));
    }

    Ok((filtered, Some(requested_names)))
}
