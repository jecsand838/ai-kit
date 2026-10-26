use serde::{Deserialize, Serialize};

use crate::{FieldType, StorageLocator};

use super::super::{ApplyDefaultLocator, HasLocator};

/// Declares a computed column to add during schema evolution.
///
/// Expression acceptance depends on the selected backend; this DTO does not
/// define one expression language that every implementation accepts. Schema
/// evolution remains an optional catalog operation. The supplied Lance
/// implementation eagerly fills existing rows from the SQL expression before
/// returning completion. This portable DTO does not declare an upstream
/// deferred computed column or promise recomputation after later writes.
///
/// # Examples
///
/// ```
/// let column = storage_sdk::database::catalog::ComputedColumn {
///     name: "summary_len".into(),
///     expression_sql: "length(summary)".into(),
/// };
/// assert_eq!(column.name, "summary_len");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComputedColumn {
    /// Column name that will be added to the collection schema.
    pub name: String,
    /// SQL expression used to compute the column.
    pub expression_sql: String,
}

/// Request to add computed columns to a collection.
///
/// The supplied Lance implementation completes eager SQL backfill before
/// returning [`CatalogResponse::Completed`](crate::database::catalog::CatalogResponse::Completed).
/// Both supplied Lance and Surreal implementations reject a present
/// `read_columns` value, including an empty list, with
/// [`StorageError::UnsupportedCapability`](crate::StorageError::UnsupportedCapability)
/// before mutation. Use `None` to let the expressions determine their inputs.
///
/// # Examples
///
/// ```
/// let request = storage_sdk::database::catalog::AddColumnsRequest {
///     locator: storage_sdk::database::StorageLocator::new("documents"),
///     expressions: vec![storage_sdk::database::catalog::ComputedColumn {
///         name: "summary_len".into(),
///         expression_sql: "length(summary)".into(),
///     }],
///     read_columns: None,
/// };
/// assert_eq!(request.expressions.len(), 1);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AddColumnsRequest {
    /// Collection that should receive the new computed columns.
    pub locator: StorageLocator,
    /// Computed columns to add.
    pub expressions: Vec<ComputedColumn>,
    /// Optional source columns that the expression depends on.
    ///
    /// This is request-specific optional behavior. The supplied Lance and
    /// Surreal implementations reject every present value, including an empty
    /// list, with [`StorageError::UnsupportedCapability`](crate::StorageError::UnsupportedCapability)
    /// before mutation. A schema-evolution capability bit does not guarantee
    /// this option.
    pub read_columns: Option<Vec<String>>,
}

impl HasLocator for AddColumnsRequest {
    fn locator_mut(&mut self) -> &mut StorageLocator {
        &mut self.locator
    }
}

/// Describes a single column alteration.
///
/// # Examples
///
/// ```
/// let alteration = storage_sdk::database::catalog::ColumnAlterationSpec {
///     path: "title".into(),
///     rename: Some("heading".into()),
///     nullable: Some(false),
///     cast_to: Some(storage_sdk::database::catalog::FieldType::String),
/// };
/// assert_eq!(alteration.rename.as_deref(), Some("heading"));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnAlterationSpec {
    /// Existing column path.
    pub path: String,
    /// New column name to apply when the alteration renames the field.
    pub rename: Option<String>,
    /// Nullability setting to apply when the alteration changes null support.
    ///
    /// The supplied SurrealDB implementation rejects nullable-only changes and
    /// requires an explicit value alongside `cast_to`.
    /// The supplied Lance implementation rejects a batch combining any cast
    /// with changing a currently nullable column to non-nullable. It returns
    /// `UnsupportedCapability` before mutation. Already non-nullable columns
    /// remain supported. Cast first, then tighten nullability in a separate
    /// request; each request completes independently.
    pub nullable: Option<bool>,
    /// Target type to cast into when the alteration changes the field type.
    pub cast_to: Option<FieldType>,
}

/// Request to alter existing columns.
///
/// # Examples
///
/// ```
/// let request = storage_sdk::database::catalog::AlterColumnsRequest {
///     locator: storage_sdk::database::StorageLocator::new("documents"),
///     alterations: vec![storage_sdk::database::catalog::ColumnAlterationSpec {
///         path: "title".into(),
///         rename: Some("heading".into()),
///         nullable: None,
///         cast_to: None,
///     }],
/// };
/// assert_eq!(request.alterations.len(), 1);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AlterColumnsRequest {
    /// Collection whose columns should be altered.
    pub locator: StorageLocator,
    /// Column alterations to apply.
    pub alterations: Vec<ColumnAlterationSpec>,
}

impl HasLocator for AlterColumnsRequest {
    fn locator_mut(&mut self) -> &mut StorageLocator {
        &mut self.locator
    }
}

/// Request to drop columns from a collection.
///
/// # Examples
///
/// ```
/// let request = storage_sdk::database::catalog::DropColumnsRequest {
///     locator: storage_sdk::database::StorageLocator::new("documents"),
///     columns: vec!["obsolete".into()],
/// };
/// assert_eq!(request.columns, vec!["obsolete"]);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DropColumnsRequest {
    /// Collection whose columns should be removed.
    pub locator: StorageLocator,
    /// Columns to drop.
    pub columns: Vec<String>,
}

impl HasLocator for DropColumnsRequest {
    fn locator_mut(&mut self) -> &mut StorageLocator {
        &mut self.locator
    }
}

/// Schema evolution request.
///
/// # Examples
///
/// ```
/// let request = storage_sdk::database::catalog::SchemaEvolutionRequest::DropColumns(
///     storage_sdk::database::catalog::DropColumnsRequest {
///         locator: storage_sdk::database::StorageLocator::new("documents"),
///         columns: vec!["obsolete".into()],
///     },
/// );
/// assert!(matches!(
///     request,
///     storage_sdk::database::catalog::SchemaEvolutionRequest::DropColumns(_)
/// ));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SchemaEvolutionRequest {
    /// Add computed columns.
    AddColumns(AddColumnsRequest),
    /// Alter existing columns.
    AlterColumns(AlterColumnsRequest),
    /// Drop existing columns.
    DropColumns(DropColumnsRequest),
}

impl ApplyDefaultLocator for SchemaEvolutionRequest {
    fn apply_default_locator(self, default: &StorageLocator) -> Self {
        match self {
            Self::AddColumns(req) => Self::AddColumns(req.apply_default_locator(default)),
            Self::AlterColumns(req) => Self::AlterColumns(req.apply_default_locator(default)),
            Self::DropColumns(req) => Self::DropColumns(req.apply_default_locator(default)),
        }
    }
}
