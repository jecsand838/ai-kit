use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Numeric element type used by vector schema fields.
///
/// # Examples
///
/// ```
/// let element = storage_kit::database::catalog::VectorElementType::F32;
/// assert!(matches!(element, storage_kit::database::catalog::VectorElementType::F32));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum VectorElementType {
    /// Stores vector elements as half-precision floating point values.
    F16,
    /// Stores vector elements as single-precision floating point values.
    F32,
    /// Stores vector elements as double-precision floating point values.
    F64,
    /// Signed 16-bit integer.
    I16,
    /// Signed 32-bit integer.
    I32,
    /// Signed 64-bit integer.
    I64,
}

/// Portable field type description used for collection schemas.
///
/// # Examples
///
/// ```
/// let field_type = storage_kit::database::catalog::FieldType::DenseVector {
///     dim: 3,
///     element: storage_kit::database::catalog::VectorElementType::F32,
/// };
/// assert!(matches!(field_type, storage_kit::database::catalog::FieldType::DenseVector { .. }));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FieldType {
    /// Used for boolean fields.
    Bool,
    /// Signed 64-bit integer field.
    I64,
    /// Unsigned 64-bit integer field.
    U64,
    /// 64-bit floating point field.
    F64,
    /// Used for UTF-8 text fields.
    String,
    /// Used for fields that hold arbitrary JSON values.
    ///
    /// This does not promise native ordering or pattern matching. Supplied
    /// LanceDB accepts scalar equality, non-equality, and membership on Json
    /// fields and rejects range and pattern predicates on them.
    Json,
    /// Dense vector field with fixed dimension and element type.
    DenseVector {
        /// Vector dimensionality.
        dim: usize,
        /// Numeric element type.
        element: VectorElementType,
    },
    /// Used for fields that hold opaque binary blobs.
    Binary,
}

/// Declares a single collection field.
///
/// # Examples
///
/// ```
/// let field = storage_kit::database::catalog::FieldSchema {
///     name: "title".into(),
///     field_type: storage_kit::database::catalog::FieldType::String,
///     nullable: false,
///     metadata: std::collections::BTreeMap::new(),
/// };
/// assert_eq!(field.name, "title");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldSchema {
    /// Field name as exposed by the backend.
    pub name: String,
    /// Portable field type assigned to the field.
    pub field_type: FieldType,
    /// Whether the field accepts null values.
    pub nullable: bool,
    /// Backend-specific metadata preserved with the field.
    pub metadata: BTreeMap<String, String>,
}

/// Portable schema description for a collection.
///
/// # Examples
///
/// ```
/// let schema = storage_kit::database::catalog::CollectionSchema {
///     fields: vec![storage_kit::database::catalog::FieldSchema {
///         name: "title".into(),
///         field_type: storage_kit::database::catalog::FieldType::String,
///         nullable: false,
///         metadata: std::collections::BTreeMap::new(),
///     }],
///     metadata: std::collections::BTreeMap::new(),
/// };
/// assert_eq!(schema.fields.len(), 1);
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CollectionSchema {
    /// Ordered field list for the collection.
    pub fields: Vec<FieldSchema>,
    /// Backend-specific schema metadata preserved with the normalized schema.
    pub metadata: BTreeMap<String, String>,
}
