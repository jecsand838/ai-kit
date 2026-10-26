use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value, json};

use crate::{DenseVector, Projection, StorageError};

use super::super::manager::QdrantManager;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum VectorSelection {
    All,
    None,
    Names(BTreeSet<String>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryUsingBehavior {
    Requested,
    Omit,
}

pub(crate) enum RemoteDenseVectorConfig<'a> {
    Single,
    Named(&'a Map<String, Value>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VectorSelectionPlan {
    selection: VectorSelection,
    query_using_behavior: QueryUsingBehavior,
}

impl VectorSelection {
    pub(crate) fn compile(&self) -> Value {
        match self {
            Self::All => json!(true),
            Self::None => json!(false),
            Self::Names(names) => json!(names.iter().cloned().collect::<Vec<_>>()),
        }
    }
}

impl VectorSelectionPlan {
    pub(crate) fn new(selection: VectorSelection) -> Self {
        Self::new_with_query_using(selection, QueryUsingBehavior::Requested)
    }

    fn new_with_query_using(
        selection: VectorSelection,
        query_using_behavior: QueryUsingBehavior,
    ) -> Self {
        Self {
            selection,
            query_using_behavior,
        }
    }

    pub(crate) fn requests_vectors(&self) -> bool {
        !matches!(self.selection, VectorSelection::None)
    }

    pub(crate) fn for_edge(
        projection: &Projection,
        available_vector_names: &BTreeSet<String>,
    ) -> Self {
        Self::new(projected_vector_selection(
            projection,
            available_vector_names,
        ))
    }

    pub(crate) fn for_remote(
        db: &QdrantManager,
        projection: &Projection,
        raw: &Value,
    ) -> Result<Self, StorageError> {
        let (available_vector_names, unnamed_dense, query_using_behavior) =
            remote_vector_names(raw)?;
        if unnamed_dense
            && projection_requests_unnamed_dense(db, projection, &available_vector_names)
        {
            return Err(super::remote_unnamed_default_vector_unsupported(
                "vector projection",
            ));
        }
        Ok(Self::new_with_query_using(
            projected_vector_selection(projection, &available_vector_names),
            query_using_behavior,
        ))
    }

    pub(crate) fn query_using(&self, requested: &str) -> Option<String> {
        match self.query_using_behavior {
            QueryUsingBehavior::Requested => Some(requested.to_owned()),
            QueryUsingBehavior::Omit => None,
        }
    }

    pub(crate) fn with_vector(&self) -> Value {
        self.selection.compile()
    }

    pub(crate) fn with_vector_for_edge(&self) -> qdrant_edge::WithVector {
        match &self.selection {
            VectorSelection::All => qdrant_edge::WithVector::Bool(true),
            VectorSelection::None => qdrant_edge::WithVector::Bool(false),
            VectorSelection::Names(names) => {
                qdrant_edge::WithVector::Selector(names.iter().cloned().collect())
            }
        }
    }

    pub(crate) fn retain_decoded_vectors(&self, vectors: &mut BTreeMap<String, DenseVector>) {
        match &self.selection {
            VectorSelection::All => {}
            VectorSelection::None => vectors.clear(),
            VectorSelection::Names(names) => vectors.retain(|name, _| names.contains(name)),
        }
    }
}

pub(crate) fn compile_projection_selector(projection: &Projection) -> Value {
    match projection {
        Projection::All => json!(true),
        Projection::Columns(columns) => json!({ "include": columns }),
    }
}

pub(crate) fn projection_selector_for_edge(
    projection: &Projection,
) -> Result<qdrant_edge::WithPayloadInterface, StorageError> {
    match projection {
        Projection::All => Ok(qdrant_edge::WithPayloadInterface::Bool(true)),
        Projection::Columns(columns) => columns
            .iter()
            .map(|column| {
                column.parse().map_err(|err| {
                    StorageError::Serialization(format!(
                        "invalid Qdrant edge payload field path `{column}`: {err:?}"
                    ))
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()
            .map(qdrant_edge::WithPayloadInterface::Fields),
    }
}

fn projected_vector_selection(
    projection: &Projection,
    available_vector_names: &BTreeSet<String>,
) -> VectorSelection {
    match projection {
        Projection::Columns(columns) => {
            let names = columns
                .iter()
                .filter(|name| available_vector_names.contains(*name))
                .cloned()
                .collect::<BTreeSet<_>>();
            if names.is_empty() {
                VectorSelection::None
            } else {
                VectorSelection::Names(names)
            }
        }
        Projection::All => VectorSelection::All,
    }
}

fn projection_requests_unnamed_dense(
    db: &QdrantManager,
    projection: &Projection,
    available_vector_names: &BTreeSet<String>,
) -> bool {
    match projection {
        Projection::All => true,
        Projection::Columns(columns) => {
            let configured_default = db.config().default_vector_name.as_deref();
            columns.iter().any(|name| {
                !available_vector_names.contains(name)
                    && (name.is_empty()
                        || name == "vector"
                        || configured_default.is_some_and(|default| name == default))
            })
        }
    }
}

fn remote_vector_names(
    raw: &Value,
) -> Result<(BTreeSet<String>, bool, QueryUsingBehavior), StorageError> {
    let mut names = BTreeSet::new();
    let mut unnamed_dense = false;
    let query_using_behavior = match remote_dense_vector_config(raw)? {
        Some(RemoteDenseVectorConfig::Single) => {
            unnamed_dense = true;
            QueryUsingBehavior::Omit
        }
        Some(RemoteDenseVectorConfig::Named(map)) => {
            names.extend(map.keys().cloned());
            QueryUsingBehavior::Requested
        }
        None => QueryUsingBehavior::Requested,
    };
    if let Some(sparse_vectors) = raw
        .pointer("/result/config/params/sparse_vectors")
        .and_then(Value::as_object)
    {
        names.extend(sparse_vectors.keys().cloned());
    }
    Ok((names, unnamed_dense, query_using_behavior))
}

pub(crate) fn remote_dense_vector_config(
    raw: &Value,
) -> Result<Option<RemoteDenseVectorConfig<'_>>, StorageError> {
    let Some(vectors) = raw.pointer("/result/config/params/vectors") else {
        return Ok(None);
    };
    match vectors {
        Value::Null => Ok(None),
        Value::Object(map) if map.is_empty() => Ok(Some(RemoteDenseVectorConfig::Named(map))),
        Value::Object(map) if map.values().all(looks_like_named_dense_vector_value) => {
            Ok(Some(RemoteDenseVectorConfig::Named(map)))
        }
        Value::Object(map) if looks_like_single_dense_vector_config(map) => {
            Ok(Some(RemoteDenseVectorConfig::Single))
        }
        Value::Object(_)
        | Value::Array(_)
        | Value::String(_)
        | Value::Number(_)
        | Value::Bool(_) => Err(crate::StorageError::Serialization(format!(
            "unsupported Qdrant collection vector config shape: {vectors}"
        ))),
    }
}

fn looks_like_named_dense_vector_value(value: &Value) -> bool {
    matches!(value, Value::Object(map) if looks_like_single_dense_vector_config(map))
}

fn looks_like_single_dense_vector_config(map: &Map<String, Value>) -> bool {
    const DENSE_CONFIG_KEYS: &[&str] = &[
        "datatype",
        "distance",
        "hnsw_config",
        "multivector_config",
        "on_disk",
        "size",
    ];
    !map.is_empty()
        && map
            .keys()
            .all(|key| DENSE_CONFIG_KEYS.contains(&key.as_str()))
}
