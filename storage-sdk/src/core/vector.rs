use serde::{Deserialize, Serialize};

use super::error::StorageError;

/// Dense vector payload stored in a backend-supported numeric representation.
///
/// # Examples
///
/// ```
/// let vector = storage_sdk::database::data::DenseVector::F32(vec![0.1, 0.2, 0.3]);
/// assert_eq!(vector.len(), 3);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DenseVector {
    /// Dense vector stored as `f32`.
    F32(Vec<f32>),
    /// Dense vector stored as `f64`.
    F64(Vec<f64>),
    /// Dense vector stored as `i16`.
    I16(Vec<i16>),
    /// Dense vector stored as `i32`.
    I32(Vec<i32>),
    /// Dense vector stored as `i64`.
    I64(Vec<i64>),
}

/// Controls numeric conversion when a backend requires `f32` vectors.
///
/// # Examples
///
/// ```
/// let vector = storage_sdk::database::data::DenseVector::I16(vec![1, 2, 3]);
/// let values = vector
///     .to_f32(storage_sdk::database::query::VectorCoercionPolicy::Lossless)
///     .expect("i16 values fit in f32 exactly");
/// assert_eq!(values, vec![1.0, 2.0, 3.0]);
/// ```
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum VectorCoercionPolicy {
    /// Reject every conversion except an already-`f32` vector.
    Exact,
    /// Permit only conversions whose concrete values round-trip exactly.
    Lossless,
    /// Allows numeric downcasts even when precision may be changed.
    Lossy,
}

impl DenseVector {
    /// Returns the vector dimensionality.
    ///
    /// # Examples
    ///
    /// ```
    /// let vector = storage_sdk::database::data::DenseVector::I32(vec![1, 2, 3, 4]);
    /// assert_eq!(vector.len(), 4);
    /// ```
    pub fn len(&self) -> usize {
        match self {
            Self::F32(v) => v.len(),
            Self::F64(v) => v.len(),
            Self::I16(v) => v.len(),
            Self::I32(v) => v.len(),
            Self::I64(v) => v.len(),
        }
    }

    /// Returns `true` when the vector has zero elements.
    ///
    /// # Examples
    ///
    /// ```
    /// let vector = storage_sdk::database::data::DenseVector::F64(Vec::new());
    /// assert!(vector.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Converts this vector into `f32` values under an explicit coercion policy.
    ///
    /// # Examples
    ///
    /// ```
    /// let vector = storage_sdk::database::data::DenseVector::F64(vec![1.0, 2.5]);
    /// let values = vector
    ///     .to_f32(storage_sdk::database::query::VectorCoercionPolicy::Lossless)
    ///     .expect("these f64 values are exactly representable as f32");
    /// assert_eq!(values, vec![1.0, 2.5]);
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when the requested policy rejects the source vector's
    /// representation or one of its concrete values.
    pub fn to_f32(&self, policy: VectorCoercionPolicy) -> Result<Vec<f32>, StorageError> {
        match self {
            Self::F32(v) => Ok(v.clone()),
            Self::F64(v) => Self::convert_f64_to_f32(v, policy),
            Self::I16(v) => Self::convert_i16_to_f32(v, policy),
            Self::I32(v) => Self::convert_i32_to_f32(v, policy),
            Self::I64(v) => Self::convert_i64_to_f32(v, policy),
        }
    }

    fn conversion_rejected(source: &'static str, policy: VectorCoercionPolicy) -> StorageError {
        StorageError::InvalidRequest(format!(
            "cannot convert {source} dense vector to f32 under {policy:?} policy"
        ))
    }

    fn value_rejected(
        source: &'static str,
        index: usize,
        value: impl std::fmt::Display,
        policy: VectorCoercionPolicy,
    ) -> StorageError {
        StorageError::InvalidRequest(format!(
            "cannot convert {source} dense vector value at index {index} (`{value}`) to f32 without precision loss under {policy:?} policy"
        ))
    }

    fn convert_f64_to_f32(
        values: &[f64],
        policy: VectorCoercionPolicy,
    ) -> Result<Vec<f32>, StorageError> {
        if !policy.allows_representation_change() {
            return Err(Self::conversion_rejected("f64", policy));
        }

        values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let converted = *value as f32;
                if policy.allows_lossy()
                    || (converted.is_finite() && f64::from(converted) == *value)
                {
                    Ok(converted)
                } else {
                    Err(Self::value_rejected("f64", index, value, policy))
                }
            })
            .collect()
    }

    fn convert_i16_to_f32(
        values: &[i16],
        policy: VectorCoercionPolicy,
    ) -> Result<Vec<f32>, StorageError> {
        if !policy.allows_representation_change() {
            return Err(Self::conversion_rejected("i16", policy));
        }

        Ok(values.iter().map(|value| f32::from(*value)).collect())
    }

    fn convert_i32_to_f32(
        values: &[i32],
        policy: VectorCoercionPolicy,
    ) -> Result<Vec<f32>, StorageError> {
        if !policy.allows_representation_change() {
            return Err(Self::conversion_rejected("i32", policy));
        }

        values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let converted = *value as f32;
                if policy.allows_lossy() || converted as i128 == i128::from(*value) {
                    Ok(converted)
                } else {
                    Err(Self::value_rejected("i32", index, value, policy))
                }
            })
            .collect()
    }

    fn convert_i64_to_f32(
        values: &[i64],
        policy: VectorCoercionPolicy,
    ) -> Result<Vec<f32>, StorageError> {
        if !policy.allows_representation_change() {
            return Err(Self::conversion_rejected("i64", policy));
        }

        values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let converted = *value as f32;
                if policy.allows_lossy() || converted as i128 == i128::from(*value) {
                    Ok(converted)
                } else {
                    Err(Self::value_rejected("i64", index, value, policy))
                }
            })
            .collect()
    }
}

impl VectorCoercionPolicy {
    fn allows_representation_change(self) -> bool {
        matches!(self, Self::Lossless | Self::Lossy)
    }

    fn allows_lossy(self) -> bool {
        matches!(self, Self::Lossy)
    }
}

/// Distance metric used for vector search and index configuration.
///
/// # Examples
///
/// ```
/// let metric = storage_sdk::database::query::DistanceMetric::Cosine;
/// assert!(matches!(metric, storage_sdk::database::query::DistanceMetric::Cosine));
/// ```
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Uses Euclidean distance for vector comparisons.
    L2,
    /// Cosine distance or similarity, depending on backend semantics.
    Cosine,
    /// Dot-product distance or similarity, depending on backend semantics.
    Dot,
    /// Uses Hamming distance for vector comparisons.
    Hamming,
}
