use serde::{Deserialize, Serialize};

use crate::StorageError;

/// Logical address for a collection/table across backends.
///
/// # Examples
///
/// ```
/// let locator = storage_kit::database::StorageLocator::new("documents")
///     .with_namespace(["tenant"])
///     .with_database("app");
/// assert_eq!(locator.collection, "documents");
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StorageLocator {
    /// Optional namespace path that scopes the collection.
    ///
    /// Backends with one native namespace value may reject multi-segment paths.
    pub namespace: Vec<String>,
    /// Optional logical database name inserted between the namespace and collection.
    pub database: Option<String>,
    /// Logical collection or table name targeted by the request.
    pub collection: String,
}

pub(crate) trait ApplyDefaultLocator: Sized {
    fn apply_default_locator(self, default: &StorageLocator) -> Self;
}

pub(crate) trait HasLocator {
    fn locator_mut(&mut self) -> &mut StorageLocator;
}

impl<T> ApplyDefaultLocator for T
where
    T: HasLocator,
{
    fn apply_default_locator(mut self, default: &StorageLocator) -> Self {
        let locator = self.locator_mut().clone().with_defaults_from(default);
        *self.locator_mut() = locator;
        self
    }
}

impl StorageLocator {
    pub(crate) fn validate_for_config(&self, field: &str) -> Result<(), StorageError> {
        if self.collection.trim().is_empty() {
            return Err(StorageError::InvalidConfig(format!(
                "{field}.collection must not be empty"
            )));
        }
        if self
            .database
            .as_deref()
            .is_some_and(|database| database.trim().is_empty())
        {
            return Err(StorageError::InvalidConfig(format!(
                "{field}.database must not be empty"
            )));
        }
        if let Some(index) = self
            .namespace
            .iter()
            .position(|segment| segment.trim().is_empty())
        {
            return Err(StorageError::InvalidConfig(format!(
                "{field}.namespace[{index}] must not be empty"
            )));
        }
        Ok(())
    }

    /// Creates a locator for a collection without a namespace or database override.
    ///
    /// # Examples
    ///
    /// ```
    /// let locator = storage_kit::database::StorageLocator::new("documents");
    /// assert_eq!(locator.collection, "documents");
    /// ```
    pub fn new(collection: impl Into<String>) -> Self {
        Self {
            namespace: Vec::new(),
            database: None,
            collection: collection.into(),
        }
    }

    /// Replaces the namespace path on the locator.
    ///
    /// Some backends accept only one native namespace segment.
    ///
    /// # Examples
    ///
    /// ```
    /// let locator = storage_kit::database::StorageLocator::new("documents").with_namespace(["tenant"]);
    /// assert_eq!(locator.namespace, vec!["tenant"]);
    /// ```
    pub fn with_namespace(
        mut self,
        namespace: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.namespace = namespace.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the logical database name on the locator.
    ///
    /// # Examples
    ///
    /// ```
    /// let locator = storage_kit::database::StorageLocator::new("documents").with_database("app");
    /// assert_eq!(locator.database.as_deref(), Some("app"));
    /// ```
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    #[must_use]
    pub(crate) fn with_defaults_from(mut self, default: &Self) -> Self {
        if self.namespace.is_empty() {
            self.namespace = default.namespace.clone();
        }
        if self.database.is_none() {
            self.database = default.database.clone();
        }
        if self.collection.is_empty() {
            self.collection = default.collection.clone();
        }
        self
    }
}

impl ApplyDefaultLocator for StorageLocator {
    fn apply_default_locator(self, default: &StorageLocator) -> Self {
        self.with_defaults_from(default)
    }
}
