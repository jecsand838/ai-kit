use crate::{StorageError, VectorSearchParams};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VectorParam {
    Offset,
    Distance,
    Ef,
    FastSearch,
}

impl VectorParam {
    const ALL: [Self; 4] = [Self::Offset, Self::Distance, Self::Ef, Self::FastSearch];

    const fn name(self) -> &'static str {
        match self {
            Self::Offset => "offset",
            Self::Distance => "distance",
            Self::Ef => "ef",
            Self::FastSearch => "fast_search",
        }
    }

    fn is_requested(self, params: &VectorSearchParams) -> bool {
        match self {
            Self::Offset => params.offset.is_some(),
            Self::Distance => params.distance.is_some(),
            Self::Ef => params.ef.is_some(),
            Self::FastSearch => params.fast_search,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct VectorParamProfile {
    supported: &'static [VectorParam],
}

impl VectorParamProfile {
    const LANCE: &'static [VectorParam] = &VectorParam::ALL;
    #[cfg(feature = "surrealdb")]
    const SURREAL: &'static [VectorParam] =
        &[VectorParam::Offset, VectorParam::Distance, VectorParam::Ef];
    #[cfg(feature = "qdrant")]
    const QDRANT: &'static [VectorParam] = &[
        VectorParam::Offset,
        VectorParam::Ef,
        VectorParam::FastSearch,
    ];

    pub(crate) const fn lance() -> Self {
        Self {
            supported: Self::LANCE,
        }
    }

    #[cfg(feature = "surrealdb")]
    pub(crate) const fn surreal() -> Self {
        Self {
            supported: Self::SURREAL,
        }
    }

    #[cfg(feature = "qdrant")]
    pub(crate) const fn qdrant_remote() -> Self {
        Self {
            supported: Self::QDRANT,
        }
    }

    #[cfg(feature = "qdrant")]
    pub(crate) const fn qdrant_edge() -> Self {
        Self::qdrant_remote()
    }

    pub(crate) fn validate(
        self,
        operation: &'static str,
        params: &VectorSearchParams,
    ) -> Result<(), StorageError> {
        let unsupported = VectorParam::ALL
            .into_iter()
            .filter(|parameter| parameter.is_requested(params))
            .filter(|parameter| !self.supported.contains(parameter))
            .map(VectorParam::name)
            .collect::<Vec<_>>();
        if unsupported.is_empty() {
            Ok(())
        } else {
            Err(StorageError::unsupported_capability(
                "VECTOR_SEARCH_PARAMS",
                format!(
                    "{operation} cannot lower vector search parameter(s): {}",
                    unsupported.join(", ")
                ),
            ))
        }
    }
}
