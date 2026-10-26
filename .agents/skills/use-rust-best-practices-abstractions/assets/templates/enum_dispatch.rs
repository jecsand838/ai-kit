pub(crate) trait SearchBackend {
    async fn search(&self, req: SearchRequest) -> Result<SearchResponse, SearchError>;
}

pub struct SearchEngine {
    inner: SearchEngineInner,
}

enum SearchEngineInner {
    Local(local::LocalSearch),
    Remote(remote::RemoteSearch),
}

impl SearchBackend for SearchEngineInner {
    async fn search(&self, req: SearchRequest) -> Result<SearchResponse, SearchError> {
        match self {
            Self::Local(inner) => inner.search(req).await,
            Self::Remote(inner) => inner.search(req).await,
        }
    }
}

impl SearchBackend for SearchEngine {
    async fn search(&self, req: SearchRequest) -> Result<SearchResponse, SearchError> {
        self.inner.search(req).await
    }
}

impl SearchEngine {
    pub async fn search(&self, req: SearchRequest) -> Result<SearchResponse, SearchError> {
        <Self as SearchBackend>::search(self, req).await
    }

    pub(crate) fn from_local(search: local::LocalSearch) -> Self {
        Self {
            inner: SearchEngineInner::Local(search),
        }
    }

    pub(crate) fn from_remote(search: remote::RemoteSearch) -> Self {
        Self {
            inner: SearchEngineInner::Remote(search),
        }
    }
}

mod local {
    use super::{SearchBackend, SearchError, SearchRequest, SearchResponse};

    pub struct LocalSearch;

    impl SearchBackend for LocalSearch {
        async fn search(&self, _req: SearchRequest) -> Result<SearchResponse, SearchError> {
            Ok(SearchResponse)
        }
    }
}

mod remote {
    use super::{SearchBackend, SearchError, SearchRequest, SearchResponse};

    pub struct RemoteSearch;

    impl SearchBackend for RemoteSearch {
        async fn search(&self, _req: SearchRequest) -> Result<SearchResponse, SearchError> {
            Ok(SearchResponse)
        }
    }
}

pub struct SearchRequest;
pub struct SearchResponse;

#[derive(Debug, thiserror::Error)]
#[error("search failed")]
pub struct SearchError;
