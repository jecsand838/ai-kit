# Enum-dispatch trait pattern

Use this reference before adding, deleting, or refactoring enum dispatch over a
closed implementation family.

## 1. Source-backed findings

- The Rust Book presents enums as values that are one of a fixed set of
  variants, and variants can carry data directly:
  <https://doc.rust-lang.org/book/ch06-01-defining-an-enum.html>.
- The Rust Book presents `match` as the normal way to bind enum variant data,
  and `match` arms must cover every possibility:
  <https://doc.rust-lang.org/book/ch06-02-match.html>.
- Rust pattern syntax includes `_`, named bindings, `|`, and ranges, but
  implementation-family dispatch should use explicit variant arms instead of a
  wildcard arm so new variants force a deliberate update:
  <https://doc.rust-lang.org/book/ch19-03-pattern-syntax.html>.
- The Rust Reference defines traits as abstract interfaces, and trait impls
  must provide all non-default associated items and cannot add extra ones:
  <https://doc.rust-lang.org/reference/items/traits.html> and
  <https://doc.rust-lang.org/reference/items/implementations.html>.
- Native `async fn` in traits is appropriate for private/internal traits when
  old MSRV support and dynamic dispatch are not needed:
  <https://blog.rust-lang.org/2023/12/21/async-fn-rpit-in-traits/>.
- Traits with `async fn` are not dyn-compatible because they return an opaque
  future, which is acceptable for closed enum dispatch that does not use
  `dyn Trait`:
  <https://doc.rust-lang.org/reference/items/traits.html#dyn-compatibility>.
- Rust API Guidelines support methods on clear receivers, builders for complex
  construction or choices, private/sealed traits for future flexibility, and
  caution around macros:
  <https://rust-lang.github.io/api-guidelines/predictability.html>,
  <https://rust-lang.github.io/api-guidelines/type-safety.html>,
  <https://rust-lang.github.io/api-guidelines/future-proofing.html>, and
  <https://doc.rust-lang.org/book/ch20-05-macros.html>.

## 2. Decision checklist

Use a data-only enum when the enum only names states or options and does not
dispatch implementation behavior.

Use generics when the caller chooses one implementation statically and the API
should be monomorphized.

Use a trait object when the implementation set is open at runtime, heterogeneous
values must be stored behind one handle, and dynamic dispatch is an intentional
part of the design.

Use enum dispatch plus a private trait when the implementation family is closed
inside the crate and the public root handle is the portable surface. In
`ai-kit`, exported root dispatchers use opaque public structs over private
inner enums so callers cannot name variants. This opacity wrapper is only for
public exports; crate-private internal dispatch enums do not need struct
wrappers when they are not exported. These private enums are still dispatch
machinery, not a license to create reusable implementation-family identity,
provenance, capability, diagnostic-label, or error-label utilities.

For the root-approved Tool `ai-kit` seam, use the hybrid form: retain the
private exhaustive enum and every concrete built-in variant, use one public
RPITIT trait as the behavioral contract, and add exactly one private erased
`Extension` variant. Only that variant uses dynamic dispatch. Do not keep a
duplicate private behavioral trait or store a bare erased trait object as the
handle's only representation.

For Provider, Model, Session, Database, StateStore, StateTransaction, Agent,
Workspace, and Conversation, keep the supplied handle's private dispatcher
closed over built-ins. Their public RPITIT traits are direct static
implementation contracts. Callers retain a concrete implementation hierarchy
or define their own exhaustive enums; the Kit must not erase or normalize it.

App's five `workflow` traits are additional static behavior seams, not new
lower-backend dispatchers. Its `ChildExecutor` alone uses the exact generic
associated future for concrete local child futures. Only the process-global
field/tag serialization registry may erase captured codec functions; it may
not retain action, hook, condition, typing, child-executor, or lower-backend
values. These exceptions do not widen any lower-crate trait or ingress.

## 3. Required shape

1. Define one provider-agnostic behavioral trait. It is `pub(crate)` for an
   ordinary closed family and public RPITIT only for an explicitly approved
   behavioral seam.
2. For exported dispatchers, expose a public struct with a private inner enum.
   For crate-private dispatchers, a plain private enum is enough.
3. Make each built-in private enum variant wrap a concrete implementation
   struct. The approved Tool seam adds one `Extension` payload containing its
   private object-safe erased adapter. Provider, Storage, and Agent dispatchers
   contain built-ins only.
4. Implement the trait for every concrete struct in that struct's owning module
   subtree.
5. Implement the trait for the private dispatcher enum in the dispatcher
   module using explicit, exhaustive `match` arms whose arms call receiver
   methods on payloads that implement the same trait. An exported opaque
   handle also implements the trait and delegates through that enum/runtime;
   it does not replace or duplicate the enum's match.
   In `ai-kit`, matches over provider backends, agent backends, database
   backends, or toolset runtime/source families must live only in root
   `{crate}/src` dispatcher files that explicitly own that closed built-in
   family/private exhaustive dispatcher.
6. Do not keep same-name inherent forwarding methods as the operation parity
   surface; the owning public-or-private trait impl on the receiver is the
   parity surface.
7. Keep shared contract modules limited to provider-agnostic traits, shared
   handles, and default helpers.
8. Keep public construction/config owners responsible for choosing and
   validating concrete built-in variants. At the approved Tool root seam,
   `from_backend` constructs only the private erased `Extension` variant.
   Child handles enter through parent associated types and have no public
   `from_backend`.
9. Cover public trait/associated-child contracts with compile and behavior
   tests. Keep source guards negative and avoid freezing private names, files,
   impl placement, or match-arm spelling.
10. Keep implementation-family facts inside the owning payload or documented
    data/config type. Do not introduce shared `*BackendKind`, `*SourceKind`,
    provenance fields, display-name helpers, capability-label renderers, or
    backend/provider/source-prefixed diagnostics as shortcuts around the
    dispatcher boundary.

## 4. Sync example

```rust
pub(crate) trait SearchBackend {
    fn search(&self, req: SearchRequest) -> Result<SearchResponse, SearchError>;
}

pub struct SearchEngine {
    inner: SearchEngineInner,
}

enum SearchEngineInner {
    Local(local::LocalSearch),
    Remote(remote::RemoteSearch),
}

impl SearchBackend for SearchEngineInner {
    fn search(&self, req: SearchRequest) -> Result<SearchResponse, SearchError> {
        match self {
            Self::Local(inner) => inner.search(req),
            Self::Remote(inner) => inner.search(req),
        }
    }
}

impl SearchBackend for SearchEngine {
    fn search(&self, req: SearchRequest) -> Result<SearchResponse, SearchError> {
        self.inner.search(req)
    }
}

mod local {
    use super::{SearchBackend, SearchError, SearchRequest, SearchResponse};

    pub struct LocalSearch;

    impl SearchBackend for LocalSearch {
        fn search(&self, _req: SearchRequest) -> Result<SearchResponse, SearchError> {
            Ok(SearchResponse)
        }
    }
}

mod remote {
    use super::{SearchBackend, SearchError, SearchRequest, SearchResponse};

    pub struct RemoteSearch;

    impl SearchBackend for RemoteSearch {
        fn search(&self, _req: SearchRequest) -> Result<SearchResponse, SearchError> {
            Ok(SearchResponse)
        }
    }
}

pub struct SearchRequest;
pub struct SearchResponse;

#[derive(Debug, thiserror::Error)]
#[error("search failed")]
pub struct SearchError;
```

In a real module tree, `impl SearchBackend for LocalSearch` belongs under the
`local` subtree, and `impl SearchBackend for RemoteSearch` belongs under the
`remote` subtree. The shared contract module must not collect concrete impls.

## 5. Native async example

```rust
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
```

Do not add `async_trait` for this pattern. An ordinary closed family needs no
dynamic dispatch. At the approved Tool seam enumerated by root
`AGENTS.md` and the owning ledger, retain the private enum and use the required
private boxed-future erased adapter only for its one `Extension` payload; this
is still the approved enum-dispatch pattern, not a public trait-object route.

## 6. Anti-patterns

Do not rely on inherent-method parity by convention:

```rust
impl SearchEngine {
    pub async fn search(&self, req: SearchRequest) -> Result<SearchResponse, SearchError> {
        match &self.inner {
            SearchEngineInner::Local(inner) => inner.search(req).await,
            SearchEngineInner::Remote(inner) => inner.search(req).await,
        }
    }
}
```

Do not write the same dispatcher as
`SearchBackend::search(inner, req)`. UFCS is useful for rare disambiguation,
but in this workspace it is invalid for enum-dispatch contract calls because it
usually signals the receiver does not own the operation cleanly or the code has
same-name inherent forwarding methods that bypass trait parity.

Do not add a same-name inherent method that forwards into the handle's trait
implementation. The owning trait implementation is the sole behavioral
contract; callers import that trait to use its methods. Legitimate inherent
constructors remain valid.

Dispatch stays inside `impl SearchBackend for SearchEngineInner`, where each
arm calls receiver methods on trait-implementing payload structs. The handle's
trait implementation delegates to that private enum and owns no second match,
as shown in the sync and native async examples above.

A concrete implementation that misses a required method fails to compile. The
owning trait is the parity contract (private in this closed-family example,
public only at a root-approved behavioral seam).

Do not use wildcard arms for implementation-family dispatch:

```rust
match self {
    Self::Local(inner) => inner.search(req).await,
    _ => Err(SearchError),
}
```

The wildcard hides new variants from the compiler's exhaustiveness pressure.

Do not move concrete impls into the shared contract module:

```rust
// contract.rs
impl SearchBackend for local::LocalSearch {
    /* ... */
}
```

Concrete implementation knowledge belongs with the concrete implementation.

Do not replace hand-written dispatch with `macro_rules!`, proc macros,
generated dispatch, generated tables, arbitrary public facade traits,
compatibility shims, or `async_trait`. The explicitly approved Tool public
RPITIT contract plus private erased extension arm is the narrow exception; the
enum and its built-in arms remain hand-written. Provider, Storage, and Agent
use their public RPITIT contracts without erasure.

Do not use a private dispatcher or config-selection enum as a reusable identity
object:

```rust
pub(crate) enum BackendKind {
    Local,
    Remote,
}

impl BackendKind {
    pub(crate) const fn display_name(self) -> &'static str {
        match self {
            Self::Local => "Local",
            Self::Remote => "Remote",
        }
    }
}
```

That pattern leaks implementation-family identity into callers that should be
talking to the owning payload or owning dispatch trait (public only at an
approved behavioral seam).

## 7. Review checklist

- Does every behavior-dispatch enum have exactly one owning behavioral
  contract—private for a closed family, public only for an approved behavioral
  seam?
- Does every built-in enum variant wrap a concrete struct, with dynamic erasure
  confined to one private `Extension` variant where approved?
- Does the enum trait impl use explicit match arms for every variant?
- Are concrete trait impls in the owning implementation subtree?
- Are shared contract modules provider-agnostic?
- Are public construction/config owners still the built-in construction
  boundary, does Toolset create only its extension arm and normalize only its
  associated ToolHandler, and do Provider, Storage, and Agent callers remain
  statically typed?
- Are implementation-family identity, provenance, capability, and diagnostics
  kept out of shared kind/helper enums?
- Are wildcard/catch-all dispatch arms and stale restoration TODO markers
  absent for implementation families?
- Do compile/behavior tests prove the public trait and associated children, and
  do narrow source guards reject prohibited erasure or generated replacement?
