# Rust abstractions, lifetimes, errors, and panics

Use this reference when refactoring repeated logic, designing APIs, choosing abstraction mechanisms, or clarifying panic and unsafe policy.

## 1. Keep code concrete until pressure is real

Start concrete when there is only one implementation and one obvious call path.

### Good first step

```rust
pub struct PriceCalculator {
    tax_rate: f64,
}

impl PriceCalculator {
    pub fn new(tax_rate: f64) -> Self {
        Self { tax_rate }
    }

    pub fn total(&self, subtotal: f64) -> f64 {
        subtotal * (1.0 + self.tax_rate)
    }
}
```

Do not introduce a trait, generic parameter, or enum-dispatch wrapper just because the code might grow later.

Refactor when you see one of these signals:

- repeated business rules across call sites
- multiple real implementations
- repeated argument groups that should become a struct
- repeated branching over a closed set of variants
- repeated need for test doubles at a boundary

## 2. Prefer structs with methods over parallel free functions

### Bad

```rust
pub fn create_invoice(customer_id: &str, due_days: u32, currency: &str) -> Invoice {
    Invoice {
        customer_id: customer_id.to_owned(),
        due_days,
        currency: currency.to_owned(),
    }
}

pub fn validate_invoice(customer_id: &str, due_days: u32, currency: &str) -> Result<(), String> {
    if customer_id.is_empty() {
        return Err("customer id required".to_owned());
    }

    if due_days == 0 {
        return Err("due days must be positive".to_owned());
    }

    if currency.len() != 3 {
        return Err("currency must be ISO-4217".to_owned());
    }

    Ok(())
}

#[derive(Debug)]
pub struct Invoice {
    customer_id: String,
    due_days: u32,
    currency: String,
}
```

### Better

```rust
#[derive(Debug, Clone)]
pub struct InvoiceDraft {
    customer_id: String,
    due_days: u32,
    currency: String,
}

impl InvoiceDraft {
    pub fn new(customer_id: impl Into<String>, due_days: u32, currency: impl Into<String>) -> Self {
        Self {
            customer_id: customer_id.into(),
            due_days,
            currency: currency.into(),
        }
    }

    pub fn validate(&self) -> Result<(), InvoiceError> {
        if self.customer_id.is_empty() {
            return Err(InvoiceError::MissingCustomerId);
        }

        if self.due_days == 0 {
            return Err(InvoiceError::InvalidDueDays);
        }

        if self.currency.len() != 3 {
            return Err(InvoiceError::InvalidCurrency(self.currency.clone()));
        }

        Ok(())
    }

    pub fn build(self) -> Result<Invoice, InvoiceError> {
        self.validate()?;
        Ok(Invoice {
            customer_id: self.customer_id,
            due_days: self.due_days,
            currency: self.currency,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Invoice {
    customer_id: String,
    due_days: u32,
    currency: String,
}

#[derive(Debug, thiserror::Error)]
pub enum InvoiceError {
    #[error("customer id is required")]
    MissingCustomerId,
    #[error("due days must be positive")]
    InvalidDueDays,
    #[error("invalid currency: {0}")]
    InvalidCurrency(String),
}
```

This groups related data and behavior coherently.

## 3. Choose the right abstraction mechanism

### Stay concrete when

- there is one implementation
- there is no real extension point
- indirection would only make the code harder to read

### Use generics when

- the caller chooses the implementation at compile time
- performance and inlining matter
- the abstraction is broadly useful and type-driven

```rust
pub fn sum_prices<I>(items: I) -> u64
where
    I: IntoIterator<Item = u64>,
{
    items.into_iter().sum()
}
```

### Use a trait when

- multiple implementations are real today
- callers benefit from a shared interface
- tests need boundary-level fakes

```rust
pub(crate) trait UserRepository {
    async fn fetch(&self, user_id: u64) -> Result<User, RepoError>;
}

#[derive(Debug, Clone)]
pub struct User {
    pub id: u64,
}

#[derive(Debug, thiserror::Error)]
#[error("repository failure")]
pub struct RepoError;
```

### Use a trait object when

- the set of implementations is open at runtime
- dynamic dispatch is acceptable
- you need heterogeneous values behind one interface

```rust
pub struct Processor {
    backends: Vec<Box<dyn JobBackend>>,
}

pub trait JobBackend {
    fn name(&self) -> &str;
    fn run(&self);
}
```

### Use an enum when

- the set of variants is closed
- pattern matching is valuable
- you want exhaustiveness and direct control

```rust
pub enum OutputFormat {
    Json,
    Yaml,
    Text,
}
```

## 4. Use the enum-dispatcher pattern for closed configurable backends

This pattern is a strong default when you have:

- a trait representing behavior
- a closed set of backend implementations
- a config enum that chooses the backend
- a builder that validates config and returns an opaque public handle over a
  private inner enum

Read `enum-dispatch-trait-pattern.md` before changing this pattern.

In `ai-kit`, this is not optional for behavior dispatch. Any behavior-dispatch
family for agent, provider, provider-model, database, tool-runtime, or
tool-source implementation behavior must have one provider-agnostic trait
contract. For these lower-crate families it is public RPITIT only at a
root-approved backend seam and private otherwise. App's five additional
workflow traits stay static, with the exact `ChildExecutor` associated-future
exception; its field/tag serialization registry alone may erase codec
functions, never runtime or lower-backend values. Exported dispatchers use an
opaque public handle
over a private inner enum; private internal dispatch enums can remain plain
private enums. Those private enums and build/config selection enums are routing
mechanisms only; they must not become reusable implementation-family identity,
provenance, capability, diagnostic-label, or error-label utilities. The trait
is hand-written, implemented by every concrete family member, and entered
through the dispatching handle or enum. Dispatcher arms
must call receiver methods on those trait-implementing payloads, such as
`backend.search(req).await`; `SearchBackend::search(backend, req).await` is an
invalid dispatch-contract shape. At the approved Tool seam the private enum
retains concrete built-ins and adds one erased `Extension` arm; only that arm
uses a trait object. Provider, Storage, and Agent dispatchers remain closed
over built-ins while their public traits support direct static implementations
and caller-owned enums. Do not replace the enum/contract with inherent-method parity by
convention, bare trait-object storage, arbitrary public facade traits,
compatibility shims, `macro_rules!`, proc macros, generated dispatch, or
generated tables. Shared contract modules may define only provider-agnostic
traits, shared handles, and default helpers; concrete impl blocks must live in
the owning backend, provider, model, database, runtime, or source subtree.
Root-handle behavior belongs in its owning trait implementation, which
delegates through the private dispatcher or its runtime owner. Callers import
the trait to use its methods. Do not add same-name inherent forwarding
methods; legitimate inherent constructors remain valid. Dispatcher arms over
concrete payloads also use receiver methods.

Private inner enum variants should wrap concrete implementation structs, and
dispatcher methods should delegate through explicit exhaustive `match` arms.
Prefer this over trait objects when the set of backends is closed and known.
Keep backend/provider/source facts in the owning payload or documented
data/config type instead of adding shared `*BackendKind`, `*SourceKind`,
display-name helpers, provenance fields, or capability-label routers.

## 5. Use lifetimes only when the borrow relationship matters

### Most functions do not need explicit lifetime syntax

```rust
pub fn normalize_username(input: &str) -> String {
    input.trim().to_lowercase()
}
```

Lifetime elision already covers this.

### Add explicit lifetimes when the output borrows from an input

```rust
pub fn first_word<'a>(input: &'a str) -> &'a str {
    input.split_whitespace().next().unwrap_or("")
}
```

### Use owned data in long-lived structs and spawned tasks

Bad:

```rust
pub struct UserService<'a> {
    api_base: &'a str,
}
```

This couples the service lifetime to whoever created it.

Better:

```rust
pub struct UserService {
    api_base: String,
}
```

### Borrowing views can still be great

```rust
#[derive(Debug)]
pub struct UserView<'a> {
    pub id: u64,
    pub email: &'a str,
}

pub fn view_user(user: &User) -> UserView<'_> {
    UserView {
        id: user.id,
        email: &user.email,
    }
}

#[derive(Debug)]
pub struct User {
    pub id: u64,
    pub email: String,
}
```

Use explicit lifetimes for short-lived views, parsers, and zero-copy helpers. Use owned data for long-lived services and tasks.

## 6. Prefer typed errors in libraries

### Good library error

```rust
#[derive(Debug, thiserror::Error)]
pub enum ParseAmountError {
    #[error("amount must be positive")]
    NonPositive,
    #[error("invalid number: {0}")]
    InvalidNumber(#[from] std::num::ParseFloatError),
}

pub fn parse_amount(input: &str) -> Result<f64, ParseAmountError> {
    let value: f64 = input.parse()?;
    if value <= 0.0 {
        return Err(ParseAmountError::NonPositive);
    }
    Ok(value)
}
```

Typed errors:

- preserve structure
- compose well
- document failure modes
- let callers branch intentionally

### Use `anyhow` at application boundaries

```rust
pub fn run_cli(input: &str) -> anyhow::Result<()> {
    let amount = parse_amount(input)
        .map_err(|error| anyhow::anyhow!("failed to parse amount from CLI input: {error}"))?;
    println!("{amount}");
    Ok(())
}
```

Use `anyhow` in binaries, top-level commands, or application orchestration. Avoid returning `anyhow::Result` from reusable library APIs unless the task explicitly wants that trade-off.

## 7. Panic only for bugs or impossible invariants

### Acceptable panic use

- impossible internal invariant
- test code
- examples
- one-time process bootstrap where failure means the program cannot continue anyway

### Prefer informative `expect` messages

Bad:

```rust
let config = CONFIG.get().unwrap();
```

Better:

```rust
let config = CONFIG
    .get()
    .expect("CONFIG must be initialized before request handling starts");
```

### Prefer `Result` for recoverable failures

If the caller could reasonably react, return `Result`.

## 8. Keep unsafe blocks tiny and behind safe abstractions

### Good shape

```rust
pub struct NonNullSlice<'a> {
    ptr: std::ptr::NonNull<u8>,
    len: usize,
    _marker: std::marker::PhantomData<&'a [u8]>,
}

impl<'a> NonNullSlice<'a> {
    pub fn from_slice(slice: &'a [u8]) -> Option<Self> {
        let ptr = std::ptr::NonNull::new(slice.as_ptr() as *mut u8)?;
        Some(Self {
            ptr,
            len: slice.len(),
            _marker: std::marker::PhantomData,
        })
    }

    pub fn as_slice(&self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}
```

Only the operation that actually requires `unsafe` is unsafe. The rest of the API is safe and documented.

### Avoid blanket guidance like this in a generic bundle

```toml
[lints.rust]
unsafe_code = "forbid"
```

That can be fine for many application crates, but it is too absolute as universal advice. FFI, low-level abstractions, and performance-sensitive internals may need narrow, audited `unsafe`.

### In Rust 2024-era code, also watch for

- `unsafe_op_in_unsafe_fn`
- newly unsafe environment mutation APIs such as `std::env::set_var` and `remove_var`

When touching these areas, audit them deliberately.

## 9. Abstraction dos and don'ts

### Do

- start concrete
- refactor repeated data + behavior into structs
- add traits only at real boundaries
- use enums for closed sets
- use explicit lifetimes only when the relationship matters
- keep library errors typed
- keep unsafe blocks tiny

### Don't

- create traits for speculative future reuse
- use generics where a concrete type is clearer
- return `anyhow::Result` from every library function by habit
- use panics for normal validation failures
- store references in long-lived services unless that lifetime coupling is intentional
- spread `unsafe` across large regions of code

## Further reading

- Rust book on generics and traits: <https://doc.rust-lang.org/book/ch10-00-generics.html>
- Rust book on lifetimes: <https://doc.rust-lang.org/book/ch10-03-lifetime-syntax.html>
- Rust book on error handling: <https://doc.rust-lang.org/book/ch09-00-error-handling.html>
- Rust book on unsafe Rust: <https://doc.rust-lang.org/book/ch20-01-unsafe-rust.html>
- Rustonomicon: <https://doc.rust-lang.org/nomicon/>


## 10. Additional merged examples

### Refactor once repetition is real

Bad:

```rust
pub fn cli_total(subtotal_cents: u64, tax_bps: u32) -> u64 {
    subtotal_cents + (subtotal_cents * tax_bps as u64 / 10_000)
}

pub fn http_total(subtotal_cents: u64, tax_bps: u32) -> u64 {
    subtotal_cents + (subtotal_cents * tax_bps as u64 / 10_000)
}
```

Better:

```rust
#[derive(Debug, Clone, Copy)]
pub struct MoneyPolicy {
    tax_bps: u32,
}

impl MoneyPolicy {
    pub fn new(tax_bps: u32) -> Self {
        Self { tax_bps }
    }

    pub fn total_cents(self, subtotal_cents: u64) -> u64 {
        subtotal_cents + (subtotal_cents * self.tax_bps as u64 / 10_000)
    }
}
```

Make the refactor when repeated logic or repeated invariants are real, not before.

### Keep enum dispatch explicit in `ai-kit`

For this workspace, do not replace repeated backend, provider, storage, tool,
wire, schema, or test routing code with `macro_rules!`, proc-macro
indirection, generated dispatch, generated tables, or codegen-style hidden
abstraction. The enum-dispatch plus builder architecture remains explicit so
reviewers can see each supported backend, provider, or source family at the
operation boundary.
Trait definitions stay in shared provider-agnostic contract modules, while
concrete impl blocks stay in the owning implementation subtree.
Use `enum-dispatch-trait-pattern.md` for the full checklist, sync example, and
native async trait example.

When forwarding boilerplate grows, first improve the model underneath:

- group repeated data into named structs
- move validation and lowering onto methods
- use closed enums for closed sets
- use typed spec data for repeated constants and tables
- use small helper functions for local behavior that has no state

The goal is to delete duplicated rules by giving them an owner, not to hide
duplicated branches behind generated code.

## Additional source links

- Rust book: generic data types: <https://doc.rust-lang.org/book/ch10-01-syntax.html>
- Rust book: traits: <https://doc.rust-lang.org/book/ch10-02-traits.html>
- Rust book: lifetimes: <https://doc.rust-lang.org/book/ch10-03-lifetime-syntax.html>
- Rust book: error handling: <https://doc.rust-lang.org/book/ch09-00-error-handling.html>
- Rust book: to panic or not to panic: <https://doc.rust-lang.org/book/ch09-03-to-panic-or-not-to-panic.html>
- `thiserror` docs: <https://docs.rs/thiserror>
- `anyhow` docs: <https://docs.rs/anyhow>
- Rust API Guidelines checklist: <https://rust-lang.github.io/api-guidelines/checklist.html>
- Rust design patterns: <https://rust-unofficial.github.io/patterns/>
- Rust builder pattern: <https://rust-unofficial.github.io/patterns/patterns/creational/builder.html>
- Rust book on enums: <https://doc.rust-lang.org/book/ch06-01-defining-an-enum.html>
- Rust book on match exhaustiveness: <https://doc.rust-lang.org/book/ch06-02-match.html>
- Rust blog on async fn in traits: <https://blog.rust-lang.org/2023/12/21/async-fn-rpit-in-traits/>
