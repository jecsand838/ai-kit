> **ai-kit policy override:** This file is background research, not normative
> workspace guidance. The parent skill's hard rules supersede permissive
> standalone-function language below. Struct fields are private by default;
> non-private fields are allowed only on an intentional passive DTO with no
> runtime resources, synchronization, lifecycle transitions, or domain
> behavior.

## Research basis

Rust’s official guidance points toward a type-centered design: use structs to create meaningful domain types with related data, use `impl` blocks to attach behavior to those types, and use enums when a value is one of several possible variants. The Rust Book also shows that enums can store data per variant and can have methods just like structs. The Rust Reference gives the deeper rule: associated items are useful when they are logically related to the type, and methods are associated functions whose first parameter is `self`. ([Rust Documentation][1])

For the agent skill you described, the practical rule should be: **standalone functions are a smell when they operate on a clear domain object.** Prefer `impl Type { ... }`, use `&self` for read-only behavior, `&mut self` for mutation, `self` for consuming transitions, and `Type::new` / `Type::build` for constructors. Rust’s method-call ergonomics are designed around this receiver model. ([Rust Documentation][1])

---

# 1. How to identify when to use a struct vs an enum

Use a **struct** when the data fields belong together and exist at the same time. A struct is a “this has these fields” model.

Use an **enum** when the value is exactly one of a fixed set of alternatives. An enum is a “this is one of these cases” model. Enum variants can carry different data, which is often cleaner than a struct full of `Option<T>` fields. The Rust Book’s `Message` example demonstrates variants with no data, named fields, tuple-like data, and single-value payloads. ([Rust Documentation][2])

Use a trait object or trait-based design when downstream code must add new kinds later. The Rust Book describes enum-based heterogeneity as a good fit when the set of types is fixed and known at compile time; trait objects are better when the valid set needs to be extensible. ([Rust Documentation][3])

### Bad: one struct pretending to be several different message types

```rust
pub struct AgentMessage {
    pub kind: String,
    pub text: Option<String>,
    pub tool_name: Option<String>,
    pub arguments_json: Option<String>,
    pub error: Option<String>,
}

pub fn render_message(message: &AgentMessage) -> String {
    match message.kind.as_str() {
        "user" | "assistant" => message.text.clone().unwrap_or_default(),

        "tool_call" => format!(
            "call {}({})",
            message.tool_name.as_deref().unwrap_or("<missing tool>"),
            message.arguments_json.as_deref().unwrap_or("{}"),
        ),

        "error" => format!(
            "error: {}",
            message.error.as_deref().unwrap_or("unknown"),
        ),

        _ => String::new(),
    }
}
```

Problems:

The `kind: String` is unchecked. `"toolcal"`, `"TOOL_CALL"`, and `""` all compile.

Most fields are invalid for most states.

The compiler cannot prove that `ToolCall` always has a tool name and arguments.

Behavior is detached from the data.

### Good: enum for mutually exclusive cases, structs for grouped payloads

```rust
pub struct ToolCall {
    pub name: String,
    pub arguments_json: String,
}

pub struct ToolFailure {
    pub tool_name: String,
    pub message: String,
}

pub enum AgentMessage {
    UserText(String),
    AssistantText(String),
    ToolCall(ToolCall),
    ToolFailure(ToolFailure),
}

impl AgentMessage {
    pub fn render(&self) -> String {
        match self {
            Self::UserText(text) => text.clone(),
            Self::AssistantText(text) => text.clone(),

            Self::ToolCall(call) => {
                format!("call {}({})", call.name, call.arguments_json)
            }

            Self::ToolFailure(failure) => {
                format!("tool {} failed: {}", failure.tool_name, failure.message)
            }
        }
    }

    pub fn is_failure(&self) -> bool {
        matches!(self, Self::ToolFailure(_))
    }
}
```

The key pattern:

```rust
// Struct: fields are simultaneously true.
pub struct ToolCall {
    pub name: String,
    pub arguments_json: String,
}

// Enum: exactly one variant is active.
pub enum AgentMessage {
    UserText(String),
    AssistantText(String),
    ToolCall(ToolCall),
    ToolFailure(ToolFailure),
}
```

---

# 2. Identifying what fields to put in the struct based on logical associations

Fields belong in the same struct when they share a **lifecycle**, **responsibility**, **invariant**, and **method surface**.

Good field-grouping questions:

Does this data get created together?

Is it passed around together?

Do methods need these fields together?

Does an invariant span these fields?

Would naming the group make the code easier to understand?

The Rust Book’s `minigrep` refactor shows this exact move: returning a tuple of related config values was a sign that the abstraction was missing, so the values were grouped into a `Config` struct with meaningful field names. ([Rust Documentation][4])

Rust API Guidelines also recommend custom types over ambiguous primitives like `bool`, `u8`, and `Option` when the type can convey meaning or invariants. Newtypes are recommended when two values share the same underlying primitive but mean different things. ([Rust Programming Language][5])

### Bad: one “god struct” mixing config, request, runtime, and error state

```rust
pub struct Agent {
    pub user_id: String,
    pub session_id: String,
    pub prompt: String,

    pub model: String,
    pub temperature: f32,
    pub max_steps: usize,

    pub step_count: usize,
    pub last_error: Option<String>,
    pub tool_names: Vec<String>,
}

pub fn run_agent(agent: &mut Agent) {
    while agent.step_count < agent.max_steps {
        agent.step_count += 1;

        // Config, request data, tool registry, and runtime state
        // are all tangled together.
    }
}
```

Problems:

`user_id`, `session_id`, and `prompt` are per-run request data.

`model`, `temperature`, and `max_steps` are configuration.

`step_count` and `last_error` are runtime state.

`tool_names` is a registry concern.

A future change to one concept risks breaking all the others.

### Good: split structs by logical association and attach methods to the owner

```rust
pub struct UserId(pub String);
pub struct SessionId(pub String);
pub struct ModelName(pub String);

pub struct Temperature(pub f32);

pub struct AgentConfig {
    pub model: ModelName,
    pub temperature: Temperature,
    pub max_steps: usize,
}

pub struct ToolRegistry {
    pub names: Vec<String>,
}

pub struct RunRequest {
    pub user: UserId,
    pub session: SessionId,
    pub prompt: String,
}

pub struct Agent {
    config: AgentConfig,
    tools: ToolRegistry,
}

pub struct AgentRun {
    request: RunRequest,
    max_steps: usize,
    step_count: usize,
    messages: Vec<AgentMessage>,
}

impl Agent {
    pub fn new(config: AgentConfig, tools: ToolRegistry) -> Self {
        Self { config, tools }
    }

    pub fn start_run(&self, request: RunRequest) -> AgentRun {
        AgentRun {
            request,
            max_steps: self.config.max_steps,
            step_count: 0,
            messages: Vec::new(),
        }
    }

    pub fn tool_count(&self) -> usize {
        self.tools.names.len()
    }
}

impl AgentRun {
    pub fn record(&mut self, message: AgentMessage) {
        self.messages.push(message);
    }

    pub fn remaining_steps(&self) -> usize {
        self.max_steps.saturating_sub(self.step_count)
    }
}
```

The struct-boundary heuristic:

```rust
// Stable setup data.
pub struct AgentConfig {
    pub model: ModelName,
    pub temperature: Temperature,
    pub max_steps: usize,
}

// Per-run input.
pub struct RunRequest {
    pub user: UserId,
    pub session: SessionId,
    pub prompt: String,
}

// Runtime state.
pub struct AgentRun {
    request: RunRequest,
    max_steps: usize,
    step_count: usize,
    messages: Vec<AgentMessage>,
}
```

Each struct now has a clear owner and a clear method surface.

---

# 3. Identifying what enum variants to use

Enum variants should represent **real, mutually exclusive states, events, commands, or outcomes**.

Good variant-identification questions:

Can exactly one of these cases be true at a time?

Does each case need different required data?

Would a `match` over these cases make the logic clearer?

Would adding a new case require the compiler to force updates in all relevant logic?

Are you currently using `String`, `bool`, or many `Option<T>` fields to represent state?

Rust enums are especially useful because variants are grouped under one type, each variant can carry its own data, and `match` can force handling of all cases. ([Rust Documentation][2])

Do **not** use an enum when multiple options can be active at the same time. Rust API Guidelines specifically note that enums represent exactly one choice among many; flag sets should use a flag representation instead. ([Rust Programming Language][5])

### Bad: enum kind plus unrelated optional fields

```rust
#[derive(Clone, Copy)]
pub enum AgentStepKind {
    Thought,
    ToolCall,
    ToolResult,
    FinalAnswer,
    Failed,
}

pub struct AgentStep {
    pub kind: AgentStepKind,

    pub text: Option<String>,
    pub tool_name: Option<String>,
    pub arguments_json: Option<String>,
    pub result_json: Option<String>,
    pub error_message: Option<String>,
}

pub fn step_is_terminal(step: &AgentStep) -> bool {
    matches!(
        step.kind,
        AgentStepKind::FinalAnswer | AgentStepKind::Failed
    )
}
```

Problems:

`AgentStepKind::ToolCall` does not guarantee `tool_name` or `arguments_json`.

`AgentStepKind::Failed` does not guarantee `error_message`.

Invalid states compile.

The behavior is in a free function even though it clearly belongs to `AgentStep`.

### Good: variants carry the required data for their case

```rust
pub struct ToolOutput {
    pub tool_name: String,
    pub result_json: String,
}

pub struct AgentError {
    pub message: String,
}

pub enum AgentStep {
    Thought(String),
    ToolCall(ToolCall),
    ToolResult(ToolOutput),
    FinalAnswer(String),
    Failed(AgentError),
}

impl AgentStep {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::FinalAnswer(_) | Self::Failed(_))
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Thought(_) => "thought",
            Self::ToolCall(_) => "tool_call",
            Self::ToolResult(_) => "tool_result",
            Self::FinalAnswer(_) => "final_answer",
            Self::Failed(_) => "failed",
        }
    }
}
```

The variant rule:

```rust
pub enum AgentStep {
    // Different case, different required data.
    Thought(String),
    ToolCall(ToolCall),
    ToolResult(ToolOutput),
    FinalAnswer(String),
    Failed(AgentError),
}
```

This is better than:

```rust
pub struct AgentStep {
    pub kind: AgentStepKind,
    pub text: Option<String>,
    pub error_message: Option<String>,
    // ...
}
```

because the compiler now helps enforce the model.

---

# 4. Using struct + enum receiver methods over standalone functions

The default agent skill should be:

> When a function’s first parameter is `&T`, `&mut T`, or `T`, strongly prefer making it a method on `T`.

Rust’s method model makes the receiver explicit: `&self` reads, `&mut self` mutates, and `self` consumes. The Rust Book highlights that method calls are ergonomically supported by automatic referencing and dereferencing, and the Reference defines methods as associated functions with a `self` parameter. ([Rust Documentation][1])

Associated functions without `self`, such as `Type::new` or `Type::build`, are still type-centered and should be preferred over loose constructor functions. The Rust API Guidelines describe constructors as static inherent methods, and the Rust Book’s `Config::build` refactor is a good example of moving construction logic onto the type. ([Rust Programming Language][6])

### Bad: standalone functions orbiting a type

```rust
pub struct AgentRun {
    pub max_steps: usize,
    pub steps: Vec<AgentStep>,
}

pub struct RunReport {
    pub steps: Vec<AgentStep>,
}

pub fn new_agent_run(max_steps: usize) -> AgentRun {
    AgentRun {
        max_steps,
        steps: Vec::new(),
    }
}

pub fn push_agent_step(run: &mut AgentRun, step: AgentStep) {
    run.steps.push(step);
}

pub fn agent_run_is_finished(run: &AgentRun) -> bool {
    run.steps.len() >= run.max_steps
        || run.steps.last().is_some_and(AgentStep::is_terminal)
}

pub fn finish_agent_run(run: AgentRun) -> RunReport {
    RunReport { steps: run.steps }
}
```

Problems:

The function names repeat the type name: `agent_run_*`.

The first argument is always `AgentRun`.

Ownership intent is hidden in function form instead of receiver form.

The API is scattered.

### Good: receiver methods express ownership and organize behavior

```rust
pub struct AgentRun {
    max_steps: usize,
    steps: Vec<AgentStep>,
}

pub struct RunReport {
    pub steps: Vec<AgentStep>,
}

impl AgentRun {
    pub fn new(max_steps: usize) -> Self {
        Self {
            max_steps,
            steps: Vec::new(),
        }
    }

    pub fn push_step(&mut self, step: AgentStep) {
        self.steps.push(step);
    }

    pub fn is_finished(&self) -> bool {
        self.steps.len() >= self.max_steps
            || self.steps.last().is_some_and(AgentStep::is_terminal)
    }

    pub fn finish(self) -> RunReport {
        RunReport { steps: self.steps }
    }
}
```

The receiver choices tell the story:

```rust
impl AgentRun {
    // Constructor: no existing instance yet.
    pub fn new(max_steps: usize) -> Self {
        Self {
            max_steps,
            steps: Vec::new(),
        }
    }

    // Mutates the run.
    pub fn push_step(&mut self, step: AgentStep) {
        self.steps.push(step);
    }

    // Reads the run.
    pub fn is_finished(&self) -> bool {
        self.steps.len() >= self.max_steps
            || self.steps.last().is_some_and(AgentStep::is_terminal)
    }

    // Consumes the run and turns it into a report.
    pub fn finish(self) -> RunReport {
        RunReport { steps: self.steps }
    }
}
```

---

# Agent skill foundation: rules to encode

Use these as the actual refactoring policy for the agent.

## Rule 1: Prefer domain types over primitive soup

Bad signs:

```rust
fn start_run(
    user_id: String,
    session_id: String,
    prompt: String,
    model: String,
    temperature: f32,
    max_steps: usize,
) {
    // ...
}
```

Good target:

```rust
pub struct RunRequest {
    pub user: UserId,
    pub session: SessionId,
    pub prompt: String,
}

pub struct AgentConfig {
    pub model: ModelName,
    pub temperature: Temperature,
    pub max_steps: usize,
}

impl Agent {
    pub fn start_run(&self, request: RunRequest) -> AgentRun {
        AgentRun {
            request,
            max_steps: self.config.max_steps,
            step_count: 0,
            messages: Vec::new(),
        }
    }
}
```

## Rule 2: Convert repeated parameter groups into structs

When the same 2–5 parameters appear together in multiple functions, introduce a struct.

Bad:

```rust
pub fn estimate_cost(model: &str, input_tokens: usize, output_tokens: usize) -> u64 {
    input_tokens as u64 + output_tokens as u64 + model.len() as u64
}

pub fn check_budget(model: &str, input_tokens: usize, output_tokens: usize, budget: u64) -> bool {
    estimate_cost(model, input_tokens, output_tokens) <= budget
}
```

Good:

```rust
pub struct TokenEstimate {
    pub model: ModelName,
    pub input_tokens: usize,
    pub output_tokens: usize,
}

impl TokenEstimate {
    pub fn cost_units(&self) -> u64 {
        self.input_tokens as u64
            + self.output_tokens as u64
            + self.model.0.len() as u64
    }

    pub fn fits_budget(&self, budget: u64) -> bool {
        self.cost_units() <= budget
    }
}
```

## Rule 3: Convert stringly typed state into enums

Bad:

```rust
pub struct ToolStatus {
    pub status: String,
    pub output: Option<String>,
    pub error: Option<String>,
}
```

Good:

```rust
pub enum ToolStatus {
    Pending,
    Running,
    Succeeded(String),
    Failed(AgentError),
}

impl ToolStatus {
    pub fn is_done(&self) -> bool {
        matches!(self, Self::Succeeded(_) | Self::Failed(_))
    }
}
```

## Rule 4: Move behavior to the type it belongs to

Bad:

```rust
pub fn tool_status_is_done(status: &ToolStatus) -> bool {
    matches!(status, ToolStatus::Succeeded(_) | ToolStatus::Failed(_))
}
```

Good:

```rust
impl ToolStatus {
    pub fn is_done(&self) -> bool {
        matches!(self, Self::Succeeded(_) | Self::Failed(_))
    }
}
```

## Rule 5: Use associated constructors instead of loose constructor functions

Bad:

```rust
pub fn make_tool_call(name: String, arguments_json: String) -> ToolCall {
    ToolCall {
        name,
        arguments_json,
    }
}
```

Good:

```rust
impl ToolCall {
    pub fn new(name: String, arguments_json: String) -> Self {
        Self {
            name,
            arguments_json,
        }
    }
}
```

For complex construction, introduce a builder. Rust API Guidelines recommend builders when construction involves many inputs, optional configuration, compound data, or several construction flavors. ([Rust Programming Language][5])

---

# Standalone function exceptions

The “99% no standalone functions” rule is a useful agent-code default, but it should not become dogma. Standalone functions are still reasonable when there is no natural receiver, when the operation is truly symmetric across unrelated types, when the function is a tiny private module helper, or when it is an entry point such as `main`. The Rust Book itself extracts standalone `run` and `search` functions while refactoring a CLI, but it also groups related config values into a struct and moves config construction to `Config::build`. ([Rust Documentation][4])

For the agent skill, the enforcement rule should be:

```text
Before writing a standalone function, ask:

1. Does this function read, mutate, or consume a domain type?
   -> Put it in impl Type.

2. Does this function construct a domain type?
   -> Use Type::new, Type::build, Type::from_*, or TryFrom.

3. Do several arguments travel together?
   -> Create a struct, then attach methods.

4. Is the function switching on strings, bools, or Option fields?
   -> Create an enum with data-carrying variants.

5. Is the operation truly not owned by any domain type?
   -> A standalone function is acceptable.
```

The clean target style is:

```rust
let mut run = agent.start_run(request);

run.push_step(AgentStep::Thought("Inspect user request".to_string()));

if run.is_finished() {
    let report = run.finish();
    // use report
}
```

not:

```rust
let mut run = start_agent_run(&agent, request);

push_agent_step(
    &mut run,
    AgentStep::Thought("Inspect user request".to_string()),
);

if agent_run_is_finished(&run) {
    let report = finish_agent_run(run);
    // use report
}
```

The first version reads like domain behavior. The second version reads like a bag of procedural helpers.

[1]: https://doc.rust-lang.org/book/ch05-03-method-syntax.html "Methods - The Rust Programming Language"
[2]: https://doc.rust-lang.org/book/ch06-01-defining-an-enum.html "Defining an Enum - The Rust Programming Language"
[3]: https://doc.rust-lang.org/book/ch18-02-trait-objects.html "Using Trait Objects to Abstract over Shared Behavior - The Rust Programming Language"
[4]: https://doc.rust-lang.org/book/ch12-03-improving-error-handling-and-modularity.html "Refactoring to Improve Modularity and Error Handling - The Rust Programming Language"
[5]: https://rust-lang.github.io/api-guidelines/type-safety.html "Type safety - Rust API Guidelines"
[6]: https://rust-lang.github.io/api-guidelines/predictability.html "Predictability - Rust API Guidelines"
