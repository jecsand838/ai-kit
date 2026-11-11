# Local Toolset Research

Use this reference for `Toolset::local(Vec<ToolRegistration>)`.

## Sources

- <https://docs.rs/schemars/latest/schemars/>
- <https://docs.rs/jsonschema/latest/jsonschema/>
- <https://docs.rs/tokio-util/latest/tokio_util/>
- [`tools-kit/AGENTS.md`](../../../../tools-kit/AGENTS.md)

## Verify

- ToolInfo contains the complete declaration and schemas.
- `ToolName` construction rejects invalid names. Local Toolset construction
  rejects duplicate names and permits an empty inventory.
- `JsonSchema` construction wraps a passive schema document. Local registration
  does not compile schemas; the owning invocation and completion operations
  compile them and reject malformed schemas or invalid arguments/results.
  Successful output must satisfy a declared output schema; completed
  model-visible error results retain their schema-validation exemption.
- ToolRegistration owns one private handler implementation without duplicated
  function/interactive registration families.
- Interaction requests remain source-neutral and resumable through one
  Handler.
- Tool-authored failures become completed error ToolResults.
- The two copy-on-write Arc unwrap paths transfer registration ownership and
  do not participate in lifecycle cleanup.

Keep typed local adapters and handler resources private. Do not add source ids,
config wrappers, public callback aliases, or alternate execution entrypoints.
