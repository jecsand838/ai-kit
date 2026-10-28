# Local Toolset Research

Use this reference for `Toolset::local(Vec<ToolRegistration>)`.

## Sources

- <https://docs.rs/schemars/latest/schemars/>
- <https://docs.rs/jsonschema/latest/jsonschema/>
- <https://docs.rs/tokio-util/latest/tokio_util/>
- [`tools-kit/AGENTS.md`](../../../../tools-kit/AGENTS.md)

## Verify

- ToolInfo contains the complete declaration and schemas.
- Registration-time validation rejects empty/duplicate names and invalid
  schemas.
- Invocation-time validation rejects invalid arguments/results.
- ToolRegistration owns one private handler implementation without duplicated
  function/interactive registration families.
- Interaction requests remain source-neutral and resumable through one
  Handler.
- Tool-authored failures become completed error ToolResults.
- The two copy-on-write Arc unwrap paths transfer registration ownership and
  do not participate in lifecycle cleanup.

Keep typed local adapters and handler resources private. Do not add source ids,
config wrappers, public callback aliases, or alternate execution entrypoints.
