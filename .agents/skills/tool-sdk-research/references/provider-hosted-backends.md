# Provider-Hosted Boundary

Provider-hosted tools are not executable tool-sdk sources.

`provider-sdk::hosted::HostedTool` is a closed request vocabulary validated and
rendered by a selected provider Model. Provider-specific combination rules and
hosted-output normalization remain in provider-sdk.

tool-sdk owns only caller-executed Local/MCP Toolsets. A caller or agent may
translate ToolInfo into model function declarations and execute returned calls
through ToolHandler. Provider-sdk renders generation requests, returns
normalized function calls, and accepts normalized function results; it does not
execute caller tools. Tool-sdk does not own provider request rendering, hosted
names/configs, provider capability checks, or hosted outputs.

Reject hosted Toolset variants, provider declaration payloads, mixed
inventories, provider route ids, or treating a provider-managed tool as a
ToolHandler.
