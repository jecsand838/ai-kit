# tools-kit Validation Reference

## Canonical Commands

```bash
bash tools-kit/scripts/check.sh
source tools-kit/strict-tests.env && bash tools-kit/scripts/verify_strict_live.sh
```

## Deterministic Proof

The non-live gate owns:

- associated custom Handler compile/runtime contracts;
- Toolset-only public ingress and child-ingress privacy;
- direct Local/MCP construction;
- ToolInfo/schema/name validation;
- handler completion, interaction, model-visible failure, invalid transition,
  cancellation, and cleanup;
- shared Toolset shutdown and terminal replay;
- MCP client transports, names, empty inventory, form interaction, and remote
  cancellation;
- independent MCP server startup, pagination, call mapping, task ownership,
  consuming shutdown, and Toolset independence;
- exact obsolete-symbol/source-contract checks;
- rustdoc/doctests and docs-site.

Repository-managed MCP services prove deterministic transport only.

## Strict-Live Proof

The strict gate requires a real independently configured MCP service, expected
tool and arguments, a complete independent successful `ToolResult` supplied in
`TEST_TOOLS_KIT_MCP_EXPECTED_RESULT_JSON`, and a bounded timeout. Parse the expected
result before connecting. After settled Toolset shutdown, compare the complete
result: exact ordered content, structured content, metadata, and failure status.
Success status alone is insufficient. Never derive expectations from observed
output or normalize volatile fields away.

This proves live inventory, one real call, exact returned contents, and settled
Toolset shutdown. Missing environment fails fast; mocks, recordings, and
repository-scripted servers never substitute for the independent service.
The [strict-live guide](../../../../tools-kit/docs/STRICT_TESTING.md#strict-live-mcp-gate)
owns the exact prerequisites and proof boundary.
