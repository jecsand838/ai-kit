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
tool and arguments, and bounded timeout. It proves live inventory, one call,
and settled Toolset shutdown. It fails fast on missing environment and never
substitutes mocks, recordings, or repository-scripted servers.
