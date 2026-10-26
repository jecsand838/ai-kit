# Canonical Prompt Examples

Use these prompts when manually validating that
`audit-workspace-for-shims` stays target-scoped, findings-first, and honest
about read-only or Plan Mode constraints.

## Manual Command Pattern

Use a fresh temporary Codex home and capture the last message to a file:

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/<output-file>.txt \
  $'Use $audit-workspace-for-shims ...'
```

If Codex exits early because of authentication, DNS, or state-db issues, record
that as an environment blocker rather than a skill failure.

## Provider-SDK Audit

```text
Use $audit-workspace-for-shims to audit provider-sdk for hacks, shims,
synthetic fallbacks, strict-live output shaping, and unit-test assumptions that
lack strict-live or provider-doc proof. Annotate every confirmed issue and
return findings first.
```

Expected checks:

- The response stays anchored to `provider-sdk`.
- The response reports findings first and does not start the removal pass.

## Agent-SDK Audit

```text
Use $audit-workspace-for-shims to audit agent-sdk for synthetic behavior,
compatibility shims, unproven non-live server assumptions, and strict-live
output shaping. Annotate every confirmed issue and return findings first.
```

Expected checks:

- The response stays anchored to `agent-sdk`.
- The response loads the matching crate contract and research skill only when
  server facts matter.

## Storage-SDK Audit

```text
Use $audit-workspace-for-shims to audit storage-sdk for hacks, synthetic
responses, silent success fallbacks, and non-live backend expectations that
lack strict-live or backend-doc proof. Annotate every confirmed issue and
return findings first.
```

Expected checks:

- The response stays anchored to `storage-sdk`.
- The response does not widen into provider or agent work.

## Tool-SDK Audit

```text
Use $audit-workspace-for-shims to audit tool-sdk for hacks, synthetic MCP
fallbacks, provider-hosted execution shims, and non-live tool expectations that
lack strict-live, MCP-spec, or provider-doc proof. Annotate every confirmed
issue and return findings first.
```

Expected checks:

- The response stays anchored to `tool-sdk`.
- The response loads `tool-sdk-change-contract` and adds `tool-sdk-research`
  only when MCP, provider-hosted, local runtime, or strict-live facts matter.

## Missing-Target Boundary

```text
Use $audit-workspace-for-shims to audit the workspace for shims and annotate
anything suspicious.
```

Expected checks:

- The response asks for the missing target crate instead of inferring one.
- The response names `agent-sdk`, `provider-sdk`, `storage-sdk`, and
  `tool-sdk` as the supported targets.

## Read-Only Or Plan-Mode Honesty Check

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/audit-workspace-for-shims-readonly.txt \
  $'Use $audit-workspace-for-shims to audit provider-sdk for shims and annotate every confirmed issue with DELETE(provider-sdk shim audit) markers.'
```

Expected checks:

- The response reports findings without pretending the markers were written.
- The response explicitly says the deletion markers were not written because
  edits were not allowed.
