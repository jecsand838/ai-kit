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

## providers-kit Audit

```text
Use $audit-workspace-for-shims to audit providers-kit for hacks, shims,
synthetic fallbacks, strict-live output shaping, and unit-test assumptions that
lack strict-live or provider-doc proof. Annotate every confirmed issue and
return findings first.
```

Expected checks:

- The response stays anchored to `providers-kit`.
- The response reports findings first and does not start the removal pass.

## agents-kit Audit

```text
Use $audit-workspace-for-shims to audit agents-kit for synthetic behavior,
compatibility shims, unproven non-live server assumptions, and strict-live
output shaping. Annotate every confirmed issue and return findings first.
```

Expected checks:

- The response stays anchored to `agents-kit`.
- The response loads the matching crate contract and research skill only when
  server facts matter.

## storage-kit Audit

```text
Use $audit-workspace-for-shims to audit storage-kit for hacks, synthetic
responses, silent success fallbacks, and non-live backend expectations that
lack strict-live or backend-doc proof. Annotate every confirmed issue and
return findings first.
```

Expected checks:

- The response stays anchored to `storage-kit`.
- The response does not widen into provider or agent work.

## tools-kit Audit

```text
Use $audit-workspace-for-shims to audit tools-kit for hacks, synthetic MCP
fallbacks, provider-hosted execution shims, and non-live tool expectations that
lack strict-live, MCP-spec, or provider-doc proof. Annotate every confirmed
issue and return findings first.
```

Expected checks:

- The response stays anchored to `tools-kit`.
- The response loads `tools-kit-change-contract` and adds `tools-kit-research`
  only when MCP, provider-hosted, local runtime, or strict-live facts matter.

## Missing-Target Boundary

```text
Use $audit-workspace-for-shims to audit the workspace for shims and annotate
anything suspicious.
```

Expected checks:

- The response asks for the missing target crate instead of inferring one.
- The response names `agents-kit`, `providers-kit`, `storage-kit`, and
  `tools-kit` as the supported targets.

## Read-Only Or Plan-Mode Honesty Check

```bash
mkdir -p /tmp/codex-skill-forwardtest
CODEX_HOME=/tmp/codex-skill-forwardtest codex exec --ephemeral --sandbox read-only \
  -C /Users/connorsanders/scratch/ai-kit \
  -o /tmp/audit-workspace-for-shims-readonly.txt \
  $'Use $audit-workspace-for-shims to audit providers-kit for shims and annotate every confirmed issue with DELETE(providers-kit shim audit) markers.'
```

Expected checks:

- The response reports findings without pretending the markers were written.
- The response explicitly says the deletion markers were not written because
  edits were not allowed.
