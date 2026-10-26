# RESEARCH_REFINEMENT Phase

Use this phase to refine existing inline planning comments with sourced guidance
and implementation-ready detail.

## Allowed Changes

- Add, refine, split, or delete stale inline TODO comments using the configured
  marker.
- Keep edits limited to comments.

## Forbidden Changes

- Do not change production behavior.
- Do not implement code, tests, docs, scripts, or manifests.
- Do not add shims, hacks, escape hatches, synthetic behavior, deterministic
  shortcuts, or compatibility bloat to the plan.

## Research Requirements

- Use research skills only when the task depends on external provider,
  backend, protocol, MCP, runtime, or strict-live behavior.
- Use official docs, vendored sources, or strict-live facts for behavior that
  affects portable APIs or validation.
- Distinguish sourced facts from implementation inference.
- Include source labels or URLs in comments when they materially constrain the
  implementation, and include full citations in the response.

## Comment Refinement Contract

Each refined TODO should make implementation decisions clear enough that a
later implementer does not need to rediscover:

- target structs, enums, traits, methods, modules, docs, and tests
- deletion and reduction opportunities
- public API impact
- validation requirements
- strict-live or external behavior assumptions
- out-of-scope dependent issues

## Completion Gate

Before final response:

- Inspect `git diff`.
- Verify the diff contains only TODO comment additions, removals, or edits.
- Verify comments are concise enough to stay maintainable in source.
- Report research findings, citations, changed comments, and remaining risks.
