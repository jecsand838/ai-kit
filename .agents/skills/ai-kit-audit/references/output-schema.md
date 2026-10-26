# Output Schema

Return findings first. If no actionable findings exist, write exactly:

```text
No findings identified.
```

## Finding Fields

Each finding must include:

- `Title`: concise issue title.
- `Severity`: `P0`, `P1`, `P2`, or `P3`.
- `Affected crates`: crate names or `workspace`.
- `Affected files/modules`: concrete paths, modules, tests, docs, or scripts.
- `Why it matters`: the boundary, behavior, validation, maintainability, or
  portability risk.
- `Expected impact`: code, docs, tests, validation, public API, or no public API.
- `Overlap risk`: files or concepts likely to conflict with other findings.
- `Parallelization`: `parallelizable`, `serial`, or `needs grouping`, with a
  short reason.
- `Recommended validation`: exact command blocks from the relevant
  `AGENTS.md` files when available.

## Quality Bar

- Prefer fewer, stronger findings over speculative noise.
- Mark weak or unproven observations as `Open question` instead of findings.
- Cite source evidence with file paths, docs, tests, scripts, or authoritative
  URLs when external behavior matters.
- Keep findings scoped so `orchestrate-workflow` can split independent work.
