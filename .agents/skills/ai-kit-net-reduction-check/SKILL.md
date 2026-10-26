---
name: ai-kit-net-reduction-check
description: "Measure whether a cleanup pass is actually net reductive, isolate the files still driving line growth, and distinguish production-code growth from test-harness or docs growth without designing the next refactor. Use when the user asks whether another delete pass is justified or wants diff-stat-based reduction analysis."
---

# ai-kit Net Reduction Check

## Purpose

Measure whether cleanup work is actually shrinking the branch.

## Trigger Boundary

Use this skill when the user wants diff-stat-based reduction analysis or wants
to know whether another delete-first pass is justified.

## Prerequisites

- Choose the comparison baseline.
- Capture the current diff stat.

## Workflow

1. Separate overall growth into production code, tests, docs, and generated
   metadata where possible.
2. Identify the files driving positive growth.
3. Decide whether the remaining growth is still justified.
4. Recommend either another delete-first pass, a narrower target, or stopping.
5. Report the numbers plainly.

## Output Rules

- Prefer per-file accountability over broad impressions.
- Treat production-code reduction as the primary signal; note test or docs
  growth separately.

## References

- `ai-kit-delete-first-pass`
