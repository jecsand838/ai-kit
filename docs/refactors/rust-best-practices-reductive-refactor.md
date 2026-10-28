# Rust Best-Practices Reductive Refactor

## Status

Archived on 2026-04-15.

This document is a historical execution record for an earlier workspace-wide refactor campaign. It is not the current operating contract for `ai-kit`.

## Use This Instead

For current guidance, use the live workspace docs:

- [`README.md`](../../README.md) for workspace orientation
- [`ARCHITECTURE.md`](../../ARCHITECTURE.md) for the current workspace model
- [`AGENTS.md`](../../AGENTS.md) for repo rules and validation
- [`SKILLS.md`](../../SKILLS.md) for the current skill catalog

For crate-specific current-state guidance, use each crate’s `README.md`, `ARCHITECTURE.md`, `AGENTS.md`, and focused `docs/` guides.

## Historical Scope

The archived campaign covered reductive cleanup across:

- `agents-kit`
- `providers-kit`
- `storage-kit`

The original plan emphasized:

- preserving crate-root-owned enum-dispatch plus builder APIs
- keeping `mod.rs` for directory-backed modules
- reducing duplication and thin forwarding layers
- improving docs and source-contract coverage alongside code changes

## Archive Note

Retain this file only as a dated reference for that campaign’s goals and inventory assumptions. When the historical record conflicts with current workspace docs, the current workspace docs win.
