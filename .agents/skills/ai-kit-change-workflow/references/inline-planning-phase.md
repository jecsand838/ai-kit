# INLINE_PLANNING Phase

Use this phase to identify code changes required to address the selected issue
or task and encode them as inline source comments.

## Allowed Changes

- Add or refine inline comments using the configured TODO marker.
- Edit comments only, using the native comment syntax for the target artifact
  such as Rust comments, Markdown HTML comments, TOML comments, or shell
  comments.

## Forbidden Changes

- Do not change production behavior.
- Do not change tests, docs, scripts, manifests, generated files, or examples
  unless the only change is an inline planning comment in the assigned target
  artifact.
- Do not format unrelated code.
- Do not start implementation.

## Comment Contract

Each required future task must have an inline planning comment. Include:

- what must change
- what should be deleted or reduced
- affected public API, docs, tests, or validation
- relevant crate boundary or architecture rule
- why the work stays inside the assigned scope

Use the configured marker. Default:

```rust
// AI-KIT-CHANGE-TODO(<scope>): <technical planning detail>
```

For crate-decoupling work, the user may choose a narrower marker such as:

```rust
// AI-KIT-CRATE-DECOUPLING-REFACTOR-TODO: <technical planning detail>
```

## Completion Gate

Before final response:

- Inspect `git diff`.
- Verify the diff contains only comments for this phase.
- Verify each identified future task has an inline TODO comment.
- Report changed files, TODO markers added, and dependent out-of-scope issues.
