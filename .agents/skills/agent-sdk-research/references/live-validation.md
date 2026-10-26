# agent-sdk Live Validation

Use this reference when planning or reviewing `agent-sdk` strict-live work.
Explicit user instructions and the active workflow select whether
specifications or source lead before HEAD or current implementation status is
inspected. When explicitly selected, the checked-in development workflow
selects `agent-sdk/specs/**`; its presence alone does not activate it.
In that workflow, current source, tests, rustdoc, and implementation-status
documentation are drift evidence rather than competing authority.

## Planning Sources

- [`agent-sdk/specs/SPECIFICATIONS.md`](../../../../agent-sdk/specs/SPECIFICATIONS.md)
- [`agent-sdk/specs/API.md`](../../../../agent-sdk/specs/API.md)
- [`agent-sdk/AGENTS.md`](../../../../agent-sdk/AGENTS.md)
- [`agent-sdk/docs/STRICT_TESTING.md`](../../../../agent-sdk/docs/STRICT_TESTING.md)
- <https://developers.openai.com/codex/app-server>
- <https://opencode.ai/docs/server/>

`SPECIFICATIONS.md` selects exact releases and `vendor/vendor-lock.toml`
mirrors them. Vendored declarations establish route, method, DTO, and event
inventory. Vendored handlers establish envelopes, routing, ordering, and side
effects. Official product documentation provides general behavior; strict-live
proves facts source cannot guarantee.

## Validation Boundary

Non-live source and compile tests prove static shape, visibility,
documentation, downstream contracts, and sourced vendor facts. Deterministic
public-contract targets may execute direct external Agent hierarchies or
direct static Provider/Storage implementations to prove agent-sdk-owned
validation, direct Agent/Provider/Storage composition, lifecycle,
cancellation, and redaction without proving built-ins.
`agent-sdk/tests/strict_live.rs`
is the sole strict-live built-in/service runtime-behavior Cargo target and
stays a thin module entry;
`agent-sdk/tests/strict_live_suite/` owns behavior modules by functional
vertical. Fixtures, mocks, doubles, fakes, stubs, recordings, snapshots,
scripted services, synthetic output, and local substitutes are forbidden
throughout that tree. Missing real dependencies fail rather than skip.

The canonical live commands are:

```bash
bash agent-sdk/scripts/precheck_strict_live.sh
bash agent-sdk/scripts/verify_strict_live.sh
```

Documentation-only work runs the non-live gates specified in
`agent-sdk/AGENTS.md` and does not claim runtime proof.

## Protocol Proof Rules

- Codex and OpenCode behavior must be backed by official docs, selected
  vendored sources, or strict-live proof.
- Codex `thread/delete` is the sourced native cleanup route. Its handler
  deletes a thread subtree and broadcasts `thread/deleted`; global events are
  never assigned an invented conversation.
- OpenCode uses only its fixed selected foreground route matrix.
- SDK native-session creation requires effective sharing to be disabled;
  spawned mode also disables environment auto-sharing. These attachment
  requirements and rejection of native share bindings remain mandatory after
  native sharing discontinuation. The selected native create delegates to
  Session, share fails, and unshare only clears the local link.
- OpenCode live delivery uses the selected global SSE envelope. Connected and
  heartbeat frames are transport control; normal and sync copies of one event
  are deduplicated by event identity. Complete events reset stream silence;
  silence enters recovery without cancelling prompts.
- OpenCode `POST /sync/history` accepts known sequence state per aggregate and
  returns aggregate-tagged event rows. Recovery advances a high-water value
  only after the corresponding event is durably applied.
- Readiness and sourced idempotent history reads may be retried within a bound.
  Prompt, interaction response, delete, shell action, and accepted continuation
  operations are never retried blindly.
- Behavior impossible for one backend maps to `Unsupported`; portable behavior
  awaiting an adapter maps to `NotYetImplemented`. Public diagnostics do not
  expose backend labels or protocol payloads.

## Runtime Scenarios

Strict-live owns proof for cascade shutdown, prompt-future drop, abort,
workspace revisions, stale handles, notification replay and backpressure,
interaction acceptance races, durable reopen, native cleanup, Codex global
routing, OpenCode recovery/reconnect, persistent managed knowledge, and the
selected Custom Agent classifications.

Codex release request coverage uses the selected executable and configured model
with existing isolated authentication. Require actual `writeStdin` approval and
process readback, and a real `isBlocking=false` user-input request with one
correlated unsupported response and no actionable notification. The owning
strict guide specifies process-local launch flags, passive observation, and
unchanged deadlines. Missing native requests are failures, never skips. Both
cases verify empty personality inventory and explicit personality rejection.
Sourced deterministic cases own rare policy/malformed/error-bearing interruption
and asynchronous-item variants; they are not claims of live model behavior.

Only these two named OpenCode Question cases separate blocking SDK/scenario
correctness from nonblocking model answer and option-description exact-copy grades:

- `strict_live_suite::delivery_and_events::opencode_question_delivery_replays_resolves_once_and_drains`
- `strict_live_suite::prompt_and_io::opencode_native_idle_terminal_waiter_reopens_in_one_build`

Require real interactions, exact typed call/result correlation and ordered
answers, existing replay/resolution/reopen/streaming behavior, untouched
native-to-portable terminal-text fidelity, and settled cleanup. Report the
whole-string answer and each description exact-copy result visibly, including
false grades. Preserve the actual descriptions across native pending/running/
completed inputs; other typed input requirements remain blocking. Never trim,
unescape, repair or retry the answer. Missing, malformed, duplicate, SDK-failed
or wrong-provenance receipts fail qualification. The owning strict guide defines
the exact receipt contract. These observations establish neither model-quality
acceptance nor statistical reliability. Knowledge, pending-interaction recovery
and every other Agent exact-output assertion remain blocking.

The selected specs require Custom strict-live scenarios across the complete
`Agent -> Workspace -> Conversation` hierarchy. Agent-sdk owns the multi-turn
prompt/tool loop, durable ids, provider-neutral committed turns, recovery,
portable notifications and interactions, and cleanup. Provider-sdk owns the
`Provider -> Model -> Session` hierarchy, selected-model calls, normalized
function calls/results, and the Model-associated continuation contract.
Agent-sdk retains that exact continuation only inside one active prompt.
Tool-sdk owns
Toolset/ToolHandler execution. Reopen builds fresh private runtime from a
caller-resupplied direct `P: ProviderBackend` and `T: ToolsetBackend` values;
handlers stay `T::Handler` and neither tool type requires Clone. Credentials
and live continuations are never durable. Unary and streaming output share the
accepted prompt owner; observer loss never cancels accepted work.

Live coverage may prove
`Unsupported` for an individual unrepresentable provider, model, or operation
and `NotYetImplemented` for an incomplete portable adapter, but it must not
encode Custom workspace creation as universally unsupported. These
requirements govern the current suite and source contract.

## Selected Codex Setup And Release Proof

Follow `agent-sdk/strict-tests.env.example` for exact isolated startup. Derive
untrusted policy from a dynamic inline TOML `projects={"<repo-root>"={trust_level="untrusted"}}`
map plus `approvals_reviewer="user"`; do not pass explicit TOML
`approval_policy="untrusted"` or quoted dotted path keys. A nullable config/read
policy is not the effective derived thread policy. Reuse existing owned services.

Require exact model selection, empty personality inventory and preacceptance
personality rejection. Real terminal-input proof must retain an approved elevated
process, send nonempty stdin, observe the actual approval and preserve exact
output, durable delivery and cleanup. Existing fresh-thread completion, abort and
reopen scenarios exercise full-read recovery with target history defaults.
Deterministic protocol fixtures cannot substitute for these live obligations.
