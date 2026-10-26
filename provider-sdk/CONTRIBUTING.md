# Contributing

Read [`AGENTS.md`](AGENTS.md), the applicable provider note, and the owning
portable API section before changing behavior.

## Rules

1. Keep `Provider -> Model -> Session` as the public ownership hierarchy.
2. Keep public RPITIT traits over private exhaustive enum dispatch.
3. Use caller implementations directly with their exact associated Model,
   Continuation, and Session types. Keep supplied handle dispatch closed over
   built-ins.
4. Put provider-wide resources on Provider, selected-model behavior on Model,
   and provider-native interactive behavior on Session.
5. Keep model ids exact. Do not add aliases, fallbacks, default slots, or
   capability matrices.
6. Preserve the Model's exact continuation on `Message<C>` and replay
   model-authored assistant messages unchanged; supplied native contents stay
   private inside `GenerationContinuation`.
7. Keep native wire DTOs, clients, renderers, and transport details private.
8. Return honest `Unsupported` errors instead of synthetic portability.
9. Use explicit structs, data-carrying enums, and receiver methods. Do not add
   macros, aliases, compatibility paths, or loose helpers.
10. Do not add implementation identity, erased adapters, Extension variants,
    normalization constructors, or provider provenance replacements.
11. Bound transport bodies, stream frames, diagnostics, downloads, and
    WebSocket payloads without buffering peer-controlled input without limit.
12. Update docs and source contracts with public or module-boundary changes.

During implementation, add or strengthen meaningful tests and record red
evidence before production edits, then require passing controls and independent
review. Run the exact completion validation contract in [`AGENTS.md`](AGENTS.md)
on the assembled change before closing the work.
