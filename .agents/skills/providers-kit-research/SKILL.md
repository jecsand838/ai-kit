---
name: providers-kit-research
description: "Research providers-kit Provider, Model, Session, generation, function-continuation, hosted-tool, and strict-live behavior against authoritative OpenAI, Anthropic, Gemini, and Ollama sources."
---

# providers-kit Research

## Purpose

Ground portable Provider, Model, and Session behavior in current provider
documentation and real-service evidence before changing API or runtime claims.

## Prerequisites

- Read [`providers-kit/AGENTS.md`](../../../providers-kit/AGENTS.md).
- Read [`providers-kit/docs/PORTABLE_API.md`](../../../providers-kit/docs/PORTABLE_API.md)
  for public-surface work.
- Pair with `providers-kit-change-contract` for code, test, docs, or validation
  changes.
- Pair with `ai-kit-public-surface-contract` for imports, visibility, or
  module changes.

## Workflow

1. Identify whether the fact belongs to Provider-wide resources, selected
   Model behavior, managed Conversation continuity, native Realtime Session
   behavior, or a concrete provider-private adapter. Conversation sessions reuse
   existing generation/continuation paths for all four supplied providers;
   native OpenAI Realtime and Gemini Live require independent mappings.
2. Check current OpenAI, Anthropic, Gemini, and Ollama official sources relevant
   to the operation, including whether formerly beta resource headers or wire
   shapes have since become GA. A portable operation may return honest
   Unsupported for providers that do not implement it.
3. Verify exact model-id behavior. Reject aliases, normalization, fallback,
   capability slot sets, and discovery-driven routing.
4. For function calling, verify call correlation, result placement, and all
   provider continuation metadata. Preserve model-authored assistant messages
   exactly in opaque typed continuation; never design a native-envelope escape
   hatch or assume continuation is serializable.
5. For HostedTool, verify provider/model support, combinations, rendering, and
   normalized output. Distinguish provider-hosted execution from
   client-executed functions.
6. Verify streaming termination, aggregate retention and native frame/body
   limits, Session audio/transcription, input capture, playback reconciliation,
   targeted cancellation and consuming cleanup. Distinguish response outcomes
   from continuity and remote-stop acknowledgement; do not assign late native
   observations a correlation that their protocol cannot establish.
7. Identify deterministic and strict-live fallout, including exact model ids,
   credentials, endpoints, and unsupported cases.
8. Feed sourced facts into the owning provider note and portable contract before
   implementation. Prove public construction, borrowed associated command/event
   halves, same-poll acceptance, duplex progress, dropped-future ownership,
   bounded queues and post-seal cleanup through tests-first work. Keep concrete
   behavior in the owning provider/model subtree. Target guidance is not a
   passing implementation or live-service receipt.

## Guardrails

- Preserve `Provider -> Model -> Session`, associated child types, and closed
  private dispatch for supplied built-ins. Caller implementations remain
  concrete and use the public traits directly.
- The only additional Provider behavioral seams are `session::SessionCommands`
  and `session::SessionEvents`, with concrete borrowed halves and no independent
  close authority. Their supplied handles retain opaque closed dispatch.
- Do not add implementation identity, erased backend adapters, Extension
  variants, normalization constructors, provider provenance replacements, or
  backend trait objects. Optional runtime composition enums are caller-owned.
- Do not restore LoopDriver, provider tool execution, factories, production
  conformance, capability matrices, model slot sets, opaque hosted payloads,
  or child ingress.
- Distinguish sourced facts from implementation inference.
- Return honest Unsupported instead of synthetic portability.

## References

- [references/strict-live.md](references/strict-live.md)
- [OpenAI API docs](https://developers.openai.com/api/docs)
- [Anthropic docs](https://platform.claude.com/docs/)
- [Gemini API docs](https://ai.google.dev/gemini-api/docs)
- [Ollama API docs](https://docs.ollama.com/api)
