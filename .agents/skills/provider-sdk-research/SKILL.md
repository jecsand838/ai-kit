---
name: provider-sdk-research
description: "Research provider-sdk Provider, Model, Session, generation, function-continuation, hosted-tool, and strict-live behavior against authoritative OpenAI, Anthropic, Gemini, and Ollama sources."
---

# provider-sdk Research

## Purpose

Ground portable Provider, Model, and Session behavior in current provider
documentation and real-service evidence before changing API or runtime claims.

## Prerequisites

- Read [`provider-sdk/AGENTS.md`](../../../provider-sdk/AGENTS.md).
- Read [`provider-sdk/docs/PORTABLE_API.md`](../../../provider-sdk/docs/PORTABLE_API.md)
  for public-surface work.
- Pair with `provider-sdk-change-contract` for code, test, docs, or validation
  changes.
- Pair with `ai-kit-public-surface-contract` for imports, visibility, or
  module changes.

## Workflow

1. Identify whether the fact belongs to Provider-wide resources, selected
   Model behavior, interactive Session behavior, or a concrete provider-private
   adapter.
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
6. Verify streaming termination, native frame/body limits, Session audio and
   transcription, cancellation, and consuming cleanup where those protocols
   apply.
7. Identify deterministic and strict-live fallout, including exact model ids,
   credentials, endpoints, and unsupported cases.
8. Feed sourced facts into the owning provider note, portable contract, tests,
   and implementation. Keep concrete behavior in the owning provider/model
   subtree.

## Guardrails

- Preserve `Provider -> Model -> Session`, associated child types, and closed
  private dispatch for supplied built-ins. Caller implementations remain
  concrete and use the public traits directly.
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
