# Ollama Provider Notes

Local Ollama construction uses typed unauthenticated config rather than an
empty API key. Model selection uses the exact installed tag through
`Provider::model(ModelId)`; no `:latest` suffix or alias is added.

## Model Operations

The selected Ollama Model owns generation, streaming, supported embeddings,
and function calling. Unsupported model operations return
`KitError` classified as `error::ErrorKind::Unsupported`.

Ollama chat function calling is client-executed. Calls normalize to
`FunctionCall`; the caller executes them and returns `FunctionResult` values.
The Kit does not claim forced tool choice when the native endpoint cannot
enforce it.
The native [chat endpoint](https://docs.ollama.com/api/chat) accepts function
declarations without a forced-choice field. Strict-live coverage requires an
actual automatic function call and result continuation, and separately verifies
that forced selection returns `Unsupported`, following the documented
[tool-calling flow](https://docs.ollama.com/capabilities/tool-calling).
The strict continuation scenario supplies its literal-result system instruction
before both requests; its unchanged sampling and exact-output requirements
belong to the [strict-testing guide](../STRICT_TESTING.md).

When Ollama supplies a call `id`, generation and streaming retain it in
`FunctionCall`; assistant replay returns that `id`, and the corresponding
`FunctionResult::call_id` becomes the tool message's `tool_call_id`. These
optional correlation fields are defined by the
[Ollama 0.40.6 protocol](https://github.com/ollama/ollama/blob/v0.40.6/api/types.go#L180-L205).
Calls without an id remain supported, and their replay omits both id fields.

## Hosted Tools

Ollama's separate web-search APIs are not model-managed hosted declarations.
Unsupported `HostedTool` values fail honestly rather than being mapped to
ordinary chat functions or external REST calls.

## Strict-Live Selection

The independent live cases use these exact configured installed models for
generation, reasoning, streaming, embeddings, and function continuation. They
also preserve the forced-choice and unavailable file/batch negative checks.
The target is not ignored and does not fall back to another model. Missing
Ollama reachability or an exact installed model fails the verifier prerequisite
check rather than substituting deterministic output or skipping this provider.

### Model artifacts after an Ollama upgrade

An unchanged tag does not prove unchanged model bytes or correct tokenizer
initialization. Preserve the selected manifest, artifact hashes, server version,
and failed evidence before repairing a local installation.

For `ministral-3:8b-instruct-2512-q4_K_M` on Ollama 0.40.12, a converted local
artifact was observed with `tokenizer.ggml.pre=default` after its legacy
compatibility markers had been removed. The runtime therefore did not apply
the model's Tekken correction. The pinned
[conversion owner](https://github.com/ollama/ollama/blob/v0.40.12/compatmigrate/mistral_pixtral.go)
copies tokenizer metadata while converting those markers; the
[loader correction](https://github.com/ollama/ollama/blob/v0.40.12/llama/compat/llama-ollama-compat.cpp)
depends on detecting them.

After preserving the old artifacts, the supported repair was:

```bash
ollama pull ministral-3:8b-instruct-2512-q4_K_M
```

Verify that the next load selects the refreshed artifact and that the native
loader reports the Mistral compatibility correction and
`tokenizer.ggml.pre = tekken`. `/api/show` can still report the original disk
metadata `default`; that field alone does not establish the effective tokenizer.
Do not edit model files or normalize model output to make a test pass. The
[repair record](../../../docs/strict-live-ollama-repair-2026-10-29.md) identifies
the observed artifacts, bounded confirmation results, and remaining evidence
limits. Future registry or runtime versions need their own verification.

### Terminal reason and function calls

Unary and streamed chat completion share one terminal-reason conversion.
Native `stop` becomes `ToolCall` only when validated calls are present; otherwise
it remains `Stop`. Native `length` remains `Length` and recognized `error`
remains `Error`, including when calls were emitted. Partial content, thinking,
usage, and calls remain available for caller policy. Missing or unknown terminal
reasons fail normalization; an unknown discriminator is available only through
explicit response-detail inspection. Native NDJSON `error` envelopes remain
outer failures and never become a successful `Completed` event.

Ollama's [v0.40.6 generation runner](https://github.com/ollama/ollama/blob/v0.40.6/llm/llama_server.go)
can report a length limit alongside tool calls; a call does not establish a
successful terminal reason. The accepted `done_reason: "error"` branch does not
replace the documented [native error envelope](https://docs.ollama.com/api/errors).

## Current settings and origin boundaries

The selected Model forwards explicit presence/frequency penalties unchanged.
The `/api/embed` request forwards an explicit dimensions override and validates
all returned vector sizes, count, and order. Unary and streaming usage preserve
absent versus reported zero `prompt_eval_cached_count`; cache-only updates are
not discarded. These behaviors follow the [embedding contract](https://docs.ollama.com/api/embed),
[usage contract](https://docs.ollama.com/api/usage), and
[pinned options schema](https://github.com/ollama/ollama/blob/v0.40.6/api/types.go).

Exact thinking profiles remain private and model-specific. `gpt-oss:20b` uses
native low/medium/high values; `qwen3.5:9b-q4_K_M` and `gemma4:e4b-it-qat`
use boolean thinking. Summary visibility does not disable thinking. Unknown
models do not acquire a profile from their name or from another model's result.
See the [thinking contract](https://docs.ollama.com/capabilities/thinking).

Direct `https://ollama.com/api` JSON/schema generation is explicitly unsupported
before transport, following the [cloud format limitation](https://docs.ollama.com/capabilities/structured-outputs).
This depends on the effective origin, including a local configuration explicitly
pointed at that origin. Credentials alone do not determine capability, and
arbitrary relays retain native error behavior. Local schema output remains
supported. Cloud generation/streaming requires its own exact model and credential;
local success does not qualify cloud behavior.

The current live additions use one reduced-dimension embedding operation and
six generation calls across five cases: penalties/actual cache observations
(unary and stream), two exact boolean-thinking models, local JSON schema
(unary and stream). Ollama Cloud execution is outside this campaign by explicit
user scope; deterministic origin rejection is not cloud execution evidence. Every generation retains the existing 180-second bound and owned
Provider shutdown. Cache absence is recorded, not retried into a hit.
