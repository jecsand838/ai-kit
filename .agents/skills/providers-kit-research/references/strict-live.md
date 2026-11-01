# providers-kit Strict Live

Use this reference for real-provider validation planning.

## Sources

- <https://developers.openai.com/api/docs>
- <https://platform.claude.com/docs/>
- <https://ai.google.dev/gemini-api/docs>
- <https://docs.ollama.com/api>
- [`providers-kit/AGENTS.md`](../../../../providers-kit/AGENTS.md)

## Commands

```bash
bash providers-kit/scripts/check.sh
source providers-kit/strict-tests.env && bash providers-kit/scripts/verify_strict_live.sh
```

## Contract

- All providers compile together.
- Model ids are exact native ids/tags. Tests and helpers do not add prefixes,
  aliases, or tags.
- Strict-live enters generation and model-bound operations through
  `Provider::model(ModelId)`.
- OpenAI, Anthropic, Gemini, and Ollama concrete Models own generation,
  demand-driven streaming, supported embeddings, and batch submission. The
  Session contract requires a Conversation baseline for each provider
  and independent OpenAI Realtime/Gemini Live profiles. The canonical live
  inventory includes two exchanges and a tool round trip per Conversation
  provider plus authentic Gemini Live cases; passing a prior inventory does not
  qualify added scenarios. Historical Gemini withdrawal remains historical
  evidence, not an active implementation prohibition. Stateless OpenAI audio remains
  separately unsupported; stateless Gemini audio is unaffected.
- Select profiles explicitly and preserve exact configured model ids. Gemini
  Live requires `TEST_GEMINI_MODEL_INTERACTIVE`, initially exact
  `models/gemini-3.8-live`; account/model unavailability is a blocker, not a
  reason to fall back. Keep env contracts, verifier, assertion helpers and
  evidence reporters synchronized with the canonical inventory. Native
  qualification requires recorded validation of the exact source and invocation.
- Provider owns discovery, files, batch lifecycle, health, and shared shutdown.
- Function-calling proof preserves exact call correlation and replays the full
  model-authored assistant Message. OpenAI, Anthropic, Gemini, and Ollama proof
  retains each provider's opaque continuation requirements.
- HostedTool proof validates and renders through the selected Model. It never
  routes through a hosted dispatcher or tools-kit.
- Strict-live uses real configured services and fails fast on missing
  credentials or prerequisites. Mocks, recordings, scripted providers,
  rewritten outputs, and synthetic support never qualify.
- Preserve the two named OpenAI Realtime cases' existing raw-copy grades and
  blocking behavioral requirements without extending their exceptions to new
  managed or Gemini cases. The approved refactor requires the full workspace
  wrapper, including long provider batches, and separate clean-checkout proof.
