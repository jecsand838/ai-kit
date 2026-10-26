# provider-sdk Strict Live

Use this reference for real-provider validation planning.

## Sources

- <https://developers.openai.com/api/docs>
- <https://platform.claude.com/docs/>
- <https://ai.google.dev/gemini-api/docs>
- <https://docs.ollama.com/api>
- [`provider-sdk/AGENTS.md`](../../../../provider-sdk/AGENTS.md)

## Commands

```bash
bash provider-sdk/scripts/check.sh
source provider-sdk/strict-tests.env && bash provider-sdk/scripts/verify_strict_live.sh
```

## Contract

- All providers compile together.
- Model ids are exact native ids/tags. Tests and helpers do not add prefixes,
  aliases, or tags.
- Strict-live enters generation and model-bound operations through
  `Provider::model(ModelId)`.
- OpenAI, Anthropic, Gemini, and Ollama concrete Models own generation,
  demand-driven streaming, supported embeddings, and batch submission. Only
  OpenAI owns supplied interactive Sessions. Gemini session opening and
  stateless OpenAI audio are temporarily unsupported; retain their historical
  evidence without presenting it as current supported coverage. Stateless
  Gemini audio and OpenAI Realtime remain supported.
- Provider owns discovery, files, batch lifecycle, health, and shared shutdown.
- Function-calling proof preserves exact call correlation and replays the full
  model-authored assistant Message. OpenAI, Anthropic, Gemini, and Ollama proof
  retains each provider's opaque continuation requirements.
- HostedTool proof validates and renders through the selected Model. It never
  routes through a hosted dispatcher or tool-sdk.
- Strict-live uses real configured services and fails fast on missing
  credentials or prerequisites. Mocks, recordings, scripted providers,
  rewritten outputs, and synthetic support never qualify.
