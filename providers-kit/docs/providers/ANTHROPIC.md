# Anthropic Provider Notes

Anthropic construction uses provider-owned config and credential types. Model
selection uses the exact Anthropic model id through
`Provider::model(ModelId)`.

## Model And Continuation

The concrete Anthropic Model owns Messages API generation, streaming, and
model-bound batch submission. The current adapter returns `Unsupported` for
embeddings; Anthropic's [embedding guide](https://platform.claude.com/docs/en/build-with-claude/embeddings)
describes a separate provider rather than an Anthropic embedding model.
Anthropic has no supplied interactive Session; its associated Session is
`Infallible`. Anthropic role conversion belongs to a private typed role owner;
do not route it through
stringly standalone conversion helpers.

The current exact generation and hosted-tool profiles include
`claude-fable-5-1`, `claude-opus-5-5`, `claude-sonnet-5-5`, and
`claude-haiku-5-5`. These models use adaptive thinking. Portable Minimal and
Low map to `low`, Medium to `medium`, High to `high`, and Maximum to `max`;
the intermediate native `xhigh` setting is not expressible through the
portable effort enum. Omitted effort and Auto summary retain native defaults.
Hidden requests `display: omitted`; Summary and Verbose request `summarized`.
These are presentation controls, not an off switch for thinking. See the
[effort contract](https://platform.claude.com/docs/en/build-with-claude/effort)
and [thinking modes](https://platform.claude.com/docs/en/build-with-claude/thinking-troubleshooting).

Adaptive-only profiles reject manual budgets and explicit sampling overrides
before I/O. Retained manual-budget profiles require at least 1,024 reasoning
tokens and fewer reasoning tokens than the configured output limit.
Forced tool selection is rejected for Fable 5.1, Opus 5.5, and Sonnet 5.5
even when no reasoning override is supplied. Adaptive thinking by itself
does not prohibit forcing: Haiku 5.5 and supported earlier adaptive profiles
accept it. Manual extended thinking still prohibits forcing. The native
[tool-choice rules](https://platform.claude.com/docs/en/agents-and-tools/tool-use/define-tools)
govern these exact distinctions; model names are never interpreted by prefix.
No restricted-access Mythos profile receives a new qualification claim.

Tool-use blocks normalize to `FunctionCall`. Tool-result blocks are rendered
from validated `FunctionResult` values while preserving extended-thinking
blocks in the opaque continuation attached to the assistant Message.
Streaming usage starts with `message_start` and applies the cumulative counts
reported by each `message_delta`. Supplied input, cache, output, and nested
thinking counts replace their previous values, including explicit zero;
omitted counts preserve the latest reported value. Both portable usage events
and the completed response reflect these updates. No aggregate token total is
synthesized. Anthropic's [delta usage schema](https://raw.githubusercontent.com/anthropics/anthropic-sdk-python/main/src/anthropic/types/message_delta_usage.py)
and [stream accumulator](https://raw.githubusercontent.com/anthropics/anthropic-sdk-python/main/src/anthropic/lib/streaming/_messages.py)
define the cumulative replacement and optional-field behavior.

Streaming [citation deltas](https://platform.claude.com/docs/en/build-with-claude/citations#streaming-support)
append to their indexed text block. Portable citations and the original native
citation objects are retained together so replay preserves opaque fields such
as `encrypted_index`. The native citations field may be absent, null, or an
array; citation accumulation uses the existing stream byte and item limits.
Citation metadata does not become a text delta or a separate tool execution.

Citation normalization follows the native `type` discriminator. The
[native citation union](https://raw.githubusercontent.com/anthropics/anthropic-sdk-python/main/src/anthropic/types/text_citation.py)
contains five forms:

| Native form | Portable attribution |
| --- | --- |
| `web_search_result_location` | URL citation with the native URL, optional title, and cited excerpt |
| `char_location`, `page_location`, `content_block_location` with `file_id` | File citation with the exact native file ID, optional document title, and cited excerpt |
| Document location without `file_id` | `Unsupported` during normalization; a document index does not supply a portable file identity |
| `search_result_location` | `Unsupported` during normalization; the Kit does not resolve request-indexed search-result sources |

The [document citation schema](https://raw.githubusercontent.com/anthropics/anthropic-sdk-python/main/src/anthropic/types/citation_char_location.py)
allows an absent or null file ID. Valid index-only citations are unsupported,
not malformed. An extra `url` cannot manufacture document attribution, and an
extra `file_id` cannot change a web-search citation into a file citation.
Citation file references have no inferred MIME type.

Native character, page, and block bounds locate passages in the **source**,
not spans of generated output. Portable citation output offsets therefore
remain absent. Source indices are validated using their documented numbering
and range semantics, while the exact native coordinates remain in opaque
continuation. See [citation indices](https://platform.claude.com/docs/en/build-with-claude/citations#citation-indices).

Each citation requires its discriminator and the fields belonging to that
variant. Missing required fields, present nulls or wrong types, and unknown
discriminators retain distinct structured Kit errors. Optional titles and
document file IDs accept omission or null. Delta schema errors occur during
streaming; range and attribution checks occur during normalization. Unknown
extension fields remain unchanged in replay, including fields unused by the
portable projection.

These rules describe response normalization, not a new request capability.
The Kit does not enable document citations or add the optional web-fetch
citations configuration. Request rendering remains unchanged; see Anthropic's
[web-fetch citation configuration](https://platform.claude.com/docs/en/agents-and-tools/tool-use/web-fetch-tool#citations).

## Current-Model Live Qualification Scope

The current-model slice adds seven cases and ten generations: one Minimal
unary generation for each of the four current IDs; default unary and Maximum
streaming on Haiku 5.5; unary and streaming schema output on Haiku 5.5; and a
two-generation High-effort streaming function continuation on Haiku 5.5.
The latter submits the exact returned call identity and complete assistant
message before requiring the result-only nonce in the final answer.
These are representative protocol profiles, not every feature on every model.

Existing thinking qualification now uses adaptive High effort on its exact
configured current model, rather than an unsupported manual budget. Existing
function and hosted-tool scenarios use Auto on the three current models that
prohibit forcing; their actual-call, execution, exact-output and cleanup
requirements remain mandatory. Current-model cases retain the existing
4,096-token output bound, 180-second per-generation observation deadline,
transport bounds and provider shutdown owner. No model or prompt retry is
introduced. Passing evidence must name the actual selected model and case;
these declarations alone are not service qualification.

## Files And Batches

Anthropic's GA Files API uses `page`/`next_page` pagination. Files, Messages,
streams, and Message Batches send no legacy Files beta header. A reusable file
reference retains its MIME type: JPEG, PNG, GIF, and WebP render as image
blocks, while PDF and plain text render as document blocks. Missing or
unsupported MIME information is rejected before request submission. Ordinary
caller-uploaded input files can report `downloadable: false`; the portable
download path remains available only for files the native lifecycle marks
downloadable.

Strict-live separates successful two-row batches, mixed successful/failed row
proof, and actual cancellation effect. The inline, mixed-row and bounded-result
completion cases each have a 24-hour deadline captured before fixture setup.
The mixed-row case shares its deadline across readability-prerequisite and
mixed-outcome jobs. It checks status immediately
then every five seconds. Only read-only completion polling uses that deadline;
setup, submission, result reads and cleanup retain their separate bounded
ownership, so total case wall time may exceed 24 hours. The native
[24-hour processing window](https://platform.claude.com/docs/en/build-with-claude/batch-processing#how-long-does-it-take-for-a-batch-to-process)
informs this test budget but does not guarantee successful row results. Cancellation, missing-resource and unsupported-input cases keep their existing
bounds; cancellation cleanup retains its separate 3-minute bound. Only the three
named Anthropic completion cases join the four OpenAI exclusions in the explicit
partial verifier mode; full coverage is the default. Native
`processing_status: ended` maps to portable Completed when no job-level error
exists, including after cancellation; this status means rows are terminal,
not that they all succeeded. The suite retrieves Kit results and checks their
exact ids and dispositions. Cancelled rows provide cancellation evidence;
ordinary completion before cancellation takes effect leaves that proof
unproven. Ordinary fixture uploads and deletions use the Kit.

The mixed-row fixture reads its owned content-only secret through the Kit in
unary and one-row batch modes using the same immutable request before deletion,
then proves absence by complete Kit listing. It accepts an identified
`invalid_request_error` as well as `not_found_error` only with the full fixture
history and successful text control. [Batch validation](https://platform.claude.com/docs/en/build-with-claude/batch-processing)
is asynchronous; file-lookup error categories are not an exhaustive row-error
contract. An outer `error` wrapper or unrelated resource diagnostic cannot
satisfy this test.

## Hosted Tools

Anthropic supports selected closed `HostedTool` families such as web search,
URL fetching, and code execution. The selected Model owns option validation,
version selection, combination rules, rendering, and output normalization.
The standalone caller-executed Bash tool and client functions must not be
relabeled as provider-hosted execution.

The hosted [code execution tool](https://platform.claude.com/docs/en/agents-and-tools/tool-use/code-execution-tool)
version selected by the Kit has a Bash subtool; this is distinct from
caller-executed Bash. Strict-live exception setup requests the exact native
`python3 -c` command and requires its associated failed result, nonzero native
return code, and matching exception sentinel in stderr. Bare Python source
equality is not required for a native shell command. Source/result association
must be unambiguous through public DTOs; assistant prose cannot fill missing
execution evidence.

Current-response server-tool invocation IDs and result links are joined
privately. Known duplicate, contradictory, or wrong-family links fail
normalization. Pending work remains pending; a result referring to a previous
turn remains honest result-only output without invented action attribution.
The original assistant continuation is replayed unchanged.

Web fetch uses `web_fetch_tool_result_error`; native requested and retrieved
URLs remain distinct. Retrieval timestamps are not citation excerpts. Only an
exactly attributed `url_not_accessible` witness satisfies the retrieval-failure
case, not quota, invalid/disallowed input, or service errors. Native nonzero Bash
return codes remain failed execution, and success requires the native zero code.

Failed batch rows decode the inner error envelope and retain its subtype and
request ID in `RemoteFailure`. The outer `error` discriminator is not the service
category. A valid batch containing a failed row remains a successful results
operation; invalid native structure remains an outer `KitError`.
