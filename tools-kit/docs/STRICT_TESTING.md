# Strict Testing

Tool validation separates portable contract proof, deterministic protocol
proof, and real MCP service proof.

## Primary Direct-Trait Lane

From the workspace root, run:

```bash
bash scripts/check_mock_contracts.sh --crate tools-kit
bash scripts/check_ci.sh
```

The scoped command runs `mock_contracts` with the crate's default features;
`check_ci.sh` owns the complete workspace primary lane. Test-local structs and
exhaustive enums implement `ToolsetBackend` and `ToolHandlerBackend` directly,
retaining unique associated handlers without `Toolset::from_backend`.
The target constructs no supplied backend, opens no sockets or backend processes,
and requires no credentials. Dependencies still compile. Scoped results are not
full-suite evidence. Follow the [shared mock contract](../../docs/mock-testing.md)
for inventory and runtime qualification. Existing extension-normalization,
Local/MCP protocol and server tests retain their separate targets, and the
broader all-feature deterministic and strict-live gates below remain required.

## Deterministic Gate

Run:

```bash
bash tools-kit/scripts/check.sh
```

The wrapper owns formatting, clippy with warnings denied, all targets and
features, compile contracts, Local/MCP behavior, source contracts,
rustdoc/doctests, and the docs-site gate.

Deterministic coverage must prove:

- external `ToolsetBackend` with an associated custom Handler;
- Toolset/ToolHandler trait implementations and ownership bounds;
- `Toolset::from_backend` and privacy of child ingress;
- direct Local/MCP construction and duplicate-name rejection;
- complete ToolInfo inventory and schema validation;
- handler completion, interaction, model-visible error, invalid transition,
  cancellation, and bounded cleanup;
- shared Toolset shutdown and retained terminal results;
- MCP native-name mapping, empty inventory, transports, form interaction, and
  remote cancellation;
- independent MCP server eager startup, pagination, call mapping, consuming
  joined shutdown, drop/cancellation ownership, and Toolset independence;
- direct non-Clone caller toolsets and associated handlers served over real
  loopback MCP transport, including form settlement, model-visible failure,
  failed bind, and separate source shutdown after server termination;
- absence of product-level test frameworks, construction layering, serving
  state on Toolset, and compatibility APIs.

Repository-managed MCP services are deterministic transport fixtures. They do
not satisfy strict-live.

## Strict-Live MCP Gate

Run:

```bash
source tools-kit/strict-tests.env && bash tools-kit/scripts/verify_strict_live.sh
```

The gate fails fast unless the caller supplies a real independently configured
MCP service, expected tool and arguments, a complete independent expected
`ToolResult` in `TEST_TOOLS_KIT_MCP_EXPECTED_RESULT_JSON`, and a bounded timeout.
The expected result is parsed before connecting. The live proof compares the
complete result after shutdown, including exact ordered content, structured
content, metadata and failure status; success status alone is insufficient.
Choose a stable operation such as reading an independently prepared marker file,
as shown in `strict-tests.env.example`. Do not derive expectations from observed
output or normalize volatile fields away.

It proves live inventory, one real call through ToolHandler, exact returned
contents, and settled Toolset shutdown.

Mocks, recordings, repository-scripted services, and synthetic MCP responses
never satisfy strict-live.

## Production regressions

Output-schema and active-server lifecycle regressions remain enabled in the
existing deterministic targets. Record behavioral failures before repairing
production code, then run each regression and its passing controls. Compile
errors, timeouts before handler entry, and fixture setup failures are not
evidence of a production defect.

Schema regressions cover Local execution, the supplied server, and MCP client
normalization against an independent native protocol fixture. This keeps a
future server-side rejection from masking a client-side validation defect.

The MCP ownership cases use the shipped server and real MCP traffic over
loopback. Handler-entry and settlement signals separate cancellation evidence
from watchdog timeouts. Failed observations are recorded before fixture gates
are released and clients are closed. Server shutdown must not shut down the
caller-owned Toolset. These protocol fixtures do not count as strict-live proof.

Cancellation cases include entered execution, resource-acquiring handler
construction, synchronous work before the handler returns its future, and
nested client elicitation. They require the handler's `Cancel` operation and
protocol cancellation of outstanding elicitation, then verify another invocation
and the caller-owned Toolset remain usable. Panic coverage requires the server
to retain the worker failure through settled shutdown.

Accepted-form cases exercise valid, schema-invalid, absent, and null payloads.
Schema validation can be disabled for object content; it cannot authorize a
non-object form. Invalid responses must not advance the pending handler, and a
gated Cancel must settle before the protocol error returns. A cleanup-failure
case requires both the original form error and cleanup error in the call and
server outcomes. Fixture Drop does not count as cancellation settlement.

The idle-session case initializes two distinct sessions and observes their SSE
bodies directly, so client reconnection or local cancellation cannot stand in
for server closure. Cleanup-failure and deadline cases require the original
handler error to remain visible and a pending `Cancel` future to remain owned
after the shutdown observer expires. Gates are released and all accessible
owners cleaned up before assertions. Socket closure complements the native
worker ownership proof; it does not establish a worker join by itself.


Lookup cases gate both direct inventory and call declaration before handler
construction. They require retained completion after shutdown timeout and reject
post-seal handler construction. An already-entered cancellation case drops its
shutdown observer only after Cancel begins, and uses a post-gate completion
counter to distinguish future settlement from Handler Drop. Capacity cases hold
64 actual requests, reject the next before construction, then prove reuse and
72 successive completed calls without accumulated capacity.

Startup controls use an HTTP 100 Continue body-demand barrier to exercise
initialize during sealing. Malformed JSON and a parsed wrong first method must
produce a client error and leave a fresh native session usable. These tests do
not freeze rmcp's particular 4xx status or infer native worker joins from socket
closure. The protocol's recommended 400 missing-session response is distinct
from these lifecycle assertions.

Native-session controls use raw HTTP with fully consumed connection-close
responses and an isolated current-thread Tokio runtime. They cover rejected
first requests, negotiated and notification-ready sessions, lost initialization
observation, independent sessions, and initialization during shutdown. The
shutdown observation samples live tasks immediately after success; cleanup and
runtime destruction precede assertions. These relative baselines show remaining
runtime work, not native task identity or consumption of inaccessible handles.

The separate session-capacity case initializes 64 actual sessions, rejects
excess admission, deletes one selected session, then proves slot reuse and peer
usability. Every observed session identity is retained for independent cleanup.
These protocol fixtures remain deterministic evidence, not strict-live results.
