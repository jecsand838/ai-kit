# Current Graph And Workflow Research

## External-Source Boundary

Use official external sources to compare semantics and expose design questions.
Never let an external framework, standard, or crate override
[`app-kit` specifications](../../../../app-kit/specs/SPECIFICATIONS.md), add an
app capability, or define app-kit vocabulary by itself.

## Verification Protocol

For every research run:

1. Confirm the official entrypoint remains accessible and authoritative.
2. Record the exact URL, access date, and displayed version or document
   channel; verify unversioned documentation against the latest official tag.
3. Record preview, experimental, deprecated, archived, or incubation status.
4. Cite the narrow official page that supports each claim.
5. Exclude inaccessible or unverifiable pages from behavioral claims and
   record the evidence gap.

Do not rely on memory, search summaries, third-party comparisons, or an
unqualified `latest` URL.

## Official Entrypoints

| Source | Official documentation | Release check |
|---|---|---|
| Apache Burr | [Documentation](https://burr.apache.org/docs/), [application concepts](https://burr.apache.org/docs/concepts/state-machine/) | [Repository and tags](https://github.com/apache/burr) |
| LangGraph | [Overview](https://docs.langchain.com/oss/python/langgraph/overview), [Graph API](https://docs.langchain.com/oss/python/langgraph/graph-api) | [Releases](https://github.com/langchain-ai/langgraph/releases) |
| Microsoft Agent Framework | [Workflows](https://learn.microsoft.com/en-us/agent-framework/workflows/workflows) | [Repository](https://github.com/microsoft/agent-framework) |
| Temporal | [Documentation](https://docs.temporal.io/) | [Server releases](https://github.com/temporalio/temporal/releases) |
| Open Workflow Specification | [Specification](https://open-workflow-specification.org/) | [Releases](https://github.com/open-workflow-specification/specification/releases) |
| petgraph | [Crate docs](https://docs.rs/petgraph/latest/petgraph/), [algorithms](https://docs.rs/petgraph/latest/petgraph/algo/) | [Repository](https://github.com/petgraph/petgraph) |

Treat these as entrypoints, not frozen versions. Record the inspected release
and access date in the dossier.

## Comparison Dimensions

Research only dimensions relevant to the owning app question:

- graph definition, identity, entry/terminal points, edges, and routing;
- validation, reachability, cycles, topology changes, and deterministic order;
- serial, parallel, message-driven, or replay-driven scheduling;
- state, reducers, checkpoint boundaries, resume, and migration;
- side-effect determinism, idempotency, retry, timeout, cancellation, and
  compensation;
- interaction, child workflows, fan-out, joins, and concurrency limits;
- observability and run inspection;
- scheduler, node executor, agent, persistence, and caller ownership; and
- Rust representation or algorithms where petgraph is relevant.

Separate framework vocabulary from runtime guarantees, and orchestration
durability from database or agent-conversation durability. A useful external
capability absent from the app specs is an `Unresolved gap`, not an
implementation requirement.

## Evidence Record

For each finding record the source and exact official URL, version or channel,
access date, narrow paraphrased fact, app concern, relationship to the target,
classification, and owner. Use comparisons to sharpen requirements, never to
copy an API.
