# app-kit

This crate is in early development. Backwards compatibility is not guaranteed
until `1.0.0`; pin exact versions and review the
[release notes](https://github.com/jecsand838/ai-kit/releases) before upgrading.

`app-kit` opens one application, materializes configured Agents into
application workspaces, and runs compact versioned workflow definitions
durably through `App` and the functional `app_kit::workflow` module.

Start with the [runnable quickstart](docs/QUICKSTART.md) or the
[guide index](docs/README.md).

The [history and recovery example](examples/history_and_recovery.rs) builds its
own embedded Lance store, restores a workflow checkpoint, and verifies the
retained result and source lineage without an LLM, service, or credentials.
Install the native [build prerequisites](../CONTRIBUTING.md#get-started) first,
then run from the repository root with its pinned Rust toolchain:

```bash
cargo run --locked -p app-kit --example history_and_recovery
```

The result is `41`: the source finishes at `2`, a restored fork starts at `40`,
and its remaining step adds `1`. The default mode reopens the result within one
process. The quickstart also shows `create DIRECTORY` and `reopen DIRECTORY`
invocations that retain the completed result across process exit. Both paths
await App cleanup. For the smallest state-only workflow, see
[pure_state](examples/pure_state.rs).

`WorkflowRun::resolve` is the single fenced control path for caller input,
exact Agent notifications, accepting/failing an unknown effect, and a retry
whose safety has been proven. Dropping ordinary run handles or wait observers
does not cancel accepted work. An owning Execution revokes uncommitted advancement
under the [execution ownership rules](docs/STREAMING.md).

Internally it remains a strict graph engine with deterministic BSP, reducers,
joins, child/map parallelism, interactions, retries, checkpoints, and recovery.

`AgentSpec<A>` consumes a fully built `A: AgentBackend` under a caller revision.
Callers choose A explicitly and provide an app-dedicated
`CS: StateStoreBackend` directly to `AppConfig::new`, including for process-local
operation. Supplied handles and caller implementations use the same traits. App preserves `<CS, A>`, uses `A::Workspace` and its
associated Conversation directly, and never erases or inspects the hierarchy.
It has no direct providers-kit or tools-kit dependency. One inventory is
homogeneous in `A`; callers needing runtime heterogeneity supply matching
Agent, Workspace, and Conversation enums.

The exact public contract is
[`specs/SPECIFICATIONS.md`](specs/SPECIFICATIONS.md).

App's packaged dependency requires exactly `agents-kit =0.0.2`, inherited from
the workspace dependency declaration. This prevents Cargo from selecting a
different Agent patch release when verifying the package. Advancing that
requirement requires explicit compatibility review and package verification;
downstream consumers cannot select a newer Agent version through this dependency.

```bash
bash app-kit/scripts/check.sh
bash app-kit/scripts/verify_strict_live.sh
```

Strict-live uses real OpenCode, Ollama and embedded LanceDB, and fails on
missing prerequisites. State and knowledge fixtures own isolated directories;
no external database service is required. See [the strict-testing guide](docs/STRICT_TESTING.md)
for setup and [`specs/workflows/TESTING.md`](specs/workflows/TESTING.md) for the
proof requirements.

## Direct public-trait tests

Run `bash scripts/check_mock_contracts.sh --crate app-kit` from the workspace
root for the credential-free mock lane. The workspace primary CI entrypoint is
`bash scripts/check_ci.sh`. See [mock testing](../docs/mock-testing.md) for
targets, evidence, runtime qualification, and the separate release/live gates.
