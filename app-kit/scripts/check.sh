#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
source "${WORKSPACE_DIR}/scripts/release_stage.sh"
cd "${WORKSPACE_DIR}"

release_stage app.format cargo fmt --all --check
release_stage app.check cargo check --locked -p app-kit --all-targets
for target in \
    public_surface \
    mock_contracts \
    workflow_mock_composition \
    core_contract \
    action_bindings \
    action_contract \
    projection_contract \
    callable_definition_contract \
    condition_contract \
    condition_display_contract \
    lifecycle_contract \
    hook_execution_contract \
    observability_contract \
    tracking_contract \
    tracking_checkpoint_contract \
    checkpoint_history \
    deletion_contract \
    revision_contract \
    child_executor_contract \
    child_composition \
    lance_conflict_control \
    execution_controls \
    bsp_execution_contract \
    native_execution_contract \
    outcome_recovery_contract \
    native_streaming_contract \
    typing_integration \
    expression_parity \
    graph_contract \
    schema_contract \
    docs_examples \
    state_contract \
    typed_state_contract \
    state_serialization_contract \
    workspace_contract; do
    release_stage "app.test.${target}" cargo test --locked -p app-kit --test "${target}" -- --nocapture --test-threads=1
done
# Run process recovery once. The strict verifier inherits this same-invocation
# result; no prior receipt can stand in for the exact inventory and execution.
PREPARE_EVIDENCE="$(mktemp -d)"
trap 'rm -rf -- "${PREPARE_EVIDENCE}"' EXIT
release_stage_fresh app.prepare.inventory cargo test --locked -p app-kit --test prepare_contract \
    -- --list --format terse | tee "${PREPARE_EVIDENCE}/inventory"
python3 - "${PREPARE_EVIDENCE}/inventory" <<'PYTHON'
from pathlib import Path
import sys

expected = {
    "prepare_keeps_checkpoint_zero_parked_through_observers_and_cancellation",
    "prepare_rejects_a_stale_workspace_before_allocating_a_visible_run",
    "prepared_checkpoint_zero_survives_process_loss",
    "staged_action_and_atomic_barrier_survive_forced_process_loss",
}
rows = [line.removesuffix(": test") for line in Path(sys.argv[1]).read_text().splitlines()
        if line.endswith(": test")]
if len(rows) != len(expected) or set(rows) != expected:
    raise SystemExit("prepare_contract inventory differs from its four required cases")
PYTHON
release_stage_fresh app.prepare.execution cargo test --locked -p app-kit --test prepare_contract \
    -- --nocapture --test-threads=1 | tee "${PREPARE_EVIDENCE}/execution"
python3 - "${PREPARE_EVIDENCE}/execution" <<'PYTHON'
from pathlib import Path
import re
import sys

text = Path(sys.argv[1]).read_text()
rows = re.findall(r"^test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out", text, re.M)
# Supervised children may emit earlier summaries; the last belongs to the parent.
if not rows or tuple(map(int, rows[-1])) != (4, 0, 0, 0, 0):
    raise SystemExit("prepare_contract did not pass all four required cases")
if not re.search(r"^test staged_action_and_atomic_barrier_survive_forced_process_loss \.\.\.", text, re.M):
    raise SystemExit("canonical four-boundary crash execution is missing")
PYTHON
release_stage app.workloads.routine python3 app-kit/scripts/check_app_workloads.py routine
release_stage app.doctest cargo test --locked -p app-kit --doc -- --nocapture
for example in state graph pure_state actions_and_inputs controls_and_streaming history_and_recovery child_composition; do
    release_stage "app.example.${example}" cargo run --locked -p app-kit --example "${example}"
done
# The executable discovery output belongs to this invocation, so neither its
# build nor the process-level persistence proof may reuse an earlier receipt.
release_stage_fresh app.example.history_and_recovery.build cargo build --locked -p app-kit --example history_and_recovery \
    --message-format=json > "${PREPARE_EVIDENCE}/history-example.jsonl"
release_stage_fresh app.example.history_and_recovery.persistence \
    python3 app-kit/tests/history_and_recovery_contract.py "${PREPARE_EVIDENCE}/history-example.jsonl"
release_stage app.clippy cargo clippy --locked -p app-kit --all-targets -- -D warnings
release_stage app.rustdoc env RUSTDOCFLAGS="-D warnings -D missing-docs" cargo doc --locked -p app-kit --no-deps
release_stage app.no_aliases bash scripts/check_no_type_aliases.sh
release_stage app.no_path_wiring bash scripts/check_no_path_wiring.sh
