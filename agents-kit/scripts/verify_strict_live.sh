#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_DIR="$(cd -- "${CRATE_DIR}/.." && pwd)"
source "${WORKSPACE_DIR}/scripts/release_stage.sh"

skip_strict_live=0
if (( $# == 1 )) && [[ "$1" == --skip-strict-live ]]; then
    skip_strict_live=1
elif (( $# != 0 )); then
    printf 'usage: bash agents-kit/scripts/verify_strict_live.sh [--skip-strict-live]\n' >&2
    exit 2
fi

printf 'Using agents-kit workspace root: %s\n' "${WORKSPACE_DIR}"

cd "${WORKSPACE_DIR}"

printf '\n==> strict precheck process contract\n'
release_stage agent.contract.precheck python3 agents-kit/tests/precheck_contract.py

printf '\n==> Question grade reporting process contract\n'
release_stage agent.contract.question_grades python3 agents-kit/tests/question_grade_contract.py

if (( ! skip_strict_live )); then
    printf '\n==> strict live precheck\n'
    release_stage_fresh agent.precheck bash agents-kit/scripts/precheck_strict_live.sh
fi

printf '\n==> agents-kit lib tests\n'
release_stage agent.test.lib cargo test --locked -p agents-kit --lib --all-features -- --nocapture

printf '\n==> selected protocol archive authority\n'
release_stage agent.contract.vendor_authority python3 -m unittest -v scripts.tests.test_vendor_update.AgentKitProtocolAuthorityTests

printf '\n==> source contract tests\n'
release_stage agent.test.source_contract cargo test --locked -p agents-kit --test source_contract --all-features -- --nocapture

printf '\n==> final static audit\n'
release_stage agent.test.final_audit cargo test --locked -p agents-kit --test final_audit --all-features -- --nocapture

printf '\n==> public constructibility and lifecycle contracts\n'
release_stage agent.test.public_api cargo test --locked -p agents-kit --test public_api --all-features -- --nocapture

printf '\n==> downstream compile contracts\n'
release_stage agent.test.compile_fail cargo test --locked -p agents-kit --test compile_fail --all-features -- --nocapture

printf '\n==> direct associated-child hierarchy contract\n'
release_stage agent.test.agent_backend cargo test --locked -p agents-kit --test agent_backend --all-features -- --nocapture

release_stage agent.test.mock_contracts cargo test --locked -p agents-kit --test mock_contracts --all-features -- --nocapture
release_stage agent.test.custom_mock_composition cargo test --locked -p agents-kit --test custom_mock_composition --all-features -- --nocapture

printf '\n==> notification stream contract\n'
release_stage agent.test.notification_stream cargo test --locked -p agents-kit --test notification_stream --all-features -- --nocapture

printf '\n==> prompt stream public contract\n'
release_stage agent.test.error_contract cargo test --locked -p agents-kit --test error_contract --all-features -- --nocapture

release_stage agent.test.prompt_stream cargo test --locked -p agents-kit --test prompt_stream --all-features -- --nocapture

printf '\n==> builder failure cleanup contract\n'
release_stage agent.test.builder_failure_cleanup cargo test --locked -p agents-kit --test builder_failure_cleanup --all-features -- --nocapture

printf '\n==> native strict-live cleanup owner contract\n'
release_stage agent.test.native_cleanup_contract cargo test --locked -p agents-kit --test native_cleanup_contract --all-features -- --nocapture

printf '\n==> native Question Kit and model-copy grader contract\n'
release_stage agent.test.strict_live_assertions cargo test --locked -p agents-kit --test strict_live_assertions --all-features -- --nocapture

printf '\n==> Custom prompt runtime contract\n'
release_stage agent.test.custom_prompt_runtime cargo test --locked -p agents-kit --test custom_prompt_runtime --all-features -- --nocapture

printf '\n==> Codex incremental prompt protocol contract\n'
release_stage agent.test.codex_prompt_stream cargo test --locked -p agents-kit --test codex_prompt_stream --all-features -- --nocapture

printf '\n==> OpenCode event and prompt ownership contract\n'
release_stage agent.test.opencode_event_ownership cargo test --locked -p agents-kit --test opencode_event_ownership --all-features -- --nocapture --test-threads=1

if (( ! skip_strict_live )); then
    printf '\n==> strict live integration suite\n'
    # Retain complete native test output privately. Only the answer/description
    # copy grades in the two approved cases are nonblocking; receipt completeness
    # and every Kit failure still fail.
    mkdir -p "${WORKSPACE_DIR}/target/agents-kit-strict-live"
    STRICT_LOG_DIR="$(umask 077; mktemp -d "${WORKSPACE_DIR}/target/agents-kit-strict-live/question-grades-XXXXXXXX")"
    STRICT_LOG="${STRICT_LOG_DIR}/strict-live.log"
    (umask 077; : > "${STRICT_LOG}")
    printf 'Private strict-live log: %s\n' "${STRICT_LOG}"
    set +e
    release_stage agent.live.strict_live cargo test --locked -p agents-kit --test strict_live --all-features -- --nocapture --test-threads=1 2>&1 | tee "${STRICT_LOG}"
    STRICT_PIPE_STATUS=("${PIPESTATUS[@]}")
    python3 agents-kit/scripts/report_question_grades.py "${STRICT_LOG}"
    STRICT_REPORT_STATUS=$?
    set -e
    if (( STRICT_PIPE_STATUS[1] != 0 )); then
        printf 'strict-live log capture failed (exit %s)\n' "${STRICT_PIPE_STATUS[1]}" >&2
    fi
    if (( STRICT_PIPE_STATUS[0] != 0 )); then
        exit "${STRICT_PIPE_STATUS[0]}"
    fi
    if (( STRICT_PIPE_STATUS[1] != 0 )); then
        exit "${STRICT_PIPE_STATUS[1]}"
    fi
    if (( STRICT_REPORT_STATUS != 0 )); then
        exit "${STRICT_REPORT_STATUS}"
    fi
fi

printf '\n==> agents-kit rustdoc\n'
release_stage agent.rustdoc cargo doc --locked -p agents-kit --all-features --no-deps

printf '\n==> agents-kit doctests\n'
release_stage agent.doctest cargo test --locked -p agents-kit --doc --all-features -- --nocapture

printf '\n==> clippy -D warnings\n'
release_stage agent.clippy cargo clippy --locked -p agents-kit --all-targets --all-features --no-deps -- -D warnings

printf '\n==> cargo fmt --check\n'
release_stage agent.format cargo fmt --package agents-kit --check

if (( skip_strict_live )); then
    printf '\nAgent qualified non-live checks passed; native/provider strict-live suite and live precheck excluded.\n'
fi
