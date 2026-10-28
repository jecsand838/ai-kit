#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_DIR="$(cd -- "${CRATE_DIR}/.." && pwd)"
source "${WORKSPACE_DIR}/scripts/release_stage.sh"

require_env() {
    local name="$1"
    if [[ -z "${!name:-}" ]]; then
        printf 'missing required strict-live variable: %s\n' "${name}" >&2
        exit 1
    fi
}

printf 'Using workspace: %s\n' "${WORKSPACE_DIR}"

cd "${WORKSPACE_DIR}"

printf '\n==> strict-live environment\n'
for name in \
    TEST_TOOLS_KIT_MCP_PROGRAM \
    TEST_TOOLS_KIT_MCP_ARGS_JSON \
    TEST_TOOLS_KIT_MCP_ENV_JSON \
    TEST_TOOLS_KIT_MCP_TIMEOUT_SECS \
    TEST_TOOLS_KIT_MCP_TOOL_NAME \
    TEST_TOOLS_KIT_MCP_TOOL_ARGUMENTS_JSON \
    TEST_TOOLS_KIT_MCP_EXPECTED_RESULT_JSON; do
    require_env "${name}"
done

printf '\n==> non-live crate gate\n'
bash tools-kit/scripts/check.sh

printf '\n==> independently configured MCP strict-live target\n'
release_stage tool.live.strict_live cargo test --locked -p tools-kit --test strict_live --all-features -- --nocapture --test-threads=1
