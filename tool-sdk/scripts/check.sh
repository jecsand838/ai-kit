#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_DIR="$(cd -- "${CRATE_DIR}/.." && pwd)"
source "${WORKSPACE_DIR}/scripts/release_stage.sh"

clean_crate_wip_dir() {
    local wip_dir="${CRATE_DIR}/wip"
    if [[ -d "${wip_dir}" ]]; then
        rm -rf "${wip_dir}"
    fi
}

check_workspace_root_artifacts() {
    local leaked
    leaked="$(find "${WORKSPACE_DIR}" -maxdepth 1 \( -name '*.rlib' -o -name '*.rmeta' \) -print)"
    if [[ -n "${leaked}" ]]; then
        printf 'tool-sdk compile-fail tests leaked workspace-root compiler artifacts:\n%s\n' "${leaked}" >&2
        exit 1
    fi
}

cd "${WORKSPACE_DIR}"

printf 'Using workspace: %s\n' "${WORKSPACE_DIR}"

printf '\n==> cargo fmt --all --check\n'
release_stage tool.format cargo fmt --all --check

printf '\n==> cargo clippy --locked -p tool-sdk --all-targets --all-features -- -D warnings\n'
release_stage tool.clippy cargo clippy --locked -p tool-sdk --all-targets --all-features -- -D warnings

printf '\n==> cargo test --locked -p tool-sdk --lib --all-features -- --nocapture\n'
release_stage tool.test.lib cargo test --locked -p tool-sdk --lib --all-features -- --nocapture

printf '\n==> cargo test --locked -p tool-sdk --test compile_fail --all-features -- --nocapture\n'
release_stage tool.test.compile_fail cargo test --locked -p tool-sdk --test compile_fail --all-features -- --nocapture

printf '\n==> compile-fail wip cleanup\n'
clean_crate_wip_dir

printf '\n==> workspace-root artifact hygiene\n'
check_workspace_root_artifacts

printf '\n==> cargo test --locked -p tool-sdk --test public_api --all-features -- --nocapture\n'
release_stage tool.test.public_api cargo test --locked -p tool-sdk --test public_api --all-features -- --nocapture

printf '\n==> cargo test --locked -p tool-sdk --test extension_backend --all-features -- --nocapture\n'
release_stage tool.test.extension_backend cargo test --locked -p tool-sdk --test extension_backend --all-features -- --nocapture

release_stage tool.test.mock_contracts cargo test --locked -p tool-sdk --test mock_contracts --all-features -- --nocapture

printf '\n==> cargo test --locked -p tool-sdk --test shared_shutdown --all-features -- --nocapture\n'
release_stage tool.test.shared_shutdown cargo test --locked -p tool-sdk --test shared_shutdown --all-features -- --nocapture

printf '\n==> cargo test --locked -p tool-sdk --test strict_env_contract --all-features -- --nocapture\n'
release_stage tool.test.strict_env_contract cargo test --locked -p tool-sdk --test strict_env_contract --all-features -- --nocapture

printf '\n==> cargo test --locked -p tool-sdk --test source_contract --all-features -- --nocapture\n'
release_stage tool.test.source_contract cargo test --locked -p tool-sdk --test source_contract --all-features -- --nocapture

printf '\n==> cargo test --locked -p tool-sdk --test local_execution --all-features -- --nocapture\n'
release_stage tool.test.local_execution cargo test --locked -p tool-sdk --test local_execution --all-features -- --nocapture

printf '\n==> cargo test --locked -p tool-sdk --test schema_validation --all-features -- --nocapture\n'
release_stage tool.test.schema_validation cargo test --locked -p tool-sdk --test schema_validation --all-features -- --nocapture

printf '\n==> cargo test --locked -p tool-sdk --test mcp_stdio --all-features -- --nocapture\n'
release_stage tool.test.mcp_stdio cargo test --locked -p tool-sdk --test mcp_stdio --all-features -- --nocapture

printf '\n==> cargo test --locked -p tool-sdk --test mcp_streamable_http --all-features -- --nocapture\n'
release_stage tool.test.mcp_streamable_http cargo test --locked -p tool-sdk --test mcp_streamable_http --all-features -- --nocapture

printf '\n==> cargo test --locked -p tool-sdk --test cancellation --all-features -- --nocapture\n'
release_stage tool.test.cancellation cargo test --locked -p tool-sdk --test cancellation --all-features -- --nocapture

printf '\n==> cargo test --locked -p tool-sdk --test handler_step_flow --all-features -- --nocapture\n'
release_stage tool.test.handler_step_flow cargo test --locked -p tool-sdk --test handler_step_flow --all-features -- --nocapture

printf '\n==> cargo test --locked -p tool-sdk --test mcp_form_interaction --all-features -- --nocapture\n'
release_stage tool.test.mcp_form_interaction cargo test --locked -p tool-sdk --test mcp_form_interaction --all-features -- --nocapture

printf '\n==> cargo test --locked -p tool-sdk --test mcp_server_feature --all-features -- --nocapture\n'
release_stage tool.test.mcp_server_feature cargo test --locked -p tool-sdk --test mcp_server_feature --all-features -- --nocapture

printf '\n==> cargo test --locked -p tool-sdk --doc --all-features -- --nocapture\n'
release_stage tool.doctest cargo test --locked -p tool-sdk --doc --all-features -- --nocapture

if [[ "${AI_KIT_WORKSPACE_OWNS_DOCS_SITE:-}" != "${WORKSPACE_DIR}" ]]; then
    printf '\n==> workspace docs site gate\n'
    bash scripts/check_docs_site.sh
fi
