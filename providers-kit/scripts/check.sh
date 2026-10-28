#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_DIR="$(cd -- "${CRATE_DIR}/.." && pwd)"
source "${WORKSPACE_DIR}/scripts/release_stage.sh"

check_workspace_root_artifacts() {
    local leaked
    leaked="$(find "${WORKSPACE_DIR}" -maxdepth 1 \( -name '*.rlib' -o -name '*.rmeta' -o \( -type f -perm -111 \) \) -print)"
    if [[ -n "${leaked}" ]]; then
        printf 'providers-kit checks leaked workspace-root build artifacts:\n%s\n' "${leaked}" >&2
        exit 1
    fi
}

cd "${WORKSPACE_DIR}"
printf 'Using workspace: %s\n' "${WORKSPACE_DIR}"

printf '\n==> cargo fmt --all --check\n'
release_stage provider.format cargo fmt --all --check

printf '\n==> cargo clippy --locked -p providers-kit --all-targets --all-features -- -D warnings\n'
release_stage provider.clippy cargo clippy --locked -p providers-kit --all-targets --all-features -- -D warnings

printf '\n==> cargo test --locked -p providers-kit --lib --all-features -- --nocapture\n'
release_stage provider.test.lib cargo test --locked -p providers-kit --lib --all-features -- --nocapture

for target in \
    compile_fail \
    public_api \
    provider_backend \
    mock_contracts \
    shutdown_lifecycle \
    session_lifecycle \
    provider_behavior \
    wire_behavior \
    error_contract \
    transport_diagnostics \
    strict_live_assertions \
    strict_env_contract \
    source_contract; do
    printf '\n==> cargo test --locked -p providers-kit --test %s --all-features -- --nocapture\n' "${target}"
    release_stage "provider.test.${target}" cargo test --locked -p providers-kit --test "${target}" --all-features -- --nocapture
done

printf '\n==> cargo test --locked -p providers-kit --doc --all-features -- --nocapture\n'
release_stage provider.doctest cargo test --locked -p providers-kit --doc --all-features -- --nocapture

printf '\n==> workspace-root artifact hygiene\n'
check_workspace_root_artifacts

if [[ "${AI_KIT_WORKSPACE_OWNS_DOCS_SITE:-}" != "${WORKSPACE_DIR}" ]]; then
    printf '\n==> workspace docs site gate\n'
    bash scripts/check_docs_site.sh
fi
