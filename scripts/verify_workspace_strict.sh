#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
source "${WORKSPACE_DIR}/scripts/release_stage.sh"

if [[ "${1-}" == --resume ]]; then
    if (( $# < 2 || $# > 3 )) || [[ -z "$2" || "$2" == --* ]] \
        || { (( $# == 3 )) && [[ "$3" != --skip-strict-live ]]; }; then
        printf 'usage: bash scripts/verify_workspace_strict.sh --resume BUNDLE [--skip-strict-live]\n' >&2
        exit 2
    fi
    exec python3 "${WORKSPACE_DIR}/scripts/release_qualification.py" "$@"
fi

provider_long_override=""
skip_strict_live=0
case "$#" in
    0) ;;
    1)
        case "$1" in
            --skip-long-provider-batches) provider_long_override=0 ;;
            --skip-strict-live) skip_strict_live=1 ;;
            *)
                printf 'usage: bash scripts/verify_workspace_strict.sh [--skip-long-provider-batches | --skip-strict-live] | --resume BUNDLE [--skip-strict-live]\n' >&2
                exit 2
                ;;
        esac
        ;;
    *)
        printf 'usage: bash scripts/verify_workspace_strict.sh [--skip-long-provider-batches | --skip-strict-live] | --resume BUNDLE [--skip-strict-live]\n' >&2
        exit 2
        ;;
esac

check_workspace_root_artifacts() {
    local leaked
    leaked="$(find "${WORKSPACE_DIR}" -maxdepth 1 \( -name '*.rlib' -o -name '*.rmeta' -o \( -type f -perm -111 \) \) -print)"
    if [[ -n "${leaked}" ]]; then
        printf 'workspace strict checks leaked workspace-root build artifacts:\n%s\n' "${leaked}" >&2
        return 1
    fi
}

run_crate_gate() {
    local label="$1"
    local env_file="$2"
    local verify_script="$3"
    local provider_long_mode="${4-}"
    local script_argument="${5-}"

    printf '\n==> %s\n' "${label}"
    (
        cd "${WORKSPACE_DIR}" || exit $?
        if [[ -n "${env_file}" ]]; then source "${env_file}" || exit $?; fi
        if [[ -n "${provider_long_mode}" ]]; then
            export PROVIDERS_KIT_RUN_LONG_BATCHES="${provider_long_mode}"
        fi
        if [[ -n "${script_argument}" ]]; then
            bash "${verify_script}" "${script_argument}"
        else
            bash "${verify_script}"
        fi
    )
}

workspace_gate_status=0
run_required_gate() {
    local label="$1" status
    shift
    if [[ "${AI_KIT_RELEASE_RECEIPT_RUN+x}" == x ]]; then
        # Continue independent owners in an explicit resumed run. Their own
        # scripts still fail closed and retain their existing cleanup rules.
        if "$@"; then
            return 0
        else
            status=$?
        fi
        printf 'required workspace gate failed: %s (exit %s)\n' "${label}" "${status}" >&2
        if (( workspace_gate_status == 0 )); then workspace_gate_status="${status}"; fi
        return 0
    fi
    "$@"
}

cd "${WORKSPACE_DIR}"

# Full runs validate the effective Provider selection before any gate starts.
# Qualified non-live runs never load strict-live environment files.
if (( ! skip_strict_live )); then
    (
        source "providers-kit/strict-tests.env"
        case "${PROVIDERS_KIT_RUN_LONG_BATCHES-1}" in
            0|1) ;;
            *)
                printf 'PROVIDERS_KIT_RUN_LONG_BATCHES must be 0 or 1\n' >&2
                exit 1
                ;;
        esac
        if [[ -z "${provider_long_override}" && "${PROVIDERS_KIT_RUN_LONG_BATCHES-1}" != 1 ]]; then
            printf 'workspace completion requires PROVIDERS_KIT_RUN_LONG_BATCHES=1 unless --skip-long-provider-batches explicitly qualifies coverage\n' >&2
            exit 1
        fi
    )
    printf 'Using strict workspace: %s\n' "${WORKSPACE_DIR}"
else
    printf 'Using workspace for QUALIFIED non-live checks: %s\n' "${WORKSPACE_DIR}"
fi

# A direct wrapper invocation verifies its runner contracts before Cargo.
# The continuation driver already runs this command before the Storage matrix;
# its verified result can be reused here within that same run.
printf '\n==> release runner contracts\n'
release_stage workspace.contract.release_runner python3 -m unittest \
    scripts.tests.test_release_stage_hooks scripts.tests.test_release_evidence \
    scripts.tests.test_release_qualification scripts.tests.test_import_release_evidence

printf '\n==> workspace-root artifact hygiene\n'
run_required_gate "workspace-root artifact hygiene" check_workspace_root_artifacts

printf '\n==> workspace path wiring guard\n'
run_required_gate "workspace path wiring" release_stage workspace.no_path_wiring bash scripts/check_no_path_wiring.sh

printf '\n==> workspace type alias guard\n'
run_required_gate "workspace type aliases" release_stage workspace.no_aliases bash scripts/check_no_type_aliases.sh

printf '\n==> cargo fmt --all --check\n'
run_required_gate "workspace formatting" release_stage workspace.format cargo fmt --all --check

printf '\n==> cargo clippy --locked --workspace --all-targets --all-features --exclude storage-kit --exclude app-kit -- -D warnings\n'
run_required_gate "workspace Clippy" release_stage workspace.clippy cargo clippy --locked --workspace --all-targets --all-features --exclude storage-kit --exclude app-kit -- -D warnings

storage_lanes="$(python3 storage-kit/scripts/feature_policy.py --all-backends --format lanes)"
while IFS= read -r features; do
    storage_args=(--no-default-features)
    if [[ "${features}" != - ]]; then storage_args+=(--features "${features}"); fi
    printf '\n==> Storage Clippy features: %s\n' "${features}"
    lane="${features//,/-}"
    if [[ "${lane}" == - ]]; then lane=lance; fi
    run_required_gate "Storage Clippy ${lane}" release_stage "workspace.storage-clippy.${lane}" cargo clippy --locked -p storage-kit --all-targets "${storage_args[@]}" -- -D warnings
done <<< "${storage_lanes}"

printf '\n==> workspace docs site gate\n'
export AI_KIT_WORKSPACE_OWNS_DOCS_SITE="${WORKSPACE_DIR}"
run_required_gate "workspace docs site" bash scripts/check_docs_site.sh

crate_order=(agents-kit providers-kit storage-kit tools-kit app-kit)
if [[ "${AI_KIT_RELEASE_RECEIPT_RUN+x}" == x ]]; then
    # Provider batches have authored multi-day budgets. Finish independent
    # owners first when collecting an explicit resumed qualification.
    crate_order=(agents-kit storage-kit tools-kit app-kit providers-kit)
fi
for crate in "${crate_order[@]}"; do
    env_file="${crate}/strict-tests.env"
    provider_mode=""
    script_argument=""
    gate_script="${crate}/scripts/verify_strict_live.sh"
    gate_label="${crate} strict completion"
    if [[ "${crate}" == storage-kit ]]; then env_file=""; fi
    if [[ "${crate}" == providers-kit ]]; then provider_mode="${provider_long_override}"; fi
    if (( skip_strict_live )); then
        env_file=""
        provider_mode=""
        gate_script="${crate}/scripts/check.sh"
        gate_label="${crate} qualified non-live checks"
        if [[ "${crate}" == agents-kit ]]; then
            gate_script="agents-kit/scripts/verify_strict_live.sh"
            script_argument="--skip-strict-live"
        fi
    fi
    run_required_gate "${gate_label}" run_crate_gate \
        "${gate_label} gate" "${env_file}" "${gate_script}" \
        "${provider_mode}" "${script_argument}"
done

printf '\n==> workspace-root artifact hygiene\n'
run_required_gate "workspace-root artifact hygiene" check_workspace_root_artifacts

if (( workspace_gate_status != 0 )); then
    printf '\nWorkspace strict completion failed; all independent resumed owners were attempted.\n' >&2
    exit "${workspace_gate_status}"
fi

if (( skip_strict_live )); then
    printf '\nWorkspace non-live qualification passed: QUALIFIED coverage; all strict-live suites and their prerequisite/service checks were excluded.\n'
    printf 'Excluded strict-live suites:\n'
    printf '  %s\n' agents-kit/strict_live providers-kit/strict_live storage-kit/strict_live \
        tools-kit/strict_live app-kit/workflow_live
    printf 'Full workspace release qualification remains unproven.\n'
elif [[ "${provider_long_override}" == 0 ]]; then
    printf '\nWorkspace strict completion passed: QUALIFIED coverage; only these seven long Provider cases were excluded:\n'
    printf '  %s\n' \
        live::batches::openai_inline_batch_completes \
        live::batches::openai_provider_file_batch_completes \
        live::batches::openai_batch_preserves_mixed_row_outcomes \
        live::batches::openai_batch_result_limit_is_enforced \
        live::batches::anthropic_batch_preserves_mixed_row_outcomes \
        live::batches::anthropic_batch_result_limit_is_enforced \
        live::batches::anthropic_inline_batch_completes
else
    printf '\nWorkspace strict completion passed: FULL crate coverage; Storage default Lance scope.\n'
fi
