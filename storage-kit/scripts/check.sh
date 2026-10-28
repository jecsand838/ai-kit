#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
source "${WORKSPACE_DIR}/scripts/release_stage.sh"
cd "${WORKSPACE_DIR}"

run_lane() {
    local features="$1" target lane
    lane="${features//,/-}"
    if [[ "${lane}" == - ]]; then lane=lance; fi
    local args=(--no-default-features)
    if [[ "${features}" != - ]]; then args+=(--features "${features}"); fi
    printf '\n==> Storage features: %s\n' "${features}"
    release_stage "storage.compile.${lane}" cargo test --locked -p storage-kit --all-targets --no-run "${args[@]}"
    local targets=(feature_contract public_api database_backend state_store_backend mock_contracts lancedb_native_feasibility lancedb_durability_feasibility lancedb_session_ownership lancedb_state_store strict_env_contract source_contract)
    case ",${features}," in *,redb,*|*,sqlite,*) targets+=(state_store_local);; esac
    case ",${features}," in *,surrealdb,*) targets+=(surreal_partition_deadline);; esac
    for target in "${targets[@]}"; do
        release_stage "storage.test.${lane}.${target}" cargo test --locked -p storage-kit --test "${target}" "${args[@]}" -- --nocapture --test-threads=1
    done
    release_stage "storage.doctest.${lane}" cargo test --locked -p storage-kit --doc "${args[@]}" -- --nocapture
    release_stage "storage.clippy.${lane}" cargo clippy --locked -p storage-kit --all-targets "${args[@]}" -- -D warnings
    release_stage "storage.rustdoc.${lane}" env RUSTDOCFLAGS="-D warnings" cargo doc --locked -p storage-kit --no-deps "${args[@]}"
}

selection="$(python3 "${SCRIPT_DIR}/feature_policy.py" "$@" --format lanes)"
release_stage storage.contract.feature_policy python3 -m unittest discover -s storage-kit/scripts/tests -v
release_stage storage.format cargo fmt --all --check
while IFS= read -r features; do run_lane "${features}"; done <<< "${selection}"

# A complete request additionally proves every legal union compiles, rather
# than assuming singleton/maximal configurations cover intermediate subsets.
for argument in "$@"; do
    if [[ "${argument}" == --all-backends ]]; then
        legal="$(python3 "${SCRIPT_DIR}/feature_policy.py" --format legal)"
        while IFS= read -r features; do
            args=(--no-default-features)
            if [[ "${features}" != - ]]; then args+=(--features "${features}"); fi
            lane="${features//,/-}"
            if [[ "${lane}" == - ]]; then lane=lance; fi
            release_stage "storage.check.${lane}" cargo check --locked -p storage-kit --all-targets "${args[@]}"
        done <<< "${legal}"
        break
    fi
done
