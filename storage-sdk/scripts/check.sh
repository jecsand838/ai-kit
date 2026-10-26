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
    release_stage "storage.compile.${lane}" cargo test --locked -p storage-sdk --all-targets --no-run "${args[@]}"
    local targets=(feature_contract public_api database_backend state_store_backend mock_contracts lancedb_native_feasibility lancedb_durability_feasibility lancedb_session_ownership lancedb_state_store strict_env_contract source_contract)
    case ",${features}," in *,redb,*|*,sqlite,*) targets+=(state_store_local);; esac
    case ",${features}," in *,surrealdb,*) targets+=(surreal_partition_deadline);; esac
    case ",${features}," in *,qdrant,*) targets+=(qdrant_rest_completion);; esac
    for target in "${targets[@]}"; do
        release_stage "storage.test.${lane}.${target}" cargo test --locked -p storage-sdk --test "${target}" "${args[@]}" -- --nocapture --test-threads=1
    done
    release_stage "storage.doctest.${lane}" cargo test --locked -p storage-sdk --doc "${args[@]}" -- --nocapture
    release_stage "storage.clippy.${lane}" cargo clippy --locked -p storage-sdk --all-targets "${args[@]}" -- -D warnings
    release_stage "storage.rustdoc.${lane}" env RUSTDOCFLAGS="-D warnings" cargo doc --locked -p storage-sdk --no-deps "${args[@]}"
}

selection="$(python3 "${SCRIPT_DIR}/feature_policy.py" "$@" --format lanes)"
release_stage storage.contract.feature_policy python3 -m unittest discover -s storage-sdk/scripts/tests -v
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
            release_stage "storage.check.${lane}" cargo check --locked -p storage-sdk --all-targets "${args[@]}"
        done <<< "${legal}"
        illegal="$(python3 "${SCRIPT_DIR}/feature_policy.py" --format illegal)"
        negative_evidence=""
        if [[ -n "${AI_KIT_RELEASE_RECEIPT_RUN-}" ]]; then
            negative_evidence="$(umask 077; mktemp -d "${AI_KIT_RELEASE_RECEIPT_RUN}/storage-negative-features.XXXXXXXX")"
            printf 'Storage negative feature evidence: %s\n' "${negative_evidence}"
        fi
        while IFS= read -r features; do
            if [[ -n "${negative_evidence}" ]]; then
                log="${negative_evidence}/${features//,/-}.log"
                (umask 077; : > "${log}")
            else
                log="$(mktemp)"
            fi
            if cargo check --locked -p storage-sdk --lib --no-default-features --features "${features}" >"${log}" 2>&1; then
                negative_status=0
            else
                negative_status=$?
            fi
            diagnostic=""
            if diagnostic="$(rg -m 1 'send_guard.*deadlock_detection|deadlock_detection.*send_guard|sqlite.*qdrant.*(incompatible|cannot)' "${log}")"; then :; fi
            if [[ -n "${negative_evidence}" ]]; then
                python3 - "${log}" "${features}" "${negative_status}" "${diagnostic}" <<'PY'
import hashlib, json, pathlib, sys
log, features, code, diagnostic = sys.argv[1:]
path = pathlib.Path(log)
receipt = {
    "schema": 1,
    "command": ["cargo", "check", "--locked", "-p", "storage-sdk", "--lib", "--no-default-features", "--features", features],
    "features": features,
    "returncode": int(code),
    "expected_diagnostic": diagnostic,
    "status": "PASS" if int(code) != 0 and diagnostic else "FAIL",
    "log": {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()},
}
path.with_suffix(".json").write_text(json.dumps(receipt, sort_keys=True) + "\n")
print("STORAGE_NEGATIVE_FEATURE_GRADE=" + json.dumps(receipt, sort_keys=True))
PY
            fi
            if (( negative_status == 0 )); then
                cat "${log}"
                if [[ -z "${negative_evidence}" ]]; then rm -f "${log}"; fi
                printf 'incompatible Storage features unexpectedly compiled: %s\n' "${features}" >&2
                exit 1
            fi
            if [[ -z "${diagnostic}" ]]; then
                cat "${log}"
                if [[ -z "${negative_evidence}" ]]; then rm -f "${log}"; fi
                printf 'negative feature check failed for an unrelated reason\n' >&2
                exit 1
            fi
            if [[ -z "${negative_evidence}" ]]; then rm -f "${log}"; fi
        done <<< "${illegal}"
        break
    fi
done
