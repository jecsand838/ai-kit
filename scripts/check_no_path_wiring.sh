#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

rust_failures="$(
    cd "${WORKSPACE_DIR}"
    while IFS= read -r file; do
        [[ -f "${file}" ]] || continue
        compact="$(LC_ALL=C tr -d '[:space:]' < "${file}")"
        if [[ "${compact}" == *"#[path="* || "${compact}" == *"#![path="* ]]; then
            printf '%s\n' "${file}"
        fi
    done < <(
        git ls-files --cached --others --exclude-standard -- \
            '*.rs' \
            ':(exclude)tmp/**'
    )
)"

allowed_workspace_member_path() {
    local file="$1"
    local line="$2"
    [[ "${file}" == "Cargo.toml" ]] || return 1
    case "${line}" in
        'agent-sdk = { path = "agent-sdk", version = "=0.1.0" }') return 0 ;;
        'provider-sdk = { path = "provider-sdk", version = "0.1.0" }') return 0 ;;
        'storage-sdk = { path = "storage-sdk", version = "0.1.0", default-features = false }') return 0 ;;
        'tool-sdk = { path = "tool-sdk", version = "0.1.0" }') return 0 ;;
        *) return 1 ;;
    esac
}

cargo_failures="$(
    cd "${WORKSPACE_DIR}"
    while IFS= read -r file; do
        line_number=0
        while IFS= read -r line || [[ -n "${line}" ]]; do
            line_number=$((line_number + 1))
            code="${line%%#*}"
            if [[ "${code}" =~ (^|[,\{[:space:]])path[[:space:]]*= ]] \
                && ! allowed_workspace_member_path "${file}" "${line}"; then
                printf '%s:%s:%s\n' "${file}" "${line_number}" "${line}"
            fi
        done < "${file}"
    done < <(
        git ls-files --cached --others --exclude-standard -- \
            ':(glob)Cargo.toml' \
            ':(glob)*/Cargo.toml' \
            ':(exclude)tmp/**'
    )
)"

if [[ -n "${rust_failures}" || -n "${cargo_failures}" ]]; then
    if [[ -n "${rust_failures}" ]]; then
        printf 'Rust #[path] wiring is banned. Offending files:\n%s\n' "${rust_failures}" >&2
    fi
    if [[ -n "${cargo_failures}" ]]; then
        printf 'Cargo.toml path keys are banned. Offending entries:\n%s\n' "${cargo_failures}" >&2
    fi
    exit 1
fi
