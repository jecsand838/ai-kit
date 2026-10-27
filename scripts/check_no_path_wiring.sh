#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

# Only complete, verified upstream source trees receive the approved exception.
approved_roots="$(python3 "${SCRIPT_DIR}/security_dependencies.py" verify --root "${WORKSPACE_DIR}" --print-roots)"
verified_upstream_file() {
    local root
    while IFS= read -r root; do
        if [[ -n "${root}" && "$1" == "${root}/"* ]]; then return 0; fi
    done <<< "${approved_roots}"
    return 1
}

rust_failures="$(
    cd "${WORKSPACE_DIR}"
    while IFS= read -r file; do
        [[ -f "${file}" ]] || continue
        if verified_upstream_file "${file}"; then continue; fi
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

cargo_failures="$(
    python3 - "${WORKSPACE_DIR}" "${approved_roots}" <<'PY'
from pathlib import Path
import re
import subprocess
import sys
import tomllib

root = Path(sys.argv[1])
approved = set(sys.argv[2].splitlines())
workspace = {
    "agent-sdk": {"path": "agent-sdk", "version": "=0.1.0"},
    "provider-sdk": {"path": "provider-sdk", "version": "0.1.0"},
    "storage-sdk": {"path": "storage-sdk", "version": "0.1.0", "default-features": False},
    "tool-sdk": {"path": "tool-sdk", "version": "0.1.0"},
}

def invalid_paths(value, context, relative):
    if isinstance(value, dict):
        if "path" in value:
            allowed_member = (relative == "Cargo.toml" and len(context) == 3
                              and context[:2] == ("workspace", "dependencies")
                              and value == workspace.get(context[2]))
            allowed_patch = (relative == "Cargo.toml" and len(context) == 3
                             and context[:2] == ("patch", "crates-io")
                             and value.get("path") in approved)
            if not (allowed_member or allowed_patch):
                return True
        return any(invalid_paths(child, (*context, key), relative) for key, child in value.items())
    if isinstance(value, list):
        return any(invalid_paths(child, context, relative) for child in value)
    return False

listing = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--",
                          ":(glob)Cargo.toml", ":(glob)*/Cargo.toml", ":(exclude)tmp/**"],
                         cwd=root, capture_output=True, check=True)
for raw in listing.stdout.split(b"\0"):
    if not raw:
        continue
    relative = raw.decode()
    source = (root / relative).read_text()
    try:
        document = tomllib.loads(source)
    except tomllib.TOMLDecodeError as error:
        print(f"{relative}: invalid Cargo manifest: {error}")
        continue
    if invalid_paths(document, (), relative):
        # Decide exceptions structurally, but retain useful line diagnostics.
        reported = False
        for number, line in enumerate(source.splitlines(), 1):
            if re.search(r"\bpath\s*=", line.split("#", 1)[0]):
                print(f"{relative}:{number}:{line}")
                reported = True
        if not reported:
            print(f"{relative}: forbidden Cargo path key")
PY
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
