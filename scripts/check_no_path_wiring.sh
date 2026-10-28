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

cargo_failures="$(
    python3 - "${WORKSPACE_DIR}" <<'PY'
from pathlib import Path
import subprocess
import sys
import tomllib

root = Path(sys.argv[1])
sys.path.insert(0, str(root / "scripts"))
from security_dependencies import SecurityPolicy

listing = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--",
                          ":(glob)Cargo.toml", ":(glob)*/Cargo.toml", ":(exclude)tmp/**"],
                         cwd=root, capture_output=True, check=True)
for raw in listing.stdout.split(b"\0"):
    if not raw:
        continue
    relative = raw.decode()
    path = root / relative
    if not path.is_file():
        continue
    try:
        SecurityPolicy.validate_manifest(tomllib.loads(path.read_text()), relative)
    except ValueError as error:
        print(f"{relative}: {error}")
PY
)"

if [[ -n "${rust_failures}" || -n "${cargo_failures}" ]]; then
    if [[ -n "${rust_failures}" ]]; then
        printf 'Rust #[path] wiring is banned. Offending files:\n%s\n' "${rust_failures}" >&2
    fi
    if [[ -n "${cargo_failures}" ]]; then
        printf 'Cargo dependency patches and non-workspace path keys are banned. Offending entries:\n%s\n' "${cargo_failures}" >&2
    fi
    exit 1
fi
