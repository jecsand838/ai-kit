#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

scan_file() {
    local file="$1"
    awk -v file="${file}" '
        function brace_delta(text,    opens, closes) {
            opens = gsub(/\{/, "{", text)
            closes = gsub(/\}/, "}", text)
            return opens - closes
        }

        function starts_associated_type_context(text) {
            return text ~ /^[[:space:]]*(pub(\([^)]*\))?[[:space:]]+)?(unsafe[[:space:]]+)?trait([<[:space:]])/ \
                || (text ~ /^[[:space:]]*(unsafe[[:space:]]+)?impl([<[:space:]])/ && text ~ /[[:space:]]for[[:space:]]/)
        }

        function is_type_alias(text) {
            return text ~ /^[[:space:]]*(pub(\([^)]*\))?[[:space:]]+)?type[[:space:]]+[A-Za-z_][A-Za-z0-9_]*([[:space:]]*<[^=;]*>)?[[:space:]]*=/
        }

        {
            code = $0
            sub(/\/\/.*/, "", code)

            if (associated_depth == 0 && pending_header != "") {
                pending_header = pending_header " " code
                if (code ~ /\{/) {
                    if (starts_associated_type_context(pending_header)) {
                        associated_depth = brace_delta(pending_header)
                    }
                    pending_header = ""
                }
                next
            }

            if (is_type_alias(code) && associated_depth == 0) {
                printf "%s:%d:%s\n", file, FNR, $0
            }

            if (associated_depth == 0 && code ~ /^[[:space:]]*(pub(\([^)]*\))?[[:space:]]+)?(unsafe[[:space:]]+)?(trait|impl)([<[:space:]])/) {
                if (code ~ /\{/) {
                    if (starts_associated_type_context(code)) {
                        associated_depth = brace_delta(code)
                    }
                } else {
                    pending_header = code
                }
                next
            }

            if (associated_depth > 0) {
                associated_depth += brace_delta(code)
                if (associated_depth < 0) {
                    associated_depth = 0
                }
            }
        }
    ' "${WORKSPACE_DIR}/${file}"
}

failures="$(
    cd "${WORKSPACE_DIR}"
    while IFS= read -r file; do
        [[ -f "${file}" ]] || continue
        scan_file "${file}"
    done < <(
        git ls-files --cached --others --exclude-standard -- \
            '*.rs' \
            '*/specs/*.md' \
            '*/specs/**/*.md' \
            ':(exclude)tmp/**'
    )
)"

if [[ -n "${failures}" ]]; then
    printf 'Rust item type aliases are banned workspace-wide at every visibility, including private, pub(crate), and public aliases. Replace aliases with concrete structs, enums, newtypes, or direct concrete types. Associated type declarations in traits and assignments in trait impls are allowed.\n%s\n' "${failures}" >&2
    exit 1
fi
