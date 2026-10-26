#!/usr/bin/env bash
# Sourcing this file does not alter shell options or the caller's environment.
# Only an explicit release receipt run activates evidence reuse.
release_stage() {
    if (( $# < 2 )) || [[ ! "$1" =~ ^[a-z0-9][a-z0-9._-]*$ ]]; then
        printf 'usage: release_stage STAGE_ID COMMAND [ARG ...]\n' >&2
        return 2
    fi
    local release_stage_id="$1"
    shift
    if [[ "${AI_KIT_RELEASE_RECEIPT_RUN+x}" != x ]]; then
        "$@"
        return $?
    fi
    if [[ -z "${AI_KIT_RELEASE_RECEIPT_RUN}" ]]; then
        printf 'AI_KIT_RELEASE_RECEIPT_RUN must name an initialized release run\n' >&2
        return 2
    fi
    local release_stage_root
    release_stage_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)" || return
    python3 "${release_stage_root}/scripts/release_evidence.py" stage "${release_stage_id}" -- "$@"
}

release_stage_fresh() {
    if [[ "${AI_KIT_RELEASE_RECEIPT_RUN+x}" == x ]]; then
        AI_KIT_RELEASE_STAGE_FRESH=1 release_stage "$@"
    else
        release_stage "$@"
    fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    set -euo pipefail
    if [[ "${1-}" == --fresh ]]; then
        shift
        release_stage_fresh "$@"
    else
        release_stage "$@"
    fi
fi
