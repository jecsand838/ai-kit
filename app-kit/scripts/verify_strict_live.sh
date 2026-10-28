#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_DIR="$(cd -- "${CRATE_DIR}/.." && pwd)"
source "${WORKSPACE_DIR}/scripts/release_stage.sh"
ENV_FILE="${CRATE_DIR}/strict-tests.env"

if [[ ! -s "${ENV_FILE}" ]]; then
    printf 'missing populated app-kit strict environment: %s\n' "${ENV_FILE}" >&2
    exit 1
fi

set -a
source "${ENV_FILE}"
set +a
cd "${WORKSPACE_DIR}"

release_stage_fresh app.precheck bash app-kit/scripts/precheck_strict_live.sh
# check.sh verifies and runs all four prepare_contract cases, including the
# canonical four-boundary crash journey, before this live execution.
bash app-kit/scripts/check.sh
release_stage app.live.workflow_live cargo test --locked -p app-kit --test workflow_live \
    -- --nocapture --test-threads=1

if [[ -d "${CRATE_DIR}/wip" ]] \
    && find "${CRATE_DIR}/wip" -mindepth 1 -print -quit | grep -q .; then
    printf 'app-kit strict gate left files under app-kit/wip\n' >&2
    exit 1
fi
