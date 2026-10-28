#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_DIR="$(cd -- "${CRATE_DIR}/.." && pwd)"
source "${WORKSPACE_DIR}/scripts/release_stage.sh"

require_env() {
    local name="$1"
    if [[ -z "${!name:-}" ]]; then
        printf 'missing required strict-live variable: %s\n' "${name}" >&2
        exit 1
    fi
}

require_prefix() {
    local name="$1"
    local prefix="$2"
    if [[ "${!name}" != "${prefix}"?* ]]; then
        printf 'invalid strict-live resource shape: %s\n' "${name}" >&2
        exit 1
    fi
}

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

RUN_LONG_BATCHES="${PROVIDERS_KIT_RUN_LONG_BATCHES-1}"
case "${RUN_LONG_BATCHES}" in
    0|1) ;;
    *)
        printf 'invalid strict-live setting: PROVIDERS_KIT_RUN_LONG_BATCHES must be 0 or 1\n' >&2
        exit 1
        ;;
esac

STRICT_LIVE_ARGS=(--nocapture --test-threads=1)
if [[ "${RUN_LONG_BATCHES}" == 0 ]]; then
    COVERAGE_MODE=PARTIAL
    STRICT_LIVE_ARGS+=(--exact)
    printf '\n==> PARTIAL strict-live coverage; omitting these seven named completion cases:\n'
    for name in \
        live::batches::openai_inline_batch_completes \
        live::batches::openai_provider_file_batch_completes \
        live::batches::openai_batch_preserves_mixed_row_outcomes \
        live::batches::openai_batch_result_limit_is_enforced \
        live::batches::anthropic_batch_preserves_mixed_row_outcomes \
        live::batches::anthropic_batch_result_limit_is_enforced \
        live::batches::anthropic_inline_batch_completes; do
        STRICT_LIVE_ARGS+=(--skip "${name}")
        printf '  %s\n' "${name}"
    done
else
    COVERAGE_MODE=FULL
    printf '\n==> FULL strict-live coverage; all completion cases enabled\n'
fi

printf '\n==> strict-live environment\n'
for name in \
    TEST_OPENAI_API_KEY \
    TEST_OPENAI_BASE_URL \
    TEST_OPENAI_MODEL_DEFAULT \
    TEST_OPENAI_MODEL_EMBEDDINGS \
    TEST_OPENAI_MODEL_INTERACTIVE \
    TEST_OPENAI_FILE_SEARCH_STORE_ID \
    TEST_ANTHROPIC_API_KEY \
    TEST_ANTHROPIC_BASE_URL \
    TEST_ANTHROPIC_MODEL_DEFAULT \
    TEST_ANTHROPIC_MODEL_THINKING \
    TEST_GEMINI_API_KEY \
    TEST_GEMINI_BASE_URL \
    TEST_GEMINI_MODEL_DEFAULT \
    TEST_GEMINI_MODEL_EMBEDDINGS \
    TEST_GEMINI_MODEL_THINKING_LEVEL \
    TEST_GEMINI_SERVICE_TIER \
    TEST_GEMINI_FILE_SEARCH_STORE_ID \
    TEST_GEMINI_MODEL_FILE_SEARCH \
    TEST_OLLAMA_BASE_URL \
    TEST_OLLAMA_MODEL_DEFAULT \
    TEST_OLLAMA_MODEL_REASONING \
    TEST_OLLAMA_MODEL_EMBEDDINGS; do
    require_env "${name}"
done

require_prefix TEST_OPENAI_FILE_SEARCH_STORE_ID 'vs_'
require_prefix TEST_GEMINI_FILE_SEARCH_STORE_ID 'fileSearchStores/'

OLLAMA_API_BASE="${TEST_OLLAMA_BASE_URL%/}"
if [[ "${OLLAMA_API_BASE}" != */api ]]; then
    printf 'TEST_OLLAMA_BASE_URL must point at the exact Ollama API base ending in /api: %s\n' "${TEST_OLLAMA_BASE_URL}" >&2
    exit 1
fi

printf '\n==> deterministic providers-kit gate\n'
bash providers-kit/scripts/check.sh

printf '\n==> Ollama availability check\n'
curl --connect-timeout 2 --max-time 10 -fsS "${OLLAMA_API_BASE}/version" >/dev/null

printf '\n==> required Ollama models\n'
TAGS_JSON="$(curl --connect-timeout 2 --max-time 10 -fsS "${OLLAMA_API_BASE}/tags")"
TAGS_JSON_COMPACT="$(printf '%s' "${TAGS_JSON}" | tr -d '[:space:]')"
for model in \
    "${TEST_OLLAMA_MODEL_DEFAULT}" \
    "${TEST_OLLAMA_MODEL_REASONING}" \
    "${TEST_OLLAMA_MODEL_EMBEDDINGS}" \
    "gemma4:e4b-it-qat" \
    "qwen3.5:9b-q4_K_M"; do
    if ! printf '%s' "${TAGS_JSON_COMPACT}" | rg -Fq "\"name\":\"${model}\"" \
        && ! printf '%s' "${TAGS_JSON_COMPACT}" | rg -Fq "\"model\":\"${model}\""; then
        printf 'missing required Ollama model: %s\n' "${model}" >&2
        exit 1
    fi
done

printf '\n==> strict live public contract suite\n'
# Keep Kit success separate from model-copy quality while requiring both receipts.
# The retained log is private because other test diagnostics can contain input data.
mkdir -p "${WORKSPACE_DIR}/target/providers-kit-strict-live"
STRICT_LOG_DIR="$(umask 077; mktemp -d "${WORKSPACE_DIR}/target/providers-kit-strict-live/realtime-grades-XXXXXXXX")"
STRICT_LOG="${STRICT_LOG_DIR}/strict-live.log"
(umask 077; : > "${STRICT_LOG}")
printf 'Private strict-live log: %s\n' "${STRICT_LOG}"
set +e
release_stage provider.live.strict_live cargo test --locked -p providers-kit --test strict_live --all-features -- "${STRICT_LIVE_ARGS[@]}" 2>&1 | tee "${STRICT_LOG}"
STRICT_PIPE_STATUS=("${PIPESTATUS[@]}")
python3 - "${STRICT_LOG}" "${TEST_OPENAI_MODEL_INTERACTIVE}" <<'PY'
import json
import sys

prefix = b"PROVIDERS_KIT_REALTIME_GRADE="
expected = {
    "live::resources::openai_interactive_session": {"function_turn", "audio_turn"},
    "live::resources::openai_interactive_cancellation_and_subsequent_progress": {
        "post_cancellation_function_turn"
    },
}
fields = {"version", "case", "model_id", "configured_model", "sdk_contract", "model_exact_copy"}
seen = set()
grades = []

def unique_object(items):
    value = dict(items)
    if len(value) != len(items):
        raise ValueError("duplicate keys")
    return value

def reject_constant(_value):
    raise ValueError("non-JSON constant")

try:
    total = 0
    line_start = True
    with open(sys.argv[1], "rb") as log:
        while chunk := log.readline(4097):
            total += len(chunk)
            if total > 64 * 1024 * 1024:
                raise ValueError("log bound")
            if line_start and chunk.startswith(prefix):
                if len(chunk) > 4096 or len(seen) >= 2:
                    raise ValueError("receipt bound")
                value = json.loads(
                    chunk[len(prefix):].decode("utf-8"),
                    object_pairs_hook=unique_object,
                    parse_constant=reject_constant,
                )
                if not isinstance(value, dict) or set(value) != fields:
                    raise ValueError("receipt fields")
                case = value["case"]
                if not isinstance(case, str) or case not in expected or case in seen:
                    raise ValueError("receipt case")
                if type(value["version"]) is not int or value["version"] != 1:
                    raise ValueError("receipt version")
                if value["model_id"] != sys.argv[2] or value["configured_model"] != sys.argv[2]:
                    raise ValueError("receipt model")
                if value["sdk_contract"] is not True:
                    raise ValueError("Kit contract")
                copy = value["model_exact_copy"]
                if not isinstance(copy, dict) or set(copy) != expected[case]:
                    raise ValueError("grade fields")
                if any(type(grade) is not bool for grade in copy.values()):
                    raise ValueError("grade type")
                seen.add(case)
                grades.extend(copy.values())
            line_start = chunk.endswith(b"\n")
    if seen != set(expected) or len(grades) != 3:
        raise ValueError("missing receipts")
except (OSError, ValueError, TypeError, RecursionError):
    print("invalid or incomplete OpenAI Realtime grade receipts; inspect the private log", file=sys.stderr)
    sys.exit(1)
print(f"Realtime Kit contracts: 2/2; exact-copy grades: {sum(grades)}/3 (nonblocking)")
PY
STRICT_REPORT_STATUS=$?
set -e
if (( STRICT_PIPE_STATUS[0] != 0 )); then
    exit "${STRICT_PIPE_STATUS[0]}"
fi
if (( STRICT_PIPE_STATUS[1] != 0 )); then
    exit "${STRICT_PIPE_STATUS[1]}"
fi
if (( STRICT_REPORT_STATUS != 0 )); then
    exit "${STRICT_REPORT_STATUS}"
fi

printf '\n==> workspace-root artifact hygiene\n'
check_workspace_root_artifacts

printf '\n==> %s strict-live selection passed\n' "${COVERAGE_MODE}"
