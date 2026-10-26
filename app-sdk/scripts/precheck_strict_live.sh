#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_DIR="$(cd -- "${CRATE_DIR}/.." && pwd)"
LOWER_SPECIFICATIONS="${WORKSPACE_DIR}/agent-sdk/specs/SPECIFICATIONS.md"

for command in curl jq opencode ollama; do
    if ! command -v "${command}" >/dev/null 2>&1; then
        printf 'missing required command: %s\n' "${command}" >&2
        exit 1
    fi
done

for name in \
    OPENCODE_CONFIG_CONTENT \
    TEST_OPENCODE_URL \
    TEST_CUSTOM_OLLAMA_URL \
    TEST_OLLAMA_MODEL_OPENCODE_DEFAULT \
    OPENCODE_TEST_HOME \
    XDG_CONFIG_HOME \
    XDG_DATA_HOME \
    XDG_CACHE_HOME \
    XDG_STATE_HOME \
    OPENCODE_DB \
    OPENCODE_AUTO_SHARE \
    OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS; do
    if [[ -z "${!name:-}" ]]; then
        printf 'missing required app-sdk strict-live variable: %s\n' "${name}" >&2
        exit 1
    fi
    if [[ "${!name}" == [[:space:]]* || "${!name}" == *[[:space:]] ]]; then
        printf 'strict-live variable contains surrounding whitespace: %s\n' "${name}" >&2
        exit 1
    fi
done

case "${TEST_OPENCODE_URL}" in
    http://* | https://*)
        ;;
    *)
        printf 'TEST_OPENCODE_URL must be an HTTP(S) base URL\n' >&2
        exit 1
        ;;
esac
case "${TEST_CUSTOM_OLLAMA_URL}" in
    http://* | https://*)
        ;;
    *)
        printf 'TEST_CUSTOM_OLLAMA_URL must be an HTTP(S) base URL\n' >&2
        exit 1
        ;;
esac
if [[ "${TEST_OPENCODE_URL}" =~ [[:space:][:cntrl:]] \
    || "${TEST_OPENCODE_URL}" == *'?'* \
    || "${TEST_OPENCODE_URL}" == *'#'* \
    || "${TEST_OPENCODE_URL}" == *'@'* \
    || "${TEST_CUSTOM_OLLAMA_URL}" =~ [[:space:][:cntrl:]] \
    || "${TEST_CUSTOM_OLLAMA_URL}" == *'?'* \
    || "${TEST_CUSTOM_OLLAMA_URL}" == *'#'* \
    || "${TEST_CUSTOM_OLLAMA_URL}" == *'@'* ]]; then
    printf 'strict-live endpoints must be credential-free bounded base URLs\n' >&2
    exit 1
fi
if [[ "${TEST_CUSTOM_OLLAMA_URL%/}" != */api ]]; then
    printf 'TEST_CUSTOM_OLLAMA_URL must end in /api\n' >&2
    exit 1
fi
if (( ${#TEST_OLLAMA_MODEL_OPENCODE_DEFAULT} > 256 )) \
    || [[ "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}" == /* \
        || "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}" == *'..'* \
        || "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}" == *'://'* \
        || "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}" == *[![:alnum:]_.:+/-]* ]]; then
    printf 'TEST_OLLAMA_MODEL_OPENCODE_DEFAULT must be a bounded exact model selector\n' >&2
    exit 1
fi
if [[ "${OPENCODE_AUTO_SHARE}" != "false" \
    || "${OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS}" != "false" ]]; then
    printf 'OpenCode sharing and background subagents must be disabled\n' >&2
    exit 1
fi
EXPECTED_OPENCODE_HOME="${WORKSPACE_DIR}/target/app-sdk-strict-live/opencode"
if [[ "${OPENCODE_TEST_HOME}" != "${EXPECTED_OPENCODE_HOME}" \
    || "${XDG_CONFIG_HOME}" != "${OPENCODE_TEST_HOME}/xdg/config" \
    || "${XDG_DATA_HOME}" != "${OPENCODE_TEST_HOME}/xdg/data" \
    || "${XDG_CACHE_HOME}" != "${OPENCODE_TEST_HOME}/xdg/cache" \
    || "${XDG_STATE_HOME}" != "${OPENCODE_TEST_HOME}/xdg/state" \
    || "${OPENCODE_DB}" != "${OPENCODE_TEST_HOME}/database/opencode.db" ]]; then
    printf 'OpenCode strict-live paths must use the exact target-backed app domain\n' >&2
    exit 1
fi

EXPECTED_OPENCODE_VERSION="$(
    sed -n 's/^- OpenCode Server: `v\([^`]*\)`$/\1/p' "${LOWER_SPECIFICATIONS}"
)"
ACTUAL_OPENCODE_VERSION="$(opencode --version)"
ACTUAL_OPENCODE_VERSION="${ACTUAL_OPENCODE_VERSION##* }"
if [[ -z "${EXPECTED_OPENCODE_VERSION}" \
    || "${ACTUAL_OPENCODE_VERSION}" != "${EXPECTED_OPENCODE_VERSION}" ]]; then
    printf 'OpenCode version does not match the selected lower contract\n' >&2
    exit 1
fi
printf 'opencode version: %s\n' "${ACTUAL_OPENCODE_VERSION}"
printf 'ollama version: %s\n' "$(ollama --version)"

OLLAMA_API_BASE="${TEST_CUSTOM_OLLAMA_URL%/}"
OLLAMA_OPENAI_BASE="${OLLAMA_API_BASE%/api}/v1"
CURL=(curl --connect-timeout 2 --max-time 10 -fsS)
"${CURL[@]}" "${OLLAMA_API_BASE}/version" >/dev/null
"${CURL[@]}" "${OLLAMA_API_BASE}/tags" \
    | jq -e \
        --arg model "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}" \
        '.models | type == "array" and any(.[]; .name == $model or .model == $model)' \
        >/dev/null

jq -e \
    --arg model "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}" \
    --arg ollama "${OLLAMA_OPENAI_BASE}" \
    '
        .share == "disabled"
        and .enabled_providers == ["ollama"]
        and .plugin == []
        and .mcp == {}
        and .model == ("ollama/" + $model)
        and .small_model == ("ollama/" + $model)
        and .agent.build.temperature == 0
        and (.provider | keys) == ["ollama"]
        and .provider.ollama.npm == "@ai-sdk/openai-compatible"
        and .provider.ollama.options.baseURL == $ollama
        and (.provider.ollama.models | has($model))
        and .provider.ollama.models[$model].name == $model
        and .provider.ollama.models[$model].temperature == true
        and .provider.ollama.models[$model].tool_call == true
        and .provider.ollama.models[$model].reasoning == false
        and .provider.ollama.models[$model].limit.context >= 65536
    ' <<<"${OPENCODE_CONFIG_CONTENT}" >/dev/null

"${CURL[@]}" "${TEST_OPENCODE_URL%/}/global/health" \
    | jq -e \
        --arg version "${EXPECTED_OPENCODE_VERSION}" \
        '.healthy == true and .version == $version' >/dev/null
"${CURL[@]}" -G \
    --data-urlencode "directory=${CRATE_DIR}" \
    "${TEST_OPENCODE_URL%/}/experimental/capabilities" \
    | jq -e '.backgroundSubagents == false' >/dev/null
"${CURL[@]}" -G \
    --data-urlencode "directory=${CRATE_DIR}" \
    "${TEST_OPENCODE_URL%/}/config" \
    | jq -e \
        --arg model "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}" \
        --arg ollama "${OLLAMA_OPENAI_BASE}" \
        '
            .share == "disabled"
            and .enabled_providers == ["ollama"]
            and .plugin == []
            and .mcp == {}
            and .model == ("ollama/" + $model)
            and .small_model == ("ollama/" + $model)
            and .agent.build.temperature == 0
            and .provider.ollama.options.baseURL == $ollama
            and (.provider.ollama.models | has($model))
            and .provider.ollama.models[$model].temperature == true
        ' >/dev/null
"${CURL[@]}" -G \
    --data-urlencode "directory=${CRATE_DIR}" \
    "${TEST_OPENCODE_URL%/}/provider" \
    | jq -e \
        --arg model "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}" \
        '
            .connected == ["ollama"]
            and (.all | type == "array" and length == 1)
            and .all[0].id == "ollama"
            and (.all[0].models | has($model))
            and .all[0].models[$model].capabilities.temperature == true
        ' >/dev/null

if [[ ! -f "${WORKSPACE_DIR}/app-sdk/Cargo.toml" || ! -e "${WORKSPACE_DIR}/.git" ]]; then
    printf 'app-sdk strict-live gate is not running from the ai-kit workspace\n' >&2
    exit 1
fi

printf 'app-sdk strict-live prerequisites are healthy\n'
