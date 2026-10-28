#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_DIR="$(cd -- "${CRATE_DIR}/.." && pwd)"
STRICT_WORKSPACE="${WORKSPACE_DIR}"
OPENCODE_DIRECTORY="${CRATE_DIR}"
SPECIFICATIONS_PATH="${WORKSPACE_DIR}/agents-kit/specs/SPECIFICATIONS.md"

# Readiness reads use the same bounds as the App and Provider gates.
# Cold model loading gets a separate finite budget below.
CURL=(curl --connect-timeout 2 --max-time 10 --retry 0 -fsS)

require_command() {
    local name="$1"
    if ! command -v "${name}" >/dev/null 2>&1; then
        printf 'missing required command: %s\n' "${name}" >&2
        exit 1
    fi
}

require_env() {
    local name="$1"
    if [[ -z "${!name:-}" ]]; then
        printf 'missing required env: %s\n' "${name}" >&2
        exit 1
    fi
    if [[ "${!name}" == [[:space:]]* || "${!name}" == *[[:space:]] ]]; then
        printf 'strict env must not contain surrounding whitespace: %s\n' "${name}" >&2
        exit 1
    fi
}

require_secret() {
    local name="$1"
    require_env "${name}"
    if [[ "${!name}" == "replace-me" ]]; then
        printf 'strict secret must be configured: %s\n' "${name}" >&2
        exit 1
    fi
}

validate_optional_secret() {
    local name="$1"
    if [[ -z "${!name+x}" ]]; then
        return 0
    fi
    require_env "${name}"
    case "${!name}" in
        replace-me | your-password)
            printf 'optional strict secret must be configured when present: %s\n' "${name}" >&2
            exit 1
            ;;
    esac
}

require_http_url() {
    local name="$1"
    local value="${!name}"
    case "${value}" in
        http://* | https://*)
            ;;
        *)
            printf '%s should use http:// or https://\n' "${name}" >&2
            exit 1
            ;;
    esac
    local rest="${value#*://}"
    local authority="${rest%%/*}"
    if [[ -z "${authority}" || "${authority}" == *'@'* || "${value}" == *'?'* || "${value}" == *'#'* || "${value}" == *\\* ]]; then
        printf '%s must be a bounded credential-free base URL without whitespace, query, or fragment\n' "${name}" >&2
        exit 1
    fi
    if [[ "${value}" =~ [[:space:][:cntrl:]] ]] || (( ${#value} > 2048 )); then
        printf '%s must be a bounded credential-free base URL without whitespace, query, or fragment\n' "${name}" >&2
        exit 1
    fi
}

require_ws_url() {
    local name="$1"
    local value="${!name}"
    case "${value}" in
        ws://* | wss://*)
            ;;
        *)
            printf '%s should use ws:// or wss://\n' "${name}" >&2
            exit 1
            ;;
    esac
    local rest="${value#*://}"
    local authority="${rest%%/*}"
    if [[ -z "${authority}" || "${authority}" == *'@'* || "${value}" == *'?'* || "${value}" == *'#'* || "${value}" == *\\* ]]; then
        printf '%s must be a bounded credential-free URL without whitespace, query, or fragment\n' "${name}" >&2
        exit 1
    fi
    if [[ "${value}" =~ [[:space:][:cntrl:]] ]] || (( ${#value} > 2048 )); then
        printf '%s must be a bounded credential-free URL without whitespace, query, or fragment\n' "${name}" >&2
        exit 1
    fi
}

require_model_selector() {
    local name="$1"
    local value="${!name}"
    if (( ${#value} > 256 )) || [[ "${value}" == /* || "${value}" == *'//'* || "${value}" == *'://'* || "${value}" == *'..'* || "${value}" == *[![:alnum:]_.:/+-]* ]]; then
        printf '%s must be a bounded model selector using only safe identifier characters\n' "${name}" >&2
        exit 1
    fi
}

reject_env() {
    local name="$1"
    if [[ -n "${!name+x}" ]]; then
        printf 'retired strict env must be unset: %s\n' "${name}" >&2
        exit 1
    fi
}

require_target_path() {
    local name="$1"
    require_env "${name}"
    local value="${!name}"
    case "${value}" in
        "${WORKSPACE_DIR}/target/"*)
            ;;
        *)
            printf '%s must be an absolute descendant of %s/target\n' "${name}" "${WORKSPACE_DIR}" >&2
            exit 1
            ;;
    esac
    if [[ "${value}" == *'/../'* || "${value}" == */.. || "${value}" == *'/./'* || "${value}" == */. ]]; then
        printf '%s must not contain dot path segments\n' "${name}" >&2
        exit 1
    fi
}

require_gemini_model_selector() {
    local name="$1"
    require_model_selector "${name}"
    case "${!name}" in
        models/*)
            ;;
        *)
            printf '%s must use an exact models/... Gemini resource id\n' "${name}" >&2
            exit 1
            ;;
    esac
}

require_gemini_service_tier() {
    local name="$1"
    case "${!name}" in
        standard | flex | priority)
            ;;
        *)
            printf '%s must be one of standard, flex, or priority\n' "${name}" >&2
            exit 1
            ;;
    esac
}

validate_workspace() {
    local workspace="$1"
    if [[ ! -d "${workspace}" ]]; then
        printf 'strict workspace does not exist: %s\n' "${workspace}" >&2
        exit 1
    fi
    if [[ ! -e "${workspace}/.git" ]]; then
        printf 'strict workspace is not Git-backed: %s\n' "${workspace}" >&2
        exit 1
    fi
    if [[ ! -f "${workspace}/agents-kit/Cargo.toml" ]]; then
        printf 'strict workspace is not the ai-kit repo root: %s\n' "${workspace}" >&2
        exit 1
    fi
}

socket_host() {
    local url="$1"
    local rest="${url#*://}"
    local hostport="${rest%%/*}"
    if [[ "${hostport}" == \[*\]:* ]]; then
        local bracketed="${hostport#\[}"
        printf '%s\n' "${bracketed%%]*}"
        return 0
    fi
    printf '%s\n' "${hostport%%:*}"
}

socket_port() {
    local url="$1"
    local rest="${url#*://}"
    local hostport="${rest%%/*}"
    if [[ "${hostport}" == \[*\]:* ]]; then
        printf '%s\n' "${hostport##*:}"
        return 0
    fi
    local port="${hostport##*:}"
    if [[ "${port}" == "${hostport}" ]]; then
        printf 'missing explicit port in URL: %s\n' "${url}" >&2
        exit 1
    fi
    printf '%s\n' "${port}"
}

is_loopback_host() {
    case "$1" in
        127.0.0.1 | localhost | ::1 | '[::1]')
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

lsof_hostport() {
    local host="$1"
    local port="$2"
    if [[ "${host}" == *:* ]]; then
        printf '[%s]:%s\n' "${host}" "${port}"
        return 0
    fi
    printf '%s:%s\n' "${host}" "${port}"
}

check_socket() {
    local host="$1"
    local port="$2"
    local probe_file
    probe_file="$(mktemp)"
    if node -e '
        const net = require("node:net");
        const socket = net.connect({host: process.argv[1], port: Number(process.argv[2])});
        const timer = setTimeout(() => {
            socket.destroy();
            process.stderr.write("TCP readiness deadline elapsed\n");
            process.exit(1);
        }, 2000);
        socket.once("connect", () => { clearTimeout(timer); socket.destroy(); });
        socket.once("error", () => {
            clearTimeout(timer);
            socket.destroy();
            process.stderr.write("TCP readiness connection failed\n");
            process.exitCode = 1;
        });
    ' "${host}" "${port}" 2>"${probe_file}"; then
        rm -f "${probe_file}"
        return 0
    fi

    local probe_error
    probe_error="$(<"${probe_file}")"
    rm -f "${probe_file}"

    if is_loopback_host "${host}"; then
        if ! command -v lsof >/dev/null 2>&1; then
            if [[ -n "${probe_error}" ]]; then
                printf '%s\n' "${probe_error}" >&2
            fi
            printf 'missing required command for sandbox localhost fallback: lsof\n' >&2
            return 1
        fi

        if lsof -nP -iTCP@"$(lsof_hostport "${host}" "${port}")" -sTCP:LISTEN >/dev/null 2>&1; then
            printf 'direct loopback socket probe for %s:%s returned nonzero; validated listener via lsof\n' "${host}" "${port}"
            return 0
        fi
    fi

    if [[ -n "${probe_error}" ]]; then
        printf '%s\n' "${probe_error}" >&2
    fi
    return 1
}

opencode_curl() {
    if [[ -n "${TEST_OPENCODE_PASSWORD:-}" ]]; then
        "${CURL[@]}" -u "${TEST_OPENCODE_USERNAME:-opencode}:${TEST_OPENCODE_PASSWORD}" "$@"
    else
        "${CURL[@]}" "$@"
    fi
}

require_selected_release() {
    local label="$1"
    local selected_tag="$2"
    local tag_prefix="$3"
    local actual_version="$4"
    local expected_version="${selected_tag#${tag_prefix}}"
    if [[ -z "${selected_tag}" || "${selected_tag}" == *$'\n'* || "${expected_version}" == "${selected_tag}" || -z "${expected_version}" ]]; then
        printf 'invalid selected %s tag: %s\n' "${label}" "${selected_tag}" >&2
        exit 1
    fi
    if [[ "${actual_version}" != "${expected_version}" ]]; then
        printf '%s version mismatch: expected %s, got %s\n' "${label}" "${expected_version}" "${actual_version}" >&2
        exit 1
    fi
}

unload_ollama_model() {
    local model="$1"
    local payload deadline remaining read_budget residency present
    deadline=$((SECONDS + 30))
    payload="$(jq -cn --arg model "${model}" '{model: $model, prompt: "", stream: false, keep_alive: 0}')" || return
    "${CURL[@]}" --max-time 30 \
        -H 'content-type: application/json' \
        --data-binary "${payload}" \
        "${TEST_CUSTOM_OLLAMA_URL%/}/generate" >/dev/null || return

    while true; do
        remaining=$((deadline - SECONDS))
        if (( remaining <= 0 )); then
            printf 'Ollama residency removal deadline elapsed for %s\n' "${model}" >&2
            return 1
        fi
        read_budget="${remaining}"
        if (( read_budget > 10 )); then
            read_budget=10
        fi
        residency="$("${CURL[@]}" --max-time "${read_budget}" "${TEST_CUSTOM_OLLAMA_URL%/}/ps")" || return
        present="$(jq -er --arg model "${model}" '
            .models | if type != "array" then error("models must be an array")
            elif all(.[];
                if type != "object" then false
                elif (.name | type) != "string" then false
                else (.name | test("\\S")) end
            ) then any(.[]; .name == $model)
            else error("every resident model must have a nonblank string name")
            end | tostring
        ' <<<"${residency}")" || return
        if [[ "${present}" == "false" ]]; then
            return 0
        fi
        sleep 0.1
    done
}

load_and_verify_ollama_model() {
    local model="$1"
    local keep_alive="$2"
    local payload
    unload_ollama_model "${model}"
    OLLAMA_CURRENT_MODEL="${model}"
    payload="$(
        jq -cn \
            --arg model "${model}" \
            --arg keep_alive "${keep_alive}" \
            '{model: $model, prompt: "", stream: false, keep_alive: $keep_alive}'
    )"
    "${CURL[@]}" --max-time 360 \
        -H 'content-type: application/json' \
        --data-binary "${payload}" \
        "${TEST_CUSTOM_OLLAMA_URL%/}/generate" >/dev/null

    "${CURL[@]}" "${TEST_CUSTOM_OLLAMA_URL%/}/ps" \
        | jq -e \
            --arg model "${model}" \
            '
                .models | type == "array" and any(.[];
                    .name == $model
                    and (.context_length | type == "number")
                    and .context_length >= 65536
                    and .context_length == (.context_length | floor)
                    and (.size | type == "number")
                    and (.size_vram | type == "number")
                    and .size_vram > 0
                    and .size <= 9007199254740991
                    and .size == (.size | floor)
                    and .size_vram == .size
                )
            ' \
            >/dev/null
}

printf '[1/8] checking local prerequisites\n'
require_command curl
require_command jq
require_command kill
require_command lsof
require_command node
require_command codex
require_command opencode

printf '[2/8] checking strict environment contract\n'
require_env TEST_CODEX_WS_URL
require_target_path TEST_CODEX_HOME
require_env TEST_CODEX_MODEL_DEFAULT
require_env TEST_CODEX_REASONING_EFFORT
require_env TEST_OPENCODE_URL
require_env TEST_CUSTOM_OLLAMA_URL
require_env TEST_OLLAMA_MODEL_OPENCODE_DEFAULT
require_env TEST_OLLAMA_MODEL_OPENCODE_ALTERNATE
require_env TEST_OLLAMA_MODEL_OPENCODE_SHELL
require_env TEST_OLLAMA_MODEL_CUSTOM_DEFAULT
require_env CARGO_INCREMENTAL
require_secret TEST_GEMINI_API_KEY
require_env TEST_GEMINI_BASE_URL
require_env TEST_GEMINI_MODEL_DEFAULT
require_env TEST_GEMINI_SERVICE_TIER
require_secret TEST_OPENAI_API_KEY
require_env TEST_OPENAI_BASE_URL
require_secret TEST_ANTHROPIC_API_KEY
require_env TEST_ANTHROPIC_BASE_URL
require_env TEST_CUSTOM_OPENAI_MODEL
require_env TEST_CUSTOM_ANTHROPIC_MODEL
require_env TEST_OPENCODE_CLOUD_MODEL
require_ws_url TEST_CODEX_WS_URL
require_http_url TEST_OPENCODE_URL
require_http_url TEST_CUSTOM_OLLAMA_URL
require_http_url TEST_GEMINI_BASE_URL
require_http_url TEST_OPENAI_BASE_URL
require_http_url TEST_ANTHROPIC_BASE_URL
require_model_selector TEST_CODEX_MODEL_DEFAULT
require_model_selector TEST_CODEX_REASONING_EFFORT
require_model_selector TEST_CUSTOM_OPENAI_MODEL
require_model_selector TEST_CUSTOM_ANTHROPIC_MODEL
require_model_selector TEST_OPENCODE_CLOUD_MODEL
require_model_selector TEST_OLLAMA_MODEL_OPENCODE_DEFAULT
require_model_selector TEST_OLLAMA_MODEL_OPENCODE_ALTERNATE
require_model_selector TEST_OLLAMA_MODEL_OPENCODE_SHELL
require_model_selector TEST_OLLAMA_MODEL_CUSTOM_DEFAULT
require_gemini_model_selector TEST_GEMINI_MODEL_DEFAULT
require_gemini_service_tier TEST_GEMINI_SERVICE_TIER
validate_optional_secret TEST_OPENCODE_PASSWORD

if [[ "${CARGO_INCREMENTAL}" != "0" ]]; then
    printf 'CARGO_INCREMENTAL must be exactly 0 so the canonical strict gate stays within this machine build-storage budget\n' >&2
    exit 1
fi

if [[ ! -s "${TEST_CODEX_HOME}/auth.json" ]]; then
    printf 'TEST_CODEX_HOME must contain a nonempty auth.json seeded without global Codex config or state\n' >&2
    exit 1
fi

for retired_name in \
    TEST_MODEL_CONFORMANCE_PROFILE \
    TEST_CODEX_BEARER_TOKEN \
    TEST_OLLAMA_MODEL_GPT_OSS_20B \
    TEST_OLLAMA_MODEL_QWEN3 \
    TEST_OLLAMA_MODEL_THINKING \
    AGENTS_KIT_STRICT_OPENCODE_DEFAULT_PROVIDER \
    AGENTS_KIT_STRICT_OPENCODE_DEFAULT_MODEL; do
    reject_env "${retired_name}"
done

if [[ "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}" == "${TEST_OLLAMA_MODEL_OPENCODE_ALTERNATE}" \
    || "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}" == "${TEST_OLLAMA_MODEL_OPENCODE_SHELL}" \
    || "${TEST_OLLAMA_MODEL_OPENCODE_ALTERNATE}" == "${TEST_OLLAMA_MODEL_OPENCODE_SHELL}" ]]; then
    printf 'OpenCode primary, alternate, and shell model roles must be pairwise distinct\n' >&2
    exit 1
fi

require_env OPENCODE_CONFIG_CONTENT
require_env OPENCODE_AUTO_SHARE
require_env OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS
require_target_path OPENCODE_TEST_HOME
require_target_path XDG_CONFIG_HOME
require_target_path XDG_DATA_HOME
require_target_path XDG_CACHE_HOME
require_target_path XDG_STATE_HOME
require_target_path OPENCODE_DB

if [[ "${OPENCODE_AUTO_SHARE}" != "false" || "${OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS}" != "false" ]]; then
    printf 'OpenCode sharing and background subagents must both be disabled\n' >&2
    exit 1
fi
if [[ "${XDG_CONFIG_HOME}" != "${OPENCODE_TEST_HOME}/xdg/config" \
    || "${XDG_DATA_HOME}" != "${OPENCODE_TEST_HOME}/xdg/data" \
    || "${XDG_CACHE_HOME}" != "${OPENCODE_TEST_HOME}/xdg/cache" \
    || "${XDG_STATE_HOME}" != "${OPENCODE_TEST_HOME}/xdg/state" \
    || "${OPENCODE_DB}" != "${OPENCODE_TEST_HOME}/database/opencode.db" ]]; then
    printf 'OpenCode XDG and database paths must use the exact isolated target-backed layout\n' >&2
    exit 1
fi

jq -e \
    --arg primary "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}" \
    --arg alternate "${TEST_OLLAMA_MODEL_OPENCODE_ALTERNATE}" \
    --arg shell "${TEST_OLLAMA_MODEL_OPENCODE_SHELL}" \
    '
        .share == "disabled"
        and .enabled_providers == ["ollama"]
        and .plugin == []
        and .mcp == {}
        and (has("agent") | not)
        and .model == ("ollama/" + $primary)
        and .small_model == ("ollama/" + $primary)
        and (.provider | keys) == ["ollama"]
        and .provider.ollama.npm == "@ai-sdk/openai-compatible"
        and .provider.ollama.options.baseURL == "http://127.0.0.1:11434/v1"
        and (.provider.ollama.models | keys | sort) == ([$primary, $alternate, $shell] | sort)
        and .provider.ollama.models[$primary].name == $primary
        and .provider.ollama.models[$primary].tool_call == true
        and .provider.ollama.models[$primary].reasoning == false
        and .provider.ollama.models[$primary].limit.context == 65536
        and .provider.ollama.models[$primary].limit.output == 4096
        and .provider.ollama.models[$alternate].name == $alternate
        and .provider.ollama.models[$alternate].tool_call == true
        and .provider.ollama.models[$alternate].reasoning == false
        and .provider.ollama.models[$alternate].limit.context == 65536
        and .provider.ollama.models[$alternate].limit.output == 4096
        and .provider.ollama.models[$shell].name == $shell
        and .provider.ollama.models[$shell].tool_call == true
        and .provider.ollama.models[$shell].reasoning == false
        and .provider.ollama.models[$shell].limit.context == 65536
        and .provider.ollama.models[$shell].limit.output == 4096
    ' <<<"${OPENCODE_CONFIG_CONTENT}" >/dev/null

if ! jq -e \
    --arg primary "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}" \
    --arg alternate "${TEST_OLLAMA_MODEL_OPENCODE_ALTERNATE}" \
    --arg shell "${TEST_OLLAMA_MODEL_OPENCODE_SHELL}" \
    '
        . as $config
        |
        all(
            [$primary, $alternate, $shell][];
            (. as $model
                | ($config.provider.ollama.models[$model] | has("options") | not)
                    or $config.provider.ollama.models[$model].options == {"reasoningEffort": "none"})
        )
    ' <<<"${OPENCODE_CONFIG_CONTENT}" >/dev/null; then
    printf 'OpenCode roles must declare only optional reasoningEffort none metadata\n' >&2
    exit 1
fi

printf '[3/8] checking strict workspace and pinned binaries\n'
validate_workspace "${STRICT_WORKSPACE}"

CODEX_SELECTED_TAG="$(
    sed -n 's/^- Codex App Server: `\(rust-v[^`]*\)`$/\1/p' "${SPECIFICATIONS_PATH}"
)"
OPENCODE_SELECTED_TAG="$(
    sed -n 's/^- OpenCode Server: `\(v[^`]*\)`$/\1/p' "${SPECIFICATIONS_PATH}"
)"
CODEX_EXPECTED_VERSION="${CODEX_SELECTED_TAG#rust-v}"
OPENCODE_EXPECTED_VERSION="${OPENCODE_SELECTED_TAG#v}"
CODEX_ACTUAL_VERSION="$(codex --version)"
CODEX_ACTUAL_VERSION="${CODEX_ACTUAL_VERSION##* }"
OPENCODE_ACTUAL_VERSION="$(opencode --version)"
OPENCODE_ACTUAL_VERSION="${OPENCODE_ACTUAL_VERSION##* }"
require_selected_release Codex "${CODEX_SELECTED_TAG}" rust-v "${CODEX_ACTUAL_VERSION}"
require_selected_release OpenCode "${OPENCODE_SELECTED_TAG}" v "${OPENCODE_ACTUAL_VERSION}"

printf '[4/8] using owned embedded Lance StateStore and Database fixtures\n'

printf '[5/8] checking Codex app-server socket\n'
CODEX_HOST="$(socket_host "${TEST_CODEX_WS_URL}")"
CODEX_PORT="$(socket_port "${TEST_CODEX_WS_URL}")"
check_socket "${CODEX_HOST}" "${CODEX_PORT}"
CODEX_PROTOCOL_PROBE="$(
    node --input-type=module -e '
        const endpoint = process.argv[1];
        const socket = new WebSocket(endpoint);
        const timer = setTimeout(() => process.exit(2), 15_000);
        let initialize;
        let account;
        let mcp;
        const finish = () => {
            if (initialize === undefined || account === undefined || mcp === undefined) return;
            clearTimeout(timer);
            process.stdout.write(JSON.stringify({
                userAgent: initialize.userAgent ?? null,
                codexHome: initialize.codexHome ?? null,
                accountPresent: account.account != null,
                requiresOpenaiAuth: account.requiresOpenaiAuth ?? null,
                mcpCount: Array.isArray(mcp.data) ? mcp.data.length : null,
                mcpNextCursor: mcp.nextCursor ?? null
            }));
            process.exit(0);
        };
        socket.addEventListener("open", () => {
            socket.send(JSON.stringify({
                id: 1,
                method: "initialize",
                params: {
                    clientInfo: {
                        name: "agents-kit-precheck",
                        title: "Agent Kit precheck",
                        version: "1"
                    },
                    capabilities: { experimentalApi: false }
                }
            }));
        });
        socket.addEventListener("message", (event) => {
            const message = JSON.parse(String(event.data));
            if (message.id === 1) {
                if (message.error !== undefined) process.exit(3);
                initialize = message.result;
                socket.send(JSON.stringify({ method: "initialized" }));
                socket.send(JSON.stringify({ id: 2, method: "account/read", params: {} }));
                socket.send(JSON.stringify({
                    id: 3,
                    method: "mcpServerStatus/list",
                    params: {
                        cursor: null,
                        limit: 100,
                        detail: "toolsAndAuthOnly",
                        threadId: null
                    }
                }));
            } else if (message.id === 2) {
                if (message.error !== undefined) process.exit(4);
                account = message.result;
                finish();
            } else if (message.id === 3) {
                if (message.error !== undefined) process.exit(5);
                mcp = message.result;
                finish();
            }
        });
        socket.addEventListener("error", () => process.exit(6));
    ' "${TEST_CODEX_WS_URL}"
)"
jq -e \
    --arg home "${TEST_CODEX_HOME}" \
    --arg version "${CODEX_EXPECTED_VERSION}" \
    '
        .codexHome == $home
        and (.userAgent | contains("/" + $version + " "))
        and .accountPresent == true
        and .requiresOpenaiAuth == true
        and .mcpCount == 0
        and .mcpNextCursor == null
    ' <<<"${CODEX_PROTOCOL_PROBE}" >/dev/null

printf '[6/8] checking OpenCode server health\n'
opencode_curl "${TEST_OPENCODE_URL}/global/health" \
    | jq -e \
        --arg version "${OPENCODE_EXPECTED_VERSION}" \
        '.healthy == true and .version == $version' >/dev/null

printf '[7/8] checking OpenCode effective provider and privacy policy\n'
opencode_curl -G \
    --data-urlencode "directory=${OPENCODE_DIRECTORY}" \
    "${TEST_OPENCODE_URL}/experimental/capabilities" \
    | jq -e '.backgroundSubagents == false' >/dev/null

OPENCODE_EFFECTIVE_CONFIG="$(opencode_curl -G \
    --data-urlencode "directory=${OPENCODE_DIRECTORY}" \
    "${TEST_OPENCODE_URL}/config")"
jq -e \
        --arg primary "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}" \
        --arg alternate "${TEST_OLLAMA_MODEL_OPENCODE_ALTERNATE}" \
        --arg shell "${TEST_OLLAMA_MODEL_OPENCODE_SHELL}" \
        '
            .share == "disabled"
            and .enabled_providers == ["ollama"]
            and .plugin == []
            and .mcp == {}
            and .model == ("ollama/" + $primary)
            and .small_model == ("ollama/" + $primary)
            and (.provider | keys) == ["ollama"]
            and .provider.ollama.npm == "@ai-sdk/openai-compatible"
            and .provider.ollama.options.baseURL == "http://127.0.0.1:11434/v1"
            and (.provider.ollama.models | keys | sort) == ([$primary, $alternate, $shell] | sort)
            and .provider.ollama.models[$primary].name == $primary
            and .provider.ollama.models[$primary].tool_call == true
            and .provider.ollama.models[$primary].reasoning == false
            and .provider.ollama.models[$primary].limit.context == 65536
            and .provider.ollama.models[$primary].limit.output == 4096
            and .provider.ollama.models[$alternate].name == $alternate
            and .provider.ollama.models[$alternate].tool_call == true
            and .provider.ollama.models[$alternate].reasoning == false
            and .provider.ollama.models[$alternate].limit.context == 65536
            and .provider.ollama.models[$alternate].limit.output == 4096
            and .provider.ollama.models[$shell].name == $shell
            and .provider.ollama.models[$shell].tool_call == true
            and .provider.ollama.models[$shell].reasoning == false
            and .provider.ollama.models[$shell].limit.context == 65536
            and .provider.ollama.models[$shell].limit.output == 4096
        ' <<<"${OPENCODE_EFFECTIVE_CONFIG}" >/dev/null

if ! jq -e \
    --arg primary "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}" \
    --arg alternate "${TEST_OLLAMA_MODEL_OPENCODE_ALTERNATE}" \
    --arg shell "${TEST_OLLAMA_MODEL_OPENCODE_SHELL}" \
    '
        . as $config
        |
        all(
            [$primary, $alternate, $shell][];
            (. as $model
                | ($config.provider.ollama.models[$model] | has("options") | not)
                    or $config.provider.ollama.models[$model].options == {"reasoningEffort": "none"})
        )
    ' <<<"${OPENCODE_EFFECTIVE_CONFIG}" >/dev/null; then
    printf 'effective OpenCode roles must expose only optional reasoningEffort none metadata\n' >&2
    exit 1
fi

opencode_curl -G \
    --data-urlencode "directory=${OPENCODE_DIRECTORY}" \
    "${TEST_OPENCODE_URL}/provider" \
    | jq -e \
        --arg primary "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}" \
        --arg alternate "${TEST_OLLAMA_MODEL_OPENCODE_ALTERNATE}" \
        --arg shell "${TEST_OLLAMA_MODEL_OPENCODE_SHELL}" \
        '
            .connected == ["ollama"]
            and (.all | type == "array" and length == 1)
            and .all[0].id == "ollama"
            and (.all[0].models | keys | sort) == ([$primary, $alternate, $shell] | sort)
            and .all[0].models[$primary].name == $primary
            and .all[0].models[$alternate].name == $alternate
            and .all[0].models[$shell].name == $shell
        ' >/dev/null

printf '[8/8] checking installed Ollama roles, context, and GPU residency\n'
"${CURL[@]}" "${TEST_CUSTOM_OLLAMA_URL%/}/tags" \
    | jq -e \
        --arg primary "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}" \
        --arg alternate "${TEST_OLLAMA_MODEL_OPENCODE_ALTERNATE}" \
        --arg shell "${TEST_OLLAMA_MODEL_OPENCODE_SHELL}" \
        --arg custom "${TEST_OLLAMA_MODEL_CUSTOM_DEFAULT}" \
        '
            .models | type == "array"
            and any(.[]; .name == $primary)
            and any(.[]; .name == $alternate)
            and any(.[]; .name == $shell)
            and any(.[]; .name == $custom)
        ' \
        >/dev/null

OLLAMA_ROLE_MODELS=(
    "${TEST_OLLAMA_MODEL_OPENCODE_ALTERNATE}"
    "${TEST_OLLAMA_MODEL_OPENCODE_SHELL}"
    "${TEST_OLLAMA_MODEL_CUSTOM_DEFAULT}"
    "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}"
)
OLLAMA_UNIQUE_MODELS=()
for candidate in "${OLLAMA_ROLE_MODELS[@]}"; do
    duplicate=false
    for selected in "${OLLAMA_UNIQUE_MODELS[@]:-}"; do
        if [[ "${candidate}" == "${selected}" ]]; then
            duplicate=true
            break
        fi
    done
    if [[ "${duplicate}" == "false" ]]; then
        OLLAMA_UNIQUE_MODELS+=("${candidate}")
    fi
done

OLLAMA_CURRENT_MODEL=""
cleanup_loaded_ollama_model() {
    local primary_status=$?
    local cleanup_status
    trap - EXIT
    if [[ -n "${OLLAMA_CURRENT_MODEL}" ]]; then
        if unload_ollama_model "${OLLAMA_CURRENT_MODEL}"; then
            :
        else
            cleanup_status=$?
            printf 'Ollama cleanup unload/observation failed (exit %s); native residency remains unconfirmed\n' "${cleanup_status}" >&2
            if (( primary_status == 0 )); then
                primary_status="${cleanup_status}"
            fi
        fi
    fi
    exit "${primary_status}"
}
trap cleanup_loaded_ollama_model EXIT

for model in "${OLLAMA_UNIQUE_MODELS[@]}"; do
    if [[ "${model}" == "${TEST_OLLAMA_MODEL_OPENCODE_DEFAULT}" ]]; then
        load_and_verify_ollama_model "${model}" 10m
    else
        load_and_verify_ollama_model "${model}" 5m
        unload_ollama_model "${model}"
    fi
    OLLAMA_CURRENT_MODEL=""
done
trap - EXIT

printf 'strict live precheck passed\n'
