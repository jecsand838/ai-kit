#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
source "${WORKSPACE_DIR}/scripts/release_stage.sh"
COMPOSE_FILE="${WORKSPACE_DIR}/storage-kit/docker-compose.strict.yml"
cd "${WORKSPACE_DIR}"
selection="$(python3 "${SCRIPT_DIR}/feature_policy.py" "$@")"
backends="$(python3 -c 'import json,sys; print("\n".join(json.load(sys.stdin)["backends"]))' <<< "${selection}")"
printf '%s\n' "${selection}"
services=()
owned_directory=''
owned_directories=()
embedded_names=(TEST_LANCEDB_EMBEDDED_PATH)
embedded_roots=()
owned_services=0
active_command=''
active_receipt=''
cleanup_settled=1
cleanup_started=0
execution_complete=0
logs="$(mktemp -d "${TMPDIR:-/tmp}/storage-strict.XXXXXX")"

# Each validation command owns a fresh process group. The shell can observe
# interruption while waiting; its Python child settles that group's processes
# before cleanup is allowed to remove datasets or services.
run_owned() {
    local result deadline=0 pending_interrupt=0
    if [[ "${1:-}" == --deadline ]]; then deadline="$2"; shift 2; fi
    if (( ! cleanup_started )); then
        trap 'pending_interrupt=130' INT
        trap 'pending_interrupt=143' TERM
    fi
    if active_receipt="$(mktemp "${logs}/command-ownership.XXXXXX")"; then :; else
        result=$?
        if (( ! cleanup_started )); then
            trap 'exit 130' INT
            trap 'exit 143' TERM
            if (( pending_interrupt )); then exit "${pending_interrupt}"; fi
        fi
        return "${result}"
    fi
    python3 - "${deadline}" "${active_receipt}" "$@" <<'PY' &
import os, signal, subprocess, sys, time
from pathlib import Path

class StorageCommand:
    def __init__(self, arguments):
        self.deadline = float(arguments[0]) or None
        self.receipt = Path(arguments[1])
        self.arguments = arguments[2:]
        self.process = None
        self.code = 1

    def interrupted(self, signum, _frame):
        self.code = 128 + signum
        # Popen must finish transferring ownership before interruption unwinds.
        if self.process is not None:
            raise InterruptedError("Storage validation interrupted")

    def execute(self):
        signal.signal(signal.SIGTERM, self.interrupted)
        signal.signal(signal.SIGINT, self.interrupted)
        try:
            environment = dict(os.environ, AI_KIT_RELEASE_PARENT_RECEIPT=str(self.receipt))
            self.process = subprocess.Popen(self.arguments, start_new_session=True, env=environment)
            self.receipt.write_text(f"{self.process.pid}\n")
            if self.code not in (130, 143):
                self.code = self.process.wait(timeout=self.deadline)
        except InterruptedError:
            pass
        except subprocess.TimeoutExpired:
            self.code = 124
            print("owned Storage command exceeded its execution deadline", file=sys.stderr)
        finally:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            if self.process is None or self.settle():
                try:
                    # Release-stage children may own a separate process group.
                    # Their acknowledgement survives an interrupted engine and
                    # prevents deleting resources until that group is absent.
                    outstanding = list(self.receipt.parent.glob(self.receipt.name + ".release-*"))
                    if outstanding:
                        if self.code == 0: self.code = 1
                        print("release-stage child settlement remains unacknowledged; retaining Storage resources", file=sys.stderr)
                    else:
                        self.receipt.unlink()
                except OSError as error:
                    if self.code == 0: self.code = 1
                    print(f"owned Storage command receipt cleanup failed: {error}", file=sys.stderr)
        return self.code if self.code >= 0 else 1

    def settle(self):
        for signum in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(self.process.pid, signum)
            except ProcessLookupError:
                pass
            except OSError as error:
                if self.code == 0: self.code = 1
                print(f"owned Storage command cleanup failed: {error}", file=sys.stderr)
                return False
            deadline = time.monotonic() + 30
            observation_error = None
            while time.monotonic() < deadline:
                self.process.poll()  # Reap the direct child before checking its group.
                try:
                    os.killpg(self.process.pid, 0)
                except ProcessLookupError:
                    if self.process.returncode is not None:
                        return True
                except PermissionError as error:
                    # A child can exit between poll() and the group probe.
                    # Darwin can report EPERM for that zombie-only interval.
                    # Reobserve within this deadline; only absence proves settlement.
                    observation_error = error
                except OSError as error:
                    if self.code == 0: self.code = 1
                    print(f"owned Storage command settlement unknown: {error}", file=sys.stderr)
                    return False
                time.sleep(0.02)
            if observation_error is not None:
                print(f"owned Storage command settlement unknown: {observation_error}", file=sys.stderr)
            if self.code == 0: self.code = 1
            print("owned Storage command exceeded cleanup deadline", file=sys.stderr)
        # killpg(0) can still observe zombies; absence is the only portable
        # settlement witness. Retain resources rather than claiming a live or
        # fully settled group when its remaining members cannot be classified.
        print("owned Storage command group remains unsettled", file=sys.stderr)
        return False

sys.exit(StorageCommand(sys.argv[1:]).execute())
PY
    active_command=$!
    if (( ! cleanup_started )); then
        trap 'exit 130' INT
        trap 'exit 143' TERM
        if (( pending_interrupt )); then exit "${pending_interrupt}"; fi
    fi
    if wait "${active_command}"; then result=0; else result=$?; fi
    active_command=''
    if [[ -e "${active_receipt}" ]]; then cleanup_settled=0; fi
    active_receipt=''
    return "${result}"
}

require_env() {
    local name="$1"
    if [[ -z "${!name:-}" ]]; then printf 'missing required strict-live variable: %s\n' "${name}" >&2; exit 1; fi
}
cleanup() {
    local result=$? cleanup_result directory
    cleanup_started=1
    trap - EXIT
    trap '' INT TERM
    if [[ -n "${active_command}" ]]; then
        kill -TERM "${active_command}" 2>/dev/null || true
        wait "${active_command}" || true
        active_command=''
    fi
    if [[ -n "${active_receipt}" && -e "${active_receipt}" ]]; then cleanup_settled=0; fi
    active_receipt=''
    if (( cleanup_settled && owned_services )); then
        if run_owned --deadline 120 docker compose -f "${COMPOSE_FILE}" down --volumes; then :; else
            cleanup_result=$?
            printf 'strict service cleanup failed with exit %s\n' "${cleanup_result}" >&2
            if (( result == 0 )); then result="${cleanup_result}"; fi
        fi
    fi
    if (( cleanup_settled && ${#owned_directories[@]} )); then
        for directory in "${owned_directories[@]}"; do
            if rm -rf -- "${directory}"; then :; else
                cleanup_result=$?
                printf 'owned lane cleanup failed with exit %s\n' "${cleanup_result}" >&2
                if (( result == 0 )); then result="${cleanup_result}"; fi
            fi
        done
    fi
    if (( cleanup_settled )) && [[ -n "${owned_directory}" ]]; then
        if rm -rf "${owned_directory}"; then :; else
            cleanup_result=$?
            printf 'owned dataset cleanup failed with exit %s\n' "${cleanup_result}" >&2
            if (( result == 0 )); then result="${cleanup_result}"; fi
        fi
    fi
    if (( ! cleanup_settled )); then
        printf 'Command settlement is unknown; retaining owned datasets and services. Receipts: %s\n' "${logs}" >&2
        if (( owned_services )); then printf 'Retained owned Compose project: %s\n' "${COMPOSE_PROJECT_NAME}" >&2; fi
        if [[ -n "${owned_directory}" ]]; then printf 'Retained owned dataset: %s\n' "${owned_directory}" >&2; fi
        if (( result == 0 )); then result=1; fi
    fi
    if (( result == 0 && execution_complete )); then
        printf 'Storage strict-live execution PASS for selected backends.\n'
    elif (( result == 130 || result == 143 )); then
        printf 'Storage strict-live execution INCOMPLETE (interrupted).\n' >&2
    else
        printf 'Storage strict-live execution FAIL.\n' >&2
    fi
    printf 'Storage selection and target logs: %s\n'  "${logs}"
    exit "${result}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf '%s\n' "${selection}" > "${logs}/selection.json"
while IFS= read -r backend; do
    case "${backend}" in
        surrealdb)
            for name in TEST_SURREALDB_ROCKSDB_PATH TEST_SURREALDB_SURREALKV_PATH TEST_SURREALDB_REMOTE_WS TEST_SURREALDB_REMOTE_HTTP TEST_SURREALDB_ROOT_USER TEST_SURREALDB_ROOT_PASS TEST_SURREALDB_NS TEST_SURREALDB_DB; do require_env "${name}"; done
            embedded_names+=(TEST_SURREALDB_ROCKSDB_PATH TEST_SURREALDB_SURREALKV_PATH)
            services+=(surrealdb);;
        postgres)
            for name in TEST_POSTGRES_STATE_STORE_HOST TEST_POSTGRES_STATE_STORE_PORT TEST_POSTGRES_STATE_STORE_DB TEST_POSTGRES_STATE_STORE_USER TEST_POSTGRES_STATE_STORE_PASS; do require_env "${name}"; done
            services+=(postgres);;
    esac
done <<< "${backends}"
if [[ -z "${TEST_LANCEDB_EMBEDDED_PATH:-}" ]]; then
    owned_directory="$(mktemp -d "${TMPDIR:-/tmp}/storage-lance.XXXXXX")"
    export TEST_LANCEDB_EMBEDDED_PATH="${owned_directory}/database"
fi
# Bind service traffic to the endpoints this invocation will actually own.
require_compose_port() {
    local name="$1" value
    value="${!name}"
    if [[ ! "${value}" =~ ^[1-9][0-9]{0,4}$ ]] || (( 10#${value} > 65535 )); then
        printf '%s must be a TCP port for the owned Compose service\n' "${name}" >&2; exit 1
    fi
}
require_compose_endpoint() {
    local name="$1" expected="$2"
    if [[ "${!name}" != "${expected}" ]]; then
        printf '%s must match the owned Compose service local endpoint\n' "${name}" >&2; exit 1
    fi
}
if (( ${#services[@]} )); then
for service in "${services[@]}"; do
    case "${service}" in
        surrealdb)
            export TEST_SURREALDB_REMOTE_PORT="${TEST_SURREALDB_REMOTE_PORT:-8000}"
            require_compose_port TEST_SURREALDB_REMOTE_PORT
            require_compose_endpoint TEST_SURREALDB_REMOTE_WS "ws://127.0.0.1:${TEST_SURREALDB_REMOTE_PORT}"
            require_compose_endpoint TEST_SURREALDB_REMOTE_HTTP "http://127.0.0.1:${TEST_SURREALDB_REMOTE_PORT}";;
        postgres)
            require_compose_port TEST_POSTGRES_STATE_STORE_PORT
            require_compose_endpoint TEST_POSTGRES_STATE_STORE_HOST 127.0.0.1;;
    esac
done
fi
for name in "${embedded_names[@]}"; do
    embedded_roots+=("${!name}")
done
# Never acquire cleanup authority over a caller's Compose project. A fresh
# project owns only the selected services; configured ports still must be free.
if (( ${#services[@]} )); then
    export COMPOSE_PROJECT_NAME="storage-kit-strict-$$-$(date +%s)"
fi

wait_for_http() {
    local label="$1" endpoint="$2" attempt
    for attempt in {1..60}; do
        if run_owned --deadline 15 curl --connect-timeout 2 --max-time 10 --fail --silent --show-error "${endpoint}" >/dev/null 2>&1; then return; fi
        sleep 1
    done
    printf '%s did not become ready\n' "${label}" >&2; exit 1
}
wait_for_postgres() {
    local attempt
    for attempt in {1..60}; do
        if run_owned --deadline 15 docker compose -f "${COMPOSE_FILE}" exec -T postgres pg_isready --host 127.0.0.1 --port 5432 --dbname "${TEST_POSTGRES_STATE_STORE_DB}" --username "${TEST_POSTGRES_STATE_STORE_USER}" >/dev/null 2>&1; then return; fi
        sleep 1
    done
    printf 'PostgreSQL did not become ready\n' >&2; exit 1
}

run_owned bash "${SCRIPT_DIR}/check.sh" "$@"
if (( ${#services[@]} )); then
    owned_services=1
    run_owned --deadline 120 docker compose -f "${COMPOSE_FILE}" up -d "${services[@]}"
    for service in "${services[@]}"; do
        case "${service}" in
            surrealdb) wait_for_http SurrealDB "${TEST_SURREALDB_REMOTE_HTTP}/ready";;
            postgres) wait_for_postgres;;
        esac
    done
    run_owned --deadline 15 docker compose -f "${COMPOSE_FILE}" images
    for service in "${services[@]}"; do
        case "${service}" in
            surrealdb) run_owned --deadline 15 docker compose -f "${COMPOSE_FILE}" exec -T surrealdb /surreal version;;
            postgres) run_owned --deadline 15 docker compose -f "${COMPOSE_FILE}" exec -T postgres postgres --version;;
        esac
    done
fi

# Each live invocation runs a complete feature-selected target. Listing before
# execution makes empty selection visible instead of treating Cargo's exit as proof.
lanes="$(python3 "${SCRIPT_DIR}/feature_policy.py" "$@" --format lanes)"
while IFS= read -r features; do
    args=(--no-default-features)
    if [[ "${features}" != - ]]; then args+=(--features "${features}"); fi
    lane="${features//,/-}"
    if [[ "${lane}" == - ]]; then lane=lance; fi
    name="${features//,/_}"
    # Each lane receives fresh child paths. Register ownership immediately;
    # partial allocation and failed commands still use the one cleanup owner.
    for (( index=0; index<${#embedded_names[@]}; index++ )); do
        base="${embedded_roots[$index]}"
        mkdir -p -- "${base}"
        directory="$(mktemp -d "${base%/}/storage-kit-${name}.XXXXXX")"
        owned_directories+=("${directory}")
        export "${embedded_names[$index]}=${directory}"
    done
    run_owned bash "${WORKSPACE_DIR}/scripts/release_stage.sh" --fresh "storage.live.${lane}.inventory" cargo test --locked -p storage-kit --test strict_live "${args[@]}" -- --list > "${logs}/${name}.inventory"
    if run_owned bash "${WORKSPACE_DIR}/scripts/release_stage.sh" "storage.live.${lane}.execution" cargo test --locked -p storage-kit --test strict_live "${args[@]}" -- --nocapture --test-threads=1 > "${logs}/${name}.log" 2>&1; then
        cat "${logs}/${name}.log"
    else
        status=$?
        cat "${logs}/${name}.log" >&2
        exit "${status}"
    fi
    python3 - "${logs}/${name}.inventory" "${logs}/${name}.log" <<'PY'
import pathlib, re, sys
inventory = pathlib.Path(sys.argv[1]).read_text()
log = pathlib.Path(sys.argv[2]).read_text()
expected = len(re.findall(r'^.+: test$', inventory, re.M))
counts = re.findall(r'^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out', log, re.M)
if not expected or not counts or tuple(map(int, counts[-1])) != (expected, 0, 0, 0, 0):
    raise SystemExit('strict-live target did not execute its complete nonempty inventory')
PY
done <<< "${lanes}"
execution_complete=1
