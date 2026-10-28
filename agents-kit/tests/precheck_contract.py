"""Ordinary process contracts; fixtures provide no live-service evidence."""
import base64
import http.server
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse


class PrecheckFixture:
    def __init__(self):
        self.root = Path(tempfile.mkdtemp(prefix="agent-precheck-")).resolve()
        self.server = self.thread = self.child = None
        self.release = threading.Event()
        self.stall_entered = threading.Event()
        self.requests = []
        self.calls = []
        self.thread_errors = []
        self.cleanup_errors = []
        self.observation_errors = []
        self.observed_timeout = False
        self.stdout = self.stderr = ""
        self.stall = None
        self.closed = False
        self.group_members = []

    def prepare(self, crate, stall=None, row=None, fail_load=False, fail_cleanup=False,
                auth=False, absence_reads=0, never_absent=False, tcp=None, absence_rows=None):
        self.stall = stall
        self.row = row or {"size": 1000000, "size_vram": 1000000, "context_length": 65536}
        self.fail_load = fail_load
        self.fail_cleanup = fail_cleanup
        self.failed_load = False
        self.auth = auth
        self.absence_reads = absence_reads
        self.never_absent = never_absent
        self.absence_rows = absence_rows
        self.expiring = {}
        self.early_loads = []
        scripts = self.root / "agents-kit/scripts"
        scripts.mkdir(parents=True)
        (self.root / "agents-kit/specs").mkdir()
        (self.root / ".git").touch()
        (self.root / "agents-kit/Cargo.toml").touch()
        shutil.copyfile(crate / "scripts/precheck_strict_live.sh", scripts / "precheck_strict_live.sh")
        shutil.copyfile(crate / "specs/SPECIFICATIONS.md", self.root / "agents-kit/specs/SPECIFICATIONS.md")
        spec = (crate / "specs/SPECIFICATIONS.md").read_text()
        self.codex_version = re.search(r"^- Codex App Server: `rust-v([^`]+)`$", spec, re.M).group(1)
        self.opencode_version = re.search(r"^- OpenCode Server: `v([^`]+)`$", spec, re.M).group(1)
        base_env = {"PATH": os.environ["PATH"], "LC_ALL": "C"}
        setup = subprocess.run(["bash", "-c", 'source "$1"; env -0', "fixture", str(crate / "strict-tests.env.example")], cwd=self.root, env=base_env, capture_output=True, check=True, timeout=5)
        self.env = dict(item.split("=", 1) for item in setup.stdout.decode().split("\0") if item)
        for key in ["TEST_GEMINI_API_KEY", "TEST_OPENAI_API_KEY", "TEST_ANTHROPIC_API_KEY"]:
            self.env[key] = "fixture-secret"
        home = Path(self.env["TEST_CODEX_HOME"])
        home.mkdir(parents=True)
        (home / "auth.json").write_text("{}")
        self.models = list(json.loads(self.env["OPENCODE_CONFIG_CONTENT"])["provider"]["ollama"]["models"])
        self.resident = set(self.models)
        fixture = self

        class Server(http.server.HTTPServer):
            def handle_error(self, _request, _client_address):
                # HTTPServer otherwise prints and suppresses handler exceptions.
                fixture.thread_errors.append(repr(sys.exc_info()[1]))

        self.server = Server(("127.0.0.1", 0), self.handler())
        self.server.timeout = 0.1
        endpoint = f"http://127.0.0.1:{self.server.server_port}"
        self.env.update(TEST_OPENCODE_URL=endpoint, TEST_CUSTOM_OLLAMA_URL=endpoint + "/api", TEST_CODEX_WS_URL=endpoint.replace("http:", "ws:"), CURL_HOME=str(self.root))
        if auth:
            self.env.update(TEST_OPENCODE_USERNAME="fixture", TEST_OPENCODE_PASSWORD="fixture-secret")
        (self.root / ".curlrc").write_text("")
        self.env["CURL_LOG"] = str(self.root / "curl.jsonl")
        self.bin = self.root / "bin"
        self.bin.mkdir()
        real_node = shutil.which("node")
        real_curl = shutil.which("curl")
        if not real_node or not real_curl:
            raise RuntimeError("real node and curl are required")
        self.executable("curl", f"#!{sys.executable}\nimport json,os,sys\nwith open(os.environ['CURL_LOG'],'a') as log: log.write(json.dumps(sys.argv[1:])+'\\n')\nos.execv({real_curl!r},[{real_curl!r}]+sys.argv[1:])\n")
        probe = json.dumps({"userAgent": f"fixture/{self.codex_version} native", "codexHome": str(home), "accountPresent": True, "requiresOpenaiAuth": True, "mcpCount": 0, "mcpNextCursor": None})
        self.executable("node", f"#!/bin/bash\ncase \"${{@: -1}}\" in ws://*) printf '%s\\n' {shlex.quote(probe)};; *) exec {shlex.quote(real_node)} \"$@\";; esac\n")
        self.executable("codex", f"#!/bin/sh\nprintf '%s\\n' 'codex-cli {self.codex_version}'\n")
        self.executable("opencode", f"#!/bin/sh\nprintf '%s\\n' '{self.opencode_version}'\n")
        # Retain the original real TCP prerequisite only for before-script runs.
        # The repaired script neither requires nor invokes this executable.
        real_nc = shutil.which("nc")
        self.executable("nc", f"#!/bin/sh\nexec {shlex.quote(real_nc)} \"$@\"\n" if real_nc else "#!/bin/sh\nexit 127\n")
        self.executable("lsof", "#!/bin/sh\nexit 1\n")
        # Baseline CLI control agrees with its /ps counters. The repaired path
        # uses real curl directly; no CLI success is used as repaired evidence.
        size, vram = self.row["size"], self.row["size_vram"]
        integers = type(size) is int and type(vram) is int
        if not integers:
            self.executable("ollama", "#!/bin/sh\nexit 1\n")
        else:
            processor = "100% GPU" if 0 < vram == size else "mixed-or-invalid"
            rows = "\n".join(f"{model} fixture 1GB {processor} never" for model in self.models)
            self.executable("ollama", f"#!/bin/sh\nprintf '%s\\n' {shlex.quote(rows)}\n")
        self.env["PATH"] = str(self.bin) + os.pathsep + os.environ["PATH"]
        if tcp == "refused":
            with socket.socket() as reserved:
                reserved.bind(("127.0.0.1", 0))
                self.env["TEST_CODEX_WS_URL"] = f"ws://127.0.0.1:{reserved.getsockname()[1]}"
        elif tcp == "pending":
            # Exercise the real Node timer with a controlled non-completing socket.
            # This is an orchestration control, not network/service evidence.
            hook = self.root / "pending-socket.cjs"
            hook.write_text("const net = require('node:net');\nconst {EventEmitter} = require('node:events');\nconst fs = require('node:fs');\nnet.connect = () => {\n  fs.writeFileSync(process.env.TCP_ENTERED, 'entered');\n  const socket = new EventEmitter();\n  socket.destroy = () => fs.writeFileSync(process.env.TCP_DESTROYED, 'destroyed');\n  return socket;\n};\n")
            self.env.update(NODE_OPTIONS=f"--require={hook}", TEST_CODEX_WS_URL="ws://fixture.invalid:43210", TCP_ENTERED=str(self.root / "tcp-entered"), TCP_DESTROYED=str(self.root / "tcp-destroyed"))
        self.thread = threading.Thread(target=self.serve, name="precheck-http")
        self.thread.start()

    def executable(self, name, content):
        path = self.bin / name
        path.write_text(content)
        path.chmod(0o700)

    def handler(self):
        fixture = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def setup(self):
                super().setup()
                self.connection.settimeout(2)

            def log_message(self, *_args):
                pass

            def do_GET(self):
                self.respond(None)

            def do_POST(self):
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 16384:
                    raise ValueError("bounded request body required")
                self.respond(json.loads(self.rfile.read(size)))

            def respond(self, body):
                path = urllib.parse.urlsplit(self.path).path
                fixture.requests.append((self.command, path, body))
                if fixture.auth and not path.startswith("/api/"):
                    expected = "Basic " + base64.b64encode(b"fixture:fixture-secret").decode()
                    if self.headers.get("Authorization") != expected:
                        self.send_error(401)
                        return
                if fixture.stall and path == fixture.stall[0]:
                    if fixture.stall[1] == "body":
                        self.send_response(200)
                        self.send_header("Content-Length", "100")
                        self.end_headers()
                        self.wfile.write(b'{"models":')
                        self.wfile.flush()
                    fixture.stall_entered.set()
                    if not fixture.release.wait(20):
                        raise TimeoutError("fixture watchdog expired")
                    return
                status = 200
                if path == "/global/health":
                    value = {"healthy": True, "version": fixture.opencode_version}
                elif path == "/experimental/capabilities":
                    value = {"backgroundSubagents": False}
                elif path == "/config":
                    value = json.loads(fixture.env["OPENCODE_CONFIG_CONTENT"])
                elif path == "/provider":
                    value = {"connected": ["ollama"], "all": [{"id": "ollama", "models": {m: {"name": m} for m in fixture.models}}]}
                elif path == "/api/tags":
                    value = {"models": [{"name": m} for m in fixture.models]}
                elif path == "/api/ps":
                    for model, remaining in list(fixture.expiring.items()):
                        if remaining == 0:
                            fixture.resident.discard(model)
                            del fixture.expiring[model]
                        elif not fixture.never_absent:
                            fixture.expiring[model] = remaining - 1
                    value = {"models": fixture.absence_rows if fixture.absence_rows is not None else [dict(fixture.row, name=m) for m in fixture.models if m in fixture.resident]}
                elif path == "/api/generate":
                    if body["prompt"] != "" or body["stream"] is not False or "options" in body:
                        raise ValueError("precheck must retain empty load without context override")
                    unload = body["keep_alive"] == 0
                    model = body["model"]
                    if not unload and model in fixture.resident:
                        fixture.early_loads.append(model)
                    if not unload and fixture.fail_load:
                        fixture.failed_load = True
                        status = 500
                    elif unload and fixture.failed_load and fixture.fail_cleanup:
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                        return
                    elif unload:
                        if fixture.absence_reads == 0 and not fixture.never_absent:
                            fixture.resident.discard(model)
                            fixture.expiring.pop(model, None)
                        else:
                            fixture.expiring[model] = max(1, fixture.absence_reads)
                    else:
                        fixture.resident.add(model)
                    value = {"done": True, "done_reason": "unload" if unload else "load"}
                else:
                    raise ValueError("unexpected precheck endpoint")
                data = json.dumps(value).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        return Handler

    def serve(self):
        try:
            while not self.release.is_set():
                self.server.handle_request()
        except BaseException as error:
            self.thread_errors.append(repr(error))

    def observe_group(self):
        if self.child is None:
            return []
        result = subprocess.run(["ps", "-axo", "pid=,pgid="], capture_output=True, text=True, check=True, timeout=3)
        return [int(pid) for pid, group in (line.split() for line in result.stdout.splitlines()) if int(group) == self.child.pid]

    def settle(self):
        self.release.set()
        if self.child is not None and self.child.poll() is None:
            try:
                self.child.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.cleanup_errors.append("verifier process group required forced cleanup")
                try:
                    os.killpg(self.child.pid, signal.SIGKILL)
                    self.child.wait(timeout=3)
                except (OSError, subprocess.TimeoutExpired) as error:
                    self.cleanup_errors.append(repr(error))
        if self.child is not None:
            try:
                deadline = time.monotonic() + 3
                while True:
                    self.group_members = self.observe_group()
                    if not self.group_members or time.monotonic() >= deadline:
                        break
                    time.sleep(0.05)
                if self.group_members:
                    self.cleanup_errors.append("owned process-group descendants required forced cleanup")
                    os.killpg(self.child.pid, signal.SIGKILL)
                    deadline = time.monotonic() + 3
                    while self.group_members and time.monotonic() < deadline:
                        time.sleep(0.05)
                        self.group_members = self.observe_group()
            except (OSError, subprocess.SubprocessError) as error:
                self.cleanup_errors.append(repr(error))
                self.group_members = ["unconfirmed"]
        if self.thread is not None and self.thread.ident is not None:
            self.thread.join(timeout=3)
            if self.thread.is_alive():
                self.cleanup_errors.append("HTTP owner did not join")
        if self.server is not None:
            self.server.server_close()

    def run(self, watchdog=15):
        out = self.root / "stdout"
        err = self.root / "stderr"
        try:
            with out.open("wb") as stdout, err.open("wb") as stderr:
                self.child = subprocess.Popen(["bash", str(self.root / "agents-kit/scripts/precheck_strict_live.sh")], cwd=self.root, env=self.env, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr, start_new_session=True)
                if self.stall and not self.stall_entered.wait(5):
                    self.observation_errors.append("configured stall request was not reached")
                try:
                    self.child.wait(timeout=watchdog)
                except subprocess.TimeoutExpired:
                    self.observed_timeout = True
        except Exception as error:
            self.observation_errors.append(repr(error))
        finally:
            self.settle()
        for path, attribute in [(out, "stdout"), (err, "stderr")]:
            try:
                setattr(self, attribute, path.read_text())
            except OSError as error:
                self.observation_errors.append(repr(error))
        try:
            log = self.root / "curl.jsonl"
            self.calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
        except (OSError, ValueError) as error:
            self.observation_errors.append(repr(error))
        return self.child.returncode if self.child is not None else None

    def facts(self):
        return json.dumps({"root": str(self.root), "returncode": self.child.returncode if self.child else None, "timeout": self.observed_timeout, "stall_entered": self.stall_entered.is_set(), "observation_errors": self.observation_errors, "cleanup_errors": self.cleanup_errors, "thread_errors": self.thread_errors, "group_members": self.group_members, "early_loads": getattr(self, "early_loads", []), "requests": self.requests, "stdout": self.stdout, "stderr": self.stderr}, indent=2)

    def close(self):
        if self.closed:
            return
        self.settle()
        joined = (self.child is None or self.child.poll() is not None) and (self.thread is None or not self.thread.is_alive())
        if not joined or self.group_members:
            raise AssertionError(f"owned resources remain; retained fixture {self.root}\n{self.facts()}")
        shutil.rmtree(self.root)
        self.closed = True


class PrecheckContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.crate = Path(__file__).resolve().parents[1]

    def test_example_exports_explicit_codex_model_and_effort(self):
        # A child process must receive the template's own exports. Neither the
        # developer's environment nor shell auto-export may fill omissions.
        observe = "import json,os; print(json.dumps({key: os.environ.get(key) for key in ('TEST_CODEX_MODEL_DEFAULT', 'TEST_CODEX_REASONING_EFFORT')}))"
        child = subprocess.run(
            ["bash", "-c", 'source "$1"; exec "$2" -c "$3"', "fixture",
             str(self.crate / "strict-tests.env.example"), sys.executable, observe],
            cwd=self.crate.parent, env={"PATH": os.environ["PATH"], "LC_ALL": "C"},
            capture_output=True, text=True, check=True, timeout=5)
        self.assertEqual(json.loads(child.stdout), {
            "TEST_CODEX_MODEL_DEFAULT": "gpt-6-luna",
            "TEST_CODEX_REASONING_EFFORT": "high",
        })

    def test_example_prepends_selected_release_binary_paths(self):
        spec = (self.crate / "specs/SPECIFICATIONS.md").read_text()
        codex = re.search(r"^- Codex App Server: `(rust-v[^`]+)`$", spec, re.M).group(1)
        opencode = re.search(r"^- OpenCode Server: `(v[^`]+)`$", spec, re.M).group(1)
        base_path = os.pathsep.join(("/usr/bin", "/bin"))
        observe = "import json,os; print(json.dumps(os.environ['PATH'].split(os.pathsep)))"
        child = subprocess.run(
            ["bash", "-c", 'source "$1"; exec "$2" -c "$3"', "fixture",
             str(self.crate / "strict-tests.env.example"), sys.executable, observe],
            cwd=self.crate.parent, env={"PATH": base_path, "LC_ALL": "C"},
            capture_output=True, text=True, check=True, timeout=5)
        self.assertEqual(json.loads(child.stdout), [
            str(self.crate.parent / "target/codex-releases" / codex / "bin"),
            str(self.crate.parent / "target/opencode-releases" / opencode / "bin"),
            *base_path.split(os.pathsep),
        ])

    def fixture(self, **kwargs):
        fixture = PrecheckFixture()
        self.addCleanup(fixture.close)
        fixture.prepare(self.crate, **kwargs)
        return fixture

    def assert_observed_and_clean(self, fixture):
        facts = fixture.facts()
        # Emit both outcomes first, then preserve the primary observation order.
        if fixture.observation_errors or fixture.observed_timeout or fixture.cleanup_errors or fixture.thread_errors:
            print(facts, file=sys.stderr)
        if fixture.stall:
            self.assertTrue(fixture.stall_entered.is_set(), facts)
        self.assertEqual(fixture.observation_errors, [], facts)
        self.assertFalse(fixture.observed_timeout, "precheck exceeded independent watchdog\n" + facts)
        self.assertEqual(fixture.thread_errors, [], facts)
        self.assertEqual(fixture.cleanup_errors, [], facts)
        self.assertIsNotNone(fixture.child, facts)
        self.assertIsNotNone(fixture.child.returncode, facts)

    def test_current_provider_roles_fail_before_network(self):
        for role in ["TEST_CODEX_REASONING_EFFORT", "TEST_OPENAI_API_KEY",
                     "TEST_OPENAI_BASE_URL", "TEST_ANTHROPIC_API_KEY",
                     "TEST_ANTHROPIC_BASE_URL", "TEST_CUSTOM_OPENAI_MODEL",
                     "TEST_CUSTOM_ANTHROPIC_MODEL", "TEST_OPENCODE_CLOUD_MODEL"]:
            with self.subTest(role=role):
                fixture = self.fixture()
                fixture.env.pop(role)
                fixture.run()
                self.assert_observed_and_clean(fixture)
                self.assertNotEqual(fixture.child.returncode, 0, fixture.facts())
                self.assertIn(role, fixture.stderr, fixture.facts())
                self.assertEqual(fixture.requests, [], fixture.facts())
                fixture.close()

    def test_real_curl_bounds_stalled_opencode_headers_with_auth(self):
        fixture = self.fixture(stall=("/global/health", "headers"), auth=True)
        code = fixture.run()
        self.assert_observed_and_clean(fixture)
        self.assertNotEqual(code, 0, fixture.facts())
        self.assertIn("curl: (28)", fixture.stderr)
        self.assertNotIn("strict live precheck passed", fixture.stdout)
        self.assertEqual([path for _, path, _ in fixture.requests], ["/global/health"])

    def test_real_curl_bounds_incomplete_ollama_tags_body(self):
        fixture = self.fixture(stall=("/api/tags", "body"))
        code = fixture.run()
        self.assert_observed_and_clean(fixture)
        self.assertNotEqual(code, 0, fixture.facts())
        self.assertIn("curl: (28)", fixture.stderr)
        self.assertNotIn("/api/generate", [path for _, path, _ in fixture.requests])

    def test_full_gpu_and_cold_load_budget_contract(self):
        fixture = self.fixture()
        code = fixture.run()
        self.assert_observed_and_clean(fixture)
        self.assertEqual(code, 0, fixture.facts())
        self.assertIn("strict live precheck passed", fixture.stdout)
        for args in fixture.calls:
            self.assertIn("--connect-timeout", args)
            self.assertIn("--retry", args)
            self.assertEqual(args[args.index("--connect-timeout") + 1], "2")
            self.assertEqual(args[args.index("--retry") + 1], "0")
            budgets = [args[i + 1] for i, arg in enumerate(args) if arg == "--max-time"]
            if args[-1].endswith("/generate"):
                payload = json.loads(args[args.index("--data-binary") + 1])
                self.assertEqual(budgets[-1], "30" if payload["keep_alive"] == 0 else "360")
                self.assertNotIn("options", payload)
            else:
                self.assertGreater(int(budgets[-1]), 0)
                self.assertLessEqual(int(budgets[-1]), 10)
        loads = [body for _, path, body in fixture.requests if path == "/api/generate" and body["keep_alive"] != 0]
        self.assertEqual({body["model"] for body in loads}, set(fixture.models))
        self.assertEqual(loads[-1]["model"], fixture.env["TEST_OLLAMA_MODEL_OPENCODE_DEFAULT"])
        self.assertEqual(loads[-1]["keep_alive"], "10m")

    def test_non_full_or_invalid_gpu_rows_fail(self):
        rows = [
            {"size": 1000000, "size_vram": 999999, "context_length": 65536},
            {"size": 0, "size_vram": 0, "context_length": 65536},
            {"size": 1, "size_vram": 2, "context_length": 65536},
            {"size": 1.5, "size_vram": 1.5, "context_length": 65536},
            {"size": "1", "size_vram": "1", "context_length": 65536},
            {"size": 1, "size_vram": 1, "context_length": 65535},
        ]
        for row in rows:
            with self.subTest(row=row):
                fixture = self.fixture(row=row)
                code = fixture.run()
                self.assert_observed_and_clean(fixture)
                self.assertNotEqual(code, 0, fixture.facts())
                self.assertNotIn("strict live precheck passed", fixture.stdout)

    def test_delayed_residency_disappearance_precedes_each_load(self):
        fixture = self.fixture(absence_reads=2)
        code = fixture.run()
        self.assert_observed_and_clean(fixture)
        self.assertEqual(code, 0, fixture.facts())
        self.assertEqual(fixture.early_loads, [], fixture.facts())
        # Delayed residency is native state; the assertion rejects early loads.
        previous_unload = None
        reads = 0
        for _, path, body in fixture.requests:
            if path == "/api/ps":
                reads += 1
            elif path == "/api/generate" and body["keep_alive"] == 0:
                previous_unload = body["model"]
                reads = 0
            elif path == "/api/generate":
                self.assertEqual(previous_unload, body["model"])
                self.assertGreaterEqual(reads, 3)

    def test_never_absent_residency_fails_before_any_load(self):
        fixture = self.fixture(never_absent=True)
        code = fixture.run(watchdog=40)
        self.assert_observed_and_clean(fixture)
        self.assertNotEqual(code, 0, fixture.facts())
        self.assertIn("Ollama residency removal deadline elapsed", fixture.stderr)
        mutations = [body for _, path, body in fixture.requests if path == "/api/generate"]
        self.assertEqual(len(mutations), 1)
        self.assertEqual(mutations[0]["keep_alive"], 0)

    def test_malformed_residency_inventory_cannot_authorize_load(self):
        rows = [[{}], [None], [{"name": None}], [{"name": 7}], [{"name": ""}], [{"name": " \t\n"}], [{"name": "other-model"}, {}]]
        for inventory in rows:
            with self.subTest(inventory=inventory):
                fixture = self.fixture(absence_rows=inventory)
                code = fixture.run()
                self.assert_observed_and_clean(fixture)
                self.assertNotEqual(code, 0, fixture.facts())
                mutations = [body for _, path, body in fixture.requests if path == "/api/generate"]
                self.assertEqual(len(mutations), 1, fixture.facts())
                self.assertEqual(mutations[0]["keep_alive"], 0)
                self.assertNotIn("strict live precheck passed", fixture.stdout)

    def test_embedded_storage_requires_no_alternative_service_configuration(self):
        fixture = self.fixture()
        for key in list(fixture.env):
            if key.startswith(("TEST_POSTGRES", "TEST_SURREALDB")):
                fixture.env.pop(key)
        fixture.env["TEST_POSTGRES_PSQL"] = "unavailable-unused-storage-monitor"
        code = fixture.run()
        self.assert_observed_and_clean(fixture)
        self.assertEqual(code, 0, fixture.facts())
        self.assertIn("strict live precheck passed", fixture.stdout)

    def test_tcp_connection_refusal_is_reported(self):
        fixture = self.fixture(tcp="refused")
        code = fixture.run()
        self.assert_observed_and_clean(fixture)
        self.assertNotEqual(code, 0, fixture.facts())
        self.assertIn("TCP readiness connection failed", fixture.stderr)
        self.assertEqual(fixture.requests, [])

    def test_tcp_pending_socket_timer_destroys_owner(self):
        fixture = self.fixture(tcp="pending")
        code = fixture.run()
        self.assert_observed_and_clean(fixture)
        self.assertTrue((fixture.root / "tcp-entered").exists(), fixture.facts())
        self.assertTrue((fixture.root / "tcp-destroyed").exists(), fixture.facts())
        self.assertNotEqual(code, 0, fixture.facts())
        self.assertIn("TCP readiness deadline elapsed", fixture.stderr)
        self.assertEqual(fixture.requests, [])

    def test_primary_http_failure_survives_successful_cleanup(self):
        fixture = self.fixture(fail_load=True)
        code = fixture.run()
        self.assert_observed_and_clean(fixture)
        self.assertEqual(code, 22, fixture.facts())
        self.assertNotIn("Ollama cleanup unload/observation failed", fixture.stderr)
        self.assertNotIn("strict live precheck passed", fixture.stdout)

    def test_primary_http_failure_survives_failed_cleanup(self):
        fixture = self.fixture(fail_load=True, fail_cleanup=True)
        code = fixture.run()
        self.assert_observed_and_clean(fixture)
        self.assertEqual(code, 22, fixture.facts())
        self.assertIn("Ollama cleanup unload/observation failed (exit 52)", fixture.stderr)
        self.assertIn("native residency remains unconfirmed", fixture.stderr)
        self.assertNotIn("strict live precheck passed", fixture.stdout)
        self.assertEqual(len([body for _, path, body in fixture.requests if path == "/api/generate"]), 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
